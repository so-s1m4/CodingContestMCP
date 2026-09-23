"""Multi-account HTTP MCP. Website cookies are request-local; game tokens stay in RAM."""

import hashlib
import logging
import os
import re
from dataclasses import replace

import httpx
import uvicorn
from starlette.responses import FileResponse, JSONResponse

from . import game_tools  # noqa: F401 -- registers game tools
from .client import APIError, CCCClient
from .context import account_service
from .download_links import download_links
from .service import Service
from .sessions import AccountSessions
from .tools import create_mcp, settings


class AccountMiddleware:
    def __init__(self, app, configured, client_factory=CCCClient):
        self.app = app
        self.settings = configured
        self.client_factory = client_factory
        self.game_sessions = AccountSessions()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        direct_download = re.fullmatch(
            r"/mcp/direct-download/([A-Za-z0-9_-]{40,60})", scope["path"]
        )
        if direct_download:
            if scope["method"] != "GET":
                return await self.reject(
                    scope, receive, send, 405, "Use GET for artifact downloads"
                )
            link = download_links.consume(direct_download.group(1))
            if link is None:
                return await self.reject(
                    scope, receive, send, 404, "Download link expired or already used"
                )
            path, filename = link
            return await FileResponse(
                path,
                media_type="application/octet-stream",
                headers={
                    "Cache-Control": "no-store",
                    "X-Content-Type-Options": "nosniff",
                },
                filename=filename,
            )(scope, receive, send)
        artifact_route = scope["path"].startswith("/mcp/artifacts/")
        if scope["path"].rstrip("/") != "/mcp" and not artifact_route:
            return await self.app(scope, receive, send)
        values = [
            value
            for key, value in scope.get("headers", [])
            if key.lower() == b"x-ccc-session"
        ]
        # Accept only the cookie value, never a Cookie header or arbitrary headers.
        if len(values) != 1 or not re.fullmatch(
            rb"[A-Za-z0-9_+/=%.-]{16,4096}", values[0]
        ):
            return await self.reject(
                scope,
                receive,
                send,
                401,
                "Supply your CCC SESSION cookie value in X-CCC-Session",
            )

        client = self.client_factory(
            replace(self.settings, cookie="", session=values[0].decode("ascii"))
        )
        try:
            try:
                user = await client.json("GET", "/api/auth/current-user")
                if (
                    not isinstance(user, dict)
                    or not isinstance(user.get("uuid"), str)
                    or not user["uuid"]
                ):
                    return await self.reject(
                        scope, receive, send, 401, "CCC session is not authenticated"
                    )
            except APIError as error:
                status = (
                    401
                    if error.status in (401, 403)
                    else 429
                    if error.status == 429
                    else 503
                )
                return await self.reject(
                    scope,
                    receive,
                    send,
                    status,
                    "CCC session expired or CCC authentication unavailable",
                    error.retry_after,
                )
            except (httpx.RequestError, ValueError):
                return await self.reject(
                    scope,
                    receive,
                    send,
                    503,
                    "CCC authentication unavailable; try later",
                )

            # Identity comes exclusively from CCC, not a caller-selected account ID.
            account = hashlib.sha256(user["uuid"].encode()).hexdigest()
            client.settings = replace(
                client.settings,
                data_dir=self.settings.data_dir / "accounts" / account,
                bot_dedupe_db=(
                    self.settings.bot_dedupe_db
                    or self.settings.data_dir / "telegram-sent.sqlite3"
                ),
            )
            try:
                with self.game_sessions.use(account) as games:
                    service = Service(client, games)
                    if artifact_route:
                        if scope["method"] not in ("GET", "HEAD"):
                            return await self.reject(
                                scope, receive, send, 405, "Use GET or HEAD"
                            )
                        try:
                            path = service.artifacts.path(
                                scope["path"].removeprefix("/mcp/artifacts/")
                            )
                        except ValueError:
                            return await self.reject(
                                scope, receive, send, 404, "Artifact not found"
                            )
                        return await FileResponse(
                            path,
                            media_type="application/octet-stream",
                            headers={
                                "Cache-Control": "no-store",
                                "X-Content-Type-Options": "nosniff",
                            },
                            filename=path.name,
                        )(scope, receive, send)
                    context_token = account_service.set(service)
                    try:
                        await self.app(scope, receive, send)
                    finally:
                        account_service.reset(context_token)
            except APIError as error:
                await self.reject(
                    scope,
                    receive,
                    send,
                    error.status,
                    str(error.detail),
                    error.retry_after,
                )
        finally:
            await client.close()

    @staticmethod
    async def reject(scope, receive, send, status, message, retry_after=None):
        headers = {"Cache-Control": "no-store"}
        if retry_after:
            headers["Retry-After"] = retry_after
        await JSONResponse(
            {"error": message, "retry_after": retry_after},
            status_code=status,
            headers=headers,
        )(scope, receive, send)


def create_app(configured=None, client_factory=CCCClient):
    return AccountMiddleware(
        create_mcp(configured or settings).streamable_http_app(),
        configured or settings,
        client_factory,
    )


def main():
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if os.getenv("MCP_TRANSPORT", "http") not in ("http", "streamable-http"):
        raise ValueError(
            "This multi-account server uses Streamable HTTP. Connect with X-CCC-Session."
        )
    uvicorn.run(
        create_app(),
        host=settings.host,
        port=settings.port,
        access_log=False,
        limit_concurrency=128,
    )
