"""Multi-account HTTP MCP. Website cookies are request-local; game tokens stay in RAM."""

import hashlib
import logging
import os
import re
import tempfile
import uuid
from dataclasses import replace
from urllib.parse import parse_qs

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

logger = logging.getLogger(__name__)


class AccountMiddleware:
    def __init__(self, app, configured, client_factory=CCCClient):
        self.app = app
        self.settings = configured
        self.client_factory = client_factory
        self.game_sessions = AccountSessions()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            async def send_with_startup_notice(message):
                if message["type"] == "lifespan.startup.complete":
                    await self.notify_startup()
                await send(message)

            return await self.app(scope, receive, send_with_startup_notice)
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
        artifact_upload = scope["path"] == "/mcp/artifacts/upload"
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
                    if artifact_upload:
                        if scope["method"] != "POST":
                            return await self.reject(
                                scope,
                                receive,
                                send,
                                405,
                                "Use POST for artifact uploads",
                            )
                        content_type = next(
                            (
                                value.split(b";", 1)[0].strip().lower()
                                for key, value in scope.get("headers", [])
                                if key.lower() == b"content-type"
                            ),
                            b"",
                        )
                        if content_type != b"application/octet-stream":
                            return await self.reject(
                                scope,
                                receive,
                                send,
                                415,
                                "Upload as application/octet-stream",
                            )
                        query = parse_qs(
                            scope.get("query_string", b"").decode("ascii", "ignore")
                        )
                        filename = query.get("filename", ["solution.out"])[0]
                        if (
                            not filename
                            or filename in (".", "..")
                            or "/" in filename
                            or "\\" in filename
                            or len(filename) > 255
                        ):
                            return await self.reject(
                                scope, receive, send, 400, "Invalid filename"
                            )
                        root = service.artifacts.root
                        temporary = None
                        digest = hashlib.sha256()
                        size = 0
                        try:
                            with tempfile.NamedTemporaryFile(
                                mode="wb", dir=root, delete=False
                            ) as target:
                                temporary = target.name
                                while True:
                                    message = await receive()
                                    if message["type"] == "http.disconnect":
                                        raise ConnectionError("Upload disconnected")
                                    chunk = message.get("body", b"")
                                    size += len(chunk)
                                    if size > client.settings.max_bytes:
                                        return await self.reject(
                                            scope,
                                            receive,
                                            send,
                                            413,
                                            "File exceeds CCC_MAX_FILE_BYTES",
                                        )
                                    target.write(chunk)
                                    digest.update(chunk)
                                    if not message.get("more_body", False):
                                        break
                            artifact = uuid.uuid4().hex
                            path = root / artifact
                            os.replace(temporary, path)
                            temporary = None
                            return await JSONResponse(
                                {
                                    "artifact_id": artifact,
                                    "filename": filename,
                                    "bytes": size,
                                    "sha256": digest.hexdigest(),
                                },
                                headers={"Cache-Control": "no-store"},
                            )(scope, receive, send)
                        except ConnectionError:
                            return
                        except OSError:
                            return await self.reject(
                                scope, receive, send, 500, "Could not store artifact"
                            )
                        finally:
                            if temporary:
                                try:
                                    os.unlink(temporary)
                                except FileNotFoundError:
                                    pass
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

    async def notify_startup(self):
        if not self.settings.bot_token or not self.settings.bot_chat_id:
            logger.info(
                "Startup Telegram notification skipped: BOT_TOKEN or BOT_CHAT_ID is not configured"
            )
            return
        try:
            async with httpx.AsyncClient(timeout=min(self.settings.timeout, 10)) as client:
                response = await client.post(
                    f"https://api.telegram.org/bot{self.settings.bot_token}/sendMessage",
                    json={
                        "chat_id": self.settings.bot_chat_id,
                        "text": "✅ CodingContest MCP успешно запущен.",
                    },
                )
            body = response.json()
            if response.is_success and isinstance(body, dict) and body.get("ok") is True:
                logger.info("Startup Telegram notification sent")
                return
            description = (
                body.get("description")
                if isinstance(body, dict)
                else "Telegram returned an invalid response"
            )
            logger.warning(
                "Startup Telegram notification failed (HTTP %s): %s",
                response.status_code,
                description if isinstance(description, str) else "unknown error",
            )
        except (httpx.HTTPError, ValueError) as error:
            logger.warning(
                "Startup Telegram notification failed: %s", type(error).__name__
            )

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
