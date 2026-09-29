"""Origin-separated HTTP transport. Mutations are never retried implicitly."""

import asyncio
import json
import re
from http.cookies import SimpleCookie
from typing import Any
from urllib.parse import unquote, urlsplit
from urllib.request import proxy_bypass

import httpx

from .config import Settings

PLATFORM = "https://codingcontest.org"


class SharedTransport(httpx.AsyncBaseTransport):
    """A per-client view of a connection pool whose owner controls shutdown."""

    def __init__(self, transport: httpx.AsyncBaseTransport, proxy_transport=None):
        self.transport = transport
        self.proxy_transport = proxy_transport
        self.proxy_bypass_cache = {}

    async def handle_async_request(self, request):
        host = request.url.host
        if self.proxy_transport is not None and host not in self.proxy_bypass_cache:
            self.proxy_bypass_cache[host] = proxy_bypass(host)
        transport = (
            self.proxy_transport
            if self.proxy_transport is not None and not self.proxy_bypass_cache[host]
            else self.transport
        )
        return await transport.handle_async_request(request)

    async def aclose(self):
        pass


class APIError(RuntimeError):
    def __init__(self, status: int, detail: Any, retry_after: str | None = None):
        self.status, self.detail, self.retry_after = status, detail, retry_after
        super().__init__(f"CCC HTTP {status}")


def game_origin(url: str) -> str:
    value = urlsplit(url)
    if (
        value.scheme != "https"
        or value.username
        or value.password
        or value.port not in (None, 443)
        or value.query
        or value.fragment
        or value.path not in ("", "/")
        or not re.fullmatch(r"[a-z0-9-]+\.codingcontest\.org", value.hostname or "")
        or value.hostname == "www.codingcontest.org"
    ):
        raise ValueError("Game URL must be a HTTPS game subdomain of codingcontest.org")
    return f"https://{value.hostname}"


def api_path(path: str, allow_game_info: bool = False) -> str:
    decoded = path
    for _ in range(4):
        decoded = unquote(decoded)
    if unquote(decoded) != decoded:
        raise ValueError("Excessively encoded API path")
    parts = urlsplit(decoded)
    if (
        parts.scheme
        or parts.netloc
        or parts.fragment
        or "\\" in decoded
        or any(ord(c) < 32 for c in decoded)
        or any(p in (".", "..") for p in parts.path.split("/"))
        or "//" in parts.path
        or not (
            parts.path.startswith("/api/")
            or allow_game_info
            and parts.path == "/game/game-info"
        )
    ):
        raise ValueError(
            "Expected a /api/ path without traversal, fragment or another origin"
        )
    return path


class CCCClient:
    def __init__(self, settings: Settings, transport=None):
        self.settings = settings
        options = dict(
            timeout=settings.timeout,
            follow_redirects=False,
            transport=transport,
            headers={"User-Agent": "codingcontest-mcp/2.0"},
        )
        self.platform = httpx.AsyncClient(**options)
        self.games = httpx.AsyncClient(**options)
        cookies = SimpleCookie()
        cookies.load(settings.cookie)
        if settings.session:
            cookies["SESSION"] = settings.session
        for key, value in cookies.items():
            if key in ("SESSION", "XSRF-TOKEN"):
                self.platform.cookies.set(
                    key, value.value, domain="codingcontest.org", path="/"
                )
        self.lock = asyncio.Lock()

    async def close(self):
        await self.platform.aclose()
        await self.games.aclose()

    async def _send(self, http, method, url, *, output=None, **kwargs):
        async with http.stream(method, url, **kwargs) as response:
            return await self._consume(response, output)

    async def _consume(self, response, output=None):
        payload = bytearray()
        size = 0
        streamed = output is not None and response.is_success and "json" not in response.headers.get("content-type", "")
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > self.settings.max_bytes:
                raise ValueError("Upstream response exceeds CCC_MAX_FILE_BYTES")
            if streamed:
                output.write(chunk)
            else:
                payload.extend(chunk)
        # aiter_bytes already decodes HTTP compression. Do not decode it twice.
        headers = dict(response.headers)
        headers.pop("content-encoding", None)
        headers.pop("content-length", None)
        result = httpx.Response(
            response.status_code,
            headers=headers,
            content=bytes(payload),
            request=response.request,
        )
        if not result.is_success:
            try:
                detail = result.json()
            except ValueError:
                detail = "Non-JSON upstream error"
            raise APIError(
                result.status_code, detail, result.headers.get("retry-after")
            )
        return result

    async def request(
        self,
        method: str,
        path: str,
        *,
        origin: str | None = None,
        token: str | None = None,
        slug: str | None = None,
        params=None,
        json_body=None,
        files=None,
        output=None,
    ):
        method = method.upper()
        if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("Unsupported HTTP method")
        api_path(path, allow_game_info=origin is not None)
        kwargs: dict[str, Any] = {}
        # Do not replace an existing ?raw=true query with an empty params dict.
        if params:
            kwargs["params"] = params
        if files is not None:
            kwargs["files"] = files
        elif json_body is not None:
            kwargs["json"] = json_body
        if origin is not None:
            origin = game_origin(origin)
            headers = {"Referer": PLATFORM + "/", "Accept": "*/*"}
            if token:
                headers["Authorization"] = token  # Game protocol uses the raw token.
            if slug:
                headers["X-CCC-SLUG"] = slug
            # No cookie jar or platform credentials on game requests.
            request = self.games.build_request(
                method, origin + path, headers=headers, **kwargs
            )
            request.headers.pop("cookie", None)
            response = await self.games.send(request, stream=True)
            try:
                return await self._consume(response, output)
            finally:
                await response.aclose()
        # Serialize cookie rotation and CSRF bootstrap, including mutations.
        async with self.lock:
            if method != "GET" and not self._xsrf():
                await self._send(self.platform, "GET", PLATFORM + "/api/games")
            headers = {"Accept": "application/json"}
            if method != "GET" and self._xsrf():
                headers["X-XSRF-TOKEN"] = unquote(self._xsrf())
            return await self._send(
                self.platform, method, PLATFORM + path, headers=headers, output=output, **kwargs
            )

    def _xsrf(self):
        return next(
            (
                c.value
                for c in self.platform.cookies.jar
                if c.name == "XSRF-TOKEN"
                and c.domain.lstrip(".") == "codingcontest.org"
            ),
            "",
        )

    async def json(self, method: str, path: str, **kwargs):
        response = await self.request(method, path, **kwargs)
        if not response.content:
            return None
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError) as error:
            raise ValueError(
                "Expected JSON from CCC; endpoint may have changed"
            ) from error
