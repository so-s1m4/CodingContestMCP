"""MCP tools operating on the authenticated request account."""

from __future__ import annotations

import asyncio
import json
import zipfile
from typing import Any
from urllib.parse import urlsplit

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pypdf.errors import PdfReadError

from .client import APIError
from .config import Settings
from .context import current_service
from .service import segment

settings = Settings.from_env()

_registered_tools = []


def create_mcp(configured: Settings):
    mcp = FastMCP(
        "codingcontest",
        host=configured.host,
        port=configured.port,
        stateless_http=True,
        json_response=True,
        instructions="list_challenges -> start_training -> prepare_level(contest, level). "
        "For existing games use active_training or a contest slug/URL directly; no start call is needed. "
        "prepare_level returns file artifact IDs and exact inputFiles IDs. For files above 256 KiB, call get_artifact_download_url and save the one-time URL directly into the workspace instead of reading chunks. View statements with render_pdf_page. "
        "Solve locally; submit_solution accepts text or artifact_id. Check evaluation.isCorrect and cooldownSec. "
        "Optionally set X-CCC-Team-Room on the MCP connection to queue accepted solutions for linked accounts in that room. "
        "On 429 wait retry_after; never blindly repeat uncertain submissions. "
        "Large files: python -m ccc_mcp --url <MCP_URL> download <artifact_id> <path> or upload <path> "
        "with the repository installed and CCC_SESSION set locally.",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[
                "localhost",
                "127.0.0.1",
                "[::1]",
                "127.0.0.1:*",
                "localhost:*",
                "[::1]:*",
                urlsplit(configured.public_origin).netloc,
            ],
            allowed_origins=[
                "http://127.0.0.1:*",
                "http://localhost:*",
                configured.public_origin,
            ],
        ),
    )
    for fn, hints in _registered_tools:
        mcp.tool(annotations=hints, structured_output=False)(fn)
    return mcp


def tool(read_only=None):
    def register(fn):
        read = (
            read_only
            if read_only is not None
            else fn.__name__.startswith(("get_", "list_", "my_", "auth_", "active_"))
        )
        annotations = ToolAnnotations(
            readOnlyHint=read,
            destructiveHint=not read,
            idempotentHint=read,
            openWorldHint=True,
        )
        _registered_tools.append((fn, annotations))
        return fn

    return register


def result(data, error=False):
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(data, ensure_ascii=False))],
        structuredContent=data,
        isError=error,
    )


def _params(values):
    return {k: v for k, v in (values or {}).items() if v is not None}


async def _call(fn):
    try:
        data = await fn()
        return (
            data
            if isinstance(data, CallToolResult)
            else result({"ok": True, "data": data})
        )
    except APIError as error:
        hint = {
            401: "Update the X-CCC-Session header in your MCP connection.",
            403: "Check participant role, registration, contest state and CSRF.",
            429: "Respect retry_after/cooldown before retrying.",
            413: "Solution exceeds upstream file limit.",
        }.get(error.status, "Inspect current contest state.")
        return result(
            {
                "ok": False,
                "error": {
                    "status": error.status,
                    "detail": error.detail,
                    "retry_after": error.retry_after,
                    "hint": hint,
                },
            },
            True,
        )
    except httpx.RequestError:
        return result(
            {
                "ok": False,
                "error": {
                    "type": "network_error",
                    "hint": "Mutation outcome may be unknown. Inspect game_info before retrying.",
                },
            },
            True,
        )
    except (ValueError, OSError, KeyError, zipfile.BadZipFile, PdfReadError) as error:
        return result(
            {
                "ok": False,
                "error": {"type": type(error).__name__, "detail": str(error)},
            },
            True,
        )


async def local(fn):
    return await asyncio.to_thread(fn)


@tool()
async def auth_status() -> CallToolResult:
    """Return the signed-in CCC user, or an authentication error if no session is configured."""

    return await _call(
        lambda: current_service().client.json("GET", "/api/auth/current-user")
    )


@tool()
async def accept_participant_role() -> CallToolResult:
    """Enable the Participant role required to start training and join contests."""

    return await _call(
        lambda: current_service().client.json(
            "POST", "/api/auth/accept-participant-role", json_body=None
        )
    )


@tool()
async def list_challenges() -> CallToolResult:
    """List all public training games. Each item includes its slug, name, description and level count."""

    return await _call(lambda: current_service().client.json("GET", "/api/games"))


@tool()
async def list_contests(
    page: int = 0,
    size: int = 20,
    statuses: list[str] | None = None,
    search: str | None = None,
) -> CallToolResult:
    """Search contests and competitions. Statuses may include UPCOMING, OPEN_REGISTRATION, RUNNING, STATS_FROZEN and FINISHED."""

    if page < 0 or size < 1 or size > 100:
        return result(
            {
                "ok": False,
                "error": {
                    "type": "validation",
                    "detail": "page >= 0 and 1 <= size <= 100 required",
                },
            },
            True,
        )
    query: dict[str, Any] = {"page": page, "size": size, "search": search}
    if statuses:
        query["status"] = statuses
    return await _call(
        lambda: current_service().client.json(
            "GET", "/api/contests", params=_params(query)
        )
    )


@tool()
async def get_contest(contest: str) -> CallToolResult:
    """Get a contest by slug, including status, game URL, venues and team limits."""

    return await _call(
        lambda: current_service().client.json(
            "GET", f"/api/contests/{segment(contest)}"
        )
    )


@tool()
async def my_registrations() -> CallToolResult:
    """List the current user's contest registrations and team/venue choices."""

    return await _call(
        lambda: current_service().client.json("GET", "/api/contests/my-registrations")
    )
