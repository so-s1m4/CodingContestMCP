"""Contest-scoped tools; importing this module registers them."""

import base64
import json
import logging
import sqlite3
from typing import Any, Literal
from urllib.parse import unquote

import httpx
from mcp.types import ImageContent

from .context import current_service
from .service import compact_progress, contest_slug, segment
from .telegram_state import claim_solution, update_solution_status
from .tools import _call, _params, local, result, tool

logger = logging.getLogger(__name__)


async def _notify_telegram_solution(
    service, contest, level, file_id, filename, payload
):
    settings = service.client.settings
    if not settings.bot_token or not settings.bot_chat_id:
        return "disabled"

    database = settings.bot_dedupe_db or settings.data_dir / "telegram-sent.sqlite3"
    slug = contest_slug(contest)
    try:
        claimed = await local(
            lambda: claim_solution(database, slug, level, str(file_id))
        )
    except (OSError, sqlite3.Error) as error:
        logger.warning(
            "Could not reserve Telegram notification: %s", type(error).__name__
        )
        return "failed"
    if not claimed:
        return "duplicate"

    def tag(value):
        safe = "".join(char if char.isalnum() or char == "_" else "_" for char in value)
        return safe.strip("_") or "unknown"

    url = f"https://api.telegram.org/bot{settings.bot_token}/sendDocument"
    caption = (
        f"Accepted solution: {slug}, level {level}, file {file_id}\n"
        f"#contest_{tag(slug)} #level_{level} #file_{tag(str(file_id))}"
    )
    status = "uncertain"
    try:
        async with httpx.AsyncClient(timeout=settings.timeout) as client:
            response = await client.post(
                url,
                data={"chat_id": settings.bot_chat_id, "caption": caption},
                files={"document": (filename, payload, "application/octet-stream")},
            )
        body = response.json()
        if isinstance(body, dict) and body.get("ok") is False:
            status = "failed"
            logger.warning(
                "Telegram solution notification failed (HTTP %s)",
                response.status_code,
            )
        elif response.is_success and isinstance(body, dict) and body.get("ok") is True:
            status = "sent"
        else:
            logger.warning(
                "Telegram solution notification returned an uncertain result (HTTP %s)",
                response.status_code,
            )
    except (httpx.HTTPError, ValueError) as error:
        logger.warning(
            "Telegram solution notification failed: %s", type(error).__name__
        )
    try:
        await local(
            lambda: update_solution_status(
                database, slug, level, str(file_id), status
            )
        )
    except (OSError, sqlite3.Error) as error:
        logger.warning(
            "Could not update Telegram notification state: %s",
            type(error).__name__,
        )
    return status


@tool(read_only=False)
async def start_training(
    query: str, mode: Literal["challenge", "exploration"] = "challenge"
):
    """Start by name, slug, UUID or CCC challenge URL. Challenge starts a timer; exploration is untimed.
    Returns training.contestName; use that value as contest in subsequent tools."""
    return await _call(lambda: current_service().start_training(query, mode))


@tool()
async def active_training():
    """List existing training sessions to resume without starting another timer."""
    return await _call(
        lambda: current_service().client.json("GET", "/api/training/active")
    )


@tool(read_only=True)
async def game_info(contest: str):
    """Get fresh progress, game metadata, inputFiles IDs and a resume reference.
    Accessible levels are hints derived from progress; CCC decides actual access."""
    return await _call(lambda: current_service().info(contest))


@tool(read_only=False)
async def prepare_level(contest: str, level: int):
    """Get fresh progress, exact inputFiles IDs, level ZIP and extracted file artifact IDs in one call.
    Then render PDF artifacts or read/download inputs. Does not start training or submit anything."""

    async def run():
        service = current_service()
        service.validate_level(level)
        info = await service.info(contest)
        archive = await service.asset(
            info["contest_slug"],
            f"/api/contestant/level/{level}/files",
            f"level-{level}.zip",
        )
        files = await local(lambda: service.artifacts.unpack(archive["artifact_id"]))
        return {
            "contest": info["contest_slug"],
            "level": level,
            "level_info": next(
                (entry for entry in info["levels"] if entry["level"] == level), None
            ),
            "participant": info["participant"],
            "archive": archive,
            "files": files,
        }

    return await _call(run)


@tool(read_only=False)
async def download_level_files(contest: str, level: int):
    """Download accessible level ZIP to an artifact. Inspect using list_archive/archive_member."""

    async def run():
        current_service().validate_level(level)
        return await current_service().asset(
            contest, f"/api/contestant/level/{level}/files", f"level-{level}.zip"
        )

    return await _call(run)


@tool(read_only=False)
async def get_level_input(contest: str, level: int, file_id: str):
    """Download one input without truncation. Read artifact contents using offsets."""

    async def run():
        current_service().validate_level(level, file_id)
        return await current_service().asset(
            contest,
            f"/api/contestant/level/{level}/input/{segment(file_id)}",
            f"level-{level}-{file_id}.in",
        )

    return await _call(run)


@tool(read_only=False)
async def get_level_sandbox(contest: str, level: int):
    """Download optional sandbox HTML as an artifact; the server does not execute it."""

    async def run():
        current_service().validate_level(level)
        return await current_service().asset(
            contest, f"/api/contestant/level/{level}/sandbox", "sandbox.html"
        )

    return await _call(run)


@tool(read_only=True)
async def read_artifact(
    artifact_id: str,
    offset: int = 0,
    length: int = 65536,
    encoding: Literal["text", "base64"] = "text",
):
    """Read a bounded byte range. Follow next_offset until null. Base64 preserves arbitrary bytes."""
    return await _call(
        lambda: local(
            lambda: current_service().artifacts.read(
                artifact_id, offset, length, encoding
            )
        )
    )


@tool(read_only=True)
async def list_archive(artifact_id: str):
    """List exact filenames and sizes in a ZIP without extracting paths."""
    return await _call(
        lambda: local(lambda: current_service().artifacts.archive(artifact_id))
    )


@tool(read_only=False)
async def archive_member(artifact_id: str, name: str):
    """Copy one exact ZIP member into its own artifact for reading, PDF extraction or submission."""
    return await _call(
        lambda: local(lambda: current_service().artifacts.member(artifact_id, name))
    )


@tool(read_only=True)
async def read_pdf(artifact_id: str, page: int = 0):
    """Extract the existing text layer from a zero-based PDF page; does not run OCR.
    Use render_pdf_page for diagrams even when text exists, or when needs_ocr is true."""
    return await _call(
        lambda: local(lambda: current_service().artifacts.pdf_text(artifact_id, page))
    )


@tool(read_only=True)
async def render_pdf_page(artifact_id: str, page: int = 0, dpi: int = 120):
    """Return a PDF page as a native MCP image for visual reading. Zero-based pages.
    Use for diagrams or when read_pdf returns needs_ocr. Requires an image-capable client/model."""

    async def run():
        data, metadata = await local(
            lambda: current_service().artifacts.pdf_image(artifact_id, page, dpi)
        )
        response = result({"ok": True, "data": metadata})
        response.content.append(
            ImageContent(
                type="image", mimeType="image/png", data=base64.b64encode(data).decode()
            )
        )
        return response

    return await _call(run)


@tool(read_only=False)
async def upload_artifact(data_base64: str, filename: str = "solution.out"):
    """Store a base64-encoded file up to CCC_MAX_FILE_BYTES; return its artifact_id.
    For large local files use python -m ccc_mcp --url <MCP_URL> upload <path> instead of generating base64 in chat."""

    def save():
        limit = current_service().client.settings.max_bytes
        if len(data_base64) > 4 * ((limit + 2) // 3):
            raise ValueError("File exceeds CCC_MAX_FILE_BYTES")
        return current_service().artifacts.save(
            base64.b64decode(data_base64, validate=True), filename
        )

    return await _call(lambda: local(save))


@tool(read_only=False)
async def submit_solution(
    contest: str,
    level: int,
    file_id: str,
    solution: str | None = None,
    artifact_id: str | None = None,
    filename: str = "solution.out",
    include_case_details: bool = False,
):
    """Submit exactly one text solution OR artifact. Use artifact_id for large outputs.
    Returns evaluation, score and cooldownSec.
    Failed-case previews are bounded and use zero-based case_index; full_result preserves the complete report.
    No automatic retries or file-ID guessing. Check evaluation.isCorrect, not only ok."""

    async def run():
        if (solution is None) == (artifact_id is None):
            raise ValueError("Supply exactly one of solution or artifact_id")
        payload = (
            solution.encode("utf-8")
            if solution is not None
            else await local(
                lambda: current_service().artifacts.path(artifact_id).read_bytes()
            )
        )
        service = current_service()
        feedback = await service.submit(contest, level, file_id, payload, filename)
        evaluation = feedback.get("evaluation") if isinstance(feedback, dict) else None
        if isinstance(evaluation, dict) and evaluation.get("isCorrect") is True:
            notification_status = await _notify_telegram_solution(
                service, contest, level, file_id, filename, payload
            )
            if isinstance(feedback, dict):
                feedback["telegram_notification"] = notification_status
        cases = evaluation.get("cases") if isinstance(evaluation, dict) else None
        if (
            isinstance(cases, list)
            and cases
            and all(isinstance(case, dict) for case in cases)
            and not include_case_details
        ):
            try:
                full = await local(
                    lambda: current_service().artifacts.save(
                        json.dumps(feedback).encode(), "submission-result.json"
                    )
                )
            except (OSError, ValueError):
                feedback["storage_warning"] = (
                    "Could not save the report; complete upstream feedback is returned inline."
                )
                return feedback
            feedback["evaluation"].pop("cases")
            failed_count = 0
            previews = []
            for index, case in enumerate(cases):
                if case.get("isCorrect"):
                    continue
                failed_count += 1
                if len(previews) >= 20:
                    continue
                encoded = json.dumps(case, ensure_ascii=False)
                previews.append(
                    {**case, "case_index": index}
                    if len(encoded) <= 1024
                    else {
                        "case_index": index,
                        "truncated": True,
                        "preview": encoded[:1024],
                    }
                )
            feedback["evaluation"].update(
                case_count=len(cases),
                failed_count=failed_count,
                failed_cases=previews,
            )
            feedback["full_result"] = full
        return feedback if include_case_details else compact_progress(feedback)

    return await _call(run)


@tool(read_only=False)
async def ccc_api_request(
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
    path: str,
    query: dict[str, Any] | None = None,
    body: Any = None,
    response_format: Literal["json", "file"] = "json",
    filename: str = "response.bin",
):
    """Platform API escape hatch. Non-GET requires CCC_ENABLE_RAW_WRITES=1.
    Use dedicated tools for ordinary operations. Account permissions still apply."""

    async def run():
        if method != "GET" and not current_service().client.settings.enable_raw_writes:
            raise ValueError("Set CCC_ENABLE_RAW_WRITES=1 for generic mutations")
        decoded = path
        for _ in range(4):
            decoded = unquote(decoded)
        if (
            decoded.split("?")[0].startswith("/api/auth/")
            and decoded.split("?")[0] != "/api/auth/current-user"
        ):
            raise ValueError(
                "Raw authentication endpoints are disabled"
            )
        response = await current_service().client.request(
            method, path, params=_params(query), json_body=body
        )
        if response_format == "file":
            return current_service().artifacts.save(response.content, filename)
        return response.json() if response.content else None

    return await _call(run)


@tool(read_only=False)
async def game_api_request(
    contest: str,
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
    path: str,
    query: dict[str, Any] | None = None,
    body: Any = None,
    response_format: Literal["json", "file"] = "json",
    filename: str = "response.bin",
):
    """Game API escape hatch scoped to contest. Non-GET requires CCC_ENABLE_RAW_WRITES=1."""

    async def run():
        if method != "GET" and not current_service().client.settings.enable_raw_writes:
            raise ValueError("Set CCC_ENABLE_RAW_WRITES=1 for generic mutations")
        response = await current_service().request(
            contest, method, path, params=_params(query), json_body=body
        )
        if response_format == "file":
            return current_service().artifacts.save(response.content, filename)
        return response.json() if response.content else None

    return await _call(run)
