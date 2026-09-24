"""Contest-scoped tools; importing this module registers them."""

import base64
import asyncio
import json
import logging
import sqlite3
import tempfile
import traceback
import zipfile
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote

import httpx
from mcp.types import ImageContent

from .context import current_service, current_team_room
from .download_links import download_links
from .service import compact_progress, contest_slug, segment
from .session_pools import SessionPools
from .telegram_state import (
    claim_solution,
    get_progress_message,
    get_solution_batch,
    save_solution_batch,
    save_progress_message,
    save_solution_payload,
    solution_progress,
    solution_payloads,
    update_solution_status,
)
from .tools import _call, _params, local, result, tool

logger = logging.getLogger(__name__)
_telegram_batch_locks = {}


async def _notify_telegram_solution(
    service, contest, level, file_id, filename, payload
):
    settings = service.client.settings
    if not settings.bot_token or not settings.bot_chat_id:
        return "disabled: configure BOT_TOKEN and BOT_CHAT_ID on the MCP server"

    database = settings.bot_dedupe_db or settings.data_dir / "telegram-sent.sqlite3"
    slug = contest_slug(contest)
    def tag(value):
        safe = "".join(char if char.isalnum() or char == "_" else "_" for char in value)
        return safe.strip("_") or "unknown"

    lock_key = (str(database.resolve()), slug, level)
    lock = _telegram_batch_locks.setdefault(lock_key, asyncio.Lock())
    async with lock:
        try:
            claimed = await local(
                lambda: claim_solution(database, slug, level, str(file_id))
            )
            if not claimed:
                return "duplicate"
            await local(
                lambda: save_solution_payload(
                    database, slug, level, str(file_id), filename, payload
                )
            )
            batch = await local(lambda: get_solution_batch(database, slug, level))
            expected_files = list(
                dict.fromkeys(
                    str(item)
                    for item in batch["expected_files"]
                    if isinstance(item, (str, int))
                )
            )
            if not expected_files:
                try:
                    info = await service.info(contest)
                    level_info = next(
                        (
                            item
                            for item in info.get("levels", [])
                            if item.get("level") == level
                        ),
                        {},
                    )
                    expected_files = [
                        str(item)
                        for item in level_info.get("inputFiles", [])
                        if isinstance(item, (str, int))
                    ]
                    expected_files = list(dict.fromkeys(expected_files))
                except Exception as error:
                    logger.info(
                        "Could not load expected Telegram batch files: %s",
                        type(error).__name__,
                    )
            entries = await local(lambda: solution_payloads(database, slug, level))
            if not entries:
                return "failed"

            accepted_ids = {entry["file_id"] for entry in entries}
            if expected_files:
                passed = len(accepted_ids.intersection(expected_files))
                progress = f"{passed}/{len(expected_files)} files passed"
                missing_expected = [
                    item for item in expected_files if item not in accepted_ids
                ]
                if missing_expected:
                    logger.info(
                        "Telegram solution bundle incomplete contest=%s level=%s accepted=%s expected=%s missing_file_ids=%s",
                        slug, level, passed, len(expected_files), missing_expected,
                    )
            else:
                progress = f"{len(entries)} files passed"
            caption = (
                f"✅ {slug} · Level {level} · {progress}\n"
                f"#contest_{tag(slug)} #level_{level}"
            )

            def build_archive():
                with tempfile.NamedTemporaryFile(
                    suffix=".zip", dir=database.parent, delete=False
                ) as archive_file:
                    archive_path = Path(archive_file.name)
                manifest = [
                    f"Contest: {slug}",
                    f"Level: {level}",
                    f"Progress: {progress}",
                    "",
                    "Accepted files:",
                ]
                with zipfile.ZipFile(
                    archive_path, "w", compression=zipfile.ZIP_DEFLATED
                ) as archive:
                    for entry in entries:
                        safe_id = tag(entry["file_id"])
                        safe_name = Path(entry["filename"]).name
                        safe_name = "".join(
                            char
                            if char.isalnum() or char in "._-"
                            else "_"
                            for char in safe_name
                        ) or "solution.out"
                        archive.write(
                            entry["path"], f"answers/{safe_id}-{safe_name}"
                        )
                        manifest.append(
                            f"- {entry['file_id']}: {entry['filename']} (accepted)"
                        )
                    archive.writestr("summary.txt", "\n".join(manifest) + "\n")
                return archive_path

            archive_path = await local(build_archive)
            method = (
                "sendDocument"
                if not batch["message_id"]
                else "editMessageMedia"
            )
            url = f"https://api.telegram.org/bot{settings.bot_token}/{method}"
            if method == "editMessageMedia":
                media = json.dumps(
                    {
                        "type": "document",
                        "media": "attach://document",
                        "caption": caption,
                    }
                )
                data = {
                    "chat_id": settings.bot_chat_id,
                    "message_id": batch["message_id"],
                    "media": media,
                }
            else:
                data = {"chat_id": settings.bot_chat_id, "caption": caption}
            status = "uncertain"
            detail = None
            try:
                async with httpx.AsyncClient(timeout=settings.timeout) as client:
                    with archive_path.open("rb") as document:
                        response = await client.post(
                            url,
                            data=data,
                            files={
                                "document": (
                                    f"{tag(slug)}-level-{level}-solutions.zip",
                                    document,
                                    "application/zip",
                                )
                            },
                        )
                body = response.json()
                result_body = body.get("result") if isinstance(body, dict) else None
                message_id = (
                    result_body.get("message_id")
                    if isinstance(result_body, dict)
                    else batch["message_id"]
                )
                if (
                    response.is_success
                    and isinstance(body, dict)
                    and body.get("ok") is True
                    and isinstance(message_id, int)
                ):
                    await local(
                        lambda: save_solution_batch(
                            database,
                            slug,
                            level,
                            message_id,
                            expected_files,
                        )
                    )
                    status = "updated" if batch["message_id"] else "sent"
                elif isinstance(body, dict) and body.get("ok") is False:
                    status = "failed"
                    description = body.get("description")
                    detail = (
                        description[:300]
                        if isinstance(description, str)
                        else "Telegram Bot API rejected the request"
                    )
                    logger.warning(
                        "Telegram level bundle failed (HTTP %s): %s",
                        response.status_code,
                        detail,
                    )
                else:
                    logger.warning(
                        "Telegram level bundle returned an uncertain result (HTTP %s)",
                        response.status_code,
                    )
            except (httpx.HTTPError, ValueError, OSError) as error:
                logger.warning(
                    "Telegram level bundle failed: %s", type(error).__name__
                )
            finally:
                try:
                    archive_path.unlink()
                except FileNotFoundError:
                    pass
            await local(
                lambda: update_solution_status(
                    database, slug, level, str(file_id), status
                )
            )
            return f"{status}: {detail}" if detail else status
        except (OSError, sqlite3.Error) as error:
            logger.warning(
                "Could not update Telegram level bundle: %s", type(error).__name__
            )
            try:
                await local(
                    lambda: update_solution_status(
                        database, slug, level, str(file_id), "failed"
                    )
                )
            except (OSError, sqlite3.Error):
                pass
            return "failed"


async def update_telegram_progress(configured, reset: bool = False):
    """Keep one Telegram message current with this deployment's solved levels."""
    if not configured.bot_token or not configured.bot_chat_id:
        return
    database = configured.bot_dedupe_db or configured.data_dir / "telegram-sent.sqlite3"
    progress_database = configured.data_dir / "telegram-progress.sqlite3"
    try:
        current = await local(lambda: get_progress_message(progress_database))
        if reset:
            text = "🔄 Новый деплой. Прогресс и сохранённые ответы сброшены. Решённых уровней пока нет."
        else:
            levels = await local(lambda: solution_progress(database))
            lines = ["📊 Решения в текущем запуске:"]
            for item in levels:
                count = (
                    f"{item['accepted']}/{item['expected']} файлов"
                    if item["expected"]
                    else f"{item['accepted']} файлов"
                )
                mark = "✅" if item["complete"] else "⏳"
                lines.append(
                    f"{mark} {item['contest']} · Level {item['level']} · {count}"
                )
            if not levels:
                lines.append("Решённых уровней пока нет.")
            text = "\n".join(lines)

        async with httpx.AsyncClient(timeout=min(configured.timeout, 15)) as client:
            if current and current["chat_id"] == str(configured.bot_chat_id):
                response = await client.post(
                    f"https://api.telegram.org/bot{configured.bot_token}/editMessageText",
                    json={
                        "chat_id": configured.bot_chat_id,
                        "message_id": current["message_id"],
                        "text": text,
                    },
                )
                body = response.json()
                if response.is_success and isinstance(body, dict) and body.get("ok") is True:
                    logger.info(
                        "Telegram progress message updated chat=%s message_id=%s reset=%s",
                        configured.bot_chat_id, current["message_id"], reset,
                    )
                    return
                description = body.get("description") if isinstance(body, dict) else None
                if isinstance(description, str) and description.startswith(
                    "Bad Request: message is not modified"
                ):
                    logger.info(
                        "Telegram progress message already has the current content chat=%s message_id=%s",
                        configured.bot_chat_id, current["message_id"],
                    )
                    return
                logger.info(
                    "Telegram progress message edit failed (HTTP %s): %s; sending a replacement",
                    response.status_code,
                    description if isinstance(description, str) else "unknown error",
                )

            response = await client.post(
                f"https://api.telegram.org/bot{configured.bot_token}/sendMessage",
                json={"chat_id": configured.bot_chat_id, "text": text},
            )
        body = response.json()
        result_body = body.get("result") if isinstance(body, dict) else None
        message_id = result_body.get("message_id") if isinstance(result_body, dict) else None
        if response.is_success and isinstance(body, dict) and body.get("ok") is True and isinstance(message_id, int):
            await local(
                lambda: save_progress_message(
                    progress_database, str(configured.bot_chat_id), message_id
                )
            )
            logger.info(
                "Telegram progress message sent and saved chat=%s message_id=%s reset=%s",
                configured.bot_chat_id, message_id, reset,
            )
        else:
            description = body.get("description") if isinstance(body, dict) else None
            logger.warning(
                "Telegram progress message failed (HTTP %s): %s",
                response.status_code,
                description if isinstance(description, str) else "unknown error",
            )
    except (httpx.HTTPError, OSError, sqlite3.Error, ValueError) as error:
        logger.warning("Could not update Telegram progress message (%s)", type(error).__name__)


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
    """Read a bounded byte range. Follow next_offset until null. Base64 preserves arbitrary bytes.
    For files over 256 KiB that the agent needs to process, use get_artifact_download_url instead."""
    return await _call(
        lambda: local(
            lambda: current_service().artifacts.read(
                artifact_id, offset, length, encoding
            )
        )
    )


@tool(read_only=True)
async def get_artifact_download_url(artifact_id: str, filename: str):
    """Create a one-time, five-minute direct download link for a file up to 30 MiB.
    Use this for large inputs so the agent can save the file in its workspace instead of reading chunks."""

    async def run():
        service = current_service()
        path = service.artifacts.path(artifact_id)
        size = path.stat().st_size
        if size > 30 * 1024 * 1024:
            raise ValueError("Direct downloads are limited to 30 MiB")
        safe_filename = filename.replace("\\", "_").replace("/", "_")
        if not safe_filename or any(ord(char) < 32 for char in safe_filename):
            raise ValueError("Invalid filename")
        link = download_links.issue(
            path, safe_filename, service.client.settings.public_origin
        )
        return {**link, "bytes": size}

    return await _call(run)


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
    No automatic retries or file-ID guessing. Check evaluation.isCorrect, not only ok.
    Accepted solutions are queued for other accounts in X-CCC-Team-Room. If the submitting account is linked to exactly one room, that room is selected automatically; set the header when it belongs to multiple rooms."""

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
            await update_telegram_progress(service.client.settings)
            if isinstance(feedback, dict):
                feedback["telegram_notification"] = notification_status
            team_room = current_team_room()
            source_suffix = "unknown"
            try:
                account = await service.client.json(
                    "GET", "/api/auth/current-user"
                )
                account_uuid = account.get("uuid") if isinstance(account, dict) else None
                if not isinstance(account_uuid, str) or not account_uuid:
                    raise ValueError("Could not verify the submitting CCC account")
                source_suffix = account_uuid[-6:]
                pools = SessionPools(
                    service.client.settings.data_dir / "telegram-pools.sqlite3",
                    service.client.settings.bot_session_encryption_key,
                )
                if not team_room:
                    linked_rooms = await local(
                        lambda: pools.rooms_for_account(account_uuid)
                    )
                    if len(linked_rooms) == 1:
                        team_room = linked_rooms[0]
                        logger.info(
                            "Auto-selected the only linked Telegram room room=%s source=CCC…%s",
                            team_room, source_suffix,
                        )
                    elif len(linked_rooms) > 1:
                        fanout_status = (
                            "skipped: CCC account belongs to multiple rooms; "
                            "set X-CCC-Team-Room to choose one"
                        )
                        logger.warning(
                            "Accepted solution not fanned out: multiple linked rooms and no X-CCC-Team-Room header source=CCC…%s rooms=%s contest=%s level=%s file_id=%s",
                            source_suffix, linked_rooms, contest_slug(contest),
                            level, file_id,
                        )
                    else:
                        fanout_status = (
                            "skipped: CCC account is not linked to a room; connect it "
                            "or set X-CCC-Team-Room"
                        )
                        logger.info(
                            "Accepted solution not fanned out: no X-CCC-Team-Room and no linked rooms source=CCC…%s contest=%s level=%s file_id=%s",
                            source_suffix, contest_slug(contest), level, file_id,
                        )
                if team_room:
                    fanout = await local(
                        lambda: pools.enqueue_fanout(
                            team_room,
                            account_uuid,
                            contest_slug(contest),
                            level,
                            str(file_id),
                            filename,
                            payload,
                        )
                    )
                    if fanout["queued"]:
                        fanout_status = f"queued {fanout['queued']} delayed submissions"
                        logger.info(
                            "Accepted solution added to room fanout room=%s contest=%s level=%s file_id=%s source=CCC…%s queued=%s targets=%s",
                            team_room, contest_slug(contest), level, file_id,
                            source_suffix, fanout["queued"], fanout["targets"],
                        )
                    elif not fanout["targets"]:
                        fanout_status = "not queued: no other CCC accounts are connected to this room"
                        logger.warning(
                            "Accepted solution has no fanout targets room=%s contest=%s level=%s file_id=%s source=CCC…%s",
                            team_room, contest_slug(contest), level, file_id,
                            source_suffix,
                        )
                    else:
                        fanout_status = "no new jobs: these target accounts already have this submission queued or recorded"
                        logger.info(
                            "Accepted solution produced no new fanout jobs room=%s contest=%s level=%s file_id=%s source=CCC…%s targets=%s (duplicate queue keys)",
                            team_room, contest_slug(contest), level, file_id,
                            source_suffix, fanout["targets"],
                        )
            except Exception as error:
                fanout_status = f"failed: {type(error).__name__}"
                detail = str(error)
                for secret in (
                    service.client.settings.session,
                    service.client.settings.bot_token,
                    service.client.settings.bot_session_encryption_key,
                ):
                    if secret:
                        detail = detail.replace(secret, "[redacted]")
                logger.error(
                    "Accepted solution fanout failed room=%s contest=%s level=%s file_id=%s source=CCC…%s (%s): %s\n%s",
                    team_room or "unselected", contest_slug(contest), level, file_id,
                    source_suffix, type(error).__name__, detail[:1000],
                    "".join(traceback.format_tb(error.__traceback__)),
                )
                if isinstance(feedback, dict):
                    feedback["team_fanout"] = fanout_status
            else:
                if isinstance(feedback, dict):
                    feedback["team_fanout"] = fanout_status
        elif current_team_room():
            logger.info(
                "Room fanout skipped because CCC did not accept solution room=%s contest=%s level=%s file_id=%s is_correct=%s",
                current_team_room(), contest_slug(contest), level, file_id,
                evaluation.get("isCorrect") if isinstance(evaluation, dict) else None,
            )
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
