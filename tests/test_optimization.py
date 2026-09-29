import asyncio
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from ccc_mcp import game_tools
from ccc_mcp.app import AccountMiddleware
from ccc_mcp.client import CCCClient, SharedTransport
from ccc_mcp.config import Settings
from ccc_mcp.context import account_uuid, session_pools
from ccc_mcp.service import Service
from ccc_mcp.session_pools import SessionPools


class OptimizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_connection_pool_keeps_account_cookies_isolated(self):
        cookies = []

        def handler(request):
            cookies.append(request.headers.get("cookie", ""))
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        shared = SharedTransport(transport)
        first = CCCClient(Settings(session="a" * 32), transport=shared)
        second = CCCClient(Settings(session="b" * 32), transport=shared)
        try:
            await first.json("GET", "/api/auth/current-user")
            await first.close()
            await second.json("GET", "/api/auth/current-user")
            self.assertIn("a" * 32, cookies[0])
            self.assertNotIn("b" * 32, cookies[0])
            self.assertIn("b" * 32, cookies[1])
            self.assertNotIn("a" * 32, cookies[1])
        finally:
            await first.close()
            await second.close()
            await transport.aclose()

    async def test_accepted_delivery_is_queued_and_processed_separately(self):
        with tempfile.TemporaryDirectory() as root:
            settings = Settings(data_dir=Path(root), session="a" * 32)
            middleware = AccountMiddleware(None, settings)
            pools = middleware.session_pools
            client = CCCClient(settings, httpx.MockTransport(lambda _: self.fail("unexpected request")))
            service = Service(client)
            service.submit = AsyncMock(return_value={"evaluation": {"isCorrect": True}})
            uuid_token = account_uuid.set("account-1")
            pools_token = session_pools.set(pools)
            try:
                with patch.object(game_tools, "current_service", return_value=service):
                    result = await game_tools.submit_solution("test", 1, "1", solution="42")
                data = result.structuredContent["data"]
                self.assertTrue(data["evaluation"]["isCorrect"])
                self.assertEqual(data["telegram_notification"], "queued")
                job_id = data["delivery_job_id"]
                self.assertEqual(pools.accepted_solution_status(job_id, "account-1")["status"], "queued")
                status_result = await game_tools.solution_delivery_status(job_id)
                self.assertEqual(status_result.structuredContent["data"]["status"], "queued")
                other_token = account_uuid.set("account-2")
                try:
                    denied = await game_tools.solution_delivery_status(job_id)
                    self.assertTrue(denied.isError)
                finally:
                    account_uuid.reset(other_token)
                with sqlite3.connect(pools.database) as connection:
                    cipher = connection.execute(
                        "SELECT session_cipher FROM accepted_solution_queue WHERE id = ?", (job_id,)
                    ).fetchone()[0]
                self.assertNotIn(settings.session.encode(), cipher)

                def handler(request):
                    if request.url.path == "/api/auth/current-user":
                        return httpx.Response(200, json={"uuid": "account-1"})
                    if request.url.path == "/api/training/active":
                        return httpx.Response(200, json=[])
                    if request.url.path == "/api/games":
                        return httpx.Response(200, json=[])
                    self.fail(f"unexpected request {request.url.path}")

                middleware.shared_transport = httpx.MockTransport(handler)
                worker = asyncio.create_task(middleware.process_accepted_solutions())
                try:
                    for _ in range(100):
                        status = pools.accepted_solution_status(job_id, "account-1")
                        if status["status"] == "done":
                            break
                        await asyncio.sleep(0.02)
                    self.assertEqual(status["status"], "done")
                    self.assertTrue(status["telegram_status"].startswith("disabled:"))
                    self.assertIn("not linked", status["fanout_status"])
                finally:
                    worker.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await worker
                with sqlite3.connect(pools.database) as connection:
                    payload, cipher = connection.execute(
                        "SELECT payload, session_cipher FROM accepted_solution_queue WHERE id = ?", (job_id,)
                    ).fetchone()
                self.assertEqual(payload, b"")
                self.assertEqual(cipher, b"")
            finally:
                session_pools.reset(pools_token)
                account_uuid.reset(uuid_token)
                await client.close()
                await middleware.http_transport.aclose()
                if middleware.proxy_transport is not None:
                    await middleware.proxy_transport.aclose()
                await middleware.telegram_http.aclose()

    async def test_due_queue_respects_time_and_target_spacing(self):
        with tempfile.TemporaryDirectory() as root:
            pools = SessionPools(Path(root) / "pools.sqlite3", "")
            now = time.time()
            with sqlite3.connect(pools.database) as connection:
                for index, target, due in ((1, "a", now - 1), (2, "a", now - 1),
                                            (3, "b", now - 1), (4, "c", now + 60)):
                    connection.execute(
                        "INSERT INTO telegram_room_members (room, telegram_user_id, account_uuid, session_cipher, added_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        ("room", str(index), target + str(index), b"cipher", now),
                    )
                    connection.execute(
                        "INSERT INTO telegram_fanout_queue "
                        "(room, contest, level, file_id, filename, payload, source_uuid, target_uuid, run_after, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ("room", "test", 1, str(index), "solution.out", b"42", "source",
                         target + str(index), due, now),
                    )
                # Give the second item the same target account as the first.
                connection.execute("UPDATE telegram_fanout_queue SET target_uuid = 'a1' WHERE file_id = '2'")
                # This test starts with confirmed jobs to isolate scheduling.
                connection.execute(
                    "UPDATE telegram_fanout_queue SET approved_by = "
                    "(SELECT telegram_user_id FROM telegram_room_members m "
                    "WHERE m.room = telegram_fanout_queue.room AND m.account_uuid = target_uuid)"
                )
            first = pools.claim_due()
            self.assertEqual(first["file_id"], "1")
            second = pools.claim_due()
            self.assertEqual(second["file_id"], "3")
            self.assertIsNone(pools.claim_due())
            pools.finish_job(first["id"], "sent")
            with sqlite3.connect(pools.database) as connection:
                connection.execute("UPDATE telegram_target_cooldowns SET next_send_at = ? WHERE account_uuid = 'a1'", (now - 1,))
            resumed = pools.claim_due()
            self.assertEqual(resumed["file_id"], "2")
            pools.finish_job(resumed["id"], "sent")
            pools.finish_job(second["id"], "sent")
            self.assertIsNone(pools.claim_due())
            with sqlite3.connect(pools.database) as connection:
                connection.execute(
                    "UPDATE telegram_fanout_queue SET status = 'queued', manual = 1, "
                    "run_after = ? WHERE file_id = '3'", (now - 1,)
                )
                connection.execute(
                    "UPDATE telegram_target_cooldowns SET next_send_at = ? "
                    "WHERE account_uuid = 'b3'", (now - 1,)
                )
                connection.execute(
                    "UPDATE telegram_fanout_queue SET status = 'queued', manual = 0, "
                    "run_after = ? WHERE file_id = '1'", (now - 2,)
                )
                connection.execute(
                    "UPDATE telegram_target_cooldowns SET next_send_at = ? "
                    "WHERE account_uuid = 'a1'", (now - 1,)
                )
            self.assertEqual(pools.claim_due()["file_id"], "3")

    async def test_large_file_response_and_artifact_submission_stream(self):
        with tempfile.TemporaryDirectory() as root:
            payload = b"answer" * 200000
            uploaded = []

            def handler(request):
                if request.url.path == "/api/contests/test":
                    return httpx.Response(200, json={
                        "slug": "test", "gameBaseUrl": "https://birds.codingcontest.org"
                    })
                if request.url.path == "/api/game-token":
                    return httpx.Response(200, json={"token": "test-token"})
                if request.url.path.endswith("/files"):
                    return httpx.Response(200, content=payload,
                                          headers={"content-type": "application/zip"})
                if "/submit-" in request.url.path:
                    uploaded.append(request.content)
                    return httpx.Response(200, json={"evaluation": {"isCorrect": True}})
                self.fail(f"unexpected request {request.url.path}")

            settings = Settings(data_dir=Path(root), cookie="XSRF-TOKEN=csrf")
            client = CCCClient(settings, httpx.MockTransport(handler))
            service = Service(client)
            try:
                artifact = await asyncio.wait_for(
                    service.asset("test", "/api/contestant/level/1/files", "level.zip"), 5
                )
                self.assertEqual(artifact["bytes"], len(payload))
                path = service.artifacts.path(artifact["artifact_id"])
                with patch.object(Path, "read_bytes", side_effect=AssertionError("read_bytes called")):
                    response = await asyncio.wait_for(
                        service.submit("test", 1, "1", path, "solution.out"), 5
                    )
                self.assertTrue(response["evaluation"]["isCorrect"])
                self.assertEqual(len(uploaded), 1)
                self.assertIn(payload, uploaded[0])
            finally:
                await client.close()
