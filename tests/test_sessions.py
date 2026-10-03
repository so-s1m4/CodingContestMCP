import asyncio
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import httpx

from ccc_mcp.app import create_app
from ccc_mcp.client import APIError, CCCClient
from ccc_mcp.config import Settings
from ccc_mcp.service import Service, contest_slug
from ccc_mcp.sessions import AccountSessions, GameSessions


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_submissions_ignore_cooldown_across_agents_and_slug_aliases(self):
        with tempfile.TemporaryDirectory() as root:
            submissions = 0
            tokens = 0

            async def handler(request):
                nonlocal submissions, tokens
                if request.url.path.startswith("/api/contests/"):
                    return httpx.Response(
                        200,
                        json={
                            "slug": "training-2026-03",
                            "gameBaseUrl": "https://birds.codingcontest.org",
                        },
                    )
                if request.url.path == "/api/game-token":
                    tokens += 1
                    return httpx.Response(200, json={"token": "game-token"})
                submissions += 1
                await asyncio.sleep(0)
                return httpx.Response(
                    200, json={"evaluation": {"isCorrect": True}, "cooldownSec": 5}
                )

            clients = [
                CCCClient(
                    Settings(cookie="XSRF-TOKEN=csrf", data_dir=Path(root)),
                    httpx.MockTransport(handler),
                )
                for _ in range(2)
            ]
            state = GameSessions()
            services = [Service(client, state) for client in clients]
            try:
                outcomes = await asyncio.gather(
                    services[0].submit("training-2026.03", 1, "1", b"answer", "1.out"),
                    services[1].submit("training-2026-03", 1, "2", b"answer", "2.out"),
                    return_exceptions=True,
                )
                self.assertEqual(submissions, 2)
                self.assertEqual(tokens, 1)
                self.assertTrue(all(isinstance(outcome, dict) for outcome in outcomes))
                await services[1].submit("training-2026.03", 1, "2", b"answer", "2.out")
                self.assertEqual(submissions, 3)
            finally:
                for client in clients:
                    await client.close()

    async def test_upstream_submission_429_is_not_retried(self):
        with tempfile.TemporaryDirectory() as root:
            count = 0

            def handler(request):
                nonlocal count
                if request.url.path.startswith("/api/contests/"):
                    return httpx.Response(
                        200,
                        json={
                            "slug": "test",
                            "gameBaseUrl": "https://birds.codingcontest.org",
                        },
                    )
                if request.url.path == "/api/game-token":
                    return httpx.Response(200, json={"token": "game-token"})
                count += 1
                return httpx.Response(429, json={}, headers={"retry-after": "25"})

            client = CCCClient(
                Settings(cookie="XSRF-TOKEN=csrf", data_dir=Path(root)),
                httpx.MockTransport(handler),
            )
            state = GameSessions()
            try:
                for _ in range(2):
                    with self.assertRaises(APIError) as caught:
                        await Service(client, state).submit(
                            "test", 1, "1", b"answer", "answer.out"
                        )
                    self.assertEqual(caught.exception.retry_after, "25")
                self.assertEqual(count, 2)
            finally:
                await client.close()

    async def test_token_reuse_across_actual_http_calls_and_accounts(self):
        counts = Counter()

        def factory(settings):
            async def handler(request):
                account = settings.session
                if request.url.path == "/api/auth/current-user":
                    return httpx.Response(200, json={"uuid": account})
                if request.url.path.startswith("/api/contests/"):
                    return httpx.Response(
                        200,
                        json={
                            "slug": "test",
                            "gameBaseUrl": "https://birds.codingcontest.org",
                        },
                    )
                if request.url.path == "/api/game-token":
                    counts[account] += 1
                    await asyncio.sleep(0.01)
                    return httpx.Response(200, json={"token": "token-" + account})
                if request.url.path == "/api/games":
                    return httpx.Response(
                        200, json=[], headers={"set-cookie": "XSRF-TOKEN=csrf; Path=/"}
                    )
                self.assertEqual(request.headers["authorization"], "token-" + account)
                self.assertNotIn("cookie", request.headers)
                return httpx.Response(200, json={"account": account})

            return CCCClient(settings, httpx.MockTransport(handler))

        with tempfile.TemporaryDirectory() as root:
            app = create_app(Settings(data_dir=Path(root)), factory)
            async with app.app.router.lifespan_context(app.app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://localhost"
                ) as client:

                    async def call(account):
                        response = await client.post(
                            "/mcp",
                            headers={
                                "X-CCC-Session": account,
                                "Accept": "application/json, text/event-stream",
                            },
                            json={
                                "jsonrpc": "2.0",
                                "id": 1,
                                "method": "tools/call",
                                "params": {
                                    "name": "game_api_request",
                                    "arguments": {
                                        "contest": "test",
                                        "method": "GET",
                                        "path": "/api/game-engine/participant/state",
                                    },
                                },
                            },
                        )
                        data = response.json()["result"]
                        self.assertFalse(data["isError"], data)
                        self.assertEqual(
                            data["structuredContent"]["data"]["account"], account
                        )

                    await asyncio.gather(
                        *(call("a" * 32) for _ in range(12)), call("b" * 32)
                    )
                    await call("a" * 32)
            self.assertEqual(counts, {"a" * 32: 1, "b" * 32: 1})

    async def test_401_burst_refreshes_once_and_post_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as root:
            calls = Counter()
            old_token_requests = 0
            ready = asyncio.Event()

            async def handler(request):
                nonlocal old_token_requests
                if request.url.path.startswith("/api/contests/"):
                    return httpx.Response(
                        200,
                        json={
                            "slug": "test",
                            "gameBaseUrl": "https://birds.codingcontest.org",
                        },
                    )
                if request.url.path == "/api/game-token":
                    calls["tokens"] += 1
                    return httpx.Response(200, json={"token": str(calls["tokens"])})
                if request.method == "POST":
                    calls["submissions"] += 1
                    return httpx.Response(401, json={})
                if request.headers["authorization"] == "1":
                    old_token_requests += 1
                    if old_token_requests == 8:
                        ready.set()
                    await asyncio.wait_for(ready.wait(), 2)
                    return httpx.Response(401, json={})
                return httpx.Response(200, json={})

            state = GameSessions()
            clients = [
                CCCClient(
                    Settings(cookie="XSRF-TOKEN=csrf", data_dir=Path(root)),
                    httpx.MockTransport(handler),
                )
                for _ in range(8)
            ]
            services = [Service(client, state) for client in clients]
            try:
                await services[0].session("test")
                await asyncio.gather(
                    *(
                        service.request("test", "GET", "/api/state")
                        for service in services
                    )
                )
                self.assertEqual(calls["tokens"], 2)
                with self.assertRaises(APIError):
                    await services[0].request("test", "POST", "/api/submit")
                self.assertEqual(calls["submissions"], 1)
                await services[1].request("test", "GET", "/api/state")
                self.assertEqual(calls["tokens"], 3)
            finally:
                for client in clients:
                    await client.close()

    async def test_upstream_token_429_does_not_block_following_requests(self):
        with tempfile.TemporaryDirectory() as root:
            count = 0

            def handler(request):
                nonlocal count
                if request.url.path.startswith("/api/contests/"):
                    return httpx.Response(
                        200,
                        json={
                            "slug": request.url.path.rsplit("/", 1)[-1],
                            "gameBaseUrl": "https://birds.codingcontest.org",
                        },
                    )
                count += 1
                if count == 1:
                    return httpx.Response(429, json={}, headers={"retry-after": "27"})
                return httpx.Response(200, json={"token": "new-token"})

            client = CCCClient(
                Settings(cookie="XSRF-TOKEN=csrf", data_dir=Path(root)),
                httpx.MockTransport(handler),
            )
            state = GameSessions()
            try:
                with self.assertRaises(APIError) as caught:
                    await Service(client, state).session("test")
                self.assertEqual(caught.exception.status, 429)
                self.assertEqual(caught.exception.retry_after, "27")
                await Service(client, state).session("test")
                await Service(client, state).session("another-contest")
                self.assertEqual(count, 3)
            finally:
                await client.close()


class CacheTests(unittest.TestCase):
    def test_idle_cleanup_removes_unused_accounts(self):
        cache = AccountSessions(idle_seconds=10)
        with patch("ccc_mcp.sessions.time.monotonic", return_value=100):
            with cache.use("alice") as state:
                pass
        with patch("ccc_mcp.sessions.time.monotonic", return_value=150):
            with cache.use("alice") as replacement:
                self.assertIsNot(replacement, state)

    def test_bounded_cache_does_not_evict_active_accounts(self):
        cache = AccountSessions(limit=1)
        with cache.use("alice") as state:
            with cache.use("alice") as same:
                self.assertIs(state, same)
            with self.assertRaises(APIError):
                with cache.use("bob"):
                    pass
        with cache.use("bob"):
            self.assertNotIn("alice", cache.entries)

    def test_resume_url(self):
        self.assertEqual(
            contest_slug("https://codingcontest.org/contests/school-2026/game"),
            "school-2026",
        )
        with self.assertRaises(ValueError):
            contest_slug("https://evil.example/contests/test/game")
        with self.assertRaises(ValueError):
            contest_slug("https://codingcontest.org/challenges/birds")
