import io
import tempfile
import unittest
import zipfile
from pathlib import Path

import httpx
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, NameObject
from starlette.testclient import TestClient

from ccc_mcp.app import create_app
from ccc_mcp.artifacts import Artifacts
from ccc_mcp.client import CCCClient
from ccc_mcp.config import Settings


class PrepareTests(unittest.TestCase):
    def test_preparation_and_binary_transfer_through_http(self):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("level-1.pdf", b"statement")
            archive.writestr("in_level-1_1-small.txt", b"5\nPR\nRR\nSS\nSR\nPS\n")
        calls = []

        def factory(settings):
            def handle(request):
                calls.append(request.url.path)
                path = request.url.path
                if path == "/api/auth/current-user":
                    return httpx.Response(200, json={"uuid": settings.session})
                if path == "/api/contests/test":
                    data = {
                        "slug": "test",
                        "gameBaseUrl": "https://birds.codingcontest.org",
                    }
                elif path == "/api/game-token":
                    data = {"token": "token"}
                elif path == "/api/games":
                    return httpx.Response(
                        200, json=[], headers={"set-cookie": "XSRF-TOKEN=csrf; Path=/"}
                    )
                elif path == "/game/game-info":
                    data = {
                        "name": "Test",
                        "levelsInfo": {"levels": [{"inputFiles": ["1-small"]}]},
                    }
                elif path == "/api/contestant/contestant-info":
                    data = {
                        "score": {
                            "gameScore": {"level": 1},
                            "state": {
                                "level1": {
                                    "submissions": [{"fileId": "old"}] * 1000,
                                    "passedFiles": {"1-small": None},
                                }
                            },
                        }
                    }
                elif path == "/api/contestant/level/1/files":
                    return httpx.Response(
                        200,
                        content=payload.getvalue(),
                        headers={"content-type": "application/zip"},
                    )
                else:
                    self.fail(path)
                return httpx.Response(200, json=data)

            return CCCClient(settings, httpx.MockTransport(handle))

        with (
            tempfile.TemporaryDirectory() as root,
            TestClient(
                create_app(Settings(data_dir=Path(root)), factory),
                base_url="http://localhost",
            ) as client,
        ):
            headers = {
                "X-CCC-Session": "a" * 32,
                "Accept": "application/json, text/event-stream",
            }
            response = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "prepare_level",
                        "arguments": {"contest": "test", "level": 1},
                    },
                },
            )
            result = response.json()["result"]
            self.assertFalse(result["isError"], result)
            data = result["structuredContent"]["data"]
            self.assertEqual(data["level_info"]["inputFiles"], ["1-small"])
            self.assertNotIn(
                "submissions", data["participant"]["score"]["state"]["level1"]
            )
            self.assertTrue(data["files"]["extracted"])
            self.assertEqual(len(data["files"]["entries"]), 2)
            small_input = next(
                entry for entry in data["files"]["entries"]
                if entry["filename"].endswith("small.txt")
            )
            self.assertEqual(small_input["content_text"], "5\nPR\nRR\nSS\nSR\nPS\n")
            self.assertFalse(small_input["content_truncated"])
            artifact = data["archive"]["artifact_id"]
            calls.clear()
            downloaded = client.get(f"/mcp/artifacts/{artifact}", headers=headers)
            self.assertEqual(downloaded.content, payload.getvalue())
            self.assertEqual(calls, ["/api/auth/current-user"])
            self.assertEqual(downloaded.headers["cache-control"], "no-store")
            self.assertEqual(client.get(f"/mcp/artifacts/{artifact}").status_code, 401)
            self.assertEqual(
                client.get(
                    f"/mcp/artifacts/{artifact}", headers={"X-CCC-Session": "b" * 32}
                ).status_code,
                404,
            )
            self.assertEqual(
                client.post(f"/mcp/artifacts/{artifact}", headers=headers).status_code,
                405,
            )

    def test_unpack_limits_and_opaque_paths(self):
        with tempfile.TemporaryDirectory() as root:
            artifacts = Artifacts(Path(root), 1000)
            for size in (5, 2000):
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
                    archive.writestr("../../escape", b"x" * size)
                stored = artifacts.save(buffer.getvalue(), "level.zip")
                unpacked = artifacts.unpack(stored["artifact_id"])
                self.assertEqual(unpacked["extracted"], size == 5)
                if unpacked["extracted"]:
                    self.assertEqual(
                        artifacts.path(
                            unpacked["entries"][0]["artifact_id"]
                        ).read_bytes(),
                        b"xxxxx",
                    )

    def test_previews_are_bounded_and_preserve_full_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            artifacts = Artifacts(Path(root), 200000)
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w") as archive:
                archive.writestr("small.txt", b"answer\n")
                archive.writestr("large.txt", b"x" * 100000)
            stored = artifacts.save(buffer.getvalue(), "level.zip")
            entries = artifacts.unpack(stored["artifact_id"], include_previews=True)["entries"]
            small, large = entries
            self.assertEqual(small["content_text"], "answer\n")
            self.assertFalse(small["content_truncated"])
            self.assertEqual(len(large["content_text"]), 4096)
            self.assertTrue(large["content_truncated"])
            self.assertEqual(
                artifacts.path(large["artifact_id"]).stat().st_size, 100000
            )

    def test_pdf_preview_uses_last_four_pages_with_original_indices(self):
        for page_count in (1, 2, 4, 7):
            with self.subTest(page_count=page_count), tempfile.TemporaryDirectory() as root:
                writer = PdfWriter()
                for index in range(page_count):
                    page = writer.add_blank_page(width=200, height=200)
                    stream = DecodedStreamObject()
                    stream.set_data(f"BT (Page {index}) Tj ET".encode())
                    page[NameObject("/Contents")] = writer._add_object(stream)
                pdf = io.BytesIO()
                writer.write(pdf)
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, "w") as archive:
                    archive.writestr("statement.pdf", pdf.getvalue())
                artifacts = Artifacts(Path(root), 200000)
                stored = artifacts.save(buffer.getvalue(), "level.zip")
                entry = artifacts.unpack(stored["artifact_id"], include_previews=True)["entries"][0]
                preview = entry["pdf_preview"]
                expected = list(range(max(0, page_count - 4), page_count))
                self.assertEqual([page["page"] for page in preview["pages"]], expected)
                self.assertEqual([page["text"] for page in preview["pages"]],
                                 [f"Page {index}" for index in expected])
                self.assertEqual(preview["total_pages"], page_count)
                self.assertEqual(preview["truncated"], page_count > 4)
                self.assertFalse(preview["needs_ocr"])
