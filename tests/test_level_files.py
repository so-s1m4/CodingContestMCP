import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from ccc_mcp.game_tools import _notify_telegram_solution
from ccc_mcp.level_files import required_level_files
from ccc_mcp.telegram_state import save_solution_batch


class LevelFilesTests(unittest.IsolatedAsyncioTestCase):
    def test_example_names_and_real_ids(self):
        self.assertEqual(
            required_level_files(["example", "level5_example.in", "EXAMPLE", 1,
                                  "1", "2-small", "counterexample", None]),
            ["1", "2-small", "counterexample"],
        )

    async def test_caption_excludes_example_from_fresh_and_saved_manifests(self):
        for cached in (False, True):
            with self.subTest(cached=cached), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "telegram.db"
                manifest = ["example", "1", "2", "3", "4", "5"]
                if cached:
                    save_solution_batch(database, "test", 5, 42, manifest)
                captions = []

                def handler(request):
                    body = request.content.decode("latin1")
                    self.assertNotIn("5/6 files passed", body)
                    captions.append(body)
                    return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    service = SimpleNamespace(
                        client=SimpleNamespace(settings=SimpleNamespace(
                            bot_token="token", bot_chat_id="chat", bot_dedupe_db=database,
                            data_dir=Path(root), timeout=10,
                        )),
                        telegram_client=client,
                        info=AsyncMock(return_value={"levels": [{"level": 5, "inputFiles": manifest}]}),
                    )
                    for file_id in range(1, 6):
                        status = await _notify_telegram_solution(
                            service, "test", 5, str(file_id), "answer.out", b"answer"
                        )
                        self.assertIn(status, ("sent", "updated"))
                self.assertIn("5/5 files passed", captions[-1])
                if cached:
                    service.info.assert_not_called()
