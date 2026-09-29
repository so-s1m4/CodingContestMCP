import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from cryptography.fernet import Fernet

from ccc_mcp.config import Settings
from ccc_mcp.session_pools import SessionPools
from ccc_mcp.telegram_pool_bot import TelegramPoolBot


class UploadConsentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.expected_files = ["1"]
        self.key = Fernet.generate_key().decode()
        self.pools = SessionPools(Path(self.directory.name) / "pools.db", self.key)
        self.pools.create_room("room", "password-long-enough", "1")
        for user, account in (("1", "source"), ("2", "target"), ("3", "other")):
            self.pools.save_member("room", user, account, "session-" + account)

    def enqueue(self, file_id="1", payload=b"answer"):
        self.pools.enqueue_fanout("room", "source", "contest", 5, file_id, "out.txt", payload, expected_files=self.expected_files)

    def offer_for(self, user):
        while offer := self.pools.next_upload_offer():
            self.pools.mark_upload_offer_sent(offer["id"])
            if offer["telegram_user_id"] == user:
                return offer
        self.fail("No offer for user")

    def test_consent_is_required_and_bound_to_recipient(self):
        self.expected_files = ["1", "2"]
        self.enqueue()
        self.assertIsNone(self.pools.next_upload_offer())
        self.enqueue("2")
        self.assertIsNone(self.pools.claim_due())
        offer = self.offer_for("2")
        self.assertEqual(offer["files"], 2)
        with self.assertRaises(ValueError):
            self.pools.answer_upload_offer(offer["id"], "1", True)
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", True), 2)
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", True), 0)
        job = self.pools.claim_due()
        self.assertEqual(job["target_uuid"], "target")
        self.assertIsNone(self.pools.claim_due())

    def test_unknown_manifest_does_not_offer_partial_pack(self):
        self.expected_files = None
        self.enqueue()
        self.assertIsNone(self.pools.next_upload_offer())
        self.assertIsNone(self.pools.claim_due())

    def test_incomplete_pack_does_not_block_other_levels(self):
        self.expected_files = ["1", "2"]
        self.enqueue()
        self.pools.enqueue_fanout("room", "source", "contest", 6, "1", "out.txt", b"answer", expected_files=["1"])
        self.assertEqual(self.offer_for("2")["level"], 6)

    def test_new_files_are_not_authorized_by_old_offer(self):
        self.enqueue()
        offer = self.offer_for("2")
        self.enqueue("2")
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", True), 1)
        job = self.pools.claim_due()
        self.pools.finish_job(job["id"], "sent")
        with sqlite3.connect(self.pools.database) as connection:
            connection.execute("DELETE FROM telegram_target_cooldowns")
        self.assertIsNone(self.pools.claim_due())
        self.assertEqual(self.offer_for("2")["files"], 1)

    def test_replacement_invalidates_old_offer(self):
        self.enqueue()
        offer = self.offer_for("2")
        self.enqueue(payload=b"replacement")
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", True), 0)
        self.assertIsNone(self.pools.claim_due())
        self.assertNotEqual(self.offer_for("2")["id"], offer["id"])

    def test_decline_restart_and_self_replay(self):
        self.enqueue()
        offer = self.offer_for("2")
        self.pools = SessionPools(self.pools.database, self.key)
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", False), 1)
        self.assertEqual(self.pools.answer_upload_offer(offer["id"], "2", True), 0)
        self.assertIsNone(self.pools.claim_due())
        self.pools.resend_level_to_self("room", "contest", 5, "2")
        self.assertEqual(self.pools.claim_due()["target_uuid"], "target")

    def test_owner_replay_and_disconnected_recipient_cannot_bypass_consent(self):
        self.enqueue()
        self.pools.resend_level("room", "contest", 5, ["target"], "1")
        self.assertIsNone(self.pools.claim_due())
        offer = self.offer_for("2")
        self.pools.remove_member("room", "2")
        with self.assertRaises(ValueError):
            self.pools.answer_upload_offer(offer["id"], "2", True)

    def test_legacy_queue_without_consent_stays_blocked(self):
        self.enqueue()
        with sqlite3.connect(self.pools.database) as connection:
            connection.execute("ALTER TABLE telegram_fanout_queue DROP COLUMN approved_by")
            connection.execute("ALTER TABLE telegram_fanout_queue DROP COLUMN offer_id")
        self.pools = SessionPools(self.pools.database, self.key)
        self.assertIsNone(self.pools.claim_due())
        self.assertIsNotNone(self.offer_for("2"))

    async def test_telegram_offer_retry_and_callback(self):
        self.enqueue()
        bot = TelegramPoolBot(Settings(), self.pools, telegram_client=AsyncMock())
        bot._telegram = AsyncMock(side_effect=ValueError("offline"))
        with self.assertRaises(ValueError):
            await bot._notify_upload_offer()
        self.assertIsNone(self.pools.claim_due())
        with sqlite3.connect(self.pools.database) as connection:
            connection.execute("UPDATE telegram_upload_offers SET retry_at = 0")
        bot._telegram = AsyncMock(return_value={})
        await bot._notify_upload_offer()
        args = bot._telegram.call_args.kwargs
        self.assertIn("уровень 5 от своего имени целым паком?", args["text"])
        callback_data = args["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
        await bot._handle_callback({
            "id": "callback", "from": {"id": int(args["chat_id"])},
            "message": {"message_id": 42, "chat": {"type": "private", "id": int(args["chat_id"])}},
            "data": callback_data,
        })
        self.assertIsNotNone(self.pools.claim_due())
