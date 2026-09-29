import sqlite3
import tempfile
import unittest
from pathlib import Path

from ccc_mcp.contest_identity import notification_contest
from ccc_mcp.telegram_state import (
    claim_solution, clear_solution_state, get_solution_batch,
    normalize_solution_contests, record_first_blood, save_solution_batch,
)


class FirstBloodTests(unittest.TestCase):
    def test_instance_suffix_removed_only_at_end(self):
        self.assertEqual(
            notification_contest("training_ccc_2024_04_classic_lawn_mower_65883_2b26f9ac"),
            "training-ccc-2024-04-classic-lawn-mower",
        )
        self.assertEqual(notification_contest("training_2024_04"), "training-2024-04")

    def test_first_complete_account_wins_and_survives_solution_reset(self):
        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "sent.sqlite3"
            contest = "training-ccc-2024-04-classic-lawn-mower"
            expected = ["example", "1", "2"]
            self.assertEqual(record_first_blood(database, contest, 5, "1", expected, "a", "Alice"), (None, False))
            self.assertEqual(record_first_blood(database, contest, 5, "2", expected, "b", "Bob"), (None, False))
            self.assertEqual(record_first_blood(database, contest, 5, "2", expected, "a", "Alice"), ("Alice", True))
            self.assertEqual(record_first_blood(database, contest, 5, "1", expected, "b", "Bob"), ("Alice", False))
            clear_solution_state(database)
            self.assertEqual(record_first_blood(database, contest, 5, "1", ["1"], "b", "Bob"), ("Alice", False))

    def test_existing_instance_notification_is_reused(self):
        with tempfile.TemporaryDirectory() as root:
            database = Path(root) / "sent.sqlite3"
            old = "training_ccc_2024_04_classic_lawn_mower_65883_2b26f9ac"
            canonical = notification_contest(old)
            self.assertTrue(claim_solution(database, old, 5, "1"))
            save_solution_batch(database, old, 5, 42, ["example", "1", "2"])
            normalize_solution_contests(database)
            self.assertFalse(claim_solution(database, canonical, 5, "1"))
            self.assertEqual(get_solution_batch(database, canonical, 5)["message_id"], 42)
            with sqlite3.connect(database) as connection:
                self.assertFalse(connection.execute("SELECT 1 FROM telegram_solutions WHERE contest=?", (old,)).fetchone())
