"""Shared persistent state for Telegram solution notifications."""

import sqlite3
from pathlib import Path


def claim_solution(database: Path, contest: str, level: int, file_id: str) -> bool:
    """Atomically reserve a contest file so concurrent accounts cannot duplicate it."""
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_solutions (
                contest TEXT NOT NULL,
                level INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (contest, level, file_id)
            )
            """
        )
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO telegram_solutions (contest, level, file_id, status)
            VALUES (?, ?, ?, 'sending')
            """,
            (contest, level, file_id),
        )
        if cursor.rowcount == 1:
            return True
        cursor = connection.execute(
            """
            UPDATE telegram_solutions
            SET status = 'sending'
            WHERE contest = ? AND level = ? AND file_id = ? AND status = 'failed'
            """,
            (contest, level, file_id),
        )
        return cursor.rowcount == 1


def update_solution_status(
    database: Path, contest: str, level: int, file_id: str, status: str
):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """
            UPDATE telegram_solutions
            SET status = ?
            WHERE contest = ? AND level = ? AND file_id = ?
            """,
            (status, contest, level, file_id),
        )
