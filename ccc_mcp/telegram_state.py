"""Shared persistent state for Telegram solution notifications."""

import hashlib
import json
import os
import sqlite3
import tempfile
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


def save_solution_payload(
    database: Path,
    contest: str,
    level: int,
    file_id: str,
    filename: str,
    payload: bytes,
):
    """Persist an accepted output until the level's Telegram bundle is complete."""
    root = database.parent / "telegram-solutions"
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f"{contest}\0{level}\0{file_id}".encode()).hexdigest()
    destination = root / f"{key}.bin"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=root, delete=False) as stream:
            temporary = stream.name
            stream.write(payload)
        os.replace(temporary, destination)
        temporary = None
        with sqlite3.connect(database, timeout=30) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_solution_payloads (
                    contest TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    file_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL,
                    PRIMARY KEY (contest, level, file_id)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO telegram_solution_payloads
                    (contest, level, file_id, filename, path)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (contest, level, file_id) DO UPDATE SET
                    filename = excluded.filename,
                    path = excluded.path
                """,
                (contest, level, file_id, filename, str(destination)),
            )
    finally:
        if temporary:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
    return str(destination)


def solution_payloads(database: Path, contest: str, level: int):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_solution_payloads (
                contest TEXT NOT NULL,
                level INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY (contest, level, file_id)
            )
            """
        )
        rows = connection.execute(
            """
            SELECT file_id, filename, path
            FROM telegram_solution_payloads
            WHERE contest = ? AND level = ?
            ORDER BY file_id
            """,
            (contest, level),
        ).fetchall()
        return [
            {"file_id": file_id, "filename": filename, "path": path}
            for file_id, filename, path in rows
            if Path(path).is_file()
        ]


def get_solution_batch(database: Path, contest: str, level: int):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_solution_batches (
                contest TEXT NOT NULL,
                level INTEGER NOT NULL,
                message_id INTEGER,
                expected_files TEXT,
                PRIMARY KEY (contest, level)
            )
            """
        )
        row = connection.execute(
            """
            SELECT message_id, expected_files
            FROM telegram_solution_batches
            WHERE contest = ? AND level = ?
            """,
            (contest, level),
        ).fetchone()
    if row is None:
        return {"message_id": None, "expected_files": []}
    try:
        expected = json.loads(row[1]) if row[1] else []
    except (TypeError, ValueError):
        expected = []
    return {
        "message_id": row[0],
        "expected_files": expected if isinstance(expected, list) else [],
    }


def clear_solution_state(database: Path):
    """Remove accepted-answer state while leaving the room/session database untouched."""
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database, timeout=30) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS telegram_solutions (
                contest TEXT NOT NULL, level INTEGER NOT NULL, file_id TEXT NOT NULL,
                status TEXT NOT NULL, PRIMARY KEY (contest, level, file_id)
            );
            CREATE TABLE IF NOT EXISTS telegram_solution_payloads (
                contest TEXT NOT NULL, level INTEGER NOT NULL, file_id TEXT NOT NULL,
                filename TEXT NOT NULL, path TEXT NOT NULL,
                PRIMARY KEY (contest, level, file_id)
            );
            CREATE TABLE IF NOT EXISTS telegram_solution_batches (
                contest TEXT NOT NULL, level INTEGER NOT NULL, message_id INTEGER,
                expected_files TEXT, PRIMARY KEY (contest, level)
            );
            """
        )
        message_ids = [
            row[0]
            for row in connection.execute(
                "SELECT message_id FROM telegram_solution_batches WHERE message_id IS NOT NULL"
            ).fetchall()
        ]
        paths = [
            Path(row[0])
            for row in connection.execute(
                "SELECT path FROM telegram_solution_payloads"
            ).fetchall()
        ]
        connection.execute("DELETE FROM telegram_solutions")
        connection.execute("DELETE FROM telegram_solution_payloads")
        connection.execute("DELETE FROM telegram_solution_batches")

    solution_dir = database.parent / "telegram-solutions"
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    if solution_dir.is_dir() and not solution_dir.is_symlink():
        for path in solution_dir.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)
        try:
            solution_dir.rmdir()
        except OSError:
            pass
    return message_ids


def solution_progress(database: Path):
    """Summarize currently retained accepted outputs for the Telegram status message."""
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS telegram_solution_payloads (
                   contest TEXT NOT NULL, level INTEGER NOT NULL, file_id TEXT NOT NULL,
                   filename TEXT NOT NULL, path TEXT NOT NULL,
                   PRIMARY KEY (contest, level, file_id)
               )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS telegram_solution_batches (
                   contest TEXT NOT NULL, level INTEGER NOT NULL, message_id INTEGER,
                   expected_files TEXT, PRIMARY KEY (contest, level)
               )"""
        )
        rows = connection.execute(
            """SELECT contest, level, file_id FROM telegram_solution_payloads
               ORDER BY contest, level, file_id"""
        ).fetchall()
        expected_rows = connection.execute(
            "SELECT contest, level, expected_files FROM telegram_solution_batches"
        ).fetchall()
    accepted = {}
    for contest, level, file_id in rows:
        accepted.setdefault((contest, level), set()).add(file_id)
    expected = {}
    for contest, level, raw in expected_rows:
        try:
            values = json.loads(raw) if raw else []
        except (TypeError, ValueError):
            values = []
        if isinstance(values, list):
            expected[(contest, level)] = {
                str(value) for value in values if isinstance(value, (str, int))
            }
    keys = sorted(set(accepted) | set(expected))
    return [
        {
            "contest": contest,
            "level": level,
            "accepted": len(accepted.get((contest, level), set())),
            "expected": len(expected.get((contest, level), set())),
            "complete": bool(expected.get((contest, level)))
            and expected[(contest, level)].issubset(accepted.get((contest, level), set())),
        }
        for contest, level in keys
    ]


def get_progress_message(database: Path):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS telegram_progress_message (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   chat_id TEXT NOT NULL, message_id INTEGER NOT NULL
               )"""
        )
        row = connection.execute(
            "SELECT chat_id, message_id FROM telegram_progress_message WHERE singleton = 1"
        ).fetchone()
    return {"chat_id": row[0], "message_id": row[1]} if row else None


def save_progress_message(database: Path, chat_id: str, message_id: int):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS telegram_progress_message (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   chat_id TEXT NOT NULL, message_id INTEGER NOT NULL
               )"""
        )
        connection.execute(
            """INSERT INTO telegram_progress_message (singleton, chat_id, message_id)
               VALUES (1, ?, ?)
               ON CONFLICT(singleton) DO UPDATE SET
                   chat_id = excluded.chat_id, message_id = excluded.message_id""",
            (chat_id, message_id),
        )


def save_solution_batch(
    database: Path,
    contest: str,
    level: int,
    message_id: int,
    expected_files: list[str] | None = None,
):
    with sqlite3.connect(database, timeout=30) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_solution_batches (
                contest TEXT NOT NULL,
                level INTEGER NOT NULL,
                message_id INTEGER,
                expected_files TEXT,
                PRIMARY KEY (contest, level)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO telegram_solution_batches
                (contest, level, message_id, expected_files)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (contest, level) DO UPDATE SET
                message_id = excluded.message_id,
                expected_files = COALESCE(
                    excluded.expected_files,
                    telegram_solution_batches.expected_files
                )
            """,
            (
                contest,
                level,
                message_id,
                json.dumps(expected_files) if expected_files else None,
            ),
        )
