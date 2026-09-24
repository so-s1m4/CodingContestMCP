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


def delete_solution_payloads(database: Path, contest: str, level: int):
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
            SELECT path FROM telegram_solution_payloads
            WHERE contest = ? AND level = ?
            """,
            (contest, level),
        ).fetchall()
        connection.execute(
            "DELETE FROM telegram_solution_payloads WHERE contest = ? AND level = ?",
            (contest, level),
        )
    for (path,) in rows:
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass


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
