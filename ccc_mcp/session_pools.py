"""Isolated Telegram team rooms and delayed accepted-solution fanout."""

from __future__ import annotations

import hashlib
import hmac
import os
import random
import secrets
import sqlite3
import time
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

RESEND_RETENTION_SECONDS = 30 * 24 * 60 * 60


class SessionPools:
    def __init__(self, database: Path, encryption_key: str):
        self.database = database
        self.fernet = Fernet(encryption_key.encode()) if encryption_key else None
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.database) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS telegram_rooms (
                    name TEXT PRIMARY KEY,
                    password_salt BLOB NOT NULL,
                    password_hash BLOB NOT NULL,
                    owner_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_room_members (
                    room TEXT NOT NULL REFERENCES telegram_rooms(name) ON DELETE CASCADE,
                    telegram_user_id TEXT NOT NULL,
                    telegram_label TEXT NOT NULL DEFAULT '',
                    account_uuid TEXT NOT NULL,
                    session_cipher BLOB NOT NULL,
                    added_at REAL NOT NULL,
                    PRIMARY KEY (room, account_uuid),
                    UNIQUE (room, telegram_user_id)
                );
                CREATE TABLE IF NOT EXISTS telegram_enrollment_links (
                    token_hash TEXT PRIMARY KEY,
                    room TEXT NOT NULL,
                    telegram_user_id TEXT NOT NULL,
                    telegram_label TEXT NOT NULL DEFAULT '',
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_fanout_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room TEXT NOT NULL,
                    contest TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    file_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    source_uuid TEXT NOT NULL,
                    target_uuid TEXT NOT NULL,
                    run_after REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    manual INTEGER NOT NULL DEFAULT 0,
                    detail TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (room, contest, level, file_id, target_uuid)
                );
                CREATE INDEX IF NOT EXISTS telegram_fanout_due
                    ON telegram_fanout_queue(status, run_after);
                CREATE TABLE IF NOT EXISTS telegram_target_cooldowns (
                    room TEXT NOT NULL,
                    account_uuid TEXT NOT NULL,
                    next_send_at REAL NOT NULL,
                    PRIMARY KEY (room, account_uuid)
                );
                CREATE TABLE IF NOT EXISTS telegram_user_preferences (
                    telegram_user_id TEXT PRIMARY KEY,
                    active_room TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            for table, column, column_type, default in (
                ("telegram_room_members", "telegram_label", "TEXT", "''"),
                ("telegram_enrollment_links", "telegram_label", "TEXT", "''"),
                ("telegram_fanout_queue", "manual", "INTEGER", "0"),
            ):
                columns = {
                    row[1]
                    for row in connection.execute(f"PRAGMA table_info({table})")
                }
                if column not in columns:
                    connection.execute(
                        f"ALTER TABLE {table} ADD COLUMN {column} {column_type} NOT NULL DEFAULT {default}"
                    )
            connection.execute(
                """UPDATE telegram_fanout_queue SET payload = X''
                   WHERE status IN ('sent', 'rejected', 'failed') AND created_at < ?""",
                (time.time() - RESEND_RETENTION_SECONDS,),
            )

    @staticmethod
    def _password_hash(password: str, salt: bytes) -> bytes:
        return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)

    @staticmethod
    def valid_room(room: str) -> bool:
        return (
            3 <= len(room) <= 40
            and all(char.isalnum() or char in "_-" for char in room)
        )

    def create_room(self, room: str, password: str, telegram_user_id: str):
        if not self.valid_room(room) or not 12 <= len(password) <= 128:
            raise ValueError("Room must be 3–40 characters; password 12–128 characters")
        salt = os.urandom(16)
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                "INSERT INTO telegram_rooms VALUES (?, ?, ?, ?, ?)",
                (room, salt, self._password_hash(password, salt), telegram_user_id, time.time()),
            )

    def verify_room(self, room: str, password: str) -> bool:
        with sqlite3.connect(self.database, timeout=30) as connection:
            row = connection.execute(
                "SELECT password_salt, password_hash FROM telegram_rooms WHERE name = ?",
                (room,),
            ).fetchone()
        return bool(
            row
            and hmac.compare_digest(
                self._password_hash(password, row[0]), row[1]
            )
        )

    def create_enrollment_link(
        self, room: str, password: str, user_id: str, user_label: str = ""
    ):
        if not self.fernet:
            raise ValueError("BOT_SESSION_ENCRYPTION_KEY is not configured")
        if not self.verify_room(room, password):
            raise ValueError("Unknown room or incorrect room password")
        token = secrets.token_urlsafe(32)
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """INSERT INTO telegram_enrollment_links
                   (token_hash, room, telegram_user_id, telegram_label, expires_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    hashlib.sha256(token.encode()).hexdigest(),
                    room,
                    user_id,
                    user_label,
                    time.time() + 600,
                ),
            )
        return token

    def take_enrollment_link(self, token: str):
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with sqlite3.connect(self.database, timeout=30) as connection:
            row = connection.execute(
                "SELECT room, telegram_user_id, telegram_label, expires_at FROM telegram_enrollment_links WHERE token_hash = ?",
                (token_hash,),
            ).fetchone()
            connection.execute(
                "DELETE FROM telegram_enrollment_links WHERE token_hash = ?",
                (token_hash,),
            )
        if not row or row[3] < time.time():
            return None
        return row[0], row[1], row[2]

    def save_member(
        self, room: str, telegram_user_id: str, account_uuid: str, session: str,
        telegram_label: str = "",
    ):
        if not self.fernet:
            raise ValueError("BOT_SESSION_ENCRYPTION_KEY is not configured")
        encrypted = self.fernet.encrypt(session.encode())
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """INSERT INTO telegram_room_members
                   (room, telegram_user_id, telegram_label, account_uuid, session_cipher, added_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(room, account_uuid) DO UPDATE SET
                     telegram_user_id=excluded.telegram_user_id,
                     telegram_label=excluded.telegram_label,
                     session_cipher=excluded.session_cipher,
                     added_at=excluded.added_at""",
                (room, telegram_user_id, telegram_label, account_uuid, encrypted, time.time()),
            )

    def remove_member(self, room: str, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            member = connection.execute(
                "SELECT account_uuid FROM telegram_room_members WHERE room = ? AND telegram_user_id = ?",
                (room, telegram_user_id),
            ).fetchone()
            cursor = connection.execute(
                "DELETE FROM telegram_room_members WHERE room = ? AND telegram_user_id = ?",
                (room, telegram_user_id),
            )
            if member:
                connection.execute(
                    """UPDATE telegram_fanout_queue SET status = 'cancelled', payload = X'',
                       detail = 'member disconnected' WHERE room = ? AND target_uuid = ?
                       AND status = 'queued'""",
                    (room, member[0]),
                )
            return cursor.rowcount > 0

    def rooms_for_user(self, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            return [
                row[0]
                for row in connection.execute(
                    """SELECT room FROM telegram_room_members WHERE telegram_user_id = ?
                       UNION SELECT name FROM telegram_rooms WHERE owner_id = ?
                       ORDER BY 1""",
                    (telegram_user_id, telegram_user_id),
                ).fetchall()
            ]

    def set_active_room(self, room: str | None, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if room is None:
                connection.execute(
                    "DELETE FROM telegram_user_preferences WHERE telegram_user_id = ?",
                    (telegram_user_id,),
                )
                return
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Room not found or you are not a room member")
            connection.execute(
                """INSERT INTO telegram_user_preferences
                   (telegram_user_id, active_room, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(telegram_user_id) DO UPDATE SET
                     active_room=excluded.active_room, updated_at=excluded.updated_at""",
                (telegram_user_id, room, time.time()),
            )

    def active_room(self, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            row = connection.execute(
                "SELECT active_room FROM telegram_user_preferences WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
            if not row or not self._can_manage_room(connection, row[0], telegram_user_id):
                return None
            return row[0]

    def _can_manage_room(self, connection, room: str, telegram_user_id: str) -> bool:
        return connection.execute(
            """SELECT 1 FROM telegram_rooms WHERE name = ? AND owner_id = ?
               UNION SELECT 1 FROM telegram_room_members
               WHERE room = ? AND telegram_user_id = ? LIMIT 1""",
            (room, telegram_user_id, room, telegram_user_id),
        ).fetchone() is not None

    def queue_snapshot(self, room: str, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Room not found or you are not a room member")
            rows = connection.execute(
                """SELECT q.id, q.contest, q.level, q.file_id, q.target_uuid,
                          m.telegram_label, m.telegram_user_id,
                          CASE WHEN q.manual = 1 THEN q.run_after
                               ELSE MAX(q.run_after, COALESCE(t.next_send_at, 0)) END
                   FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   LEFT JOIN telegram_target_cooldowns t
                     ON t.room = q.room AND t.account_uuid = q.target_uuid
                   WHERE q.room = ? AND q.status = 'queued'
                   ORDER BY 8, q.id""",
                (room,),
            ).fetchall()
        return [
            {
                "job_id": row[0],
                "contest": row[1],
                "level": row[2],
                "file_id": row[3],
                "target_uuid": row[4],
                "telegram_label": row[5],
                "telegram_user_id": row[6],
                "due_at": row[7],
            }
            for row in rows
        ]

    def release_job_now(self, job_id: int, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            job = connection.execute(
                """SELECT room, target_uuid FROM telegram_fanout_queue
                   WHERE id = ? AND status = 'queued'""",
                (job_id,),
            ).fetchone()
            if not job or not self._can_manage_room(connection, job[0], telegram_user_id):
                raise ValueError("Room not found or you are not a room member")
            connection.execute(
                "UPDATE telegram_fanout_queue SET run_after = ?, manual = 1 WHERE id = ?",
                (time.time(), job_id),
            )
            return True

    def history_snapshot(self, room: str, telegram_user_id: str, limit: int = 20):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Room not found or you are not a room member")
            rows = connection.execute(
                """SELECT q.id, q.contest, q.level, q.file_id, q.target_uuid,
                          m.telegram_label, m.telegram_user_id, q.status, q.detail,
                          q.created_at
                   FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.room = ? AND q.status IN ('sent', 'rejected', 'failed')
                     AND length(q.payload) > 0
                   ORDER BY q.created_at DESC, q.id DESC LIMIT ?""",
                (room, max(1, min(limit, 50))),
            ).fetchall()
        return [
            {
                "job_id": row[0],
                "contest": row[1],
                "level": row[2],
                "file_id": row[3],
                "target_uuid": row[4],
                "telegram_label": row[5],
                "telegram_user_id": row[6],
                "status": row[7],
                "detail": row[8],
                "created_at": row[9],
            }
            for row in rows
        ]

    def resend_job(self, job_id: int, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            job = connection.execute(
                """SELECT room, target_uuid, status, payload, created_at
                   FROM telegram_fanout_queue WHERE id = ?""",
                (job_id,),
            ).fetchone()
            if not job or not self._can_manage_room(connection, job[0], telegram_user_id):
                raise ValueError("Отправка не найдена или у вас нет доступа к комнате")
            if job[2] not in ("sent", "rejected", "failed"):
                raise ValueError("Повторить можно только завершённую отправку")
            if not job[3] or job[4] < time.time() - RESEND_RETENTION_SECONDS:
                raise ValueError("Файл для повтора больше не хранится")
            member = connection.execute(
                """SELECT 1 FROM telegram_room_members
                   WHERE room = ? AND account_uuid = ?""",
                (job[0], job[1]),
            ).fetchone()
            if not member:
                raise ValueError("Аккаунт отключён от комнаты; сначала подключите его снова")
            connection.execute(
                """UPDATE telegram_fanout_queue
                   SET status = 'queued', run_after = ?, manual = 1, detail = NULL
                   WHERE id = ?""",
                (time.time(), job_id),
            )
            return True

    def enqueue_fanout(
        self,
        room: str,
        source_uuid: str,
        contest: str,
        level: int,
        file_id: str,
        filename: str,
        payload: bytes,
    ):
        now = time.time()
        added = 0
        delay = 0
        with sqlite3.connect(self.database, timeout=30) as connection:
            members = connection.execute(
                """SELECT account_uuid FROM telegram_room_members
                   WHERE room = ? AND account_uuid != ? ORDER BY added_at""",
                (room, source_uuid),
            ).fetchall()
            source = connection.execute(
                "SELECT 1 FROM telegram_room_members WHERE room = ? AND account_uuid = ?",
                (room, source_uuid),
            ).fetchone()
            if not source:
                raise ValueError("The submitting CCC account is not connected to this room")
            for (target_uuid,) in members:
                delay += random.randint(60, 180)
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO telegram_fanout_queue
                       (room, contest, level, file_id, filename, payload, source_uuid,
                        target_uuid, run_after, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        room, contest, level, file_id, filename, payload,
                        source_uuid, target_uuid, now + delay, now,
                    ),
                )
                added += cursor.rowcount
        return added

    def claim_due(self):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT q.*, m.session_cipher FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.status = 'queued' AND q.run_after <= ?
                   ORDER BY q.run_after LIMIT 1""",
                (time.time(),),
            ).fetchone()
            if row is None:
                return None
            cooldown = connection.execute(
                "SELECT next_send_at FROM telegram_target_cooldowns WHERE room = ? AND account_uuid = ?",
                (row["room"], row["target_uuid"]),
            ).fetchone()
            next_allowed = cooldown[0] if cooldown else 0
            if not row["manual"] and next_allowed > time.time():
                connection.execute(
                    "UPDATE telegram_fanout_queue SET run_after = ? WHERE id = ?",
                    (next_allowed, row["id"]),
                )
                return None
            connection.execute(
                "UPDATE telegram_fanout_queue SET status = 'sending' WHERE id = ?",
                (row["id"],),
            )
            return dict(row)

    def decrypt_session(self, encrypted: bytes):
        if not self.fernet:
            raise ValueError("BOT_SESSION_ENCRYPTION_KEY is not configured")
        try:
            return self.fernet.decrypt(encrypted).decode()
        except (InvalidToken, UnicodeDecodeError) as error:
            raise ValueError("Could not decrypt a connected session") from error

    def finish_job(self, job_id: int, status: str, detail: str | None = None):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if status in ("sent", "rejected", "failed"):
                row = connection.execute(
                    "SELECT room, target_uuid FROM telegram_fanout_queue WHERE id = ?",
                    (job_id,),
                ).fetchone()
                if row:
                    next_send_at = time.time() + random.randint(60, 180)
                    connection.execute(
                        """INSERT INTO telegram_target_cooldowns
                           (room, account_uuid, next_send_at) VALUES (?, ?, ?)
                           ON CONFLICT(room, account_uuid) DO UPDATE SET
                             next_send_at=excluded.next_send_at""",
                        (row[0], row[1], next_send_at),
                    )
            connection.execute(
                "UPDATE telegram_fanout_queue SET status = ?, detail = ? WHERE id = ?",
                (status, detail, job_id),
            )
