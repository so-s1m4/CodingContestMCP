"""Isolated Telegram team rooms and accepted-solution fanout."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import tempfile
import time
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from .contest_identity import notification_contest
from .level_files import required_level_files

RESEND_RETENTION_SECONDS = 30 * 24 * 60 * 60
logger = logging.getLogger(__name__)


class SessionPools:
    def __init__(self, database: Path, encryption_key: str):
        self.database = database
        self.fernet = Fernet(encryption_key.encode()) if encryption_key else None
        self.database.parent.mkdir(parents=True, exist_ok=True)
        key_path = self.database.with_suffix(".queue-key")
        if not key_path.exists():
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=self.database.parent, delete=False) as key_file:
                    temporary = Path(key_file.name)
                    os.chmod(temporary, 0o600)
                    key_file.write(Fernet.generate_key())
                    key_file.flush()
                    os.fsync(key_file.fileno())
                os.link(temporary, key_path)
            except FileExistsError:
                pass
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        self.queue_fernet = Fernet(key_path.read_bytes())
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
                    game_slug TEXT,
                    source_uuid TEXT NOT NULL,
                    target_uuid TEXT NOT NULL,
                    run_after REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    claimed_at REAL,
                    manual INTEGER NOT NULL DEFAULT 0,
                    detail TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (room, contest, level, file_id, target_uuid)
                );
                CREATE INDEX IF NOT EXISTS telegram_fanout_due
                    ON telegram_fanout_queue(status, run_after);
                CREATE TABLE IF NOT EXISTS telegram_level_packs (
                    room TEXT NOT NULL, contest TEXT NOT NULL, level INTEGER NOT NULL,
                    expected_files TEXT NOT NULL,
                    PRIMARY KEY (room, contest, level)
                );
                CREATE TABLE IF NOT EXISTS telegram_upload_offers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_user_id TEXT NOT NULL,
                    room TEXT NOT NULL,
                    contest TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    target_uuid TEXT NOT NULL,
                    retry_at REAL NOT NULL DEFAULT 0,
                    notified INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS accepted_solution_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_uuid TEXT NOT NULL,
                    session_cipher BLOB NOT NULL,
                    contest TEXT NOT NULL,
                    level INTEGER NOT NULL,
                    file_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    room TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    telegram_status TEXT,
                    fanout_status TEXT,
                    detail TEXT,
                    claimed_at REAL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS accepted_solution_due
                    ON accepted_solution_queue(status, id);
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
                CREATE TABLE IF NOT EXISTS telegram_bot_ui_messages (
                    chat_id TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    PRIMARY KEY (chat_id, message_id)
                );
                """
            )
            for table, column, column_type, default in (
                ("telegram_room_members", "telegram_label", "TEXT", "''"),
                ("telegram_enrollment_links", "telegram_label", "TEXT", "''"),
                ("telegram_fanout_queue", "manual", "INTEGER", "0"),
                ("telegram_fanout_queue", "approved_by", "TEXT", "''"),
                ("telegram_fanout_queue", "offer_id", "INTEGER", "0"),
                ("telegram_fanout_queue", "game_slug", "TEXT", "''"),
            ):
                columns = {
                    row[1]
                    for row in connection.execute(f"PRAGMA table_info({table})")
                }
                if column not in columns:
                    connection.execute(
                        f"ALTER TABLE {table} ADD COLUMN {column} {column_type} NOT NULL DEFAULT {default}"
                    )
            queue_columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(telegram_fanout_queue)")
            }
            if "claimed_at" not in queue_columns:
                connection.execute(
                    "ALTER TABLE telegram_fanout_queue ADD COLUMN claimed_at REAL"
                )
            connection.execute(
                """UPDATE telegram_fanout_queue SET payload = X''
                   WHERE status IN ('sent', 'rejected', 'failed', 'declined') AND created_at < ?""",
                (time.time() - RESEND_RETENTION_SECONDS,),
            )
        self.recover_stale_jobs()
        self.recover_accepted_solutions()

    def enqueue_accepted_solution(self, account_uuid, session, contest, level,
                                  file_id, filename, payload, room):
        with sqlite3.connect(self.database, timeout=30) as connection:
            cursor = connection.execute(
                """INSERT INTO accepted_solution_queue
                   (account_uuid, session_cipher, contest, level, file_id, filename,
                    payload, room, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (account_uuid, self.queue_fernet.encrypt(session.encode()), contest,
                 level, str(file_id), filename, payload, room, time.time()),
            )
            return cursor.lastrowid

    def claim_accepted_solution(self):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM accepted_solution_queue WHERE status = 'queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                "UPDATE accepted_solution_queue SET status = 'working', claimed_at = ? WHERE id = ?",
                (time.time(), row["id"]),
            )
            return dict(row)

    def finish_accepted_solution(self, job_id, status, telegram_status=None,
                                 fanout_status=None, detail=None):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """UPDATE accepted_solution_queue SET status = ?, telegram_status = ?,
                   fanout_status = ?, detail = ?, payload = X'', session_cipher = X'',
                   claimed_at = NULL WHERE id = ?""",
                (status, telegram_status, fanout_status, detail, job_id),
            )

    def accepted_solution_status(self, job_id, account_uuid):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                """SELECT id, status, telegram_status, fanout_status, detail
                   FROM accepted_solution_queue WHERE id = ? AND account_uuid = ?""",
                (job_id, account_uuid),
            ).fetchone()
            return dict(row) if row else None

    def recover_accepted_solutions(self, stale_after=600):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """DELETE FROM accepted_solution_queue
                   WHERE status IN ('done', 'failed') AND created_at < ?""",
                (time.time() - RESEND_RETENTION_SECONDS,),
            )
            return connection.execute(
                """UPDATE accepted_solution_queue SET status = 'queued', claimed_at = NULL
                   WHERE status = 'working' AND claimed_at <= ?""",
                (time.time() - stale_after,),
            ).rowcount

    def requeue_accepted_solution(self, job_id):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """UPDATE accepted_solution_queue SET status = 'queued', claimed_at = NULL
                   WHERE id = ? AND status = 'working'""",
                (job_id,),
            )

    def deployment_instance_changed(self, instance_id: str) -> bool:
        """Check the container identity without mixing it with room/session state."""
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS app_runtime_state (
                       key TEXT PRIMARY KEY,
                       value TEXT NOT NULL
                   )"""
            )
            row = connection.execute(
                "SELECT value FROM app_runtime_state WHERE key = 'instance_id'"
            ).fetchone()
            return row is None or row[0] != instance_id

    def record_deployment_instance(self, instance_id: str) -> None:
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """INSERT INTO app_runtime_state (key, value)
                   VALUES ('instance_id', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (instance_id,),
            )

    def tracked_ui_messages(self, chat_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            return [
                row[0]
                for row in connection.execute(
                    "SELECT message_id FROM telegram_bot_ui_messages WHERE chat_id = ?",
                    (chat_id,),
                )
            ]

    def forget_ui_message(self, chat_id: str, message_id: int):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                "DELETE FROM telegram_bot_ui_messages WHERE chat_id = ? AND message_id = ?",
                (chat_id, message_id),
            )

    def track_ui_message(self, chat_id: str, message_id: int):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO telegram_bot_ui_messages (chat_id, message_id) VALUES (?, ?)",
                (chat_id, message_id),
            )

    def clear_solution_queue(self) -> int:
        """Discard answer submissions/history while keeping room members and sessions."""
        with sqlite3.connect(self.database, timeout=30) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM telegram_fanout_queue"
            ).fetchone()[0]
            connection.execute("DELETE FROM telegram_fanout_queue")
            connection.execute("DELETE FROM telegram_upload_offers")
            connection.execute("DELETE FROM telegram_target_cooldowns")
        return count

    def recover_stale_jobs(self, stale_after: int = 600):
        """Requeue claims abandoned by a killed worker after a safety timeout."""
        now = time.time()
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                """UPDATE telegram_fanout_queue SET claimed_at = run_after
                   WHERE status = 'sending' AND claimed_at IS NULL"""
            )
            recovered = connection.execute(
                """UPDATE telegram_fanout_queue
                   SET status = 'queued', claimed_at = NULL,
                       run_after = MIN(run_after, ?),
                       detail = 'Recovered after interrupted worker'
                   WHERE status = 'sending' AND claimed_at <= ?""",
                (now, now - stale_after),
            ).rowcount
        if recovered:
            logger.warning(
                "Recovered %s room queue job(s) left in sending after worker interruption",
                recovered,
            )
        return recovered

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
                """UPDATE telegram_fanout_queue SET approved_by = '', offer_id = 0
                   WHERE room = ? AND target_uuid = ? AND status = 'queued'
                     AND EXISTS (SELECT 1 FROM telegram_room_members
                         WHERE room = ? AND account_uuid = ? AND telegram_user_id != ?)""",
                (room, account_uuid, room, account_uuid, telegram_user_id),
            )
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

    def rooms_for_account(self, account_uuid: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            return [
                row[0]
                for row in connection.execute(
                    """SELECT DISTINCT room FROM telegram_room_members
                       WHERE account_uuid = ? ORDER BY room""",
                    (account_uuid,),
                ).fetchall()
            ]

    def members_snapshot(self, room: str, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Only the room creator can view its connected accounts")
            rows = connection.execute(
                """SELECT telegram_label, telegram_user_id, account_uuid, added_at
                   FROM telegram_room_members WHERE room = ? ORDER BY added_at""",
                (room,),
            ).fetchall()
        return [
            {
                "telegram_label": row[0],
                "telegram_user_id": row[1],
                "account_uuid": row[2],
                "added_at": row[3],
            }
            for row in rows
        ]

    def set_active_room(self, room: str | None, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if room is None:
                connection.execute(
                    "DELETE FROM telegram_user_preferences WHERE telegram_user_id = ?",
                    (telegram_user_id,),
                )
                return
            if not self._can_access_room(connection, room, telegram_user_id):
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
            if not row or not self._can_access_room(connection, row[0], telegram_user_id):
                return None
            return row[0]

    def is_room_owner(self, room: str, telegram_user_id: str) -> bool:
        with sqlite3.connect(self.database, timeout=30) as connection:
            return connection.execute(
                "SELECT 1 FROM telegram_rooms WHERE name = ? AND owner_id = ?",
                (room, telegram_user_id),
            ).fetchone() is not None

    def _can_access_room(self, connection, room: str, telegram_user_id: str) -> bool:
        return connection.execute(
            """SELECT 1 FROM telegram_rooms WHERE name = ? AND owner_id = ?
               UNION SELECT 1 FROM telegram_room_members
               WHERE room = ? AND telegram_user_id = ? LIMIT 1""",
            (room, telegram_user_id, room, telegram_user_id),
        ).fetchone() is not None

    def _can_manage_room(self, connection, room: str, telegram_user_id: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM telegram_rooms WHERE name = ? AND owner_id = ?",
            (room, telegram_user_id),
        ).fetchone() is not None

    def queue_snapshot(self, room: str, telegram_user_id: str):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Only the room creator can manage its queue and history")
            rows = connection.execute(
                """SELECT q.id, q.contest, q.level, q.file_id, q.target_uuid,
                          m.telegram_label, m.telegram_user_id,
                          q.run_after,
                          CASE WHEN q.status = 'queued' AND q.approved_by != m.telegram_user_id
                               THEN 'awaiting_confirmation' ELSE q.status END
                   FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.room = ? AND q.status IN ('queued', 'sending')
                   ORDER BY q.manual DESC, 8, q.id""",
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
                "status": row[8],
            }
            for row in rows
        ]

    def history_snapshot(self, room: str, telegram_user_id: str, limit: int = 20):
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Only the room creator can manage its queue and history")
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

    def resend_catalog(self, room: str, telegram_user_id: str):
        """List games and levels with answer files still available for a room owner."""
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Only the room creator can resend full levels")
            rows = connection.execute(
                """SELECT q.contest, q.level, q.file_id, q.game_slug
                   FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.room = ? AND length(q.payload) > 0
                   ORDER BY q.created_at DESC, q.id DESC""",
                (room,),
            ).fetchall()
        games = {}
        for contest, level, file_id, game_slug in rows:
            game = games.setdefault(
                contest,
                {"contest": contest, "game_slug": game_slug, "levels": {}},
            )
            if game["game_slug"] is None and game_slug:
                game["game_slug"] = game_slug
            game["levels"].setdefault(level, set()).add(file_id)
        result = []
        for game in games.values():
            result.append(
                {
                    "contest": game["contest"],
                    "game_slug": game["game_slug"],
                    "levels": [
                        {"level": level, "files": len(files)}
                        for level, files in sorted(game["levels"].items())
                    ],
                }
            )
        return result

    def resend_level(
        self,
        room: str,
        contest: str,
        level: int,
        target_uuids: list[str],
        telegram_user_id: str,
    ):
        """Queue all retained files for one game level to selected room members."""
        targets = list(dict.fromkeys(target_uuids))
        if not targets:
            raise ValueError("Выберите хотя бы один аккаунт")
        now = time.time()
        with sqlite3.connect(self.database, timeout=30) as connection:
            if not self._can_manage_room(connection, room, telegram_user_id):
                raise ValueError("Only the room creator can resend full levels")
            members = {
                row[0]
                for row in connection.execute(
                    "SELECT account_uuid FROM telegram_room_members WHERE room = ?",
                    (room,),
                )
            }
            if any(target not in members for target in targets):
                raise ValueError("Аккаунт больше не подключён к этой комнате")

            payload_rows = connection.execute(
                """SELECT file_id, filename, payload, game_slug, source_uuid
                   FROM telegram_fanout_queue
                   WHERE room = ? AND contest = ? AND level = ? AND length(payload) > 0
                   ORDER BY created_at DESC, id DESC""",
                (room, contest, level),
            ).fetchall()
            files = {}
            for file_id, filename, payload, game_slug, source_uuid in payload_rows:
                files.setdefault(
                    file_id,
                    {
                        "filename": filename,
                        "payload": payload,
                        "game_slug": game_slug,
                        "source_uuid": source_uuid,
                    },
                )
            if not files:
                raise ValueError("Для этого уровня больше нет сохранённых файлов повторной отправки")

            queued = 0
            already_active = 0
            for target_uuid in targets:
                for file_id, item in files.items():
                    existing = connection.execute(
                        """SELECT id, status FROM telegram_fanout_queue
                           WHERE room = ? AND contest = ? AND level = ?
                             AND file_id = ? AND target_uuid = ?""",
                        (room, contest, level, file_id, target_uuid),
                    ).fetchone()
                    if existing and existing[1] in ("queued", "sending"):
                        already_active += 1
                        if existing[1] == "queued":
                            connection.execute(
                                """UPDATE telegram_fanout_queue
                                   SET filename = ?, payload = ?,
                                       game_slug = COALESCE(?, game_slug), approved_by = '', offer_id = 0
                                   WHERE id = ? AND status = 'queued'""",
                                (item["filename"], item["payload"], item["game_slug"], existing[0]),
                            )
                        continue
                    if existing:
                        connection.execute(
                            """UPDATE telegram_fanout_queue
                               SET filename = ?, payload = ?,
                                   game_slug = COALESCE(?, game_slug), source_uuid = ?,
                                   run_after = ?, status = 'queued', claimed_at = NULL,
                                   manual = 0, detail = NULL, approved_by = '', offer_id = 0, created_at = ?
                               WHERE id = ?""",
                            (
                                item["filename"], item["payload"], item["game_slug"],
                                item["source_uuid"], now, now, existing[0],
                            ),
                        )
                    else:
                        connection.execute(
                            """INSERT INTO telegram_fanout_queue
                               (room, contest, level, file_id, filename, payload,
                                game_slug, source_uuid, target_uuid, run_after, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                room, contest, level, file_id, item["filename"],
                                item["payload"], item["game_slug"], item["source_uuid"],
                                target_uuid, now, now,
                            ),
                        )
                    queued += 1
        return {
            "queued": queued,
            "files": len(files),
            "targets": len(targets),
            "already_active": already_active,
        }

    def self_resend_catalog(self, room: str, telegram_user_id: str):
        """List retained levels available for a room member to resend to themselves."""
        with sqlite3.connect(self.database, timeout=30) as connection:
            member = connection.execute(
                "SELECT 1 FROM telegram_room_members WHERE room = ? AND telegram_user_id = ?",
                (room, telegram_user_id),
            ).fetchone()
            if not member:
                raise ValueError("Сначала подключите свой CCC-аккаунт к этой комнате")
            rows = connection.execute(
                """SELECT q.contest, q.level, q.file_id, q.game_slug
                   FROM telegram_fanout_queue q
                   WHERE q.room = ? AND length(q.payload) > 0
                   ORDER BY q.created_at DESC, q.id DESC""",
                (room,),
            ).fetchall()
        games = {}
        for contest, level, file_id, game_slug in rows:
            game = games.setdefault(
                contest,
                {"contest": contest, "game_slug": game_slug, "levels": {}},
            )
            if game["game_slug"] is None and game_slug:
                game["game_slug"] = game_slug
            game["levels"].setdefault(level, set()).add(file_id)
        return [
            {
                "contest": game["contest"],
                "game_slug": game["game_slug"],
                "levels": [
                    {"level": level, "files": len(files)}
                    for level, files in sorted(game["levels"].items())
                ],
            }
            for game in games.values()
        ]

    def resend_level_to_self(
        self, room: str, contest: str, level: int, telegram_user_id: str
    ):
        """Queue every retained file of a level only for the requesting room member."""
        now = time.time()
        with sqlite3.connect(self.database, timeout=30) as connection:
            member = connection.execute(
                """SELECT account_uuid FROM telegram_room_members
                   WHERE room = ? AND telegram_user_id = ?""",
                (room, telegram_user_id),
            ).fetchone()
            if not member:
                raise ValueError("Сначала подключите свой CCC-аккаунт к этой комнате")
            target_uuid = member[0]
            payload_rows = connection.execute(
                """SELECT file_id, filename, payload, game_slug, source_uuid
                   FROM telegram_fanout_queue
                   WHERE room = ? AND contest = ? AND level = ? AND length(payload) > 0
                   ORDER BY created_at DESC, id DESC""",
                (room, contest, level),
            ).fetchall()
            files = {}
            for file_id, filename, payload, game_slug, source_uuid in payload_rows:
                files.setdefault(
                    file_id,
                    {
                        "filename": filename,
                        "payload": payload,
                        "game_slug": game_slug,
                        "source_uuid": source_uuid,
                    },
                )
            if not files:
                raise ValueError("Для этого уровня больше нет сохранённых файлов повторной отправки")

            queued = 0
            already_active = 0
            for file_id, item in files.items():
                existing = connection.execute(
                    """SELECT id, status FROM telegram_fanout_queue
                       WHERE room = ? AND contest = ? AND level = ?
                         AND file_id = ? AND target_uuid = ?""",
                    (room, contest, level, file_id, target_uuid),
                ).fetchone()
                if existing and existing[1] in ("queued", "sending"):
                    already_active += 1
                    if existing[1] == "queued":
                        connection.execute(
                            """UPDATE telegram_fanout_queue
                               SET filename = ?, payload = ?,
                                   game_slug = COALESCE(?, game_slug), approved_by = '', offer_id = 0
                               WHERE id = ? AND status = 'queued'""",
                            (item["filename"], item["payload"], item["game_slug"], existing[0]),
                        )
                    continue
                if existing:
                    connection.execute(
                        """UPDATE telegram_fanout_queue
                           SET filename = ?, payload = ?,
                               game_slug = COALESCE(?, game_slug), source_uuid = ?,
                               run_after = ?, status = 'queued', claimed_at = NULL,
                               manual = 0, detail = NULL, approved_by = '', offer_id = 0, created_at = ?
                           WHERE id = ?""",
                        (
                            item["filename"], item["payload"], item["game_slug"],
                            item["source_uuid"], now, now, existing[0],
                        ),
                    )
                else:
                    connection.execute(
                        """INSERT INTO telegram_fanout_queue
                           (room, contest, level, file_id, filename, payload,
                            game_slug, source_uuid, target_uuid, run_after, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            room, contest, level, file_id, item["filename"],
                            item["payload"], item["game_slug"], item["source_uuid"],
                            target_uuid, now, now,
                        ),
                    )
                queued += 1
            connection.execute(
                """UPDATE telegram_fanout_queue SET approved_by = ?
                   WHERE room = ? AND contest = ? AND level = ? AND target_uuid = ?
                     AND status = 'queued'""",
                (telegram_user_id, room, contest, level, target_uuid),
            )
        return {"queued": queued, "files": len(files), "already_active": already_active}

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
                   SET status = 'queued', run_after = ?, manual = 1, detail = NULL, approved_by = '', offer_id = 0
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
        game_slug: str | None = None,
        expected_files: list[str] | None = None,
    ):
        if game_slug:
            contest = notification_contest(contest)
        now = time.time()
        added = 0
        requeued = 0
        existing_statuses = {}
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
            if expected_files:
                connection.execute(
                    """INSERT INTO telegram_level_packs VALUES (?, ?, ?, ?)
                       ON CONFLICT(room, contest, level) DO UPDATE SET expected_files = excluded.expected_files""",
                    (room, contest, level, json.dumps(required_level_files(expected_files))),
                )
            for (target_uuid,) in members:
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO telegram_fanout_queue
                       (room, contest, level, file_id, filename, payload, game_slug, source_uuid,
                        target_uuid, run_after, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        room, contest, level, file_id, filename, payload, game_slug,
                        source_uuid, target_uuid, now, now,
                    ),
                )
                added += cursor.rowcount
                if cursor.rowcount:
                    continue
                existing = connection.execute(
                    """SELECT id, status FROM telegram_fanout_queue
                       WHERE room = ? AND contest = ? AND level = ?
                         AND file_id = ? AND target_uuid = ?""",
                    (room, contest, level, file_id, target_uuid),
                ).fetchone()
                if not existing:
                    continue
                job_id, status = existing
                existing_statuses[status] = existing_statuses.get(status, 0) + 1
                if status in ("failed", "rejected", "cancelled"):
                    updated = connection.execute(
                        """UPDATE telegram_fanout_queue
                           SET filename = ?, payload = ?, game_slug = COALESCE(?, game_slug), source_uuid = ?,
                               run_after = ?, status = 'queued', claimed_at = NULL,
                               manual = 0, detail = NULL, approved_by = '', offer_id = 0
                           WHERE id = ? AND status IN ('failed', 'rejected', 'cancelled')""",
                        (
                            filename, payload, game_slug, source_uuid,
                            now, job_id,
                        ),
                    )
                    requeued += updated.rowcount
                elif status == "queued":
                    # Another account or delivery retry may submit identical bytes.
                    # Keep the existing offer and consent unless the answer changes.
                    connection.execute(
                        """UPDATE telegram_fanout_queue
                           SET filename = ?, payload = ?,
                               game_slug = COALESCE(?, game_slug), source_uuid = ?, approved_by = '', offer_id = 0
                           WHERE id = ? AND status = 'queued' AND payload != ?""",
                        (filename, payload, game_slug, source_uuid, job_id, payload),
                    )
        return {
            "queued": added + requeued,
            "targets": len(members),
            "requeued": requeued,
            "existing": sum(existing_statuses.values()),
            "existing_statuses": existing_statuses,
        }

    @staticmethod
    def _pack_complete(connection, room, contest, level, target_uuid):
        pack = connection.execute(
            "SELECT expected_files FROM telegram_level_packs WHERE room = ? AND contest = ? AND level = ?",
            (room, contest, level),
        ).fetchone()
        if not pack:
            return False
        expected = set(required_level_files(json.loads(pack[0])))
        available = {row[0] for row in connection.execute(
            """SELECT file_id FROM telegram_fanout_queue
               WHERE room = ? AND contest = ? AND level = ? AND target_uuid = ?""",
            (room, contest, level, target_uuid),
        )}
        return bool(expected) and expected <= available

    def next_upload_offer(self):
        """Persist a snapshot and reserve notification delivery across workers."""
        now = time.time()
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            candidates = connection.execute(
                """SELECT q.room, q.contest, q.level, q.target_uuid, m.telegram_user_id
                   FROM telegram_fanout_queue q JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.status = 'queued' AND q.approved_by != m.telegram_user_id
                     AND q.offer_id = 0 GROUP BY q.room, q.contest, q.level, q.target_uuid ORDER BY MIN(q.id)"""
            ).fetchall()
            pending = next((row for row in candidates if self._pack_complete(connection, *tuple(row)[:4])), None)
            if pending:
                cursor = connection.execute(
                    """INSERT INTO telegram_upload_offers
                       (room, contest, level, target_uuid, telegram_user_id)
                       VALUES (?, ?, ?, ?, ?)""", tuple(pending),
                )
                connection.execute(
                    """UPDATE telegram_fanout_queue SET offer_id = ?
                       WHERE room = ? AND contest = ? AND level = ? AND target_uuid = ?
                         AND status = 'queued' AND approved_by != ? AND offer_id = 0""",
                    (cursor.lastrowid, *tuple(pending)),
                )
            offers = connection.execute(
                """SELECT o.*, COUNT(q.id) AS files FROM telegram_upload_offers o
                   JOIN telegram_fanout_queue q ON q.offer_id = o.id
                   JOIN telegram_room_members m ON m.room = q.room
                     AND m.account_uuid = q.target_uuid AND m.telegram_user_id = o.telegram_user_id
                   WHERE o.notified = 0 AND o.retry_at <= ? AND q.status = 'queued'
                     AND q.approved_by != m.telegram_user_id
                   GROUP BY o.id ORDER BY o.id""", (now,),
            ).fetchall()
            offer = next((row for row in offers if self._pack_complete(
                connection, row["room"], row["contest"], row["level"], row["target_uuid"]
            )), None)
            if offer:
                connection.execute(
                    "UPDATE telegram_upload_offers SET retry_at = ? WHERE id = ?",
                    (now + 60, offer["id"]),
                )
            return dict(offer) if offer else None

    def mark_upload_offer_sent(self, offer_id):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute(
                "UPDATE telegram_upload_offers SET notified = 1 WHERE id = ?", (offer_id,)
            )

    def answer_upload_offer(self, offer_id: int, user_id: str, accept: bool):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute("BEGIN IMMEDIATE")
            offer = connection.execute(
                """SELECT o.room, o.contest, o.level, o.target_uuid FROM telegram_upload_offers o JOIN telegram_room_members m
                   ON m.room = o.room AND m.account_uuid = o.target_uuid
                     AND m.telegram_user_id = o.telegram_user_id
                   WHERE o.id = ? AND o.telegram_user_id = ?""",
                (offer_id, user_id),
            ).fetchone()
            if not offer:
                raise ValueError("Предложение недоступно для вашего аккаунта")
            if not self._pack_complete(connection, *offer):
                raise ValueError("Полный пак уровня ещё не готов")
            return connection.execute(
                """UPDATE telegram_fanout_queue SET approved_by = ?, status = ?
                   WHERE offer_id = ? AND status = 'queued' AND approved_by = ''""",
                (user_id, "queued" if accept else "declined", offer_id),
            ).rowcount

    def claim_due(self):
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT q.*, m.session_cipher FROM telegram_fanout_queue q
                   JOIN telegram_room_members m
                     ON m.room = q.room AND m.account_uuid = q.target_uuid
                   WHERE q.status = 'queued' AND q.run_after <= ?
                     AND q.approved_by = m.telegram_user_id
                     AND NOT EXISTS (
                         SELECT 1 FROM telegram_fanout_queue active
                         WHERE active.target_uuid = q.target_uuid AND active.status = 'sending'
                     )
                   ORDER BY q.manual DESC, q.run_after, q.id LIMIT 1""",
                (time.time(),),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """UPDATE telegram_fanout_queue
                   SET status = 'sending', claimed_at = ? WHERE id = ?""",
                (time.time(), row["id"]),
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
            connection.execute(
                """UPDATE telegram_fanout_queue
                   SET status = ?, detail = ?, claimed_at = NULL WHERE id = ?""",
                (status, detail, job_id),
            )
