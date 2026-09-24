import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    cookie: str = field(default="", repr=False)
    session: str = field(default="", repr=False)
    data_dir: Path = Path("./data")
    host: str = "127.0.0.1"
    port: int = 8000
    timeout: float = 30
    max_bytes: int = 64 * 1024 * 1024
    public_origin: str = "http://localhost:8000"
    enable_raw_writes: bool = False
    bot_token: str = field(default="", repr=False)
    bot_chat_id: str = field(default="", repr=False)
    bot_session_encryption_key: str = field(default="", repr=False)
    bot_dedupe_db: Path | None = None

    def __post_init__(self):
        if not 1 <= self.port <= 65535 or self.timeout <= 0 or self.max_bytes <= 0:
            raise ValueError("Invalid port, timeout or file size limit")
        origin = urlsplit(self.public_origin)
        if (
            origin.scheme not in ("http", "https")
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.query
            or origin.fragment
            or origin.path not in ("", "/")
        ):
            raise ValueError(
                "MCP_PUBLIC_ORIGIN must be an HTTP(S) origin without a path"
            )

    @classmethod
    def from_env(cls):
        load_dotenv(override=False)
        data_dir = Path(os.getenv("CCC_DATA_DIR", "./data")).resolve()
        dedupe_db = os.getenv("BOT_DEDUPE_DB")
        return cls(
            data_dir=data_dir,
            host=os.getenv("MCP_HOST", "127.0.0.1"),
            port=int(os.getenv("MCP_PORT", "8000")),
            timeout=float(os.getenv("CCC_TIMEOUT", "30")),
            max_bytes=int(os.getenv("CCC_MAX_FILE_BYTES", str(64 * 1024 * 1024))),
            public_origin=os.getenv("MCP_PUBLIC_ORIGIN", "http://localhost:8000"),
            enable_raw_writes=os.getenv("CCC_ENABLE_RAW_WRITES") == "1",
            bot_token=os.getenv("BOT_TOKEN", ""),
            bot_chat_id=os.getenv("BOT_CHAT_ID", ""),
            bot_session_encryption_key=os.getenv("BOT_SESSION_ENCRYPTION_KEY", ""),
            bot_dedupe_db=(
                Path(dedupe_db).resolve()
                if dedupe_db
                else data_dir / "telegram-sent.sqlite3"
            ),
        )
