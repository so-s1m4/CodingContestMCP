"""Short-lived one-time links for transferring artifacts to an MCP client."""

import secrets
import threading
import time
from pathlib import Path


class DownloadLinks:
    def __init__(self, ttl_seconds=300):
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._links = {}

    def issue(self, path: Path, filename: str, origin: str):
        token = secrets.token_urlsafe(32)
        expires = time.monotonic() + self.ttl_seconds
        with self._lock:
            self._purge_expired()
            self._links[token] = (path, filename, expires)
        return {
            "url": f"{origin.rstrip('/')}/mcp/direct-download/{token}",
            "filename": filename,
            "expires_in_seconds": self.ttl_seconds,
        }

    def consume(self, token: str):
        with self._lock:
            self._purge_expired()
            link = self._links.pop(token, None)
        if link is None:
            return None
        path, filename, _ = link
        return (path, filename) if path.is_file() and not path.is_symlink() else None

    def _purge_expired(self):
        now = time.monotonic()
        self._links = {
            token: item for token, item in self._links.items() if item[2] > now
        }


download_links = DownloadLinks()
