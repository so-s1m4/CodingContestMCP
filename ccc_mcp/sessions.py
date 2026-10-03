"""Bounded, process-local game state. No website cookies or HTTP clients are kept."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .client import APIError

if TYPE_CHECKING:
    from .service import Game


@dataclass
class GameSessions:
    games: dict[str, Game] = field(default_factory=dict, repr=False)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    users: int = 0
    touched: float = field(default_factory=time.monotonic)

    def can_evict(self, now):
        return not self.users


class AccountSessions:
    def __init__(self, limit=1024, idle_seconds=900):
        self.entries: OrderedDict[str, GameSessions] = OrderedDict()
        self.limit, self.idle_seconds = limit, idle_seconds

    @contextmanager
    def use(self, account):
        now = time.monotonic()
        for key, value in list(self.entries.items()):
            if value.can_evict(now) and now - value.touched > self.idle_seconds:
                del self.entries[key]
        state = self.entries.get(account)
        if state is None:
            if len(self.entries) >= self.limit:
                victim = next(
                    (
                        key
                        for key, value in self.entries.items()
                        if value.can_evict(now)
                    ),
                    None,
                )
                if victim is None:
                    raise APIError(503, "Game session cache busy", "1")
                del self.entries[victim]
            state = self.entries[account] = GameSessions()
        self.entries.move_to_end(account)
        state.users += 1
        try:
            yield state
        finally:
            state.users -= 1
            state.touched = time.monotonic()
