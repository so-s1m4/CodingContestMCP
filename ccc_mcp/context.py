"""Request-local account context; never shared between MCP callers."""

from contextvars import ContextVar
from typing import TYPE_CHECKING

from .service import Service

if TYPE_CHECKING:
    from .session_pools import SessionPools

account_service: ContextVar[Service] = ContextVar("ccc_account_service")
team_room: ContextVar[str | None] = ContextVar("ccc_team_room", default=None)
session_pools: ContextVar["SessionPools"] = ContextVar("ccc_session_pools")
account_uuid: ContextVar[str | None] = ContextVar("ccc_account_uuid", default=None)


def current_account_uuid() -> str | None:
    return account_uuid.get()


def current_service() -> Service:
    try:
        return account_service.get()
    except LookupError as error:
        raise RuntimeError("No authenticated CCC request context") from error


def current_team_room() -> str | None:
    return team_room.get()


def current_session_pools() -> "SessionPools":
    try:
        return session_pools.get()
    except LookupError as error:
        raise RuntimeError("No Telegram room storage in authenticated request context") from error
