"""Stable contest names for shared Telegram notifications."""

import re


def notification_contest(contest: str) -> str:
    """Remove the training instance suffix, preserving the contest/year name."""
    return re.sub(r"[-_]\d+[-_][0-9a-fA-F]{8}$", "", contest).replace("_", "-")
