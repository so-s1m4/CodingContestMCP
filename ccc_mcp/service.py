"""Contest-scoped workflow with private game tokens and artifact handles."""

import asyncio
import math
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from .artifacts import Artifacts
from .client import APIError, CCCClient, game_origin
from .sessions import GameSessions, retry_seconds


def site_slug(value: str, collection: str) -> str:
    """Accept a name/slug or a corresponding frontend link, without fetching that URL."""
    if "://" in value:
        url = urlsplit(value)
        parts = url.path.strip("/").split("/")
        if (
            url.scheme != "https"
            or url.hostname not in ("codingcontest.org", "www.codingcontest.org")
            or url.username
            or url.password
            or url.port not in (None, 443)
            or len(parts) not in (2, 3)
            or parts[0] != collection
            or len(parts) == 3
            and (collection != "contests" or parts[2] != "game")
        ):
            raise ValueError(
                f"Use a name/slug or a codingcontest.org/{collection}/ link; challenge links need start_training"
            )
        value = unquote(parts[1])
    segment(value)
    return value


def contest_slug(value: str) -> str:
    return site_slug(value, "contests")


def segment(value):
    value = str(value)
    if not value or value in (".", "..") or any(c in value for c in "/\\\r\n"):
        raise ValueError("Invalid identifier")
    return quote(value, safe="")


def compact_progress(value):
    if isinstance(value, dict):
        return {k: compact_progress(v) for k, v in value.items() if k != "submissions"}
    if isinstance(value, list):
        return [compact_progress(v) for v in value]
    return value


@dataclass
class Game:
    slug: str
    origin: str
    contest: dict
    token: str = field(repr=False)
    created: float = field(default_factory=time.monotonic)


class Service:
    def __init__(self, client: CCCClient, games: GameSessions | None = None):
        self.client = client
        self.artifacts = Artifacts(client.settings.data_dir, client.settings.max_bytes)
        self.games = games if games is not None else GameSessions()

    async def challenge(self, query: str):
        query = site_slug(query.strip(), "challenges")
        games = await self.client.json("GET", "/api/games")
        exact = [
            g
            for g in games
            if query.casefold()
            in [str(g.get(k, "")).casefold() for k in ("slug", "name", "uuid")]
        ]
        matches = exact or [
            g for g in games if query.casefold() in g["name"].casefold()
        ]
        if len(matches) != 1:
            raise ValueError(
                "Challenge not found or ambiguous; use a slug from list_challenges"
            )
        return matches[0]

    async def start_training(self, query: str, mode: str):
        game = await self.challenge(query)
        result = await self.client.json(
            "POST",
            f"/api/training/{segment(game['slug'])}/start",
            params={"mode": mode},
        )
        return {
            "training": result,
            "next": "Call prepare_level with training.contestName and level=1",
        }

    async def session(self, contest: str, rejected: Game | None = None):
        contest = contest_slug(contest)
        async with self.games.lock:
            existing = self.games.games.get(contest)
            if (
                existing
                and existing is not rejected
                and time.monotonic() - existing.created < 240
            ):
                return existing
            self.games.check_cooldown()
            data = await self.client.json("GET", f"/api/contests/{segment(contest)}")
            origin = game_origin(data.get("gameBaseUrl", ""))
            slug = data["slug"]
            existing = self.games.games.get(slug)
            if (
                existing
                and existing is not rejected
                and time.monotonic() - existing.created < 240
            ):
                if len(self.games.games) < 100:
                    self.games.games[contest] = existing
                return existing
            try:
                result = await self.client.json(
                    "POST", "/api/game-token", json_body={"contestSlug": slug}
                )
            except APIError as error:
                if error.status == 429:
                    self.games.rate_limited(error)
                raise
            token = result.get("token")
            if not isinstance(token, str) or not token:
                raise ValueError("CCC returned no game token")
            game = Game(slug, origin, data, token)
            if len(self.games.games) >= 100:
                oldest = min(self.games.games.values(), key=lambda item: item.created)
                self.games.games = {
                    key: value
                    for key, value in self.games.games.items()
                    if value is not oldest
                }
            self.games.games[contest] = self.games.games[slug] = game
            return game

    async def request(self, contest, method, path, **kwargs):
        game = await self.session(contest)
        try:
            return await self.client.request(
                method,
                path,
                origin=game.origin,
                token=game.token,
                slug=game.slug,
                **kwargs,
            )
        except APIError as error:
            # Only replay reads. A timed out submission may already have been accepted.
            if error.status == 401:
                self.games.games = {
                    key: value
                    for key, value in self.games.games.items()
                    if value is not game
                }
            if method != "GET" or error.status != 401:
                raise
            game = await self.session(contest, rejected=game)
            return await self.client.request(
                method,
                path,
                origin=game.origin,
                token=game.token,
                slug=game.slug,
                **kwargs,
            )

    async def info(self, contest):
        game = await self.session(contest)
        metadata_response, progress_response = await asyncio.gather(
            self.request(contest, "GET", "/game/game-info"),
            self.request(contest, "GET", "/api/contestant/contestant-info"),
        )
        metadata, progress = metadata_response.json(), progress_response.json()
        unlocked = ((progress.get("score") or {}).get("gameScore") or {}).get("level")
        levels = [
            {
                **level,
                "level": i,
                "accessible": True
                if isinstance(unlocked, int) and i <= unlocked
                else None,
            }
            for i, level in enumerate(
                (metadata.get("levelsInfo") or {}).get("levels", []), 1
            )
        ]
        return dict(
            contest_slug=game.slug,
            game={
                k: v
                for k, v in metadata.items()
                if k not in ("levelsInfo", "versions", "dev")
            },
            levels=levels,
            participant=compact_progress(progress),
            access_note="Level accessibility is a progress hint only; CCC decides access on download/submission",
            resume={
                "contest": game.slug,
                "url": f"https://codingcontest.org/contests/{game.slug}/game",
            },
        )

    @staticmethod
    def validate_level(level, file_id=None):
        # No inferred progress gating: exploration and competition rules may differ.
        if level < 1:
            raise ValueError("Level must be positive")
        if file_id is not None:
            segment(file_id)

    async def asset(self, contest, path, filename):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.artifacts.root, delete=False) as output:
                temporary = Path(output.name)
                response = await self.request(contest, "GET", path, output=output)
                if "json" in response.headers.get("content-type", ""):
                    # The frontend's fallback avoids exposing the game token to signed URLs.
                    response = await self.request(
                        contest, "GET", path, params={"raw": "true"}, output=output
                    )
                    if "json" in response.headers.get("content-type", ""):
                        raise ValueError("Raw asset endpoint returned JSON instead of a file")
            result = await asyncio.to_thread(self.artifacts.adopt, temporary, filename)
            temporary = None
            return result
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    async def response_file(self, method, path, filename, *, contest=None, **kwargs):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.artifacts.root, delete=False) as output:
                temporary = Path(output.name)
                if contest is None:
                    response = await self.client.request(method, path, output=output, **kwargs)
                else:
                    response = await self.request(contest, method, path, output=output, **kwargs)
                if response.content:
                    output.write(response.content)
            result = await asyncio.to_thread(self.artifacts.adopt, temporary, filename)
            temporary = None
            return result
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    async def submit(self, contest, level, file_id, payload, filename):
        size = payload.stat().st_size if isinstance(payload, Path) else len(payload)
        if size > self.client.settings.max_bytes:
            raise ValueError("Solution exceeds CCC_MAX_FILE_BYTES")
        self.validate_level(level, file_id)
        game = await self.session(contest)
        async with self.games.submission_lock:
            now = time.monotonic()
            cooldowns = self.games.submission_cooldowns
            for key in list(cooldowns):
                if cooldowns[key] <= now:
                    del cooldowns[key]
            remaining = math.ceil(cooldowns.get(game.slug, 0) - now)
            if remaining > 0:
                raise APIError(
                    429,
                    {
                        "source": "codingcontest.org",
                        "contest": game.slug,
                        "message": "Submission cooldown; no upstream submission was sent",
                    },
                    str(remaining),
                )
            try:
                if isinstance(payload, Path):
                    with payload.open("rb") as stream:
                        response = await self.request(
                            game.slug, "POST", f"/api/contestant/submit-{level}-{segment(file_id)}",
                            files={"solution": (filename, stream, "application/octet-stream")},
                        )
                else:
                    response = await self.request(
                        game.slug, "POST", f"/api/contestant/submit-{level}-{segment(file_id)}",
                        files={"solution": (filename, payload, "application/octet-stream")},
                    )
            except APIError as error:
                if error.status == 429:
                    seconds = retry_seconds(error)
                    cooldowns[game.slug] = time.monotonic() + seconds
                    error.retry_after = str(seconds)
                raise
            feedback = response.json()
            seconds = (
                feedback.get("cooldownSec", 0) if isinstance(feedback, dict) else 0
            )
            if (
                isinstance(seconds, (int, float))
                and math.isfinite(seconds)
                and seconds > 0
            ):
                cooldowns[game.slug] = time.monotonic() + seconds
            return feedback
