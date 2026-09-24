"""Private Telegram enrollment and room-scoped accepted submissions."""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
import sqlite3
import time
import traceback
from datetime import datetime, timezone
from dataclasses import replace

import httpx

from .client import CCCClient
from .config import Settings
from .service import Service
from .session_pools import SessionPools

logger = logging.getLogger(__name__)


def _safe_error_text(error: BaseException, secrets=()) -> str:
    text = str(error)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[redacted]")
    return text[:1000]


class TelegramPoolBot:
    def __init__(self, settings: Settings, pools: SessionPools):
        self.settings = settings
        self.pools = pools
        self.base = f"https://api.telegram.org/bot{settings.bot_token}"
        self.offset = 0
        self.pending_actions = {}
        self.callback_source_message_id = None
        self.replay_flows = {}
        self.queue_wakeup = asyncio.Event()

    async def _recipient_contest(self, client: CCCClient, job) -> str:
        game_slug = job.get("game_slug")
        trainings = await client.json("GET", "/api/training/active")
        if not isinstance(trainings, list):
            raise ValueError("CCC returned an invalid active-training list")
        if not game_slug:
            normalized_contest = "".join(
                char for char in job["contest"].casefold() if char.isalnum()
            )
            candidates = {
                item.get("gameSlug")
                for item in trainings
                if isinstance(item, dict)
                and isinstance(item.get("gameSlug"), str)
                and "".join(
                    char for char in item["gameSlug"].casefold() if char.isalnum()
                ) in normalized_contest
            }
            if candidates:
                longest = max(len(candidate) for candidate in candidates)
                best = {
                    candidate for candidate in candidates
                    if len(candidate) == longest
                }
                if len(best) == 1:
                    game_slug = best.pop()
                    logger.info(
                        "Derived target game slug from source contest ID job_id=%s game=%s target=CCC…%s",
                        job["id"], game_slug, job["target_uuid"][-6:],
                    )
        if not game_slug:
            return job["contest"]
        matches = [
            item
            for item in trainings
            if isinstance(item, dict)
            and item.get("gameSlug") == game_slug
            and isinstance(item.get("contestName"), str)
        ]
        now = datetime.now(timezone.utc)
        active = []
        for item in matches:
            try:
                start_time = item.get("startTime")
                if not isinstance(start_time, str):
                    continue
                started = datetime.fromisoformat(
                    start_time.replace("Z", "+00:00")
                )
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                duration = float(item["durationMinutes"])
                if (started.timestamp() + duration * 60) > now.timestamp():
                    active.append((started, item["contestName"]))
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
        if not active:
            raise ValueError(
                f"No active training for game {game_slug}; the recipient must start the same challenge"
            )
        active.sort(key=lambda entry: entry[0], reverse=True)
        return active[0][1]

    @staticmethod
    def _menu_markup():
        return {
            "inline_keyboard": [
                [
                    {"text": "🏠 Мои комнаты", "callback_data": "menu:rooms"},
                    {"text": "➕ Создать комнату", "callback_data": "menu:create"},
                ],
                [{"text": "🔗 Подключить аккаунт", "callback_data": "menu:connect"}],
                [{"text": "ℹ️ Помощь", "callback_data": "menu:help"}],
            ]
        }

    async def _telegram(self, method: str, **kwargs):
        async with httpx.AsyncClient(timeout=min(self.settings.timeout, 35)) as client:
            response = await client.post(f"{self.base}/{method}", json=kwargs)
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("ok") is not True:
                raise ValueError("Telegram Bot API rejected the request")
            return body.get("result")

    async def _send(
        self, chat_id: int, text: str, reply_markup=None, replace_previous=True
    ):
        if reply_markup is None:
            reply_markup = self._menu_markup()
        elif isinstance(reply_markup, dict) and isinstance(reply_markup.get("inline_keyboard"), list):
            reply_markup["inline_keyboard"].append(
                [{"text": "🏠 Главное меню", "callback_data": "menu:home"}]
            )
        chat_key = str(chat_id)
        if replace_previous:
            old_ids = await asyncio.to_thread(
                self.pools.tracked_ui_messages, chat_key
            )
            if (
                isinstance(self.callback_source_message_id, int)
                and self.callback_source_message_id not in old_ids
            ):
                old_ids.append(self.callback_source_message_id)
            self.callback_source_message_id = None
            for old_id in old_ids:
                try:
                    await self._telegram(
                        "deleteMessage", chat_id=chat_id, message_id=old_id
                    )
                except (httpx.HTTPError, ValueError) as error:
                    response = getattr(error, "response", None)
                    body = None
                    if response is not None:
                        try:
                            body = response.json()
                        except ValueError:
                            pass
                    description = body.get("description") if isinstance(body, dict) else ""
                    if "message to delete not found" in str(description).casefold():
                        await asyncio.to_thread(
                            self.pools.forget_ui_message, chat_key, old_id
                        )
                    else:
                        logger.warning(
                            "Previous Telegram control message could not be deleted chat=%s message_id=%s (%s)",
                            chat_id, old_id, type(error).__name__,
                        )
                else:
                    await asyncio.to_thread(
                        self.pools.forget_ui_message, chat_key, old_id
                    )
        args = {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
        sent = await self._telegram("sendMessage", **args)
        if isinstance(sent, dict) and isinstance(sent.get("message_id"), int):
            await asyncio.to_thread(
                self.pools.track_ui_message, chat_key, sent["message_id"]
            )

    async def _poll_loop(self):
        while True:
            try:
                updates = await self._telegram(
                    "getUpdates", offset=self.offset, timeout=25,
                    allowed_updates=["message", "callback_query"],
                )
                for update in updates or []:
                    self.offset = int(update.get("update_id", 0)) + 1
                    await self._handle_update(update)
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, ValueError, TypeError, KeyError) as error:
                logger.error(
                    "Telegram pool polling failed (%s): %s\n%s",
                    type(error).__name__,
                    _safe_error_text(error, (self.settings.bot_token,)),
                    "".join(traceback.format_tb(error.__traceback__)),
                )
                await asyncio.sleep(5)

    async def _handle_update(self, update):
        self.callback_source_message_id = None
        callback = update.get("callback_query")
        if isinstance(callback, dict):
            await self._handle_callback(callback)
            return
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        chat_id = chat.get("id")
        user_id = sender.get("id")
        text = message.get("text")
        if chat.get("type") != "private" or not isinstance(user_id, int):
            return
        # Remove room passwords and commands from the visible chat history before processing.
        if isinstance(message.get("message_id"), int):
            try:
                await self._telegram(
                    "deleteMessage", chat_id=chat_id, message_id=message["message_id"]
                )
            except (httpx.HTTPError, ValueError):
                pass
        if not isinstance(text, str):
            return
        parts = text.strip().split()
        if str(user_id) in self.pending_actions and not text.strip().startswith("/"):
            action = self.pending_actions.pop(str(user_id))
            command = {"create": "/create_room", "connect": "/connect", "disconnect": "/disconnect"}[action]
            parts = [command, *parts]
        command = parts[0].split("@", 1)[0].lower() if parts else ""
        user = str(user_id)
        try:
            if command in ("/start", "/help"):
                await self._send(
                    chat_id,
                    "Панель управления комнатами и аккаунтами. Выберите действие кнопками ниже.\n\n"
                    "Также доступны команды:\n"
                    "/create_room <комната> <пароль> — создать комнату\n"
                    "/connect <комната> <пароль> — подключить CCC-аккаунт через одноразовую HTTPS-форму\n"
                    "/disconnect <комната> — удалить свою сессию из комнаты\n"
                    "/rooms — показать ваши комнаты\n"
                    "/queue <комната> — показать ожидающие аккаунты и сроки\n"
                    "/send_now <ID> — запустить одну выбранную отправку\n"
                    "/history <комната> — последние отправки с файлами для повтора\n"
                    "/resend <ID> — повторить отправку выбранному аккаунту",
                )
            elif command == "/create_room" and len(parts) == 3:
                self.pools.create_room(parts[1], parts[2], user)
                self.pools.set_active_room(parts[1], user)
                await self._send(
                    chat_id,
                    f"Комната {parts[1]} создана. Вы создатель и управляете очередью, историей и повторами. Подключите CCC-аккаунт кнопкой ниже.",
                    self._room_markup(parts[1], True),
                )
            elif command == "/connect" and len(parts) == 3:
                if not self.settings.public_origin.startswith("https://"):
                    raise ValueError("Для подключения сессий нужен HTTPS в MCP_PUBLIC_ORIGIN")
                user_label = sender.get("username")
                if user_label:
                    user_label = "@" + user_label
                else:
                    user_label = " ".join(
                        part for part in (sender.get("first_name"), sender.get("last_name"))
                        if isinstance(part, str) and part
                    ) or user
                token = self.pools.create_enrollment_link(
                    parts[1], parts[2], user, user_label[:80]
                )
                url = f"{self.settings.public_origin}/telegram/session/{token}"
                await self._send(
                    chat_id,
                    "Откройте одноразовую ссылку в течение 10 минут и вставьте CCC SESSION cookie. "
                    "Ссылка принимает только один запрос:\n" + url,
                )
            elif command == "/disconnect" and len(parts) == 2:
                removed = self.pools.remove_member(parts[1], user)
                if await asyncio.to_thread(self.pools.active_room, user) == parts[1]:
                    await asyncio.to_thread(self.pools.set_active_room, None, user)
                await self._send(chat_id, "Сессия отключена." if removed else "В этой комнате вашей сессии нет.")
            elif command == "/rooms" and len(parts) == 1:
                rooms = self.pools.rooms_for_user(user)
                await self._send(chat_id, "Комнаты: " + (", ".join(rooms) if rooms else "нет подключённых комнат"))
            elif command == "/queue" and len(parts) == 2:
                await self._send_queue(chat_id, parts[1], user)
            elif command == "/history" and len(parts) == 2:
                await self._send_history(chat_id, parts[1], user)
            elif command == "/send_now" and len(parts) == 2 and parts[1].isdigit():
                await asyncio.to_thread(
                    self.pools.release_job_now, int(parts[1]), user
                )
                await self._send(
                    chat_id,
                    f"Отправка #{parts[1]} для выбранного аккаунта поставлена на ближайшее время.",
                )
            elif command == "/resend" and len(parts) == 2 and parts[1].isdigit():
                await asyncio.to_thread(self.pools.resend_job, int(parts[1]), user)
                await self._send(
                    chat_id,
                    f"Повтор отправки #{parts[1]} выбранному аккаунту поставлен на ближайшее время.",
                )
            else:
                await self._send(chat_id, "Неизвестная команда или неверный формат. Отправьте /help.")
        except (ValueError, sqlite3.Error) as error:
            logger.warning(
                "Telegram pool command rejected command=%s (%s): %s",
                command or "empty command",
                type(error).__name__,
                _safe_error_text(
                    error,
                    (self.settings.bot_token, self.settings.bot_session_encryption_key),
                ),
            )
            await self._send(chat_id, str(error)[:300])
        except Exception as error:
            logger.error(
                "Telegram pool command failed for %s (%s): %s\n%s",
                command or "empty command",
                type(error).__name__,
                _safe_error_text(
                    error,
                    (self.settings.bot_token, self.settings.bot_session_encryption_key),
                ),
                "".join(traceback.format_tb(error.__traceback__)),
            )
            await self._send(chat_id, "Не удалось выполнить команду. Проверьте комнату и пароль.")

    async def _send_queue(self, chat_id: int, room: str, user_id: str):
        is_owner = await asyncio.to_thread(self.pools.is_room_owner, room, user_id)
        items = await asyncio.to_thread(self.pools.queue_snapshot, room, user_id)
        if not items:
            await self._send(chat_id, f"Очередь комнаты {room} пуста.", self._room_markup(room, is_owner))
            return
        for start in range(0, len(items), 12):
            chunk = items[start : start + 12]
            lines = [f"Ожидающие отправки в комнате {room} ({start + 1}–{start + len(chunk)} из {len(items)}):"]
            keyboard_rows = []
            for item in chunk:
                label = item["telegram_label"] or item["telegram_user_id"]
                account_suffix = item["target_uuid"][-6:]
                status_text = (
                    "  отправляется сейчас"
                    if item["status"] == "sending"
                    else "  ожидает отправки"
                )
                lines.append(
                    f"• #{item['job_id']} · {label} · CCC…{account_suffix}\n"
                    f"  {item['contest']} · уровень {item['level']} · файл {item['file_id']} ·{status_text}"
                )
                if item["status"] == "queued":
                    button_label = f"Отправить #{item['job_id']} для {label}"
                    keyboard_rows.append(
                        [{"text": button_label[:60], "callback_data": f"sendnow:{item['job_id']}"}]
                    )
            await self._send(
                chat_id,
                "\n".join(lines),
                {"inline_keyboard": keyboard_rows + self._room_markup(room, is_owner)["inline_keyboard"]},
                replace_previous=start == 0,
            )

    async def _handle_callback(self, callback):
        callback_id = callback.get("id")
        sender = callback.get("from") or {}
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        data = callback.get("data", "")
        if chat.get("type") != "private" or not isinstance(sender.get("id"), int):
            if isinstance(callback_id, str):
                await self._telegram(
                    "answerCallbackQuery", callback_query_id=callback_id,
                    text="Откройте бота в личном чате.", show_alert=True,
                )
            return
        source_message_id = message.get("message_id")
        self.callback_source_message_id = (
            source_message_id if isinstance(source_message_id, int) else None
        )
        if isinstance(data, str) and data.startswith("replay:"):
            await self._handle_replay_callback(callback, data)
            return
        if isinstance(data, str) and data.startswith("queueall:"):
            await self._handle_queueall_callback(callback, data)
            return
        if not isinstance(data, str) or not data.startswith(("sendnow:", "resend:")):
            if isinstance(data, str) and data.startswith("menu:"):
                await self._handle_menu_callback(callback, data)
            return
        action, raw_job_id = data.split(":", 1)
        if not raw_job_id.isdigit():
            return
        try:
            if action == "resend":
                await asyncio.to_thread(
                    self.pools.resend_job, int(raw_job_id), str(sender["id"])
                )
                message_text = f"Повтор отправки #{raw_job_id} выбранному аккаунту поставлен на ближайшее время."
                callback_text = "Повтор для этого аккаунта запущен."
            else:
                await asyncio.to_thread(
                    self.pools.release_job_now, int(raw_job_id), str(sender["id"])
                )
                message_text = f"Отправка #{raw_job_id} для выбранного аккаунта поставлена на ближайшее время."
                callback_text = "Отправка для этого аккаунта запущена."
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text=callback_text,
            )
            await self._send(chat["id"], message_text)
        except (ValueError, sqlite3.Error) as error:
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text=str(error)[:180], show_alert=True,
            )

    async def _handle_menu_callback(self, callback, data: str):
        callback_id = callback.get("id")
        sender = callback.get("from") or {}
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        if chat.get("type") != "private" or not isinstance(sender.get("id"), int):
            if isinstance(callback_id, str):
                await self._telegram(
                    "answerCallbackQuery", callback_query_id=callback_id,
                    text="Откройте меню в личном чате с ботом.", show_alert=True,
                )
            return
        user_id = str(sender["id"])
        action = data.removeprefix("menu:")
        await self._telegram("answerCallbackQuery", callback_query_id=callback_id)
        if action == "home":
            await self._send(chat["id"], "Главное меню")
        elif action.startswith("replay:"):
            room = action.split(":", 1)[1]
            if not await asyncio.to_thread(self.pools.is_room_owner, room, user_id):
                await self._send(chat["id"], "Повторной отправкой уровня управляет только создатель комнаты.")
                return
            catalog = await asyncio.to_thread(self.pools.resend_catalog, room, user_id)
            if not catalog:
                await self._send(
                    chat["id"],
                    "Для этой комнаты пока нет сохранённых файлов уровней для повтора.",
                    self._room_markup(room, True),
                )
                return
            flow_id = secrets.token_urlsafe(6).rstrip("=")
            self.replay_flows[flow_id] = {
                "owner": user_id,
                "room": room,
                "catalog": catalog,
            }
            await self._render_replay_games(chat["id"], flow_id)
        elif action.startswith("queueall:"):
            room = action.split(":", 1)[1]
            if not await asyncio.to_thread(self.pools.is_room_owner, room, user_id):
                await self._send(chat["id"], "Управлять очередью может только создатель комнаты.")
                return
            items = await asyncio.to_thread(self.pools.queue_snapshot, room, user_id)
            count = sum(item["status"] == "queued" for item in items)
            if not count:
                await self._send(chat["id"], "В очереди нет ожидающих отправок.", self._room_markup(room, True))
                return
            await self._send(
                chat["id"],
                f"Подтвердить запуск всех {count} ожидающих отправок комнаты {room}?\n"
                "Все ожидающие отправки будут переданы worker сразу.",
                {"inline_keyboard": [[
                    {"text": f"✅ Подтвердить все ({count})", "callback_data": f"queueall:confirm:{room}"},
                    {"text": "Отмена", "callback_data": f"queueall:cancel:{room}"},
                ]]},
            )
        elif action == "rooms":
            rooms = await asyncio.to_thread(self.pools.rooms_for_user, user_id)
            if not rooms:
                await self._send(chat["id"], "У вас пока нет комнат. Создайте свою или подключитесь к существующей.")
            else:
                keyboard = [
                    [{"text": f"Открыть {room}", "callback_data": f"menu:open:{room}"}]
                    for room in rooms
                ]
                await self._send(chat["id"], "Выберите комнату один раз — откроется её панель управления:", {"inline_keyboard": keyboard})
        elif action == "queue":
            room = await asyncio.to_thread(self.pools.active_room, user_id)
            if room:
                await self._send(chat["id"], "Откройте комнату через «Мои комнаты», чтобы перейти к её панели.")
            else:
                await self._send(chat["id"], "Сначала откройте комнату через «Мои комнаты».")
        elif action == "history":
            await self._send(chat["id"], "История и повторы находятся в панели создателя комнаты.")
        elif action in ("create", "connect"):
            self.pending_actions[user_id] = action
            prompt = (
                "Отправьте название комнаты и пароль одним сообщением (через пробел).\n"
                "Например: team-ccc длинный-пароль"
                if action == "create"
                else "Отправьте название комнаты и пароль комнаты одним сообщением (через пробел)."
            )
            await self._send(chat["id"], prompt)
        elif action == "help":
            await self._send(
                chat["id"],
                "Очередь показывает каждую ожидающую отправку и аккаунт. В истории можно повторить недавнюю отправку конкретному аккаунту. Файлы повтора хранятся 30 дней.",
            )
        elif action.startswith("open:"):
            room = action.split(":", 1)[1]
            rooms = await asyncio.to_thread(self.pools.rooms_for_user, user_id)
            if room not in rooms:
                await self._send(chat["id"], "Комната больше не доступна вашему аккаунту.")
                return
            await asyncio.to_thread(self.pools.set_active_room, room, user_id)
            is_owner = await asyncio.to_thread(self.pools.is_room_owner, room, user_id)
            title = f"Комната {room} · панель создателя" if is_owner else f"Комната {room} · ваш аккаунт"
            await self._send(chat["id"], title, self._room_markup(room, is_owner))
        elif action.startswith(("roomqueue:", "roomhistory:", "roommembers:", "roomdisconnect:")):
            route, room = action.split(":", 1)
            rooms = await asyncio.to_thread(self.pools.rooms_for_user, user_id)
            if room not in rooms:
                await self._send(chat["id"], "Комната больше не доступна вашему аккаунту.")
                return
            await asyncio.to_thread(self.pools.set_active_room, room, user_id)
            if route == "roomqueue":
                if not await asyncio.to_thread(self.pools.is_room_owner, room, user_id):
                    await self._send(chat["id"], "Очередью и повторами управляет только создатель комнаты.")
                    return
                await self._send_queue(chat["id"], room, user_id)
            elif route == "roomhistory":
                if not await asyncio.to_thread(self.pools.is_room_owner, room, user_id):
                    await self._send(chat["id"], "Очередью и повторами управляет только создатель комнаты.")
                    return
                await self._send_history(chat["id"], room, user_id)
            elif route == "roommembers":
                if not await asyncio.to_thread(self.pools.is_room_owner, room, user_id):
                    await self._send(chat["id"], "Список аккаунтов комнаты доступен только её создателю.")
                    return
                await self._send_members(chat["id"], room, user_id)
            else:
                removed = await asyncio.to_thread(self.pools.remove_member, room, user_id)
                if await asyncio.to_thread(self.pools.active_room, user_id) == room:
                    await asyncio.to_thread(self.pools.set_active_room, None, user_id)
                await self._send(
                    chat["id"],
                    "Сессия отключена." if removed else "В этой комнате вашей сессии нет.",
                )

    @staticmethod
    def _room_markup(room: str, is_owner: bool):
        keyboard = []
        if is_owner:
            keyboard.extend([
                [
                    {"text": "⏳ Очередь", "callback_data": f"menu:roomqueue:{room}"},
                    {"text": "📤 История и повторы", "callback_data": f"menu:roomhistory:{room}"},
                ],
                [
                    {"text": "🔁 Повторить уровень", "callback_data": f"menu:replay:{room}"},
                    {"text": "🚀 Подтвердить очередь", "callback_data": f"menu:queueall:{room}"},
                ],
                [{"text": "👥 Подключённые аккаунты", "callback_data": f"menu:roommembers:{room}"}],
            ])
        keyboard.append([{"text": "➖ Отключить мой аккаунт", "callback_data": f"menu:roomdisconnect:{room}"}])
        keyboard.append([{"text": "🔄 Сменить комнату", "callback_data": "menu:rooms"}])
        return {
            "inline_keyboard": keyboard
        }

    @staticmethod
    def _game_label(game):
        slug = game.get("game_slug")
        if not isinstance(slug, str) or not slug:
            return game["contest"]
        display = slug.removeprefix("training-").removeprefix("ccc-")
        display = re.sub(r"^\d{4}[-._]\d{1,2}[-._]", "", display)
        display = display.removeprefix("school-").replace("-", " ").title()
        suffix = game["contest"].rsplit("-", 1)[-1]
        return f"{display} · {suffix}"

    async def _render_replay_games(self, chat_id: int, flow_id: str):
        flow = self.replay_flows[flow_id]
        keyboard = [
            [{
                "text": self._game_label(game)[:60],
                "callback_data": f"replay:game:{flow_id}:{index}",
            }]
            for index, game in enumerate(flow["catalog"])
        ]
        keyboard.append([{"text": "Отмена", "callback_data": f"replay:cancel:{flow_id}"}])
        await self._send(chat_id, "Выберите игру:", {"inline_keyboard": keyboard})

    async def _render_replay_levels(self, chat_id: int, flow_id: str):
        flow = self.replay_flows[flow_id]
        game = flow["catalog"][flow["game_index"]]
        keyboard = [
            [{
                "text": f"Level {item['level']} · {item['files']} сохранённых файлов",
                "callback_data": f"replay:level:{flow_id}:{index}",
            }]
            for index, item in enumerate(game["levels"])
        ]
        keyboard.extend([
            [{"text": "← К выбору игры", "callback_data": f"replay:games:{flow_id}"}],
            [{"text": "Отмена", "callback_data": f"replay:cancel:{flow_id}"}],
        ])
        await self._send(
            chat_id,
            f"{self._game_label(game)}\nВыберите уровень для полного повтора:",
            {"inline_keyboard": keyboard},
        )

    async def _render_replay_targets(self, chat_id: int, flow_id: str):
        flow = self.replay_flows[flow_id]
        if "members" not in flow:
            flow["members"] = await asyncio.to_thread(
                self.pools.members_snapshot, flow["room"], flow["owner"]
            )
            flow["member_tokens"] = {}
            for member in flow["members"]:
                token = secrets.token_urlsafe(5).rstrip("=")
                flow["member_tokens"][token] = member["account_uuid"]
        selected = flow.setdefault("selected", set())
        page = flow.get("page", 0)
        page_size = 8
        members = flow["members"]
        page_count = max(1, (len(members) + page_size - 1) // page_size)
        page = max(0, min(page, page_count - 1))
        flow["page"] = page
        keyboard = []
        for member in members[page * page_size : (page + 1) * page_size]:
            token = next(
                token for token, account_uuid in flow["member_tokens"].items()
                if account_uuid == member["account_uuid"]
            )
            checked = member["account_uuid"] in selected
            label = member["telegram_label"] or member["telegram_user_id"]
            keyboard.append([{
                "text": f"{'✅' if checked else '⬜'} {label} · CCC…{member['account_uuid'][-6:]}"[:60],
                "callback_data": f"replay:toggle:{flow_id}:{token}",
            }])
        keyboard.append([
            {"text": "Выбрать всех", "callback_data": f"replay:all:{flow_id}"},
            {"text": "Снять выбор", "callback_data": f"replay:none:{flow_id}"},
        ])
        if page_count > 1:
            keyboard.append([
                {"text": "←", "callback_data": f"replay:page:{flow_id}:{page - 1}"}
                if page > 0 else {"text": "·", "callback_data": f"replay:page:{flow_id}:0"},
                {"text": f"{page + 1}/{page_count}", "callback_data": f"replay:page:{flow_id}:{page}"},
                {"text": "→", "callback_data": f"replay:page:{flow_id}:{page + 1}"}
                if page + 1 < page_count else {"text": "·", "callback_data": f"replay:page:{flow_id}:{page}"},
            ])
        keyboard.append([
            {"text": "← Уровни", "callback_data": f"replay:levels:{flow_id}"},
            {"text": f"✅ Повторить ({len(selected)})", "callback_data": f"replay:confirm:{flow_id}"},
        ])
        keyboard.append([{"text": "Отмена", "callback_data": f"replay:cancel:{flow_id}"}])
        game = flow["catalog"][flow["game_index"]]
        level = game["levels"][flow["level_index"]]
        await self._send(
            chat_id,
            f"{self._game_label(game)} · Level {level['level']} · {level['files']} сохранённых файлов\n"
            f"Выбрано аккаунтов: {len(selected)}. Можно выбрать одного или нескольких.",
            {"inline_keyboard": keyboard},
        )

    async def _handle_replay_callback(self, callback, data: str):
        callback_id = callback.get("id")
        sender = callback.get("from") or {}
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        parts = data.split(":")
        if chat.get("type") != "private" or not isinstance(sender.get("id"), int):
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text="Откройте меню в личном чате с ботом.", show_alert=True,
            )
            return
        if len(parts) < 3:
            return
        action, flow_id = parts[1], parts[2]
        flow = self.replay_flows.get(flow_id)
        if not flow or flow["owner"] != str(sender["id"]):
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text="Это меню устарело. Откройте повтор уровня заново.", show_alert=True,
            )
            return
        if action == "confirm" and not flow.get("selected"):
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text="Сначала выберите аккаунт.", show_alert=True,
            )
            return
        await self._telegram("answerCallbackQuery", callback_query_id=callback_id)
        chat_id = chat["id"]
        if action == "cancel":
            self.replay_flows.pop(flow_id, None)
            await self._send(chat_id, "Повтор уровня отменён.", self._room_markup(flow["room"], True))
        elif action == "game" and len(parts) == 4 and parts[3].isdigit():
            index = int(parts[3])
            if index >= len(flow["catalog"]):
                return
            flow["game_index"] = index
            await self._render_replay_levels(chat_id, flow_id)
        elif action == "games":
            await self._render_replay_games(chat_id, flow_id)
        elif action == "level" and len(parts) == 4 and parts[3].isdigit():
            levels = flow["catalog"][flow["game_index"]]["levels"]
            index = int(parts[3])
            if index >= len(levels):
                return
            flow["level_index"] = index
            flow["selected"] = set()
            flow.pop("members", None)
            flow.pop("member_tokens", None)
            flow["page"] = 0
            await self._render_replay_targets(chat_id, flow_id)
        elif action == "levels":
            await self._render_replay_levels(chat_id, flow_id)
        elif action == "toggle" and len(parts) == 4:
            account_uuid = flow.get("member_tokens", {}).get(parts[3])
            if account_uuid is None:
                await self._render_replay_targets(chat_id, flow_id)
                return
            selected = flow.setdefault("selected", set())
            selected.symmetric_difference_update({account_uuid})
            await self._render_replay_targets(chat_id, flow_id)
        elif action == "all":
            flow.setdefault("selected", set()).update(
                member["account_uuid"] for member in flow.get("members", [])
            )
            await self._render_replay_targets(chat_id, flow_id)
        elif action == "none":
            flow["selected"] = set()
            await self._render_replay_targets(chat_id, flow_id)
        elif action == "page" and len(parts) == 4 and parts[3].lstrip("-").isdigit():
            flow["page"] = int(parts[3])
            await self._render_replay_targets(chat_id, flow_id)
        elif action == "confirm":
            selected = list(flow.get("selected", set()))
            game = flow["catalog"][flow["game_index"]]
            level = game["levels"][flow["level_index"]]["level"]
            try:
                outcome = await asyncio.to_thread(
                    self.pools.resend_level,
                    flow["room"], game["contest"], level, selected, flow["owner"],
                )
                self.queue_wakeup.set()
                self.replay_flows.pop(flow_id, None)
                text = (
                    f"✅ Level {level} поставлен на повтор: {outcome['queued']} файлов "
                    f"для {outcome['targets']} аккаунтов."
                )
                if outcome["already_active"]:
                    text += f"\nУже в очереди или отправляются: {outcome['already_active']} файловых отправок."
                text += "\nВсе файлы отправляются без искусственных задержек."
                await self._send(chat_id, text, self._room_markup(flow["room"], True))
            except (ValueError, sqlite3.Error) as error:
                await self._send(chat_id, str(error)[:500], self._room_markup(flow["room"], True))

    async def _handle_queueall_callback(self, callback, data: str):
        callback_id = callback.get("id")
        sender = callback.get("from") or {}
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        parts = data.split(":", 2)
        if chat.get("type") != "private" or not isinstance(sender.get("id"), int):
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text="Откройте меню в личном чате с ботом.", show_alert=True,
            )
            return
        if len(parts) != 3:
            return
        _, action, room = parts
        await self._telegram("answerCallbackQuery", callback_query_id=callback_id)
        if action == "cancel":
            await self._send(chat["id"], "Массовый запуск отменён.", self._room_markup(room, True))
            return
        if action != "confirm":
            return
        try:
            count = await asyncio.to_thread(
                self.pools.release_queued_batch, room, str(sender["id"])
            )
            self.queue_wakeup.set()
            await self._send(
                chat["id"],
                f"🚀 В работу сразу передано {count} отправок без искусственных задержек. Статус можно посмотреть в очереди.",
                {
                    "inline_keyboard": [[
                        {"text": "⏳ Смотреть очередь", "callback_data": f"menu:roomqueue:{room}"}
                    ]]
                },
            )
        except (ValueError, sqlite3.Error) as error:
            await self._send(chat["id"], str(error)[:500])

    async def _send_members(self, chat_id: int, room: str, user_id: str):
        members = await asyncio.to_thread(self.pools.members_snapshot, room, user_id)
        if not members:
            message = f"В комнате {room} пока нет подключённых аккаунтов."
        else:
            lines = [f"Подключённые аккаунты комнаты {room} ({len(members)}):"]
            for member in members:
                label = member["telegram_label"] or member["telegram_user_id"]
                suffix = member["account_uuid"][-6:]
                lines.append(f"• {label} · Telegram ID {member['telegram_user_id']} · CCC…{suffix}")
            message = "\n".join(lines)
        await self._send(chat_id, message, self._room_markup(room, True))

    async def _show_room_picker(self, chat_id: int, user_id: str, action: str):
        rooms = await asyncio.to_thread(self.pools.rooms_for_user, user_id)
        if not rooms:
            await self._send(chat_id, "У вас пока нет подключённых комнат. Создайте комнату или подключитесь к существующей.")
            return
        keyboard = [
            [{"text": f"Открыть {room}", "callback_data": f"menu:open:{room}"}]
            for room in rooms
        ]
        await self._send(chat_id, "Выберите комнату:", {"inline_keyboard": keyboard})

    async def _send_history(self, chat_id: int, room: str, user_id: str):
        items = await asyncio.to_thread(self.pools.history_snapshot, room, user_id)
        is_owner = await asyncio.to_thread(self.pools.is_room_owner, room, user_id)
        if not items:
            await self._send(
                chat_id,
                f"В комнате {room} пока нет недавних отправок, для которых сохранён файл повтора.",
                self._room_markup(room, is_owner),
            )
            return
        for start in range(0, len(items), 12):
            chunk = items[start : start + 12]
            lines = [f"Недавние отправки комнаты {room}:"]
            keyboard_rows = []
            for item in chunk:
                label = item["telegram_label"] or item["telegram_user_id"]
                account_suffix = item["target_uuid"][-6:]
                status = {"sent": "принято", "rejected": "отклонено", "failed": "ошибка"}.get(
                    item["status"], item["status"]
                )
                lines.append(
                    f"• #{item['job_id']} · {label} · CCC…{account_suffix} · {status}\n"
                    f"  {item['contest']} · уровень {item['level']} · файл {item['file_id']}"
                )
                keyboard_rows.append(
                    [{
                        "text": f"Повторить #{item['job_id']} для {label}"[:60],
                        "callback_data": f"resend:{item['job_id']}",
                    }]
                )
            await self._send(
                chat_id,
                "\n".join(lines),
                {"inline_keyboard": keyboard_rows + self._room_markup(room, is_owner)["inline_keyboard"]},
                replace_previous=start == 0,
            )

    async def _deliver_one(self):
        job = await asyncio.to_thread(self.pools.claim_due)
        if not job:
            return False
        target_suffix = job["target_uuid"][-6:]
        logger.info(
            "Claimed room queue job id=%s room=%s contest=%s level=%s file_id=%s target=CCC…%s",
            job["id"], job["room"], job["contest"], job["level"],
            job["file_id"], target_suffix,
        )
        session = None
        try:
            session = self.pools.decrypt_session(job["session_cipher"])
            client = CCCClient(replace(self.settings, cookie="", session=session))
            try:
                user = await client.json("GET", "/api/auth/current-user")
                if not isinstance(user, dict) or user.get("uuid") != job["target_uuid"]:
                    raise ValueError("Connected CCC account identity changed")
                service = Service(client)
                target_contest = await self._recipient_contest(client, job)
                logger.info(
                    "Resolved room queue contest job_id=%s game=%s source_contest=%s target_contest=%s target=CCC…%s",
                    job["id"], job.get("game_slug") or "unknown", job["contest"],
                    target_contest, target_suffix,
                )
                result = await service.submit(
                    target_contest, job["level"], job["file_id"],
                    bytes(job["payload"]), job["filename"],
                )
                evaluation = result.get("evaluation") if isinstance(result, dict) else None
                success = isinstance(evaluation, dict) and evaluation.get("isCorrect") is True
                await asyncio.to_thread(
                    self.pools.finish_job, job["id"], "sent" if success else "rejected",
                    None if success else "CCC did not accept this account's submission",
                )
                logger.info(
                    "Finished room queue job id=%s status=%s room=%s target=CCC…%s",
                    job["id"], "sent" if success else "rejected", job["room"],
                    target_suffix,
                )
            finally:
                await client.close()
        except asyncio.CancelledError:
            await asyncio.to_thread(self.pools.finish_job, job["id"], "failed", "worker stopped")
            logger.warning(
                "Room queue job id=%s cancelled during shutdown; recorded as failed",
                job["id"],
            )
            raise
        except Exception as error:
            logger.error(
                "Room queue job id=%s failed room=%s contest=%s level=%s file_id=%s target=CCC…%s (%s): %s\n%s",
                job["id"], job["room"], job["contest"], job["level"],
                job["file_id"], target_suffix, type(error).__name__,
                _safe_error_text(
                    error,
                    (self.settings.bot_token, self.settings.bot_session_encryption_key, session),
                ),
                "".join(traceback.format_tb(error.__traceback__)),
            )
            await asyncio.to_thread(
                self.pools.finish_job, job["id"], "failed", type(error).__name__
            )
        return True

    async def _queue_loop(self):
        last_recovery = 0.0
        while True:
            try:
                now = time.monotonic()
                if now - last_recovery >= 30:
                    await asyncio.to_thread(self.pools.recover_stale_jobs)
                    last_recovery = now
                delivered = await self._deliver_one()
                if not delivered:
                    try:
                        await asyncio.wait_for(self.queue_wakeup.wait(), timeout=2)
                    except asyncio.TimeoutError:
                        pass
                    else:
                        self.queue_wakeup.clear()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error(
                    "Telegram room queue loop failed (%s): %s\n%s",
                    type(error).__name__,
                    _safe_error_text(
                        error,
                        (self.settings.bot_token, self.settings.bot_session_encryption_key),
                    ),
                    "".join(traceback.format_tb(error.__traceback__)),
                )
                await asyncio.sleep(5)

    async def run(self):
        if not self.settings.bot_token:
            logger.info("Telegram pool bot disabled: BOT_TOKEN is not configured")
            return
        try:
            webhook = await self._telegram("getWebhookInfo")
            if isinstance(webhook, dict) and webhook.get("url"):
                logger.warning(
                    "Telegram pool bot needs long polling, but a webhook is configured"
                )
        except (httpx.HTTPError, ValueError) as error:
            logger.error(
                "Could not inspect Telegram webhook status (%s): %s\n%s",
                type(error).__name__,
                _safe_error_text(error, (self.settings.bot_token,)),
                "".join(traceback.format_tb(error.__traceback__)),
            )
        logger.info("Telegram pool bot polling and room queue workers started")
        await asyncio.gather(self._poll_loop(), self._queue_loop())
