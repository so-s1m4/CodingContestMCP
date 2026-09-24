"""Private Telegram enrollment and delayed, room-scoped accepted submissions."""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import time
from dataclasses import replace

import httpx

from .client import CCCClient
from .config import Settings
from .service import Service
from .session_pools import SessionPools

logger = logging.getLogger(__name__)


class TelegramPoolBot:
    def __init__(self, settings: Settings, pools: SessionPools):
        self.settings = settings
        self.pools = pools
        self.base = f"https://api.telegram.org/bot{settings.bot_token}"
        self.offset = 0
        self.pending_actions = {}

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

    async def _send(self, chat_id: int, text: str, reply_markup=None):
        if reply_markup is None:
            reply_markup = self._menu_markup()
        elif isinstance(reply_markup, dict) and isinstance(reply_markup.get("inline_keyboard"), list):
            reply_markup["inline_keyboard"].append(
                [{"text": "🏠 Главное меню", "callback_data": "menu:home"}]
            )
        args = {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
        await self._telegram("sendMessage", **args)

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
                logger.warning("Telegram pool polling failed: %s", type(error).__name__)
                await asyncio.sleep(5)

    async def _handle_update(self, update):
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
            await self._send(chat_id, str(error)[:300])
        except Exception as error:
            logger.warning("Telegram pool command failed: %s", type(error).__name__)
            await self._send(chat_id, "Не удалось выполнить команду. Проверьте комнату и пароль.")

    @staticmethod
    def _eta(seconds: float) -> str:
        remaining = max(0, math.ceil(seconds))
        if remaining == 0:
            return "сейчас"
        if remaining < 60:
            return f"{remaining} сек"
        return f"{math.ceil(remaining / 60)} мин"

    async def _send_queue(self, chat_id: int, room: str, user_id: str):
        is_owner = await asyncio.to_thread(self.pools.is_room_owner, room, user_id)
        items = await asyncio.to_thread(self.pools.queue_snapshot, room, user_id)
        if not items:
            await self._send(chat_id, f"Очередь комнаты {room} пуста.", self._room_markup(room, is_owner))
            return
        now = time.time()
        for start in range(0, len(items), 12):
            chunk = items[start : start + 12]
            lines = [f"Ожидающие отправки в комнате {room} ({start + 1}–{start + len(chunk)} из {len(items)}):"]
            keyboard_rows = []
            for item in chunk:
                label = item["telegram_label"] or item["telegram_user_id"]
                account_suffix = item["target_uuid"][-6:]
                eta = self._eta(item["due_at"] - now)
                lines.append(
                    f"• #{item['job_id']} · {label} · CCC…{account_suffix}\n"
                    f"  {item['contest']} · уровень {item['level']} · файл {item['file_id']} — через {eta}"
                )
                button_label = f"Отправить #{item['job_id']} для {label}"
                keyboard_rows.append(
                    [{"text": button_label[:60], "callback_data": f"sendnow:{item['job_id']}"}]
                )
            await self._send(
                chat_id,
                "\n".join(lines),
                {"inline_keyboard": keyboard_rows + self._room_markup(room, is_owner)["inline_keyboard"]},
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
        elif action.startswith(("roomqueue:", "roomhistory:", "roomdisconnect:")):
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
            ])
        keyboard.append([{"text": "➖ Отключить мой аккаунт", "callback_data": f"menu:roomdisconnect:{room}"}])
        keyboard.append([{"text": "🔄 Сменить комнату", "callback_data": "menu:rooms"}])
        return {
            "inline_keyboard": keyboard
        }

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
            )

    async def _deliver_one(self):
        job = await asyncio.to_thread(self.pools.claim_due)
        if not job:
            return False
        try:
            session = self.pools.decrypt_session(job["session_cipher"])
            client = CCCClient(replace(self.settings, cookie="", session=session))
            try:
                user = await client.json("GET", "/api/auth/current-user")
                if not isinstance(user, dict) or user.get("uuid") != job["target_uuid"]:
                    raise ValueError("Connected CCC account identity changed")
                service = Service(client)
                result = await service.submit(
                    job["contest"], job["level"], job["file_id"],
                    bytes(job["payload"]), job["filename"],
                )
                evaluation = result.get("evaluation") if isinstance(result, dict) else None
                success = isinstance(evaluation, dict) and evaluation.get("isCorrect") is True
                await asyncio.to_thread(
                    self.pools.finish_job, job["id"], "sent" if success else "rejected",
                    None if success else "CCC did not accept this account's submission",
                )
            finally:
                await client.close()
        except asyncio.CancelledError:
            await asyncio.to_thread(self.pools.finish_job, job["id"], "failed", "worker stopped")
            raise
        except Exception as error:
            logger.warning("Queued room submission failed: %s", type(error).__name__)
            await asyncio.to_thread(
                self.pools.finish_job, job["id"], "failed", type(error).__name__
            )
        return True

    async def _queue_loop(self):
        while True:
            try:
                delivered = await self._deliver_one()
                if not delivered:
                    await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("Telegram room queue failed: %s", type(error).__name__)
                await asyncio.sleep(5)

    async def run(self):
        if not self.settings.bot_token:
            return
        try:
            webhook = await self._telegram("getWebhookInfo")
            if isinstance(webhook, dict) and webhook.get("url"):
                logger.warning(
                    "Telegram pool bot needs long polling, but a webhook is configured"
                )
        except (httpx.HTTPError, ValueError):
            pass
        await asyncio.gather(self._poll_loop(), self._queue_loop())
