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

    async def _telegram(self, method: str, **kwargs):
        async with httpx.AsyncClient(timeout=min(self.settings.timeout, 35)) as client:
            response = await client.post(f"{self.base}/{method}", json=kwargs)
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("ok") is not True:
                raise ValueError("Telegram Bot API rejected the request")
            return body.get("result")

    async def _send(self, chat_id: int, text: str, reply_markup=None):
        args = {"chat_id": chat_id, "text": text}
        if reply_markup:
            args["reply_markup"] = reply_markup
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
        command = parts[0].split("@", 1)[0].lower() if parts else ""
        user = str(user_id)
        try:
            if command in ("/start", "/help"):
                await self._send(
                    chat_id,
                    "Команды в личке:\n"
                    "/create_room <комната> <пароль> — создать комнату\n"
                    "/connect <комната> <пароль> — подключить CCC-аккаунт через одноразовую HTTPS-форму\n"
                    "/disconnect <комната> — удалить свою сессию из комнаты\n"
                    "/rooms — показать ваши комнаты\n"
                    "/queue <комната> — показать ожидающие аккаунты и сроки\n"
                    "/send_now <ID> — запустить одну выбранную отправку",
                )
            elif command == "/create_room" and len(parts) == 3:
                self.pools.create_room(parts[1], parts[2], user)
                await self._send(chat_id, f"Комната {parts[1]} создана. Подключите аккаунт командой /connect.")
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
                await self._send(chat_id, "Сессия отключена." if removed else "В этой комнате вашей сессии нет.")
            elif command == "/rooms" and len(parts) == 1:
                rooms = self.pools.rooms_for_user(user)
                await self._send(chat_id, "Комнаты: " + (", ".join(rooms) if rooms else "нет подключённых комнат"))
            elif command == "/queue" and len(parts) == 2:
                await self._send_queue(chat_id, parts[1], user)
            elif command == "/send_now" and len(parts) == 2 and parts[1].isdigit():
                await asyncio.to_thread(
                    self.pools.release_job_now, int(parts[1]), user
                )
                await self._send(
                    chat_id,
                    f"Отправка #{parts[1]} для выбранного аккаунта поставлена на ближайшее время.",
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
        items = await asyncio.to_thread(self.pools.queue_snapshot, room, user_id)
        if not items:
            await self._send(chat_id, f"Очередь комнаты {room} пуста.")
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
                {"inline_keyboard": keyboard_rows},
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
        if not isinstance(data, str) or not data.startswith("sendnow:"):
            return
        raw_job_id = data.removeprefix("sendnow:")
        if not raw_job_id.isdigit():
            return
        try:
            await asyncio.to_thread(
                self.pools.release_job_now, int(raw_job_id), str(sender["id"])
            )
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text="Отправка для этого аккаунта запущена.",
            )
            await self._send(
                chat["id"],
                f"Отправка #{raw_job_id} для выбранного аккаунта поставлена на ближайшее время.",
            )
        except (ValueError, sqlite3.Error) as error:
            await self._telegram(
                "answerCallbackQuery", callback_query_id=callback_id,
                text=str(error)[:180], show_alert=True,
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
