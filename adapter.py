"""
Bale (بله) Platform Adapter for Hermes Agent.

Bale's Bot API is Telegram-compatible, using https://tapi.bale.ai/bot<TOKEN>/
as the base URL. This adapter implements long-polling via getUpdates and
sendMessage, following the same patterns as the Telegram adapter.

Configuration via environment variables:
    BALE_BOT_TOKEN          — Bale bot token from @Bot_Father
    BALE_ALLOWED_USERS      — comma-separated chat IDs allowed to use the bot
    BALE_ALLOW_ALL_USERS    — set to "true" to allow all users
    BALE_HOME_CHANNEL       — default chat ID for cron/notification delivery
"""

import asyncio
import datetime
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import aiohttp

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform

logger = logging.getLogger(__name__)

BALE_API_BASE = "https://tapi.bale.ai/bot"

# Bale message size limit (same as Telegram)
MAX_MESSAGE_LENGTH = 4096

# Bale media size limit (~50MB)
MAX_MEDIA_SIZE = 50 * 1024 * 1024

# Command regex
COMMAND_PATTERN = r"^/([a-zA-Z0-9_]+)(?:@(\w+))?\s*(.*)$"

# Stats tracking
_stats: Dict[str, Any] = {
    "messages_sent": 0,
    "messages_received": 0,
    "media_sent": 0,
    "errors": 0,
    "last_error": None,
    "connect_time": None,
    "reconnect_count": 0,
}


class BaleAdapter(BasePlatformAdapter):
    """Async Bale adapter using Telegram-compatible Bot API."""

    def __init__(self, config, **kwargs):
        platform = Platform("bale")
        super().__init__(config=config, platform=platform)

        self.token = os.getenv("BALE_BOT_TOKEN", "").strip()
        self.base_url = f"{BALE_API_BASE}{self.token}"

        # Auth
        extra = getattr(config, "extra", {}) or {}
        allowed_env = os.getenv("BALE_ALLOWED_USERS", "").strip()
        self._allowed_users: set = set()
        if allowed_env:
            self._allowed_users = {
                uid.strip() for uid in allowed_env.split(",") if uid.strip()
            }
        self._allow_all = (
            os.getenv("BALE_ALLOW_ALL_USERS", "").strip().lower()
            in {"1", "true", "yes"}
        )

        # Commands registry
        self._commands: Dict[str, str] = {}
        self._register_default_commands()

        # Runtime state
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._offset: int = 0  # getUpdates offset
        self._running = False
        self._reconnect_delay: int = 1

    def _register_default_commands(self) -> None:
        """Register default commands in Persian."""
        self._commands["start"] = "شروع — ربات را راه‌اندازی کنید"
        self._commands["help"] = "راهنما — نمایش راهنما"
        self._commands["settings"] = "تنظیمات — تغییر تنظیمات"
        self._commands["menu"] = "منو — نمایش منوی اصلی"
        self._commands["status"] = "وضعیت — بررسی وضعیت اتصال"
        self._commands["info"] = "اطلاعات — اطلاعات ربات"
        self._commands["contact"] = "تماس — ارتباط با پشتیبانی"
        self._commands["feedback"] = "بازخورد — ارسال بازخورد"

    @property
    def name(self) -> str:
        return "Bale"

    @property
    def commands(self) -> Dict[str, str]:
        """Return registered commands in Persian."""
        return self._commands

    @property
    def stats(self) -> Dict[str, Any]:
        """Return connection and message statistics."""
        return _stats.copy()

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Bale Bot API via long-polling."""
        if not self.token:
            logger.error("Bale: BALE_BOT_TOKEN is not configured")
            self._set_fatal_error(
                "config_missing",
                "BALE_BOT_TOKEN is not configured",
                retryable=False,
            )
            return False

        # Verify the token with getMe
        try:
            me = await self._api_get("getMe")
            if not me.get("ok"):
                raise RuntimeError(f"getMe failed: {me}")
            bot_info = me.get("result", {})
            logger.info(
                "Bale: connected as @%s (%s)",
                bot_info.get("username", "unknown"),
                bot_info.get("first_name", "Bale Bot"),
            )
        except Exception as e:
            logger.error("Bale: getMe verification failed: %s", e)
            self._set_fatal_error("auth_failed", str(e), retryable=False)
            return False

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        )
        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())

        # Set bot commands
        await self._set_bot_commands()

        # Update stats
        _stats["connect_time"] = datetime.datetime.now().isoformat()
        if is_reconnect:
            _stats["reconnect_count"] = _stats.get("reconnect_count", 0) + 1

        self._mark_connected()
        logger.info("Bale: adapter connected and polling started")
        return True

    async def disconnect(self) -> None:
        """Stop polling and close connections."""
        self._running = False

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass

        self._session = None
        self._mark_disconnected()
        logger.info("Bale: adapter disconnected")
        _stats["connect_time"] = None

    async def _set_bot_commands(self) -> None:
        """Set bot commands on Bale."""
        try:
            commands = [
                {"command": cmd, "description": desc}
                for cmd, desc in self._commands.items()
            ]
            await self._api_post("setMyCommands", {"commands": commands})
        except Exception as e:
            logger.warning("Bale: failed to set commands: %s", e)

    # ── Polling ────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main long-polling loop for getUpdates."""
        backoff = 1
        while self._running:
            try:
                updates = await self._get_updates()
                backoff = 1  # reset on success
                for update in updates:
                    try:
                        await self._handle_update(update)
                        _stats["messages_received"] = _stats.get("messages_received", 0) + 1
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Bale: error handling update")
                        _stats["errors"] = _stats.get("errors", 0) + 1
                        _stats["last_error"] = str(update)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Bale: polling error: %s — retrying in %ds", e, backoff)
                _stats["errors"] = _stats.get("errors", 0) + 1
                _stats["last_error"] = str(e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _get_updates(self) -> List[dict]:
        """Fetch updates via long-polling."""
        params: Dict[str, Any] = {
            "timeout": 30,
            "offset": self._offset,
        }
        data = await self._api_get("getUpdates", params)
        if not data.get("ok"):
            raise RuntimeError(f"getUpdates failed: {data}")

        results = data.get("result", [])
        if results:
            # Advance offset past the last received update
            self._offset = results[-1].get("update_id", self._offset) + 1
        return results

    async def _handle_update(self, update: dict) -> None:
        """Parse an update and dispatch it as a MessageEvent."""
        # Handle callback query (inline keyboard)
        if "callback_query" in update:
            await self._handle_callback_query(update["callback_query"])
            return

        message = update.get("message")
        if not message:
            return

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        if not chat_id:
            return

        # Auth check
        if not self._allow_all and self._allowed_users:
            if chat_id not in self._allowed_users:
                logger.debug("Bale: ignoring unauthorized chat %s", chat_id)
                return

        from_obj = message.get("from", {})
        user_id = str(from_obj.get("id", ""))
        user_name = from_obj.get(
            "first_name", from_obj.get("username", "")
        )
        chat_type_raw = chat.get("type", "private")
        chat_type = "dm" if chat_type_raw == "private" else "group"
        chat_name = chat.get("title") or chat.get("username", "") or chat_id

        text = message.get("text", "")
        message_id = str(message.get("message_id", ""))

        # Check for commands
        if text.startswith("/"):
            await self._handle_command(text, chat_id, user_id, user_name, message_id)
            return

        # Build source and dispatch
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
            message_id=message_id,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
            raw_message=message,
            timestamp=datetime.datetime.now(),
        )

        await self.handle_message(event)

    async def _handle_callback_query(self, callback_query: dict) -> None:
        """Handle inline keyboard callback query."""
        query_id = callback_query.get("id", "")
        message = callback_query.get("message")
        if not message:
            return

        chat = message.get("chat", {})
        chat_id = str(chat.get("id", ""))
        from_obj = callback_query.get("from", {})
        user_id = str(from_obj.get("id", ""))
        user_name = from_obj.get("first_name", "")

        # Answer callback query (required by Bale API)
        try:
            await self._api_post("answerCallbackQuery", {"callback_query_id": query_id})
        except Exception:
            pass

        # Build data from callback
        data = callback_query.get("data", "")
        payload = f"🔔 کلیک روی دکمه دریافت شد!\n\n👤 کاربر: {user_name}\n💬 آیدی: {user_id}\n📊 داده: {data}"

        # Send response with inline keyboard
        keyboard = self._build_help_keyboard()
        await self.send(chat_id, payload, keyboard=keyboard)

    async def _handle_command(self, text: str, chat_id: str, user_id: str, user_name: str, message_id: str) -> None:
        """Handle slash commands in Persian."""
        match = re.match(COMMAND_PATTERN, text)
        if not match:
            return

        command = match.group(1).lower()
        args = match.group(3) or ""

        responses = {
            "start": "سلام! 👋\n\nربات با موفقیت راه‌اندازی شد.\nاز دستورات زیر استفاده کنید:\n• /help — راهنما\n• /status — وضعیت اتصال\n• /menu — منوی اصلی",
            "help": "📚 راهنمای ربات\n\n🔹 دستورات:\n/start — شروع کار با ربات\n/help — نمایش این راهنما\n/settings — تنظیمات ربات\n/menu — منوی اصلی\n/status — بررسی وضعیت\n/info — اطلاعات ربات\n/feedback — ارسال بازخورد\n\n🔹 دکمه‌های کمکی:\nروی دکمه‌های پایین صفحه کلیک کنید.",
            "settings": "⚙️ تنظیمات ربات\n\n🔸 زبان: فارسی\n🔸 حالت: عادی\n🔸 اعلان‌ها: فعال\n\nبرای تغییر تنظیمات با پشتیبانی تماس بگیرید.",
            "menu": "📋 منوی اصلی\n\n🔹 درباره ما\n🔹 خدمات\n🔹 پشتیبانی\n🔹 تنظیمات\n\nروی هر گزینه کلیک کنید.",
            "status": "📊 وضعیت اتصال\n\n🟢 وضعیت: متصل\n🔗 سرور: tapi.bale.ai\n⏰ آخرین به‌روزرسانی: همین الان\n\nاتصال پایدار است.",
            "info": "ℹ️ اطلاعات ربات\n\n🤖 نام: ربات هوشمند\n👤 توسعه‌دهنده: Erfan\n📦 نسخه: 1.1.0\n🔗 API: tapi.bale.ai\n🌐 پلتفرم: بله (Bale)\n\n✅ ربات آماده استفاده است.",
            "contact": "📞 اطلاعات تماس\n\n🎯 پشتیبانی:\n@support_bot\n\n📧 ایمیل:\nsupport@example.com\n\n🌐 وب‌سایت:\nhttps://example.com\n\n⏰ ساعات پاسخگویی:\nشنبه تا پنج‌شنبه ۹ تا ۱۸",
            "feedback": "💬 ارسال بازخورد\n\nمتن پیام خود را ارسال کنید.\nپشتیبانی پس از بررسی پاسخ خواهد داد.",
        }

        response = responses.get(command, f"❌ دستور /{command} یافت نشد.\n/from help — راهنما")

        keyboard = self._build_command_keyboard()
        await self.send(chat_id, response, keyboard=keyboard)

    def _build_command_keyboard(self) -> dict:
        """Build inline keyboard with command buttons in Persian."""
        return {
            "inline_keyboard": [
                [
                    {"text": "🏠 خانه", "callback_data": "menu"},
                    {"text": "❓ راهنما", "callback_data": "help"},
                ],
                [
                    {"text": "⚙️ تنظیمات", "callback_data": "settings"},
                    {"text": "📊 وضعیت", "callback_data": "status"},
                ],
                [
                    {"text": "ℹ️ اطلاعات", "callback_data": "info"},
                    {"text": "💬 بازخورد", "callback_data": "feedback"},
                ],
            ]
        }

    def _build_help_keyboard(self) -> dict:
        """Build inline keyboard with help buttons in Persian."""
        return {
            "inline_keyboard": [
                [
                    {"text": "🏠 منوی اصلی", "callback_data": "menu"},
                    {"text": "📞 تماس", "callback_data": "contact"},
                ],
                [
                    {"text": "⚙️ تنظیمات", "callback_data": "settings"},
                    {"text": "❌ بستن", "callback_data": "close"},
                ],
            ]
        }

    # ── Sending ────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        keyboard: Optional[Dict] = None,
    ) -> SendResult:
        """Send a text message to a Bale chat."""
        try:
            params: Dict[str, Any] = {
                "chat_id": str(chat_id),
                "text": content,
            }
            if reply_to:
                params["reply_to_message_id"] = str(reply_to)
            if keyboard:
                params["reply_markup"] = json.dumps(keyboard)

            data = await self._api_post("sendMessage", params)
            if not data.get("ok"):
                error_msg = str(data.get("description", data))
                return SendResult(success=False, error=error_msg)

            result = data.get("result", {})
            _stats["messages_sent"] = _stats.get("messages_sent", 0) + 1
            return SendResult(
                success=True,
                message_id=str(result.get("message_id", "")),
                raw_response=data,
            )
        except Exception as e:
            logger.error("Bale: send failed: %s", e)
            _stats["errors"] = _stats.get("errors", 0) + 1
            _stats["last_error"] = str(e)
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_photo(
        self,
        chat_id: str,
        photo: bytes,
        caption: str = "",
        reply_to: Optional[str] = None,
        keyboard: Optional[Dict] = None,
    ) -> SendResult:
        """Send a photo to a Bale chat."""
        try:
            # Validate file size
            if len(photo) > MAX_MEDIA_SIZE:
                return SendResult(
                    success=False,
                    error=f"Photo too large: {len(photo)} bytes (max {MAX_MEDIA_SIZE})",
                )

            # Prepare form data
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("photo", photo, filename="photo.jpg", content_type="image/jpeg")
            if caption:
                data.add_field("caption", caption)
            if reply_to:
                data.add_field("reply_to_message_id", str(reply_to))
            if keyboard:
                data.add_field("reply_markup", json.dumps(keyboard))

            url = f"{self.base_url}/sendPhoto"
            async with self._session.post(url, data=data) as resp:
                result = await resp.json()
                if not result.get("ok"):
                    return SendResult(success=False, error=str(result.get("description", result)))

                _stats["media_sent"] = _stats.get("media_sent", 0) + 1
                msg = result.get("result", {})
                return SendResult(
                    success=True,
                    message_id=str(msg.get("message_id", "")),
                    raw_response=result,
                )
        except Exception as e:
            logger.error("Bale: send_photo failed: %s", e)
            _stats["errors"] = _stats.get("errors", 0) + 1
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_video(
        self,
        chat_id: str,
        video: bytes,
        caption: str = "",
        reply_to: Optional[str] = None,
        keyboard: Optional[Dict] = None,
    ) -> SendResult:
        """Send a video to a Bale chat."""
        try:
            # Validate file size
            if len(video) > MAX_MEDIA_SIZE:
                return SendResult(
                    success=False,
                    error=f"Video too large: {len(video)} bytes (max {MAX_MEDIA_SIZE})",
                )

            # Prepare form data
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("video", video, filename="video.mp4", content_type="video/mp4")
            if caption:
                data.add_field("caption", caption)
            if reply_to:
                data.add_field("reply_to_message_id", str(reply_to))
            if keyboard:
                data.add_field("reply_markup", json.dumps(keyboard))

            url = f"{self.base_url}/sendVideo"
            async with self._session.post(url, data=data) as resp:
                result = await resp.json()
                if not result.get("ok"):
                    return SendResult(success=False, error=str(result.get("description", result)))

                _stats["media_sent"] = _stats.get("media_sent", 0) + 1
                msg = result.get("result", {})
                return SendResult(
                    success=True,
                    message_id=str(msg.get("message_id", "")),
                    raw_response=result,
                )
        except Exception as e:
            logger.error("Bale: send_video failed: %s", e)
            _stats["errors"] = _stats.get("errors", 0) + 1
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_audio(
        self,
        chat_id: str,
        audio: bytes,
        caption: str = "",
        reply_to: Optional[str] = None,
        keyboard: Optional[Dict] = None,
    ) -> SendResult:
        """Send an audio file to a Bale chat."""
        try:
            # Validate file size
            if len(audio) > MAX_MEDIA_SIZE:
                return SendResult(
                    success=False,
                    error=f"Audio too large: {len(audio)} bytes (max {MAX_MEDIA_SIZE})",
                )

            # Prepare form data
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("audio", audio, filename="audio.mp3", content_type="audio/mpeg")
            if caption:
                data.add_field("caption", caption)
            if reply_to:
                data.add_field("reply_to_message_id", str(reply_to))
            if keyboard:
                data.add_field("reply_markup", json.dumps(keyboard))

            url = f"{self.base_url}/sendAudio"
            async with self._session.post(url, data=data) as resp:
                result = await resp.json()
                if not result.get("ok"):
                    return SendResult(success=False, error=str(result.get("description", result)))

                _stats["media_sent"] = _stats.get("media_sent", 0) + 1
                msg = result.get("result", {})
                return SendResult(
                    success=True,
                    message_id=str(msg.get("message_id", "")),
                    raw_response=result,
                )
        except Exception as e:
            logger.error("Bale: send_audio failed: %s", e)
            _stats["errors"] = _stats.get("errors", 0) + 1
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_document(
        self,
        chat_id: str,
        document: bytes,
        filename: str = "file",
        caption: str = "",
        reply_to: Optional[str] = None,
        keyboard: Optional[Dict] = None,
    ) -> SendResult:
        """Send a document to a Bale chat."""
        try:
            # Validate file size
            if len(document) > MAX_MEDIA_SIZE:
                return SendResult(
                    success=False,
                    error=f"Document too large: {len(document)} bytes (max {MAX_MEDIA_SIZE})",
                )

            # Prepare form data
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("document", document, filename=filename)
            if caption:
                data.add_field("caption", caption)
            if reply_to:
                data.add_field("reply_to_message_id", str(reply_to))
            if keyboard:
                data.add_field("reply_markup", json.dumps(keyboard))

            url = f"{self.base_url}/sendDocument"
            async with self._session.post(url, data=data) as resp:
                result = await resp.json()
                if not result.get("ok"):
                    return SendResult(success=False, error=str(result.get("description", result)))

                _stats["media_sent"] = _stats.get("media_sent", 0) + 1
                msg = result.get("result", {})
                return SendResult(
                    success=True,
                    message_id=str(msg.get("message_id", "")),
                    raw_response=result,
                )
        except Exception as e:
            logger.error("Bale: send_document failed: %s", e)
            _stats["errors"] = _stats.get("errors", 0) + 1
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing indicator (chat action)."""
        try:
            await self._api_post(
                "sendChatAction",
                {"chat_id": str(chat_id), "action": "typing"},
            )
        except Exception:
            pass  # Best-effort

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic chat info."""
        return {
            "name": chat_id,
            "type": "dm",
            "chat_id": str(chat_id),
        }

    # ── API helpers ────────────────────────────────────────────────────────

    async def _api_get(
        self, method: str, params: Optional[Dict] = None
    ) -> dict:
        """Make a GET request to the Bale API."""
        url = f"{self.base_url}/{method}"
        if self._session and not self._session.closed:
            async with self._session.get(url, params=params) as resp:
                return await resp.json()
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    return await resp.json()

    async def _api_post(
        self, method: str, json_data: Optional[Dict] = None
    ) -> dict:
        """Make a POST request to the Bale API."""
        url = f"{self.base_url}/{method}"
        if self._session and not self._session.closed:
            async with self._session.post(url, json=json_data) as resp:
                return await resp.json()
        else:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=json_data) as resp:
                    return await resp.json()


# ── Plugin registration hooks ──────────────────────────────────────────────

def check_requirements() -> bool:
    """Check if Bale is minimally configured."""
    return bool(os.getenv("BALE_BOT_TOKEN", "").strip())


def validate_config(config) -> bool:
    """Validate that the platform config has a token."""
    return bool(os.getenv("BALE_BOT_TOKEN", "").strip())


def is_connected(config) -> bool:
    """Check whether Bale is configured."""
    return bool(os.getenv("BALE_BOT_TOKEN", "").strip())


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env vars before adapter construction."""
    token = os.getenv("BALE_BOT_TOKEN", "").strip()
    if not token:
        return None

    seed: dict = {"token": token}

    home = os.getenv("BALE_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": "Bale Home",
        }

    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process delivery for cron jobs running separately from gateway."""
    token = os.getenv("BALE_BOT_TOKEN", "").strip()
    if not token:
        return {"error": "BALE_BOT_TOKEN not configured"}

    # Resolve target: explicit chat_id > home channel > error
    home = os.getenv("BALE_HOME_CHANNEL", "").strip()
    target = chat_id or home
    if not target:
        return {"error": "No chat_id or BALE_HOME_CHANNEL configured"}

    base_url = f"{BALE_API_BASE}{token}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/sendMessage",
                json={"chat_id": str(target), "text": message},
            ) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return {
                        "success": True,
                        "message_id": str(
                            data.get("result", {}).get("message_id", "")
                        ),
                    }
                return {"error": f"Bale API error: {data}"}
    except Exception as e:
        return {"error": f"Bale standalone send failed: {e}"}


def interactive_setup() -> None:
    """Interactive `hermes gateway setup` flow for Bale."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Bale (بله)")

    existing_token = get_env_value("BALE_BOT_TOKEN")
    if existing_token:
        print_info("Bale: already configured")
        if not prompt_yes_no("Reconfigure Bale?", False):
            return

    print_info("Connect Hermes to the Bale messenger (بله).")
    print_info("   1. Open Bale and message @Bot_Father")
    print_info("   2. Send /newbot and follow the prompts")
    print_info("   3. Copy the bot token")
    print()

    token = prompt("Bale bot token", password=True)
    if not token:
        print_warning("Token is required — skipping Bale setup")
        return
    save_env_value("BALE_BOT_TOKEN", token.strip())

    # Verify token
    import requests as _requests
    try:
        resp = _requests.get(
            f"{BALE_API_BASE}{token.strip()}/getMe", timeout=10
        )
        data = resp.json()
        if data.get("ok"):
            bot = data.get("result", {})
            print_success(
                f"Token verified: @{bot.get('username', 'unknown')}"
            )
        else:
            print_warning(f"Token verification failed: {data}")
    except Exception as e:
        print_warning(f"Could not verify token: {e}")

    print()
    print_info("🔒 Access control")
    allow_all = prompt_yes_no("Allow all Bale users to talk to the bot?", False)
    if allow_all:
        save_env_value("BALE_ALLOW_ALL_USERS", "true")
        save_env_value("BALE_ALLOWED_USERS", "")
    else:
        save_env_value("BALE_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed chat IDs (comma-separated, leave empty to deny everyone)",
            default="",
        )
        if allowed:
            save_env_value("BALE_ALLOWED_USERS", allowed.replace(" ", ""))

    home = prompt("Home channel chat ID for notifications (optional)", default="")
    if home:
        save_env_value("BALE_HOME_CHANNEL", home.strip())

    print()
    print_success("Bale configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway: hermes gateway restart")


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="bale",
        label="Bale",
        adapter_factory=lambda cfg: BaleAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["BALE_BOT_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        # Env-driven auto-configuration
        env_enablement_fn=_env_enablement,
        # Cron delivery support
        cron_deliver_env_var="BALE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        # Auth env vars
        allowed_users_env="BALE_ALLOWED_USERS",
        allow_all_env="BALE_ALLOW_ALL_USERS",
        # Message size limit
        max_message_length=MAX_MESSAGE_LENGTH,
        # Display
        emoji="🟦",
        pii_safe=True,
        allow_update_command=True,
        # LLM guidance
        platform_hint=(
            "You are communicating via Bale (بله), an Iranian messenger. "
            "Bale supports basic markdown formatting similar to Telegram. "
            "Keep responses concise. Use Persian when the user writes in Persian."
        ),
    )
