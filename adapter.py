"""
Bale (بله) Platform Adapter for Hermes Agent.

Bale's Bot API is Telegram-compatible, using https://tapi.bale.ai/bot<TOKEN>/
as the base URL. This adapter implements long-polling via getUpdates and
sendMessage, following the same patterns as the Telegram adapter.

Environment variables:
    BALE_BOT_TOKEN          — Bale bot token from @Bot_Father
    BALE_ALLOWED_USERS      — comma-separated chat IDs allowed to use the bot
    BALE_ALLOW_ALL_USERS    — set to "true" to allow all users
    BALE_HOME_CHANNEL       — default chat ID for cron/notification delivery
"""

import asyncio
import datetime
import logging
import os
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
MAX_MESSAGE_LENGTH = 4096


class BaleAdapter(BasePlatformAdapter):
    """Async Bale adapter using the Telegram-compatible Bot API."""

    def __init__(self, config, **kwargs):
        platform = Platform("bale")
        super().__init__(config=config, platform=platform)

        self.token = os.getenv("BALE_BOT_TOKEN", "").strip()
        self.base_url = f"{BALE_API_BASE}{self.token}"

        # Auth
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

        # Runtime state
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._offset: int = 0
        self._running = False

    @property
    def name(self) -> str:
        return "Bale"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Bale Bot API and start long-polling."""
        if not self.token:
            logger.error("Bale: BALE_BOT_TOKEN is not configured")
            self._set_fatal_error(
                "config_missing",
                "BALE_BOT_TOKEN is not configured",
                retryable=False,
            )
            return False

        # Verify token via getMe
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

    # ── Polling ────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main long-polling loop for getUpdates."""
        backoff = 1
        while self._running:
            try:
                updates = await self._get_updates()
                backoff = 1
                for update in updates:
                    try:
                        await self._handle_update(update)
                    except Exception:
                        logger.exception("Bale: error handling update")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "Bale: polling error: %s — retrying in %ds", e, backoff
                )
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
            self._offset = results[-1].get("update_id", self._offset) + 1
        return results

    async def _handle_update(self, update: dict) -> None:
        """Parse an update and dispatch it as a MessageEvent."""
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

    # ── Sending ────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a Bale chat."""
        try:
            params: Dict[str, Any] = {
                "chat_id": str(chat_id),
                "text": content,
            }
            if reply_to:
                params["reply_to_message_id"] = str(reply_to)

            data = await self._api_post("sendMessage", params)
            if not data.get("ok"):
                error_msg = str(data.get("description", data))
                return SendResult(success=False, error=error_msg)

            result = data.get("result", {})
            return SendResult(
                success=True,
                message_id=str(result.get("message_id", "")),
                raw_response=data,
            )
        except Exception as e:
            logger.error("Bale: send failed: %s", e)
            return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing indicator."""
        try:
            await self._api_post(
                "sendChatAction",
                {"chat_id": str(chat_id), "action": "typing"},
            )
        except Exception:
            pass

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
        session = self._session or aiohttp.ClientSession()
        try:
            async with session.get(url, params=params) as resp:
                return await resp.json()
        finally:
            if self._session is None:
                await session.close()

    async def _api_post(
        self, method: str, json_data: Optional[Dict] = None
    ) -> dict:
        """Make a POST request to the Bale API."""
        url = f"{self.base_url}/{method}"
        session = self._session or aiohttp.ClientSession()
        try:
            async with session.post(url, json=json_data) as resp:
                return await resp.json()
        finally:
            if self._session is None:
                await session.close()


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
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="BALE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="BALE_ALLOWED_USERS",
        allow_all_env="BALE_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🟦",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are communicating via Bale (بله), an Iranian messenger. "
            "Bale supports basic markdown formatting similar to Telegram. "
            "Keep responses concise. Use Persian when the user writes in Persian."
        ),
    )
