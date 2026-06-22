from __future__ import annotations

import httpx

from app.common.config import Settings


class TelegramClient:
    def __init__(self, settings: Settings) -> None:
        self.base_url = settings.telegram_api_base_url.rstrip("/")
        self.main_token = settings.telegram_bot_token.get_secret_value()
        self.main_chat_ids = settings.telegram_main_chat_ids
        self.error_token = settings.telegram_error_bot_token.get_secret_value()
        self.error_chat_ids = settings.telegram_failure_chat_ids
        self.http = httpx.AsyncClient(timeout=30)

    async def aclose(self) -> None:
        await self.http.aclose()

    async def send(self, text: str, *, chat_id: str, error_channel: bool = False) -> None:
        token = self.error_token if error_channel else self.main_token
        if not token or not chat_id:
            channel = "error" if error_channel else "main"
            raise RuntimeError(f"Telegram {channel} bot token/chat id are not configured")
        response = await self.http.post(
            f"{self.base_url}/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": False},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API rejected message: {payload}")
