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
        self.http = httpx.AsyncClient(
            timeout=30,
            proxy=settings.telegram_effective_proxy_url,
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self.http.aclose()

    async def send_document(
        self,
        content: bytes,
        *,
        filename: str,
        chat_id: str,
        caption: str | None = None,
        message_thread_id: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        if not self.main_token or not chat_id:
            raise RuntimeError("Telegram main bot token/chat id are not configured")
        data = {"chat_id": chat_id}
        if message_thread_id is not None:
            data["message_thread_id"] = str(message_thread_id)

        if caption:
            data["caption"] = caption
        response = await self.http.post(
            f"{self.base_url}/bot{self.main_token}/sendDocument",
            data=data,
            files={"document": (filename, content, content_type)},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API rejected document: {payload}")

    async def send_audio_file(
        self,
        audio: bytes,
        *,
        filename: str,
        chat_id: str,
        caption: str | None = None,
        message_thread_id: int | None = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        await self.send_document(
            audio,
            filename=filename,
            chat_id=chat_id,
            caption=caption,
            message_thread_id=message_thread_id,
            content_type=content_type,
        )

    async def send(
        self,
        text: str,
        *,
        chat_id: str,
        error_channel: bool = False,
        message_thread_id: int | None = None,
    ) -> None:
        token = self.error_token if error_channel else self.main_token
        if not token or not chat_id:
            channel = "error" if error_channel else "main"
            raise RuntimeError(
                f"Telegram {channel} bot token/chat id are not configured"
            )
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        response = await self.http.post(
            f"{self.base_url}/bot{token}/sendMessage",
            json=payload,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API rejected message: {payload}")
