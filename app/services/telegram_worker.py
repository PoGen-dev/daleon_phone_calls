from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.telegram import TelegramClient
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.formatting import format_analysis_message, format_dead_letter_message
from app.common.kafka import commit_after, kafka_consumer, kafka_producer
from app.common.logging import configure_logging
from app.common.models import QualityResult
from app.common.repository import Repository
from app.common.retry import retry_or_dead_letter

logger = logging.getLogger(__name__)


async def process_notification(
    payload: dict[str, Any], *, repo: Repository, telegram: TelegramClient, settings: Settings
) -> None:
    event_id = payload.get("event_id")
    call_id = payload.get("call_id")
    if not event_id or not call_id:
        raise ValueError("Notification payload has no event_id/call_id")
    if await repo.notification_exists(event_id, call_id, "main"):
        return
    call = await repo.get_call_with_results(call_id)
    if not call or call.get("score") is None:
        raise ValueError(f"No analysis for call_id={call_id}")
    quality = QualityResult.model_validate(call)
    await telegram.send(format_analysis_message(call, quality, settings.dashboard_base_url))
    await repo.save_notification(event_id, call_id, "main")
    logger.info("Telegram notification sent", extra={"call_id": call_id})


async def process_dead_letter(payload: dict[str, Any], *, repo: Repository, telegram: TelegramClient) -> None:
    event_id = payload.get("event_id")
    if not event_id:
        raise ValueError("Dead-letter payload has no event_id")
    if await repo.notification_exists(event_id):
        return
    await telegram.send(format_dead_letter_message(payload), error_channel=True)
    await repo.save_notification(event_id, (payload.get("payload") or {}).get("call_id"), "error")
    logger.info("Dead-letter notification sent", extra={"event_id": event_id})


async def process_dead_letter_with_retries(
    payload: dict[str, Any], *, repo: Repository, telegram: TelegramClient, settings: Settings
) -> None:
    for attempt in range(1, settings.retry_max_attempts + 1):
        try:
            await process_dead_letter(payload, repo=repo, telegram=telegram)
            return
        except Exception:
            if attempt == settings.retry_max_attempts:
                raise
            if settings.retry_backoff_seconds:
                await asyncio.sleep(settings.retry_backoff_seconds)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        kafka_producer(settings) as producer,
        kafka_consumer(
            settings, topic=(settings.topic_to_notify, settings.topic_dead_letter), group_suffix="telegram"
        ) as consumer,
    ):
        repo = Repository(pg)
        telegram = TelegramClient(settings)
        try:
            async for record in consumer:
                payload = record.value if isinstance(record.value, dict) else {"invalid_payload": record.value}
                try:
                    if record.topic == settings.topic_dead_letter:
                        await process_dead_letter_with_retries(
                            payload, repo=repo, telegram=telegram, settings=settings
                        )
                    else:
                        await process_notification(payload, repo=repo, telegram=telegram, settings=settings)
                except Exception as exc:
                    logger.exception("Telegram task failed", extra={"topic": record.topic})
                    if record.topic != settings.topic_dead_letter:
                        await retry_or_dead_letter(
                            producer=producer,
                            settings=settings,
                            source_topic=record.topic,
                            payload=payload,
                            error=exc,
                            service="telegram-worker",
                        )
                await commit_after(record, consumer)
        finally:
            await telegram.aclose()


if __name__ == "__main__":
    asyncio.run(run())
