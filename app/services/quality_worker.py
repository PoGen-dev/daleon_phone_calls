from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.openai_qa import OpenAIQaClient
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.kafka import commit_after, kafka_consumer, kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import NotificationRequestedEvent
from app.common.repository import Repository
from app.common.retry import retry_or_dead_letter

logger = logging.getLogger(__name__)


async def process_task(
    payload: dict[str, Any], *, repo: Repository, ai: OpenAIQaClient, producer: Any, settings: Settings
) -> None:
    call_id = payload.get("call_id")
    if not call_id:
        raise ValueError("Kafka payload has no call_id")
    if not await repo.quality_exists(call_id):
        transcript = await repo.get_transcription(call_id)
        if not transcript:
            raise ValueError(f"No transcription for call_id={call_id}")
        quality, raw = await ai.score_quality(transcript=transcript)
        await repo.save_quality(call_id=call_id, quality=quality, model=settings.openai_quality_model, raw=raw)
        logger.info("Call analyzed", extra={"call_id": call_id, "score": quality.score})
    event = NotificationRequestedEvent(call_id=call_id)
    await publish_json(producer, settings.topic_to_notify, event.model_dump(mode="json"), key=call_id)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        kafka_producer(settings) as producer,
        kafka_consumer(settings, topic=settings.topic_to_analyze, group_suffix="quality") as consumer,
    ):
        repo = Repository(pg)
        ai = OpenAIQaClient(settings)
        try:
            async for record in consumer:
                payload = record.value if isinstance(record.value, dict) else {"invalid_payload": record.value}
                call_id = payload.get("call_id")
                try:
                    await process_task(payload, repo=repo, ai=ai, producer=producer, settings=settings)
                except Exception as exc:
                    logger.exception("Analysis task failed", extra={"call_id": call_id})
                    moved = await retry_or_dead_letter(
                        producer=producer,
                        settings=settings,
                        source_topic=record.topic,
                        payload=payload,
                        error=exc,
                        service="quality-worker",
                    )
                    if call_id:
                        await repo.mark_call_status(call_id, "analysis_failed" if moved else "analysis_retry", str(exc))
                await commit_after(record, consumer)
        finally:
            await ai.aclose()


if __name__ == "__main__":
    asyncio.run(run())
