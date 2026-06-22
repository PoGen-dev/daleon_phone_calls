from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.openai_qa import OpenAIQaClient
from app.common.config import get_settings
from app.common.db import mongo_db, postgres_pool
from app.common.kafka import commit_after, kafka_consumer, kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import DeadLetterEvent, QualityScoredEvent
from app.common.repository import Repository

logger = logging.getLogger(__name__)


async def _dead_letter(
    *,
    producer,
    settings,
    source_topic: str,
    payload: dict[str, Any],
    error: Exception,
) -> None:
    event = DeadLetterEvent(source_topic=source_topic, payload=payload, error=str(error), service="quality-worker")
    await publish_json(producer, settings.topic_dead_letter, event.model_dump(mode="json"), key=payload.get("call_id"))


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        mongo_db(settings) as mongo,
        kafka_producer(settings) as producer,
        kafka_consumer(settings, topic=settings.topic_transcribed, group_suffix="quality") as consumer,
    ):
        repo = Repository(pg, mongo)
        openai_client = OpenAIQaClient(settings)
        async for record in consumer:
            payload = record.value
            call_id = payload.get("call_id")
            try:
                if not call_id:
                    raise ValueError("Kafka payload has no call_id")
                if await repo.quality_exists(call_id):
                    logger.info("Quality score already exists, skipping", extra={"call_id": call_id})
                    await commit_after(record, consumer)
                    continue

                transcript = await repo.get_transcription(call_id)
                if not transcript:
                    raise ValueError(f"No transcription for call_id={call_id}")

                quality, raw = await openai_client.score_quality(transcript=transcript)
                await repo.save_quality(
                    call_id=call_id,
                    quality=quality,
                    model=settings.openai_quality_model,
                    raw=raw,
                )
                event = QualityScoredEvent(call_id=call_id, score=quality.score, model=settings.openai_quality_model)
                await publish_json(producer, settings.topic_quality, event.model_dump(mode="json"), key=call_id)
                await commit_after(record, consumer)
            except Exception as exc:
                logger.exception("Quality scoring failed", extra={"call_id": call_id})
                if call_id:
                    await repo.mark_call_status(call_id, "quality_failed", str(exc))
                await _dead_letter(
                    producer=producer,
                    settings=settings,
                    source_topic=record.topic,
                    payload=payload,
                    error=exc,
                )
                await commit_after(record, consumer)


if __name__ == "__main__":
    asyncio.run(run())
