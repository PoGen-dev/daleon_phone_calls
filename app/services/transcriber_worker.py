from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.mango import MangoClient
from app.clients.openai_qa import OpenAIQaClient
from app.common.config import get_settings
from app.common.db import mongo_db, postgres_pool
from app.common.kafka import commit_after, kafka_consumer, kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import CallTranscribedEvent, DeadLetterEvent
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
    event = DeadLetterEvent(source_topic=source_topic, payload=payload, error=str(error), service="transcriber-worker")
    await publish_json(producer, settings.topic_dead_letter, event.model_dump(mode="json"), key=payload.get("call_id"))


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        mongo_db(settings) as mongo,
        kafka_producer(settings) as producer,
        kafka_consumer(settings, topic=settings.topic_to_transcribe, group_suffix="transcriber") as consumer,
    ):
        repo = Repository(pg, mongo)
        mango = MangoClient(settings)
        openai_client = OpenAIQaClient(settings)
        try:
            async for record in consumer:
                payload = record.value
                call_id = payload.get("call_id")
                try:
                    if not call_id:
                        raise ValueError("Kafka payload has no call_id")
                    if await repo.transcription_exists(call_id):
                        logger.info("Transcription already exists, skipping", extra={"call_id": call_id})
                        await commit_after(record, consumer)
                        continue

                    recording_url = payload.get("recording_url")
                    recording_id = payload.get("recording_id")
                    audio, filename = await mango.download_recording(recording_url=recording_url, recording_id=recording_id)
                    transcript, raw = await openai_client.transcribe(audio=audio, filename=filename)
                    if not transcript.strip():
                        raise ValueError("OpenAI transcription returned empty text")

                    await repo.save_transcription(
                        call_id=call_id,
                        transcript=transcript,
                        model=settings.openai_transcribe_model,
                        raw=raw,
                    )
                    event = CallTranscribedEvent(
                        call_id=call_id,
                        transcript_chars=len(transcript),
                        model=settings.openai_transcribe_model,
                    )
                    await publish_json(producer, settings.topic_transcribed, event.model_dump(mode="json"), key=call_id)
                    await commit_after(record, consumer)
                except Exception as exc:
                    logger.exception("Transcription task failed", extra={"call_id": call_id})
                    if call_id:
                        await repo.mark_call_status(call_id, "transcription_failed", str(exc))
                    await _dead_letter(
                        producer=producer,
                        settings=settings,
                        source_topic=record.topic,
                        payload=payload,
                        error=exc,
                    )
                    await commit_after(record, consumer)
        finally:
            await mango.aclose()


if __name__ == "__main__":
    asyncio.run(run())
