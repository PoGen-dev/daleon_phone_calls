from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.minio import MinioStorage
from app.clients.openai_qa import OpenAIQaClient
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.kafka import commit_after, kafka_consumer, kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import AnalysisRequestedEvent
from app.common.repository import Repository
from app.common.retry import retry_or_dead_letter

logger = logging.getLogger(__name__)


async def process_task(
    payload: dict[str, Any],
    *,
    repo: Repository,
    storage: MinioStorage,
    ai: OpenAIQaClient,
    producer: Any,
    settings: Settings,
) -> None:
    call_id = payload.get("call_id")
    if not call_id:
        raise ValueError("Kafka payload has no call_id")
    if not await repo.transcription_exists(call_id):
        object_name = payload.get("object_name")
        if not object_name:
            raise ValueError("Kafka payload has no MinIO object_name")
        audio = await storage.download(object_name)
        source_transcript, raw = await ai.transcribe(
            audio=audio,
            filename=payload.get("filename") or "recording.mp3",
        )
        if not source_transcript.strip():
            raise ValueError("Transcription returned empty text")
        transcript, role_raw = await ai.structure_transcript(source_transcript)
        raw["source_text"] = source_transcript
        raw["role_structuring"] = role_raw
        await repo.save_transcription(
            call_id=call_id,
            transcript=transcript,
            model=settings.openai_transcribe_model,
            language=settings.openai_transcribe_language,
            raw=raw,
        )
        logger.info(
            "Call transcribed: call_id=%s chars=%s roles_validated=%s",
            call_id,
            len(transcript),
            role_raw.get("validated"),
        )
    event = AnalysisRequestedEvent(call_id=call_id)
    await publish_json(producer, settings.topic_to_analyze, event.model_dump(mode="json"), key=call_id)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        kafka_producer(settings) as producer,
        kafka_consumer(settings, topic=settings.topic_to_transcribe, group_suffix="transcriber") as consumer,
    ):
        repo = Repository(pg)
        storage = MinioStorage(settings)
        ai = OpenAIQaClient(settings)
        try:
            async for record in consumer:
                payload = record.value if isinstance(record.value, dict) else {"invalid_payload": record.value}
                call_id = payload.get("call_id")
                try:
                    await process_task(payload, repo=repo, storage=storage, ai=ai, producer=producer, settings=settings)
                except Exception as exc:
                    logger.exception("Transcription task failed", extra={"call_id": call_id})
                    moved = await retry_or_dead_letter(
                        producer=producer,
                        settings=settings,
                        source_topic=record.topic,
                        payload=payload,
                        error=exc,
                        service="transcriber-worker",
                    )
                    if call_id:
                        status = "transcription_failed" if moved else "transcription_retry"
                        await repo.mark_call_status(call_id, status, str(exc))
                await commit_after(record, consumer)
        finally:
            await ai.aclose()


if __name__ == "__main__":
    asyncio.run(run())
