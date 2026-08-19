from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.clients.minio import MinioStorage
from app.clients.openai_qa import OpenAIQaClient
from app.common.audio import normalize_phone_audio
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.kafka import commit_after, kafka_consumer, kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import AnalysisRequestedEvent
from app.common.repository import Repository
from app.common.retry import retry_or_dead_letter

logger = logging.getLogger(__name__)


def _role_context(call: dict[str, Any] | None) -> dict[str, Any]:
    call = call or {}
    raw = call.get("raw") or {}
    summary = raw.get("mango_employee_summary") or {}
    if not isinstance(summary, dict):
        summary = {}
    direction = str(call.get("direction") or "")
    from_number = call.get("from_number") or raw.get("from_number")
    to_number = call.get("to_number") or raw.get("to_number")
    if direction == "incoming":
        client_number = from_number
        extension = raw.get("to_extension")
    elif direction == "outgoing":
        client_number = to_number
        extension = raw.get("from_extension")
    else:
        client_number = from_number or to_number
        extension = raw.get("to_extension") or raw.get("from_extension")
    return {
        "direction": direction or None,
        "client_number": client_number,
        "employee_extension": summary.get("extension") or extension,
        "employee_name": summary.get("name"),
        "from_number": from_number,
        "to_number": to_number,
    }


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

    transcript: str | None = None
    if not await repo.transcription_exists(call_id):
        object_name = payload.get("object_name")
        if not object_name:
            raise ValueError("Kafka payload has no MinIO object_name")
        audio = await storage.download(object_name)
        source_filename = payload.get("filename") or "recording.mp3"
        normalized_audio, normalized_filename, preprocess_raw = (
            await normalize_phone_audio(
                audio,
                enabled=settings.transcription_preprocess_audio,
                timeout_seconds=settings.transcription_ffmpeg_timeout_seconds,
            )
        )
        stt_filename = (
            normalized_filename if preprocess_raw.get("applied") else source_filename
        )
        source_transcript, raw = await ai.transcribe(
            audio=normalized_audio,
            filename=stt_filename,
        )
        if not source_transcript.strip():
            raise ValueError("Transcription returned empty text")
        call = await repo.get_call(call_id)
        transcript, role_raw = await ai.structure_transcript(
            source_transcript, context=_role_context(call)
        )
        raw["source_text"] = source_transcript
        raw["audio_preprocessing"] = preprocess_raw
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

    if not await repo.classification_exists(call_id):
        transcript = transcript or await repo.get_transcription(call_id)
        if not transcript:
            raise ValueError(f"No transcription for call_id={call_id}")
        classification, classification_raw = await ai.classify_call(
            transcript=transcript
        )
        await repo.save_classification(
            call_id=call_id,
            classification=classification,
            model=settings.openai_classification_model,
            raw=classification_raw,
        )
        logger.info(
            "Call classified: call_id=%s call_type=%s confidence=%s",
            call_id,
            classification.call_type,
            classification.confidence,
        )

    event = AnalysisRequestedEvent(call_id=call_id)
    await publish_json(
        producer, settings.topic_to_analyze, event.model_dump(mode="json"), key=call_id
    )


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with (
        postgres_pool(settings) as pg,
        kafka_producer(settings) as producer,
        kafka_consumer(
            settings, topic=settings.topic_to_transcribe, group_suffix="transcriber"
        ) as consumer,
    ):
        repo = Repository(pg)
        storage = MinioStorage(settings)
        ai = OpenAIQaClient(settings)
        try:
            async for record in consumer:
                payload = (
                    record.value
                    if isinstance(record.value, dict)
                    else {"invalid_payload": record.value}
                )
                call_id = payload.get("call_id")
                try:
                    await process_task(
                        payload,
                        repo=repo,
                        storage=storage,
                        ai=ai,
                        producer=producer,
                        settings=settings,
                    )
                except Exception as exc:
                    logger.exception(
                        "Transcription task failed", extra={"call_id": call_id}
                    )
                    moved = await retry_or_dead_letter(
                        producer=producer,
                        settings=settings,
                        source_topic=record.topic,
                        payload=payload,
                        error=exc,
                        service="transcriber-worker",
                    )
                    if call_id:
                        status = (
                            "transcription_failed" if moved else "transcription_retry"
                        )
                        await repo.mark_call_status(call_id, status, str(exc))
                await commit_after(record, consumer)
        finally:
            await ai.aclose()


if __name__ == "__main__":
    asyncio.run(run())
