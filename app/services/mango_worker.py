from __future__ import annotations

import asyncio
import logging
import mimetypes
import re
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any

from app.clients.mango import MangoClient
from app.clients.minio import MinioStorage
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.kafka import kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import CallDiscoveredEvent, CallRecord, DeadLetterEvent, TranscriptionRequestedEvent
from app.common.repository import Repository

logger = logging.getLogger(__name__)
STATE_NAME = "mango_worker_cursor"


def _parse_cursor(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _safe_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip(".")
    return cleaned or fallback


async def _store_call(
    call: CallRecord,
    *,
    repo: Repository,
    mango: MangoClient,
    storage: MinioStorage,
    producer: Any,
    settings: Settings,
) -> None:
    await repo.save_call(call)
    if not call.recording_url and not call.recording_id:
        await repo.mark_call_status(call.id, "no_recording", "Mango payload contains no recording reference")
        return

    existing = await repo.get_call(call.id)
    object_name = existing.get("audio_object_name") if existing else None
    filename = existing.get("audio_filename") if existing else None
    if not object_name or not filename:
        audio, downloaded_name = await mango.download_recording(
            recording_url=call.recording_url, recording_id=call.recording_id
        )
        filename = _safe_component(PurePath(downloaded_name).name, "recording.mp3")
        object_name = f"{_safe_component(call.id, 'call')}/{filename}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        await storage.upload(object_name, audio, content_type=content_type)
        await repo.save_call(
            call,
            status="recorded",
            audio_bucket=storage.bucket,
            audio_object_name=object_name,
            audio_filename=filename,
        )

    discovered = CallDiscoveredEvent(call=call)
    requested = TranscriptionRequestedEvent(call_id=call.id, object_name=object_name, filename=filename)
    await publish_json(producer, settings.topic_mango_raw, discovered.model_dump(mode="json"), key=call.id)
    await publish_json(producer, settings.topic_to_transcribe, requested.model_dump(mode="json"), key=call.id)
    logger.info("Call recording stored and transcription requested", extra={"call_id": call.id, "object": object_name})


async def _store_call_with_retries(call: CallRecord, **dependencies: Any) -> None:
    settings: Settings = dependencies["settings"]
    repo: Repository = dependencies["repo"]
    for attempt in range(1, settings.retry_max_attempts + 1):
        try:
            await _store_call(call, **dependencies)
            return
        except Exception as exc:
            logger.exception("Mango call ingestion failed", extra={"call_id": call.id, "attempt": attempt})
            if attempt < settings.retry_max_attempts:
                await asyncio.sleep(settings.retry_backoff_seconds)
                continue
            event = DeadLetterEvent(
                source_topic=settings.topic_mango_raw,
                payload={"call_id": call.id, "call": call.model_dump(mode="json"), "attempt": attempt},
                error=str(exc),
                service="mango-worker",
                attempts=attempt,
            )
            await publish_json(
                dependencies["producer"], settings.topic_dead_letter, event.model_dump(mode="json"), key=call.id
            )
            try:
                await repo.mark_call_status(call.id, "ingestion_failed", str(exc))
            except Exception:
                logger.exception("Cannot persist ingestion failure status", extra={"call_id": call.id})


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with postgres_pool(settings) as pg, kafka_producer(settings) as producer:
        repo = Repository(pg)
        mango = MangoClient(settings)
        storage = MinioStorage(settings)
        semaphore = asyncio.Semaphore(settings.mango_worker_concurrency)

        async def process(call: CallRecord) -> None:
            async with semaphore:
                await _store_call_with_retries(
                    call, repo=repo, mango=mango, storage=storage, producer=producer, settings=settings
                )

        try:
            await storage.ensure_bucket()
            while True:
                state = await repo.get_state(STATE_NAME) or {}
                last_cursor = _parse_cursor(state.get("cursor"))
                now = datetime.now(timezone.utc)
                date_from = (
                    now - timedelta(seconds=settings.mango_request_window_seconds)
                    if last_cursor is None
                    else last_cursor - timedelta(seconds=settings.mango_lookback_seconds)
                )
                date_to = min(date_from + timedelta(seconds=settings.mango_request_window_seconds), now)
                logger.info(
                    "Polling Mango calls",
                    extra={"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
                )
                try:
                    calls = await mango.fetch_calls(date_from, date_to)
                except Exception:
                    logger.exception("Mango polling failed")
                    await asyncio.sleep(settings.mango_poll_interval_seconds)
                    continue

                logger.info("Mango calls fetched", extra={"count": len(calls)})
                await asyncio.gather(*(process(call) for call in calls))
                max_seen = max(
                    (candidate for call in calls for candidate in (call.finished_at, call.started_at) if candidate),
                    default=date_to,
                )
                await repo.set_state(
                    STATE_NAME,
                    {"cursor": max(max_seen, date_to).isoformat(), "last_poll_at": now.isoformat()},
                )
                await asyncio.sleep(settings.mango_poll_interval_seconds)
        finally:
            await mango.aclose()


if __name__ == "__main__":
    asyncio.run(run())
