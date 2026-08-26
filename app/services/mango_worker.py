from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePath
from typing import Any

from app.clients.mango import MangoClient
from app.clients.minio import MinioStorage
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.kafka import kafka_producer
from app.common.logging import configure_logging
from app.common.models import (
    CallDiscoveredEvent,
    CallRecord,
    DeadLetterEvent,
    OutboxMessage,
    TranscriptionRequestedEvent,
)
from app.common.outbox import publish_pending_outbox
from app.common.repository import Repository

logger = logging.getLogger(__name__)
STATE_NAME = "mango_worker_cursor"


@dataclass(frozen=True)
class MangoSource:
    name: str
    client: MangoClient
    state_name: str


def _configured_mango_sources(settings: Settings) -> list[MangoSource]:
    primary_key = settings.mango_api_key.get_secret_value().strip()
    primary_salt = settings.mango_api_salt.get_secret_value().strip()
    if bool(primary_key) != bool(primary_salt):
        raise RuntimeError(
            "MANGO_API_KEY and MANGO_API_SALT must either both be set or both be empty"
        )

    specs: list[tuple[str, object, object, bool, str]] = []
    used_names: set[str] = set()
    if primary_key and primary_salt:
        specs.append(
            (
                "primary",
                settings.mango_api_key,
                settings.mango_api_salt,
                False,
                STATE_NAME,
            )
        )
        used_names.add("primary")

    for account in settings.mango_accounts:
        account_key = account.api_key.get_secret_value().strip()
        account_salt = account.api_salt.get_secret_value().strip()
        if not account_key or not account_salt:
            raise RuntimeError(
                f"MANGO_ACCOUNTS entry {account.name!r} must contain api_key and api_salt"
            )
        if account.name in used_names:
            raise RuntimeError(f"Duplicate Mango account name: {account.name}")
        used_names.add(account.name)
        specs.append(
            (
                account.name,
                account.api_key,
                account.api_salt,
                True,
                f"{STATE_NAME}:{account.name}",
            )
        )

    if not specs:
        raise RuntimeError(
            "No Mango credentials configured: set MANGO_API_KEY/MANGO_API_SALT "
            "or MANGO_ACCOUNTS"
        )

    return [
        MangoSource(
            name=name,
            client=MangoClient(
                settings,
                api_key=api_key,
                api_salt=api_salt,
                account_name=name,
                namespace_call_ids=namespace_call_ids,
            ),
            state_name=state_name,
        )
        for name, api_key, api_salt, namespace_call_ids, state_name in specs
    ]


def _parse_cursor(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _safe_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip(".")
    return cleaned or fallback


def _poll_window(
    last_cursor: datetime | None, now: datetime, settings: Settings
) -> tuple[datetime, datetime]:
    if last_cursor is None:
        return now - timedelta(seconds=settings.mango_request_window_seconds), now
    cursor = min(last_cursor, now)
    date_from = cursor - timedelta(seconds=settings.mango_lookback_seconds)
    date_to = min(
        cursor + timedelta(seconds=settings.mango_request_window_seconds), now
    )
    return date_from, date_to


def _latest_recorded_call(calls: list[CallRecord]) -> CallRecord | None:
    candidates = [call for call in calls if call.recording_url or call.recording_id]
    if not candidates:
        return None
    earliest = datetime.min.replace(tzinfo=timezone.utc)
    return max(
        candidates, key=lambda call: call.finished_at or call.started_at or earliest
    )


async def _store_call(
    call: CallRecord,
    *,
    repo: Repository,
    mango: MangoClient,
    storage: MinioStorage,
    settings: Settings,
) -> None:
    call_payload = json.dumps(
        call.model_dump(mode="json"),
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )
    logger.info("Mango call received: call_id=%s call=%s", call.id, call_payload)

    await repo.save_call(call)
    if not call.recording_url and not call.recording_id:
        await repo.mark_call_status(
            call.id, "no_recording", "Mango payload contains no recording reference"
        )
        return

    existing = await repo.get_call(call.id)
    object_name = existing.get("audio_object_name") if existing else None
    filename = existing.get("audio_filename") if existing else None
    uploaded_object: str | None = None
    try:
        if not object_name or not filename:
            audio, downloaded_name = await mango.download_recording(
                recording_url=call.recording_url, recording_id=call.recording_id
            )
            filename = _safe_component(PurePath(downloaded_name).name, "recording.mp3")
            object_name = f"{_safe_component(call.id, 'call')}/{filename}"
            content_type = (
                mimetypes.guess_type(filename)[0] or "application/octet-stream"
            )
            await storage.upload(object_name, audio, content_type=content_type)
            uploaded_object = object_name

        if not object_name or not filename:
            raise RuntimeError(f"Call {call.id} has no persisted recording location")
        discovered = CallDiscoveredEvent(call=call)
        requested = TranscriptionRequestedEvent(
            call_id=call.id, object_name=object_name, filename=filename
        )
        messages = [
            OutboxMessage(
                topic=settings.topic_mango_raw,
                key=call.id,
                payload=discovered.model_dump(mode="json"),
                dedupe_key=f"mango:{call.id}:discovered",
            ),
            OutboxMessage(
                topic=settings.topic_to_transcribe,
                key=call.id,
                payload=requested.model_dump(mode="json"),
                dedupe_key=f"mango:{call.id}:transcription",
            ),
        ]
        await repo.save_call_and_enqueue(
            call,
            status="recorded",
            audio_bucket=storage.bucket,
            audio_object_name=object_name,
            audio_filename=filename,
            messages=messages,
        )
    except BaseException:
        if uploaded_object:
            try:
                await asyncio.shield(storage.remove(uploaded_object))
            except BaseException:
                logger.exception(
                    "Cannot remove MinIO object after database failure",
                    extra={"call_id": call.id, "object": uploaded_object},
                )
        raise
    logger.info(
        "Call recording stored and events enqueued",
        extra={"call_id": call.id, "object": object_name},
    )


async def _store_call_with_retries(call: CallRecord, **dependencies: Any) -> None:
    settings: Settings = dependencies["settings"]
    repo: Repository = dependencies["repo"]
    for attempt in range(1, settings.retry_max_attempts + 1):
        try:
            await _store_call(call, **dependencies)
            return
        except Exception as exc:
            logger.exception(
                "Mango call ingestion failed",
                extra={"call_id": call.id, "attempt": attempt},
            )
            if attempt < settings.retry_max_attempts:
                await asyncio.sleep(settings.retry_backoff_seconds)
                continue
            event = DeadLetterEvent(
                source_topic=settings.topic_mango_raw,
                payload={
                    "call_id": call.id,
                    "call": call.model_dump(mode="json"),
                    "attempt": attempt,
                },
                error=str(exc),
                service="mango-worker",
                attempts=attempt,
            )
            message = OutboxMessage(
                topic=settings.topic_dead_letter,
                key=call.id,
                payload=event.model_dump(mode="json"),
                dedupe_key=f"mango:{call.id}:ingestion-failed",
            )
            await repo.mark_call_failed_and_enqueue(call.id, str(exc), message)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with postgres_pool(settings) as pg, kafka_producer(settings) as producer:
        repo = Repository(pg)
        sources = _configured_mango_sources(settings)
        storage = MinioStorage(settings)
        semaphore = asyncio.Semaphore(settings.mango_worker_concurrency)

        logger.info(
            "Mango sources configured: count=%s accounts=%s",
            len(sources),
            ",".join(source.name for source in sources),
        )

        async def process(source: MangoSource, call: CallRecord) -> None:
            async with semaphore:
                await _store_call_with_retries(
                    call,
                    repo=repo,
                    mango=source.client,
                    storage=storage,
                    settings=settings,
                )

        async def flush_outbox() -> None:
            while True:
                published = await publish_pending_outbox(
                    repo, producer, limit=settings.outbox_batch_size
                )
                if published < settings.outbox_batch_size:
                    return

        async def latest_for_source(
            source: MangoSource, date_from: datetime, date_to: datetime
        ) -> tuple[MangoSource, CallRecord] | None:
            try:
                calls = await source.client.fetch_calls(date_from, date_to)
            except Exception:
                logger.exception(
                    "Mango latest-call test polling failed",
                    extra={"mango_account": source.name},
                )
                return None
            latest = _latest_recorded_call(calls)
            logger.info(
                "Mango latest-call source fetched: account=%s count=%s selected=%s",
                source.name,
                len(calls),
                latest.id if latest else None,
            )
            return (source, latest) if latest is not None else None

        async def poll_source(source: MangoSource, now: datetime) -> bool | None:
            state = await repo.get_state(source.state_name) or {}
            last_cursor = _parse_cursor(state.get("cursor"))
            date_from, date_to = _poll_window(last_cursor, now, settings)
            logger.info(
                "Polling Mango calls: account=%s date_from=%s date_to=%s",
                source.name,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            try:
                calls = await source.client.fetch_calls(date_from, date_to)
                logger.info(
                    "Mango calls fetched: account=%s count=%s",
                    source.name,
                    len(calls),
                )
                await asyncio.gather(*(process(source, call) for call in calls))
                await repo.set_state(
                    source.state_name,
                    {
                        "cursor": date_to.isoformat(),
                        "last_poll_at": now.isoformat(),
                        "mango_account": source.name,
                    },
                )
                return date_to >= now
            except Exception:
                logger.exception(
                    "Mango polling failed", extra={"mango_account": source.name}
                )
                return None

        try:
            await storage.ensure_bucket()
            if settings.mango_test_latest_call_only:
                await flush_outbox()
                now = datetime.now(timezone.utc)
                date_from = now - timedelta(
                    seconds=settings.mango_test_lookback_seconds
                )
                logger.info(
                    "Mango latest-call test started: date_from=%s date_to=%s accounts=%s",
                    date_from.isoformat(),
                    now.isoformat(),
                    ",".join(source.name for source in sources),
                )
                results = await asyncio.gather(
                    *(latest_for_source(source, date_from, now) for source in sources)
                )
                candidates = [result for result in results if result is not None]
                if not candidates:
                    logger.warning(
                        "Mango latest-call test found no calls with recordings in any account"
                    )
                    return
                earliest = datetime.min.replace(tzinfo=timezone.utc)
                source, latest = max(
                    candidates,
                    key=lambda item: item[1].finished_at
                    or item[1].started_at
                    or earliest,
                )
                logger.info(
                    "Mango latest-call test selected: account=%s call_id=%s "
                    "started_at=%s finished_at=%s",
                    source.name,
                    latest.id,
                    latest.started_at,
                    latest.finished_at,
                )
                await process(source, latest)
                await flush_outbox()
                logger.info(
                    "Mango latest-call test completed: account=%s call_id=%s",
                    source.name,
                    latest.id,
                )
                return

            while True:
                await flush_outbox()
                now = datetime.now(timezone.utc)
                statuses = await asyncio.gather(
                    *(poll_source(source, now) for source in sources)
                )
                await flush_outbox()

                if any(status is None for status in statuses):
                    await asyncio.sleep(settings.mango_poll_interval_seconds)
                elif all(statuses):
                    await asyncio.sleep(settings.mango_poll_interval_seconds)
                else:
                    await asyncio.sleep(settings.mango_catchup_interval_seconds)
        finally:
            await asyncio.gather(
                *(source.client.aclose() for source in sources),
                return_exceptions=True,
            )


if __name__ == "__main__":
    asyncio.run(run())
