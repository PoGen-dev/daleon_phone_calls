from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import re
import mimetypes
from pathlib import PurePath
from typing import Any

from app.clients.minio import MinioStorage
from app.clients.telegram import TelegramClient
from app.common.config import Settings, get_settings
from app.common.db import postgres_pool
from app.common.formatting import (
    call_service_group,
    format_analysis_message,
    format_dead_letter_message,
    format_transcript_caption,
)
from app.common.kafka import commit_after, kafka_consumer, kafka_producer
from app.common.logging import configure_logging
from app.common.models import QualityResult
from app.common.repository import Repository
from app.common.retry import retry_or_dead_letter

logger = logging.getLogger(__name__)

TELEGRAM_DOCUMENT_CAPTION_LIMIT = 1024
TRANSCRIPT_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"


@dataclass(frozen=True)
class TelegramDestination:
    name: str
    chat_id: str
    message_thread_id: int | None = None

    @property
    def notification_key(self) -> str:
        thread = (
            self.message_thread_id if self.message_thread_id is not None else "root"
        )
        return f"{self.chat_id}:{thread}:{self.name}"


def _thread_id(value: str | int | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid Telegram message_thread_id value: {value!r}"
        ) from exc


def _destination(
    name: str, chat_id: str, thread_id: str | int | None
) -> TelegramDestination | None:
    if not str(chat_id or "").strip():
        return None
    return TelegramDestination(
        name=name, chat_id=str(chat_id).strip(), message_thread_id=_thread_id(thread_id)
    )


def _is_closed_deal(quality: QualityResult) -> bool:
    next_step = quality.next_step
    return bool(next_step and next_step.status == "agreed")


def _report_topic(call: dict[str, Any], quality: QualityResult) -> str:
    call_type = str(call.get("call_type") or "").strip()
    if call_type == "completed_deal":
        return "closed_deals"
    if not call_type and _is_closed_deal(quality):
        return "closed_deals"
    # Бизнес-критичные звонки отправляются обычным ботом в общий поток.
    # Отдельный error-канал ниже используется только для технических DLQ-событий.
    return "other"


def _thread_for_report_topic(settings: Settings, prefix: str, topic: str) -> str:
    if topic == "critical":
        return str(getattr(settings, f"telegram_{prefix}_critical_thread_id"))
    if topic == "closed_deals":
        return str(getattr(settings, f"telegram_{prefix}_closed_deals_thread_id"))
    return str(getattr(settings, f"telegram_{prefix}_other_thread_id"))


def _report_destinations(
    settings: Settings,
    telegram: TelegramClient,
    call: dict[str, Any],
    quality: QualityResult,
) -> list[TelegramDestination]:
    topic = _report_topic(call, quality)
    destinations: list[TelegramDestination] = []

    admin = _destination(
        f"admin:{topic}",
        settings.telegram_admin_chat_id,
        _thread_for_report_topic(settings, "admin", topic),
    )
    if admin:
        destinations.append(admin)

    service_group = call_service_group(call)
    if service_group == "toyota_nissan":
        team = _destination(
            f"toyota_nissan:{topic}",
            settings.telegram_toyota_nissan_chat_id,
            _thread_for_report_topic(settings, "toyota_nissan", topic),
        )
        if team:
            destinations.append(team)
    elif service_group == "volvo_vag":
        team = _destination(
            f"volvo_vag:{topic}",
            settings.telegram_volvo_vag_chat_id,
            _thread_for_report_topic(settings, "volvo_vag", topic),
        )
        if team:
            destinations.append(team)

    if destinations:
        return destinations

    return [
        TelegramDestination(name=f"legacy:{index}", chat_id=chat_id)
        for index, chat_id in enumerate(telegram.main_chat_ids, start=1)
    ]


def _admin_transcript_destination(settings: Settings) -> TelegramDestination | None:
    return _destination(
        "admin:transcripts",
        settings.telegram_admin_chat_id,
        settings.telegram_admin_transcripts_thread_id,
    )


def _document_caption(text: str) -> str:
    if len(text) <= TELEGRAM_DOCUMENT_CAPTION_LIMIT:
        return text
    suffix = "\n\n… Отчёт сокращён из-за лимита подписи Telegram."
    return f"{text[: TELEGRAM_DOCUMENT_CAPTION_LIMIT - len(suffix)].rstrip()}{suffix}"


def _transcript_notification_key(destination: TelegramDestination, asset: str) -> str:
    return f"{destination.notification_key}:{asset}"


def _safe_filename_stem(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    return cleaned[:80] or "call"


def _transcript_filename(call: dict[str, Any], audio_filename: str | None) -> str:
    source = (
        PurePath(audio_filename).stem
        if audio_filename
        else str(call.get("id") or "call")
    )
    return f"{_safe_filename_stem(source)}_transcript.txt"


def _transcript_document(call: dict[str, Any]) -> bytes:
    transcript = str(call.get("transcript") or "").strip()
    if not transcript:
        raise ValueError(f"No transcript text for call_id={call.get('id')}")
    return f"{transcript}\n".encode("utf-8")


async def process_notification(
    payload: dict[str, Any],
    *,
    repo: Repository,
    telegram: TelegramClient,
    settings: Settings,
    storage: MinioStorage | None = None,
) -> None:
    event_id = payload.get("event_id")
    call_id = payload.get("call_id")
    if not event_id or not call_id:
        raise ValueError("Notification payload has no event_id/call_id")
    call = await repo.get_call_with_results(call_id)
    if not call or call.get("score") is None:
        raise ValueError(f"No analysis for call_id={call_id}")
    quality = QualityResult.model_validate(call)
    report_destinations = _report_destinations(settings, telegram, call, quality)
    transcript_destination = _admin_transcript_destination(settings)
    if not report_destinations and transcript_destination is None:
        raise RuntimeError(
            "Telegram report destinations are empty. Configure TELEGRAM_ADMIN_CHAT_ID / "
            "team chat ids or TELEGRAM_CHAT_IDS."
        )

    pending_report_destinations = []
    for destination in report_destinations:
        if not await repo.notification_exists(
            event_id, destination.notification_key, call_id, "main"
        ):
            pending_report_destinations.append(destination)

    pending_transcript_audio = False
    pending_transcript_text = False
    transcript_audio_key = None
    transcript_text_key = None
    if transcript_destination is not None:
        transcript_audio_key = _transcript_notification_key(
            transcript_destination, "audio"
        )
        transcript_text_key = _transcript_notification_key(
            transcript_destination, "text"
        )
        pending_transcript_audio = not await repo.notification_exists(
            event_id, transcript_audio_key, call_id, "main"
        )
        pending_transcript_text = not await repo.notification_exists(
            event_id, transcript_text_key, call_id, "main"
        )

    audio = None
    audio_filename = None
    audio_content_type = "application/octet-stream"
    audio_object_name = call.get("audio_object_name")
    needs_audio = bool(pending_report_destinations or pending_transcript_audio)
    if storage and audio_object_name and needs_audio:
        audio = await storage.download(str(audio_object_name))
        audio_filename = str(
            call.get("audio_filename")
            or PurePath(str(audio_object_name)).name
            or "recording.mp3"
        )
        audio_content_type = (
            mimetypes.guess_type(audio_filename)[0] or "application/octet-stream"
        )
    if pending_transcript_audio and (audio is None or not audio_filename):
        raise RuntimeError(
            f"Transcript topic requires audio file for call_id={call_id}; "
            "storage or audio_object_name is unavailable"
        )
    message = format_analysis_message(
        call,
        quality,
        timezone_name=settings.mango_default_timezone,
    )
    for destination in pending_report_destinations:
        if audio is not None and audio_filename:
            await telegram.send_audio_file(
                audio,
                filename=audio_filename,
                chat_id=destination.chat_id,
                caption=_document_caption(message),
                message_thread_id=destination.message_thread_id,
                content_type=audio_content_type,
            )
        else:
            await telegram.send(
                message,
                chat_id=destination.chat_id,
                message_thread_id=destination.message_thread_id,
            )
        await repo.save_notification(
            event_id, destination.notification_key, call_id, "main"
        )
    if transcript_destination is not None:
        transcript_caption = format_transcript_caption(
            call, timezone_name=settings.mango_default_timezone
        )
        if pending_transcript_audio:
            await telegram.send_audio_file(
                audio,
                filename=audio_filename,
                chat_id=transcript_destination.chat_id,
                caption=_document_caption(transcript_caption),
                message_thread_id=transcript_destination.message_thread_id,
                content_type=audio_content_type,
            )
            await repo.save_notification(
                event_id, transcript_audio_key, call_id, "main"
            )
        if pending_transcript_text:
            await telegram.send_document(
                _transcript_document(call),
                filename=_transcript_filename(call, audio_filename),
                chat_id=transcript_destination.chat_id,
                caption="📄 Транскрипция по ролям",
                message_thread_id=transcript_destination.message_thread_id,
                content_type=TRANSCRIPT_TEXT_CONTENT_TYPE,
            )
            await repo.save_notification(event_id, transcript_text_key, call_id, "main")
    await repo.mark_call_status(call_id, "notified")
    logger.info(
        "Telegram notification sent",
        extra={
            "call_id": call_id,
            "report_destinations": [
                destination.name for destination in pending_report_destinations
            ],
            "transcript_destination": (
                transcript_destination.name if transcript_destination else None
            ),
            "transcript_audio_sent": pending_transcript_audio,
            "transcript_text_sent": pending_transcript_text,
        },
    )


async def process_dead_letter(
    payload: dict[str, Any], *, repo: Repository, telegram: TelegramClient
) -> None:
    event_id = payload.get("event_id")
    if not event_id:
        raise ValueError("Dead-letter payload has no event_id")
    if not telegram.error_chat_ids:
        raise RuntimeError("TELEGRAM_ERROR_CHAT_IDS is empty")
    call_id = (payload.get("payload") or {}).get("call_id")
    message = format_dead_letter_message(payload)
    for chat_id in telegram.error_chat_ids:
        if await repo.notification_exists(event_id, chat_id):
            continue
        await telegram.send(message, chat_id=chat_id, error_channel=True)
        await repo.save_notification(event_id, chat_id, call_id, "error")
    logger.info("Dead-letter notification sent", extra={"event_id": event_id})


async def process_dead_letter_with_retries(
    payload: dict[str, Any],
    *,
    repo: Repository,
    telegram: TelegramClient,
    settings: Settings,
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
            settings,
            topic=(settings.topic_to_notify, settings.topic_dead_letter),
            group_suffix="telegram",
        ) as consumer,
    ):
        repo = Repository(pg)
        telegram = TelegramClient(settings)
        storage = MinioStorage(settings)
        try:
            async for record in consumer:
                payload = (
                    record.value
                    if isinstance(record.value, dict)
                    else {"invalid_payload": record.value}
                )
                try:
                    if record.topic == settings.topic_dead_letter:
                        await process_dead_letter_with_retries(
                            payload, repo=repo, telegram=telegram, settings=settings
                        )
                    else:
                        await process_notification(
                            payload,
                            repo=repo,
                            telegram=telegram,
                            settings=settings,
                            storage=storage,
                        )
                except Exception as exc:
                    logger.exception(
                        "Telegram task failed", extra={"topic": record.topic}
                    )
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
