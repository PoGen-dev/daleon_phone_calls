from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.common.models import CallRecord, QualityResult
from app.services import mango_worker, quality_worker, telegram_worker, transcriber_worker


def call(**overrides) -> CallRecord:
    payload = {
        "id": "c1",
        "recording_id": "r1",
        "recording_url": "https://example.test/recording",
        "started_at": datetime(2026, 5, 7, 18, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 5, 7, 18, 3, 44, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return CallRecord(**payload)


def quality() -> QualityResult:
    return QualityResult.model_validate(
        {
            "score": 60,
            "risk_level": "critical",
            "risk_reason": "Нет следующего шага",
            "summary": "Обсудили услугу",
            "errors": ["Не отработано возражение"],
            "recommendation": "Перезвонить",
            "criteria": {
                "greeting": 75,
                "needs_discovery": 60,
                "urgency": 20,
                "target_action": 80,
                "objection_handling": 40,
                "closing": 40,
            },
        }
    )


def test_cursor_and_poll_windows(settings) -> None:
    assert mango_worker._parse_cursor(None) is None
    assert mango_worker._parse_cursor("2026-01-01T00:00:00Z").tzinfo is not None
    assert mango_worker._safe_component("../bad/id", "fallback") == "_bad_id"
    assert mango_worker._safe_component("...", "fallback") == "fallback"
    now = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    assert mango_worker._poll_window(None, now, settings) == (
        datetime(2026, 1, 1, 11, 55, tzinfo=timezone.utc),
        now,
    )
    assert mango_worker._poll_window(datetime(2026, 1, 1, 11, tzinfo=timezone.utc), now, settings) == (
        datetime(2026, 1, 1, 10, 55, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 11, 5, tzinfo=timezone.utc),
    )


def test_latest_recorded_call_selects_newest_eligible_call() -> None:
    older = call(id="older", recording_id="r1", finished_at=datetime(2026, 5, 7, 18, tzinfo=timezone.utc))
    newer = call(id="newer", recording_id="r2", finished_at=datetime(2026, 5, 8, 18, tzinfo=timezone.utc))
    no_recording = call(
        id="ignored",
        recording_id=None,
        recording_url=None,
        finished_at=datetime(2026, 5, 9, 18, tzinfo=timezone.utc),
    )

    assert mango_worker._latest_recorded_call([newer, no_recording, older]) == newer
    assert mango_worker._latest_recorded_call([no_recording]) is None


@pytest.mark.asyncio
async def test_mango_store_downloads_uploads_and_enqueues(settings) -> None:
    repo = SimpleNamespace(
        save_call=AsyncMock(),
        get_call=AsyncMock(return_value=None),
        save_call_and_enqueue=AsyncMock(),
    )
    mango = SimpleNamespace(download_recording=AsyncMock(return_value=(b"audio", "../call.mp3")))
    storage = SimpleNamespace(bucket="bucket", upload=AsyncMock(), remove=AsyncMock())
    await mango_worker._store_call(call(), repo=repo, mango=mango, storage=storage, settings=settings)
    storage.upload.assert_awaited_once_with("c1/call.mp3", b"audio", content_type="audio/mpeg")
    assert repo.save_call.await_count == 1
    kwargs = repo.save_call_and_enqueue.await_args.kwargs
    assert kwargs["audio_object_name"] == "c1/call.mp3"
    assert [message.topic for message in kwargs["messages"]] == [
        settings.topic_mango_raw,
        settings.topic_to_transcribe,
    ]
    assert kwargs["messages"][1].payload["object_name"] == "c1/call.mp3"


@pytest.mark.asyncio
async def test_mango_store_is_idempotent_and_handles_no_recording(settings) -> None:
    repo = SimpleNamespace(
        save_call=AsyncMock(),
        get_call=AsyncMock(return_value={"audio_object_name": "c1/old.wav", "audio_filename": "old.wav"}),
        mark_call_status=AsyncMock(),
        save_call_and_enqueue=AsyncMock(),
    )
    mango = SimpleNamespace(download_recording=AsyncMock())
    storage = SimpleNamespace(bucket="bucket", upload=AsyncMock(), remove=AsyncMock())
    await mango_worker._store_call(call(), repo=repo, mango=mango, storage=storage, settings=settings)
    mango.download_recording.assert_not_awaited()
    messages = repo.save_call_and_enqueue.await_args.kwargs["messages"]
    assert messages[1].payload["filename"] == "old.wav"

    await mango_worker._store_call(
        call(recording_id=None, recording_url=None),
        repo=repo,
        mango=mango,
        storage=storage,
        settings=settings,
    )
    repo.mark_call_status.assert_awaited_with(
        "c1", "no_recording", "Mango payload contains no recording reference"
    )
    assert repo.save_call_and_enqueue.await_count == 1


@pytest.mark.asyncio
async def test_mango_ingestion_retries_three_times_then_enqueues_dlq(settings) -> None:
    repo = SimpleNamespace(
        save_call=AsyncMock(),
        get_call=AsyncMock(return_value=None),
        mark_call_failed_and_enqueue=AsyncMock(),
    )
    mango = SimpleNamespace(download_recording=AsyncMock(side_effect=RuntimeError("download failed")))
    storage = SimpleNamespace(bucket="bucket", upload=AsyncMock(), remove=AsyncMock())
    await mango_worker._store_call_with_retries(
        call(), repo=repo, mango=mango, storage=storage, settings=settings
    )
    assert mango.download_recording.await_count == 3
    args = repo.mark_call_failed_and_enqueue.await_args.args
    assert args[:2] == ("c1", "download failed")
    assert args[2].topic == settings.topic_dead_letter
    assert args[2].payload["attempts"] == 3


@pytest.mark.asyncio
async def test_mango_removes_new_object_when_database_transaction_fails(settings) -> None:
    repo = SimpleNamespace(
        save_call=AsyncMock(),
        get_call=AsyncMock(return_value=None),
        save_call_and_enqueue=AsyncMock(side_effect=RuntimeError("database down")),
    )
    mango = SimpleNamespace(download_recording=AsyncMock(return_value=(b"audio", "call.mp3")))
    storage = SimpleNamespace(bucket="bucket", upload=AsyncMock(), remove=AsyncMock())
    with pytest.raises(RuntimeError, match="database down"):
        await mango_worker._store_call(call(), repo=repo, mango=mango, storage=storage, settings=settings)
    storage.remove.assert_awaited_once_with("c1/call.mp3")


@pytest.mark.asyncio
async def test_transcriber_processes_audio_and_continues_idempotently(settings, monkeypatch) -> None:
    repo = SimpleNamespace(
        transcription_exists=AsyncMock(side_effect=[False, True]),
        save_transcription=AsyncMock(),
    )
    storage = SimpleNamespace(download=AsyncMock(return_value=b"audio"))
    ai = SimpleNamespace(transcribe=AsyncMock(return_value=("готовый текст", {"language": "ru"})))
    publish = AsyncMock()
    monkeypatch.setattr(transcriber_worker, "publish_json", publish)
    payload = {"call_id": "c1", "object_name": "c1/a.mp3", "filename": "a.mp3"}
    await transcriber_worker.process_task(
        payload, repo=repo, storage=storage, ai=ai, producer=object(), settings=settings
    )
    repo.save_transcription.assert_awaited_once()
    assert publish.await_args.args[1] == settings.topic_to_analyze
    await transcriber_worker.process_task(
        payload, repo=repo, storage=storage, ai=ai, producer=object(), settings=settings
    )
    assert storage.download.await_count == 1 and publish.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "transcript", "message"),
    [
        ({}, "text", "call_id"),
        ({"call_id": "c1"}, "text", "object_name"),
        ({"call_id": "c1", "object_name": "x"}, " ", "empty"),
    ],
)
async def test_transcriber_validates_payload(settings, monkeypatch, payload, transcript, message) -> None:
    repo = SimpleNamespace(transcription_exists=AsyncMock(return_value=False), save_transcription=AsyncMock())
    storage = SimpleNamespace(download=AsyncMock(return_value=b"audio"))
    ai = SimpleNamespace(transcribe=AsyncMock(return_value=(transcript, {})))
    monkeypatch.setattr(transcriber_worker, "publish_json", AsyncMock())
    with pytest.raises(ValueError, match=message):
        await transcriber_worker.process_task(
            payload, repo=repo, storage=storage, ai=ai, producer=object(), settings=settings
        )


@pytest.mark.asyncio
async def test_quality_worker_analyzes_and_continues_idempotently(settings, monkeypatch) -> None:
    repo = SimpleNamespace(
        quality_exists=AsyncMock(side_effect=[False, True]),
        get_transcription=AsyncMock(return_value="transcript"),
        save_quality=AsyncMock(),
    )
    ai = SimpleNamespace(score_quality=AsyncMock(return_value=(quality(), {"raw": True})))
    publish = AsyncMock()
    monkeypatch.setattr(quality_worker, "publish_json", publish)
    await quality_worker.process_task({"call_id": "c1"}, repo=repo, ai=ai, producer=object(), settings=settings)
    repo.save_quality.assert_awaited_once()
    assert publish.await_args.args[1] == settings.topic_to_notify
    await quality_worker.process_task({"call_id": "c1"}, repo=repo, ai=ai, producer=object(), settings=settings)
    assert ai.score_quality.await_count == 1 and publish.await_count == 2


@pytest.mark.asyncio
async def test_quality_worker_validates_input(settings, monkeypatch) -> None:
    repo = SimpleNamespace(
        quality_exists=AsyncMock(return_value=False), get_transcription=AsyncMock(return_value=None)
    )
    monkeypatch.setattr(quality_worker, "publish_json", AsyncMock())
    with pytest.raises(ValueError, match="call_id"):
        await quality_worker.process_task({}, repo=repo, ai=object(), producer=object(), settings=settings)
    with pytest.raises(ValueError, match="No transcription"):
        await quality_worker.process_task(
            {"call_id": "c1"}, repo=repo, ai=object(), producer=object(), settings=settings
        )


@pytest.mark.asyncio
async def test_telegram_worker_sends_normal_and_error_messages(settings) -> None:
    result = quality().model_dump(mode="json")
    call_data = {
        "id": "c1",
        "started_at": "2026-05-07T18:00:00+00:00",
        "finished_at": "2026-05-07T18:03:44+00:00",
        "recording_url": "https://example.test",
        "raw": {},
        **result,
    }
    repo = SimpleNamespace(
        notification_exists=AsyncMock(side_effect=[False, False, True, True, False, False]),
        get_call_with_results=AsyncMock(return_value=call_data),
        save_notification=AsyncMock(),
        mark_call_status=AsyncMock(),
    )
    telegram = SimpleNamespace(
        send=AsyncMock(),
        main_chat_ids=["main-1", "main-2"],
        error_chat_ids=["error-1", "error-2"],
    )
    payload = {"event_id": "e1", "call_id": "c1"}
    await telegram_worker.process_notification(payload, repo=repo, telegram=telegram, settings=settings)
    assert "РИСК СРЫВА" in telegram.send.await_args.args[0]
    assert telegram.send.await_count == 2
    repo.save_notification.assert_awaited_with("e1", "main-2", "c1", "main")
    repo.mark_call_status.assert_awaited_with("c1", "notified")
    await telegram_worker.process_notification(payload, repo=repo, telegram=telegram, settings=settings)
    assert telegram.send.await_count == 2

    dlq = {"event_id": "e2", "payload": {"call_id": "c1"}, "attempts": 3, "error": "bad"}
    await telegram_worker.process_dead_letter(dlq, repo=repo, telegram=telegram)
    assert telegram.send.await_args.kwargs["error_channel"] is True
    assert telegram.send.await_count == 4
    repo.save_notification.assert_awaited_with("e2", "error-2", "c1", "error")


@pytest.mark.asyncio
async def test_telegram_worker_validates_payload_and_analysis(settings) -> None:
    repo = SimpleNamespace(
        notification_exists=AsyncMock(return_value=False),
        get_call_with_results=AsyncMock(return_value=None),
    )
    telegram = SimpleNamespace(send=AsyncMock(), main_chat_ids=["main"], error_chat_ids=["error"])
    with pytest.raises(ValueError, match="event_id/call_id"):
        await telegram_worker.process_notification({}, repo=repo, telegram=telegram, settings=settings)
    with pytest.raises(ValueError, match="No analysis"):
        await telegram_worker.process_notification(
            {"event_id": "e", "call_id": "c"}, repo=repo, telegram=telegram, settings=settings
        )
    with pytest.raises(ValueError, match="event_id"):
        await telegram_worker.process_dead_letter({}, repo=repo, telegram=telegram)


@pytest.mark.asyncio
async def test_telegram_worker_retries_only_missing_recipients(settings) -> None:
    call_data = {
        "id": "c1",
        "score": 80,
        "risk_level": "normal",
        "risk_reason": "Риска нет",
        "summary": "Успешный звонок",
        "errors": [],
        "recommendation": "Продолжить работу",
        "criteria": {
            "greeting": 80,
            "needs_discovery": 80,
            "urgency": 80,
            "target_action": 80,
            "objection_handling": 80,
            "closing": 80,
        },
        "raw": {},
    }
    repo = SimpleNamespace(
        get_call_with_results=AsyncMock(return_value=call_data),
        notification_exists=AsyncMock(side_effect=[False, False, True, False]),
        save_notification=AsyncMock(),
        mark_call_status=AsyncMock(),
    )
    telegram = SimpleNamespace(
        main_chat_ids=["first", "second"],
        send=AsyncMock(side_effect=[None, RuntimeError("temporary"), None]),
    )
    payload = {"event_id": "e1", "call_id": "c1"}
    with pytest.raises(RuntimeError, match="temporary"):
        await telegram_worker.process_notification(payload, repo=repo, telegram=telegram, settings=settings)
    await telegram_worker.process_notification(payload, repo=repo, telegram=telegram, settings=settings)
    assert telegram.send.await_count == 3
    assert telegram.send.await_args.kwargs["chat_id"] == "second"
    repo.mark_call_status.assert_awaited_once_with("c1", "notified")


@pytest.mark.asyncio
async def test_telegram_worker_rejects_empty_recipient_lists(settings) -> None:
    call_data = {"id": "c1", "score": 1, **quality().model_dump(mode="json")}
    repo = SimpleNamespace(get_call_with_results=AsyncMock(return_value=call_data))
    telegram = SimpleNamespace(main_chat_ids=[], error_chat_ids=[], send=AsyncMock())
    with pytest.raises(RuntimeError, match="TELEGRAM_CHAT_IDS"):
        await telegram_worker.process_notification(
            {"event_id": "e", "call_id": "c1"}, repo=repo, telegram=telegram, settings=settings
        )
    with pytest.raises(RuntimeError, match="TELEGRAM_ERROR_CHAT_IDS"):
        await telegram_worker.process_dead_letter({"event_id": "e", "payload": {}}, repo=repo, telegram=telegram)


@pytest.mark.asyncio
async def test_error_bot_delivery_is_retried_three_times(settings) -> None:
    repo = SimpleNamespace(notification_exists=AsyncMock(return_value=False))
    telegram = SimpleNamespace(
        send=AsyncMock(side_effect=RuntimeError("telegram down")), error_chat_ids=["error"]
    )
    payload = {"event_id": "e", "payload": {"call_id": "c"}}
    with pytest.raises(RuntimeError, match="telegram down"):
        await telegram_worker.process_dead_letter_with_retries(
            payload, repo=repo, telegram=telegram, settings=settings
        )
    assert telegram.send.await_count == 3
