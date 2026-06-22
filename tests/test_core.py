from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from app.common.config import Settings, get_settings
from app.common.formatting import format_analysis_message, format_dead_letter_message
from app.common.models import CallRecord, QualityCriteria, QualityResult, TranscriptionRequestedEvent
from app.common.serialization import json_dumps_bytes, json_loads_bytes
from app.prompts.quality import build_quality_user_prompt


def quality(**overrides) -> QualityResult:
    payload = {
        "score": 60,
        "risk_level": "critical",
        "risk_reason": "Клиент ушел подумать без следующего шага.",
        "summary": "Обсудили обслуживание автомобиля.",
        "errors": ["Не отработано возражение", "Не назначен следующий шаг"],
        "recommendation": "Перезвонить через два дня.",
        "criteria": {
            "greeting": 75,
            "needs_discovery": 60,
            "urgency": 20,
            "target_action": 80,
            "objection_handling": 40,
            "closing": 40,
        },
    }
    payload.update(overrides)
    return QualityResult.model_validate(payload)


def test_settings_parse_fields_and_cache() -> None:
    value = Settings(_env_file=None, mango_stats_fields=" one, two ,,three ")
    assert value.mango_fields_list == ["one", "two", "three"]
    get_settings.cache_clear()
    assert get_settings() is get_settings()
    get_settings.cache_clear()


def test_models_normalize_naive_datetime_and_validate_attempt() -> None:
    call = CallRecord(id="1", started_at=datetime(2026, 5, 7, 18))
    assert call.started_at and call.started_at.utcoffset().total_seconds() == 0
    event = TranscriptionRequestedEvent(call_id="1", object_name="1/a.mp3", filename="a.mp3")
    assert event.attempt == 1 and event.event_type == "transcription.requested"
    with pytest.raises(ValidationError):
        TranscriptionRequestedEvent(call_id="1", object_name="a", filename="a", attempt=0)


def test_quality_schema_rejects_invalid_scores() -> None:
    assert quality().criteria.greeting == 75
    with pytest.raises(ValidationError):
        QualityCriteria(
            greeting=101,
            needs_discovery=1,
            urgency=1,
            target_action=1,
            objection_handling=1,
            closing=1,
        )


def test_serialization_handles_datetime_and_rejects_unknown() -> None:
    encoded = json_dumps_bytes({"at": datetime(2026, 1, 2, 3, 4)})
    assert json_loads_bytes(encoded)["at"].startswith("2026-01-02T03:04")
    with pytest.raises(TypeError):
        json_dumps_bytes({"bad": object()})


def test_analysis_message_matches_business_template() -> None:
    call = {
        "id": "137630",
        "started_at": "2026-05-07T18:00:00+00:00",
        "finished_at": "2026-05-07T18:03:44+00:00",
        "direction": "incoming",
        "from_number": "79990000000",
        "recording_url": "https://example.test/play/1",
        "raw": {"user_name": "user2", "deal_id": "42"},
    }
    message = format_analysis_message(call, quality(), "https://ainakontrole.ru/app/dashboard")
    assert "🚨 РИСК СРЫВА СДЕЛКИ" in message
    assert "👤 user2 · incoming · 79990000000" in message
    assert "07.05.2026 18:00 · 3:44" in message
    assert "👋75 · 🔍60 · 🔥20 · 🎯80 · 🛡40 · 🏁40" in message
    assert "callId=137630" in message and "Сделка: 42" in message


def test_noncritical_message_and_missing_call_fields() -> None:
    message = format_analysis_message({"id": "x", "raw": {}}, quality(risk_level="normal", errors=[]), "https://x")
    assert message.startswith("📞 АНАЛИЗ ЗВОНКА")
    assert "Критичных ошибок не выявлено" in message
    assert "📅 - · -" in message


def test_dead_letter_format_and_prompt() -> None:
    message = format_dead_letter_message(
        {
            "service": "quality-worker",
            "source_topic": "calls.to_analyze",
            "payload": {"call_id": "c1"},
            "attempts": 3,
            "error": "timeout",
        }
    )
    assert "❗ ОШИБКА" in message and "Звонок: c1" in message and "Попыток: 3" in message
    assert "текст звонка" in build_quality_user_prompt("текст звонка")
