from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.common.models import QualityCriteria, QualityResult

SEPARATOR = "━━━━━━━━━━━━"


def _display_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_datetime(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _duration(call: dict[str, Any]) -> str:
    try:
        start = _parse_datetime(call["started_at"])
        finish = _parse_datetime(call["finished_at"])
        seconds = max(0, int((finish - start).total_seconds()))
        return f"{seconds // 60}:{seconds % 60:02d}"
    except (KeyError, TypeError, ValueError):
        return "-"


def _date(call: dict[str, Any], *, timezone_name: str) -> str:
    try:
        value = _parse_datetime(call["started_at"]).astimezone(_display_timezone(timezone_name))
        return value.strftime("%d.%m.%Y %H:%M")
    except (KeyError, TypeError, ValueError):
        return "-"


def _manager(call: dict[str, Any]) -> str:
    raw = call.get("raw") or {}
    return str(raw.get("manager_name") or raw.get("user_name") or raw.get("from_extension") or "-")


def _deal(call: dict[str, Any]) -> str:
    raw = call.get("raw") or {}
    return str(
        raw.get("deal_id")
        or raw.get("crm_deal_id")
        or raw.get("amo_deal_id")
        or raw.get("bitrix_deal_id")
        or "не привязана"
    )


def _recording(call: dict[str, Any]) -> str:
    if call.get("recording_url"):
        return str(call["recording_url"])
    if call.get("audio_object_name"):
        bucket = call.get("audio_bucket") or "MinIO"
        return f"{bucket}/{call['audio_object_name']}"
    if call.get("recording_id"):
        return f"ID записи Mango: {call['recording_id']}"
    return "не найдена"


def _risk_reason(quality: QualityResult) -> str:
    if quality.risk_reason:
        return quality.risk_reason
    if quality.risk_level == "normal":
        return "Риск не выявлен"
    return "Причина риска не указана моделью"


def _criteria_line(criteria: QualityCriteria) -> str:
    return (
        f"👋{criteria.greeting} · 🔍{criteria.needs_discovery} · 🔥{criteria.urgency} · "
        f"🎯{criteria.target_action} · 🛡{criteria.objection_handling} · 🏁{criteria.closing}"
    )


def _is_no_risk_reason(value: str) -> bool:
    normalized = " ".join(value.strip().lower().replace(".", "").split())
    return normalized in {"риск не выявлен", "риска нет", "риск не обнаружен", "не выявлен"}


def _visible_errors(errors: list[str]) -> list[str]:
    hidden = {
        "критичных ошибок не выявлено",
        "критические ошибки не выявлены",
        "ошибок не выявлено",
    }
    result = []
    for error in errors:
        normalized = " ".join(error.strip().lower().replace(".", "").split())
        if normalized and normalized not in hidden:
            result.append(error)
    return result


def format_analysis_message(
    call: dict[str, Any],
    quality: QualityResult,
    *,
    timezone_name: str = "Europe/Moscow",
    recording_download_url: str | None = None,
) -> str:
    title = (
        "🚨 РИСК СРЫВА СДЕЛКИ"
        if quality.risk_level == "critical"
        else "📞 АНАЛИЗ ЗВОНКА"
    )
    started = _date(call, timezone_name=timezone_name)
    lines = [
        title,
        "",
        f"👤 {_manager(call)} · {call.get('direction') or '-'} · {call.get('from_number') or '-'}",
        f"📅 {started} · {_duration(call)}",
        f"🔗 Сделка: {_deal(call)}",
        f"🎧 Звонок: {_recording(call)}",
    ]
    if recording_download_url:
        lines.append(f"💾 Запись MinIO: {recording_download_url}")

    lines.extend(["", SEPARATOR, ""])
    risk_reason = _risk_reason(quality)
    if risk_reason and not _is_no_risk_reason(risk_reason):
        lines.extend([f"⚠️ Почему риск: {risk_reason}", ""])

    lines.extend([f"💬 Итог: {quality.summary}", ""])
    errors = _visible_errors(quality.errors)
    if errors:
        lines.extend([f"❌ Ошибки: {'; '.join(errors)}", ""])

    lines.extend(
        [
            SEPARATOR,
            "",
            f"📊 Оценка: {quality.score}, взвешенная",
            "",
            _criteria_line(quality.criteria),
            "",
            SEPARATOR,
            "",
            "✅ Что делать:",
            quality.recommendation,
        ]
    )
    return "\n".join(lines)


def format_dead_letter_message(payload: dict[str, Any]) -> str:
    task = payload.get("payload") or {}
    return "\n".join(
        [
            "❗ ОШИБКА ОБРАБОТКИ ЗВОНКА",
            "",
            f"Сервис: {payload.get('service', '-')}",
            f"Очередь: {payload.get('source_topic', '-')}",
            f"Звонок: {task.get('call_id', '-')}",
            f"Попыток: {payload.get('attempts', '-')}",
            f"Ошибка: {payload.get('error', '-')}",
        ]
    )
