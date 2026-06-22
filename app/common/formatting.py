from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from app.common.models import QualityCriteria, QualityResult

SEPARATOR = "━━━━━━━━━━━━"


def _duration(call: dict[str, Any]) -> str:
    try:
        start = datetime.fromisoformat(str(call["started_at"]).replace("Z", "+00:00"))
        finish = datetime.fromisoformat(str(call["finished_at"]).replace("Z", "+00:00"))
        seconds = max(0, int((finish - start).total_seconds()))
        return f"{seconds // 60}:{seconds % 60:02d}"
    except (KeyError, TypeError, ValueError):
        return "-"


def _date(call: dict[str, Any]) -> tuple[str, str]:
    try:
        value = datetime.fromisoformat(str(call["started_at"]).replace("Z", "+00:00"))
        return value.strftime("%d.%m.%Y %H:%M"), value.strftime("%Y-%m-%d")
    except (KeyError, TypeError, ValueError):
        return "-", datetime.now().strftime("%Y-%m-%d")


def _manager(call: dict[str, Any]) -> str:
    raw = call.get("raw") or {}
    return str(raw.get("manager_name") or raw.get("user_name") or raw.get("from_extension") or "-")


def _deal(call: dict[str, Any]) -> str:
    raw = call.get("raw") or {}
    return str(raw.get("deal_id") or raw.get("crm_deal_id") or "-")


def _criteria_line(criteria: QualityCriteria) -> str:
    return (
        f"👋{criteria.greeting} · 🔍{criteria.needs_discovery} · 🔥{criteria.urgency} · "
        f"🎯{criteria.target_action} · 🛡{criteria.objection_handling} · 🏁{criteria.closing}"
    )


def format_analysis_message(
    call: dict[str, Any], quality: QualityResult, dashboard_base_url: str
) -> str:
    title = (
        "🚨 РИСК СРЫВА СДЕЛКИ"
        if quality.risk_level == "critical"
        else "📞 АНАЛИЗ ЗВОНКА"
    )
    started, query_date = _date(call)
    query = urlencode({"start": query_date, "end": query_date, "funnel": "sales", "callId": call["id"]})
    dashboard_url = f"{dashboard_base_url}?{query}"
    errors = "; ".join(quality.errors) if quality.errors else "Критичных ошибок не выявлено"
    return "\n".join(
        [
            title,
            "",
            f"👤 {_manager(call)} · {call.get('direction') or '-'} · {call.get('from_number') or '-'}",
            f"📅 {started} · {_duration(call)}",
            f"🔗 Сделка: {_deal(call)}",
            f"🎧 Звонок: {call.get('recording_url') or '-'}",
            "",
            SEPARATOR,
            "",
            f"⚠️ Почему риск: {quality.risk_reason}",
            "",
            f"💬 Итог: {quality.summary}",
            "",
            f"❌ Ошибки: {errors}",
            "",
            SEPARATOR,
            "",
            f"📊 Оценка: {quality.score}",
            "",
            _criteria_line(quality.criteria),
            "",
            SEPARATOR,
            "",
            "✅ Что делать:",
            quality.recommendation,
            "",
            f"🌐 Подробнее: {dashboard_url}",
        ]
    )


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
