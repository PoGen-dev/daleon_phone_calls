from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import asyncpg

from app.common.serialization import json_loads_bytes

SortOrder = Literal["asc", "desc"]


@dataclass(slots=True)
class AnalyticsFilters:
    date_from: datetime | None = None
    date_to: datetime | None = None
    account: str | None = None
    manager: str | None = None
    direction: str | None = None
    status: str | None = None
    call_type: str | None = None
    risk_level: str | None = None
    score_min: int | None = None
    score_max: int | None = None
    duration_min: float | None = None
    duration_max: float | None = None
    has_transcription: bool | None = None
    has_quality: bool | None = None
    search: str | None = None


_SORT_COLUMNS: dict[str, str] = {
    "started_at": "e.started_at",
    "finished_at": "e.finished_at",
    "duration_seconds": "e.duration_seconds",
    "score": "e.score",
    "risk_level": "e.risk_level",
    "call_type": "e.call_type",
    "status": "e.status",
    "manager": "e.manager_name",
    "account": "e.mango_account",
    "created_at": "e.created_at",
}


_BASE_CTE = """
WITH enriched AS (
    SELECT
        c.id,
        c.entry_id,
        c.call_id,
        c.recording_id,
        c.direction,
        c.from_number,
        c.to_number,
        c.started_at,
        c.finished_at,
        c.disconnect_reason,
        c.status,
        c.error,
        c.raw,
        c.audio_object_name,
        c.audio_filename,
        c.created_at,
        c.updated_at,
        t.transcript,
        t.language AS transcription_language,
        t.model AS transcription_model,
        t.created_at AS transcription_created_at,
        q.score,
        q.risk_level,
        q.risk_reason,
        q.summary,
        q.errors,
        q.recommendation,
        q.criteria,
        q.raw AS quality_raw,
        q.model AS quality_model,
        q.created_at AS quality_created_at,
        cc.call_type,
        cc.reason AS classification_reason,
        cc.critical_errors,
        cc.confidence AS classification_confidence,
        cc.model AS classification_model,
        EXISTS (
            SELECT 1
            FROM notifications n
            WHERE n.call_id = c.id AND n.channel = 'main'
        ) AS has_notification,
        COALESCE(
            t.duration_seconds::double precision,
            EXTRACT(EPOCH FROM (c.finished_at - c.started_at))::double precision
        ) AS duration_seconds,
        COALESCE(NULLIF(c.raw->>'mango_account', ''), 'primary') AS mango_account,
        COALESCE(NULLIF(c.raw#>>'{mango_employee_summary,name}', ''), '—') AS manager_name,
        NULLIF(c.raw#>>'{mango_employee_summary,department}', '') AS manager_department,
        NULLIF(c.raw#>>'{mango_employee_summary,position}', '') AS manager_position,
        NULLIF(c.raw#>>'{mango_employee_summary,extension}', '') AS manager_extension
    FROM calls c
    LEFT JOIN transcriptions t ON t.call_id = c.id
    LEFT JOIN quality_scores q ON q.call_id = c.id
    LEFT JOIN call_classifications cc ON cc.call_id = c.id
)
"""

_SUMMARY_SELECT = """
SELECT
    COUNT(*)::int AS total_calls,
    COUNT(*) FILTER (WHERE direction = 'inbound')::int AS inbound_calls,
    COUNT(*) FILTER (WHERE direction = 'outbound')::int AS outbound_calls,
    COUNT(*) FILTER (WHERE transcript IS NOT NULL)::int AS transcribed_calls,
    COUNT(*) FILTER (WHERE score IS NOT NULL)::int AS analyzed_calls,
    COUNT(*) FILTER (WHERE call_type IS NOT NULL)::int AS classified_calls,
    COUNT(*) FILTER (WHERE has_notification)::int AS notified_calls,
    COUNT(*) FILTER (WHERE error IS NOT NULL OR status LIKE '%failed%')::int AS pipeline_error_calls,
    COUNT(*) FILTER (WHERE risk_level = 'critical')::int AS critical_calls,
    COUNT(*) FILTER (WHERE risk_level = 'warning')::int AS warning_calls,
    COUNT(*) FILTER (WHERE risk_level = 'normal')::int AS normal_calls,
    COUNT(*) FILTER (WHERE call_type = 'appointment')::int AS appointment_calls,
    COUNT(*) FILTER (WHERE call_type = 'completed_deal')::int AS completed_deal_calls,
    ROUND(AVG(score)::numeric, 1) AS avg_score,
    ROUND(AVG(duration_seconds)::numeric, 1) AS avg_duration_seconds,
    ROUND(SUM(duration_seconds)::numeric, 1) AS total_duration_seconds
FROM enriched e
{where}
"""


def _enrich_summary(summary: dict[str, Any]) -> dict[str, Any]:
    total = int(summary.get("total_calls") or 0)
    transcribed = int(summary.get("transcribed_calls") or 0)
    classified = int(summary.get("classified_calls") or 0)
    analyzed = int(summary.get("analyzed_calls") or 0)
    notified = int(summary.get("notified_calls") or 0)
    completed = int(summary.get("completed_deal_calls") or 0)
    appointments = int(summary.get("appointment_calls") or 0)

    def rate(value: int, denominator: int) -> float | None:
        return round(value * 100 / denominator, 1) if denominator else None

    summary["transcription_coverage"] = rate(transcribed, total)
    summary["classification_coverage"] = rate(classified, total)
    summary["analysis_coverage"] = rate(analyzed, total)
    summary["notification_coverage"] = rate(notified, total)
    summary["deal_rate"] = rate(completed, classified)
    summary["appointment_rate"] = rate(appointments, classified)
    summary["critical_rate"] = rate(int(summary.get("critical_calls") or 0), analyzed)
    return summary


def _pct_change(
    current: int | float | None, previous: int | float | None
) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return round((float(current) - float(previous)) * 100 / abs(float(previous)), 1)


def _difference(
    current: int | float | None, previous: int | float | None
) -> float | None:
    if current is None or previous is None:
        return None
    return round(float(current) - float(previous), 1)


def _comparison(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    return {
        "previous": previous,
        "delta": {
            "total_calls_pct": _pct_change(
                current.get("total_calls"), previous.get("total_calls")
            ),
            "avg_score": _difference(
                current.get("avg_score"), previous.get("avg_score")
            ),
            "critical_calls_pct": _pct_change(
                current.get("critical_calls"), previous.get("critical_calls")
            ),
            "completed_deal_calls_pct": _pct_change(
                current.get("completed_deal_calls"),
                previous.get("completed_deal_calls"),
            ),
            "deal_rate_pp": _difference(
                current.get("deal_rate"), previous.get("deal_rate")
            ),
            "analysis_coverage_pp": _difference(
                current.get("analysis_coverage"), previous.get("analysis_coverage")
            ),
        },
    }


def _to_primitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _to_primitive(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_primitive(item) for item in value]
    if isinstance(value, tuple):
        return [_to_primitive(item) for item in value]
    if isinstance(value, bytes):
        try:
            return json_loads_bytes(value)
        except Exception:
            return value.decode("utf-8", errors="replace")
    if isinstance(value, Decimal):
        return float(value)
    return value


def _row(row: asyncpg.Record | None) -> dict[str, Any] | None:
    return _to_primitive(dict(row)) if row else None


def _rows(rows: list[asyncpg.Record]) -> list[dict[str, Any]]:
    return [_to_primitive(dict(item)) for item in rows]


def _build_where(
    filters: AnalyticsFilters, *, start_index: int = 1
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    values: list[Any] = []

    def add(sql: str, value: Any) -> None:
        values.append(value)
        clauses.append(sql.format(n=start_index + len(values) - 1))

    if filters.date_from is not None:
        add("e.started_at >= ${n}", filters.date_from)
    if filters.date_to is not None:
        add("e.started_at < ${n}", filters.date_to)
    if filters.account:
        add("e.mango_account = ${n}", filters.account)
    if filters.manager:
        add("e.manager_name = ${n}", filters.manager)
    if filters.direction:
        add("e.direction = ${n}", filters.direction)
    if filters.status:
        add("e.status = ${n}", filters.status)
    if filters.call_type:
        add("e.call_type = ${n}", filters.call_type)
    if filters.risk_level:
        add("e.risk_level = ${n}", filters.risk_level)
    if filters.score_min is not None:
        add("e.score >= ${n}", filters.score_min)
    if filters.score_max is not None:
        add("e.score <= ${n}", filters.score_max)
    if filters.duration_min is not None:
        add("e.duration_seconds >= ${n}", filters.duration_min)
    if filters.duration_max is not None:
        add("e.duration_seconds <= ${n}", filters.duration_max)
    if filters.has_transcription is True:
        clauses.append("e.transcript IS NOT NULL")
    elif filters.has_transcription is False:
        clauses.append("e.transcript IS NULL")
    if filters.has_quality is True:
        clauses.append("e.score IS NOT NULL")
    elif filters.has_quality is False:
        clauses.append("e.score IS NULL")
    if filters.search:
        needle = f"%{filters.search.strip()}%"
        add(
            "("
            "e.id ILIKE ${n} OR "
            "COALESCE(e.from_number, '') ILIKE ${n} OR "
            "COALESCE(e.to_number, '') ILIKE ${n} OR "
            "COALESCE(e.manager_name, '') ILIKE ${n} OR "
            "COALESCE(e.transcript, '') ILIKE ${n} OR "
            "COALESCE(e.summary, '') ILIKE ${n}"
            ")",
            needle,
        )

    return ("WHERE " + " AND ".join(clauses)) if clauses else "", values


class AnalyticsRepository:
    def __init__(self, pg: asyncpg.Pool) -> None:
        self.pg = pg

    async def filter_options(self) -> dict[str, Any]:
        async with self.pg.acquire() as conn:
            meta = await conn.fetchrow("""
                SELECT MIN(started_at) AS min_started_at, MAX(started_at) AS max_started_at
                FROM calls
                """)
            accounts = await conn.fetch("""
                SELECT DISTINCT COALESCE(NULLIF(raw->>'mango_account', ''), 'primary') AS value
                FROM calls ORDER BY value
                """)
            managers = await conn.fetch("""
                SELECT DISTINCT COALESCE(NULLIF(raw#>>'{mango_employee_summary,name}', ''), '—') AS value
                FROM calls ORDER BY value
                """)
            directions = await conn.fetch(
                "SELECT DISTINCT direction AS value FROM calls WHERE direction IS NOT NULL ORDER BY value"
            )
            statuses = await conn.fetch(
                "SELECT DISTINCT status AS value FROM calls WHERE status IS NOT NULL ORDER BY value"
            )
            call_types = await conn.fetch(
                "SELECT DISTINCT call_type AS value FROM call_classifications ORDER BY value"
            )
            risk_levels = await conn.fetch(
                "SELECT DISTINCT risk_level AS value FROM quality_scores ORDER BY value"
            )

        meta_dict = _row(meta) or {}
        return {
            **meta_dict,
            "accounts": [row["value"] for row in accounts],
            "managers": [row["value"] for row in managers],
            "directions": [row["value"] for row in directions],
            "statuses": [row["value"] for row in statuses],
            "call_types": [row["value"] for row in call_types],
            "risk_levels": [row["value"] for row in risk_levels],
        }

    async def overview(self, filters: AnalyticsFilters) -> dict[str, Any]:
        where, values = _build_where(filters)
        async with self.pg.acquire() as conn:
            summary = await conn.fetchrow(
                _BASE_CTE + _SUMMARY_SELECT.format(where=where),
                *values,
            )

            timeline = await conn.fetch(
                _BASE_CTE + f"""
                SELECT
                    date_trunc('day', started_at) AS bucket,
                    COUNT(*)::int AS calls,
                    COUNT(*) FILTER (WHERE risk_level = 'critical')::int AS critical,
                    COUNT(*) FILTER (WHERE call_type = 'completed_deal')::int AS completed_deals,
                    ROUND(AVG(score)::numeric, 1) AS avg_score
                FROM enriched e
                {where}
                GROUP BY 1
                ORDER BY 1
                """,
                *values,
            )

            risk = await conn.fetch(
                _BASE_CTE + f"""
                SELECT COALESCE(risk_level, 'not_analyzed') AS name, COUNT(*)::int AS value
                FROM enriched e
                {where}
                GROUP BY 1
                ORDER BY value DESC, name
                """,
                *values,
            )

            call_types = await conn.fetch(
                _BASE_CTE + f"""
                SELECT COALESCE(call_type, 'not_classified') AS name, COUNT(*)::int AS value
                FROM enriched e
                {where}
                GROUP BY 1
                ORDER BY value DESC, name
                """,
                *values,
            )

            statuses = await conn.fetch(
                _BASE_CTE + f"""
                SELECT COALESCE(status, 'unknown') AS name, COUNT(*)::int AS value
                FROM enriched e
                {where}
                GROUP BY 1
                ORDER BY value DESC, name
                """,
                *values,
            )

            managers = await conn.fetch(
                _BASE_CTE + f"""
                SELECT
                    manager_name AS name,
                    COUNT(*)::int AS calls,
                    COUNT(*) FILTER (WHERE score IS NOT NULL)::int AS analyzed,
                    COUNT(*) FILTER (WHERE call_type IS NOT NULL)::int AS classified,
                    ROUND(AVG(score)::numeric, 1) AS avg_score,
                    COUNT(*) FILTER (WHERE risk_level = 'critical')::int AS critical,
                    COUNT(*) FILTER (WHERE call_type = 'completed_deal')::int AS completed_deals,
                    COUNT(*) FILTER (WHERE call_type = 'appointment')::int AS appointments,
                    ROUND(AVG(duration_seconds)::numeric, 1) AS avg_duration_seconds
                FROM enriched e
                {where}
                GROUP BY manager_name
                ORDER BY calls DESC, manager_name
                LIMIT 50
                """,
                *values,
            )

            duration_distribution = await conn.fetch(
                _BASE_CTE + f"""
                SELECT bucket, COUNT(*)::int AS value
                FROM (
                    SELECT CASE
                        WHEN duration_seconds IS NULL THEN 'unknown'
                        WHEN duration_seconds < 60 THEN 'lt_1m'
                        WHEN duration_seconds < 180 THEN '1_3m'
                        WHEN duration_seconds < 300 THEN '3_5m'
                        WHEN duration_seconds < 600 THEN '5_10m'
                        ELSE '10m_plus'
                    END AS bucket
                    FROM enriched e
                    {where}
                ) d
                GROUP BY bucket
                ORDER BY CASE bucket
                    WHEN 'lt_1m' THEN 1
                    WHEN '1_3m' THEN 2
                    WHEN '3_5m' THEN 3
                    WHEN '5_10m' THEN 4
                    WHEN '10m_plus' THEN 5
                    ELSE 6
                END
                """,
                *values,
            )

            attention_calls = await conn.fetch(
                _BASE_CTE + f"""
                SELECT
                    id, started_at, mango_account, manager_name, from_number, to_number,
                    status, error, call_type, score, risk_level, summary, duration_seconds
                FROM enriched e
                {where}
                  {"AND" if where else "WHERE"} (
                    risk_level = 'critical'
                    OR error IS NOT NULL
                    OR status LIKE '%failed%'
                    OR (score IS NOT NULL AND score < 60)
                )
                ORDER BY
                    CASE WHEN error IS NOT NULL OR status LIKE '%failed%' THEN 0
                         WHEN risk_level = 'critical' THEN 1
                         ELSE 2 END,
                    score ASC NULLS LAST, started_at DESC
                LIMIT 12
                """,
                *values,
            )

            criteria = await conn.fetchrow(
                _BASE_CTE + f"""
                SELECT
                    ROUND(AVG(NULLIF(criteria->>'greeting', '')::numeric), 1) AS greeting,
                    ROUND(AVG(NULLIF(criteria->>'needs_discovery', '')::numeric), 1) AS needs_discovery,
                    ROUND(AVG(NULLIF(criteria->>'urgency', '')::numeric), 1) AS urgency,
                    ROUND(AVG(NULLIF(criteria->>'target_action', '')::numeric), 1) AS target_action,
                    ROUND(AVG(NULLIF(criteria->>'objection_handling', '')::numeric), 1) AS objection_handling,
                    ROUND(AVG(NULLIF(criteria->>'closing', '')::numeric), 1) AS closing
                FROM enriched e
                {where}
                """,
                *values,
            )

        result_summary = _enrich_summary(_row(summary) or {})

        comparison: dict[str, Any] | None = None
        if (
            filters.date_from is not None
            and filters.date_to is not None
            and filters.date_to > filters.date_from
        ):
            period = filters.date_to - filters.date_from
            previous_filters = replace(
                filters,
                date_from=filters.date_from - period,
                date_to=filters.date_from,
            )
            previous_where, previous_values = _build_where(previous_filters)
            async with self.pg.acquire() as conn:
                previous_row = await conn.fetchrow(
                    _BASE_CTE + _SUMMARY_SELECT.format(where=previous_where),
                    *previous_values,
                )
            previous_summary = _enrich_summary(_row(previous_row) or {})
            comparison = _comparison(result_summary, previous_summary)

        manager_items = _rows(list(managers))
        for manager in manager_items:
            calls = int(manager.get("calls") or 0)
            analyzed_calls = int(manager.get("analyzed") or 0)
            classified_calls = int(manager.get("classified") or 0)
            deals = int(manager.get("completed_deals") or 0)
            critical = int(manager.get("critical") or 0)
            manager["analysis_coverage"] = (
                round(analyzed_calls * 100 / calls, 1) if calls else None
            )
            manager["deal_rate"] = (
                round(deals * 100 / classified_calls, 1) if classified_calls else None
            )
            manager["critical_rate"] = (
                round(critical * 100 / analyzed_calls, 1) if analyzed_calls else None
            )

        criteria_dict = _row(criteria) or {}
        criteria_items = [
            {"key": key, "value": criteria_dict.get(key)}
            for key in (
                "greeting",
                "needs_discovery",
                "urgency",
                "target_action",
                "objection_handling",
                "closing",
            )
        ]

        return {
            "summary": result_summary,
            "timeline": _rows(list(timeline)),
            "risk_distribution": _rows(list(risk)),
            "call_type_distribution": _rows(list(call_types)),
            "status_distribution": _rows(list(statuses)),
            "duration_distribution": _rows(list(duration_distribution)),
            "attention_calls": _rows(list(attention_calls)),
            "managers": manager_items,
            "criteria": criteria_items,
            "comparison": comparison,
        }

    async def list_calls(
        self,
        filters: AnalyticsFilters,
        *,
        page: int,
        page_size: int,
        sort_by: str,
        sort_order: SortOrder,
    ) -> dict[str, Any]:
        where, values = _build_where(filters)
        sort_column = _SORT_COLUMNS.get(sort_by, _SORT_COLUMNS["started_at"])
        direction = "ASC" if sort_order == "asc" else "DESC"
        offset = (page - 1) * page_size
        limit_index = len(values) + 1
        offset_index = len(values) + 2

        async with self.pg.acquire() as conn:
            total = await conn.fetchval(
                _BASE_CTE + f"SELECT COUNT(*) FROM enriched e {where}", *values
            )
            rows = await conn.fetch(
                _BASE_CTE + f"""
                SELECT
                    e.id,
                    e.mango_account,
                    e.manager_name,
                    e.manager_department,
                    e.manager_position,
                    e.manager_extension,
                    e.started_at,
                    e.finished_at,
                    e.duration_seconds,
                    e.direction,
                    e.from_number,
                    e.to_number,
                    e.status,
                    e.error,
                    e.call_type,
                    e.classification_confidence,
                    e.score,
                    e.risk_level,
                    e.summary,
                    e.disconnect_reason,
                    (e.transcript IS NOT NULL) AS has_transcription,
                    (e.audio_object_name IS NOT NULL) AS has_audio,
                    e.has_notification
                FROM enriched e
                {where}
                ORDER BY {sort_column} {direction} NULLS LAST, e.id DESC
                LIMIT ${limit_index} OFFSET ${offset_index}
                """,
                *values,
                page_size,
                offset,
            )

        total_int = int(total or 0)
        pages = max(1, (total_int + page_size - 1) // page_size)
        return {
            "items": _rows(list(rows)),
            "page": page,
            "page_size": page_size,
            "total": total_int,
            "pages": pages,
        }

    async def export_calls(
        self, filters: AnalyticsFilters, *, limit: int = 50000
    ) -> list[dict[str, Any]]:
        where, values = _build_where(filters)
        limit_index = len(values) + 1
        async with self.pg.acquire() as conn:
            rows = await conn.fetch(
                _BASE_CTE + f"""
                SELECT
                    id, started_at, finished_at, mango_account, manager_name,
                    manager_department, manager_position, direction, from_number, to_number,
                    duration_seconds, status, error, call_type, classification_confidence,
                    score, risk_level, summary, recommendation, disconnect_reason,
                    (transcript IS NOT NULL) AS has_transcription,
                    (audio_object_name IS NOT NULL) AS has_audio,
                    has_notification
                FROM enriched e
                {where}
                ORDER BY started_at DESC NULLS LAST, id DESC
                LIMIT ${limit_index}
                """,
                *values,
                limit,
            )
        return _rows(list(rows))

    async def get_call_detail(self, call_id: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            row = await conn.fetchrow(
                _BASE_CTE + """
                SELECT e.*,
                    COALESCE((
                        SELECT COUNT(*)::int
                        FROM notifications n
                        WHERE n.call_id = e.id AND n.channel = 'main'
                    ), 0) AS notification_count
                FROM enriched e
                WHERE e.id = $1
                """,
                call_id,
            )
        return _row(row)


__all__ = ["AnalyticsFilters", "AnalyticsRepository", "SortOrder"]
