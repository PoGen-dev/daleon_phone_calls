from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import asyncpg

from app.common.models import CallRecord, QualityResult
from app.common.serialization import json_dumps_bytes


def _json(payload: Any) -> str:
    return json_dumps_bytes(payload).decode("utf-8")


def _to_primitive(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _to_primitive(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_primitive(item) for item in value]
    return value


class Repository:
    def __init__(self, pg: asyncpg.Pool) -> None:
        self.pg = pg

    async def save_call(
        self,
        call: CallRecord,
        *,
        status: str = "discovered",
        audio_bucket: str | None = None,
        audio_object_name: str | None = None,
        audio_filename: str | None = None,
    ) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calls (
                    id, entry_id, call_id, recording_id, recording_url, direction,
                    from_number, to_number, started_at, finished_at, disconnect_reason, raw, status,
                    audio_bucket, audio_object_name, audio_filename
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$16)
                ON CONFLICT (id) DO UPDATE SET
                    entry_id = EXCLUDED.entry_id,
                    call_id = EXCLUDED.call_id,
                    recording_id = COALESCE(EXCLUDED.recording_id, calls.recording_id),
                    recording_url = COALESCE(EXCLUDED.recording_url, calls.recording_url),
                    direction = COALESCE(EXCLUDED.direction, calls.direction),
                    from_number = COALESCE(EXCLUDED.from_number, calls.from_number),
                    to_number = COALESCE(EXCLUDED.to_number, calls.to_number),
                    started_at = COALESCE(EXCLUDED.started_at, calls.started_at),
                    finished_at = COALESCE(EXCLUDED.finished_at, calls.finished_at),
                    disconnect_reason = COALESCE(EXCLUDED.disconnect_reason, calls.disconnect_reason),
                    raw = calls.raw || EXCLUDED.raw,
                    audio_bucket = COALESCE(EXCLUDED.audio_bucket, calls.audio_bucket),
                    audio_object_name = COALESCE(EXCLUDED.audio_object_name, calls.audio_object_name),
                    audio_filename = COALESCE(EXCLUDED.audio_filename, calls.audio_filename),
                    status = CASE
                        WHEN calls.status IN ('recorded', 'transcribed', 'analyzed', 'notified') THEN calls.status
                        ELSE EXCLUDED.status
                    END,
                    error = NULL
                """,
                call.id,
                call.entry_id,
                call.call_id,
                call.recording_id,
                call.recording_url,
                call.direction,
                call.from_number,
                call.to_number,
                call.started_at,
                call.finished_at,
                call.disconnect_reason,
                _json(call.raw),
                status,
                audio_bucket,
                audio_object_name,
                audio_filename,
            )

    async def mark_call_status(self, call_id: str, status: str, error: str | None = None) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute("UPDATE calls SET status=$2, error=$3 WHERE id=$1", call_id, status, error)

    async def get_call(self, call_id: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
        return _to_primitive(dict(row)) if row else None

    async def get_call_with_results(self, call_id: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT c.*, t.transcript, t.model AS transcription_model,
                    t.language AS transcription_language, q.score, q.risk_level, q.risk_reason,
                    q.summary, q.errors, q.recommendation, q.criteria, q.model AS quality_model
                FROM calls c
                LEFT JOIN transcriptions t ON t.call_id = c.id
                LEFT JOIN quality_scores q ON q.call_id = c.id
                WHERE c.id = $1
                """,
                call_id,
            )
        return _to_primitive(dict(row)) if row else None

    async def save_transcription(
        self,
        *,
        call_id: str,
        transcript: str,
        model: str,
        raw: dict[str, Any] | None = None,
        language: str | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        async with self.pg.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO transcriptions (call_id, transcript, model, language, duration_seconds, raw)
                    VALUES ($1,$2,$3,$4,$5,$6::jsonb)
                    ON CONFLICT (call_id) DO UPDATE SET transcript=EXCLUDED.transcript, model=EXCLUDED.model,
                        language=EXCLUDED.language, duration_seconds=EXCLUDED.duration_seconds,
                        raw=EXCLUDED.raw, created_at=now()
                    """,
                    call_id,
                    transcript,
                    model,
                    language,
                    duration_seconds,
                    _json(raw or {}),
                )
                await conn.execute("UPDATE calls SET status='transcribed', error=NULL WHERE id=$1", call_id)

    async def transcription_exists(self, call_id: str) -> bool:
        async with self.pg.acquire() as conn:
            value = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM transcriptions WHERE call_id=$1)", call_id)
        return bool(value)

    async def get_transcription(self, call_id: str) -> str | None:
        async with self.pg.acquire() as conn:
            return await conn.fetchval("SELECT transcript FROM transcriptions WHERE call_id=$1", call_id)

    async def quality_exists(self, call_id: str) -> bool:
        async with self.pg.acquire() as conn:
            value = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM quality_scores WHERE call_id=$1)", call_id)
        return bool(value)

    async def save_quality(
        self,
        *,
        call_id: str,
        quality: QualityResult,
        model: str,
        raw: dict[str, Any] | None = None,
    ) -> None:
        async with self.pg.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO quality_scores (
                        call_id, score, risk_level, risk_reason, summary, errors,
                        recommendation, criteria, raw, model
                    ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,$10)
                    ON CONFLICT (call_id) DO UPDATE SET score=EXCLUDED.score,
                        risk_level=EXCLUDED.risk_level, risk_reason=EXCLUDED.risk_reason,
                        summary=EXCLUDED.summary, errors=EXCLUDED.errors,
                        recommendation=EXCLUDED.recommendation, criteria=EXCLUDED.criteria,
                        raw=EXCLUDED.raw, model=EXCLUDED.model, created_at=now()
                    """,
                    call_id,
                    quality.score,
                    quality.risk_level,
                    quality.risk_reason,
                    quality.summary,
                    _json(quality.errors),
                    quality.recommendation,
                    _json(quality.criteria.model_dump(mode="json")),
                    _json(raw or quality.model_dump(mode="json")),
                    model,
                )
                await conn.execute("UPDATE calls SET status='analyzed', error=NULL WHERE id=$1", call_id)

    async def notification_exists(
        self, event_id: str, call_id: str | None = None, channel: str | None = None
    ) -> bool:
        async with self.pg.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM notifications
                    WHERE event_id=$1 OR ($2::text IS NOT NULL AND call_id=$2 AND channel=$3)
                )
                """,
                event_id,
                call_id,
                channel,
            )
        return bool(value)

    async def save_notification(self, event_id: str, call_id: str | None, channel: str) -> None:
        async with self.pg.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO notifications (event_id, call_id, channel) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    event_id,
                    call_id,
                    channel,
                )
                if call_id and channel == "main":
                    await conn.execute("UPDATE calls SET status='notified', error=NULL WHERE id=$1", call_id)

    async def get_state(self, name: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM worker_state WHERE name=$1", name)
        return dict(value) if value else None

    async def set_state(self, name: str, value: dict[str, Any]) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO worker_state (name, value) VALUES ($1, $2::jsonb)
                ON CONFLICT (name) DO UPDATE SET value=EXCLUDED.value, updated_at=now()
                """,
                name,
                _json(value),
            )
