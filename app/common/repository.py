from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from motor.motor_asyncio import AsyncIOMotorDatabase

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
        return {k: _to_primitive(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_primitive(v) for v in value]
    return value


class Repository:
    def __init__(self, pg: asyncpg.Pool, mongo: AsyncIOMotorDatabase) -> None:
        self.pg = pg
        self.mongo = mongo

    async def save_call(self, call: CallRecord, *, status: str = "discovered") -> None:
        async with self.pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO calls (
                    id, entry_id, call_id, recording_id, recording_url, direction,
                    from_number, to_number, started_at, finished_at, disconnect_reason, raw, status
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
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
                    status = CASE
                        WHEN calls.status IN ('transcribed', 'quality_scored') THEN calls.status
                        ELSE EXCLUDED.status
                    END
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
            )
        await self.mongo.calls_raw.update_one(
            {"_id": call.id},
            {
                "$set": {
                    **call.model_dump(mode="json"),
                    "status": status,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()},
            },
            upsert=True,
        )

    async def mark_call_status(self, call_id: str, status: str, error: str | None = None) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute("UPDATE calls SET status=$2, error=$3 WHERE id=$1", call_id, status, error)
        await self.mongo.calls_raw.update_one(
            {"_id": call_id},
            {"$set": {"status": status, "error": error, "updated_at": datetime.now(timezone.utc).isoformat()}},
            upsert=False,
        )

    async def get_call(self, call_id: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM calls WHERE id=$1", call_id)
        return dict(row) if row else None

    async def get_call_with_results(self, call_id: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    c.*,
                    t.transcript,
                    t.model AS transcription_model,
                    t.language AS transcription_language,
                    q.score,
                    q.summary,
                    q.positives,
                    q.negatives,
                    q.recommendations,
                    q.criteria,
                    q.model AS quality_model
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
            await conn.execute(
                """
                INSERT INTO transcriptions (call_id, transcript, model, language, duration_seconds, raw)
                VALUES ($1,$2,$3,$4,$5,$6::jsonb)
                ON CONFLICT (call_id) DO UPDATE SET
                    transcript = EXCLUDED.transcript,
                    model = EXCLUDED.model,
                    language = EXCLUDED.language,
                    duration_seconds = EXCLUDED.duration_seconds,
                    raw = EXCLUDED.raw,
                    created_at = now()
                """,
                call_id,
                transcript,
                model,
                language,
                duration_seconds,
                _json(raw or {}),
            )
        await self.mark_call_status(call_id, "transcribed")
        await self.mongo.transcriptions.update_one(
            {"_id": call_id},
            {
                "$set": {
                    "call_id": call_id,
                    "transcript": transcript,
                    "model": model,
                    "language": language,
                    "duration_seconds": duration_seconds,
                    "raw": raw or {},
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()},
            },
            upsert=True,
        )

    async def transcription_exists(self, call_id: str) -> bool:
        async with self.pg.acquire() as conn:
            exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM transcriptions WHERE call_id=$1)", call_id)
        return bool(exists)

    async def get_transcription(self, call_id: str) -> str | None:
        async with self.pg.acquire() as conn:
            return await conn.fetchval("SELECT transcript FROM transcriptions WHERE call_id=$1", call_id)

    async def quality_exists(self, call_id: str) -> bool:
        async with self.pg.acquire() as conn:
            exists = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM quality_scores WHERE call_id=$1)", call_id)
        return bool(exists)

    async def save_quality(
        self,
        *,
        call_id: str,
        quality: QualityResult,
        model: str,
        raw: dict[str, Any] | None = None,
    ) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO quality_scores (
                    call_id, score, summary, positives, negatives, recommendations, criteria, raw, model
                ) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9)
                ON CONFLICT (call_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    summary = EXCLUDED.summary,
                    positives = EXCLUDED.positives,
                    negatives = EXCLUDED.negatives,
                    recommendations = EXCLUDED.recommendations,
                    criteria = EXCLUDED.criteria,
                    raw = EXCLUDED.raw,
                    model = EXCLUDED.model,
                    created_at = now()
                """,
                call_id,
                quality.score,
                quality.summary,
                _json(quality.positives),
                _json(quality.negatives),
                _json(quality.recommendations),
                _json(quality.criteria),
                _json(raw or quality.model_dump(mode="json")),
                model,
            )
        await self.mark_call_status(call_id, "quality_scored")
        await self.mongo.quality_scores.update_one(
            {"_id": call_id},
            {
                "$set": {
                    "call_id": call_id,
                    **quality.model_dump(mode="json"),
                    "model": model,
                    "raw": raw or quality.model_dump(mode="json"),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc).isoformat()},
            },
            upsert=True,
        )

    async def get_state(self, name: str) -> dict[str, Any] | None:
        async with self.pg.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM worker_state WHERE name=$1", name)
        return dict(value) if value else None

    async def set_state(self, name: str, value: dict[str, Any]) -> None:
        async with self.pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO worker_state (name, value)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (name) DO UPDATE SET value=EXCLUDED.value, updated_at=now()
                """,
                name,
                _json(value),
            )
