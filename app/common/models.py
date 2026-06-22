from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class CallRecord(BaseModel):
    id: str
    entry_id: str | None = None
    call_id: str | None = None
    recording_id: str | None = None
    recording_url: str | None = None
    direction: str | None = None
    from_number: str | None = None
    to_number: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    disconnect_reason: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("started_at", "finished_at", mode="before")
    @classmethod
    def normalize_dt(cls, value: Any) -> Any:
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_version: str = "1.0"
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CallDiscoveredEvent(BaseEvent):
    event_type: Literal["call.discovered"] = "call.discovered"
    call: CallRecord


class TranscriptionRequestedEvent(BaseEvent):
    event_type: Literal["transcription.requested"] = "transcription.requested"
    call_id: str
    recording_id: str | None = None
    recording_url: str | None = None


class CallTranscribedEvent(BaseEvent):
    event_type: Literal["call.transcribed"] = "call.transcribed"
    call_id: str
    transcript_chars: int
    model: str


class QualityScoredEvent(BaseEvent):
    event_type: Literal["call.quality_scored"] = "call.quality_scored"
    call_id: str
    score: int
    model: str


class DeadLetterEvent(BaseEvent):
    event_type: Literal["dead_letter"] = "dead_letter"
    source_topic: str
    payload: dict[str, Any]
    error: str
    service: str


class QualityResult(BaseModel):
    score: int = Field(ge=0, le=100)
    summary: str
    positives: list[str] = Field(default_factory=list)
    negatives: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    criteria: dict[str, int] = Field(default_factory=dict)
