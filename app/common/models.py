from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


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


class TaskEvent(BaseEvent):
    attempt: int = Field(default=1, ge=1)


class CallDiscoveredEvent(BaseEvent):
    event_type: Literal["call.discovered"] = "call.discovered"
    call: CallRecord


class TranscriptionRequestedEvent(TaskEvent):
    event_type: Literal["transcription.requested"] = "transcription.requested"
    call_id: str
    object_name: str
    filename: str


class AnalysisRequestedEvent(TaskEvent):
    event_type: Literal["analysis.requested"] = "analysis.requested"
    call_id: str


class NotificationRequestedEvent(TaskEvent):
    event_type: Literal["notification.requested"] = "notification.requested"
    call_id: str


class DeadLetterEvent(BaseEvent):
    event_type: Literal["dead_letter"] = "dead_letter"
    source_topic: str
    payload: dict[str, Any]
    error: str
    service: str
    attempts: int


class OutboxMessage(BaseModel):
    topic: str
    key: str | None = None
    payload: dict[str, Any]
    dedupe_key: str


class QualityCriteria(BaseModel):
    greeting: int = Field(ge=0, le=100)
    needs_discovery: int = Field(ge=0, le=100)
    urgency: int = Field(ge=0, le=100)
    target_action: int = Field(ge=0, le=100)
    objection_handling: int = Field(ge=0, le=100)
    closing: int = Field(ge=0, le=100)


CriterionStatus = Literal["observed", "not_observed", "not_applicable", "uncertain"]
ObjectionStep = Literal[
    "acknowledged",
    "clarified",
    "answered",
    "checked_resolution",
    "agreed_next_step",
]


class QualityCriteriaStatus(BaseModel):
    greeting: CriterionStatus
    needs_discovery: CriterionStatus
    urgency: CriterionStatus
    target_action: CriterionStatus
    objection_handling: CriterionStatus
    closing: CriterionStatus


class QualityCriteriaEvidence(BaseModel):
    greeting: list[str]
    needs_discovery: list[str]
    urgency: list[str]
    target_action: list[str]
    objection_handling: list[str]
    closing: list[str]


class ObjectionAnalysis(BaseModel):
    customer_quote: str
    kind: Literal["explicit", "soft_deferral", "condition"]
    category: Literal["price", "timing", "trust", "need", "authority", "competitor", "other"]
    manager_response_quote: str | None
    completed_steps: list[ObjectionStep]
    missing_steps: list[ObjectionStep]
    resolution: Literal["resolved", "unresolved", "unclear"]


class NextStepAnalysis(BaseModel):
    status: Literal["agreed", "proposed", "absent", "unclear"]
    quote: str | None


CallType = Literal[
    "appointment",
    "sales",
    "delivery",
    "consultation",
    "completed_deal",
    "critical",
]


class CallClassification(BaseModel):
    call_type: CallType
    reason: str
    critical_errors: list[str] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"]

    @field_validator("critical_errors", mode="after")
    @classmethod
    def normalize_critical_errors(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(item.strip() for item in value if item.strip()))

    @model_validator(mode="after")
    def validate_critical_details(self) -> "CallClassification":
        if self.call_type == "critical" and not self.critical_errors:
            raise ValueError("critical call must contain at least one critical error")
        return self


class QualityResult(BaseModel):
    score: int = Field(ge=0, le=100)
    risk_level: Literal["critical", "warning", "normal"]
    risk_reason: str
    summary: str
    errors: list[str] = Field(default_factory=list)
    recommendation: str
    criteria: QualityCriteria
    analysis_confidence: Literal["high", "medium", "low"] | None = None
    limitations: list[str] = Field(default_factory=list)
    criteria_status: QualityCriteriaStatus | None = None
    criteria_evidence: QualityCriteriaEvidence | None = None
    objections: list[ObjectionAnalysis] = Field(default_factory=list)
    next_step: NextStepAnalysis | None = None
