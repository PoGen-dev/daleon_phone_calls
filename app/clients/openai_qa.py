from __future__ import annotations

import base64
import json
import re
import unicodedata
from pathlib import PurePath
from typing import Any

import httpx
from openai import AsyncOpenAI

from app.common.config import Settings
from app.common.models import QualityResult
from app.prompts.quality import QUALITY_SYSTEM_PROMPT, build_quality_user_prompt
from app.prompts.transcription import TRANSCRIPT_ROLE_SYSTEM_PROMPT, build_transcript_role_prompt

CRITERIA_NAMES = ("greeting", "needs_discovery", "urgency", "target_action", "objection_handling", "closing")
CRITERIA_PROPERTIES = {
    name: {"type": "integer", "minimum": 0, "maximum": 100}
    for name in CRITERIA_NAMES
}
CRITERIA_STATUS_PROPERTIES = {
    name: {"type": "string", "enum": ["observed", "not_observed", "not_applicable", "uncertain"]}
    for name in CRITERIA_NAMES
}
CRITERIA_EVIDENCE_PROPERTIES = {
    name: {"type": "array", "items": {"type": "string"}} for name in CRITERIA_NAMES
}

TRANSCRIPT_ROLE_JSON_SCHEMA: dict[str, Any] = {
    "name": "speaker_turns",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "turns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "speaker": {"type": "string", "enum": ["manager", "client", "unknown"]},
                        "text": {"type": "string"},
                    },
                    "required": ["speaker", "text"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["turns"],
        "additionalProperties": False,
    },
}

QUALITY_JSON_SCHEMA: dict[str, Any] = {
    "name": "call_analysis",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "risk_level": {"type": "string", "enum": ["critical", "warning", "normal"]},
            "risk_reason": {"type": "string"},
            "summary": {"type": "string"},
            "errors": {"type": "array", "items": {"type": "string"}},
            "recommendation": {"type": "string"},
            "analysis_confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            "limitations": {"type": "array", "items": {"type": "string"}},
            "criteria": {
                "type": "object",
                "properties": CRITERIA_PROPERTIES,
                "required": list(CRITERIA_PROPERTIES),
                "additionalProperties": False,
            },
            "criteria_status": {
                "type": "object",
                "properties": CRITERIA_STATUS_PROPERTIES,
                "required": list(CRITERIA_STATUS_PROPERTIES),
                "additionalProperties": False,
            },
            "criteria_evidence": {
                "type": "object",
                "properties": CRITERIA_EVIDENCE_PROPERTIES,
                "required": list(CRITERIA_EVIDENCE_PROPERTIES),
                "additionalProperties": False,
            },
            "objections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "customer_quote": {"type": "string"},
                        "kind": {"type": "string", "enum": ["explicit", "soft_deferral", "condition"]},
                        "category": {
                            "type": "string",
                            "enum": ["price", "timing", "trust", "need", "authority", "competitor", "other"],
                        },
                        "manager_response_quote": {"type": ["string", "null"]},
                        "completed_steps": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": [
                                    "acknowledged",
                                    "clarified",
                                    "answered",
                                    "checked_resolution",
                                    "agreed_next_step",
                                ],
                            },
                        },
                        "missing_steps": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": [
                                    "acknowledged",
                                    "clarified",
                                    "answered",
                                    "checked_resolution",
                                    "agreed_next_step",
                                ],
                            },
                        },
                        "resolution": {"type": "string", "enum": ["resolved", "unresolved", "unclear"]},
                    },
                    "required": [
                        "customer_quote",
                        "kind",
                        "category",
                        "manager_response_quote",
                        "completed_steps",
                        "missing_steps",
                        "resolution",
                    ],
                    "additionalProperties": False,
                },
            },
            "next_step": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["agreed", "proposed", "absent", "unclear"]},
                    "quote": {"type": ["string", "null"]},
                },
                "required": ["status", "quote"],
                "additionalProperties": False,
            },
        },
        "required": [
            "score",
            "risk_level",
            "risk_reason",
            "summary",
            "errors",
            "recommendation",
            "analysis_confidence",
            "limitations",
            "criteria",
            "criteria_status",
            "criteria_evidence",
            "objections",
            "next_step",
        ],
        "additionalProperties": False,
    },
}

CRITERIA_WEIGHTS = {
    "greeting": 0.10,
    "needs_discovery": 0.20,
    "urgency": 0.10,
    "target_action": 0.20,
    "objection_handling": 0.20,
    "closing": 0.20,
}
SPEAKER_LABELS = {
    "manager": "Менеджер",
    "client": "Клиент",
    "unknown": "Спикер не определён",
}


class OpenAIQaClient:
    def __init__(self, settings: Settings) -> None:
        api_key = settings.openrouter_api_key.get_secret_value()
        headers = {"X-Title": settings.openrouter_app_name}
        if settings.openrouter_http_referer:
            headers["HTTP-Referer"] = settings.openrouter_http_referer
        self.client = AsyncOpenAI(
            api_key=api_key or None,
            base_url=settings.openrouter_base_url,
            default_headers=headers,
        )
        self.stt_http = httpx.AsyncClient(
            headers={**headers, "Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(30.0, read=600.0),
        )
        self.transcription_url = f"{settings.openrouter_base_url.rstrip('/')}/audio/transcriptions"
        self.transcribe_model = settings.openai_transcribe_model
        self.transcribe_language = settings.openai_transcribe_language
        self.transcript_role_model = settings.openai_transcript_role_model
        self.quality_model = settings.openai_quality_model
        self.quality_temperature = settings.openai_quality_temperature

    async def aclose(self) -> None:
        await self.client.close()
        await self.stt_http.aclose()

    async def transcribe(self, *, audio: bytes, filename: str) -> tuple[str, dict[str, Any]]:
        payload: dict[str, Any] = {
            "model": self.transcribe_model,
            "input_audio": {
                "data": base64.b64encode(audio).decode("ascii"),
                "format": self._audio_format(filename, audio),
            },
        }
        if self.transcribe_language:
            payload["language"] = self.transcribe_language
        response = await self.stt_http.post(self.transcription_url, json=payload)
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict):
            raise ValueError(f"OpenRouter transcription returned {type(raw).__name__}, expected object")
        text = raw.get("text") or ""
        return str(text), raw

    @staticmethod
    def _audio_format(filename: str, audio: bytes) -> str:
        if len(audio) >= 12 and audio.startswith(b"RIFF") and audio[8:12] == b"WAVE":
            return "wav"
        if audio.startswith(b"ID3"):
            return "mp3"
        if audio.startswith(b"OggS"):
            return "ogg"
        if audio.startswith(b"fLaC"):
            return "flac"
        if len(audio) >= 8 and audio[4:8] == b"ftyp":
            return "m4a"
        if audio.startswith(b"\x1aE\xdf\xa3"):
            return "webm"
        suffix = PurePath(filename).suffix.lower().lstrip(".")
        aliases = {"wave": "wav", "oga": "ogg", "mp4": "m4a"}
        suffix = aliases.get(suffix, suffix)
        if suffix in {"wav", "mp3", "aiff", "aac", "ogg", "flac", "m4a", "webm"}:
            return suffix
        raise ValueError(f"Cannot determine supported audio format from filename: {filename!r}")

    async def structure_transcript(self, transcript: str) -> tuple[str, dict[str, Any]]:
        if not self.transcript_role_model:
            return f"{SPEAKER_LABELS['unknown']}: {transcript.strip()}", {"enabled": False, "validated": True}
        completion = await self.client.chat.completions.create(
            model=self.transcript_role_model,
            temperature=0,
            messages=[
                {"role": "system", "content": TRANSCRIPT_ROLE_SYSTEM_PROMPT},
                {"role": "user", "content": build_transcript_role_prompt(transcript)},
            ],
            response_format={"type": "json_schema", "json_schema": TRANSCRIPT_ROLE_JSON_SCHEMA},
        )
        content = completion.choices[0].message.content or "{}"
        response_raw = completion.model_dump(mode="json") if hasattr(completion, "model_dump") else {"content": content}
        try:
            payload = json.loads(content)
            turns = payload.get("turns")
            if not isinstance(turns, list) or not turns:
                raise ValueError("speaker turn list is empty")
            texts: list[str] = []
            lines: list[str] = []
            for turn in turns:
                speaker = turn.get("speaker")
                text = str(turn.get("text") or "").strip()
                if speaker not in SPEAKER_LABELS or not text:
                    raise ValueError("speaker turn has invalid speaker or empty text")
                texts.append(text)
                lines.append(f"{SPEAKER_LABELS[speaker]}: {text}")
            if self._normalized_content(" ".join(texts)) != self._normalized_content(transcript):
                raise ValueError("speaker structuring changed transcript words or their order")
            return "\n".join(lines), {
                "enabled": True,
                "validated": True,
                "model": self.transcript_role_model,
                "turns": turns,
                "response": response_raw,
            }
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            return f"{SPEAKER_LABELS['unknown']}: {transcript.strip()}", {
                "enabled": True,
                "validated": False,
                "model": self.transcript_role_model,
                "error": str(exc),
                "response": response_raw,
            }

    @staticmethod
    def _normalized_content(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).casefold()
        return re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)

    async def score_quality(self, *, transcript: str) -> tuple[QualityResult, dict[str, Any]]:
        completion = await self.client.chat.completions.create(
            model=self.quality_model,
            temperature=self.quality_temperature,
            messages=[
                {"role": "system", "content": QUALITY_SYSTEM_PROMPT},
                {"role": "user", "content": build_quality_user_prompt(transcript)},
            ],
            response_format={"type": "json_schema", "json_schema": QUALITY_JSON_SCHEMA},
        )
        content = completion.choices[0].message.content or "{}"
        quality = QualityResult.model_validate(json.loads(content))
        raw = completion.model_dump(mode="json") if hasattr(completion, "model_dump") else {"content": content}
        self._validate_quality_evidence(quality, transcript)
        self._validate_risk_consistency(quality)
        model_score = quality.score
        model_criteria = quality.criteria.model_dump(mode="json")
        quality = self._normalize_criteria_scores(quality)
        quality = quality.model_copy(update={"score": self._compute_quality_score(quality)})
        raw["quality_control"] = {
            "model_score": model_score,
            "model_criteria": model_criteria,
            "computed_score": quality.score,
            "evidence_validated": True,
        }
        raw["analysis"] = quality.model_dump(mode="json")
        return quality, raw

    @classmethod
    def _validate_quality_evidence(cls, quality: QualityResult, transcript: str) -> None:
        transcript_normalized = cls._normalized_content(transcript)
        quotes: list[tuple[str, str]] = []
        if quality.criteria_status and quality.criteria_evidence:
            for name in CRITERIA_NAMES:
                status = getattr(quality.criteria_status, name)
                evidence = getattr(quality.criteria_evidence, name)
                if status == "observed" and not evidence:
                    raise ValueError(f"Criterion {name} is observed but has no evidence")
                quotes.extend((f"criteria_evidence.{name}", quote) for quote in evidence)
        for index, objection in enumerate(quality.objections):
            quotes.append((f"objections[{index}].customer_quote", objection.customer_quote))
            if objection.manager_response_quote:
                quotes.append((f"objections[{index}].manager_response_quote", objection.manager_response_quote))
        if quality.next_step and quality.next_step.quote:
            quotes.append(("next_step.quote", quality.next_step.quote))
        for field, quote in quotes:
            quote_normalized = cls._normalized_content(quote)
            if not quote_normalized or quote_normalized not in transcript_normalized:
                raise ValueError(f"Analysis contains unsupported quote in {field}: {quote!r}")

    @staticmethod
    def _validate_risk_consistency(quality: QualityResult) -> None:
        if quality.risk_level != "critical":
            return
        if quality.next_step and quality.next_step.status == "agreed":
            raise ValueError("Critical risk cannot be set when next step is agreed")
        has_unresolved_risk = any(
            objection.kind in {"explicit", "soft_deferral"} and objection.resolution == "unresolved"
            for objection in quality.objections
        )
        if not has_unresolved_risk:
            raise ValueError("Critical risk requires an unresolved explicit objection or soft deferral")

    @staticmethod
    def _normalize_criteria_scores(quality: QualityResult) -> QualityResult:
        if not quality.criteria_status:
            return quality
        updates: dict[str, int] = {}
        for name in CRITERIA_NAMES:
            status = getattr(quality.criteria_status, name)
            if status == "not_observed":
                updates[name] = 0
            elif status in {"not_applicable", "uncertain"}:
                updates[name] = 50
        return quality.model_copy(update={"criteria": quality.criteria.model_copy(update=updates)})

    @staticmethod
    def _compute_quality_score(quality: QualityResult) -> int:
        weighted_sum = 0.0
        weight_sum = 0.0
        for name, weight in CRITERIA_WEIGHTS.items():
            if quality.criteria_status and getattr(quality.criteria_status, name) in {
                "not_applicable",
                "uncertain",
            }:
                continue
            weighted_sum += getattr(quality.criteria, name) * weight
            weight_sum += weight
        return round(weighted_sum / weight_sum) if weight_sum else 0
