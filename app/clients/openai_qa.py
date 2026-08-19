from __future__ import annotations

import base64
import json
import re
import logging
import unicodedata
from pathlib import PurePath
from typing import Any

import httpx
from openai import AsyncOpenAI

from app.common.config import Settings
from app.common.models import CallClassification, QualityResult
from app.prompts.classification import (
    CALL_CLASSIFICATION_SYSTEM_PROMPT,
    build_call_classification_prompt,
)
from app.prompts.quality import QUALITY_SYSTEM_PROMPT, build_quality_user_prompt
from app.prompts.transcription import (
    TRANSCRIPT_ROLE_REVIEW_SYSTEM_PROMPT,
    TRANSCRIPT_ROLE_SYSTEM_PROMPT,
    build_transcript_role_prompt,
    build_transcript_role_review_prompt,
)

logger = logging.getLogger(__name__)

CRITERIA_NAMES = (
    "greeting",
    "needs_discovery",
    "urgency",
    "target_action",
    "objection_handling",
    "closing",
)
CRITERIA_PROPERTIES = {
    name: {"type": "integer", "minimum": 0, "maximum": 100} for name in CRITERIA_NAMES
}
CRITERIA_STATUS_PROPERTIES = {
    name: {
        "type": "string",
        "enum": ["observed", "not_observed", "not_applicable", "uncertain"],
    }
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
                        "speaker": {
                            "type": "string",
                            "enum": ["manager", "client", "unknown"],
                        },
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

CALL_CLASSIFICATION_JSON_SCHEMA: dict[str, Any] = {
    "name": "call_classification",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "call_type": {
                "type": "string",
                "enum": [
                    "appointment",
                    "sales",
                    "delivery",
                    "consultation",
                    "completed_deal",
                    "critical",
                ],
            },
            "reason": {"type": "string"},
            "critical_errors": {"type": "array", "items": {"type": "string"}},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        },
        "required": ["call_type", "reason", "critical_errors", "confidence"],
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
            "analysis_confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
            },
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
                        "kind": {
                            "type": "string",
                            "enum": ["explicit", "soft_deferral", "condition"],
                        },
                        "category": {
                            "type": "string",
                            "enum": [
                                "price",
                                "timing",
                                "trust",
                                "need",
                                "authority",
                                "competitor",
                                "other",
                            ],
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
                        "resolution": {
                            "type": "string",
                            "enum": ["resolved", "unresolved", "unclear"],
                        },
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
                    "status": {
                        "type": "string",
                        "enum": ["agreed", "proposed", "absent", "unclear"],
                    },
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
        proxy_url = settings.openrouter_proxy_url or None
        openrouter_http_client = (
            httpx.AsyncClient(proxy=proxy_url) if proxy_url else None
        )

        proxy = settings.openrouter_proxy_url

        self.client = AsyncOpenAI(
            api_key=api_key or None,
            base_url=settings.openrouter_base_url,
            default_headers=headers,
            http_client=openrouter_http_client,
        )
        self.transcribe_timeout = httpx.Timeout(
            connect=settings.openrouter_transcribe_connect_timeout_seconds,
            write=settings.openrouter_transcribe_write_timeout_seconds,
            read=settings.openrouter_transcribe_read_timeout_seconds,
            pool=settings.openrouter_transcribe_pool_timeout_seconds,
        )

        self.stt_http = httpx.AsyncClient(
            headers={**headers, "Authorization": f"Bearer {api_key}"},
            timeout=self.transcribe_timeout,
            proxy=proxy_url,
        )
        self.transcribe_timeout_log = {
            "connect": settings.openrouter_transcribe_connect_timeout_seconds,
            "write": settings.openrouter_transcribe_write_timeout_seconds,
            "read": settings.openrouter_transcribe_read_timeout_seconds,
            "pool": settings.openrouter_transcribe_pool_timeout_seconds,
        }

        self.transcription_url = (
            f"{settings.openrouter_base_url.rstrip('/')}/audio/transcriptions"
        )
        self.transcribe_model = settings.openai_transcribe_model
        self.transcribe_language = settings.openai_transcribe_language
        self.transcribe_temperature = settings.openrouter_transcribe_temperature
        self.transcript_role_model = settings.openai_transcript_role_model
        self.transcript_role_review_model = settings.openai_transcript_role_review_model
        self.classification_model = settings.openai_classification_model
        self.quality_model = settings.openai_quality_model
        self.quality_temperature = settings.openai_quality_temperature

    async def aclose(self) -> None:
        await self.client.close()
        await self.stt_http.aclose()

    async def transcribe(
        self, *, audio: bytes, filename: str
    ) -> tuple[str, dict[str, Any]]:
        audio_format = self._audio_format(filename, audio)
        logger.info(
            "OpenRouter transcription request started: model=%s filename=%s audio_format=%s "
            "audio_size_mb=%.2f connect_timeout=%s write_timeout=%s read_timeout=%s pool_timeout=%s",
            self.transcribe_model,
            filename,
            audio_format,
            len(audio) / 1024 / 1024,
            self.transcribe_timeout_log["connect"],
            self.transcribe_timeout_log["write"],
            self.transcribe_timeout_log["read"],
            self.transcribe_timeout_log["pool"],
        )

        payload: dict[str, Any] = {
            "model": self.transcribe_model,
            "input_audio": {
                "data": base64.b64encode(audio).decode("ascii"),
                "format": audio_format,
            },
        }
        if self.transcribe_language:
            payload["language"] = self.transcribe_language
        payload["temperature"] = self.transcribe_temperature
        response = await self.stt_http.post(self.transcription_url, json=payload)
        status_code = getattr(response, "status_code", None)
        if status_code is not None and status_code >= 400:
            logger.error(
                "OpenRouter transcription request failed: status=%s body=%s",
                status_code,
                getattr(response, "text", "")[:1000],
            )

        response.raise_for_status()
        logger.info(
            "OpenRouter transcription request completed: model=%s filename=%s status=%s",
            self.transcribe_model,
            filename,
            status_code,
        )

        raw = response.json()
        if not isinstance(raw, dict):
            raise ValueError(
                f"OpenRouter transcription returned {type(raw).__name__}, expected object"
            )
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
        raise ValueError(
            f"Cannot determine supported audio format from filename: {filename!r}"
        )

    async def structure_transcript(
        self,
        transcript: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        if not self.transcript_role_model:
            return f"{SPEAKER_LABELS['unknown']}: {transcript.strip()}", {
                "enabled": False,
                "validated": True,
            }

        first = await self._request_role_structuring(
            model=self.transcript_role_model,
            system_prompt=TRANSCRIPT_ROLE_SYSTEM_PROMPT,
            user_prompt=build_transcript_role_prompt(transcript, context),
        )
        first_result = self._validate_role_structuring(transcript, first)

        review_reason = self._role_review_reason(
            transcript,
            first_result.get("turns") if first_result.get("validated") else None,
        )
        if not review_reason and first_result.get("validated"):
            return str(first_result["transcript"]), {
                "enabled": True,
                "validated": True,
                "reviewed": False,
                "model": self.transcript_role_model,
                "turns": first_result["turns"],
                "response": first["response"],
            }

        if self.transcript_role_review_model:
            review = await self._request_role_structuring(
                model=self.transcript_role_review_model,
                system_prompt=TRANSCRIPT_ROLE_REVIEW_SYSTEM_PROMPT,
                user_prompt=build_transcript_role_review_prompt(
                    transcript,
                    first_result.get("turns"),
                    context,
                ),
            )
            review_result = self._validate_role_structuring(transcript, review)
            if review_result.get("validated"):
                return str(review_result["transcript"]), {
                    "enabled": True,
                    "validated": True,
                    "reviewed": True,
                    "review_reason": review_reason or first_result.get("error"),
                    "model": self.transcript_role_review_model,
                    "initial_model": self.transcript_role_model,
                    "turns": review_result["turns"],
                    "initial_turns": first_result.get("turns"),
                    "response": review["response"],
                    "initial_response": first["response"],
                }

        if first_result.get("validated"):
            return str(first_result["transcript"]), {
                "enabled": True,
                "validated": True,
                "reviewed": bool(self.transcript_role_review_model),
                "review_reason": review_reason,
                "model": self.transcript_role_model,
                "turns": first_result["turns"],
                "response": first["response"],
            }

        return f"{SPEAKER_LABELS['unknown']}: {transcript.strip()}", {
            "enabled": True,
            "validated": False,
            "reviewed": bool(self.transcript_role_review_model),
            "model": self.transcript_role_model,
            "error": first_result.get("error") or "speaker structuring failed",
            "response": first["response"],
        }

    async def _request_role_structuring(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
    ) -> dict[str, Any]:
        completion = await self.client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": TRANSCRIPT_ROLE_JSON_SCHEMA,
            },
        )
        content = completion.choices[0].message.content or "{}"
        response_raw = (
            completion.model_dump(mode="json")
            if hasattr(completion, "model_dump")
            else {"content": content}
        )
        return {"content": content, "response": response_raw}

    def _validate_role_structuring(
        self, transcript: str, result: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            payload = json.loads(str(result.get("content") or "{}"))
            turns = payload.get("turns")
            if not isinstance(turns, list) or not turns:
                raise ValueError("speaker turn list is empty")
            texts: list[str] = []
            lines: list[str] = []
            normalized_turns: list[dict[str, str]] = []
            for turn in turns:
                if not isinstance(turn, dict):
                    raise ValueError("speaker turn is not an object")
                speaker = turn.get("speaker")
                text = str(turn.get("text") or "").strip()
                if speaker not in SPEAKER_LABELS or not text:
                    raise ValueError("speaker turn has invalid speaker or empty text")
                texts.append(text)
                normalized_turns.append({"speaker": str(speaker), "text": text})
                lines.append(f"{SPEAKER_LABELS[str(speaker)]}: {text}")

            if self._normalized_word_sequence(
                " ".join(texts)
            ) != self._normalized_word_sequence(transcript):
                raise ValueError(
                    "speaker structuring changed transcript words or their order"
                )
            return {
                "validated": True,
                "transcript": "\n".join(lines),
                "turns": normalized_turns,
            }
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            return {"validated": False, "error": str(exc), "turns": None}

    @staticmethod
    def _role_review_reason(
        transcript: str, turns: list[dict[str, Any]] | None
    ) -> str | None:
        if not turns:
            return "initial role structuring is invalid"

        speakers = [str(turn.get("speaker")) for turn in turns]
        if "unknown" in speakers:
            return "initial role structuring contains unknown speaker"

        word_count = len(re.findall(r"\w+", transcript, flags=re.UNICODE))
        known = {speaker for speaker in speakers if speaker in {"manager", "client"}}
        if word_count >= 30 and len(known) < 2:
            return "long dialogue has only one known speaker"
        if word_count >= 80 and len(turns) <= 2:
            return "long dialogue has too few turns"
        return None

    async def classify_call(
        self, *, transcript: str
    ) -> tuple[CallClassification, dict[str, Any]]:
        completion = await self.client.chat.completions.create(
            model=self.classification_model,
            temperature=0,
            messages=[
                {"role": "system", "content": CALL_CLASSIFICATION_SYSTEM_PROMPT},
                {"role": "user", "content": build_call_classification_prompt(transcript)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": CALL_CLASSIFICATION_JSON_SCHEMA,
            },
        )
        content = completion.choices[0].message.content or "{}"
        classification = CallClassification.model_validate(json.loads(content))
        if classification.call_type != "critical" and classification.critical_errors:
            classification = classification.model_copy(update={"critical_errors": []})
        raw = (
            completion.model_dump(mode="json")
            if hasattr(completion, "model_dump")
            else {"content": content}
        )
        raw["classification"] = classification.model_dump(mode="json")
        return classification, raw

    @staticmethod
    def _normalized_word_sequence(value: str) -> list[str]:
        normalized = unicodedata.normalize("NFKC", value).casefold()
        return re.findall(r"\w+", normalized, flags=re.UNICODE)

    @staticmethod
    def _normalized_content(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).casefold()
        return re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)

    async def score_quality(
        self, *, transcript: str
    ) -> tuple[QualityResult, dict[str, Any]]:
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
        raw = (
            completion.model_dump(mode="json")
            if hasattr(completion, "model_dump")
            else {"content": content}
        )
        self._validate_quality_evidence(quality, transcript)
        model_score = quality.score
        model_criteria = quality.criteria.model_dump(mode="json")
        model_risk_level = quality.risk_level
        quality, risk_warnings = self._normalize_risk_consistency(quality)
        quality = self._normalize_criteria_scores(quality)
        quality = quality.model_copy(
            update={"score": self._compute_quality_score(quality)}
        )
        raw["quality_control"] = {
            "model_score": model_score,
            "model_criteria": model_criteria,
            "model_risk_level": model_risk_level,
            "risk_warnings": risk_warnings,
            "computed_score": quality.score,
            "evidence_validated": True,
        }
        raw["analysis"] = quality.model_dump(mode="json")
        return quality, raw

    @classmethod
    def _validate_quality_evidence(
        cls, quality: QualityResult, transcript: str
    ) -> None:
        transcript_normalized = cls._normalized_content(transcript)
        quotes: list[tuple[str, str]] = []
        if quality.criteria_status and quality.criteria_evidence:
            for name in CRITERIA_NAMES:
                status = getattr(quality.criteria_status, name)
                evidence = getattr(quality.criteria_evidence, name)
                if status == "observed" and not evidence:
                    raise ValueError(
                        f"Criterion {name} is observed but has no evidence"
                    )
                quotes.extend(
                    (f"criteria_evidence.{name}", quote) for quote in evidence
                )
        for index, objection in enumerate(quality.objections):
            quotes.append(
                (f"objections[{index}].customer_quote", objection.customer_quote)
            )
            if objection.manager_response_quote:
                quotes.append(
                    (
                        f"objections[{index}].manager_response_quote",
                        objection.manager_response_quote,
                    )
                )
        if quality.next_step and quality.next_step.quote:
            quotes.append(("next_step.quote", quality.next_step.quote))
        for field, quote in quotes:
            quote_normalized = cls._normalized_content(quote)
            if not quote_normalized or quote_normalized not in transcript_normalized:
                raise ValueError(
                    f"Analysis contains unsupported quote in {field}: {quote!r}"
                )

    @staticmethod
    def _critical_risk_consistency_issue(quality: QualityResult) -> str | None:
        if quality.risk_level != "critical":
            return None
        if quality.next_step and quality.next_step.status == "agreed":
            return "critical risk cannot be set when next step is agreed"
        has_unresolved_risk = any(
            objection.kind in {"explicit", "soft_deferral"}
            and objection.resolution == "unresolved"
            for objection in quality.objections
        )
        if not has_unresolved_risk:
            return "critical risk requires an unresolved explicit objection or soft deferral"
        return None

    @classmethod
    def _normalize_risk_consistency(
        cls, quality: QualityResult
    ) -> tuple[QualityResult, list[str]]:
        issue = cls._critical_risk_consistency_issue(quality)
        if issue is None:
            return quality, []

        warning = f"Model critical risk downgraded to warning: {issue}"
        original_reason = quality.risk_reason.strip() or "не указана"
        risk_reason = (
            "Риск понижен до warning программной проверкой: нет достаточных фактов для critical. "
            f"Причина модели: {original_reason}"
        )
        return quality.model_copy(
            update={
                "risk_level": "warning",
                "risk_reason": risk_reason,
                "limitations": [*quality.limitations, warning],
            }
        ), [warning]

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
        return quality.model_copy(
            update={"criteria": quality.criteria.model_copy(update=updates)}
        )

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
