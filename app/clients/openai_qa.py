from __future__ import annotations

import base64
import json
from pathlib import PurePath
from typing import Any

import httpx
from openai import AsyncOpenAI

from app.common.config import Settings
from app.common.models import QualityResult
from app.prompts.quality import QUALITY_SYSTEM_PROMPT, build_quality_user_prompt

CRITERIA_PROPERTIES = {
    name: {"type": "integer", "minimum": 0, "maximum": 100}
    for name in ("greeting", "needs_discovery", "urgency", "target_action", "objection_handling", "closing")
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
            "criteria": {
                "type": "object",
                "properties": CRITERIA_PROPERTIES,
                "required": list(CRITERIA_PROPERTIES),
                "additionalProperties": False,
            },
        },
        "required": ["score", "risk_level", "risk_reason", "summary", "errors", "recommendation", "criteria"],
        "additionalProperties": False,
    },
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
        return quality, raw
