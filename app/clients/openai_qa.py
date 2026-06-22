from __future__ import annotations

import json
from typing import Any

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
        self.transcribe_model = settings.openai_transcribe_model
        self.quality_model = settings.openai_quality_model
        self.quality_temperature = settings.openai_quality_temperature

    async def aclose(self) -> None:
        await self.client.close()

    async def transcribe(self, *, audio: bytes, filename: str) -> tuple[str, dict[str, Any]]:
        result = await self.client.audio.transcriptions.create(
            model=self.transcribe_model,
            file=(filename, audio),
            response_format="json",
        )
        raw = (
            result.model_dump(mode="json")
            if hasattr(result, "model_dump")
            else {"text": str(getattr(result, "text", result))}
        )
        text = raw.get("text") or getattr(result, "text", "")
        return str(text), raw

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
