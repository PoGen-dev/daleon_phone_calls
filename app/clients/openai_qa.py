from __future__ import annotations

import json
from typing import Any

from openai import AsyncOpenAI

from app.common.config import Settings
from app.common.models import QualityResult
from app.prompts.quality import QUALITY_SYSTEM_PROMPT, build_quality_user_prompt

QUALITY_JSON_SCHEMA: dict[str, Any] = {
    "name": "call_quality_score",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "score": {"type": "integer", "minimum": 0, "maximum": 100},
            "summary": {"type": "string"},
            "positives": {"type": "array", "items": {"type": "string"}},
            "negatives": {"type": "array", "items": {"type": "string"}},
            "recommendations": {"type": "array", "items": {"type": "string"}},
            "criteria": {
                "type": "object",
                "properties": {
                    "greeting": {"type": "integer", "minimum": 0, "maximum": 100},
                    "needs_discovery": {"type": "integer", "minimum": 0, "maximum": 100},
                    "clarity": {"type": "integer", "minimum": 0, "maximum": 100},
                    "empathy": {"type": "integer", "minimum": 0, "maximum": 100},
                    "resolution": {"type": "integer", "minimum": 0, "maximum": 100},
                    "compliance": {"type": "integer", "minimum": 0, "maximum": 100},
                },
                "required": ["greeting", "needs_discovery", "clarity", "empathy", "resolution", "compliance"],
                "additionalProperties": False,
            },
        },
        "required": ["score", "summary", "positives", "negatives", "recommendations", "criteria"],
        "additionalProperties": False,
    },
}


class OpenAIQaClient:
    def __init__(self, settings: Settings) -> None:
        api_key = settings.openai_api_key.get_secret_value()
        self.client = AsyncOpenAI(api_key=api_key or None)
        self.transcribe_model = settings.openai_transcribe_model
        self.quality_model = settings.openai_quality_model
        self.quality_temperature = settings.openai_quality_temperature

    async def transcribe(self, *, audio: bytes, filename: str) -> tuple[str, dict[str, Any]]:
        result = await self.client.audio.transcriptions.create(
            model=self.transcribe_model,
            file=(filename, audio),
            response_format="json",
        )
        raw = result.model_dump(mode="json") if hasattr(result, "model_dump") else {"text": str(result)}
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
        parsed = json.loads(content)
        quality = QualityResult.model_validate(parsed)
        raw = completion.model_dump(mode="json") if hasattr(completion, "model_dump") else {"content": content}
        return quality, raw
