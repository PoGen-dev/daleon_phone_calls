from __future__ import annotations

import asyncio
from typing import Any

from app.common.models import DeadLetterEvent


async def retry_or_dead_letter(
    *,
    producer: Any,
    settings: Any,
    source_topic: str,
    payload: dict[str, Any],
    error: Exception,
    service: str,
) -> bool:
    """Republish a failed task, or send it to DLQ after the final attempt.

    Returns True when the task was moved to DLQ and False when it was scheduled for retry.
    """
    from app.common.kafka import publish_json

    try:
        attempt = max(1, int(payload.get("attempt", 1)))
    except (TypeError, ValueError):
        attempt = 1
    key = payload.get("call_id")
    if attempt < settings.retry_max_attempts:
        retry_payload = {**payload, "attempt": attempt + 1}
        if settings.retry_backoff_seconds:
            await asyncio.sleep(settings.retry_backoff_seconds)
        await publish_json(producer, source_topic, retry_payload, key=key)
        return False

    event = DeadLetterEvent(
        source_topic=source_topic,
        payload=payload,
        error=str(error),
        service=service,
        attempts=attempt,
    )
    await publish_json(producer, settings.topic_dead_letter, event.model_dump(mode="json"), key=key)
    return True
