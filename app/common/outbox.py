from __future__ import annotations

import logging
from typing import Any

from app.common.kafka import publish_json
from app.common.repository import Repository

logger = logging.getLogger(__name__)


async def publish_pending_outbox(repo: Repository, producer: Any, *, limit: int = 100) -> int:
    published = 0
    for event in await repo.get_pending_outbox(limit):
        try:
            await publish_json(
                producer,
                event["topic"],
                event["payload"],
                key=event["message_key"],
            )
            await repo.mark_outbox_published(event["id"])
            published += 1
        except Exception as exc:
            logger.exception("Outbox event publishing failed", extra={"outbox_id": event["id"]})
            try:
                await repo.mark_outbox_failed(event["id"], str(exc))
            except Exception:
                logger.exception("Cannot persist outbox failure", extra={"outbox_id": event["id"]})
            break
    return published
