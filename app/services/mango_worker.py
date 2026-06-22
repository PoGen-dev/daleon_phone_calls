from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.clients.mango import MangoClient
from app.common.config import get_settings
from app.common.db import mongo_db, postgres_pool
from app.common.kafka import kafka_producer, publish_json
from app.common.logging import configure_logging
from app.common.models import CallDiscoveredEvent, TranscriptionRequestedEvent
from app.common.repository import Repository

logger = logging.getLogger(__name__)
STATE_NAME = "mango_worker_cursor"


def _parse_cursor(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with postgres_pool(settings) as pg, mongo_db(settings) as mongo, kafka_producer(settings) as producer:
        repo = Repository(pg, mongo)
        mango = MangoClient(settings)
        try:
            while True:
                state = await repo.get_state(STATE_NAME) or {}
                last_cursor = _parse_cursor(state.get("cursor"))
                now = datetime.now(timezone.utc)
                if last_cursor is None:
                    date_from = now - timedelta(seconds=settings.mango_request_window_seconds)
                else:
                    date_from = last_cursor - timedelta(seconds=settings.mango_lookback_seconds)
                date_to = min(date_from + timedelta(seconds=settings.mango_request_window_seconds), now)

                logger.info("Polling Mango calls", extra={"date_from": date_from.isoformat(), "date_to": date_to.isoformat()})
                try:
                    calls = await mango.fetch_calls(date_from, date_to)
                except Exception:
                    logger.exception("Mango polling failed")
                    await asyncio.sleep(settings.mango_poll_interval_seconds)
                    continue

                logger.info("Mango calls fetched", extra={"count": len(calls)})
                max_seen = date_to
                for call in calls:
                    await repo.save_call(call)
                    discovered = CallDiscoveredEvent(call=call)
                    await publish_json(producer, settings.topic_mango_raw, discovered.model_dump(mode="json"), key=call.id)

                    if call.recording_url or call.recording_id:
                        requested = TranscriptionRequestedEvent(
                            call_id=call.id,
                            recording_id=call.recording_id,
                            recording_url=call.recording_url,
                        )
                        await publish_json(producer, settings.topic_to_transcribe, requested.model_dump(mode="json"), key=call.id)
                    else:
                        await repo.mark_call_status(call.id, "no_recording", "No recording_url/recording_id in Mango payload")

                    for candidate in (call.finished_at, call.started_at):
                        if candidate and candidate > max_seen:
                            max_seen = candidate

                await repo.set_state(STATE_NAME, {"cursor": max_seen.isoformat(), "last_poll_at": now.isoformat()})
                await asyncio.sleep(settings.mango_poll_interval_seconds)
        finally:
            await mango.aclose()


if __name__ == "__main__":
    asyncio.run(run())
