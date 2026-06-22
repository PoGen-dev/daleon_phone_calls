from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.common.models import CallRecord, QualityResult
from app.common.repository import Repository, _json, _to_primitive


class Context:
    def __init__(self, value) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class Connection:
    def __init__(self) -> None:
        self.execute = AsyncMock()
        self.fetchrow = AsyncMock()
        self.fetchval = AsyncMock()

    def transaction(self):
        return Context(self)


class Pool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return Context(self.conn)


def quality() -> QualityResult:
    return QualityResult.model_validate(
        {
            "score": 65,
            "risk_level": "critical",
            "risk_reason": "Нет следующего шага",
            "summary": "Обсудили ремонт",
            "errors": ["Не отработано возражение"],
            "recommendation": "Перезвонить",
            "criteria": {
                "greeting": 80,
                "needs_discovery": 75,
                "urgency": 70,
                "target_action": 75,
                "objection_handling": 40,
                "closing": 40,
            },
        }
    )


def test_primitive_conversion_and_json() -> None:
    value = {
        "amount": Decimal("1.5"),
        "at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "nested": [{"x": Decimal("2")}],
    }
    result = _to_primitive(value)
    assert result == {
        "amount": 1.5,
        "at": "2026-01-01T00:00:00+00:00",
        "nested": [{"x": 2.0}],
    }
    assert _json({"ok": True}) == '{"ok":true}'


@pytest.mark.asyncio
async def test_save_and_read_call() -> None:
    conn = Connection()
    repo = Repository(Pool(conn))
    call = CallRecord(id="c1", recording_id="r1", raw={"source": "mango"})
    await repo.save_call(
        call,
        status="recorded",
        audio_bucket="bucket",
        audio_object_name="c1/a.mp3",
        audio_filename="a.mp3",
    )
    args = conn.execute.await_args.args
    assert args[1] == "c1" and args[-3:] == ("bucket", "c1/a.mp3", "a.mp3")
    await repo.mark_call_status("c1", "failed", "bad")
    assert conn.execute.await_args.args[1:] == ("c1", "failed", "bad")

    conn.fetchrow.side_effect = [
        {"id": "c1", "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        None,
        {"id": "c1", "duration_seconds": Decimal("2.5")},
        None,
    ]
    assert (await repo.get_call("c1"))["created_at"].startswith("2026")
    assert await repo.get_call("missing") is None
    assert (await repo.get_call_with_results("c1"))["duration_seconds"] == 2.5
    assert await repo.get_call_with_results("missing") is None


@pytest.mark.asyncio
async def test_transcription_operations() -> None:
    conn = Connection()
    repo = Repository(Pool(conn))
    await repo.save_transcription(call_id="c1", transcript="text", model="m", raw={"x": 1}, duration_seconds=3.5)
    assert conn.execute.await_count == 2
    insert_args = conn.execute.await_args_list[0].args
    assert insert_args[1:4] == ("c1", "text", "m")

    conn.fetchval.side_effect = [1, "transcript"]
    assert await repo.transcription_exists("c1")
    assert await repo.get_transcription("c1") == "transcript"


@pytest.mark.asyncio
async def test_quality_and_notification_operations() -> None:
    conn = Connection()
    repo = Repository(Pool(conn))
    result = quality()
    await repo.save_quality(call_id="c1", quality=result, model="mini")
    assert conn.execute.await_count == 2
    insert_args = conn.execute.await_args_list[0].args
    assert insert_args[1:6] == (
        "c1",
        65,
        "critical",
        "Нет следующего шага",
        "Обсудили ремонт",
    )

    conn.fetchval.side_effect = [True, False]
    assert await repo.quality_exists("c1")
    assert not await repo.notification_exists("e1")
    await repo.save_notification("e1", "c1", "main")
    assert conn.execute.await_count == 4
    await repo.save_notification("e2", None, "error")
    assert conn.execute.await_count == 5


@pytest.mark.asyncio
async def test_worker_state_operations() -> None:
    conn = Connection()
    repo = Repository(Pool(conn))
    conn.fetchval.side_effect = [{"cursor": "now"}, None]
    assert await repo.get_state("worker") == {"cursor": "now"}
    assert await repo.get_state("missing") is None
    await repo.set_state("worker", {"cursor": "later"})
    assert conn.execute.await_args.args[1] == "worker"
