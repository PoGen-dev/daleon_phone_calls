from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.common import outbox


@pytest.mark.asyncio
async def test_outbox_publishes_in_order(settings, monkeypatch) -> None:
    repo = SimpleNamespace(
        get_pending_outbox=AsyncMock(
            return_value=[
                {"id": 1, "topic": "raw", "message_key": "c1", "payload": {"n": 1}},
                {"id": 2, "topic": "next", "message_key": "c1", "payload": {"n": 2}},
            ]
        ),
        mark_outbox_published=AsyncMock(),
        mark_outbox_failed=AsyncMock(),
    )
    publish = AsyncMock()
    monkeypatch.setattr(outbox, "publish_json", publish)
    assert await outbox.publish_pending_outbox(repo, object(), limit=10) == 2
    assert [item.args[1] for item in publish.await_args_list] == ["raw", "next"]
    assert [item.args[0] for item in repo.mark_outbox_published.await_args_list] == [1, 2]
    repo.mark_outbox_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_outbox_stops_after_first_failure(monkeypatch) -> None:
    repo = SimpleNamespace(
        get_pending_outbox=AsyncMock(
            return_value=[
                {"id": 1, "topic": "raw", "message_key": "c1", "payload": {}},
                {"id": 2, "topic": "next", "message_key": "c1", "payload": {}},
            ]
        ),
        mark_outbox_published=AsyncMock(),
        mark_outbox_failed=AsyncMock(),
    )
    monkeypatch.setattr(outbox, "publish_json", AsyncMock(side_effect=RuntimeError("kafka down")))
    assert await outbox.publish_pending_outbox(repo, object()) == 0
    repo.mark_outbox_failed.assert_awaited_once_with(1, "kafka down")
    repo.mark_outbox_published.assert_not_awaited()
