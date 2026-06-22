from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.common.logging import configure_logging
from app.services import api


class Context:
    def __init__(self, value) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


@pytest.mark.asyncio
async def test_health_and_topics(monkeypatch) -> None:
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=1))
    repo = SimpleNamespace(pg=SimpleNamespace(acquire=lambda: Context(conn)))
    storage = SimpleNamespace(is_available=AsyncMock(return_value=True))
    monkeypatch.setattr(api.app, "state", SimpleNamespace(repo=repo, storage=storage))
    assert await api.health() == {"status": "ok", "postgres": True, "minio": True}
    result = await api.topics()
    assert result["to_transcribe"] == "calls.to_transcribe"
    assert result["to_analyze"] == "calls.to_analyze"
    assert result["to_notify"] == "calls.to_notify"


@pytest.mark.asyncio
async def test_degraded_health_and_call_endpoint(monkeypatch) -> None:
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=0))
    repo = SimpleNamespace(
        pg=SimpleNamespace(acquire=lambda: Context(conn)),
        get_call_with_results=AsyncMock(side_effect=[{"id": "c1"}, None]),
    )
    storage = SimpleNamespace(is_available=AsyncMock(return_value=False))
    monkeypatch.setattr(api.app, "state", SimpleNamespace(repo=repo, storage=storage))
    assert (await api.health())["status"] == "degraded"
    assert await api.get_call("c1") == {"id": "c1"}
    with pytest.raises(HTTPException) as exc:
        await api.get_call("missing")
    assert exc.value.status_code == 404


def test_logging_configuration_accepts_unknown_level() -> None:
    configure_logging("NOT_A_LEVEL")
    assert logging.getLogger().level in {logging.INFO, logging.WARNING}


@pytest.mark.asyncio
async def test_api_lifespan_opens_and_closes_resources(monkeypatch) -> None:
    pool = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(api.asyncpg, "create_pool", AsyncMock(return_value=pool))
    storage = object()
    monkeypatch.setattr(api, "MinioStorage", lambda _settings: storage)
    fake_app = SimpleNamespace(state=SimpleNamespace())
    async with api.lifespan(fake_app):
        assert fake_app.state.pg is pool
        assert fake_app.state.storage is storage
        assert fake_app.state.repo.pg is pool
    pool.close.assert_awaited_once()
