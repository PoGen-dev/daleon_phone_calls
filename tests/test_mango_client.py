from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.clients.mango import MangoApiError, MangoClient


class Response:
    def __init__(self, *, data=None, text="", content=b"audio", headers=None) -> None:
        self._data = data
        self.text = text
        self.content = content
        self.headers = headers or {}
        self.raise_for_status = lambda: None

    def json(self):
        return self._data


def test_signature_and_json_are_stable(settings) -> None:
    client = MangoClient(settings)
    payload = {"date_from": 1, "message": "тест"}
    json_payload, signature = client.sign(payload)
    expected = hashlib.sha256(f"mango-key{json_payload}mango-salt".encode()).hexdigest()
    assert signature == expected
    assert " " not in json_payload and "тест" in json_payload
    assert client.sign(json_payload)[0] == json_payload


@pytest.mark.asyncio
async def test_request_handles_json_text_empty_and_missing_credentials(settings) -> None:
    client = MangoClient(settings)
    await client.http.aclose()
    client.http = SimpleNamespace(
        post=AsyncMock(
            side_effect=[
                Response(data={"key": "1"}, headers={"content-type": "application/json"}),
                Response(text='{"value": 2}'),
                Response(text="plain"),
                Response(text=""),
            ]
        )
    )
    assert await client.request("/stats", {}) == {"key": "1"}
    assert await client.request("stats", {}) == {"value": 2}
    assert await client.request("stats", {}) == "plain"
    assert await client.request("stats", {}) is None
    request = client.http.post.await_args_list[0]
    assert request.args[0].endswith("/stats") and request.kwargs["data"]["vpbx_api_key"] == "mango-key"

    client.api_key = ""
    with pytest.raises(MangoApiError, match="required"):
        await client.request("stats", {})


@pytest.mark.asyncio
async def test_fetch_calls_polls_until_ready(settings, monkeypatch) -> None:
    settings.mango_stats_fields = "call_id,records,start,finish"
    client = MangoClient(settings)
    client.request = AsyncMock(
        side_effect=[
            {"result": {"key": "report-key"}},
            {"code": 202},
            {"data": [{"call_id": "c1", "records": "r1", "start": "1710000000"}]},
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr("app.clients.mango.asyncio.sleep", sleep)
    calls = await client.fetch_calls(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assert calls[0].id == "c1" and calls[0].recording_id == "r1"
    sleep.assert_awaited_once()
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_calls_rejects_missing_key_and_poll_timeout(settings, monkeypatch) -> None:
    client = MangoClient(settings)
    client.request = AsyncMock(return_value={})
    with pytest.raises(MangoApiError, match="did not return key"):
        await client.fetch_calls(datetime.now(timezone.utc), datetime.now(timezone.utc))

    settings.mango_result_poll_attempts = 2
    client.request = AsyncMock(side_effect=[{"key": "k"}, None, {"code": 202}])
    monkeypatch.setattr("app.clients.mango.asyncio.sleep", AsyncMock())
    with pytest.raises(MangoApiError, match="not ready"):
        await client.fetch_calls(datetime.now(timezone.utc), datetime.now(timezone.utc))
    await client.aclose()


def test_result_parsing_and_readiness(settings) -> None:
    client = MangoClient(settings)
    fields = ["records", "start"]
    csv = "records;start\n[rec-1];1710000000\n"
    assert client._parse_stats_result(csv, fields)[0]["records"] == "rec-1"
    assert client._parse_stats_result({"data": csv}, fields)[0]["start"] == "1710000000"
    assert client._parse_stats_result({"result": {"csv": csv}}, fields)[0]["records"] == "rec-1"
    assert client._parse_stats_result({"rows": [{"records": "r"}]}, fields) == [{"records": "r"}]
    with pytest.raises(MangoApiError, match="Unsupported"):
        client._parse_stats_result(42, fields)
    assert not client._is_result_ready(None)
    assert not client._is_result_ready({"code": 202})
    assert client._is_result_ready({"code": 200, "data": []})
    assert client._is_result_ready({})
    assert not client._is_result_ready(" ")
    assert client._is_result_ready("csv") and client._is_result_ready([])
    assert client._extract_key(SimpleNamespace(key="x")) == "x"
    assert client._extract_key({"result": "not-a-dict"}) is None


def test_row_mapping_urls_dates_and_fallback(settings) -> None:
    client = MangoClient(settings)
    row = {
        "entry_id": "e1",
        "records": "[https://example.test/audio.mp3]",
        "start": "2026-05-07 18:00:00",
        "finish": "1809703880000",
        "from_number": "1",
        "to_number": "2",
        "direction": "incoming",
        "disconnect_reason": "normal",
        "empty": "",
    }
    call = client._row_to_call(row)
    assert call.id == "e1" and call.recording_url.startswith("https://")
    assert call.recording_id is None and call.raw.get("empty") is None
    assert call.started_at.tzinfo == timezone.utc and call.finished_at.tzinfo == timezone.utc
    assert client._extract_recording_url({"record_url": "https://direct"}) == "https://direct"
    assert client._extract_recording_url({}) is None
    assert client._extract_recording_id({"record_id": "r"}) == "r"
    assert client._extract_recording_id({}) is None
    assert client._extract_recording_id({"records": "r1, r2"}) == "r1"
    assert client._parse_mango_datetime(None) is None
    assert client._parse_mango_datetime("bad date") is None
    fallback = client._row_to_call({"from_number": "1"})
    assert len(fallback.id) == 32 and fallback.id == client._stable_fallback_id({"from_number": "1"})


@pytest.mark.asyncio
async def test_download_recording_all_supported_responses(settings) -> None:
    client = MangoClient(settings)
    await client.http.aclose()
    client.http = SimpleNamespace(
        get=AsyncMock(
            side_effect=[
                Response(content=b"one", headers={"content-disposition": 'attachment; filename="call.wav"'}),
                Response(content=b"two"),
                Response(content=b"three"),
            ]
        )
    )
    assert await client.download_recording(recording_url="https://a", recording_id=None) == (b"one", "call.wav")
    client.request = AsyncMock(side_effect=["https://b", {"download_url": "https://c"}, {}])
    assert await client.download_recording(recording_url=None, recording_id="r2") == (b"two", "r2.mp3")
    assert await client.download_recording(recording_url=None, recording_id="r3") == (b"three", "r3.mp3")
    with pytest.raises(MangoApiError, match="Cannot resolve"):
        await client.download_recording(recording_url=None, recording_id="r4")
    with pytest.raises(MangoApiError, match="No recording"):
        await client.download_recording(recording_url=None, recording_id=None)
