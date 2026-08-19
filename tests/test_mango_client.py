from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.clients.mango import MangoApiError, MangoClient


class Response:
    def __init__(
        self, *, data=None, text="", content=b"audio", headers=None, status_code=200
    ) -> None:
        self._data = data
        self.text = text
        self.content = content
        self.headers = headers or {}
        self.status_code = status_code
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
async def test_request_handles_json_text_empty_and_missing_credentials(
    settings,
) -> None:
    client = MangoClient(settings)
    await client.http.aclose()
    client.http = SimpleNamespace(
        post=AsyncMock(
            side_effect=[
                Response(
                    data={"key": "1"}, headers={"content-type": "application/json"}
                ),
                Response(text='{"value": 2}'),
                Response(text="plain"),
                Response(text="", content=b""),
                Response(text="", content=b"", status_code=204),
            ]
        )
    )
    assert await client.request("/stats", {}) == {"key": "1"}
    assert await client.request("stats", {}) == {"value": 2}
    assert await client.request("stats", {}) == "plain"
    assert await client.request("stats", {}) is None
    assert await client.request_with_status("stats", {}) == (204, None)
    request = client.http.post.await_args_list[0]
    assert (
        request.args[0].endswith("/stats")
        and request.kwargs["data"]["vpbx_api_key"] == "mango-key"
    )

    client.api_key = ""
    with pytest.raises(MangoApiError, match="required"):
        await client.request("stats", {})


@pytest.mark.asyncio
async def test_request_retries_mango_rate_limit(settings, monkeypatch) -> None:
    settings.retry_backoff_seconds = 2
    client = MangoClient(settings)
    await client.http.aclose()
    client.http = SimpleNamespace(
        post=AsyncMock(
            side_effect=[
                Response(status_code=429, headers={"retry-after": "3"}),
                Response(
                    data={"ok": True}, headers={"content-type": "application/json"}
                ),
            ]
        )
    )
    sleep = AsyncMock()
    monkeypatch.setattr("app.clients.mango.asyncio.sleep", sleep)

    assert await client.request("stats", {}) == {"ok": True}

    assert client.http.post.await_count == 2
    sleep.assert_awaited_once_with(3.0)


@pytest.mark.asyncio
async def test_fetch_calls_polls_until_ready(settings, monkeypatch) -> None:
    settings.mango_stats_fields = "call_id,records,start,finish"
    client = MangoClient(settings)
    initial = {"result": {"key": "report-key", "expires": 1710000300}}
    client.request = AsyncMock(return_value=initial)
    client.request_with_status = AsyncMock(
        side_effect=[
            (204, None),
            (
                200,
                {"data": [{"call_id": "c1", "records": "r1", "start": "1710000000"}]},
            ),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr("app.clients.mango.asyncio.sleep", sleep)
    calls = await client.fetch_calls(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assert calls[0].id == "c1" and calls[0].recording_id == "r1"
    request_payload = client.request.await_args_list[0].args[1]
    result_payload = client.request_with_status.await_args_list[0].args[1]
    assert "request_id" not in request_payload
    assert request_payload["date_from"] == "1767225600"
    assert request_payload["date_to"] == "1767312000"
    assert request_payload["from"] == {"extension": "", "number": ""}
    assert request_payload["to"] == {"extension": "", "number": ""}
    assert result_payload == initial["result"]
    sleep.assert_awaited_once()
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_calls_rejects_missing_key_and_poll_timeout(
    settings, monkeypatch
) -> None:
    client = MangoClient(settings)
    client.request = AsyncMock(return_value={})
    with pytest.raises(MangoApiError, match="did not return key"):
        await client.fetch_calls(datetime.now(timezone.utc), datetime.now(timezone.utc))

    settings.mango_result_poll_attempts = 2
    client.request = AsyncMock(return_value={"key": "k"})
    client.request_with_status = AsyncMock(side_effect=[(204, None), (202, None)])
    monkeypatch.setattr("app.clients.mango.asyncio.sleep", AsyncMock())
    with pytest.raises(MangoApiError, match="not ready"):
        await client.fetch_calls(datetime.now(timezone.utc), datetime.now(timezone.utc))
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_calls_treats_empty_200_as_ready_report(settings) -> None:
    client = MangoClient(settings)
    client.request = AsyncMock(return_value={"key": "empty-report"})
    client.request_with_status = AsyncMock(return_value=(200, None))

    calls = await client.fetch_calls(
        datetime.now(timezone.utc), datetime.now(timezone.utc)
    )

    assert calls == []
    client.request_with_status.assert_awaited_once()
    await client.aclose()


def test_result_parsing_and_readiness(settings) -> None:
    client = MangoClient(settings)
    fields = ["records", "start"]
    csv = "records;start\n[rec-1];1710000000\n"
    assert client._parse_stats_result(csv, fields)[0]["records"] == "rec-1"
    assert client._parse_stats_result({"data": csv}, fields)[0]["start"] == "1710000000"
    assert (
        client._parse_stats_result({"result": {"csv": csv}}, fields)[0]["records"]
        == "rec-1"
    )
    assert client._parse_stats_result({"rows": [{"records": "r"}]}, fields) == [
        {"records": "r"}
    ]
    with pytest.raises(MangoApiError, match="Unsupported"):
        client._parse_stats_result(42, fields)
    assert not client._is_result_ready(None)
    assert not client._is_result_ready({"code": 202})
    assert client._is_result_ready({"code": 200, "data": []})
    assert not client._is_result_ready({})
    assert not client._is_result_ready(" ")
    assert client._is_result_ready("csv") and client._is_result_ready([])
    assert client._extract_key(SimpleNamespace(key="x")) == "x"
    assert client._extract_key({"result": "not-a-dict"}) is None
    assert client._stats_result_payload({"key": "k", "expires": 1}, "k") == {
        "key": "k",
        "expires": 1,
    }
    assert client._stats_result_payload(
        {"result": {"key": "k", "expires": 1}}, "k"
    ) == {
        "key": "k",
        "expires": 1,
    }
    assert client._stats_result_payload("unexpected", "k") == {"key": "k"}
    assert client._parse_stats_result({"data": []}, fields) == []


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
    assert (
        call.started_at.tzinfo == timezone.utc
        and call.finished_at.tzinfo == timezone.utc
    )
    assert (
        client._extract_recording_url({"record_url": "https://direct"})
        == "https://direct"
    )
    assert client._extract_recording_url({}) is None
    assert client._extract_recording_id({"record_id": "r"}) == "r"
    assert client._extract_recording_id({}) is None
    assert client._extract_recording_id({"records": "r1, r2"}) == "r1"
    assert client._parse_mango_datetime(None) is None
    assert client._parse_mango_datetime("bad date") is None
    fallback = client._row_to_call({"from_number": "1"})
    assert len(fallback.id) == 32 and fallback.id == client._stable_fallback_id(
        {"from_number": "1"}
    )
    assert (
        client._row_to_call({"call_id": "out", "from_extension": "101"}).direction
        == "outgoing"
    )
    assert (
        client._row_to_call({"call_id": "in", "to_extension": "102"}).direction
        == "incoming"
    )


@pytest.mark.asyncio
async def test_download_recording_all_supported_responses(settings) -> None:
    client = MangoClient(settings)
    await client.http.aclose()
    client.http = SimpleNamespace(
        get=AsyncMock(
            side_effect=[
                Response(
                    content=b"RIFF0000WAVEone",
                    headers={"content-disposition": 'attachment; filename="call.wav"'},
                ),
                Response(content=b"OggSthree"),
            ]
        )
    )
    assert await client.download_recording(
        recording_url="https://a", recording_id=None
    ) == (
        b"RIFF0000WAVEone",
        "call.wav",
    )
    client._post = AsyncMock(
        side_effect=[
            Response(content=b"ID3two"),
            Response(
                data={"download_url": "https://c"},
                headers={"content-type": "application/json"},
            ),
            Response(data={}, headers={"content-type": "application/json"}),
        ]
    )
    assert await client.download_recording(recording_url=None, recording_id="r2") == (
        b"ID3two",
        "r2.mp3",
    )
    assert await client.download_recording(recording_url=None, recording_id="r3") == (
        b"OggSthree",
        "r3.mp3",
    )
    endpoint, payload = client._post.await_args_list[0].args
    assert endpoint == "queries/recording/post/"
    assert payload == {"recording_id": "r2", "action": "download"}
    with pytest.raises(MangoApiError, match="Cannot resolve"):
        await client.download_recording(recording_url=None, recording_id="r4")
    with pytest.raises(MangoApiError, match="No recording"):
        await client.download_recording(recording_url=None, recording_id=None)


@pytest.mark.parametrize(
    ("content", "headers", "message"),
    [
        (b"", {}, "empty"),
        (b"<html>player</html>", {"content-type": "text/html"}, "non-audio"),
        (
            b'{"error":"denied"}',
            {"content-type": "application/octet-stream"},
            "document",
        ),
        (b"unknown", {}, "unsupported"),
    ],
)
def test_recording_content_validation(content, headers, message) -> None:
    with pytest.raises(MangoApiError, match=message):
        MangoClient._audio_content(Response(content=content, headers=headers))


def test_recording_content_accepts_declared_audio() -> None:
    response = Response(
        content=b"opaque audio", headers={"content-type": "audio/mpeg; charset=binary"}
    )
    assert MangoClient._audio_content(response) == b"opaque audio"


def test_resolve_mango_user_by_extension_and_sip(settings) -> None:
    client = MangoClient(settings)
    user = {
        "general": {
            "name": "Иван Иванов",
            "user_id": 42,
            "mobile": "79990001122",
            "sips": [{"number": "user2@vpbx400100296.mangosip.ru"}],
        },
        "telephony": {
            "extension": "11",
            "outgoingline": "78127608026",
            "line_id": 100,
            "numbers": [
                {"number": "sip:user2@vpbx400100296.mangosip.ru", "protocol": "sip"},
                {
                    "number": "8127608026",
                    "number_normalized": "78127608026",
                    "protocol": "tel",
                },
            ],
        },
        "groups": [7],
    }
    row = {
        "to_extension": "11",
        "to_number": "sip:user2@vpbx400100296.mangosip.ru",
    }
    assert client.resolve_user(row, [user]) == user
    assert client.resolve_user("sip:user2@vpbx400100296.mangosip.ru", [user]) == user
    summary = client.employee_summary(user)
    assert summary["name"] == "Иван Иванов"
    assert summary["extension"] == "11"
    assert summary["outgoingline"] == "78127608026"
    assert summary["phone_numbers"] == ["78127608026"]


@pytest.mark.asyncio
async def test_fetch_and_find_mango_user(settings) -> None:
    client = MangoClient(settings)
    user = {
        "general": {"name": "Иван"},
        "telephony": {
            "extension": "11",
            "numbers": [{"number": "sip:user2@example.mangosip.ru", "protocol": "sip"}],
        },
    }
    client.request = AsyncMock(return_value={"users": user})
    assert await client.find_user("11") == user
    endpoint, payload = client.request.await_args.args
    assert endpoint == "config/users/request"
    assert payload["extension"] == "11"
    assert "general.sips" in payload["ext_fields"]
    await client.aclose()


@pytest.mark.asyncio
async def test_employee_service_phone_resolves_line_id_to_public_number(
    settings,
) -> None:
    client = MangoClient(settings)
    employee = {
        "general": {
            "name": "Сотрудник 44",
            "sips": [{"number": "user44@vpbx400100296.mangosip.ru"}],
        },
        "telephony": {
            "extension": "44",
            "outgoingline": "sip:user44@vpbx400100296.mangosip.ru",
            "line_id": 300024487,
            "numbers": [
                {
                    "number": "sip:user44@vpbx400100296.mangosip.ru",
                    "protocol": "sip",
                }
            ],
        },
    }
    client.request = AsyncMock(
        return_value={
            "result": 1000,
            "lines": [
                {
                    "line_id": 300024487,
                    "number": "78124253026",
                    "name": None,
                }
            ],
        }
    )

    assert await client.employee_service_phone_candidates(employee) == ["78124253026"]
    endpoint, payload = client.request.await_args.args
    assert endpoint == "incominglines"
    assert payload == {}
    assert (
        client._normalize_public_phone("sip:user44@vpbx400100296.mangosip.ru") is None
    )
    assert client._normalize_public_phone("400100296") is None
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_calls_resolves_sip_even_when_full_enrichment_is_disabled(
    settings,
) -> None:
    settings.mango_enrich_user_metadata = False
    settings.mango_resolve_sip_service_number = True
    client = MangoClient(settings)
    row = {
        "records": "recording-id",
        "start": "1786702560",
        "finish": "1786702654",
        "from_number": "79052209632",
        "to_extension": "44",
        "to_number": "sip:user44@vpbx400100296.mangosip.ru",
        "line_number": "sip:line28247@vpbx400100296.mangosip.ru",
        "entry_id": "sip-call",
    }
    client.request = AsyncMock(
        side_effect=[
            {"key": "report"},
            {
                "users": {
                    "general": {"name": "Сотрудник 44"},
                    "telephony": {
                        "extension": "44",
                        "outgoingline": "sip:user44@vpbx400100296.mangosip.ru",
                        "line_id": 300024487,
                        "numbers": [],
                    },
                }
            },
            {
                "result": 1000,
                "lines": [{"line_id": 300024487, "number": "78124253026"}],
            },
        ]
    )
    client.request_with_status = AsyncMock(return_value=(200, {"data": [row]}))

    calls = await client.fetch_calls(
        datetime(2026, 8, 14, tzinfo=timezone.utc),
        datetime(2026, 8, 15, tzinfo=timezone.utc),
    )

    assert len(calls) == 1
    raw = calls[0].raw
    assert raw["mango_employee_summary"]["extension"] == "44"
    assert raw["mango_employee_service_phone_candidates"] == ["78124253026"]
    assert "mango_employee" not in raw
    assert [call.args[0] for call in client.request.await_args_list] == [
        "stats/request",
        "config/users/request",
        "incominglines",
    ]
    await client.aclose()
