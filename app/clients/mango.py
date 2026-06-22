from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from dateutil import parser as dt_parser
from zoneinfo import ZoneInfo

from app.common.config import Settings
from app.common.models import CallRecord

logger = logging.getLogger(__name__)

_HTTP_RE = re.compile(r"https?://[^\s,;\]]+")
_AUDIO_CONTENT_TYPES = {
    "application/ogg",
}


class MangoApiError(RuntimeError):
    pass


class MangoClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.mango_api_base_url.rstrip("/")
        self.api_key = settings.mango_api_key.get_secret_value()
        self.api_salt = settings.mango_api_salt.get_secret_value()
        self.tz = ZoneInfo(settings.mango_default_timezone)
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=90.0), follow_redirects=True)

    async def aclose(self) -> None:
        await self.http.aclose()

    def sign(self, payload: dict[str, Any] | str) -> tuple[str, str]:
        json_payload = payload if isinstance(payload, str) else self._json_dumps(payload)
        digest = hashlib.sha256(f"{self.api_key}{json_payload}{self.api_salt}".encode("utf-8")).hexdigest()
        return json_payload, digest

    @staticmethod
    def _json_dumps(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    async def request(self, endpoint: str, payload: dict[str, Any]) -> Any:
        if not self.api_key or not self.api_salt:
            raise MangoApiError("MANGO_API_KEY/MANGO_API_SALT are required")
        json_payload, sign = self.sign(payload)
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = await self.http.post(
            url,
            data={"vpbx_api_key": self.api_key, "sign": sign, "json": json_payload},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return response.json()
        text = response.text.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    async def fetch_calls(self, date_from: datetime, date_to: datetime) -> list[CallRecord]:
        fields = self.settings.mango_fields_list
        request_id = str(uuid4())
        request_payload = {
            "date_from": int(date_from.timestamp()),
            "date_to": int(date_to.timestamp()),
            "fields": ",".join(fields),
            "request_id": request_id,
        }
        initial = await self.request(self.settings.mango_stats_request_endpoint, request_payload)
        key = self._extract_key(initial)
        if not key:
            raise MangoApiError(f"Mango stats/request did not return key: {initial!r}")

        result_payload = {"key": key, "request_id": request_id}
        result: Any = None
        for attempt in range(1, self.settings.mango_result_poll_attempts + 1):
            result = await self.request(self.settings.mango_stats_result_endpoint, result_payload)
            if self._is_result_ready(result):
                break
            logger.info("Mango stats result is not ready", extra={"attempt": attempt, "result": result})
            await asyncio.sleep(self.settings.mango_result_poll_interval_seconds)
        else:
            raise MangoApiError(f"Mango stats/result is not ready after polling: {result!r}")

        rows = self._parse_stats_result(result, fields)
        return [self._row_to_call(row) for row in rows]

    def _extract_key(self, value: Any) -> str | None:
        if isinstance(value, dict):
            nested = value.get("result")
            return value.get("key") or (nested.get("key") if isinstance(nested, dict) else None)
        return getattr(value, "key", None)

    def _is_result_ready(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, dict):
            code = str(value.get("code", ""))
            # Mango often returns numeric code while report is being prepared.
            if code and code not in {"0", "200"}:
                return False
            if "data" in value or "result" in value:
                return True
            return "rows" in value or "csv" in value
        if isinstance(value, str):
            return bool(value.strip())
        return True

    def _parse_stats_result(self, result: Any, fields: list[str]) -> list[dict[str, Any]]:
        if isinstance(result, dict):
            payload = result
            for key in ("data", "result", "rows"):
                if key in result:
                    payload = result[key]
                    break
            if isinstance(payload, list):
                return [dict(row) for row in payload]
            if isinstance(payload, str):
                return self._parse_csv(payload, fields)
            if isinstance(payload, dict) and "csv" in payload:
                return self._parse_csv(str(payload["csv"]), fields)
        if isinstance(result, str):
            return self._parse_csv(result, fields)
        raise MangoApiError(f"Unsupported Mango stats/result format: {type(result)!r}")

    @staticmethod
    def _parse_csv(payload: str, fields: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        reader = csv.reader(io.StringIO(payload.strip()), delimiter=";")
        for values in reader:
            if not values:
                continue
            cleaned = [value.strip().strip("[]") for value in values]
            if cleaned == fields:
                continue
            row = {field: cleaned[idx] if idx < len(cleaned) else None for idx, field in enumerate(fields)}
            rows.append(row)
        return rows

    def _row_to_call(self, row: dict[str, Any]) -> CallRecord:
        raw = {k: v for k, v in row.items() if v not in (None, "")}
        entry_id = self._first(row, "entry_id", "call_entry_id")
        call_id = self._first(row, "call_id", "id")
        recording_id = self._extract_recording_id(row)
        recording_url = self._extract_recording_url(row)
        started_at = self._parse_mango_datetime(self._first(row, "start", "create_time", "started_at"))
        finished_at = self._parse_mango_datetime(self._first(row, "finish", "end_time", "finished_at"))
        generated_id = call_id or entry_id or recording_id or self._stable_fallback_id(row)
        direction = self._first(row, "call_direction", "direction")
        if not direction:
            if self._first(row, "from_extension"):
                direction = "outgoing"
            elif self._first(row, "to_extension"):
                direction = "incoming"
        return CallRecord(
            id=str(generated_id),
            entry_id=entry_id,
            call_id=call_id,
            recording_id=recording_id,
            recording_url=recording_url,
            direction=direction,
            from_number=self._first(row, "from_number", "from.number", "from"),
            to_number=self._first(row, "to_number", "to.number", "to"),
            started_at=started_at,
            finished_at=finished_at,
            disconnect_reason=self._first(row, "disconnect_reason"),
            raw=raw,
        )

    @staticmethod
    def _first(row: dict[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = row.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _extract_recording_url(self, row: dict[str, Any]) -> str | None:
        direct = self._first(row, "recording_url", "record_url", "url")
        if direct:
            return direct
        records = self._first(row, "records", "recording", "record")
        if not records:
            return None
        match = _HTTP_RE.search(records)
        return match.group(0) if match else None

    def _extract_recording_id(self, row: dict[str, Any]) -> str | None:
        direct = self._first(row, "recording_id", "record_id")
        if direct:
            return direct
        records = self._first(row, "records", "recording", "record")
        if not records:
            return None
        if _HTTP_RE.search(records):
            return None
        token = re.split(r"[,;\s]+", records.strip())[0]
        return token or None

    def _parse_mango_datetime(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            if value.isdigit():
                number = int(value)
                if number > 10_000_000_000:
                    number = number // 1000
                return datetime.fromtimestamp(number, tz=timezone.utc)
            parsed = dt_parser.parse(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=self.tz)
            return parsed.astimezone(timezone.utc)
        except (ValueError, OverflowError) as exc:
            logger.warning("Cannot parse Mango datetime", extra={"value": value, "error": str(exc)})
            return None

    @staticmethod
    def _stable_fallback_id(row: dict[str, Any]) -> str:
        digest = hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
        return digest[:32]

    async def download_recording(self, *, recording_url: str | None, recording_id: str | None) -> tuple[bytes, str]:
        if recording_url:
            response = await self.http.get(recording_url)
            response.raise_for_status()
            filename = self._filename_from_response(response, fallback=f"{recording_id or 'recording'}.mp3")
            return self._audio_content(response), filename

        if not recording_id:
            raise MangoApiError("No recording_url or recording_id supplied")
        endpoint = self.settings.mango_recording_download_endpoint.format(recording_id=recording_id)
        result = await self.request(endpoint, {"recording_id": recording_id})
        if isinstance(result, str) and result.startswith("http"):
            response = await self.http.get(result)
            response.raise_for_status()
            filename = self._filename_from_response(response, fallback=f"{recording_id}.mp3")
            return self._audio_content(response), filename
        if isinstance(result, dict):
            url = result.get("recording_url") or result.get("url") or result.get("download_url")
            if url:
                response = await self.http.get(str(url))
                response.raise_for_status()
                filename = self._filename_from_response(response, fallback=f"{recording_id}.mp3")
                return self._audio_content(response), filename
        raise MangoApiError(f"Cannot resolve Mango recording download URL for {recording_id}: {result!r}")

    @staticmethod
    def _audio_content(response: httpx.Response) -> bytes:
        content = response.content
        if not content:
            raise MangoApiError("Mango recording response is empty")

        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        prefix = content[:64].lstrip().lower()
        if content_type.startswith("text/") or content_type in {"application/json", "application/xml"}:
            raise MangoApiError(f"Mango recording returned non-audio Content-Type: {content_type}")
        if prefix.startswith((b"<!doctype", b"<html", b"<?xml", b"{")):
            raise MangoApiError("Mango recording returned a document instead of audio")

        has_audio_signature = (
            content.startswith((b"ID3", b"OggS", b"fLaC", b"\x1aE\xdf\xa3"))
            or (len(content) >= 12 and content.startswith(b"RIFF") and content[8:12] == b"WAVE")
            or (len(content) >= 8 and content[4:8] == b"ftyp")
            or (len(content) >= 2 and content[0] == 0xFF and content[1] & 0xE0 == 0xE0)
        )
        declared_audio = content_type.startswith(("audio/", "video/")) or content_type in _AUDIO_CONTENT_TYPES
        if not declared_audio and not has_audio_signature:
            raise MangoApiError(f"Mango recording has unsupported Content-Type: {content_type or 'missing'}")
        return content

    @staticmethod
    def _filename_from_response(response: httpx.Response, *, fallback: str) -> str:
        disposition = response.headers.get("content-disposition", "")
        match = re.search(r'filename="?([^";]+)"?', disposition)
        return match.group(1) if match else fallback
