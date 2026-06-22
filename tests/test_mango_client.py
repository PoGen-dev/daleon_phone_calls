from __future__ import annotations

import hashlib

from pydantic import SecretStr

from app.clients.mango import MangoClient
from app.common.config import Settings


def test_mango_signature_matches_sha256_api_key_json_salt() -> None:
    settings = Settings(mango_api_key=SecretStr("key"), mango_api_salt=SecretStr("salt"))
    client = MangoClient(settings)
    payload = {"date_from": 1, "date_to": 2, "fields": "records,start"}
    json_payload, sign = client.sign(payload)
    expected = hashlib.sha256(f"key{json_payload}salt".encode("utf-8")).hexdigest()
    assert sign == expected


def test_parse_csv_uses_configured_fields() -> None:
    fields = ["records", "start", "finish", "from_number", "to_number"]
    payload = "rec-1;1710000000;1710000300;79990000000;78000000000\n"
    rows = MangoClient._parse_csv(payload, fields)
    assert rows == [
        {
            "records": "rec-1",
            "start": "1710000000",
            "finish": "1710000300",
            "from_number": "79990000000",
            "to_number": "78000000000",
        }
    ]
