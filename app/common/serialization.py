from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import orjson


def _default(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    raise TypeError


def json_dumps_bytes(payload: Any) -> bytes:
    return orjson.dumps(payload, default=_default)


def json_loads_bytes(payload: bytes | bytearray | str) -> Any:
    return orjson.loads(payload)
