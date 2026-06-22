from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from app.common.config import Settings
from app.common.serialization import json_dumps_bytes, json_loads_bytes


async def _configure_connection(conn: asyncpg.Connection) -> None:
    def encoder(value: object) -> str:
        if isinstance(value, str):
            return value
        return json_dumps_bytes(value).decode("utf-8")

    decoder = json_loads_bytes
    await conn.set_type_codec("json", schema="pg_catalog", encoder=encoder, decoder=decoder, format="text")
    await conn.set_type_codec("jsonb", schema="pg_catalog", encoder=encoder, decoder=decoder, format="text")


@asynccontextmanager
async def postgres_pool(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn,
        min_size=1,
        max_size=10,
        init=_configure_connection,
    )
    try:
        yield pool
    finally:
        await pool.close()
