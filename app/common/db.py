from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from app.common.config import Settings


@asynccontextmanager
async def postgres_pool(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    try:
        yield pool
    finally:
        await pool.close()
