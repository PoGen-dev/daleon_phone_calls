from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException

from app.clients.minio import MinioStorage
from app.common.config import Settings, get_settings
from app.common.logging import configure_logging
from app.common.repository import Repository

settings = get_settings()
configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pg = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    app.state.pg = pg
    app.state.repo = Repository(pg)
    app.state.storage = MinioStorage(settings)
    try:
        yield
    finally:
        await pg.close()


app = FastAPI(title="Mango Transcribe Analysis", version="2.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    repo: Repository = app.state.repo
    async with repo.pg.acquire() as conn:
        pg_ok = await conn.fetchval("SELECT 1")
    minio_ok = await app.state.storage.is_available()
    return {
        "status": "ok" if pg_ok == 1 and minio_ok else "degraded",
        "postgres": pg_ok == 1,
        "minio": minio_ok,
    }


@app.get("/calls/{call_id}")
async def get_call(call_id: str) -> dict[str, Any]:
    data = await app.state.repo.get_call_with_results(call_id)
    if not data:
        raise HTTPException(status_code=404, detail="call not found")
    return data


@app.get("/settings/topics")
async def topics() -> dict[str, str]:
    s: Settings = settings
    return {
        "mango_raw": s.topic_mango_raw,
        "to_transcribe": s.topic_to_transcribe,
        "to_analyze": s.topic_to_analyze,
        "to_notify": s.topic_to_notify,
        "dead_letter": s.topic_dead_letter,
    }


if __name__ == "__main__":
    uvicorn.run("app.services.api:app", host=settings.api_host, port=settings.api_port, reload=False)
