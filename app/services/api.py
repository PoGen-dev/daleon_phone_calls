from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient

from app.common.config import Settings, get_settings
from app.common.logging import configure_logging
from app.common.repository import Repository

logger = logging.getLogger(__name__)
settings = get_settings()
configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pg = await asyncpg.create_pool(dsn=settings.postgres_dsn, min_size=1, max_size=10)
    mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(settings.mongo_dsn)
    app.state.pg = pg
    app.state.mongo_client = mongo_client
    app.state.mongo = mongo_client[settings.mongo_database]
    app.state.repo = Repository(pg, app.state.mongo)
    try:
        yield
    finally:
        await pg.close()
        mongo_client.close()


app = FastAPI(title="Mango Calls QA", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    repo: Repository = app.state.repo
    async with repo.pg.acquire() as conn:
        pg_ok = await conn.fetchval("SELECT 1")
    mongo_ok = await app.state.mongo.command("ping")
    return {"status": "ok", "postgres": pg_ok == 1, "mongo": bool(mongo_ok.get("ok"))}


@app.get("/calls/{call_id}")
async def get_call(call_id: str) -> dict[str, Any]:
    repo: Repository = app.state.repo
    data = await repo.get_call_with_results(call_id)
    if not data:
        raise HTTPException(status_code=404, detail="call not found")
    return data


@app.get("/settings/topics")
async def topics() -> dict[str, str]:
    s: Settings = settings
    return {
        "mango_raw": s.topic_mango_raw,
        "to_transcribe": s.topic_to_transcribe,
        "transcribed": s.topic_transcribed,
        "quality": s.topic_quality,
        "dead_letter": s.topic_dead_letter,
    }


if __name__ == "__main__":
    uvicorn.run("app.services.api:app", host=settings.api_host, port=settings.api_port, reload=False)
