from __future__ import annotations

from contextlib import asynccontextmanager
import csv
from datetime import datetime
import io
import logging
import mimetypes
from typing import Any, Literal

import asyncpg
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.clients.minio import MinioStorage
from app.common.analytics import AnalyticsFilters, AnalyticsRepository
from app.common.config import Settings, get_settings
from app.common.dashboard_auth import DashboardAuthManager
from app.common.db import _configure_connection
from app.common.logging import configure_logging
from app.common.repository import Repository

settings = get_settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    dashboard_auth = DashboardAuthManager.from_settings(settings)
    pg = await asyncpg.create_pool(
        dsn=settings.postgres_dsn,
        min_size=1,
        max_size=10,
        init=_configure_connection,
    )
    app.state.pg = pg
    app.state.repo = Repository(pg)
    app.state.analytics = AnalyticsRepository(pg)
    app.state.storage = MinioStorage(settings)
    app.state.dashboard_auth = dashboard_auth
    try:
        yield
    finally:
        await pg.close()


app = FastAPI(title="Mango Transcribe Analysis", version="2.0.0", lifespan=lifespan)


class DashboardLoginRequest(BaseModel):
    username: str
    password: str


def _dashboard_auth() -> DashboardAuthManager:
    auth = getattr(app.state, "dashboard_auth", None)
    if auth is None:
        # Useful for direct unit tests that do not execute lifespan. Production always
        # initializes this object in lifespan before serving requests.
        auth = DashboardAuthManager.from_settings(settings)
    return auth


async def require_dashboard_admin(request: Request) -> str:
    auth = _dashboard_auth()
    session = auth.verify_session(request.cookies.get(auth.cookie_name))
    if session is None:
        raise HTTPException(status_code=401, detail="authentication required")
    return session.username


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


@app.post("/auth/login")
async def dashboard_login(
    payload: DashboardLoginRequest, request: Request
) -> JSONResponse:
    auth = _dashboard_auth()
    if not auth.authenticate(payload.username, payload.password):
        logger.warning(
            "Dashboard login rejected: ip=%s username=%s",
            _client_ip(request),
            payload.username[:64],
        )
        raise HTTPException(status_code=401, detail="invalid username or password")

    token, session = auth.issue_session()
    response = JSONResponse(
        {
            "authenticated": True,
            "username": session.username,
            "expires_at": session.expires_at,
        }
    )
    response.set_cookie(
        key=auth.cookie_name,
        value=token,
        max_age=auth.ttl_seconds,
        httponly=True,
        secure=settings.dashboard_cookie_secure,
        samesite="strict",
        path="/",
    )
    logger.info(
        "Dashboard login accepted: ip=%s username=%s",
        _client_ip(request),
        session.username,
    )
    return response


@app.get("/auth/me")
async def dashboard_me(admin: str = Depends(require_dashboard_admin)) -> dict[str, Any]:
    return {
        "authenticated": True,
        "username": admin,
        "session_ttl_seconds": settings.dashboard_session_ttl_seconds,
    }


@app.post("/auth/logout")
async def dashboard_logout() -> JSONResponse:
    response = JSONResponse({"authenticated": False})
    response.delete_cookie(_dashboard_auth().cookie_name, path="/")
    return response


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


def _analytics_filters(
    *,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    account: str | None = None,
    manager: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    call_type: str | None = None,
    risk_level: str | None = None,
    score_min: int | None = None,
    score_max: int | None = None,
    duration_min: float | None = None,
    duration_max: float | None = None,
    has_transcription: bool | None = None,
    has_quality: bool | None = None,
    search: str | None = None,
) -> AnalyticsFilters:
    return AnalyticsFilters(
        date_from=date_from,
        date_to=date_to,
        account=account,
        manager=manager,
        direction=direction,
        status=status,
        call_type=call_type,
        risk_level=risk_level,
        score_min=score_min,
        score_max=score_max,
        duration_min=duration_min,
        duration_max=duration_max,
        has_transcription=has_transcription,
        has_quality=has_quality,
        search=search.strip() if search and search.strip() else None,
    )


@app.get("/analytics/filters")
async def analytics_filters(_admin: str = Depends(require_dashboard_admin)) -> dict[str, Any]:
    return await app.state.analytics.filter_options()


@app.get("/analytics/overview")
async def analytics_overview(
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    account: str | None = None,
    manager: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    call_type: str | None = None,
    risk_level: str | None = None,
    score_min: int | None = Query(default=None, ge=0, le=100),
    score_max: int | None = Query(default=None, ge=0, le=100),
    duration_min: float | None = Query(default=None, ge=0),
    duration_max: float | None = Query(default=None, ge=0),
    has_transcription: bool | None = None,
    has_quality: bool | None = None,
    search: str | None = None,
    _admin: str = Depends(require_dashboard_admin),
) -> dict[str, Any]:
    filters = _analytics_filters(
        date_from=date_from,
        date_to=date_to,
        account=account,
        manager=manager,
        direction=direction,
        status=status,
        call_type=call_type,
        risk_level=risk_level,
        score_min=score_min,
        score_max=score_max,
        duration_min=duration_min,
        duration_max=duration_max,
        has_transcription=has_transcription,
        has_quality=has_quality,
        search=search,
    )
    return await app.state.analytics.overview(filters)


@app.get("/analytics/calls")
async def analytics_calls(
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    account: str | None = None,
    manager: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    call_type: str | None = None,
    risk_level: str | None = None,
    score_min: int | None = Query(default=None, ge=0, le=100),
    score_max: int | None = Query(default=None, ge=0, le=100),
    duration_min: float | None = Query(default=None, ge=0),
    duration_max: float | None = Query(default=None, ge=0),
    has_transcription: bool | None = None,
    has_quality: bool | None = None,
    search: str | None = None,
    _admin: str = Depends(require_dashboard_admin),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=10, le=200),
    sort_by: Literal[
        "started_at",
        "finished_at",
        "duration_seconds",
        "score",
        "risk_level",
        "call_type",
        "status",
        "manager",
        "account",
        "created_at",
    ] = "started_at",
    sort_order: Literal["asc", "desc"] = "desc",
) -> dict[str, Any]:
    filters = _analytics_filters(
        date_from=date_from,
        date_to=date_to,
        account=account,
        manager=manager,
        direction=direction,
        status=status,
        call_type=call_type,
        risk_level=risk_level,
        score_min=score_min,
        score_max=score_max,
        duration_min=duration_min,
        duration_max=duration_max,
        has_transcription=has_transcription,
        has_quality=has_quality,
        search=search,
    )
    return await app.state.analytics.list_calls(
        filters,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order,
    )

@app.get("/analytics/export.csv")
async def analytics_export_csv(
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    account: str | None = None,
    manager: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    call_type: str | None = None,
    risk_level: str | None = None,
    score_min: int | None = Query(default=None, ge=0, le=100),
    score_max: int | None = Query(default=None, ge=0, le=100),
    duration_min: float | None = Query(default=None, ge=0),
    duration_max: float | None = Query(default=None, ge=0),
    has_transcription: bool | None = None,
    has_quality: bool | None = None,
    search: str | None = None,
    _admin: str = Depends(require_dashboard_admin),
) -> Response:
    filters = _analytics_filters(
        date_from=date_from,
        date_to=date_to,
        account=account,
        manager=manager,
        direction=direction,
        status=status,
        call_type=call_type,
        risk_level=risk_level,
        score_min=score_min,
        score_max=score_max,
        duration_min=duration_min,
        duration_max=duration_max,
        has_transcription=has_transcription,
        has_quality=has_quality,
        search=search,
    )
    rows = await app.state.analytics.export_calls(filters)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    columns = [
        "id", "started_at", "finished_at", "mango_account", "manager_name",
        "manager_department", "manager_position", "direction", "from_number", "to_number",
        "duration_seconds", "status", "error", "call_type", "classification_confidence",
        "score", "risk_level", "summary", "recommendation", "disconnect_reason",
        "has_transcription", "has_audio", "has_notification",
    ]
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(column) for column in columns])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        content="\ufeff" + buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="calls_{stamp}.csv"'},
    )


@app.get("/analytics/calls/{call_id}/audio")
async def analytics_call_audio(
    call_id: str, _admin: str = Depends(require_dashboard_admin)
) -> Response:
    data = await app.state.repo.get_call(call_id)
    if not data:
        raise HTTPException(status_code=404, detail="call not found")
    object_name = data.get("audio_object_name")
    if not object_name:
        raise HTTPException(status_code=404, detail="call has no audio")
    try:
        content = await app.state.storage.download(object_name)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail="audio storage is unavailable"
        ) from exc
    filename = data.get("audio_filename") or "recording.mp3"
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return Response(content=content, media_type=content_type)


@app.get("/analytics/calls/{call_id}")
async def analytics_call_detail(
    call_id: str, _admin: str = Depends(require_dashboard_admin)
) -> dict[str, Any]:
    data = await app.state.analytics.get_call_detail(call_id)
    if not data:
        raise HTTPException(status_code=404, detail="call not found")
    data["audio_url"] = (
        f"/api/analytics/calls/{call_id}/audio"
        if data.get("audio_object_name")
        else None
    )
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
    uvicorn.run(
        "app.services.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
