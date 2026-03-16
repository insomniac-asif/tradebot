# dashboard/api_projects.py
"""
FastAPI router for cross-project ingest and status endpoints.

POST endpoints require X-API-Key header (PROJECTS_API_KEY from .env).
GET endpoints are open (behind Cloudflare).

Namespaced under /api/projects/ to avoid conflicts with existing routes.
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from core.projects_db import (
    VALID_PROJECTS,
    insert_trade,
    insert_signal,
    insert_snapshot,
    insert_heartbeat,
    get_project_summary,
    get_all_summaries,
    get_all_heartbeats,
    get_recent_trades,
    get_recent_signals,
    get_snapshots,
)

router = APIRouter(prefix="/api/projects", tags=["projects"])

_API_KEY = os.environ.get("PROJECTS_API_KEY", "")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _check_key(x_api_key: str = Header(None)):
    if not _API_KEY:
        raise HTTPException(500, "PROJECTS_API_KEY not configured")
    if x_api_key != _API_KEY:
        raise HTTPException(401, "Invalid API key")


def _validate_project(project: str) -> str:
    p = project.lower().strip()
    if p not in VALID_PROJECTS:
        raise HTTPException(400, f"Invalid project '{project}'. Must be one of: {sorted(VALID_PROJECTS)}")
    return p


# ---------------------------------------------------------------------------
# Pydantic models — flexible schemas with metadata dict for extensibility
# ---------------------------------------------------------------------------

class TradePayload(BaseModel):
    timestamp: str | None = None
    instrument: str | None = None
    direction: str | None = None
    side: str | None = None
    size: float | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    pnl: float | None = None
    pnl_pct: float | None = None
    status: str = "closed"
    strategy: str | None = None
    sim_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SignalPayload(BaseModel):
    timestamp: str | None = None
    signal_type: str | None = None
    instrument: str | None = None
    direction: str | None = None
    confidence: float | None = None
    timeframe: str | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SnapshotPayload(BaseModel):
    timestamp: str | None = None
    daily_pnl: float = 0
    cumulative_pnl: float = 0
    win_rate: float | None = None
    total_trades: int = 0
    open_positions: int = 0
    balance: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class HeartbeatPayload(BaseModel):
    timestamp: str | None = None
    status: str = "online"
    version: str | None = None
    uptime_seconds: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestPayload(BaseModel):
    """Unified ingest envelope — send one or more data types in a single call."""
    trades: list[TradePayload] = Field(default_factory=list)
    signals: list[SignalPayload] = Field(default_factory=list)
    snapshot: SnapshotPayload | None = None
    heartbeat: HeartbeatPayload | None = None


# ---------------------------------------------------------------------------
# POST — Ingest (requires API key)
# ---------------------------------------------------------------------------

@router.post("/ingest/{project}")
async def ingest(project: str, payload: IngestPayload, x_api_key: str = Header(None)):
    _check_key(x_api_key)
    proj = _validate_project(project)

    counts = {"trades": 0, "signals": 0, "snapshot": False, "heartbeat": False}

    for t in payload.trades:
        insert_trade(proj, t.model_dump())
        counts["trades"] += 1

    for s in payload.signals:
        insert_signal(proj, s.model_dump())
        counts["signals"] += 1

    if payload.snapshot:
        insert_snapshot(proj, payload.snapshot.model_dump())
        counts["snapshot"] = True

    if payload.heartbeat:
        insert_heartbeat(proj, payload.heartbeat.model_dump())
        counts["heartbeat"] = True

    return {"ok": True, "project": proj, "ingested": counts}


# ---------------------------------------------------------------------------
# GET — Status & queries (open)
# ---------------------------------------------------------------------------

@router.get("/status/{project}")
async def project_status(project: str):
    proj = _validate_project(project)
    return get_project_summary(proj)


@router.get("/status")
async def all_status():
    return get_all_summaries()


@router.get("/health")
async def health():
    heartbeats = get_all_heartbeats()
    now = datetime.now(timezone.utc)
    result = {}
    for proj, hb in heartbeats.items():
        if hb is None:
            result[proj] = {"status": "never_seen", "last_seen": None, "alive": False}
        else:
            try:
                ts = datetime.fromisoformat(hb["timestamp"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = (now - ts).total_seconds()
            except (ValueError, TypeError):
                age = 999999
            result[proj] = {
                "status": hb.get("status", "unknown"),
                "last_seen": hb.get("timestamp"),
                "age_seconds": round(age, 1),
                "alive": age < 300,  # 5 min threshold
                "version": hb.get("version"),
            }
    return result


@router.get("/trades")
async def trades(
    project: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    if project:
        project = _validate_project(project)
    return get_recent_trades(project, limit)


@router.get("/signals")
async def signals(
    project: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    if project:
        project = _validate_project(project)
    return get_recent_signals(project, limit)


@router.get("/snapshots")
async def snapshots(
    project: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    if project:
        project = _validate_project(project)
    return get_snapshots(project, limit)
