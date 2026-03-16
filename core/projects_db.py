# core/projects_db.py
"""
SQLite-backed cross-project data store at data/projects.db.

Stores trades, signals, P&L snapshots, and heartbeats from all projects
(qqq, crypto, futures).  Each table has a `project` column and a `metadata`
JSON column for extensibility.

Thread-safe: each call opens its own connection (same pattern as analytics_db).
"""

import json
import os
import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from core.paths import DATA_DIR

DB_PATH = os.path.join(DATA_DIR, "projects.db")

VALID_PROJECTS = {"qqq", "crypto", "futures"}

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = {
    "project_trades": """
        CREATE TABLE IF NOT EXISTS project_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            instrument TEXT,
            direction TEXT,
            side TEXT,
            size REAL,
            entry_price REAL,
            exit_price REAL,
            pnl REAL,
            pnl_pct REAL,
            status TEXT DEFAULT 'closed',
            strategy TEXT,
            sim_id TEXT,
            metadata TEXT DEFAULT '{}'
        )
    """,
    "project_signals": """
        CREATE TABLE IF NOT EXISTS project_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            signal_type TEXT,
            instrument TEXT,
            direction TEXT,
            confidence REAL,
            timeframe TEXT,
            source TEXT,
            metadata TEXT DEFAULT '{}'
        )
    """,
    "project_snapshots": """
        CREATE TABLE IF NOT EXISTS project_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            daily_pnl REAL DEFAULT 0,
            cumulative_pnl REAL DEFAULT 0,
            win_rate REAL,
            total_trades INTEGER DEFAULT 0,
            open_positions INTEGER DEFAULT 0,
            balance REAL,
            metadata TEXT DEFAULT '{}'
        )
    """,
    "project_heartbeats": """
        CREATE TABLE IF NOT EXISTS project_heartbeats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            status TEXT DEFAULT 'online',
            version TEXT,
            uptime_seconds REAL,
            metadata TEXT DEFAULT '{}'
        )
    """,
}

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pt_project ON project_trades(project)",
    "CREATE INDEX IF NOT EXISTS idx_pt_ts ON project_trades(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_ps_project ON project_signals(project)",
    "CREATE INDEX IF NOT EXISTS idx_ps_ts ON project_signals(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_psnap_project ON project_snapshots(project)",
    "CREATE INDEX IF NOT EXISTS idx_psnap_ts ON project_snapshots(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_ph_project ON project_heartbeats(project)",
    "CREATE INDEX IF NOT EXISTS idx_ph_ts ON project_heartbeats(timestamp)",
]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_conn():
    """Return a new SQLite connection with WAL mode."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def transaction():
    """Context manager that commits on success, rolls back on error."""
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_projects_db():
    """Create tables and indexes if they don't exist."""
    conn = get_conn()
    try:
        for ddl in _SCHEMA.values():
            conn.execute(ddl)
        for idx in _INDEXES:
            conn.execute(idx)
        conn.commit()
        logging.debug("projects_db: schema ensured at %s", DB_PATH)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def insert_trade(project: str, data: dict) -> int:
    meta = json.dumps(data.get("metadata") or {})
    with transaction() as conn:
        cur = conn.execute(
            """INSERT INTO project_trades
               (project, timestamp, instrument, direction, side, size,
                entry_price, exit_price, pnl, pnl_pct, status, strategy,
                sim_id, metadata)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                project,
                data.get("timestamp", _now()),
                data.get("instrument"),
                data.get("direction"),
                data.get("side"),
                data.get("size"),
                data.get("entry_price"),
                data.get("exit_price"),
                data.get("pnl"),
                data.get("pnl_pct"),
                data.get("status", "closed"),
                data.get("strategy"),
                data.get("sim_id"),
                meta,
            ),
        )
        return cur.lastrowid


def insert_signal(project: str, data: dict) -> int:
    meta = json.dumps(data.get("metadata") or {})
    with transaction() as conn:
        cur = conn.execute(
            """INSERT INTO project_signals
               (project, timestamp, signal_type, instrument, direction,
                confidence, timeframe, source, metadata)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                project,
                data.get("timestamp", _now()),
                data.get("signal_type"),
                data.get("instrument"),
                data.get("direction"),
                data.get("confidence"),
                data.get("timeframe"),
                data.get("source"),
                meta,
            ),
        )
        return cur.lastrowid


def insert_snapshot(project: str, data: dict) -> int:
    meta = json.dumps(data.get("metadata") or {})
    with transaction() as conn:
        cur = conn.execute(
            """INSERT INTO project_snapshots
               (project, timestamp, daily_pnl, cumulative_pnl, win_rate,
                total_trades, open_positions, balance, metadata)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                project,
                data.get("timestamp", _now()),
                data.get("daily_pnl", 0),
                data.get("cumulative_pnl", 0),
                data.get("win_rate"),
                data.get("total_trades", 0),
                data.get("open_positions", 0),
                data.get("balance"),
                meta,
            ),
        )
        return cur.lastrowid


def insert_heartbeat(project: str, data: dict) -> int:
    meta = json.dumps(data.get("metadata") or {})
    with transaction() as conn:
        cur = conn.execute(
            """INSERT INTO project_heartbeats
               (project, timestamp, status, version, uptime_seconds, metadata)
               VALUES (?,?,?,?,?,?)""",
            (
                project,
                data.get("timestamp", _now()),
                data.get("status", "online"),
                data.get("version"),
                data.get("uptime_seconds"),
                meta,
            ),
        )
        return cur.lastrowid


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_latest_heartbeat(project: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM project_heartbeats WHERE project=? ORDER BY id DESC LIMIT 1",
            (project,),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def get_all_heartbeats() -> dict:
    """Return latest heartbeat per project."""
    result = {}
    for proj in VALID_PROJECTS:
        hb = get_latest_heartbeat(proj)
        result[proj] = hb
    return result


def get_latest_snapshot(project: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM project_snapshots WHERE project=? ORDER BY id DESC LIMIT 1",
            (project,),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def get_recent_trades(project: str | None = None, limit: int = 50) -> list[dict]:
    conn = get_conn()
    try:
        if project:
            rows = conn.execute(
                "SELECT * FROM project_trades WHERE project=? ORDER BY id DESC LIMIT ?",
                (project, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM project_trades ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_recent_signals(project: str | None = None, limit: int = 50) -> list[dict]:
    conn = get_conn()
    try:
        if project:
            rows = conn.execute(
                "SELECT * FROM project_signals WHERE project=? ORDER BY id DESC LIMIT ?",
                (project, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM project_signals ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_snapshots(project: str | None = None, limit: int = 100) -> list[dict]:
    conn = get_conn()
    try:
        if project:
            rows = conn.execute(
                "SELECT * FROM project_snapshots WHERE project=? ORDER BY id DESC LIMIT ?",
                (project, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM project_snapshots ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_project_summary(project: str) -> dict:
    """Return a combined summary for a single project."""
    return {
        "project": project,
        "heartbeat": get_latest_heartbeat(project),
        "snapshot": get_latest_snapshot(project),
        "recent_trades": get_recent_trades(project, limit=10),
        "recent_signals": get_recent_signals(project, limit=10),
    }


def get_all_summaries() -> dict:
    """Return summary across all projects."""
    return {proj: get_project_summary(proj) for proj in VALID_PROJECTS}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    # Parse metadata JSON back to dict
    if "metadata" in d and isinstance(d["metadata"], str):
        try:
            d["metadata"] = json.loads(d["metadata"])
        except (json.JSONDecodeError, TypeError):
            d["metadata"] = {}
    return d
