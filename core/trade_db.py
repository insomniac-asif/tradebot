"""
SQLite trade journal for sim trades.

All closed sim trades are persisted here in addition to the per-sim JSON files.
INSERT OR IGNORE idempotency ensures replaying from JSON on startup is safe.

Usage:
    from core.trade_db import insert_trade, query_trades, sync_from_sim_jsons
"""

import json
import logging
import os
import sqlite3
from typing import Optional

from core.paths import DATA_DIR

_DB_PATH = os.path.join(DATA_DIR, "trade_journal.db")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS sim_trades (
    trade_id          TEXT PRIMARY KEY,
    sim_id            TEXT,
    entry_time        TEXT,
    exit_time         TEXT,
    direction         TEXT,
    option_symbol     TEXT,
    qty               REAL,
    entry_price       REAL,
    exit_price        REAL,
    realized_pnl_dollars REAL,
    realized_pnl_pct  REAL,
    exit_reason       TEXT,
    regime            TEXT,
    setup             TEXT,
    signal            TEXT,
    extra_json        TEXT
);
"""

_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_sim_trades_sim_id ON sim_trades (sim_id);",
    "CREATE INDEX IF NOT EXISTS idx_sim_trades_exit_time ON sim_trades (exit_time);",
]


def _connect() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn


def create_tables() -> None:
    """Create tables and indexes if they don't exist."""
    try:
        with _connect() as conn:
            conn.execute(_CREATE_SQL)
            for idx_sql in _INDEX_SQL:
                conn.execute(idx_sql)
            conn.commit()
    except Exception as exc:
        logging.error("trade_db_create_tables_failed: %s", exc)


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

_KNOWN_COLS = {
    "trade_id", "sim_id", "entry_time", "exit_time", "direction",
    "option_symbol", "qty", "entry_price", "exit_price",
    "realized_pnl_dollars", "realized_pnl_pct", "exit_reason",
    "regime", "setup", "signal",
}


def insert_trade(record: dict) -> bool:
    """
    Insert a closed trade. Returns True on success, False on failure.
    INSERT OR IGNORE: safe to call repeatedly with the same trade_id.
    """
    if not isinstance(record, dict):
        return False
    trade_id = record.get("trade_id")
    if not trade_id:
        return False

    # Extract known columns; everything else goes into extra_json
    extra = {k: v for k, v in record.items() if k not in _KNOWN_COLS}
    extra_json = json.dumps(extra, default=str) if extra else None

    def _safe(key, cast=None):
        v = record.get(key)
        if v is None:
            return None
        if cast is not None:
            try:
                return cast(v)
            except (TypeError, ValueError):
                return None
        return str(v)

    row = (
        str(trade_id),
        _safe("sim_id"),
        _safe("entry_time"),
        _safe("exit_time"),
        _safe("direction") or _safe("type"),
        _safe("option_symbol"),
        _safe("qty", float),
        _safe("entry_price", float),
        _safe("exit_price", float),
        _safe("realized_pnl_dollars", float),
        _safe("realized_pnl_pct", float),
        _safe("exit_reason"),
        _safe("regime"),
        _safe("setup"),
        _safe("signal"),
        extra_json,
    )

    try:
        with _connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sim_trades
                   (trade_id, sim_id, entry_time, exit_time, direction,
                    option_symbol, qty, entry_price, exit_price,
                    realized_pnl_dollars, realized_pnl_pct, exit_reason,
                    regime, setup, signal, extra_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                row,
            )
            conn.commit()
        return True
    except Exception as exc:
        logging.error("trade_db_insert_failed: trade_id=%s err=%s", trade_id, exc)
        return False


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def query_trades(
    sim_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """
    Return closed trades sorted by exit_time descending.
    Optionally filtered by sim_id.
    """
    try:
        with _connect() as conn:
            if sim_id:
                rows = conn.execute(
                    "SELECT * FROM sim_trades WHERE sim_id=? ORDER BY exit_time DESC LIMIT ? OFFSET ?",
                    (sim_id, limit, offset),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM sim_trades ORDER BY exit_time DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logging.error("trade_db_query_failed: %s", exc)
        return []


def trade_count(sim_id: Optional[str] = None) -> int:
    """Return total number of trades (optionally for one sim)."""
    try:
        with _connect() as conn:
            if sim_id:
                row = conn.execute(
                    "SELECT COUNT(*) FROM sim_trades WHERE sim_id=?", (sim_id,)
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) FROM sim_trades").fetchone()
        return row[0] if row else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Startup sync from existing JSON trade logs
# ---------------------------------------------------------------------------

def sync_from_sim_jsons() -> int:
    """
    Walk all data/sims/SIM*.json files and INSERT OR IGNORE each closed trade
    into the DB.  Returns number of records processed (not necessarily inserted
    — duplicates are silently skipped).
    """
    sims_dir = os.path.join(DATA_DIR, "sims")
    if not os.path.isdir(sims_dir):
        return 0

    create_tables()
    total = 0
    for fname in os.listdir(sims_dir):
        if not fname.upper().startswith("SIM") or not fname.endswith(".json"):
            continue
        fpath = os.path.join(sims_dir, fname)
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
            trade_log = data.get("trade_log", [])
            if not isinstance(trade_log, list):
                continue
            sim_id = data.get("sim_id") or fname.replace(".json", "")
            for trade in trade_log:
                if not isinstance(trade, dict):
                    continue
                trade.setdefault("sim_id", sim_id)
                insert_trade(trade)
                total += 1
        except Exception as exc:
            logging.warning("trade_db_sync_error: file=%s err=%s", fname, exc)

    return total
