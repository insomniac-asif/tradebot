# core/project_reporter.py
"""
Local reporter that writes QQQbot data directly to projects.db.

Mirrors closed trades, predictions, aggregated P&L snapshots, and heartbeats
into the cross-project database so the unified dashboard can show QQQbot
alongside crypto and futures projects.

All writes go directly to SQLite — no HTTP overhead since QQQbot runs
on the same machine as the dashboard.

Skips disabled and dead sims.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import yaml

from core.paths import DATA_DIR
from core.projects_db import (
    insert_trade,
    insert_signal,
    insert_snapshot,
    insert_heartbeat,
    get_conn,
)

PROJECT = "qqq"
SIM_DIR = os.path.join(DATA_DIR, "sims")
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "simulation",
    "sim_config.yaml",
)

_ALL_SESSIONS = {"PREMARKET", "OPENING_HOUR", "MIDDAY", "POWER_HOUR", "CLOSING"}

_last_sync_trades = 0  # epoch timestamp of last trade sync
_synced_trade_ids: set = set()  # trade IDs already written


def _is_sim_active(profile: dict, sim_data: dict) -> bool:
    """Return True if a sim should be included in reporting."""
    if profile.get("enabled") is False:
        return False
    blocked = set(profile.get("blocked_sessions") or [])
    if _ALL_SESSIONS.issubset(blocked):
        return False
    if sim_data.get("is_dead", False):
        return False
    return True


def _load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _load_sim(sim_id: str) -> dict:
    path = os.path.join(SIM_DIR, f"{sim_id}.json")
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _get_active_sims() -> list[tuple[str, dict, dict]]:
    """Return list of (sim_id, profile, sim_data) for active sims."""
    config = _load_config()
    results = []
    for key, profile in config.items():
        if not isinstance(profile, dict):
            continue
        sid = str(key).upper()
        if not (sid.startswith("SIM") and sid[3:].isdigit()):
            continue
        sim_data = _load_sim(sid)
        if _is_sim_active(profile, sim_data):
            results.append((sid, profile, sim_data))
    return results


# ---------------------------------------------------------------------------
# Sync closed trades
# ---------------------------------------------------------------------------

def sync_trades():
    """Mirror closed trades from sim state files to projects.db.

    Only syncs trades not already written (tracked by trade_id).
    """
    global _synced_trade_ids

    # On first call, load existing trade IDs from DB to avoid duplicates
    if not _synced_trade_ids:
        try:
            conn = get_conn()
            rows = conn.execute(
                "SELECT metadata FROM project_trades WHERE project=?", (PROJECT,)
            ).fetchall()
            for row in rows:
                try:
                    meta = json.loads(row[0]) if row[0] else {}
                    tid = meta.get("trade_id")
                    if tid:
                        _synced_trade_ids.add(tid)
                except (json.JSONDecodeError, TypeError):
                    pass
            conn.close()
        except Exception:
            pass

    active_sims = _get_active_sims()
    count = 0

    for sim_id, profile, sim_data in active_sims:
        trade_log = sim_data.get("trade_log") or []
        for trade in trade_log:
            trade_id = trade.get("trade_id")
            if not trade_id or trade_id in _synced_trade_ids:
                continue
            # Only sync closed trades (have realized_pnl_dollars)
            if trade.get("realized_pnl_dollars") is None:
                continue

            insert_trade(PROJECT, {
                "timestamp": trade.get("exit_time") or trade.get("entry_time"),
                "instrument": trade.get("option_symbol") or trade.get("symbol"),
                "direction": trade.get("direction"),
                "side": "entry" if trade.get("direction") == "BULLISH" else "entry",
                "size": trade.get("qty"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "pnl": trade.get("realized_pnl_dollars"),
                "pnl_pct": trade.get("realized_pnl_pct"),
                "status": "closed",
                "strategy": profile.get("signal_mode"),
                "sim_id": sim_id,
                "metadata": {
                    "trade_id": trade_id,
                    "symbol": trade.get("symbol"),
                    "strike": trade.get("strike"),
                    "expiry": trade.get("expiry"),
                    "contract_type": trade.get("contract_type"),
                    "horizon": trade.get("horizon"),
                    "exit_reason": trade.get("exit_reason"),
                },
            })
            _synced_trade_ids.add(trade_id)
            count += 1

    if count > 0:
        logging.debug("project_reporter: synced %d new trades", count)
    return count


# ---------------------------------------------------------------------------
# Sync predictions as signals
# ---------------------------------------------------------------------------

def sync_predictions(limit: int = 50):
    """Mirror recent predictions from analytics.db to projects.db as signals."""
    try:
        from core.analytics_db import get_conn as get_analytics_conn
    except ImportError:
        return 0

    # Get the latest signal timestamp we've already synced
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT timestamp FROM project_signals WHERE project=? ORDER BY id DESC LIMIT 1",
            (PROJECT,),
        ).fetchone()
        last_ts = row[0] if row else "2000-01-01"
    finally:
        conn.close()

    # Fetch newer predictions from analytics.db
    aconn = get_analytics_conn()
    try:
        rows = aconn.execute(
            "SELECT time, symbol, timeframe, direction, confidence, regime "
            "FROM predictions WHERE time > ? ORDER BY time ASC LIMIT ?",
            (last_ts, limit),
        ).fetchall()
    except Exception:
        aconn.close()
        return 0
    aconn.close()

    count = 0
    for row in rows:
        insert_signal(PROJECT, {
            "timestamp": row[0],
            "signal_type": "prediction",
            "instrument": row[1],
            "direction": row[3],
            "confidence": row[4],
            "timeframe": row[2],
            "source": "ml_predictor",
            "metadata": {"regime": row[5]},
        })
        count += 1

    if count > 0:
        logging.debug("project_reporter: synced %d predictions", count)
    return count


# ---------------------------------------------------------------------------
# P&L snapshot
# ---------------------------------------------------------------------------

def write_pnl_snapshot():
    """Write an aggregated P&L snapshot across all active sims."""
    active_sims = _get_active_sims()

    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    total_open = 0
    total_balance = 0.0
    total_start = 0.0

    for sim_id, profile, sim_data in active_sims:
        balance = float(sim_data.get("balance", 0))
        balance_start = float(profile.get("balance_start", 3000))
        trade_log = sim_data.get("trade_log") or []
        open_trades = sim_data.get("open_trades") or []

        closed = [t for t in trade_log if t.get("realized_pnl_dollars") is not None]
        wins = [t for t in closed if (t.get("realized_pnl_dollars") or 0) > 0]
        pnl = sum(float(t.get("realized_pnl_dollars") or 0) for t in closed)

        total_pnl += pnl
        total_trades += len(closed)
        total_wins += len(wins)
        total_open += len(open_trades)
        total_balance += balance
        total_start += balance_start

    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else None

    # Daily P&L: sum of trades closed today
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_pnl = 0.0
    for _, _, sim_data in active_sims:
        for t in (sim_data.get("trade_log") or []):
            exit_time = t.get("exit_time") or ""
            if today_str in exit_time and t.get("realized_pnl_dollars") is not None:
                daily_pnl += float(t.get("realized_pnl_dollars") or 0)

    insert_snapshot(PROJECT, {
        "daily_pnl": round(daily_pnl, 2),
        "cumulative_pnl": round(total_pnl, 2),
        "win_rate": round(win_rate, 1) if win_rate is not None else None,
        "total_trades": total_trades,
        "open_positions": total_open,
        "balance": round(total_balance, 2),
        "metadata": {
            "active_sims": len(active_sims),
            "balance_start_total": round(total_start, 2),
            "growth_pct": round((total_balance - total_start) / total_start * 100, 2) if total_start > 0 else 0,
        },
    })
    logging.debug("project_reporter: wrote P&L snapshot (pnl=%.2f, trades=%d)", total_pnl, total_trades)


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def write_heartbeat():
    """Write a heartbeat for the QQQbot project."""
    # Try to read the existing heartbeat.json for uptime info
    hb_path = os.path.join(DATA_DIR, "heartbeat.json")
    uptime = None
    try:
        with open(hb_path, "r") as f:
            hb_data = json.load(f)
        start_ts = hb_data.get("started_at")
        if start_ts:
            started = datetime.fromisoformat(start_ts)
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            uptime = (datetime.now(timezone.utc) - started).total_seconds()
    except Exception:
        pass

    insert_heartbeat(PROJECT, {
        "status": "online",
        "version": "qqqbot-1.0",
        "uptime_seconds": uptime,
        "metadata": {
            "host": "desktop",
            "dashboard_port": os.environ.get("DASHBOARD_PORT", "8090"),
        },
    })
    logging.debug("project_reporter: heartbeat written")


# ---------------------------------------------------------------------------
# Full sync (call periodically from watchers)
# ---------------------------------------------------------------------------

def full_sync():
    """Run all sync tasks. Safe to call frequently — idempotent for trades."""
    try:
        sync_trades()
        sync_predictions(limit=50)
        write_pnl_snapshot()
        write_heartbeat()
    except Exception as e:
        logging.error("project_reporter_error: %s", e)
