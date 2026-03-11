"""
dashboard/app_helpers.py
------------------------
Standalone helper functions extracted from dashboard/app.py.
No references to the FastAPI `app` object.
"""

import asyncio
import json
import os
import re
import math
import time as _time
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
import yaml

# ---------------------------------------------------------------------------
# Path constants (duplicated here so helpers are self-contained)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIM_DIR = os.path.join(BASE_DIR, "data", "sims")
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")
NARRATIVES_DIR = os.path.join(BASE_DIR, "data", "trade_narratives")
CHARTS_DIR = os.path.join(BASE_DIR, "data", "trade_charts")

_CHART_NARRATIVE_TTL = 15 * 60  # 15 minutes in seconds


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

async def _cleanup_trade_files():
    """Delete trade charts and narratives older than 15 minutes."""
    dirs = [
        os.path.join(BASE_DIR, "data", "trade_charts"),
        os.path.join(BASE_DIR, "data", "trade_narratives"),
    ]
    while True:
        await asyncio.sleep(5 * 60)  # run every 5 minutes
        cutoff = _time.time() - _CHART_NARRATIVE_TTL
        for d in dirs:
            if not os.path.isdir(d):
                continue
            for fname in os.listdir(d):
                fpath = os.path.join(d, fname)
                try:
                    if os.path.getmtime(fpath) < cutoff:
                        os.remove(fpath)
                except Exception:
                    pass


def _load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _load_sim(sim_id: str) -> Optional[dict]:
    path = os.path.join(SIM_DIR, f"{sim_id}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_underlying(option_symbol: str) -> str:
    """Extract underlying ticker from OCC option_symbol (alphabetic prefix)."""
    import re as _re
    m = _re.match(r'^([A-Z]{1,6})', (option_symbol or '').upper())
    return m.group(1) if m else 'SPY'


def _to_naive_et(ts_str: str):
    """Parse timestamp string to naive Eastern Time datetime.
    Handles both tz-aware (converts to ET) and naive (assumed ET) inputs."""
    if not ts_str:
        return None
    dt = pd.to_datetime(ts_str, errors="coerce")
    if pd.isnull(dt):
        return None
    if dt.tzinfo is not None:
        return dt.tz_convert("US/Eastern").tz_localize(None)
    return dt  # naive: assume already ET


def _get_candle_window(
    entry_time_str: str,
    exit_time_str: str,
    symbol: str = 'SPY',
    before_min: int = 30,
    after_min: int = 10,
) -> list[dict]:
    """Load candle data for a trade window using get_candle_data (symbol-aware)."""
    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from core.data_service import get_candle_data

        entry_dt = _to_naive_et(entry_time_str)
        exit_dt  = _to_naive_et(exit_time_str) or entry_dt

        if entry_dt is None:
            return []

        start = entry_dt.to_pydatetime() - pd.Timedelta(minutes=before_min).to_pytimedelta()
        end   = exit_dt.to_pydatetime()  + pd.Timedelta(minutes=after_min).to_pytimedelta()

        return get_candle_data(symbol, start, end)
    except Exception:
        return []


def _get_trade_by_id(sim_id: str, trade_id: str) -> tuple[dict | None, dict | None]:
    """Return (trade_dict, sim_profile) or (None, None)."""
    data = _load_sim(sim_id)
    if not data:
        return None, None
    config = _load_config()
    profile = config.get(sim_id, {})
    for t in (data.get("trade_log") or []):
        if t.get("trade_id") == trade_id:
            return t, profile
    return None, None


def _parse_occ(option_symbol: str) -> dict:
    """Parse OCC option symbol into components."""
    import re as _re
    m = _re.match(r'^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$', (option_symbol or '').upper())
    if not m:
        return {}
    yy, mm, dd = m.group(2), m.group(3), m.group(4)
    contract_type = "CALL" if m.group(5) == "C" else "PUT"
    strike = int(m.group(6)) / 1000
    return {
        "ticker": m.group(1),
        "expiry": f"20{yy}-{mm}-{dd}",
        "contract_type": contract_type,
        "strike": strike,
    }


_ALL_SESSIONS = {"PREMARKET", "OPENING_HOUR", "MIDDAY", "POWER_HOUR", "CLOSING"}

def _get_account_phase(balance: float) -> str:
    """Return MICRO / EARLY_GROWTH / SCALING based on current balance."""
    try:
        from simulation.sim_account_mode import get_account_phase
        return get_account_phase(balance)
    except Exception:
        return "UNKNOWN"


def _is_sim_disabled(profile: dict) -> bool:
    """Return True if a sim has all trading sessions blocked (effectively disabled)."""
    if profile.get("enabled") is False:
        return True
    blocked = set(profile.get("blocked_sessions") or [])
    return _ALL_SESSIONS.issubset(blocked)


def _compute_stats(sim_id: str, data: dict, profile: dict) -> dict:
    trade_log = data.get("trade_log") or []
    open_trades = data.get("open_trades") or []
    balance = float(data.get("balance", 0))
    balance_start = float(profile.get("balance_start", 25000))

    closed = [t for t in trade_log if t.get("realized_pnl_dollars") is not None]
    wins = [t for t in closed if _safe_float(t.get("realized_pnl_dollars", 0)) > 0]
    total = len(closed)
    win_rate = len(wins) / total if total > 0 else None
    total_pnl = sum(_safe_float(t.get("realized_pnl_dollars", 0)) for t in closed)
    avg_pnl = total_pnl / total if total > 0 else None

    # Current streak (walk backwards through closed trades)
    streak_count, streak_type = 0, None
    for t in reversed(closed):
        result = "win" if _safe_float(t.get("realized_pnl_dollars", 0)) > 0 else "loss"
        if streak_type is None:
            streak_type = result
            streak_count = 1
        elif result == streak_type:
            streak_count += 1
        else:
            break

    pnl_dollars = balance - balance_start
    pnl_pct = pnl_dollars / balance_start if balance_start > 0 else 0
    daily_loss = float(data.get("daily_loss", 0))

    # Best/worst trade
    pnls = [_safe_float(t.get("realized_pnl_dollars", 0)) for t in closed]
    best = max(pnls) if pnls else None
    worst = min(pnls) if pnls else None

    # Max drawdown (peak-to-trough on cumulative balance)
    max_dd = 0.0
    peak = balance_start
    running = balance_start
    for t in closed:
        running += _safe_float(t.get("realized_pnl_dollars", 0))
        if running > peak:
            peak = running
        dd = (peak - running) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # Current open trade summaries (all open positions)
    open_summary = None
    open_summaries = []
    for t in open_trades:
        s = {
            "direction": t.get("direction"),
            "option_symbol": t.get("option_symbol"),
            "symbol": t.get("symbol") or _parse_underlying(t.get("option_symbol", "")),
            "entry_price": t.get("entry_price"),
            "qty": t.get("qty"),
            "entry_time": t.get("entry_time"),
            "strike": t.get("strike"),
            "expiry": t.get("expiry"),
            "regime": t.get("regime_at_entry"),
            "time_bucket": t.get("time_of_day_bucket"),
            "signal_mode": t.get("signal_mode"),
            "structure_score": t.get("structure_score"),
        }
        open_summaries.append(s)
    if open_summaries:
        open_summary = open_summaries[0]

    # Per-symbol breakdown
    sym_stats = {}
    for t in closed:
        sym = (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper()
        if sym not in sym_stats:
            sym_stats[sym] = {"trades": 0, "wins": 0, "pnl": 0.0}
        sym_stats[sym]["trades"] += 1
        pnl_v = _safe_float(t.get("realized_pnl_dollars", 0))
        sym_stats[sym]["pnl"] = round(sym_stats[sym]["pnl"] + pnl_v, 2)
        if pnl_v > 0:
            sym_stats[sym]["wins"] += 1
    for sym in sym_stats:
        t = sym_stats[sym]["trades"]
        sym_stats[sym]["win_rate"] = round(sym_stats[sym]["wins"] / t * 100, 1) if t > 0 else None

    # Today's session stats (ET date)
    _et = pytz.timezone("America/New_York")
    _today_str = datetime.now(_et).strftime("%Y-%m-%d")
    today_closed = []
    for t in closed:
        ts = t.get("exit_time") or t.get("entry_time") or ""
        if ts[:10] == _today_str:
            today_closed.append(t)
    today_open = [t for t in open_trades if (t.get("entry_time") or "")[:10] == _today_str]
    today_pnl = sum(_safe_float(t.get("realized_pnl_dollars", 0)) for t in today_closed)
    today_wins = [t for t in today_closed if _safe_float(t.get("realized_pnl_dollars", 0)) > 0]
    today_wr = round(len(today_wins) / len(today_closed) * 100, 1) if today_closed else None
    today_best = max((_safe_float(t.get("realized_pnl_dollars", 0)) for t in today_closed), default=None)
    today_worst = min((_safe_float(t.get("realized_pnl_dollars", 0)) for t in today_closed), default=None)
    session = {
        "trades": len(today_closed),
        "open": len(today_open),
        "pnl": round(today_pnl, 2),
        "win_rate": today_wr,
        "best": round(today_best, 2) if today_best is not None else None,
        "worst": round(today_worst, 2) if today_worst is not None else None,
    }

    # Configured symbols list
    _cfg_syms = profile.get("symbols") or ([profile.get("symbol")] if profile.get("symbol") else ["SPY"])

    return {
        "sim_id": sim_id,
        "name": profile.get("name", sim_id),
        "signal_mode": profile.get("signal_mode", ""),
        "strategy_family": profile.get("signal_mode", "").lower().replace("_", " ").title(),
        "features_enabled": bool(profile.get("features_enabled")),
        "horizon": profile.get("horizon", ""),
        "dte_min": profile.get("dte_min"),
        "dte_max": profile.get("dte_max"),
        "symbols": _cfg_syms,
        "balance": round(balance, 2),
        "balance_start": round(balance_start, 2),
        "pnl_dollars": round(pnl_dollars, 2),
        "pnl_pct": round(pnl_pct * 100, 2),
        "total_trades": total,
        "win_rate": round(win_rate * 100, 1) if win_rate is not None else None,
        "avg_pnl": round(avg_pnl, 2) if avg_pnl is not None else None,
        "total_pnl": round(total_pnl, 2),
        "daily_loss": round(daily_loss, 2),
        "open_count": len(open_trades),
        "open_trade": open_summary,
        "open_trades": open_summaries,
        "best_trade": round(best, 2) if best is not None else None,
        "worst_trade": round(worst, 2) if worst is not None else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "symbol_stats": sym_stats,
        "session": session,
        "streak": {"type": streak_type, "count": streak_count} if streak_type else None,
        "is_disabled": _is_sim_disabled(profile),
        # Small-account compounding fields
        "is_dead": bool(data.get("is_dead", False)),
        "death_time": data.get("death_time"),
        "death_balance": data.get("death_balance"),
        "reset_count": int(data.get("reset_count", 0)),
        "account_phase": _get_account_phase(balance),
        "peak_balance": round(float(data.get("peak_balance", balance_start)), 2),
        "growth_from_start_pct": round(pnl_pct * 100, 2),
        "small_account_mode": bool(profile.get("small_account_mode", False)),
        "death_threshold": float(profile.get("death_threshold", 25.0)),
        # Strategy overview / profile fields
        "stop_loss_pct": profile.get("stop_loss_pct"),
        "profit_target_pct": profile.get("profit_target_pct"),
        "trailing_stop_activate_pct": profile.get("trailing_stop_activate_pct"),
        "trailing_stop_trail_pct": profile.get("trailing_stop_trail_pct"),
        "risk_per_trade_pct": profile.get("risk_per_trade_pct"),
        "daily_loss_limit_pct": profile.get("daily_loss_limit_pct"),
        "regime_filter": profile.get("regime_filter"),
        "hold_min_seconds": profile.get("hold_min_seconds"),
        "hold_max_seconds": profile.get("hold_max_seconds"),
        "cutoff_time_et": profile.get("cutoff_time_et"),
        "otm_pct": profile.get("otm_pct"),
        "session_filter": profile.get("session_filter"),
        "blocked_sessions": profile.get("blocked_sessions"),
        "execution_mode": profile.get("execution_mode"),
        "enabled": profile.get("enabled", True),
    }


# ---------------------------------------------------------------------------
# Route handler helpers (extracted to app_helpers3.py to keep file under 500 lines)
# ---------------------------------------------------------------------------
from dashboard.app_helpers3 import (  # noqa: E402 (re-export for callers)
    _handle_get_chart,
    _handle_get_recent_trades,
    _handle_get_trade_history,
)
