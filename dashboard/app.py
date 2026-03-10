"""
SpyBot Classroom Dashboard
--------------------------
Run from repo root:
    python -m dashboard.app
or:
    uvicorn dashboard.app:app --host 0.0.0.0 --port 8080 --reload

Access at: http://<laptop-ip>:8080
"""

import asyncio
import json
import os
import re
import math
import time as _time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

import pandas as pd
import pytz
import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIM_DIR = os.path.join(BASE_DIR, "data", "sims")
PREDICTIONS_CSV = os.path.join(BASE_DIR, "data", "predictions.csv")
CANDLES_CSV = os.path.join(BASE_DIR, "data", "spy_1m.csv")
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")
HEARTBEAT_PATH = os.path.join(BASE_DIR, "data", "heartbeat.json")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

_CHART_NARRATIVE_TTL = 15 * 60  # 15 minutes in seconds

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

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(_cleanup_trade_files())
    yield
    task.cancel()

app = FastAPI(title="SpyBot Dashboard", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
    }


def _safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/sims")
async def list_sims():
    config = _load_config()
    results = []
    for sim_id, profile in config.items():
        if str(sim_id).startswith("_") or not isinstance(profile, dict):
            continue
        if not re.match(r'^SIM\d+$', str(sim_id).upper()):
            continue
        data = _load_sim(sim_id)
        _cfg_syms = profile.get("symbols") or ([profile.get("symbol")] if profile.get("symbol") else ["SPY"])
        if data is None:
            results.append({
                "sim_id": sim_id,
                "name": profile.get("name", sim_id),
                "signal_mode": profile.get("signal_mode", ""),
                "features_enabled": bool(profile.get("features_enabled")),
                "horizon": profile.get("horizon", ""),
                "dte_min": profile.get("dte_min"),
                "dte_max": profile.get("dte_max"),
                "symbols": _cfg_syms,
                "balance": float(profile.get("balance_start", 25000)),
                "balance_start": float(profile.get("balance_start", 25000)),
                "pnl_dollars": 0.0,
                "pnl_pct": 0.0,
                "total_trades": 0,
                "win_rate": None,
                "avg_pnl": None,
                "total_pnl": 0.0,
                "daily_loss": 0.0,
                "open_count": 0,
                "open_trade": None,
                "best_trade": None,
                "worst_trade": None,
                "max_drawdown_pct": 0.0,
                "symbol_stats": {},
                "no_data": True,
            })
        else:
            s = _compute_stats(sim_id, data, profile)
            results.append(s)

    results.sort(key=lambda x: int(x["sim_id"].replace("SIM", ""))
                 if x["sim_id"].replace("SIM", "").isdigit() else 0)
    return results


@app.get("/api/sim/{sim_id}")
async def get_sim(sim_id: str):
    sim_id = sim_id.upper()
    config = _load_config()
    profile = config.get(sim_id)
    if not isinstance(profile, dict):
        raise HTTPException(status_code=404, detail="Sim not found")

    data = _load_sim(sim_id)
    if data is None:
        bs = float(profile.get("balance_start", 25000))
        _cfg_syms = profile.get("symbols") or ([profile.get("symbol")] if profile.get("symbol") else ["SPY"])
        stub_stats = {
            "sim_id": sim_id,
            "name": profile.get("name", sim_id),
            "signal_mode": profile.get("signal_mode", ""),
            "strategy_family": profile.get("signal_mode", "").lower().replace("_", " ").title(),
            "features_enabled": bool(profile.get("features_enabled")),
            "horizon": profile.get("horizon", ""),
            "dte_min": profile.get("dte_min"),
            "dte_max": profile.get("dte_max"),
            "symbols": _cfg_syms,
            "balance": bs,
            "balance_start": bs,
            "pnl_dollars": 0.0,
            "pnl_pct": 0.0,
            "total_trades": 0,
            "win_rate": None,
            "avg_pnl": None,
            "total_pnl": 0.0,
            "daily_loss": 0.0,
            "open_count": 0,
            "open_trade": None,
            "open_trades": [],
            "best_trade": None,
            "worst_trade": None,
            "max_drawdown_pct": 0.0,
            "symbol_stats": {},
            "session": {"trades": 0, "open": 0, "pnl": 0.0, "win_rate": None, "best": None, "worst": None},
            "streak": None,
        }
        return {
            "sim_id": sim_id,
            "name": profile.get("name", sim_id),
            "profile": {k: v for k, v in profile.items() if not str(k).startswith("_")},
            "stats": stub_stats,
            "open_trades": [],
            "recent_trades": [],
            "balance_history": [],
        }

    stats = _compute_stats(sim_id, data, profile)

    # Balance history (running cumulative)
    trade_log = data.get("trade_log") or []
    balance_history = []
    running = float(profile.get("balance_start", 25000))
    for t in trade_log:
        pnl = t.get("realized_pnl_dollars")
        if pnl is None:
            continue
        try:
            running += float(pnl)
            balance_history.append({
                "time": t.get("exit_time", ""),
                "balance": round(running, 2),
                "pnl": round(float(pnl), 2),
                "exit_reason": t.get("exit_reason", ""),
            })
        except Exception:
            pass

    # SL/TP pcts from profile (for display)
    _sl_pct = _safe_float(profile.get("stop_loss_pct"), 0)
    _tp_pct = _safe_float(profile.get("profit_target_pct"), 0)

    # Recent trades (newest first, last 30)
    recent_trades = []
    for t in reversed(trade_log[-30:]):
        ep = t.get("entry_price")
        sl_price = round(ep * (1 - _sl_pct), 4) if ep and _sl_pct else None
        tp_price = round(ep * (1 + _tp_pct), 4) if ep and _tp_pct else None
        recent_trades.append({
            "trade_id": (t.get("trade_id") or "")[-8:],
            "symbol": (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper(),
            "entry_time": t.get("entry_time", ""),
            "exit_time": t.get("exit_time", ""),
            "direction": t.get("direction", ""),
            "option_symbol": t.get("option_symbol", ""),
            "strike": t.get("strike"),
            "expiry": t.get("expiry"),
            "contract_type": t.get("contract_type"),
            "entry_price": ep,
            "exit_price": t.get("exit_price"),
            "sl_price": sl_price,
            "tp_price": tp_price,
            "qty": t.get("qty"),
            "pnl": t.get("realized_pnl_dollars"),
            "pnl_pct": t.get("realized_pnl_pct"),
            "exit_reason": t.get("exit_reason", ""),
            "exit_context": t.get("exit_context", ""),
            "regime": t.get("regime_at_entry", ""),
            "time_bucket": t.get("time_of_day_bucket", ""),
            "structure_score": t.get("structure_score"),
            "strategy_family": t.get("strategy_family", ""),
        })

    return {
        "sim_id": sim_id,
        "name": profile.get("name", sim_id),
        "profile": {k: v for k, v in profile.items() if not str(k).startswith("_")},
        "stats": stats,
        "open_trades": data.get("open_trades") or [],
        "recent_trades": recent_trades,
        "balance_history": balance_history,
    }


@app.get("/api/chart")
async def get_chart(symbol: str = "SPY", bars: int = 60):
    """Return last N 1-min candles for a symbol using live data_service (never stale CSV)."""
    import asyncio
    sym = symbol.upper()
    try:
        from core.data_service import get_market_dataframe, _fetch_from_alpaca, _prepare_dataframe, _load_symbol_registry
        if sym == "SPY":
            df = await asyncio.to_thread(get_market_dataframe)
        else:
            # Always read CSV first (like SPY), supplement with Alpaca when market open
            df = None
            registry = _load_symbol_registry()
            csv_path = registry.get(sym, {}).get("data_file")
            if csv_path:
                _abs = os.path.join(BASE_DIR, csv_path)
                if os.path.exists(_abs):
                    try:
                        df = pd.read_csv(_abs, parse_dates=[0])
                    except Exception:
                        df = None
            # Try live Alpaca data and merge with CSV
            raw = await asyncio.to_thread(_fetch_from_alpaca, sym)
            fresh = _prepare_dataframe(raw) if raw is not None and not raw.empty else None
            if fresh is not None and not fresh.empty:
                if df is not None and not df.empty:
                    # Supplement: concat CSV + fresh, drop duplicates on timestamp
                    fresh_reset = fresh.reset_index()
                    df_all = pd.concat([df, fresh_reset], ignore_index=True)
                    _tc = next((c for c in df_all.columns if "time" in c.lower() or "date" in c.lower()), None)
                    if _tc:
                        df_all[_tc] = pd.to_datetime(df_all[_tc], errors="coerce")
                        df_all = df_all.drop_duplicates(subset=[_tc], keep="last").sort_values(_tc)
                    df = df_all
                else:
                    df = fresh

        if df is None or df.empty:
            return {"candles": [], "symbol": sym, "error": f"no_data_{sym}"}

        # df may have a DatetimeIndex (from _prepare_dataframe) or plain RangeIndex (CSV); reset for easier handling
        df = df.reset_index()
        # Drop spurious 'index' column from RangeIndex reset
        if "index" in df.columns and df["index"].dtype != "datetime64[ns]" and not hasattr(df["index"].dtype, "tz"):
            df = df.drop(columns=["index"])
        ts_col = next((c for c in df.columns if "time" in c.lower() or "date" in c.lower()), None)
        if ts_col is None:
            return {"candles": [], "symbol": sym, "error": "no_timestamp_col"}

        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.dropna(subset=[ts_col]).sort_values(ts_col)
        # Localize naive ET timestamps correctly (do NOT use utc=True which shifts times by -4/-5h)
        import pytz as _pytz
        _et = _pytz.timezone("America/New_York")
        if df[ts_col].dt.tz is None:
            df[ts_col] = df[ts_col].dt.tz_localize(_et, ambiguous="infer", nonexistent="shift_forward")
        else:
            df[ts_col] = df[ts_col].dt.tz_convert(_et)

        # Filter to the most recent session so tail(60) doesn't span multiple days/gaps
        _now_et = pd.Timestamp.now(tz=_et)
        _today_et = _now_et.date()
        _today_start = pd.Timestamp(_today_et, tz=_et)
        _today_end   = _today_start + pd.Timedelta(days=1)
        df_today = df[(df[ts_col] >= _today_start) & (df[ts_col] < _today_end)]
        if len(df_today) >= 10:
            df = df_today
        else:
            # Market closed or no data today — use the most recent trading day
            _last_date = df[ts_col].dt.date.iloc[-1]
            _last_start = pd.Timestamp(_last_date, tz=_et)
            _last_end   = _last_start + pd.Timedelta(days=1)
            df_last = df[(df[ts_col] >= _last_start) & (df[ts_col] < _last_end)]
            if len(df_last) >= 10:
                df = df_last

        df = df.tail(max(bars, 1))
        candles = []
        for _, row in df.iterrows():
            candles.append({
                "t": row[ts_col].isoformat(),
                "o": round(_safe_float(row.get("open")), 4),
                "h": round(_safe_float(row.get("high")), 4),
                "l": round(_safe_float(row.get("low")), 4),
                "c": round(_safe_float(row.get("close")), 4),
                "v": int(_safe_float(row.get("volume", 0))),
            })
        return {"candles": candles, "symbol": sym}
    except Exception as e:
        return {"candles": [], "symbol": sym, "error": str(e)}


@app.get("/api/predictions")
async def get_predictions(symbol: str = None):
    """Return last 30 minutes of predictions from predictions.csv, optionally filtered by symbol."""
    try:
        df = pd.read_csv(PREDICTIONS_CSV)
        df.columns = [c.lower() for c in df.columns]
        ts_col = next((c for c in ("time", "timestamp", "datetime") if c in df.columns), None)
        if ts_col is None:
            return {"predictions": [], "latest": None}

        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.dropna(subset=[ts_col])
        df = df.sort_values(ts_col)

        # Symbol filter — rows without "symbol" column are treated as SPY (legacy)
        if symbol:
            sym_upper = symbol.upper()
            if "symbol" in df.columns:
                df = df[df["symbol"].fillna("SPY").str.upper() == sym_upper]
            elif sym_upper != "SPY":
                # No symbol column yet and requesting non-SPY → no results
                return {"predictions": [], "latest": None, "symbol": sym_upper}

        # Use today's ET date as cutoff — fall back to last day with predictions if today is empty
        _et_tz = pytz.timezone("America/New_York")
        _today_date = datetime.now(_et_tz).date()
        if df[ts_col].dt.tz is not None:
            cutoff = pd.Timestamp(_today_date, tz=_et_tz)
        else:
            cutoff = pd.Timestamp(_today_date)

        recent = df[df[ts_col] >= cutoff].tail(30)

        # If no predictions today, fall back to the last day that has data
        if recent.empty and not df.empty:
            _last_date = df[ts_col].dt.date.iloc[-1]
            if df[ts_col].dt.tz is not None:
                _last_cutoff = pd.Timestamp(_last_date, tz=_et_tz)
            else:
                _last_cutoff = pd.Timestamp(_last_date)
            recent = df[df[ts_col] >= _last_cutoff].tail(30)

        preds = []
        for _, row in recent.iterrows():
            ts_val = row[ts_col]
            if pd.notnull(ts_val) and ts_val.tzinfo is None:
                ts_val = ts_val.tz_localize(pytz.timezone("America/New_York"), ambiguous=True, nonexistent="shift_forward")
            entry = {"time": ts_val.isoformat()}
            for col in df.columns:
                if col == ts_col:
                    continue
                val = row[col]
                try:
                    entry[col] = float(val) if pd.notna(val) and val != "" else None
                except Exception:
                    entry[col] = str(val) if pd.notna(val) else None
            preds.append(entry)

        latest = preds[-1] if preds else None
        return {"predictions": preds, "latest": latest}
    except Exception as e:
        return {"predictions": [], "latest": None, "error": str(e)}


@app.get("/api/status")
async def get_status():
    """Return bot heartbeat and market status."""
    alive = False
    age_seconds = None
    pid = None
    last_heartbeat = None
    try:
        if os.path.exists(HEARTBEAT_PATH):
            with open(HEARTBEAT_PATH, "r") as f:
                hb = json.load(f)
            ts = hb.get("ts")
            pid = hb.get("pid")
            if ts:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = pytz.timezone("US/Eastern").localize(dt)
                age_seconds = round((datetime.now(pytz.utc) - dt.astimezone(pytz.utc)).total_seconds())
                alive = age_seconds < 180
                last_heartbeat = ts
    except Exception:
        pass

    # Market open check (ET 9:30–16:00 weekdays)
    et = datetime.now(pytz.timezone("US/Eastern"))
    weekday = et.weekday()  # 0=Mon, 4=Fri
    market_open = (
        weekday < 5
        and (et.hour, et.minute) >= (9, 30)
        and et.hour < 16
    )
    return {
        "alive": alive,
        "age_seconds": age_seconds,
        "pid": pid,
        "last_heartbeat": last_heartbeat,
        "market_open": market_open,
        "market_time": et.strftime("%H:%M ET"),
        "market_date": et.strftime("%a %b %d"),
    }


# ---------------------------------------------------------------------------
# Trade history helpers
# ---------------------------------------------------------------------------

NARRATIVES_DIR = os.path.join(BASE_DIR, "data", "trade_narratives")
CHARTS_DIR     = os.path.join(BASE_DIR, "data", "trade_charts")


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


# ---------------------------------------------------------------------------
# New Routes
# ---------------------------------------------------------------------------

@app.get("/api/trades/recent")
async def get_recent_trades(
    limit: int = 200,
    sim_id: str = Query(None),
    symbol: str = Query(None),
    max_entry_price: float = Query(None),
):
    """Return closed + open trades across all sims. Open trades appear first."""
    config = _load_config()
    closed_trades, open_trades_out = [], []

    for sid, profile in config.items():
        if str(sid).startswith("_") or not isinstance(profile, dict):
            continue
        if not re.match(r'^SIM\d+$', str(sid).upper()):
            continue
        if sim_id and sid.upper() != sim_id.upper():
            continue
        data = _load_sim(sid)
        if not data:
            continue
        _sl_pct = _safe_float(profile.get("stop_loss_pct"), 0)
        _tp_pct = _safe_float(profile.get("profit_target_pct"), 0)

        def _make_record(t, status):
            sym = (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper()
            if symbol and sym != symbol.upper():
                return None
            ep = t.get("entry_price")
            if max_entry_price is not None and ep is not None and float(ep) > max_entry_price:
                return None
            opt_sym = t.get("option_symbol", "")
            parsed = _parse_occ(opt_sym)
            sl_price = round(ep * (1 - _sl_pct), 4) if ep and _sl_pct else None
            tp_price = round(ep * (1 + _tp_pct), 4) if ep and _tp_pct else None
            return {
                "status": status,
                "sim_id": sid,
                "trade_id": t.get("trade_id", ""),
                "symbol": sym,
                "direction": t.get("direction", ""),
                "option_symbol": opt_sym,
                "strike": parsed.get("strike") or t.get("strike"),
                "contract_type": parsed.get("contract_type") or t.get("contract_type", ""),
                "expiry": parsed.get("expiry") or t.get("expiry", ""),
                "entry_price": ep,
                "exit_price": t.get("exit_price"),
                "sl_price": sl_price,
                "tp_price": tp_price,
                "qty": t.get("qty"),
                "pnl": t.get("realized_pnl_dollars"),
                "pnl_pct": t.get("realized_pnl_pct"),
                "entry_time": t.get("entry_time", ""),
                "exit_time": t.get("exit_time", ""),
                "exit_reason": t.get("exit_reason", ""),
                "exit_context": t.get("exit_context", ""),
                "regime": t.get("regime_at_entry", ""),
                "signal_mode": t.get("signal_mode", profile.get("signal_mode", "")),
                "mae_pct": t.get("mae_pct"),
                "mfe_pct": t.get("mfe_pct"),
            }

        for t in (data.get("open_trades") or []):
            rec = _make_record(t, "open")
            if rec:
                open_trades_out.append(rec)

        for t in (data.get("trade_log") or []):
            if t.get("exit_time") and t.get("realized_pnl_dollars") is not None:
                rec = _make_record(t, "closed")
                if rec:
                    closed_trades.append(rec)

    open_trades_out.sort(key=lambda x: x.get("entry_time") or "", reverse=True)
    closed_trades.sort(key=lambda x: x.get("exit_time") or "", reverse=True)
    all_trades = open_trades_out + closed_trades

    # Collect unique sim IDs and symbols for filter options
    all_sims = sorted({t["sim_id"] for t in all_trades})
    all_syms = sorted({t["symbol"] for t in all_trades if t["symbol"]})

    return {
        "trades": all_trades[:limit],
        "total": len(all_trades),
        "open_count": len(open_trades_out),
        "sims": all_sims,
        "symbols": all_syms,
    }


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


@app.get("/api/symbols")
async def get_symbols():
    """Return the symbol registry from sim_config.yaml."""
    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from core.data_service import _load_symbol_registry
        return _load_symbol_registry()
    except Exception:
        return {}


@app.get("/api/trades/{sim_id}/history")
async def get_trade_history(
    sim_id: str,
    date: str = Query(None),
    direction: str = Query(None),
    result: str = Query(None),
    symbol: str = Query(None),
    page: int = Query(1),
    per_page: int = Query(50),
):
    """Full paginated trade log with optional filters."""
    sim_id = sim_id.upper()
    data = _load_sim(sim_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Sim not found")

    trade_log = list(reversed(data.get("trade_log") or []))  # newest first

    # Filters
    if date:
        trade_log = [t for t in trade_log if (t.get("entry_time") or "").startswith(date)]
    if direction and direction.upper() != "ALL":
        d = direction.upper()
        trade_log = [t for t in trade_log if (t.get("direction") or "").upper() == d]
    if symbol and symbol.upper() != "ALL":
        sym = symbol.upper()
        trade_log = [t for t in trade_log
                     if (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper() == sym]
    if result and result.lower() != "all":
        if result.lower() == "win":
            trade_log = [t for t in trade_log if _safe_float(t.get("realized_pnl_dollars", 0)) > 0]
        elif result.lower() == "loss":
            trade_log = [t for t in trade_log if _safe_float(t.get("realized_pnl_dollars", 0)) <= 0]

    total = len(trade_log)
    start = (page - 1) * per_page
    page_trades = trade_log[start:start + per_page]

    # Load profile for SL/TP pcts
    config = _load_config()
    _prof = config.get(sim_id) or {}
    _sl_pct = _safe_float(_prof.get("stop_loss_pct"), 0)
    _tp_pct = _safe_float(_prof.get("profit_target_pct"), 0)

    try:
        from analytics.grader import grade_trade as _grade_trade
    except Exception:
        _grade_trade = None

    trades_out = []
    for t in page_trades:
        safe_id = re.sub(r"[^\w\-]", "_", t.get("trade_id", ""))
        # Grade: prefer GPT narrative grade if cached, else algorithmic
        grade = ""
        narr_path = os.path.join(NARRATIVES_DIR, f"{sim_id}_{safe_id}.json")
        if os.path.exists(narr_path):
            try:
                narr_data = json.loads(open(narr_path).read())
                grade = narr_data.get("grade", "") or ""
            except Exception:
                pass
        if not grade and _grade_trade and t.get("exit_price") is not None:
            grade = _grade_trade(t)
        ep = t.get("entry_price")
        sl_price = round(ep * (1 - _sl_pct), 4) if ep and _sl_pct else None
        tp_price = round(ep * (1 + _tp_pct), 4) if ep and _tp_pct else None
        trades_out.append({
            "trade_id":    t.get("trade_id", ""),
            "symbol":      (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper(),
            "option_symbol": t.get("option_symbol", ""),
            "direction":   t.get("direction", ""),
            "entry_price": ep,
            "exit_price":  t.get("exit_price"),
            "sl_price":    sl_price,
            "tp_price":    tp_price,
            "entry_time":  t.get("entry_time", ""),
            "exit_time":   t.get("exit_time", ""),
            "qty":         t.get("qty"),
            "pnl":         t.get("realized_pnl_dollars"),
            "pnl_pct":     t.get("realized_pnl_pct"),
            "exit_reason": t.get("exit_reason", ""),
            "exit_context": t.get("exit_context", ""),
            "signal_mode": t.get("signal_mode", ""),
            "regime":      t.get("regime_at_entry", ""),
            "time_bucket": t.get("time_of_day_bucket", ""),
            "strike":      t.get("strike"),
            "expiry":      t.get("expiry", ""),
            "contract_type": t.get("contract_type", ""),
            "mae_pct":     t.get("mae_pct") or t.get("mae"),
            "mfe_pct":     t.get("mfe_pct") or t.get("mfe"),
            "structure_score": t.get("structure_score"),
            "grade":       grade,
            "has_narrative": os.path.exists(narr_path),
        })

    return {"sim_id": sim_id, "total": total, "page": page, "per_page": per_page, "trades": trades_out}


@app.get("/api/trades/{sim_id}/{trade_id}/chart")
async def get_trade_chart(sim_id: str, trade_id: str, refresh: bool = False):
    """Return annotated trade chart as PNG."""
    sim_id = sim_id.upper()
    safe   = re.sub(r"[^\w\-]", "_", trade_id)
    cache  = os.path.join(CHARTS_DIR, f"{sim_id}_{safe}.png")

    if os.path.exists(cache) and not refresh:
        age = _time.time() - os.path.getmtime(cache)
        if age < 1800:
            return Response(content=open(cache, "rb").read(), media_type="image/png")

    trade, profile = _get_trade_by_id(sim_id, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="Trade not found")

    candle_data = _get_candle_window(
        trade.get("entry_time", ""),
        trade.get("exit_time", ""),
        symbol=trade.get("symbol") or _parse_underlying(trade.get("option_symbol", "")),
    )

    # Load narrative if cached (for S/R lines)
    narrative = None
    narr_path = os.path.join(NARRATIVES_DIR, f"{sim_id}_{safe}.json")
    if os.path.exists(narr_path):
        try:
            narrative = json.loads(open(narr_path).read())
        except Exception:
            pass

    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from charts.trade_chart import generate_trade_chart
        png = generate_trade_chart(
            trade, candle_data, narrative=narrative,
            force_refresh=refresh,
        )
        if isinstance(png, str):
            png = open(png, "rb").read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chart generation failed: {e}")

    return Response(content=png, media_type="image/png")


@app.get("/api/sim/{sim_id}/replay/{trade_index}")
async def get_replay_chart(sim_id: str, trade_index: int):
    """Return trade replay chart PNG. trade_index is 1-based from most recent closed trade."""
    sim_id = sim_id.upper()
    data = _load_sim(sim_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Sim not found: {sim_id}")

    trades = [t for t in (data.get("trade_log") or []) if t.get("exit_time")]
    if not trades:
        raise HTTPException(status_code=404, detail="No closed trades")
    if trade_index < 1 or trade_index > len(trades):
        raise HTTPException(status_code=404, detail=f"Trade index out of range (1–{len(trades)})")

    trade = trades[-trade_index]
    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from interface.charting import generate_trade_replay

        chart_path = await asyncio.to_thread(generate_trade_replay, sim_id, trade)
        if chart_path and os.path.exists(chart_path):
            return Response(content=open(chart_path, "rb").read(), media_type="image/png")

        # Fallback placeholder if candle data unavailable
        from charts.trade_chart import _render_placeholder
        placeholder = _render_placeholder(
            f"Price data unavailable for this trade's time range\n"
            f"({trade.get('entry_time', '')[:10]})",
            (800, 400),
        )
        return Response(content=placeholder, media_type="image/png")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Chart generation failed: {exc}")


@app.get("/api/trades/{sim_id}/{trade_id}/narrative")
async def get_narrative(sim_id: str, trade_id: str):
    """Return cached GPT narrative JSON, or 404 if not yet generated."""
    sim_id = sim_id.upper()
    safe   = re.sub(r"[^\w\-]", "_", trade_id)
    path   = os.path.join(NARRATIVES_DIR, f"{sim_id}_{safe}.json")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Narrative not yet generated")
    try:
        return json.loads(open(path).read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/trades/{sim_id}/{trade_id}/narrate")
async def trigger_narrative(sim_id: str, trade_id: str, force: bool = False):
    """Trigger GPT narrative generation. Returns narrative JSON."""
    sim_id = sim_id.upper()
    trade, profile = _get_trade_by_id(sim_id, trade_id)
    if trade is None:
        raise HTTPException(status_code=404, detail="Trade not found")

    candle_data = _get_candle_window(
        trade.get("entry_time", ""),
        trade.get("exit_time", ""),
        symbol=trade.get("symbol") or _parse_underlying(trade.get("option_symbol", "")),
    )

    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from analytics.trade_narrator import narrate_trade
        narrative = await narrate_trade(trade, candle_data, profile, force_refresh=force)
        # Invalidate chart cache so it re-renders with new S/R lines
        safe  = re.sub(r"[^\w\-]", "_", trade_id)
        chart = os.path.join(CHARTS_DIR, f"{sim_id}_{safe}.png")
        if force and os.path.exists(chart):
            os.remove(chart)
        return narrative
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/strategy_performance")
async def api_strategy_performance():
    """Return strategy performance store data for dashboard heatmaps."""
    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from analytics.strategy_performance import PERF_STORE
        PERF_STORE._load()   # reload from disk in case exits happened since process start
        return PERF_STORE._data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    print(f"Starting SpyBot Dashboard on http://0.0.0.0:{port}")
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=port, reload=False)
