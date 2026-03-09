"""
SpyBot Classroom Dashboard
--------------------------
Run from repo root:
    python -m dashboard.app
or:
    uvicorn dashboard.app:app --host 0.0.0.0 --port 8080 --reload

Access at: http://<laptop-ip>:8080
"""

import json
import os
import math
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIM_DIR = os.path.join(BASE_DIR, "data", "sims")
PREDICTIONS_CSV = os.path.join(BASE_DIR, "data", "predictions.csv")
CANDLES_CSV = os.path.join(BASE_DIR, "data", "qqq_1m.csv")
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")
HEARTBEAT_PATH = os.path.join(BASE_DIR, "data", "heartbeat.json")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="SpyBot Dashboard", docs_url=None, redoc_url=None)
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

    # Current open trade summary
    open_summary = None
    if open_trades:
        t = open_trades[0]
        open_summary = {
            "direction": t.get("direction"),
            "option_symbol": t.get("option_symbol"),
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

    return {
        "sim_id": sim_id,
        "name": profile.get("name", sim_id),
        "signal_mode": profile.get("signal_mode", ""),
        "strategy_family": profile.get("signal_mode", "").lower().replace("_", " ").title(),
        "features_enabled": bool(profile.get("features_enabled")),
        "horizon": profile.get("horizon", ""),
        "dte_min": profile.get("dte_min"),
        "dte_max": profile.get("dte_max"),
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
        "best_trade": round(best, 2) if best is not None else None,
        "worst_trade": round(worst, 2) if worst is not None else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
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
        data = _load_sim(sim_id)
        if data is None:
            results.append({
                "sim_id": sim_id,
                "name": profile.get("name", sim_id),
                "signal_mode": profile.get("signal_mode", ""),
                "features_enabled": bool(profile.get("features_enabled")),
                "horizon": profile.get("horizon", ""),
                "dte_min": profile.get("dte_min"),
                "dte_max": profile.get("dte_max"),
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
        return {
            "sim_id": sim_id,
            "name": profile.get("name", sim_id),
            "profile": {k: v for k, v in profile.items() if not str(k).startswith("_")},
            "stats": None,
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

    # Recent trades (newest first, last 30)
    recent_trades = []
    for t in reversed(trade_log[-30:]):
        recent_trades.append({
            "trade_id": (t.get("trade_id") or "")[-8:],
            "entry_time": t.get("entry_time", ""),
            "exit_time": t.get("exit_time", ""),
            "direction": t.get("direction", ""),
            "option_symbol": t.get("option_symbol", ""),
            "entry_price": t.get("entry_price"),
            "exit_price": t.get("exit_price"),
            "qty": t.get("qty"),
            "pnl": t.get("realized_pnl_dollars"),
            "pnl_pct": t.get("realized_pnl_pct"),
            "exit_reason": t.get("exit_reason", ""),
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
async def get_chart(bars: int = 60):
    """Return last N candles from qqq_1m.csv."""
    try:
        df = pd.read_csv(CANDLES_CSV)
        df.columns = [c.lower() for c in df.columns]
        ts_col = next((c for c in ("timestamp", "time", "datetime", "date") if c in df.columns), None)
        if ts_col is None:
            return {"candles": [], "error": "no_timestamp_col"}
        df = df.sort_values(ts_col).tail(max(bars, 1))
        candles = []
        for _, row in df.iterrows():
            candles.append({
                "t": str(row[ts_col]),
                "o": round(_safe_float(row.get("open")), 4),
                "h": round(_safe_float(row.get("high")), 4),
                "l": round(_safe_float(row.get("low")), 4),
                "c": round(_safe_float(row.get("close")), 4),
                "v": int(_safe_float(row.get("volume"))),
            })
        return {"candles": candles}
    except Exception as e:
        return {"candles": [], "error": str(e)}


@app.get("/api/predictions")
async def get_predictions():
    """Return last 30 minutes of predictions from predictions.csv."""
    try:
        df = pd.read_csv(PREDICTIONS_CSV)
        df.columns = [c.lower() for c in df.columns]
        ts_col = next((c for c in ("time", "timestamp", "datetime") if c in df.columns), None)
        if ts_col is None:
            return {"predictions": [], "latest": None}

        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.dropna(subset=[ts_col])
        df = df.sort_values(ts_col)

        # Handle tz-aware vs naive
        if df[ts_col].dt.tz is not None:
            cutoff = pd.Timestamp.now(tz="US/Eastern") - pd.Timedelta(minutes=30)
        else:
            cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=30)

        recent = df[df[ts_col] >= cutoff].tail(30)

        preds = []
        for _, row in recent.iterrows():
            entry = {"time": str(row[ts_col])}
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
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    print(f"Starting SpyBot Dashboard on http://0.0.0.0:{port}")
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=port, reload=False)
