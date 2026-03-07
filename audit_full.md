# Full Bot State Audit

Generated: 2026-03-02T21:39:36.531047

## 1. Project Structure
(Excluded per user ignore list: __pycache__, venv/.venv, node_modules, dist, build, caches)

├── .claude — Directory
│   └── settings.local.json — JSON config/state
├── .env — File
├── .gitignore — File
├── .vscode — Directory
│   └── settings.json — JSON config/state
├── analytics — Directory
│   ├── adaptive_threshold.py — Python module
│   ├── blocked_signal_tracker.py — analytics/blocked_signal_tracker.py
│   ├── capital_protection.py — analytics/capital_protection.py
│   ├── career_updater.py — analytics/career_updater.py
│   ├── contract_logger.py — analytics/contract_logger.py
│   ├── conviction_stats.py — Python module
│   ├── decision_analysis.py — Python module
│   ├── edge_compression.py — analytics/edge_compression.py
│   ├── edge_decay.py — Python module
│   ├── edge_momentum.py — analytics/edge_momentum.py
│   ├── edge_stability.py — analytics/edge_stability.py
│   ├── equity_curve.py — Python module
│   ├── execution_logger.py — analytics/execution_logger.py
│   ├── expectancy.py — Python module
│   ├── feature_drift.py — Python module
│   ├── feature_importance.py — Python module
│   ├── feature_logger.py — Python module
│   ├── grader.py — analytics/grader.py
│   ├── indicators.py — Python module
│   ├── iv_features.py — Python module
│   ├── market_regime.py — Python module
│   ├── ml_accuracy.py — Python module
│   ├── ml_loader.py — Python module
│   ├── options_greeks.py — Python module
│   ├── performance.py — analytics/performance.py
│   ├── prediction_stats.py — analytics/prediction_stats.py
│   ├── progressive_influence.py — Python module
│   ├── regime_expectancy.py — Python module
│   ├── regime_memory.py — analytics/regime_memory.py
│   ├── regime_persistence.py — analytics/regime_persistence.py
│   ├── regime_transition.py — analytics/regime_transition.py
│   ├── review_engine.py — Python module
│   ├── risk_control.py — analytics/risk_control.py
│   ├── risk_metrics.py — analytics/risk_metrics.py
│   ├── run_stats.py — analytics/run_stats.py
│   ├── setup_expectancy.py — Python module
│   ├── setup_intelligence.py — analytics/setup_intelligence.py
│   ├── signal_logger.py — analytics/signal_logger.py
│   ├── sim_features.py — Python module
│   ├── stability_mode.py — analytics/stability_mode.py
│   └── structure_pricing.py — Python module
├── charts — Directory
│   ├── chart.png — File
│   ├── chartqqq.png — File
│   └── live.png — File
├── core — Directory
│   ├── account_repository.py — core/account_repository.py
│   ├── data_integrity.py — Python module
│   ├── data_service.py — core/data_service.py
│   ├── debug.py — Python module
│   ├── decision_context.py — Python module
│   ├── market_clock.py — core/market_clock.py
│   ├── md_state.py — Python module
│   ├── paths.py — Python module
│   ├── rate_limiter.py — Python module
│   ├── session_scope.py — Python module
│   └── startup_sync.py — Python module
├── data — Directory
│   ├── account.json — JSON config/state
│   ├── account.json.bak1 — File
│   ├── account.json.bak2 — File
│   ├── account.json.bak3 — File
│   ├── blocked_signals.csv — CSV data
│   ├── career_stats.json — JSON config/state
│   ├── contract_selection_log.csv — CSV data
│   ├── conviction_expectancy.csv — CSV data
│   ├── edge_stats.json — JSON config/state
│   ├── execution_quality_log.csv — CSV data
│   ├── predictions.csv — CSV data
│   ├── qqq_1m.csv — CSV data
│   ├── signal_log.csv — CSV data
│   ├── sims — Directory
│   │   ├── SIM00.json — JSON config/state
│   │   ├── SIM01.json — JSON config/state
│   │   ├── SIM01.json.bak — File
│   │   ├── SIM02.json — JSON config/state
│   │   ├── SIM02.json.bak — File
│   │   ├── SIM03.json — JSON config/state
│   │   ├── SIM03.json.bak — File
│   │   ├── SIM04.json — JSON config/state
│   │   ├── SIM04.json.bak — File
│   │   ├── SIM05.json — JSON config/state
│   │   ├── SIM05.json.bak — File
│   │   ├── SIM06.json — JSON config/state
│   │   ├── SIM06.json.bak — File
│   │   ├── SIM07.json — JSON config/state
│   │   ├── SIM07.json.bak — File
│   │   ├── SIM08.json — JSON config/state
│   │   ├── SIM08.json.bak — File
│   │   ├── SIM09.json — JSON config/state
│   │   ├── SIM09.json.bak — File
│   │   ├── SIM10.json — JSON config/state
│   │   └── SIM11.json — JSON config/state
│   └── trade_features.csv — CSV data
├── decision — Directory
│   └── trader.py — decision/trader.py
├── execution — Directory
│   ├── ml_gate.py — Python module
│   └── option_executor.py — Python module
├── interface — Directory
│   ├── ai_assistant.py — Python module
│   ├── bot.py — ==============================
│   ├── charting.py — interface/charting.py
│   ├── fmt.py — ANSI formatting helpers for Discord embeds.
│   ├── health_monitor.py — interface/health_monitor.py
│   └── watchers.py — interface/watchers.py
├── logs — Directory
│   └── recorder.py — logs/recorder.py
├── package-lock.json — JSON config/state
├── package.json — JSON config/state
├── pyrightconfig.json — JSON config/state
├── research — Directory
│   └── train_ai.py — Python module
├── runbot.sh — Shell script
├── signals — Directory
│   ├── conviction.py — signals/conviction.py
│   ├── environment_filter.py — signals/environment_filter.py
│   ├── opportunity.py — signal/opportunity.py
│   ├── predictor.py — signals/predictor.py
│   ├── regime.py — signals/regime.py
│   ├── session_classifier.py — signals/session_classifier.py
│   ├── setup_classifier.py — signals/setup_classifier.py
│   ├── signal_evaluator.py — signals/signal_evaluator.py
│   └── volatility.py — signals/volatility.py
├── simulation — Directory
│   ├── sim_config.yaml — YAML config
│   ├── sim_contract.py — Python module
│   ├── sim_engine.py — Python module
│   ├── sim_evaluation.py — Python module
│   ├── sim_executor.py — Python module
│   ├── sim_live_router.py — Python module
│   ├── sim_metrics.py — Python module
│   ├── sim_ml.py — Python module
│   ├── sim_portfolio.py — simulation/sim_portfolio.py
│   ├── sim_signals.py — Python module
│   ├── sim_validator.py — Python module
│   └── sim_watcher.py — simulation/sim_watcher.py
├── start_recorder.sh — Shell script
└── system.log — Log file

## 2. Architecture Overview

### 2.1 What this bot does
- Trades SPY options (calls/puts) with live trading and multiple simulation profiles.
- Strategies include trend pullback, mean reversion, breakout, ORB breakout, swing trend, and opportunity-following.

### 2.2 Modes
- Hybrid: live trading (SIM00 via live execution) plus simulations (SIM01–SIM11).

### 2.3 Broker/Data APIs
- Alpaca Trading API (orders/account).
- Alpaca Market Data (option snapshots and historical data).
- CSV recorder as a local market data source with fallback to Alpaca.

### 2.4 Deployment
- Local machine (runbot.sh restarts `python -m interface.bot` in a loop).

### 2.5 Main execution loop
- `interface.bot` starts Discord bot and schedules watcher tasks.
- Watchers poll market data, compute signals, place orders (live) or simulate orders (sims).
- Open trades are managed in `decision/trader.py` (live) and `simulation/sim_engine.py` (sims).
- Exits handled by stop/target/trailing/time-based rules; results logged to CSV/JSON and Discord.

## 3. File-by-File Code Dump

### 3.1 Python files

#### `analytics/adaptive_threshold.py`
```python
from analytics.edge_stability import calculate_edge_stability
from analytics.ml_accuracy import ml_rolling_accuracy
from core.account_repository import load_account


def adaptive_ml_threshold(total_trades):
    """
    Institutional-grade adaptive ML threshold.

    Adjusted by:
    - Sample size
    - Edge stability
    - Current drawdown pressure
    """

    # ========================
    # 1️⃣ Base Threshold Curve
    # ========================

    if total_trades < 20:
        base = 0.55
    elif total_trades < 50:
        base = 0.58
    elif total_trades < 100:
        base = 0.60
    else:
        base = 0.62

    # ========================
    # 2️⃣ Edge Stability Adjustment
    # ========================

    stability_data = calculate_edge_stability()

    stability_adjustment = 0

    if stability_data:

        stability = stability_data["stability"]

        # Scale adjustment strength by sample size
        sample_scale = min(total_trades / 100, 1)

        # Stability centered at 0.5
        stability_shift = (stability - 0.5)

        # Max ±0.06 adjustment when fully scaled
        stability_adjustment = stability_shift * 0.06 * sample_scale
    dynamic_threshold = base - stability_adjustment

    ml_stats = ml_rolling_accuracy()
    if ml_stats:
        acc = ml_stats.get("accuracy")
        if acc is not None:
            # If ML performing poorly → tighten threshold
            if acc < 52:
                dynamic_threshold += 0.03

            # If ML strong → slightly loosen
            if acc > 60:
                dynamic_threshold -= 0.02

    # ========================
    # 3️⃣ Drawdown Pressure
    # ========================

    acc = load_account()

    peak = acc.get("peak_balance", acc["starting_balance"])
    balance = acc.get("balance", 0)

    if peak > 0:
        drawdown = (peak - balance) / peak
    else:
        drawdown = 0

    # If >5% drawdown → tighten
    if drawdown > 0.05:
        drawdown_adjustment = min(drawdown * 0.15, 0.05)
    else:
        drawdown_adjustment = 0

    # ========================
    # 4️⃣ Final Threshold
    # ========================

    dynamic_threshold = dynamic_threshold + drawdown_adjustment

    # Hard clamp safety
    dynamic_threshold = max(0.50, min(dynamic_threshold, 0.72))

    return round(dynamic_threshold, 3)
```

#### `analytics/blocked_signal_tracker.py`
```python
# analytics/blocked_signal_tracker.py
#
# Logs every blocked signal with SPY price at decision time, then
# retrospectively fills in forward SPY returns (+5m, +15m) so you can
# measure the "cost" of each gate — i.e. would the trade have worked?
#
# Call log_blocked_signal() immediately after a blocked decision.
# Call update_blocked_outcomes(df) periodically (e.g. from conviction_watcher)
# to fill in the forward-return columns once market data is available.

import os
import csv
from datetime import datetime, timedelta

import pandas as pd
from pandas.errors import EmptyDataError
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "blocked_signals.csv")
HEADERS = [
    "timestamp",
    "spy_price",
    "regime",
    "volatility",
    "direction",          # direction_60m (primary bias)
    "confidence",         # confidence_60m
    "blended_score",
    "threshold",
    "threshold_delta",
    "block_reason",
    # forward return columns — filled by update_blocked_outcomes()
    "fwd_5m",
    "fwd_15m",
    "fwd_5m_price",
    "fwd_15m_price",
    "fwd_5m_status",      # "filled" | "estimated" | "pending"
    "fwd_15m_status",
]


def _ensure_file() -> None:
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe_round(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_blocked_signal(ctx, spy_price) -> None:
    """
    Write one row per blocked auto_trader cycle.

    Parameters
    ----------
    ctx       : DecisionContext after open_trade_if_valid() returned blocked
    spy_price : float — current SPY price at decision time (pass df.iloc[-1]["close"])
    """
    try:
        _ensure_file()

        blended = getattr(ctx, "blended_score", None)
        threshold = getattr(ctx, "threshold", None)
        delta = (
            _safe_round(blended - threshold, 6)
            if blended is not None and threshold is not None
            else ""
        )

        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            _safe_round(spy_price),
            getattr(ctx, "regime", None) or "",
            getattr(ctx, "volatility", None) or "",
            getattr(ctx, "direction_60m", None) or "",
            _safe_round(getattr(ctx, "confidence_60m", None)),
            _safe_round(blended),
            _safe_round(threshold),
            delta,
            getattr(ctx, "block_reason", None) or "",
            "", "", "", "", "pending", "pending",   # forward-return placeholders
        ]

        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass


def update_blocked_outcomes(df=None) -> None:
    """
    Fill in fwd_5m / fwd_15m columns for rows that are still "pending".
    Call this from conviction_watcher (or any periodic task) so the update
    happens in the same cycle that already has a fresh market DataFrame.

    Only rows whose status is not already "filled" are updated.
    Writes back to CSV only when at least one row changed.
    """
    try:
        if df is None:
            from core.data_service import get_market_dataframe
            df = get_market_dataframe()
        if df is None:
            return

        _ensure_file()

        try:
            signals = pd.read_csv(FILE, parse_dates=["timestamp"])
        except (EmptyDataError, Exception):
            return

        if signals.empty:
            return

        # Ensure all forward-return columns exist
        for col in HEADERS:
            if col not in signals.columns:
                signals[col] = ""

        signals["timestamp"] = pd.to_datetime(signals["timestamp"], errors="coerce")
        signals = signals.dropna(subset=["timestamp"])
        if signals.empty:
            return

        # Build aligned market series
        mdf = df.reset_index()
        if "timestamp" not in mdf.columns and "index" in mdf.columns:
            mdf.rename(columns={"index": "timestamp"}, inplace=True)
        mdf["timestamp"] = pd.to_datetime(mdf["timestamp"], errors="coerce")
        mdf = (
            mdf.dropna(subset=["timestamp", "close"])
            .drop_duplicates("timestamp")
            .sort_values("timestamp")
        )
        if mdf.empty:
            return

        # Strip tz so numpy comparison works uniformly
        try:
            if signals["timestamp"].dt.tz is not None:
                signals["timestamp"] = (
                    signals["timestamp"].dt.tz_convert("US/Eastern").dt.tz_localize(None)
                )
        except Exception:
            pass
        try:
            if mdf["timestamp"].dt.tz is not None:
                mdf["timestamp"] = (
                    mdf["timestamp"].dt.tz_convert("US/Eastern").dt.tz_localize(None)
                )
        except Exception:
            pass

        market_ts = mdf["timestamp"].to_numpy(dtype="datetime64[ns]")
        market_close = mdf["close"].to_numpy()

        def _lookup(target_time):
            """Return (price, status) for the candle closest to target_time."""
            if len(market_ts) == 0:
                return None, "no_market_data"
            try:
                t64 = pd.Timestamp(target_time).to_datetime64()
            except Exception:
                return None, "invalid_time"
            idx = market_ts.searchsorted(t64)
            candidate = min(idx, len(market_ts) - 1)
            delta_sec = abs(
                (pd.Timestamp(market_ts[candidate]) - pd.Timestamp(t64)).total_seconds()
            )
            price = float(market_close[candidate])
            if delta_sec <= 120:
                return price, "filled"
            return price, "estimated"

        changed = False
        for i, row in signals.iterrows():
            try:
                base_price = float(row["spy_price"])
            except (TypeError, ValueError):
                continue

            ts = row["timestamp"]

            # 5-minute forward return
            if row.get("fwd_5m_status") != "filled":
                p5, s5 = _lookup(ts + timedelta(minutes=5))
                if p5 is not None:
                    signals.loc[i, "fwd_5m_price"] = round(p5, 4)
                    signals.loc[i, "fwd_5m"] = round(p5 - base_price, 4)
                    signals.loc[i, "fwd_5m_status"] = s5
                    changed = True

            # 15-minute forward return
            if row.get("fwd_15m_status") != "filled":
                p15, s15 = _lookup(ts + timedelta(minutes=15))
                if p15 is not None:
                    signals.loc[i, "fwd_15m_price"] = round(p15, 4)
                    signals.loc[i, "fwd_15m"] = round(p15 - base_price, 4)
                    signals.loc[i, "fwd_15m_status"] = s15
                    changed = True

        if changed:
            signals.to_csv(FILE, index=False)
    except Exception:
        pass
```

#### `analytics/capital_protection.py`
```python
# analytics/capital_protection.py

from core.account_repository import load_account
from analytics.edge_stability import calculate_edge_stability


def get_capital_mode():

    acc = load_account()

    balance = acc.get("balance", 0)
    starting = acc.get("starting_balance", balance)
    daily_loss = acc.get("daily_loss", 0)
    max_daily = acc.get("max_daily_loss", 1)

    drawdown = 0
    if starting > 0:
        drawdown = (starting - balance) / starting

    stability_data = calculate_edge_stability()
    stability = stability_data["stability"] if stability_data else 0.5

    # ---------------------------------
    # MODE LOGIC
    # ---------------------------------

    # 🚨 LOCKDOWN
    if drawdown >= 0.20:
        return {
            "mode": "LOCKDOWN",
            "risk_multiplier": 0.0,
            "threshold_buffer": 0.15
        }

    # 🔴 CRITICAL
    if drawdown >= 0.12 or daily_loss >= max_daily:
        return {
            "mode": "CRITICAL",
            "risk_multiplier": 0.4,
            "threshold_buffer": 0.08
        }

    # 🟡 DEFENSIVE
    if drawdown >= 0.07 or stability < 0.45:
        return {
            "mode": "DEFENSIVE",
            "risk_multiplier": 0.7,
            "threshold_buffer": 0.04
        }

    # 🟢 NORMAL
    return {
        "mode": "NORMAL",
        "risk_multiplier": 1.0,
        "threshold_buffer": 0.0
    }
```

#### `analytics/career_updater.py`
```python
# analytics/career_updater.py

from core.account_repository import load_career, save_career
from signals.session_classifier import classify_session


def update_career_after_trade(trade, result, pnl, new_balance):

    career = load_career()

    career["total_trades_all_time"] += 1

    if result == "win":
        career["total_wins_all_time"] += 1
    else:
        career["total_losses_all_time"] += 1

    # ----- Best Balance
    if new_balance > career["best_balance"]:
        career["best_balance"] = new_balance

    # ----- Time of Day
    session = classify_session(trade["entry_time"])
    if session in career["time_of_day"]:
        if result == "win":
            career["time_of_day"][session]["wins"] += 1
        else:
            career["time_of_day"][session]["losses"] += 1
        career["time_of_day"][session]["pnl"] += pnl

    # ----- Setup Tracking
    setup = trade.get("setup", "UNKNOWN")
    if setup in career["setups"]:
        if result == "win":
            career["setups"][setup]["wins"] += 1
        else:
            career["setups"][setup]["losses"] += 1

    # ----- Confidence Calibration
    conf = trade.get("confidence", 0) * 100

    bucket = None
    if 50 <= conf < 60:
        bucket = "50-60"
    elif 60 <= conf < 70:
        bucket = "60-70"
    elif 70 <= conf < 80:
        bucket = "70-80"
    elif conf >= 80:
        bucket = "80-100"

    if bucket:
        career["confidence"][bucket]["total"] += 1
        if result == "win":
            career["confidence"][bucket]["correct"] += 1

    save_career(career)
```

#### `analytics/contract_logger.py`
```python
# analytics/contract_logger.py
#
# Logs every option contract selection attempt (success or failure) from both
# the main trader and the sim engine.  Useful for measuring:
#   - How often are chains empty / bid=0 off-hours?
#   - Which strikes/expiries actually have liquid quotes?
#   - What IV/delta the system traded at vs. what was available.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "contract_selection_log.csv")
HEADERS = [
    "timestamp",
    "source",            # "main" | "sim:<sim_id>"
    "direction",
    "underlying_price",
    "expiry",
    "dte",
    "strike",
    "result",            # "selected" | "rejected" | "error"
    "reason",            # e.g. "spread_too_wide", "no_snapshot", "selected"
    "bid",
    "ask",
    "mid",
    "spread_pct",
    # greeks — None when snapshot has no greeks data
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
]


def _ensure_file() -> None:
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_contract_attempt(
    source: str,
    direction: str,
    underlying_price,
    expiry,
    dte,
    strike,
    result: str,
    reason: str,
    bid=None,
    ask=None,
    mid=None,
    spread_pct=None,
    iv=None,
    delta=None,
    gamma=None,
    theta=None,
    vega=None,
) -> None:
    """
    Log one row per contract candidate evaluated.

    Call for every strike tried (success or rejection) so you get a complete
    picture of chain quality at any given time.
    """
    try:
        _ensure_file()
        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            source,
            direction,
            _safe(underlying_price),
            str(expiry) if expiry is not None else "",
            dte if dte is not None else "",
            _safe(strike),
            result,
            reason or "",
            _safe(bid),
            _safe(ask),
            _safe(mid),
            _safe(spread_pct),
            _safe(iv),
            _safe(delta),
            _safe(gamma, decimals=6),
            _safe(theta),
            _safe(vega),
        ]
        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass
```

#### `analytics/conviction_stats.py`
```python
import os
import csv
import pandas as pd
from pandas.errors import EmptyDataError
from datetime import timedelta
from core.paths import DATA_DIR
from core.data_service import get_market_dataframe

FILE = os.path.join(DATA_DIR, "conviction_expectancy.csv") 
HEADERS = [
    "time",
    "direction",
    "impulse",
    "follow",
    "price",
    "fwd_5m",
    "fwd_10m",
    "fwd_5m_price",
    "fwd_5m_time",
    "fwd_5m_status",
    "fwd_10m_price",
    "fwd_10m_time",
    "fwd_10m_status",
]


def ensure_conviction_file():
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        try:
            with open(FILE, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                raise ValueError("empty_file")
            header = rows[0]
            if header != HEADERS:
                padded = []
                for row in rows[1:]:
                    if not row:
                        continue
                    if row[0] == "time":
                        continue
                    new_row = row[:len(HEADERS)]
                    if len(new_row) < len(HEADERS):
                        new_row += [""] * (len(HEADERS) - len(new_row))
                    padded.append(new_row)
                with open(FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(HEADERS)
                    writer.writerows(padded)
        except Exception:
            pass
        return

    with open(FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)


def get_last_logged_time():
    if not os.path.exists(FILE) or os.path.getsize(FILE) == 0:
        return None

    last_row = None
    with open(FILE, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            if row[0] == "time":
                continue
            last_row = row

    if not last_row:
        return None
    return last_row[0]

# ==================================
# 1️⃣ LOG SIGNAL
# ==================================

def log_conviction_signal(df, direction, impulse, follow):
    if df is None or df.empty:
        print("No data to log for conviction signal.")
        return

    ensure_conviction_file()

    last = df.iloc[-1]  # Check for the last valid data row
    if pd.isna(last["close"]):
        print("Invalid data in the last row, skipping log.")
        return

    timestamp = last.name if hasattr(last, "name") else None
    if timestamp is None and "timestamp" in df.columns:
        timestamp = df["timestamp"].iloc[-1]
    if timestamp is None:
        print("Missing timestamp, skipping conviction log.")
        return

    timestamp_iso = pd.to_datetime(timestamp).isoformat()
    last_logged_time = get_last_logged_time()
    # Normalize both to isoformat before comparing (pandas to_csv uses space separator,
    # isoformat() uses T separator — without normalization the dedup check never fires)
    try:
        last_logged_normalized = pd.to_datetime(last_logged_time).isoformat() if last_logged_time else None
    except Exception:
        last_logged_normalized = None
    if last_logged_normalized == timestamp_iso:
        return

    with open(FILE, "a", newline="") as f:
        writer = csv.writer(f)

        # Safely log the conviction signal
        writer.writerow([
            timestamp_iso,
            str(direction),
            round(float(impulse), 3),
            round(float(follow), 3),
            float(last["close"]),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ])
    print(f"Logged conviction signal at {timestamp}.")


# ==================================
# 2️⃣ UPDATE EXPECTANCY
# ==================================

def update_expectancy(df=None):
    if df is None:
        df = get_market_dataframe()

    if df is None:
        print("No data available or file missing.")
        return

    ensure_conviction_file()

    try:
        signals = pd.read_csv(FILE, parse_dates=["time"])
    except EmptyDataError:
        ensure_conviction_file()
        return
    except Exception:
        ensure_conviction_file()
        return

    for col in HEADERS:
        if col not in signals.columns:
            if col in {"fwd_5m_time", "fwd_10m_time"}:
                signals[col] = pd.NaT
            elif col in {"fwd_5m_status", "fwd_10m_status"}:
                signals[col] = pd.Series([None] * len(signals), dtype="object")
            else:
                signals[col] = None

    signals["time"] = pd.to_datetime(signals["time"], errors="coerce")
    signals = signals.dropna(subset=["time"])

    # Force numeric types safely
    signals["price"] = pd.to_numeric(signals["price"], errors="coerce")
    signals["fwd_5m"] = pd.to_numeric(signals["fwd_5m"], errors="coerce")
    signals["fwd_10m"] = pd.to_numeric(signals["fwd_10m"], errors="coerce")
    signals["fwd_5m_price"] = pd.to_numeric(signals["fwd_5m_price"], errors="coerce")
    signals["fwd_10m_price"] = pd.to_numeric(signals["fwd_10m_price"], errors="coerce")
    signals["fwd_5m_time"] = pd.to_datetime(signals["fwd_5m_time"], errors="coerce")
    signals["fwd_10m_time"] = pd.to_datetime(signals["fwd_10m_time"], errors="coerce")
    signals["fwd_5m_status"] = signals["fwd_5m_status"].astype("object")
    signals["fwd_10m_status"] = signals["fwd_10m_status"].astype("object")

    if signals.empty:
        print("No conviction signals available.")
        return

    df = df.reset_index()
    if "timestamp" not in df.columns and "index" in df.columns:
        df.rename(columns={"index": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    try:
        signals_tz = signals["time"].dt.tz
    except Exception:
        signals_tz = None
    if signals_tz is not None:
        signals["time"] = signals["time"].dt.tz_convert("US/Eastern").dt.tz_localize(None)

    try:
        df_tz = df["timestamp"].dt.tz
    except Exception:
        df_tz = None
    if df_tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_convert("US/Eastern").dt.tz_localize(None)

    market = df[["timestamp", "close"]].dropna(subset=["timestamp", "close"]).copy()
    if market.empty:
        print("No market timestamps available for expectancy update.")
        return
    market = market.drop_duplicates(subset=["timestamp"], keep="last")
    market = market.sort_values("timestamp")
    market["timestamp"] = pd.to_datetime(market["timestamp"], errors="coerce")
    market = market.dropna(subset=["timestamp"])
    if market.empty:
        print("No market timestamps available for expectancy update.")
        return
    market_ts = market["timestamp"].to_numpy(dtype="datetime64[ns]")
    market_close = market["close"].to_numpy()

    def _select_future(target_time):
        if len(market_ts) == 0:
            return None, None, "no_market_data"
        try:
            target_dt = pd.Timestamp(target_time)
        except Exception:
            return None, None, "invalid_target_time"
        if bool(pd.isna(target_dt)):
            return None, None, "invalid_target_time"
        target_dt64 = target_dt.to_datetime64()
        idx = market_ts.searchsorted(target_dt64)
        candidate_idx = None
        if idx < len(market_ts):
            candidate_idx = idx
            if idx > 0:
                prev_ts = market_ts[idx - 1]
                curr_ts = market_ts[idx]
                prev_delta = abs((pd.Timestamp(target_dt64) - pd.Timestamp(prev_ts)).total_seconds())
                curr_delta = abs((pd.Timestamp(curr_ts) - pd.Timestamp(target_dt64)).total_seconds())
                if prev_delta <= curr_delta:
                    candidate_idx = idx - 1
        else:
            candidate_idx = len(market_ts) - 1

        ts = market_ts[candidate_idx]
        price = market_close[candidate_idx]
        ts_val = pd.Timestamp(ts)
        target_val = pd.Timestamp(target_dt64)
        if ts_val is pd.NaT or target_val is pd.NaT:
            return None, None, "no_market_data"
        delta_sec = abs((ts_val - target_val).total_seconds())
        if delta_sec <= 120:
            return price, ts, "filled"
        if ts_val.value < target_val.value:
            return price, ts, "estimated_last"
        return price, ts, "estimated_gap"

    for i, row in signals.iterrows():
        if bool(pd.isna(row["time"])) or bool(pd.isna(row["price"])):
            continue

        base_price = row["price"]
        if bool(pd.isna(base_price)):
            continue

        future_5 = row["time"] + timedelta(minutes=5)
        future_10 = row["time"] + timedelta(minutes=10)

        fwd_5m_val = row.get("fwd_5m")
        if fwd_5m_val is None or bool(pd.isna(fwd_5m_val)) or row.get("fwd_5m_status") != "filled":
            price_5, ts_5, status_5 = _select_future(future_5)
            if price_5 is not None:
                signals.loc[i, "fwd_5m_price"] = price_5
                if ts_5 is not None:
                    signals.loc[i, "fwd_5m_time"] = pd.Timestamp(ts_5)
                signals.loc[i, "fwd_5m"] = price_5 - base_price
            signals.loc[i, "fwd_5m_status"] = status_5

        fwd_10m_val = row.get("fwd_10m")
        if fwd_10m_val is None or bool(pd.isna(fwd_10m_val)) or row.get("fwd_10m_status") != "filled":
            price_10, ts_10, status_10 = _select_future(future_10)
            if price_10 is not None:
                signals.loc[i, "fwd_10m_price"] = price_10
                if ts_10 is not None:
                    signals.loc[i, "fwd_10m_time"] = pd.Timestamp(ts_10)
                signals.loc[i, "fwd_10m"] = price_10 - base_price
            signals.loc[i, "fwd_10m_status"] = status_10

    # Only save the data if valid
    try:
        signals.to_csv(FILE, index=False)
        print("Conviction expectancy updated successfully.")
    except Exception as e:
        print(f"Error while saving conviction expectancy: {e}")


def get_conviction_expectancy_stats():
    ensure_conviction_file()

    try:
        df = pd.read_csv(FILE)
    except EmptyDataError:
        ensure_conviction_file()
        return None
    if not isinstance(df, pd.DataFrame) or df.empty:
        print("No data available in conviction expectancy file.")
        return None

    if "fwd_5m_status" in df.columns:
        df = df[df["fwd_5m_status"] == "filled"]
    if "fwd_10m_status" in df.columns:
        df = df[df["fwd_10m_status"] == "filled"]

    # Drop rows where 'fwd_5m' or 'fwd_10m' are NaN
    if isinstance(df, pd.DataFrame):
        df = df.dropna(subset=["fwd_5m", "fwd_10m"])

    # Check if the remaining data is too sparse
    if len(df) < 5:
        print("Not enough data to calculate conviction expectancy.")
        return {
            "avg_5m": None,
            "avg_10m": None,
            "wr_5m": None,
            "wr_10m": None,
            "samples": len(df)
        }

    avg_5m = df["fwd_5m"].mean()
    avg_10m = df["fwd_10m"].mean()

    winrate_5m = (df["fwd_5m"] > 0).mean() * 100
    winrate_10m = (df["fwd_10m"] > 0).mean() * 100

    return {
        "avg_5m": round(avg_5m, 4),
        "avg_10m": round(avg_10m, 4),
        "wr_5m": round(winrate_5m, 1),
        "wr_10m": round(winrate_10m, 1),
        "samples": len(df)
    }
```

#### `analytics/decision_analysis.py`
```python
import pandas as pd
from typing import cast


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def analyze_decision_quality(trade_log):
    rows = []

    for trade in trade_log or []:
        snapshot = trade.get("decision_snapshot")
        if not isinstance(snapshot, dict):
            continue

        r_val = trade.get("R")
        if r_val is None:
            continue

        row = {
            "R": _safe_float(r_val),
            "blended_score": _safe_float(snapshot.get("blended_score")),
            "threshold": _safe_float(snapshot.get("threshold")),
            "threshold_delta": _safe_float(snapshot.get("threshold_delta")),
            "ml_weight": _safe_float(snapshot.get("ml_weight")),
            "regime_samples": _safe_float(snapshot.get("regime_samples")),
            "expectancy_samples": _safe_float(snapshot.get("expectancy_samples")),
            "dual_alignment": bool(snapshot.get("dual_alignment")) if snapshot.get("dual_alignment") is not None else None,
            "confidence_15m": _safe_float(snapshot.get("confidence_15m")),
            "confidence_60m": _safe_float(snapshot.get("confidence_60m")),
            "spread_pct": None,
            "slippage_pct": None,
            "win": None,
        }

        bid = _safe_float(trade.get("bid"))
        ask = _safe_float(trade.get("ask"))
        if bid is not None and ask is not None and ask > 0 and ask >= bid:
            row["spread_pct"] = (ask - bid) / ask

        expected_mid = _safe_float(trade.get("expected_mid"))
        if expected_mid is None and bid is not None and ask is not None:
            expected_mid = (bid + ask) / 2

        fill_price = _safe_float(trade.get("entry_price"))
        if fill_price is None:
            fill_price = _safe_float(trade.get("fill_price"))

        if expected_mid is not None and fill_price is not None and expected_mid > 0:
            row["slippage_pct"] = (fill_price - expected_mid) / expected_mid

        result = trade.get("result")
        if result is not None:
            row["win"] = str(result).lower() == "win"
        else:
            row["win"] = bool(row["R"] is not None and row["R"] > 0)

        rows.append(row)

    if not rows:
        return {
            "total_trades_analyzed": 0,
            "corr_threshold_delta_vs_R": None,
            "corr_blended_vs_R": None,
            "avg_spread_at_entry": None,
            "avg_slippage_vs_mid": None,
            "winrate_by_delta_quartile": {},
            "winrate_by_ml_weight_quartile": {},
            "winrate_by_spread_quartile": {},
            "regime_maturity_comparison": {},
        }

    df: pd.DataFrame = pd.DataFrame(rows)
    df = df.dropna(subset=["R"])

    if df.empty:
        return {
            "total_trades_analyzed": 0,
            "corr_threshold_delta_vs_R": None,
            "corr_blended_vs_R": None,
            "avg_spread_at_entry": None,
            "avg_slippage_vs_mid": None,
            "winrate_by_delta_quartile": {},
            "winrate_by_ml_weight_quartile": {},
            "winrate_by_spread_quartile": {},
            "regime_maturity_comparison": {},
        }

    def _safe_corr(x_col, y_col):
        sub = df[[x_col, y_col]].dropna()
        if len(sub) < 2:
            return None
        x_series = cast(pd.Series, sub[x_col])
        y_series = cast(pd.Series, sub[y_col])
        val = x_series.corr(y_series)
        if val is None or pd.isna(val):
            return None
        return float(val)

    corr_delta_r = _safe_corr("threshold_delta", "R")
    corr_blended_r = _safe_corr("blended_score", "R")

    spread_sub = df[["spread_pct"]].dropna()
    avg_spread_at_entry = None
    if not spread_sub.empty:
        avg_spread_at_entry = float(spread_sub["spread_pct"].mean())

    slippage_sub = df[["slippage_pct"]].dropna()
    avg_slippage_vs_mid = None
    if not slippage_sub.empty:
        avg_slippage_vs_mid = float(slippage_sub["slippage_pct"].mean())

    def _quartile_winrate(col, label_prefix):
        sub = df[[col, "win"]].dropna()
        if len(sub) < 4:
            return {}
        try:
            q = pd.qcut(sub[col], 4, labels=False, duplicates="drop")
            q_series = cast(pd.Series, q)
            q_values = [int(v) for v in q_series.dropna().unique().tolist()]
        except Exception:
            return {}

        out = {}
        for idx in sorted(q_values):
            mask = q_series == idx
            bucket_df = sub.loc[mask, ["win"]]
            bucket = [bool(v) for v in bucket_df["win"]]
            if len(bucket) == 0:
                continue
            out[f"{label_prefix}_q{int(idx) + 1}"] = {
                "trades": int(len(bucket)),
                "winrate": float(sum(bucket) / len(bucket)),
            }
        return out

    winrate_by_delta_quartile = _quartile_winrate("threshold_delta", "delta")
    winrate_by_ml_weight_quartile = _quartile_winrate("ml_weight", "ml_weight")
    winrate_by_spread_quartile = _quartile_winrate("spread_pct", "spread")

    maturity_sub = df[["regime_samples", "win"]].dropna()
    regime_maturity_comparison = {}
    if not maturity_sub.empty:
        mature_df = maturity_sub.loc[maturity_sub["regime_samples"] >= 20, ["win"]]
        immature_df = maturity_sub.loc[maturity_sub["regime_samples"] < 20, ["win"]]
        mature = [bool(v) for v in mature_df["win"]]
        immature = [bool(v) for v in immature_df["win"]]

        regime_maturity_comparison = {
            "regime_samples_ge_20": {
                "trades": int(len(mature)),
                "winrate": float(sum(mature) / len(mature)) if len(mature) > 0 else None,
            },
            "regime_samples_lt_20": {
                "trades": int(len(immature)),
                "winrate": float(sum(immature) / len(immature)) if len(immature) > 0 else None,
            },
        }

    return {
        "total_trades_analyzed": int(len(df)),
        "corr_threshold_delta_vs_R": corr_delta_r,
        "corr_blended_vs_R": corr_blended_r,
        "avg_spread_at_entry": avg_spread_at_entry,
        "avg_slippage_vs_mid": avg_slippage_vs_mid,
        "winrate_by_delta_quartile": winrate_by_delta_quartile,
        "winrate_by_ml_weight_quartile": winrate_by_ml_weight_quartile,
        "winrate_by_spread_quartile": winrate_by_spread_quartile,
        "regime_maturity_comparison": regime_maturity_comparison,
    }
```

#### `analytics/edge_compression.py`
```python
# analytics/edge_compression.py

from analytics.edge_stability import calculate_edge_stability
from analytics.edge_momentum import calculate_edge_momentum


def get_edge_compression():

    """
    Detects instability and compresses exposure gradually.
    Does NOT disable trading.
    """

    stability_data = calculate_edge_stability()
    momentum_data = calculate_edge_momentum()

    if not stability_data:
        return {
            "active": False,
            "risk_multiplier": 1.0,
            "position_multiplier": 1.0
        }

    stability = stability_data["stability"]

    momentum = 0
    if momentum_data:
        momentum = momentum_data["momentum"]

    compression = 1.0
    active = False

    # ----------------------------------------
    # Trigger Conditions
    # ----------------------------------------

    if stability < 0.45:
        compression *= 0.85
        active = True

    if stability < 0.35:
        compression *= 0.80
        active = True

    if momentum < -0.20:
        compression *= 0.85
        active = True

    # Clamp compression
    compression = max(0.60, compression)

    return {
        "active": active,
        "risk_multiplier": compression,
        "position_multiplier": compression
    }
```

#### `analytics/edge_decay.py`
```python
import os
import json
import numpy as np
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")

def get_recent_r_values(lookback=15):

    if not os.path.exists(ACCOUNT_FILE):
        return []

    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except:
        return []

    trades = acc.get("trade_log", [])

    r_values = []

    for trade in trades[-lookback:]:
        risk = trade.get("risk", 0)
        pnl = trade.get("pnl", 0)

        if risk > 0:
            r_values.append(pnl / risk)

    return r_values


def edge_decay_status():

    r_values = get_recent_r_values()

    if len(r_values) < 8:
        return {"status": "INSUFFICIENT_DATA"}

    avg_r = np.mean(r_values)
    std_r = np.std(r_values)

    # Decay rules
    if avg_r < 0:
        return {
            "status": "WEAK",
            "reason": "Negative recent expectancy",
            "avg_r": round(avg_r, 3)
        }

    if std_r > 2.0:
        return {
            "status": "DISABLE",
            "reason": "Edge unstable (high variance)",
            "std": round(std_r, 3)
        }

    if avg_r < 0.3:
        return {
            "status": "THROTTLE",
            "reason": "Weak expectancy",
            "avg_r": round(avg_r, 3)
        }

    return {"status": "OK"}
```

#### `analytics/edge_momentum.py`
```python
# analytics/edge_momentum.py

from core.account_repository import load_account
import numpy as np


def calculate_edge_momentum():
    """
    Measures short-term expectancy acceleration.

    Compares:
    - Recent 20 trades
    - Recent 50 trades

    Returns:
        {
            "momentum": float (-1 to +1),
            "recent_avg": float,
            "baseline_avg": float
        }
    """

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 30:
        return None

    # Extract R multiples
    Rs = [t.get("R") for t in trades if "R" in t]

    if len(Rs) < 30:
        return None

    recent_20 = Rs[-20:]
    recent_50 = Rs[-50:] if len(Rs) >= 50 else Rs[:-20]

    if len(recent_50) < 20:
        return None

    recent_avg = np.mean(recent_20)
    baseline_avg = np.mean(recent_50)

    # Normalize difference
    diff = recent_avg - baseline_avg

    # Convert to bounded momentum score
    momentum = np.tanh(diff)

    return {
        "momentum": round(momentum, 3),
        "recent_avg": round(recent_avg, 3),
        "baseline_avg": round(baseline_avg, 3)
    }
```

#### `analytics/edge_stability.py`
```python
# analytics/edge_stability.py

import os
import json
import numpy as np
from math import exp
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_edge_stability(lookback=30):
    """
    Composite Edge Stability Model

    Measures:
    - R multiple variance
    - Expectancy trend
    - Winrate consistency
    - Sample confidence

    Returns score between 0 and 1
    """

    if not os.path.exists(ACCOUNT_FILE):
        return None

    try:
        with open(ACCOUNT_FILE, "r") as f:
            account = json.load(f)
    except:
        return None

    trade_log = account.get("trade_log", [])

    if len(trade_log) < 5:
        return {
            "stability": 0.2,  # Low trust early
            "confidence": 0.1,
            "samples": len(trade_log),
            "note": "Low sample size"
        }

    trades = trade_log[-lookback:]

    r_values = []
    wins = 0

    for trade in trades:
        risk = trade.get("risk", 0)
        pnl = trade.get("pnl", 0)

        if risk > 0:
            r = pnl / risk
            r_values.append(r)

            if r > 0:
                wins += 1

    if len(r_values) < 3:
        return None

    r_values = np.array(r_values)

    # ------------------------
    # 1️⃣ Variance Stability
    # ------------------------
    std_dev = np.std(r_values)
    variance_score = 1 / (1 + std_dev)

    # ------------------------
    # 2️⃣ Expectancy Strength
    # ------------------------
    avg_R = np.mean(r_values)
    expectancy_score = 1 / (1 + exp(-avg_R))  # sigmoid scaling

    # ------------------------
    # 3️⃣ Winrate Stability
    # ------------------------
    winrate = wins / len(r_values)
    winrate_stability = 1 - abs(winrate - 0.5)  # penalize extremes early

    # ------------------------
    # 4️⃣ Sample Confidence
    # ------------------------
    sample_size = len(trade_log)
    confidence = 1 - exp(-sample_size / 40)

    # ------------------------
    # Composite Score
    # ------------------------
    composite = (
        variance_score * 0.35 +
        expectancy_score * 0.35 +
        winrate_stability * 0.15 +
        confidence * 0.15
    )

    composite = round(float(composite), 3)

    return {
        "stability": composite,
        "variance_score": round(float(variance_score), 3),
        "expectancy_score": round(float(expectancy_score), 3),
        "winrate_stability": round(float(winrate_stability), 3),
        "confidence": round(float(confidence), 3),
        "avg_R": round(float(avg_R), 3),
        "samples": sample_size
    }
```

#### `analytics/equity_curve.py`
```python
import json
import matplotlib.pyplot as plt
from core.account_repository import load_account
from core.paths import DATA_DIR

def generate_equity_curve():

    acc = load_account()

    trades = acc.get("trade_log", [])

    if len(trades) < 2:
        return None

    balances = [t["balance_after"] for t in trades]
    x = list(range(1, len(balances)+1))

    plt.figure(figsize=(8,4))
    plt.plot(x, balances)
    plt.xlabel("Trade Number")
    plt.ylabel("Account Balance ($)")
    plt.title("AI Trader Equity Curve")
    plt.tight_layout()
    plt.savefig("equity.png")
    plt.close()

    return "equity.png"
```

#### `analytics/execution_logger.py`
```python
# analytics/execution_logger.py
#
# Records execution quality for every real broker fill (entry and exit).
# Captures:
#   - Slippage: (fill_price - expected_mid) / expected_mid
#   - Spread at order time
#   - Partial fill ratio
#   - Exit quality (fill vs. mid at exit time)
#
# Use this to build a realistic slippage model for sim profiles over time.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "execution_quality_log.csv")
HEADERS = [
    "timestamp",
    "option_symbol",
    "side",              # "entry" | "exit"
    "order_type",        # "limit_mid_plus" | "limit_ask" | "market"
    "qty_requested",
    "qty_filled",
    "fill_ratio",
    "expected_mid",      # mid quote at time of order submission
    "fill_price",
    "slippage_pct",      # (fill - expected_mid) / expected_mid  (+ = paid more)
    "bid_at_order",
    "ask_at_order",
    "spread_at_order_pct",
]


def _ensure_file() -> None:
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_execution(
    option_symbol: str,
    side: str,
    order_type: str,
    qty_requested: int,
    qty_filled: int,
    fill_ratio,
    expected_mid,
    fill_price,
    bid_at_order,
    ask_at_order,
) -> None:
    """
    Record one fill event.

    Parameters
    ----------
    side        : "entry" or "exit"
    order_type  : "limit_mid_plus" (first attempt), "limit_ask" (retry), "market"
    expected_mid: mid-price at the moment the order was submitted
    fill_price  : actual fill from broker
    bid_at_order / ask_at_order : quote snapshot at submission time
    """
    try:
        _ensure_file()

        slippage_pct = ""
        try:
            if expected_mid and float(expected_mid) > 0:
                slippage_pct = _safe(
                    (float(fill_price) - float(expected_mid)) / float(expected_mid), 5
                )
        except (TypeError, ValueError):
            pass

        spread_at_order_pct = ""
        try:
            a = float(ask_at_order)
            b = float(bid_at_order)
            if a > 0:
                spread_at_order_pct = _safe((a - b) / a)
        except (TypeError, ValueError):
            pass

        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            option_symbol,
            side,
            order_type,
            qty_requested,
            qty_filled,
            _safe(fill_ratio),
            _safe(expected_mid),
            _safe(fill_price),
            slippage_pct,
            _safe(bid_at_order),
            _safe(ask_at_order),
            spread_at_order_pct,
        ]
        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass
```

#### `analytics/expectancy.py`
```python
from core.account_repository import load_account

def calculate_expectancy():

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 10:
        return None

    Rs = [t["R"] for t in trades if "R" in t]

    avg_R = sum(Rs) / len(Rs)

    win_Rs = [r for r in Rs if r > 0]
    loss_Rs = [r for r in Rs if r < 0]

    winrate = len(win_Rs) / len(Rs)

    avg_win = sum(win_Rs)/len(win_Rs) if win_Rs else 0
    avg_loss = sum(loss_Rs)/len(loss_Rs) if loss_Rs else 0

    expectancy = (winrate * avg_win) + ((1-winrate) * avg_loss)

    return {
        "avg_R": round(avg_R, 3),
        "winrate": round(winrate*100, 1),
        "expectancy": round(expectancy, 3),
        "samples": len(Rs)
    }
```

#### `analytics/feature_drift.py`
```python
import os
import pandas as pd
from pandas.errors import EmptyDataError
from core.paths import DATA_DIR

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")


def detect_feature_drift():
    """
    Detect statistical feature drift using
    rolling Z-score deviation.

    Returns:
        None (if no drift)
        OR structured drift report dict
    """

    if not os.path.exists(FEATURE_FILE):
        return None

    if os.path.getsize(FEATURE_FILE) == 0:
        return None

    try:
        df = pd.read_csv(FEATURE_FILE)
    except (EmptyDataError, Exception):
        return None

    if len(df) < 80:
        return None

    numeric_cols = df.select_dtypes(include="number").columns

    if len(numeric_cols) == 0:
        return None

    # ==========================
    # Split Data
    # ==========================

    recent_window = 40
    baseline_window = len(df) - recent_window

    if baseline_window < 40:
        return None

    recent = df.tail(recent_window)
    baseline = df.head(baseline_window)

    drift_flags = []
    severity_score = 0

    # ==========================
    # Z-score Drift Check
    # ==========================

    for col in numeric_cols:

        try:
            base_mean = float(baseline[col].mean())
            base_std = float(baseline[col].std())
        except Exception:
            continue

        if base_std == 0 or pd.isna(base_std):
            continue

        try:
            recent_mean = float(recent[col].mean())
        except Exception:
            continue

        z_score = abs(recent_mean - base_mean) / base_std

        if z_score > 2.0:
            drift_flags.append(f"{col} Z={round(z_score,2)}")
            severity_score += min(z_score / 4, 1.0)

    if not drift_flags:
        return None

    # Normalize severity
    severity_score = min(severity_score / len(numeric_cols), 1.0)

    return {
        "features": drift_flags,
        "severity": round(severity_score, 3)
    }
```

#### `analytics/feature_importance.py`
```python
import os
import joblib
import pandas as pd
from core.paths import DATA_DIR

EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

FEATURE_NAMES = [
    "regime",
    "volatility",
    "conviction_score",
    "impulse",
    "follow_through",
    "confidence"
]

def get_feature_importance():

    if not os.path.exists(EDGE_MODEL_FILE):
        return None

    model = joblib.load(EDGE_MODEL_FILE)

    if not hasattr(model, "feature_importances_"):
        return None

    importances = model.feature_importances_

    data = sorted(
        zip(FEATURE_NAMES, importances),
        key=lambda x: x[1],
        reverse=True
    )

    return data
```

#### `analytics/feature_logger.py`
```python
import os
import csv
from core.paths import DATA_DIR
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.regime_expectancy import calculate_regime_expectancy
from signals.session_classifier import classify_session


FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")
FEATURE_HEADERS = [
    "regime_encoded",
    "volatility_encoded",
    "conviction_score",
    "impulse",
    "follow_through",
    "confidence",
    "style_encoded",
    "setup_encoded",
    "session_encoded",
    "setup_raw_avg_R",
    "regime_raw_avg_R",
    "ml_probability",
    "predicted_won",
    "won"
]


def ensure_feature_file(reset_if_invalid: bool = False):
    if os.path.exists(FEATURE_FILE) and os.path.getsize(FEATURE_FILE) > 0:
        try:
            with open(FEATURE_FILE, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                raise ValueError("empty_file")
            header = rows[0]
            if header != FEATURE_HEADERS:
                if reset_if_invalid:
                    with open(FEATURE_FILE, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(FEATURE_HEADERS)
                return
        except Exception:
            if reset_if_invalid:
                with open(FEATURE_FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(FEATURE_HEADERS)
        return

    with open(FEATURE_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FEATURE_HEADERS)


# ----------------------------
# Encoders
# ----------------------------

REGIME_MAP = {
    "TREND": 1,
    "RANGE": 2,
    "VOLATILE": 3,
    "COMPRESSION": 4,
    "NO_DATA": 0
}

VOL_MAP = {
    "DEAD": 0,
    "LOW": 1,
    "NORMAL": 2,
    "HIGH": 3
}

SETUP_MAP = {
    "BREAKOUT": 1,
    "PULLBACK": 2,
    "REVERSAL": 3,
    "UNKNOWN": 0
}

STYLE_MAP = {
    "scalp": 1,
    "mini_swing": 2,
    "momentum": 3
}

SESSION_MAP = {
    "OPEN": 1,
    "MIDDAY": 2,
    "AFTERNOON": 3,
    "POWER": 4,
    "UNKNOWN": 0
}


# ----------------------------
# Main Logger
# ----------------------------

def log_trade_features(trade, result, pnl):
    ensure_feature_file(reset_if_invalid=True)

    with open(FEATURE_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        # ----------------------------
        # Encoded values
        # ----------------------------

        regime_encoded = REGIME_MAP.get(trade.get("regime"), 0)
        vol_encoded = VOL_MAP.get(trade.get("volatility"), 0)
        setup_encoded = SETUP_MAP.get(trade.get("setup"), 0)
        style_encoded = STYLE_MAP.get(trade.get("style"), 0)

        timestamp = trade.get("entry_time")
        session = classify_session(timestamp)
        session_encoded = SESSION_MAP.get(session, 0)

        # ----------------------------
        # Expectancy Intelligence
        # ----------------------------

        setup_stats = calculate_setup_expectancy()
        regime_stats = calculate_regime_expectancy()

        setup_raw_avg_R = 0
        regime_raw_avg_R = 0

        if setup_stats:
            s = setup_stats.get(trade.get("setup"))
            if s:
                setup_raw_avg_R = s.get("raw_avg_R", 0)

        if regime_stats:
            r = regime_stats.get(trade.get("regime"))
            if r:
                regime_raw_avg_R = r.get("avg_R", 0)

        # ----------------------------
        # ML + Result
        # ----------------------------

        ml_prob = trade.get("ml_probability")
        predicted_won = 1 if ml_prob and ml_prob >= 0.5 else 0
        won = 1 if result == "win" else 0

        # ----------------------------
        # Write Row
        # ----------------------------

        writer.writerow([
            regime_encoded,
            vol_encoded,
            trade.get("conviction_score"),
            trade.get("impulse"),
            trade.get("follow_through"),
            trade.get("confidence"),
            style_encoded,
            setup_encoded,
            session_encoded,
            setup_raw_avg_R,
            regime_raw_avg_R,
            ml_prob,
            predicted_won,
            won
        ])
```

#### `analytics/grader.py`
```python
# analytics/grader.py

import os
import json
import logging
import pandas as pd
from datetime import timedelta
from pandas.errors import EmptyDataError

from core.paths import DATA_DIR
from core.data_service import get_market_dataframe
from core.debug import debug_log

PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")
EDGE_FILE = os.path.join(DATA_DIR, "edge_stats.json")


def confidence_band(conf):
    if conf < 0.6:
        return "low"
    elif conf < 0.75:
        return "medium"
    else:
        return "high"


def _safe_confidence_band(val):
    try:
        return confidence_band(float(val))
    except (TypeError, ValueError):
        return ""


def check_predictions(trade=None):
    if isinstance(trade, dict):
        try:
            trade_id = trade.get("trade_id")
            option_symbol = trade.get("option_symbol")
            entry_price = float(trade.get("entry_price", 0) or 0)
            exit_price = float(trade.get("exit_price", 0) or 0)
            pnl = trade.get("realized_pnl_dollars", trade.get("pnl"))
            if pnl is None:
                qty = trade.get("qty")
                try:
                    qty_val = float(qty) if qty is not None else 0
                except (TypeError, ValueError):
                    qty_val = 0
                pnl = (exit_price - entry_price) * qty_val * 100 if qty_val else None

            if exit_price > entry_price:
                actual_direction = "BULLISH"
            elif exit_price < entry_price:
                actual_direction = "BEARISH"
            else:
                actual_direction = "FLAT"

            predicted_direction = trade.get("predicted_direction") or trade.get("direction")
            prediction_confidence = trade.get("prediction_confidence")
            if prediction_confidence is None:
                prediction_confidence = trade.get("confidence")
            direction_prob = trade.get("direction_prob")
            edge_prob = trade.get("edge_prob")
            correct_prediction = (
                predicted_direction in {"BULLISH", "BEARISH"}
                and predicted_direction == actual_direction
            )

            debug_log(
                "prediction_graded_trade",
                trade_id=trade_id,
                option_symbol=option_symbol,
                predicted=predicted_direction,
                actual=actual_direction,
                pnl=pnl,
                confidence=prediction_confidence,
                direction_prob=direction_prob,
                edge_prob=edge_prob,
                correct=bool(correct_prediction),
            )
            return correct_prediction
        except Exception as e:
            logging.exception("trade_prediction_grade_error: %s", e)
            return None

    if not os.path.exists(PRED_FILE):
        return

    try:
        preds = pd.read_csv(PRED_FILE)
    except EmptyDataError:
        return
    except Exception as e:
        logging.warning("prediction_read_failed: %s", e)
        return

    if preds.empty or "time" not in preds.columns:
        return
    raw_len = len(preds)

    preds["time"] = pd.to_datetime(preds["time"], errors="coerce", utc=True)
    if bool(preds["time"].isna().all()):
        # Avoid wiping file if parsing fails across all rows.
        logging.warning("prediction_time_parse_failed_all")
        return
    preds["time"] = preds["time"].dt.tz_convert("US/Eastern").dt.tz_localize(None)
    preds = preds.dropna(subset=["time"])

    if preds.empty:
        # Avoid overwriting a non-empty file with only headers.
        if raw_len > 0:
            logging.warning("prediction_rows_dropped_after_parse", extra={"raw_len": raw_len})
        return

    # Ensure required columns exist
    if "checked" not in preds.columns:
        preds["checked"] = False

    if "actual" not in preds.columns:
        preds["actual"] = ""

    if "correct" not in preds.columns:
        preds["correct"] = 0

    if "high_hit" not in preds.columns:
        preds["high_hit"] = 0

    if "low_hit" not in preds.columns:
        preds["low_hit"] = 0

    if "price_at_check" not in preds.columns:
        preds["price_at_check"] = 0.0

    if "close_at_check" not in preds.columns:
        preds["close_at_check"] = 0.0

    if "confidence" in preds.columns:
        if "confidence_band" not in preds.columns:
            preds["confidence_band"] = ""
        # Ensure object dtype before assigning strings
        try:
            preds["confidence_band"] = preds["confidence_band"].astype("object")
        except Exception:
            pass
        mask = preds["confidence_band"].isna() | (preds["confidence_band"] == "")
        if mask.any():
            preds.loc[mask, "confidence_band"] = preds.loc[mask, "confidence"].apply(_safe_confidence_band)

    # Load price data
    df = get_market_dataframe()
    if df is None or df.empty:
        return

    df = df.reset_index()
    df.rename(columns={"index": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    if df.empty:
        return

    # Grade predictions
    for i, row in preds.iterrows():

        if bool(row.get("checked", False)):
            continue

        # Safe timeframe parsing
        try:
            tf = int(row["timeframe"])
        except Exception:
            continue

        target_time = row["time"] + timedelta(minutes=tf)

        # Round to nearest minute (prevents seconds mismatch bug)
        target_time = target_time.replace(second=0, microsecond=0)
        
        # --------------------------------
        # Only grade if timeframe has fully passed
        # --------------------------------
        current_time = pd.Timestamp.now(tz="US/Eastern").tz_localize(None)

        if current_time < target_time:
            continue

        # Find first available bar at or after target_time.
        future = df[df["timestamp"] >= target_time]
        if future.empty:
            continue

        future_high = future.iloc[0]["high"]
        future_low = future.iloc[0]["low"]
        future_close = future.iloc[0]["close"]

        predicted_high = row["high"]
        predicted_low = row["low"]

        # --------------------------------
        # Check range accuracy
        # --------------------------------
        high_hit = int(future_high >= predicted_high)
        low_hit = int(future_low <= predicted_low)

        # --------------------------------
        # Determine directional result
        # --------------------------------
        if high_hit and not low_hit:
            result = "bullish"
        elif low_hit and not high_hit:
            result = "bearish"
        elif high_hit and low_hit:
            result = "both"
        else:
            result = "range"

        direction_correct = int(result == row["direction"])

        # --------------------------------
        # Save results
        # --------------------------------
        preds.loc[i, "actual"] = result
        preds.loc[i, "correct"] = direction_correct
        preds.loc[i, "high_hit"] = high_hit
        preds.loc[i, "low_hit"] = low_hit
        preds.loc[i, "price_at_check"] = future_close
        preds.loc[i, "close_at_check"] = future_close
        preds.loc[i, "checked"] = True
        debug_log(
            "prediction_graded",
            timeframe=tf,
            predicted=row["direction"],
            actual=result,
            correct=direction_correct,
            high_hit=high_hit,
            low_hit=low_hit,
            price_at_check=round(float(future_close), 2)
        )

    graded = preds[preds["checked"] == True]

    update_edge_stats(graded)

    preds.to_csv(PRED_FILE, index=False)


def update_edge_stats(graded):

    if graded.empty:
        return

    stats = {}

    total = len(graded)
    wins = int(graded["correct"].sum())

    stats["overall"] = {
        "total": total,
        "wins": wins,
        "winrate": round(wins / total, 4)
    }

    tf_group = graded.groupby("timeframe")["correct"].agg(
        total="count",
        wins="sum"
    )

    stats["timeframes"] = {}

    for tf, row in tf_group.iterrows():
        stats["timeframes"][str(int(tf))] = {
            "total": int(row["total"]),
            "wins": int(row["wins"]),
            "winrate": round(row["wins"] / row["total"], 4)
        }

    if "regime" in graded.columns:
        regime_group = graded.groupby("regime")["correct"].agg(
            total="count",
            wins="sum"
        )

        stats["regimes"] = {}

        for regime, row in regime_group.iterrows():
            stats["regimes"][regime] = {
                "total": int(row["total"]),
                "wins": int(row["wins"]),
                "winrate": round(row["wins"] / row["total"], 4)
            }

    if "session" in graded.columns:
        session_group = graded.groupby("session")["correct"].agg(
            total="count",
            wins="sum"
        )

        stats["sessions"] = {}

        for session, row in session_group.iterrows():
            stats["sessions"][session] = {
                "total": int(row["total"]),
                "wins": int(row["wins"]),
                "winrate": round(row["wins"] / row["total"], 4)
            }

    with open(EDGE_FILE, "w") as f:
        json.dump(stats, f, indent=4)
```

#### `analytics/indicators.py`
```python
import math
from datetime import datetime, timedelta, time as dt_time
import pandas as pd
import pytz


def _safe_float(val):
    try:
        if val is None:
            return None
        out = float(val)
        if math.isfinite(out):
            return out
    except (TypeError, ValueError):
        return None
    return None


def _index_to_et_naive(idx):
    if not isinstance(idx, pd.DatetimeIndex) or len(idx) == 0:
        return None
    eastern = pytz.timezone("US/Eastern")
    try:
        if idx.tz is not None:
            return idx.tz_convert(eastern).tz_localize(None)
    except Exception:
        pass
    return idx


def _session_open_ts(idx_local: pd.DatetimeIndex):
    try:
        last_ts = idx_local[-1]
        session_date = last_ts.date()
        return datetime.combine(session_date, dt_time(9, 30))
    except Exception:
        return None


def opening_range(df, minutes: int = 15):
    """
    Compute opening range high/low for the current ET session.
    Returns (high, low) or (None, None).
    """
    if df is None or df.empty or "high" not in df.columns or "low" not in df.columns:
        return None, None
    idx_local = _index_to_et_naive(df.index)
    if idx_local is None:
        return None, None
    session_open = _session_open_ts(idx_local)
    if session_open is None:
        return None, None
    window_end = session_open + timedelta(minutes=minutes)
    try:
        # Filter to current session date before applying ORB window
        session_mask = idx_local.date == session_open.date()
        if not session_mask.any():
            return None, None
        session_df = df.loc[session_mask]
        session_idx = idx_local[session_mask]
        window = session_df[(session_idx >= session_open) & (session_idx < window_end)]
        if window.empty:
            return None, None
        return _safe_float(window["high"].max()), _safe_float(window["low"].min())
    except Exception:
        return None, None


def compute_indicators(df, orb_minutes: int = 15) -> dict:
    """
    Pull most recent indicator values from the dataframe.
    Uses existing columns created by core.data_service._prepare_dataframe().
    """
    if df is None or df.empty:
        return {}
    last = df.iloc[-1]
    close = _safe_float(last.get("close"))
    ema9 = _safe_float(last.get("ema9"))
    ema20 = _safe_float(last.get("ema20"))
    rsi = _safe_float(last.get("rsi"))
    atr = _safe_float(last.get("atr"))
    vwap = _safe_float(last.get("vwap"))
    orb_high, orb_low = opening_range(df, minutes=orb_minutes)

    vwap_dist = None
    ema_spread = None
    if close and vwap and close > 0:
        vwap_dist = (close - vwap) / close
    if ema9 and ema20:
        ema_spread = (ema9 - ema20) / ema20

    return {
        "close": close,
        "ema9": ema9,
        "ema20": ema20,
        "rsi": rsi,
        "atr": atr,
        "vwap": vwap,
        "vwap_dist": vwap_dist,
        "ema_spread": ema_spread,
        "orb_high": orb_high,
        "orb_low": orb_low,
    }


def compute_zscores(df, window: int = 30) -> dict:
    """
    Additive z-scores for SIM analytics (no decision impact).
    """
    if df is None or df.empty:
        return {}
    if len(df) < (window + 2):
        return {}
    out = {}
    try:
        close = pd.to_numeric(df["close"], errors="coerce")
        vwap = pd.to_numeric(df["vwap"], errors="coerce") if "vwap" in df.columns else None
        volume = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else None
        atr = pd.to_numeric(df["atr"], errors="coerce") if "atr" in df.columns else None

        tail = close.tail(window)
        if tail.notna().sum() >= 5:
            sma = tail.mean()
            std = tail.std()
            if std and std > 0:
                out["close_z"] = _safe_float((close.iloc[-1] - sma) / std)

        if vwap is not None:
            dev = (close - vwap).tail(window)
            if dev.notna().sum() >= 5:
                std = dev.std()
                if std and std > 0:
                    out["vwap_z"] = _safe_float(dev.iloc[-1] / std)

        if volume is not None:
            vol_tail = volume.tail(window)
            if vol_tail.notna().sum() >= 5:
                sma = vol_tail.mean()
                std = vol_tail.std()
                if std and std > 0:
                    out["vol_z"] = _safe_float((volume.iloc[-1] - sma) / std)

        if atr is not None:
            atr_tail = atr.tail(window)
            if atr_tail.notna().sum() >= 5:
                sma = atr_tail.mean()
                if sma and sma > 0:
                    out["atr_expansion"] = _safe_float(atr.iloc[-1] / sma)
    except Exception:
        return out
    return out
```

#### `analytics/iv_features.py`
```python
def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def compute_iv_features(iv_series: list[float] | None, current_iv: float | None) -> dict:
    """
    Lightweight IV features (proxy). If history is missing, returns current IV only.
    """
    iv = _safe_float(current_iv)
    out = {"iv": iv}
    if iv is None or not iv_series:
        return out
    clean = [v for v in iv_series if _safe_float(v) is not None]
    if len(clean) < 5:
        return out
    try:
        low = min(clean)
        high = max(clean)
        if high > low:
            rank = (iv - low) / (high - low)
            if rank is not None:
                rank = max(0.0, min(1.0, rank))
            out["iv_rank_proxy"] = rank
    except Exception:
        pass
    return out
```

#### `analytics/market_regime.py`
```python
import math
import pandas as pd

from signals.regime import get_regime


def _safe_float(val):
    try:
        if val is None:
            return None
        out = float(val)
        if math.isfinite(out):
            return out
    except (TypeError, ValueError):
        return None
    return None


def compute_market_regime(df) -> dict:
    """
    Additive regime metrics for SIM analytics.
    Returns a dict that can be stored in trade["feature_snapshot"].
    """
    if df is None or df.empty:
        return {}
    out = {"regime": get_regime(df)}
    try:
        close = pd.to_numeric(df["close"], errors="coerce")
        if close.notna().sum() >= 10:
            returns = close.pct_change().dropna()
            rv = returns.tail(30).std()
            if rv is not None:
                out["realized_vol_30"] = _safe_float(rv)
    except Exception:
        pass
    return out
```

#### `analytics/ml_accuracy.py`
```python
import os
import pandas as pd
from pandas.errors import EmptyDataError
from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "trade_features.csv")


def ml_rolling_accuracy(lookback=30):
    """
    Returns advanced rolling ML performance metrics.
    """

    if not os.path.exists(FILE):
        return None

    if os.path.getsize(FILE) == 0:
        return None

    try:
        df = pd.read_csv(FILE)
    except EmptyDataError:
        return None
    except Exception:
        return None

    required_cols = ["won", "predicted_won"]

    for col in required_cols:
        if col not in df.columns:
            return None

    # Remove incomplete rows
    df = df.dropna(subset=required_cols)

    if len(df) < lookback:
        return None

    recent = df.tail(lookback)

    accuracy = (recent["won"] == recent["predicted_won"]).mean()

    # Optional: probability calibration check
    calibration_score = None
    if "ml_probability" in recent.columns:

        confident = recent[recent["ml_probability"] > 0.65]

        if len(confident) >= 5:
            confident_acc = (confident["won"] == confident["predicted_won"]).mean()
            calibration_score = round(confident_acc * 100, 2)

    return {
        "accuracy": round(accuracy * 100, 2),
        "samples": len(recent),
        "confident_accuracy": calibration_score
    }
```

#### `analytics/ml_loader.py`
```python
import os
import joblib
from core.paths import DATA_DIR

EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

def load_edge_model():
    if not os.path.exists(EDGE_MODEL_FILE):
        return None
    return joblib.load(EDGE_MODEL_FILE)
def build_feature_vector(trade, regime_encoded, volatility_encoded,
                         conviction_score, impulse, follow_through,
                         setup_encoded, session_encoded):

    return [[
        regime_encoded,
        volatility_encoded,
        conviction_score,
        impulse,
        follow_through,
        setup_encoded,
        session_encoded,
        trade["confidence"]
    ]]
```

#### `analytics/options_greeks.py`
```python
def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def extract_greeks(snapshot) -> dict:
    if snapshot is None:
        return {}
    greeks = getattr(snapshot, "greeks", None)
    if greeks is None and isinstance(snapshot, dict):
        greeks = snapshot.get("greeks")
    if greeks is None:
        return {}

    def _pick(obj, *keys):
        if isinstance(obj, dict):
            for key in keys:
                if key in obj:
                    return obj.get(key)
            return None
        for key in keys:
            try:
                if hasattr(obj, key):
                    return getattr(obj, key)
            except Exception:
                continue
        return None

    return {
        "iv": _safe_float(_pick(greeks, "implied_volatility", "iv", "impliedVolatility")),
        "delta": _safe_float(_pick(greeks, "delta")),
        "gamma": _safe_float(_pick(greeks, "gamma")),
        "theta": _safe_float(_pick(greeks, "theta")),
        "vega": _safe_float(_pick(greeks, "vega")),
    }


def extract_greeks_from_trade(trade: dict | None) -> dict:
    if not isinstance(trade, dict):
        return {}
    return {
        "iv": trade.get("iv_at_entry"),
        "delta": trade.get("delta_at_entry"),
        "gamma": trade.get("gamma_at_entry"),
        "theta": trade.get("theta_at_entry"),
        "vega": trade.get("vega_at_entry"),
    }
```

#### `analytics/performance.py`
```python
# analytics/performance.py

from core.account_repository import load_account, load_career


def get_paper_stats():

    acc = load_account()

    trades = acc["wins"] + acc["losses"]

    if trades == 0:
        winrate = 0
    else:
        winrate = (acc["wins"] / trades) * 100

    pnl = acc["balance"] - acc["starting_balance"]

    return {
        "balance": acc["balance"],
        "pnl": pnl,
        "wins": acc["wins"],
        "losses": acc["losses"],
        "winrate": round(winrate, 2)
    }


def get_career_stats():

    c = load_career()

    total = c["total_trades_all_time"]

    if total == 0:
        winrate = 0
    else:
        winrate = (c["total_wins_all_time"] / total) * 100

    return {
        "total_trades": total,
        "wins": c["total_wins_all_time"],
        "losses": c["total_losses_all_time"],
        "winrate": round(winrate, 2),
        "best_balance": c["best_balance"]
    }
```

#### `analytics/prediction_stats.py`
```python
# analytics/prediction_stats.py

import os
import logging
import pandas as pd
import csv
from core.paths import DATA_DIR

from signals.session_classifier import classify_session

PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")
PRED_HEADERS = [
    "time",
    "timeframe",
    "direction",
    "confidence",
    "high",
    "low",
    "regime",
    "volatility",
    "session",
    "actual",
    "correct",
    "checked",
    "high_hit",
    "low_hit",
    "price_at_check",
    "close_at_check",
    "confidence_band",
]


def ensure_prediction_file():
    if os.path.exists(PRED_FILE) and os.path.getsize(PRED_FILE) > 0:
        try:
            with open(PRED_FILE, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                raise ValueError("empty_file")
            header = rows[0]
            if header != PRED_HEADERS:
                now = pd.Timestamp.now(tz="US/Eastern").tz_localize(None)
                cutoff = now - pd.Timedelta(days=30)
                padded = []
                for row in rows[1:]:
                    if not row:
                        continue
                    if row[0] == "time":
                        continue
                    ts = pd.to_datetime(row[0], errors="coerce")
                    # If timestamp is parseable and stale, drop it.
                    # If unparseable, keep the row to avoid silent data loss.
                    if not pd.isna(ts):
                        if ts < cutoff:
                            continue
                    new_row = row[:len(PRED_HEADERS)]
                    if len(new_row) < len(PRED_HEADERS):
                        new_row += [""] * (len(PRED_HEADERS) - len(new_row))
                    padded.append(new_row)
                with open(PRED_FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(PRED_HEADERS)
                    writer.writerows(padded)
        except Exception as e:
            logging.warning("ensure_prediction_file_failed: %s", e)
        return

    with open(PRED_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(PRED_HEADERS)

def log_prediction(pred, regime, volatility):

    if pred is None:
        return

    ensure_prediction_file()

    # Normalize time to ISO string to avoid pandas parse issues later.
    try:
        pred_time = pd.to_datetime(pred.get("time")).isoformat()
    except Exception:
        pred_time = str(pred.get("time"))

    session = classify_session(pred_time)

    with open(PRED_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            pred_time,
            pred["timeframe"],
            pred["direction"],
            pred["confidence"],
            pred["high"],
            pred["low"],
            regime,
            volatility,
            session,
            "",
            0,
            False,
            "",
            "",
            "",
            "",
            ""
        ])


def calculate_accuracy():

    if not os.path.exists(PRED_FILE):
        return None
 
    try:
        df = pd.read_csv(PRED_FILE)
    except:
        return None

    if df.empty:
        return None

    # Ensure required columns exist
    if "checked" not in df.columns:
        return None

    df = df[df["checked"] == True]

    if len(df) == 0:
        return None

    result = {}

    for timeframe in [30, 60]:

        subset = df[df["timeframe"] == timeframe]

        if len(subset) == 0:
            result[timeframe] = (0, 0, 0)
            continue

        total = len(subset)
        correct = subset["correct"].sum()
        accuracy = (correct / total) * 100

        result[timeframe] = (total, correct, round(accuracy, 2))

    # Confidence reliability
    high_conf = df[df["confidence"] >= 0.65]
    low_conf = df[df["confidence"] < 0.50]

    def conf_acc(sub):
        if len(sub) == 0:
            return 0
        return round((sub["correct"].sum() / len(sub)) * 100, 2)

    return {
        "30": result.get(30, (0, 0, 0)),
        "60": result.get(60, (0, 0, 0)),
        "high_conf": conf_acc(high_conf),
        "low_conf": conf_acc(low_conf)
    }
```

#### `analytics/progressive_influence.py`
```python
import os
import pandas as pd
from core.paths import DATA_DIR
from analytics.edge_momentum import calculate_edge_momentum

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")

def get_ml_weight(max_trades=200):
    """
    Gradually increases ML influence as trade count grows.
    """

    if not os.path.exists(FEATURE_FILE):
        return 0.0

    try:
        df = pd.read_csv(FEATURE_FILE)
    except:
        return 0.0
    trade_count = len(df)

    weight = min(trade_count / max_trades, 1.0)
    momentum_data = calculate_edge_momentum()

    if momentum_data:
        momentum = momentum_data.get("momentum")
        if momentum is not None:
            if momentum > 0.2:
                weight += 0.05
            elif momentum < -0.2:
                weight -= 0.05

    weight = max(0.0, min(weight, 1.0))

    return round(weight, 3)
```

#### `analytics/regime_expectancy.py`
```python
import os
import json
import math
from collections import defaultdict
from statistics import mean, stdev
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_regime_expectancy():
    """
    Advanced regime expectancy analytics.

    Returns:
        dict of regime -> metrics
        OR None if insufficient data
    """

    if not os.path.exists(ACCOUNT_FILE):
        return None

    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except Exception:
        return None

    trades = acc.get("trade_log", [])

    if len(trades) < 15:
        return None

    regime_data = defaultdict(list)

    for t in trades:
        risk = t.get("risk", 0)
        regime = t.get("regime", "UNKNOWN")

        if not risk or risk == 0:
            continue

        r_multiple = t.get("R", None)

        if r_multiple is None:
            continue

        regime_data[regime].append(r_multiple)

    if not regime_data:
        return None

    results = {}

    for regime, r_list in regime_data.items():

        total = len(r_list)

        if total < 5:
            continue  # ignore low-sample regimes

        avg_r = mean(r_list)

        winrate = sum(1 for r in r_list if r > 0) / total * 100

        volatility = stdev(r_list) if total > 1 else 0

        # Stability metric (higher is better)
        if volatility == 0:
            stability = 1
        else:
            stability = max(0, 1 - (volatility / 3))

        # Confidence score
        sample_factor = min(total / 50, 1.0)
        confidence = round(stability * sample_factor, 3)

        results[regime] = {
            "trades": total,
            "regime_sample_count": total,
            "avg_R": round(avg_r, 3),
            "winrate": round(winrate, 1),
            "volatility": round(volatility, 3),
            "stability": round(stability, 3),
            "confidence": confidence
        }

    if not results:
        return None

    return results
```

#### `analytics/regime_memory.py`
```python
# analytics/regime_memory.py

from analytics.regime_persistence import calculate_regime_persistence


_previous_regime = None
_regime_age = 0


def get_regime_memory():
    """
    Tracks regime age and transition confidence.
    """

    global _previous_regime
    global _regime_age

    data = calculate_regime_persistence()

    current_regime = data["current_regime"]

    if _previous_regime is None:
        _previous_regime = current_regime
        _regime_age = 1

    if current_regime == _previous_regime:
        _regime_age += 1
    else:
        _previous_regime = current_regime
        _regime_age = 1

    # Trust factor grows with age
    if _regime_age < 5:
        trust = 0.5
    elif _regime_age < 15:
        trust = 0.75
    else:
        trust = 1.0

    return {
        "regime": current_regime,
        "age": _regime_age,
        "trust": trust
    }
```

#### `analytics/regime_persistence.py`
```python
# analytics/regime_persistence.py

from core.data_service import get_market_dataframe
from signals.regime import get_regime


def calculate_regime_persistence(lookback=40):
    """
    Measures how dominant current regime is.
    Returns score between 0 and 1.
    """

    df = get_market_dataframe()

    if df is None or len(df) < lookback:
        return {
            "persistence": 0.5,
            "current_regime": "UNKNOWN"
        }

    regimes = []

    for i in range(lookback):
        slice_df = df.iloc[:-i] if i != 0 else df
        regime = get_regime(slice_df)
        regimes.append(regime)

    current_regime = regimes[0]

    same_count = regimes.count(current_regime)

    persistence = same_count / lookback

    return {
        "persistence": round(persistence, 3),
        "current_regime": current_regime
    }
```

#### `analytics/regime_transition.py`
```python
# analytics/regime_transition.py

from core.data_service import get_market_dataframe
from signals.regime import get_regime
from signals.volatility import volatility_state


def detect_regime_transition(lookback=30):
    """
    Detects unstable regime shifts.
    """

    df = get_market_dataframe()

    if df is None or len(df) < lookback:
        return {
            "transition": False,
            "severity": 0.0
        }

    recent_regimes = []

    # Check regime across recent candles
    for i in range(lookback):
        slice_df = df.iloc[:-i] if i != 0 else df
        regime = get_regime(slice_df)
        recent_regimes.append(regime)

    unique_regimes = len(set(recent_regimes))

    severity = 0.0

    # ----------------------------------------
    # 1️⃣ Regime Flipping
    # ----------------------------------------

    if unique_regimes >= 3:
        severity += 0.4
    elif unique_regimes == 2:
        severity += 0.2

    # ----------------------------------------
    # 2️⃣ Volatility Expansion Check
    # ----------------------------------------

    current_vol = volatility_state(df)
    prev_vol = volatility_state(df.iloc[:-10])

    if current_vol == "HIGH" and prev_vol in ["LOW", "NORMAL"]:
        severity += 0.3

    # ----------------------------------------
    # 3️⃣ Instability Threshold
    # ----------------------------------------

    transition = severity >= 0.4

    return {
        "transition": transition,
        "severity": round(severity, 2)
    }
```

#### `analytics/review_engine.py`
```python
from signals.regime import get_regime
from signals.volatility import volatility_state
from core.data_service import get_market_dataframe
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_stability import calculate_edge_stability
from analytics.ml_accuracy import ml_rolling_accuracy


def review_trade(trade, result):
    """
    Post-trade forensic analysis.
    Fully hardened.
    """

    df = get_market_dataframe()
    if df is None:
        return "Market data unavailable for review."

    regime = get_regime(df)
    vol = volatility_state(df)

    setup = trade.get("setup", "UNKNOWN")
    confidence = round(trade.get("confidence", 0) * 100, 1)
    r_multiple = trade.get("R", None)
    ml_prob = trade.get("ml_probability", None)

    diagnosis = []

    # ---------------------------------
    # Structural Context
    # ---------------------------------

    if setup == "BREAKOUT" and regime == "RANGE":
        diagnosis.append("Breakout taken in range market (structural mismatch).")

    if setup == "REVERSAL" and regime == "TREND":
        diagnosis.append("Reversal attempted against active trend.")

    if setup == "PULLBACK" and regime == "TREND":
        diagnosis.append("Pullback aligned with trend structure (favorable context).")

    # ---------------------------------
    # Volatility Commentary
    # ---------------------------------

    if vol in ["LOW", "DEAD"]:
        diagnosis.append("Low volatility likely reduced follow-through.")

    if vol == "HIGH" and setup == "BREAKOUT":
        diagnosis.append("High volatility supports breakout expansion.")

    # ---------------------------------
    # Confidence Evaluation
    # ---------------------------------

    if confidence < 60:
        diagnosis.append("Low confidence trade (statistically weaker edge).")

    elif confidence >= 75:
        diagnosis.append("High conviction signal.")

    # ---------------------------------
    # Regime Expectancy Context
    # ---------------------------------

    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:
        avg_r = regime_stats[regime]["avg_R"]
        diagnosis.append(f"Regime expectancy avg R: {avg_r}")

    # ---------------------------------
    # ML Performance Context
    # ---------------------------------

    ml_stats = ml_rolling_accuracy()

    if ml_stats:
        acc = ml_stats["accuracy"]

        if acc < 52:
            diagnosis.append("ML currently underperforming.")
        elif acc > 60:
            diagnosis.append("ML currently performing strongly.")

    # ---------------------------------
    # Stability Context
    # ---------------------------------

    stability_data = calculate_edge_stability()

    if stability_data:
        stability = round(stability_data["stability"], 2)

        if stability < 0.4:
            diagnosis.append("Edge stability LOW — system in fragile state.")
        elif stability > 0.7:
            diagnosis.append("Edge stability HIGH — strong statistical regime.")

    # ---------------------------------
    # R-Multiple Commentary
    # ---------------------------------

    if r_multiple is not None:

        if r_multiple >= 2:
            diagnosis.append("Captured strong expansion move.")

        if r_multiple <= -1:
            diagnosis.append("Full stop loss hit.")

        if -0.5 < r_multiple < 0.5:
            diagnosis.append("Weak move — lacked expansion.")

    # ---------------------------------
    # Build Report
    # ---------------------------------

    report = f"""📊 **AI Trade Review**

Setup: {setup}
Regime: {regime}
Volatility: {vol}
Confidence: {confidence}%
Result: {result.upper()}
"""

    if ml_prob is not None:
        report += f"ML Probability: {round(ml_prob * 100,1)}%\n"

    if r_multiple is not None:
        report += f"R-Multiple: {r_multiple}\n"

    if diagnosis:
        report += "\nDiagnosis:\n"
        for d in diagnosis:
            report += f"- {d}\n"

    return report
```

#### `analytics/risk_control.py`
```python
# analytics/risk_control.py

from analytics.edge_stability import calculate_edge_stability
from analytics.edge_momentum import calculate_edge_momentum
from analytics.stability_mode import get_stability_mode
from analytics.capital_protection import get_capital_mode
from analytics.setup_intelligence import get_setup_intelligence
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_compression import get_edge_compression
from analytics.regime_transition import detect_regime_transition
from analytics.regime_memory import get_regime_memory
from signals.regime import get_regime
from core.data_service import get_market_dataframe


def get_dynamic_risk_percent(acc):
    trade_log = acc.get("trade_log", [])
    closed = [t for t in trade_log if t.get("result") is not None]
    closed_count = len(closed)

    if closed_count < 50:
        return 0.005  # 0.5%

    if closed_count < 100:
        return 0.01  # 1%

    last_50 = closed[-50:]
    if not last_50:
        return 0.005

    wins = 0
    for t in last_50:
        result = t.get("result")
        if result is not None and str(result).lower() == "win":
            wins += 1

    winrate = wins / len(last_50)

    if winrate > 0.55:
        return 0.015
    if winrate < 0.45:
        return 0.005
    return 0.01


def dynamic_risk_percent(setup_type=None):

    """
    Regime + Stability + Capital Protection Risk Engine
    """

    # ------------------------------------------------
    # 1️⃣ Base Risk From Stability
    # ------------------------------------------------

    stability_data = calculate_edge_stability()

    if not stability_data:
        base_risk = 0.005  # ultra conservative early
        stability = 0.5
    else:
        stability = stability_data["stability"]
        base_risk = 0.005 + (stability * 0.01)

    base_risk = max(0.005, min(base_risk, 0.015))

    adjusted_risk = base_risk

    # ------------------------------------------------
    # 2️⃣ Stability Mode Layer
    # ------------------------------------------------

    mode = get_stability_mode()
    adjusted_risk *= mode["risk_multiplier"]

    compression = get_edge_compression()
    adjusted_risk *= compression["risk_multiplier"]

    # ------------------------------------------------
    # 3️⃣ Capital Protection Layer
    # ------------------------------------------------

    from analytics.capital_protection import get_capital_mode
    capital_mode = get_capital_mode()

    adjusted_risk *= capital_mode["risk_multiplier"]

    # ------------------------------------------------
    # 4️⃣ Regime Expectancy Layer
    # ------------------------------------------------

    df = get_market_dataframe()
    if df is not None:

        current_regime = get_regime(df)
        regime_stats = calculate_regime_expectancy()

        if regime_stats and current_regime in regime_stats:

            regime_data = regime_stats[current_regime]

            trades = regime_data["trades"]
            avg_R = regime_data["avg_R"]
            winrate = regime_data["winrate"]

            if trades >= 10:

                if avg_R > 0.5 and winrate > 55:
                    adjusted_risk *= 1.15

                if avg_R < 0:
                    adjusted_risk *= 0.80

                if winrate < 45:
                    adjusted_risk *= 0.85

            else:
                adjusted_risk *= 0.85  # not enough regime data

    # ------------------------------------------------
    # 5️⃣ Edge Momentum Layer
    # ------------------------------------------------

    momentum_data = calculate_edge_momentum()

    if momentum_data:
        momentum = momentum_data["momentum"]

        if momentum > 0.15:
            adjusted_risk *= 1.10

        elif momentum < -0.15:
            adjusted_risk *= 0.85

    # ------------------------------------------------
    # 6️⃣ Setup Intelligence Layer
    # ------------------------------------------------

    if setup_type:

        intelligence = get_setup_intelligence(
            setup_type=setup_type,
            regime=None,
            ml_probability=None
        )

        if intelligence:
            adjusted_risk *= (1 + intelligence["risk_boost"])
    transition_data = detect_regime_transition()

    if transition_data["transition"]:
        adjusted_risk *= (1 - transition_data["severity"] * 0.3)
    
    memory = get_regime_memory()

    # Reduce risk if regime is new
    if memory["trust"] < 1.0:
        adjusted_risk *= memory["trust"]

    # ------------------------------------------------
    # 7️⃣ Final Clamp
    # ------------------------------------------------

    adjusted_risk = max(0.003, min(adjusted_risk, 0.02))

    return round(adjusted_risk, 4)
```

#### `analytics/risk_metrics.py`
```python
# analytics/risk_metrics.py

import json
import os
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_r_metrics():

    if not os.path.exists(ACCOUNT_FILE):
        return None

    with open(ACCOUNT_FILE, "r") as f:
        acc = json.load(f)

    trades = acc.get("trade_log", [])

    if not trades:
        return None

    r_values = []

    for t in trades:

        risk = t.get("risk", 0)

        if risk == 0:
            continue

        r = t["pnl"] / risk
        r_values.append(r)

    if not r_values:
        return None

    avg_r = sum(r_values) / len(r_values)
    win_r = [r for r in r_values if r > 0]
    loss_r = [r for r in r_values if r <= 0]

    return {
        "total_trades": len(r_values),
        "avg_R": round(avg_r, 2),
        "avg_win_R": round(sum(win_r)/len(win_r), 2) if win_r else 0,
        "avg_loss_R": round(sum(loss_r)/len(loss_r), 2) if loss_r else 0,
        "max_R": round(max(r_values), 2),
        "min_R": round(min(r_values), 2),
    }

def calculate_drawdown():

    if not os.path.exists(ACCOUNT_FILE):
        return None

    with open(ACCOUNT_FILE, "r") as f:
        acc = json.load(f)

    trades = acc.get("trade_log", [])

    if not trades:
        return None

    balances = [t["balance_after"] for t in trades]

    peak = balances[0]
    max_drawdown = 0

    for b in balances:
        if b > peak:
            peak = b

        drawdown = peak - b

        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "max_drawdown_dollars": round(max_drawdown, 2),
        "max_drawdown_percent": round(
            (max_drawdown / peak) * 100 if peak != 0 else 0,
            2
        )
    }
```

#### `analytics/run_stats.py`
```python
# analytics/run_stats.py

from core.account_repository import load_account


def get_run_stats():

    acc = load_account()

    trades = acc.get("trade_log", [])

    total_trades = len(trades)
    wins = acc.get("wins", 0)
    losses = acc.get("losses", 0)

    starting = acc.get("starting_balance", 0)
    current = acc.get("balance", 0)

    pnl = current - starting

    # Equity curve balances
    balances = [starting]
    for t in trades:
        balances.append(t["balance_after"])

    peak = balances[0]
    max_peak = balances[0]
    max_drawdown = 0

    for b in balances:
        if b > max_peak:
            max_peak = b
        drawdown = max_peak - b
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "trades": total_trades,
        "wins": wins,
        "losses": losses,
        "pnl": pnl,
        "current": current,
        "start": starting,
        "peak": max_peak,
        "drawdown": max_drawdown
    }
```

#### `analytics/setup_expectancy.py`
```python
from core.account_repository import load_account
from collections import defaultdict
import numpy as np


def calculate_setup_expectancy():
    """
    Advanced Setup Expectancy Engine

    - Bayesian-style confidence weighting
    - Sample size protection
    - Variance penalty
    - Stability-aware adjustment
    """

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 15:
        return None

    setup_data = defaultdict(list)

    for t in trades:
        if "setup" in t and "R" in t:
            setup_data[t["setup"]].append(t["R"])

    stats = {}

    for setup, Rs in setup_data.items():

        n = len(Rs)

        # Require minimum meaningful sample
        if n < 5:
            continue

        avg_R = np.mean(Rs)
        winrate = np.mean([r > 0 for r in Rs]) * 100
        variance = np.var(Rs)

        # -----------------------------
        # 1️⃣ Bayesian Confidence Curve
        # -----------------------------
        confidence_weight = 1 - np.exp(-n / 20)

        # -----------------------------
        # 2️⃣ Variance Penalty
        # High variance reduces trust
        # -----------------------------
        variance_penalty = 1 / (1 + variance)

        # -----------------------------
        # 3️⃣ Adjusted Expectancy
        # -----------------------------
        adjusted_avg_R = avg_R * confidence_weight * variance_penalty

        stats[setup] = {
            "avg_R": round(adjusted_avg_R, 3),
            "raw_avg_R": round(avg_R, 3),
            "samples": n,
            "winrate": round(winrate, 1),
            "variance": round(variance, 3),
            "confidence_weight": round(confidence_weight, 3),
        }

    return stats if stats else None
```

#### `analytics/setup_intelligence.py`
```python
# analytics/setup_intelligence.py

from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_stability import calculate_edge_stability


def get_setup_intelligence(setup_type, regime, ml_probability=None):
    """
    Combines:
    - Setup historical expectancy
    - Regime expectancy
    - ML probability
    - Edge stability

    Returns:
        {
            "score": float (0-1),
            "confidence_boost": float,
            "risk_boost": float
        }
    """

    score = 0.5  # neutral baseline
    confidence_boost = 0
    risk_boost = 0

    # -------------------------
    # 1️⃣ Setup Expectancy Layer
    # -------------------------

    setup_stats = calculate_setup_expectancy()

    if setup_stats and setup_type in setup_stats:
        avg_R = setup_stats[setup_type]["avg_R"]

        if avg_R > 0:
            score += min(avg_R * 0.2, 0.15)
            risk_boost += 0.05

        if avg_R < 0:
            score -= 0.1
            risk_boost -= 0.05

    # -------------------------
    # 2️⃣ Regime Alignment Layer
    # -------------------------

    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:
        regime_avg = regime_stats[regime]["avg_R"]

        if regime_avg > 0:
            score += 0.05
        if regime_avg < 0:
            score -= 0.05

    # -------------------------
    # 3️⃣ ML Probability Layer
    # -------------------------

    if ml_probability is not None:
        score += (ml_probability - 0.5) * 0.3

    # -------------------------
    # 4️⃣ Edge Stability Layer
    # -------------------------

    stability_data = calculate_edge_stability()

    if stability_data:
        stability = stability_data["stability"]

        score += (stability - 0.5) * 0.1

    # Clamp final score
    score = max(0.0, min(score, 1.0))

    return {
        "score": round(score, 3),
        "confidence_boost": round(confidence_boost, 3),
        "risk_boost": round(risk_boost, 3)
    }
```

#### `analytics/signal_logger.py`
```python
# analytics/signal_logger.py
#
# One CSV row per signal evaluation cycle — opened or blocked.
# Gives you a fully labeled dataset of every decision the bot made,
# without waiting for fills. Use for gate calibration and ML training.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "signal_log.csv")
HEADERS = [
    "timestamp",
    "outcome",           # "opened" | "blocked"
    "block_reason",
    "regime",
    "volatility",
    "direction_60m",
    "confidence_60m",
    "direction_15m",
    "confidence_15m",
    "dual_alignment",
    "conviction_score",
    "impulse",
    "follow_through",    # ctx.follow — same value, "follow_through" is the descriptive label
    "blended_score",
    "threshold",
    "threshold_delta",   # blended - threshold (positive = passed gate)
    "ml_weight",
    "regime_samples",
    "expectancy_samples",
    "regime_transition",         # bool: was a regime transition detected?
    "regime_transition_severity",
    "spy_price",
]


def _ensure_file() -> None:
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe_round(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_signal_attempt(ctx, trade=None) -> None:
    """
    Call once per auto_trader cycle, after open_trade_if_valid() returns.

    Parameters
    ----------
    ctx   : DecisionContext — fully populated by open_trade_if_valid()
    trade : the return value from open_trade_if_valid() (dict on success, else None/str)
    """
    try:
        _ensure_file()

        outcome = getattr(ctx, "outcome", "blocked")
        blended = getattr(ctx, "blended_score", None)
        threshold = getattr(ctx, "threshold", None)
        delta = (
            _safe_round(blended - threshold, 6)
            if blended is not None and threshold is not None
            else ""
        )

        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            outcome,
            getattr(ctx, "block_reason", None) or "",
            getattr(ctx, "regime", None) or "",
            getattr(ctx, "volatility", None) or "",
            getattr(ctx, "direction_60m", None) or "",
            _safe_round(getattr(ctx, "confidence_60m", None)),
            getattr(ctx, "direction_15m", None) or "",
            _safe_round(getattr(ctx, "confidence_15m", None)),
            getattr(ctx, "dual_alignment", ""),
            getattr(ctx, "conviction_score", "") if getattr(ctx, "conviction_score", None) is not None else "",
            _safe_round(getattr(ctx, "impulse", None)),
            _safe_round(getattr(ctx, "follow", None)),
            _safe_round(blended),
            _safe_round(threshold),
            delta,
            _safe_round(getattr(ctx, "ml_weight", None)),
            getattr(ctx, "regime_samples", "") if getattr(ctx, "regime_samples", None) is not None else "",
            getattr(ctx, "expectancy_samples", "") if getattr(ctx, "expectancy_samples", None) is not None else "",
            getattr(ctx, "regime_transition", ""),
            _safe_round(getattr(ctx, "regime_transition_severity", None)),
            _safe_round(getattr(ctx, "spy_price", None)),
        ]

        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass  # never crash the watcher
```

#### `analytics/sim_features.py`
```python
from analytics.indicators import compute_indicators, compute_zscores
from analytics.market_regime import compute_market_regime
from analytics.iv_features import compute_iv_features
from analytics.options_greeks import extract_greeks
from signals.volatility import volatility_state
from signals.session_classifier import classify_session


def compute_sim_features(df, context: dict | None = None, option_snapshot=None) -> dict:
    """
    Additive features for SIM analytics and later portfolio optimization.
    Safe to call every bar; does not alter any trading decisions.
    """
    context = context or {}
    features = {}
    orb_minutes = context.get("orb_minutes", 15)
    zscore_window = context.get("zscore_window", 30)
    features.update(compute_indicators(df, orb_minutes=orb_minutes))
    features.update(compute_zscores(df, window=zscore_window))
    features.update(compute_market_regime(df))
    try:
        features["volatility_state"] = volatility_state(df)
    except Exception:
        features["volatility_state"] = None
    try:
        ts = context.get("timestamp") or context.get("entry_time")
        features["session"] = classify_session(ts)
    except Exception:
        features["session"] = "UNKNOWN"

    # Option greeks (if snapshot provided)
    if option_snapshot is not None:
        features.update(extract_greeks(option_snapshot))

    # IV features (proxy)
    try:
        iv_series = context.get("iv_series")
    except Exception:
        iv_series = None
    features.update(compute_iv_features(iv_series, features.get("iv")))

    # Context passthrough
    for key in ("direction", "signal_mode", "horizon", "dte_min", "dte_max"):
        if key in context:
            features[key] = context.get(key)

    return features
```

#### `analytics/stability_mode.py`
```python
# analytics/stability_mode.py

from analytics.edge_stability import calculate_edge_stability

def get_stability_mode():
    """
    Returns current defense stage + control multipliers.
    """

    stats = calculate_edge_stability()

    if not stats:
        return {
            "mode": "NORMAL",
            "risk_multiplier": 1.0,
            "threshold_buffer": 0.0
        }

    stability = stats.get("stability", 1)
    avg_R = stats.get("avg_R", 0)

    # -----------------------------
    # 🟢 NORMAL MODE
    # -----------------------------
    if stability >= 0.60 and avg_R >= 0:
        return {
            "mode": "NORMAL",
            "risk_multiplier": 1.0,
            "threshold_buffer": 0.0
        }

    # -----------------------------
    # 🟡 SOFT DEFENSE
    # -----------------------------
    if stability >= 0.40:
        return {
            "mode": "SOFT_DEFENSE",
            "risk_multiplier": 0.6,      # 40% risk reduction
            "threshold_buffer": 0.03     # Slightly harder to enter
        }

    # -----------------------------
    # 🔴 HARD DEFENSE
    # -----------------------------
    return {
        "mode": "HARD_DEFENSE",
        "risk_multiplier": 0.3,      # 70% risk reduction
        "threshold_buffer": 0.07     # Much harder to enter
    }
```

#### `analytics/structure_pricing.py`
```python
def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def long_option_max_loss(premium: float, qty: int) -> float | None:
    prem = _safe_float(premium)
    if prem is None:
        return None
    return prem * float(qty) * 100


def long_option_break_even(strike: float, premium: float, option_type: str) -> float | None:
    strike_val = _safe_float(strike)
    prem = _safe_float(premium)
    if strike_val is None or prem is None:
        return None
    opt = (option_type or "").upper()
    if opt == "CALL":
        return strike_val + prem
    if opt == "PUT":
        return strike_val - prem
    return None


def long_option_pnl(entry_price: float, exit_price: float, qty: int) -> float | None:
    entry_val = _safe_float(entry_price)
    exit_val = _safe_float(exit_price)
    if entry_val is None or exit_val is None:
        return None
    return (exit_val - entry_val) * float(qty) * 100
```

#### `core/account_repository.py`
```python
# core/account_repository.py
import os
import json
import shutil
import logging
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")
CAREER_FILE = os.path.join(DATA_DIR, "career_stats.json")


def _backup_paths():
    return (
        f"{ACCOUNT_FILE}.bak",
        f"{ACCOUNT_FILE}.bak1",
        f"{ACCOUNT_FILE}.bak2",
        f"{ACCOUNT_FILE}.bak3",
    )


def _rotate_account_backups():
    bak, bak1, bak2, bak3 = _backup_paths()

    if os.path.exists(bak3):
        os.remove(bak3)
    if os.path.exists(bak2):
        os.replace(bak2, bak3)
    if os.path.exists(bak1):
        os.replace(bak1, bak2)

    if os.path.exists(ACCOUNT_FILE):
        shutil.copy2(ACCOUNT_FILE, bak)
        os.replace(bak, bak1)


def _load_newest_valid_account_backup():
    _, bak1, bak2, bak3 = _backup_paths()
    for path in (bak1, bak2, bak3):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
    return None


def load_account():
    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except json.JSONDecodeError as e:
        recovered = _load_newest_valid_account_backup()
        if recovered is None:
            raise RuntimeError("account_primary_corrupt_no_valid_backup") from e
        logging.error("account_recovered_from_backup")
        acc = recovered

    today = date.today().isoformat()

    if acc.get("last_trade_day") != today:
        acc["daily_loss"] = 0
        acc["last_trade_day"] = today
        save_account(acc)

    return acc


def save_account(acc):
    tmp_path = f"{ACCOUNT_FILE}.tmp"

    try:
        # Inline test idea: interrupt process before os.replace to verify primary file remains intact.
        with open(tmp_path, "w") as f:
            json.dump(acc, f, indent=4)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, ACCOUNT_FILE)
        dir_fd = os.open(os.path.dirname(ACCOUNT_FILE), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        # Inline test idea: corrupt primary account.json and ensure load_account restores from bak1/bak2/bak3.
        _rotate_account_backups()
        # Inline test idea: after repeated saves, verify only bak1..bak3 remain and oldest rolls off.
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def load_career():
    with open(CAREER_FILE, "r") as f:
        return json.load(f)


def save_career(data):
    with open(CAREER_FILE, "w") as f:
        json.dump(data, f, indent=4)
```

#### `core/data_integrity.py`
```python
import pandas as pd
import pytz


def validate_market_dataframe(df):
    """
    Validate market dataframe integrity before trading decisions.

    Returns:
        {
            "valid": bool,
            "errors": [str, ...]
        }

    Inline test ideas:
    - Missing minute scenario:
      A dataframe with 09:30, 09:31, 09:33 in the same RTH session should fail continuity.
    - Duplicate timestamp scenario:
      A dataframe with duplicated 10:15 timestamp should fail duplicate check.
    - Valid dataframe scenario:
      Continuous 1-minute RTH bars, unique/monotonic index, sane OHLC should pass.
    """
    errors = []

    # A) Empty or insufficient length
    if df is None:
        return {"valid": False, "errors": ["df_none"]}

    if len(df) < 30:
        errors.append("insufficient_length_lt_30")

    if not isinstance(df.index, pd.DatetimeIndex):
        errors.append("index_not_datetimeindex")
        return {"valid": False, "errors": errors}

    # C) Duplicate timestamps
    if df.index.has_duplicates:
        errors.append("duplicate_timestamps")

    # D) Monotonic index (strictly increasing)
    if not df.index.is_monotonic_increasing:
        errors.append("index_not_monotonic_increasing")

    # B) Timestamp continuity during RTH (09:30-16:00 America/New_York)
    eastern = pytz.timezone("America/New_York")
    if df.index.tz is None:
        idx_eastern = df.index.tz_localize("UTC").tz_convert(eastern)
    else:
        idx_eastern = df.index.tz_convert(eastern)

    df_eastern = df.copy()
    df_eastern.index = idx_eastern

    rth_df = df_eastern.between_time("09:30", "16:00")
    if not rth_df.empty:
        for session_date, session in rth_df.groupby(rth_df.index.date):
            session_idx = session.index.sort_values()
            if session_idx.empty:
                continue
            expected = pd.date_range(
                start=session_idx[0],
                end=session_idx[-1],
                freq="1min",
                tz=session_idx.tz
            )
            missing = expected.difference(session_idx)
            if len(missing) > 5:
                errors.append(
                    f"missing_rth_minutes:{session_date}:count={len(missing)}"
                )

    # E) OHLC sanity
    required_cols = ["open", "high", "low", "close"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        errors.append(f"missing_ohlc_columns:{','.join(missing_cols)}")
    else:
        o = pd.Series(pd.to_numeric(df["open"], errors="coerce"), index=df.index)
        h = pd.Series(pd.to_numeric(df["high"], errors="coerce"), index=df.index)
        l = pd.Series(pd.to_numeric(df["low"], errors="coerce"), index=df.index)
        c = pd.Series(pd.to_numeric(df["close"], errors="coerce"), index=df.index)

        invalid_nan = o.isna() | h.isna() | l.isna() | c.isna()
        if invalid_nan.any():
            errors.append("ohlc_non_numeric_or_nan")

        bad_high = h < pd.concat([o, c], axis=1).max(axis=1)
        bad_low = l > pd.concat([o, c], axis=1).min(axis=1)
        bad_range = h < l
        if bad_high.any() or bad_low.any() or bad_range.any():
            errors.append("ohlc_sanity_violation")

    return {"valid": len(errors) == 0, "errors": errors}
```

#### `core/data_service.py`
```python
# core/data_service.py

import os
import fcntl
import logging
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import pytz
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from dotenv import load_dotenv
from core.market_clock import market_is_open
from core.rate_limiter import rate_limit_sleep

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")

_client = None


def get_client():
    global _client
    if _client is None:
        if not API_KEY or not SECRET_KEY:
            logging.error("alpaca_keys_missing: data_service client not initialized")
            return None
        _client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    return _client


def get_market_dataframe():
    """
    Returns full SPY dataframe with indicators.
    Never fails due to small dataset.
    Only returns None if data truly unavailable.
    """

    df = None
    open_now = market_is_open()

    # -----------------------------
    # Try Local File First
    # -----------------------------
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", newline="") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    df = pd.read_csv(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            logging.warning("data_service_csv_read_failed: %s", DATA_FILE)
            df = None
    else:
        logging.warning("data_service_csv_missing: %s", DATA_FILE)

    # -----------------------------
    # If file missing or unreadable, fetch
    # -----------------------------
    if df is None or df.empty:
        df = _fetch_from_alpaca()
        if df is not None:
            df.attrs["source"] = "alpaca"
        else:
            logging.warning("data_service_alpaca_fallback_failed: no data")
    else:
        # Check staleness on CSV before using it
        try:
            temp = df.copy()
            if "timestamp" in temp.columns:
                temp["timestamp"] = pd.to_datetime(temp["timestamp"], errors="coerce")
                temp = temp.dropna(subset=["timestamp"])
                if not temp.empty:
                    last_ts = temp["timestamp"].iloc[-1]
                    if isinstance(last_ts, pd.Timestamp):
                        if last_ts.tzinfo is None:
                            last_ts = last_ts.tz_localize("US/Eastern")
                        else:
                            last_ts = last_ts.tz_convert("US/Eastern")
                    now = datetime.now(pytz.timezone("US/Eastern"))
                    age_seconds = (now - last_ts).total_seconds()
                    if age_seconds > 110:
                        if open_now:
                            fresh = _fetch_from_alpaca()
                            if fresh is not None and not fresh.empty:
                                fresh.attrs["source"] = "alpaca"
                                df = fresh
                            else:
                                df.attrs["source"] = "csv_stale"
                        else:
                            # Market closed: keep CSV if present; fetch only if CSV is unusable
                            if df is None or df.empty:
                                fresh = _fetch_from_alpaca()
                                if fresh is not None and not fresh.empty:
                                    fresh.attrs["source"] = "alpaca"
                                    df = fresh
                                else:
                                    df = None
                            if df is not None:
                                df.attrs["source"] = "csv_closed"
                    else:
                        df.attrs["source"] = "csv"
        except Exception:
            if df is not None:
                df.attrs["source"] = "csv"

    # -----------------------------
    # If still nothing → real failure
    # -----------------------------
    if df is None or len(df) == 0:
        return None

    # -----------------------------
    # Prepare dataframe safely
    # -----------------------------
    df = _prepare_dataframe(df)

    # If preparation failed completely
    if df is None or len(df) == 0:
        return None

    try:
        df.attrs["market_open"] = open_now
        df.attrs["market_status"] = "open" if open_now else "closed"
    except Exception:
        pass

    return df

def get_recent_candles(n=60):
    df = get_market_dataframe()
    if df is None:
        return None
    return df.tail(n)


def get_latest_price():
    df = get_market_dataframe()
    if df is None:
        return None
    return df.iloc[-1]["close"]


def get_price_at(timestamp):
    df = get_market_dataframe()
    if df is None:
        return None

    target = pd.to_datetime(timestamp)
    df["diff"] = abs(df.index - target)
    row = df.loc[df["diff"].idxmin()]
    return row

def _prepare_dataframe(df):

    # -----------------------------
    # Validate timestamp
    # -----------------------------
    if "timestamp" not in df.columns:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    if df.empty:
        return None

    # -----------------------------
    # Set index properly
    # -----------------------------
    df = df.set_index("timestamp")

    # Remove duplicate timestamps
    df = df[~df.index.duplicated(keep="last")]

    # Strict sort
    df = df.sort_index()

    # Ensure ordered DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        return None

    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    # -----------------------------
    # MINIMUM SAFETY SIZE
    # -----------------------------
    if len(df) < 5:
        # Not enough for indicators yet
        return df

    # -----------------------------
    # SAFE INDICATORS
    # -----------------------------

    # EMA safe
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()

    # RSI safe
    try:
        df["rsi"] = ta.rsi(df["close"], length=14)
    except:
        df["rsi"] = None

    # ATR safe
    try:
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    except:
        df["atr"] = None

    # VWAP SAFE (critical fix)
    try:
        if len(df) > 10:
            df["vwap"] = ta.vwap(
                df["high"],
                df["low"],
                df["close"],
                df["volume"]
            )
        else:
            df["vwap"] = None
    except:
        df["vwap"] = None

    # Do NOT dropna() globally anymore
    # That was silently killing small datasets

    return df


def _fetch_from_alpaca():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    if now.weekday() == 5:
        now -= timedelta(days=1)
    elif now.weekday() == 6:
        now -= timedelta(days=2)

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    end = now.replace(hour=16, minute=0, second=0, microsecond=0)

    start_utc = market_open.astimezone(pytz.UTC)
    end_utc = end.astimezone(pytz.UTC)

    client = get_client()
    if client is None:
        return None

    request = StockBarsRequest(
        symbol_or_symbols="SPY",
        timeframe=TimeFrame(1, TimeFrameUnit("Min")),
        start=start_utc,
        end=end_utc,
        feed=DataFeed.IEX
    )

    # Rate-limit Alpaca calls
    rate_limit_sleep("alpaca_stock_bars", ALPACA_MIN_CALL_INTERVAL_SEC)
    bars = client.get_stock_bars(request)
    df = getattr(bars, "df", None)

    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    df = df.reset_index()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("US/Eastern")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)

    return df
```

#### `core/debug.py`
```python
import time

DEBUG_MODE = True
_last_debug_times = {}


def debug_log(event, **fields):
    if not DEBUG_MODE:
        return
    now = time.time()
    last_time = _last_debug_times.get(event)
    if last_time is not None and (now - last_time) < 60:
        return
    _last_debug_times[event] = now

    if not fields:
        print(f"[DEBUG] {event}")
        return

    ordered = ", ".join(f"{k}={fields[k]}" for k in sorted(fields))
    print(f"[DEBUG] {event} | {ordered}")
```

#### `core/decision_context.py`
```python
from datetime import datetime
import pytz


class DecisionContext:
    def __init__(self):
        self.timestamp = datetime.now(pytz.timezone("US/Eastern"))
        self.regime = None
        self.volatility = None
        self.direction_60m = None
        self.confidence_60m = None
        self.direction_15m = None
        self.confidence_15m = None
        self.dual_alignment = None
        self.conviction_score = None
        self.impulse = None
        self.follow = None
        self.blended_score = None
        self.threshold = None
        self.ml_weight = None
        self.regime_samples = None
        self.expectancy_samples = None
        self.block_reason = None
        self.outcome = "blocked"
        # --- data collection extensions ---
        self.spy_price = None                   # SPY close at decision time
        self.regime_transition = None           # bool: transition detected?
        self.regime_transition_severity = None  # float severity 0–1

    def set_block(self, reason):
        self.block_reason = reason
        self.outcome = "blocked"

    def set_opened(self):
        self.block_reason = None
        self.outcome = "opened"

    def snapshot_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "regime": self.regime,
            "volatility": self.volatility,
            "direction_60m": self.direction_60m,
            "confidence_60m": self.confidence_60m,
            "direction_15m": self.direction_15m,
            "confidence_15m": self.confidence_15m,
            "dual_alignment": self.dual_alignment,
            "conviction_score": self.conviction_score,
            "impulse": self.impulse,
            "follow": self.follow,
            "blended_score": self.blended_score,
            "threshold": self.threshold,
            "ml_weight": self.ml_weight,
            "regime_samples": self.regime_samples,
            "expectancy_samples": self.expectancy_samples,
            "block_reason": self.block_reason,
            "outcome": self.outcome,
            "spy_price": self.spy_price,
            "regime_transition": self.regime_transition,
            "regime_transition_severity": self.regime_transition_severity,
        }
```

#### `core/market_clock.py`
```python
# core/market_clock.py

from datetime import datetime
import pytz


def market_is_open():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    if now.weekday() >= 5:
        return False

    minutes = now.hour * 60 + now.minute
    return 570 <= minutes < 960  # 9:30–4:00
```

#### `core/md_state.py`
```python
import os
import json
from datetime import datetime, timedelta
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "md_state.json")


def _now_et():
    return datetime.now(pytz.timezone("US/Eastern"))


def _load_state() -> dict:
    default = {
        "enabled": False,
        "last_decay": None,
        "last_changed": None,
    }
    if not os.path.exists(FILE):
        return default
    try:
        with open(FILE, "r") as f:
            data = json.load(f) or {}
        return {**default, **data}
    except Exception:
        return default


def _save_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(FILE), exist_ok=True)
        with open(FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def is_md_enabled() -> bool:
    return bool(_load_state().get("enabled"))


def set_md_enabled(enabled: bool) -> dict:
    state = _load_state()
    state["enabled"] = bool(enabled)
    state["last_changed"] = _now_et().isoformat()
    _save_state(state)
    return state


def record_md_decay(ts: datetime | None = None) -> dict:
    state = _load_state()
    dt = ts if ts is not None else _now_et()
    state["last_decay"] = dt.isoformat()
    _save_state(state)
    return state


def get_md_state() -> dict:
    return _load_state()


def md_needs_warning(state: dict | None = None, max_age_minutes: int = 30) -> bool:
    st = state or _load_state()
    if not st.get("enabled"):
        return False
    last_decay = st.get("last_decay")
    if not last_decay:
        return True
    try:
        dt = datetime.fromisoformat(last_decay)
    except Exception:
        return True
    if dt.tzinfo is None:
        dt = pytz.timezone("US/Eastern").localize(dt)
    age = (_now_et() - dt).total_seconds()
    return age > max_age_minutes * 60
```

#### `core/paths.py`
```python
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
CHART_DIR = os.path.join(BASE_DIR, "charts")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CHART_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
```

#### `core/rate_limiter.py`
```python
import time
import threading
import logging

# Simple shared rate limiter for Alpaca REST calls.
# Use rate_limit_wait() to get a suggested delay and optionally sleep.

_LOCK = threading.Lock()
_LAST_CALL = {}


def rate_limit_wait(key: str, min_interval: float) -> float:
    """
    Returns seconds to wait before the next call for this key.
    Also reserves the next slot to prevent thundering herds.
    """
    now = time.monotonic()
    with _LOCK:
        last = _LAST_CALL.get(key, 0.0)
        wait = max(0.0, float(min_interval) - (now - last))
        # Reserve the next slot even if we need to wait
        _LAST_CALL[key] = now + wait if wait > 0 else now
    return wait


def rate_limit_sleep(key: str, min_interval: float, sleep_fn=time.sleep) -> float:
    """
    Sleep for the required wait time (if any) and return the wait.
    """
    wait = rate_limit_wait(key, min_interval)
    if wait > 0:
        logging.warning("alpaca_rate_limit_wait: key=%s wait=%.2fs", key, wait)
        sleep_fn(wait)
    return wait
```

#### `core/session_scope.py`
```python
import pandas as pd
from typing import cast
import pytz


EASTERN_TZ = pytz.timezone("US/Eastern")


def get_rth_session_view(df):
    """
    Return current-session Regular Trading Hours (09:30-16:00 ET) slice.
    Handles both tz-aware and naive indices.
    """
    if df is None or df.empty:
        return df

    if not isinstance(df.index, pd.DatetimeIndex):
        return df

    idx = df.index

    # Normalize index to US/Eastern.
    # If naive, this project's data convention is local ET timestamps.
    if idx.tz is None:
        idx_eastern = idx.tz_localize(EASTERN_TZ)
    else:
        idx_eastern = idx.tz_convert(EASTERN_TZ)

    df_eastern = df.copy()
    df_eastern.index = idx_eastern

    # Current session date from latest bar.
    last_ts = cast(pd.Timestamp, df_eastern.index[-1])
    session_date = last_ts.date()
    session_mask = df_eastern.index.to_series().dt.date == session_date
    session_df = df_eastern[session_mask]
    if session_df.empty:
        return df

    # Strict regular trading hours only.
    rth_df = session_df.between_time("09:30", "16:00")
    if rth_df.empty:
        rth_df = session_df

    return rth_df
```

#### `core/startup_sync.py`
```python
import os
import uuid
from datetime import datetime
import pytz

from core.account_repository import load_account, save_account
from core.debug import debug_log


def _parse_occ_option_symbol(symbol):
    if not isinstance(symbol, str):
        return None
    idx = 0
    while idx < len(symbol) and symbol[idx].isalpha():
        idx += 1
    if idx < 1:
        return None
    underlying = symbol[:idx]
    expected_min_length = idx + 6 + 1 + 8
    if len(symbol) != expected_min_length:
        return None
    expiry_raw = symbol[idx : idx + 6]
    contract_type = symbol[idx + 6 : idx + 7]
    strike_raw = symbol[idx + 7 : idx + 15]
    if not expiry_raw.isdigit():
        return None
    if contract_type not in {"C", "P"}:
        return None
    if not strike_raw.isdigit() or len(strike_raw) != 8:
        return None
    expiry = f"20{expiry_raw[0:2]}-{expiry_raw[2:4]}-{expiry_raw[4:6]}"
    strike = int(strike_raw) / 1000.0
    trade_type = "BULLISH" if contract_type == "C" else "BEARISH"
    return {
        "underlying": underlying,
        "expiry": expiry,
        "strike": strike,
        "contract_type": "CALL" if contract_type == "C" else "PUT",
        "type": trade_type,
    }


async def perform_startup_broker_sync(bot):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        debug_log("startup_sync_skipped", reason="missing_api_keys")
        return

    from alpaca.trading.client import TradingClient

    acc = load_account()
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []

    client = TradingClient(api_key, secret_key, paper=True)
    try:
        positions = client.get_all_positions()
    except Exception as e:
        debug_log("startup_sync_failed", error=str(e))
        return

    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        orders_request = GetOrdersRequest(status=QueryOrderStatus.ALL, limit=50)
        orders = client.get_orders(orders_request)
    except Exception:
        orders = []

    broker_positions = {}
    for pos in positions:
        symbol = getattr(pos, "symbol", None)
        if symbol:
            broker_positions[str(symbol)] = pos

    broker_symbols = set(broker_positions.keys())
    internal_symbols = set(
        t.get("option_symbol")
        for t in open_trades
        if isinstance(t, dict) and t.get("option_symbol")
    )
    open_trade = acc.get("open_trade")
    if isinstance(open_trade, dict):
        open_symbol = open_trade.get("option_symbol")
        if open_symbol:
            internal_symbols.add(open_symbol)

    # ----------------------------
    # Reconstruct missing broker positions
    # ----------------------------
    reconstructed = []
    for symbol, pos in broker_positions.items():
        if symbol in internal_symbols:
            continue
        qty_raw = getattr(pos, "qty", None)
        avg_price_raw = getattr(pos, "avg_entry_price", None)
        if qty_raw is None or avg_price_raw is None:
            continue
        try:
            qty = float(qty_raw)
            avg_price = float(avg_price_raw)
        except (TypeError, ValueError):
            continue
        occ = _parse_occ_option_symbol(symbol) or {}
        reconstructed_trade = {
            "trade_id": uuid.uuid4().hex,
            "option_symbol": symbol,
            "quantity": int(abs(qty)),
            "entry_price": avg_price,
            "fill_price": avg_price,
            "type": occ.get("type"),
            "underlying": occ.get("underlying"),
            "expiry": occ.get("expiry"),
            "strike": occ.get("strike"),
            "contract_type": occ.get("contract_type"),
            "reconstructed": True,
            "protection_policy": {
                "mode": "emergency_stop_only",
                "max_loss_pct": 0.50,
                "min_hold_seconds": 30,
                "created_at": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            },
            "last_manage_ts": None,
            "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "risk": None,
        }
        open_trades.append(reconstructed_trade)
        reconstructed.append(reconstructed_trade)
        debug_log("startup_reconstructed_trade", symbol=symbol, qty=qty)

    if reconstructed:
        channel_id = getattr(bot, "paper_channel_id", None)
        channel = bot.get_channel(channel_id) if channel_id else None
        if channel is not None:
            await channel.send("⚠️ Reconstructed open broker position at startup.")

    # ----------------------------
    # Finalize missing internal trades
    # ----------------------------
    def _find_sell_order(symbol):
        for order in orders or []:
            order_symbol = getattr(order, "symbol", None)
            if order_symbol != symbol:
                continue
            status = getattr(order, "status", None)
            if status and str(status).lower() not in {"filled", "partially_filled"}:
                continue
            side = getattr(order, "side", None)
            intent = getattr(order, "position_intent", None)
            if intent and "SELL_TO_CLOSE" in str(intent):
                pass
            elif side and str(side).lower() == "sell":
                pass
            else:
                continue
            price = getattr(order, "filled_avg_price", None)
            if price is None:
                price = getattr(order, "limit_price", None)
            if price is None:
                continue
            qty = getattr(order, "filled_qty", None)
            if qty is None:
                qty = getattr(order, "qty", None)
            return price, qty
        return None, None

    remaining_open_trades = []
    closed_trades = []
    for t in open_trades:
        if not isinstance(t, dict):
            continue
        symbol = t.get("option_symbol")
        if not symbol:
            remaining_open_trades.append(t)
            continue
        if symbol in broker_symbols:
            remaining_open_trades.append(t)
            continue
        closed_trades.append(t)

    if isinstance(open_trade, dict):
        symbol = open_trade.get("option_symbol")
        if symbol and symbol not in broker_symbols:
            closed_trades.append(open_trade)
            acc["open_trade"] = None

    trade_log = acc.get("trade_log", [])
    if not isinstance(trade_log, list):
        trade_log = []

    for trade in closed_trades:
        symbol = trade.get("option_symbol")
        sell_price_raw, sell_qty_raw = _find_sell_order(symbol)
        try:
            sell_price = float(sell_price_raw) if sell_price_raw is not None else None
        except (TypeError, ValueError):
            sell_price = None
        try:
            sell_qty = int(float(sell_qty_raw)) if sell_qty_raw is not None else None
        except (TypeError, ValueError):
            sell_qty = None

        entry_price = trade.get("entry_price") or trade.get("fill_price")
        try:
            entry_price = float(entry_price) if entry_price is not None else None
        except (TypeError, ValueError):
            entry_price = None

        quantity = trade.get("quantity") or trade.get("size")
        try:
            quantity = int(quantity) if quantity is not None else None
        except (TypeError, ValueError):
            quantity = None

        pnl = None
        if sell_price is not None and entry_price is not None and quantity:
            pnl = (sell_price - entry_price) * quantity

        result = "unknown"
        result_reason = None
        if sell_price is None or entry_price is None or quantity is None:
            result = "closed_unknown"
            result_reason = "closed_unknown"
        elif pnl is not None:
            if pnl > 0:
                result = "win"
            elif pnl < 0:
                result = "loss"
            else:
                result = "breakeven"

        trade_record = {
            "trade_id": trade.get("trade_id"),
            "entry_time": trade.get("entry_time"),
            "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "type": trade.get("type"),
            "style": trade.get("style", "unknown"),
            "risk": trade.get("risk"),
            "R": None,
            "regime": trade.get("regime"),
            "setup": trade.get("setup", "UNKNOWN"),
            "underlying": trade.get("underlying"),
            "strike": trade.get("strike"),
            "expiry": trade.get("expiry"),
            "option_symbol": symbol,
            "quantity": quantity,
            "confidence": trade.get("confidence", 0),
            "result": result,
            "pnl": pnl,
            "balance_after": acc.get("balance"),
            "reconstructed": trade.get("reconstructed", False),
            "offline_close": True,
            "result_reason": result_reason,
        }
        trade_log.append(trade_record)
        debug_log("startup_closed_missing_trade", symbol=symbol)

    acc["open_trades"] = remaining_open_trades
    acc["trade_log"] = trade_log

    save_account(acc)
```

#### `decision/trader.py`
```python
# decision/trader.py
from analytics.career_updater import update_career_after_trade
from analytics.risk_control import get_dynamic_risk_percent
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.edge_decay import edge_decay_status
from core.md_state import is_md_enabled
from analytics.feature_logger import log_trade_features, FEATURE_FILE
from analytics.ml_loader import load_edge_model, build_feature_vector
from analytics.adaptive_threshold import adaptive_ml_threshold
from analytics.progressive_influence import get_ml_weight
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.review_engine import review_trade
from analytics.stability_mode import get_stability_mode
from analytics.setup_intelligence import get_setup_intelligence
from analytics.edge_compression import get_edge_compression
from analytics.regime_transition import detect_regime_transition
from analytics.regime_persistence import calculate_regime_persistence
from analytics.regime_memory import get_regime_memory

from datetime import datetime, timedelta, date
import pytz
import os
import uuid
import time
import threading
import logging

from core.account_repository import (
    load_account,
    save_account,
)

from core.data_service import (
    get_latest_price,
    get_market_dataframe
)
from core.market_clock import market_is_open
from core.debug import debug_log
from core.decision_context import DecisionContext
from research.train_ai import train_direction_model, train_edge_model

from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state
from signals.setup_classifier import classify_trade
from signals.conviction import calculate_conviction
from signals.signal_evaluator import grade_trade
from signals.session_classifier import classify_session

import joblib
from collections import deque
from core.paths import DATA_DIR

from execution.ml_gate import ml_probability_gate
from execution.option_executor import execute_option_entry, close_option_position, get_option_price
from core.rate_limiter import rate_limit_sleep
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.trading.enums import ContractType

from analytics.contract_logger import log_contract_attempt


DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")
ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))
MODEL_RETRAIN_INTERVAL_MINUTES = float(os.getenv("ML_RETRAIN_INTERVAL_MINUTES", "1440"))
# If ML_MODEL_MAX_AGE_HOURS is not set, align model staleness with retrain interval
MODEL_MAX_AGE_HOURS = float(
    os.getenv("ML_MODEL_MAX_AGE_HOURS", str(MODEL_RETRAIN_INTERVAL_MINUTES / 60.0))
)
MODEL_RETRAIN_MIN_TRADES = int(os.getenv("ML_RETRAIN_MIN_TRADES", "50"))

direction_model = None
edge_model = None
_TRAINING = False
_LAST_RETRAIN_TS = 0.0

# ==============================
# ML WARMUP CONFIG
# ==============================
MIN_TRADES_FOR_ML = 50  # change this if you want
MAX_OPEN_TRADES = 3
RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED = False


# ==============================
# CONFIDENCE DISTRIBUTION STATS
# ==============================
_conf_stats = {
    "samples": 0,
    "conf_15m_values": [],
    "conf_60m_values": [],
    "conf_15m_ge_055": 0,
    "conf_60m_ge_055": 0,
    "dual_alignment_ge_055": 0
}

_blend_history = deque(maxlen=20)
_threshold_history = deque(maxlen=20)

_gate_stats = {
    "attempts": 0,
    "blocked": {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    }
}

_daily_stats = {
    "date": None,
    "summary_logged": False,
    "attempts": 0,
    "trades_opened": 0,
    "blocks": {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    },
    "conf_15m_values": [],
    "conf_60m_values": [],
    "dual_alignment_ge_055": 0,
    "blended_scores": [],
    "thresholds": [],
    "misaligned_warning_logged": False
}


def _reset_daily_stats(for_date):
    _daily_stats["date"] = for_date
    _daily_stats["summary_logged"] = False
    _daily_stats["attempts"] = 0
    _daily_stats["trades_opened"] = 0
    _daily_stats["blocks"] = {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    }
    _daily_stats["conf_15m_values"] = []
    _daily_stats["conf_60m_values"] = []
    _daily_stats["dual_alignment_ge_055"] = 0
    _daily_stats["blended_scores"] = []
    _daily_stats["thresholds"] = []
    _daily_stats["misaligned_warning_logged"] = False


def _get_daily_trade_stats(acc, day):
    trades = acc.get("trade_log", [])
    day_trades = []
    for trade in trades:
        exit_time = trade.get("exit_time")
        if not exit_time:
            continue
        try:
            exit_dt = datetime.fromisoformat(exit_time)
        except Exception:
            continue
        if exit_dt.date() == day:
            day_trades.append(trade)

    if not day_trades:
        return None, None

    wins = sum(1 for t in day_trades if t.get("result") == "win")
    winrate = (wins / len(day_trades)) * 100 if day_trades else None

    r_values = [
        float(t.get("R"))
        for t in day_trades
        if t.get("R") is not None
    ]
    avg_r = (sum(r_values) / len(r_values)) if r_values else None
    return winrate, avg_r


def _emit_daily_summary(acc, day):
    if _daily_stats["summary_logged"]:
        return

    attempts = _daily_stats["attempts"]
    blocks = _daily_stats["blocks"]

    def pct(count):
        return round((count / attempts) * 100, 1) if attempts > 0 else 0.0

    avg_15 = (
        sum(_daily_stats["conf_15m_values"]) / len(_daily_stats["conf_15m_values"])
        if _daily_stats["conf_15m_values"]
        else None
    )
    avg_60 = (
        sum(_daily_stats["conf_60m_values"]) / len(_daily_stats["conf_60m_values"])
        if _daily_stats["conf_60m_values"]
        else None
    )
    avg_blended = (
        sum(_daily_stats["blended_scores"]) / len(_daily_stats["blended_scores"])
        if _daily_stats["blended_scores"]
        else None
    )
    avg_threshold = (
        sum(_daily_stats["thresholds"]) / len(_daily_stats["thresholds"])
        if _daily_stats["thresholds"]
        else None
    )
    winrate, avg_r = _get_daily_trade_stats(acc, day)

    debug_log(
        "daily_calibration_summary",
        day=str(day),
        total_signal_attempts=attempts,
        total_trades_opened=_daily_stats["trades_opened"],
        block_pct_regime=pct(blocks["regime"]),
        block_pct_volatility=pct(blocks["volatility"]),
        block_pct_confidence=pct(blocks["confidence"]),
        block_pct_ml_threshold=pct(blocks["ml_threshold"]),
        block_pct_expectancy=pct(blocks["expectancy"]),
        block_pct_protection_layer=pct(blocks["protection_layer"]),
        avg_15m_confidence=round(avg_15, 3) if avg_15 is not None else "N/A",
        avg_60m_confidence=round(avg_60, 3) if avg_60 is not None else "N/A",
        avg_blended_score=round(avg_blended, 3) if avg_blended is not None else "N/A",
        avg_threshold=round(avg_threshold, 3) if avg_threshold is not None else "N/A",
        winrate=round(winrate, 1) if winrate is not None else "N/A",
        avg_R_multiple=round(avg_r, 3) if avg_r is not None else "N/A"
    )
    _daily_stats["summary_logged"] = True


def _roll_daily_summary_if_needed(acc, now_eastern):
    today = now_eastern.date()

    if _daily_stats["date"] is None:
        _reset_daily_stats(today)
        return

    if today != _daily_stats["date"]:
        _emit_daily_summary(acc, _daily_stats["date"])
        _reset_daily_stats(today)


def _record_signal_attempt():
    _gate_stats["attempts"] += 1
    _daily_stats["attempts"] += 1
    attempts = _gate_stats["attempts"]
    if attempts % 25 == 0:
        blocked = _gate_stats["blocked"]
        debug_log(
            "gate_breakdown_summary",
            attempts=attempts,
            blocked_regime=blocked["regime"],
            blocked_volatility=blocked["volatility"],
            blocked_confidence=blocked["confidence"],
            blocked_ml_threshold=blocked["ml_threshold"],
            blocked_expectancy=blocked["expectancy"],
            blocked_protection_layer=blocked["protection_layer"]
        )


def _record_gate_block(category):
    if category in _gate_stats["blocked"]:
        _gate_stats["blocked"][category] += 1
    if category in _daily_stats["blocks"]:
        _daily_stats["blocks"][category] += 1


def _category_for_block_reason(reason):
    if not reason:
        return None
    if reason.startswith("protection_"):
        return "protection_layer"
    if reason.startswith("regime_"):
        return "regime"
    if reason.startswith("volatility_"):
        return "volatility"
    if reason in {"confidence", "direction_mismatch"}:
        return "confidence"
    if reason in {"ml_threshold"}:
        return "ml_threshold"
    if reason in {"expectancy_negative_regime"}:
        return "expectancy"
    return None


def get_ml_visibility_snapshot():
    avg_blended = None
    avg_threshold = None
    if _blend_history:
        avg_blended = sum(_blend_history) / len(_blend_history)
    if _threshold_history:
        avg_threshold = sum(_threshold_history) / len(_threshold_history)

    delta = None
    if avg_blended is not None and avg_threshold is not None:
        delta = avg_blended - avg_threshold

    return {
        "ml_weight": get_ml_weight(),
        "avg_blended": avg_blended,
        "avg_threshold": avg_threshold,
        "avg_delta": delta,
    }


def _track_confidence_distribution(bias, trigger):
    bias_conf = float(bias.get("confidence", 0))
    trigger_conf = float(trigger.get("confidence", 0))
    aligned_direction = bias.get("direction") == trigger.get("direction")
    dual_conf_aligned = (
        bias_conf >= 0.55 and trigger_conf >= 0.55 and aligned_direction
    )

    _conf_stats["samples"] += 1
    _conf_stats["conf_60m_values"].append(bias_conf)
    _conf_stats["conf_15m_values"].append(trigger_conf)
    _daily_stats["conf_60m_values"].append(bias_conf)
    _daily_stats["conf_15m_values"].append(trigger_conf)

    if bias_conf >= 0.55:
        _conf_stats["conf_60m_ge_055"] += 1
    if trigger_conf >= 0.55:
        _conf_stats["conf_15m_ge_055"] += 1
    if dual_conf_aligned:
        _conf_stats["dual_alignment_ge_055"] += 1
        _daily_stats["dual_alignment_ge_055"] += 1

    samples = _conf_stats["samples"]
    if samples % 30 == 0:
        pct_15 = (_conf_stats["conf_15m_ge_055"] / samples) * 100
        pct_60 = (_conf_stats["conf_60m_ge_055"] / samples) * 100
        pct_dual = (_conf_stats["dual_alignment_ge_055"] / samples) * 100

        debug_log(
            "confidence_distribution_summary",
            samples=samples,
            pct_15m_ge_055=round(pct_15, 1),
            pct_60m_ge_055=round(pct_60, 1),
            pct_dual_alignment_ge_055=round(pct_dual, 1)
        )

    daily_samples = len(_daily_stats["conf_15m_values"])
    if (
        daily_samples >= 100
        and not _daily_stats["misaligned_warning_logged"]
    ):
        dual_pct = (_daily_stats["dual_alignment_ge_055"] / daily_samples) * 100
        if dual_pct < 2:
            debug_log(
                "confidence_threshold_misaligned",
                samples=daily_samples,
                dual_alignment_ge_055_pct=round(dual_pct, 2)
            )
            _daily_stats["misaligned_warning_logged"] = True

def load_models():
    global direction_model, edge_model
    if direction_model is None and os.path.exists(DIR_MODEL_FILE):
        if _model_is_fresh(DIR_MODEL_FILE):
            try:
                direction_model = joblib.load(DIR_MODEL_FILE)
            except Exception:
                direction_model = None
        else:
            debug_log("ml_model_stale", model="direction_model")

    if edge_model is None and os.path.exists(EDGE_MODEL_FILE):
        if _model_is_fresh(EDGE_MODEL_FILE):
            try:
                edge_model = joblib.load(EDGE_MODEL_FILE)
            except Exception:
                edge_model = None
        else:
            debug_log("ml_model_stale", model="edge_model")


def _model_is_fresh(path: str) -> bool:
    try:
        if not os.path.exists(path):
            return False
        if MODEL_MAX_AGE_HOURS <= 0:
            return True
        age_sec = time.time() - os.path.getmtime(path)
        return age_sec <= (MODEL_MAX_AGE_HOURS * 3600)
    except Exception:
        return False


def _feature_trade_count() -> int:
    try:
        if not os.path.exists(FEATURE_FILE):
            return 0
        with open(FEATURE_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return max(0, len(lines) - 1)
    except Exception:
        return 0


def maybe_retrain_models() -> None:
    global _TRAINING, _LAST_RETRAIN_TS, direction_model, edge_model
    if _TRAINING:
        return
    # Only retrain if we have enough data
    trade_count = _feature_trade_count()
    if trade_count < MODEL_RETRAIN_MIN_TRADES:
        return
    # Enforce time-based retrain interval
    now = time.time()
    if _LAST_RETRAIN_TS and (now - _LAST_RETRAIN_TS) < (MODEL_RETRAIN_INTERVAL_MINUTES * 60):
        return

    _TRAINING = True

    def _run():
        global _TRAINING, _LAST_RETRAIN_TS, direction_model, edge_model
        try:
            train_direction_model()
            train_edge_model()
            # Reset loaded models so next call reloads fresh models
            direction_model = None
            edge_model = None
            _LAST_RETRAIN_TS = time.time()
            debug_log("ml_retrain_completed", trade_count=trade_count)
        except Exception:
            logging.exception("ml_retrain_failed")
        finally:
            _TRAINING = False

    threading.Thread(target=_run, daemon=True).start()

def build_ml_features(df, trade, conviction_score, impulse, follow):
    """
    Build ML feature vector from current state.
    Must match training feature order exactly.
    """

    last = df.iloc[-1]

    regime_map = {"TREND": 1, "RANGE": 2, "VOLATILE": 3, "COMPRESSION": 4}
    vol_map = {"DEAD": 1, "LOW": 2, "NORMAL": 3, "HIGH": 4}

    regime_encoded = regime_map.get(trade["regime"], 0)
    volatility_encoded = vol_map.get(volatility_state(df), 0)

    features = [[
        regime_encoded,
        volatility_encoded,
        conviction_score,
        impulse,
        follow,
        trade["confidence"]
    ]]

    return features

# =========================
# DAY TRADE LIMIT CHECK
# =========================

def can_day_trade(acc):
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    acc["day_trades"] = [
        t for t in acc["day_trades"]
        if datetime.fromisoformat(t) > now - timedelta(days=5)
    ]

    return len(acc["day_trades"]) < 3

# =========================
# STYLE SELECTION
# =========================

def select_style(regime, volatility, conviction_score):

    if regime == "TREND" and conviction_score >= 5:
        return "momentum"

    if regime == "TREND" and conviction_score >= 3:
        return "mini_swing"

    if regime == "RANGE":
        return "scalp"

    if volatility == "HIGH" and conviction_score >= 4:
        return "momentum"

    return "scalp"


def _get_option_client():
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None
    return OptionHistoricalDataClient(api_key, secret_key)


def _parse_option_symbol(symbol):
    # OCC format: SPYyymmddC/P######## (strike with 3 decimals)
    if not symbol or len(symbol) < 15:
        return None
    if not symbol.startswith("SPY"):
        return None
    tail = symbol[3:]
    if len(tail) < 15:
        return None
    date_part = tail[:6]
    cp = tail[6:7]
    strike_part = tail[7:]
    if not (date_part.isdigit() and strike_part.isdigit() and cp in {"C", "P"}):
        return None
    yy = int(date_part[0:2])
    mm = int(date_part[2:4])
    dd = int(date_part[4:6])
    expiry = date(2000 + yy, mm, dd)
    strike = int(strike_part) / 1000.0
    return expiry, cp, strike


def _select_option_contract(direction, underlying_price):
    client = _get_option_client()
    if client is None or underlying_price is None:
        return None, None

    eastern = pytz.timezone("US/Eastern")
    today = datetime.now(eastern).date()
    contract_type = ContractType.CALL if direction == "bullish" else ContractType.PUT

    # Prefer same-day expiry when market is open.
    expiry_date = today if market_is_open() else None

    request = OptionChainRequest(
        underlying_symbol="SPY",
        type=contract_type,
        expiration_date=expiry_date,
        strike_price_gte=underlying_price * 0.9,
        strike_price_lte=underlying_price * 1.1,
    )

    rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
    chain = client.get_option_chain(request)
    if not chain:
        request = OptionChainRequest(
            underlying_symbol="SPY",
            type=contract_type,
            expiration_date_gte=today,
            strike_price_gte=underlying_price * 0.9,
            strike_price_lte=underlying_price * 1.1,
        )
        rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
        chain = client.get_option_chain(request)

    if not chain:
        return None, None

    # Alpaca SDK may return a custom mapping type rather than a plain dict on
    # some SDK versions.  Normalise to dict so .items() is guaranteed to work.
    if not isinstance(chain, dict):
        try:
            chain = dict(chain)
        except (TypeError, ValueError):
            return None, None
    if not chain:
        return None, None

    candidates = []
    for symbol, snap in chain.items():
        parsed = _parse_option_symbol(symbol)
        if not parsed:
            continue
        exp, cp, strike = parsed
        if (cp == "C" and contract_type != ContractType.CALL) or (
            cp == "P" and contract_type != ContractType.PUT
        ):
            continue
        quote = getattr(snap, "latest_quote", None)
        bid = quote.bid_price if quote is not None else None
        ask = quote.ask_price if quote is not None else None
        if bid is None or ask is None:
            continue
        if bid <= 0 or ask <= 0:
            continue
        entry_price = (bid + ask) / 2
        candidates.append((symbol, exp, strike, bid, ask, entry_price))

    if not candidates:
        return None, None

    # Prefer nearest expiry (same-day already filtered if available), then closest ATM strike.
    candidates.sort(key=lambda x: (abs((x[1] - today).days), abs(x[2] - underlying_price)))
    attempts = 0
    for symbol, exp, strike, bid, ask, entry_price in candidates:
        if attempts >= 3:
            break
        attempts += 1
        spread = ask - bid
        if ask <= 0 or spread < 0:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="invalid_quote", bid=bid, ask=ask,
            )
            continue
        spread_pct = spread / ask
        if spread_pct > 0.15:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="spread_too_wide",
                bid=bid, ask=ask, spread_pct=spread_pct,
                mid=round((bid + ask) / 2, 4),
            )
            continue
        log_contract_attempt(
            source="main", direction=direction, underlying_price=underlying_price,
            expiry=exp, dte=abs((exp - today).days), strike=strike,
            result="selected", reason="selected",
            bid=bid, ask=ask, spread_pct=spread_pct,
            mid=round((bid + ask) / 2, 4),
        )
        return {
            "symbol": symbol,
            "expiry": exp.isoformat(),
            "strike": strike,
            "entry_price": float(entry_price),
            "bid": float(bid),
            "ask": float(ask),
        }, None

    return None, "spread_too_wide"
def apply_ml_and_edge_filters(
    acc,
    df,
    regime,
    vol_state,
    direction,
    confidence,
    score,
    impulse,
    follow,
    setup_type,
    ctx
):

    style = select_style(regime, vol_state, score)
    threshold = None

    total_trades = len(acc.get("trade_log", []))
    ml_weight_current = get_ml_weight()
    ctx.ml_weight = ml_weight_current
    
    # ----------------------------------
    # HARD ML WARMUP BYPASS
    # ----------------------------------
    if total_trades < MIN_TRADES_FOR_ML:
        style = select_style(regime, vol_state, score)

        conviction_norm = min(score / 6, 1.0)
        ctx.blended_score = conviction_norm
        ctx.threshold = None
        debug_log(
            "trade_filter_pass",
            layer="ml_warmup_bypass",
            threshold="N/A",
            blended_score=round(conviction_norm, 3),
            total_trades=total_trades,
            ml_weight=round(ml_weight_current, 3)
        )

        return True, conviction_norm, style

    # ------------------------------
    # Load models if needed
    # ------------------------------
    load_models()

    # ------------------------------
    # Get ML probability
    # ------------------------------
    allow_ml, ml_probability = ml_probability_gate(
        df,
        regime,
        score,
        impulse,
        follow,
        confidence,
        total_trades,
        direction_model,
        edge_model
    )

    # ------------------------------
    # Progressive Influence
    # ------------------------------
    if ml_probability is None:
        blended_score = confidence
    else:
        ml_weight = get_ml_weight()

        conviction_norm = min(score / 6, 1.0)

        blended_score = (
            conviction_norm * (1 - ml_weight)
            + ml_probability * ml_weight
        )

    # ------------------------------
    # Setup Intelligence Layer
    # ------------------------------

    intelligence = get_setup_intelligence(
        setup_type,
        regime,
        ml_probability
    )

    intelligence_score = intelligence["score"]

    # Blend intelligence with blended_score
    blended_score = (blended_score * 0.7) + (intelligence_score * 0.3)

    transition_data = detect_regime_transition()
    ctx.regime_transition = transition_data["transition"]
    ctx.regime_transition_severity = transition_data["severity"]
    # ------------------------------
    # Adaptive Threshold
    # ------------------------------
    threshold = adaptive_ml_threshold(total_trades)

    if transition_data["transition"]:
        threshold += transition_data["severity"] * 0.05

    # ------------------------------
    # Regime Stability Influence
    # ------------------------------

    persistence_data = calculate_regime_persistence()
    memory_data = get_regime_memory()

    # If persistence low → tighten
    if persistence_data["persistence"] < 0.6:
        threshold += 0.03

    # If new regime → distrust slightly
    threshold += (1 - memory_data["trust"]) * 0.05


    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:

        regime_conf = regime_stats[regime]["confidence"]
        regime_avg_R = regime_stats[regime]["avg_R"]
        regime_samples = regime_stats[regime].get(
            "regime_sample_count",
            regime_stats[regime].get("trades", 0)
        )
        ctx.regime_samples = regime_samples

        # Penalize negative expectancy regimes
        if regime_samples >= 20 and regime_avg_R < 0:
            ctx.set_block("expectancy_negative_regime")
            debug_log(
                "trade_blocked",
                gate="apply_ml_and_edge_filters",
                reason="negative_regime_expectancy",
                regime_samples=regime_samples,
                threshold=round(threshold, 3),
                blended_score=round(blended_score, 3)
            )
            return False, None, None

        # Tighten threshold if regime unstable
        if regime_samples >= 20 and regime_conf < 0.3:
            ctx.set_block("regime_low_confidence")
            debug_log(
                "trade_blocked",
                gate="apply_ml_and_edge_filters",
                reason="low_regime_confidence",
                regime_samples=regime_samples,
                threshold=round(threshold, 3),
                blended_score=round(blended_score, 3)
            )
            return False, None, None

    # Early stage forgiveness
    confidence_decay = 1 - get_ml_weight()
    threshold -= 0.05 * confidence_decay
    # ------------------------------
    # Stability Mode Tightening
    # ------------------------------
    mode = get_stability_mode()

    threshold += mode["threshold_buffer"]

    debug_log(
        "ml_visibility",
        total_trades=total_trades,
        ml_weight=round(ml_weight_current, 3),
        threshold=round(threshold, 3),
        blended_score=round(blended_score, 3)
    )
    ctx.blended_score = blended_score
    ctx.threshold = threshold
    _daily_stats["blended_scores"].append(float(blended_score))
    _daily_stats["thresholds"].append(float(threshold))
    _blend_history.append(float(blended_score))
    _threshold_history.append(float(threshold))
    if len(_blend_history) == 20 and len(_threshold_history) == 20:
        avg_blended_last20 = sum(_blend_history) / 20
        avg_threshold_last20 = sum(_threshold_history) / 20
        debug_log(
            "ml_window_summary",
            samples=20,
            avg_blended_score_last20=round(avg_blended_last20, 3),
            avg_threshold_last20=round(avg_threshold_last20, 3)
        )

    if blended_score < threshold:
        ctx.set_block("ml_threshold")
        debug_log(
            "trade_blocked",
            gate="apply_ml_and_edge_filters",
            reason="blended_below_threshold",
            threshold=round(threshold, 3),
            blended_score=round(blended_score, 3)
        )
        return False, None, None

    # ------------------------------
    # Setup Expectancy Influence
    # ------------------------------
    setup_stats = calculate_setup_expectancy()

    if setup_stats and setup_type in setup_stats:
        ctx.expectancy_samples = setup_stats[setup_type].get(
            "trades",
            setup_stats[setup_type].get("count")
        )

        avg_R = setup_stats[setup_type]["avg_R"]

        if avg_R < 0:
            style = "scalp"
        elif avg_R > 1.0:
            style = "momentum"

    debug_log(
        "trade_filter_pass",
        layer="apply_ml_and_edge_filters",
        threshold=round(threshold, 3),
        blended_score=round(blended_score, 3),
        style=style
    )
    return True, blended_score, style
# =========================
# OPEN TRADE ENGINE
# =========================

async def open_trade_if_valid(ctx=None):
    if ctx is None:
        ctx = DecisionContext()

    _record_signal_attempt()

    acc = load_account()
    eastern = pytz.timezone("US/Eastern")
    now_eastern = datetime.now(eastern)
    _roll_daily_summary_if_needed(acc, now_eastern)

    # ----------------------------
    # 1️⃣ Pre-Trade Protection Layer
    # ----------------------------
    protection = pre_trade_checks(acc, ctx)
    if protection is not None:
        if ctx.block_reason is None:
            ctx.set_block(f"protection_{protection}")
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return protection

    # ----------------------------
    # 2️⃣ Signal Generation Layer
    # ----------------------------
    signal = generate_signal(acc, ctx)
    if signal is None:
        if ctx.block_reason is None:
            ctx.set_block("signal_none")
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return None

    df, regime, vol_state, direction, confidence, score, impulse, follow, price, setup_type = signal

    # ----------------------------
    # 3️⃣ ML + Edge Filtering Layer
    # ----------------------------
    allow, blended_score, style = apply_ml_and_edge_filters(
        acc,
        df,
        regime,
        vol_state,
        direction,
        confidence,
        score,
        impulse,
        follow,
        setup_type,
        ctx
    )
    
    if not allow:
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return None

    # ----------------------------
    # 4️⃣ Execution Plan Layer
    # ----------------------------
    execution = build_execution_plan(
        acc,
        df,
        regime,
        vol_state,
        direction,
        style,
        price,
        setup_type
    )

    if execution is None:
        ctx.set_block("execution_plan_none")
        return None
    if isinstance(execution, dict) and execution.get("block_reason"):
        ctx.set_block(execution["block_reason"])
        return None
    if not isinstance(execution, tuple) or len(execution) != 4:
        ctx.set_block("execution_plan_none")
        return None

    risk_dollars, trade_size, option, target_R = execution
    try:
        risk_dollars = float(risk_dollars)
        trade_size = int(trade_size)
        target_R = float(target_R)
    except (TypeError, ValueError):
        ctx.set_block("execution_plan_none")
        return None
    if not isinstance(option, dict):
        ctx.set_block("execution_plan_none")
        return None

    virtual_cap = acc.get("virtual_capital_limit", acc["balance"])
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []

    open_trades_updated = False
    for t in open_trades:
        if isinstance(t, dict) and t.get("trade_id") is None:
            t["trade_id"] = str(uuid.uuid4())
            open_trades_updated = True
    if open_trades_updated:
        acc["open_trades"] = open_trades
        save_account(acc)

    unique_ids = {
        t.get("trade_id")
        for t in open_trades
        if isinstance(t, dict) and t.get("trade_id") is not None
    }

    if len(unique_ids) >= MAX_OPEN_TRADES:
        ctx.set_block("max_open_trades_reached")
        return None
    unique_risk = {}
    for t in open_trades:
        if isinstance(t, dict):
            trade_id = t.get("trade_id")
            if trade_id:
                unique_risk[trade_id] = float(t.get("risk", 0))

    open_trade = acc.get("open_trade")
    if isinstance(open_trade, dict):
        trade_id = open_trade.get("trade_id")
        if trade_id and trade_id not in unique_risk:
            unique_risk[trade_id] = float(open_trade.get("risk", 0))

    total_open_risk = sum(unique_risk.values())
    if total_open_risk + risk_dollars > virtual_cap:
        ctx.set_block("capital_exposure_limit")
        return None

    option_symbol = option.get("symbol") if option else None
    bid = option.get("bid") if option else None
    ask = option.get("ask") if option else None
    if not option_symbol or bid is None or ask is None:
        ctx.set_block("execution_plan_none")
        return None
    try:
        bid = float(bid)
        ask = float(ask)
    except (TypeError, ValueError):
        ctx.set_block("execution_plan_none")
        return None
    if ask <= 0 or bid < 0:
        ctx.set_block("execution_plan_none")
        return None
    spread = ask - bid
    if spread < 0:
        ctx.set_block("execution_plan_none")
        return None
    spread_pct = spread / ask
    if spread_pct > 0.15:
        ctx.set_block("spread_too_wide")
        return None

    fill_result, exec_block = await execute_option_entry(option_symbol, trade_size, bid, ask, ctx=ctx, acc=acc)
    if fill_result is None:
        ctx.set_block(exec_block or "limit_not_filled")
        return None
    fill_price = fill_result.get("fill_price")
    filled_qty = fill_result.get("filled_qty")
    requested_qty = fill_result.get("requested_qty")
    fill_ratio = fill_result.get("fill_ratio")
    if fill_price is None or filled_qty is None or requested_qty is None:
        ctx.set_block("limit_not_filled")
        return None
    try:
        filled_qty = int(filled_qty)
        requested_qty = int(requested_qty)
        fill_ratio = float(fill_ratio) if fill_ratio is not None else None
    except (TypeError, ValueError):
        ctx.set_block("limit_not_filled")
        return None
    if filled_qty <= 0:
        ctx.set_block("limit_not_filled")
        return None
    if requested_qty <= 0:
        ctx.set_block("limit_not_filled")
        return None
    if fill_ratio is None:
        fill_ratio = filled_qty / requested_qty
    if fill_ratio < 0.5:
        ctx.set_block("partial_fill_below_threshold")
        return None
    if filled_qty < requested_qty:
        trade_size = filled_qty
        risk_dollars = risk_dollars * fill_ratio

    stop_loss_frac = 0.5
    if is_md_enabled():
        stop_loss_frac = 0.35
    stop = fill_price - (fill_price * stop_loss_frac)
    risk_per_contract = fill_price - stop              # option price units (per share)
    target = fill_price + (risk_per_contract * target_R)
    risk = trade_size * risk_per_contract * 100        # dollars: qty × $/share × 100 shares/contract

    # ----------------------------
    # 5️⃣ Create Trade Object
    # ----------------------------
    trade = create_trade_object(
        direction,
        style,
        fill_price,
        stop,
        target,
        risk,
        trade_size,
        confidence,
        regime,
        vol_state,
        score,
        impulse,
        follow,
        setup_type,
        blended_score,
        ctx
    )
    if option:
        trade["underlying"] = "SPY"
        trade["option_symbol"] = option.get("symbol")
        trade["strike"] = option.get("strike")
        trade["expiry"] = option.get("expiry")
        trade["quantity"] = trade_size
        trade["entry_price"] = fill_price
        trade["stop"] = stop
        trade["initial_stop"] = stop
        trade["target"] = target
        trade["stop_price"] = stop
        trade["target_price"] = target

    acc["open_trade"] = trade
    save_account(acc)
    _daily_stats["trades_opened"] += 1
    ctx.set_opened()
    debug_log(
        "trade_opened",
        direction=trade["type"],
        entry=round(trade["entry_price"], 2),
        confidence=round(trade["confidence"], 3),
        blended=trade.get("ml_probability")
    )

    return trade


def build_execution_plan(
    acc,
    df,
    regime,
    vol_state,
    direction,
    style,
    price,
    setup_type
):
    option, selection_block = _select_option_contract(direction, price)
    if selection_block:
        return {"block_reason": selection_block}
    if option is None:
        return None

    entry_price = option["entry_price"]
    # One options contract = 100 shares.  risk_per_contract is the dollar
    # loss if the position hits the 50%-of-premium stop.
    risk_per_contract = entry_price * 100 * 0.5   # e.g. $1.50 mid → $75 risk/contract
    if risk_per_contract <= 0:
        return None

    # ----------------------------
    # Target Based on Style
    # ----------------------------
    if style == "momentum":
        target_R = 2.5
    elif style == "mini_swing":
        target_R = 2.0
    else:
        target_R = 1.2

    # ----------------------------
    # Position Sizing
    # ----------------------------
    risk_percent = get_dynamic_risk_percent(acc)
    debug_log("risk_percent_update", percent=risk_percent)
    effective_balance = min(
        acc["balance"],
        acc.get("virtual_capital_limit", acc["balance"])
    )
    risk_dollars = effective_balance * risk_percent
    if risk_dollars < 50:
        risk_dollars = 50

    quantity = int(risk_dollars // risk_per_contract)
    if quantity <= 0:
        return None

    compression = get_edge_compression()
    quantity = int(quantity * compression["position_multiplier"])
    if quantity <= 0:
        return None

    return risk_dollars, quantity, option, target_R

# =========================
# TRADE MANAGEMENT ENGINE
# =========================
def _finalize_reconstructed_trade(acc, trade, pnl, result_reason):
    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    result = "win" if pnl > 0 else "loss"
    if result == "win":
        acc["wins"] = acc.get("wins", 0) + 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "entry_time": trade.get("entry_time"),
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("emergency_exit_price"),
        "pnl": pnl,
        "result": result,
        "result_reason": result_reason,
        "reconstructed": True,
        "R": None,
        "risk_unknown": True,
        "balance_after": acc["balance"],
    }

    trade_log = acc.get("trade_log", [])
    if not isinstance(trade_log, list):
        trade_log = []
    trade_log.append(trade_record)
    acc["trade_log"] = trade_log

    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []
    acc["open_trades"] = [
        t for t in open_trades if not (isinstance(t, dict) and t.get("trade_id") == trade.get("trade_id"))
    ]

    save_account(acc)
    return result, pnl, acc["balance"], trade


def _manage_reconstructed_trades(acc):
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list) or not open_trades:
        return None

    now = datetime.now(pytz.timezone("US/Eastern"))
    for trade in open_trades:
        if not isinstance(trade, dict) or not trade.get("reconstructed"):
            continue
        if trade.get("stop") is not None or trade.get("target") is not None:
            continue
        policy = trade.get("protection_policy", {})
        max_loss_pct = policy.get("max_loss_pct", 0.50)
        min_hold_seconds = policy.get("min_hold_seconds", 0)
        created_at = policy.get("created_at")
        if created_at:
            try:
                created_dt = datetime.fromisoformat(created_at)
                if created_dt.tzinfo is None:
                    created_dt = pytz.timezone("US/Eastern").localize(created_dt)
                if (now - created_dt).total_seconds() < float(min_hold_seconds):
                    continue
            except Exception:
                pass

        entry_price = trade.get("entry_price")
        option_symbol = trade.get("option_symbol")
        qty = trade.get("quantity")
        if entry_price is None or option_symbol is None or qty is None:
            continue
        try:
            entry_price = float(entry_price)
            qty = int(qty)
        except (TypeError, ValueError):
            continue
        if qty <= 0 or entry_price <= 0:
            continue

        current_price = get_option_price(option_symbol)
        if current_price is None:
            continue

        trade["last_manage_ts"] = now.isoformat()

        if current_price <= entry_price * (1 - float(max_loss_pct)):
            close_result = close_option_position(option_symbol, qty)
            filled_avg = close_result.get("filled_avg_price")
            if close_result.get("ok"):
                exit_price = None
                source = "estimated_mid"
                if filled_avg is not None:
                    exit_price = filled_avg
                    source = "broker_fill"
                else:
                    exit_price = current_price
                trade["emergency_exit_price"] = exit_price
                trade["emergency_exit_price_source"] = source
                pnl = (exit_price - entry_price) * qty * 100
                trade["result_reason"] = "reconstructed_emergency_stop"
                trade["recon_notice"] = {
                    "type": "emergency_stop_success",
                    "symbol": option_symbol,
                    "qty": qty,
                    "entry": entry_price,
                    "price": exit_price,
                    "ts": now.isoformat(),
                }
                return _finalize_reconstructed_trade(
                    acc, trade, pnl, "reconstructed_emergency_stop"
                )

            trade["emergency_stop_failed"] = True
            trade["recon_notice"] = {
                "type": "emergency_stop_failure",
                "symbol": option_symbol,
                "qty": qty,
                "entry": entry_price,
                "price": current_price,
                "ts": now.isoformat(),
            }
            save_account(acc)
            return None

    return None


def _manage_reconstructed_advanced(acc):
    if not RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED:
        return None
    try:
        from core.account_repository import load_account
        from core.paths import DATA_DIR
        import json
        stats_path = os.path.join(DATA_DIR, "career_stats.json")
        with open(stats_path, "r", encoding="utf-8") as f:
            stats = json.load(f)
        closed_count = int(stats.get("total_trades", 0))
    except Exception:
        closed_count = 0
    if closed_count < 20:
        return None
    # TODO: trailing stop / take-profit for reconstructed trades.
    return None


def pre_trade_checks(acc, ctx):

    decay = edge_decay_status()

    if decay["status"] == "DISABLE":
        ctx.set_block("protection_EDGE_DECAY")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="EDGE_DECAY")
        return "EDGE_DECAY"

    if acc["balance"] <= acc["starting_balance"] * 0.85:
        ctx.set_block("protection_EQUITY_PROTECTION")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="EQUITY_PROTECTION")
        return "EQUITY_PROTECTION"

    if acc["daily_loss"] >= acc["max_daily_loss"]:
        ctx.set_block("protection_DAILY_LIMIT")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="DAILY_LIMIT")
        return "DAILY_LIMIT"

    if acc["open_trade"] is not None:
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="OPEN_TRADE_EXISTS")
        return "OPEN_TRADE_EXISTS"

    if not can_day_trade(acc):
        ctx.set_block("protection_PDT_LIMIT")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="PDT_LIMIT")
        return "PDT_LIMIT"

    return None
def generate_signal(acc, ctx):

    df = get_market_dataframe()
    if df is None:
        ctx.set_block("no_market_data")
        debug_log("trade_gate_exit", gate="generate_signal", reason="NO_MARKET_DATA")
        return None

    regime = get_regime(df)
    ctx.regime = regime
    if regime in ["COMPRESSION", "RANGE", "NO_DATA"]:
        ctx.set_block(f"regime_{regime.lower()}")
        debug_log("trade_gate_exit", gate="generate_signal", reason=f"REGIME_{regime}")
        return None

    vol_state = volatility_state(df)
    ctx.volatility = vol_state
    if vol_state in ["DEAD", "LOW"]:
        ctx.set_block(f"volatility_{vol_state.lower()}")
        debug_log("trade_gate_exit", gate="generate_signal", reason=f"VOL_{vol_state}")
        return None

    bias = make_prediction(60, df)
    trigger = make_prediction(15, df)

    if bias is None or trigger is None:
        ctx.set_block("prediction_none")
        debug_log("trade_gate_exit", gate="generate_signal", reason="PREDICTION_NONE")
        return None

    _track_confidence_distribution(bias, trigger)
    ctx.direction_60m = bias.get("direction")
    ctx.confidence_60m = bias.get("confidence")
    ctx.direction_15m = trigger.get("direction")
    ctx.confidence_15m = trigger.get("confidence")
    ctx.dual_alignment = bias.get("direction") == trigger.get("direction")

    if bias["direction"] != trigger["direction"]:
        ctx.set_block("direction_mismatch")
        debug_log(
            "trade_gate_exit",
            gate="generate_signal",
            reason="DIRECTION_MISMATCH",
            bias=bias["direction"],
            trigger=trigger["direction"]
        )
        return None

    if bias["confidence"] < 0.55 or trigger["confidence"] < 0.55:
        ctx.set_block("confidence")
        debug_log(
            "trade_gate_exit",
            gate="generate_signal",
            reason="CONFIDENCE_BELOW_THRESHOLD",
            bias_conf=bias["confidence"],
            trigger_conf=trigger["confidence"]
        )
        return None

    direction = bias["direction"]
    confidence = bias["confidence"]

    price = get_latest_price()
    if price is None:
        ctx.set_block("no_latest_price")
        debug_log("trade_gate_exit", gate="generate_signal", reason="NO_LATEST_PRICE")
        return None

    ctx.spy_price = price
    setup_type = classify_trade(price, direction)

    score, impulse, follow, _ = calculate_conviction(df)
    ctx.conviction_score = score
    ctx.impulse = impulse
    ctx.follow = follow
    debug_log(
        "signal_generated",
        direction=direction,
        confidence=round(confidence, 3),
        regime=regime,
        volatility=vol_state,
        conviction=score
    )

    return (
        df,
        regime,
        vol_state,
        direction,
        confidence,
        score,
        impulse,
        follow,
        price,
        setup_type
    )

def create_trade_object(
    direction,
    style,
    price,
    stop,
    target,
    risk,
    trade_size,
    confidence,
    regime,
    vol_state,
    score,
    impulse,
    follow,
    setup_type,
    blended_score,
    ctx
):

    trade_id = str(uuid.uuid4())
    return {
        "trade_id": trade_id,
        "type": direction,
        "style": style,
        "entry_price": price,
        "size": trade_size,
        "risk": risk,
        "confidence": confidence,
        "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "stop": stop,
        "initial_stop": stop,
        "target": target,
        "regime": regime,
        "volatility": vol_state,
        "conviction_score": score,
        "impulse": impulse,
        "follow_through": follow,
        "ml_probability": round(blended_score, 3) if blended_score else None,
        "setup": setup_type,
        "underlying": None,
        "strike": None,
        "expiry": None,
        "option_symbol": None,
        "quantity": None,
        "decision_snapshot": {
            "timestamp": ctx.timestamp.isoformat(),
            "regime": ctx.regime,
            "volatility": ctx.volatility,
            "direction_60m": ctx.direction_60m,
            "confidence_60m": ctx.confidence_60m,
            "direction_15m": ctx.direction_15m,
            "confidence_15m": ctx.confidence_15m,
            "dual_alignment": ctx.dual_alignment,
            "conviction_score": ctx.conviction_score,
            "impulse": ctx.impulse,
            "follow": ctx.follow,
            "blended_score": ctx.blended_score,
            "threshold": ctx.threshold,
            "threshold_delta": (
                round(ctx.blended_score - ctx.threshold, 6)
                if ctx.blended_score is not None and ctx.threshold is not None
                else None
            ),
            "ml_weight": ctx.ml_weight,
            "regime_samples": ctx.regime_samples,
            "expectancy_samples": ctx.expectancy_samples
        },
        "runner_active": False,
        "partial_taken": False,
        "regime_transition_at_entry": getattr(ctx, "regime_transition", None),
        "regime_transition_severity": getattr(ctx, "regime_transition_severity", None),
    }
def manage_trade():

    acc = load_account()

    recon_result = _manage_reconstructed_trades(acc)
    if recon_result:
        return recon_result

    advanced_result = _manage_reconstructed_advanced(acc)
    if advanced_result:
        return advanced_result

    if acc["open_trade"] is None:
        return None

    trade = acc["open_trade"]

    # Use the live option price for all stop/target comparisons.
    # stop and target are expressed in option-price units, so comparing
    # them to the underlying stock price (get_latest_price) is wrong.
    option_symbol = trade.get("option_symbol") if isinstance(trade, dict) else None
    if not option_symbol:
        return None
    price = get_option_price(option_symbol)
    if price is None:
        return None

    # Expiry handling: close same-day expiry positions 5 minutes before close
    try:
        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        expiry_raw = trade.get("expiry")
        expiry_date = None
        if isinstance(expiry_raw, str):
            expiry_date = datetime.fromisoformat(expiry_raw).date()
        if expiry_date == now_et.date() and now_et.time() >= datetime.strptime("15:55", "%H:%M").time():
            result = "win" if price >= trade["entry_price"] else "loss"
            pnl = calculate_pnl(trade, result, price)
            return finalize_trade(acc, trade, "expiry_close", pnl)
    except Exception:
        pass

    # 1️⃣ Expectancy Protection
    expectancy_exit = check_expectancy_exit(acc, trade, price)
    if expectancy_exit:
        return expectancy_exit

    # 2️⃣ Partial Logic
    partial = check_partial_logic(acc, trade, price)
    if partial is not None:
        return None

    # 3️⃣ Hard Exit Conditions
    exit_result = check_exit_conditions(trade, price)
    if exit_result is None:
        return None

    result = exit_result

    # 4️⃣ Calculate PnL
    pnl = calculate_pnl(trade, result, price)

    # 5️⃣ Finalize Trade
    return finalize_trade(acc, trade, result, pnl)

def check_expectancy_exit(acc, trade, price):
    """
    price must be the current option price (not stock price).
    Uses the same R-based P&L formula as calculate_pnl so that balance
    updates and R_multiple are consistent across all exit paths.
    """
    setup_stats = calculate_setup_expectancy()
    current_setup = trade.get("setup")

    if setup_stats and current_setup in setup_stats:

        avg_R = setup_stats[current_setup]["avg_R"]

        if avg_R < -0.25:
            entry = trade.get("entry_price")
            initial_stop = trade.get("initial_stop") or trade.get("stop")
            risk_amount = trade.get("risk", 0)

            if not entry or not initial_stop or entry == initial_stop or not risk_amount:
                return None

            risk_per = abs(entry - initial_stop)
            move = (price - entry) if trade["type"] == "bullish" else (entry - price)
            move_ratio = move / risk_per
            pnl = risk_amount * move_ratio

            return finalize_trade(acc, trade, "edge_exit", pnl)

    return None
def check_partial_logic(acc, trade, price):

    if trade["style"] != "momentum":
        return None

    if trade["partial_taken"]:
        return None

    hit_target = (
        trade["type"] == "bullish" and price >= trade["target"]
    ) or (
        trade["type"] == "bearish" and price <= trade["target"]
    )

    if not hit_target:
        return None

    move_ratio = abs(price - trade["entry_price"]) / abs(
        trade["entry_price"] - trade["initial_stop"]
    )

    partial_pnl = trade["risk"] * move_ratio * 0.5

    acc["balance"] += partial_pnl

    trade["partial_taken"] = True
    trade["runner_active"] = True
    trade["stop"] = trade["entry_price"]

    acc["open_trade"] = trade
    save_account(acc)

    return True
def check_exit_conditions(trade, price):

    if trade["type"] == "bullish":

        if price <= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price >= trade["target"]:
            return "win"

    if trade["type"] == "bearish":

        if price >= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price <= trade["target"]:
            return "win"

    return None
def calculate_pnl(trade, result, price):

    risk_amount = trade["risk"]

    if trade["style"] == "momentum" and trade["partial_taken"]:
        return 0

    if result == "win":

        move_ratio = abs(price - trade["entry_price"]) / abs(
            trade["entry_price"] - trade["initial_stop"]
        )

        return risk_amount * move_ratio

    return -risk_amount
def finalize_trade(acc, trade, result, pnl):

    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    if result == "win":
        acc["wins"] += 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    if trade["risk"] > 0:
        R_multiple = round(pnl / trade["risk"], 3)
    else:
        R_multiple = 0

    if not trade.get("option_symbol"):
        raise RuntimeError("Missing option metadata")
    if trade.get("quantity") is None or trade.get("quantity") <= 0:
        raise RuntimeError("Invalid quantity for option trade")

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "entry_time": trade["entry_time"],
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "type": trade["type"],
        "style": trade.get("style", "unknown"),
        "risk": trade["risk"],
        "R": R_multiple,
        "regime": trade.get("regime"),
        "setup": trade.get("setup", "UNKNOWN"),
        "underlying": trade.get("underlying"),
        "strike": trade.get("strike"),
        "expiry": trade.get("expiry"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "confidence": trade.get("confidence", 0),
        "result": result,
        "pnl": pnl,
        "balance_after": acc["balance"],
    }
    if trade_record.get("R") is not None:
        if trade_record["R"] > 0:
            trade_record["result"] = "win"
        elif trade_record["R"] < 0:
            trade_record["result"] = "loss"
        else:
            trade_record["result"] = "breakeven"
    review = review_trade(trade_record, result)
    print(review)
    acc["trade_log"].append(trade_record)
    acc["day_trades"].append(datetime.now(pytz.timezone("US/Eastern")).isoformat())
    acc["open_trade"] = None

    save_account(acc)
    # FIX: Trigger periodic ML retraining in background (non-blocking)
    maybe_retrain_models()

    return result, pnl, acc["balance"], trade
```

#### `execution/ml_gate.py`
```python
import numpy as np
from analytics.edge_stability import calculate_edge_stability

def ml_probability_gate(
    df,
    regime,
    conviction_score,
    impulse,
    follow,
    confidence,
    total_trades,
    direction_model,
    edge_model
):
    """
    Soft activation ML gate.
    Gradually blends ML into conviction as data grows.
    """

    # ---------------------------------------
    # If models not loaded → bypass
    # ---------------------------------------
    if direction_model is None or edge_model is None:
        return True, None

    # ---------------------------------------
    # Soft activation weight
    # 0 → pure conviction
    # 1 → pure ML
    # ---------------------------------------
    weight = min(total_trades / 200, 1.0)

    # Build feature vector exactly as trained
    features = [[
        conviction_score,
        impulse,
        follow,
        confidence
    ]]

    try:
        direction_prob = direction_model.predict_proba(features)[0][1]
        edge_prob = edge_model.predict_proba(features)[0][1]
    except Exception:
        return True, None

    ml_score = (direction_prob * 0.6) + (edge_prob * 0.4)

    # Soft blended score
    blended_score = (
        conviction_score / 6 * (1 - weight)
        + ml_score * weight
    )

    threshold = adaptive_ml_threshold(regime)

    allow_trade = blended_score >= threshold

    return allow_trade, round(blended_score, 3)
def adaptive_ml_threshold(regime):

    base = {
        "TREND": 0.55,
        "VOLATILE": 0.60,
        "RANGE": 0.65,
        "COMPRESSION": 0.70
    }.get(regime, 0.60)

    stability_data = calculate_edge_stability()

    if stability_data is None:
        return base - 0.05  # early stage forgiveness

    stability = stability_data["stability"]

    # Reduce threshold if unstable (forgiving early)
    buffer = 0.05 * (1 - stability)

    return round(base - buffer, 3)
```

#### `execution/option_executor.py`
```python
import os
import time
import asyncio
from typing import Any

from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest
import alpaca.data.enums as alpaca_enums
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, PositionIntent, OrderStatus
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep
from analytics.execution_logger import log_execution

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))
_LAST_OPTION_QUOTES: dict[str, dict[str, float]] = {}
_QUOTE_TTL_SECONDS = 120.0


def _extract_snapshot(response, symbol: str):
    if response is None or not symbol:
        return None
    if isinstance(response, dict):
        if symbol in response:
            return response.get(symbol)
        try:
            if len(response) == 1:
                return next(iter(response.values()))
        except Exception:
            pass
    for attr in ("snapshots", "data"):
        data = getattr(response, attr, None)
        if isinstance(data, dict):
            if symbol in data:
                return data.get(symbol)
            try:
                if len(data) == 1:
                    return next(iter(data.values()))
            except Exception:
                pass
        if isinstance(data, list):
            for item in data:
                sym = getattr(item, "symbol", None) if item is not None else None
                if sym is None and isinstance(item, dict):
                    sym = item.get("symbol")
                if sym == symbol:
                    return item
            try:
                if len(data) == 1:
                    return data[0]
            except Exception:
                pass
    try:
        if getattr(response, "symbol", None) == symbol:
            return response
    except Exception:
        pass
    return None


def _get_options_feed():
    feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
    if feed_enum is None:
        return None
    desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
    try:
        if desired == "opra":
            return feed_enum.OPRA
        if desired == "indicative":
            return feed_enum.INDICATIVE
    except Exception:
        return None
    return None


def _build_snapshot_request(symbol: str, feed_override=None):
    feed_val = feed_override if feed_override is not None else _get_options_feed()
    try:
        if feed_val is not None:
            return OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=feed_val)
    except Exception:
        pass
    try:
        return OptionSnapshotRequest(symbol_or_symbols=[symbol])
    except Exception:
        return OptionSnapshotRequest(symbol_or_symbols=symbol)


def _cache_quote(symbol: str, bid: float, ask: float) -> None:
    try:
        _LAST_OPTION_QUOTES[symbol] = {
            "bid": float(bid),
            "ask": float(ask),
            "ts": time.time(),
        }
    except Exception:
        pass


def _get_cached_quote(symbol: str):
    try:
        item = _LAST_OPTION_QUOTES.get(symbol)
        if not item:
            return None
        if (time.time() - float(item.get("ts", 0))) > _QUOTE_TTL_SECONDS:
            return None
        bid = float(item.get("bid", 0))
        ask = float(item.get("ask", 0))
        if bid > 0 and ask > 0:
            return bid, ask
    except Exception:
        return None
    return None


def _get_option_quote(api_key: str, secret_key: str, symbol: str):
    if not symbol or not isinstance(symbol, str) or len(symbol) < 15:
        debug_log("option_snapshot_invalid_symbol", symbol=symbol)
        return None
    client = OptionHistoricalDataClient(api_key, secret_key)
    last_err = None
    feed_val = _get_options_feed()
    for attempt in range(2):
        try:
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            debug_log("option_snapshot_request", symbol=symbol)
            req = _build_snapshot_request(symbol, feed_override=feed_val)
            start_ts = time.time()
            snapshots = client.get_option_snapshot(req)
            elapsed_ms = int((time.time() - start_ts) * 1000)
            snap = _extract_snapshot(snapshots, symbol)
            if snap is None:
                debug_log(
                    "option_snapshot_missing",
                    symbol=symbol,
                    attempt=attempt + 1,
                    response_type=type(snapshots).__name__,
                    response_time_ms=elapsed_ms,
                )
                last_err = "no_snapshot"
                # If no explicit feed set, retry indicative once before backoff
                if feed_val is None:
                    try:
                        feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                        if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                            debug_log("option_snapshot_retry_feed", symbol=symbol, feed="indicative")
                            req = _build_snapshot_request(symbol, feed_override=feed_enum.INDICATIVE)
                            snapshots = client.get_option_snapshot(req)
                            snap = _extract_snapshot(snapshots, symbol)
                            if snap is not None:
                                quote = getattr(snap, "latest_quote", None)
                                if quote and quote.ask_price is not None and quote.bid_price is not None:
                                    bid = quote.bid_price
                                    ask = quote.ask_price
                                    if bid is not None and ask is not None and bid > 0 and ask > 0:
                                        _cache_quote(symbol, float(bid), float(ask))
                                        return float(bid), float(ask)
                    except Exception:
                        pass
                time.sleep(0.2 * (attempt + 1))
                continue
            quote = getattr(snap, "latest_quote", None)
            if quote and quote.ask_price is not None and quote.bid_price is not None:
                bid = quote.bid_price
                ask = quote.ask_price
                if bid is not None and ask is not None and bid > 0 and ask > 0:
                    _cache_quote(symbol, float(bid), float(ask))
                    return float(bid), float(ask)
            last_err = "no_quote"
        except Exception as e:
            last_err = str(e)
            debug_log("option_snapshot_error", symbol=symbol, error=str(e))
            time.sleep(0.2 * (attempt + 1))
    cached = _get_cached_quote(symbol)
    if cached:
        debug_log("option_snapshot_fallback_cached", symbol=symbol)
        return cached
    if last_err:
        debug_log("option_snapshot_failed", symbol=symbol, reason=last_err)
    return None


async def _sleep(seconds: float) -> None:
    try:
        asyncio.get_running_loop()
        await asyncio.sleep(seconds)
    except RuntimeError:
        time.sleep(seconds)


def _increment_no_record_exit(acc, reason: str) -> None:
    if not isinstance(acc, dict):
        return
    stats = acc.get("execution_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    no_record = stats.get("no_record_exits", {})
    if not isinstance(no_record, dict):
        no_record = {}
    no_record[reason] = no_record.get(reason, 0) + 1
    stats["no_record_exits"] = no_record
    acc["execution_stats"] = stats


async def execute_option_entry(option_symbol: str, quantity: int, bid: float, ask: float, ctx=None, acc=None):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        _increment_no_record_exit(acc, "missing_api_keys")
        debug_log("option_snapshot_missing_keys", symbol=option_symbol)
        return None, "missing_api_keys"

    debug_log(
        "option_entry_quote_request",
        symbol=option_symbol,
        qty=quantity,
        bid=bid,
        ask=ask,
    )
    refreshed = await asyncio.to_thread(_get_option_quote, api_key, secret_key, option_symbol)
    if refreshed is None:
        _increment_no_record_exit(acc, "quote_fetch_failed")
        return None, "quote_fetch_failed"
    bid, ask = refreshed

    mid = (bid + ask) / 2
    expected_mid = mid
    spread = ask - bid
    if mid <= 0 or spread < 0:
        _increment_no_record_exit(acc, "invalid_quote_or_spread")
        return None, "invalid_quote_or_spread"
    spread_pct = spread / ask
    if spread_pct > 0.15:
        return None, "spread_too_wide"

    client = TradingClient(api_key, secret_key, paper=True)
    first_limit = round(mid + (spread * 0.25), 2)
    order: Any = client.submit_order(
        LimitOrderRequest(
            symbol=option_symbol,
            qty=quantity,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=float(first_limit),
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
    )

    # --- first attempt: limit at mid + 25% of spread ---
    start = time.time()
    while (time.time() - start) < 5:
        current: Any = client.get_order_by_id(order.id)
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                fill_price = float(current.filled_avg_price)
                fill_ratio = 1.0
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=quantity,
                    ratio=round(fill_ratio, 3),
                    accepted=True
                )
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_mid_plus",
                    qty_requested=quantity, qty_filled=quantity,
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=fill_price, bid_at_order=bid, ask_at_order=ask,
                )
                if fill_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=quantity,
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(fill_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                return {
                    "fill_price": fill_price,
                    "filled_qty": quantity,
                    "partial": False,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
            except (TypeError, ValueError):
                return None, "limit_not_filled"
        filled_qty = getattr(current, "filled_qty", None)
        if filled_qty is not None and current.status != OrderStatus.FILLED:
            try:
                qty_val = int(float(filled_qty))
            except (TypeError, ValueError):
                qty_val = 0
            if qty_val > 0 and current.filled_avg_price is not None:
                try:
                    client.cancel_order_by_id(order.id)
                except Exception:
                    pass
                try:
                    filled_price = float(current.filled_avg_price)
                except (TypeError, ValueError):
                    return None, "limit_not_filled"
                fill_ratio = min(qty_val, quantity) / float(quantity)
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=min(qty_val, quantity),
                    ratio=round(fill_ratio, 3),
                    accepted=fill_ratio >= 0.5
                )
                if fill_ratio < 0.5:
                    try:
                        asyncio.create_task(asyncio.to_thread(close_option_position, option_symbol, min(qty_val, quantity)))
                    except Exception:
                        pass
                    if ctx is not None:
                        ctx.set_block("partial_fill_below_threshold")
                    _increment_no_record_exit(acc, "partial_fill_below_threshold")
                    return None, "partial_fill_below_threshold"
                if filled_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=min(qty_val, quantity),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(filled_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_partial",
                    qty_requested=quantity, qty_filled=min(qty_val, quantity),
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=filled_price, bid_at_order=bid, ask_at_order=ask,
                )
                return {
                    "fill_price": filled_price,
                    "filled_qty": min(qty_val, quantity),
                    "partial": True,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
        if current.status in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}:
            break
        await _sleep(1)

    try:
        client.cancel_order_by_id(order.id)
    except Exception:
        pass

    order: Any = client.submit_order(
        LimitOrderRequest(
            symbol=option_symbol,
            qty=quantity,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=float(ask),
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
    )

    # --- second attempt: limit at ask ---
    start = time.time()
    while (time.time() - start) < 5:
        current: Any = client.get_order_by_id(order.id)
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                fill_price = float(current.filled_avg_price)
                fill_ratio = 1.0
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=quantity,
                    ratio=round(fill_ratio, 3),
                    accepted=True
                )
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_ask",
                    qty_requested=quantity, qty_filled=quantity,
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=fill_price, bid_at_order=bid, ask_at_order=ask,
                )
                if fill_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=quantity,
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(fill_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                return {
                    "fill_price": fill_price,
                    "filled_qty": quantity,
                    "partial": False,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
            except (TypeError, ValueError):
                return None, "limit_not_filled"
        filled_qty = getattr(current, "filled_qty", None)
        if filled_qty is not None and current.status != OrderStatus.FILLED:
            try:
                qty_val = int(float(filled_qty))
            except (TypeError, ValueError):
                qty_val = 0
            if qty_val > 0 and current.filled_avg_price is not None:
                try:
                    client.cancel_order_by_id(order.id)
                except Exception:
                    pass
                try:
                    filled_price = float(current.filled_avg_price)
                except (TypeError, ValueError):
                    return None, "limit_not_filled"
                fill_ratio = min(qty_val, quantity) / float(quantity)
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=min(qty_val, quantity),
                    ratio=round(fill_ratio, 3),
                    accepted=fill_ratio >= 0.5
                )
                if fill_ratio < 0.5:
                    try:
                        asyncio.create_task(asyncio.to_thread(close_option_position, option_symbol, min(qty_val, quantity)))
                    except Exception:
                        pass
                    if ctx is not None:
                        ctx.set_block("partial_fill_below_threshold")
                    _increment_no_record_exit(acc, "partial_fill_below_threshold")
                    return None, "partial_fill_below_threshold"
                if filled_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=min(qty_val, quantity),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(filled_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_partial",
                    qty_requested=quantity, qty_filled=min(qty_val, quantity),
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=filled_price, bid_at_order=bid, ask_at_order=ask,
                )
                return {
                    "fill_price": filled_price,
                    "filled_qty": min(qty_val, quantity),
                    "partial": True,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
        if current.status in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}:
            break
        await _sleep(1)

    try:
        client.cancel_order_by_id(order.id)
    except Exception:
        pass

    _increment_no_record_exit(acc, "limit_not_filled")
    return None, "limit_not_filled"


def close_option_position(option_symbol: str, quantity: int) -> dict:
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return {"ok": False, "filled_avg_price": None, "order_id": None}

    try:
        client = TradingClient(api_key, secret_key, paper=True)
        order = client.submit_order(
            MarketOrderRequest(
                symbol=option_symbol,
                qty=quantity,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                position_intent=PositionIntent.SELL_TO_CLOSE,
            )
        )
    except Exception:
        return {"ok": False, "filled_avg_price": None, "order_id": None}

    order_id = getattr(order, "id", None)
    filled_avg_price = None
    start = time.time()
    while order_id and (time.time() - start) < 10:
        try:
            current: Any = client.get_order_by_id(order_id)
        except Exception:
            break
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                filled_avg_price = float(current.filled_avg_price)
            except (TypeError, ValueError):
                filled_avg_price = None
            break
        time.sleep(1)

    if filled_avg_price is not None:
        log_execution(
            option_symbol=option_symbol, side="exit",
            order_type="market",
            qty_requested=quantity, qty_filled=quantity,
            fill_ratio=1.0, expected_mid=None,
            fill_price=filled_avg_price, bid_at_order=None, ask_at_order=None,
        )

    return {"ok": True, "filled_avg_price": filled_avg_price, "order_id": order_id}


def get_option_price(option_symbol: str):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        debug_log("option_snapshot_missing_keys", symbol=option_symbol)
        return None
    try:
        client = OptionHistoricalDataClient(api_key, secret_key)
        last_err = None
        for attempt in range(2):
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            req = _build_snapshot_request(option_symbol)
            start_ts = time.time()
            snapshots = client.get_option_snapshot(req)
            elapsed_ms = int((time.time() - start_ts) * 1000)
            snap = _extract_snapshot(snapshots, option_symbol)
            if snap is None:
                debug_log(
                    "option_snapshot_missing",
                    symbol=option_symbol,
                    attempt=attempt + 1,
                    response_type=type(snapshots).__name__,
                    response_time_ms=elapsed_ms,
                )
                last_err = "no_snapshot"
                time.sleep(0.2 * (attempt + 1))
                continue
            quote = getattr(snap, "latest_quote", None)
            if quote and quote.ask_price is not None and quote.bid_price is not None:
                bid = float(quote.bid_price)
                ask = float(quote.ask_price)
                if bid > 0 and ask > 0:
                    _cache_quote(option_symbol, bid, ask)
                    return (bid + ask) / 2
            trade = getattr(snap, "latest_trade", None)
            if trade and trade.price is not None:
                try:
                    return float(trade.price)
                except (TypeError, ValueError):
                    pass
            last_err = "no_quote"
        cached = _get_cached_quote(option_symbol)
        if cached:
            debug_log("option_snapshot_fallback_cached", symbol=option_symbol)
            bid, ask = cached
            return (bid + ask) / 2
        if last_err:
            debug_log("option_snapshot_failed", symbol=option_symbol, reason=last_err)
        return None
    except Exception:
        debug_log("option_snapshot_error", symbol=option_symbol, error="exception")
        return None
    return None
```

#### `interface/ai_assistant.py`
```python
import os
import json
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def ask_ai(question, market_summary, paper_stats, career_stats, last_trades):

    system_prompt = f"""
You are an options trading performance coach.

You do NOT hype trades.
You do NOT encourage gambling.
You analyze objectively.

Your job:
Evaluate SPY trading decisions using the user's real trading data.

Market Context:
{market_summary}

Paper Account Stats:
{paper_stats}

Career Stats:
{career_stats}

Recent Trades:
{last_trades}

When answering:
- Explain WHY
- Point out behavioral mistakes
- Mention overtrading, chasing, late entries if seen
- Prefer risk management over prediction
- Keep responses under 800 characters.
- Respond in one short paragraph unless specifically asked for detail.
- Never exceed 250 tokens. One compact paragraph only.
- No essays.
- No bullet walls.
- Be concise and practical.
"""

    response = client.chat.completions.create(
        model="gpt-5-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ],
        max_completion_tokens=300
    )
    content = response.choices[0].message.content
    print("FULL RESPONSE:", response)
    return content
```

#### `interface/bot.py`
```python
# ==============================
# INTERFACE LAYER ONLY
# ==============================

import os
import asyncio
import logging
import re
import csv
import json
import yaml
import discord
import pytz
from datetime import datetime
from discord.ext import commands
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from typing import Any
import joblib

# -------- Interface --------
from interface.fmt import ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col, delta_col, ml_col, exit_reason_col, balance_col, wr_col, tier_col, drawdown_col, signed_col
from interface.charting import generate_chart, generate_live_chart
from interface.health_monitor import start_heartbeat
from interface.watchers import (
    auto_trader,
    conviction_watcher,
    forecast_watcher,
    heart_monitor,
    prediction_grader,
    opportunity_watcher,
    get_decision_buffer_snapshot,
    preopen_check_loop,
    eod_open_trade_report_loop,
    option_chain_health_loop
    
)
from simulation.sim_watcher import sim_entry_loop, sim_exit_loop, sim_eod_report_loop, set_sim_bot, get_sim_last_skip_state
from core.md_state import set_md_enabled, get_md_state, md_needs_warning
# -------- Core --------
from core.market_clock import market_is_open
from core.data_service import get_market_dataframe
from execution.option_executor import get_option_price
from core.session_scope import get_rth_session_view
from core.account_repository import load_account
from core.paths import DATA_DIR
from core.account_repository import load_account

# -------- Decision --------

# -------- Signal --------
from signals.conviction import calculate_conviction
from signals.opportunity import evaluate_opportunity
from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state

# -------- Analytics --------
from analytics.prediction_stats import calculate_accuracy
from analytics.run_stats import get_run_stats
from analytics.performance import get_paper_stats, get_career_stats
from analytics.equity_curve import generate_equity_curve
from analytics.risk_metrics import calculate_r_metrics, calculate_drawdown
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.expectancy import calculate_expectancy
from analytics.feature_importance import get_feature_importance
from analytics.ml_accuracy import ml_rolling_accuracy
from analytics.decision_analysis import analyze_decision_quality
from analytics.conviction_stats import update_expectancy
from analytics.feature_logger import FEATURE_FILE, FEATURE_HEADERS, ensure_feature_file
from analytics.prediction_stats import PRED_FILE, PRED_HEADERS
from simulation.sim_contract import select_sim_contract_with_reason

# -------- AI --------
from interface.ai_assistant import ask_ai
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    vol_col,
    delta_col,
    ml_col,
    result_col,
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)
from research.train_ai import train_direction_model, train_edge_model
from logs.recorder import start_recorder_background
from core.startup_sync import perform_startup_broker_sync
from simulation.sim_portfolio import SimPortfolio


# ==============================
# CONFIG
# ==============================



logging.basicConfig(
    filename="system.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s:%(message)s"
)

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

if DISCORD_TOKEN is None or DISCORD_TOKEN.strip() == "":
    raise RuntimeError("DISCORD_TOKEN is missing or empty.")

intents = discord.Intents.default()
intents.message_content = True

PAPER_CHANNEL_ID = 1470599150843203636
ALERT_CHANNEL_ID = 1470846800423551230
FORECAST_CHANNEL_ID = 1470931720739098774
HEART_CHANNEL_ID = 1470992071514132562
EOD_REPORT_CHANNEL_ID = 1476863964473196586

BOT_TIMEZONE = "America/New_York"

CONVICTION_HEADERS = [
    "time",
    "direction",
    "impulse",
    "follow",
    "price",
    "fwd_5m",
    "fwd_10m",
    "fwd_5m_price",
    "fwd_5m_time",
    "fwd_5m_status",
    "fwd_10m_price",
    "fwd_10m_time",
    "fwd_10m_status",
]
LEGACY_CONVICTION_HEADERS = [
    "time", "direction", "impulse", "follow", "price", "fwd_5m", "fwd_10m"
]
PREDICTION_REQUIRED_HEADERS = [
    "time", "timeframe", "direction", "confidence", "high", "low",
    "regime", "volatility", "session", "actual", "correct", "checked"
]
ACCOUNT_REQUIRED_KEYS = [
    "balance", "starting_balance", "open_trade", "trade_log", "wins",
    "losses", "day_trades", "risk_per_trade", "max_trade_size",
    "daily_loss", "max_daily_loss", "last_trade_day"
]


def _read_csv_headers(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return None


def run_startup_phase_gates():
    errors = []

    # 1) Timezone check
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        if tz.zone != BOT_TIMEZONE:
            errors.append(f"timezone_invalid:{tz.zone}")
    except Exception as e:
        errors.append(f"timezone_error:{e}")

    # 2) Env / config checks
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        errors.append("alpaca_api_keys_missing")

    # 3) CSV header checks
    conviction_file = os.path.join(DATA_DIR, "conviction_expectancy.csv")
    pred_file = os.path.join(DATA_DIR, "predictions.csv")

    conviction_headers = _read_csv_headers(conviction_file)
    if conviction_headers not in (CONVICTION_HEADERS, LEGACY_CONVICTION_HEADERS):
        errors.append("conviction_csv_header_invalid")

    pred_headers = _read_csv_headers(pred_file)
    if pred_headers is None:
        errors.append("predictions_csv_header_missing")
    else:
        missing_pred = [h for h in PREDICTION_REQUIRED_HEADERS if h not in pred_headers]
        if missing_pred:
            errors.append(f"predictions_csv_header_missing_fields:{','.join(missing_pred)}")
        if pred_headers != PRED_HEADERS:
            try:
                with open(pred_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(PRED_HEADERS)
            except Exception:
                errors.append("predictions_csv_header_reset_failed")

    # 4) ML model load checks
    direction_model_path = os.path.join(DATA_DIR, "direction_model.pkl")
    edge_model_path = os.path.join(DATA_DIR, "edge_model.pkl")
    for model_path, model_name in [
        (direction_model_path, "direction_model"),
        (edge_model_path, "edge_model"),
    ]:
        if not os.path.exists(model_path):
            continue
        try:
            joblib.load(model_path)
        except Exception as e:
            errors.append(f"{model_name}_load_error:{e}")

    # 5) Account structure check
    account_file = os.path.join(DATA_DIR, "account.json")
    if not os.path.exists(account_file):
        errors.append("account_missing")
    else:
        try:
            with open(account_file, "r") as f:
                acc = json.load(f)
            missing_keys = [k for k in ACCOUNT_REQUIRED_KEYS if k not in acc]
            if missing_keys:
                errors.append(f"account_missing_keys:{','.join(missing_keys)}")
        except Exception as e:
            errors.append(f"account_read_error:{e}")

    return errors

class QQQBot(commands.Bot):

    async def safe_task(self, coro_func, *args):
        while True:
            try:
                await coro_func(*args)
            except Exception as e:
                logging.error(f"[CRASH] Background task {coro_func.__name__} crashed: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def setup_hook(self):
        print("Launching background systems...")

        if not hasattr(self, "recorder_thread"):
            self.recorder_thread = start_recorder_background()

        self.trading_enabled = True
        self.startup_errors = run_startup_phase_gates()
        self.startup_notice_sent = False
        self.data_stale_state = False
        self.last_stale_warning_time = None
        self.data_integrity_state = False
        self.last_integrity_warning_time = None
        self.last_holiday_notice_date = None
        self.predictor_winrate_history = []
        self.predictor_baseline_winrate = None
        self.predictor_drift_state = False
        self.last_predictor_drift_warning_time = None
        self.recorder_stalled_state = False
        self.last_recorder_stall_warning_time = None
        self.block_reason_last_time = {}
        self.last_recorder_thread_dead_warning_time = None
        self.paper_channel_id = PAPER_CHANNEL_ID

        if self.startup_errors:
            self.trading_enabled = False
            for err in self.startup_errors:
                logging.error(f"startup_error: {err}")
        try:
            ensure_feature_file(reset_if_invalid=True)
        except Exception as e:
            logging.exception("feature_file_init_error: %s", e)

        await perform_startup_broker_sync(self)

        self.loop.create_task(
            self.safe_task(auto_trader, self, PAPER_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(conviction_watcher, self, ALERT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(forecast_watcher, self, FORECAST_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(heart_monitor, self, HEART_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(prediction_grader, self, PAPER_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(start_heartbeat)
        )
        self.loop.create_task(
            self.safe_task(opportunity_watcher, self, ALERT_CHANNEL_ID)
        )
        set_sim_bot(self)
        self.loop.create_task(
            self.safe_task(sim_entry_loop)
        )
        self.loop.create_task(
            self.safe_task(sim_exit_loop)
        )
        self.loop.create_task(
            self.safe_task(sim_eod_report_loop, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(preopen_check_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(eod_open_trade_report_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(option_chain_health_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        

    async def safe_task_with_timeout(self, task_coro, name, timeout=10):
        try:
            await asyncio.wait_for(task_coro, timeout=timeout)
        except asyncio.TimeoutError:
            logging.error(f"[TIMEOUT] Task {name} exceeded timeout.")
        except Exception as e:
            logging.exception(f"[CRASH] Background task {name} crashed: {e}")


bot = QQQBot(command_prefix="!", intents=intents, help_command=None)
# ==============================
# START
# ==============================

@bot.event
async def on_ready():
    print(f"Bot online as {bot.user}")

print("TOKEN LOADED: ", DISCORD_TOKEN is not None)


BOT_START_TIME = datetime.now(pytz.timezone("US/Eastern"))


def _infer_embed_style(title: str | None, description: str | None):
    text = f"{title or ''} {description or ''}".lower()
    error_terms = ("error", "failed", "invalid", "unknown", "missing")
    warn_terms = ("warning", "warn", "blocked", "disabled", "limit", "skip")
    success_terms = ("success", "complete", "updated", "reset", "ok", "done")
    if any(t in text for t in error_terms):
        return 0xE74C3C, "❌"
    if any(t in text for t in warn_terms):
        return 0xF39C12, "⚠️"
    if any(t in text for t in success_terms):
        return 0x2ECC71, "✅"
    return 0x3498DB, "ℹ️"


def _maybe_prefix_emoji(title: str | None, emoji: str) -> str | None:
    if not title:
        return title
    emoji_prefixes = ("✅", "❌", "⚠️", "ℹ️", "📘", "📋", "📈", "📊", "🧠", "🖥", "🤖", "📥", "📤", "🧪", "🧾")
    if title.startswith(emoji_prefixes):
        return title
    return f"{emoji} {title}"

def _add_field_icons(name: str) -> str:
    icon_map = {
        "Total Trades": "📦",
        "Open Trades": "📂",
        "Win Rate": "🎯",
        "Total PnL": "💰",
        "Avg Win": "📈",
        "Avg Loss": "📉",
        "Expectancy": "🧮",
        "Best Trade": "🥇",
        "Worst Trade": "🧯",
        "Max Drawdown": "📉",
        "Regime Breakdown": "🧭",
        "Time Bucket Breakdown": "🕒",
        "Exit Reasons": "🚪",
        "Last Trade": "🕘",
        "Risk / Balance": "🧰",
        "Details": "📋",
        "Context": "🧠",
        "ML": "🤖",
        "Reason": "⚠️",
        "Status": "✅",
        "Trader": "🧭",
        "Sims": "🧪",
        "Start Balance": "💵",
        "Strike": "🎯",
        "Premium": "💵",
        "Contracts": "📦",
        "Total Cost": "🧾",
        "Expiry": "📅",
        "Market State": "🧠",
        "Structure Context": "📐",
        "Final Grade": "🏆",
        "Market": "🟢",
        "System Health": "🧠",
        "System Diagnostics": "🧪",
        "Balance": "💰",
        "Trade Activity": "📒",
        "Background Systems": "⚙️",
        "Analytics Status": "📊",
        "Last Price": "📈",
        "Data Freshness": "⏱",
        "Option Snapshot": "🧩",
    }
    icon = icon_map.get(name)
    if icon and not name.startswith(icon):
        return f"{icon} {name}"
    return name

def _format_ts(value) -> str:
    eastern = pytz.timezone("America/New_York")
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return value
    else:
        return str(value)
    if dt.tzinfo is None:
        dt = eastern.localize(dt)
    else:
        dt = dt.astimezone(eastern)
    return dt.strftime("%Y-%m-%d %H:%M:%S ET")

def _format_pct_signed(val) -> str:
    try:
        num = float(val) * 100
        return f"{'+' if num >= 0 else '-'}{abs(num):.1f}%"
    except (TypeError, ValueError):
        return "N/A"

def _format_duration_short(seconds) -> str:
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return "N/A"
    if total < 0:
        return "N/A"
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"

def _load_sim_profiles() -> dict:
    sim_config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
    )
    try:
        with open(sim_config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def _collect_sim_metrics():
    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _parse_ts(val):
        try:
            if not val:
                return None
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    profiles = _load_sim_profiles()
    if not profiles:
        return [], {}

    metrics = []
    for sim_key, profile in profiles.items():
        try:
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                continue
            sim = SimPortfolio(sim_key, profile)
            sim.load()
            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            pnl_vals = []
            wins = 0
            win_loss_seq = []
            regime_stats = {}
            time_stats = {}
            dte_stats = {}
            setup_stats = {}
            exit_counts = {}
            hold_times = []
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    continue
                pnl_vals.append(pnl_val)
                if pnl_val > 0:
                    wins += 1
                    win_loss_seq.append(1)
                else:
                    win_loss_seq.append(0)

                regime_key = t.get("regime_at_entry") or "UNKNOWN"
                regime_stats.setdefault(regime_key, {"wins": 0, "total": 0})
                regime_stats[regime_key]["total"] += 1
                if pnl_val > 0:
                    regime_stats[regime_key]["wins"] += 1

                time_key = t.get("time_of_day_bucket") or "UNKNOWN"
                time_stats.setdefault(time_key, {"wins": 0, "total": 0})
                time_stats[time_key]["total"] += 1
                if pnl_val > 0:
                    time_stats[time_key]["wins"] += 1

                dte_key = t.get("dte_bucket") or "UNKNOWN"
                dte_stats.setdefault(
                    dte_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                dte_stats[dte_key]["total"] += 1
                dte_stats[dte_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    dte_stats[dte_key]["wins"] += 1
                    dte_stats[dte_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    dte_stats[dte_key]["pnl_neg"] += abs(pnl_val)

                setup_key = t.get("setup") or t.get("setup_type") or "UNKNOWN"
                setup_stats.setdefault(
                    setup_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                setup_stats[setup_key]["total"] += 1
                setup_stats[setup_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    setup_stats[setup_key]["wins"] += 1
                    setup_stats[setup_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    setup_stats[setup_key]["pnl_neg"] += abs(pnl_val)

                reason = t.get("exit_reason", "unknown") or "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1

                hold_sec = _safe_float(t.get("time_in_trade_seconds"))
                if hold_sec is not None:
                    hold_times.append(hold_sec)

            total_trades = len(trade_log)
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            win_rate = wins / total_trades if total_trades > 0 else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            max_win = max(pnl_vals) if pnl_vals else 0.0
            max_loss = min(pnl_vals) if pnl_vals else 0.0
            win_sum = sum(p for p in pnl_vals if p > 0)
            loss_sum = abs(sum(p for p in pnl_vals if p < 0))
            profit_factor = win_sum / loss_sum if loss_sum > 0 else None

            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            return_pct = (total_pnl / start_balance) if start_balance > 0 else 0.0
            peak_balance = _safe_float(getattr(sim, "peak_balance", None)) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0

            times = []
            for t in trade_log:
                ts = _parse_ts(t.get("exit_time")) or _parse_ts(t.get("entry_time"))
                if ts:
                    times.append(ts)
            if len(times) >= 2:
                days_active = max((max(times) - min(times)).total_seconds() / 86400.0, 1 / 24)
            else:
                days_active = 1.0
            equity_speed = total_pnl / days_active if days_active else None

            max_win_streak = 0
            max_loss_streak = 0
            cur_win = 0
            cur_loss = 0
            for outcome in win_loss_seq:
                if outcome == 1:
                    cur_win += 1
                    cur_loss = 0
                else:
                    cur_loss += 1
                    cur_win = 0
                max_win_streak = max(max_win_streak, cur_win)
                max_loss_streak = max(max_loss_streak, cur_loss)

            avg_hold = sum(hold_times) / len(hold_times) if hold_times else None
            pnl_stdev = None
            pnl_median = None
            try:
                if len(pnl_vals) >= 2:
                    import statistics
                    pnl_stdev = statistics.pstdev(pnl_vals)
                    pnl_median = statistics.median(pnl_vals)
            except Exception:
                pnl_stdev = None
                pnl_median = None

            metrics.append({
                "sim_id": sim_key,
                "name": profile.get("name", sim_key),
                "trades": total_trades,
                "wins": wins,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "return_pct": return_pct,
                "expectancy": expectancy,
                "max_win": max_win,
                "max_loss": max_loss,
                "profit_factor": profit_factor,
                "max_drawdown": max_drawdown,
                "equity_speed": equity_speed,
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "avg_hold": avg_hold,
                "pnl_stdev": pnl_stdev,
                "pnl_median": pnl_median,
                "regime_stats": regime_stats,
                "time_stats": time_stats,
                "dte_stats": dte_stats,
                "setup_stats": setup_stats,
                "exit_counts": exit_counts,
                "start_balance": start_balance,
                "balance": balance,
            })
        except Exception:
            continue

    return metrics, profiles

def _get_data_freshness_text() -> str | None:
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("America/New_York")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        now = datetime.now(eastern)
        age = (now - ts).total_seconds()
        if age < 0:
            age = 0
        def _fmt_age(seconds: float) -> str:
            total = int(seconds)
            hours = total // 3600
            minutes = (total % 3600) // 60
            secs = total % 60
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)
        source = ""
        try:
            source = df.attrs.get("source")
        except Exception:
            source = ""
        source_text = f" | src: {source}" if source else ""
        market_open = None
        try:
            market_open = df.attrs.get("market_open")
        except Exception:
            market_open = None
        status_text = "Market open" if market_open else "Market closed"
        return f"{status_text} | Data age: {_fmt_age(age)} (last candle {ts.strftime('%H:%M:%S')} ET){source_text}"
    except Exception:
        return None

def _get_status_line() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        return f"Status: {risk_mode} | Last trade: {last_trade}"
    except Exception:
        return None

def _get_status_banner() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        balance = acc.get("balance")
        bal_text = f"${float(balance):,.2f}" if isinstance(balance, (int, float)) else "N/A"
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        freshness = _get_data_freshness_text() or "Data age: N/A"
        return (
            f"🧭 Risk: {risk_mode}\n"
            f"💰 Balance: {bal_text}\n"
            f"🕘 Last Trade: {last_trade}\n"
            f"⏱ {freshness}"
        )
    except Exception:
        return None

def _add_trend_arrow(val, good_when_high: bool = True) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return ""
    if num == 0:
        return "➖"
    if good_when_high:
        return "⬆️" if num > 0 else "⬇️"
    return "⬇️" if num > 0 else "⬆️"

def _tag_trade_mode(trade: dict) -> str:
    try:
        mode = (trade or {}).get("mode")
        if isinstance(mode, str) and mode:
            return mode.upper()
        if (trade or {}).get("reconstructed"):
            return "RECON"
    except Exception:
        pass
    return "SIM"

def _append_footer(embed: "discord.Embed", extra: str | None = None) -> None:
    try:
        parts = []
        footer_text = embed.footer.text if embed.footer and embed.footer.text else ""
        if footer_text:
            parts.append(footer_text)
        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        if status_line and status_line not in footer_text:
            parts.append(status_line)
        if freshness_line and freshness_line not in footer_text:
            parts.append(freshness_line)
        if extra:
            parts.append(extra)
        if parts:
            footer_text = " | ".join(p for p in parts if p)
            if len(footer_text) > 2000:
                footer_text = footer_text[:2000]
            embed.set_footer(text=footer_text)
    except Exception:
        return


async def _send_embed(ctx, description: str, title: str | None = None, color: int | None = None):
    inferred_color, inferred_emoji = _infer_embed_style(title, description)
    final_color = color or inferred_color
    final_title = _maybe_prefix_emoji(title, inferred_emoji)
    final_description = description or ""
    banner = _get_status_banner()
    if banner is None:
        if "Data age:" not in final_description and "Status:" not in final_description and "🧭" not in final_description:
            status_line = _get_status_line()
            freshness_line = _get_data_freshness_text()
            context_lines = []
            if status_line:
                context_lines.append(status_line)
            if freshness_line:
                context_lines.append(freshness_line)
            if context_lines:
                appended = "\n".join(context_lines)
                if len(final_description) + len(appended) + 2 <= 3500:
                    final_description = f"{final_description}\n{appended}" if final_description else appended
    if final_title is None and description:
        if not description.startswith(("✅", "❌", "⚠️", "ℹ️")):
            final_description = f"{inferred_emoji} {description}"
    embed = discord.Embed(title=final_title, description=final_description, color=final_color)
    await ctx.send(embed=embed)

# ==============================
# COMMANDS
# ==============================

@bot.command()
async def spy(ctx):
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            await _send_embed(ctx, "Market data unavailable.")
            return

        last = df.iloc[-1]
        recent = df.tail(120)

        high_price = recent["high"].max()
        low_price = recent["low"].min()

        high_time = recent["high"].idxmax()
        low_time = recent["low"].idxmin()

        high_time_str = high_time.strftime('%H:%M') if hasattr(high_time, "strftime") else str(high_time)
        low_time_str = low_time.strftime('%H:%M') if hasattr(low_time, "strftime") else str(low_time)

        plt.figure(figsize=(8, 4))
        plt.plot(df.index[-120:], df["close"][-120:], label="Price")
        plt.plot(df.index[-120:], df["ema9"][-120:], label="EMA9")
        plt.plot(df.index[-120:], df["ema20"][-120:], label="EMA20")
        plt.plot(df.index[-120:], df["vwap"][-120:], label="VWAP")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("charts/chartqqq.png")
        plt.close()

        spy_embed = discord.Embed(title="📡 SPY Snapshot", color=0x3498DB)
        spy_embed.add_field(name="💰 Price",    value=ab(A(f"${last['close']:.2f}", "white", bold=True)), inline=True)
        spy_embed.add_field(name="📊 VWAP",     value=ab(A(f"${last['vwap']:.2f}", "cyan")), inline=True)
        spy_embed.add_field(name="📈 EMA9",     value=ab(A(f"${last['ema9']:.2f}", "yellow")), inline=True)
        spy_embed.add_field(name="📉 EMA20",    value=ab(A(f"${last['ema20']:.2f}", "yellow")), inline=True)
        spy_embed.add_field(name="⬆️ Session High", value=ab(f"{A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}"), inline=True)
        spy_embed.add_field(name="⬇️ Session Low",  value=ab(f"{A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}"), inline=True)
        _append_footer(spy_embed)
        await ctx.send(embed=spy_embed)
        await ctx.send(file=discord.File("charts/chartqqq.png"))
    except Exception as e:
        logging.exception(f"!spy failed: {e}")
        await _send_embed(ctx, "⚠️ Chart error — data still warming up.")

@bot.command()
async def risk(ctx):

    r = calculate_r_metrics()
    dd = calculate_drawdown()

    if not r:
        await _send_embed(
            ctx,
            "⚠️ Need at least 10 closed trades to calculate R metrics.\n"
            "Close more trades before evaluating performance."
        )
        return

    if not dd:
        await _send_embed(
            ctx,
            "⚠️ Drawdown metrics unavailable.\n"
            "Close at least 1 trade to calculate drawdown."
        )
        return

    risk_embed = discord.Embed(title="📊 Risk Metrics", color=0x3498DB)
    risk_embed.add_field(name="📊 Avg R",       value=ab(A(str(r['avg_R']), "yellow", bold=True)), inline=True)
    risk_embed.add_field(name="✅ Avg Win R",   value=ab(A(str(r['avg_win_R']), "green", bold=True)), inline=True)
    risk_embed.add_field(name="❌ Avg Loss R",  value=ab(A(str(r['avg_loss_R']), "red", bold=True)), inline=True)
    risk_embed.add_field(name="🏆 Max R",       value=ab(A(str(r['max_R']), "green")), inline=True)
    risk_embed.add_field(name="💀 Min R",       value=ab(A(str(r['min_R']), "red")), inline=True)
    risk_embed.add_field(name="📉 Max Drawdown", value=ab(drawdown_col(dd['max_drawdown_dollars'])), inline=True)
    _append_footer(risk_embed)
    await ctx.send(embed=risk_embed)

@bot.command()
async def md(ctx, action: str | None = None):
    try:
        cmd = (action or "status").strip().lower()
        if cmd not in {"enable", "disable", "status"}:
            await _send_embed(ctx, "Usage: `!md enable`, `!md disable`, or `!md status`")
            return

        if cmd == "enable":
            state = set_md_enabled(True)
            status_text = "ENABLED"
        elif cmd == "disable":
            state = set_md_enabled(False)
            status_text = "DISABLED"
        else:
            state = get_md_state()
            status_text = "ENABLED" if state.get("enabled") else "DISABLED"

        enabled = bool(state.get("enabled"))
        last_decay = state.get("last_decay")
        last_change = state.get("last_changed")

        embed = discord.Embed(
            title="🧭 Momentum Decay Strict Mode",
            color=0x2ECC71 if enabled else 0xE74C3C,
        )
        embed.add_field(
            name="Status",
            value=ab(A(status_text, "green" if enabled else "red", bold=True)),
            inline=False,
        )
        embed.add_field(
            name="Last Decay",
            value=ab(A(_format_ts(last_decay) if last_decay else "None", "cyan")),
            inline=True,
        )
        embed.add_field(
            name="Last Change",
            value=ab(A(_format_ts(last_change) if last_change else "None", "cyan")),
            inline=True,
        )

        if enabled and md_needs_warning(state):
            embed.add_field(
                name="⚠️ Warning",
                value=ab(A("MD strict is enabled but no recent decay detected. Consider disabling.", "yellow")),
                inline=False,
            )

        if not enabled:
            embed.add_field(
                name="How It Works",
                value=ab(A("When enabled, stop losses tighten during momentum decay.", "yellow")),
                inline=False,
            )

        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("md_error")
        await _send_embed(ctx, "md failed due to an internal error.")

@bot.event
async def on_error(event, *args, **kwargs):
    print(f"An error occurred: {event}")
    print(f"Args: {args}")
    print(f"Kwargs: {kwargs}")

@bot.command()
async def expectancy(ctx):

    stats = calculate_expectancy()

    if not stats:
        await _send_embed(ctx, "Need at least 10 closed trades to calculate expectancy.")
        return

    exp_embed = discord.Embed(title="📊 Rolling Expectancy", color=0x3498DB)
    exp_embed.add_field(name="📊 Avg R",      value=ab(A(str(stats['avg_R']), "yellow", bold=True)), inline=True)
    exp_embed.add_field(name="🎯 Win Rate",   value=ab(wr_col(stats['winrate'] / 100.0)), inline=True)
    exp_embed.add_field(name="💰 Expectancy", value=ab(pnl_col(stats['expectancy'])), inline=True)
    exp_embed.add_field(name="📦 Trades",     value=ab(A(str(stats['samples']), "white")), inline=True)
    _append_footer(exp_embed)
    await ctx.send(embed=exp_embed)

@bot.command()
async def retrain(ctx):

    feature_file = os.path.join(DATA_DIR, "trade_features.csv")

    if not os.path.exists(feature_file):
        await _send_embed(
            ctx,
            "No trade feature data found.\n"
            "You need at least 50 logged trades before retraining."
        )
        return

    await _send_embed(ctx, "🔄 Retraining models...")

    train_direction_model()
    train_edge_model()

    await _send_embed(ctx, "✅ Models retrained successfully.")

@bot.command()
async def plan(ctx, side=None, strike=None, premium=None, contracts=None, expiry=None):

    if not all([side, strike, premium, contracts, expiry]):
        await _send_embed(
            ctx,
            "Usage:\n"
            "!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>\n\n"
            "Example:\n"
            "!plan call 435 1.20 2 2026-02-14"
        )
        return

    if not isinstance(side, str):
        await _send_embed(ctx, "Side must be provided as text.")
        return

    side = side.lower()

    if side not in ["call", "put"]:
        await _send_embed(ctx, "Side must be 'call' or 'put'.")
        return

    if strike is None or premium is None or contracts is None:
        await _send_embed(ctx, "Strike, premium, and contracts are required.")
        return

    if isinstance(strike, str):
        strike_text = strike.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", strike_text):
            await _send_embed(ctx, "Strike must be numeric.")
            return
        strike = float(strike_text)
    elif isinstance(strike, (int, float)):
        strike = float(strike)
    else:
        await _send_embed(ctx, "Strike must be numeric.")
        return

    if isinstance(premium, str):
        premium_text = premium.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", premium_text):
            await _send_embed(ctx, "Premium must be numeric.")
            return
        premium = float(premium_text)
    elif isinstance(premium, (int, float)):
        premium = float(premium)
    else:
        await _send_embed(ctx, "Premium must be numeric.")
        return

    if isinstance(contracts, str):
        contracts_text = contracts.strip()
        if not re.fullmatch(r"[+-]?\d+", contracts_text):
            await _send_embed(ctx, "Contracts must be a whole number.")
            return
        contracts = int(contracts_text)
    elif isinstance(contracts, int):
        contracts = int(contracts)
    else:
        await _send_embed(ctx, "Contracts must be a whole number.")
        return
    try:
        df = get_market_dataframe()
        if df is None:
            await _send_embed(ctx, "Market data unavailable.")
            return

        regime = get_regime(df)
        vol = volatility_state(df)
        score, impulse, follow, direction = calculate_conviction(df)

        price = df.iloc[-1]["close"]

        # Determine bias alignment
        bias_alignment = "Aligned" if (
            (side == "call" and direction == "bullish") or
            (side == "put" and direction == "bearish")
        ) else "Against Bias"

        # ATR context
        atr = df.iloc[-1]["atr"]
        distance_from_strike = abs(price - strike)

        # Basic risk math
        total_cost = premium * contracts * 100

        # Basic grade logic
        grade_score = 0

        if bias_alignment == "Aligned":
            grade_score += 1
        if regime == "TREND":
            grade_score += 1
        if vol == "NORMAL" or vol == "HIGH":
            grade_score += 1
        if score >= 4:
            grade_score += 1

        if grade_score >= 4:
            grade = "A"
        elif grade_score == 3:
            grade = "B"
        elif grade_score == 2:
            grade = "C"
        else:
            grade = "D"

        embed = discord.Embed(
            title="📋 Trade Plan Analysis",
            color=discord.Color.green() if grade in ["A", "B"] else discord.Color.orange()
        )

        embed.add_field(name="Side", value=side.upper(), inline=True)
        embed.add_field(name=_add_field_icons("Strike"), value=strike, inline=True)
        embed.add_field(name=_add_field_icons("Premium"), value=premium, inline=True)

        embed.add_field(name=_add_field_icons("Contracts"), value=contracts, inline=True)
        embed.add_field(name=_add_field_icons("Total Cost"), value=f"${total_cost:.2f}", inline=True)
        embed.add_field(name=_add_field_icons("Expiry"), value=expiry, inline=True)

        embed.add_field(
            name=_add_field_icons("Market State"),
            value=(
                f"Regime: {regime}\n"
                f"Volatility: {vol}\n"
                f"Conviction Score: {score}\n"
                f"Impulse: {round(impulse,2)}\n"
                f"Follow Through: {round(follow*100,1)}%\n"
            ),
            inline=False
        )

        embed.add_field(
            name=_add_field_icons("Structure Context"),
            value=(
                f"Current Price: {price:.2f}\n"
                f"Distance from Strike: {distance_from_strike:.2f}\n"
                f"ATR: {atr:.2f}\n"
                f"Bias Alignment: {bias_alignment}"
            ),
            inline=False
        )

        embed.add_field(name=_add_field_icons("Final Grade"), value=f"🏆 {grade}", inline=False)
        _append_footer(embed)

        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception(f"!plan failed: {e}")
        await _send_embed(ctx, "⚠️ Plan error — data still warming up.")

@bot.command()
async def mlstats(ctx):

    acc = ml_rolling_accuracy()

    if acc is None:
        await _send_embed(
            ctx,
            "Not enough ML trade data.\n"
            "Need at least 30 ML-evaluated trades."
        )
    else:
        await _send_embed(ctx, f"🧠 ML Rolling Accuracy (Last 30 Trades): {acc}%")


@bot.command()
async def predict(ctx, minutes: str | None = None):

    if minutes is None:
        await _send_embed(
            ctx,
            "Usage: `!predict <minutes>`\n"
            "Allowed values: 30 or 60\n"
            "Example: `!predict 30`"
        )
        return

    if not isinstance(minutes, str):
        await _send_embed(
            ctx,
            "Minutes must be text input.\n"
            "Example: `!predict 60`"
        )
        return

    if not minutes.isdigit():
        await _send_embed(
            ctx,
            "Minutes must be a number.\n"
            "Example: `!predict 60`"
        )
        return

    timeframe_minutes = int(minutes)

    if timeframe_minutes not in [30, 60]:
        await _send_embed(
            ctx,
            "Invalid timeframe.\n"
            "Allowed values: 30 or 60."
        )
        return

    df = get_market_dataframe()

    if df is None:
        await _send_embed(ctx, "Market data unavailable.")
        return

    pred = make_prediction(timeframe_minutes, df)

    if not pred:
        await _send_embed(
            ctx,
            "Not enough graded predictions.\n"
            "Need at least 5 graded predictions."
        )
        return

    pred_color = 0x2ECC71 if pred['direction'] == "bullish" else 0xE74C3C if pred['direction'] == "bearish" else 0x95A5A6
    pred_embed = discord.Embed(title=f"📊 SPY {timeframe_minutes}m Forecast", color=pred_color)
    pred_embed.add_field(name="📍 Direction",   value=ab(dir_col(pred['direction'])), inline=True)
    pred_embed.add_field(name="💡 Confidence",  value=ab(conf_col(pred['confidence'])), inline=True)
    pred_embed.add_field(name="🎯 Predicted High", value=ab(A(str(pred['high']), "green", bold=True)), inline=True)
    pred_embed.add_field(name="🎯 Predicted Low",  value=ab(A(str(pred['low']), "red", bold=True)), inline=True)

    if timeframe_minutes == 30:
        last = df.iloc[-1]
        session_recent = get_rth_session_view(df)
        if session_recent is None or session_recent.empty:
            session_recent = df

        high_price = session_recent["high"].max()
        low_price = session_recent["low"].min()

        high_time = session_recent["high"].idxmax()
        low_time = session_recent["low"].idxmin()

        high_time_str = high_time.strftime("%H:%M") if hasattr(high_time, "strftime") else str(high_time)
        low_time_str = low_time.strftime("%H:%M") if hasattr(low_time, "strftime") else str(low_time)
        if isinstance(high_time_str, str) and "ET" not in high_time_str:
            high_time_str = f"{high_time_str} ET"
        if isinstance(low_time_str, str) and "ET" not in low_time_str:
            low_time_str = f"{low_time_str} ET"

        _pc = f"${last['close']:.2f}"
        _pv = f"${last['vwap']:.2f}"
        _pe9 = f"${last['ema9']:.2f}"
        _pe20 = f"${last['ema20']:.2f}"
        pred_embed.add_field(
            name="📍 Market Snapshot",
            value=ab(
                f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
            ),
            inline=False
        )
        pred_embed.add_field(
            name="📈 Session Range",
            value=ab(
                f"{lbl('High')} {A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}",
                f"{lbl('Low')}  {A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}",
            ),
            inline=False
        )

    _append_footer(pred_embed)
    await ctx.send(embed=pred_embed)

@bot.command(name="help")
async def help_command(ctx, command_name: str | int | None = None):

    command_levels = {
        "spy": "basic",
        "predict": "basic",
        "regime": "basic",
        "conviction": "basic",
        "opportunity": "basic",
        "plan": "basic",
        "trades": "basic",
        "conviction_fix": "advanced",
        "features_reset": "advanced",
        "pred_reset": "advanced",
        "analysis": "advanced",
        "attempts": "advanced",
        "run": "advanced",
        "paperstats": "advanced",
        "career": "advanced",
        "equity": "advanced",
        "risk": "advanced",
        "expectancy": "advanced",
        "regimes": "advanced",
        "accuracy": "advanced",
        "mlstats": "advanced",
        "retrain": "advanced",
        "importance": "advanced",
        "md": "advanced",
        "simstats": "advanced",
        "simcompare": "advanced",
        "simtrades": "advanced",
        "simopen": "advanced",
        "simreset": "advanced",
        "simleaderboard": "advanced",
        "simstreaks": "advanced",
        "simregimes": "advanced",
        "simtimeofday": "advanced",
        "simpf": "advanced",
        "simconsistency": "advanced",
        "simexits": "advanced",
        "simhold": "advanced",
        "simdte": "advanced",
        "simsetups": "advanced",
        "simhealth": "advanced",
        "preopen": "advanced",
        "lastskip": "advanced",
        "system": "advanced",
        "replay": "advanced",
        "helpplan": "advanced",
        "ask": "advanced",
    }

    def _send_help_page(page_num: int):
        pages = [
            {
                "title": "📘 Help — Page 1/3 (Market + Core)",
                "color": 0x3498DB,
                "fields": [
                    ("🟢 Market", "`!spy`, `!predict`, `!regime`, `!conviction`, `!opportunity`, `!plan`"),
                    ("🟦 Core Performance", "`!trades`, `!analysis`, `!attempts`, `!run`"),
                    ("🟣 Risk + Expectancy", "`!risk`, `!expectancy`, `!regimes`, `!accuracy`, `!md`"),
                ],
            },
            {
                "title": "📗 Help — Page 2/3 (ML + Sims)",
                "color": 0x2ECC71,
                "fields": [
                    ("🧠 ML", "`!mlstats`, `!retrain`, `!importance`"),
                    ("🧪 Sims", "`!simstats`, `!simcompare`, `!simtrades`, `!simopen`, `!simleaderboard`, `!simstreaks`, `!simregimes`, `!simtimeofday`, `!simdte`, `!simsetups`, `!simpf`, `!simconsistency`, `!simexits`, `!simhold`, `!simreset`, `!simhealth`"),
                    ("⏸ Skip Status", "`!lastskip`, `!preopen`"),
                ],
            },
            {
                "title": "📙 Help — Page 3/3 (System + AI)",
                "color": 0xF39C12,
                "fields": [
                    ("🖥 System", "`!system`, `!replay`, `!helpplan`"),
                    ("🤖 AI Coach", "`!ask`"),
                    ("🧰 Maintenance", "`!conviction_fix`, `!features_reset`, `!pred_reset`"),
                ],
            },
        ]
        page_index = max(1, min(page_num, len(pages))) - 1
        page = pages[page_index]
        embed = discord.Embed(title=page["title"], color=page["color"])
        embed.description = "Use `!help <command>` for detailed usage. Use `!help 1|2|3` for pages."
        for name, value in page["fields"]:
            embed.add_field(name=name, value=value, inline=False)
        _append_footer(embed, extra=f"Page {page_index + 1}/{len(pages)}")
        return embed

    async def _send_help_paginated(start_page: int):
        pages_count = 3
        page = max(1, min(start_page, pages_count))
        message = await ctx.send(embed=_send_help_page(page))
        if pages_count <= 1:
            return
        try:
            for emoji in ("◀️", "▶️", "⏹️"):
                await message.add_reaction(emoji)
        except Exception:
            return

        def _check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
            )

        while True:
            try:
                reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
            except asyncio.TimeoutError:
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break

            emoji = str(reaction.emoji)
            if emoji == "⏹️":
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break
            if emoji == "◀️":
                page = pages_count if page == 1 else page - 1
            elif emoji == "▶️":
                page = 1 if page == pages_count else page + 1

            try:
                await message.edit(embed=_send_help_page(page))
            except Exception:
                pass
            try:
                await message.remove_reaction(reaction.emoji, user)
            except Exception:
                pass

    if command_name is None:
        await _send_help_paginated(1)
        return
    if isinstance(command_name, int):
        await _send_help_paginated(command_name)
        return
    if isinstance(command_name, str):
        page_text = command_name.strip().lower()
        if page_text.startswith("page"):
            page_text = page_text.replace("page", "").strip()
        if page_text.isdigit():
            await _send_help_paginated(int(page_text))
            return

    if not isinstance(command_name, str):
        await _send_embed(ctx, "Command name must be text.")
        return

    command_name = command_name.lower()

    command_guides = {
        "plan": """
`!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>`

Analyzes a proposed options trade using:

• Market Regime
• Volatility State
• Conviction Score
• Structure Alignment
• ATR Context
• Bias Alignment

Example:
`!plan call 435 1.20 2 2026-02-14`

Outputs:
• Market Diagnostics
• Strike Context
• Exposure Size
• AI Grade (A–D)

This does NOT place a trade.
It evaluates the idea against your engine.
""",
        "predict": """
`!predict <minutes>`

Forecasts SPY direction.

Allowed values:
30 or 60

Examples:
`!predict 30`
`!predict 60`
""",

        "risk": """
`!risk`

Displays:
• Avg R
• Avg Win R
• Avg Loss R
• Max R
• Drawdown

Requires:
Minimum 10 closed trades.
""",

        "expectancy": """
`!expectancy`

Displays rolling expectancy (R-based).

Requires:
Minimum 10 closed trades.
""",
        "spy": """
`!spy`

Shows current SPY price snapshot:
• Price, VWAP, EMA9, EMA20
• Recent high/low with timestamps
• Sends a chart image
""",
        "regime": """
`!regime`

Displays current market regime label.
""",
        "conviction": """
`!conviction`

Displays:
• Conviction score
• Direction
• Impulse
• Follow-through
""",
        "conviction_fix": """
`!conviction_fix`

Forces a backfill of conviction expectancy:
• Fills fwd_5m / fwd_10m where possible
• Adds price/time metadata and status markers
""",
        "features_reset": """
`!features_reset`

Resets trade_features.csv to a clean header.
Use when the feature file is malformed or legacy.
""",
        "pred_reset": """
`!pred_reset`

Resets predictions.csv to a clean header.
Use when old/stale predictions are present.
""",
        "opportunity": """
`!opportunity`

Returns current opportunity zone if available.
""",
        "run": """
`!run`

Shows runtime stats:
• Trades
• Wins/Losses
• Balance
""",
        "paperstats": """
`!paperstats`

Shows paper account stats:
• Balance
• PnL
• Winrate
""",
        "career": """
`!career`

Shows career stats:
• Total trades
• Winrate
• Best balance
""",
        "equity": """
`!equity`

Sends equity curve chart (requires closed trades).
""",
        "accuracy": """
`!accuracy`

Shows prediction accuracy (requires graded predictions).
""",
        "analysis": """
`!analysis`

Decision analysis summary:
• Trades analyzed
• Corr Delta vs R
• Corr Blended vs R
• Execution no-record exits (if present)
""",
        "attempts": """
`!attempts`

Decision attempt summary (runtime):
• Attempts / Opened / Blocked
• Top block reason
• ML weight
• Avg blended vs threshold
""",
        "trades": """
`!trades <page>`

Shows paginated trade log (5 per page).
Example: `!trades 2`
""",
        "simstats": """
`!simstats` or `!simstats SIM03`

Shows sim performance stats:
• Total trades, win rate, total PnL
• Avg win/loss, expectancy, drawdown
• Best/worst trade
• Regime/time-of-day breakdowns
""",
        "simcompare": """
`!simcompare`

Side-by-side sim comparison table.
""",
        "simleaderboard": """
`!simleaderboard`

Ranks sims by key performance metrics:
• Best win rate
• Best total return / PnL
• Fastest equity growth
• Best expectancy
• Biggest winner
• High-risk / high-reward
""",
        "simstreaks": """
`!simstreaks`

Win/loss streak leaders across sims.
""",
        "simregimes": """
`!simregimes`

Best sim by regime (win rate).
""",
        "simtimeofday": """
`!simtimeofday`

Best sim by time-of-day bucket (win rate).
""",
        "simpf": """
`!simpf`

Profit factor leaderboard.
""",
        "simconsistency": """
`!simconsistency`

Most consistent sims (lowest PnL volatility).
""",
        "simexits": """
`!simexits`

Best exit reason hit rates.
""",
        "simhold": """
`!simhold`

Fastest/slowest average hold time.
""",
        "md": """
`!md status`
`!md enable`
`!md disable`

Toggles Momentum Decay strict mode:
• Enabled = tighter stops during decay
• Status shows last decay + warnings
""",
        "simdte": """
`!simdte`

Best sim by DTE bucket (win rate).
""",
        "simsetups": """
`!simsetups`

Best sim by setup type (win rate).
""",
        "preopen": """
`!preopen`

Runs a pre-open readiness check:
• Market open/closed status
• Data age + source
• Latest SPY close
• Option snapshot sanity (call/put + 3 OTM variants)
""",
        "simtrades": """
`!simtrades SIM03 [page]`

Shows paginated sim trade history.
""",
        "simopen": """
`!simopen` or `!simopen SIM03 [page]`

Shows open sim trades:
• Hold time
• SPY CALL/PUT expiry strike
• Entry cost + current PnL
""",
        "simreset": """
`!simreset SIM03`
`!simreset all`
`!simreset live`

Resets a sim to starting balance and clears trade history.
""",
        "lastskip": """
`!lastskip`

Shows the most recent skip reason
for trade attempts.
""",
        "regimes": """
`!regimes`

Regime expectancy stats (R-multiple).
""",
        "system": """
`!system`

Displays system health summary.
""",
        "replay": """
`!replay`

Sends recorded session chart and live chart if available.
""",
        "helpplan": """
`!helpplan`

Quick reference for `!plan` usage.
""",

        "mlstats": """
`!mlstats`

Displays rolling ML accuracy (last 30 trades).

Requires:
At least 30 ML-evaluated trades.
""",

        "retrain": """
`!retrain`

Retrains:
• Direction model
• Edge model

Requires:
Minimum 50 logged trades in feature file.
""",

        "importance": """
`!importance`

Displays feature importance from Edge ML model.

Model must be trained first.
""",

        "system": """
`!system`

Displays:
• Market status
• System health
• Balance
• Active background systems
""",

        "ask": """
`!ask <question>`

AI reviews your performance.

Example:
`!ask Did I overtrade?`
"""
    }

    if command_name in command_guides:
        await _send_embed(ctx, command_guides[command_name], title=f"!{command_name}")
    else:
        await _send_embed(
            ctx,
            "Unknown command.\n"
            "Type `!help` to view available commands.\n"
            "Type `!help <command>` for detailed usage."
        )


@bot.command(name="helpplan")
async def help_plan(ctx):
    await _send_embed(ctx, """
📋 **!plan Usage**

!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>

Example:
!plan call 435 1.20 2 2026-02-14

Analyzes:
• Market structure
• Volatility
• Statistical alignment
• AI model grade (A–D)
""", title="Help")



@bot.command(name="replay")
async def replay(ctx):

    try:
        chart_path = generate_chart()

        if chart_path:
            await _send_embed(ctx, "📊 Recorded Session:")
            await ctx.send(file=discord.File(chart_path))
        else:
            await _send_embed(ctx, "No recorded session data available.")

        live_path = generate_live_chart()

        if live_path and os.path.exists(live_path):
            await _send_embed(ctx, "📈 Live Market:")
            await ctx.send(file=discord.File(live_path))
        else:
            await _send_embed(ctx, "Market closed — no live data available.")

    except Exception as e:
        logging.exception(f"Replay error: {e}")
        await _send_embed(ctx, "Replay failed — check logs.")

@bot.command()
async def importance(ctx):

    data = get_feature_importance()

    if not data:
        await _send_embed(
            ctx,
            "Model not trained yet.\n"
            "Run `!retrain` after 50+ trades."
        )
        return

    message = "📊 Feature Importance (Edge Model)\n\n"

    for name, score in data:
        message += f"{name}: {round(score, 3)}\n"

    await _send_embed(ctx, message, title="Feature Importance")


@bot.command()
async def regime(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return
    await _send_embed(ctx, f"Market Regime: {get_regime(df)}", title="Regime")

@bot.command()
async def regimes(ctx):

    data = calculate_regime_expectancy()

    if not data:
        await _send_embed(ctx, "Need at least 10 closed trades to calculate meaningful expextancy metrics.")
        return

    message = "📊 Regime Expectancy (R-Multiple)\n\n"

    for regimes, stats in data.items():
        message += (
            f"{regimes}\n"
            f"Trades: {stats['trades']}\n"
            f"Avg R: {stats['avg_R']}\n"
            f"Winrate: {stats['winrate']}%\n\n"
        )

    await _send_embed(ctx, message, title="Regime Expectancy")


@bot.command()
async def conviction(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return

    score, impulse, follow, direction = calculate_conviction(df)

    await _send_embed(
        ctx,
        (
            f"Conviction Score: {score}\n"
            f"Direction: {direction}\n"
            f"Impulse: {impulse:.2f}\n"
            f"Follow: {follow*100:.0f}%"
        ),
        title="Conviction"
    )


@bot.command()
async def opportunity(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return

    result = evaluate_opportunity(df)

    if result:
        if not isinstance(result, (list, tuple)) or len(result) < 5:
            await _send_embed(ctx, "No opportunity right now.")
            return
        side = result[0]
        low = result[1]
        high = result[2]
        price = result[3]
        tp_low = tp_high = stop_loss = None
        if len(result) >= 8:
            tp_low = result[5]
            tp_high = result[6]
            stop_loss = result[7]
        lines = [
            f"{side} setup",
            f"Zone: {low:.2f}-{high:.2f}",
            f"Current: ${price:.2f}",
        ]
        if tp_low is not None and tp_high is not None:
            lines.append(f"Take-Profit: {tp_low:.2f}-{tp_high:.2f}")
        if stop_loss is not None:
            lines.append(f"Stop-Loss: {stop_loss:.2f}")
        await _send_embed(
            ctx,
            "\n".join(lines),
            title="Opportunity"
        )
    else:
        await _send_embed(ctx, "No opportunity right now.")


@bot.command()
async def run(ctx):
    s = get_run_stats()
    await _send_embed(
        ctx,
        (
            f"Trades: {s['trades']}\n"
            f"Wins: {s['wins']}\n"
            f"Losses: {s['losses']}\n"
            f"Balance: ${s['current']}"
        ),
        title="Run Stats"
    )


@bot.command()
async def paperstats(ctx):
    s = get_paper_stats()
    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    pnl_badge = "⚪"
    try:
        pnl_val = float(s["pnl"])
        if pnl_val > 0:
            pnl_badge = "✅"
        elif pnl_val < 0:
            pnl_badge = "❌"
    except (TypeError, ValueError, KeyError):
        pnl_badge = "⚪"
    lines = [
        f"💰 Balance: ${s['balance']:.2f}",
        f"📈 PnL: {pnl_badge} ${s['pnl']:.2f}",
        f"🎯 Winrate: {s['winrate']}%",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)
    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Paper Stats"
    )


@bot.command()
async def career(ctx):
    c = get_career_stats()
    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    win_badge = "⚪"
    try:
        winrate_val = float(c["winrate"])
        if winrate_val >= 50:
            win_badge = "✅"
        else:
            win_badge = "❌"
    except (TypeError, ValueError, KeyError):
        win_badge = "⚪"
    lines = [
        f"📦 Total Trades: {c['total_trades']}",
        f"🎯 Winrate: {win_badge} {c['winrate']}%",
        f"🏆 Best Balance: ${c['best_balance']:.2f}",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)
    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Career Stats"
    )


@bot.command()
async def equity(ctx):
    chart = generate_equity_curve()
    if chart:
        embed = discord.Embed(title="📈 Equity Curve", color=0x3498DB)
        _append_footer(embed)
        await ctx.send(embed=embed, file=discord.File(chart))
    else:
        await _send_embed(ctx, "Need at least 1 closed trade(s) to see equity.")



@bot.command()
async def accuracy(ctx):
    stats = calculate_accuracy()
    if stats:
        await _send_embed(ctx, str(stats), title="Accuracy")
    else:
        await _send_embed(ctx, "Need at least 5 graded predictions before accuracy can be calculated.")


@bot.command()
async def analysis(ctx):
    try:
        acc = load_account()
    except Exception:
        await _send_embed(ctx, "Could not load account data.")
        return

    results = analyze_decision_quality(acc.get("trade_log", []))
    total = results.get("total_trades_analyzed", 0)
    corr_delta = results.get("corr_threshold_delta_vs_R")
    corr_blended = results.get("corr_blended_vs_R")

    corr_delta_text = "N/A" if corr_delta is None else f"{corr_delta:.4f}"
    corr_blended_text = "N/A" if corr_blended is None else f"{corr_blended:.4f}"

    lines = [
        "📊 Decision Analysis Summary",
        f"📦 Trades Analyzed: {total}",
        f"📈 Corr Delta vs R: {corr_delta_text}",
        f"🧠 Corr Blended vs R: {corr_blended_text}",
    ]

    exec_stats = acc.get("execution_stats")
    no_record = exec_stats.get("no_record_exits") if isinstance(exec_stats, dict) else None
    if isinstance(no_record, dict) and no_record:
        no_record_lines = []
        for key in sorted(no_record.keys()):
            no_record_lines.append(f"{key}: {no_record.get(key)}")
        stats_text = "\n".join(no_record_lines)
        lines.append("Execution No-Record Exits:")
        lines.append(stats_text)

    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)

    await _send_embed(ctx, "\n".join(lines)[:1500], title="Analysis")


@bot.command()
async def attempts(ctx):
    snap = get_decision_buffer_snapshot()
    avg_delta = snap.get("avg_delta")
    avg_delta_text = "N/A" if avg_delta is None else f"{avg_delta:.3f}"
    ml_weight = snap.get("ml_weight")
    ml_weight_text = "N/A" if ml_weight is None else f"{ml_weight:.3f}"

    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    lines = [
        "📊 **Decision Attempts (Runtime)**",
        f"Attempts: {snap.get('attempts', 0)}",
        f"Opened: {snap.get('opened', 0)}",
        f"Blocked: {snap.get('blocked', 0)}",
        f"Top Block Reason: {snap.get('top_block_reason', 'N/A')}",
        f"ML Weight: {ml_weight_text}",
        f"Avg Blended vs Threshold (Last 20): {avg_delta_text}",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)

    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Attempts"
    )


@bot.command()
async def simstats(ctx, sim_id: str | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_signed_money(val):
        try:
            num = float(val)
            return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_drawdown(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "N/A"
        if num <= 0:
            return "$0.00"
        return f"-${abs(num):,.2f}"

    def _format_pct(val):
        try:
            return f"{float(val) * 100:.1f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _format_pct_signed(val):
        try:
            num = float(val) * 100
            return f"{'+' if num >= 0 else '-'}{abs(num):.1f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _pnl_badge(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    def _extract_reason(ctx):
        if not ctx or not isinstance(ctx, str):
            return None
        if "reason=" not in ctx:
            return None
        try:
            return ctx.rsplit("reason=", 1)[-1].split("|")[0].strip()
        except Exception:
            return None

    def _format_feature_snapshot(fs: dict | None) -> str | None:
        if not isinstance(fs, dict) or not fs:
            return None

        def _f(key, fmt="{:.3f}"):
            val = fs.get(key)
            if val is None:
                return None
            try:
                return fmt.format(float(val))
            except Exception:
                return str(val)

        parts = []
        orb_h = _f("orb_high", "{:.2f}")
        orb_l = _f("orb_low", "{:.2f}")
        if orb_h and orb_l:
            parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
        vol_z = _f("vol_z")
        if vol_z:
            parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
        atr_exp = _f("atr_expansion")
        if atr_exp:
            parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
        vwap_z = _f("vwap_z")
        if vwap_z:
            parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
        close_z = _f("close_z")
        if close_z:
            parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
        iv_rank = _f("iv_rank_proxy")
        if iv_rank:
            parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")
        if not parts:
            return None
        return "  |  ".join(parts)

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _compute_breakdown(trade_log, key, order=None):
        stats = {}
        for t in trade_log:
            bucket = t.get(key) if isinstance(t, dict) else None
            bucket = bucket if bucket not in (None, "") else "UNKNOWN"
            stats.setdefault(bucket, {"wins": 0, "total": 0})
            pnl_val = _safe_float(t.get("realized_pnl_dollars"))
            if pnl_val is None:
                stats[bucket]["total"] += 1
                continue
            stats[bucket]["total"] += 1
            if pnl_val > 0:
                stats[bucket]["wins"] += 1
        lines = []
        keys = order if order else sorted(stats.keys())
        for k in keys:
            if k not in stats:
                continue
            total = stats[k]["total"]
            wins = stats[k]["wins"]
            win_rate = wins / total if total > 0 else 0
            lines.append(f"{k}: {wins}/{total} ({win_rate * 100:.1f}%)")
        return "\n".join(lines) if lines else "N/A"

    def _ansi_breakdown(text: str):
        if not text or text == "N/A":
            return ab(A("N/A", "gray"))
        return ab(*[A(line, "cyan") for line in text.splitlines()])

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        if sim_id:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return

            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                await _send_embed(ctx, f"No data for {sim_key} yet.")
                return

            sim = SimPortfolio(sim_key, profile)
            sim.load()

            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            total_trades = len(trade_log)
            open_count = len(sim.open_trades) if isinstance(sim.open_trades, list) else 0

            wins = 0
            losses = 0
            pnl_vals = []
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    losses += 1
                    continue
                pnl_vals.append(pnl_val)
                if pnl_val > 0:
                    wins += 1
                else:
                    losses += 1

            win_rate = wins / total_trades if total_trades > 0 else 0
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            avg_win = sum([p for p in pnl_vals if p > 0]) / len([p for p in pnl_vals if p > 0]) if any(p > 0 for p in pnl_vals) else 0.0
            avg_loss = sum([p for p in pnl_vals if p < 0]) / len([p for p in pnl_vals if p < 0]) if any(p < 0 for p in pnl_vals) else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            best_trade = max(pnl_vals) if pnl_vals else 0.0
            worst_trade = min(pnl_vals) if pnl_vals else 0.0

            peak_balance = _safe_float(sim.peak_balance) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0

            regime_breakdown = _compute_breakdown(
                trade_log,
                "regime_at_entry",
                order=["TREND", "RANGE", "VOLATILE", "UNKNOWN"]
            )
            time_breakdown = _compute_breakdown(
                trade_log,
                "time_of_day_bucket",
                order=["MORNING", "MIDDAY", "AFTERNOON", "CLOSE", "UNKNOWN"]
            )
            exit_counts = {}
            for t in trade_log:
                reason = t.get("exit_reason", "unknown")
                reason = reason if reason not in (None, "") else "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1
            if total_trades > 0 and exit_counts:
                exit_lines = []
                for reason, count in sorted(exit_counts.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / total_trades) * 100
                    exit_lines.append(f"{reason}: {count} ({pct:.1f}%)")
                exit_breakdown = "\n".join(exit_lines)
            else:
                exit_breakdown = "No exits recorded"

            name = profile.get("name", sim_key)
            total_pnl_badge = _pnl_badge(total_pnl)
            if total_pnl > 0:
                embed_color = 0x2ECC71
            elif total_pnl < 0:
                embed_color = 0xE74C3C
            else:
                embed_color = 0x3498DB
            embed = discord.Embed(title=f"📊 {name} ({sim_key})", color=embed_color)

            last_trade_text = "N/A"
            if trade_log:
                last = trade_log[-1]
                last_exit_time = _format_ts(last.get("exit_time", "N/A"))
                last_reason = last.get("exit_reason", "unknown")
                last_pnl = _safe_float(last.get("realized_pnl_dollars"))
                last_hold = _format_duration(last.get("time_in_trade_seconds"))
                last_trade_text = ab(
                    f"{lbl('Exit')} {A(last_exit_time, 'white')}",
                    f"{lbl('PnL')}  {pnl_col(last_pnl) if last_pnl is not None else A('N/A', 'gray')}  {lbl('Reason')} {exit_reason_col(last_reason)}",
                    *([ f"{lbl('Hold')} {A(last_hold, 'cyan')}" ] if last_hold != "N/A" else []),
                )

            embed.add_field(name=_add_field_icons("Last Trade"),   value=last_trade_text, inline=False)
            embed.add_field(name=_add_field_icons("Total Trades"), value=ab(A(str(total_trades), "white", bold=True)), inline=True)
            embed.add_field(name=_add_field_icons("Open Trades"),  value=ab(A(str(open_count), "cyan")), inline=True)
            embed.add_field(name=_add_field_icons("Win Rate"),     value=ab(wr_col(win_rate)), inline=True)
            embed.add_field(name=_add_field_icons("Total PnL"),    value=ab(pnl_col(total_pnl)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Win"),      value=ab(pnl_col(avg_win)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Loss"),     value=ab(pnl_col(avg_loss)), inline=True)
            embed.add_field(name=_add_field_icons("Expectancy"),   value=ab(pnl_col(expectancy)), inline=True)
            embed.add_field(name=_add_field_icons("Best Trade"),   value=ab(pnl_col(best_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Worst Trade"),  value=ab(pnl_col(worst_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Max Drawdown"), value=ab(drawdown_col(max_drawdown)), inline=True)
            embed.add_field(name=_add_field_icons("Regime Breakdown"), value=_ansi_breakdown(regime_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Time Bucket Breakdown"), value=_ansi_breakdown(time_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Exit Reasons"), value=_ansi_breakdown(exit_breakdown), inline=False)
            # Gates summary (if configured)
            gates = []
            if profile.get("orb_minutes") is not None:
                gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
            if profile.get("vol_z_min") is not None:
                gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
            if profile.get("atr_expansion_min") is not None:
                gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
            if gates:
                embed.add_field(name=_add_field_icons("SIM Gates"), value=ab("  |  ".join(gates)), inline=False)
            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            footer = f"Balance: { _format_money(balance) } | Start: { _format_money(start_balance) }"
            freshness_line = _get_data_freshness_text()
            if freshness_line:
                footer = f"{footer} | {freshness_line}"
            embed.set_footer(text=footer)
            _append_footer(embed)
            await ctx.send(embed=embed)
            return

        # summary for all sims
        embed = discord.Embed(title="📊 Sim Overview — All Portfolios", color=0x3498DB)
        max_abs_pnl = 0.0
        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                pnl_vals = []
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is not None:
                        pnl_vals.append(pnl_val)
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                max_abs_pnl = max(max_abs_pnl, abs(total_pnl))
            except Exception:
                continue

        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    embed.add_field(name=sim_key, value="No data", inline=False)
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                total_trades = len(trade_log)
                wins = 0
                pnl_vals = []
                exit_counts = {}
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is None:
                        continue
                    pnl_vals.append(pnl_val)
                    if pnl_val > 0:
                        wins += 1
                    reason = t.get("exit_reason", "unknown")
                    reason = reason if reason not in (None, "") else "unknown"
                    exit_counts[reason] = exit_counts.get(reason, 0) + 1
                win_rate = wins / total_trades if total_trades > 0 else 0
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                balance = _safe_float(sim.balance) or 0.0
                top_exit = None
                if exit_counts:
                    top_exit = max(exit_counts.items(), key=lambda x: x[1])[0]
                top_exit_text = top_exit if top_exit is not None else "unknown"
                badge = _pnl_badge(total_pnl)
                bar = ""
                if max_abs_pnl > 0:
                    ratio = abs(total_pnl) / max_abs_pnl
                    bars = int(round(ratio * 10))
                    bars = max(0, min(bars, 10))
                    bar = "█" * bars + "░" * (10 - bars)
                else:
                    bar = "░" * 10
                gates = []
                if profile.get("orb_minutes") is not None:
                    gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
                if profile.get("vol_z_min") is not None:
                    gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
                if profile.get("atr_expansion_min") is not None:
                    gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None

                summary = ab(
                    f"{A(str(total_trades), 'white', bold=True)} trades  "
                    f"{lbl('WR')} {wr_col(win_rate)}  "
                    f"{lbl('PnL')} {pnl_col(total_pnl)}",
                    f"{lbl('Bal')} {balance_col(balance)}  "
                    f"{lbl('Exit')} {exit_reason_col(top_exit_text)}  "
                    f"{A(bar, 'cyan')}",
                )
                if gate_line:
                    summary = ab(
                        f"{A(str(total_trades), 'white', bold=True)} trades  "
                        f"{lbl('WR')} {wr_col(win_rate)}  "
                        f"{lbl('PnL')} {pnl_col(total_pnl)}",
                        f"{lbl('Bal')} {balance_col(balance)}  "
                        f"{lbl('Exit')} {exit_reason_col(top_exit_text)}  "
                        f"{A(bar, 'cyan')}",
                        gate_line,
                    )
                embed.add_field(name=sim_key, value=summary, inline=False)
            except Exception:
                embed.add_field(name=sim_key, value="No data", inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simstats_error")
        await _send_embed(ctx, "simstats failed due to an internal error.")


@bot.command()
async def simcompare(ctx):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_signed_money(val):
        try:
            num = float(val)
            return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        rows = []
        agg_pnl = 0.0
        agg_pnl_count = 0
        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    rows.append({
                        "sim_id": sim_key,
                        "no_data": True,
                        "balance_start": profile.get("balance_start", 0.0),
                    })
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                total_trades = len(trade_log)
                if total_trades == 0:
                    rows.append({
                        "sim_id": sim_key,
                        "no_data": True,
                        "balance_start": profile.get("balance_start", 0.0),
                    })
                    continue
                wins = 0
                pnl_vals = []
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is None:
                        continue
                    pnl_vals.append(pnl_val)
                    if pnl_val > 0:
                        wins += 1
                win_rate = wins / total_trades if total_trades > 0 else 0.0
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                balance = _safe_float(sim.balance) or 0.0
                peak_balance = _safe_float(sim.peak_balance) or balance
                max_dd = peak_balance - balance if peak_balance > balance else 0.0
                expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
                rows.append({
                    "sim_id": sim_key,
                    "no_data": False,
                    "total_trades": total_trades,
                    "win_rate": win_rate,
                    "total_pnl": total_pnl,
                    "balance": balance,
                    "max_dd": max_dd,
                    "expectancy": expectancy,
                })
                agg_pnl += total_pnl
                agg_pnl_count += 1
            except Exception:
                rows.append({
                    "sim_id": sim_key,
                    "no_data": True,
                    "balance_start": profile.get("balance_start", 0.0),
                })

        # ANSI colors: build the header in gray, then color each data row
        _hdr = "\u001b[30mSIM   | Trades | WR%     | PnL          | Balance      | MaxDD       | Expectancy\u001b[0m"
        lines = [_hdr]
        for row in rows:
            sim_key = row["sim_id"]
            if row.get("no_data"):
                total_trades = "--"
                win_rate = None
                total_pnl = None
                balance = _safe_float(row.get("balance_start")) or 0.0
                max_dd = None
                expectancy = None
            else:
                total_trades = row["total_trades"]
                win_rate = row["win_rate"]
                total_pnl = row["total_pnl"]
                balance = row["balance"]
                max_dd = row["max_dd"]
                expectancy = row["expectancy"]

            trades_raw = f"{total_trades}" if isinstance(total_trades, str) else f"{total_trades:d}"

            # Pre-pad then color WR
            if win_rate is None:
                wr_display = f"{'--':>7}"
            else:
                wr_raw = f"{win_rate * 100:.1f}%"
                wr_padded = f"{wr_raw:>7}"
                wr_clr = "green" if win_rate >= 0.55 else "yellow" if win_rate >= 0.45 else "red"
                wr_display = A(wr_padded, wr_clr, bold=True)

            # Pre-pad then color PnL
            if total_pnl is None:
                pnl_display = f"{'--':>12}"
            else:
                pnl_raw = _format_signed_money(total_pnl)
                pnl_padded = f"{pnl_raw:>12}"
                pnl_clr = "green" if total_pnl > 0 else "red" if total_pnl < 0 else "white"
                pnl_display = A(pnl_padded, pnl_clr, bold=True)

            bal_display = f"{_format_money(balance):>12}"

            # Pre-pad then color MaxDD
            if max_dd is None:
                dd_display = f"{'--':>11}"
            else:
                dd_raw = _format_signed_money(-abs(max_dd)) if max_dd > 0 else _format_signed_money(0)
                dd_padded = f"{dd_raw:>11}"
                dd_display = A(dd_padded, "red" if max_dd > 0 else "white")

            # Pre-pad then color Expectancy
            if expectancy is None:
                exp_display = f"{'--':>10}"
            else:
                exp_raw = _format_signed_money(expectancy)
                exp_padded = f"{exp_raw:>10}"
                exp_clr = "green" if expectancy > 0 else "red" if expectancy < 0 else "white"
                exp_display = A(exp_padded, exp_clr)

            lines.append(
                f"{A(f'{sim_key:<5}', 'cyan')}|"
                f"{trades_raw:>7} | "
                f"{wr_display} | "
                f"{pnl_display} | "
                f"{bal_display} | "
                f"{dd_display} | "
                f"{exp_display}"
            )
        table = "```ansi\n" + "\n".join(lines) + "\n\u001b[0m```"
        if agg_pnl_count > 0:
            if agg_pnl > 0:
                color = 0x2ECC71
            elif agg_pnl < 0:
                color = 0xE74C3C
            else:
                color = 0x3498DB
        else:
            color = 0x95A5A6
        freshness_line = _get_data_freshness_text()
        description = table
        if freshness_line:
            description = f"{table}\n{freshness_line}"
        await _send_embed(ctx, description, title="Sim Compare", color=color)
    except Exception:
        logging.exception("simcompare_error")
        await _send_embed(ctx, "simcompare failed due to an internal error.")


@bot.command()
async def simleaderboard(ctx):
    def _color_pct(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return A("N/A", "gray")
        color = "green" if num >= 0 else "red"
        return A(f"{num:+.1f}%", color)

    def _pnl_or_na(val):
        return pnl_col(val) if val is not None else A("N/A", "gray")

    def _pick_best(items, key, prefer_high=True, filter_fn=None):
        pool = [m for m in items if (filter_fn(m) if filter_fn else True)]
        if not pool:
            return None
        return max(pool, key=lambda x: x.get(key, 0)) if prefer_high else min(pool, key=lambda x: x.get(key, 0))

    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _fmt_summary(m, extra_line=None):
            ret_pct = (m.get("return_pct", 0.0) or 0.0) * 100
            return ab(
                f"{lbl('Sim')} {A(m['sim_id'], 'cyan', bold=True)}  {lbl('Trades')} {A(str(m['trades']), 'white', bold=True)}",
                f"{lbl('WR')} {wr_col(m['win_rate'])}  {lbl('PnL')} {pnl_col(m['total_pnl'])}  {lbl('Return')} {_color_pct(ret_pct)}",
                *([extra_line] if extra_line else []),
            )

        eligible = lambda m: m["trades"] >= 3
        best_wr = _pick_best(metrics, "win_rate", filter_fn=lambda m: eligible(m))
        best_pnl = _pick_best(metrics, "total_pnl", filter_fn=lambda m: eligible(m))
        fastest = _pick_best(metrics, "equity_speed", filter_fn=lambda m: eligible(m) and m["equity_speed"] is not None)
        best_exp = _pick_best(metrics, "expectancy", filter_fn=lambda m: eligible(m))
        biggest_win = _pick_best(metrics, "max_win", filter_fn=lambda m: eligible(m))
        risky = _pick_best(metrics, "max_drawdown", filter_fn=lambda m: eligible(m) and m["total_pnl"] > 0)
        most_active = _pick_best(metrics, "trades", filter_fn=lambda m: True)

        embed = discord.Embed(title="🏁 Sim Leaderboard — Best At Each Role", color=0x3498DB)

        if best_wr:
            embed.add_field(name="🏆 Best Win Rate", value=_fmt_summary(best_wr), inline=False)
        if best_pnl:
            embed.add_field(name="💰 Best Total PnL", value=_fmt_summary(best_pnl), inline=False)
        if fastest:
            speed_line = f"{lbl('Speed')} {_pnl_or_na(fastest.get('equity_speed'))} {A('/day', 'cyan')}"
            embed.add_field(name="⚡ Fastest Equity Growth", value=_fmt_summary(fastest, speed_line), inline=False)
        if best_exp:
            exp_line = f"{lbl('Expectancy')} {_pnl_or_na(best_exp.get('expectancy'))}"
            embed.add_field(name="📈 Best Expectancy", value=_fmt_summary(best_exp, exp_line), inline=False)
        if biggest_win:
            win_line = f"{lbl('Max Win')} {_pnl_or_na(biggest_win.get('max_win'))}  {lbl('Max Loss')} {_pnl_or_na(biggest_win.get('max_loss'))}"
            embed.add_field(name="💥 Biggest Winner", value=_fmt_summary(biggest_win, win_line), inline=False)
        if risky:
            risk_line = f"{lbl('Drawdown')} {drawdown_col(risky.get('max_drawdown'))}  {lbl('PnL')} {pnl_col(risky.get('total_pnl'))}"
            embed.add_field(name="⚠️ High-Risk / High-Reward", value=_fmt_summary(risky, risk_line), inline=False)
        if most_active:
            embed.add_field(name="🧮 Most Active", value=_fmt_summary(most_active), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simleaderboard_error")
        await _send_embed(ctx, "simleaderboard failed due to an internal error.")


@bot.command()
async def simstreaks(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        eligible = [m for m in metrics if m.get("trades", 0) >= 3]
        if not eligible:
            await _send_embed(ctx, "Not enough trades to rank streaks (need 3+).")
            return

        win_rank = sorted(eligible, key=lambda m: m.get("max_win_streak", 0), reverse=True)[:5]
        loss_rank = sorted(eligible, key=lambda m: m.get("max_loss_streak", 0), reverse=True)[:5]

        embed = discord.Embed(title="🔁 Sim Streaks", color=0x9B59B6)
        win_lines = []
        for m in win_rank:
            win_lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_win_streak', 0)), 'green', bold=True)} "
                f"{lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        loss_lines = []
        for m in loss_rank:
            loss_lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_loss_streak', 0)), 'red', bold=True)} "
                f"{lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        embed.add_field(name="✅ Longest Win Streaks", value=ab(*win_lines) if win_lines else ab(A("N/A", "gray")), inline=False)
        embed.add_field(name="❌ Longest Loss Streaks", value=ab(*loss_lines) if loss_lines else ab(A("N/A", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simstreaks_error")
        await _send_embed(ctx, "simstreaks failed due to an internal error.")


@bot.command()
async def simregimes(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        regimes = ["TREND", "RANGE", "VOLATILE", "UNKNOWN"]
        lines = []
        for reg in regimes:
            best = None
            best_wr = -1.0
            for m in metrics:
                stats = m.get("regime_stats", {}).get(reg)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                if wr > best_wr:
                    best_wr = wr
                    best = (m["sim_id"], stats["wins"], stats["total"])
            if best:
                sim_id, wins, total = best
                lines.append(f"{A(reg, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{wins}/{total}', 'white')} {wr_col(wins/total)}")

        embed = discord.Embed(title="🧭 Best by Regime (Win Rate)", color=0x1ABC9C)
        embed.add_field(name="Regime Leaders", value=ab(*lines) if lines else ab(A("No regime data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simregimes_error")
        await _send_embed(ctx, "simregimes failed due to an internal error.")


@bot.command()
async def simtimeofday(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        buckets = ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE", "UNKNOWN"]
        lines = []
        for bucket in buckets:
            best = None
            best_wr = -1.0
            for m in metrics:
                stats = m.get("time_stats", {}).get(bucket)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                if wr > best_wr:
                    best_wr = wr
                    best = (m["sim_id"], stats["wins"], stats["total"])
            if best:
                sim_id, wins, total = best
                lines.append(f"{A(bucket, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{wins}/{total}', 'white')} {wr_col(wins/total)}")

        embed = discord.Embed(title="🕒 Best by Time‑of‑Day (Win Rate)", color=0x2980B9)
        embed.add_field(name="Time‑of‑Day Leaders", value=ab(*lines) if lines else ab(A("No time‑bucket data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simtimeofday_error")
        await _send_embed(ctx, "simtimeofday failed due to an internal error.")


@bot.command()
async def simpf(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("profit_factor") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for profit factor (need 3+ trades).")
            return
        ranked = sorted(eligible, key=lambda m: m.get("profit_factor", 0), reverse=True)[:7]
        lines = []
        for m in ranked:
            pf = m.get("profit_factor")
            pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
            lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('PF')} {pf_text} {lbl('WR')} {wr_col(m.get('win_rate', 0))} {lbl('PnL')} {pnl_col(m.get('total_pnl'))}"
            )
        embed = discord.Embed(title="🧮 Profit Factor Leaders", color=0x16A085)
        embed.add_field(name="Top Profit Factors", value=ab(*lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simpf_error")
        await _send_embed(ctx, "simpf failed due to an internal error.")


@bot.command()
async def simconsistency(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("pnl_stdev") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for consistency (need 3+ trades).")
            return
        ranked = sorted(eligible, key=lambda m: m.get("pnl_stdev", 0))[:7]
        lines = []
        for m in ranked:
            sigma = m.get("pnl_stdev")
            med = m.get("pnl_median")
            lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('σ')} {pnl_col(sigma)} "
                f"{lbl('Median')} {pnl_col(med)} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        embed = discord.Embed(title="📏 Most Consistent Sims", color=0x8E44AD)
        embed.add_field(name="Lowest PnL Volatility", value=ab(*lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simconsistency_error")
        await _send_embed(ctx, "simconsistency failed due to an internal error.")


@bot.command()
async def simexits(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        reasons = ["profit_target", "trailing_stop", "stop_loss", "eod_daytrade_close", "hold_max_elapsed"]
        lines = []
        for reason in reasons:
            best = None
            best_rate = -1.0
            for m in metrics:
                total = m.get("trades", 0)
                if total < 3:
                    continue
                count = m.get("exit_counts", {}).get(reason, 0)
                rate = count / total if total > 0 else 0.0
                if rate > best_rate:
                    best_rate = rate
                    best = (m["sim_id"], count, total)
            if best:
                sim_id, count, total = best
                lines.append(f"{A(reason, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{count}/{total}', 'white')} {wr_col(count/total)}")

        embed = discord.Embed(title="🎯 Best Exit Hit Rates", color=0xF39C12)
        embed.add_field(name="Exit Reason Leaders", value=ab(*lines) if lines else ab(A("No exit data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simexits_error")
        await _send_embed(ctx, "simexits failed due to an internal error.")


@bot.command()
async def simhold(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("avg_hold") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for hold‑time stats (need 3+ trades).")
            return
        fastest = sorted(eligible, key=lambda m: m.get("avg_hold", 0))[:5]
        slowest = sorted(eligible, key=lambda m: m.get("avg_hold", 0), reverse=True)[:5]
        fast_lines = [
            f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            for m in fastest
        ]
        slow_lines = [
            f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            for m in slowest
        ]
        embed = discord.Embed(title="⏱ Sim Hold‑Time Leaders", color=0x2C3E50)
        embed.add_field(name="Fastest Average Holds", value=ab(*fast_lines), inline=False)
        embed.add_field(name="Slowest Average Holds", value=ab(*slow_lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simhold_error")
        await _send_embed(ctx, "simhold failed due to an internal error.")


@bot.command()
async def simdte(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data", "gray")), inline=False)
                return
            chunks = []
            current = []
            current_len = 0
            for line in lines:
                est = len(line) + 1
                if current and current_len + est > 900:
                    chunks.append(current)
                    current = [line]
                    current_len = len(line)
                else:
                    current.append(line)
                    current_len += est
            if current:
                chunks.append(current)
            for idx, chunk in enumerate(chunks):
                name = title if idx == 0 else f"{title} (cont.)"
                embed.add_field(name=name, value=ab(*chunk), inline=False)

        # Build a combined list of DTE buckets across sims
        bucket_totals = {}
        for m in metrics:
            for k, v in (m.get("dte_stats") or {}).items():
                bucket_totals[k] = bucket_totals.get(k, 0) + v.get("total", 0)

        # Sort by overall sample size, top 8 for readability
        buckets = sorted(bucket_totals.items(), key=lambda x: x[1], reverse=True)
        buckets = [b for b, _ in buckets][:8]
        if not buckets:
            await _send_embed(ctx, "No DTE bucket data yet.")
            return

        lines = []
        for bucket in buckets:
            candidates = []
            for m in metrics:
                stats = (m.get("dte_stats") or {}).get(bucket)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]
            bottom = sorted(candidates, key=lambda x: x[0])[:3]

            def _fmt_row(tag, sim_id, stats):
                wins = stats.get("wins", 0)
                total = stats.get("total", 0)
                pf_text = A("N/A", "gray")
                exp_text = A("N/A", "gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_text = pnl_col(exp)
                return (
                    f"{A(tag, 'cyan')} {A(sim_id, 'white', bold=True)} "
                    f"{A(f'{wins}/{total}', 'white')} {wr_col(wins/total)} "
                    f"{lbl('PF')} {pf_text} {lbl('Exp')} {exp_text}"
                )

            bucket_total = bucket_totals.get(bucket, 0)
            lines.append(A(f"DTE {bucket} | n={bucket_total}", "magenta", bold=True))
            for idx, (_, sim_id, stats) in enumerate(top, start=1):
                lines.append(_fmt_row(f"Top{idx}", sim_id, stats))
            for idx, (_, sim_id, stats) in enumerate(bottom, start=1):
                lines.append(_fmt_row(f"Bot{idx}", sim_id, stats))

        embed = discord.Embed(
            title="📆 Best by DTE Bucket (Win Rate)",
            description="Ranking by win rate (min 3 trades per sim+bucket). PF=profit factor, Exp=avg PnL per trade.",
            color=0x27AE60,
        )
        _add_lines_field(embed, "DTE Leaders (Top/Bottom 3)", lines)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simdte_error")
        await _send_embed(ctx, "simdte failed due to an internal error.")


@bot.command()
async def simsetups(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data", "gray")), inline=False)
                return
            chunks = []
            current = []
            current_len = 0
            for line in lines:
                est = len(line) + 1
                if current and current_len + est > 900:
                    chunks.append(current)
                    current = [line]
                    current_len = len(line)
                else:
                    current.append(line)
                    current_len += est
            if current:
                chunks.append(current)
            for idx, chunk in enumerate(chunks):
                name = title if idx == 0 else f"{title} (cont.)"
                embed.add_field(name=name, value=ab(*chunk), inline=False)

        setup_totals = {}
        for m in metrics:
            for k, v in (m.get("setup_stats") or {}).items():
                setup_totals[k] = setup_totals.get(k, 0) + v.get("total", 0)

        setups = sorted(setup_totals.items(), key=lambda x: x[1], reverse=True)
        setups = [s for s, _ in setups][:8]
        if not setups:
            await _send_embed(ctx, "No setup data yet.")
            return

        lines = []
        for setup in setups:
            candidates = []
            for m in metrics:
                stats = (m.get("setup_stats") or {}).get(setup)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]
            bottom = sorted(candidates, key=lambda x: x[0])[:3]

            def _fmt_row(tag, sim_id, stats):
                wins = stats.get("wins", 0)
                total = stats.get("total", 0)
                pf_text = A("N/A", "gray")
                exp_text = A("N/A", "gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_text = pnl_col(exp)
                return (
                    f"{A(tag, 'cyan')} {A(sim_id, 'white', bold=True)} "
                    f"{A(f'{wins}/{total}', 'white')} {wr_col(wins/total)} "
                    f"{lbl('PF')} {pf_text} {lbl('Exp')} {exp_text}"
                )

            setup_total = setup_totals.get(setup, 0)
            lines.append(A(f"Setup {setup} | n={setup_total}", "magenta", bold=True))
            for idx, (_, sim_id, stats) in enumerate(top, start=1):
                lines.append(_fmt_row(f"Top{idx}", sim_id, stats))
            for idx, (_, sim_id, stats) in enumerate(bottom, start=1):
                lines.append(_fmt_row(f"Bot{idx}", sim_id, stats))

        embed = discord.Embed(
            title="🧩 Best by Setup Type (Win Rate)",
            description="Ranking by win rate (min 3 trades per sim+setup). PF=profit factor, Exp=avg PnL per trade.",
            color=0xE67E22,
        )
        _add_lines_field(embed, "Setup Leaders (Top/Bottom 3)", lines)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simsetups_error")
        await _send_embed(ctx, "simsetups failed due to an internal error.")
@bot.command()
async def simtrades(ctx, sim_id: str | None = None, page: str | int = 1):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _pnl_badge(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    try:
        profiles = _load_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        # Aggregate mode if sim_id is missing or "all"
        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            all_trades = []
            for sim_key, profile in profiles.items():
                try:
                    sim_path = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                    )
                    if not os.path.exists(sim_path):
                        continue
                    sim = SimPortfolio(sim_key, profile)
                    sim.load()
                    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                    if not trade_log:
                        continue
                    start_balance = _safe_float(profile.get("balance_start")) or 0.0
                    running_balance = start_balance
                    for t in trade_log:
                        pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                        if pnl_val is None:
                            pnl_val = 0.0
                        running_balance += pnl_val
                        t_copy: dict[str, Any] = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                        t_copy["sim_id"] = sim_key
                        t_copy["balance_after"] = running_balance
                        all_trades.append(t_copy)
                except Exception:
                    continue

            if not all_trades:
                await _send_embed(ctx, "No trades recorded yet.")
                return

            def _parse_ts(val):
                if val is None:
                    return None
                if isinstance(val, datetime):
                    return val
                try:
                    return datetime.fromisoformat(str(val))
                except Exception:
                    return None

            def _sort_key(t):
                ts = _parse_ts(t.get("exit_time")) or _parse_ts(t.get("entry_time"))
                return ts or datetime.min

            all_trades = sorted(all_trades, key=_sort_key, reverse=True)
            trade_log = all_trades
            sim_key = "ALL"
            balance_after = {str(t.get("trade_id")): t.get("balance_after") for t in trade_log}
        else:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return

            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                await _send_embed(ctx, f"No data for {sim_key} yet.")
                return

            sim = SimPortfolio(sim_key, profile)
            sim.load()

            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            if not trade_log:
                await _send_embed(ctx, "No trades recorded yet.")
                return

            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            running_balance = start_balance
            balance_after = {}
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    pnl_val = 0.0
                running_balance += pnl_val
                trade_id_val = t.get("trade_id")
                if trade_id_val:
                    balance_after[str(trade_id_val)] = running_balance

        per_page = 5
        total = len(trade_log)
        total_pages = (total + per_page - 1) // per_page
        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simtrades {sim_key} 1` to `!simtrades {sim_key} {total_pages}`.")
            return

        newest_first = list(trade_log)

        def _build_simtrades_embed(page_num: int) -> "discord.Embed":
            page_num = max(1, min(page_num, total_pages))
            start = (page_num - 1) * per_page
            end = start + per_page
            page_trades = newest_first[start:end]

            page_pnl = 0.0
            page_pnl_count = 0
            for t in page_trades:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is not None:
                    page_pnl += pnl_val
                    page_pnl_count += 1
            if page_pnl_count > 0:
                if page_pnl > 0:
                    page_color = 0x2ECC71
                elif page_pnl < 0:
                    page_color = 0xE74C3C
                else:
                    page_color = 0x3498DB
            else:
                page_color = 0x3498DB

            embed = discord.Embed(title=f"🧾 Sim Trades — {sim_key} (Page {page_num}/{total_pages})", color=page_color)
            for idx, t in enumerate(page_trades, start=start + 1):
                trade_id = str(t.get("trade_id", "N/A"))
                direction = str(t.get("direction", "unknown")).upper()
                entry_price = _safe_float(t.get("entry_price"))
                exit_price = _safe_float(t.get("exit_price"))
                pnl = _safe_float(t.get("realized_pnl_dollars"))
                qty = t.get("qty")
                entry_time = _format_ts(t.get("entry_time", "N/A"))
                exit_time = _format_ts(t.get("exit_time", "N/A"))
                exit_reason = t.get("exit_reason", "unknown")
                hold_text = _format_duration(t.get("time_in_trade_seconds"))
                balance_text = _format_money(balance_after.get(trade_id))
                sim_label = t.get("sim_id") or sim_key
                prof = profile_map.get(sim_label)
                gates = []
                if isinstance(prof, dict):
                    if prof.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(prof.get('orb_minutes')), 'white')}")
                    if prof.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(prof.get('vol_z_min')), 'white')}")
                    if prof.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(prof.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None

                def _format_opt_price(val):
                    try:
                        return f"${float(val):.4f}"
                    except (TypeError, ValueError):
                        return "N/A"
                entry_text = _format_opt_price(entry_price) if entry_price is not None else "N/A"
                exit_text = _format_opt_price(exit_price) if exit_price is not None else "N/A"
                pnl_text = _format_money(pnl) if pnl is not None else "N/A"
                pnl_pct_text = "N/A"
                pnl_pct_val = _safe_float(t.get("realized_pnl_pct"))
                if pnl_pct_val is None and entry_price is not None and exit_price is not None and entry_price != 0:
                    try:
                        pnl_pct_val = (float(exit_price) - float(entry_price)) / float(entry_price)
                    except (TypeError, ValueError):
                        pnl_pct_val = None
                if pnl_pct_val is not None:
                    pnl_pct_text = _format_pct_signed(pnl_pct_val)

                badge = _pnl_badge(pnl)
                mode_tag = _tag_trade_mode(t)
                reason_text = _extract_reason(t.get("entry_context"))
                fs_text = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe"))
                mae = _safe_float(t.get("mae"))
                field_name = f"{badge} {sim_label} #{idx} {direction} | {trade_id[:8]}"
                pct_suffix = f" ({A(pnl_pct_text, 'cyan')})" if pnl_pct_text != "N/A" else ""
                lines = [
                    f"{lbl('Mode')} {A(mode_tag, 'magenta')}  "
                    f"{lbl('PnL')} {pnl_col(pnl) if pnl is not None else A('N/A', 'gray')}{pct_suffix}  "
                    f"{lbl('Qty')} {A(str(qty), 'white')}",
                    f"{lbl('Entry')} {A(entry_text, 'white')} @ {A(entry_time, 'gray')}",
                    f"{lbl('Exit')}  {A(exit_text, 'white')} @ {A(exit_time, 'gray')}  {lbl('Reason')} {exit_reason_col(exit_reason)}",
                    f"{lbl('Hold')} {A(hold_text, 'cyan')}  {lbl('Bal')} {balance_col(balance_after.get(trade_id))}",
                ]
                if gate_line:
                    lines.append(gate_line)
                if reason_text:
                    lines.append(f"{lbl('Signal reason')} {A(reason_text, 'yellow')}")
                if fs_text:
                    lines.append(f"{lbl('Feature')} {A(fs_text, 'white')}")
                if mfe is not None or mae is not None:
                    mfe_text = f"{mfe:.2%}" if mfe is not None else "N/A"
                    mae_text = f"{mae:.2%}" if mae is not None else "N/A"
                    lines.append(f"{lbl('MFE')} {A(mfe_text, 'green')}  {lbl('MAE')} {A(mae_text, 'red')}")
                field_value = ab(*lines)
                embed.add_field(name=field_name, value=field_value, inline=False)

            _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
            return embed

        async def _send_simtrades_paginated(start_page: int):
            page_num = max(1, min(start_page, total_pages))
            message = await ctx.send(embed=_build_simtrades_embed(page_num))
            if total_pages <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page_num = total_pages if page_num == 1 else page_num - 1
                elif emoji == "▶️":
                    page_num = 1 if page_num == total_pages else page_num + 1

                try:
                    await message.edit(embed=_build_simtrades_embed(page_num))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        await _send_simtrades_paginated(page_num)
    except Exception:
        logging.exception("simtrades_error")
        await _send_embed(ctx, "simtrades failed due to an internal error.")


@bot.command()
async def simopen(ctx, sim_id: str | None = None, page: str | int = 1):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _parse_strike_from_symbol(symbol):
        if not symbol or not isinstance(symbol, str):
            return None
        try:
            strike_part = symbol[-8:]
            return int(strike_part) / 1000.0
        except Exception:
            return None

    def _contract_label(symbol, direction, expiry, strike):
        cp = None
        if isinstance(direction, str):
            d = direction.lower()
            if d == "bullish":
                cp = "CALL"
            elif d == "bearish":
                cp = "PUT"
        if cp is None and isinstance(symbol, str) and len(symbol) >= 10:
            try:
                cp_char = symbol[9]
                if cp_char == "C":
                    cp = "CALL"
                elif cp_char == "P":
                    cp = "PUT"
            except Exception:
                cp = None
        expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
        label = "SPY"
        if cp:
            label = f"{label} {cp}"
        if expiry_text:
            label = f"{label} {expiry_text}"
        if strike is None:
            strike = _parse_strike_from_symbol(symbol)
        if isinstance(strike, (int, float)):
            label = f"{label} {strike:g}"
        return label

    def _extract_reason(ctx):
        if not ctx or not isinstance(ctx, str):
            return None
        if "reason=" not in ctx:
            return None
        try:
            return ctx.rsplit("reason=", 1)[-1].split("|")[0].strip()
        except Exception:
            return None

    def _format_feature_snapshot(fs: dict | None) -> str | None:
        if not isinstance(fs, dict) or not fs:
            return None

        def _f(key, fmt="{:.3f}"):
            val = fs.get(key)
            if val is None:
                return None
            try:
                return fmt.format(float(val))
            except Exception:
                return str(val)

        parts = []
        orb_h = _f("orb_high", "{:.2f}")
        orb_l = _f("orb_low", "{:.2f}")
        if orb_h and orb_l:
            parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
        vol_z = _f("vol_z")
        if vol_z:
            parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
        atr_exp = _f("atr_expansion")
        if atr_exp:
            parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
        vwap_z = _f("vwap_z")
        if vwap_z:
            parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
        close_z = _f("close_z")
        if close_z:
            parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
        iv_rank = _f("iv_rank_proxy")
        if iv_rank:
            parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")
        if not parts:
            return None
        return "  |  ".join(parts)

    try:
        profiles = _load_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        trades = []

        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            sim_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        else:
            sim_key = sim_id.strip().upper()
            if sim_key not in profiles:
                await _send_embed(ctx, "Unknown sim ID.")
                return
            sim_keys = [sim_key]

        for sim_key in sim_keys:
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    continue
                sim = SimPortfolio(sim_key, profiles.get(sim_key, {}))
                sim.load()
                open_trades = sim.open_trades if isinstance(sim.open_trades, list) else []
                for t in open_trades:
                    t_copy = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                    t_copy["sim_id"] = sim_key
                    trades.append(t_copy)
            except Exception:
                continue

        if not trades:
            await _send_embed(ctx, "No open sim trades.")
            return

        def _parse_ts(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            try:
                return datetime.fromisoformat(str(val))
            except Exception:
                return None

        def _sort_key(t):
            ts = _parse_ts(t.get("entry_time"))
            return ts or datetime.min

        trades = sorted(trades, key=_sort_key, reverse=True)

        per_page = 5
        total = len(trades)
        total_pages = (total + per_page - 1) // per_page
        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simopen {sim_id or 'all'} 1` to `!simopen {sim_id or 'all'} {total_pages}`.")
            return

        async def _build_embed(page_num: int) -> "discord.Embed":
            page_num = max(1, min(page_num, total_pages))
            start = (page_num - 1) * per_page
            end = start + per_page
            page_trades = trades[start:end]
            embed = discord.Embed(title=f"📌 Open Sim Trades (Page {page_num}/{total_pages})", color=0x3498DB)
            now_et = datetime.now(pytz.timezone("US/Eastern"))
            for idx, t in enumerate(page_trades, start=start + 1):
                trade_id = str(t.get("trade_id", "N/A"))
                sim_label = t.get("sim_id") or "SIM"
                prof = profile_map.get(sim_label)
                gates = []
                if isinstance(prof, dict):
                    if prof.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(prof.get('orb_minutes')), 'white')}")
                    if prof.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(prof.get('vol_z_min')), 'white')}")
                    if prof.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(prof.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                direction = str(t.get("direction") or t.get("type") or "unknown").upper()
                option_symbol = t.get("option_symbol")
                expiry = t.get("expiry")
                strike = _safe_float(t.get("strike"))
                qty = t.get("qty") or t.get("quantity")
                entry_price = _safe_float(t.get("entry_price"))
                entry_notional = _safe_float(t.get("entry_notional"))
                if entry_notional is None and entry_price is not None and qty is not None:
                    try:
                        entry_notional = float(entry_price) * float(qty) * 100
                    except Exception:
                        entry_notional = None
                entry_time = _format_ts(t.get("entry_time", "N/A"))
                hold_secs = None
                try:
                    dt = _parse_ts(t.get("entry_time"))
                    if dt is not None:
                        eastern = pytz.timezone("America/New_York")
                        if dt.tzinfo is None:
                            dt = eastern.localize(dt)
                        else:
                            dt = dt.astimezone(eastern)
                        hold_secs = (now_et - dt).total_seconds()
                except Exception:
                    hold_secs = None

                current_price = None
                if option_symbol:
                    try:
                        current_price = await asyncio.to_thread(get_option_price, option_symbol)
                    except Exception:
                        current_price = None
                pnl_val = None
                if current_price is not None and entry_price is not None and qty is not None:
                    try:
                        pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                    except Exception:
                        pnl_val = None

                entry_text = f"${entry_price:.4f}" if entry_price is not None else "N/A"
                now_text = f"${float(current_price):.4f}" if current_price is not None else "N/A"
                cost_text = _format_money(entry_notional) if entry_notional is not None else "N/A"
                hold_text = _format_duration(hold_secs)
                contract_label = _contract_label(option_symbol, direction, expiry, strike)
                reason_text = _extract_reason(t.get("entry_context"))
                fs_text = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe_pct"))
                mae = _safe_float(t.get("mae_pct"))

                field_name = f"🟡 {sim_label} #{idx} {direction} | {trade_id[:8]}"
                lines = [
                    f"{lbl('Contract')} {A(contract_label, 'magenta', bold=True)}",
                    f"{lbl('Qty')} {A(str(qty), 'white')}  {lbl('Entry')} {A(entry_text, 'white')}  {lbl('Cost')} {A(cost_text, 'white')}",
                    f"{lbl('Now')} {A(now_text, 'white')}  {lbl('PnL')} {pnl_col(pnl_val) if pnl_val is not None else A('N/A', 'gray')}",
                    f"{lbl('Hold')} {A(hold_text, 'cyan')}  {lbl('Entry Time')} {A(entry_time, 'gray')}",
                ]
                if gate_line:
                    lines.append(gate_line)
                if reason_text:
                    lines.append(f"{lbl('Signal reason')} {A(reason_text, 'yellow')}")
                if fs_text:
                    lines.append(f"{lbl('Feature')} {A(fs_text, 'white')}")
                if mfe is not None or mae is not None:
                    mfe_text = f"{mfe:.2%}" if mfe is not None else "N/A"
                    mae_text = f"{mae:.2%}" if mae is not None else "N/A"
                    lines.append(f"{lbl('MFE')} {A(mfe_text, 'green')}  {lbl('MAE')} {A(mae_text, 'red')}")
                field_value = ab(*lines)
                embed.add_field(name=field_name, value=field_value, inline=False)
            _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
            return embed

        async def _send_paginated(start_page: int):
            page_num = max(1, min(start_page, total_pages))
            message = await ctx.send(embed=await _build_embed(page_num))
            if total_pages <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page_num = total_pages if page_num == 1 else page_num - 1
                elif emoji == "▶️":
                    page_num = 1 if page_num == total_pages else page_num + 1

                try:
                    await message.edit(embed=await _build_embed(page_num))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        await _send_paginated(page_num)
    except Exception:
        logging.exception("simopen_error")
        await _send_embed(ctx, "simopen failed due to an internal error.")


@bot.command()
async def simreset(ctx, sim_id: str | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    if sim_id is None:
        await _send_embed(ctx, "Usage: `!simreset SIM03`, `!simreset all`, or `!simreset live`")
        return

    try:
        profiles = _load_profiles()
        sim_key = sim_id.strip().upper()

        def _reset_one(_sim_key: str, _profile: dict) -> tuple[bool, str]:
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{_sim_key}.json")
                )
                if os.path.exists(sim_path):
                    try:
                        os.remove(sim_path)
                    except Exception:
                        logging.exception("simreset_remove_failed")
                sim = SimPortfolio(_sim_key, _profile)
                sim.load()
                sim.save()
                return True, "reset"
            except Exception:
                logging.exception("simreset_one_failed")
                return False, "error"

        target_keys = []
        if sim_key == "ALL":
            target_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        elif sim_key == "LIVE":
            for k, p in profiles.items():
                try:
                    if p.get("execution_mode") == "live":
                        target_keys.append(k)
                except Exception:
                    continue
            target_keys = sorted(target_keys)
        else:
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return
            target_keys = [sim_key]

        if not target_keys:
            await _send_embed(ctx, "No sims matched your reset request.")
            return

        results = []
        for key in target_keys:
            profile = profiles.get(key, {})
            ok, status = _reset_one(key, profile)
            results.append((key, ok, status))

        title = f"✅ Sim Reset — {sim_key}" if sim_key in {"ALL", "LIVE"} else f"✅ Sim Reset — {target_keys[0]}"
        embed = discord.Embed(title=title, color=0x2ECC71)
        ok_keys = [k for k, ok, _ in results if ok]
        fail_keys = [k for k, ok, _ in results if not ok]
        for key, ok, status in results:
            start_balance = profiles.get(key, {}).get("balance_start", 0.0)
            status_text = "Reset to starting balance." if ok else "Reset failed."
            embed.add_field(
                name=_add_field_icons(key),
                value=f"{status_text} Start: ${float(start_balance):,.2f}",
                inline=False
            )
        if len(results) > 1:
            summary_parts = []
            if ok_keys:
                summary_parts.append(f"Reset: {', '.join(ok_keys)}")
            if fail_keys:
                summary_parts.append(f"Failed: {', '.join(fail_keys)}")
            if summary_parts:
                embed.add_field(name=_add_field_icons("Summary"), value=" | ".join(summary_parts), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simreset_error")
        await _send_embed(ctx, "simreset failed due to an internal error.")


@bot.command(name="conviction_fix")
async def conviction_fix(ctx):
    try:
        df = get_market_dataframe()
        update_expectancy(df)
        await _send_embed(
            ctx,
            "Conviction expectancy backfill complete.\n"
            "fwd_5m / fwd_10m now updated where possible with price/time metadata.",
            title="Conviction Fix",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("conviction_fix_error: %s", e)
        await _send_embed(
            ctx,
            f"Conviction fix failed: {e}",
            title="Conviction Fix",
            color=0xe74c3c,
        )


@bot.command(name="features_reset")
async def features_reset(ctx):
    try:
        with open(FEATURE_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(FEATURE_HEADERS)
        await _send_embed(
            ctx,
            "trade_features.csv reset to clean header.",
            title="Features Reset",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("features_reset_error: %s", e)
        await _send_embed(
            ctx,
            f"Features reset failed: {e}",
            title="Features Reset",
            color=0xe74c3c,
        )


@bot.command(name="pred_reset")
async def pred_reset(ctx):
    try:
        with open(PRED_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(PRED_HEADERS)
        await _send_embed(
            ctx,
            "predictions.csv reset to clean header.",
            title="Predictions Reset",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("pred_reset_error: %s", e)
        await _send_embed(
            ctx,
            f"Predictions reset failed: {e}",
            title="Predictions Reset",
            color=0xe74c3c,
        )


@bot.command()
async def simhealth(ctx, page: str | int | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    required_keys = [
        "signal_mode",
        "dte_min",
        "dte_max",
        "balance_start",
        "risk_per_trade_pct",
        "daily_loss_limit_pct",
        "max_open_trades",
        "exposure_cap_pct",
        "max_spread_pct",
        "cutoff_time_et",
    ]

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        sim_items = list(profiles.items())
        sim_items = [item for item in sim_items if item[0].startswith("SIM")]

        # Run validator for a quick config health summary
        validator_summary = None
        validator_details = []
        try:
            from simulation.sim_validator import collect_sim_validation
            errors, total_errors = collect_sim_validation()
            if total_errors == 0:
                validator_summary = "OK"
            else:
                validator_summary = f"FAIL ({total_errors} issues)"
                validator_details = errors[:3]
        except Exception:
            validator_summary = "FAIL (validator error)"

        def _build_simhealth_embed(page_num: int) -> "discord.Embed":
            page_total = max(1, (len(sim_items) + 2) // 3)
            page_index = max(1, min(page_num, page_total)) - 1
            start = page_index * 3
            end = start + 3
            embed = discord.Embed(
                title=f"🧪 Sim Health Check — Page {page_index + 1}/{page_total}",
                color=0x3498DB
            )
            if validator_summary:
                lines = [A(validator_summary, "green" if validator_summary.startswith("OK") else "red", bold=True)]
                for line in validator_details:
                    severity = "yellow"
                    if any(token in line for token in ("missing:", "cutoff_format_invalid", "orb_requires_features")):
                        severity = "red"
                    lines.append(A(line, severity))
                embed.add_field(
                    name="SIM Validator",
                    value=ab(*lines),
                    inline=False
                )
            embed.add_field(
                name="SIM Profiles Loaded",
                value=ab(A(str(len(sim_items)), "white", bold=True)),
                inline=True
            )
            for sim_id, profile in sim_items[start:end]:
                try:
                    sim_path = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_id}.json")
                    )
                    file_exists = os.path.exists(sim_path)
                    file_status = "✅" if file_exists else "❌"
                    missing_keys = [k for k in required_keys if k not in profile]
                    missing_text = ", ".join(missing_keys) if missing_keys else "None"

                    if not file_exists:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'red' if not file_exists else 'green', bold=True)}",
                            f"{A('Not initialized', 'yellow')}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        )
                        embed.add_field(name=sim_id, value=value, inline=False)
                        continue

                    try:
                        with open(sim_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                    except Exception:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                            f"{lbl('Schema')} {A('⚠️', 'yellow', bold=True)}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        )
                        embed.add_field(name=sim_id, value=value, inline=False)
                        continue

                    schema_ok = "✅" if data.get("schema_version") is not None else "⚠️"
                    open_trades = data.get("open_trades")
                    trade_log = data.get("trade_log")
                    open_count = len(open_trades) if isinstance(open_trades, list) else 0
                    trade_count = len(trade_log) if isinstance(trade_log, list) else 0
                    balance_val = _safe_float(data.get("balance"))
                    peak_balance = _safe_float(data.get("peak_balance"))
                    daily_loss_val = _safe_float(data.get("daily_loss"))
                    # Compute daily PnL from trade log for accuracy
                    daily_pnl = None
                    try:
                        today = datetime.now(pytz.timezone("US/Eastern")).date()
                        total = 0.0
                        start_balance = _safe_float(profile.get("balance_start")) or 0.0
                        running_balance = start_balance
                        computed_peak = start_balance
                        for t in (trade_log if isinstance(trade_log, list) else []):
                            pnl_val = _safe_float(t.get("realized_pnl_dollars")) or 0.0
                            running_balance += pnl_val
                            if running_balance > computed_peak:
                                computed_peak = running_balance
                            exit_time = t.get("exit_time")
                            if not exit_time:
                                continue
                            dt = datetime.fromisoformat(str(exit_time))
                            if dt.tzinfo is None:
                                dt = pytz.timezone("US/Eastern").localize(dt)
                            if dt.date() == today:
                                total += pnl_val
                        daily_pnl = total
                        if peak_balance is None:
                            peak_balance = computed_peak
                        else:
                            peak_balance = max(peak_balance, computed_peak)
                    except Exception:
                        daily_pnl = None

                    last_reason = "N/A"
                    try:
                        ctx = None
                        if isinstance(trade_log, list):
                            for t in reversed(trade_log):
                                if isinstance(t, dict) and t.get("entry_context"):
                                    ctx = t.get("entry_context")
                                    break
                        if ctx is None and isinstance(open_trades, list):
                            for t in reversed(open_trades):
                                if isinstance(t, dict) and t.get("entry_context"):
                                    ctx = t.get("entry_context")
                                    break
                        if ctx and "reason=" in ctx:
                            last_reason = ctx.split("reason=")[-1].split("|")[0].strip()
                    except Exception:
                        last_reason = "N/A"

                    gates = []
                    if profile.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
                    if profile.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
                    if profile.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
                    gate_text = "  |  ".join(gates) if gates else None

                    value = ab(
                        f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                        f"{lbl('Schema')} {A(schema_ok, 'green' if schema_ok == '✅' else 'yellow', bold=True)}",
                        f"{lbl('Open trades')} {A(str(open_count), 'white')}",
                        f"{lbl('Trade log')} {A(str(trade_count), 'white')}",
                        f"{lbl('Balance')} {balance_col(balance_val)}",
                        f"{lbl('Peak balance')} {balance_col(peak_balance)}",
                        f"{lbl('Daily PnL')} {pnl_col(daily_pnl) if daily_pnl is not None else A('N/A','gray')}",
                        f"{lbl('Daily loss')} {pnl_col(-abs(daily_loss_val)) if daily_loss_val is not None else A('N/A','gray')}",
                        f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                        f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                        f"{lbl('last_reason')} {A(last_reason, 'yellow')}",
                    )
                    if gate_text:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                            f"{lbl('Schema')} {A(schema_ok, 'green' if schema_ok == '✅' else 'yellow', bold=True)}",
                            f"{lbl('Open trades')} {A(str(open_count), 'white')}",
                            f"{lbl('Trade log')} {A(str(trade_count), 'white')}",
                            f"{lbl('Balance')} {balance_col(balance_val)}",
                            f"{lbl('Peak balance')} {balance_col(peak_balance)}",
                            f"{lbl('Daily PnL')} {pnl_col(daily_pnl) if daily_pnl is not None else A('N/A','gray')}",
                            f"{lbl('Daily loss')} {pnl_col(-abs(daily_loss_val)) if daily_loss_val is not None else A('N/A','gray')}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                            f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                            f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                            f"{lbl('gates')} {A(gate_text, 'white')}",
                            f"{lbl('last_reason')} {A(last_reason, 'yellow')}",
                        )
                    embed.add_field(name=sim_id, value=value, inline=False)
                except Exception:
                    embed.add_field(name=sim_id, value="Error reading sim data", inline=False)

            checked_text = f"Checked: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}"
            embed.set_footer(text=checked_text)
            _append_footer(embed)
            return embed

        async def _send_simhealth_paginated(start_page: int):
            pages_count = max(1, (len(sim_items) + 2) // 3)
            page = max(1, min(start_page, pages_count))
            message = await ctx.send(embed=_build_simhealth_embed(page))
            if pages_count <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page = pages_count if page == 1 else page - 1
                elif emoji == "▶️":
                    page = 1 if page == pages_count else page + 1

                try:
                    await message.edit(embed=_build_simhealth_embed(page))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        await _send_simhealth_paginated(page_num)
    except Exception:
        logging.exception("simhealth_error")
        await _send_embed(ctx, "simhealth failed due to an internal error.")


@bot.command(name="lastskip")
async def lastskip(ctx):
    try:
        reason = getattr(bot, "last_skip_reason", None)
        ts = getattr(bot, "last_skip_time", None)
        if ts is not None:
            ts_text = _format_ts(ts)
        else:
            ts_text = "N/A"

        sim_state = get_sim_last_skip_state()
        sim_lines = []
        for sim_id in sorted(sim_state.keys()):
            item = sim_state.get(sim_id, {})
            sim_reason = item.get("reason") or "N/A"
            sim_time = item.get("time")
            if sim_time is not None:
                sim_time_text = _format_ts(sim_time)
            else:
                sim_time_text = "N/A"
            sim_lines.append(f"{sim_id}: {sim_reason} ({sim_time_text})")
        sim_text = "\n".join(sim_lines) if sim_lines else "None"

        embed = discord.Embed(title="⏸ Last Skip Reasons", color=0xF39C12)
        trader_text = "None"
        if reason:
            trader_text = f"{reason} ({ts_text})"
        embed.add_field(name=_add_field_icons("Trader"), value=ab(A(trader_text, "yellow")), inline=False)
        embed.add_field(name=_add_field_icons("Sims"), value=ab(A(sim_text, "yellow")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("lastskip_error")
        await _send_embed(ctx, "lastskip failed due to an internal error.")

@bot.command(name="preopen")
async def preopen(ctx):
    """
    Pre-open readiness check:
    - Market status
    - Data freshness
    - Option contract snapshot sanity (best-effort)
    """
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            await _send_embed(ctx, "Market data unavailable.", title="Pre-Open Check", color=0xE74C3C)
            return

        market_open = df.attrs.get("market_open")
        market_status = "OPEN" if market_open else "CLOSED"
        data_freshness = _get_data_freshness_text() or "Data age: N/A"

        last_close = df.iloc[-1].get("close") if len(df) > 0 else None
        last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
        close_text = f"{last_close_val:.2f}" if isinstance(last_close_val, (int, float)) else "N/A"

        profile_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        profile = None
        try:
            with open(profile_path, "r") as f:
                profiles = yaml.safe_load(f) or {}
                profile = profiles.get("SIM03") or profiles.get("SIM01")
        except Exception:
            profile = None

        def _format_contract_table(rows):
            header = f"{'OTM':<6} {'Status':<8} Detail"
            lines = [header]
            for row in rows:
                label = row.get("label", "")
                ok = row.get("ok", False)
                if ok:
                    status = "🟢 OK"
                    detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
                else:
                    status = "🔴 FAIL"
                    detail = row.get("reason", "unavailable")
                lines.append(f"{label:<6} {status:<8} {detail}")
            return "```\n" + "\n".join(lines) + "\n```"

        def _check_contracts(direction: str, base_profile: dict) -> tuple[str, bool]:
            rows = []
            any_ok = False
            if not base_profile:
                return "Profile unavailable", False
            if last_close_val is None:
                return "Price unavailable", False
            try:
                base_otm = float(base_profile.get("otm_pct", 0.0))
            except (TypeError, ValueError):
                base_otm = 0.0
            otm_variants = [
                ("OTM x1.0", base_otm),
                ("OTM x1.5", base_otm * 1.5),
            ]
            for label, otm_val in otm_variants:
                try:
                    prof = dict(base_profile)
                    prof["otm_pct"] = max(0.0, float(otm_val))
                    contract, reason = select_sim_contract_with_reason(direction, last_close_val, prof)
                    if contract:
                        any_ok = True
                        symbol = contract.get("option_symbol", "symbol")
                        spread = contract.get("spread_pct")
                        spread_text = f"{spread:.3f}" if isinstance(spread, (int, float)) else "N/A"
                        rows.append({
                            "label": label,
                            "ok": True,
                            "symbol": symbol,
                            "spread": spread_text,
                        })
                    else:
                        rows.append({
                            "label": label,
                            "ok": False,
                            "reason": reason or "unavailable",
                        })
                except Exception:
                    rows.append({
                        "label": label,
                        "ok": False,
                        "reason": "error",
                    })
            return _format_contract_table(rows), any_ok

        contract_status = "Not checked"
        contract_reason = None
        bull_ok = False
        bear_ok = False
        bull_text = "Not checked"
        bear_text = "Not checked"
        if profile and isinstance(last_close, (int, float)) and last_close > 0:
            bull_text, bull_ok = _check_contracts("BULLISH", profile)
            bear_text, bear_ok = _check_contracts("BEARISH", profile)
            if bull_ok or bear_ok:
                contract_status = "OK"
            else:
                contract_status = "Unavailable"
                contract_reason = "no_contracts_found"

        color = 0x2ECC71
        if bull_ok or bear_ok:
            color = 0x2ECC71
        elif market_status == "CLOSED":
            color = 0xF39C12
        else:
            color = 0xE74C3C

        title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
        embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
        embed.add_field(name=_add_field_icons("Market"), value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Last Price"), value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Data Freshness"), value=ab(A(data_freshness, "cyan")), inline=False)
        embed.add_field(name=_add_field_icons("Option Snapshot"), value=ab(A(contract_status, "green" if contract_status == "OK" else "yellow", bold=True)), inline=False)
        embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
        embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)
        if contract_reason:
            embed.add_field(name=_add_field_icons("Reason"), value=ab(A(contract_reason, "red", bold=True)), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception("preopen_error: %s", e)
        await _send_embed(ctx, "Pre-open check failed.", title="Pre-Open Check", color=0xE74C3C)

@bot.command()
async def trades(ctx, page: str | int = 1):
    def _safe_money(val, decimals=2):
        try:
            return f"${float(val):.{decimals}f}"
        except (TypeError, ValueError):
            return "N/A"

    def _safe_float(val):
        try:
            if val is None:
                return None
            return float(val)
        except (TypeError, ValueError):
            return None

    def _safe_r(val):
        try:
            return f"{float(val):.3f}R"
        except (TypeError, ValueError):
            return "N/A"

    def _badge_from_pnl(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    try:
        acc = load_account()
    except Exception:
        await _send_embed(ctx, "Could not load account data.")
        return

    trade_log = acc.get("trade_log", [])
    if not trade_log:
        await _send_embed(ctx, "No closed trades yet.")
        return

    per_page = 5
    total = len(trade_log)
    total_pages = (total + per_page - 1) // per_page

    page_num = 1
    if isinstance(page, str):
        page_text = page.strip().lower()
        if page_text.startswith("page"):
            page_text = page_text.replace("page", "").strip()
        if page_text.isdigit():
            page_num = int(page_text)
    elif isinstance(page, int):
        page_num = int(page)
    if page_num < 1 or page_num > total_pages:
        await _send_embed(ctx, f"Invalid page. Use `!trades 1` to `!trades {total_pages}`.")
        return

    # Show newest trades first
    newest_first = list(reversed(trade_log))

    def _build_trades_embed(page_num: int) -> "discord.Embed":
        page_num = max(1, min(page_num, total_pages))
        start = (page_num - 1) * per_page
        end = start + per_page
        page_trades = newest_first[start:end]

        lines = [f"📒 **Trade Log** (Page {page_num}/{total_pages})"]
        page_pnl = 0.0
        page_pnl_count = 0
        for idx, t in enumerate(page_trades, start=start + 1):
            trade_type = str(t.get("type", "unknown")).upper()
            style = t.get("style", "unknown")
            result = str(t.get("result", "unknown")).upper()
            entry_time = _format_ts(t.get("entry_time", "N/A"))
            exit_time = _format_ts(t.get("exit_time", "N/A"))
            exit_reason = t.get("result_reason") or t.get("exit_reason") or "N/A"

            risk = t.get("risk")
            pnl = t.get("pnl")
            r_mult = t.get("R")
            balance_after = t.get("balance_after")
            hold_text = _format_duration(t.get("time_in_trade_seconds"))
            mode_tag = _tag_trade_mode(t)
            pnl_arrow = _add_trend_arrow(pnl, good_when_high=True)

            risk_text = _safe_money(risk)
            pnl_text = _safe_money(pnl)
            r_text = _safe_r(r_mult)
            bal_text = _safe_money(balance_after)
            badge = _badge_from_pnl(pnl)
            pnl_pct_text = "N/A"
            entry_price = _safe_float(t.get("entry_price"))
            exit_price = _safe_float(t.get("exit_price"))
            try:
                if entry_price is not None and exit_price is not None and entry_price != 0:
                    pnl_pct = (float(exit_price) - float(entry_price)) / float(entry_price)
                    pnl_pct_text = _format_pct_signed(pnl_pct)
            except (TypeError, ValueError):
                pnl_pct_text = "N/A"

            snapshot = t.get("decision_snapshot", {})
            delta = snapshot.get("threshold_delta") if isinstance(snapshot, dict) else None
            blended = snapshot.get("blended_score") if isinstance(snapshot, dict) else None
            delta_text = None
            try:
                if delta is not None and blended is not None:
                    delta_val = float(delta)
                    blended_val = float(blended)
                    delta_text = f"Delta: {delta_val:+.4f} | Blended: {blended_val:.4f}"
            except (TypeError, ValueError):
                delta_text = None

            symbol = t.get("option_symbol")
            strike = t.get("strike")
            expiry = t.get("expiry")
            qty = t.get("quantity")

            contract_line = ""
            if symbol:
                contract_line = f"Contract: {symbol}\n"
            elif strike and expiry:
                contract_line = f"{strike} | Exp: {expiry} | Qty: {qty}\n"

            lines.append(
                f"\n{badge} **#{idx}** {trade_type} ({style}) - {result} | {mode_tag}\n"
                f"{contract_line}"
                f"Risk: {risk_text} | PnL: {pnl_text} ({pnl_pct_text}) {pnl_arrow} | R: {r_text}\n"
                f"{delta_text + '\n' if delta_text else ''}"
                f"Entry: {entry_time}\n"
                f"Exit: {exit_time}\n"
                f"Hold: {hold_text} | Reason: {exit_reason}\n"
                f"Balance After: {bal_text}"
            )

            try:
                pnl_val = float(pnl) if pnl is not None else None
            except (TypeError, ValueError):
                pnl_val = None
            if pnl_val is not None:
                page_pnl += pnl_val
                page_pnl_count += 1

        final_message = "\n".join(lines)
        if page_pnl_count > 0:
            if page_pnl > 0:
                color = 0x2ECC71
            elif page_pnl < 0:
                color = 0xE74C3C
            else:
                color = 0x3498DB
        else:
            color = 0x3498DB

        embed = discord.Embed(title=f"📒 Trade Log (Page {page_num}/{total_pages})", description=final_message, color=color)
        banner = _get_status_banner()
        if banner:
            embed.add_field(name="🧭 Status Banner", value=banner, inline=False)
        _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
        return embed

    async def _send_trades_paginated(start_page: int):
        page_num = max(1, min(start_page, total_pages))
        message = await ctx.send(embed=_build_trades_embed(page_num))
        if total_pages <= 1:
            return
        try:
            for emoji in ("◀️", "▶️", "⏹️"):
                await message.add_reaction(emoji)
        except Exception:
            return

        def _check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
            )

        while True:
            try:
                reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
            except asyncio.TimeoutError:
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break

            emoji = str(reaction.emoji)
            if emoji == "⏹️":
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break
            if emoji == "◀️":
                page_num = total_pages if page_num == 1 else page_num - 1
            elif emoji == "▶️":
                page_num = 1 if page_num == total_pages else page_num + 1

            try:
                await message.edit(embed=_build_trades_embed(page_num))
            except Exception:
                pass
            try:
                await message.remove_reaction(reaction.emoji, user)
            except Exception:
                pass

    await _send_trades_paginated(page_num)



@bot.command()
async def system(ctx):

    import pytz
    from datetime import datetime
    from core.account_repository import load_account
    from interface.health_monitor import check_health

    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # ---------------------------
    # Safe Account Load
    # ---------------------------
    try:
        acc = load_account()
    except:
        acc = {}

    balance = acc.get("balance", 0)
    trade_log = acc.get("trade_log", [])

    # ---------------------------
    # Safe Trade Count
    # ---------------------------
    total_trades = len(trade_log)

    if total_trades == 0:
        trade_status = "No closed trades yet."
    else:
        trade_status = f"{total_trades} closed trades"

    # ---------------------------
    # Safe Last Trade
    # ---------------------------
    if trade_log:
        last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
    else:
        last_trade = "None"

    # ---------------------------
    # Health Check (Safe)
    # ---------------------------
    try:
        status, report = check_health()
    except:
        status = "UNKNOWN"
        report = "Health monitor unavailable."

    market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

    # ---------------------------
    # Embed
    # ---------------------------
    embed = discord.Embed(
        title="🧠 SPY AI Control Center",
        color=discord.Color.green() if status == "HEALTHY" else discord.Color.orange()
    )
    embed.add_field(name=_add_field_icons("Market"), value=market_status, inline=True)
    embed.add_field(name=_add_field_icons("System Health"), value=status, inline=True)
    embed.add_field(name=_add_field_icons("System Diagnostics"), value=f"```\n{report}\n```", inline=False)
    embed.add_field(name=_add_field_icons("Balance"), value=f"${balance:.2f}", inline=True)

    embed.add_field(
        name=_add_field_icons("Trade Activity"),
        value=f"{trade_status}\nLast Trade: {last_trade}",
        inline=False
    )

    embed.add_field(
        name=_add_field_icons("Background Systems"),
        value=(
            "Auto Trader: Running\n"
            "Forecast Engine: Active\n"
            "Conviction Watcher: Active\n"
            "Prediction Grader: Active\n"
            "Heart Monitor: Active"
        ),
        inline=False
    )

    if total_trades < 10:
        embed.add_field(
            name=_add_field_icons("Analytics Status"),
            value=(
                "⚠️ Not enough trade data for:\n"
                "• Expectancy\n"
                "• Risk Metrics\n"
                "• Edge Stability\n"
                "System is collecting data."
            ),
            inline=False
        )

    embed.set_footer(text=f"System time: {_format_ts(now)}")
    _append_footer(embed)

    await ctx.send(embed=embed)




@bot.command()
async def ask(ctx, *, question=None):

    if not question:
        await _send_embed(ctx, "Usage: !ask <question>\nExample: !ask Did I overtrade?")
        return

    paper = get_paper_stats()
    career = get_career_stats()
    acc = load_account()

    answer = ask_ai(
        question,
        "Live market snapshot",
        paper,
        career,
        acc.get("trade_log", [])[-5:]
    )
    if answer is None:
        answer = "No response available."

    await _send_embed(ctx, answer, title="AI Coach")

@bot.event
async def on_command_error(ctx, error):

    logging.exception(f"[COMMAND ERROR] {error}")

    if isinstance(error, commands.MissingRequiredArgument):
        await _send_embed(ctx, "Missing required argument. Use `!help <command>`.")
        return

    if isinstance(error, commands.BadArgument):
        await _send_embed(ctx, "Invalid argument type. Use `!help <command>`.")
        return

    if isinstance(error, commands.CommandNotFound):
        await _send_embed(ctx, "Unknown command. Type `!help`.")
        return

    await _send_embed(ctx, "⚠️ Internal error occurred. Logged for review.")

bot.run(DISCORD_TOKEN)
```

#### `interface/charting.py`
```python
# interface/charting.py

import os
import pandas as pd
from typing import cast
import pandas_ta as ta
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import pytz
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

from core.paths import DATA_DIR, CHART_DIR
from core.data_service import get_market_dataframe
from core.rate_limiter import rate_limit_sleep

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

DATA_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")
from core.data_service import get_client

client = get_client()

def generate_chart():

    df = None
    if os.path.exists(DATA_FILE):
        try:
            df = pd.read_csv(DATA_FILE)
        except Exception:
            df = None

    if df is None or df.empty:
        # FIX: fallback to Alpaca data via data_service if CSV is missing/unreadable
        df = get_market_dataframe()
        if df is None or df.empty:
            print("No data available (CSV missing + Alpaca fallback failed)")
            return False

    if df.empty or "timestamp" not in df.columns:
        # data_service returns index-based DF; normalize
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns and "timestamp" not in df.columns:
                df.rename(columns={"index": "timestamp"}, inplace=True)
        if "timestamp" not in df.columns:
            return False

    # ---------- Timestamp Cleanup ----------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df.set_index("timestamp", inplace=True)

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    if len(df) < 2:
        return False  # not enough candles to plot

    # ---------- Indicator Safe Zone ----------
    df["ema9_r"] = None
    df["ema20_r"] = None
    df["vwap_r"] = None

    try:
        if len(df) >= 9:
            close = cast(pd.Series, df["close"])
            df["ema9_r"] = ta.ema(close, length=9)

        if len(df) >= 20:
            close = cast(pd.Series, df["close"])
            df["ema20_r"] = ta.ema(close, length=20)

        if len(df) >= 15:
            high = cast(pd.Series, df["high"])
            low = cast(pd.Series, df["low"])
            close = cast(pd.Series, df["close"])
            volume = cast(pd.Series, df["volume"])
            df["vwap_r"] = ta.vwap(high, low, close, volume)
    except Exception as e:
        print("Indicator calculation skipped:", e)

    df = df.tail(200)

    os.makedirs(CHART_DIR, exist_ok=True)
    filepath = os.path.join(CHART_DIR, "chart.png")

    apds = []

    ema9 = cast(pd.Series, df["ema9_r"])
    if ema9.notna().any():
        apds.append(mpf.make_addplot(ema9, color="yellow"))

    ema20 = cast(pd.Series, df["ema20_r"])
    if ema20.notna().any():
        apds.append(mpf.make_addplot(ema20, color="purple"))

    vwap = cast(pd.Series, df["vwap_r"])
    if vwap.notna().any():
        apds.append(mpf.make_addplot(vwap, color="blue"))

    try:
        mpf.plot(
            df,
            type="candle",
            style="yahoo",
            addplot=apds if apds else None,
            volume=True,
            savefig=filepath
        )
    except Exception as e:
        print("Chart plotting failed:", e)
        return False

    return filepath

def generate_live_chart():

    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # Set start and end times properly in Eastern Time zone
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)

    # If before open → no chart
    if now < market_open:
        return False

    start_utc = market_open.astimezone(pytz.UTC)
    end_utc = now.astimezone(pytz.UTC)

    if client is None:
        return False

    request = StockBarsRequest(
        symbol_or_symbols="SPY",
        timeframe=TimeFrame(1, TimeFrameUnit("Min")),
        start=start_utc,
        end=end_utc,
        feed=DataFeed.IEX
    )

    try:
        rate_limit_sleep("alpaca_stock_bars", ALPACA_MIN_CALL_INTERVAL_SEC)
        bars = client.get_stock_bars(request)
        df = getattr(bars, "df", None)
    except Exception as e:
        print("Alpaca fetch failed:", e)
        return False

    if not isinstance(df, pd.DataFrame) or df.empty:
        return False

    # Drop symbol level if multi-index
    if isinstance(df.index, pd.MultiIndex):
        df.index = df.index.get_level_values("timestamp")

    # Convert timezone properly
    dt_index = pd.DatetimeIndex(pd.to_datetime(df.index))
    if dt_index.tz is None:
        dt_index = dt_index.tz_localize("UTC")
    dt_index = dt_index.tz_convert("US/Eastern").tz_localize(None)
    df.index = dt_index

    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    # Ensure index is ordered
    df = df.sort_index()
    if len(df) < 5:
        return False

    df = df.tail(400)

    # Calculate indicators
    try:
        df["ema9"] = df["close"].ewm(span=9).mean()
        df["ema20"] = df["close"].ewm(span=20).mean()
        if "volume" in df.columns:
            high = cast(pd.Series, df["high"])
            low = cast(pd.Series, df["low"])
            close = cast(pd.Series, df["close"])
            volume = cast(pd.Series, df["volume"])
            df["vwap"] = ta.vwap(high, low, close, volume)
    except Exception:
        pass

    # --- PLOTTING ---
    fig, (ax1, ax2) = plt.subplots(
        nrows=2,
        ncols=1,
        sharex=True,
        figsize=(14, 10),
        gridspec_kw={"height_ratios": [3, 1]},
    )

    # PRICE (top axis)
    ax1.plot(df.index, df["close"], label="Price", linewidth=2)
    ax1.plot(df.index, df["ema9"], label="EMA 9", linewidth=2)
    ax1.plot(df.index, df["ema20"], label="EMA 20", linewidth=2)
    if "vwap" in df.columns:
        ax1.plot(df.index, df["vwap"], label="VWAP", linewidth=2)

    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left")
    ax1.grid(True, axis='y', linestyle='--', alpha=0.3)

    # VOLUME (bottom axis)
    colors = ["green" if df["close"].iloc[i] >= df["open"].iloc[i] else "red"
        for i in range(len(df))]

    ax2.bar(df.index, df["volume"], color=colors, alpha=0.4, width=((1/1440) * 8))
    ax2.set_ylabel("Volume")
    ax2.grid(True, axis='y', linestyle='--', alpha=0.3)

    ax1.set_title("SPY Live Session (Alpaca)")

    # =====================
    # FINAL PLOTTING
    # =====================
    plt.xticks(df.index[::5], rotation=45)  # Show every 5th tick for readability

    # Add gridlines
    ax1.grid(True, axis='y', linestyle='--', alpha=0.3)

    # Ensure that the date formatting and session times are correct
    session_date = cast(pd.Timestamp, df.index[0]).date()

    market_open = datetime.combine(session_date, datetime.min.time()).replace(hour=9, minute=30)
    market_close = datetime.combine(session_date, datetime.min.time()).replace(hour=16, minute=0)

    ax1.set_xlim(float(mdates.date2num(market_open)), float(mdates.date2num(market_close)))

    # Format x-axis to display time properly (removes date)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Make sure the ticks appear at 30-minute intervals
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))

    plt.tight_layout()
    os.makedirs(CHART_DIR, exist_ok=True)
    filepath = os.path.join(CHART_DIR, "live.png")
    plt.savefig(filepath)
    plt.close()
    return filepath
```

#### `interface/fmt.py`
```python
"""
ANSI formatting helpers for Discord embeds.

Usage:
    from interface.fmt import ab, A, lbl, pnl_col, conf_col, ...
    embed.add_field(name="PnL", value=ab(pnl_col(pnl)))

Notes:
    - ANSI colors render in Discord desktop/web inside ```ansi``` blocks.
    - Mobile shows plain text in a code block (still readable).
"""

_RST = "\u001b[0m"
_BLD = "\u001b[1m"
_GRN = "\u001b[32m"
_RED = "\u001b[31m"
_YLW = "\u001b[33m"
_BLU = "\u001b[34m"
_MGT = "\u001b[35m"
_CYN = "\u001b[36m"
_WHT = "\u001b[37m"
_GRY = "\u001b[30m"


def _color_code(name: str) -> str:
    return {
        "gray": _GRY,
        "red": _RED,
        "green": _GRN,
        "yellow": _YLW,
        "blue": _BLU,
        "magenta": _MGT,
        "cyan": _CYN,
        "white": _WHT,
    }.get(name, _WHT)


def A(text, color: str = "white", bold: bool = False) -> str:
    """Wrap text in ANSI color codes."""
    c = _color_code(color)
    b = _BLD if bold else ""
    return f"{b}{c}{text}{_RST}"


def ab(*lines) -> str:
    """Wrap lines in a Discord ```ansi``` block."""
    body = "\n".join(str(ln) for ln in lines)
    return f"```ansi\n{body}\n{_RST}```"


def lbl(text: str) -> str:
    """Cyan label for key: value pairs."""
    return A(f"{text}:", "cyan")


def pnl_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num > 0 else "red" if num < 0 else "yellow"
    sign = "+" if num >= 0 else "-"
    return A(f"{sign}${abs(num):,.2f}", color, bold=True)


def signed_col(val, prefix: str = "$") -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num > 0 else "red" if num < 0 else "yellow"
    sign = "+" if num >= 0 else "-"
    return A(f"{sign}{prefix}{abs(num):,.2f}", color)


def pct_col(val, good_when_high: bool = True, multiply: bool = True) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    pct = num * 100 if multiply else num
    if good_when_high:
        color = "green" if pct >= 55 else "yellow" if pct >= 45 else "red"
    else:
        color = "red" if pct >= 55 else "yellow" if pct >= 45 else "green"
    return A(f"{pct:.1f}%", color, bold=True)


def conf_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num >= 0.65:
        return A(f"{num:.2f} (HIGH)", "green", bold=True)
    if num >= 0.52:
        return A(f"{num:.2f} (MED)", "yellow", bold=True)
    return A(f"{num:.2f} (LOW)", "red", bold=True)


def dir_col(direction: str) -> str:
    d = (direction or "").upper()
    if d in {"BULLISH", "BULL", "CALL", "LONG"}:
        return A(d, "green", bold=True)
    if d in {"BEARISH", "BEAR", "PUT", "SHORT"}:
        return A(d, "red", bold=True)
    if d in {"RANGE", "NEUTRAL", "FLAT"}:
        return A(d, "yellow", bold=True)
    return A(d or "N/A", "gray")


def result_col(result: str) -> str:
    r = (result or "").upper()
    if r in {"WIN", "PROFIT"}:
        return A(r, "green", bold=True)
    if r in {"LOSS", "LOSE"}:
        return A(r, "red", bold=True)
    return A(r or "N/A", "yellow")


def regime_col(regime: str) -> str:
    r = (regime or "").upper()
    if r == "TREND":
        return A(r, "green", bold=True)
    if r == "RANGE":
        return A(r, "yellow", bold=True)
    if r == "VOLATILE":
        return A(r, "red", bold=True)
    if r in {"COMPRESSION", "QUIET"}:
        return A(r, "gray", bold=True)
    return A(r or "N/A", "gray")


def vol_col(vol: str) -> str:
    v = (vol or "").upper()
    if v == "HIGH":
        return A(v, "red", bold=True)
    if v == "NORMAL":
        return A(v, "green", bold=True)
    if v == "LOW":
        return A(v, "yellow", bold=True)
    if v in {"DEAD", "QUIET"}:
        return A(v, "gray", bold=True)
    return A(v or "N/A", "gray")


def delta_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num >= 0 else "red"
    return A(f"{num:+.2f}", color, bold=True)


def drawdown_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num <= 0:
        return A("$0.00", "green")
    return A(f"-${abs(num):,.2f}", "red", bold=True)


def tier_col(tier: str) -> str:
    t = (tier or "").upper()
    if t == "HIGH":
        return A(t, "red", bold=True)
    if t == "MEDIUM":
        return A(t, "yellow", bold=True)
    if t == "LOW":
        return A(t, "gray", bold=True)
    return A(t or "N/A", "gray")


def exit_reason_col(reason: str) -> str:
    r = (reason or "").lower()
    if any(k in r for k in ["profit", "target", "win"]):
        return A(reason, "green", bold=True)
    if any(k in r for k in ["stop", "loss"]):
        return A(reason, "red", bold=True)
    if any(k in r for k in ["trailing", "timeout", "hold_max", "expiry"]):
        return A(reason, "yellow", bold=True)
    return A(reason or "unknown", "gray")


def ml_col(val) -> str:
    if val is None:
        return A("Warming Up", "gray")
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num >= 0.6:
        return A(f"{num:.2f}", "green", bold=True)
    if num >= 0.5:
        return A(f"{num:.2f}", "yellow", bold=True)
    return A(f"{num:.2f}", "red", bold=True)


def balance_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    return A(f"${num:,.2f}", "white", bold=True)


def wr_col(win_rate) -> str:
    try:
        num = float(win_rate)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    pct = num * 100
    if pct >= 55:
        color = "green"
    elif pct >= 45:
        color = "yellow"
    else:
        color = "red"
    return A(f"{pct:.1f}%", color, bold=True)
```

#### `interface/health_monitor.py`
```python
# interface/health_monitor.py

import os
import time
import logging
import asyncio
import pandas as pd
from datetime import datetime
import pytz

from core.paths import DATA_DIR
from core.market_clock import market_is_open
from analytics.execution_logger import _ensure_file as _ensure_exec_file

DATA_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")
ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")

# Analytics / telemetry files (non-critical, but should be visible in health)
PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")
CONV_FILE = os.path.join(DATA_DIR, "conviction_expectancy.csv")
FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")
SIGNAL_FILE = os.path.join(DATA_DIR, "signal_log.csv")
BLOCKED_FILE = os.path.join(DATA_DIR, "blocked_signals.csv")
CONTRACT_FILE = os.path.join(DATA_DIR, "contract_selection_log.csv")
EXEC_FILE = os.path.join(DATA_DIR, "execution_quality_log.csv")


def check_health():

    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    report = []
    healthy = True

    # Market status
    if market_is_open():
        report.append("Market: OPEN")
    else:
        report.append("Market: CLOSED")

    def _fmt_age(seconds: float) -> str:
        total = int(seconds)
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0 or hours > 0:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    def _file_age(path: str):
        if not os.path.exists(path):
            return None
        try:
            return time.time() - os.path.getmtime(path)
        except Exception:
            return None

    def _file_status(label: str, path: str, stale_after: int | None = None, critical: bool = False):
        nonlocal healthy
        if not os.path.exists(path):
            report.append(f"{label}: MISSING")
            if critical:
                healthy = False
            return
        if os.path.getsize(path) == 0:
            report.append(f"{label}: EMPTY")
            if critical:
                healthy = False
            return
        age = _file_age(path)
        if age is None:
            report.append(f"{label}: ERROR READING")
            if critical:
                healthy = False
            return
        if stale_after is not None and market_is_open() and age > stale_after:
            report.append(f"{label}: STALE ({_fmt_age(age)} old)")
            if critical:
                healthy = False
        else:
            report.append(f"{label}: OK ({_fmt_age(age)} old)")

    # --- Recorder Check ---
    if not os.path.exists(DATA_FILE):
        report.append("Recorder: FILE MISSING")
        healthy = False

    else:
        try:
            df = pd.read_csv(DATA_FILE, parse_dates=["timestamp"])

            if df.empty:
                report.append("Recorder: EMPTY FILE")
                healthy = False

            else:
                last_time = pd.to_datetime(df["timestamp"].iloc[-1])

                # Make last_time tz-aware in Eastern
                if last_time.tzinfo is None:
                    last_time = pytz.timezone("US/Eastern").localize(last_time)

                age = (now - last_time).total_seconds()

                if not market_is_open():
                    report.append("Recorder: Idle (market closed)")

                elif age > 300:
                    report.append(f"Recorder: STALLED ({int(age)}s old)")
                    healthy = False

                else:
                    report.append(f"Recorder: OK ({int(age)}s old)")

        except Exception as e:
            report.append(f"Recorder: ERROR READING FILE: {e}")
            healthy = False

    # --- Account File Check ---
    _file_status("Account File", ACCOUNT_FILE, critical=True)

    # --- Analytics / telemetry checks (non-critical) ---
    try:
        _ensure_exec_file()
    except Exception:
        pass
    _file_status("Predictions CSV", PRED_FILE, stale_after=3600, critical=False)
    _file_status("Conviction CSV", CONV_FILE, stale_after=300, critical=False)
    _file_status("Trade Features", FEATURE_FILE, stale_after=None, critical=False)
    _file_status("Signal Log", SIGNAL_FILE, stale_after=300, critical=False)
    _file_status("Blocked Signals", BLOCKED_FILE, stale_after=900, critical=False)
    _file_status("Contract Log", CONTRACT_FILE, stale_after=900, critical=False)
    _file_status("Execution Log", EXEC_FILE, stale_after=900, critical=False)

    status = "HEALTHY" if healthy else "ATTENTION NEEDED"

    return status, "\n".join(report)

# ==============================
# HEARTBEAT SYSTEM
# ==============================

async def start_heartbeat():
    while True:
        try:
            # Log the bot's heartbeat
            logging.info("Bot is alive at " + str(time.time()))
        except Exception as e:
            logging.exception(f"Heartbeat error: {e}")
        await asyncio.sleep(600)  # log every 10 minutes
```

#### `interface/watchers.py`
```python
# interface/watchers.py

import asyncio
import os
import yaml
import logging
from datetime import datetime, time as dtime, timedelta
import uuid
import pytz
import discord
try:
    import pandas_market_calendars as mcal
except ImportError:
    mcal = None
import pandas as pd
from pandas.errors import EmptyDataError

from core.market_clock import market_is_open
from core.data_service import get_market_dataframe
from core.debug import debug_log
from core.account_repository import save_account
from core.session_scope import get_rth_session_view
from core.data_integrity import validate_market_dataframe
from core.decision_context import DecisionContext
from core.paths import DATA_DIR
from decision.trader import (
    open_trade_if_valid,
    manage_trade,
    get_ml_visibility_snapshot,
    _finalize_reconstructed_trade,
)

from signals.conviction import calculate_conviction, momentum_is_decaying
from core.md_state import record_md_decay, is_md_enabled, get_md_state, md_needs_warning
from signals.opportunity import evaluate_opportunity 
from signals.environment_filter import trader_environment_filter
from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state

from analytics.prediction_stats import log_prediction
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.risk_control import dynamic_risk_percent
from analytics.feature_drift import detect_feature_drift
from analytics.grader import check_predictions
from simulation.sim_contract import select_sim_contract_with_reason, get_contract_error_stats, get_snapshot_probe
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    vol_col,
    delta_col,
    ml_col,
    result_col,
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)

def _infer_embed_style(text: str):
    lower = (text or "").lower()
    error_terms = ("error", "failed", "invalid", "unknown", "missing")
    warn_terms = ("warning", "warn", "blocked", "disabled", "limit", "skip")
    success_terms = ("success", "complete", "updated", "reset", "ok", "done")
    if any(t in lower for t in error_terms):
        return 0xE74C3C, "❌"
    if any(t in lower for t in warn_terms):
        return 0xF39C12, "⚠️"
    if any(t in lower for t in success_terms):
        return 0x2ECC71, "✅"
    return 0x3498DB, "ℹ️"

def _format_et(ts: datetime | None) -> str:
    if ts is None:
        return "N/A"
    eastern = pytz.timezone("America/New_York")
    if ts.tzinfo is None:
        ts = eastern.localize(ts)
    else:
        ts = ts.astimezone(eastern)
    return ts.strftime("%Y-%m-%d %H:%M:%S ET")

def _last_spy_price(df) -> float | None:
    try:
        if df is None or len(df) == 0:
            return None
        last = df.iloc[-1]
        val = last.get("close")
        return float(val) if val is not None else None
    except Exception:
        return None

def _parse_strike_from_symbol(option_symbol: str | None) -> float | None:
    if not option_symbol or not isinstance(option_symbol, str):
        return None
    try:
        strike_part = option_symbol[-8:]
        return int(strike_part) / 1000.0
    except Exception:
        return None


def _format_contract_simple(option_symbol: str | None, direction: str | None, expiry: str | None, strike: float | None = None) -> str:
    cp = None
    if isinstance(direction, str):
        d = direction.lower()
        if d == "bullish":
            cp = "CALL"
        elif d == "bearish":
            cp = "PUT"
    if cp is None and isinstance(option_symbol, str) and len(option_symbol) >= 10:
        try:
            cp_char = option_symbol[9]
            if cp_char == "C":
                cp = "CALL"
            elif cp_char == "P":
                cp = "PUT"
        except Exception:
            cp = None
    if strike is None:
        strike = _parse_strike_from_symbol(option_symbol)
    expiry_text = ""
    if isinstance(expiry, str) and len(expiry) >= 10:
        expiry_text = expiry[:10]
    label = "SPY"
    if cp:
        label = f"{label} {cp}"
    if expiry_text:
        label = f"{label} {expiry_text}"
    if isinstance(strike, (int, float)):
        label = f"{label} {strike:g}"
    return label

def _get_data_age_text() -> str | None:
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("America/New_York")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        now = datetime.now(eastern)
        age = (now - ts).total_seconds()
        if age < 0:
            age = 0
        def _fmt_age(seconds: float) -> str:
            total = int(seconds)
            hours = total // 3600
            minutes = (total % 3600) // 60
            secs = total % 60
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)
        market_open = None
        try:
            market_open = df.attrs.get("market_open")
        except Exception:
            market_open = None
        status_text = "Market open" if market_open else "Market closed"
        return f"{status_text} | Data age: {_fmt_age(age)} (last candle {ts.strftime('%H:%M:%S')} ET)"
    except Exception:
        return None


async def _send_embed_message(channel, message: str, title: str | None = None):
    if channel is None:
        return
    text = message if isinstance(message, str) else str(message)
    color, emoji = _infer_embed_style(text)
    if title:
        if not title.startswith(("✅", "❌", "⚠️", "ℹ️", "📘", "📋", "📈", "📊", "🧠", "🖥", "🤖", "📥", "📤")):
            title = f"{emoji} {title}"
    else:
        if text and not text.startswith(("✅", "❌", "⚠️", "ℹ️")):
            text = f"{emoji} {text}"
    embed = discord.Embed(title=title, description=text, color=color)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
    data_age = _get_data_age_text()
    if data_age:
        footer_parts.append(data_age)
    embed.set_footer(text=" | ".join(footer_parts))
    await channel.send(embed=embed)


async def _send(channel, message=None, **kwargs):
    if channel is None:
        return
    if message is None:
        message = ""
    if "embed" in kwargs:
        return await channel.send(message, **kwargs)
    if isinstance(message, discord.Embed):
        return await channel.send(embed=message)
    return await _send_embed_message(channel, message)


async def preopen_check_loop(bot, channel_id: int):
    """
    Auto pre-open readiness check. Runs once per trading day around 09:25 ET.
    Posts a summary embed to the given channel.
    """
    last_run_date = None
    eastern = pytz.timezone("America/New_York")
    target_time = dtime(9, 25)
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < target_time:
                await asyncio.sleep(30)
                continue
            if last_run_date == now_et.date():
                await asyncio.sleep(300)
                continue

            channel = bot.get_channel(channel_id) if bot else None
            if channel is None:
                await asyncio.sleep(300)
                continue

            df = get_market_dataframe()
            if df is None or df.empty:
                await _send_embed_message(channel, "Market data unavailable.", title="Pre-Open Check")
                last_run_date = now_et.date()
                continue

            # Alpaca connectivity check (best-effort)
            alpaca_status = "OK"
            try:
                api_key = os.getenv("APCA_API_KEY_ID")
                secret_key = os.getenv("APCA_API_SECRET_KEY")
                if not api_key or not secret_key:
                    alpaca_status = "Missing API keys"
                else:
                    from alpaca.trading.client import TradingClient
                    client = TradingClient(api_key, secret_key, paper=True)
                    _ = client.get_account()
            except Exception as e:
                alpaca_status = f"Error: {str(e).splitlines()[0]}"

            market_open = df.attrs.get("market_open")
            market_status = "OPEN" if market_open else "CLOSED"
            data_freshness = _get_data_age_text() or "Data age: N/A"
            last_close = df.iloc[-1].get("close") if len(df) > 0 else None
            close_text = f"{float(last_close):.2f}" if isinstance(last_close, (int, float)) else "N/A"

            profile_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
            )
            profile = None
            try:
                with open(profile_path, "r") as f:
                    profiles = yaml.safe_load(f) or {}
                    profile = profiles.get("SIM03") or profiles.get("SIM01")
            except Exception:
                profile = None

            def _format_contract_table(rows):
                header = f"{'OTM':<6} {'Status':<8} Detail"
                lines = [header]
                for row in rows:
                    label = row.get("label", "")
                    ok = row.get("ok", False)
                    if ok and row.get("probe"):
                        status = "🟡 PROBE"
                        detail = row.get("detail", "next expiry")
                    elif ok:
                        status = "🟢 OK"
                        detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
                    else:
                        status = "🔴 FAIL"
                        detail = row.get("reason", "unavailable")
                    lines.append(f"{label:<6} {status:<8} {detail}")
                return "```\n" + "\n".join(lines) + "\n```"

            expiry_notice = {"flag": False, "text": ""}

            def _next_trading_day(d):
                nd = d + timedelta(days=1)
                while nd.weekday() >= 5:
                    nd += timedelta(days=1)
                return nd

            def _check_contracts(direction: str, base_profile: dict) -> tuple[str, bool]:
                rows = []
                any_ok = False
                if not base_profile:
                    return "Profile unavailable", False
                last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
                if last_close_val is None:
                    return "Price unavailable", False
                try:
                    base_otm = float(base_profile.get("otm_pct", 0.0))
                except (TypeError, ValueError):
                    base_otm = 0.0
                otm_variants = [
                    ("OTM x1.0", base_otm),
                    ("OTM x1.5", base_otm * 1.5),
                ]
                for label, otm_val in otm_variants:
                    try:
                        prof = dict(base_profile)
                        prof["otm_pct"] = max(0.0, float(otm_val))
                        contract, reason = select_sim_contract_with_reason(
                            direction, last_close_val, prof
                        )
                        if contract is None and reason in {"no_candidate_expiry", "cutoff_passed"}:
                            # Probe next trading-day expiry (pre-open only)
                            probe_prof = dict(prof)
                            try:
                                probe_prof["dte_min"] = 1
                                probe_prof["dte_max"] = max(1, int(base_profile.get("dte_max", 1)))
                            except Exception:
                                probe_prof["dte_min"] = 1
                                probe_prof["dte_max"] = 1
                            probe_contract, probe_reason = select_sim_contract_with_reason(
                                direction, last_close_val, probe_prof
                            )
                            if probe_contract:
                                next_exp = probe_contract.get("expiry", "")
                                exp_text = next_exp[:10] if isinstance(next_exp, str) else "next expiry"
                                rows.append({
                                    "label": label,
                                    "ok": True,
                                    "probe": True,
                                    "detail": f"{probe_contract.get('option_symbol','symbol')} {exp_text} (next trading day)",
                                })
                                continue
                            try:
                                dte_min_val = int(base_profile.get("dte_min", 0))
                                dte_max_val = int(base_profile.get("dte_max", 0))
                            except Exception:
                                dte_min_val = base_profile.get("dte_min", "?")
                                dte_max_val = base_profile.get("dte_max", "?")
                            next_exp = _next_trading_day(now_et.date())
                            cutoff_note = ""
                            if reason == "cutoff_passed":
                                cutoff_note = " | 0DTE cutoff passed (13:30 ET)"
                            expiry_notice["flag"] = True
                            expiry_notice["text"] = (
                                f"dte_min={dte_min_val} dte_max={dte_max_val}{cutoff_note} "
                                f"| next expiry {next_exp.isoformat()}"
                            )
                        if contract:
                            any_ok = True
                            symbol = contract.get("option_symbol", "symbol")
                            spread = contract.get("spread_pct")
                            spread_text = f"{spread:.3f}" if isinstance(spread, (int, float)) else "N/A"
                            rows.append({
                                "label": label,
                                "ok": True,
                                "symbol": symbol,
                                "spread": spread_text,
                            })
                        else:
                            rows.append({
                                "label": label,
                                "ok": False,
                                "reason": reason or "unavailable",
                            })
                    except Exception:
                        rows.append({
                            "label": label,
                            "ok": False,
                            "reason": "error",
                        })
                return _format_contract_table(rows), any_ok

            contract_status = "Not checked"
            contract_reason = None
            bull_ok = False
            bear_ok = False
            bull_text = "Not checked"
            bear_text = "Not checked"
            if profile and isinstance(last_close, (int, float)) and last_close > 0:
                bull_text, bull_ok = _check_contracts("BULLISH", profile)
                bear_text, bear_ok = _check_contracts("BEARISH", profile)
                if bull_ok or bear_ok:
                    contract_status = "OK"
                else:
                    contract_status = "Unavailable"
                    contract_reason = "no_contracts_found"

            snapshot_probe = None
            if contract_status != "OK":
                try:
                    from alpaca.data.historical import OptionHistoricalDataClient
                    from alpaca.data.requests import OptionChainRequest, OptionSnapshotRequest
                    import alpaca.data.enums as alpaca_enums
                    from core.rate_limiter import rate_limit_wait
                    api_key = os.getenv("APCA_API_KEY_ID")
                    secret_key = os.getenv("APCA_API_SECRET_KEY")
                    if api_key and secret_key and isinstance(last_close, (int, float)):
                        client = OptionHistoricalDataClient(api_key, secret_key)
                        expiry_date = now_et.date()
                        wait = rate_limit_wait("alpaca_option_chain", 0.5)
                        if wait > 0:
                            await asyncio.sleep(wait)
                        contract_type = getattr(alpaca_enums, "ContractType", None)
                        options_feed = getattr(alpaca_enums, "OptionsFeed", None)
                        feed_val = None
                        try:
                            desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
                            if options_feed is not None:
                                if desired == "opra":
                                    feed_val = options_feed.OPRA
                                elif desired == "indicative":
                                    feed_val = options_feed.INDICATIVE
                        except Exception:
                            feed_val = None
                        type_call = None
                        try:
                            if contract_type is not None and hasattr(contract_type, "CALL"):
                                type_call = contract_type.CALL
                        except Exception:
                            type_call = None
                        chain = client.get_option_chain(
                            OptionChainRequest(
                                underlying_symbol="SPY",
                                type=type_call,
                                feed=feed_val,
                                expiration_date=expiry_date
                            )
                        )
                        symbol = None
                        if isinstance(chain, dict):
                            symbol = next(iter(chain.keys()), None)
                        else:
                            data = getattr(chain, "data", None)
                            if isinstance(data, dict):
                                symbol = next(iter(data.keys()), None)
                            elif isinstance(data, list) and data:
                                symbol = getattr(data[0], "symbol", None) or (data[0].get("symbol") if isinstance(data[0], dict) else None)
                            chains = getattr(chain, "chains", None)
                            if symbol is None and isinstance(chains, dict):
                                symbol = next(iter(chains.keys()), None)
                            df_chain = getattr(chain, "df", None)
                            if symbol is None and df_chain is not None:
                                try:
                                    if "symbol" in df_chain.columns and not df_chain.empty:
                                        symbol = df_chain["symbol"].iloc[0]
                                except Exception:
                                    symbol = None
                        if symbol:
                            snapshot_probe_lines = []
                            def _snap_meta(resp, label: str):
                                try:
                                    if isinstance(resp, dict):
                                        keys = list(resp.keys())[:3]
                                        size = len(resp)
                                        keys_text = ",".join(keys) if keys else "none"
                                        snapshot_probe_lines.append(f"{label}: size={size} keys={keys_text}")
                                    else:
                                        snapshot_probe_lines.append(f"{label}: type={type(resp).__name__}")
                                except Exception:
                                    snapshot_probe_lines.append(f"{label}: error")
                            try:
                                debug_log(
                                    "preopen_snapshot_probe_request",
                                    symbol=symbol,
                                    expiry=expiry_date.isoformat(),
                                )
                            except Exception:
                                pass
                            wait = rate_limit_wait("alpaca_option_snapshot", 0.5)
                            if wait > 0:
                                await asyncio.sleep(wait)
                            try:
                                if feed_val is not None:
                                    req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=feed_val)
                                else:
                                    req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
                            except Exception:
                                req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
                            snap_resp = client.get_option_snapshot(req)
                            _snap_meta(snap_resp, "default")
                            if isinstance(snap_resp, dict) and len(snap_resp) == 0 and feed_val is None:
                                try:
                                    options_feed = getattr(alpaca_enums, "OptionsFeed", None)
                                    if options_feed is not None and hasattr(options_feed, "INDICATIVE"):
                                        debug_log("preopen_snapshot_probe_retry", symbol=symbol, feed="indicative")
                                        req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=options_feed.INDICATIVE)
                                        snap_resp = client.get_option_snapshot(req)
                                        _snap_meta(snap_resp, "indicative")
                                except Exception:
                                    pass
                            snap_type = type(snap_resp).__name__
                            keys_hint = ""
                            if isinstance(snap_resp, dict):
                                keys_hint = f" keys={list(snap_resp.keys())[:5]}"
                            snapshot_probe = f"probe_symbol={symbol} resp_type={snap_type}{keys_hint}"
                            if snapshot_probe_lines:
                                snapshot_probe = f"{snapshot_probe} | feeds: " + " / ".join(snapshot_probe_lines)
                except Exception as e:
                    snapshot_probe = f"probe_error={str(e).splitlines()[0]}"

            color = 0x2ECC71 if (bull_ok or bear_ok) else (0xF39C12 if market_status == "CLOSED" else 0xE74C3C)
            title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
            embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
            alpaca_color = "green" if alpaca_status == "OK" else "red" if "Error" in alpaca_status or "Missing" in alpaca_status else "yellow"
            embed.add_field(name="Alpaca Connectivity", value=ab(A(alpaca_status, alpaca_color, bold=True)), inline=False)
            embed.add_field(name="Market", value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
            embed.add_field(name="Last Price", value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
            embed.add_field(name="Recorder Freshness", value=ab(A(data_freshness, "cyan")), inline=False)
            status_color = "green" if contract_status == "OK" else "yellow" if contract_status == "Unavailable" else "red"
            embed.add_field(name="Option Snapshot", value=ab(A(contract_status, status_color, bold=True)), inline=False)
            embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
            embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)
            if expiry_notice["flag"] and expiry_notice["text"]:
                embed.add_field(name="Expiry Window", value=ab(A(expiry_notice["text"], "yellow")), inline=False)
            if contract_reason:
                embed.add_field(name="Reason", value=ab(A(contract_reason, "red", bold=True)), inline=False)
            try:
                stats = get_contract_error_stats(3600)
                last_snap = stats.get("last_snapshot_error")
                if contract_status != "OK" and last_snap and isinstance(last_snap, (list, tuple)) and len(last_snap) == 2:
                    err_msg = str(last_snap[1])
                    if err_msg:
                        embed.add_field(name="Snapshot Debug", value=ab(A(err_msg, "yellow")), inline=False)
            except Exception:
                pass
            if snapshot_probe:
                embed.add_field(name="Snapshot Probe", value=ab(A(snapshot_probe, "yellow")), inline=False)
            try:
                probe = get_snapshot_probe()
                if probe and contract_status != "OK":
                    keys = probe.get("keys") or []
                    size = probe.get("size")
                    resp_type = probe.get("response_type")
                    keys_text = ", ".join([str(k) for k in keys]) if keys else "none"
                    size_text = str(size) if size is not None else "N/A"
                    probe_lines = [
                        f"resp_type={resp_type}",
                        f"size={size_text}",
                        f"keys={keys_text}",
                    ]
                    if probe.get("data_attr"):
                        probe_lines.append(f"data_attr={probe.get('data_attr')}")
                    if probe.get("snapshots_attr"):
                        probe_lines.append(f"snapshots_attr={probe.get('snapshots_attr')}")
                    embed.add_field(name="Raw Snapshot Probe", value=ab(A(" | ".join(probe_lines), "yellow")), inline=False)
            except Exception:
                pass
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {data_freshness}")
            await channel.send(embed=embed)

            last_run_date = now_et.date()
        except Exception:
            logging.exception("preopen_check_loop_error")
        await asyncio.sleep(30)


async def eod_open_trade_report_loop(bot, channel_id: int):
    """
    End-of-day open trade report for live trades (main trader).
    Posts summary of open trades at 16:00 ET.
    """
    last_run_date = None
    eastern = pytz.timezone("America/New_York")
    cutoff = dtime(16, 0)
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < cutoff:
                await asyncio.sleep(30)
                continue
            if last_run_date == now_et.date():
                await asyncio.sleep(300)
                continue

            channel = bot.get_channel(channel_id) if bot else None
            if channel is None:
                await asyncio.sleep(300)
                continue

            from core.account_repository import load_account
            acc = load_account()
            open_items = []
            # main open_trade
            t = acc.get("open_trade")
            if isinstance(t, dict):
                open_items.append(t)
            # reconstructed/open list
            open_trades = acc.get("open_trades")
            if isinstance(open_trades, list):
                for item in open_trades:
                    if isinstance(item, dict):
                        open_items.append(item)

            if not open_items:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Trades (Live)",
                    description="No open live trades at end of day.",
                    color=0x2ECC71,
                )
            else:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Trades (Live)",
                    description="Open trades at market close.",
                    color=0xF39C12,
                )
                for trade in open_items:
                    symbol = trade.get("option_symbol") or trade.get("symbol", "unknown")
                    qty = trade.get("quantity") or trade.get("qty")
                    entry_price = trade.get("entry_price")
                    stop = trade.get("stop")
                    target = trade.get("target")
                    entry_text = f"${entry_price:.2f}" if isinstance(entry_price, (int, float)) else "N/A"
                    stop_text = f"${stop:.2f}" if isinstance(stop, (int, float)) else "N/A"
                    target_text = f"${target:.2f}" if isinstance(target, (int, float)) else "N/A"
                    pnl_text = "N/A"
                    try:
                        current_price = get_option_price(symbol)
                        if current_price is not None and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
                            pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                            pnl_text = f"{'+' if pnl_val >= 0 else ''}${pnl_val:.2f}"
                    except Exception:
                        pnl_text = "N/A"
                    embed.add_field(
                        name=f"{symbol}",
                        value=f"Qty {qty} | Entry {entry_text} | Stop {stop_text} | Target {target_text} | PnL {pnl_text}",
                        inline=False
                    )
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {_get_data_age_text() or 'Data age: N/A'}")
            await channel.send(embed=embed)
            last_run_date = now_et.date()
        except Exception:
            logging.exception("eod_open_trade_report_error")
        await asyncio.sleep(30)


async def option_chain_health_loop(bot, channel_id: int):
    """
    Hourly during market hours: report chain/snapshot errors for sims.
    """
    last_run_hour = None
    eastern = pytz.timezone("America/New_York")
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4 or not market_is_open():
                await asyncio.sleep(300)
                continue
            # Run once per hour
            if last_run_hour == (now_et.date(), now_et.hour):
                await asyncio.sleep(30)
                continue
            if now_et.minute != 0:
                await asyncio.sleep(30)
                continue

            stats = get_contract_error_stats(3600)
            chain_errors = stats.get("chain_errors", 0)
            snapshot_errors = stats.get("snapshot_errors", 0)
            last_chain = stats.get("last_chain_error")
            last_snap = stats.get("last_snapshot_error")
            last_success = stats.get("last_success")

            color = 0x2ECC71 if (chain_errors + snapshot_errors) == 0 else 0xF39C12
            title = "✅ Option Chain Health (Last 60m)" if color == 0x2ECC71 else "⚠️ Option Chain Health (Last 60m)"
            embed = discord.Embed(title=title, color=color)
            embed.add_field(name="Chain Errors", value=ab(A(str(chain_errors), "red" if chain_errors else "green", bold=True)), inline=True)
            embed.add_field(name="Snapshot Errors", value=ab(A(str(snapshot_errors), "red" if snapshot_errors else "green", bold=True)), inline=True)
            if last_chain:
                embed.add_field(name="Last Chain Error", value=ab(A(str(last_chain[1])[:200], "red")), inline=False)
            else:
                embed.add_field(name="Last Chain Error", value=ab(A("None", "green")), inline=False)
            if last_snap:
                embed.add_field(name="Last Snapshot Error", value=ab(A(str(last_snap[1])[:200], "red")), inline=False)
            else:
                embed.add_field(name="Last Snapshot Error", value=ab(A("None", "green")), inline=False)
            if last_success:
                sym = last_success.get("symbol")
                spr = last_success.get("spread_pct")
                ct = last_success.get("contract_type")
                spr_text = f"{spr:.3f}" if isinstance(spr, (int, float)) else "N/A"
                embed.add_field(
                    name="Last Success",
                    value=ab(
                        f"{lbl('Symbol')} {A(sym or 'N/A', 'magenta')}  |  "
                        f"{lbl('Type')} {A(ct or 'N/A', 'cyan')}  |  "
                        f"{lbl('Spr')} {A(spr_text, 'yellow', bold=True)}"
                    ),
                    inline=False
                )
            else:
                embed.add_field(name="Last Success", value=ab(A("None", "yellow")), inline=False)
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {_get_data_age_text() or 'Data age: N/A'}")

            if bot is not None and channel_id:
                channel = bot.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            last_run_hour = (now_et.date(), now_et.hour)
        except Exception:
            logging.exception("option_chain_health_loop_error")
        await asyncio.sleep(30)
from analytics.conviction_stats import (
    log_conviction_signal,
    update_expectancy,
    get_conviction_expectancy_stats
)
from analytics.signal_logger import log_signal_attempt
from analytics.blocked_signal_tracker import log_blocked_signal, update_blocked_outcomes
from interface.fmt import ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col, delta_col, ml_col, result_col, exit_reason_col, balance_col, wr_col, tier_col, drawdown_col, pct_col

from interface.health_monitor import check_health
from execution.option_executor import close_option_position, get_option_price


_decision_buffer = {
    "attempts": 0,
    "blocked": 0,
    "opened": 0,
    "top_block_reason": {},
    "last_emit_time": None
}


def explain_block_reason(reason: str) -> str:
    mapping = {
        "regime_compression": "Regime is in compression; trend clarity is too low to trade.",
        "regime_range": "Regime is range-bound; trend conditions are not met.",
        "regime_no_data": "Regime unavailable due to insufficient data.",
        "volatility_dead": "Volatility is too low to support a trade.",
        "volatility_low": "Volatility is below minimum threshold.",
        "prediction_none": "Prediction unavailable for this cycle.",
        "direction_mismatch": "15m and 60m direction did not align.",
        "confidence": "Confidence did not meet minimum threshold.",
        "ml_threshold": "Blended score below adaptive threshold.",
        "expectancy_negative_regime": "Regime expectancy is negative.",
        "regime_low_confidence": "Regime confidence is too low.",
        "execution_plan_none": "Execution plan could not be generated.",
        "signal_none": "Signal generation returned no valid setup.",
        "no_market_data": "Market data unavailable.",
        "no_latest_price": "Latest price unavailable.",
        "protection_EDGE_DECAY": "Protection layer: edge decay active.",
        "protection_EQUITY_PROTECTION": "Protection layer: equity drawdown.",
        "protection_DAILY_LIMIT": "Protection layer: daily loss limit.",
        "protection_PDT_LIMIT": "Protection layer: PDT limit reached.",
        "capital_exposure_limit": "Capital exposure limit reached.",
        "max_open_trades_reached": "Maximum open trades limit reached.",
        "order_not_filled": "Order not filled; liquidity or price unavailable.",
        "spread_too_wide": "Option spread too wide; liquidity insufficient.",
        "limit_not_filled": "Limit order not filled; liquidity insufficient.",
        "slippage_guard_triggered": "Execution slippage exceeded 10%.",
        "partial_fill_too_small": "Partial fill below 50%; position closed.",
        "partial_fill_below_threshold": "Partial fill below 50%; position closed.",
        "reconstructed_emergency_stop": "Reconstructed position hit emergency stop after restart.",
    }
    return mapping.get(reason, f"Trade skipped: {reason}")


def _record_decision_attempt(trade_result, ctx):
    _decision_buffer["attempts"] += 1

    if ctx is not None and ctx.outcome == "opened":
        _decision_buffer["opened"] += 1
        return

    _decision_buffer["blocked"] += 1
    reason = ctx.block_reason if ctx is not None else None
    reason = reason or "unknown"
    counts = _decision_buffer["top_block_reason"]
    counts[reason] = counts.get(reason, 0) + 1


def get_decision_buffer_snapshot():
    block_counts = _decision_buffer["top_block_reason"]
    top_reason = "N/A"
    if block_counts:
        top_reason = max(block_counts, key=block_counts.get)

    ml_snapshot = get_ml_visibility_snapshot()
    ml_weight = ml_snapshot.get("ml_weight")
    avg_delta = ml_snapshot.get("avg_delta")
    return {
        "attempts": _decision_buffer["attempts"],
        "opened": _decision_buffer["opened"],
        "blocked": _decision_buffer["blocked"],
        "top_block_reason": top_reason,
        "ml_weight": ml_weight,
        "avg_delta": avg_delta,
    }


# =========================================================
# OPPORTUNITY WATCHER
# =========================================================
print("Opportunity watcher started")

async def opportunity_watcher(bot, alert_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(alert_channel_id)

    last_alert = None

    while not bot.is_closed():

        if not market_is_open():
            await asyncio.sleep(120)
            continue

        df = get_market_dataframe()
        if df is None:
            await asyncio.sleep(120)
            continue

        result = evaluate_opportunity(df)

        if result and result != last_alert:

            side = result[0]
            low = result[1]
            high = result[2]
            price = result[3]
            conviction_score = result[4]
            tp_low = result[5] if len(result) > 5 else None
            tp_high = result[6] if len(result) > 6 else None
            stop_loss = result[7] if len(result) > 7 else None

            vol = volatility_state(df)
            regime = get_regime(df)

            # ----- Signal Strength Tier -----
            tier_score = conviction_score

            if vol == "HIGH":
                tier_score += 1

            if regime == "TREND":
                tier_score += 1

            if tier_score >= 6:
                tier = "HIGH"
                emoji = "🔥"
            elif tier_score >= 4:
                tier = "MEDIUM"
                emoji = "⚡"
            else:
                tier = "LOW"
                emoji = "🟡"

            opp_color = 0x2ECC71 if side == "CALL" else 0xE74C3C if side == "PUT" else 0x3498DB
            if tier == "HIGH":
                opp_color = 0x27AE60 if side == "CALL" else 0xC0392B
            opp_embed = discord.Embed(
                title=f"{emoji} {tier} Strength {side} Opportunity",
                color=opp_color
            )
            side_color = "green" if side == "CALL" else "red" if side == "PUT" else "blue"
            opp_embed.add_field(name="📍 Side", value=ab(A(side, side_color, bold=True)), inline=True)
            opp_embed.add_field(name="🧭 Tier", value=ab(tier_col(tier)), inline=True)
            opp_embed.add_field(name="💰 Current Price", value=ab(A(f"${price:.2f}", "white", bold=True)), inline=True)
            opp_embed.add_field(name="📐 Entry Zone", value=ab(A(f"${low:.2f} – ${high:.2f}", "cyan")), inline=True)
            if tp_low is not None and tp_high is not None:
                opp_embed.add_field(name="🎯 Take-Profit", value=ab(A(f"${tp_low:.2f} – ${tp_high:.2f}", "green", bold=True)), inline=True)
            if stop_loss is not None:
                opp_embed.add_field(name="🛑 Stop-Loss", value=ab(A(f"${stop_loss:.2f}", "red", bold=True)), inline=True)
            opp_embed.add_field(name="🔢 Conviction", value=ab(A(str(conviction_score), "yellow", bold=True)), inline=True)
            opp_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
            opp_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vol)), inline=True)
            opp_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
            await _send(channel, embed=opp_embed)

            last_alert = result

        await asyncio.sleep(120)


# =========================================================
# AUTO TRADER (Detailed + Structured)
# =========================================================
print("Auto trader started")
async def auto_trader(bot, channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id)
    nyse_calendar = mcal.get_calendar("NYSE") if mcal is not None else None
    while not bot.is_closed():
        if not getattr(bot, "trading_enabled", True):
            if not getattr(bot, "startup_notice_sent", False):
                startup_errors = getattr(bot, "startup_errors", [])
                err_text = "\n".join(f"• {e}" for e in startup_errors) if startup_errors else "• unknown"
                await _send(channel, 
                    "🛑 **Trading Disabled – Startup Phase Gate Failed**\n\n"
                    f"{err_text}"
                )
                bot.startup_notice_sent = True
            await asyncio.sleep(60)
            continue

        eastern = pytz.timezone("America/New_York")
        now_eastern = datetime.now(eastern)
        today = now_eastern.date()
        if nyse_calendar is not None and today.weekday() < 5:
            today_schedule = nyse_calendar.schedule(
                start_date=today.isoformat(),
                end_date=today.isoformat()
            )
            if today_schedule.empty:
                notice_date = getattr(bot, "last_holiday_notice_date", None)
                if notice_date != today.isoformat():
                    await _send(channel, 
                        "🗓️ **NYSE Holiday Closure**\n\n"
                        "Market is fully closed today. Auto trading attempts paused."
                    )
                    bot.last_holiday_notice_date = today.isoformat()
                await asyncio.sleep(60)
                continue

        if not market_is_open():
            await asyncio.sleep(60)
            continue

        df = get_market_dataframe()
        if df is None:
            await asyncio.sleep(60)
            continue
        spy_price = _last_spy_price(df)

        # Strict data integrity precheck before stale-data guard.
        validation = validate_market_dataframe(df)
        if not validation["valid"]:
            eastern = pytz.timezone("America/New_York")
            now = datetime.now(eastern)
            errors = validation.get("errors", [])

            debug_log("data_integrity_block", errors="; ".join(errors))

            was_invalid = getattr(bot, "data_integrity_state", False)
            last_warn = getattr(bot, "last_integrity_warning_time", None)
            allow_warn = (not was_invalid)
            if last_warn is None:
                allow_warn = True
            elif (now - last_warn).total_seconds() >= 300:
                allow_warn = True

            if allow_warn:
                top_error = errors[0] if errors else "unknown_integrity_error"
                await _send(channel, 
                    "⚠️ **data_integrity_block**\n\n"
                    "Market data failed integrity validation.\n"
                    f"Primary Reason: {top_error}\n"
                    "Trading attempt skipped."
                )
                bot.last_integrity_warning_time = now

            bot.data_integrity_state = True
            await asyncio.sleep(60)
            continue

        bot.data_integrity_state = False

        # Strict data freshness guard: block if latest candle is older than 2 minutes.
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is not None:
            eastern = pytz.timezone("America/New_York")
            ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            now = datetime.now(eastern)
            age_seconds = (now - ts).total_seconds()
            if age_seconds > 120:
                debug_log(
                    "data_stale_block",
                    candle_time=ts.isoformat(),
                    age_seconds=round(age_seconds, 1)
                )
                was_stale = getattr(bot, "data_stale_state", False)
                last_warn = getattr(bot, "last_stale_warning_time", None)
                allow_warn = (not was_stale)
                if last_warn is None:
                    allow_warn = True
                elif (now - last_warn).total_seconds() >= 300:
                    allow_warn = True
                if getattr(bot, "last_skip_reason", None) == "data_stale":
                    allow_warn = False

                if allow_warn:
                    stale_embed = discord.Embed(
                        title="⚠️ Market Data Stale — Trading Paused",
                        color=0xE67E22
                    )
                    stale_embed.add_field(name="⏱️ Data Age", value=ab(A(f"{age_seconds:.0f}s", "red", bold=True)), inline=True)
                    stale_embed.add_field(name="⚠️ Threshold", value=ab(A("120s", "yellow", bold=True)), inline=True)
                    stale_embed.add_field(name="📋 Action", value=ab(A("Bot will retry once data freshens. No trades will open until feed recovers.", "yellow")), inline=False)
                    stale_embed.set_footer(text=f"{_format_et(now)}")
                    await _send(channel, embed=stale_embed)
                    bot.last_stale_warning_time = now
                    bot.last_skip_reason = "data_stale"
                    bot.last_skip_time = now
                bot.data_stale_state = True
                await asyncio.sleep(60)
                continue
            bot.data_stale_state = False

        decision_ctx = DecisionContext()
        trade = await open_trade_if_valid(decision_ctx)
        _record_decision_attempt(trade, decision_ctx)

        # --- data collection: log every signal cycle ---
        log_signal_attempt(decision_ctx, trade)
        if decision_ctx.outcome == "blocked":
            _spy_price = df.iloc[-1]["close"] if df is not None and len(df) > 0 else None
            log_blocked_signal(decision_ctx, _spy_price)

        if decision_ctx.outcome == "blocked":
            reason = decision_ctx.block_reason or "unknown"
            eastern = pytz.timezone("America/New_York")
            now = datetime.now(eastern)
            last_reason = getattr(bot, "last_skip_reason", None)
            if last_reason != reason:
                last_time = getattr(bot, "block_reason_last_time", {}).get(reason)
                if last_time is None or (now - last_time).total_seconds() >= 300:
                    friendly_reason = explain_block_reason(reason)
                    skip_embed = discord.Embed(
                        title="⏸️ Trade Skipped",
                        color=0xF39C12
                    )
                    skip_embed.add_field(name="🚫 Reason Code", value=ab(A(reason, "red")), inline=False)
                    skip_embed.add_field(name="📋 Explanation", value=ab(A(friendly_reason, "yellow")), inline=False)
                    regime_now = get_regime(df) if df is not None else "N/A"
                    vol_now = volatility_state(df) if df is not None else "N/A"
                    blended_val = decision_ctx.blended_score
                    threshold_val = decision_ctx.threshold
                    _delta = (blended_val - threshold_val) if blended_val is not None and threshold_val is not None else None
                    conf_val = getattr(decision_ctx, "confidence_60m", None)
                    skip_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime_now)), inline=True)
                    skip_embed.add_field(name="⚡ Vol", value=ab(vol_col(vol_now)), inline=True)
                    skip_embed.add_field(name="📊 Score Δ", value=ab(delta_col(_delta) if _delta is not None else A("N/A", "gray")), inline=True)
                    if conf_val is not None:
                        try:
                            conf_pct = float(conf_val) * 100
                            conf_text = f"{conf_pct:.1f}%"
                        except (TypeError, ValueError):
                            conf_text = "N/A"
                        skip_embed.add_field(
                            name="🎯 Confidence",
                            value=ab(A(conf_text, "white", bold=True)),
                            inline=True
                        )
                    skip_embed.set_footer(text=f"Suppressed for 5m per reason | {_format_et(now)}")
                    await _send(channel, embed=skip_embed)
                    bot.block_reason_last_time[reason] = now
                    bot.last_skip_reason = reason
                    bot.last_skip_time = now

        if trade == "EQUITY_PROTECTION":
            debug_log("trade_gate", reason="EQUITY_PROTECTION")
            await asyncio.sleep(60)
            continue

        if trade == "EDGE_DECAY":
            debug_log("trade_gate", reason="EDGE_DECAY")
            await asyncio.sleep(60)
            continue


        if trade and isinstance(trade, dict):

            # ----------------------------
            # SETUP EXPECTANCY CHECK
            # ----------------------------
            setup_stats = calculate_setup_expectancy()
            current_setup = trade.get("setup")

            if setup_stats and current_setup in setup_stats:

                setup_avg_R = setup_stats[current_setup]["avg_R"]

                if setup_avg_R < 0:
                    debug_log(
                        "trade_blocked",
                        gate="setup_expectancy",
                        setup=current_setup,
                        avg_R=round(setup_avg_R, 3)
                    )
                    await asyncio.sleep(60)
                    continue

            # ----------------------------
            # RISK THROTTLE
            # ----------------------------
            risk_percent = dynamic_risk_percent()

            if risk_percent < 0.01:
                throttle_embed = discord.Embed(
                    title="⚠️ Risk Throttled",
                    description="System is in drawdown protection mode. Position sizing has been reduced.",
                    color=0xF39C12
                )
                throttle_embed.add_field(name="📉 Current Risk/Trade", value=ab(pct_col(risk_percent, good_when_high=False, multiply=True)), inline=True)
                throttle_embed.add_field(name="🛡️ Normal Risk", value=ab(A("1.0%", "green", bold=True)), inline=True)
                throttle_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=throttle_embed)

            # ----------------------------
            # ENVIRONMENT FILTER
            # ----------------------------
            expectancy = get_conviction_expectancy_stats()

            env = trader_environment_filter(
                df,
                trade["type"],
                trade["confidence"],
                expectancy,
                trade.get("regime", get_regime(df))
            )

            if not env["allow"]:
                debug_log(
                    "trade_blocked",
                    gate="environment_filter",
                    adjusted_conf=round(env["adjusted_conf"], 3),
                    reasons="; ".join(env["blocks"])
                )
            else:
                ml_prob = trade.get("ml_probability")
                debug_log(
                    "trade_opened",
                    direction=trade["type"],
                    entry=round(trade["entry_price"], 2),
                    confidence=round(trade["confidence"], 3),
                    regime=trade.get("regime")
                )

                ml_line = (
                    f"\nML Probability: {ml_prob*100:.1f}%"
                    if ml_prob is not None
                    else "\nML Probability: (warming up)"
                )
                dual_text = "YES" if decision_ctx.dual_alignment else "NO"
                conf_15 = (
                    f"{decision_ctx.confidence_15m:.2f}"
                    if decision_ctx.confidence_15m is not None
                    else "N/A"
                )
                conf_60 = (
                    f"{decision_ctx.confidence_60m:.2f}"
                    if decision_ctx.confidence_60m is not None
                    else "N/A"
                )
                blended_val = decision_ctx.blended_score
                threshold_val = decision_ctx.threshold
                blended_text = f"{blended_val:.2f}" if blended_val is not None else "N/A"
                threshold_text = f"{threshold_val:.2f}" if threshold_val is not None else "N/A"
                delta_text = (
                    f"{(blended_val - threshold_val):+0.2f}"
                    if blended_val is not None and threshold_val is not None
                    else "N/A"
                )
                ml_weight_text = (
                    f"{decision_ctx.ml_weight:.2f}"
                    if decision_ctx.ml_weight is not None
                    else "N/A"
                )
                regime_samples_text = (
                    str(decision_ctx.regime_samples)
                    if decision_ctx.regime_samples is not None
                    else "N/A"
                )
                expectancy_samples_text = (
                    str(decision_ctx.expectancy_samples)
                    if decision_ctx.expectancy_samples is not None
                    else "N/A"
                )
                decision_factors = (
                    "\n\n🧠 **Decision Factors:**\n"
                    f"Dual Alignment: {dual_text}\n"
                    f"15m: {conf_15} | 60m: {conf_60}\n"
                    f"Blended: {blended_text}\n"
                    f"Threshold: {threshold_text}\n"
                    f"Delta: {delta_text}\n"
                    f"ML Weight: {ml_weight_text}\n"
                    f"Regime Samples: {regime_samples_text}\n"
                    f"Expectancy Samples: {expectancy_samples_text}"
                )

                direction_color = 0x2ECC71 if trade["type"] == "bullish" else 0xE74C3C
                direction_emoji = "🟢" if trade["type"] == "bullish" else "🔴"
                open_embed = discord.Embed(
                    title=f"🤖 Trade Opened — {direction_emoji} {trade['type'].upper()}",
                    color=direction_color
                )
                qty_val = trade.get("quantity") or trade.get("qty") or "?"
                risk_val = trade.get("risk_dollars")
                risk_text = ab(A(f"${risk_val:.2f}", "yellow", bold=True)) if isinstance(risk_val, (int, float)) else ab(A("N/A", "gray"))
                _delta_num = (blended_val - threshold_val) if blended_val is not None and threshold_val is not None else None
                open_embed.add_field(name="💵 Entry", value=ab(A(f"${trade['entry_price']:.4f}", "white", bold=True)), inline=True)
                open_embed.add_field(name="🛑 Stop",   value=ab(A(f"${trade['stop']:.4f}", "red")), inline=True)
                open_embed.add_field(name="🎯 Target", value=ab(A(f"${trade['target']:.4f}", "green")), inline=True)
                open_embed.add_field(name="📦 Contracts", value=ab(A(str(qty_val), "white", bold=True)), inline=True)
                open_embed.add_field(name="🧾 Style", value=ab(A(trade["style"], "cyan")), inline=True)
                open_embed.add_field(name="💰 Risk", value=risk_text, inline=True)
                open_embed.add_field(name="💡 Confidence", value=ab(conf_col(trade["confidence"])), inline=True)
                open_embed.add_field(name="🤖 ML Score", value=ab(ml_col(ml_prob)), inline=True)
                open_embed.add_field(name="🧭 Regime", value=ab(regime_col(get_regime(df))), inline=True)
                open_embed.add_field(name="⚡ Volatility", value=ab(vol_col(volatility_state(df))), inline=True)
                dual_color = "green" if decision_ctx.dual_alignment else "red"
                open_embed.add_field(name="🔗 Dual Align", value=ab(A(dual_text, dual_color, bold=True)), inline=True)
                open_embed.add_field(
                    name="📊 Score / Threshold",
                    value=ab(
                        f"{lbl('Blended')} {A(blended_text, 'white', bold=True)}  "
                        f"{lbl('Thresh')} {A(threshold_text, 'white')}  "
                        f"{lbl('Δ')} {delta_col(_delta_num) if _delta_num is not None else A('N/A','gray')}"
                    ),
                    inline=False
                )
                open_embed.add_field(
                    name="🧠 Signal Detail",
                    value=ab(
                        f"{lbl('15m')} {A(conf_15,'cyan')}  {lbl('60m')} {A(conf_60,'cyan')}",
                        f"{lbl('ML wt')} {A(ml_weight_text,'magenta')}  {lbl('Reg samples')} {A(regime_samples_text,'white')}  {lbl('Exp samples')} {A(expectancy_samples_text,'white')}",
                    ),
                    inline=False
                )
                option_sym = trade.get("option_symbol")
                expiry = trade.get("expiry")
                strike = trade.get("strike")
                contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
                contract_lines = [A(contract_label, "magenta", bold=True)]
                if option_sym:
                    contract_lines.append(A(option_sym, "white"))
                open_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)
                if isinstance(spy_price, (int, float)):
                    open_embed.add_field(name="📈 SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
                open_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=open_embed)


        result = await asyncio.to_thread(manage_trade)

        if result:
            res, pnl, bal, trade = result
            close_color = 0x2ECC71 if res == "win" else 0xE74C3C
            close_emoji = "✅" if res == "win" else "❌"
            close_embed = discord.Embed(
                title=f"{close_emoji} Trade Closed — {res.upper()}",
                color=close_color
            )
            close_embed.add_field(name="💰 PnL", value=ab(pnl_col(pnl)), inline=True)
            close_embed.add_field(name="💵 Balance", value=ab(balance_col(bal)), inline=True)
            if isinstance(trade, dict):
                exit_reason = trade.get("exit_reason") or trade.get("result_reason") or "unknown"
                hold_secs = trade.get("time_in_trade_seconds")
                if hold_secs is not None:
                    try:
                        h_total = int(hold_secs)
                        h_mins = h_total // 60
                        h_secs = h_total % 60
                        hold_text = f"{h_mins}m {h_secs}s"
                    except (TypeError, ValueError):
                        hold_text = "N/A"
                else:
                    hold_text = "N/A"
                close_embed.add_field(name="🚪 Exit Reason", value=ab(exit_reason_col(exit_reason)), inline=True)
                close_embed.add_field(name="⏱️ Hold Time", value=ab(A(hold_text, "cyan")), inline=True)
                entry_price = trade.get("entry_price")
                exit_price = trade.get("exit_price")
                if entry_price and exit_price:
                    close_embed.add_field(
                        name="📍 Entry → Exit",
                        value=ab(f"{A(f'${entry_price:.4f}', 'white')} → {A(f'${exit_price:.4f}', 'white', bold=True)}"),
                        inline=True
                    )
                option_sym = trade.get("option_symbol")
                expiry = trade.get("expiry")
                strike = trade.get("strike")
                contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
                contract_lines = [A(contract_label, "magenta", bold=True)]
                if option_sym:
                    contract_lines.append(A(option_sym, "white"))
                close_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)
                if isinstance(spy_price, (int, float)):
                    close_embed.add_field(name="📈 SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
            close_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
            await _send(channel, embed=close_embed)

        await asyncio.sleep(60)



# =========================================================
# PREDICTION GRADER LOOP
# =========================================================
print("Prediction grader started")
async def prediction_grader(bot, channel_id=None):

    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id) if channel_id is not None else None
    pred_file = f"{DATA_DIR}/predictions.csv"

    while not bot.is_closed():
        check_predictions()

        try:
            preds = pd.read_csv(pred_file)
        except (FileNotFoundError, EmptyDataError):
            preds = None
        except Exception:
            preds = None

        if preds is not None and not preds.empty and "checked" in preds.columns and "correct" in preds.columns:
            graded = preds[preds["checked"] == True].tail(100)
            if len(graded) > 0:
                correct_series = pd.to_numeric(graded["correct"], errors="coerce")
                if isinstance(correct_series, pd.Series):
                    current_winrate = float(correct_series.fillna(0).mean())
                else:
                    current_winrate = 0.0

                history = getattr(bot, "predictor_winrate_history", [])
                history.append(current_winrate)
                if len(history) > 20:
                    history = history[-20:]
                bot.predictor_winrate_history = history

                baseline = sum(history) / len(history) if history else current_winrate
                bot.predictor_baseline_winrate = baseline

                degraded = current_winrate < (baseline - 0.15)
                was_degraded = getattr(bot, "predictor_drift_state", False)
                now = datetime.now(pytz.timezone("America/New_York"))
                last_warn = getattr(bot, "last_predictor_drift_warning_time", None)
                allow_warn = (not was_degraded)
                if last_warn is None:
                    allow_warn = True
                elif (now - last_warn).total_seconds() >= 300:
                    allow_warn = True

                if degraded:
                    debug_log(
                        "predictor_drift_warning",
                        rolling_samples=len(graded),
                        current_winrate=round(current_winrate, 4),
                        baseline_winrate=round(baseline, 4),
                        degradation=round(baseline - current_winrate, 4)
                    )
                    if allow_warn and channel is not None:
                        color = 0xE74C3C
                        embed = discord.Embed(
                            title="⚠️ Predictor Drift Warning",
                            color=color
                        )
                        embed.add_field(name="📉 Rolling Winrate (last 100)", value=ab(wr_col(current_winrate)), inline=True)
                        embed.add_field(name="📊 Baseline Winrate", value=ab(wr_col(baseline)), inline=True)
                        embed.add_field(name="📉 Degradation", value=ab(A(f"{(baseline - current_winrate)*100:.1f}pp", "red", bold=True)), inline=True)
                        embed.add_field(name="⚡ Action Needed", value=ab(A("Model accuracy has dropped >15% vs baseline. Consider retraining with `!retrain`.", "yellow")), inline=False)
                        embed.set_footer(text=f"Samples: {len(graded)} | {_format_et(now)}")
                        await _send(channel, embed=embed)
                        bot.last_predictor_drift_warning_time = now
                    bot.predictor_drift_state = True
                else:
                    bot.predictor_drift_state = False

        await asyncio.sleep(300)


# =========================================================
# CONVICTION WATCHER (Detailed + Setup Intelligence)
# =========================================================
print("Conviction watcher started")

async def conviction_watcher(bot, alert_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(alert_channel_id)

    conviction_state = "LOW"
    drift = detect_feature_drift()

    if drift:
        severity = drift["severity"]
        features = "\n".join(drift["features"])

        await _send(channel, 
            f"⚠️ **Feature Drift Detected**\n\n"
            f"Severity: {severity}\n"
            f"{features}"
        )

    while not bot.is_closed():
        try:
            if not market_is_open():
                await asyncio.sleep(120)
                continue

            df = get_market_dataframe()
            if df is None:
                await asyncio.sleep(120)
                continue

            # ---------------------------------------
            # Update Forward Expectancy Tracking
            # ---------------------------------------
            score, impulse, follow, direction = calculate_conviction(df)
            log_conviction_signal(df, direction, impulse, follow)
            update_expectancy(df)
            update_blocked_outcomes(df)  # fill forward returns for blocked signals

            regime = get_regime(df)
            vol = volatility_state(df)

            # ---------------------------------------
            # Setup Expectancy Context
            # ---------------------------------------
            setup_stats = calculate_setup_expectancy()

            profitable_setups = []
            negative_setups = []

            if setup_stats:
                for setup_name, stats in setup_stats.items():

                    if stats["avg_R"] > 0.5:
                        profitable_setups.append(setup_name)

                    if stats["avg_R"] < 0:
                        negative_setups.append(setup_name)

            # ---------------------------------------
            # Tier Calculation
            # ---------------------------------------
            tier_score = score

            if vol == "HIGH":
                tier_score += 1

            if regime == "TREND":
                tier_score += 1

            if tier_score >= 6:
                tier = "HIGH"
                emoji = "🔥"
            elif tier_score >= 4:
                tier = "MEDIUM"
                emoji = "⚡"
            else:
                tier = "LOW"
                emoji = "🟡"

            # ---------------------------------------
            # Tier Change Alert
            # ---------------------------------------
            if tier != conviction_state and tier in ["MEDIUM", "HIGH"]:
                tier_color = 0xFF6B35 if tier == "HIGH" else 0xF39C12
                direction_color_bar = "🟢" if direction == "bullish" else "🔴" if direction == "bearish" else "⚪"
                conv_embed = discord.Embed(
                    title=f"{emoji} Conviction Upgrade — {tier}",
                    color=tier_color
                )
                conv_embed.add_field(name="📍 Direction", value=ab(dir_col(direction)), inline=True)
                conv_embed.add_field(name="🔢 Score", value=ab(A(str(score), "yellow", bold=True)), inline=True)
                conv_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                conv_embed.add_field(name="⚡ Impulse", value=ab(A(f"{impulse:.2f}×", "green" if impulse >= 1 else "yellow", bold=True)), inline=True)
                follow_color = "green" if follow >= 0.5 else "yellow" if follow >= 0.3 else "red"
                conv_embed.add_field(name="🔗 Follow-Through", value=ab(A(f"{follow*100:.0f}%", follow_color, bold=True)), inline=True)
                conv_embed.add_field(name="📊 Volatility", value=ab(vol_col(vol)), inline=True)
                if profitable_setups:
                    conv_embed.add_field(
                        name="✅ Profitable Setups",
                        value=ab(*[A(f"• {s}", "green") for s in profitable_setups]),
                        inline=False
                    )
                if negative_setups:
                    conv_embed.add_field(
                        name="⚠️ Negative Expectancy Setups",
                        value=ab(*[A(f"• {s}", "red") for s in negative_setups]),
                        inline=False
                    )
                conv_embed.set_footer(text=f"Previous: {conviction_state} → {tier} | {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=conv_embed)

                conviction_state = tier

            # ---------------------------------------
            # Momentum Decay Detection
            # ---------------------------------------
            if conviction_state in ["MEDIUM", "HIGH"] and momentum_is_decaying(df):
                md_state = record_md_decay()
                md_enabled = bool(md_state.get("enabled"))
                decay_embed = discord.Embed(
                    title="⚠️ Momentum Decay Detected",
                    description="Impulse is weakening while conviction was elevated. Risk management action recommended.",
                    color=0xE67E22
                )
                decay_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                decay_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vol)), inline=True)
                decay_embed.add_field(name="📊 Conviction Level", value=ab(tier_col(conviction_state)), inline=True)
                md_text = A("ON", "green", bold=True) if md_enabled else A("OFF", "red", bold=True)
                md_hint = A("Use `!md enable` to tighten stops.", "yellow") if not md_enabled else A("MD strict mode is active.", "green")
                decay_embed.add_field(name="🧰 MD Strict", value=ab(f"{md_text}  {md_hint}"), inline=False)
                decay_embed.add_field(name="💡 Suggested Action", value=ab(A("Tighten stops, reduce size, or stand aside until impulse recovers.", "yellow")), inline=False)
                decay_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=decay_embed)

                conviction_state = "LOW"
        except Exception:
            logging.exception("conviction_watcher_error")
        await asyncio.sleep(120)


# =========================================================
# FORECAST WATCHER (Detailed + Clean Logging)
# =========================================================
print("Forecast watcher started")
async def forecast_watcher(bot, forecast_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(forecast_channel_id)

    last_logged_slot = None

    while not bot.is_closed():

        try:
            if not market_is_open():
                await asyncio.sleep(60)
                continue

            eastern = pytz.timezone("US/Eastern")
            now = datetime.now(eastern)

            slot_minute = 0 if now.minute < 30 else 30
            slot_time = now.replace(minute=slot_minute, second=0, microsecond=0)
            if last_logged_slot is None or slot_time > last_logged_slot:

                df = get_market_dataframe()
                if df is None:
                    await asyncio.sleep(60)
                    continue

                try:
                    pred = make_prediction(30, df)
                except Exception:
                    logging.exception("forecast_prediction_error")
                    await asyncio.sleep(60)
                    continue

                if pred is None:
                    logging.warning("forecast_prediction_none")
                    await asyncio.sleep(60)
                    continue

                regime = get_regime(df)
                vola = volatility_state(df)

                log_prediction(pred, regime, vola)
                try:
                    logging.info(
                        "prediction_logged",
                        extra={
                            "time": pred.get("time"),
                            "direction": pred.get("direction"),
                            "confidence": pred.get("confidence"),
                            "tf": pred.get("timeframe"),
                            "slot_time": slot_time.isoformat(),
                        },
                    )
                except Exception:
                    pass
                last_logged_slot = slot_time

                last = df.iloc[-1]
                session_recent = get_rth_session_view(df)
                if session_recent is None or session_recent.empty:
                    session_recent = df

                high_price = session_recent["high"].max()
                low_price = session_recent["low"].min()
                high_time = session_recent["high"].idxmax()
                low_time = session_recent["low"].idxmin()

                high_time_str = high_time.strftime("%H:%M") if hasattr(high_time, "strftime") else str(high_time)
                low_time_str = low_time.strftime("%H:%M") if hasattr(low_time, "strftime") else str(low_time)

                direction = pred["direction"]
                conf = pred["confidence"]
                if direction == "bullish":
                    fcast_color = 0x2ECC71
                    dir_emoji = "🟢"
                elif direction == "bearish":
                    fcast_color = 0xE74C3C
                    dir_emoji = "🔴"
                else:
                    fcast_color = 0x95A5A6
                    dir_emoji = "⚪"
                # Confidence tier
                if conf >= 0.65:
                    conf_label = "🔥 High"
                elif conf >= 0.52:
                    conf_label = "⚡ Medium"
                else:
                    conf_label = "🟡 Low"

                def _safe_price(val):
                    try:
                        return f"{float(val):.2f}"
                    except (TypeError, ValueError):
                        return "N/A"

                fcast_embed = discord.Embed(
                    title=f"📊 30-Minute Forecast — {dir_emoji} {direction.upper()}",
                    color=fcast_color
                )
                fcast_embed.add_field(name="📍 Direction", value=ab(dir_col(direction)), inline=True)
                fcast_embed.add_field(name="💡 Confidence", value=ab(conf_col(conf)), inline=True)
                fcast_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                fcast_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vola)), inline=True)
                _ph = f"${_safe_price(pred['high'])}"
                _pl = f"${_safe_price(pred['low'])}"
                _pc = f"${_safe_price(last.get('close'))}"
                _pv = f"${_safe_price(last.get('vwap'))}"
                _pe9 = f"${_safe_price(last.get('ema9'))}"
                _pe20 = f"${_safe_price(last.get('ema20'))}"
                _psh = f"${_safe_price(high_price)}"
                _psl = f"${_safe_price(low_price)}"
                fcast_embed.add_field(name="🎯 Predicted High", value=ab(A(_ph, "green", bold=True)), inline=True)
                fcast_embed.add_field(name="🎯 Predicted Low", value=ab(A(_pl, "red", bold=True)), inline=True)
                fcast_embed.add_field(
                    name="📍 Market Snapshot",
                    value=ab(
                        f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                        f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                        f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
                    ),
                    inline=False
                )
                fcast_embed.add_field(
                    name="📈 Session Range",
                    value=ab(
                        f"{lbl('High')} {A(_psh, 'green')} @ {A(high_time_str, 'white')}",
                        f"{lbl('Low')}  {A(_psl, 'red')} @ {A(low_time_str, 'white')}",
                    ),
                    inline=False
                )
                fcast_embed.set_footer(text=f"Forecast logged | {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=fcast_embed)

                last_logged_slot = slot_time

            await asyncio.sleep(20)
        except Exception:
            logging.exception("forecast_watcher_error")
            await asyncio.sleep(60)


# =========================================================
# HEART MONITOR
# =========================================================

import discord
from datetime import datetime
import pytz
import os
import time

# Track uptime
START_TIME = time.time()


# Health Monitor with Embed
async def heart_monitor(bot, channel_id):
    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id)
    last_health_emit = None
    last_reconcile_time = None

    while not bot.is_closed():
        try:
            from core.account_repository import load_account
            from core.market_clock import market_is_open
            from interface.health_monitor import check_health
            import logs.recorder as recorder_module

            eastern = pytz.timezone("US/Eastern")
            now = datetime.now(eastern)

            if hasattr(bot, "recorder_thread"):
                if not bot.recorder_thread.is_alive():
                    debug_log("recorder_thread_dead")
                    last_warn = getattr(bot, "last_recorder_thread_dead_warning_time", None)
                    if last_warn is None or (now - last_warn).total_seconds() >= 300:
                        await _send(channel, 
                            "⚠️ **recorder_thread_dead**\n\n"
                            "Recorder background thread is not alive."
                        )
                        bot.last_recorder_thread_dead_warning_time = now

            # Recorder stall monitoring (market-open only).
            if market_is_open():
                last_saved = getattr(recorder_module, "last_saved_timestamp", None)
                if last_saved:
                    try:
                        last_dt = datetime.strptime(last_saved, "%Y-%m-%d %H:%M:%S")
                        last_dt = eastern.localize(last_dt)
                        age_seconds = (now - last_dt).total_seconds()

                        if age_seconds > 120:
                            debug_log(
                                "recorder_stalled_warning",
                                last_saved_timestamp=last_saved,
                                age_seconds=round(age_seconds, 1)
                            )
                            was_stalled = getattr(bot, "recorder_stalled_state", False)
                            last_warn = getattr(bot, "last_recorder_stall_warning_time", None)
                            allow_warn = (not was_stalled)
                            if last_warn is None:
                                allow_warn = True
                            elif (now - last_warn).total_seconds() >= 300:
                                allow_warn = True

                            if allow_warn:
                                await _send(channel, 
                                    "⚠️ **recorder_stalled_warning**\n\n"
                                    f"Last saved candle: {last_saved} ET\n"
                                    f"Age: {age_seconds:.0f}s\n"
                                    "Recorder may not be appending new candles."
                                )
                                bot.last_recorder_stall_warning_time = now
                            bot.recorder_stalled_state = True
                        else:
                            bot.recorder_stalled_state = False
                    except Exception:
                        pass

            acc = load_account()

            if last_reconcile_time is None or (now - last_reconcile_time).total_seconds() >= 60:
                api_key = os.getenv("APCA_API_KEY_ID")
                secret_key = os.getenv("APCA_API_SECRET_KEY")
                if api_key and secret_key:
                    from alpaca.trading.client import TradingClient
                    client = TradingClient(api_key, secret_key, paper=True)
                    positions = client.get_all_positions()

                    broker_by_symbol = {}
                    for p in positions:
                        symbol = getattr(p, "symbol", None)
                        if symbol:
                            broker_by_symbol[str(symbol)] = p
                    broker_symbols = set(broker_by_symbol.keys())
                    internal_symbols = set(
                        t.get("option_symbol")
                        for t in acc.get("open_trades", [])
                        if isinstance(t, dict) and t.get("option_symbol")
                    )
                    open_trade = acc.get("open_trade")
                    if isinstance(open_trade, dict):
                        open_symbol = open_trade.get("option_symbol")
                        if open_symbol:
                            internal_symbols.add(open_symbol)

                    if broker_symbols != internal_symbols:
                        debug_log(
                            "broker_state_mismatch",
                            broker=sorted(str(s) for s in broker_symbols),
                            internal=sorted(str(s) for s in internal_symbols)
                        )
                        last_mismatch = getattr(bot, "broker_mismatch_last_time", None)
                        if last_mismatch is None or (now - last_mismatch).total_seconds() >= 300:
                            await _send(channel, 
                                "⚠️ **Broker State Mismatch**\n\n"
                                "Broker positions do not match internal open trades."
                            )
                            bot.broker_mismatch_last_time = now

                    orphan_symbols = broker_symbols.difference(internal_symbols)
                    if orphan_symbols:
                        for symbol in orphan_symbols:
                            position = broker_by_symbol.get(symbol)
                            if position is None:
                                continue
                            qty_raw = getattr(position, "qty", None)
                            if qty_raw is None:
                                continue
                            try:
                                qty_val = float(qty_raw)
                            except (TypeError, ValueError):
                                continue
                            if qty_val <= 0:
                                continue
                            close_result = await asyncio.to_thread(close_option_position, symbol, int(abs(qty_val)))
                            if not close_result.get("ok"):
                                debug_log(
                                    "orphan_position_close_failed",
                                    symbol=symbol,
                                    qty=qty_val
                                )
                                continue
                            filled_avg = close_result.get("filled_avg_price")
                            exit_price = None
                            if filled_avg is not None:
                                exit_price = filled_avg
                            else:
                                exit_price = get_option_price(symbol)
                            entry_price = getattr(position, "avg_entry_price", None)
                            try:
                                entry_price = float(entry_price) if entry_price is not None else None
                            except (TypeError, ValueError):
                                entry_price = None
                            pnl = 0.0
                            if entry_price is not None and exit_price is not None:
                                pnl = (exit_price - entry_price) * float(abs(qty_val)) * 100

                            trade = {
                                "trade_id": uuid.uuid4().hex,
                                "option_symbol": symbol,
                                "quantity": int(abs(qty_val)),
                                "entry_time": datetime.now(eastern).isoformat(),
                                "entry_price": entry_price,
                                "emergency_exit_price": exit_price,
                                "reconstructed": True,
                            }
                            _finalize_reconstructed_trade(
                                acc, trade, pnl, "orphan_broker_close"
                            )

                            debug_log(
                                "orphan_position_closed",
                                symbol=symbol,
                                qty=qty_val
                            )
                            last_times = getattr(bot, "orphan_close_last_time", {})
                            last = last_times.get(symbol)
                            if last is None or (now - last).total_seconds() >= 300:
                                await _send(channel, 
                                    f"⚠️ **Orphan Position Closed**\n\n"
                                    f"{symbol} (qty: {qty_val}) was not tracked internally."
                                )
                                last_times[symbol] = now
                                bot.orphan_close_last_time = last_times
                last_reconcile_time = now

            open_trades = acc.get("open_trades", [])
            if isinstance(open_trades, list):
                last_times = getattr(bot, "recon_notice_last_time", {})
                updated = False
                for t in open_trades:
                    if not isinstance(t, dict):
                        continue
                    notice = t.get("recon_notice")
                    if not isinstance(notice, dict):
                        continue
                    symbol = notice.get("symbol") or "unknown"
                    last = last_times.get(symbol)
                    if last is not None and (now - last).total_seconds() < 300:
                        continue
                    ntype = notice.get("type")
                    qty = notice.get("qty")
                    entry = notice.get("entry")
                    price = notice.get("price")
                    if ntype == "emergency_stop_success":
                        await _send(channel, 
                            f"🛑 Reconstructed position emergency-stopped: {symbol} "
                            f"qty={qty} entry={entry} price={price}"
                        )
                    elif ntype == "emergency_stop_failure":
                        await _send(channel, 
                            f"⚠️ Reconstructed emergency stop failed: {symbol} qty={qty}"
                        )
                    else:
                        continue
                    last_times[symbol] = now
                    t.pop("recon_notice", None)
                    updated = True
                bot.recon_notice_last_time = last_times
                if updated:
                    acc["open_trades"] = open_trades
                    save_account(acc)

            if last_health_emit is None or (now - last_health_emit).total_seconds() >= 1800:
                status, report = check_health()
                trades = acc.get("trade_log", [])
                balance = acc.get("balance", 0)
                risk_mode = acc.get("risk_mode", "NORMAL")

                market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

                # Uptime calculation
                uptime_seconds = int(time.time() - START_TIME)
                hours = uptime_seconds // 3600
                minutes = (uptime_seconds % 3600) // 60

                # Last trade
                last_trade = "None"
                if trades:
                    last_trade = trades[-1].get("exit_time", "Unknown")

                # Color coding
                if status == "HEALTHY":
                    color = discord.Color.green()
                    status_icon = "🟢"
                else:
                    color = discord.Color.red()
                    status_icon = "🔴"

                embed = discord.Embed(
                    title="🧠 SPY AI System Health",
                    color=color
                )

                embed.add_field(name="🧠 System Status", value=f"{status_icon} {status}", inline=True)
                embed.add_field(name="🟢 Market", value=market_status, inline=True)
                embed.add_field(name="🧰 Risk Mode", value=risk_mode, inline=True)

                embed.add_field(name="💰 Balance", value=f"${balance:.2f}", inline=True)
                embed.add_field(name="📦 Total Trades", value=str(len(trades)), inline=True)
                embed.add_field(name="🕘 Last Closed Trade", value=last_trade, inline=False)

                embed.add_field(
                    name="⏱ Uptime",
                    value=f"{hours}h {minutes}m",
                    inline=True
                )

                embed.add_field(
                    name="🧪 Health Report",
                    value=report if report else "All subsystems responding.",
                    inline=False
                )

                embed.set_footer(text=f"System Time: {_format_et(now)}")

                await _send(channel, embed=embed)
                last_health_emit = now

        except Exception as e:
            await _send(channel, "⚠️ Health monitor encountered an error.")
            print("Health monitor error:", e)

        await asyncio.sleep(60)
```

#### `logs/recorder.py`
```python
# logs/recorder.py
import sys
import os
import time
import csv
import fcntl
from datetime import datetime, timedelta
import pytz
import pandas as pd
from collections import deque

from core.paths import DATA_DIR
from core.market_clock import market_is_open
from core.data_service import get_client
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

# Get the absolute path to the root of your project folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add the core directory to the sys.path
FILE = os.path.join(DATA_DIR, "qqq_1m.csv")

last_saved_timestamp = None
_RECENT_TS = set()
_RECENT_TS_MAX = 300
_RECENT_TS_QUEUE = deque()


def _get_last_saved_timestamp():
    if not os.path.exists(FILE):
        return None
    try:
        with open(FILE, "rb") as f:
            try:
                f.seek(-2, os.SEEK_END)
                while f.tell() > 0:
                    if f.read(1) == b"\n":
                        break
                    f.seek(-2, os.SEEK_CUR)
            except OSError:
                f.seek(0)
            last_line = f.readline().decode("utf-8", errors="ignore").strip()
        if not last_line or last_line.lower().startswith("timestamp"):
            return None
        return last_line.split(",")[0].strip()
    except Exception:
        return None


def get_latest_candle():
    try:
        client = get_client()
        if client is None:
            print("No Alpaca client available.")
            return None

        eastern = pytz.timezone("US/Eastern")

        end = datetime.now()
        end = eastern.localize(end)

        start = end - timedelta(minutes=5)

        start_utc = start.astimezone(pytz.UTC)
        end_utc = end.astimezone(pytz.UTC)

        request = StockBarsRequest(
            symbol_or_symbols="SPY",
            timeframe=TimeFrame(1, TimeFrameUnit("Min")),
            start=start_utc,
            end=end_utc,
            feed=DataFeed.IEX
        )

        bars = client.get_stock_bars(request)
        bars_df = getattr(bars, "df", None)
        if not isinstance(bars_df, pd.DataFrame):
            print("No dataframe returned from Alpaca.")
            return None

        if bars_df.empty:
            print("No data returned from Alpaca.")
            return None

        df = bars_df.reset_index()

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("US/Eastern")
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)

        return df.iloc[-1]

    except Exception as e:
        print(f"Error in get_latest_candle: {e}")
        return None


def append_candle(row):
    """
    Append a new candle data row to the CSV file.
    """
    file_exists = os.path.exists(FILE)

    with open(FILE, "a", newline="") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            # Re-check last line under the lock to prevent duplicates
            try:
                last_ts = _get_last_saved_timestamp()
                if last_ts and row.get("timestamp") == last_ts:
                    return
            except Exception:
                pass
            writer = csv.DictWriter(
                f,
                fieldnames=["timestamp", "open", "high", "low", "close", "volume"]
            )

            # Write header only if the file doesn't already exist
            if not file_exists:
                writer.writeheader()

            writer.writerow(row)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def save_candle():
    global last_saved_timestamp

    candle = get_latest_candle()
    if candle is None:
        return

    ts = candle["timestamp"]

    # Convert the timestamp to string for easier comparison (make sure it's in the correct timezone)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    if last_saved_timestamp is None:
        last_saved_timestamp = _get_last_saved_timestamp()

    # Compare timestamps
    if ts_str == last_saved_timestamp:
        print("Duplicate candle detected. Skipping save.")
        return
    if ts_str in _RECENT_TS:
        print("Duplicate candle detected (recent set). Skipping save.")
        return

    last_saved_timestamp = ts_str  # Update the timestamp after saving
    _RECENT_TS.add(ts_str)
    _RECENT_TS_QUEUE.append(ts_str)
    while len(_RECENT_TS_QUEUE) > _RECENT_TS_MAX:
        old = _RECENT_TS_QUEUE.popleft()
        _RECENT_TS.discard(old)

    row = {
        "timestamp": ts_str,  # Save the timestamp as string
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"]
    }

    append_candle(row)
    print("Saved:", ts_str)



def _dedupe_file():
    if not os.path.exists(FILE):
        return
    try:
        with open(FILE, "r", newline="") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                df = pd.read_csv(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        if df.empty or "timestamp" not in df.columns:
            return
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            return
        df = df.sort_values("timestamp")
        df = df.drop_duplicates(subset=["timestamp"], keep="last")
        with open(FILE, "w", newline="") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df.to_csv(f, index=False)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        print("Recorder dedupe error:", e)


def run_recorder():
    """
    Run the recorder in a continuous loop while the market is open.
    """
    print("SPY recorder started...")
    _dedupe_file()

    while True:
        if market_is_open():
            try:
                save_candle()
            except Exception as e:
                print("Recorder error:", e)

        time.sleep(60)  # Sleep for 1 minute


def start_recorder_background():
    import threading

    def _run():
        while True:
            try:
                run_recorder()
            except Exception as e:
                print("Recorder crashed, restarting in 5s:", e)
                time.sleep(5)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread
```

#### `research/train_ai.py`
```python
import os
import pandas as pd
from typing import cast
import pandas_ta as ta
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
from datetime import datetime, timedelta
import pytz
import time

from core.paths import DATA_DIR

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")
CANDLE_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")

DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")
DATA_MAX_AGE_MINUTES = int(os.getenv("ML_TRAIN_DATA_MAX_AGE_MINUTES", "1440"))


def _is_file_recent(path: str, max_age_minutes: int) -> bool:
    try:
        if not os.path.exists(path):
            return False
        age_sec = time.time() - os.path.getmtime(path)
        return age_sec <= (max_age_minutes * 60)
    except Exception:
        return False


def _is_market_data_fresh(max_age_minutes: int) -> bool:
    if not os.path.exists(CANDLE_FILE):
        return False
    try:
        df = pd.read_csv(CANDLE_FILE)
        if df.empty or "timestamp" not in df.columns:
            return False
        last_ts = pd.to_datetime(df["timestamp"].iloc[-1], errors="coerce")
        if pd.isna(last_ts):
            return False
        if last_ts.tzinfo is None:
            last_ts = pytz.timezone("US/Eastern").localize(last_ts)
        else:
            last_ts = last_ts.tz_convert("US/Eastern")
        now = datetime.now(pytz.timezone("US/Eastern"))
        return (now - last_ts) <= timedelta(minutes=max_age_minutes)
    except Exception:
        return False


# =========================================================
# 1️⃣ Direction Model (Market Bias Model)
# =========================================================

def train_direction_model():

    if not os.path.exists(CANDLE_FILE):
        print("Market data file not found.")
        return
    if not _is_market_data_fresh(DATA_MAX_AGE_MINUTES):
        print("Market data is stale. Skipping direction model retrain.")
        return

    try:
        df = pd.read_csv(CANDLE_FILE)
    except Exception:
        print("Market data file unreadable. Skipping direction model retrain.")
        return

    close = cast(pd.Series, df["close"])
    high = cast(pd.Series, df["high"])
    low = cast(pd.Series, df["low"])
    volume = cast(pd.Series, df["volume"])
    df["ema9"] = ta.ema(close, length=9)
    df["ema20"] = ta.ema(close, length=20)
    df["rsi"] = ta.rsi(close, length=14)
    df["vwap"] = ta.vwap(high, low, close, volume)

    df["future_close"] = df["close"].shift(-30)
    df["target"] = (df["future_close"] > df["close"]).astype(int)

    df = df.dropna()

    features = df[["ema9", "ema20", "rsi", "vwap", "volume"]]
    labels = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        features, labels, test_size=0.2, random_state=42
    )

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        random_state=42
    )

    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    print("Direction Model Accuracy:", round(accuracy, 4))

    joblib.dump(model, DIR_MODEL_FILE)
    print("Direction model saved.")


# =========================================================
# 2️⃣ Edge Model (Trade Quality Filter)
# =========================================================

def train_edge_model():

    if not os.path.exists(FEATURE_FILE):
        print("No trade feature file found.")
        return
    if not _is_file_recent(FEATURE_FILE, DATA_MAX_AGE_MINUTES):
        print("Trade feature data is stale. Skipping edge model retrain.")
        return

    try:
        df = pd.read_csv(FEATURE_FILE)
    except Exception:
        print("Trade feature file unreadable. Skipping edge model retrain.")
        return

    if len(df) < 50:
        print("Not enough trade samples to train.")
        return

    df = df.dropna()

    # -----------------------------------
    # Add Expectancy Intelligence Columns
    # -----------------------------------

    if "setup_raw_avg_R" not in df.columns:
        df["setup_raw_avg_R"] = 0

    if "regime_raw_avg_R" not in df.columns:
        df["regime_raw_avg_R"] = 0

    feature_cols = [
        "regime_encoded",
        "volatility_encoded",
        "conviction_score",
        "impulse",
        "follow_through",
        "setup_encoded",
        "session_encoded",
        "confidence",
        "style_encoded",
        "setup_raw_avg_R",
        "regime_raw_avg_R"
    ]

    X = df[feature_cols]
    y = df["won"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=6,
        random_state=42
    )

    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    print("Edge Model Accuracy:", round(accuracy, 4))

    joblib.dump(model, EDGE_MODEL_FILE)
    print("Edge model saved.")

# =========================================================
# Run Both
# =========================================================

if __name__ == "__main__":
    train_direction_model()
    train_edge_model()
```

#### `signals/conviction.py`
```python
# signals/conviction.py
import pandas as pd
from core.data_service import get_market_dataframe


def calculate_conviction(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return 0, 0, 0, "neutral"

    try:
        recent = df.tail(30).copy()

        # Force numeric safety
        recent["high"] = pd.to_numeric(recent["high"], errors="coerce")
        recent["low"] = pd.to_numeric(recent["low"], errors="coerce")
        recent["close"] = pd.to_numeric(recent["close"], errors="coerce")
        recent["ema9"] = pd.to_numeric(recent["ema9"], errors="coerce")

        recent["range"] = recent["high"] - recent["low"]

        baseline = recent.head(25)
        avg_range = baseline["range"].mean()

        last5 = recent.tail(5)

        price_change = last5["close"].iloc[-1] - last5["close"].iloc[0]

        if price_change > 0:
            direction = "bullish"
        elif price_change < 0:
            direction = "bearish"
        else:
            direction = "neutral"

        impulse = last5["range"].mean() / avg_range if avg_range != 0 else 0

        closes = last5["close"].values
        direction_moves = sum(
            1 for i in range(1, len(closes))
            if (closes[i] - closes[i - 1]) * price_change > 0
        )

        follow_through = direction_moves / 4

        if "ema9" not in df.columns:
            return 0, 0, 0, "neutral"

        price = float(df["close"].iloc[-1])
        ema9 = float(df["ema9"].iloc[-1])

        pullback_depth = abs(price - ema9) / price if price != 0 else 0

        score = 0

        if impulse > 1.6:
            score += 2
        if follow_through > 0.6:
            score += 2
        if pullback_depth < 0.0025:
            score += 2

        return score, impulse, follow_through, direction

    except Exception:
        return 0, 0, 0, "neutral"


def momentum_is_decaying(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 10:
        return False

    recent = df.tail(10).copy()
    recent["range"] = recent["high"] - recent["low"]

    last5 = recent.tail(5)
    first5 = recent.head(5)

    last_impulse = last5["range"].mean()
    first_impulse = first5["range"].mean()

    if first_impulse == 0:
        return False

    decay_ratio = last_impulse / first_impulse

    return decay_ratio < 0.7


def scalp_context_valid(df, direction):

    if df is None:
        return False

    last = df.iloc[-1]
    price = last["close"]
    vwap = last["vwap"]

    if direction == "bullish" and price < vwap:
        return False
    if direction == "bearish" and price > vwap:
        return False

    return True
```

#### `signals/environment_filter.py`
```python
# signals/environment_filter.py

from signals.conviction import momentum_is_decaying


def adjust_confidence(base_confidence, regime, expectancy, df):

    adjusted = base_confidence
    penalties = []
    boosts = []

    if regime not in ["TREND", "VOLATILE"]:
        adjusted -= 0.10
        penalties.append("Unfavorable regime")

    expectancy_samples = 0
    if expectancy:
        expectancy_samples = int(expectancy.get("samples") or 0)

    if expectancy and expectancy_samples >= 30:
        if expectancy.get("avg_5m", 0) < 0:
            adjusted -= 0.10
            penalties.append("Negative 5m expectancy")

        if expectancy.get("wr_5m", 100) < 50:
            adjusted -= 0.05
            penalties.append("Low conviction winrate")

    if momentum_is_decaying(df):
        adjusted -= 0.07
        penalties.append("Momentum decay detected")

    if (
        regime in ["TREND", "VOLATILE"]
        and expectancy
        and expectancy_samples >= 30
        and expectancy.get("avg_5m", 0) > 0
    ):
        adjusted += 0.05
        boosts.append("Momentum regime alignment")

    adjusted = max(0, min(1, adjusted))

    return adjusted, penalties, boosts


def trader_environment_filter(df, model_direction, raw_confidence, expectancy, regime):

    adjusted_conf, penalties, boosts = adjust_confidence(
        raw_confidence,
        regime,
        expectancy,
        df
    )

    decay = momentum_is_decaying(df)

    allow_trade = True
    reason_block = []
    expectancy_samples = int(expectancy.get("samples") or 0) if expectancy else 0

    if adjusted_conf < 0.50:
        allow_trade = False
        reason_block.append("Adjusted confidence below 50%")

    if regime == "COMPRESSION":
        allow_trade = False
        reason_block.append("Compression regime")

    if expectancy and expectancy_samples >= 30 and expectancy.get("avg_5m", 0) < 0:
        allow_trade = False
        reason_block.append("Negative momentum expectancy")

    if decay:
        allow_trade = False
        reason_block.append("Momentum decaying")

    return {
        "allow": allow_trade,
        "adjusted_conf": adjusted_conf,
        "penalties": penalties,
        "boosts": boosts,
        "blocks": reason_block
    }
```

#### `signals/opportunity.py`
```python
# signal/opportunity.py

from core.data_service import get_market_dataframe
from signals.conviction import calculate_conviction, momentum_is_decaying
from signals.volatility import volatility_state


def evaluate_opportunity(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return None

    last = df.iloc[-1]

    try:
        price = float(last["close"])
        vwap = float(last["vwap"])
        ema9 = float(last["ema9"])
        ema20 = float(last["ema20"])
        rsi = float(last["rsi"])
    except Exception:
        return None

    # -----------------------------------
    # CONVICTION FIRST
    # -----------------------------------

    conviction_score, impulse, follow, direction = calculate_conviction(df)

    if conviction_score < 3:
        return None

    # -----------------------------------
    # VOLATILITY FILTER
    # -----------------------------------

    vol_state = volatility_state(df)

    if vol_state == "DEAD":
        return None

    if conviction_score >= 4 and vol_state == "LOW":
        return None

    if vol_state == "LOW":
        conviction_score -= 1

    if vol_state == "HIGH":
        conviction_score += 1

    # -----------------------------------
    # MOMENTUM HEALTH
    # -----------------------------------

    if momentum_is_decaying(df):
        return None

    # -----------------------------------
    # STRUCTURE SCORE
    # -----------------------------------

    structure_score = 0

    if price > vwap:
        structure_score += 1

    if ema9 > ema20:
        structure_score += 1

    if direction == "bullish" and 45 < rsi < 70:
        structure_score += 1

    if direction == "bearish" and 30 < rsi < 55:
        structure_score += 1

    try:
        atr = float(last["atr"])
    except Exception:
        return None
    if atr <= 0:
        return None

    distance_from_ema = abs(price - ema9)

    extended = distance_from_ema > (atr * 0.5)

    # -----------------------------------
    # FINAL ALIGNMENT
    # -----------------------------------

    if direction == "bullish" and structure_score >= 2 and not extended:
        entry_low = ema9 - (atr * 0.2)
        entry_high = ema9 + (atr * 0.2)
        entry_mid = (entry_low + entry_high) / 2
        tp_mult = 1.5
        sl_mult = 0.5
        if vol_state == "HIGH":
            tp_mult += 0.5
            sl_mult += 0.2
        elif vol_state == "LOW":
            tp_mult -= 0.3
            sl_mult -= 0.1
        if conviction_score >= 5:
            tp_mult += 0.3
            sl_mult += 0.1
        elif conviction_score <= 3:
            tp_mult -= 0.2
            sl_mult -= 0.1
        tp_mult = max(0.8, tp_mult)
        sl_mult = max(0.2, sl_mult)
        take_profit_low = entry_mid + (atr * tp_mult * 0.9)
        take_profit_high = entry_mid + (atr * tp_mult * 1.1)
        stop_loss = entry_mid - (atr * sl_mult)
        return (
            "CALLS",
            entry_low,
            entry_high,
            price,
            conviction_score,
            take_profit_low,
            take_profit_high,
            stop_loss,
        )

    if direction == "bearish" and structure_score >= 2 and not extended:
        entry_low = ema9 - (atr * 0.2)
        entry_high = ema9 + (atr * 0.2)
        entry_mid = (entry_low + entry_high) / 2
        tp_mult = 1.5
        sl_mult = 0.5
        if vol_state == "HIGH":
            tp_mult += 0.5
            sl_mult += 0.2
        elif vol_state == "LOW":
            tp_mult -= 0.3
            sl_mult -= 0.1
        if conviction_score >= 5:
            tp_mult += 0.3
            sl_mult += 0.1
        elif conviction_score <= 3:
            tp_mult -= 0.2
            sl_mult -= 0.1
        tp_mult = max(0.8, tp_mult)
        sl_mult = max(0.2, sl_mult)
        take_profit_high = entry_mid - (atr * tp_mult * 0.9)
        take_profit_low = entry_mid - (atr * tp_mult * 1.1)
        stop_loss = entry_mid + (atr * sl_mult)
        return (
            "PUTS",
            entry_low,
            entry_high,
            price,
            conviction_score,
            take_profit_low,
            take_profit_high,
            stop_loss,
        )

    return None
```

#### `signals/predictor.py`
```python
# signals/predictor.py

from datetime import datetime
import pytz
import math
from core.data_service import get_market_dataframe


def make_prediction(minutes=60, df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return None

    recent = df.tail(30)

    last = recent.iloc[-1]

    # FIX: defensive numeric parsing to avoid crashing the forecast loop
    try:
        price = float(last["close"])
    except Exception:
        return None

    vwap = last.get("vwap", price)
    try:
        vwap = float(vwap)
    except Exception:
        vwap = price
    if not math.isfinite(vwap):
        vwap = price

    try:
        high30 = float(recent["high"].max())
        low30 = float(recent["low"].min())
    except Exception:
        return None

    try:
        trend = float(recent["close"].iloc[-1]) - float(recent["close"].iloc[0])
    except Exception:
        return None

    vol = (high30 - low30) / price if price != 0 else 0

    # Deterministic evidence model:
    # turn structural features into directional evidence scores,
    # then map scores to probabilities via temperature-scaled softmax.
    bullish_score = 0.15
    bearish_score = 0.15
    range_score = 0.15
    reasons = []

    vwap_dist = (price - vwap) / price if price else 0
    loc_strength = min(abs(vwap_dist) / 0.004, 1.0)
    trend_strength = min(abs(trend) / price / 0.004, 1.0) if price else 0.0

    # Location vs VWAP
    if price > vwap:
        bullish_score += 0.9 * loc_strength
        reasons.append("Price above VWAP")
    else:
        bearish_score += 0.9 * loc_strength
        reasons.append("Price below VWAP")

    # Momentum
    if trend > 0:
        bullish_score += 0.9 * trend_strength
        reasons.append("Upward momentum")
    elif trend < 0:
        bearish_score += 0.9 * trend_strength
        reasons.append("Downward momentum")
    else:
        range_score += 0.15
        reasons.append("Flat momentum")

    # Volatility
    if vol < 0.0018:
        range_score += 1.0
        bullish_score += 0.1
        bearish_score += 0.1
        reasons.append("Low volatility (range favored)")
    elif vol > 0.004:
        bullish_score += 0.3
        bearish_score += 0.3
        reasons.append("High volatility (directional expansion)")
    else:
        bullish_score += 0.15
        bearish_score += 0.15
        range_score += 0.05
        reasons.append("Normal volatility")

    # Temperature keeps probabilities realistic and avoids overconfidence.
    temperature = 1.35
    exp_bull = math.exp(bullish_score / temperature)
    exp_bear = math.exp(bearish_score / temperature)
    exp_range = math.exp(range_score / temperature)
    total = exp_bull + exp_bear + exp_range
    bullish = exp_bull / total
    bearish = exp_bear / total
    range_prob = exp_range / total

    expected_move = price * (vol * (minutes / 30))
    pred_high = price + expected_move / 2
    pred_low = price - expected_move / 2

    direction = max(
        [("bullish", bullish), ("bearish", bearish), ("range", range_prob)],
        key=lambda x: x[1]
    )[0]

    confidence = max(bullish, bearish, range_prob)

    return {
        "time": datetime.now(pytz.timezone("US/Eastern")),
        "timeframe": minutes,
        "direction": direction,
        "confidence": round(confidence, 3),
        "high": round(pred_high, 2),
        "low": round(pred_low, 2),
        "reasons": reasons
    }
```

#### `signals/regime.py`
```python
# signals/regime.py
from core.data_service import get_market_dataframe

def get_regime(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 60:
        return "NO_DATA"

    recent = df.tail(60)

    price_change = recent["close"].iloc[-1] - recent["close"].iloc[0]
    high = recent["high"].max()
    low = recent["low"].min()

    total_range = high - low
    avg_candle = (recent["high"] - recent["low"]).mean()

    vwap = recent["vwap"].mean() if "vwap" in recent.columns else recent["close"].mean()

    above_vwap = (recent["close"] > vwap).sum()
    below_vwap = (recent["close"] < vwap).sum()

    directionality = abs(price_change) / total_range if total_range != 0 else 0

    if avg_candle < 0.08:
        return "COMPRESSION"

    if directionality > 0.6 and abs(above_vwap - below_vwap) > 30:
        return "TREND"

    if total_range > 1.2:
        return "VOLATILE"

    return "RANGE"
```

#### `signals/session_classifier.py`
```python
# signals/session_classifier.py

from datetime import datetime
import pytz

def classify_session(timestamp_iso):

    if not timestamp_iso:
        return "UNKNOWN"

    eastern = pytz.timezone("US/Eastern")

    try:
        t = datetime.fromisoformat(timestamp_iso)

        # If naive datetime → assume Eastern
        if t.tzinfo is None:
            t = eastern.localize(t)
        else:
            t = t.astimezone(eastern)

        minutes = t.hour * 60 + t.minute

        # 9:30 – 10:30
        if 570 <= minutes < 630:
            return "OPEN"

        # 10:30 – 1:30
        elif 630 <= minutes < 810:
            return "MIDDAY"

        # 1:30 – 3:00
        elif 810 <= minutes < 900:
            return "AFTERNOON"

        # 3:00 – 4:00
        elif 900 <= minutes < 960:
            return "POWER"

        else:
            return "UNKNOWN"

    except Exception:
        return "UNKNOWN"
```

#### `signals/setup_classifier.py`
```python
# signals/setup_classifier.py

from core.data_service import get_market_dataframe


def classify_trade(entry_price, direction):

    df = get_market_dataframe()

    if df is None or len(df) < 25:
        return "UNKNOWN"

    recent = df.tail(20)

    recent_high = recent["high"].max()
    recent_low = recent["low"].min()

    last_close = recent["close"].iloc[-1]
    first_close = recent["close"].iloc[0]

    trend_up = last_close > first_close

    # Breakout detection
    if direction == "bullish" and entry_price >= recent_high * 0.999:
        return "BREAKOUT"

    if direction == "bearish" and entry_price <= recent_low * 1.001:
        return "BREAKOUT"

    # Pullback detection
    if direction == "bullish" and trend_up:
        return "PULLBACK"

    if direction == "bearish" and not trend_up:
        return "PULLBACK"

    return "REVERSAL"
```

#### `signals/signal_evaluator.py`
```python
# signals/signal_evaluator.py

from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state


def grade_trade(direction):

    pred = make_prediction(60)
    regime = get_regime()
    vol = volatility_state()

    pred_direction = pred.get("direction") if isinstance(pred, dict) else None
    pred_conf = pred.get("confidence") if isinstance(pred, dict) else None

    if pred_direction is None or pred_conf is None:
        return {
            "grade": "N/A",
            "score": 0,
            "confidence": None,
            "model_direction": pred_direction,
            "regime": regime,
            "volatility": vol,
            "reasons": ["Prediction unavailable"],
        }

    score = 0
    reasons = []

    # Alignment with model
    if direction == pred_direction:
        score += 2
        reasons.append("Aligned with model prediction")
    else:
        reasons.append("Against model prediction")

    # Confidence weighting
    confidence = round(float(pred_conf) * 100, 1)

    if confidence >= 70:
        score += 1
        reasons.append("High statistical confidence")
    elif confidence < 55:
        reasons.append("Low statistical confidence")

    # Regime filter
    if regime == "TREND":
        score += 1
        reasons.append("Trending market conditions")
    elif regime == "RANGE":
        reasons.append("Range market")

    # Volatility filter
    if vol in ["NORMAL", "HIGH"]:
        score += 1
        reasons.append("Sufficient volatility")
    else:
        reasons.append("Low volatility")

    grade = (
        "A" if score >= 4
        else "B" if score == 3
        else "C" if score == 2
        else "D"
    )

    return {
        "grade": grade,
        "score": score,
        "confidence": confidence,
        "model_direction": pred_direction,
        "regime": regime,
        "volatility": vol,
        "reasons": reasons
    }
```

#### `signals/volatility.py`
```python
# signals/volatility.py
from core.data_service import get_market_dataframe


def get_intraday_volatility(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return 0

    recent = df.tail(30)

    high = recent["high"].max()
    low = recent["low"].min()

    return round(high - low, 3)


def volatility_state(df=None):

    vol = get_intraday_volatility(df)

    if vol < 0.35:
        return "DEAD"

    if vol < 0.75:
        return "LOW"

    if vol < 1.5:
        return "NORMAL"

    return "HIGH"
```

#### `simulation/sim_contract.py`
```python
import os
import pytz
import time
from collections import deque
from datetime import datetime, timedelta, date
from typing import Optional, Any
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest, OptionChainRequest
import alpaca.data.enums as alpaca_enums

from analytics.contract_logger import log_contract_attempt
from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

# Track chain/snapshot errors for hourly health reporting
_CHAIN_ERROR_EVENTS = deque()
_SNAPSHOT_ERROR_EVENTS = deque()
_LAST_CHAIN_ERROR = None
_LAST_SNAPSHOT_ERROR = None
_LAST_SUCCESS = None
_LAST_SNAPSHOT_PROBE = None
_IV_SERIES = deque(maxlen=int(os.getenv("SIM_IV_SERIES_MAX", "500")))


def _prune_events(q: deque, cutoff_ts: float) -> None:
    while q and q[0][0] < cutoff_ts:
        q.popleft()


def _record_error(kind: str, message: str) -> None:
    global _LAST_CHAIN_ERROR, _LAST_SNAPSHOT_ERROR
    now = time.time()
    if kind == "chain":
        _CHAIN_ERROR_EVENTS.append((now, message))
        _LAST_CHAIN_ERROR = (now, message)
    elif kind == "snapshot":
        _SNAPSHOT_ERROR_EVENTS.append((now, message))
        _LAST_SNAPSHOT_ERROR = (now, message)


def _record_success(symbol: str, spread_pct: float | None, contract_type_char: str | None) -> None:
    global _LAST_SUCCESS
    _LAST_SUCCESS = {
        "ts": time.time(),
        "symbol": symbol,
        "spread_pct": spread_pct,
        "contract_type": contract_type_char,
    }


def get_contract_error_stats(window_seconds: int = 3600) -> dict:
    now = time.time()
    cutoff = now - window_seconds
    _prune_events(_CHAIN_ERROR_EVENTS, cutoff)
    _prune_events(_SNAPSHOT_ERROR_EVENTS, cutoff)
    return {
        "chain_errors": len(_CHAIN_ERROR_EVENTS),
        "snapshot_errors": len(_SNAPSHOT_ERROR_EVENTS),
        "last_chain_error": _LAST_CHAIN_ERROR,
        "last_snapshot_error": _LAST_SNAPSHOT_ERROR,
        "last_success": _LAST_SUCCESS,
    }


def get_snapshot_probe() -> dict | None:
    return _LAST_SNAPSHOT_PROBE


def record_iv_sample(iv: float | None) -> None:
    try:
        if iv is None:
            return
        _IV_SERIES.append(float(iv))
    except Exception:
        return


def get_iv_series(window: int | None = None) -> list[float]:
    try:
        if window is None:
            return list(_IV_SERIES)
        if window <= 0:
            return []
        return list(_IV_SERIES)[-int(window):]
    except Exception:
        return []


def _record_snapshot_probe(response, symbols: list[str] | None = None) -> None:
    global _LAST_SNAPSHOT_PROBE
    try:
        probe = {
            "ts": time.time(),
            "response_type": type(response).__name__,
            "symbols": symbols[:5] if symbols else [],
            "size": None,
            "keys": [],
            "data_attr": None,
            "snapshots_attr": None,
        }
        if isinstance(response, dict):
            probe["size"] = len(response)
            probe["keys"] = list(response.keys())[:5]
        else:
            data = getattr(response, "data", None)
            snaps = getattr(response, "snapshots", None)
            probe["data_attr"] = type(data).__name__ if data is not None else None
            probe["snapshots_attr"] = type(snaps).__name__ if snaps is not None else None
            if isinstance(data, dict):
                probe["size"] = len(data)
                probe["keys"] = list(data.keys())[:5]
            elif isinstance(snaps, dict):
                probe["size"] = len(snaps)
                probe["keys"] = list(snaps.keys())[:5]
            elif isinstance(data, list):
                probe["size"] = len(data)
            elif isinstance(snaps, list):
                probe["size"] = len(snaps)
        _LAST_SNAPSHOT_PROBE = probe
    except Exception:
        _LAST_SNAPSHOT_PROBE = {"ts": time.time(), "response_type": "error"}


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_snapshot(response, symbol: str):
    if response is None or not symbol:
        return None
    if isinstance(response, dict):
        if symbol in response:
            return response.get(symbol)
        try:
            if len(response) == 1:
                return next(iter(response.values()))
        except Exception:
            pass
    for attr in ("snapshots", "data"):
        data = getattr(response, attr, None)
        if isinstance(data, dict):
            if symbol in data:
                return data.get(symbol)
            try:
                if len(data) == 1:
                    return next(iter(data.values()))
            except Exception:
                pass
        if isinstance(data, list):
            for item in data:
                sym = getattr(item, "symbol", None) if item is not None else None
                if sym is None and isinstance(item, dict):
                    sym = item.get("symbol")
                if sym == symbol:
                    return item
            try:
                if len(data) == 1:
                    return data[0]
            except Exception:
                pass
    try:
        if getattr(response, "symbol", None) == symbol:
            return response
    except Exception:
        pass
    return None


def _get_options_feed():
    feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
    if feed_enum is None:
        return None
    desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
    try:
        if desired == "opra":
            return feed_enum.OPRA
        if desired == "indicative":
            return feed_enum.INDICATIVE
    except Exception:
        return None
    return None


def _build_snapshot_request(symbols, feed_override=None):
    # Guard against string input (list("SPY...") -> ["S","P","Y",...])
    if isinstance(symbols, str):
        symbols = [symbols]
    elif symbols is None:
        symbols = []
    feed_val = feed_override if feed_override is not None else _get_options_feed()
    if feed_val is not None:
        try:
            return OptionSnapshotRequest(symbol_or_symbols=list(symbols), feed=feed_val)
        except TypeError:
            pass
    return OptionSnapshotRequest(symbol_or_symbols=list(symbols))


class _FallbackContractType:
    CALL = "CALL"
    PUT = "PUT"


ContractType = getattr(alpaca_enums, "ContractType", _FallbackContractType)


def select_sim_contract_with_reason(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None
) -> tuple[dict | None, str | None]:
    if direction not in {"BULLISH", "BEARISH"}:
        return None, "invalid_direction"
    if underlying_price is None or underlying_price <= 0:
        return None, "invalid_price"
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None, "missing_api_keys"
    if now_et is None:
        now_et = datetime.now(pytz.timezone("US/Eastern"))
    today = now_et.date()

    dte_min = int(profile["dte_min"])
    dte_max = int(profile["dte_max"])
    # Build candidate expiries using trading-day DTE (weekdays only).
    candidate_dates: list[tuple[date, int]] = []
    trading_dte = 0
    expiry_date = today
    while trading_dte <= dte_max:
        if expiry_date.weekday() < 5:
            dte = trading_dte
            if dte_min <= dte <= dte_max:
                if not (expiry_date == today and (now_et.hour, now_et.minute) >= (13, 30)):
                    candidate_dates.append((expiry_date, dte))
            trading_dte += 1
        expiry_date += timedelta(days=1)
    candidate_dates.sort(key=lambda x: x[1])
    if not candidate_dates:
        if dte_max == 0 and (now_et.hour, now_et.minute) >= (13, 30):
            return None, "cutoff_passed"
        return None, "no_candidate_expiry"

    otm_pct = float(profile["otm_pct"])
    contract_type_enum: Any = ContractType.CALL if direction == "BULLISH" else ContractType.PUT
    contract_type_char = "C" if direction == "BULLISH" else "P"
    # SPY options only have whole-number strikes — round to nearest integer
    # and probe ATM-2 through ATM+2 from the OTM-adjusted base.
    if direction == "BULLISH":
        base_strike = underlying_price * (1 + otm_pct)
        base_whole = round(base_strike)
        strike_retry = [
            base_whole,
            base_whole - 1,
            base_whole + 1,
            base_whole - 2,
            base_whole + 2,
        ]
    else:
        base_strike = underlying_price * (1 - otm_pct)
        base_whole = round(base_strike)
        strike_retry = [
            base_whole,
            base_whole + 1,
            base_whole - 1,
            base_whole + 2,
            base_whole - 2,
        ]

    def _build_occ(expiry_date, contract_type_char: str, strike: float) -> str:
        date_str = expiry_date.strftime("%y%m%d")
        strike_int = int(round(strike * 1000))
        return f"SPY{date_str}{contract_type_char}{strike_int:08d}"

    def _is_chain_empty(chain_obj) -> bool:
        if chain_obj is None:
            return True
        if isinstance(chain_obj, dict):
            return len(chain_obj) == 0
        data = getattr(chain_obj, "data", None)
        if data is not None:
            try:
                return len(data) == 0
            except Exception:
                pass
        chains = getattr(chain_obj, "chains", None)
        if chains is not None:
            try:
                return len(chains) == 0
            except Exception:
                pass
        df = getattr(chain_obj, "df", None)
        if df is not None:
            try:
                return df.empty
            except Exception:
                pass
        return False

    client = OptionHistoricalDataClient(api_key, secret_key)
    last_reason = "no_contract"

    for expiry_date, dte in candidate_dates:
        chain = None
        feed_val = None
        try:
            rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
            feed_val = _get_options_feed()
            chain = client.get_option_chain(
                OptionChainRequest(
                    underlying_symbol="SPY",
                    type=contract_type_enum,
                    feed=feed_val,
                    expiration_date=expiry_date
                )
            )
            try:
                sample_sym = None
                if isinstance(chain, dict) and chain:
                    sample_sym = next(iter(chain.keys()))
                else:
                    data = getattr(chain, "data", None)
                    if isinstance(data, dict) and data:
                        sample_sym = next(iter(data.keys()))
                    elif isinstance(data, list) and data:
                        sample_sym = getattr(data[0], "symbol", None) or (
                            data[0].get("symbol") if isinstance(data[0], dict) else None
                        )
                debug_log(
                    "sim_chain_loaded",
                    expiry=expiry_date.isoformat(),
                    direction=direction,
                    sample_symbol=sample_sym,
                    feed=str(feed_val) if feed_val is not None else "default",
                )
            except Exception:
                pass
        except Exception as e:
            last_reason = "chain_error"
            try:
                import logging
                logging.warning("sim_contract_chain_error: %s", e)
            except Exception:
                pass
            _record_error("chain", str(e))
            chain = None
        if chain is not None and _is_chain_empty(chain):
            last_reason = "empty_chain"
            continue

        strike_candidates = list(dict.fromkeys(strike_retry[:3]))
        chain_symbols_found = False
        symbol_candidates: list[tuple[str, float]] = []
        if chain is not None:
            try:
                def _iter_chain_symbols(chain_obj):
                    if isinstance(chain_obj, dict):
                        for sym in chain_obj.keys():
                            yield sym
                        return
                    data = getattr(chain_obj, "data", None)
                    if isinstance(data, dict):
                        for sym in data.keys():
                            yield sym
                        return
                    if isinstance(data, list):
                        for item in data:
                            sym = getattr(item, "symbol", None) if item is not None else None
                            if sym is None and isinstance(item, dict):
                                sym = item.get("symbol")
                            if sym:
                                yield sym
                        return
                    chains = getattr(chain_obj, "chains", None)
                    if isinstance(chains, dict):
                        for sym in chains.keys():
                            yield sym
                        return
                    if isinstance(chains, list):
                        for item in chains:
                            sym = getattr(item, "symbol", None) if item is not None else None
                            if sym is None and isinstance(item, dict):
                                sym = item.get("symbol")
                            if sym:
                                yield sym
                        return
                    df = getattr(chain_obj, "df", None)
                    if df is not None:
                        try:
                            if "symbol" in df.columns:
                                for sym in df["symbol"].dropna().tolist():
                                    yield sym
                            else:
                                for sym in df.index.tolist():
                                    yield sym
                        except Exception:
                            return

                for symbol in _iter_chain_symbols(chain):
                    chain_symbols_found = True
                    try:
                        if isinstance(symbol, str) and len(symbol) >= 15:
                            strike_part = symbol[-8:]
                            strike_val = int(strike_part) / 1000.0
                            symbol_candidates.append((symbol, strike_val))
                    except Exception:
                        continue
                # sort symbols by proximity to underlying
                symbol_candidates = sorted(symbol_candidates, key=lambda s: abs(s[1] - underlying_price))
                # extend strike candidates with a few closest strikes
                for _sym, s in symbol_candidates[:5]:
                    strike_candidates.append(int(round(s)))
            except Exception:
                pass

        strike_candidates = strike_candidates[:8]
        symbol_candidates = symbol_candidates[:8]
        if chain is not None and not chain_symbols_found:
            last_reason = "no_chain_symbols"

        reason_counts = {
            "no_snapshot": 0,
            "no_quote": 0,
            "invalid_quote": 0,
            "spread_too_wide": 0,
            "snapshot_error": 0,
        }
        symbol_loop = symbol_candidates if symbol_candidates else [(None, s) for s in strike_candidates]

        # Batch snapshot call when chain symbols are available
        if symbol_candidates:
            try:
                batch_symbols = [s for s, _ in symbol_candidates if s]
                if batch_symbols:
                    rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
                    debug_log(
                        "sim_snapshot_request",
                        symbols=batch_symbols[:5],
                        count=len(batch_symbols),
                        expiry=expiry_date.isoformat(),
                        direction=direction,
                    )
                    snapshots = client.get_option_snapshot(
                        _build_snapshot_request(batch_symbols, feed_override=feed_val)
                    )
                    _record_snapshot_probe(snapshots, batch_symbols)
                    try:
                        if isinstance(snapshots, dict) and len(snapshots) == 0:
                            _record_error("snapshot", "empty_snapshot_response")
                    except Exception:
                        pass
                    # If we got an empty response and no explicit feed, retry indicative
                    if isinstance(snapshots, dict) and len(snapshots) == 0 and feed_val is None:
                        try:
                            feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                            if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                                debug_log("sim_snapshot_retry_feed", feed="indicative", count=len(batch_symbols))
                                snapshots = client.get_option_snapshot(
                                    _build_snapshot_request(batch_symbols, feed_override=feed_enum.INDICATIVE)
                                )
                                _record_snapshot_probe(snapshots, batch_symbols)
                        except Exception as e:
                            _record_error("snapshot", f"indicative_retry_error: {str(e)}")
                    for symbol, strike in symbol_candidates:
                        snap = _extract_snapshot(snapshots, symbol)
                        if snap is None:
                            last_reason = "no_snapshot"
                            reason_counts["no_snapshot"] += 1
                            debug_log("sim_snapshot_missing", symbol=symbol, expiry=expiry_date.isoformat(), strike=strike)
                            try:
                                _record_error("snapshot", f"no_snapshot symbol={symbol}")
                            except Exception:
                                pass
                            continue
                        greeks = getattr(snap, "greeks", None)
                        iv    = _safe_float(getattr(greeks, "implied_volatility", None)) if greeks else None
                        delta = _safe_float(getattr(greeks, "delta", None)) if greeks else None
                        gamma = _safe_float(getattr(greeks, "gamma", None)) if greeks else None
                        theta = _safe_float(getattr(greeks, "theta", None)) if greeks else None
                        vega  = _safe_float(getattr(greeks, "vega", None)) if greeks else None

                        record_iv_sample(iv)
                        quote = getattr(snap, "latest_quote", None)
                        if quote is None:
                            last_reason = "no_quote"
                            reason_counts["no_quote"] += 1
                            continue

                        bid = float(quote.bid_price) if quote.bid_price is not None else 0.0
                        ask = float(quote.ask_price) if quote.ask_price is not None else 0.0
                        if bid <= 0 or ask <= 0:
                            last_reason = "invalid_quote"
                            reason_counts["invalid_quote"] += 1
                            continue

                        spread_pct = (ask - bid) / ask
                        mid = round((bid + ask) / 2, 4)
                        if spread_pct > float(profile["max_spread_pct"]):
                            last_reason = "spread_too_wide"
                            reason_counts["spread_too_wide"] += 1
                            continue

                        otm_pct_applied = abs(strike - underlying_price) / underlying_price
                        log_contract_attempt(
                            source=f"sim:{profile.get('sim_id', 'unknown')}",
                            direction=direction, underlying_price=underlying_price,
                            expiry=expiry_date, dte=dte, strike=strike,
                            result="selected", reason="selected",
                            bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                            iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                        )
                        _record_success(symbol, spread_pct, contract_type_char)
                        return {
                            "option_symbol": symbol,
                            "expiry": expiry_date.isoformat(),
                            "dte": dte,
                            "strike": strike,
                            "contract_type": contract_type_char,
                            "bid": bid,
                            "ask": ask,
                            "mid": mid,
                            "spread_pct": round(spread_pct, 4),
                            "underlying_price": underlying_price,
                            "otm_pct_applied": round(otm_pct_applied, 6),
                            "selection_method": "chain_symbols",
                            "iv": iv,
                            "delta": delta,
                            "gamma": gamma,
                            "theta": theta,
                            "vega": vega,
                        }, None
            except Exception as e:
                _record_error("snapshot", f"batch_snapshot_error: {str(e)}")

        for symbol_item in symbol_loop:
            strike = None
            try:
                if isinstance(symbol_item, tuple):
                    symbol = symbol_item[0]
                    strike = symbol_item[1]
                else:
                    symbol = None
                    strike = symbol_item
                if not symbol:
                    symbol = _build_occ(expiry_date, contract_type_char, strike)
                rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
                debug_log(
                    "sim_snapshot_request_single",
                    symbol=symbol,
                    expiry=expiry_date.isoformat(),
                    strike=strike,
                    direction=direction,
                )
                snapshots = client.get_option_snapshot(
                    _build_snapshot_request([symbol], feed_override=feed_val)
                )
                _record_snapshot_probe(snapshots, [symbol])
                if isinstance(snapshots, dict) and len(snapshots) == 0 and feed_val is None:
                    try:
                        feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                        if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                            debug_log("sim_snapshot_retry_feed", feed="indicative", symbol=symbol)
                            snapshots = client.get_option_snapshot(
                                _build_snapshot_request([symbol], feed_override=feed_enum.INDICATIVE)
                            )
                            _record_snapshot_probe(snapshots, [symbol])
                    except Exception as e:
                        _record_error("snapshot", f"indicative_retry_error: {str(e)}")
                snap = _extract_snapshot(snapshots, symbol)
                if snap is None:
                    last_reason = "no_snapshot"
                    reason_counts["no_snapshot"] += 1
                    try:
                        resp_type = type(snapshots).__name__
                        keys_hint = ""
                        if isinstance(snapshots, dict):
                            keys = list(snapshots.keys())[:5]
                            keys_hint = f" keys={keys}"
                        _record_error("snapshot", f"no_snapshot symbol={symbol} type={resp_type}{keys_hint}")
                    except Exception:
                        pass
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="no_snapshot",
                    )
                    continue

                # --- greeks capture ---
                greeks = getattr(snap, "greeks", None)
                iv    = _safe_float(getattr(greeks, "implied_volatility", None)) if greeks else None
                delta = _safe_float(getattr(greeks, "delta", None)) if greeks else None
                gamma = _safe_float(getattr(greeks, "gamma", None)) if greeks else None
                theta = _safe_float(getattr(greeks, "theta", None)) if greeks else None
                vega  = _safe_float(getattr(greeks, "vega", None)) if greeks else None

                record_iv_sample(iv)
                quote = getattr(snap, "latest_quote", None)
                if quote is None:
                    last_reason = "no_quote"
                    reason_counts["no_quote"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="no_quote",
                        iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                bid = float(quote.bid_price) if quote.bid_price is not None else 0.0
                ask = float(quote.ask_price) if quote.ask_price is not None else 0.0
                if bid <= 0 or ask <= 0:
                    last_reason = "invalid_quote"
                    reason_counts["invalid_quote"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="invalid_quote",
                        bid=bid, ask=ask, iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                spread_pct = (ask - bid) / ask
                mid = round((bid + ask) / 2, 4)
                if spread_pct > float(profile["max_spread_pct"]):
                    last_reason = "spread_too_wide"
                    reason_counts["spread_too_wide"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="spread_too_wide",
                        bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                        iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                otm_pct_applied = abs(strike - underlying_price) / underlying_price
                log_contract_attempt(
                    source=f"sim:{profile.get('sim_id', 'unknown')}",
                    direction=direction, underlying_price=underlying_price,
                    expiry=expiry_date, dte=dte, strike=strike,
                    result="selected", reason="selected",
                    bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                    iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                )
                _record_success(symbol, spread_pct, contract_type_char)
                return {
                    "option_symbol": symbol,
                    "expiry": expiry_date.isoformat(),
                    "dte": dte,
                    "strike": strike,
                    "contract_type": contract_type_char,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread_pct": round(spread_pct, 4),
                    "underlying_price": underlying_price,
                    "otm_pct_applied": round(otm_pct_applied, 6),
                    "selection_method": "otm_pct",
                    # greeks at entry — stored on the contract dict so caller
                    # can persist them on the trade record
                    "iv": iv,
                    "delta": delta,
                    "gamma": gamma,
                    "theta": theta,
                    "vega": vega,
                }, None
            except Exception as e:
                last_reason = "snapshot_error"
                reason_counts["snapshot_error"] += 1
                log_contract_attempt(
                    source=f"sim:{profile.get('sim_id', 'unknown')}",
                    direction=direction, underlying_price=underlying_price,
                    expiry=expiry_date, dte=dte, strike=strike,
                    result="error", reason="snapshot_error",
                )
                try:
                    import logging
                    logging.warning("sim_contract_snapshot_error: %s", e)
                except Exception:
                    pass
                _record_error("snapshot", str(e))
                continue

        # If we exhaust strikes without success, make the reason explicit
        total_attempts = sum(reason_counts.values())
        if total_attempts > 0:
            top_reason = max(reason_counts.items(), key=lambda x: x[1])[0]
            if reason_counts[top_reason] == total_attempts:
                last_reason = f"{top_reason}_all"
            else:
                last_reason = f"{top_reason}_most"
        if chain is not None and not chain_symbols_found and last_reason in {
            "no_snapshot_all",
            "no_snapshot_most",
            "no_quote_all",
            "no_quote_most",
            "invalid_quote_all",
            "invalid_quote_most",
            "spread_too_wide_all",
            "spread_too_wide_most",
            "snapshot_error_all",
            "snapshot_error_most",
        }:
            last_reason = f"{last_reason}_no_chain_symbols"

    return None, last_reason


def select_sim_contract(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None
) -> dict | None:
    contract, _reason = select_sim_contract_with_reason(
        direction,
        underlying_price,
        profile,
        now_et=now_et
    )
    return contract
```

#### `simulation/sim_engine.py`
```python
import os
import asyncio
import uuid
import yaml
import pytz
import math
import logging
import time as _time
from datetime import datetime, time
from simulation.sim_portfolio import SimPortfolio
from simulation.sim_executor import sim_try_fill, sim_compute_risk_dollars, sim_should_trade_now
from simulation.sim_contract import select_sim_contract, select_sim_contract_with_reason, get_iv_series
from simulation.sim_live_router import sim_live_router, manage_live_exit
from execution.option_executor import get_option_price
from simulation.sim_watcher import _get_time_of_day_bucket
from simulation.sim_signals import derive_sim_signal
from simulation.sim_ml import predict_sim_trade, record_sim_trade_close
from analytics.sim_features import compute_sim_features
from core.md_state import is_md_enabled
from signals.volatility import volatility_state


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


_PROFILES = _load_profiles()
_LAST_CHAIN_CALL_TS = 0.0
_CHAIN_CALL_MIN_INTERVAL = 1.0  # seconds between Alpaca snapshot calls
DAYTRADE_EOD_CUTOFF = time(15, 55)  # ET cutoff for day-trading sims to flatten
EXPIRY_EOD_CUTOFF = time(15, 55)    # ET cutoff for same-day expiries


async def run_sim_entries(
    df,
    regime: str | None = None
) -> list[dict]:
    global _LAST_CHAIN_CALL_TS
    results = []
    if not _PROFILES:
        return [{"sim_id": None, "status": "error", "reason": "no_profiles_loaded"}]
    time_of_day_bucket = _get_time_of_day_bucket()
    for sim_id, profile in _PROFILES.items():
        try:
            sim = SimPortfolio(sim_id, profile)
            sim.load()

            signal_mode = sim.profile.get("signal_mode", "TREND_PULLBACK")
            trade_count = len(sim.trade_log) if isinstance(sim.trade_log, list) else 0
            signal_meta = None
            direction = None
            underlying_price = None
            signal_meta = None
            entry_context = f"signal_mode={signal_mode} | regime={regime or 'N/A'} | bucket={time_of_day_bucket or 'N/A'}"
            if isinstance(signal_meta, dict) and signal_meta.get("entry_context"):
                entry_context = f"{entry_context} | {signal_meta.get('entry_context')}"
            if isinstance(signal_meta, dict) and signal_meta.get("reason"):
                entry_context = f"{entry_context} | reason={signal_meta.get('reason')}"
            effective_profile = dict(profile)
            if isinstance(signal_meta, dict):
                for k in [
                    "dte_min",
                    "dte_max",
                    "hold_min_seconds",
                    "hold_max_seconds",
                    "horizon",
                    "orb_minutes",
                    "zscore_window",
                ]:
                    if signal_meta.get(k) is not None:
                        effective_profile[k] = signal_meta.get(k)
            ml_context = {
                "direction": direction,
                "price": underlying_price,
                "regime": regime,
                "horizon": effective_profile.get("horizon"),
                "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            }
            ml_prediction = predict_sim_trade(df, ml_context)
            feature_snapshot = None
            if profile.get("features_enabled"):
                try:
                    feature_snapshot = compute_sim_features(
                        df,
                        {
                            "direction": direction,
                            "price": underlying_price,
                            "regime": regime,
                            "signal_mode": signal_mode,
                            "horizon": effective_profile.get("horizon", profile.get("horizon")),
                            "dte_min": effective_profile.get("dte_min"),
                            "dte_max": effective_profile.get("dte_max"),
                            "orb_minutes": effective_profile.get("orb_minutes", profile.get("orb_minutes", 15)),
                            "zscore_window": effective_profile.get("zscore_window", profile.get("zscore_window", 30)),
                            "iv_series": get_iv_series(profile.get("iv_series_window", 200)),
                        },
                    )
                except Exception:
                    feature_snapshot = None
            sig = derive_sim_signal(
                df,
                signal_mode,
                {
                    "trade_count": trade_count,
                    "atr_expansion_min": profile.get("atr_expansion_min"),
                    "vol_z_min": profile.get("vol_z_min"),
                    "require_trend_bias": profile.get("require_trend_bias"),
                },
                feature_snapshot=feature_snapshot,
            )
            if isinstance(sig, tuple):
                if len(sig) >= 2:
                    direction = sig[0]
                    underlying_price = sig[1]
                if len(sig) >= 3:
                    signal_meta = sig[2]
            if direction is None or underlying_price is None:
                if isinstance(signal_meta, dict) and signal_meta.get("reason"):
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": signal_meta.get("reason"),
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                continue
            regime_filter = sim.profile.get("regime_filter")
            if regime_filter is not None:
                if regime_filter == "TREND_ONLY" and regime != "TREND":
                    continue

            execution_mode = sim.profile.get("execution_mode")
            if execution_mode == "live":
                # Graduation gate: this sim activates only after a source sim
                # (e.g. SIM03) has logged enough closed paper trades.
                source_sim_id = sim.profile.get("source_sim")
                min_source_trades = 0
                try:
                    min_source_trades = int(sim.profile.get("min_source_trades", 0))
                except (TypeError, ValueError):
                    min_source_trades = 0
                if source_sim_id and min_source_trades > 0:
                    src_trade_count = 0
                    try:
                        src_profile = _PROFILES.get(source_sim_id, {})
                        src_sim = SimPortfolio(source_sim_id, src_profile)
                        src_sim.load()
                        src_trade_count = len(src_sim.trade_log) if isinstance(src_sim.trade_log, list) else 0
                    except Exception:
                        src_trade_count = 0
                    if src_trade_count < min_source_trades:
                        results.append({
                            "sim_id": sim_id,
                            "status": "skipped",
                            "reason": "insufficient_trade_history",
                            "trade_count": src_trade_count,
                            "min_trades_for_live": min_source_trades,
                            "entry_context": entry_context,
                            "signal_mode": signal_mode,
                        })
                        continue

                if not sim.profile.get("enabled"):
                    results.append({"sim_id": sim_id, "status": "skipped", "reason": "live_disabled"})
                    continue
                ok, reason = sim_should_trade_now(effective_profile)
                if not ok:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": reason,
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue
                ok, reason = sim.can_trade()
                if not ok:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": reason,
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue
                elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
                if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                    await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
                _LAST_CHAIN_CALL_TS = _time.monotonic()
                live_result = await sim_live_router(
                    sim_id=sim_id,
                    direction=direction,
                    price=underlying_price,
                    ml_prediction=ml_prediction,
                    regime=regime,
                    time_of_day_bucket=time_of_day_bucket,
                    signal_mode=signal_mode,
                    entry_context=entry_context,
                    feature_snapshot=feature_snapshot,
                )
                if not isinstance(live_result, dict) or live_result.get("status") != "success":
                    results.append({
                        "sim_id": sim_id,
                        "status": "error",
                        "reason": (live_result or {}).get("message", "live_order_failed")
                    })
                    continue
                results.append({
                    "sim_id": sim_id,
                    "status": "live_submitted",
                    "option_symbol": live_result.get("option_symbol"),
                    "qty": live_result.get("qty"),
                    "fill_price": live_result.get("fill_price"),
                    "entry_price": live_result.get("fill_price"),
                    "direction": direction,
                    "risk_dollars": live_result.get("risk_dollars"),
                    "strike": live_result.get("strike"),
                    "expiry": live_result.get("expiry"),
                    "dte": live_result.get("dte"),
                    "spread_pct": live_result.get("spread_pct"),
                    "regime": regime,
                    "time_bucket": time_of_day_bucket,
                    "mode": "LIVE",
                    "balance": live_result.get("balance_after"),
                    "entry_context": live_result.get("entry_context") or entry_context,
                    "signal_mode": live_result.get("signal_mode") or signal_mode,
                    "predicted_direction": live_result.get("predicted_direction") or ml_prediction.get("predicted_direction"),
                    "prediction_confidence": live_result.get("prediction_confidence") or ml_prediction.get("prediction_confidence"),
                    "edge_prob": live_result.get("edge_prob") or ml_prediction.get("edge_prob"),
                    "direction_prob": live_result.get("direction_prob") or ml_prediction.get("direction_prob"),
                })
                continue

            ok, reason = sim_should_trade_now(effective_profile)
            if not ok:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": reason,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            ok, reason = sim.can_trade()
            if not ok:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": reason,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
            if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
            _LAST_CHAIN_CALL_TS = _time.monotonic()

            contract, contract_reason = select_sim_contract_with_reason(direction, underlying_price, {**effective_profile, "sim_id": sim_id})
            if contract is None:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": contract_reason or "no_contract",
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            bid = contract["bid"]
            ask = contract["ask"]

            fill_result, err = sim_try_fill(
                contract["option_symbol"],
                qty=1,
                bid=bid,
                ask=ask,
                profile=profile,
                side="entry"
            )
            if err or fill_result is None:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": err,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            fill_price = fill_result["fill_price"]
            risk_dollars = sim_compute_risk_dollars(sim.balance, profile)
            qty = max(1, math.floor(risk_dollars / (fill_price * 100)))

            fill_result, err = sim_try_fill(
                contract["option_symbol"],
                qty=qty,
                bid=bid,
                ask=ask,
                profile=profile,
                side="entry"
            )
            if err or fill_result is None:
                results.append({"sim_id": sim_id, "status": "skipped", "reason": err})
                continue

            trade = {
                "trade_id": f"{sim_id}__{uuid.uuid4()}",
                "sim_id": sim_id,
                "option_symbol": contract["option_symbol"],
                "entry_price": fill_result["fill_price"],
                "qty": qty,
                "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                "horizon": effective_profile.get("horizon", profile.get("horizon")),
                "dte_bucket": str(contract["dte"]),
                "otm_pct": contract["otm_pct_applied"],
                "direction": direction,
                "strike": contract["strike"],
                "expiry": contract["expiry"],
                "contract_type": contract["contract_type"],
                "hold_min_seconds": int(effective_profile.get("hold_min_seconds", profile.get("hold_min_seconds"))),
                "hold_max_seconds": int(effective_profile.get("hold_max_seconds", profile.get("hold_max_seconds"))),
                "entry_price_source": fill_result.get("price_source", "mid_plus_slippage"),
            }
            if feature_snapshot:
                trade["feature_snapshot"] = feature_snapshot
            if sim_id == "SIM09" or str(signal_mode).upper() == "OPPORTUNITY":
                vol_state = volatility_state(df)
                vol_stop_map = {
                    "DEAD": 0.03,
                    "LOW": 0.04,
                    "NORMAL": 0.06,
                    "HIGH": 0.08,
                }
                dynamic_stop = vol_stop_map.get(str(vol_state).upper(), 0.06)
                trade["stop_loss_pct"] = min(0.10, max(0.01, float(dynamic_stop)))
            trade["regime_at_entry"] = regime
            trade["time_of_day_bucket"] = time_of_day_bucket
            trade["signal_mode"] = signal_mode
            trade["entry_context"] = entry_context
            # greeks at entry (from contract snapshot)
            trade["iv_at_entry"] = contract.get("iv")
            trade["delta_at_entry"] = contract.get("delta")
            trade["gamma_at_entry"] = contract.get("gamma")
            trade["theta_at_entry"] = contract.get("theta")
            trade["vega_at_entry"] = contract.get("vega")
            if isinstance(ml_prediction, dict):
                trade["predicted_direction"] = ml_prediction.get("predicted_direction")
                trade["prediction_confidence"] = ml_prediction.get("prediction_confidence")
                trade["direction_prob"] = ml_prediction.get("direction_prob")
                trade["edge_prob"] = ml_prediction.get("edge_prob")
                trade["regime"] = ml_prediction.get("regime")
                trade["volatility"] = ml_prediction.get("volatility")
                trade["conviction_score"] = ml_prediction.get("conviction_score")
                trade["impulse"] = ml_prediction.get("impulse")
                trade["follow_through"] = ml_prediction.get("follow_through")
                trade["setup"] = ml_prediction.get("setup")
                trade["style"] = ml_prediction.get("style")
                trade["confidence"] = ml_prediction.get("confidence")
                trade["ml_probability"] = ml_prediction.get("edge_prob")

            sim.record_open(trade)
            sim.save()

            results.append({
                "sim_id": sim_id,
                "status": "opened",
                "trade_id": trade["trade_id"],
                "fill_price": trade["entry_price"],
                "entry_price": trade["entry_price"],
                "qty": qty,
                "option_symbol": trade["option_symbol"],
                "expiry": contract["expiry"],
                "strike": contract["strike"],
                "dte": contract["dte"],
                "spread_pct": contract["spread_pct"],
                "direction": direction,
                "risk_dollars": risk_dollars,
                "regime": regime,
                "time_bucket": time_of_day_bucket,
                "mode": "SIM",
                "balance": sim.balance,
                "entry_context": entry_context,
                "signal_mode": signal_mode,
                "predicted_direction": trade.get("predicted_direction"),
                "prediction_confidence": trade.get("prediction_confidence"),
                "edge_prob": trade.get("edge_prob"),
                "direction_prob": trade.get("direction_prob"),
            })
        except Exception as e:
            logging.exception("run_sim_entries_error: %s", e)
            results.append({"sim_id": sim_id, "status": "error", "reason": str(e)})
    return results


async def run_sim_exits() -> list[dict]:
    global _LAST_CHAIN_CALL_TS
    results = []
    if not _PROFILES:
        return [{"sim_id": None, "status": "error", "reason": "no_profiles_loaded"}]
    eastern = pytz.timezone("US/Eastern")

    for sim_id, profile in _PROFILES.items():
        try:
            sim = SimPortfolio(sim_id, profile)
            sim.load()
        except Exception as e:
            logging.exception("run_sim_exits_load_error: %s", e)
            results.append({
                "sim_id": sim_id,
                "status": "error",
                "reason": str(e)
            })
            continue
        if profile.get("execution_mode") == "live" and profile.get("enabled"):
            for trade in list(sim.open_trades):
                try:
                    live_result = await manage_live_exit(sim, trade)
                    if live_result and live_result.get("status") == "success":
                        results.append({
                            "sim_id": sim_id,
                            "trade_id": trade.get("trade_id"),
                            "status": "closed",
                            "exit_price": live_result.get("exit_price"),
                        "exit_reason": live_result.get("exit_reason"),
                        "exit_context": live_result.get("exit_context"),
                        "option_symbol": live_result.get("option_symbol"),
                            "strike": live_result.get("strike") or trade.get("strike"),
                            "expiry": live_result.get("expiry") or trade.get("expiry"),
                            "direction": live_result.get("direction") or trade.get("direction"),
                            "qty": live_result.get("qty"),
                            "entry_price": live_result.get("entry_price"),
                            "pnl": live_result.get("pnl"),
                            "mode": "LIVE",
                            "balance_after": live_result.get("balance_after"),
                            "time_in_trade_seconds": live_result.get("time_in_trade_seconds"),
                            "predicted_direction": live_result.get("predicted_direction"),
                            "prediction_confidence": live_result.get("prediction_confidence"),
                            "edge_prob": live_result.get("edge_prob"),
                        "direction_prob": live_result.get("direction_prob"),
                    })
                except Exception as e:
                    logging.exception("run_sim_exits_live_error: %s", e)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade.get("trade_id"),
                        "status": "error",
                        "reason": str(e)
                    })
            continue
        for trade in list(sim.open_trades):
            try:
                now_et = datetime.now(eastern)
                entry_time_et = datetime.fromisoformat(trade["entry_time"])
                if entry_time_et.tzinfo is None:
                    entry_time_et = eastern.localize(entry_time_et)
                elapsed_seconds = (now_et - entry_time_et).total_seconds()
                elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
                if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                    await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
                _LAST_CHAIN_CALL_TS = _time.monotonic()
                current_price = get_option_price(trade["option_symbol"])
                if current_price is None:
                    logging.warning("sim_exit_missing_quote: %s", trade["trade_id"])
                    continue
                sim.update_open_trade_excursion(trade["trade_id"], current_price)
                should_exit = False
                exit_reason = None
                exit_context = None
                spread_guard_bypass = False

                # Force exit for same-day expiry (all sims) before market close
                expiry_date = None
                try:
                    expiry_raw = trade.get("expiry")
                    if isinstance(expiry_raw, str):
                        expiry_date = datetime.fromisoformat(expiry_raw).date()
                except Exception:
                    expiry_date = None
                if expiry_date == now_et.date() and now_et.time() >= EXPIRY_EOD_CUTOFF:
                    should_exit = True
                    exit_reason = "expiry_close"
                    spread_guard_bypass = True
                    expiry_text = expiry_date.isoformat() if expiry_date else "unknown"
                    exit_context = f"expiry={expiry_text} cutoff={EXPIRY_EOD_CUTOFF.strftime('%H:%M')}"

                # Force exit for day-trading sims before market close
                is_daytrade = int(profile.get("dte_max", 0)) == 0
                if not should_exit and is_daytrade and now_et.time() >= DAYTRADE_EOD_CUTOFF:
                    should_exit = True
                    exit_reason = "eod_daytrade_close"
                    spread_guard_bypass = True
                    exit_context = f"daytrade_cutoff={DAYTRADE_EOD_CUTOFF.strftime('%H:%M')}"
                elif elapsed_seconds < trade["hold_min_seconds"]:
                    continue

                def _trade_grade(tr):
                    candidates = []
                    for key in ("edge_prob", "prediction_confidence", "confidence", "ml_probability"):
                        val = tr.get(key)
                        if isinstance(val, (int, float)):
                            candidates.append(float(val))
                    return max(candidates) if candidates else None

                stop_loss_pct = trade.get("stop_loss_pct", profile.get("stop_loss_pct"))
                if stop_loss_pct is not None and is_md_enabled() and sim_id != "SIM09":
                    try:
                        stop_loss_pct = max(float(stop_loss_pct) * 0.7, 0.05)
                    except (TypeError, ValueError):
                        pass
                if stop_loss_pct is not None:
                    try:
                        entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            loss_pct = (current_price - entry_price) / entry_price
                            if loss_pct <= -abs(float(stop_loss_pct)):
                                should_exit = True
                                exit_reason = "stop_loss"
                                spread_guard_bypass = True
                                exit_context = f"loss_pct={loss_pct:.3%} <= -{abs(float(stop_loss_pct)):.3%}"
                    except (TypeError, ValueError):
                        pass

                profit_target_pct = profile.get("profit_target_pct")
                entry_price = None
                gain_pct = None
                try:
                    entry_price = float(trade.get("entry_price", 0))
                    if entry_price > 0:
                        gain_pct = (current_price - entry_price) / entry_price
                except (TypeError, ValueError):
                    gain_pct = None

                # Near-TP adaptive lock + TP2 (optional)
                if not should_exit and profit_target_pct is not None and gain_pct is not None:
                    try:
                        base_target = abs(float(profit_target_pct))
                        if base_target > 0 and profile.get("tp2_enabled", True):
                            near_ratio = float(profile.get("near_tp_trigger_ratio", 0.85))
                            grade_min = float(profile.get("near_tp_grade_min", 0.6))
                            tp2_mult = float(profile.get("tp2_multiplier", 1.3))
                            grade = _trade_grade(trade)
                            if grade is not None and grade >= grade_min and gain_pct >= base_target * near_ratio:
                                if not trade.get("tp2_activated"):
                                    lock_pct = trade.get("lock_profit_pct")
                                    if lock_pct is None:
                                        # Lock ~50% of the base target, minimum 5%
                                        lock_pct = max(0.05, min(base_target * 0.5, base_target - 0.02))
                                    trade["lock_profit_pct"] = float(lock_pct)
                                    trade["tp2_target_pct"] = float(base_target * tp2_mult)
                                    trade["tp2_activated"] = True
                                    sim.save()
                    except (TypeError, ValueError):
                        pass

                # Profit lock: exit if retrace below locked profit level after TP2 activation
                lock_pct = trade.get("lock_profit_pct")
                if not should_exit and gain_pct is not None and trade.get("tp2_activated") and isinstance(lock_pct, (int, float)):
                    try:
                        if gain_pct <= float(lock_pct):
                            should_exit = True
                            exit_reason = "profit_lock"
                            exit_context = f"gain_pct={gain_pct:.3%} <= lock_pct={float(lock_pct):.3%}"
                    except (TypeError, ValueError):
                        pass

                effective_target = profit_target_pct
                if trade.get("tp2_activated") and trade.get("tp2_target_pct") is not None:
                    effective_target = trade.get("tp2_target_pct")

                if not should_exit and effective_target is not None:
                    try:
                        if entry_price is None:
                            entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            if gain_pct is None:
                                gain_pct = (current_price - entry_price) / entry_price
                            target_val = abs(float(effective_target))
                            if gain_pct >= target_val:
                                should_exit = True
                                exit_reason = "profit_target_2" if trade.get("tp2_activated") else "profit_target"
                                exit_context = f"gain_pct={gain_pct:.3%} >= {target_val:.3%}"
                    except (TypeError, ValueError):
                        pass

                trailing_activate = profile.get("trailing_stop_activate_pct")
                trailing_trail = profile.get("trailing_stop_trail_pct")
                if not should_exit and trailing_activate is not None and trailing_trail is not None:
                    try:
                        entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            gain_pct = (current_price - entry_price) / entry_price
                            trailing_activate_f = abs(float(trailing_activate))
                            trailing_trail_f = abs(float(trailing_trail))
                            if not trade.get("trailing_stop_activated", False):
                                if gain_pct >= trailing_activate_f:
                                    trade["trailing_stop_activated"] = True
                                    trade["trailing_stop_high"] = current_price
                                    sim.save()
                            else:
                                if current_price > trade.get("trailing_stop_high", 0):
                                    trade["trailing_stop_high"] = current_price
                                    sim.save()
                                trail_high = float(trade.get("trailing_stop_high", 0))
                                if trail_high > 0:
                                    drop_from_high = (current_price - trail_high) / trail_high
                                    if drop_from_high <= -trailing_trail_f:
                                        should_exit = True
                                        exit_reason = "trailing_stop"
                                        exit_context = f"drop_from_high={drop_from_high:.3%} <= -{trailing_trail_f:.3%} (high={trail_high:.4f})"
                    except (TypeError, ValueError):
                        pass

                if should_exit and exit_reason:
                    bid = current_price * 0.99
                    ask = current_price * 1.01
                    fill_result, err = sim_try_fill(
                        trade["option_symbol"],
                        qty=trade["qty"],
                        bid=bid,
                        ask=ask,
                        profile=profile,
                        side="exit"
                    )
                    if err and not spread_guard_bypass:
                        continue
                    if err and spread_guard_bypass:
                        fill_result = None
                    exit_price = fill_result["fill_price"] if fill_result else current_price
                    exit_price_source = (
                        fill_result.get("price_source", "mid_minus_slippage")
                        if fill_result
                        else "market_raw_price"
                    )
                    exit_data = {
                        "exit_price": exit_price,
                        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                        "exit_reason": exit_reason,
                        "exit_context": exit_context,
                        "exit_price_source": exit_price_source,
                        "exit_quote_model": "market_raw_price" if (spread_guard_bypass and fill_result is None) else "synthetic_1pct",
                        "entry_price_source": trade.get("entry_price_source", "mid_plus_slippage"),
                        "time_in_trade_seconds": int(elapsed_seconds),
                        "spread_guard_bypassed": spread_guard_bypass and fill_result is None,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "regime_at_entry": trade.get("regime_at_entry"),
                        "time_of_day_bucket": trade.get("time_of_day_bucket"),
                    }
                    sim.record_close(trade["trade_id"], exit_data)
                    sim.save()
                    entry_price = trade.get("entry_price")
                    qty_val = trade.get("qty")
                    pnl_val = None
                    try:
                        entry_price_f = float(entry_price)
                        qty_f = float(qty_val)
                        pnl_val = (exit_price - entry_price_f) * qty_f * 100
                    except (TypeError, ValueError):
                        pnl_val = None
                    record_sim_trade_close(trade, pnl_val)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade["trade_id"],
                        "status": "closed",
                        "exit_price": exit_price,
                        "exit_reason": exit_data["exit_reason"],
                        "exit_context": exit_data.get("exit_context"),
                        "option_symbol": trade.get("option_symbol"),
                        "strike": trade.get("strike"),
                        "expiry": trade.get("expiry"),
                        "direction": trade.get("direction"),
                        "qty": qty_val,
                        "entry_price": entry_price,
                        "pnl": pnl_val,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "feature_snapshot": trade.get("feature_snapshot"),
                        "mode": "SIM",
                        "balance_after": sim.balance,
                        "time_in_trade_seconds": exit_data.get("time_in_trade_seconds"),
                        "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
                        "prediction_confidence": trade.get("prediction_confidence"),
                        "edge_prob": trade.get("edge_prob"),
                        "direction_prob": trade.get("direction_prob"),
                    })
                    continue
                # Hold-max force exit (final fallback only)
                if elapsed_seconds >= trade["hold_max_seconds"]:
                    bid = current_price * 0.99
                    ask = current_price * 1.01
                    fill_result, err = sim_try_fill(
                        trade["option_symbol"],
                        qty=trade["qty"],
                        bid=bid,
                        ask=ask,
                        profile=profile,
                        side="exit"
                    )
                    if err:
                        fill_result = None
                    exit_price = fill_result["fill_price"] if fill_result else current_price
                    exit_price_source = (
                        fill_result.get("price_source", "mid_minus_slippage")
                        if fill_result
                        else "market_raw_price"
                    )
                    exit_context = f"elapsed={int(elapsed_seconds)}s >= hold_max={int(trade['hold_max_seconds'])}s"
                    exit_data = {
                        "exit_price": exit_price,
                        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                        "exit_reason": "hold_max_elapsed",
                        "exit_context": exit_context,
                        "exit_price_source": exit_price_source,
                        "exit_quote_model": "market_raw_price" if fill_result is None else "synthetic_1pct",
                        "entry_price_source": trade.get("entry_price_source", "mid_plus_slippage"),
                        "time_in_trade_seconds": int(elapsed_seconds),
                        "spread_guard_bypassed": fill_result is None,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "regime_at_entry": trade.get("regime_at_entry"),
                        "time_of_day_bucket": trade.get("time_of_day_bucket"),
                    }
                    sim.record_close(trade["trade_id"], exit_data)
                    sim.save()
                    entry_price = trade.get("entry_price")
                    qty_val = trade.get("qty")
                    pnl_val = None
                    try:
                        entry_price_f = float(entry_price)
                        qty_f = float(qty_val)
                        pnl_val = (exit_price - entry_price_f) * qty_f * 100
                    except (TypeError, ValueError):
                        pnl_val = None
                    record_sim_trade_close(trade, pnl_val)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade["trade_id"],
                        "status": "closed",
                        "exit_price": exit_price,
                        "exit_reason": "hold_max_elapsed",
                        "exit_context": exit_context,
                        "option_symbol": trade.get("option_symbol"),
                        "strike": trade.get("strike"),
                        "expiry": trade.get("expiry"),
                        "direction": trade.get("direction"),
                        "qty": qty_val,
                        "entry_price": entry_price,
                        "pnl": pnl_val,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "feature_snapshot": trade.get("feature_snapshot"),
                        "mode": "SIM",
                        "balance_after": sim.balance,
                        "time_in_trade_seconds": exit_data.get("time_in_trade_seconds"),
                        "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
                        "prediction_confidence": trade.get("prediction_confidence"),
                        "edge_prob": trade.get("edge_prob"),
                        "direction_prob": trade.get("direction_prob"),
                    })
            except Exception as e:
                logging.exception("run_sim_exits_error: %s", e)
                results.append({
                    "sim_id": sim_id,
                    "trade_id": trade.get("trade_id"),
                    "status": "error",
                    "reason": str(e)
                })
    return results
```

#### `simulation/sim_evaluation.py`
```python
import os
import json
import csv
import argparse
import logging
from datetime import datetime, timedelta
import pytz


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIM_DIR = os.path.join(BASE_DIR, "data", "sims")

REGIME_BUCKETS = {"TREND", "RANGE", "VOLATILE"}
TIME_BUCKETS = {"MORNING", "MIDDAY", "AFTERNOON", "CLOSE"}


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            eastern = pytz.timezone("US/Eastern")
            dt = eastern.localize(dt)
        return dt
    except Exception:
        return None


def _init_group():
    return {
        "total": 0,
        "wins": 0,
        "pnl_sum": 0.0,
        "pnl_vals": [],
        "cum_pnl": 0.0,
        "peak": 0.0,
        "max_dd": 0.0,
    }


def _update_group(group, pnl):
    group["total"] += 1
    group["pnl_sum"] += pnl
    group["pnl_vals"].append(pnl)
    if pnl > 0:
        group["wins"] += 1
    group["cum_pnl"] += pnl
    if group["cum_pnl"] > group["peak"]:
        group["peak"] = group["cum_pnl"]
    dd = group["peak"] - group["cum_pnl"]
    if dd > group["max_dd"]:
        group["max_dd"] = dd


def _compute_stats(group):
    total = group["total"]
    wins = group["wins"]
    win_rate = wins / total if total > 0 else 0.0
    pnl_sum = group["pnl_sum"]
    expectancy = pnl_sum / total if total > 0 else 0.0
    pos = [p for p in group["pnl_vals"] if p > 0]
    neg = [p for p in group["pnl_vals"] if p < 0]
    avg_win = sum(pos) / len(pos) if pos else 0.0
    avg_loss = sum(neg) / len(neg) if neg else 0.0
    return {
        "total": total,
        "win_rate": win_rate,
        "pnl_sum": pnl_sum,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "max_dd": group["max_dd"],
    }


def _format_pct(val):
    return f"{val * 100:.1f}%"


def _format_money(val):
    return f"${val:,.2f}"


def _load_sim_files():
    if not os.path.exists(SIM_DIR):
        return []
    sims = []
    for name in sorted(os.listdir(SIM_DIR)):
        if not name.endswith(".json"):
            continue
        sim_id = os.path.splitext(name)[0]
        sims.append((sim_id, os.path.join(SIM_DIR, name)))
    return sims


def _filter_trade_by_date(trade, start_dt, end_dt):
    ts = trade.get("exit_time") or trade.get("entry_time")
    dt = _parse_iso(ts)
    if dt is None:
        return False
    if start_dt and dt < start_dt:
        return False
    if end_dt and dt > end_dt:
        return False
    return True


def evaluate_sims(start_dt=None, end_dt=None, csv_path=None, plot=False):
    sims = _load_sim_files()
    if not sims:
        print("No sim data files found.")
        return

    regime_stats = {}
    time_stats = {}
    sim_stats = {}
    trades_skipped = 0

    for sim_id, path in sims:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            print(f"Failed to load {sim_id}.json")
            continue

        trade_log = data.get("trade_log", [])
        if not trade_log:
            print(f"No data recorded for {sim_id}")
            continue

        sim_group = _init_group()

        for trade in trade_log:
            if not isinstance(trade, dict):
                continue
            if not _filter_trade_by_date(trade, start_dt, end_dt):
                continue

            regime = trade.get("regime_at_entry")
            bucket = trade.get("time_of_day_bucket")
            if not regime or not bucket:
                logging.warning("sim_trade_missing_meta: %s", sim_id)
                trades_skipped += 1
                continue
            if regime not in REGIME_BUCKETS:
                logging.warning("sim_trade_bad_regime: %s", regime)
                trades_skipped += 1
                continue
            if bucket not in TIME_BUCKETS:
                logging.warning("sim_trade_bad_bucket: %s", bucket)
                trades_skipped += 1
                continue

            pnl_val = trade.get("realized_pnl_dollars")
            try:
                pnl = float(pnl_val) if pnl_val is not None else None
            except (TypeError, ValueError):
                pnl = None
            if pnl is None:
                trades_skipped += 1
                continue

            sim_group_key = sim_id
            if sim_group_key not in sim_stats:
                sim_stats[sim_group_key] = _init_group()
            _update_group(sim_stats[sim_group_key], pnl)

            if regime not in regime_stats:
                regime_stats[regime] = _init_group()
            _update_group(regime_stats[regime], pnl)

            if bucket not in time_stats:
                time_stats[bucket] = _init_group()
            _update_group(time_stats[bucket], pnl)

    print("\n=== Regime Breakdown ===")
    for regime in sorted(regime_stats.keys()):
        stats = _compute_stats(regime_stats[regime])
        print(
            f"{regime:<8} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | "
            f"Avg Win: {_format_money(stats['avg_win'])} | Avg Loss: {_format_money(stats['avg_loss'])} | "
            f"Exp: {_format_money(stats['expectancy'])} | MaxDD: {_format_money(stats['max_dd'])}"
        )

    print("\n=== Time Bucket Breakdown ===")
    for bucket in ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE"]:
        if bucket not in time_stats:
            continue
        stats = _compute_stats(time_stats[bucket])
        print(
            f"{bucket:<9} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | "
            f"Avg Win: {_format_money(stats['avg_win'])} | Avg Loss: {_format_money(stats['avg_loss'])} | "
            f"Exp: {_format_money(stats['expectancy'])} | MaxDD: {_format_money(stats['max_dd'])}"
        )

    print("\n=== Per-Sim Summary ===")
    for sim_id in sorted(sim_stats.keys()):
        stats = _compute_stats(sim_stats[sim_id])
        print(
            f"{sim_id:<5} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | MaxDD: {_format_money(stats['max_dd'])} | "
            f"Exp: {_format_money(stats['expectancy'])}"
        )

    if trades_skipped:
        print(f"\nSkipped trades (missing/invalid metadata or PnL): {trades_skipped}")

    if csv_path:
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["category_type", "category", "trades", "win_rate", "pnl", "avg_win", "avg_loss", "expectancy", "max_drawdown"])
                for regime, group in regime_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "regime", regime, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
                for bucket, group in time_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "time_bucket", bucket, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
                for sim_id, group in sim_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "sim", sim_id, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
            print(f"\nCSV exported to {csv_path}")
        except Exception:
            logging.exception("sim_evaluation_csv_export_failed")

    if plot:
        try:
            import matplotlib.pyplot as plt
            regimes = sorted(regime_stats.keys())
            regime_pnls = [_compute_stats(regime_stats[r])["pnl_sum"] for r in regimes]
            buckets = [b for b in ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE"] if b in time_stats]
            bucket_pnls = [_compute_stats(time_stats[b])["pnl_sum"] for b in buckets]

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            axes[0].bar(regimes, regime_pnls)
            axes[0].set_title("PnL by Regime")
            axes[0].set_ylabel("PnL ($)")
            axes[1].bar(buckets, bucket_pnls)
            axes[1].set_title("PnL by Time Bucket")
            axes[1].set_ylabel("PnL ($)")
            plt.tight_layout()
            plt.show()
        except Exception:
            logging.exception("sim_evaluation_plot_failed")


def main():
    parser = argparse.ArgumentParser(description="Sim performance evaluation by regime and time bucket.")
    parser.add_argument("--since-days", type=int, default=None)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--csv", type=str, default=None)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()

    eastern = pytz.timezone("US/Eastern")
    start_dt = None
    end_dt = None
    if args.since_days is not None:
        end_dt = datetime.now(eastern)
        start_dt = end_dt - timedelta(days=int(args.since_days))
    if args.start_date:
        try:
            start_dt = eastern.localize(datetime.fromisoformat(args.start_date))
        except Exception:
            start_dt = None
    if args.end_date:
        try:
            end_dt = eastern.localize(datetime.fromisoformat(args.end_date))
        except Exception:
            end_dt = None

    evaluate_sims(start_dt=start_dt, end_dt=end_dt, csv_path=args.csv, plot=args.plot)


if __name__ == "__main__":
    main()
```

#### `simulation/sim_executor.py`
```python
from datetime import datetime
import pytz
from typing import Dict, Tuple


def sim_try_fill(
    option_symbol: str,
    qty: int,
    bid: float,
    ask: float,
    profile: dict,
    side: str = "entry"
) -> tuple[dict | None, str | None]:
    if side not in {"entry", "exit"}:
        return None, "invalid_side"
    if bid <= 0 or ask <= 0 or ask < bid:
        return None, "invalid_quote"
    spread_pct = (ask - bid) / ask
    if spread_pct > profile["max_spread_pct"]:
        return None, "spread_too_wide"
    mid = (bid + ask) / 2
    slippage = float(profile.get("entry_slippage" if side == "entry" else "exit_slippage", 0.01))
    if side == "entry":
        fill_price = mid * (1 + slippage)
        price_source = "mid_plus_slippage"
    else:
        fill_price = mid * (1 - slippage)
        price_source = "mid_minus_slippage"
    return {
        "fill_price": round(fill_price, 4),
        "filled_qty": int(qty),
        "mid": round(mid, 4),
        "spread_pct": round(spread_pct, 4),
        "slippage_applied": slippage,
        "side": side,
        "price_source": price_source
    }, None


def sim_compute_risk_dollars(balance: float, profile: dict) -> float:
    risk = balance * float(profile["risk_per_trade_pct"])
    return max(risk, 50.0)


def sim_should_trade_now(profile: dict) -> tuple[bool, str]:
    now_et = datetime.now(pytz.timezone("US/Eastern"))
    if int(profile["dte_max"]) == 0:
        cutoff_0dte = now_et.replace(hour=13, minute=30, second=0, microsecond=0)
        if now_et >= cutoff_0dte:
            return False, "0dte_cutoff"
    try:
        h, m = map(int, profile["cutoff_time_et"].split(":"))
        cutoff = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
        if now_et >= cutoff:
            return False, "past_cutoff"
    except Exception:
        return False, "invalid_cutoff"
    return True, ""
```

#### `simulation/sim_live_router.py`
```python
import os
import asyncio
import yaml
import math
import logging
import pytz
from datetime import datetime

from simulation.sim_portfolio import SimPortfolio
from simulation.sim_contract import select_sim_contract, select_sim_contract_with_reason
from simulation.sim_ml import record_sim_trade_close
from execution.option_executor import execute_option_entry, close_option_position, get_option_price


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


_PROFILES = _load_profiles()


def _now_et_iso() -> str:
    return datetime.now(pytz.timezone("US/Eastern")).isoformat()


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


async def sim_live_router(sim_id, direction, price, ml_prediction=None, regime=None, time_of_day_bucket=None, signal_mode=None, entry_context=None, feature_snapshot=None):
    try:
        if direction not in {"BULLISH", "BEARISH"}:
            return {"status": "error", "message": "invalid_direction"}
        try:
            price_val = float(price)
        except (TypeError, ValueError):
            return {"status": "error", "message": "invalid_price"}
        if price_val <= 0:
            return {"status": "error", "message": "invalid_price"}

        profiles = _PROFILES or _load_profiles()
        live_active = [
            key
            for key, prof in profiles.items()
            if isinstance(prof, dict)
            and prof.get("execution_mode") == "live"
            and prof.get("enabled")
        ]
        if len(live_active) > 1:
            logging.error("sim_live_router_multi_live_enabled: %s", live_active)
            return {"status": "error", "message": "multiple_live_sims"}

        profile = profiles.get(sim_id)
        if not isinstance(profile, dict):
            return {"status": "error", "message": "invalid_sim_id"}
        if profile.get("execution_mode") != "live":
            return {"status": "error", "message": "sim_not_live"}
        if not profile.get("enabled"):
            return {"status": "error", "message": "sim_disabled"}

        sim = SimPortfolio(sim_id, profile)
        sim.load()

        daily_loss_limit = profile.get("daily_loss_limit")
        if daily_loss_limit is not None:
            try:
                if float(sim.daily_loss) >= float(daily_loss_limit):
                    return {"status": "error", "message": "daily_loss_limit"}
            except (TypeError, ValueError):
                pass

        max_open_trades = profile.get("max_open_trades")
        if max_open_trades is not None:
            try:
                if int(max_open_trades) > 0 and len(sim.open_trades) >= int(max_open_trades):
                    logging.info("sim_live_router_blocked_max_open_trades: %s", sim_id)
                    return {"status": "error", "message": "max_open_trades"}
            except (TypeError, ValueError):
                pass

        capital_limit = profile.get("capital_limit_dollars", 25000)
        capital_limit_val = _safe_float(capital_limit)
        if capital_limit_val is None or capital_limit_val <= 0:
            return {"status": "error", "message": "invalid_capital_limit"}

        contract, contract_reason = select_sim_contract_with_reason(direction, price_val, profile)
        if contract is None:
            return {"status": "error", "message": contract_reason or "no_contract"}

        mid = contract.get("mid")
        mid_val = _safe_float(mid)
        if mid_val is None or mid_val <= 0:
            return {"status": "error", "message": "invalid_mid"}

        risk_pct = profile.get("risk_per_trade_pct")
        risk_pct_val = _safe_float(risk_pct)
        if risk_pct_val is None:
            return {"status": "error", "message": "invalid_risk_pct"}

        effective_balance = min(sim.balance, capital_limit_val)
        risk_dollars = effective_balance * risk_pct_val
        if risk_dollars < 50.0:
            risk_dollars = 50.0

        qty = max(1, math.floor(risk_dollars / (mid_val * 100)))

        open_exposure = 0.0
        for t in sim.open_trades:
            if isinstance(t, dict):
                try:
                    entry_price = float(t.get("entry_price", 0.0))
                    qty_val = float(t.get("qty", 0.0))
                    open_exposure += entry_price * qty_val * 100
                except (TypeError, ValueError):
                    pass
        est_exposure = mid_val * qty * 100
        if open_exposure + est_exposure > capital_limit_val:
            return {"status": "error", "message": "capital_limit_reached"}

        fill_result, block = await execute_option_entry(
            contract["option_symbol"],
            qty,
            contract["bid"],
            contract["ask"],
        )
        if fill_result is None:
            return {"status": "error", "message": block or "order_not_filled"}

        trade = {
            "trade_id": f"{sim_id}__{os.urandom(8).hex()}",
            "sim_id": sim_id,
            "option_symbol": contract["option_symbol"],
            "entry_price": fill_result.get("fill_price"),
            "qty": qty,
            "entry_time": _now_et_iso(),
            "horizon": profile.get("horizon"),
            "dte_bucket": str(contract.get("dte")),
            "otm_pct": contract.get("otm_pct_applied"),
            "direction": direction,
            "strike": contract.get("strike"),
            "expiry": contract.get("expiry"),
            "contract_type": contract.get("contract_type"),
            "hold_min_seconds": int(profile.get("hold_min_seconds", 0)),
            "hold_max_seconds": int(profile.get("hold_max_seconds", 0)),
            "entry_price_source": "live_fill",
            "execution_mode": "live",
        }
        if feature_snapshot:
            trade["feature_snapshot"] = feature_snapshot
        trade["regime_at_entry"] = regime
        trade["time_of_day_bucket"] = time_of_day_bucket
        trade["signal_mode"] = signal_mode
        trade["entry_context"] = entry_context
        # greeks at entry (from contract snapshot)
        trade["iv_at_entry"] = contract.get("iv")
        trade["delta_at_entry"] = contract.get("delta")
        trade["gamma_at_entry"] = contract.get("gamma")
        trade["theta_at_entry"] = contract.get("theta")
        trade["vega_at_entry"] = contract.get("vega")
        if isinstance(ml_prediction, dict):
            trade["predicted_direction"] = ml_prediction.get("predicted_direction")
            trade["prediction_confidence"] = ml_prediction.get("prediction_confidence")
            trade["direction_prob"] = ml_prediction.get("direction_prob")
            trade["edge_prob"] = ml_prediction.get("edge_prob")
            trade["regime"] = ml_prediction.get("regime")
            trade["volatility"] = ml_prediction.get("volatility")
            trade["conviction_score"] = ml_prediction.get("conviction_score")
            trade["impulse"] = ml_prediction.get("impulse")
            trade["follow_through"] = ml_prediction.get("follow_through")
            trade["setup"] = ml_prediction.get("setup")
            trade["style"] = ml_prediction.get("style")
            trade["confidence"] = ml_prediction.get("confidence")
            trade["ml_probability"] = ml_prediction.get("edge_prob")
        sim.record_open(trade)
        sim.save()

        return {
            "status": "success",
            "message": "live_trade_submitted",
            "option_symbol": contract["option_symbol"],
            "qty": qty,
            "fill_price": fill_result.get("fill_price"),
            "risk_dollars": risk_dollars,
            "strike": contract.get("strike"),
            "expiry": contract.get("expiry"),
            "dte": contract.get("dte"),
            "spread_pct": contract.get("spread_pct"),
            "balance_after": sim.balance,
            "entry_context": entry_context,
            "signal_mode": signal_mode,
            "predicted_direction": trade.get("predicted_direction"),
            "prediction_confidence": trade.get("prediction_confidence"),
            "edge_prob": trade.get("edge_prob"),
            "direction_prob": trade.get("direction_prob"),
        }
    except Exception as e:
        logging.exception("sim_live_router_error: %s", e)
        return {"status": "error", "message": str(e)}


async def manage_live_exit(sim, trade):
    try:
        if not isinstance(trade, dict):
            return {"status": "error", "message": "invalid_trade"}
        option_symbol = trade.get("option_symbol")
        if not option_symbol:
            return {"status": "error", "message": "missing_symbol"}
        entry_time = trade.get("entry_time")
        if not entry_time:
            return {"status": "error", "message": "missing_entry_time"}

        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        entry_time_et = datetime.fromisoformat(entry_time)
        if entry_time_et.tzinfo is None:
            entry_time_et = eastern.localize(entry_time_et)
        elapsed_seconds = (now_et - entry_time_et).total_seconds()

        hold_min_seconds = int(trade.get("hold_min_seconds", 0))
        if elapsed_seconds < hold_min_seconds:
            return {"status": "skipped", "message": "hold_min"}

        current_price = get_option_price(option_symbol)
        if current_price is None:
            logging.warning("sim_live_exit_missing_price: %s", trade.get("trade_id"))
            return {"status": "error", "message": "missing_price"}

        sim.update_open_trade_excursion(trade.get("trade_id"), current_price)

        profile = sim.profile
        should_exit = False
        exit_reason = None
        exit_context = None

        stop_loss_pct = profile.get("stop_loss_pct")
        if stop_loss_pct is not None:
            try:
                entry_price = float(trade.get("entry_price", 0))
                if entry_price > 0:
                    loss_pct = (current_price - entry_price) / entry_price
                    if loss_pct <= -abs(float(stop_loss_pct)):
                        should_exit = True
                        exit_reason = "stop_loss"
                        exit_context = f"loss_pct={loss_pct:.3%} <= -{abs(float(stop_loss_pct)):.3%}"
            except (TypeError, ValueError):
                pass

        profit_target_pct = profile.get("profit_target_pct")
        if not should_exit and profit_target_pct is not None:
            try:
                entry_price = float(trade.get("entry_price", 0))
                if entry_price > 0:
                    gain_pct = (current_price - entry_price) / entry_price
                    if gain_pct >= abs(float(profit_target_pct)):
                        should_exit = True
                        exit_reason = "profit_target"
                        exit_context = f"gain_pct={gain_pct:.3%} >= {abs(float(profit_target_pct)):.3%}"
            except (TypeError, ValueError):
                pass

        trailing_activate = profile.get("trailing_stop_activate_pct")
        trailing_trail = profile.get("trailing_stop_trail_pct")
        if not should_exit and trailing_activate is not None and trailing_trail is not None:
            try:
                entry_price = float(trade.get("entry_price", 0))
                if entry_price > 0:
                    gain_pct = (current_price - entry_price) / entry_price
                    trailing_activate_f = abs(float(trailing_activate))
                    trailing_trail_f = abs(float(trailing_trail))
                    if not trade.get("trailing_stop_activated", False):
                        if gain_pct >= trailing_activate_f:
                            trade["trailing_stop_activated"] = True
                            trade["trailing_stop_high"] = current_price
                            sim.save()
                    else:
                        if current_price > trade.get("trailing_stop_high", 0):
                            trade["trailing_stop_high"] = current_price
                            sim.save()
                        trail_high = float(trade.get("trailing_stop_high", 0))
                        if trail_high > 0:
                            drop_from_high = (current_price - trail_high) / trail_high
                            if drop_from_high <= -trailing_trail_f:
                                should_exit = True
                                exit_reason = "trailing_stop"
                                exit_context = f"drop_from_high={drop_from_high:.3%} <= -{trailing_trail_f:.3%} (high={trail_high:.4f})"
            except (TypeError, ValueError):
                pass

        if not should_exit:
            hold_max_seconds = int(trade.get("hold_max_seconds", 0))
            if hold_max_seconds > 0 and elapsed_seconds >= hold_max_seconds:
                should_exit = True
                exit_reason = "hold_max_elapsed"
                exit_context = f"elapsed={int(elapsed_seconds)}s >= hold_max={int(hold_max_seconds)}s"

        if not should_exit or not exit_reason:
            return {"status": "skipped", "message": "no_exit"}

        qty = trade.get("qty")
        qty_val = _safe_int(qty)
        if qty_val is None:
            return {"status": "error", "message": "invalid_qty"}
        if qty_val <= 0:
            return {"status": "error", "message": "invalid_qty"}

        close_result = await asyncio.to_thread(close_option_position, option_symbol, qty_val)
        if not close_result.get("ok"):
            logging.warning("sim_live_exit_failed: %s", trade.get("trade_id"))
            return {"status": "error", "message": "exit_failed"}

        filled_avg_price = close_result.get("filled_avg_price")
        exit_price = filled_avg_price if filled_avg_price is not None else current_price
        exit_price_source = "broker_fill" if filled_avg_price is not None else "estimated_mid"

        exit_data = {
            "exit_price": exit_price,
            "exit_time": _now_et_iso(),
            "exit_reason": exit_reason,
            "exit_context": exit_context,
            "exit_price_source": exit_price_source,
            "exit_quote_model": "live_market",
            "entry_price_source": trade.get("entry_price_source", "live_fill"),
            "time_in_trade_seconds": int(elapsed_seconds),
            "spread_guard_bypassed": exit_price_source == "estimated_mid",
            "mae": trade.get("mae_pct"),
            "mfe": trade.get("mfe_pct"),
            "regime_at_entry": trade.get("regime_at_entry"),
            "time_of_day_bucket": trade.get("time_of_day_bucket"),
        }
        sim.record_close(trade.get("trade_id"), exit_data)
        sim.save()
        pnl_val = None
        try:
            entry_price_val = float(trade.get("entry_price", 0))
            if entry_price_val > 0:
                pnl_val = (exit_price - entry_price_val) * qty_val * 100
        except (TypeError, ValueError):
            pnl_val = None
        record_sim_trade_close(trade, pnl_val)

        return {
            "status": "success",
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "exit_context": exit_context,
            "option_symbol": option_symbol,
            "strike": trade.get("strike"),
            "expiry": trade.get("expiry"),
            "direction": trade.get("direction"),
            "qty": qty_val,
            "entry_price": trade.get("entry_price"),
            "pnl": pnl_val,
            "mae": trade.get("mae_pct"),
            "mfe": trade.get("mfe_pct"),
            "feature_snapshot": trade.get("feature_snapshot"),
            "balance_after": sim.balance,
            "time_in_trade_seconds": int(elapsed_seconds),
            "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
            "prediction_confidence": trade.get("prediction_confidence"),
            "edge_prob": trade.get("edge_prob"),
            "direction_prob": trade.get("direction_prob"),
        }
    except Exception as e:
        logging.exception("sim_live_exit_exception: %s", e)
        return {"status": "error", "message": str(e)}
```

#### `simulation/sim_metrics.py`
```python
import os
import math
import yaml
import logging

from simulation.sim_portfolio import SimPortfolio


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


_PROFILES = _load_profiles()


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _summarize_trade_log(trade_log: list[dict]) -> dict:
    total = len(trade_log)
    pnl_vals = []
    wins = 0
    for t in trade_log:
        pnl = _safe_float(t.get("realized_pnl_dollars"))
        if pnl is None:
            pnl = _safe_float(t.get("pnl"))
        if pnl is None:
            continue
        pnl_vals.append(pnl)
        if pnl > 0:
            wins += 1

    total_pnl = sum(pnl_vals) if pnl_vals else 0.0
    win_rate = (wins / total) if total > 0 else 0.0
    expectancy = (total_pnl / total) if total > 0 else 0.0

    mean_pnl = (sum(pnl_vals) / len(pnl_vals)) if pnl_vals else 0.0
    variance = 0.0
    if len(pnl_vals) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnl_vals) / (len(pnl_vals) - 1)
    std_pnl = math.sqrt(variance) if variance > 0 else 0.0
    sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0

    return {
        "total_trades": total,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "expectancy": expectancy,
        "sharpe": sharpe,
        "volatility": std_pnl,
    }


def _regime_breakdown(trade_log: list[dict]) -> dict:
    buckets: dict[str, list[float]] = {}
    for t in trade_log:
        regime = t.get("regime_at_entry") or "UNKNOWN"
        pnl = _safe_float(t.get("realized_pnl_dollars"))
        if pnl is None:
            pnl = _safe_float(t.get("pnl"))
        if pnl is None:
            continue
        buckets.setdefault(regime, []).append(pnl)

    out = {}
    for regime, pnls in buckets.items():
        total = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        total_pnl = sum(pnls)
        win_rate = (wins / total) if total > 0 else 0.0
        expectancy = (total_pnl / total) if total > 0 else 0.0
        out[regime] = {
            "total_trades": total,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "expectancy": expectancy,
        }
    return out


def _confidence_distribution(trade_log: list[dict]) -> dict:
    buckets = {"low": 0, "medium": 0, "high": 0, "unknown": 0}
    for t in trade_log:
        conf = _safe_float(t.get("prediction_confidence"))
        if conf is None:
            conf = _safe_float(t.get("confidence"))
        if conf is None:
            buckets["unknown"] += 1
        elif conf < 0.6:
            buckets["low"] += 1
        elif conf < 0.75:
            buckets["medium"] += 1
        else:
            buckets["high"] += 1

    total = sum(buckets.values())
    dist = {}
    for key, count in buckets.items():
        pct = (count / total) if total > 0 else 0.0
        dist[key] = {"count": count, "pct": pct}
    return dist


def get_sim_performance_profile(sim_id: str) -> dict:
    profile = _PROFILES.get(sim_id, {})
    sim = SimPortfolio(sim_id, profile)
    try:
        sim.load()
    except Exception:
        logging.exception("sim_metrics_load_failed: %s", sim_id)
        return {"sim_id": sim_id, "error": "load_failed"}

    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
    summary = _summarize_trade_log(trade_log)
    regime_stats = _regime_breakdown(trade_log)
    confidence_stats = _confidence_distribution(trade_log)

    return {
        "sim_id": sim_id,
        "profile_name": profile.get("name"),
        "balance": sim.balance,
        "peak_balance": sim.peak_balance,
        "open_trades": len(sim.open_trades) if isinstance(sim.open_trades, list) else 0,
        "summary": summary,
        "regime_stats": regime_stats,
        "confidence_stats": confidence_stats,
    }


def compare_sim_performance(sim_a: str, sim_b: str) -> dict:
    a = get_sim_performance_profile(sim_a)
    b = get_sim_performance_profile(sim_b)
    a_sum = a.get("summary", {})
    b_sum = b.get("summary", {})

    a_score = (a_sum.get("expectancy", 0.0), a_sum.get("sharpe", 0.0))
    b_score = (b_sum.get("expectancy", 0.0), b_sum.get("sharpe", 0.0))

    if a_score > b_score:
        winner = sim_a
    elif b_score > a_score:
        winner = sim_b
    else:
        winner = "tie"

    return {
        "sim_a": sim_a,
        "sim_b": sim_b,
        "winner": winner,
        "sim_a_summary": a_sum,
        "sim_b_summary": b_sum,
    }
```

#### `simulation/sim_ml.py`
```python
import os
import joblib
import logging
import threading
from datetime import datetime

from core.paths import DATA_DIR
from analytics.feature_logger import log_trade_features, FEATURE_FILE
from research.train_ai import train_direction_model, train_edge_model
from signals.conviction import calculate_conviction
from signals.regime import get_regime
from signals.volatility import volatility_state
from signals.setup_classifier import classify_trade
from signals.session_classifier import classify_session


DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

_direction_model = None
_edge_model = None
_TRAINING = False
_LAST_TRAIN_COUNT = 0
_TRADE_COUNT = None


REGIME_MAP = {
    "TREND": 1,
    "RANGE": 2,
    "VOLATILE": 3,
    "COMPRESSION": 4,
    "NO_DATA": 0,
}

VOL_MAP = {
    "DEAD": 0,
    "LOW": 1,
    "NORMAL": 2,
    "HIGH": 3,
}

SETUP_MAP = {
    "BREAKOUT": 1,
    "PULLBACK": 2,
    "REVERSAL": 3,
    "UNKNOWN": 0,
}

STYLE_MAP = {
    "scalp": 1,
    "mini_swing": 2,
    "momentum": 3,
}


def _load_models():
    global _direction_model, _edge_model
    if _direction_model is None and os.path.exists(DIR_MODEL_FILE):
        try:
            _direction_model = joblib.load(DIR_MODEL_FILE)
        except Exception:
            _direction_model = None
    if _edge_model is None and os.path.exists(EDGE_MODEL_FILE):
        try:
            _edge_model = joblib.load(EDGE_MODEL_FILE)
        except Exception:
            _edge_model = None
    return _direction_model, _edge_model


def _style_from_horizon(horizon: str | None) -> str:
    if horizon == "scalp":
        return "scalp"
    if horizon == "swing":
        return "momentum"
    return "mini_swing"


def predict_sim_trade(df, context: dict) -> dict:
    """
    Generates ML predictions for sim trades without altering trade logic.
    Returns prediction fields + feature context for logging/grading.
    """
    direction_model, edge_model = _load_models()

    predicted_direction = None
    direction_prob = None
    prediction_confidence = None
    edge_prob = None
    direction_ready = False
    edge_ready = False

    conviction_score, impulse, follow_through, _ = calculate_conviction(df)
    regime = context.get("regime") or get_regime(df)
    volatility = volatility_state(df)
    horizon = context.get("horizon")
    style = _style_from_horizon(horizon)

    entry_price = context.get("price")
    trade_direction = context.get("direction")
    setup = "UNKNOWN"
    try:
        setup = classify_trade(entry_price, (trade_direction or "").lower())
    except Exception:
        setup = "UNKNOWN"

    timestamp = context.get("timestamp") or datetime.now().isoformat()
    session = classify_session(timestamp)

    # Direction model: uses market features (ema9, ema20, rsi, vwap, volume)
    if direction_model is not None and df is not None and not df.empty:
        try:
            last = df.iloc[-1]
            ema9 = float(last.get("ema9"))
            ema20 = float(last.get("ema20"))
            rsi = float(last.get("rsi"))
            vwap = float(last.get("vwap"))
            volume = float(last.get("volume"))
            features = [[ema9, ema20, rsi, vwap, volume]]
            direction_prob = float(direction_model.predict_proba(features)[0][1])
            predicted_direction = "BULLISH" if direction_prob >= 0.5 else "BEARISH"
            prediction_confidence = max(direction_prob, 1 - direction_prob)
            direction_ready = True
        except Exception:
            predicted_direction = None
            direction_prob = None
            prediction_confidence = None

    # Edge model: uses trade-quality features
    if edge_model is not None:
        try:
            regime_encoded = REGIME_MAP.get(regime, 0)
            vol_encoded = VOL_MAP.get(volatility, 0)
            setup_encoded = SETUP_MAP.get(setup, 0)
            style_encoded = STYLE_MAP.get(style, 0)
            session_encoded = {
                "OPEN": 1,
                "MIDDAY": 2,
                "AFTERNOON": 3,
                "POWER": 4,
                "UNKNOWN": 0,
            }.get(session, 0)
            confidence_val = prediction_confidence if prediction_confidence is not None else 0.5
            feature_vec = [[
                regime_encoded,
                vol_encoded,
                conviction_score,
                impulse,
                follow_through,
                setup_encoded,
                session_encoded,
                confidence_val,
                style_encoded,
                0,
                0,
            ]]
            edge_prob = float(edge_model.predict_proba(feature_vec)[0][1])
            edge_ready = True
        except Exception:
            edge_prob = None

    if predicted_direction is None:
        if trade_direction in {"BULLISH", "BEARISH"}:
            predicted_direction = trade_direction

    if predicted_direction is not None:
        if direction_prob is None:
            direction_prob = 0.5
        if prediction_confidence is None:
            prediction_confidence = 0.5
        if edge_prob is None:
            edge_prob = 0.5

    if prediction_confidence is None and direction_prob is not None:
        prediction_confidence = max(direction_prob, 1 - direction_prob)

    if prediction_confidence is not None and edge_prob is not None:
        prediction_confidence = (prediction_confidence * 0.6) + (edge_prob * 0.4)

    return {
        "predicted_direction": predicted_direction,
        "prediction_confidence": prediction_confidence,
        "direction_prob": direction_prob,
        "edge_prob": edge_prob,
        "ml_ready": direction_ready or edge_ready,
        "regime": regime,
        "volatility": volatility,
        "conviction_score": conviction_score,
        "impulse": impulse,
        "follow_through": follow_through,
        "setup": setup,
        "style": style,
        "confidence": prediction_confidence,
    }


def _init_trade_count() -> int:
    try:
        if os.path.exists(FEATURE_FILE):
            with open(FEATURE_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            count = max(0, len(lines) - 1)
            return count
    except Exception:
        return 0
    return 0


def maybe_retrain_models(trade_count: int, min_trades: int = 50) -> bool:
    global _TRAINING, _LAST_TRAIN_COUNT
    if trade_count is None or trade_count < min_trades:
        return False
    if _TRAINING:
        return False
    if trade_count - _LAST_TRAIN_COUNT < min_trades:
        return False

    _TRAINING = True

    def _run(count: int):
        global _TRAINING, _LAST_TRAIN_COUNT
        try:
            train_direction_model()
            train_edge_model()
            _LAST_TRAIN_COUNT = count
        except Exception:
            logging.exception("sim_ml_retrain_failed")
        finally:
            _TRAINING = False

    threading.Thread(target=_run, args=(trade_count,), daemon=True).start()
    return True


def record_sim_trade_close(trade: dict, pnl: float | None) -> None:
    global _TRADE_COUNT
    if not isinstance(trade, dict):
        return
    if _TRADE_COUNT is None:
        _TRADE_COUNT = _init_trade_count()
    result = None
    if pnl is not None:
        result = "win" if pnl > 0 else "loss"
    try:
        if result is not None:
            log_trade_features(trade, result, pnl)
    except Exception:
        logging.exception("sim_ml_log_trade_features_failed")
    _TRADE_COUNT += 1
    maybe_retrain_models(_TRADE_COUNT)
```

#### `simulation/sim_portfolio.py`
```python
# simulation/sim_portfolio.py
import json
import logging
import os
import shutil
from datetime import datetime, date
import pytz


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
SIM_DIR = os.path.join(DATA_DIR, "sims")


class SimPortfolio:
    def __init__(self, sim_id: str, profile: dict):
        self.sim_id = sim_id
        self.profile = profile or {}
        self.balance = 0.0
        self.open_trades = []
        self.trade_log = []
        self.daily_loss = 0.0
        self.last_trade_day = self._today_et()
        self.peak_balance = 0.0
        self.schema_version = 1
        self.created_at = self._now_et_iso()
        self.last_updated_at = self._now_et_iso()

    def _now_et_iso(self) -> str:
        eastern = pytz.timezone("US/Eastern")
        return datetime.now(eastern).isoformat()

    def _today_et(self) -> str:
        eastern = pytz.timezone("US/Eastern")
        return datetime.now(eastern).date().isoformat()

    def _init_from_profile(self) -> None:
        starting_balance = self.profile.get("balance_start", 0.0)
        self.balance = float(starting_balance)
        self.open_trades = []
        self.trade_log = []
        self.daily_loss = 0.0
        self.last_trade_day = self._today_et()
        self.peak_balance = float(starting_balance)
        self.schema_version = 1
        self.created_at = self._now_et_iso()
        self.last_updated_at = self.created_at

    def _path(self) -> str:
        os.makedirs(SIM_DIR, exist_ok=True)
        return os.path.join(SIM_DIR, f"{self.sim_id}.json")

    def load(self) -> None:
        path = self._path()
        if not os.path.exists(path):
            self._init_from_profile()
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            self._init_from_profile()
            return

        schema = data.get("schema_version")
        if schema != 1:
            logging.warning("sim_schema_version_mismatch: %s", schema)

        self.balance = float(data.get("balance", self.profile.get("balance_start", 0.0)))
        self.open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
        self.trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
        self.daily_loss = float(data.get("daily_loss", 0.0))
        self.last_trade_day = data.get("last_trade_day", self._today_et())
        self.peak_balance = float(data.get("peak_balance", self.balance))
        self.schema_version = 1
        self.created_at = data.get("created_at", self._now_et_iso())
        self.last_updated_at = data.get("last_updated_at", self.created_at)
        self.profile_snapshot = data.get("profile_snapshot", {})
        self.reset_daily_if_needed()

    def save(self) -> None:
        path = self._path()
        tmp_path = f"{path}.tmp"
        bak_path = f"{path}.bak"
        self.last_updated_at = self._now_et_iso()

        data = {
            "sim_id": self.sim_id,
            "schema_version": self.schema_version,
            "created_at": self.created_at,
            "last_updated_at": self.last_updated_at,
            "profile_snapshot": self.profile,
            "balance": self.balance,
            "open_trades": self.open_trades,
            "trade_log": self.trade_log,
            "daily_loss": self.daily_loss,
            "last_trade_day": self.last_trade_day,
            "peak_balance": self.peak_balance,
        }
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
                f.flush()
                os.fsync(f.fileno())

            if os.path.exists(path):
                shutil.copy2(path, bak_path)

            os.replace(tmp_path, path)
            dir_fd = os.open(os.path.dirname(path), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def reset_daily_if_needed(self) -> None:
        today = self._today_et()
        if self.last_trade_day != today:
            self.daily_loss = 0.0
            self.last_trade_day = today
            self.save()

    def can_trade(self) -> tuple[bool, str]:
        daily_loss_limit_pct = self.profile.get("daily_loss_limit_pct")
        if daily_loss_limit_pct is not None:
            daily_loss_limit = self.balance * float(daily_loss_limit_pct)
            if self.daily_loss >= daily_loss_limit:
                return False, "daily_loss_limit"

        max_open = self.profile.get("max_open_trades")
        if max_open is not None and len(self.open_trades) >= int(max_open):
            return False, "max_open_trades"

        exposure_cap_pct = self.profile.get("exposure_cap_pct")
        if exposure_cap_pct is not None:
            exposure_cap = self.balance * float(exposure_cap_pct)
            total_exposure = 0.0
            for t in self.open_trades:
                if isinstance(t, dict):
                    try:
                        entry_price = float(t.get("entry_price", 0.0))
                        qty = float(t.get("qty", 0.0))
                        total_exposure += entry_price * qty * 100
                    except (TypeError, ValueError):
                        pass
            if total_exposure >= exposure_cap:
                return False, "exposure_cap"

        return True, ""

    def record_open(self, trade: dict) -> None:
        if not isinstance(trade, dict):
            return
        required = [
            "trade_id",
            "option_symbol",
            "entry_price",
            "qty",
            "entry_time",
            "sim_id",
            "horizon",
            "dte_bucket",
            "otm_pct",
        ]
        missing = [k for k in required if k not in trade]
        if missing:
            logging.warning("sim_trade_missing_fields: %s", ",".join(missing))
        if any(k not in trade or trade[k] is None for k in ["trade_id", "entry_price", "qty"]):
            logging.warning("sim_record_open_blocked_missing_critical: %s", trade.get("trade_id"))
            return
        # Reserve notional at entry so balance reflects cash in use.
        try:
            entry_price_val = float(trade.get("entry_price", 0))
            qty_val = float(trade.get("qty", 0))
            if entry_price_val > 0 and qty_val > 0 and not trade.get("cash_adjusted"):
                notional = entry_price_val * qty_val * 100
                trade["entry_notional"] = notional
                trade["cash_adjusted"] = True
                self.balance -= notional
        except (TypeError, ValueError):
            pass
        self.open_trades.append(trade)

    def record_close(self, trade_id: str, exit_data: dict) -> None:
        trade = None
        remaining = []
        for t in self.open_trades:
            if isinstance(t, dict) and t.get("trade_id") == trade_id:
                trade = t
            else:
                remaining.append(t)
        if trade is None:
            logging.warning("sim_trade_not_found: %s", trade_id)
            return
        self.open_trades = remaining

        if not isinstance(exit_data, dict):
            exit_data = {}

        entry_price = trade.get("entry_price")
        qty = trade.get("qty")
        exit_price = exit_data.get("exit_price")

        try:
            entry_price_val = float(entry_price) if entry_price is not None else None
        except (TypeError, ValueError):
            entry_price_val = None
        try:
            qty_val = float(qty) if qty is not None else None
        except (TypeError, ValueError):
            qty_val = None
        try:
            exit_price_val = float(exit_price) if exit_price is not None else None
        except (TypeError, ValueError):
            exit_price_val = None

        realized_pnl_dollars = None
        if entry_price_val is not None and exit_price_val is not None and qty_val is not None:
            realized_pnl_dollars = (exit_price_val - entry_price_val) * qty_val * 100

        realized_pnl_pct = None
        if entry_price_val is not None and entry_price_val > 0 and exit_price_val is not None:
            realized_pnl_pct = (exit_price_val - entry_price_val) / entry_price_val

        if realized_pnl_dollars is not None:
            if trade.get("cash_adjusted"):
                # Add back exit notional (includes pnl).
                if entry_price_val is not None and exit_price_val is not None and qty_val is not None:
                    exit_value = exit_price_val * qty_val * 100
                    self.balance += exit_value
            else:
                # Legacy behavior (no notional reserved at entry).
                self.balance += realized_pnl_dollars
            if realized_pnl_dollars < 0:
                self.daily_loss += abs(realized_pnl_dollars)
            if self.balance > self.peak_balance:
                self.peak_balance = self.balance

        trade_record = {}
        trade_record.update(trade)
        trade_record.update(exit_data)
        trade_record["trade_id"] = trade_id
        trade_record["sim_id"] = self.sim_id
        trade_record["realized_pnl_dollars"] = realized_pnl_dollars
        trade_record["realized_pnl_pct"] = realized_pnl_pct

        self.trade_log.append(trade_record)

    def update_open_trade_excursion(self, trade_id: str, current_price: float) -> None:
        try:
            trade = None
            for t in self.open_trades:
                if isinstance(t, dict) and t.get("trade_id") == trade_id:
                    trade = t
                    break
            if trade is None:
                return
            entry_price = float(trade.get("entry_price", 0.0))
            if entry_price <= 0:
                return
            if current_price is None:
                return
            excursion = (float(current_price) - entry_price) / entry_price
            old_mae_raw = trade.get("mae_pct")
            old_mfe_raw = trade.get("mfe_pct")
            if old_mae_raw is None or old_mfe_raw is None:
                new_mae = excursion
                new_mfe = excursion
                changed = True
            else:
                try:
                    old_mae = float(old_mae_raw)
                    old_mfe = float(old_mfe_raw)
                except (ValueError, TypeError):
                    new_mae = excursion
                    new_mfe = excursion
                    changed = True
                else:
                    new_mae = min(old_mae, excursion)
                    new_mfe = max(old_mfe, excursion)
                    changed = (new_mae != old_mae or new_mfe != old_mfe)
            if changed:
                trade["mae_pct"] = new_mae
                trade["mfe_pct"] = new_mfe
                self.save()
        except Exception:
            logging.exception("sim_update_excursion_error")
            return
```

#### `simulation/sim_signals.py`
```python
import logging
from signals.opportunity import evaluate_opportunity
from signals.volatility import volatility_state
from signals.predictor import make_prediction


def _find_col(df, candidates):
    if df is None:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _signal_mean_reversion(df) -> tuple[str | None, float | None]:
    RSI_OVERSOLD = 30
    RSI_OVERBOUGHT = 70
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        rsi_col = _find_col(df, ["rsi", "RSI", "rsi14", "RSI14"])
        vwap_col = _find_col(df, ["vwap", "VWAP"])
        if close_col is None or rsi_col is None or vwap_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        rsi = float(last[rsi_col])
        vwap = float(last[vwap_col])
        if rsi < RSI_OVERSOLD and close < vwap:
            return "BULLISH", close
        if rsi > RSI_OVERBOUGHT and close > vwap:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_breakout(df) -> tuple[str | None, float | None]:
    BREAKOUT_LOOKBACK = 20
    try:
        if df is None or len(df) < BREAKOUT_LOOKBACK + 1:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])
        if close_col is None or high_col is None or low_col is None:
            return None, None
        close = float(df.iloc[-1][close_col])
        highs = df[high_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        lows = df[low_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        if len(highs) < 1 or len(lows) < 1:
            return None, None
        recent_high = max(highs)
        recent_low = min(lows)
        if close > recent_high:
            return "BULLISH", close
        if close < recent_low:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_trend_pullback(df) -> tuple[str | None, float | None]:
    PULLBACK_TOLERANCE = 0.001
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        ema9_col = _find_col(df, ["ema9", "EMA9", "ema_9"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema9_col is None or ema20_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        ema9 = float(last[ema9_col])
        ema20 = float(last[ema20_col])
        if ema9 > ema20:
            if close <= ema9 * (1 + PULLBACK_TOLERANCE) and close >= ema9 * (1 - PULLBACK_TOLERANCE):
                return "BULLISH", close
        if ema9 < ema20:
            if close >= ema9 * (1 - PULLBACK_TOLERANCE) and close <= ema9 * (1 + PULLBACK_TOLERANCE):
                return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_orb_breakout(feature_snapshot: dict, profile: dict) -> tuple[str | None, float | None, str | None]:
    try:
        close = feature_snapshot.get("close")
        orb_high = feature_snapshot.get("orb_high")
        orb_low = feature_snapshot.get("orb_low")
        vol_z = feature_snapshot.get("vol_z")
        ema_spread = feature_snapshot.get("ema_spread")
        if close is None or orb_high is None or orb_low is None:
            return None, None, "orb_unavailable"
        vol_z_min = float(profile.get("vol_z_min", 0.0))
        if vol_z is None or float(vol_z) < vol_z_min:
            return None, None, "vol_z_filter"
        require_trend_bias = bool(profile.get("require_trend_bias", False))
        if close > orb_high:
            if require_trend_bias and (ema_spread is None or float(ema_spread) <= 0.0):
                return None, None, "trend_bias_fail"
            return "BULLISH", float(close), "orb_break_high"
        if close < orb_low:
            if require_trend_bias and (ema_spread is None or float(ema_spread) >= 0.0):
                return None, None, "trend_bias_fail"
            return "BEARISH", float(close), "orb_break_low"
        return None, None, "no_orb_break"
    except Exception:
        return None, None, "orb_error"


def _signal_swing_trend(df) -> tuple[str | None, float | None]:
    SWING_SLOPE_LOOKBACK = 10
    try:
        if df is None or len(df) < SWING_SLOPE_LOOKBACK + 1:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema20_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        ema20_now = float(last[ema20_col])
        ema20_past = float(df[ema20_col].iloc[-(SWING_SLOPE_LOOKBACK + 1)])
        ema20_slope_positive = ema20_now > ema20_past
        if ema20_slope_positive and close > ema20_now:
            return "BULLISH", close
        if (not ema20_slope_positive) and close < ema20_now:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _pick_opportunity_horizon(conviction_score, vol_state):
    try:
        score = float(conviction_score) if conviction_score is not None else 0.0
    except (TypeError, ValueError):
        score = 0.0
    if score >= 6:
        return "WEEKLY"
    if score <= 3:
        return "DAYTRADE"
    if vol_state == "HIGH":
        return "DAYTRADE"
    if vol_state == "LOW":
        return "WEEKLY"
    return "SWING"


def _signal_opportunity(df, context: dict | None = None):
    try:
        result = evaluate_opportunity(df)
        if not result:
            return None, None, None
        side = result[0]
        entry_low = result[1]
        entry_high = result[2]
        price = result[3]
        conviction_score = result[4]
        tp_low = result[5] if len(result) > 5 else None
        tp_high = result[6] if len(result) > 6 else None
        stop_loss = result[7] if len(result) > 7 else None

        direction = "BULLISH" if str(side).upper() in {"CALL", "CALLS"} else "BEARISH"
        underlying_price = float(price)
        vol_state = volatility_state(df)
        horizon_type = _pick_opportunity_horizon(conviction_score, vol_state)

        trade_count = 0
        if isinstance(context, dict):
            try:
                trade_count = int(context.get("trade_count", 0))
            except (TypeError, ValueError):
                trade_count = 0
        optimize_ready = trade_count >= 50

        # Use wider prediction horizons to decide whether to trade and how long to hold
        pred_minutes_map = {
            "DAYTRADE": 30,
            "SWING": 120,
            "WEEKLY": 390,
        }
        pred_minutes = pred_minutes_map.get(horizon_type, 60)
        pred = make_prediction(pred_minutes, df)
        pred_dir = pred.get("direction") if isinstance(pred, dict) else None
        pred_conf = pred.get("confidence") if isinstance(pred, dict) else None
        pred_dir_up = pred_dir.upper() if isinstance(pred_dir, str) else None

        # Safety gate: only enforce after enough trades
        if optimize_ready:
            if pred_dir_up in {"BULLISH", "BEARISH"} and pred_conf is not None:
                try:
                    if float(pred_conf) >= 0.6:
                        if (direction == "BULLISH" and pred_dir_up == "BEARISH") or (
                            direction == "BEARISH" and pred_dir_up == "BULLISH"
                        ):
                            return None, None, None
                except (TypeError, ValueError):
                    pass
            if pred_dir_up == "RANGE" and pred_conf is not None:
                try:
                    if float(pred_conf) >= 0.6:
                        return None, None, None
                except (TypeError, ValueError):
                    pass

        horizon_map = {
            "DAYTRADE": {
                "dte_min": 0,
                "dte_max": 0,
                "hold_min_seconds": 300,
                "hold_max_seconds": 3600,
                "horizon": "scalp",
                "cutoff_time_et": "15:30",
            },
            "SWING": {
                "dte_min": 1,
                "dte_max": 5,
                "hold_min_seconds": 1800,
                "hold_max_seconds": 86400,
                "horizon": "intraday",
                "cutoff_time_et": "15:45",
            },
            "WEEKLY": {
                "dte_min": 7,
                "dte_max": 21,
                "hold_min_seconds": 3600,
                "hold_max_seconds": 604800,
                "horizon": "swing",
                "cutoff_time_et": "15:45",
            },
        }
        meta = dict(horizon_map.get(horizon_type, {}))
        if optimize_ready:
            try:
                meta["hold_max_seconds"] = max(int(meta["hold_min_seconds"]), int(pred_minutes * 60))
            except Exception:
                pass
        entry_ctx = []
        try:
            entry_ctx.append(f"opp_entry={float(entry_low):.2f}-{float(entry_high):.2f}")
        except Exception:
            pass
        if tp_low is not None and tp_high is not None:
            try:
                entry_ctx.append(f"tp={float(tp_low):.2f}-{float(tp_high):.2f}")
            except Exception:
                pass
        if stop_loss is not None:
            try:
                entry_ctx.append(f"sl={float(stop_loss):.2f}")
            except Exception:
                pass
        if horizon_type:
            entry_ctx.append(f"horizon={horizon_type}")
        if pred_dir_up:
            entry_ctx.append(f"pred={pred_dir_up}@{pred_minutes}m")
        if pred_conf is not None:
            try:
                entry_ctx.append(f"pred_conf={float(pred_conf):.2f}")
            except (TypeError, ValueError):
                pass
        entry_ctx.append(f"opt_ready={int(optimize_ready)}")
        entry_ctx.append(f"cutoff={meta.get('cutoff_time_et', 'N/A')}")
        if entry_ctx:
            meta["entry_context"] = " | ".join(entry_ctx)
        meta["opportunity_type"] = horizon_type
        meta["take_profit_low"] = tp_low
        meta["take_profit_high"] = tp_high
        meta["stop_loss"] = stop_loss
        meta["horizon_type"] = horizon_type
        meta["predicted_direction"] = pred_dir_up
        meta["prediction_confidence"] = pred_conf
        meta["prediction_timeframe"] = pred_minutes
        meta["optimize_ready"] = optimize_ready

        return direction, underlying_price, meta
    except Exception:
        return None, None, None


def derive_sim_signal(df, signal_mode, context: dict | None = None, feature_snapshot: dict | None = None):
    try:
        if signal_mode == "MEAN_REVERSION":
            return _signal_mean_reversion(df)
        elif signal_mode == "BREAKOUT":
            return _signal_breakout(df)
        elif signal_mode == "TREND_PULLBACK":
            direction, price = _signal_trend_pullback(df)
            if direction is None or price is None:
                return None, None, None
            min_exp = context.get("atr_expansion_min") if isinstance(context, dict) else None
            if min_exp is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                atr_exp = feature_snapshot.get("atr_expansion")
                try:
                    if atr_exp is None or float(atr_exp) < float(min_exp):
                        return None, None, {"reason": "atr_expansion_filter"}
                except Exception:
                    return None, None, {"reason": "atr_expansion_invalid"}
            return direction, price, {"reason": "trend_pullback"}
        elif signal_mode == "SWING_TREND":
            return _signal_swing_trend(df)
        elif signal_mode == "OPPORTUNITY":
            return _signal_opportunity(df, context)
        elif signal_mode == "ORB_BREAKOUT":
            if feature_snapshot is None:
                return None, None, {"reason": "features_required"}
            direction, price, reason = _signal_orb_breakout(feature_snapshot, context or {})
            if direction is None or price is None:
                return None, None, {"reason": reason or "orb_no_signal"}
            return direction, price, {"reason": reason or "orb_break"}
        else:
            return None, None
    except Exception:
        return None, None
```

#### `simulation/sim_validator.py`
```python
import argparse
import os
import yaml
from datetime import datetime

from core.data_service import get_market_dataframe

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")



def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _validate_sim(sim_id: str, profile: dict, df=None) -> list[str]:
    errors = []
    required = [
        "signal_mode",
        "dte_min",
        "dte_max",
        "otm_pct",
        "hold_min_seconds",
        "hold_max_seconds",
        "max_spread_pct",
        "risk_per_trade_pct",
        "stop_loss_pct",
        "profit_target_pct",
        "cutoff_time_et",
    ]
    for key in required:
        if key not in profile:
            errors.append(f"missing:{key}")

    dte_min = _safe_float(profile.get("dte_min"))
    dte_max = _safe_float(profile.get("dte_max"))
    if dte_min is not None and dte_max is not None and dte_min > dte_max:
        errors.append("dte_window_invalid")

    hold_min = _safe_float(profile.get("hold_min_seconds"))
    hold_max = _safe_float(profile.get("hold_max_seconds"))
    if hold_min is not None and hold_max is not None and hold_min > hold_max:
        errors.append("hold_window_invalid")

    stop_loss = _safe_float(profile.get("stop_loss_pct"))
    if stop_loss is not None and not (0.01 <= stop_loss <= 2.0):
        errors.append("stop_loss_out_of_range")

    profit_target = _safe_float(profile.get("profit_target_pct"))
    if profit_target is not None and not (0.05 <= profit_target <= 5.0):
        errors.append("profit_target_out_of_range")

    max_spread = _safe_float(profile.get("max_spread_pct"))
    if max_spread is not None and not (0.01 <= max_spread <= 0.50):
        errors.append("max_spread_out_of_range")

    risk_pct = _safe_float(profile.get("risk_per_trade_pct"))
    if risk_pct is not None and not (0.001 <= risk_pct <= 0.05):
        errors.append("risk_pct_out_of_range")

    # Cutoff format
    cutoff = profile.get("cutoff_time_et")
    try:
        datetime.strptime(str(cutoff), "%H:%M")
    except Exception:
        errors.append("cutoff_format_invalid")

    # Feature gating: if enabled, require indicators.
    if profile.get("features_enabled"):
        if df is None:
            errors.append("features_enabled_no_df")
        else:
            zwin = _safe_float(profile.get("zscore_window", 30))
            min_bars = max(20, int(zwin) + 2 if zwin is not None else 32)
            if len(df) < min_bars:
                errors.append("features_insufficient_bars")
            for col in ("ema9", "ema20", "rsi", "atr", "vwap"):
                if col not in df.columns:
                    errors.append(f"features_missing:{col}")

    mode = str(profile.get("signal_mode", "")).upper()
    if mode == "ORB_BREAKOUT":
        if not profile.get("features_enabled"):
            errors.append("orb_requires_features")
        orb_minutes = _safe_float(profile.get("orb_minutes"))
        if orb_minutes is None or not (5 <= orb_minutes <= 120):
            errors.append("orb_minutes_invalid")

    return errors


def validate_sims() -> int:
    errors, total_errors = collect_sim_validation()
    error_map = {}
    for err in errors:
        if ":" in err:
            sim_id, msg = err.split(":", 1)
            error_map[sim_id.strip()] = msg.strip()
        else:
            error_map[err.strip()] = ""

    profiles = _load_profiles()
    if profiles:
        for sim_id in sorted(profiles):
            if sim_id in error_map:
                if error_map[sim_id]:
                    print(f"{sim_id}: {error_map[sim_id]}")
                else:
                    print(f"{sim_id}: ERROR")
            else:
                print(f"{sim_id}: OK")

    if not errors and total_errors == 0:
        print("SIM validation OK.")
        return 0
    if total_errors:
        print(f"Total issues: {total_errors}")
    return 2


def collect_sim_validation(df=None) -> tuple[list[str], int]:
    profiles = _load_profiles()
    if not profiles:
        return ["no_profiles_found"], 1
    if df is None:
        df = get_market_dataframe()
    errors = []
    total_errors = 0
    for sim_id in sorted(profiles):
        profile = profiles[sim_id]
        errs = _validate_sim(sim_id, profile, df=df)
        if errs:
            total_errors += len(errs)
            errors.append(f"{sim_id}: {', '.join(errs)}")
    return errors, total_errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SIM config validator")
    parser.add_argument("--validate", action="store_true", help="Validate SIM profiles")
    args = parser.parse_args()
    if args.validate:
        raise SystemExit(validate_sims())
```

#### `simulation/sim_watcher.py`
```python
# simulation/sim_watcher.py
import asyncio
import logging
import os
import yaml
import pytz
import discord
from datetime import datetime, time
from core.data_service import get_market_dataframe
from simulation.sim_portfolio import SimPortfolio
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    vol_col,
    delta_col,
    ml_col,
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)

ENTRY_INTERVAL_SECONDS = 60
EXIT_INTERVAL_SECONDS = 30
EOD_REPORT_TIME_ET = time(16, 0)

SIM_CHANNEL_MAP = {
    "SIM00": 1477023293545648258,  # Live version of SIM03
    "SIM01": 1476794016019386460,
    "SIM02": 1476794016019386460,
    "SIM03": 1476794039067218102,
    "SIM04": 1476794039067218102,
    "SIM05": 1476794065793323120,
    "SIM06": 1476794956826935339,
    "SIM07": 1476794956826935339,
    "SIM08": 1476795166751854654,
    "SIM09": 1477017498451705968,
    "SIM10": 1478200317035417641,
    "SIM11": 1478200298466971679,
}

_SIM_BOT = None
_SIM_LAST_SKIP_REASON = {}
_SIM_LAST_SKIP_TIME = {}
_SIM_LAST_DATA_AGE = None
_SIM_EOD_REPORT_DATE = None

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


_SIM_PROFILES = _load_profiles()


def _now_et() -> datetime:
    return datetime.now(pytz.timezone("US/Eastern"))


def _format_et(ts: datetime | None) -> str:
    if ts is None:
        return "N/A"
    try:
        if ts.tzinfo is None:
            ts = pytz.timezone("US/Eastern").localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("US/Eastern"))
        return ts.strftime("%Y-%m-%d %H:%M:%S ET")
    except Exception:
        return "N/A"

def _parse_strike_from_symbol(option_symbol: str | None) -> float | None:
    if not option_symbol or not isinstance(option_symbol, str):
        return None
    try:
        strike_part = option_symbol[-8:]
        return int(strike_part) / 1000.0
    except Exception:
        return None


def _get_data_age_text(df=None) -> str | None:
    try:
        if df is None:
            df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("US/Eastern")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        age = (_now_et() - ts).total_seconds()
        if age < 0:
            age = 0
        return f"Data age: {age:.0f}s (last candle {ts.strftime('%H:%M:%S')} ET)"
    except Exception:
        return None


def set_sim_bot(bot) -> None:
    global _SIM_BOT
    _SIM_BOT = bot


def get_sim_last_skip_state() -> dict:
    state = {}
    for sim_id, reason in _SIM_LAST_SKIP_REASON.items():
        state[sim_id] = {
            "reason": reason,
            "time": _SIM_LAST_SKIP_TIME.get(sim_id),
        }
    return state


async def _post_sim_event(sim_id: str, embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = SIM_CHANNEL_MAP.get(sim_id)
    if channel_id is None:
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        return
    try:
        await channel.send(embed=embed)
    except Exception:
        return


async def _post_main_skip(embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = getattr(_SIM_BOT, "paper_channel_id", None)
    if channel_id is None:
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        return
    try:
        main_embed = discord.Embed(
            title=embed.title,
            description=embed.description,
            color=embed.color.value if embed.color else 0xF1C40F
        )
        for field in embed.fields:
            main_embed.add_field(name=field.name, value=field.value, inline=field.inline)
        if embed.footer and embed.footer.text:
            main_embed.set_footer(text=embed.footer.text)
        await channel.send(embed=main_embed)
    except Exception:
        return


async def _post_main_embed(embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = getattr(_SIM_BOT, "paper_channel_id", None)
    if channel_id is None:
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        return
    try:
        await channel.send(embed=embed)
    except Exception:
        return


async def sim_eod_report_loop(channel_id: int) -> None:
    """
    End-of-day report for non-daytrading sims:
    show any open positions that will carry overnight.
    """
    global _SIM_EOD_REPORT_DATE
    eastern = pytz.timezone("US/Eastern")
    while True:
        try:
            now_et = _now_et()
            # Weekday only
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < EOD_REPORT_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _SIM_EOD_REPORT_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            lines_by_sim = {}
            for sim_id, profile in _SIM_PROFILES.items():
                try:
                    # Non-daytrading sims: dte_max > 0
                    if int(profile.get("dte_max", 0)) == 0:
                        continue
                    sim = SimPortfolio(sim_id, profile)
                    sim.load()
                    if not sim.open_trades:
                        continue
                    lines = []
                    for t in sim.open_trades:
                        symbol = t.get("option_symbol", "unknown")
                        qty = t.get("qty")
                        entry_price = t.get("entry_price")
                        entry_time = t.get("entry_time")
                        entry_text = f"${entry_price:.4f}" if isinstance(entry_price, (int, float)) else "N/A"
                        pnl_text = "N/A"
                        try:
                            from execution.option_executor import get_option_price
                            current_price = get_option_price(symbol)
                            if current_price is not None and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
                                pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                                pnl_text = f"{'+' if pnl_val >= 0 else ''}${pnl_val:.2f}"
                        except Exception:
                            pnl_text = "N/A"
                        time_text = entry_time or "N/A"
                        lines.append(f"{symbol} | qty {qty} | entry {entry_text} | pnl {pnl_text} | {time_text}")
                    if lines:
                        lines_by_sim[sim_id] = lines
                except Exception:
                    continue

            if not lines_by_sim:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Positions",
                    description=ab(A("No open non-daytrade SIM positions.", "green", bold=True)),
                    color=0x2ECC71,
                )
            else:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Positions",
                    description=ab(A("Non-daytrade SIM positions carrying overnight.", "yellow", bold=True)),
                    color=0xF39C12,
                )
                for sim_id, lines in sorted(lines_by_sim.items()):
                    embed.add_field(
                        name=f"{sim_id}",
                        value=ab(*[A(line, "white") for line in lines]),
                        inline=False
                    )

            footer_parts = [f"Time: {_format_et(now_et)}"]
            if _SIM_LAST_DATA_AGE:
                footer_parts.append(_SIM_LAST_DATA_AGE)
            embed.set_footer(text=" | ".join(footer_parts))
            if _SIM_BOT is not None and channel_id:
                channel = _SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            _SIM_EOD_REPORT_DATE = now_et.date()
        except Exception:
            logging.exception("sim_eod_report_error")
        await asyncio.sleep(60)


def _build_entry_embed(sim_id: str, result: dict) -> "discord.Embed":
    status = result.get("status", "opened")
    live_flag = "LIVE" if status == "live_submitted" else "SIM"
    title = f"📥 {sim_id} {live_flag} Entry"
    embed = discord.Embed(title=title, color=0x2ecc71)
    option_symbol = result.get("option_symbol") or "unknown"
    expiry = result.get("expiry")
    direction = result.get("direction") or "N/A"
    strike = result.get("strike") or _parse_strike_from_symbol(option_symbol)
    call_put = "CALL" if str(direction).upper() == "BULLISH" else "PUT" if str(direction).upper() == "BEARISH" else None
    expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
    contract_label = "SPY"
    if call_put:
        contract_label = f"{contract_label} {call_put}"
    if expiry_text:
        contract_label = f"{contract_label} {expiry_text}"
    if isinstance(strike, (int, float)):
        contract_label = f"{contract_label} {strike:g}"
    qty = result.get("qty")
    fill_price = result.get("fill_price")
    fill_text = f"${fill_price:.4f}" if isinstance(fill_price, (int, float)) else "N/A"
    direction = result.get("direction") or "N/A"
    mode = result.get("mode") or ("LIVE" if status == "live_submitted" else "SIM")
    contract_lines = [A(contract_label, "magenta", bold=True)]
    if option_symbol:
        contract_lines.append(A(option_symbol, "white"))
    embed.add_field(name="Contract", value=ab(*contract_lines), inline=False)
    embed.add_field(
        name="Order",
        value=ab(
            f"{lbl('Qty')} {A(qty if qty is not None else 'N/A', 'white', bold=True)}  |  "
            f"{lbl('Fill')} {A(fill_text, 'white', bold=True)}  |  "
            f"{lbl('Dir')} {dir_col(direction)}"
        ),
        inline=False,
    )
    embed.add_field(name="Mode", value=ab(A(f"{mode}", "cyan", bold=True)), inline=False)
    entry_price = result.get("entry_price")
    entry_text = f"${entry_price:.4f}" if isinstance(entry_price, (int, float)) else "N/A"
    notional_text = "N/A"
    try:
        if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
            notional_text = f"${(float(entry_price) * float(qty) * 100):,.2f}"
    except Exception:
        notional_text = "N/A"
    risk_dollars = result.get("risk_dollars")
    risk_text = f"${risk_dollars:.0f}" if isinstance(risk_dollars, (int, float)) else "N/A"
    balance_val = result.get("balance")
    balance_text = f"${balance_val:,.2f}" if isinstance(balance_val, (int, float)) else "N/A"
    embed.add_field(
        name="Risk / Balance",
        value=ab(
            f"{lbl('Entry')} {A(entry_text, 'white', bold=True)}  |  "
            f"{lbl('Notional')} {A(notional_text, 'white', bold=True)}  |  "
            f"{lbl('Risk')} {A(risk_text, 'yellow', bold=True)}  |  "
            f"{lbl('Bal')} {balance_col(balance_val)}"
        ),
        inline=False
    )
    strike = result.get("strike")
    expiry = result.get("expiry")
    dte = result.get("dte")
    spread = result.get("spread_pct")
    regime = result.get("regime") or "N/A"
    time_bucket = result.get("time_bucket") or "N/A"
    details = []
    if strike is not None and expiry is not None:
        details.append(f"{strike} {expiry}")
    if dte is not None:
        details.append(f"DTE {dte}")
    if isinstance(spread, (int, float)):
        details.append(f"spr {spread:.3f}")
    detail_text = " | ".join(details) if details else "details N/A"
    embed.add_field(name="Details", value=ab(A(detail_text, "cyan")), inline=False)
    embed.add_field(name="Context", value=ab(f"{lbl('Regime')} {regime_col(regime)}  |  {lbl('Time')} {A(time_bucket, 'cyan')}"), inline=False)
    feature_snapshot = result.get("feature_snapshot")
    fs_text = _format_feature_snapshot(feature_snapshot)
    if fs_text:
        embed.add_field(name="Feature Snapshot", value=ab(fs_text), inline=False)
    entry_context = result.get("entry_context")
    signal_mode = result.get("signal_mode")
    if entry_context or signal_mode:
        ctx_lines = []
        if signal_mode:
            ctx_lines.append(f"{lbl('Signal')} {A(signal_mode, 'magenta', bold=True)}")
        if entry_context:
            ctx_lines.append(A(entry_context, "cyan"))
        embed.add_field(name="Entry Context", value=ab(*ctx_lines), inline=False)
    pred_dir = result.get("predicted_direction") or "N/A"
    conf_val = result.get("prediction_confidence")
    edge_val = result.get("edge_prob")
    conf_text = f"{conf_val:.2f}" if isinstance(conf_val, (int, float)) else "N/A"
    edge_text = f"{edge_val:.2f}" if isinstance(edge_val, (int, float)) else "N/A"
    embed.add_field(
        name="ML",
        value=ab(
            f"{lbl('Pred')} {dir_col(pred_dir)}  |  "
            f"{lbl('Conf')} {conf_col(conf_val) if isinstance(conf_val, (int, float)) else A('N/A','gray')}  |  "
            f"{lbl('Edge')} {pct_col(edge_val, good_when_high=True, multiply=True) if isinstance(edge_val, (int, float)) else A('N/A','gray')}"
        ),
        inline=False
    )
    try:
        df = get_market_dataframe()
        last_close = df.iloc[-1].get("close") if df is not None and len(df) else None
        spy_price = float(last_close) if last_close is not None else None
    except Exception:
        spy_price = None
    if isinstance(spy_price, (int, float)):
        embed.add_field(name="SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


def _build_exit_embed(sim_id: str, result: dict) -> "discord.Embed":
    pnl_val = result.get("pnl")
    badge = "🟡"
    color = 0xF39C12
    if isinstance(pnl_val, (int, float)):
        if pnl_val > 0:
            badge = "✅"
            color = 0x2ECC71
        elif pnl_val < 0:
            badge = "❌"
            color = 0xE74C3C
        else:
            badge = "⚪"
            color = 0x95A5A6
    embed = discord.Embed(title=f"{badge} {sim_id} Exit", color=color)
    option_symbol = result.get("option_symbol") or "unknown"
    expiry = result.get("expiry")
    direction = result.get("direction") or "N/A"
    call_put = "CALL" if str(direction).upper() == "BULLISH" else "PUT" if str(direction).upper() == "BEARISH" else None
    expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
    contract_label = "SPY"
    if call_put:
        contract_label = f"{contract_label} {call_put}"
    if expiry_text:
        contract_label = f"{contract_label} {expiry_text}"
    qty = result.get("qty")
    exit_price = result.get("exit_price")
    exit_reason = result.get("exit_reason", "unknown")
    mode = result.get("mode") or "SIM"
    entry_price = result.get("entry_price")
    pnl_val = result.get("pnl")
    balance_after = result.get("balance_after")
    hold_seconds = result.get("time_in_trade_seconds")
    exit_text = f"${exit_price:.4f}" if isinstance(exit_price, (int, float)) else "N/A"
    entry_text = f"${entry_price:.4f}" if isinstance(entry_price, (int, float)) else "N/A"
    pnl_text = f"${pnl_val:.2f}" if isinstance(pnl_val, (int, float)) else "N/A"
    balance_text = f"${balance_after:,.2f}" if isinstance(balance_after, (int, float)) else "N/A"
    if isinstance(hold_seconds, (int, float)):
        hold_seconds = int(hold_seconds)
        hours = hold_seconds // 3600
        minutes = (hold_seconds % 3600) // 60
        seconds = hold_seconds % 60
        if hours > 0:
            hold_text = f"{hours}h {minutes}m {seconds}s"
        else:
            hold_text = f"{minutes}m {seconds}s"
    else:
        hold_text = "N/A"
    contract_lines = [A(contract_label, "magenta", bold=True)]
    if option_symbol:
        contract_lines.append(A(option_symbol, "white"))
    embed.add_field(name="Contract", value=ab(*contract_lines), inline=False)
    embed.add_field(
        name="Exit",
        value=ab(
            f"{lbl('Qty')} {A(qty if qty is not None else 'N/A', 'white', bold=True)}  |  "
            f"{lbl('Exit')} {A(exit_text, 'white', bold=True)}  |  "
            f"{lbl('PnL')} {pnl_col(pnl_val)}"
        ),
        inline=False,
    )
    embed.add_field(name="Entry", value=ab(f"{lbl('Entry')} {A(entry_text, 'white')}  |  {lbl('Mode')} {A(mode, 'cyan', bold=True)}"), inline=False)
    embed.add_field(name="Hold / Balance", value=ab(f"{lbl('Hold')} {A(hold_text, 'cyan')}  |  {lbl('Bal')} {balance_col(balance_after)}"), inline=False)
    embed.add_field(name="Reason", value=ab(exit_reason_col(exit_reason)), inline=False)
    exit_context = result.get("exit_context")
    if exit_context:
        embed.add_field(name="Exit Context", value=ab(A(exit_context, "cyan")), inline=False)
    feature_snapshot = result.get("feature_snapshot")
    fs_text = _format_feature_snapshot(feature_snapshot)
    if fs_text:
        embed.add_field(name="Feature Snapshot", value=ab(fs_text), inline=False)

    mae = result.get("mae")
    mfe = result.get("mfe")
    if isinstance(mae, (int, float)) or isinstance(mfe, (int, float)):
        mae_text = f"{mae:.2%}" if isinstance(mae, (int, float)) else "N/A"
        mfe_text = f"{mfe:.2%}" if isinstance(mfe, (int, float)) else "N/A"
        embed.add_field(
            name="Excursion",
            value=ab(f"{lbl('MFE')} {A(mfe_text, 'green')}  |  {lbl('MAE')} {A(mae_text, 'red')}"),
            inline=False,
        )
    pred_dir = result.get("predicted_direction") or "N/A"
    conf_val = result.get("prediction_confidence")
    edge_val = result.get("edge_prob")
    conf_text = f"{conf_val:.2f}" if isinstance(conf_val, (int, float)) else "N/A"
    edge_text = f"{edge_val:.2f}" if isinstance(edge_val, (int, float)) else "N/A"
    embed.add_field(
        name="ML",
        value=ab(
            f"{lbl('Pred')} {dir_col(pred_dir)}  |  "
            f"{lbl('Conf')} {conf_col(conf_val) if isinstance(conf_val, (int, float)) else A('N/A','gray')}  |  "
            f"{lbl('Edge')} {pct_col(edge_val, good_when_high=True, multiply=True) if isinstance(edge_val, (int, float)) else A('N/A','gray')}"
        ),
        inline=False
    )
    try:
        df = get_market_dataframe()
        last_close = df.iloc[-1].get("close") if df is not None and len(df) else None
        spy_price = float(last_close) if last_close is not None else None
    except Exception:
        spy_price = None
    if isinstance(spy_price, (int, float)):
        embed.add_field(name="SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


def _build_skip_embed(sim_id: str, result: dict) -> "discord.Embed":
    reason = result.get("reason") or "unknown"
    detail = _format_skip_reason(reason)
    title = f"⏸ {sim_id} Live Skipped"
    embed = discord.Embed(title=title, color=0xF1C40F)
    embed.add_field(name="Reason", value=ab(A(reason, "yellow", bold=True)), inline=False)
    if detail:
        embed.add_field(name="Details", value=ab(A(detail, "cyan")), inline=False)
    entry_context = result.get("entry_context")
    signal_mode = result.get("signal_mode")
    if entry_context or signal_mode:
        lines = []
        if signal_mode:
            lines.append(f"{lbl('Signal')} {A(signal_mode, 'magenta', bold=True)}")
        if entry_context:
            lines.append(A(entry_context, "cyan"))
        embed.add_field(name="Context", value=ab(*lines), inline=False)
    if reason == "insufficient_trade_history":
        trade_count = result.get("trade_count")
        min_trades = result.get("min_trades_for_live")
        if isinstance(trade_count, int) and isinstance(min_trades, int):
            embed.add_field(
                name="Trade History",
                value=ab(A(f"{trade_count} / {min_trades} closed trades", "white", bold=True)),
                inline=False,
            )
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


def _format_skip_reason(reason: str) -> str:
    mapping = {
        "insufficient_trade_history": "Not enough closed trades to allow live execution.",
        "cutoff_passed": "0DTE cutoff passed (after 13:30 ET).",
        "no_candidate_expiry": "No valid expiries for DTE window.",
        "empty_chain": "Option chain returned empty for expiry.",
        "chain_error": "Option chain request failed (API/market data).",
        "no_snapshot": "No snapshot returned for candidate contract.",
        "no_snapshot_all": "No snapshot returned for any candidate strike.",
        "no_snapshot_most": "Most candidate strikes returned no snapshot.",
        "no_snapshot_all_no_chain_symbols": "No snapshots and chain symbols missing (likely off-hours or API issue).",
        "no_quote": "Snapshot missing bid/ask quote.",
        "no_quote_all": "All candidate snapshots missing bid/ask.",
        "no_quote_most": "Most candidate snapshots missing bid/ask.",
        "no_quote_all_no_chain_symbols": "Quotes missing and chain symbols missing (off-hours or API issue).",
        "invalid_quote": "Bid/ask invalid (likely off-hours or illiquid).",
        "invalid_quote_all": "All candidate quotes invalid (off-hours or illiquid).",
        "invalid_quote_most": "Most candidate quotes invalid.",
        "invalid_quote_all_no_chain_symbols": "Quotes invalid and chain symbols missing.",
        "spread_too_wide": "Spread exceeds max_spread_pct.",
        "spread_too_wide_all": "All candidate contracts had spreads above max_spread_pct.",
        "spread_too_wide_most": "Most candidate contracts had spreads above max_spread_pct.",
        "spread_too_wide_all_no_chain_symbols": "Spreads too wide and chain symbols missing.",
        "snapshot_error": "Snapshot request failed (API/market data).",
        "snapshot_error_all": "All snapshot requests failed.",
        "snapshot_error_most": "Most snapshot requests failed.",
        "snapshot_error_all_no_chain_symbols": "Snapshot failures and chain symbols missing.",
        "no_chain_symbols": "Chain returned no symbols (SDK format mismatch or API issue).",
        "missing_api_keys": "Missing Alpaca API keys.",
        "invalid_price": "Underlying price invalid.",
        "invalid_direction": "Signal direction invalid.",
        "no_contract": "No contract met selection rules.",
    }
    return mapping.get(reason, "")


def _format_feature_snapshot(fs: dict | None) -> str | None:
    if not isinstance(fs, dict) or not fs:
        return None

    def _f(key, fmt="{:.3f}"):
        val = fs.get(key)
        if val is None:
            return None
        try:
            return fmt.format(float(val))
        except Exception:
            return str(val)

    parts = []
    orb_h = _f("orb_high", "{:.2f}")
    orb_l = _f("orb_low", "{:.2f}")
    if orb_h and orb_l:
        parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
    vol_z = _f("vol_z")
    if vol_z:
        parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
    atr_exp = _f("atr_expansion")
    if atr_exp:
        parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
    vwap_z = _f("vwap_z")
    if vwap_z:
        parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
    close_z = _f("close_z")
    if close_z:
        parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
    iv_rank = _f("iv_rank_proxy")
    if iv_rank:
        parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")

    if not parts:
        return None
    return "  |  ".join(parts)


def _is_market_hours() -> bool:
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    return (
        now_et.weekday() <= 4
        and (now_et.hour, now_et.minute) >= (9, 30)
        and (now_et.hour, now_et.minute) <= (16, 0)
    )


# Simple heuristic regime classifier used for sim metadata (not live decisions).
def _derive_regime(df):
    """
    Classify market regime from indicator columns.
    Returns "VOLATILE", "TREND", "RANGE", or None if data insufficient.
    """
    TREND_SEPARATION_THRESHOLD = 0.001
    ATR_LOOKBACK = 50
    ATR_VOLATILE_PERCENTILE = 75
    MIN_BARS_REQUIRED = 2

    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None

        last = df.iloc[-1]
        close = float(last.get("close", 0))
        if close <= 0:
            return None

        atr_col = None
        for candidate in ["atr", "ATR", "atr14", "ATR14"]:
            if candidate in df.columns:
                atr_col = candidate
                break

        if atr_col is not None:
            lookback = min(ATR_LOOKBACK, len(df))
            atr_series = df[atr_col].iloc[-lookback:].dropna()
            if len(atr_series) >= 2:
                current_atr = float(atr_series.iloc[-1])
                pct_rank = (atr_series < current_atr).sum() / len(atr_series) * 100
                if pct_rank > ATR_VOLATILE_PERCENTILE:
                    ema9_col = None
                    for c in ["ema9", "EMA9", "ema_9"]:
                        if c in df.columns:
                            ema9_col = c
                            break
                    if ema9_col and len(df) >= 2:
                        ema9_now = float(df[ema9_col].iloc[-1])
                        ema9_prev = float(df[ema9_col].iloc[-2])
                        if ema9_now < ema9_prev:
                            return "VOLATILE"

        ema9_val = None
        ema20_val = None
        for c in ["ema9", "EMA9", "ema_9"]:
            if c in df.columns:
                ema9_val = float(last[c])
                break
        for c in ["ema20", "EMA20", "ema_20"]:
            if c in df.columns:
                ema20_val = float(last[c])
                break

        if ema9_val is not None and ema20_val is not None:
            separation = abs(ema9_val - ema20_val) / close
            if separation > TREND_SEPARATION_THRESHOLD:
                return "TREND"

        return "RANGE"

    except Exception:
        return None


# Time-of-day bucket for sim metadata.
def _get_time_of_day_bucket():
    """Classify current ET time into a trading session bucket."""
    try:
        now_et = datetime.now(pytz.timezone("US/Eastern"))
        t = now_et.time()
        if t < time(10, 30):
            return "MORNING"
        elif t < time(12, 0):
            return "MIDDAY"
        elif t < time(14, 0):
            return "AFTERNOON"
        else:
            return "CLOSE"
    except Exception:
        return None


async def sim_entry_loop() -> None:
    eastern = pytz.timezone("US/Eastern")
    while True:
        iter_start = datetime.now(eastern)
        try:
            if _is_market_hours():
                from simulation.sim_engine import run_sim_entries
                df = get_market_dataframe()
                global _SIM_LAST_DATA_AGE
                _SIM_LAST_DATA_AGE = _get_data_age_text(df)
                results = await run_sim_entries(df, regime=_derive_regime(df))
                for result in results:
                    logging.debug("sim_entry_result: %s", result)
                    try:
                        sim_id = result.get("sim_id")
                        status = result.get("status")
                        if sim_id and status in {"opened", "live_submitted"}:
                            entry_embed = _build_entry_embed(sim_id, result)
                            await _post_sim_event(sim_id, entry_embed)
                            if status == "live_submitted":
                                await _post_main_embed(entry_embed)
                            _SIM_LAST_SKIP_REASON.pop(sim_id, None)
                        elif sim_id and status == "skipped":
                            reason = result.get("reason") or "unknown"
                            if sim_id == "SIM00":
                                _SIM_LAST_SKIP_REASON[sim_id] = reason
                                _SIM_LAST_SKIP_TIME[sim_id] = _now_et()
                                continue
                            if reason in {
                                "insufficient_trade_history",
                                "cutoff_passed",
                                "no_candidate_expiry",
                                "empty_chain",
                                "chain_error",
                                "no_snapshot",
                                "no_snapshot_all",
                                "no_snapshot_most",
                                "no_snapshot_all_no_chain_symbols",
                                "no_quote",
                                "no_quote_all",
                                "no_quote_most",
                                "no_quote_all_no_chain_symbols",
                                "invalid_quote",
                                "invalid_quote_all",
                                "invalid_quote_most",
                                "invalid_quote_all_no_chain_symbols",
                                "spread_too_wide",
                                "spread_too_wide_all",
                                "spread_too_wide_most",
                                "spread_too_wide_all_no_chain_symbols",
                                "snapshot_error",
                                "snapshot_error_all",
                                "snapshot_error_most",
                                "snapshot_error_all_no_chain_symbols",
                                "no_chain_symbols",
                                "missing_api_keys",
                                "invalid_price",
                                "invalid_direction",
                                "no_contract",
                            }:
                                last_reason = _SIM_LAST_SKIP_REASON.get(sim_id)
                                if last_reason != reason:
                                    _SIM_LAST_SKIP_REASON[sim_id] = reason
                                    _SIM_LAST_SKIP_TIME[sim_id] = _now_et()
                                    skip_embed = _build_skip_embed(sim_id, result)
                                    await _post_sim_event(sim_id, skip_embed)
                    except Exception:
                        pass
        except Exception as e:
            logging.exception("sim_entry_loop_error: %s", e)
        elapsed = (datetime.now(eastern) - iter_start).total_seconds()
        await asyncio.sleep(max(0.0, ENTRY_INTERVAL_SECONDS - elapsed))


async def sim_exit_loop() -> None:
    eastern = pytz.timezone("US/Eastern")
    while True:
        iter_start = datetime.now(eastern)
        try:
            from simulation.sim_engine import run_sim_exits
            global _SIM_LAST_DATA_AGE
            _SIM_LAST_DATA_AGE = _get_data_age_text()
            results = await run_sim_exits()
            for result in results:
                logging.debug("sim_exit_result: %s", result)
                try:
                    sim_id = result.get("sim_id")
                    status = result.get("status")
                    if sim_id and status == "closed":
                        exit_embed = _build_exit_embed(sim_id, result)
                        await _post_sim_event(sim_id, exit_embed)
                        if result.get("mode") == "LIVE":
                            await _post_main_embed(exit_embed)
                        try:
                            from analytics.grader import check_predictions
                            check_predictions(result)
                        except Exception:
                            logging.exception("sim_prediction_grader_error")
                except Exception:
                    pass
        except Exception as e:
            logging.exception("sim_exit_loop_error: %s", e)
        elapsed = (datetime.now(eastern) - iter_start).total_seconds()
        await asyncio.sleep(max(0.0, EXIT_INTERVAL_SECONDS - elapsed))


# To wire into bot.py, add to QQQBot.setup_hook():
#
#   from simulation.sim_watcher import sim_entry_loop, sim_exit_loop
#   self.loop.create_task(self.safe_task(sim_entry_loop))
#   self.loop.create_task(self.safe_task(sim_exit_loop))
```

### 3.2 Config and state files (redacted)

#### `.claude/settings.local.json`
```json
{
  "permissions": {
    "allow": [
      "Bash(python -c \":*)",
      "Bash(python -m py_compile decision/trader.py simulation/sim_contract.py execution/option_executor.py interface/watchers.py core/decision_context.py)"
    ]
  }
}
```

#### `.env`
```
DISCORD_TOKEN=NjE4NTYzNzI1NDQ3NzkwNjEx.GVQSu2.x4i7-hL7xusvRNpu9U3Mfu0_mIzTi7r9jj5LjQ
APCA_API_KEY_ID=REDACTED
APCA_API_SECRET_KEY=REDACTED
OPENAI_API_KEY=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
```

#### `.vscode/settings.json`
```json
{
    "git.ignoreLimitWarning": true
}
```

#### `data/account.json`
```json
{
    "balance": 25000,
    "starting_balance": 25000,
    "open_trade": null,
    "trade_log": [],
    "wins": 0,
    "losses": 0,
    "day_trades": [],
    "risk_per_trade": 100,
    "max_trade_size": 200,
    "daily_loss": 0,
    "max_daily_loss": 200,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000,
    "virtual_capital_limit": 25000,
    "open_trades": []
}
```

#### `data/career_stats.json`
```json
{
    "total_trades_all_time": 0,
    "total_wins_all_time": 0,
    "total_losses_all_time": 0,
    "best_balance": 25000,
    "setups": {
        "BREAKOUT": {
            "wins": 0,
            "losses": 0
        },
        "PULLBACK": {
            "wins": 0,
            "losses": 0
        },
        "REVERSAL": {
            "wins": 0,
            "losses": 0
        }
    },
    "confidence": {
        "50-60": {
            "correct": 0,
            "total": 0
        },
        "60-70": {
            "correct": 0,
            "total": 0
        },
        "70-80": {
            "correct": 0,
            "total": 0
        },
        "80-100": {
            "correct": 0,
            "total": 0
        }
    },
    "time_of_day": {
        "OPEN": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        },
        "MIDDAY": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        },
        "AFTERNOON": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        },
        "POWER": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        }
    },
    "styles": {
        "scalp": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        },
        "mini_swing": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        },
        "momentum": {
            "wins": 0,
            "losses": 0,
            "pnl": 0
        }
    }
}
```

#### `data/edge_stats.json`
```json
{
    "overall": {
        "total": 1,
        "wins": 0,
        "winrate": 0.0
    },
    "timeframes": {
        "30": {
            "total": 1,
            "wins": 0,
            "winrate": 0.0
        }
    },
    "regimes": {
        "VOLATILE": {
            "total": 1,
            "wins": 0,
            "winrate": 0.0
        }
    },
    "sessions": {
        "POWER": {
            "total": 1,
            "wins": 0,
            "winrate": 0.0
        }
    }
}
```

#### `data/sims/SIM00.json`
```json
{
    "sim_id": "SIM00",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.109218-05:00",
    "last_updated_at": "2026-03-02T21:17:07.109243-05:00",
    "profile_snapshot": {
        "name": "Intraday Trend Pullback (LIVE)",
        "horizon": "intraday",
        "signal_mode": "TREND_PULLBACK",
        "features_enabled": false,
        "execution_mode": "live",
        "enabled": true,
        "source_sim": "SIM03",
        "min_source_trades": 50,
        "capital_limit_dollars": 1500,
        "dte_min": 0,
        "dte_max": 1,
        "otm_pct": 0.003,
        "hold_min_seconds": 600,
        "hold_max_seconds": 5400,
        "cutoff_time_et": "15:00",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 1,
        "daily_loss_limit": 300,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.3,
        "profit_target_pct": 0.55,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct": 0.08
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM01.json`
```json
{
    "sim_id": "SIM01",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.219976-05:00",
    "last_updated_at": "2026-03-02T21:17:07.220001-05:00",
    "profile_snapshot": {
        "name": "0DTE Scalp Mean Reversion",
        "horizon": "scalp",
        "signal_mode": "MEAN_REVERSION",
        "features_enabled": false,
        "dte_min": 0,
        "dte_max": 0,
        "otm_pct": 0.008,
        "hold_min_seconds": 60,
        "hold_max_seconds": 300,
        "cutoff_time_et": "13:30",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.005,
        "daily_loss_limit_pct": 0.02,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.1,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.2,
        "profit_target_pct": 0.35,
        "trailing_stop_activate_pct": null,
        "trailing_stop_trail_pct": null
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM02.json`
```json
{
    "sim_id": "SIM02",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.290324-05:00",
    "last_updated_at": "2026-03-02T21:17:07.290350-05:00",
    "profile_snapshot": {
        "name": "0DTE Scalp Breakout",
        "horizon": "scalp",
        "signal_mode": "BREAKOUT",
        "features_enabled": false,
        "dte_min": 0,
        "dte_max": 0,
        "otm_pct": 0.01,
        "hold_min_seconds": 60,
        "hold_max_seconds": 600,
        "cutoff_time_et": "13:30",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.005,
        "daily_loss_limit_pct": 0.02,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.1,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.22,
        "profit_target_pct": 0.4,
        "trailing_stop_activate_pct": null,
        "trailing_stop_trail_pct": null
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM03.json`
```json
{
    "sim_id": "SIM03",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.360717-05:00",
    "last_updated_at": "2026-03-02T21:17:07.360742-05:00",
    "profile_snapshot": {
        "name": "Intraday Trend Pullback",
        "horizon": "intraday",
        "signal_mode": "TREND_PULLBACK",
        "features_enabled": false,
        "dte_min": 0,
        "dte_max": 1,
        "otm_pct": 0.003,
        "hold_min_seconds": 600,
        "hold_max_seconds": 5400,
        "cutoff_time_et": "15:00",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 1,
        "daily_loss_limit": 300,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.3,
        "profit_target_pct": 0.55,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct": 0.08
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM04.json`
```json
{
    "sim_id": "SIM04",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.430623-05:00",
    "last_updated_at": "2026-03-02T21:17:07.430648-05:00",
    "profile_snapshot": {
        "name": "Intraday Range Fade",
        "horizon": "intraday",
        "signal_mode": "MEAN_REVERSION",
        "features_enabled": false,
        "dte_min": 0,
        "dte_max": 1,
        "otm_pct": 0.005,
        "hold_min_seconds": 300,
        "hold_max_seconds": 2700,
        "cutoff_time_et": "15:00",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.28,
        "profit_target_pct": 0.5,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct": 0.08
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM05.json`
```json
{
    "sim_id": "SIM05",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.500480-05:00",
    "last_updated_at": "2026-03-02T21:17:07.500505-05:00",
    "profile_snapshot": {
        "name": "1DTE Afternoon Continuation",
        "horizon": "intraday",
        "signal_mode": "TREND_PULLBACK",
        "features_enabled": false,
        "dte_min": 1,
        "dte_max": 1,
        "otm_pct": 0.003,
        "hold_min_seconds": 1800,
        "hold_max_seconds": 10800,
        "cutoff_time_et": "15:30",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.32,
        "profit_target_pct": 0.6,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct": 0.08
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM06.json`
```json
{
    "sim_id": "SIM06",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.570264-05:00",
    "last_updated_at": "2026-03-02T21:17:07.570290-05:00",
    "profile_snapshot": {
        "name": "7-10 DTE Short Swing",
        "horizon": "swing",
        "signal_mode": "SWING_TREND",
        "features_enabled": false,
        "dte_min": 7,
        "dte_max": 10,
        "otm_pct": 0.0,
        "hold_min_seconds": 3600,
        "hold_max_seconds": 259200,
        "cutoff_time_et": "15:45",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.01,
        "daily_loss_limit_pct": 0.04,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.2,
        "max_spread_pct": 0.12,
        "entry_slippage": 0.008,
        "exit_slippage": 0.008,
        "stop_loss_pct": 0.45,
        "profit_target_pct": 0.9,
        "trailing_stop_activate_pct": 0.12,
        "trailing_stop_trail_pct": 0.06
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM07.json`
```json
{
    "sim_id": "SIM07",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.640018-05:00",
    "last_updated_at": "2026-03-02T21:17:07.640043-05:00",
    "profile_snapshot": {
        "name": "14-21 DTE Swing Trend",
        "horizon": "swing",
        "signal_mode": "SWING_TREND",
        "features_enabled": false,
        "dte_min": 14,
        "dte_max": 21,
        "otm_pct": 0.0,
        "hold_min_seconds": 86400,
        "hold_max_seconds": 604800,
        "cutoff_time_et": "15:45",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.01,
        "daily_loss_limit_pct": 0.04,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.2,
        "max_spread_pct": 0.12,
        "entry_slippage": 0.008,
        "exit_slippage": 0.008,
        "stop_loss_pct": 0.5,
        "profit_target_pct": 1.0,
        "trailing_stop_activate_pct": 0.12,
        "trailing_stop_trail_pct": 0.06
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM08.json`
```json
{
    "sim_id": "SIM08",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.709791-05:00",
    "last_updated_at": "2026-03-02T21:17:07.709815-05:00",
    "profile_snapshot": {
        "name": "Regime Filter Agent",
        "horizon": "intraday",
        "signal_mode": "TREND_PULLBACK",
        "features_enabled": false,
        "dte_min": 1,
        "dte_max": 1,
        "otm_pct": 0.003,
        "hold_min_seconds": 1800,
        "hold_max_seconds": 10800,
        "cutoff_time_et": "15:00",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.02,
        "max_open_trades": 1,
        "exposure_cap_pct": 0.1,
        "max_spread_pct": 0.12,
        "entry_slippage": 0.008,
        "exit_slippage": 0.008,
        "regime_filter": "TREND_ONLY",
        "stop_loss_pct": 0.28,
        "profit_target_pct": 0.5,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct": 0.08
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM09.json`
```json
{
    "sim_id": "SIM09",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.779838-05:00",
    "last_updated_at": "2026-03-02T21:17:07.779861-05:00",
    "profile_snapshot": {
        "name": "Opportunity Follower",
        "horizon": "adaptive",
        "signal_mode": "OPPORTUNITY",
        "features_enabled": false,
        "dte_min": 0,
        "dte_max": 7,
        "otm_pct": 0.005,
        "hold_min_seconds": 300,
        "hold_max_seconds": 86400,
        "cutoff_time_et": "15:30",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 1,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.15,
        "profit_target_pct": 0.4,
        "trailing_stop_activate_pct": 0.12,
        "trailing_stop_trail_pct": 0.06
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM10.json`
```json
{
    "sim_id": "SIM10",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.849712-05:00",
    "last_updated_at": "2026-03-02T21:17:07.849735-05:00",
    "profile_snapshot": {
        "name": "ORB Breakout",
        "horizon": "scalp",
        "signal_mode": "ORB_BREAKOUT",
        "features_enabled": true,
        "orb_minutes": 30,
        "vol_z_min": 1.0,
        "require_trend_bias": false,
        "dte_min": 0,
        "dte_max": 0,
        "otm_pct": 0.01,
        "hold_min_seconds": 60,
        "hold_max_seconds": 900,
        "cutoff_time_et": "11:00",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.005,
        "daily_loss_limit_pct": 0.02,
        "max_open_trades": 1,
        "exposure_cap_pct": 0.1,
        "max_spread_pct": 0.15,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.25,
        "profit_target_pct": 0.35,
        "trailing_stop_activate_pct": null,
        "trailing_stop_trail_pct": null,
        "zscore_window": 30,
        "iv_series_window": 200
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `data/sims/SIM11.json`
```json
{
    "sim_id": "SIM11",
    "schema_version": 1,
    "created_at": "2026-03-02T21:17:07.919743-05:00",
    "last_updated_at": "2026-03-02T21:17:07.919766-05:00",
    "profile_snapshot": {
        "name": "Vol Expansion Trend",
        "horizon": "intraday",
        "signal_mode": "TREND_PULLBACK",
        "features_enabled": true,
        "atr_expansion_min": 1.2,
        "dte_min": 1,
        "dte_max": 3,
        "otm_pct": 0.01,
        "hold_min_seconds": 300,
        "hold_max_seconds": 7200,
        "cutoff_time_et": "15:30",
        "balance_start": 25000,
        "risk_per_trade_pct": 0.0075,
        "daily_loss_limit_pct": 0.03,
        "max_open_trades": 2,
        "exposure_cap_pct": 0.15,
        "max_spread_pct": 0.18,
        "entry_slippage": 0.01,
        "exit_slippage": 0.01,
        "stop_loss_pct": 0.35,
        "profit_target_pct": 0.6,
        "trailing_stop_activate_pct": 0.12,
        "trailing_stop_trail_pct": 0.06,
        "zscore_window": 30,
        "iv_series_window": 200
    },
    "balance": 25000.0,
    "open_trades": [],
    "trade_log": [],
    "daily_loss": 0.0,
    "last_trade_day": "2026-03-02",
    "peak_balance": 25000.0
}
```

#### `package-lock.json`
```json
{
  "name": "qqqbot",
  "lockfileVersion": 3,
  "requires": true,
  "packages": {
    "": {
      "devDependencies": {
        "pyright": "^1.1.408"
      }
    },
    "node_modules/fsevents": {
      "version": "2.3.3",
      "resolved": "https://registry.npmjs.org/fsevents/-/fsevents-2.3.3.tgz",
      "integrity": "sha512-5xoDfX+fL7faATnagmWPpbFtwh/R77WmMMqqHGS65C3vvB0YHrgF+B1YmZ3441tMj5n63k0212XNoJwzlhffQw==",
      "dev": true,
      "hasInstallScript": true,
      "optional": true,
      "os": [
        "darwin"
      ],
      "engines": {
        "node": "^8.16.0 || ^10.6.0 || >=11.0.0"
      }
    },
    "node_modules/pyright": {
      "version": "1.1.408",
      "resolved": "https://registry.npmjs.org/pyright/-/pyright-1.1.408.tgz",
      "integrity": "sha512-N61pxaLLCsPcUuPPHMNIrGoZgGBgrbjBX5UqkaT5UV8NVZdL7ExsO6N3ectv1DzAUsLOzdlyqoYtX76u8eF4YA==",
      "dev": true,
      "bin": {
        "pyright": "index.js",
        "pyright-langserver": "langserver.index.js"
      },
      "engines": {
        "node": ">=14.0.0"
      },
      "optionalDependencies": {
        "fsevents": "~2.3.3"
      }
    }
  }
}
```

#### `package.json`
```json
{
  "devDependencies": {
    "pyright": "^1.1.408"
  }
}
```

#### `pyrightconfig.json`
```json
{
  "venvPath": "/home/asif420/qqqbot",
  "venv": "venv",
  "pythonVersion": "3.12",
  "reportMissingModuleSource": false
}
```

#### `simulation/sim_config.yaml`
```yaml
SIM00:
  name: "Intraday Trend Pullback (LIVE)"
  horizon: "intraday"
  signal_mode: "TREND_PULLBACK"
  features_enabled: false
  execution_mode: "live"
  enabled: true
  source_sim: "SIM03"
  min_source_trades: 50
  capital_limit_dollars: 1500
  dte_min: 0
  dte_max: 1
  otm_pct: 0.003
  hold_min_seconds: 600
  hold_max_seconds: 5400
  cutoff_time_et: "15:00"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 1
  daily_loss_limit: 300
  exposure_cap_pct: 0.15
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.30
  profit_target_pct: 0.55
  trailing_stop_activate_pct: 0.15
  trailing_stop_trail_pct: 0.08

SIM01:
  name: "0DTE Scalp Mean Reversion"
  horizon: "scalp"
  signal_mode: "MEAN_REVERSION"
  features_enabled: false
  dte_min: 0
  dte_max: 0
  otm_pct: 0.008
  hold_min_seconds: 60
  hold_max_seconds: 300
  cutoff_time_et: "13:30"
  balance_start: 25000
  risk_per_trade_pct: 0.005
  daily_loss_limit_pct: 0.02
  max_open_trades: 2
  exposure_cap_pct: 0.10
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.25
  profit_target_pct: 0.35
  trailing_stop_activate_pct: null
  trailing_stop_trail_pct: null

SIM02:
  name: "0DTE Scalp Breakout"
  horizon: "scalp"
  signal_mode: "BREAKOUT"
  features_enabled: false
  dte_min: 0
  dte_max: 0
  otm_pct: 0.010
  hold_min_seconds: 60
  hold_max_seconds: 600
  cutoff_time_et: "13:30"
  balance_start: 25000
  risk_per_trade_pct: 0.005
  daily_loss_limit_pct: 0.02
  max_open_trades: 2
  exposure_cap_pct: 0.10
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.25
  profit_target_pct: 0.35
  trailing_stop_activate_pct: null
  trailing_stop_trail_pct: null

SIM03:
  name: "Intraday Trend Pullback"
  horizon: "intraday"
  signal_mode: "TREND_PULLBACK"
  features_enabled: false
  dte_min: 0
  dte_max: 1
  otm_pct: 0.003
  hold_min_seconds: 600
  hold_max_seconds: 5400
  cutoff_time_et: "15:00"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 1
  daily_loss_limit: 300
  exposure_cap_pct: 0.15
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.30
  profit_target_pct: 0.55
  trailing_stop_activate_pct: 0.15
  trailing_stop_trail_pct: 0.08

SIM04:
  name: "Intraday Range Fade"
  horizon: "intraday"
  signal_mode: "MEAN_REVERSION"
  features_enabled: false
  dte_min: 0
  dte_max: 1
  otm_pct: 0.005
  hold_min_seconds: 300
  hold_max_seconds: 2700
  cutoff_time_et: "15:00"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 2
  exposure_cap_pct: 0.15
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.30
  profit_target_pct: 0.55
  trailing_stop_activate_pct: 0.15
  trailing_stop_trail_pct: 0.08

SIM05:
  name: "1DTE Afternoon Continuation"
  horizon: "intraday"
  signal_mode: "TREND_PULLBACK"
  features_enabled: false
  dte_min: 1
  dte_max: 1
  otm_pct: 0.003
  hold_min_seconds: 1800
  hold_max_seconds: 10800
  cutoff_time_et: "15:30"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 2
  exposure_cap_pct: 0.15
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.30
  profit_target_pct: 0.55
  trailing_stop_activate_pct: 0.15
  trailing_stop_trail_pct: 0.08

SIM06:
  name: "7-10 DTE Short Swing"
  horizon: "swing"
  signal_mode: "SWING_TREND"
  features_enabled: false
  dte_min: 7
  dte_max: 10
  otm_pct: 0.0
  hold_min_seconds: 3600
  hold_max_seconds: 259200
  cutoff_time_et: "15:45"
  balance_start: 25000
  risk_per_trade_pct: 0.01
  daily_loss_limit_pct: 0.04
  max_open_trades: 2
  exposure_cap_pct: 0.20
  max_spread_pct: 0.12
  entry_slippage: 0.008
  exit_slippage: 0.008
  stop_loss_pct: 0.45
  profit_target_pct: 0.90
  trailing_stop_activate_pct: 0.12
  trailing_stop_trail_pct: 0.06

SIM07:
  name: "14-21 DTE Swing Trend"
  horizon: "swing"
  signal_mode: "SWING_TREND"
  features_enabled: false
  dte_min: 14
  dte_max: 21
  otm_pct: 0.0
  hold_min_seconds: 86400
  hold_max_seconds: 604800
  cutoff_time_et: "15:45"
  balance_start: 25000
  risk_per_trade_pct: 0.01
  daily_loss_limit_pct: 0.04
  max_open_trades: 2
  exposure_cap_pct: 0.20
  max_spread_pct: 0.12
  entry_slippage: 0.008
  exit_slippage: 0.008
  stop_loss_pct: 0.55
  profit_target_pct: 1.00
  trailing_stop_activate_pct: 0.12
  trailing_stop_trail_pct: 0.06

SIM08:
  name: "Regime Filter Agent"
  horizon: "intraday"
  signal_mode: "TREND_PULLBACK"
  features_enabled: false
  dte_min: 1
  dte_max: 1
  otm_pct: 0.003
  hold_min_seconds: 1800
  hold_max_seconds: 10800
  cutoff_time_et: "15:00"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.02
  max_open_trades: 1
  exposure_cap_pct: 0.10
  max_spread_pct: 0.12
  entry_slippage: 0.008
  exit_slippage: 0.008
  regime_filter: "TREND_ONLY"
  stop_loss_pct: 0.30
  profit_target_pct: 0.55
  trailing_stop_activate_pct: 0.15
  trailing_stop_trail_pct: 0.08

SIM09:
  name: "Opportunity Follower"
  horizon: "adaptive"
  signal_mode: "OPPORTUNITY"
  features_enabled: false
  dte_min: 0
  dte_max: 7
  otm_pct: 0.005
  hold_min_seconds: 300
  hold_max_seconds: 86400
  cutoff_time_et: "15:30"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 1
  exposure_cap_pct: 0.15
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.40
  profit_target_pct: 0.60
  trailing_stop_activate_pct: 0.12
  trailing_stop_trail_pct: 0.06

SIM10:
  name: "ORB Breakout"
  horizon: "scalp"
  signal_mode: "ORB_BREAKOUT"
  features_enabled: true
  orb_minutes: 30
  vol_z_min: 1.0
  require_trend_bias: false
  dte_min: 0
  dte_max: 0
  otm_pct: 0.01
  hold_min_seconds: 60
  hold_max_seconds: 900
  cutoff_time_et: "11:00"
  balance_start: 25000
  risk_per_trade_pct: 0.005
  daily_loss_limit_pct: 0.02
  max_open_trades: 1
  exposure_cap_pct: 0.10
  max_spread_pct: 0.15
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.25
  profit_target_pct: 0.35
  trailing_stop_activate_pct: null
  trailing_stop_trail_pct: null
  zscore_window: 30
  iv_series_window: 200

SIM11:
  name: "Vol Expansion Trend"
  horizon: "intraday"
  signal_mode: "TREND_PULLBACK"
  features_enabled: true
  atr_expansion_min: 1.2
  dte_min: 1
  dte_max: 3
  otm_pct: 0.01
  hold_min_seconds: 300
  hold_max_seconds: 7200
  cutoff_time_et: "15:30"
  balance_start: 25000
  risk_per_trade_pct: 0.0075
  daily_loss_limit_pct: 0.03
  max_open_trades: 2
  exposure_cap_pct: 0.15
  max_spread_pct: 0.18
  entry_slippage: 0.01
  exit_slippage: 0.01
  stop_loss_pct: 0.35
  profit_target_pct: 0.60
  trailing_stop_activate_pct: 0.12
  trailing_stop_trail_pct: 0.06
  zscore_window: 30
  iv_series_window: 200
```

## 4. Current State of Key Systems

### 4.1 Entry Logic (Sims)
- Signals come from `simulation/sim_signals.py` based on profile `signal_mode`.
- Contract selection via `simulation/sim_contract.py` honors `dte_min/dte_max`, OTM %, spread guard, and cutoff time.
- `simulation/sim_engine.py` computes risk, qty, tries to fill, and opens trades.

### 4.2 Entry Logic (Live)
- Live entries via `decision/trader.py` and `execution/option_executor.py`.
- `SIM00` uses live execution and source SIM03 gating (min trades).

### 4.3 Exit Logic (Sims)
Exit reasons (sims):
- eod_daytrade_close
- expiry_close
- hold_max_elapsed
- profit_lock
- profit_target
- profit_target_2
- stop_loss
- trailing_stop

Exit checks order in `simulation/sim_engine.py`:
1) Expiry close (same-day expiry at 15:55 ET)
2) Daytrade EOD close (15:55 ET for dte_max==0)
3) Stop-loss (option premium % drop)
4) Near-TP lock/TP2 logic (profit_lock, profit_target_2)
5) Profit target
6) Trailing stop (if configured)
7) Hold max elapsed

### 4.4 Exit Logic (Live)
- Live exits in `decision/trader.py` via stop/target checks, partial logic, and expectancy-based exits (edge_exit).
- Live expiry close at 15:55 ET for same-day expiry.

### 4.5 Trailing Stop
- Implemented in `simulation/sim_engine.py` and `simulation/sim_live_router.py`.
- `trailing_stop_activated` and `trailing_stop_high` stored on trade dict; `sim.save()` called when mutated.

### 4.6 MAE/MFE Tracking
- Implemented in `simulation/sim_portfolio.py:update_open_trade_excursion`.
- `mae_pct` and `mfe_pct` persisted on open trades and included in exit_data.

### 4.7 Simulation Manager
- Sim states stored as JSON in `data/sims/SIMxx.json`.
- Managed by `simulation/sim_engine.py` and `simulation/sim_portfolio.py`.

### 4.8 Spread Guard
- Enforced in sim contract selection and sim fills; can be bypassed for forced exits.

### 4.9 Regime Detection
- Regime derived in `decision/trader.py` and analytics modules; used as filters and context.

## 5. Data Flow
- Market data via CSV recorder (data/qqq_1m.csv or equivalent) with Alpaca fallback in `core/data_service.py`.
- Watchers poll data on intervals defined in `interface/watchers.py`.

## 6. Logging & Observability
- Logging uses Python logging + CSV logs + Discord embeds.
- Analytics logs: signal_log.csv, blocked_signals.csv, contract_selection_log.csv, execution_quality_log.csv.

## 7. Known Bugs / TODOs
- decision/trader.py:1334 — # TODO: trailing stop / take-profit for reconstructed trades.

## 8. Recent Changes
- Git history has only one commit; recent changes inferred from working tree.
- Notable modifications include: sim TP2/profit_lock logic; sim_config DTE-tier SL/TP; sim health/validator updates; sim feature snapshot reporting.

## 9. Dependencies
- No requirements.txt or pyproject.toml in repo root.
- Node dependencies exist (package.json) for pyright tooling.
- Python runtime expected 3.12 (venv present).

## 10. Config & Parameters
- SIM parameters defined in `simulation/sim_config.yaml` (see config dump).
- Live trader parameters in `decision/trader.py` and profile data.
