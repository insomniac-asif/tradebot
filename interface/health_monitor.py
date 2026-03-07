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
