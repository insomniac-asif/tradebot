"""
Crash recovery helpers.

write_heartbeat()   — called every ~30s from heart_monitor to prove the bot is alive.
read_heartbeat()    — returns the last heartbeat dict (or None).
full_reconcile(bot) — called at startup; if the last heartbeat is stale (bot was down),
                      re-runs perform_startup_broker_sync to close any offline-filled exits
                      and reconstruct any positions opened while the bot was offline.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import pytz

from core.paths import DATA_DIR

_HB_FILE = os.path.join(DATA_DIR, "heartbeat.json")
_STALE_SECONDS = 120  # heartbeat older than this → assume crash


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def write_heartbeat() -> None:
    """Write current timestamp + PID to heartbeat file."""
    payload = {
        "ts": datetime.now(pytz.UTC).isoformat(),
        "pid": os.getpid(),
    }
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = _HB_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, _HB_FILE)
    except Exception as exc:
        logging.warning("write_heartbeat_failed: %s", exc)


def read_heartbeat() -> Optional[dict]:
    """Return last heartbeat dict, or None if file is missing/corrupt."""
    try:
        with open(_HB_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def heartbeat_age_seconds() -> Optional[float]:
    """Seconds since last heartbeat, or None if no heartbeat file."""
    hb = read_heartbeat()
    if hb is None:
        return None
    try:
        ts = datetime.fromisoformat(hb["ts"])
        if ts.tzinfo is None:
            ts = pytz.UTC.localize(ts)
        age = (datetime.now(pytz.UTC) - ts).total_seconds()
        return max(0.0, age)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Full reconcile
# ---------------------------------------------------------------------------

async def full_reconcile(bot) -> dict:
    """
    Called at startup.  If the last heartbeat is stale (indicating a crash /
    unclean shutdown), re-runs the broker sync to handle offline closes.

    Returns a summary dict: {crash_detected, age_seconds, sync_run}.
    """
    age = heartbeat_age_seconds()
    crash_detected = age is None or age > _STALE_SECONDS

    result = {
        "crash_detected": crash_detected,
        "age_seconds": age,
        "sync_run": False,
    }

    if crash_detected:
        logging.warning(
            "crash_recovery: stale heartbeat (age=%.0fs) — running broker sync",
            age if age is not None else -1,
        )
        try:
            from core.startup_sync import perform_startup_broker_sync
            await perform_startup_broker_sync(bot)
            result["sync_run"] = True
        except Exception as exc:
            logging.error("crash_recovery_sync_failed: %s", exc)
            result["sync_error"] = str(exc)

    # Write fresh heartbeat after reconcile so next startup sees a clean state
    write_heartbeat()
    return result
