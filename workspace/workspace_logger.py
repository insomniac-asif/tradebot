# workspace/workspace_logger.py
#
# Appends timestamped events to daily markdown logs under workspace/daily_logs/.

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LOG_DIR  = os.path.join(_BASE_DIR, "workspace", "daily_logs")

_VALID_EVENT_TYPES = frozenset({
    "trade_entry", "trade_exit", "signal_skip", "circuit_breaker",
    "hypothesis_created", "behavior_report", "manual_note",
})


def _log_path(date_str: str) -> str:
    return os.path.join(_LOG_DIR, f"{date_str}.md")


def _summarize(event_type: str, data: dict) -> str:
    """Build a one-line summary from event data. Never raises."""
    try:
        if event_type == "trade_entry":
            return (
                f"SIM{data.get('sim_id', '?')} "
                f"{data.get('direction', '?')} "
                f"entry at ${data.get('price', '?')}"
            )
        if event_type == "trade_exit":
            return (
                f"SIM{data.get('sim_id', '?')} "
                f"exit: {data.get('exit_reason', '?')}, "
                f"PnL: ${data.get('pnl', '?')}"
            )
        if event_type == "signal_skip":
            return f"SIM{data.get('sim_id', '?')} skipped: {data.get('reason', '?')}"
        if event_type == "hypothesis_created":
            claim = str(data.get("claim", ""))[:80]
            return f"New hypothesis: {claim}"
        if event_type == "behavior_report":
            return f"Report generated for SIM{data.get('sim_id', '?')}"
        if event_type == "manual_note":
            return str(data.get("message", ""))
        # Fallback for unknown / circuit_breaker / etc.
        return json.dumps(data)
    except Exception:
        try:
            return json.dumps(data)
        except Exception:
            return str(data)


def log_event(event_type: str, data: dict) -> None:
    """
    Append a timestamped event entry to today's workspace markdown log.

    File: workspace/daily_logs/YYYY-MM-DD.md
    Format:
        ### HH:MM:SS — {event_type}
        {one-line summary}
    """
    try:
        os.makedirs(_LOG_DIR, exist_ok=True)
        today   = datetime.now().strftime("%Y-%m-%d")
        ts      = datetime.now().strftime("%H:%M:%S")
        summary = _summarize(event_type, data)
        entry   = f"\n### {ts} — {event_type}\n{summary}\n"
        with open(_log_path(today), "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception as exc:
        logger.error("log_event failed: %s", exc)


def get_todays_log() -> str:
    """Return today's log content, or a message if no log exists yet."""
    return get_log(datetime.now().strftime("%Y-%m-%d"))


def get_log(date_str: str) -> str:
    """Return the log content for a given YYYY-MM-DD date, or a not-found message."""
    path = _log_path(date_str)
    if not os.path.exists(path):
        return f"No log found for {date_str}."
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as exc:
        logger.error("get_log failed for %s: %s", date_str, exc)
        return f"No log found for {date_str}."
