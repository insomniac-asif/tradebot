"""
Structured (JSON-lines) logger for qqqbot.

Usage:
    from core.structured_logger import slog, slog_critical, setup_structured_logging

    slog("trade_entry", sim_id="SIM03", symbol="SPY260312C00580000", price=2.45)
    slog_critical("crash_detected", error="...")

File: logs/structured.log  (rotating, 5 MB × 3 backups)

Optional Discord webhook for CRITICAL events:
    Set DISCORD_WEBHOOK_ERRORS env var to a webhook URL.
"""

import json
import logging
import os
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import Any
from urllib.request import urlopen, Request
from urllib.error import URLError

from core.paths import LOG_DIR

# ---------------------------------------------------------------------------
# File: logs/structured.log
# ---------------------------------------------------------------------------

_LOG_FILE = os.path.join(LOG_DIR, "structured.log")
_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
_BACKUP_COUNT = 3
_LOCK = threading.Lock()

# Lazy-initialised handler
_file_handler: RotatingFileHandler | None = None


def _get_handler() -> RotatingFileHandler:
    global _file_handler
    if _file_handler is None:
        with _LOCK:
            if _file_handler is None:
                os.makedirs(LOG_DIR, exist_ok=True)
                _file_handler = RotatingFileHandler(
                    _LOG_FILE,
                    maxBytes=_MAX_BYTES,
                    backupCount=_BACKUP_COUNT,
                    encoding="utf-8",
                )
    return _file_handler


# ---------------------------------------------------------------------------
# Core writer
# ---------------------------------------------------------------------------

def slog(event: str, level: str = "info", **fields: Any) -> None:
    """Write a structured JSON-line log entry."""
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "event": event,
        "level": level,
        **fields,
    }
    try:
        line = json.dumps(entry, default=str)
        handler = _get_handler()
        with _LOCK:
            handler.stream.write(line + "\n")
            handler.stream.flush()
            # Let RotatingFileHandler check rollover
            handler.doRollover() if handler.stream.tell() >= handler.maxBytes else None
    except Exception:
        pass

    if level == "critical":
        _send_webhook(entry)


def slog_error(event: str, **fields: Any) -> None:
    slog(event, level="error", **fields)


def slog_critical(event: str, **fields: Any) -> None:
    slog(event, level="critical", **fields)


# ---------------------------------------------------------------------------
# Discord webhook for CRITICAL events
# ---------------------------------------------------------------------------

_WEBHOOK_URL: str | None = None
_WEBHOOK_LOADED = False


def _get_webhook_url() -> str | None:
    global _WEBHOOK_URL, _WEBHOOK_LOADED
    if not _WEBHOOK_LOADED:
        _WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_ERRORS") or None
        _WEBHOOK_LOADED = True
    return _WEBHOOK_URL


def _send_webhook(entry: dict) -> None:
    url = _get_webhook_url()
    if not url:
        return
    try:
        event = entry.get("event", "unknown")
        fields_text = " | ".join(
            f"{k}={v}" for k, v in entry.items()
            if k not in ("ts", "event", "level")
        )
        content = f"🚨 **CRITICAL** `{event}` @ `{entry.get('ts')}`"
        if fields_text:
            content += f"\n```{fields_text[:500]}```"
        payload = json.dumps({"content": content}).encode("utf-8")
        req = Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urlopen(req, timeout=5)
    except (URLError, Exception):
        pass


# ---------------------------------------------------------------------------
# Integration with Python logging (optional setup)
# ---------------------------------------------------------------------------

class _StructuredHandler(logging.Handler):
    """Bridge Python's logging.ERROR+ calls into structured log."""

    def emit(self, record: logging.LogRecord) -> None:
        level = record.levelname.lower()
        slog(
            "python_log",
            level=level,
            logger=record.name,
            message=self.format(record),
        )


def setup_structured_logging() -> None:
    """
    Attach the structured handler to the root logger so that any
    logging.error() / logging.critical() also lands in structured.log.
    Call once from bot.py setup_hook.
    """
    root = logging.getLogger()
    # Avoid double-attaching
    for h in root.handlers:
        if isinstance(h, _StructuredHandler):
            return
    handler = _StructuredHandler()
    handler.setLevel(logging.ERROR)
    root.addHandler(handler)


# ---------------------------------------------------------------------------
# Convenience: tail last N lines from the structured log
# ---------------------------------------------------------------------------

def tail_structured_log(n: int = 50) -> list[dict]:
    """Read last n entries from structured.log. Returns list of dicts."""
    try:
        with open(_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        results = []
        for line in lines[-n:]:
            try:
                results.append(json.loads(line.strip()))
            except Exception:
                pass
        return results
    except FileNotFoundError:
        return []
    except Exception:
        return []
