"""
Historical 1-minute bar backfill from Alpaca.

Fetches SPY 1m bars for a date range in daily chunks (to stay within
Alpaca rate limits) and merges into data/qqq_1m.csv.

Usage:
    from core.backfill import run_backfill, backfill_status

    # Synchronous (blocking) — use from scripts/
    result = run_backfill(days_back=30)

    # Async wrapper — use from bot commands
    result = await run_backfill_async(days_back=30, progress_cb=cb)
"""

import asyncio
import fcntl
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Callable, Optional

import pandas as pd
import pytz
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from dotenv import load_dotenv

from core.paths import DATA_DIR
from core.rate_limiter import rate_limit_sleep

load_dotenv()

_SYMBOL = "SPY"
_DATA_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")
_ET = pytz.timezone("US/Eastern")
_ALPACA_MIN_INTERVAL = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))
_INTER_CHUNK_SLEEP = 1.0  # extra wait between daily chunks to be polite

# Minimum bars per day expected (short days/holidays handled by ≥ 0 guard)
_MIN_BARS_WARNING = 200  # rows in the full CSV below this = warn "sparse"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client() -> Optional[StockHistoricalDataClient]:
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None
    return StockHistoricalDataClient(api_key, secret_key)


def _is_market_day(d: date) -> bool:
    """Very simple weekday check — excludes weekends. Good enough for backfill."""
    return d.weekday() < 5


def _fetch_day(client: StockHistoricalDataClient, day: date) -> Optional[pd.DataFrame]:
    """Fetch 1m SPY bars for a single trading day. Returns None on failure."""
    market_open_et = _ET.localize(datetime(day.year, day.month, day.day, 9, 30, 0))
    market_close_et = _ET.localize(datetime(day.year, day.month, day.day, 16, 0, 0))
    start_utc = market_open_et.astimezone(pytz.UTC)
    end_utc = market_close_et.astimezone(pytz.UTC)

    try:
        rate_limit_sleep("alpaca_backfill", _ALPACA_MIN_INTERVAL)
        req = StockBarsRequest(
            symbol_or_symbols=_SYMBOL,
            timeframe=TimeFrame(1, TimeFrameUnit("Min")),
            start=start_utc,
            end=end_utc,
            feed=DataFeed.IEX,
        )
        bars = client.get_stock_bars(req)
        df = getattr(bars, "df", None)
        if not isinstance(df, pd.DataFrame) or df.empty:
            return None
        df = df.reset_index()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("US/Eastern").dt.tz_localize(None)
        # Keep only OHLCV columns that the CSV already has
        keep = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c in df.columns]
        return df[keep]
    except Exception as exc:
        logging.warning("backfill_fetch_day_error: day=%s err=%s", day, exc)
        return None


def _load_csv() -> Optional[pd.DataFrame]:
    if not os.path.exists(_DATA_FILE):
        return None
    try:
        with open(_DATA_FILE, "r", newline="") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                df = pd.read_csv(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        return df if not df.empty else None
    except Exception:
        return None


def _save_csv(df: pd.DataFrame) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _DATA_FILE + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, _DATA_FILE)


def _merge(existing: Optional[pd.DataFrame], new_rows: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        merged = new_rows
    else:
        merged = pd.concat([existing, new_rows], ignore_index=True)
    merged["timestamp"] = pd.to_datetime(merged["timestamp"], errors="coerce")
    merged = merged.dropna(subset=["timestamp"])
    merged = merged.drop_duplicates(subset=["timestamp"], keep="last")
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    # Store naive ET strings (consistent with existing CSV format)
    merged["timestamp"] = merged["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return merged


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def backfill_status() -> dict:
    """Return basic info about the current CSV."""
    df = _load_csv()
    if df is None or df.empty:
        return {"rows": 0, "earliest": None, "latest": None, "sparse": True}
    rows = len(df)
    ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
    earliest = ts.min().isoformat() if not ts.empty else None
    latest = ts.max().isoformat() if not ts.empty else None
    return {
        "rows": rows,
        "earliest": earliest,
        "latest": latest,
        "sparse": rows < _MIN_BARS_WARNING,
    }


def run_backfill(
    days_back: int = 30,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Synchronous backfill.  Fetches up to `days_back` calendar days of history
    (market days only), merges into qqq_1m.csv.

    Returns a summary dict: {fetched_days, added_rows, total_rows, errors}.
    """
    client = _get_client()
    if client is None:
        return {"ok": False, "error": "alpaca_keys_missing"}

    today = date.today()
    days: list[date] = []
    for i in range(1, days_back + 1):
        d = today - timedelta(days=i)
        if _is_market_day(d):
            days.append(d)
    days.sort()  # oldest first

    existing = _load_csv()
    # Track which timestamps we already have (for dedup short-circuit)
    existing_ts_set: set = set()
    if existing is not None and not existing.empty and "timestamp" in existing.columns:
        existing_ts_set = set(existing["timestamp"].astype(str).tolist())

    all_new: list[pd.DataFrame] = []
    fetched_days = 0
    error_days = 0

    for day in days:
        # Skip if we already have a full day (rough check: at least 1 bar at 9:30)
        day_prefix = day.strftime("%Y-%m-%d 09:30")
        if day_prefix in existing_ts_set:
            continue

        if progress_cb:
            progress_cb(f"Fetching {day.isoformat()}…")

        day_df = _fetch_day(client, day)
        if day_df is None or day_df.empty:
            error_days += 1
        else:
            all_new.append(day_df)
            fetched_days += 1
            # Update set so duplicate days in same run are skipped
            for ts in day_df["timestamp"].astype(str).tolist():
                existing_ts_set.add(ts)

        time.sleep(_INTER_CHUNK_SLEEP)

    if not all_new:
        total = len(existing) if existing is not None else 0
        return {"ok": True, "fetched_days": 0, "added_rows": 0, "total_rows": total, "errors": error_days}

    new_df = pd.concat(all_new, ignore_index=True)
    merged = _merge(existing, new_df)
    added = len(merged) - (len(existing) if existing is not None else 0)
    _save_csv(merged)

    return {
        "ok": True,
        "fetched_days": fetched_days,
        "added_rows": max(added, 0),
        "total_rows": len(merged),
        "errors": error_days,
    }


async def run_backfill_async(
    days_back: int = 30,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> dict:
    """Async wrapper — runs the blocking backfill in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: run_backfill(days_back=days_back, progress_cb=progress_cb),
    )
