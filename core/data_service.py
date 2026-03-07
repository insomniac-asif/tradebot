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
                                # If fresh has fewer rows than the CSV, supplement
                                # with historical CSV data so regime/indicators have
                                # enough context (e.g. early in the trading day).
                                if len(fresh) < len(df):
                                    try:
                                        df_ts = pd.to_datetime(df["timestamp"], errors="coerce")
                                        fresh_ts = pd.to_datetime(fresh["timestamp"], errors="coerce")
                                        cutoff = fresh_ts.min()
                                        historical = df[df_ts < cutoff]
                                        if not historical.empty:
                                            fresh = pd.concat([historical, fresh], ignore_index=True)
                                    except Exception:
                                        pass
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
