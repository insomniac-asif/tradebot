"""
backtest/data_fetcher.py
Downloads and caches stock + option bars from Alpaca.
Stock bars: monthly parquet chunks in backtest/cache/
Option bars: per-contract per-date parquet in backtest/cache/
"""
import os
import time
import logging
from datetime import date, timedelta

try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    # Fallback if dateutil not available
    class relativedelta:
        def __init__(self, months=0):
            self.months = months
        def __add__(self, d):
            import calendar
            m = d.month - 1 + self.months
            year = d.year + m // 12
            month = m % 12 + 1
            day = min(d.day, calendar.monthrange(year, month)[1])
            return d.replace(year=year, month=month, day=day)
        def __radd__(self, d):
            return self.__add__(d)

import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

RATE_LIMIT_SLEEP = 0.35  # seconds between Alpaca calls


def _get_alpaca_keys():
    """Read API keys from environment / .env."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Alpaca API keys not found in environment")
    return key, secret


def fetch_stock_bars(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch 1-minute stock bars for symbol between start_date and end_date.
    Uses monthly parquet cache. Returns combined DataFrame with DatetimeIndex (ET naive).
    Columns: open, high, low, close, volume, vwap (if available)
    """
    key, secret = _get_alpaca_keys()
    from alpaca.data.historical.stock import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = StockHistoricalDataClient(key, secret)
    start = pd.Timestamp(start_date, tz="America/New_York")
    end = pd.Timestamp(end_date, tz="America/New_York")

    # Build monthly chunks
    all_dfs = []
    cur = start.replace(day=1)
    while cur <= end:
        month_str = cur.strftime("%Y%m")
        cache_path = os.path.join(CACHE_DIR, f"stock_bars_{symbol}_{month_str}.parquet")

        month_end = (cur + relativedelta(months=1)) - timedelta(seconds=1)
        actual_start = max(cur, start)
        actual_end = min(month_end, end)

        if os.path.exists(cache_path):
            try:
                df = pd.read_parquet(cache_path)
                if not df.empty:
                    # Ensure tz-aware ET index for filtering
                    if df.index.tz is None:
                        df.index = df.index.tz_localize("America/New_York")
                    df_filtered = df[(df.index >= actual_start) & (df.index <= actual_end)]
                    if not df_filtered.empty:
                        all_dfs.append(df_filtered)
            except Exception as e:
                logging.warning("fetch_stock_bars_cache_read_failed: %s %s %s", symbol, month_str, e)
        else:
            print(f"  Fetching {symbol} stock bars {month_str}...")
            try:
                req = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=TimeFrame(1, TimeFrameUnit("Min")),
                    start=actual_start.isoformat(),
                    end=actual_end.isoformat(),
                    adjustment="all",
                )
                bars = client.get_stock_bars(req)
                time.sleep(RATE_LIMIT_SLEEP)
                if bars and symbol in bars.data and bars.data[symbol]:
                    records = [
                        {
                            "timestamp": b.timestamp,
                            "open": b.open,
                            "high": b.high,
                            "low": b.low,
                            "close": b.close,
                            "volume": b.volume,
                            "vwap": getattr(b, "vwap", None),
                        }
                        for b in bars.data[symbol]
                    ]
                    if records:
                        df = pd.DataFrame(records).set_index("timestamp")
                        df.index = pd.DatetimeIndex(df.index).tz_convert("America/New_York")
                        df.to_parquet(cache_path)
                        df_filtered = df[(df.index >= actual_start) & (df.index <= actual_end)]
                        if not df_filtered.empty:
                            all_dfs.append(df_filtered)
            except Exception as e:
                logging.warning("fetch_stock_bars_failed: %s %s %s", symbol, month_str, e)

        cur = cur + relativedelta(months=1)

    if not all_dfs:
        return pd.DataFrame()
    result = pd.concat(all_dfs)
    result = result[~result.index.duplicated(keep="first")].sort_index()
    # Strip tz to match what data_service returns (naive ET)
    if result.index.tz is not None:
        result.index = result.index.tz_localize(None)
    return result


def fetch_option_bars(contract: str, trade_date: str) -> pd.DataFrame:
    """
    Fetch 1-minute option bars for OCC contract symbol on trade_date.
    Cache: backtest/cache/option_bars_{contract}_{date}.parquet
    Returns DataFrame with DatetimeIndex (ET naive), columns: open, high, low, close, volume
    """
    safe_contract = contract.replace("/", "_")
    cache_path = os.path.join(CACHE_DIR, f"option_bars_{safe_contract}_{trade_date}.parquet")

    if os.path.exists(cache_path):
        try:
            df = pd.read_parquet(cache_path)
            return df
        except Exception:
            pass

    key, secret = _get_alpaca_keys()
    from alpaca.data.historical.option import OptionHistoricalDataClient
    from alpaca.data.requests import OptionBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = OptionHistoricalDataClient(key, secret)
    start = pd.Timestamp(trade_date + " 09:25:00", tz="America/New_York")
    end = pd.Timestamp(trade_date + " 16:05:00", tz="America/New_York")

    try:
        req = OptionBarsRequest(
            symbol_or_symbols=contract,
            timeframe=TimeFrame(1, TimeFrameUnit("Min")),
            start=start.isoformat(),
            end=end.isoformat(),
        )
        bars = client.get_option_bars(req)
        time.sleep(RATE_LIMIT_SLEEP)

        if bars and contract in bars.data and bars.data[contract]:
            records = [
                {
                    "timestamp": b.timestamp,
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": getattr(b, "volume", 0),
                }
                for b in bars.data[contract]
            ]
            df = pd.DataFrame(records).set_index("timestamp")
            df.index = pd.DatetimeIndex(df.index).tz_convert("America/New_York")
            df.index = df.index.tz_localize(None)
            df.to_parquet(cache_path)
            return df
        else:
            # Cache empty result so we don't re-fetch
            empty = pd.DataFrame()
            empty.to_parquet(cache_path)
            return empty
    except Exception as e:
        logging.warning("fetch_option_bars_failed: %s %s %s", contract, trade_date, e)
        return pd.DataFrame()


def build_occ_symbol(underlying: str, expiry: date, direction: str, strike: float) -> str:
    """
    Build OCC option symbol.
    Format: {UNDERLYING}{YY}{MM}{DD}{C/P}{STRIKE_8_DIGITS}
    Strike is in dollars, formatted as integer * 1000 padded to 8 digits.
    e.g. SPY 530.00 CALL 2024-03-01 -> SPY240301C00530000
    """
    side = "C" if direction.upper() in ("CALL", "BULLISH") else "P"
    strike_int = round(strike * 1000)
    date_str = expiry.strftime("%y%m%d")
    return f"{underlying}{date_str}{side}{strike_int:08d}"


def prefetch_stock_data(symbols: list, start_date: str, end_date: str):
    """Pre-download all stock bars for all symbols."""
    for sym in symbols:
        print(f"Pre-fetching {sym}...")
        fetch_stock_bars(sym, start_date, end_date)
