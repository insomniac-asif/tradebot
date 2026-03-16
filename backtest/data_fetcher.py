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

RATE_LIMIT_SLEEP = 0.5  # seconds between Alpaca calls (slightly slower = less OS throttling)

# Module-level client singletons — reuse one persistent HTTPS connection for all
# requests instead of opening a new TCP/SSL handshake per call (which triggers
# Windows Defender / router flood-detection when done at high volume).
_stock_client = None
_option_client = None


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


def _get_stock_client():
    """Return a shared StockHistoricalDataClient (created once, reused)."""
    global _stock_client
    if _stock_client is None:
        key, secret = _get_alpaca_keys()
        from alpaca.data.historical.stock import StockHistoricalDataClient
        _stock_client = StockHistoricalDataClient(key, secret)
    return _stock_client


def _get_option_client():
    """Return a shared OptionHistoricalDataClient (created once, reused)."""
    global _option_client
    if _option_client is None:
        key, secret = _get_alpaca_keys()
        from alpaca.data.historical.option import OptionHistoricalDataClient
        _option_client = OptionHistoricalDataClient(key, secret)
    return _option_client


def _reset_option_client():
    """Force-recreate the option client (call after a connection error)."""
    global _option_client
    _option_client = None


def _fetch_option_bars_via_curl(
    contracts: list,
    trade_date: str,
) -> dict:
    """
    Fetch option bars for multiple contracts using curl.exe (Windows WinHTTP stack).

    This bypasses Python's SSL socket layer entirely, which is useful when an
    antivirus/network filter blocks python.exe's HTTPS connections but allows
    system tools like curl.exe through.

    Returns dict of {contract: list_of_bar_dicts} (raw, no parquet written here).
    """
    import subprocess
    import json

    key, secret = _get_alpaca_keys()

    start_ts = f"{trade_date}T09:25:00-04:00"
    end_ts   = f"{trade_date}T16:05:00-04:00"
    symbols_str = ",".join(contracts)

    base_url = "https://data.alpaca.markets/v2/options/bars"
    all_bars: dict = {c: [] for c in contracts}
    page_token = None

    while True:
        params = (
            f"symbols={symbols_str}"
            f"&timeframe=1Min"
            f"&start={start_ts}"
            f"&end={end_ts}"
            f"&feed=indicative"
            f"&limit=10000"
        )
        if page_token:
            params += f"&page_token={page_token}"

        url = f"{base_url}?{params}"
        cmd = [
            "curl.exe",
            "-s",               # silent
            "--max-time", "30", # 30s timeout
            "-H", f"APCA-API-KEY-ID: {key}",
            "-H", f"APCA-API-SECRET-KEY: {secret}",
            url,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=35,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logging.warning("curl.exe failed: %s", e)
            return {}

        if result.returncode != 0 or not result.stdout:
            logging.warning(
                "curl.exe non-zero exit %d for date=%s: %s",
                result.returncode, trade_date, result.stderr[:200],
            )
            return {}

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logging.warning("curl.exe JSON parse error date=%s: %s", trade_date, e)
            return {}

        bars_data = data.get("bars") or {}
        for contract, bars in bars_data.items():
            if contract in all_bars:
                all_bars[contract].extend(bars)

        page_token = data.get("next_page_token")
        if not page_token:
            break
        # Small sleep between pagination calls
        time.sleep(0.1)

    return all_bars


def _curl_available() -> bool:
    """Check whether curl.exe is available on this system."""
    import subprocess
    try:
        r = subprocess.run(["curl.exe", "--version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


# Cached result of curl availability check
_CURL_AVAILABLE: bool | None = None


def fetch_stock_bars(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch 1-minute stock bars for symbol between start_date and end_date.
    Uses monthly parquet cache. Returns combined DataFrame with DatetimeIndex (ET naive).
    Columns: open, high, low, close, volume, vwap (if available)
    """
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = _get_stock_client()
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

    from alpaca.data.requests import OptionBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = _get_option_client()
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
    except (PermissionError, ConnectionAbortedError, ConnectionResetError) as e:
        # OS-level socket block. Reset client so next retry gets a fresh socket.
        _reset_option_client()
        raise
    except OSError as e:
        err_str = str(e).lower()
        if any(k in err_str for k in ("permission denied", "connection aborted",
                                       "connection reset", "broken pipe",
                                       "connection refused")):
            _reset_option_client()
            raise  # Let prefetch script handle with backoff
        logging.warning("fetch_option_bars_failed: %s %s %s", contract, trade_date, e)
        return pd.DataFrame()
    except Exception as e:
        logging.warning("fetch_option_bars_failed: %s %s %s", contract, trade_date, e)
        return pd.DataFrame()


def fetch_option_bars_batch(
    contracts: list,
    trade_date: str,
) -> dict:
    """
    Fetch 1-minute option bars for MULTIPLE contracts on the same date.
    Writes each contract's data to its own parquet cache file.
    Returns dict of {contract: DataFrame}.

    Strategy:
    1. Try curl.exe (WinHTTP stack) — bypasses Python SSL blocks from antivirus
    2. Fall back to Alpaca SDK (requests/socket) if curl unavailable
    """
    global _CURL_AVAILABLE
    if _CURL_AVAILABLE is None:
        _CURL_AVAILABLE = _curl_available()

    # Filter out already-cached contracts
    to_fetch = []
    results = {}
    for contract in contracts:
        safe = contract.replace("/", "_")
        cache_path = os.path.join(CACHE_DIR, f"option_bars_{safe}_{trade_date}.parquet")
        if os.path.exists(cache_path):
            try:
                results[contract] = pd.read_parquet(cache_path)
                continue
            except Exception:
                pass
        to_fetch.append(contract)

    if not to_fetch:
        return results

    # ── Method 1: curl.exe (WinHTTP — bypasses Python SSL blocking) ────────
    if _CURL_AVAILABLE:
        raw = _fetch_option_bars_via_curl(to_fetch, trade_date)
        # No rate-limit sleep needed for curl — WinHTTP manages its own connection
        for contract in to_fetch:
            safe = contract.replace("/", "_")
            cache_path = os.path.join(CACHE_DIR, f"option_bars_{safe}_{trade_date}.parquet")
            bar_list = raw.get(contract, [])
            
            if bar_list:
                df = pd.DataFrame(bar_list)
                col_map = {"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
                df.rename(columns=col_map, inplace=True)
                # Keep only known columns if Alpaca adds extras
                df = df.reindex(columns=[c for c in col_map.values() if c in df.columns])
                
                df.set_index("timestamp", inplace=True)
                df.index = pd.to_datetime(df.index, utc=True).tz_convert("America/New_York").tz_localize(None)
                df.to_parquet(cache_path)
                results[contract] = df
            else:
                # Skip writing empty parquet — each AV-scanned write costs ~170ms
                # on Windows. Contracts with no data will be retried on next run
                # (cheap: curl call is fast). Only cache files with real bars.
                results[contract] = pd.DataFrame()
        return results

    # ── Method 2: Alpaca SDK (Python requests — may be blocked by AV) ──────
    from alpaca.data.requests import OptionBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    client = _get_option_client()
    start = pd.Timestamp(trade_date + " 09:25:00", tz="America/New_York")
    end   = pd.Timestamp(trade_date + " 16:05:00", tz="America/New_York")

    try:
        req = OptionBarsRequest(
            symbol_or_symbols=to_fetch,
            timeframe=TimeFrame(1, TimeFrameUnit("Min")),
            start=start.isoformat(),
            end=end.isoformat(),
        )
        bars = client.get_option_bars(req)
        time.sleep(RATE_LIMIT_SLEEP)

        for contract in to_fetch:
            safe = contract.replace("/", "_")
            cache_path = os.path.join(CACHE_DIR, f"option_bars_{safe}_{trade_date}.parquet")
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
                results[contract] = df
            else:
                empty = pd.DataFrame()
                empty.to_parquet(cache_path)
                results[contract] = empty

    except (PermissionError, ConnectionAbortedError, ConnectionResetError, OSError) as e:
        _reset_option_client()
        raise
    except Exception as e:
        logging.warning("fetch_option_bars_batch_failed: date=%s n=%d: %s", trade_date, len(to_fetch), e)

    return results





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
