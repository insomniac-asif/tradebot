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
