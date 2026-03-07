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

    timestamp_iso = pd.to_datetime(timestamp).strftime("%Y-%m-%d %H:%M:%S")
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

        # Safely log the conviction signal (strftime format matches to_csv output)
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

    signals["time"] = pd.to_datetime(signals["time"], errors="coerce", format="mixed")
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
