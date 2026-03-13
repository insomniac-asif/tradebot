import os
import pandas as pd
from datetime import timedelta
from core.paths import DATA_DIR
from core.data_service import get_market_dataframe
from core.analytics_db import insert, read_df, transaction, scalar

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
    """No-op: schema is ensured at startup via analytics_db.init_db()."""
    pass


def get_last_logged_time():
    return scalar("SELECT MAX(time) FROM conviction_expectancy")

# ==================================
# 1. LOG SIGNAL
# ==================================

def log_conviction_signal(df, direction, impulse, follow):
    if df is None or df.empty:
        print("No data to log for conviction signal.")
        return

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
    # Normalize both to isoformat before comparing
    try:
        last_logged_normalized = pd.to_datetime(last_logged_time).isoformat() if last_logged_time else None
    except Exception:
        last_logged_normalized = None
    if last_logged_normalized == timestamp_iso:
        return

    insert("conviction_expectancy", {
        "time": timestamp_iso,
        "direction": str(direction),
        "impulse": round(float(impulse), 3),
        "follow": round(float(follow), 3),
        "price": float(last["close"]),
        "fwd_5m": None,
        "fwd_10m": None,
        "fwd_5m_price": None,
        "fwd_5m_time": None,
        "fwd_5m_status": None,
        "fwd_10m_price": None,
        "fwd_10m_time": None,
        "fwd_10m_status": None,
    })
    print(f"Logged conviction signal at {timestamp}.")


# ==================================
# 2. UPDATE EXPECTANCY
# ==================================

def update_expectancy(df=None):
    if df is None:
        df = get_market_dataframe()

    if df is None:
        print("No data available or file missing.")
        return

    signals = read_df(
        "SELECT * FROM conviction_expectancy WHERE fwd_5m_status != 'filled' OR fwd_10m_status != 'filled' OR fwd_5m_status IS NULL OR fwd_10m_status IS NULL"
    )

    if signals.empty:
        return

    signals["time"] = pd.to_datetime(signals["time"], errors="coerce", format="mixed")
    signals = signals.dropna(subset=["time"])

    # Force numeric types safely
    signals["price"] = pd.to_numeric(signals["price"], errors="coerce")

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

    with transaction() as conn:
        for _, row in signals.iterrows():
            if bool(pd.isna(row["time"])) or bool(pd.isna(row["price"])):
                continue

            base_price = row["price"]
            if bool(pd.isna(base_price)):
                continue

            row_id = int(row["id"])
            updates = {}

            future_5 = row["time"] + timedelta(minutes=5)
            future_10 = row["time"] + timedelta(minutes=10)

            fwd_5m_val = row.get("fwd_5m")
            if fwd_5m_val is None or bool(pd.isna(fwd_5m_val)) or row.get("fwd_5m_status") != "filled":
                price_5, ts_5, status_5 = _select_future(future_5)
                if price_5 is not None:
                    updates["fwd_5m_price"] = float(price_5)
                    if ts_5 is not None:
                        updates["fwd_5m_time"] = str(pd.Timestamp(ts_5))
                    updates["fwd_5m"] = float(price_5 - base_price)
                updates["fwd_5m_status"] = status_5

            fwd_10m_val = row.get("fwd_10m")
            if fwd_10m_val is None or bool(pd.isna(fwd_10m_val)) or row.get("fwd_10m_status") != "filled":
                price_10, ts_10, status_10 = _select_future(future_10)
                if price_10 is not None:
                    updates["fwd_10m_price"] = float(price_10)
                    if ts_10 is not None:
                        updates["fwd_10m_time"] = str(pd.Timestamp(ts_10))
                    updates["fwd_10m"] = float(price_10 - base_price)
                updates["fwd_10m_status"] = status_10

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                conn.execute(
                    f"UPDATE conviction_expectancy SET {set_clause} WHERE id = ?",
                    list(updates.values()) + [row_id],
                )

    print("Conviction expectancy updated successfully.")


def get_conviction_expectancy_stats():

    try:
        df = read_df(
            "SELECT * FROM conviction_expectancy WHERE fwd_5m_status = 'filled' AND fwd_10m_status = 'filled'"
        )
    except Exception:
        return None

    if not isinstance(df, pd.DataFrame) or df.empty:
        print("No data available in conviction expectancy.")
        return None

    df["fwd_5m"] = pd.to_numeric(df["fwd_5m"], errors="coerce")
    df["fwd_10m"] = pd.to_numeric(df["fwd_10m"], errors="coerce")
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
