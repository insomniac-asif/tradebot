# analytics/prediction_stats.py

import os
import logging
import pandas as pd
import csv
from core.paths import DATA_DIR

from signals.session_classifier import classify_session

PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")
PRED_HEADERS = [
    "time",
    "symbol",
    "timeframe",
    "direction",
    "confidence",
    "high",
    "low",
    "regime",
    "volatility",
    "session",
    "actual",
    "correct",
    "checked",
    "high_hit",
    "low_hit",
    "price_at_check",
    "close_at_check",
    "confidence_band",
]


def ensure_prediction_file():
    if os.path.exists(PRED_FILE) and os.path.getsize(PRED_FILE) > 0:
        try:
            with open(PRED_FILE, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                raise ValueError("empty_file")
            header = rows[0]
            if header != PRED_HEADERS:
                now = pd.Timestamp.now(tz="US/Eastern").tz_localize(None)
                cutoff = now - pd.Timedelta(days=30)
                padded = []
                for row in rows[1:]:
                    if not row:
                        continue
                    if row[0] == "time":
                        continue
                    ts = pd.to_datetime(row[0], errors="coerce")
                    # If timestamp is parseable and stale, drop it.
                    # If unparseable, keep the row to avoid silent data loss.
                    if not pd.isna(ts):
                        if ts < cutoff:
                            continue
                    new_row = row[:len(PRED_HEADERS)]
                    if len(new_row) < len(PRED_HEADERS):
                        new_row += [""] * (len(PRED_HEADERS) - len(new_row))
                    padded.append(new_row)
                with open(PRED_FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(PRED_HEADERS)
                    writer.writerows(padded)
        except Exception as e:
            logging.warning("ensure_prediction_file_failed: %s", e)
        return

    with open(PRED_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(PRED_HEADERS)

def log_prediction(pred, regime, volatility, symbol="SPY"):

    if pred is None:
        return

    ensure_prediction_file()

    # Normalize time to naive ET ISO string. Storing tz-aware strings causes the
    # grader to shift timestamps on each rewrite cycle (naive → treated as UTC → -5h).
    try:
        t = pd.to_datetime(pred.get("time"))
        if t.tzinfo is not None:
            t = t.tz_convert("US/Eastern").tz_localize(None)
        pred_time = t.isoformat()
    except Exception:
        pred_time = str(pred.get("time"))

    session = classify_session(pred_time)

    with open(PRED_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            pred_time,
            symbol.upper(),
            pred["timeframe"],
            pred["direction"],
            pred["confidence"],
            pred["high"],
            pred["low"],
            regime,
            volatility,
            session,
            "",
            0,
            False,
            "",
            "",
            "",
            "",
            ""
        ])


def calculate_accuracy():

    if not os.path.exists(PRED_FILE):
        return None
 
    try:
        df = pd.read_csv(PRED_FILE)
    except:
        return None

    if df.empty:
        return None

    # Ensure required columns exist
    if "checked" not in df.columns:
        return None

    df = df[df["checked"] == True]

    if len(df) == 0:
        return None

    result = {}

    for timeframe in [30, 60]:

        subset = df[df["timeframe"] == timeframe]

        if len(subset) == 0:
            result[timeframe] = (0, 0, 0)
            continue

        total = len(subset)
        correct = subset["correct"].sum()
        accuracy = (correct / total) * 100

        result[timeframe] = (total, correct, round(accuracy, 2))

    # Confidence reliability
    high_conf = df[df["confidence"] >= 0.65]
    low_conf = df[df["confidence"] < 0.50]

    def conf_acc(sub):
        if len(sub) == 0:
            return 0
        return round((sub["correct"].sum() / len(sub)) * 100, 2)

    return {
        "30": result.get(30, (0, 0, 0)),
        "60": result.get(60, (0, 0, 0)),
        "high_conf": conf_acc(high_conf),
        "low_conf": conf_acc(low_conf)
    }
