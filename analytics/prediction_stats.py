# analytics/prediction_stats.py

import os
import logging
import pandas as pd
from core.paths import DATA_DIR
from core.analytics_db import insert, read_df, delete_all, row_count

from signals.session_classifier import classify_session

# Legacy constants — kept for backward compat (scripts import these)
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
    """No-op: schema is ensured at startup via analytics_db.init_db()."""
    pass


def log_prediction(pred, regime, volatility, symbol=None):

    if pred is None:
        return

    # Normalize time to naive ET ISO string.
    try:
        t = pd.to_datetime(pred.get("time"))
        if t.tzinfo is not None:
            t = t.tz_convert("US/Eastern").tz_localize(None)
        pred_time = t.isoformat()
    except Exception:
        pred_time = str(pred.get("time"))

    session = classify_session(pred_time)

    try:
        confidence = float(pred["confidence"])
    except (TypeError, ValueError, KeyError):
        confidence = None

    try:
        high = float(pred["high"])
    except (TypeError, ValueError, KeyError):
        high = None

    try:
        low = float(pred["low"])
    except (TypeError, ValueError, KeyError):
        low = None

    insert("predictions", {
        "time": pred_time,
        "symbol": symbol.upper(),
        "timeframe": str(pred["timeframe"]),
        "direction": pred["direction"],
        "confidence": confidence,
        "high": high,
        "low": low,
        "regime": regime,
        "volatility": volatility,
        "session": session,
        "actual": None,
        "correct": 0,
        "checked": 0,
        "high_hit": 0,
        "low_hit": 0,
        "price_at_check": None,
        "close_at_check": None,
        "confidence_band": None,
    })


def calculate_accuracy():

    try:
        df = read_df("SELECT * FROM predictions WHERE checked = 1")
    except Exception:
        return None

    if df.empty:
        return None

    result = {}

    for timeframe in [30, 60]:

        subset = df[df["timeframe"].astype(str) == str(timeframe)]

        if len(subset) == 0:
            result[timeframe] = (0, 0, 0)
            continue

        total = len(subset)
        correct = int(subset["correct"].sum())
        accuracy = (correct / total) * 100

        result[timeframe] = (total, correct, round(accuracy, 2))

    # Confidence reliability
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
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
