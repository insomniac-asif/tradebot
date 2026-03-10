# Startup phase-gate validation (extracted from bot.py)

import os
import csv
import json
import pytz
import joblib

from core.paths import DATA_DIR
from analytics.prediction_stats import PRED_HEADERS
from interface.shared_state import BOT_TIMEZONE

CONVICTION_HEADERS = [
    "time", "direction", "impulse", "follow", "price",
    "fwd_5m", "fwd_10m", "fwd_5m_price", "fwd_5m_time", "fwd_5m_status",
    "fwd_10m_price", "fwd_10m_time", "fwd_10m_status",
]
LEGACY_CONVICTION_HEADERS = [
    "time", "direction", "impulse", "follow", "price", "fwd_5m", "fwd_10m"
]
PREDICTION_REQUIRED_HEADERS = [
    "time", "timeframe", "direction", "confidence", "high", "low",
    "regime", "volatility", "session", "actual", "correct", "checked"
]
ACCOUNT_REQUIRED_KEYS = [
    "balance", "starting_balance", "open_trade", "trade_log", "wins",
    "losses", "day_trades", "risk_per_trade", "max_trade_size",
    "daily_loss", "max_daily_loss", "last_trade_day"
]


def _read_csv_headers(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return None


def run_startup_phase_gates():
    errors = []

    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        if tz.zone != BOT_TIMEZONE:
            errors.append(f"timezone_invalid:{tz.zone}")
    except Exception as e:
        errors.append(f"timezone_error:{e}")

    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        errors.append("alpaca_api_keys_missing")

    conviction_file = os.path.join(DATA_DIR, "conviction_expectancy.csv")
    pred_file = os.path.join(DATA_DIR, "predictions.csv")

    conviction_headers = _read_csv_headers(conviction_file)
    if conviction_headers not in (CONVICTION_HEADERS, LEGACY_CONVICTION_HEADERS):
        errors.append("conviction_csv_header_invalid")

    pred_headers = _read_csv_headers(pred_file)
    if pred_headers is None:
        errors.append("predictions_csv_header_missing")
    else:
        missing_pred = [h for h in PREDICTION_REQUIRED_HEADERS if h not in pred_headers]
        if missing_pred:
            errors.append(f"predictions_csv_header_missing_fields:{','.join(missing_pred)}")
        if pred_headers != PRED_HEADERS:
            try:
                with open(pred_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(PRED_HEADERS)
            except Exception:
                errors.append("predictions_csv_header_reset_failed")

    direction_model_path = os.path.join(DATA_DIR, "direction_model.pkl")
    edge_model_path = os.path.join(DATA_DIR, "edge_model.pkl")
    for model_path, model_name in [
        (direction_model_path, "direction_model"),
        (edge_model_path, "edge_model"),
    ]:
        if not os.path.exists(model_path):
            continue
        try:
            joblib.load(model_path)
        except Exception as e:
            errors.append(f"{model_name}_load_error:{e}")

    account_file = os.path.join(DATA_DIR, "account.json")
    if not os.path.exists(account_file):
        errors.append("account_missing")
    else:
        try:
            with open(account_file, "r") as f:
                acc = json.load(f)
            missing_keys = [k for k in ACCOUNT_REQUIRED_KEYS if k not in acc]
            if missing_keys:
                errors.append(f"account_missing_keys:{','.join(missing_keys)}")
        except Exception as e:
            errors.append(f"account_read_error:{e}")

    return errors
