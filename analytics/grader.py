# analytics/grader.py

import os
import json
import logging
import pandas as pd
from datetime import timedelta
from pandas.errors import EmptyDataError

from core.paths import DATA_DIR
from core.data_service import get_market_dataframe
from core.debug import debug_log

PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")
EDGE_FILE = os.path.join(DATA_DIR, "edge_stats.json")


def confidence_band(conf):
    if conf < 0.6:
        return "low"
    elif conf < 0.75:
        return "medium"
    else:
        return "high"


def _safe_confidence_band(val):
    try:
        return confidence_band(float(val))
    except (TypeError, ValueError):
        return ""


def check_predictions(trade=None):
    if isinstance(trade, dict):
        try:
            trade_id = trade.get("trade_id")
            option_symbol = trade.get("option_symbol")
            entry_price = float(trade.get("entry_price", 0) or 0)
            exit_price = float(trade.get("exit_price", 0) or 0)
            pnl = trade.get("realized_pnl_dollars", trade.get("pnl"))
            if pnl is None:
                qty = trade.get("qty")
                try:
                    qty_val = float(qty) if qty is not None else 0
                except (TypeError, ValueError):
                    qty_val = 0
                pnl = (exit_price - entry_price) * qty_val * 100 if qty_val else None

            if exit_price > entry_price:
                actual_direction = "BULLISH"
            elif exit_price < entry_price:
                actual_direction = "BEARISH"
            else:
                actual_direction = "FLAT"

            predicted_direction = trade.get("predicted_direction") or trade.get("direction")
            prediction_confidence = trade.get("prediction_confidence")
            if prediction_confidence is None:
                prediction_confidence = trade.get("confidence")
            direction_prob = trade.get("direction_prob")
            edge_prob = trade.get("edge_prob")
            correct_prediction = (
                predicted_direction in {"BULLISH", "BEARISH"}
                and predicted_direction == actual_direction
            )

            debug_log(
                "prediction_graded_trade",
                trade_id=trade_id,
                option_symbol=option_symbol,
                predicted=predicted_direction,
                actual=actual_direction,
                pnl=pnl,
                confidence=prediction_confidence,
                direction_prob=direction_prob,
                edge_prob=edge_prob,
                correct=bool(correct_prediction),
            )
            return correct_prediction
        except Exception as e:
            logging.exception("trade_prediction_grade_error: %s", e)
            return None

    if not os.path.exists(PRED_FILE):
        return

    try:
        preds = pd.read_csv(PRED_FILE)
    except EmptyDataError:
        return
    except Exception as e:
        logging.warning("prediction_read_failed: %s", e)
        return

    if preds.empty or "time" not in preds.columns:
        return
    raw_len = len(preds)

    def _parse_pred_time(val):
        """Parse prediction timestamp to naive ET. Handles both tz-aware ISO strings
        and naive strings (already stored as ET). Using utc=True on naive strings would
        misinterpret them as UTC and shift by -5h on each grader run."""
        if pd.isna(val):
            return pd.NaT
        try:
            t = pd.to_datetime(val)
            if t.tzinfo is not None:
                return t.tz_convert("US/Eastern").tz_localize(None)
            return t  # already naive ET — no conversion needed
        except Exception:
            return pd.NaT

    preds["time"] = preds["time"].apply(_parse_pred_time)
    if bool(preds["time"].isna().all()):
        # Avoid wiping file if parsing fails across all rows.
        logging.warning("prediction_time_parse_failed_all")
        return
    preds = preds.dropna(subset=["time"])

    if preds.empty:
        # Avoid overwriting a non-empty file with only headers.
        if raw_len > 0:
            logging.warning("prediction_rows_dropped_after_parse", extra={"raw_len": raw_len})
        return

    # Ensure required columns exist
    if "checked" not in preds.columns:
        preds["checked"] = False

    if "actual" not in preds.columns:
        preds["actual"] = ""

    if "correct" not in preds.columns:
        preds["correct"] = 0

    if "high_hit" not in preds.columns:
        preds["high_hit"] = 0

    if "low_hit" not in preds.columns:
        preds["low_hit"] = 0

    if "price_at_check" not in preds.columns:
        preds["price_at_check"] = 0.0

    if "close_at_check" not in preds.columns:
        preds["close_at_check"] = 0.0

    if "confidence" in preds.columns:
        if "confidence_band" not in preds.columns:
            preds["confidence_band"] = ""
        # Ensure object dtype before assigning strings
        try:
            preds["confidence_band"] = preds["confidence_band"].astype("object")
        except Exception:
            pass
        mask = preds["confidence_band"].isna() | (preds["confidence_band"] == "")
        if mask.any():
            preds.loc[mask, "confidence_band"] = preds.loc[mask, "confidence"].apply(_safe_confidence_band)

    # Load price data
    df = get_market_dataframe()
    if df is None or df.empty:
        return

    df = df.reset_index()
    df.rename(columns={"index": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    if df.empty:
        return

    # Grade predictions
    for i, row in preds.iterrows():

        if bool(row.get("checked", False)):
            continue

        # Safe timeframe parsing
        try:
            tf = int(row["timeframe"])
        except Exception:
            continue

        target_time = row["time"] + timedelta(minutes=tf)

        # Round to nearest minute (prevents seconds mismatch bug)
        target_time = target_time.replace(second=0, microsecond=0)
        
        # --------------------------------
        # Only grade if timeframe has fully passed
        # --------------------------------
        current_time = pd.Timestamp.now(tz="US/Eastern").tz_localize(None)

        if current_time < target_time:
            continue

        # Find first available bar at or after target_time.
        future = df[df["timestamp"] >= target_time]
        if future.empty:
            continue

        future_high = future.iloc[0]["high"]
        future_low = future.iloc[0]["low"]
        future_close = future.iloc[0]["close"]

        predicted_high = row["high"]
        predicted_low = row["low"]

        # --------------------------------
        # Check range accuracy
        # --------------------------------
        high_hit = int(future_high >= predicted_high)
        low_hit = int(future_low <= predicted_low)

        # --------------------------------
        # Determine directional result
        # --------------------------------
        if high_hit and not low_hit:
            result = "bullish"
        elif low_hit and not high_hit:
            result = "bearish"
        elif high_hit and low_hit:
            result = "both"
        else:
            result = "range"

        direction_correct = int(result == row["direction"])

        # --------------------------------
        # Save results
        # --------------------------------
        preds.loc[i, "actual"] = result
        preds.loc[i, "correct"] = direction_correct
        preds.loc[i, "high_hit"] = high_hit
        preds.loc[i, "low_hit"] = low_hit
        preds.loc[i, "price_at_check"] = future_close
        preds.loc[i, "close_at_check"] = future_close
        preds.loc[i, "checked"] = True
        debug_log(
            "prediction_graded",
            timeframe=tf,
            predicted=row["direction"],
            actual=result,
            correct=direction_correct,
            high_hit=high_hit,
            low_hit=low_hit,
            price_at_check=round(float(future_close), 2)
        )

    graded = preds[preds["checked"] == True]

    update_edge_stats(graded)

    preds.to_csv(PRED_FILE, index=False)


def update_edge_stats(graded):

    if graded.empty:
        return

    stats = {}

    total = len(graded)
    wins = int(graded["correct"].sum())

    stats["overall"] = {
        "total": total,
        "wins": wins,
        "winrate": round(wins / total, 4)
    }

    tf_group = graded.groupby("timeframe")["correct"].agg(
        total="count",
        wins="sum"
    )

    stats["timeframes"] = {}

    for tf, row in tf_group.iterrows():
        stats["timeframes"][str(int(tf))] = {
            "total": int(row["total"]),
            "wins": int(row["wins"]),
            "winrate": round(row["wins"] / row["total"], 4)
        }

    if "regime" in graded.columns:
        regime_group = graded.groupby("regime")["correct"].agg(
            total="count",
            wins="sum"
        )

        stats["regimes"] = {}

        for regime, row in regime_group.iterrows():
            stats["regimes"][regime] = {
                "total": int(row["total"]),
                "wins": int(row["wins"]),
                "winrate": round(row["wins"] / row["total"], 4)
            }

    if "session" in graded.columns:
        session_group = graded.groupby("session")["correct"].agg(
            total="count",
            wins="sum"
        )

        stats["sessions"] = {}

        for session, row in session_group.iterrows():
            stats["sessions"][session] = {
                "total": int(row["total"]),
                "wins": int(row["wins"]),
                "winrate": round(row["wins"] / row["total"], 4)
            }

    with open(EDGE_FILE, "w") as f:
        json.dump(stats, f, indent=4)
