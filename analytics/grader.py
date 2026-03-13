# analytics/grader.py

import os
import json
import logging
import pandas as pd
from datetime import timedelta

from core.paths import DATA_DIR
from core.data_service import get_market_dataframe
from core.debug import debug_log
from core.analytics_db import read_df, update, transaction, get_conn

PRED_FILE = os.path.join(DATA_DIR, "predictions.csv")  # legacy constant
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

    # Grade unchecked predictions from SQLite
    try:
        preds = read_df("SELECT * FROM predictions WHERE checked = 0")
    except Exception as e:
        logging.warning("prediction_read_failed: %s", e)
        return

    if preds.empty or "time" not in preds.columns:
        return

    def _parse_pred_time(val):
        """Parse prediction timestamp to naive ET."""
        if pd.isna(val):
            return pd.NaT
        try:
            t = pd.to_datetime(val)
            if t.tzinfo is not None:
                return t.tz_convert("US/Eastern").tz_localize(None)
            return t
        except Exception:
            return pd.NaT

    preds["time"] = preds["time"].apply(_parse_pred_time)
    preds = preds.dropna(subset=["time"])

    if preds.empty:
        return

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

    # Grade predictions and collect updates
    updates = []
    for i, row in preds.iterrows():

        try:
            tf = int(row["timeframe"])
        except Exception:
            continue

        target_time = row["time"] + timedelta(minutes=tf)
        target_time = target_time.replace(second=0, microsecond=0)

        current_time = pd.Timestamp.now(tz="US/Eastern").tz_localize(None)
        if current_time < target_time:
            continue

        future = df[df["timestamp"] >= target_time]
        if future.empty:
            continue

        future_high = future.iloc[0]["high"]
        future_low = future.iloc[0]["low"]
        future_close = future.iloc[0]["close"]

        predicted_high = row["high"]
        predicted_low = row["low"]

        high_hit = int(future_high >= predicted_high)
        low_hit = int(future_low <= predicted_low)

        if high_hit and not low_hit:
            result = "bullish"
        elif low_hit and not high_hit:
            result = "bearish"
        elif high_hit and low_hit:
            result = "both"
        else:
            result = "range"

        # Binary directional grading:
        # bullish correct if high_hit, bearish correct if low_hit,
        # range correct if neither hit. Handles "both" outcomes properly.
        pred_dir = row["direction"]
        if pred_dir == "bullish":
            direction_correct = int(high_hit == 1)
        elif pred_dir == "bearish":
            direction_correct = int(low_hit == 1)
        else:  # range
            direction_correct = int(high_hit == 0 and low_hit == 0)

        # Compute confidence_band if missing
        cb = row.get("confidence_band")
        if not cb or pd.isna(cb) or cb == "":
            cb = _safe_confidence_band(row.get("confidence"))

        updates.append({
            "row_id": int(row["id"]),
            "actual": result,
            "correct": direction_correct,
            "high_hit": high_hit,
            "low_hit": low_hit,
            "price_at_check": round(float(future_close), 4),
            "close_at_check": round(float(future_close), 4),
            "checked": 1,
            "confidence_band": cb if cb else None,
        })

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

    # Batch update graded predictions
    if updates:
        with transaction() as conn:
            for u in updates:
                row_id = u.pop("row_id")
                set_clause = ", ".join(f"{k} = ?" for k in u.keys())
                conn.execute(
                    f"UPDATE predictions SET {set_clause} WHERE id = ?",
                    list(u.values()) + [row_id],
                )

    # Update edge stats from all graded predictions
    graded = read_df("SELECT * FROM predictions WHERE checked = 1")
    update_edge_stats(graded)

    # Refresh learned predictor bias weights
    try:
        from analytics.predictor_optimizer import update_predictor_weights
        update_predictor_weights()
    except Exception:
        pass


def update_edge_stats(graded):

    if graded.empty:
        return

    graded["correct"] = pd.to_numeric(graded["correct"], errors="coerce").fillna(0)

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
        stats["timeframes"][str(int(float(tf)))] = {
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


def grade_trade(trade: dict) -> str:
    """
    Grade a completed trade A–F based on outcome quality.
    Only grades closed trades (must have exit_price and realized_pnl_pct).
    Returns '' for open/incomplete trades.
    """
    try:
        if trade.get("exit_price") is None or trade.get("realized_pnl_pct") is None:
            return ""

        pnl_pct = float(trade.get("realized_pnl_pct") or 0) * 100  # e.g. 30.7
        exit_reason = (trade.get("exit_reason") or "").lower()
        mae = float(trade.get("mae_pct") or trade.get("mae") or 0)
        mfe = float(trade.get("mfe_pct") or trade.get("mfe") or 0)

        # Clean exit bonus: hit target or trailing stop (trade managed well)
        clean_exit = exit_reason in ("take_profit", "trailing_stop", "target")
        # Bad exit: stopped out at max loss
        hard_stop = "stop" in exit_reason and "trailing" not in exit_reason

        # MFE/MAE ratio — how much of the best move was captured
        capture_ratio = (pnl_pct / (mfe * 100)) if mfe > 0.001 else None

        if pnl_pct >= 25 or (pnl_pct >= 15 and clean_exit):
            return "A"
        elif pnl_pct >= 10 or (pnl_pct >= 5 and clean_exit):
            return "B"
        elif pnl_pct >= 0:
            return "C"
        elif pnl_pct >= -20 and not hard_stop:
            return "D"
        else:
            return "F"
    except Exception:
        return ""
