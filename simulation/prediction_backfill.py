"""
simulation/prediction_backfill.py

Late-start prediction backfill: when the bot starts after market open,
replays the prediction pipeline for each missed 10-minute slot so the
prediction log in SQLite has no gaps.

Also provides a prediction lock mechanism that stops new predictions
from being written after 16:00 ET (market close).
"""

import logging
import os
from datetime import datetime, date, timedelta

import pytz

ET = pytz.timezone("US/Eastern")

# ---------------------------------------------------------------------------
# Prediction Lock — prevents writes after market close
# ---------------------------------------------------------------------------

_PREDICTION_LOCK_DATE: date | None = None


def is_prediction_locked() -> bool:
    """Returns True if predictions are locked for today (after 16:00 ET)."""
    global _PREDICTION_LOCK_DATE
    now_et = datetime.now(ET)
    today = now_et.date()

    if _PREDICTION_LOCK_DATE == today:
        return True

    if now_et.hour >= 16:
        _PREDICTION_LOCK_DATE = today
        logging.error("predictions_locked: date=%s", today.isoformat())
        return True

    return False


def reset_prediction_lock() -> None:
    """Reset the lock (for testing or new-day reset)."""
    global _PREDICTION_LOCK_DATE
    _PREDICTION_LOCK_DATE = None


# ---------------------------------------------------------------------------
# Backfill missed predictions
# ---------------------------------------------------------------------------

def backfill_missed_predictions() -> dict:
    """
    Detect gap between market open and now, replay predictions for missed
    10-minute slots. Uses the same make_prediction() + log_prediction() path
    as the live forecast_watcher.

    Returns summary dict: {total_slots, predictions_generated, symbols}
    """
    try:
        now_et = datetime.now(ET)

        # Skip weekends (no market data)
        if now_et.weekday() >= 5:
            return {"total_slots": 0, "predictions_generated": 0, "reason": "weekend"}

        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

        # Too early — market hasn't opened yet
        if now_et < market_open:
            return {"total_slots": 0, "predictions_generated": 0, "reason": "before_market_open"}

        # If bot started within first 10 minutes, no backfill needed
        if now_et <= market_open + timedelta(minutes=10):
            return {"total_slots": 0, "predictions_generated": 0, "reason": "near_market_open"}

        # Find existing prediction timestamps for today to detect ALL gaps
        existing_slots = _get_prediction_slots_today()

        # Cap gap_end at market close (don't backfill beyond 16:00)
        gap_end = min(now_et, market_close) - timedelta(minutes=1)

        # Generate all expected 10-minute slots from 9:40 to gap_end
        first_slot = market_open + timedelta(minutes=10)
        all_expected = []
        current = first_slot
        while current <= gap_end:
            if market_open <= current <= market_close:
                all_expected.append(current)
            current += timedelta(minutes=10)

        # Find missing slots (expected but no prediction exists within that window)
        slots = [s for s in all_expected if s not in existing_slots]

        if not slots:
            return {"total_slots": 0, "predictions_generated": 0, "reason": "no_gap"}

        # Load symbol registry
        from core.data_service import _load_symbol_registry, get_symbol_dataframe
        registry = _load_symbol_registry()
        if not registry:
            return {"total_slots": 0, "predictions_generated": 0, "reason": "no_symbol_registry"}

        symbols = [s.upper() for s in registry]

        # Load dataframes for all symbols
        sym_dfs = {}
        for sym in symbols:
            try:
                sym_df = get_symbol_dataframe(sym)
                if sym_df is not None and len(sym_df) > 30:
                    sym_dfs[sym] = sym_df
            except Exception:
                continue

        if not sym_dfs:
            return {"total_slots": 0, "predictions_generated": 0, "reason": "no_dataframes"}

        # Generate predictions for missing slots
        from signals.predictor import make_prediction
        from signals.regime import get_regime
        from signals.volatility import volatility_state
        from analytics.prediction_stats import log_prediction

        total_predictions = 0

        for slot_time in slots:
            for sym, sym_df in sym_dfs.items():
                try:
                    # Slice df up to this slot time (simulate having data only up to that point)
                    # The dataframe may have timestamp as index or as a column
                    slot_naive = slot_time.replace(tzinfo=None) if slot_time.tzinfo else slot_time
                    if "timestamp" in sym_df.columns:
                        ts_col = sym_df["timestamp"]
                        if hasattr(ts_col.iloc[0], "tzinfo") and ts_col.iloc[0].tzinfo is not None:
                            slot_cmp = slot_time if slot_time.tzinfo else ET.localize(slot_time)
                        else:
                            slot_cmp = slot_naive
                        sliced = sym_df[ts_col <= slot_cmp]
                    elif sym_df.index.name and "time" in sym_df.index.name.lower():
                        # Timestamp is the index (common from get_symbol_dataframe)
                        idx = sym_df.index
                        if hasattr(idx, "tz") and idx.tz is not None:
                            slot_cmp = slot_time if slot_time.tzinfo else ET.localize(slot_time)
                        else:
                            slot_cmp = slot_naive
                        sliced = sym_df[idx <= slot_cmp]
                    else:
                        sliced = sym_df

                    if sliced is None or len(sliced) < 30:
                        continue

                    pred = make_prediction(10, sliced)
                    if pred is None:
                        continue

                    # Override prediction time with the slot time
                    pred["time"] = slot_time.isoformat()

                    regime = get_regime(sliced)
                    vola = volatility_state(sliced)

                    log_prediction(pred, regime, vola, symbol=sym)
                    total_predictions += 1
                except Exception:
                    continue

        logging.error(
            "prediction_backfill_complete: slots=%d predictions=%d symbols=%d gap=%s_to_%s",
            len(slots), total_predictions, len(sym_dfs),
            slots[0].strftime("%H:%M") if slots else "N/A",
            slots[-1].strftime("%H:%M") if slots else "N/A",
        )

        return {
            "total_slots": len(slots),
            "predictions_generated": total_predictions,
            "symbols": list(sym_dfs.keys()),
            "gap_start": slots[0].isoformat() if slots else None,
            "gap_end": slots[-1].isoformat() if slots else None,
        }

    except Exception:
        logging.exception("prediction_backfill_error")
        return {"total_slots": 0, "predictions_generated": 0, "reason": "error"}


def _get_prediction_slots_today() -> set:
    """Return the set of 10-min slot datetimes that already have predictions today."""
    try:
        from core.analytics_db import get_conn
        today_str = datetime.now(ET).strftime("%Y-%m-%d")
        conn = get_conn()
        try:
            cursor = conn.execute(
                "SELECT time FROM predictions WHERE time LIKE ?",
                (f"{today_str}%",),
            )
            slots = set()
            for row in cursor.fetchall():
                try:
                    dt = datetime.fromisoformat(str(row[0]))
                    if dt.tzinfo is None:
                        dt = ET.localize(dt)
                    # Round down to 10-min boundary
                    slot = dt.replace(
                        minute=(dt.minute // 10) * 10,
                        second=0,
                        microsecond=0,
                    )
                    slots.add(slot)
                except Exception:
                    continue
            return slots
        finally:
            conn.close()
    except Exception:
        pass
    return set()
