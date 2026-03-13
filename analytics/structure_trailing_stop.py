"""
analytics/structure_trailing_stop.py
Market-structure-aware trailing stop that sets stop levels at actual swing
points instead of fixed percentages.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _col(df, candidates):
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def compute_structure_stop(
    df,
    direction: str,
    pivot_lookback: int = 5,
    increment_factor: float = 0.5,
) -> dict:
    """Compute a structure-aware trailing stop from swing highs/lows.

    Parameters
    ----------
    df : DataFrame
        1-min OHLCV bars with columns like close/Close, high/High, low/Low.
    direction : str
        "BULLISH" or "BEARISH".
    pivot_lookback : int
        Number of bars on each side to confirm a swing point.
    increment_factor : float
        Fraction of new high/low movement to advance the stop.

    Returns
    -------
    dict with keys: structure_stop, structure_trend, swing_high, swing_low.
    All values may be None if structure cannot be determined.
    """
    _none_result: dict = {
        "structure_stop": None,
        "structure_trend": None,
        "swing_high": None,
        "swing_low": None,
    }
    try:
        min_bars = pivot_lookback * 3
        if df is None or len(df) < min_bars:
            return _none_result

        high_col = _col(df, ["high", "High", "HIGH"])
        low_col = _col(df, ["low", "Low", "LOW"])
        close_col = _col(df, ["close", "Close", "CLOSE"])
        if high_col is None or low_col is None or close_col is None:
            return _none_result

        highs = df[high_col].values
        lows = df[low_col].values
        closes = df[close_col].values
        n = len(highs)

        # --- Find swing highs ---
        swing_highs: list[tuple[int, float]] = []
        for i in range(pivot_lookback, n - pivot_lookback):
            window = highs[i - pivot_lookback: i + pivot_lookback + 1]
            if highs[i] == max(window):
                swing_highs.append((i, float(highs[i])))
        swing_highs = swing_highs[-5:]  # keep last 5

        # --- Find swing lows ---
        swing_lows: list[tuple[int, float]] = []
        for i in range(pivot_lookback, n - pivot_lookback):
            window = lows[i - pivot_lookback: i + pivot_lookback + 1]
            if lows[i] == min(window):
                swing_lows.append((i, float(lows[i])))
        swing_lows = swing_lows[-5:]  # keep last 5

        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return _none_result

        latest_sh = swing_highs[-1][1]
        prev_sh = swing_highs[-2][1]
        latest_sl = swing_lows[-1][1]
        prev_sl = swing_lows[-2][1]

        # --- Detect structure direction ---
        if latest_sh > prev_sh and latest_sl > prev_sl:
            structure_trend = "bullish"
        elif latest_sh < prev_sh and latest_sl < prev_sl:
            structure_trend = "bearish"
        else:
            # Neutral — no clear structure to trail
            return {
                "structure_stop": None,
                "structure_trend": "neutral",
                "swing_high": latest_sh,
                "swing_low": latest_sl,
            }

        # --- Compute trailing stop ---
        current_close = float(closes[-1])
        current_high = float(highs[-1])
        current_low = float(lows[-1])

        if direction == "BULLISH":
            stop = latest_sl
            # Advance stop if current high makes a new trailing maximum
            if len(swing_highs) >= 2:
                prev_high_val = swing_highs[-2][1]
                if current_high > prev_high_val:
                    advance = increment_factor * (current_high - prev_high_val)
                    stop = stop + advance
            # Stop must only move up, never down — but since we compute fresh
            # each call, we just ensure it's below current close
            if stop >= current_close:
                return {
                    "structure_stop": None,
                    "structure_trend": structure_trend,
                    "swing_high": latest_sh,
                    "swing_low": latest_sl,
                }
        elif direction == "BEARISH":
            stop = latest_sh
            # Lower stop if current low makes a new trailing minimum
            if len(swing_lows) >= 2:
                prev_low_val = swing_lows[-2][1]
                if current_low < prev_low_val:
                    lower = increment_factor * (prev_low_val - current_low)
                    stop = stop - lower
            # Stop must be above current close for BEARISH
            if stop <= current_close:
                return {
                    "structure_stop": None,
                    "structure_trend": structure_trend,
                    "swing_high": latest_sh,
                    "swing_low": latest_sl,
                }
        else:
            return _none_result

        return {
            "structure_stop": float(stop),
            "structure_trend": structure_trend,
            "swing_high": latest_sh,
            "swing_low": latest_sl,
        }
    except Exception:
        logger.exception("compute_structure_stop error")
        return _none_result
