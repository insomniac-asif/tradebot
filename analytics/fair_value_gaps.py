"""
analytics/fair_value_gaps.py
Fair Value Gap (FVG) detection and feature computation.

No state, no mutation, no side effects. Every public function
returns a safe default on error and is wrapped in try/except.
"""
import logging
from typing import Optional


def _col(df, *candidates):
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _mark_mitigated(gap: dict) -> bool:
    """Mark gap as mitigated and return True (for use in list comprehension filter)."""
    gap["mitigated"] = True
    return True


def detect_fvgs(df, max_gaps: int = 20) -> list[dict]:
    """
    Detect unmitigated Fair Value Gaps in a price DataFrame.

    Bullish FVG: low[i] > high[i-2]  (gap up)
    Bearish FVG: high[i] < low[i-2]  (gap down)

    Returns most recent ``max_gaps`` unmitigated gaps.
    """
    try:
        high_col = _col(df, "high", "High")
        low_col = _col(df, "low", "Low")
        if high_col is None or low_col is None:
            return []

        highs = df[high_col].values
        lows = df[low_col].values
        n = len(df)
        if n < 3:
            return []

        gaps = []
        for i in range(2, n):
            if lows[i] > highs[i - 2]:
                gaps.append({
                    "type": "bull",
                    "top": float(lows[i]),
                    "bottom": float(highs[i - 2]),
                    "bar_idx": i,
                    "mitigated": False,
                })
            elif highs[i] < lows[i - 2]:
                gaps.append({
                    "type": "bear",
                    "top": float(lows[i - 2]),
                    "bottom": float(highs[i]),
                    "bar_idx": i,
                    "mitigated": False,
                })

        # Check mitigation — single forward pass instead of per-gap scan.
        # Gaps created at bar_idx are mitigatable starting at bar_idx+1.
        active_bull = []
        active_bear = []
        gaps.sort(key=lambda g: g["bar_idx"])
        gap_iter = iter(gaps)
        next_gap = next(gap_iter, None)
        for j in range(2, n):
            # Add gaps whose bar_idx < j (so mitigation starts at j = bar_idx+1)
            while next_gap is not None and next_gap["bar_idx"] < j:
                if next_gap["type"] == "bull":
                    active_bull.append(next_gap)
                else:
                    active_bear.append(next_gap)
                next_gap = next(gap_iter, None)
            # Check mitigation for active gaps
            if active_bull:
                active_bull = [
                    g for g in active_bull
                    if not (lows[j] <= g["top"] and _mark_mitigated(g))
                ]
            if active_bear:
                active_bear = [
                    g for g in active_bear
                    if not (highs[j] >= g["bottom"] and _mark_mitigated(g))
                ]

        unmitigated = [g for g in gaps if not g["mitigated"]]
        return unmitigated[-max_gaps:]

    except Exception:
        logging.exception("detect_fvgs_error")
        return []


def compute_fvg_features(df, max_gaps: int = 20) -> dict:
    """
    Compute 5 FVG-derived features for ML snapshot.

    Keys returned:
      fvg_nearest_bull_dist  – pct distance to nearest bull FVG top
      fvg_nearest_bear_dist  – pct distance to nearest bear FVG bottom
      fvg_bull_count         – unmitigated bull FVGs below current close
      fvg_bear_count         – unmitigated bear FVGs above current close
      fvg_imbalance          – bull / (bull + bear), 0.5 if none
    """
    defaults = {
        "fvg_nearest_bull_dist": None,
        "fvg_nearest_bear_dist": None,
        "fvg_bull_count": None,
        "fvg_bear_count": None,
        "fvg_imbalance": None,
    }
    try:
        close_col = _col(df, "close", "Close")
        if close_col is None or len(df) < 3:
            return defaults

        close = float(df[close_col].iloc[-1])
        if close <= 0:
            return defaults

        gaps = detect_fvgs(df, max_gaps)

        bull_gaps = [g for g in gaps if g["type"] == "bull" and g["top"] < close]
        bear_gaps = [g for g in gaps if g["type"] == "bear" and g["bottom"] > close]

        bull_count = len(bull_gaps)
        bear_count = len(bear_gaps)

        # Nearest bull FVG distance
        nearest_bull_dist = None
        if bull_gaps:
            nearest = min(bull_gaps, key=lambda g: abs(close - g["top"]))
            nearest_bull_dist = (close - nearest["top"]) / close

        # Nearest bear FVG distance
        nearest_bear_dist = None
        if bear_gaps:
            nearest = min(bear_gaps, key=lambda g: abs(g["bottom"] - close))
            nearest_bear_dist = (nearest["bottom"] - close) / close

        total = bull_count + bear_count
        imbalance = bull_count / total if total > 0 else 0.5

        return {
            "fvg_nearest_bull_dist": nearest_bull_dist,
            "fvg_nearest_bear_dist": nearest_bear_dist,
            "fvg_bull_count": bull_count,
            "fvg_bear_count": bear_count,
            "fvg_imbalance": imbalance,
        }

    except Exception:
        logging.exception("compute_fvg_features_error")
        return defaults
