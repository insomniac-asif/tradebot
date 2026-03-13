"""
analytics/confluence_scorer.py
Multi-indicator confluence scoring for ML feature enrichment.

No state, no mutation, no side effects. The scorer is read-only —
it adds features to the snapshot dict, it does NOT gate or suppress entries.
Every public function returns a safe default on error.
"""
import logging
from typing import Optional


def _col(df, *candidates):
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


_DEFAULTS = {
    "conf_score": 0,
    "conf_trend": None,
    "conf_momentum": None,
    "conf_volume": None,
    "conf_vwap": None,
    "conf_structure": None,
    "conf_xasset": None,
}


def compute_confluence_score(df, direction, feature_snapshot=None) -> dict:
    """
    Compute a 0-100 confluence score from 6 dimensions measuring
    multi-indicator agreement with the proposed trade direction.

    Returns 7 keys: conf_score (total) plus 6 component scores.
    """
    if direction is None:
        return dict(_DEFAULTS)

    try:
        last = df.iloc[-1]

        # ── dim1: Trend (max 20) ──────────────────────────────────
        ema9_col = _col(df, "ema9", "EMA9", "ema_9")
        ema20_col = _col(df, "ema20", "EMA20", "ema_20")
        if ema9_col is not None and ema20_col is not None:
            ema9 = float(last[ema9_col])
            ema20 = float(last[ema20_col])
            if direction == "BULLISH":
                dim1 = 20 if ema9 > ema20 else 0
            else:
                dim1 = 20 if ema9 < ema20 else 0
        else:
            dim1 = 10

        # ── dim2: Momentum (max 15) ──────────────────────────────
        rsi_col = _col(df, "rsi", "RSI", "rsi14", "RSI14")
        if rsi_col is not None:
            rsi = float(last[rsi_col])
            if direction == "BULLISH":
                if 40 <= rsi <= 70:
                    dim2 = 15
                elif rsi > 70:
                    dim2 = 5
                else:
                    dim2 = 0
            else:
                if 30 <= rsi <= 60:
                    dim2 = 15
                elif rsi < 30:
                    dim2 = 5
                else:
                    dim2 = 0
        else:
            dim2 = 8

        # ── dim3: Volume (max 15) ────────────────────────────────
        if feature_snapshot and "vol_z" in feature_snapshot:
            vol_z = feature_snapshot["vol_z"]
            if vol_z is not None:
                vol_z = float(vol_z)
                if vol_z > 0.5:
                    dim3 = 15
                elif vol_z > 0:
                    dim3 = 10
                else:
                    dim3 = 3
            else:
                dim3 = 8
        else:
            dim3 = 8

        # ── dim4: VWAP alignment (max 15) ────────────────────────
        close_col = _col(df, "close", "Close")
        vwap_col = _col(df, "vwap", "VWAP")
        if close_col is not None and vwap_col is not None:
            close = float(last[close_col])
            vwap = float(last[vwap_col])
            if direction == "BULLISH":
                dim4 = 15 if close > vwap else 3
            else:
                dim4 = 15 if close < vwap else 3
        else:
            dim4 = 8

        # ── dim5: Structure (max 15) ─────────────────────────────
        if (feature_snapshot
                and "struct_near_support_dist" in feature_snapshot
                and "struct_near_resist_dist" in feature_snapshot):
            support_dist = feature_snapshot["struct_near_support_dist"]
            resist_dist = feature_snapshot["struct_near_resist_dist"]
            if support_dist is not None and resist_dist is not None:
                support_dist = float(support_dist)
                resist_dist = float(resist_dist)
                if direction == "BULLISH" and support_dist < 0.005:
                    dim5 = 15
                elif direction == "BEARISH" and resist_dist < 0.005:
                    dim5 = 15
                else:
                    dim5 = 8
            else:
                dim5 = 8
        else:
            dim5 = 8

        # ── dim6: Cross-asset coherence (max 10) ─────────────────
        if feature_snapshot and "xasset_index_divergence" in feature_snapshot:
            div = feature_snapshot["xasset_index_divergence"]
            if div is not None:
                div = float(div)
                if div < 0.3:
                    dim6 = 10
                elif div <= 0.7:
                    dim6 = 5
                else:
                    dim6 = 2
            else:
                dim6 = 5
        else:
            dim6 = 5

        total = dim1 + dim2 + dim3 + dim4 + dim5 + dim6

        return {
            "conf_score": total,
            "conf_trend": dim1,
            "conf_momentum": dim2,
            "conf_volume": dim3,
            "conf_vwap": dim4,
            "conf_structure": dim5,
            "conf_xasset": dim6,
        }

    except Exception:
        logging.exception("compute_confluence_score_error")
        return dict(_DEFAULTS)
