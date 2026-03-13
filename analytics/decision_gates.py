"""analytics/decision_gates.py
Wire analytics systems into trade gating decisions.

Three auto-adjustments:
1. Blocked signal win-rate: if >60% of blocked trades would have won, loosen filter
2. Predictor accuracy: if <28% on rolling 200, disable predictor for that sim
3. Feature drift: if severity >0.7, reduce position size by 50%

Every adjustment is logged at ERROR level so it appears in system.log.
"""
import logging
from datetime import datetime

import pytz

# Cache adjustments for 50 trades (re-evaluate after each window)
_ADJUSTMENT_CACHE: dict[str, dict] = {}
_LAST_EVAL_TIME: dict[str, str] = {}


def get_analytics_adjustments(sim_id: str, profile: dict) -> dict:
    """
    Return a dict of adjustments for the given sim.

    Keys:
      loosen_filters: dict[str, float] — filter_name -> multiplier (1.1 = 10% looser)
      predictor_override: str | None — "disabled" if predictor should be turned off
      size_multiplier: float — 1.0 normal, 0.5 if drift detected
      reasons: list[str] — human-readable log of what was adjusted and why

    Called once per sim per entry cycle. Results cached for efficiency.
    """
    # Re-evaluate at most once per 5 minutes
    cache_key = sim_id
    now_iso = datetime.now(pytz.timezone("US/Eastern")).isoformat()
    last = _LAST_EVAL_TIME.get(cache_key, "")
    if last and cache_key in _ADJUSTMENT_CACHE:
        try:
            delta = (datetime.fromisoformat(now_iso) - datetime.fromisoformat(last)).total_seconds()
            if delta < 300:  # 5 minutes
                return _ADJUSTMENT_CACHE[cache_key]
        except Exception:
            pass

    result = {
        "loosen_filters": {},
        "predictor_override": None,
        "size_multiplier": 1.0,
        "reasons": [],
    }

    # ── 1. Blocked signal win rate ────────────────────────────────────────────
    try:
        _check_blocked_signal_winrate(result)
    except Exception:
        pass

    # ── 2. Predictor accuracy ─────────────────────────────────────────────────
    try:
        _check_predictor_accuracy(result)
    except Exception:
        pass

    # ── 3. Feature drift ──────────────────────────────────────────────────────
    try:
        _check_feature_drift(sim_id, profile, result)
    except Exception:
        pass

    # Log adjustments
    if result["reasons"]:
        for reason in result["reasons"]:
            logging.error(
                "analytics_auto_adjust: sim=%s %s",
                sim_id, reason,
            )

    _ADJUSTMENT_CACHE[cache_key] = result
    _LAST_EVAL_TIME[cache_key] = now_iso
    return result


def _check_blocked_signal_winrate(result: dict) -> None:
    """If >60% of recently blocked trades would have been winners, loosen filters."""
    from core.analytics_db import read_df

    df = read_df(
        "SELECT block_reason, fwd_5m, fwd_5m_status FROM blocked_signals "
        "WHERE fwd_5m_status = 'filled' "
        "ORDER BY id DESC LIMIT 50"
    )
    if df.empty or len(df) < 20:
        return

    # A blocked trade "would have won" if the 5-minute forward return was positive
    # (for bullish signals) — simplified: positive fwd_5m means the block was costly
    df["fwd_5m"] = df["fwd_5m"].astype(float, errors="ignore")
    positive = df[df["fwd_5m"] > 0]
    win_rate = len(positive) / len(df)

    if win_rate > 0.60:
        # Find which block_reason is most common
        top_reason = df["block_reason"].value_counts().index[0] if not df["block_reason"].isna().all() else None
        if top_reason:
            result["loosen_filters"][top_reason] = 1.10  # 10% looser
            result["reasons"].append(
                f"blocked_signal_winrate={win_rate:.0%} on {len(df)} samples, "
                f"loosening '{top_reason}' by 10%"
            )


def _check_predictor_accuracy(result: dict) -> None:
    """If predictor accuracy <28% on rolling 200, recommend disabling."""
    from analytics.ml_accuracy import ml_rolling_accuracy

    acc = ml_rolling_accuracy(lookback=200)
    if acc is None:
        return

    accuracy_pct = acc.get("accuracy", 50)
    samples = acc.get("samples", 0)

    if samples >= 100 and accuracy_pct < 28:
        result["predictor_override"] = "disabled"
        result["reasons"].append(
            f"predictor_accuracy={accuracy_pct:.1f}% on {samples} samples (<28%), "
            f"recommending predictor_mode=disabled"
        )


def _check_feature_drift(sim_id: str, profile: dict, result: dict) -> None:
    """If feature drift severity >0.7, reduce position size by 50%."""
    from analytics.feature_drift import detect_feature_drift

    drift = detect_feature_drift()
    if drift is None:
        return

    severity = drift.get("severity", 0)
    features = drift.get("features", [])

    if severity > 0.7:
        result["size_multiplier"] = 0.5
        result["reasons"].append(
            f"feature_drift severity={severity:.2f} (>0.7), "
            f"drifted: {', '.join(features[:5])}, reducing size to 50%"
        )
