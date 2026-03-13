# analytics/signal_logger.py
#
# One CSV row per signal evaluation cycle — opened or blocked.
# Gives you a fully labeled dataset of every decision the bot made,
# without waiting for fills. Use for gate calibration and ML training.

import os
from datetime import datetime
import pytz

from core.paths import DATA_DIR
from core.analytics_db import insert

FILE = os.path.join(DATA_DIR, "signal_log.csv")
HEADERS = [
    "timestamp",
    "outcome",           # "opened" | "blocked"
    "block_reason",
    "regime",
    "volatility",
    "direction_60m",
    "confidence_60m",
    "direction_15m",
    "confidence_15m",
    "dual_alignment",
    "conviction_score",
    "impulse",
    "follow_through",    # ctx.follow — same value, "follow_through" is the descriptive label
    "blended_score",
    "threshold",
    "threshold_delta",   # blended - threshold (positive = passed gate)
    "ml_weight",
    "regime_samples",
    "expectancy_samples",
    "regime_transition",         # bool: was a regime transition detected?
    "regime_transition_severity",
    "spy_price",
]


def _ensure_file() -> None:
    """No-op: schema is ensured at startup via analytics_db.init_db()."""
    pass


def _safe_round(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def log_signal_attempt(ctx, trade=None) -> None:
    """
    Call once per auto_trader cycle, after open_trade_if_valid() returns.

    Parameters
    ----------
    ctx   : DecisionContext — fully populated by open_trade_if_valid()
    trade : the return value from open_trade_if_valid() (dict on success, else None/str)
    """
    try:
        outcome = getattr(ctx, "outcome", "blocked")
        blended = getattr(ctx, "blended_score", None)
        threshold = getattr(ctx, "threshold", None)
        delta = (
            _safe_round(blended - threshold, 6)
            if blended is not None and threshold is not None
            else None
        )

        insert("signal_log", {
            "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "outcome": outcome,
            "block_reason": getattr(ctx, "block_reason", None) or None,
            "regime": getattr(ctx, "regime", None) or None,
            "volatility": getattr(ctx, "volatility", None) or None,
            "direction_60m": getattr(ctx, "direction_60m", None) or None,
            "confidence_60m": _safe_round(getattr(ctx, "confidence_60m", None)),
            "direction_15m": getattr(ctx, "direction_15m", None) or None,
            "confidence_15m": _safe_round(getattr(ctx, "confidence_15m", None)),
            "dual_alignment": str(getattr(ctx, "dual_alignment", "")) if getattr(ctx, "dual_alignment", None) is not None else None,
            "conviction_score": _safe_round(getattr(ctx, "conviction_score", None)) if getattr(ctx, "conviction_score", None) is not None else None,
            "impulse": _safe_round(getattr(ctx, "impulse", None)),
            "follow_through": _safe_round(getattr(ctx, "follow", None)),
            "blended_score": _safe_round(blended),
            "threshold": _safe_round(threshold),
            "threshold_delta": delta,
            "ml_weight": _safe_round(getattr(ctx, "ml_weight", None)),
            "regime_samples": _safe_int(getattr(ctx, "regime_samples", None)),
            "expectancy_samples": _safe_int(getattr(ctx, "expectancy_samples", None)),
            "regime_transition": str(getattr(ctx, "regime_transition", "")) if getattr(ctx, "regime_transition", None) is not None else None,
            "regime_transition_severity": _safe_round(getattr(ctx, "regime_transition_severity", None)),
            "spy_price": _safe_round(getattr(ctx, "spy_price", None)),
        })
    except Exception:
        pass  # never crash the watcher
