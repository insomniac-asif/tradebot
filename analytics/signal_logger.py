# analytics/signal_logger.py
#
# One CSV row per signal evaluation cycle — opened or blocked.
# Gives you a fully labeled dataset of every decision the bot made,
# without waiting for fills. Use for gate calibration and ML training.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

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
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe_round(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_signal_attempt(ctx, trade=None) -> None:
    """
    Call once per auto_trader cycle, after open_trade_if_valid() returns.

    Parameters
    ----------
    ctx   : DecisionContext — fully populated by open_trade_if_valid()
    trade : the return value from open_trade_if_valid() (dict on success, else None/str)
    """
    try:
        _ensure_file()

        outcome = getattr(ctx, "outcome", "blocked")
        blended = getattr(ctx, "blended_score", None)
        threshold = getattr(ctx, "threshold", None)
        delta = (
            _safe_round(blended - threshold, 6)
            if blended is not None and threshold is not None
            else ""
        )

        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            outcome,
            getattr(ctx, "block_reason", None) or "",
            getattr(ctx, "regime", None) or "",
            getattr(ctx, "volatility", None) or "",
            getattr(ctx, "direction_60m", None) or "",
            _safe_round(getattr(ctx, "confidence_60m", None)),
            getattr(ctx, "direction_15m", None) or "",
            _safe_round(getattr(ctx, "confidence_15m", None)),
            getattr(ctx, "dual_alignment", ""),
            getattr(ctx, "conviction_score", "") if getattr(ctx, "conviction_score", None) is not None else "",
            _safe_round(getattr(ctx, "impulse", None)),
            _safe_round(getattr(ctx, "follow", None)),
            _safe_round(blended),
            _safe_round(threshold),
            delta,
            _safe_round(getattr(ctx, "ml_weight", None)),
            getattr(ctx, "regime_samples", "") if getattr(ctx, "regime_samples", None) is not None else "",
            getattr(ctx, "expectancy_samples", "") if getattr(ctx, "expectancy_samples", None) is not None else "",
            getattr(ctx, "regime_transition", ""),
            _safe_round(getattr(ctx, "regime_transition_severity", None)),
            _safe_round(getattr(ctx, "spy_price", None)),
        ]

        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass  # never crash the watcher
