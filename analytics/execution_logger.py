# analytics/execution_logger.py
#
# Records execution quality for every real broker fill (entry and exit).
# Captures:
#   - Slippage: (fill_price - expected_mid) / expected_mid
#   - Spread at order time
#   - Partial fill ratio
#   - Exit quality (fill vs. mid at exit time)
#
# Use this to build a realistic slippage model for sim profiles over time.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "execution_quality_log.csv")
HEADERS = [
    "timestamp",
    "option_symbol",
    "side",              # "entry" | "exit"
    "order_type",        # "limit_mid_plus" | "limit_ask" | "market"
    "qty_requested",
    "qty_filled",
    "fill_ratio",
    "expected_mid",      # mid quote at time of order submission
    "fill_price",
    "slippage_pct",      # (fill - expected_mid) / expected_mid  (+ = paid more)
    "bid_at_order",
    "ask_at_order",
    "spread_at_order_pct",
]


def _ensure_file() -> None:
    if os.path.exists(FILE) and os.path.getsize(FILE) > 0:
        return
    with open(FILE, "w", newline="") as f:
        csv.writer(f).writerow(HEADERS)


def _safe(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return ""


def log_execution(
    option_symbol: str,
    side: str,
    order_type: str,
    qty_requested: int,
    qty_filled: int,
    fill_ratio,
    expected_mid,
    fill_price,
    bid_at_order,
    ask_at_order,
) -> None:
    """
    Record one fill event.

    Parameters
    ----------
    side        : "entry" or "exit"
    order_type  : "limit_mid_plus" (first attempt), "limit_ask" (retry), "market"
    expected_mid: mid-price at the moment the order was submitted
    fill_price  : actual fill from broker
    bid_at_order / ask_at_order : quote snapshot at submission time
    """
    try:
        _ensure_file()

        slippage_pct = ""
        try:
            if expected_mid and float(expected_mid) > 0:
                slippage_pct = _safe(
                    (float(fill_price) - float(expected_mid)) / float(expected_mid), 5
                )
        except (TypeError, ValueError):
            pass

        spread_at_order_pct = ""
        try:
            a = float(ask_at_order)
            b = float(bid_at_order)
            if a > 0:
                spread_at_order_pct = _safe((a - b) / a)
        except (TypeError, ValueError):
            pass

        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            option_symbol,
            side,
            order_type,
            qty_requested,
            qty_filled,
            _safe(fill_ratio),
            _safe(expected_mid),
            _safe(fill_price),
            slippage_pct,
            _safe(bid_at_order),
            _safe(ask_at_order),
            spread_at_order_pct,
        ]
        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass
