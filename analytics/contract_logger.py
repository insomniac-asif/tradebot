# analytics/contract_logger.py
#
# Logs every option contract selection attempt (success or failure) from both
# the main trader and the sim engine.  Useful for measuring:
#   - How often are chains empty / bid=0 off-hours?
#   - Which strikes/expiries actually have liquid quotes?
#   - What IV/delta the system traded at vs. what was available.

import os
import csv
from datetime import datetime
import pytz

from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "contract_selection_log.csv")
HEADERS = [
    "timestamp",
    "source",            # "main" | "sim:<sim_id>"
    "direction",
    "underlying_price",
    "expiry",
    "dte",
    "strike",
    "result",            # "selected" | "rejected" | "error"
    "reason",            # e.g. "spread_too_wide", "no_snapshot", "selected"
    "bid",
    "ask",
    "mid",
    "spread_pct",
    # greeks — None when snapshot has no greeks data
    "iv",
    "delta",
    "gamma",
    "theta",
    "vega",
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


def log_contract_attempt(
    source: str,
    direction: str,
    underlying_price,
    expiry,
    dte,
    strike,
    result: str,
    reason: str,
    bid=None,
    ask=None,
    mid=None,
    spread_pct=None,
    iv=None,
    delta=None,
    gamma=None,
    theta=None,
    vega=None,
) -> None:
    """
    Log one row per contract candidate evaluated.

    Call for every strike tried (success or rejection) so you get a complete
    picture of chain quality at any given time.
    """
    try:
        _ensure_file()
        row = [
            datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            source,
            direction,
            _safe(underlying_price),
            str(expiry) if expiry is not None else "",
            dte if dte is not None else "",
            _safe(strike),
            result,
            reason or "",
            _safe(bid),
            _safe(ask),
            _safe(mid),
            _safe(spread_pct),
            _safe(iv),
            _safe(delta),
            _safe(gamma, decimals=6),
            _safe(theta),
            _safe(vega),
        ]
        with open(FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
    except Exception:
        pass
