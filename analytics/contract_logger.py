# analytics/contract_logger.py
#
# Logs every option contract selection attempt (success or failure) from both
# the main trader and the sim engine.  Useful for measuring:
#   - How often are chains empty / bid=0 off-hours?
#   - Which strikes/expiries actually have liquid quotes?
#   - What IV/delta the system traded at vs. what was available.

import os
from datetime import datetime
import pytz

from core.paths import DATA_DIR
from core.analytics_db import insert

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
    """No-op: schema is ensured at startup via analytics_db.init_db()."""
    pass


def _safe(val, decimals=4):
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return None


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
        insert("contract_selection_log", {
            "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "source": source,
            "direction": direction,
            "underlying_price": _safe(underlying_price),
            "expiry": str(expiry) if expiry is not None else None,
            "dte": str(dte) if dte is not None else None,
            "strike": _safe(strike),
            "result": result,
            "reason": reason or None,
            "bid": _safe(bid),
            "ask": _safe(ask),
            "mid": _safe(mid),
            "spread_pct": _safe(spread_pct),
            "iv": _safe(iv),
            "delta": _safe(delta),
            "gamma": _safe(gamma, decimals=6),
            "theta": _safe(theta),
            "vega": _safe(vega),
        })
    except Exception:
        pass
