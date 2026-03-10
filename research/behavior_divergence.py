# research/behavior_divergence.py
#
# Analyzes completed sim trades to find patterns in winners vs losers.
# Reads data/sims/{sim_id}.json directly — no simulation package imports.

import json
import logging
import os
from collections import Counter
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SIM_DIR  = os.path.join(_BASE_DIR, "data", "sims")

# Field mapping: raw JSON key → normalized internal key.
# The trade log stores PnL as 'realized_pnl_dollars'; all other target
# fields (entry_time, exit_time, direction, entry_price, exit_price,
# exit_reason, signal_mode) already match their raw names.
_FIELD_MAP = {
    "realized_pnl_dollars": "pnl",
}

# Time-of-day buckets as (name, start_min, end_min) — minutes since midnight.
_TIME_BUCKETS = [
    ("pre_market",  0,         9 * 60 + 30),
    ("first_30min", 9 * 60 + 30, 10 * 60),
    ("mid_morning", 10 * 60,   11 * 60 + 30),
    ("midday",      11 * 60 + 30, 13 * 60),
    ("afternoon",   13 * 60,   15 * 60),
    ("last_hour",   15 * 60,   16 * 60),
]


def _bucket_minutes(minutes: int) -> Optional[str]:
    for name, start, end in _TIME_BUCKETS:
        if start <= minutes < end:
            return name
    return None


def load_trade_history(sim_id: str) -> list:
    """
    Load closed trades for sim_id from data/sims/{sim_id}.json.

    Normalizes field names:
      realized_pnl_dollars  →  pnl
      (all other fields keep their raw names)

    Returns empty list if file is missing or unreadable.
    """
    path = os.path.join(_SIM_DIR, f"{sim_id}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        logger.debug("load_trade_history: cannot read %s: %s", path, exc)
        return []

    raw_trades = data.get("trade_log", [])
    normalized = []
    for t in raw_trades:
        if not isinstance(t, dict):
            continue
        trade = {_FIELD_MAP.get(k, k): v for k, v in t.items()}
        normalized.append(trade)
    return normalized


def _parse_time(ts: Any) -> Optional[datetime]:
    """Parse an ISO timestamp string. Returns None on any failure."""
    if ts is None:
        return None
    try:
        if isinstance(ts, datetime):
            return ts
        return datetime.fromisoformat(str(ts))
    except Exception:
        return None


def _hold_seconds(trade: dict) -> Optional[float]:
    """
    Return hold duration in seconds.
    Prefers time_in_trade_seconds (stored on every closed trade) and falls
    back to computing from entry_time / exit_time.
    """
    raw = trade.get("time_in_trade_seconds")
    if raw is not None:
        try:
            return float(raw)
        except Exception:
            pass
    entry = _parse_time(trade.get("entry_time"))
    exit_ = _parse_time(trade.get("exit_time"))
    if entry is not None and exit_ is not None:
        try:
            return abs((exit_ - entry).total_seconds())
        except Exception:
            pass
    return None


def _time_bucket(trade: dict) -> Optional[str]:
    """Return time-of-day bucket name for a trade's entry_time, or None."""
    entry = _parse_time(trade.get("entry_time"))
    if entry is None:
        return None
    return _bucket_minutes(entry.hour * 60 + entry.minute)


def _classify_group(trades: list) -> dict:
    """Compute summary stats for one group (winners or losers)."""
    count = len(trades)
    if count == 0:
        return {
            "count": 0,
            "avg_pnl": 0.0,
            "avg_hold_seconds": None,
            "most_common_exit_reason": None,
            "time_of_day_distribution": {},
        }

    avg_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades) / count

    hold_times = [s for s in (_hold_seconds(t) for t in trades) if s is not None]
    avg_hold   = round(sum(hold_times) / len(hold_times), 1) if hold_times else None

    reasons     = [str(t["exit_reason"]) for t in trades if t.get("exit_reason")]
    most_common = Counter(reasons).most_common(1)[0][0] if reasons else None

    bucket_counts: dict = {}
    for t in trades:
        b = _time_bucket(t)
        if b:
            bucket_counts[b] = bucket_counts.get(b, 0) + 1

    return {
        "count":                     count,
        "avg_pnl":                   round(avg_pnl, 4),
        "avg_hold_seconds":          avg_hold,
        "most_common_exit_reason":   most_common,
        "time_of_day_distribution":  bucket_counts,
    }


def classify_trades(trades: list) -> dict:
    """
    Split trades into winners (pnl > 0) and losers (pnl <= 0) and compute
    summary statistics for each group.
    """
    winners = [t for t in trades if float(t.get("pnl", 0) or 0) > 0]
    losers  = [t for t in trades if float(t.get("pnl", 0) or 0) <= 0]
    return {
        "winners": _classify_group(winners),
        "losers":  _classify_group(losers),
    }


def _dominant_bucket(dist: dict) -> Optional[str]:
    """Return the bucket name with the highest count, or None."""
    return max(dist, key=dist.__getitem__) if dist else None


def find_behavior_gaps(sim_id: str) -> dict:
    """
    Analyze winner / loser patterns for a sim.

    Returns a gap report dict with keys:
      status, sim_id, trade_count, win_rate, gaps
    Returns {"status": "insufficient_data"} when < 10 trades are found.
    Returns {"status": "error"} on unexpected exceptions.
    """
    try:
        trades = load_trade_history(sim_id)
        total  = len(trades)
        if total < 10:
            return {
                "status":      "insufficient_data",
                "sim_id":      sim_id,
                "trade_count": total,
            }

        classified = classify_trades(trades)
        winners    = classified["winners"]
        losers     = classified["losers"]

        w_hold = winners["avg_hold_seconds"]
        l_hold = losers["avg_hold_seconds"]

        return {
            "status":      "ok",
            "sim_id":      sim_id,
            "trade_count": total,
            "win_rate":    round(winners["count"] / total, 4),
            "gaps": {
                "hold_time": {
                    "winners_avg_sec": w_hold,
                    "losers_avg_sec":  l_hold,
                },
                "time_of_day": {
                    "best_bucket":  _dominant_bucket(winners["time_of_day_distribution"]),
                    "worst_bucket": _dominant_bucket(losers["time_of_day_distribution"]),
                },
                "exit_reason": {
                    "winner_dominant": winners["most_common_exit_reason"],
                    "loser_dominant":  losers["most_common_exit_reason"],
                },
            },
        }
    except Exception as exc:
        logger.exception("find_behavior_gaps failed for %s", sim_id)
        return {"status": "error", "sim_id": sim_id, "error": str(exc)}


def generate_all_reports() -> list:
    """Run behavior gap analysis for SIM01–SIM35 (SIM00 is live, skip it)."""
    reports = []
    for i in range(1, 36):
        sim_id = f"SIM{i:02d}"
        try:
            reports.append(find_behavior_gaps(sim_id))
        except Exception as exc:
            logger.error("generate_all_reports: error on %s: %s", sim_id, exc)
            reports.append({"status": "error", "sim_id": sim_id, "error": str(exc)})
    return reports
