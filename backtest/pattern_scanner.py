"""
backtest/pattern_scanner.py
Post-backtest pattern discovery from trade logs.

Takes the trade_log list from a completed backtest run (same format as
dashboard_data.json) and finds statistically significant edges by grouping
trades across time-of-day, day-of-week, direction, hold duration, and
week-of-month dimensions.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

MIN_TRADES = 15
MIN_WIN_RATE = 0.62
MIN_PROFIT_FACTOR = 1.4
TOP_N = 30

# ── Time-of-day buckets (ET) ─────────────────────────────────────────────

_TOD_BUCKETS = [
    ("open_15",    (9, 30),  (9, 45)),
    ("open_30",    (9, 45),  (10, 0)),
    ("morning",    (10, 0),  (11, 30)),
    ("lunch",      (11, 30), (13, 0)),
    ("afternoon",  (13, 0),  (15, 0)),
    ("power_hour", (15, 0),  (15, 45)),
    ("close_15",   (15, 45), (16, 0)),
]

_DAY_NAMES = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

_DIRECTION_MAP = {"BULLISH": "CALL", "BEARISH": "PUT"}


def _parse_entry_time(entry_time_str: str) -> datetime | None:
    """Parse an entry_time string to datetime. Handles various formats."""
    if not entry_time_str:
        return None
    try:
        # "2024-06-03 10:15:00" or ISO variants
        s = str(entry_time_str).replace("T", " ")
        # Strip timezone suffix if present
        for tz in (" EDT", " EST", " ET", "+00:00", "-04:00", "-05:00"):
            s = s.replace(tz, "")
        # Trim microseconds if longer than 6 digits
        if "." in s:
            base, frac = s.split(".", 1)
            s = base + "." + frac[:6]
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _time_of_day(dt: datetime) -> str:
    h, m = dt.hour, dt.minute
    hm = (h, m)
    for label, start, end in _TOD_BUCKETS:
        if start <= hm < end:
            return label
    # Before open or after close
    if hm < (9, 30):
        return "open_15"
    return "close_15"


def _hold_bucket(holding_seconds: float) -> str:
    mins = holding_seconds / 60.0
    if mins < 5:
        return "scalp"
    if mins < 30:
        return "short"
    if mins < 120:
        return "medium"
    return "swing"


def _week_of_month(dt: datetime) -> tuple[int, bool]:
    """Return (week_number 1-5, is_opex_week).
    OPEX week contains the 3rd Friday of the month."""
    day = dt.day
    week = (day - 1) // 7 + 1

    # Find 3rd Friday: first day of month, find first Friday, add 14
    from calendar import monthrange, weekday
    first_dow = weekday(dt.year, dt.month, 1)  # 0=Mon
    # Days until first Friday (4=Fri)
    first_friday = 1 + (4 - first_dow) % 7
    third_friday = first_friday + 14
    # OPEX week = week containing third_friday
    opex_week = (third_friday - 1) // 7 + 1
    return week, week == opex_week


def _tag_trade(t: dict) -> dict | None:
    """Add context tags to a single trade dict. Returns None if unparseable."""
    dt = _parse_entry_time(t.get("entry_time", ""))
    if dt is None:
        return None

    raw_dir = (t.get("direction") or "").upper()
    direction = _DIRECTION_MAP.get(raw_dir, raw_dir)
    holding_sec = float(t.get("holding_seconds") or 0)
    pnl = float(t.get("realized_pnl_dollars") or t.get("pnl") or 0)
    week, is_opex = _week_of_month(dt)

    return {
        # Original fields carried forward
        "entry_time": t.get("entry_time"),
        "exit_time": t.get("exit_time"),
        "realized_pnl_dollars": pnl,
        "holding_seconds": holding_sec,
        "symbol": t.get("symbol", ""),
        "signal_mode": t.get("signal_mode", ""),
        "exit_reason": t.get("exit_reason", ""),
        # Derived tags
        "time_of_day": _time_of_day(dt),
        "day_of_week": _DAY_NAMES.get(dt.weekday(), str(dt.weekday())),
        "direction": direction,
        "hold_bucket": _hold_bucket(holding_sec),
        "week_of_month": f"W{week}" + ("_opex" if is_opex else ""),
        "pnl_outcome": "win" if pnl > 0 else "loss",
        "_dt": dt,
    }


def _group_stats(trades: list[dict], date_range_weeks: float) -> dict | None:
    """Compute stats for a group of tagged trades. Returns None if below thresholds."""
    n = len(trades)
    if n < MIN_TRADES:
        return None

    wins = [t for t in trades if t["pnl_outcome"] == "win"]
    losses = [t for t in trades if t["pnl_outcome"] == "loss"]
    win_rate = len(wins) / n

    gross_profit = sum(t["realized_pnl_dollars"] for t in wins)
    gross_loss = abs(sum(t["realized_pnl_dollars"] for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (
        float("inf") if gross_profit > 0 else 0.0
    )

    if win_rate < MIN_WIN_RATE or profit_factor < MIN_PROFIT_FACTOR:
        return None

    avg_pnl = sum(t["realized_pnl_dollars"] for t in trades) / n
    avg_hold_min = sum(t["holding_seconds"] for t in trades) / n / 60.0
    trades_per_week = n / date_range_weeks if date_range_weeks > 0 else 0.0

    # Top 5 sample trades by pnl
    sorted_by_pnl = sorted(trades, key=lambda t: t["realized_pnl_dollars"], reverse=True)
    samples = [
        {
            "entry_time": t["entry_time"],
            "exit_time": t["exit_time"],
            "pnl": round(t["realized_pnl_dollars"], 2),
        }
        for t in sorted_by_pnl[:5]
    ]

    return {
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 99.0,
        "avg_pnl": round(avg_pnl, 2),
        "total_trades": n,
        "avg_hold_minutes": round(avg_hold_min, 1),
        "trades_per_week": round(trades_per_week, 2),
        "sample_trades": samples,
    }


# ── Dimension definitions ────────────────────────────────────────────────

_SINGLE_DIMS = ["time_of_day", "day_of_week", "direction", "hold_bucket"]
_PAIR_DIMS = [
    ("time_of_day", "day_of_week"),
    ("time_of_day", "direction"),
    ("day_of_week", "direction"),
    ("day_of_week", "hold_bucket"),
    ("direction", "hold_bucket"),
    ("week_of_month", "direction"),
]

_TOD_LABELS = {
    "open_15": "first 15min (9:30-9:45)",
    "open_30": "second 15min (9:45-10:00)",
    "morning": "morning (10:00-11:30)",
    "lunch": "lunch (11:30-13:00)",
    "afternoon": "afternoon (13:00-15:00)",
    "power_hour": "power hour (15:00-15:45)",
    "close_15": "last 15min (15:45-16:00)",
}


def _describe(filters: dict) -> str:
    """Build a human-readable description from filter dict."""
    parts = []
    if "direction" in filters:
        parts.append(f"{filters['direction']} trades")
    if "time_of_day" in filters:
        parts.append(f"during {_TOD_LABELS.get(filters['time_of_day'], filters['time_of_day'])}")
    if "day_of_week" in filters:
        parts.append(f"on {filters['day_of_week']}s")
    if "hold_bucket" in filters:
        parts.append(f"held {filters['hold_bucket']}")
    if "week_of_month" in filters:
        wk = filters["week_of_month"]
        label = f"week {wk}" if "opex" not in wk else f"OPEX week"
        parts.append(f"in {label}")
    return " ".join(parts) if parts else "all trades"


class PatternScanner:
    """Scan a backtest trade_log for statistically significant patterns."""

    def scan(self, trade_log: list[dict], sim_id: str) -> list[dict]:
        """
        Analyze trade_log, group by 1D and 2D dimension combos,
        filter for high win-rate / profit-factor groups, return top patterns.
        """
        # Tag every trade
        tagged = []
        for t in trade_log:
            tt = _tag_trade(t)
            if tt is not None:
                tagged.append(tt)

        if not tagged:
            return []

        # Compute date range in weeks
        dates = [t["_dt"] for t in tagged]
        min_dt, max_dt = min(dates), max(dates)
        date_range_weeks = max(1.0, (max_dt - min_dt).days / 7.0)

        # Build groups: key -> list of tagged trades
        groups: dict[tuple, list[dict]] = defaultdict(list)

        for t in tagged:
            # Single-dimension groups
            for dim in _SINGLE_DIMS:
                key = (dim, t[dim])
                groups[key].append(t)
            # week_of_month as single dim
            groups[("week_of_month", t["week_of_month"])].append(t)

            # Pair-dimension groups
            for dim_a, dim_b in _PAIR_DIMS:
                key = (dim_a, t[dim_a], dim_b, t[dim_b])
                groups[key].append(t)

        # Compute stats per group, filter, collect
        candidates = []
        for key, trades in groups.items():
            stats = _group_stats(trades, date_range_weeks)
            if stats is None:
                continue

            # Build filters dict and pattern_id
            if len(key) == 2:
                # Single dimension
                dim, val = key
                filters = {dim: val}
                pid = f"{sim_id}_{val}"
            else:
                # Pair
                dim_a, val_a, dim_b, val_b = key
                filters = {dim_a: val_a, dim_b: val_b}
                pid = f"{sim_id}_{val_a}_{val_b}"

            candidates.append({
                "pattern_id": pid,
                "sim_id": sim_id,
                "description": _describe(filters),
                "filters": filters,
                **stats,
            })

        # Sort by profit_factor * win_rate descending, take top N
        candidates.sort(key=lambda p: p["profit_factor"] * p["win_rate"], reverse=True)
        patterns = candidates[:TOP_N]

        # Run recurrence analysis on each pattern
        from backtest.timeframe_analyzer import TimeframeAnalyzer
        analyzer = TimeframeAnalyzer()
        for pattern in patterns:
            # Collect entry_times for trades matching this pattern's filters
            filters = pattern["filters"]
            matching_times = [
                t["entry_time"] for t in tagged
                if all(t.get(dim) == val for dim, val in filters.items())
            ]
            pattern["recurrence_analysis"] = analyzer.analyze(matching_times)

        # Save to JSON
        if patterns:
            path = os.path.join(RESULTS_DIR, f"patterns_{sim_id}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(patterns, f, indent=2, default=str)

        return patterns
