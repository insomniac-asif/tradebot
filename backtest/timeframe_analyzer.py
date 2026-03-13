"""
backtest/timeframe_analyzer.py
Recurrence interval detection for discovered patterns.

Given a list of trade entry_time strings, determines how regularly
a pattern fires and at what cadence.
"""
from __future__ import annotations

import math
from collections import Counter
from datetime import datetime

_MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}
_DAY_NAMES = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

# Gap buckets: (label, min_minutes, max_minutes)
_GAP_BUCKETS = [
    ("<30min",     0,        30),
    ("30min-2hr",  30,       120),
    ("2hr-1day",   120,      480),      # trading day ~6.5hr = 390min, use 480
    ("1-3days",    480,      480 * 3),
    ("3-7days",    480 * 3,  480 * 7),
    ("1-4weeks",   480 * 7,  480 * 28),
    ("1-3months",  480 * 28, 480 * 90),
]

_BUCKET_TO_LABEL = {
    "<30min":     "multiple times daily",
    "30min-2hr":  "multiple times daily",
    "2hr-1day":   "daily",
    "1-3days":    "every few days",
    "3-7days":    "weekly",
    "1-4weeks":   "bi-weekly",
    "1-3months":  "monthly",
}


def _parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        cleaned = str(s).replace("T", " ")
        for tz in (" EDT", " EST", " ET", "+00:00", "-04:00", "-05:00"):
            cleaned = cleaned.replace(tz, "")
        if "." in cleaned:
            base, frac = cleaned.split(".", 1)
            cleaned = base + "." + frac[:6]
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def _trading_day_gap(a: datetime, b: datetime) -> int:
    """Count trading days (weekdays) between two datetimes, exclusive of start."""
    if a.date() == b.date():
        return 0
    count = 0
    d = a.date()
    end = b.date()
    from datetime import timedelta
    d += timedelta(days=1)
    while d <= end:
        if d.weekday() < 5:
            count += 1
        d += timedelta(days=1)
    return count


class TimeframeAnalyzer:
    """Analyze recurrence intervals from a list of trade timestamps."""

    def analyze(self, trade_timestamps: list[str]) -> dict:
        # Parse and sort
        dts = sorted(dt for s in trade_timestamps if (dt := _parse_ts(s)) is not None)

        if len(dts) < 2:
            return {
                "primary_interval": "insufficient data",
                "avg_gap_minutes": 0,
                "regularity_score": 0.0,
                "hour_distribution": {},
                "day_distribution": {},
                "month_distribution": {},
                "trend": "unknown",
                "total_occurrences": len(dts),
            }

        # Compute inter-trade gaps in minutes
        gaps_minutes = []
        for i in range(1, len(dts)):
            gap = (dts[i] - dts[i - 1]).total_seconds() / 60.0
            gaps_minutes.append(gap)

        # Bucket gaps
        bucket_counts: Counter = Counter()
        for gap in gaps_minutes:
            placed = False
            for label, lo, hi in _GAP_BUCKETS:
                if lo <= gap < hi:
                    bucket_counts[label] += 1
                    placed = True
                    break
            if not placed:
                # Longer than 3 months
                bucket_counts["1-3months"] += 1

        # Primary interval = bucket with most gaps
        if bucket_counts:
            primary_bucket = bucket_counts.most_common(1)[0][0]
            primary_interval = _BUCKET_TO_LABEL.get(primary_bucket, primary_bucket)
        else:
            primary_bucket = "unknown"
            primary_interval = "unknown"

        # Average gap
        avg_gap = sum(gaps_minutes) / len(gaps_minutes) if gaps_minutes else 0

        # Regularity score: 1 - coefficient of variation
        if gaps_minutes and avg_gap > 0:
            std_gap = math.sqrt(sum((g - avg_gap) ** 2 for g in gaps_minutes) / len(gaps_minutes))
            cv = std_gap / avg_gap
            regularity = max(0.0, min(1.0, 1.0 - cv))
        else:
            regularity = 0.0

        # Distributions
        hour_dist = Counter(dt.hour for dt in dts)
        day_dist = Counter(_DAY_NAMES[dt.weekday()] for dt in dts)
        month_dist = Counter(_MONTH_NAMES[dt.month] for dt in dts)

        # Convert to plain dicts with string keys (for JSON)
        hour_distribution = {str(h): c for h, c in sorted(hour_dist.items())}
        day_distribution = {d: day_dist[d] for d in ["Mon", "Tue", "Wed", "Thu", "Fri"] if day_dist.get(d)}
        month_distribution = {_MONTH_NAMES[m]: month_dist[_MONTH_NAMES[m]]
                              for m in range(1, 13) if month_dist.get(_MONTH_NAMES[m])}

        # Trend detection: compare first-half vs second-half rate
        mid = len(dts) // 2
        first_half = dts[:mid]
        second_half = dts[mid:]

        first_days = max(1, (first_half[-1] - first_half[0]).days) if len(first_half) > 1 else 1
        second_days = max(1, (second_half[-1] - second_half[0]).days) if len(second_half) > 1 else 1

        first_rate = len(first_half) / first_days
        second_rate = len(second_half) / second_days

        if second_rate > first_rate * 1.2:
            trend = "accelerating"
        elif second_rate < first_rate * 0.8:
            trend = "fading"
        else:
            trend = "stable"

        return {
            "primary_interval": primary_interval,
            "avg_gap_minutes": round(avg_gap, 1),
            "regularity_score": round(regularity, 4),
            "hour_distribution": hour_distribution,
            "day_distribution": day_distribution,
            "month_distribution": month_distribution,
            "trend": trend,
            "total_occurrences": len(dts),
        }
