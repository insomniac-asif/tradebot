import json
import logging
import os

logger = logging.getLogger(__name__)
STORE_PATH = "data/strategy_performance.json"


class StrategyPerformanceStore:
    """
    Tracks per-strategy, per-regime, per-time-bucket performance.
    Updated after every trade close.

    Schema:
    {
        "TREND_PULLBACK": {
            "TREND": {
                "OPENING_HOUR": {
                    "trades": 12,
                    "wins": 8,
                    "total_pnl": 142.50,
                    "total_pnl_pct": 0.034,
                    "max_win_pct": 0.18,
                    "max_loss_pct": -0.22,
                    "avg_hold_seconds": 1840,
                    "grade_a_count": 6,
                    "avg_spread_pct": 0.09
                }
            }
        }
    }
    """

    def __init__(self):
        self._data = {}
        self._load()

    def _load(self):
        if os.path.exists(STORE_PATH):
            try:
                with open(STORE_PATH) as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def _save(self):
        tmp = STORE_PATH + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, STORE_PATH)
        except Exception as e:
            logger.error("strategy_performance save failed: %s", e)

    def record_close(self, strategy: str, regime: str, time_bucket: str,
                     pnl: float, pnl_pct: float, hold_seconds: float,
                     grade: str = None, spread_pct: float = None):
        """Called after every trade close (paper or live)."""
        strat = self._data.setdefault(strategy, {})
        reg = strat.setdefault(regime, {})
        bucket = reg.setdefault(time_bucket, {
            "trades": 0, "wins": 0, "total_pnl": 0.0,
            "total_pnl_pct": 0.0, "max_win_pct": 0.0,
            "max_loss_pct": 0.0, "avg_hold_seconds": 0.0,
            "grade_a_count": 0, "avg_spread_pct": 0.0,
        })

        n = bucket["trades"]
        bucket["trades"] = n + 1
        if pnl > 0:
            bucket["wins"] += 1
        bucket["total_pnl"] += pnl
        bucket["total_pnl_pct"] += pnl_pct
        bucket["max_win_pct"] = max(bucket["max_win_pct"], pnl_pct)
        bucket["max_loss_pct"] = min(bucket["max_loss_pct"], pnl_pct)
        bucket["avg_hold_seconds"] = (bucket["avg_hold_seconds"] * n + hold_seconds) / (n + 1)
        if grade and str(grade).upper() == "A":
            bucket["grade_a_count"] += 1
        if spread_pct is not None:
            bucket["avg_spread_pct"] = (bucket["avg_spread_pct"] * n + spread_pct) / (n + 1)

        self._save()

    def get_score(self, strategy: str, regime: str, time_bucket: str) -> dict:
        """Returns stats for a strategy in specific conditions."""
        try:
            return self._data[strategy][regime][time_bucket]
        except KeyError:
            return None

    def get_strategy_summary(self, strategy: str) -> dict:
        """Aggregate across all regimes/buckets for one strategy."""
        if strategy not in self._data:
            return None
        total = {"trades": 0, "wins": 0, "total_pnl": 0.0, "regimes": set()}
        for regime, buckets in self._data[strategy].items():
            total["regimes"].add(regime)
            for bucket_data in buckets.values():
                total["trades"] += bucket_data["trades"]
                total["wins"] += bucket_data["wins"]
                total["total_pnl"] += bucket_data["total_pnl"]
        total["regimes"] = list(total["regimes"])
        total["win_rate"] = total["wins"] / total["trades"] if total["trades"] else 0.0
        return total


# Singleton
PERF_STORE = StrategyPerformanceStore()
