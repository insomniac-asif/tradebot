import os
import json
import math
from collections import defaultdict
from statistics import mean, stdev
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_regime_expectancy():
    """
    Advanced regime expectancy analytics.

    Returns:
        dict of regime -> metrics
        OR None if insufficient data
    """

    if not os.path.exists(ACCOUNT_FILE):
        return None

    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except Exception:
        return None

    trades = acc.get("trade_log", [])

    if len(trades) < 15:
        return None

    regime_data = defaultdict(list)

    for t in trades:
        risk = t.get("risk", 0)
        regime = t.get("regime", "UNKNOWN")

        if not risk or risk == 0:
            continue

        r_multiple = t.get("R", None)

        if r_multiple is None:
            continue

        regime_data[regime].append(r_multiple)

    if not regime_data:
        return None

    results = {}

    for regime, r_list in regime_data.items():

        total = len(r_list)

        if total < 5:
            continue  # ignore low-sample regimes

        avg_r = mean(r_list)

        winrate = sum(1 for r in r_list if r > 0) / total * 100

        volatility = stdev(r_list) if total > 1 else 0

        # Stability metric (higher is better)
        if volatility == 0:
            stability = 1
        else:
            stability = max(0, 1 - (volatility / 3))

        # Confidence score
        sample_factor = min(total / 50, 1.0)
        confidence = round(stability * sample_factor, 3)

        results[regime] = {
            "trades": total,
            "regime_sample_count": total,
            "avg_R": round(avg_r, 3),
            "winrate": round(winrate, 1),
            "volatility": round(volatility, 3),
            "stability": round(stability, 3),
            "confidence": confidence
        }

    if not results:
        return None

    return results
