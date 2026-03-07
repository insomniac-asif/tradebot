from core.account_repository import load_account
from collections import defaultdict
import numpy as np


def calculate_setup_expectancy():
    """
    Advanced Setup Expectancy Engine

    - Bayesian-style confidence weighting
    - Sample size protection
    - Variance penalty
    - Stability-aware adjustment
    """

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 15:
        return None

    setup_data = defaultdict(list)

    for t in trades:
        if "setup" in t and "R" in t:
            setup_data[t["setup"]].append(t["R"])

    stats = {}

    for setup, Rs in setup_data.items():

        n = len(Rs)

        # Require minimum meaningful sample
        if n < 5:
            continue

        avg_R = np.mean(Rs)
        winrate = np.mean([r > 0 for r in Rs]) * 100
        variance = np.var(Rs)

        # -----------------------------
        # 1️⃣ Bayesian Confidence Curve
        # -----------------------------
        confidence_weight = 1 - np.exp(-n / 20)

        # -----------------------------
        # 2️⃣ Variance Penalty
        # High variance reduces trust
        # -----------------------------
        variance_penalty = 1 / (1 + variance)

        # -----------------------------
        # 3️⃣ Adjusted Expectancy
        # -----------------------------
        adjusted_avg_R = avg_R * confidence_weight * variance_penalty

        stats[setup] = {
            "avg_R": round(adjusted_avg_R, 3),
            "raw_avg_R": round(avg_R, 3),
            "samples": n,
            "winrate": round(winrate, 1),
            "variance": round(variance, 3),
            "confidence_weight": round(confidence_weight, 3),
        }

    return stats if stats else None
