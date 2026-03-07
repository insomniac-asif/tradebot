# analytics/edge_stability.py

import os
import json
import numpy as np
from math import exp
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_edge_stability(lookback=30):
    """
    Composite Edge Stability Model

    Measures:
    - R multiple variance
    - Expectancy trend
    - Winrate consistency
    - Sample confidence

    Returns score between 0 and 1
    """

    if not os.path.exists(ACCOUNT_FILE):
        return None

    try:
        with open(ACCOUNT_FILE, "r") as f:
            account = json.load(f)
    except:
        return None

    trade_log = account.get("trade_log", [])

    if len(trade_log) < 5:
        return {
            "stability": 0.2,  # Low trust early
            "confidence": 0.1,
            "samples": len(trade_log),
            "note": "Low sample size"
        }

    trades = trade_log[-lookback:]

    r_values = []
    wins = 0

    for trade in trades:
        risk = trade.get("risk", 0)
        pnl = trade.get("pnl", 0)

        if risk > 0:
            r = pnl / risk
            r_values.append(r)

            if r > 0:
                wins += 1

    if len(r_values) < 3:
        return None

    r_values = np.array(r_values)

    # ------------------------
    # 1️⃣ Variance Stability
    # ------------------------
    std_dev = np.std(r_values)
    variance_score = 1 / (1 + std_dev)

    # ------------------------
    # 2️⃣ Expectancy Strength
    # ------------------------
    avg_R = np.mean(r_values)
    expectancy_score = 1 / (1 + exp(-avg_R))  # sigmoid scaling

    # ------------------------
    # 3️⃣ Winrate Stability
    # ------------------------
    winrate = wins / len(r_values)
    winrate_stability = 1 - abs(winrate - 0.5)  # penalize extremes early

    # ------------------------
    # 4️⃣ Sample Confidence
    # ------------------------
    sample_size = len(trade_log)
    confidence = 1 - exp(-sample_size / 40)

    # ------------------------
    # Composite Score
    # ------------------------
    composite = (
        variance_score * 0.35 +
        expectancy_score * 0.35 +
        winrate_stability * 0.15 +
        confidence * 0.15
    )

    composite = round(float(composite), 3)

    return {
        "stability": composite,
        "variance_score": round(float(variance_score), 3),
        "expectancy_score": round(float(expectancy_score), 3),
        "winrate_stability": round(float(winrate_stability), 3),
        "confidence": round(float(confidence), 3),
        "avg_R": round(float(avg_R), 3),
        "samples": sample_size
    }
