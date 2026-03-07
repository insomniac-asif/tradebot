import os
import json
import numpy as np
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")

def get_recent_r_values(lookback=15):

    if not os.path.exists(ACCOUNT_FILE):
        return []

    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except:
        return []

    trades = acc.get("trade_log", [])

    r_values = []

    for trade in trades[-lookback:]:
        risk = trade.get("risk", 0)
        pnl = trade.get("pnl", 0)

        if risk > 0:
            r_values.append(pnl / risk)

    return r_values


def edge_decay_status():

    r_values = get_recent_r_values()

    if len(r_values) < 8:
        return {"status": "INSUFFICIENT_DATA"}

    avg_r = np.mean(r_values)
    std_r = np.std(r_values)

    # Decay rules
    if avg_r < 0:
        return {
            "status": "WEAK",
            "reason": "Negative recent expectancy",
            "avg_r": round(avg_r, 3)
        }

    if std_r > 2.0:
        return {
            "status": "DISABLE",
            "reason": "Edge unstable (high variance)",
            "std": round(std_r, 3)
        }

    if avg_r < 0.3:
        return {
            "status": "THROTTLE",
            "reason": "Weak expectancy",
            "avg_r": round(avg_r, 3)
        }

    return {"status": "OK"}