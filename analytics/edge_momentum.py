# analytics/edge_momentum.py

from core.account_repository import load_account
import numpy as np


def calculate_edge_momentum():
    """
    Measures short-term expectancy acceleration.

    Compares:
    - Recent 20 trades
    - Recent 50 trades

    Returns:
        {
            "momentum": float (-1 to +1),
            "recent_avg": float,
            "baseline_avg": float
        }
    """

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 30:
        return None

    # Extract R multiples
    Rs = [t.get("R") for t in trades if "R" in t]

    if len(Rs) < 30:
        return None

    recent_20 = Rs[-20:]
    recent_50 = Rs[-50:] if len(Rs) >= 50 else Rs[:-20]

    if len(recent_50) < 20:
        return None

    recent_avg = np.mean(recent_20)
    baseline_avg = np.mean(recent_50)

    # Normalize difference
    diff = recent_avg - baseline_avg

    # Convert to bounded momentum score
    momentum = np.tanh(diff)

    return {
        "momentum": round(momentum, 3),
        "recent_avg": round(recent_avg, 3),
        "baseline_avg": round(baseline_avg, 3)
    }
