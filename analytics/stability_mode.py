# analytics/stability_mode.py

from analytics.edge_stability import calculate_edge_stability

def get_stability_mode():
    """
    Returns current defense stage + control multipliers.
    """

    stats = calculate_edge_stability()

    if not stats:
        return {
            "mode": "NORMAL",
            "risk_multiplier": 1.0,
            "threshold_buffer": 0.0
        }

    stability = stats.get("stability", 1)
    avg_R = stats.get("avg_R", 0)

    # -----------------------------
    # 🟢 NORMAL MODE
    # -----------------------------
    if stability >= 0.60 and avg_R >= 0:
        return {
            "mode": "NORMAL",
            "risk_multiplier": 1.0,
            "threshold_buffer": 0.0
        }

    # -----------------------------
    # 🟡 SOFT DEFENSE
    # -----------------------------
    if stability >= 0.40:
        return {
            "mode": "SOFT_DEFENSE",
            "risk_multiplier": 0.6,      # 40% risk reduction
            "threshold_buffer": 0.03     # Slightly harder to enter
        }

    # -----------------------------
    # 🔴 HARD DEFENSE
    # -----------------------------
    return {
        "mode": "HARD_DEFENSE",
        "risk_multiplier": 0.3,      # 70% risk reduction
        "threshold_buffer": 0.07     # Much harder to enter
    }
