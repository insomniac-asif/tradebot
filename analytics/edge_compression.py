# analytics/edge_compression.py

from analytics.edge_stability import calculate_edge_stability
from analytics.edge_momentum import calculate_edge_momentum


def get_edge_compression():

    """
    Detects instability and compresses exposure gradually.
    Does NOT disable trading.
    """

    stability_data = calculate_edge_stability()
    momentum_data = calculate_edge_momentum()

    if not stability_data:
        return {
            "active": False,
            "risk_multiplier": 1.0,
            "position_multiplier": 1.0
        }

    stability = stability_data["stability"]

    momentum = 0
    if momentum_data:
        momentum = momentum_data["momentum"]

    compression = 1.0
    active = False

    # ----------------------------------------
    # Trigger Conditions
    # ----------------------------------------

    if stability < 0.45:
        compression *= 0.85
        active = True

    if stability < 0.35:
        compression *= 0.80
        active = True

    if momentum < -0.20:
        compression *= 0.85
        active = True

    # Clamp compression
    compression = max(0.60, compression)

    return {
        "active": active,
        "risk_multiplier": compression,
        "position_multiplier": compression
    }
