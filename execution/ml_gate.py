import numpy as np
from analytics.edge_stability import calculate_edge_stability

def ml_probability_gate(
    df,
    regime,
    conviction_score,
    impulse,
    follow,
    confidence,
    total_trades,
    direction_model,
    edge_model
):
    """
    Soft activation ML gate.
    Gradually blends ML into conviction as data grows.
    """

    # ---------------------------------------
    # If models not loaded → bypass
    # ---------------------------------------
    if direction_model is None or edge_model is None:
        return True, None

    # ---------------------------------------
    # Soft activation weight
    # 0 → pure conviction
    # 1 → pure ML
    # ---------------------------------------
    weight = min(total_trades / 200, 1.0)

    # Build feature vector exactly as trained
    features = [[
        conviction_score,
        impulse,
        follow,
        confidence
    ]]

    try:
        direction_prob = direction_model.predict_proba(features)[0][1]
        edge_prob = edge_model.predict_proba(features)[0][1]
    except Exception:
        return True, None

    ml_score = (direction_prob * 0.6) + (edge_prob * 0.4)

    # Soft blended score
    blended_score = (
        conviction_score / 6 * (1 - weight)
        + ml_score * weight
    )

    threshold = adaptive_ml_threshold(regime)

    allow_trade = blended_score >= threshold

    return allow_trade, round(blended_score, 3)
def adaptive_ml_threshold(regime):

    base = {
        "TREND": 0.55,
        "VOLATILE": 0.60,
        "RANGE": 0.65,
        "COMPRESSION": 0.70
    }.get(regime, 0.60)

    stability_data = calculate_edge_stability()

    if stability_data is None:
        return base - 0.05  # early stage forgiveness

    stability = stability_data["stability"]

    # Reduce threshold if unstable (forgiving early)
    buffer = 0.05 * (1 - stability)

    return round(base - buffer, 3)
