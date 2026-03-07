# analytics/setup_intelligence.py

from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_stability import calculate_edge_stability


def get_setup_intelligence(setup_type, regime, ml_probability=None):
    """
    Combines:
    - Setup historical expectancy
    - Regime expectancy
    - ML probability
    - Edge stability

    Returns:
        {
            "score": float (0-1),
            "confidence_boost": float,
            "risk_boost": float
        }
    """

    score = 0.5  # neutral baseline
    confidence_boost = 0
    risk_boost = 0

    # -------------------------
    # 1️⃣ Setup Expectancy Layer
    # -------------------------

    setup_stats = calculate_setup_expectancy()

    if setup_stats and setup_type in setup_stats:
        avg_R = setup_stats[setup_type]["avg_R"]

        if avg_R > 0:
            score += min(avg_R * 0.2, 0.15)
            risk_boost += 0.05

        if avg_R < 0:
            score -= 0.1
            risk_boost -= 0.05

    # -------------------------
    # 2️⃣ Regime Alignment Layer
    # -------------------------

    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:
        regime_avg = regime_stats[regime]["avg_R"]

        if regime_avg > 0:
            score += 0.05
        if regime_avg < 0:
            score -= 0.05

    # -------------------------
    # 3️⃣ ML Probability Layer
    # -------------------------

    if ml_probability is not None:
        score += (ml_probability - 0.5) * 0.3

    # -------------------------
    # 4️⃣ Edge Stability Layer
    # -------------------------

    stability_data = calculate_edge_stability()

    if stability_data:
        stability = stability_data["stability"]

        score += (stability - 0.5) * 0.1

    # Clamp final score
    score = max(0.0, min(score, 1.0))

    return {
        "score": round(score, 3),
        "confidence_boost": round(confidence_boost, 3),
        "risk_boost": round(risk_boost, 3)
    }
