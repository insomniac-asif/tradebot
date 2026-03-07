from signals.regime import get_regime
from signals.volatility import volatility_state
from core.data_service import get_market_dataframe
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_stability import calculate_edge_stability
from analytics.ml_accuracy import ml_rolling_accuracy


def review_trade(trade, result):
    """
    Post-trade forensic analysis.
    Fully hardened.
    """

    df = get_market_dataframe()
    if df is None:
        return "Market data unavailable for review."

    regime = get_regime(df)
    vol = volatility_state(df)

    setup = trade.get("setup", "UNKNOWN")
    confidence = round(trade.get("confidence", 0) * 100, 1)
    r_multiple = trade.get("R", None)
    ml_prob = trade.get("ml_probability", None)

    diagnosis = []

    # ---------------------------------
    # Structural Context
    # ---------------------------------

    if setup == "BREAKOUT" and regime == "RANGE":
        diagnosis.append("Breakout taken in range market (structural mismatch).")

    if setup == "REVERSAL" and regime == "TREND":
        diagnosis.append("Reversal attempted against active trend.")

    if setup == "PULLBACK" and regime == "TREND":
        diagnosis.append("Pullback aligned with trend structure (favorable context).")

    # ---------------------------------
    # Volatility Commentary
    # ---------------------------------

    if vol in ["LOW", "DEAD"]:
        diagnosis.append("Low volatility likely reduced follow-through.")

    if vol == "HIGH" and setup == "BREAKOUT":
        diagnosis.append("High volatility supports breakout expansion.")

    # ---------------------------------
    # Confidence Evaluation
    # ---------------------------------

    if confidence < 60:
        diagnosis.append("Low confidence trade (statistically weaker edge).")

    elif confidence >= 75:
        diagnosis.append("High conviction signal.")

    # ---------------------------------
    # Regime Expectancy Context
    # ---------------------------------

    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:
        avg_r = regime_stats[regime]["avg_R"]
        diagnosis.append(f"Regime expectancy avg R: {avg_r}")

    # ---------------------------------
    # ML Performance Context
    # ---------------------------------

    ml_stats = ml_rolling_accuracy()

    if ml_stats:
        acc = ml_stats["accuracy"]

        if acc < 52:
            diagnosis.append("ML currently underperforming.")
        elif acc > 60:
            diagnosis.append("ML currently performing strongly.")

    # ---------------------------------
    # Stability Context
    # ---------------------------------

    stability_data = calculate_edge_stability()

    if stability_data:
        stability = round(stability_data["stability"], 2)

        if stability < 0.4:
            diagnosis.append("Edge stability LOW — system in fragile state.")
        elif stability > 0.7:
            diagnosis.append("Edge stability HIGH — strong statistical regime.")

    # ---------------------------------
    # R-Multiple Commentary
    # ---------------------------------

    if r_multiple is not None:

        if r_multiple >= 2:
            diagnosis.append("Captured strong expansion move.")

        if r_multiple <= -1:
            diagnosis.append("Full stop loss hit.")

        if -0.5 < r_multiple < 0.5:
            diagnosis.append("Weak move — lacked expansion.")

    # ---------------------------------
    # Build Report
    # ---------------------------------

    report = f"""📊 **AI Trade Review**

Setup: {setup}
Regime: {regime}
Volatility: {vol}
Confidence: {confidence}%
Result: {result.upper()}
"""

    if ml_prob is not None:
        report += f"ML Probability: {round(ml_prob * 100,1)}%\n"

    if r_multiple is not None:
        report += f"R-Multiple: {r_multiple}\n"

    if diagnosis:
        report += "\nDiagnosis:\n"
        for d in diagnosis:
            report += f"- {d}\n"

    return report
