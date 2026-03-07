# analytics/risk_control.py

from analytics.edge_stability import calculate_edge_stability
from analytics.edge_momentum import calculate_edge_momentum
from analytics.stability_mode import get_stability_mode
from analytics.capital_protection import get_capital_mode
from analytics.setup_intelligence import get_setup_intelligence
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.edge_compression import get_edge_compression
from analytics.regime_transition import detect_regime_transition
from analytics.regime_memory import get_regime_memory
from signals.regime import get_regime
from core.data_service import get_market_dataframe


def get_dynamic_risk_percent(acc):
    trade_log = acc.get("trade_log", [])
    closed = [t for t in trade_log if t.get("result") is not None]
    closed_count = len(closed)

    if closed_count < 50:
        return 0.005  # 0.5%

    if closed_count < 100:
        return 0.01  # 1%

    last_50 = closed[-50:]
    if not last_50:
        return 0.005

    wins = 0
    for t in last_50:
        result = t.get("result")
        if result is not None and str(result).lower() == "win":
            wins += 1

    winrate = wins / len(last_50)

    if winrate > 0.55:
        return 0.015
    if winrate < 0.45:
        return 0.005
    return 0.01


def dynamic_risk_percent(setup_type=None):

    """
    Regime + Stability + Capital Protection Risk Engine
    """

    # ------------------------------------------------
    # 1️⃣ Base Risk From Stability
    # ------------------------------------------------

    stability_data = calculate_edge_stability()

    if not stability_data:
        base_risk = 0.005  # ultra conservative early
        stability = 0.5
    else:
        stability = stability_data["stability"]
        base_risk = 0.005 + (stability * 0.01)

    base_risk = max(0.005, min(base_risk, 0.015))

    adjusted_risk = base_risk

    # ------------------------------------------------
    # 2️⃣ Stability Mode Layer
    # ------------------------------------------------

    mode = get_stability_mode()
    adjusted_risk *= mode["risk_multiplier"]

    compression = get_edge_compression()
    adjusted_risk *= compression["risk_multiplier"]

    # ------------------------------------------------
    # 3️⃣ Capital Protection Layer
    # ------------------------------------------------

    from analytics.capital_protection import get_capital_mode
    capital_mode = get_capital_mode()

    adjusted_risk *= capital_mode["risk_multiplier"]

    # ------------------------------------------------
    # 4️⃣ Regime Expectancy Layer
    # ------------------------------------------------

    df = get_market_dataframe()
    if df is not None:

        current_regime = get_regime(df)
        regime_stats = calculate_regime_expectancy()

        if regime_stats and current_regime in regime_stats:

            regime_data = regime_stats[current_regime]

            trades = regime_data["trades"]
            avg_R = regime_data["avg_R"]
            winrate = regime_data["winrate"]

            if trades >= 10:

                if avg_R > 0.5 and winrate > 55:
                    adjusted_risk *= 1.15

                if avg_R < 0:
                    adjusted_risk *= 0.80

                if winrate < 45:
                    adjusted_risk *= 0.85

            else:
                adjusted_risk *= 0.85  # not enough regime data

    # ------------------------------------------------
    # 5️⃣ Edge Momentum Layer
    # ------------------------------------------------

    momentum_data = calculate_edge_momentum()

    if momentum_data:
        momentum = momentum_data["momentum"]

        if momentum > 0.15:
            adjusted_risk *= 1.10

        elif momentum < -0.15:
            adjusted_risk *= 0.85

    # ------------------------------------------------
    # 6️⃣ Setup Intelligence Layer
    # ------------------------------------------------

    if setup_type:

        intelligence = get_setup_intelligence(
            setup_type=setup_type,
            regime=None,
            ml_probability=None
        )

        if intelligence:
            adjusted_risk *= (1 + intelligence["risk_boost"])
    transition_data = detect_regime_transition()

    if transition_data["transition"]:
        adjusted_risk *= (1 - transition_data["severity"] * 0.3)
    
    memory = get_regime_memory()

    # Reduce risk if regime is new
    if memory["trust"] < 1.0:
        adjusted_risk *= memory["trust"]

    # ------------------------------------------------
    # 7️⃣ Final Clamp
    # ------------------------------------------------

    adjusted_risk = max(0.003, min(adjusted_risk, 0.02))

    return round(adjusted_risk, 4)
