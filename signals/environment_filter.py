# signals/environment_filter.py

from signals.conviction import momentum_is_decaying


def adjust_confidence(base_confidence, regime, expectancy, df):

    adjusted = base_confidence
    penalties = []
    boosts = []

    if regime not in ["TREND", "VOLATILE"]:
        adjusted -= 0.10
        penalties.append("Unfavorable regime")

    expectancy_samples = 0
    if expectancy:
        expectancy_samples = int(expectancy.get("samples") or 0)

    if expectancy and expectancy_samples >= 30:
        if expectancy.get("avg_5m", 0) < 0:
            adjusted -= 0.10
            penalties.append("Negative 5m expectancy")

        if expectancy.get("wr_5m", 100) < 50:
            adjusted -= 0.05
            penalties.append("Low conviction winrate")

    if momentum_is_decaying(df):
        adjusted -= 0.07
        penalties.append("Momentum decay detected")

    if (
        regime in ["TREND", "VOLATILE"]
        and expectancy
        and expectancy_samples >= 30
        and expectancy.get("avg_5m", 0) > 0
    ):
        adjusted += 0.05
        boosts.append("Momentum regime alignment")

    adjusted = max(0, min(1, adjusted))

    return adjusted, penalties, boosts


def trader_environment_filter(df, model_direction, raw_confidence, expectancy, regime):

    adjusted_conf, penalties, boosts = adjust_confidence(
        raw_confidence,
        regime,
        expectancy,
        df
    )

    decay = momentum_is_decaying(df)

    allow_trade = True
    reason_block = []
    expectancy_samples = int(expectancy.get("samples") or 0) if expectancy else 0

    if adjusted_conf < 0.50:
        allow_trade = False
        reason_block.append("Adjusted confidence below 50%")

    if regime == "COMPRESSION":
        allow_trade = False
        reason_block.append("Compression regime")

    if expectancy and expectancy_samples >= 30 and expectancy.get("avg_5m", 0) < 0:
        allow_trade = False
        reason_block.append("Negative momentum expectancy")

    if decay:
        allow_trade = False
        reason_block.append("Momentum decaying")

    return {
        "allow": allow_trade,
        "adjusted_conf": adjusted_conf,
        "penalties": penalties,
        "boosts": boosts,
        "blocks": reason_block
    }
