# analytics/regime_transition.py

from core.data_service import get_market_dataframe
from signals.regime import get_regime
from signals.volatility import volatility_state


def detect_regime_transition(lookback=30):
    """
    Detects unstable regime shifts.
    """

    df = get_market_dataframe()

    if df is None or len(df) < lookback:
        return {
            "transition": False,
            "severity": 0.0
        }

    recent_regimes = []

    # Check regime across recent candles
    for i in range(lookback):
        slice_df = df.iloc[:-i] if i != 0 else df
        regime = get_regime(slice_df)
        recent_regimes.append(regime)

    unique_regimes = len(set(recent_regimes))

    severity = 0.0

    # ----------------------------------------
    # 1️⃣ Regime Flipping
    # ----------------------------------------

    if unique_regimes >= 3:
        severity += 0.4
    elif unique_regimes == 2:
        severity += 0.2

    # ----------------------------------------
    # 2️⃣ Volatility Expansion Check
    # ----------------------------------------

    current_vol = volatility_state(df)
    prev_vol = volatility_state(df.iloc[:-10])

    if current_vol == "HIGH" and prev_vol in ["LOW", "NORMAL"]:
        severity += 0.3

    # ----------------------------------------
    # 3️⃣ Instability Threshold
    # ----------------------------------------

    transition = severity >= 0.4

    return {
        "transition": transition,
        "severity": round(severity, 2)
    }
