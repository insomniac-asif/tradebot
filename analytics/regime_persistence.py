# analytics/regime_persistence.py

from core.data_service import get_market_dataframe
from signals.regime import get_regime


def calculate_regime_persistence(lookback=40):
    """
    Measures how dominant current regime is.
    Returns score between 0 and 1.
    """

    df = get_market_dataframe()

    if df is None or len(df) < lookback:
        return {
            "persistence": 0.5,
            "current_regime": "UNKNOWN"
        }

    regimes = []

    for i in range(lookback):
        slice_df = df.iloc[:-i] if i != 0 else df
        regime = get_regime(slice_df)
        regimes.append(regime)

    current_regime = regimes[0]

    same_count = regimes.count(current_regime)

    persistence = same_count / lookback

    return {
        "persistence": round(persistence, 3),
        "current_regime": current_regime
    }
