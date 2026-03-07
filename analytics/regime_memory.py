# analytics/regime_memory.py

from analytics.regime_persistence import calculate_regime_persistence


_previous_regime = None
_regime_age = 0


def get_regime_memory():
    """
    Tracks regime age and transition confidence.
    """

    global _previous_regime
    global _regime_age

    data = calculate_regime_persistence()

    current_regime = data["current_regime"]

    if _previous_regime is None:
        _previous_regime = current_regime
        _regime_age = 1

    if current_regime == _previous_regime:
        _regime_age += 1
    else:
        _previous_regime = current_regime
        _regime_age = 1

    # Trust factor grows with age
    if _regime_age < 5:
        trust = 0.5
    elif _regime_age < 15:
        trust = 0.75
    else:
        trust = 1.0

    return {
        "regime": current_regime,
        "age": _regime_age,
        "trust": trust
    }
