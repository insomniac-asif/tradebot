from analytics.indicators import compute_indicators, compute_zscores
from analytics.market_regime import compute_market_regime
from analytics.iv_features import compute_iv_features
from analytics.options_greeks import extract_greeks
from signals.volatility import volatility_state
from signals.session_classifier import classify_session


def compute_sim_features(df, context: dict | None = None, option_snapshot=None) -> dict:
    """
    Additive features for SIM analytics and later portfolio optimization.
    Safe to call every bar; does not alter any trading decisions.
    """
    context = context or {}
    features = {}
    orb_minutes = context.get("orb_minutes", 15)
    zscore_window = context.get("zscore_window", 30)
    features.update(compute_indicators(df, orb_minutes=orb_minutes))
    features.update(compute_zscores(df, window=zscore_window))
    features.update(compute_market_regime(df))
    try:
        features["volatility_state"] = volatility_state(df)
    except Exception:
        features["volatility_state"] = None
    try:
        ts = context.get("timestamp") or context.get("entry_time")
        features["session"] = classify_session(ts)
    except Exception:
        features["session"] = "UNKNOWN"

    # Option greeks (if snapshot provided)
    if option_snapshot is not None:
        features.update(extract_greeks(option_snapshot))

    # IV features (proxy)
    try:
        iv_series = context.get("iv_series")
    except Exception:
        iv_series = None
    features.update(compute_iv_features(iv_series, features.get("iv")))

    # Context passthrough
    for key in ("direction", "signal_mode", "horizon", "dte_min", "dte_max"):
        if key in context:
            features[key] = context.get(key)

    return features
