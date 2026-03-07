import math
import pandas as pd

from signals.regime import get_regime


def _safe_float(val):
    try:
        if val is None:
            return None
        out = float(val)
        if math.isfinite(out):
            return out
    except (TypeError, ValueError):
        return None
    return None


def compute_market_regime(df) -> dict:
    """
    Additive regime metrics for SIM analytics.
    Returns a dict that can be stored in trade["feature_snapshot"].
    """
    if df is None or df.empty:
        return {}
    out = {"regime": get_regime(df)}
    try:
        close = pd.to_numeric(df["close"], errors="coerce")
        if close.notna().sum() >= 10:
            returns = close.pct_change().dropna()
            rv = returns.tail(30).std()
            if rv is not None:
                out["realized_vol_30"] = _safe_float(rv)
    except Exception:
        pass
    return out
