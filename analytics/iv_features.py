def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def compute_iv_features(iv_series: list[float] | None, current_iv: float | None) -> dict:
    """
    Lightweight IV features (proxy). If history is missing, returns current IV only.
    """
    iv = _safe_float(current_iv)
    out = {"iv": iv}
    if iv is None or not iv_series:
        return out
    clean = [v for v in iv_series if _safe_float(v) is not None]
    if len(clean) < 5:
        return out
    try:
        low = min(clean)
        high = max(clean)
        if high > low:
            rank = (iv - low) / (high - low)
            if rank is not None:
                rank = max(0.0, min(1.0, rank))
            out["iv_rank_proxy"] = rank
    except Exception:
        pass
    return out
