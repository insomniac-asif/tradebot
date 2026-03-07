def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def extract_greeks(snapshot) -> dict:
    if snapshot is None:
        return {}
    greeks = getattr(snapshot, "greeks", None)
    if greeks is None and isinstance(snapshot, dict):
        greeks = snapshot.get("greeks")
    if greeks is None:
        return {}

    def _pick(obj, *keys):
        if isinstance(obj, dict):
            for key in keys:
                if key in obj:
                    return obj.get(key)
            return None
        for key in keys:
            try:
                if hasattr(obj, key):
                    return getattr(obj, key)
            except Exception:
                continue
        return None

    return {
        "iv": _safe_float(_pick(greeks, "implied_volatility", "iv", "impliedVolatility")),
        "delta": _safe_float(_pick(greeks, "delta")),
        "gamma": _safe_float(_pick(greeks, "gamma")),
        "theta": _safe_float(_pick(greeks, "theta")),
        "vega": _safe_float(_pick(greeks, "vega")),
    }


def extract_greeks_from_trade(trade: dict | None) -> dict:
    if not isinstance(trade, dict):
        return {}
    return {
        "iv": trade.get("iv_at_entry"),
        "delta": trade.get("delta_at_entry"),
        "gamma": trade.get("gamma_at_entry"),
        "theta": trade.get("theta_at_entry"),
        "vega": trade.get("vega_at_entry"),
    }
