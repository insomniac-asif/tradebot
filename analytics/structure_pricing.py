def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def long_option_max_loss(premium: float, qty: int) -> float | None:
    prem = _safe_float(premium)
    if prem is None:
        return None
    return prem * float(qty) * 100


def long_option_break_even(strike: float, premium: float, option_type: str) -> float | None:
    strike_val = _safe_float(strike)
    prem = _safe_float(premium)
    if strike_val is None or prem is None:
        return None
    opt = (option_type or "").upper()
    if opt == "CALL":
        return strike_val + prem
    if opt == "PUT":
        return strike_val - prem
    return None


def long_option_pnl(entry_price: float, exit_price: float, qty: int) -> float | None:
    entry_val = _safe_float(entry_price)
    exit_val = _safe_float(exit_price)
    if entry_val is None or exit_val is None:
        return None
    return (exit_val - entry_val) * float(qty) * 100
