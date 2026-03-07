from datetime import datetime
import pytz
from typing import Dict, Tuple


def sim_try_fill(
    option_symbol: str,
    qty: int,
    bid: float,
    ask: float,
    profile: dict,
    side: str = "entry"
) -> tuple[dict | None, str | None]:
    if side not in {"entry", "exit"}:
        return None, "invalid_side"
    if bid <= 0 or ask <= 0 or ask < bid:
        return None, "invalid_quote"
    spread_pct = (ask - bid) / ask
    if spread_pct > profile["max_spread_pct"]:
        return None, "spread_too_wide"
    mid = (bid + ask) / 2
    slippage = float(profile.get("entry_slippage" if side == "entry" else "exit_slippage", 0.01))
    if side == "entry":
        fill_price = mid * (1 + slippage)
        price_source = "mid_plus_slippage"
    else:
        fill_price = mid * (1 - slippage)
        price_source = "mid_minus_slippage"
    return {
        "fill_price": round(fill_price, 4),
        "filled_qty": int(qty),
        "mid": round(mid, 4),
        "spread_pct": round(spread_pct, 4),
        "slippage_applied": slippage,
        "side": side,
        "price_source": price_source
    }, None


def sim_compute_risk_dollars(balance: float, profile: dict) -> float:
    risk = balance * float(profile["risk_per_trade_pct"])
    return max(risk, 50.0)


def sim_should_trade_now(profile: dict) -> tuple[bool, str]:
    now_et = datetime.now(pytz.timezone("US/Eastern"))
    try:
        h, m = map(int, profile["cutoff_time_et"].split(":"))
        cutoff = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
        if now_et >= cutoff:
            return False, "past_cutoff"
    except Exception:
        return False, "invalid_cutoff"
    return True, ""
