from datetime import datetime
import pytz
from typing import Dict, Tuple
from core.slippage import estimate_spread_pct, compute_slippage


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
    max_spread = float(profile.get("max_spread_pct", 0.15))
    if spread_pct > max_spread:
        return None, "spread_too_wide"
    mid = (bid + ask) / 2

    # Spread-aware slippage: use actual bid/ask spread
    real_spread_pct = estimate_spread_pct(bid=bid, ask=ask)
    slippage = compute_slippage(side, real_spread_pct)

    if side == "entry":
        fill_price = mid * (1 + slippage)
        price_source = "mid_plus_spread_slippage"
    else:
        fill_price = mid * (1 - slippage)
        price_source = "mid_minus_spread_slippage"
    return {
        "fill_price": round(fill_price, 4),
        "filled_qty": int(qty),
        "mid": round(mid, 4),
        "spread_pct": round(spread_pct, 4),
        "slippage_applied": round(slippage, 4),
        "side": side,
        "price_source": price_source
    }, None


def sim_compute_risk_dollars(balance: float, profile: dict) -> float:
    risk = balance * float(profile["risk_per_trade_pct"])
    # Floor scales with balance: minimum 1 % of balance, hard minimum $3.
    # Replaces the old hard $50 floor that was a large percentage of a small account.
    min_floor = max(3.0, balance * 0.01)
    return max(risk, min_floor)


def sim_should_trade_now(profile: dict) -> tuple[bool, str]:
    now_et = datetime.now(pytz.timezone("US/Eastern"))

    # Optional start-of-window gate
    entry_start = profile.get("entry_start_time_et")
    if entry_start is not None:
        try:
            h, m = map(int, str(entry_start).split(":"))
            start_time = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
            if now_et < start_time:
                return False, "before_entry_window"
        except Exception:
            pass

    try:
        h, m = map(int, profile["cutoff_time_et"].split(":"))
        cutoff = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
        if now_et >= cutoff:
            return False, "past_cutoff"
    except Exception:
        return False, "invalid_cutoff"
    return True, ""
