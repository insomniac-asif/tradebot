"""
simulation/correlation_guard.py
Correlation-aware exposure management.

Symbols within the same correlation group (e.g. SPY/QQQ/IWM) are treated
as a single exposure bucket.  When too many sims hold the same directional
bet within a correlated group the guard blocks further entries.

VXX is inverse-correlated with equities: a BULLISH VXX position counts as
BEARISH equity_index exposure and vice-versa.
"""
import logging
from typing import Optional

from simulation.sim_portfolio import SimPortfolio


# ── Correlation groups ────────────────────────────────────────────────────
# Each symbol maps to its group name.  Symbols not listed are treated as
# their own independent group (no correlation adjustment).
_SYMBOL_TO_GROUP = {
    "SPY": "equity_index",
    "QQQ": "equity_index",
    "IWM": "equity_index",
    "TSLA": "mega_tech",
    "AAPL": "mega_tech",
    "NVDA": "mega_tech",
    "MSFT": "mega_tech",
}

# Groups whose direction should be flipped when aggregating into their
# *parent* group.  e.g. VXX BULLISH → equity_index BEARISH.
_INVERSE_MAP = {
    "VXX": "equity_index",  # VXX is inverse equity
}


def get_correlation_group(symbol: str) -> str:
    """Return the correlation group for *symbol*, or the symbol itself."""
    s = symbol.upper()
    if s in _INVERSE_MAP:
        return _INVERSE_MAP[s]
    return _SYMBOL_TO_GROUP.get(s, s)


def is_inverse(symbol: str) -> bool:
    """True if *symbol* has an inverse relationship to its group."""
    return symbol.upper() in _INVERSE_MAP


def effective_direction(symbol: str, direction: str) -> str:
    """Flip direction for inverse symbols so aggregation is consistent."""
    if is_inverse(symbol):
        return "BEARISH" if direction.upper() == "BULLISH" else "BULLISH"
    return direction.upper()


# ── Exposure scanner ──────────────────────────────────────────────────────

def count_correlated_exposure(
    direction: str,
    symbol: str,
    profiles: dict,
    exclude_sim: Optional[str] = None,
) -> int:
    """Count how many sims have open trades whose *effective* direction
    matches *direction* within the same correlation group as *symbol*.

    Each sim counts at most once (even if it has multiple open trades in
    the group).

    Parameters
    ----------
    direction : str
        The proposed trade direction ("BULLISH" / "BEARISH").
    symbol : str
        The symbol the new trade would be placed on.
    profiles : dict
        Full ``_PROFILES`` dict (sim_id → profile).
    exclude_sim : str, optional
        Sim to skip (e.g. the sim being evaluated).

    Returns
    -------
    int
        Number of sims with matching correlated exposure.
    """
    target_group = get_correlation_group(symbol)
    target_dir = effective_direction(symbol, direction)

    count = 0
    for sid, prof in profiles.items():
        if str(sid).startswith("_"):
            continue
        if exclude_sim and sid == exclude_sim:
            continue
        try:
            s = SimPortfolio(sid, prof)
            s.load()
            for t in s.open_trades:
                if not isinstance(t, dict):
                    continue
                t_sym = (t.get("symbol") or "SPY").upper()
                t_dir = (t.get("direction") or "").upper()
                if not t_dir:
                    continue
                t_group = get_correlation_group(t_sym)
                if t_group != target_group:
                    continue
                t_eff_dir = effective_direction(t_sym, t_dir)
                if t_eff_dir == target_dir:
                    count += 1
                    break  # one match per sim is enough
        except Exception:
            continue
    return count


def check_correlation_limit(
    direction: str,
    symbol: str,
    profiles: dict,
    max_correlated: int = 6,
    exclude_sim: Optional[str] = None,
) -> Optional[dict]:
    """Return a skip-result dict if the correlation limit is breached,
    or ``None`` if the entry is allowed.
    """
    current = count_correlated_exposure(
        direction, symbol, profiles, exclude_sim=exclude_sim,
    )
    if current >= max_correlated:
        group = get_correlation_group(symbol)
        eff_dir = effective_direction(symbol, direction)
        return {
            "reason": "correlated_exposure_limit",
            "correlation_group": group,
            "effective_direction": eff_dir,
            "current_count": current,
            "max_allowed": max_correlated,
        }
    return None
