"""core/slippage.py
Spread-aware slippage model shared by backtest engine and sim executor.

Instead of a flat 1% slippage, we estimate the bid/ask spread from the option's
characteristics and compute slippage as a fraction of the spread.
"""


# ── Spread estimation lookup ─────────────────────────────────────────────────
# Key: (dte_bucket, moneyness_bucket) → estimated spread as fraction of mid price
# moneyness_bucket: "ATM" (< 1%), "NTM" (1-2%), "OTM" (> 2%)
# dte_bucket: 0 (0DTE), 1 (1-2 DTE), 7 (3-7 DTE), 14 (8-14 DTE), 30 (15+ DTE)

_SPREAD_TABLE = {
    (0,  "ATM"): 0.05,    # 0DTE ATM: ~5% spread
    (0,  "NTM"): 0.08,    # 0DTE near-the-money: ~8%
    (0,  "OTM"): 0.15,    # 0DTE OTM: ~15% spread
    (1,  "ATM"): 0.04,    # 1-2 DTE ATM: ~4%
    (1,  "NTM"): 0.06,
    (1,  "OTM"): 0.12,
    (7,  "ATM"): 0.03,    # 3-7 DTE ATM: ~3%
    (7,  "NTM"): 0.05,
    (7,  "OTM"): 0.10,
    (14, "ATM"): 0.025,   # 8-14 DTE: ~2.5%
    (14, "NTM"): 0.04,
    (14, "OTM"): 0.08,
    (30, "ATM"): 0.02,    # 15+ DTE: ~2%
    (30, "NTM"): 0.03,
    (30, "OTM"): 0.06,
}


def _dte_bucket(dte: int) -> int:
    if dte <= 0:
        return 0
    if dte <= 2:
        return 1
    if dte <= 7:
        return 7
    if dte <= 14:
        return 14
    return 30


def _moneyness_bucket(otm_pct: float) -> str:
    """otm_pct = abs(strike - underlying) / underlying."""
    if otm_pct < 0.01:
        return "ATM"
    if otm_pct < 0.02:
        return "NTM"
    return "OTM"


def estimate_spread_pct(
    dte: int = 1,
    otm_pct: float = 0.01,
    bid: float | None = None,
    ask: float | None = None,
) -> float:
    """
    Return spread as a fraction of mid price.

    If bid/ask are available, compute directly.
    Otherwise, estimate from DTE and moneyness using the lookup table.
    """
    if bid is not None and ask is not None and bid > 0 and ask > bid:
        mid = (bid + ask) / 2
        return (ask - bid) / mid

    db = _dte_bucket(int(dte))
    mb = _moneyness_bucket(abs(float(otm_pct)))
    return _SPREAD_TABLE.get((db, mb), 0.05)


def compute_slippage(
    side: str,
    spread_pct: float,
) -> float:
    """
    Compute slippage as fraction of mid price.

    Entry: pay mid + half the spread (fill towards the ask).
    Exit:  receive mid - 60% of the spread (slightly worse on exits).
    Floor at 1% to avoid unrealistically cheap fills.
    """
    if side == "entry":
        return max(0.01, spread_pct * 0.5)
    else:
        return max(0.01, spread_pct * 0.6)
