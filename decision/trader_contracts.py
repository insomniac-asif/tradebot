# decision/trader_contracts.py
#
# Option contract selection helpers extracted from decision/trader.py.
# Covers: _get_option_client, _parse_option_symbol, _select_option_contract.
#
# All function signatures are identical to their originals in trader.py.
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime, date
import pytz
import os

from core.market_clock import market_is_open
from core.rate_limiter import rate_limit_sleep

from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.trading.enums import ContractType

from analytics.contract_logger import log_contract_attempt
from analytics.risk_control import get_dynamic_risk_percent
from analytics.edge_compression import get_edge_compression

from core.debug import debug_log


ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))


def _get_option_client():
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None
    return OptionHistoricalDataClient(api_key, secret_key)


def _parse_option_symbol(symbol):
    # OCC format: {UNDERLYING}{YYMMDD}{C|P}{########} — underlying 1-6 chars
    import re as _re_occ
    if not symbol or len(symbol) < 15:
        return None
    m = _re_occ.match(r'^([A-Z]{1,6})(\d{6})([CP])(\d{8})$', symbol)
    if not m:
        return None
    _und, date_part, cp, strike_part = m.groups()
    yy = int(date_part[0:2])
    mm = int(date_part[2:4])
    dd = int(date_part[4:6])
    expiry = date(2000 + yy, mm, dd)
    strike = int(strike_part) / 1000.0
    return expiry, cp, strike


def _select_option_contract(direction, underlying_price, symbol="SPY"):
    client = _get_option_client()
    if client is None or underlying_price is None:
        return None, None

    underlying_sym = symbol.upper()
    eastern = pytz.timezone("US/Eastern")
    today = datetime.now(eastern).date()
    contract_type = ContractType.CALL if direction == "bullish" else ContractType.PUT

    # Prefer same-day expiry when market is open.
    expiry_date = today if market_is_open() else None

    request = OptionChainRequest(
        underlying_symbol=underlying_sym,
        type=contract_type,
        expiration_date=expiry_date,
        strike_price_gte=underlying_price * 0.9,
        strike_price_lte=underlying_price * 1.1,
    )

    rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
    chain = client.get_option_chain(request)
    if not chain:
        request = OptionChainRequest(
            underlying_symbol=underlying_sym,
            type=contract_type,
            expiration_date_gte=today,
            strike_price_gte=underlying_price * 0.9,
            strike_price_lte=underlying_price * 1.1,
        )
        rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
        chain = client.get_option_chain(request)

    if not chain:
        return None, "no_option_chain"

    # Alpaca SDK may return a custom mapping type rather than a plain dict on
    # some SDK versions.  Normalise to dict so .items() is guaranteed to work.
    if not isinstance(chain, dict):
        try:
            chain = dict(chain)
        except (TypeError, ValueError):
            return None, "no_option_chain"
    if not chain:
        return None, "no_option_chain"

    candidates = []
    for symbol, snap in chain.items():
        parsed = _parse_option_symbol(symbol)
        if not parsed:
            continue
        exp, cp, strike = parsed
        if (cp == "C" and contract_type != ContractType.CALL) or (
            cp == "P" and contract_type != ContractType.PUT
        ):
            continue
        quote = getattr(snap, "latest_quote", None)
        bid = quote.bid_price if quote is not None else None
        ask = quote.ask_price if quote is not None else None
        if bid is None or ask is None:
            continue
        if bid <= 0 or ask <= 0:
            continue
        entry_price = (bid + ask) / 2
        candidates.append((symbol, exp, strike, bid, ask, entry_price))

    if not candidates:
        return None, "no_valid_quote"

    # Prefer nearest expiry (same-day already filtered if available), then closest ATM strike.
    candidates.sort(key=lambda x: (abs((x[1] - today).days), abs(x[2] - underlying_price)))
    attempts = 0
    for symbol, exp, strike, bid, ask, entry_price in candidates:
        if attempts >= 3:
            break
        attempts += 1
        spread = ask - bid
        if ask <= 0 or spread < 0:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="invalid_quote", bid=bid, ask=ask,
            )
            continue
        spread_pct = spread / ask
        if spread_pct > 0.15:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="spread_too_wide",
                bid=bid, ask=ask, spread_pct=spread_pct,
                mid=round((bid + ask) / 2, 4),
            )
            continue
        log_contract_attempt(
            source="main", direction=direction, underlying_price=underlying_price,
            expiry=exp, dte=abs((exp - today).days), strike=strike,
            result="selected", reason="selected",
            bid=bid, ask=ask, spread_pct=spread_pct,
            mid=round((bid + ask) / 2, 4),
        )
        return {
            "symbol": symbol,
            "expiry": exp.isoformat(),
            "strike": strike,
            "entry_price": float(entry_price),
            "bid": float(bid),
            "ask": float(ask),
        }, None

    return None, "spread_too_wide"


def build_execution_plan(
    acc,
    df,
    regime,
    vol_state,
    direction,
    style,
    price,
    setup_type,
    symbol="SPY",
):
    option, selection_block = _select_option_contract(direction, price, symbol=symbol)
    if selection_block:
        return {"block_reason": selection_block}
    if option is None:
        return {"block_reason": "no_option_chain"}

    entry_price = option["entry_price"]
    # One options contract = 100 shares.  risk_per_contract is the dollar
    # loss if the position hits the 25%-of-premium stop.
    risk_per_contract = entry_price * 100 * 0.25  # e.g. $1.50 mid → $37.50 risk/contract
    if risk_per_contract <= 0:
        return {"block_reason": "no_valid_quote"}

    # ----------------------------
    # Target Based on Style
    # ----------------------------
    if style == "momentum":
        target_R = 2.5
    elif style == "mini_swing":
        target_R = 2.0
    else:
        target_R = 1.2

    # ----------------------------
    # Position Sizing
    # ----------------------------
    risk_percent = get_dynamic_risk_percent(acc)
    debug_log("risk_percent_update", percent=risk_percent)
    effective_balance = min(
        acc["balance"],
        acc.get("virtual_capital_limit", acc["balance"])
    )
    risk_dollars = effective_balance * risk_percent
    if risk_dollars < 50:
        risk_dollars = 50

    quantity = int(risk_dollars // risk_per_contract)
    if quantity <= 0:
        return {"block_reason": "quantity_zero"}

    compression = get_edge_compression()
    quantity = int(quantity * compression["position_multiplier"])
    if quantity <= 0:
        return {"block_reason": "quantity_zero"}

    return risk_dollars, quantity, option, target_R
