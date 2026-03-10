import os
import pytz
from datetime import datetime, timedelta, date
from typing import Optional, Any
from alpaca.data.historical import OptionHistoricalDataClient
import alpaca.data.enums as alpaca_enums

from analytics.contract_logger import log_contract_attempt
from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep, get_cache, get_breaker, get_bucket

# Helpers extracted to keep this file under 500 lines
from simulation.sim_contract_helpers import (
    ALPACA_MIN_CALL_INTERVAL_SEC,
    _CHAIN_CACHE_TTL,
    _SNAPSHOT_CACHE_TTL,
    ContractType,
    _prune_events,
    _record_error,
    _record_success,
    _record_snapshot_probe,
    _safe_float,
    _extract_snapshot,
    _get_options_feed,
    _build_snapshot_request,
    _build_contract_result,
    _attempt_contract_select,
    _build_occ_symbol,
    _is_chain_empty,
    _fetch_chain_for_expiry,
    _iter_chain_symbols,
    _build_candidates_from_chain,
    _try_batch_snapshot_select,
    _try_single_strike_select,
    get_contract_error_stats,
    get_snapshot_probe,
    record_iv_sample,
    get_iv_series,
)


def _resolve_contract_params(
    profile: dict,
    direction: str,
    underlying_price: float,
    now_et: datetime,
    symbol: Optional[str],
):
    """Resolve and validate contract selection parameters from profile.

    Returns a tuple of:
        (underlying_sym, candidate_dates, otm_pct, contract_type_enum,
         contract_type_char, strike_retry)
    or raises ValueError with a reason string as the message on failure.
    """
    today = now_et.date()

    # Resolve underlying symbol: explicit param > profile key > default SPY
    underlying_sym = (symbol or profile.get("symbol") or "SPY").upper()

    dte_min = int(profile["dte_min"])
    dte_max = int(profile["dte_max"])
    # Build candidate expiries using trading-day DTE (weekdays only).
    candidate_dates: list[tuple[date, int]] = []
    trading_dte = 0
    expiry_date = today
    while trading_dte <= dte_max:
        if expiry_date.weekday() < 5:
            dte = trading_dte
            if dte_min <= dte <= dte_max:
                if not (expiry_date == today and (now_et.hour, now_et.minute) >= (13, 30)):
                    candidate_dates.append((expiry_date, dte))
            trading_dte += 1
        expiry_date += timedelta(days=1)
    candidate_dates.sort(key=lambda x: x[1])
    if not candidate_dates:
        if dte_max == 0 and (now_et.hour, now_et.minute) >= (13, 30):
            raise ValueError("cutoff_passed")
        raise ValueError("no_candidate_expiry")

    otm_pct = float(profile["otm_pct"])
    contract_type_enum: Any = ContractType.CALL if direction == "BULLISH" else ContractType.PUT
    if isinstance(contract_type_enum, str):
        contract_type_enum = contract_type_enum.lower()
    else:
        try:
            # If enum-like, keep value to satisfy pydantic validation.
            if hasattr(contract_type_enum, "value"):
                contract_type_enum = contract_type_enum.value
        except Exception:
            pass
    contract_type_char = "C" if direction == "BULLISH" else "P"
    # Round to nearest integer strike and probe ATM-2 through ATM+2.
    # Works for SPY/QQQ/IWM (whole-dollar strikes); chain walk below handles finer granularity.
    if direction == "BULLISH":
        base_strike = underlying_price * (1 + otm_pct)
        base_whole = round(base_strike)
        strike_retry = [
            base_whole,
            base_whole - 1,
            base_whole + 1,
            base_whole - 2,
            base_whole + 2,
        ]
    else:
        base_strike = underlying_price * (1 - otm_pct)
        base_whole = round(base_strike)
        strike_retry = [
            base_whole,
            base_whole + 1,
            base_whole - 1,
            base_whole + 2,
            base_whole - 2,
        ]

    return underlying_sym, candidate_dates, otm_pct, contract_type_enum, contract_type_char, strike_retry


def _build_contract_result(
    option_symbol: str,
    expiry_date,
    dte: int,
    strike: float,
    contract_type_char: str,
    bid: float,
    ask: float,
    mid: float,
    spread_pct: float,
    underlying_price: float,
    otm_pct_applied: float,
    selection_method: str,
    iv, delta, gamma, theta, vega,
) -> dict:
    """Build the standard contract result dictionary."""
    return {
        "option_symbol": option_symbol,
        "expiry": expiry_date.isoformat(),
        "dte": dte,
        "strike": strike,
        "contract_type": contract_type_char,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_pct": round(spread_pct, 4),
        "underlying_price": underlying_price,
        "otm_pct_applied": round(otm_pct_applied, 6),
        "selection_method": selection_method,
        "iv": iv,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
    }


def _attempt_contract_select(
    snap,
    symbol: str,
    strike: float,
    underlying_price: float,
    profile: dict,
    direction: str,
    expiry_date,
    dte: int,
    contract_type_char: str,
) -> tuple[Optional[dict], Optional[str]]:
    """Process a single option snapshot to determine if it is selectable.

    Returns:
        (result_dict, None)  — contract accepted
        (None, reason_str)   — contract rejected for reason_str
        (None, "")           — snapshot missing (no_snapshot)
    """
    if snap is None:
        return None, ""

    greeks = getattr(snap, "greeks", None)
    iv    = _safe_float(getattr(greeks, "implied_volatility", None)) if greeks else None
    delta = _safe_float(getattr(greeks, "delta", None)) if greeks else None
    gamma = _safe_float(getattr(greeks, "gamma", None)) if greeks else None
    theta = _safe_float(getattr(greeks, "theta", None)) if greeks else None
    vega  = _safe_float(getattr(greeks, "vega", None)) if greeks else None

    record_iv_sample(iv)
    quote = getattr(snap, "latest_quote", None)
    if quote is None:
        return None, "no_quote"

    bid = float(quote.bid_price) if quote.bid_price is not None else 0.0
    ask = float(quote.ask_price) if quote.ask_price is not None else 0.0
    if bid <= 0 or ask <= 0:
        return None, "invalid_quote"

    spread_pct = (ask - bid) / ask
    mid = round((bid + ask) / 2, 4)
    if spread_pct > float(profile["max_spread_pct"]):
        return None, "spread_too_wide"

    otm_pct_applied = abs(strike - underlying_price) / underlying_price
    result = _build_contract_result(
        symbol, expiry_date, dte, strike, contract_type_char,
        bid, ask, mid, spread_pct, underlying_price, otm_pct_applied,
        "otm_pct", iv, delta, gamma, theta, vega,
    )
    return result, None


def select_sim_contract_with_reason(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None,
    symbol: str | None = None,
) -> tuple[dict | None, str | None]:
    if direction not in {"BULLISH", "BEARISH"}:
        return None, "invalid_direction"
    if underlying_price is None or underlying_price <= 0:
        return None, "invalid_price"
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None, "missing_api_keys"
    if now_et is None:
        now_et = datetime.now(pytz.timezone("US/Eastern"))

    try:
        underlying_sym, candidate_dates, otm_pct, contract_type_enum, contract_type_char, strike_retry = \
            _resolve_contract_params(profile, direction, underlying_price, now_et, symbol)
    except ValueError as exc:
        return None, str(exc)

    client = OptionHistoricalDataClient(api_key, secret_key)
    last_reason = "no_contract"

    for expiry_date, dte in candidate_dates:
        chain, feed_val, fetch_error = _fetch_chain_for_expiry(
            client, underlying_sym, direction, expiry_date, contract_type_enum
        )
        if fetch_error == "circuit_breaker_open":
            last_reason = "circuit_breaker_open"
            break
        if fetch_error:
            last_reason = fetch_error
            chain = None
        if chain is not None and _is_chain_empty(chain):
            last_reason = "empty_chain"
            continue

        symbol_candidates, strike_candidates, chain_symbols_found = _build_candidates_from_chain(
            chain, underlying_price, strike_retry
        )
        if chain is not None and not chain_symbols_found:
            last_reason = "no_chain_symbols"

        reason_counts: dict = {
            "no_snapshot": 0, "no_quote": 0, "invalid_quote": 0,
            "spread_too_wide": 0, "snapshot_error": 0,
        }
        symbol_loop = symbol_candidates if symbol_candidates else [(None, s) for s in strike_candidates]

        # ── Phase 1: batch snapshot (chain symbols available) ─────────────────
        if symbol_candidates:
            found = _try_batch_snapshot_select(
                client, symbol_candidates, expiry_date, dte, direction,
                underlying_price, profile, contract_type_char, feed_val, reason_counts,
            )
            if found is not None:
                return found
            # Update last_reason from reason_counts after batch attempt
            if reason_counts.get("no_snapshot", 0) > 0:
                last_reason = "no_snapshot"
            elif reason_counts:
                nr = max(reason_counts.items(), key=lambda x: x[1])
                if nr[1] > 0:
                    last_reason = nr[0]
            else:
                last_reason = "circuit_breaker_open"

        # ── Phase 2: individual strike fallback ───────────────────────────────
        for symbol_item in symbol_loop:
            result_tuple = _try_single_strike_select(
                client, symbol_item, expiry_date, dte, direction,
                underlying_price, profile, contract_type_char,
                underlying_sym, feed_val, reason_counts,
            )
            if result_tuple[0] is not None:
                return result_tuple
            r = result_tuple[1]
            if r:
                last_reason = r

        # ── Summarize reason after exhausting all strikes ─────────────────────
        total_attempts = sum(reason_counts.values())
        if total_attempts > 0:
            top_reason = max(reason_counts.items(), key=lambda x: x[1])[0]
            if reason_counts[top_reason] == total_attempts:
                last_reason = f"{top_reason}_all"
            else:
                last_reason = f"{top_reason}_most"
        if chain is not None and not chain_symbols_found and last_reason in {
            "no_snapshot_all", "no_snapshot_most",
            "no_quote_all", "no_quote_most",
            "invalid_quote_all", "invalid_quote_most",
            "spread_too_wide_all", "spread_too_wide_most",
            "snapshot_error_all", "snapshot_error_most",
        }:
            last_reason = f"{last_reason}_no_chain_symbols"

    return None, last_reason


def select_sim_contract(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None,
    symbol: str | None = None,
) -> dict | None:
    contract, _reason = select_sim_contract_with_reason(
        direction,
        underlying_price,
        profile,
        now_et=now_et,
        symbol=symbol
    )
    return contract
