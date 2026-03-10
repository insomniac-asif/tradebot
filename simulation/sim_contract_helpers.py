"""
simulation/sim_contract_helpers.py
Private helper functions for sim_contract.py.
Extracted to keep sim_contract.py under 500 lines.
"""
import os
import time
from collections import deque
from typing import Optional, Any

import alpaca.data.enums as alpaca_enums
from alpaca.data.requests import OptionSnapshotRequest, OptionChainRequest

try:
    from alpaca.trading.enums import ContractType as TradingContractType
except Exception:
    TradingContractType = None

from analytics.contract_logger import log_contract_attempt
from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep, get_cache, get_breaker, get_bucket

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

_CHAIN_CACHE_TTL = 30.0
_SNAPSHOT_CACHE_TTL = 10.0

_CHAIN_ERROR_EVENTS = deque()
_SNAPSHOT_ERROR_EVENTS = deque()
_LAST_CHAIN_ERROR = None
_LAST_SNAPSHOT_ERROR = None
_LAST_SUCCESS = None
_LAST_SNAPSHOT_PROBE = None
_IV_SERIES = deque(maxlen=int(os.getenv("SIM_IV_SERIES_MAX", "500")))


def _prune_events(q: deque, cutoff_ts: float) -> None:
    while q and q[0][0] < cutoff_ts:
        q.popleft()


def _record_error(kind: str, message: str) -> None:
    global _LAST_CHAIN_ERROR, _LAST_SNAPSHOT_ERROR
    now = time.time()
    if kind == "chain":
        _CHAIN_ERROR_EVENTS.append((now, message))
        _LAST_CHAIN_ERROR = (now, message)
    elif kind == "snapshot":
        _SNAPSHOT_ERROR_EVENTS.append((now, message))
        _LAST_SNAPSHOT_ERROR = (now, message)


def _record_success(symbol: str, spread_pct: "float | None", contract_type_char: "str | None") -> None:
    global _LAST_SUCCESS
    _LAST_SUCCESS = {
        "ts": time.time(),
        "symbol": symbol,
        "spread_pct": spread_pct,
        "contract_type": contract_type_char,
    }


def get_contract_error_stats(window_seconds: int = 3600) -> dict:
    now = time.time()
    cutoff = now - window_seconds
    _prune_events(_CHAIN_ERROR_EVENTS, cutoff)
    _prune_events(_SNAPSHOT_ERROR_EVENTS, cutoff)
    return {
        "chain_errors": len(_CHAIN_ERROR_EVENTS),
        "snapshot_errors": len(_SNAPSHOT_ERROR_EVENTS),
        "last_chain_error": _LAST_CHAIN_ERROR,
        "last_snapshot_error": _LAST_SNAPSHOT_ERROR,
        "last_success": _LAST_SUCCESS,
    }


def get_snapshot_probe() -> "dict | None":
    return _LAST_SNAPSHOT_PROBE


def record_iv_sample(iv: "float | None") -> None:
    try:
        if iv is None:
            return
        _IV_SERIES.append(float(iv))
    except Exception:
        return


def get_iv_series(window: "int | None" = None) -> "list[float]":
    try:
        if window is None:
            return list(_IV_SERIES)
        if window <= 0:
            return []
        return list(_IV_SERIES)[-int(window):]
    except Exception:
        return []


def _record_snapshot_probe(response, symbols: "list[str] | None" = None) -> None:
    global _LAST_SNAPSHOT_PROBE
    try:
        probe = {
            "ts": time.time(),
            "response_type": type(response).__name__,
            "symbols": symbols[:5] if symbols else [],
            "size": None,
            "keys": [],
            "data_attr": None,
            "snapshots_attr": None,
        }
        if isinstance(response, dict):
            probe["size"] = len(response)
            probe["keys"] = list(response.keys())[:5]
        else:
            data = getattr(response, "data", None)
            snaps = getattr(response, "snapshots", None)
            probe["data_attr"] = type(data).__name__ if data is not None else None
            probe["snapshots_attr"] = type(snaps).__name__ if snaps is not None else None
            if isinstance(data, dict):
                probe["size"] = len(data)
                probe["keys"] = list(data.keys())[:5]
            elif isinstance(snaps, dict):
                probe["size"] = len(snaps)
                probe["keys"] = list(snaps.keys())[:5]
            elif isinstance(data, list):
                probe["size"] = len(data)
            elif isinstance(snaps, list):
                probe["size"] = len(snaps)
        _LAST_SNAPSHOT_PROBE = probe
    except Exception:
        _LAST_SNAPSHOT_PROBE = {"ts": time.time(), "response_type": "error"}


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _extract_snapshot(response, symbol: str):
    if response is None or not symbol:
        return None
    if isinstance(response, dict):
        if symbol in response:
            return response.get(symbol)
        try:
            if len(response) == 1:
                return next(iter(response.values()))
        except Exception:
            pass
    for attr in ("snapshots", "data"):
        data = getattr(response, attr, None)
        if isinstance(data, dict):
            if symbol in data:
                return data.get(symbol)
            try:
                if len(data) == 1:
                    return next(iter(data.values()))
            except Exception:
                pass
        if isinstance(data, list):
            for item in data:
                sym = getattr(item, "symbol", None) if item is not None else None
                if sym is None and isinstance(item, dict):
                    sym = item.get("symbol")
                if sym == symbol:
                    return item
            try:
                if len(data) == 1:
                    return data[0]
            except Exception:
                pass
    try:
        if getattr(response, "symbol", None) == symbol:
            return response
    except Exception:
        pass
    return None


def _get_options_feed():
    feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
    if feed_enum is None:
        return None
    desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
    try:
        if desired == "opra":
            return feed_enum.OPRA
        if desired == "indicative":
            return feed_enum.INDICATIVE
    except Exception:
        return None
    return None


def _build_snapshot_request(symbols, feed_override=None):
    if isinstance(symbols, str):
        symbols = [symbols]
    elif symbols is None:
        symbols = []
    feed_val = feed_override if feed_override is not None else _get_options_feed()
    if feed_val is not None:
        try:
            return OptionSnapshotRequest(symbol_or_symbols=list(symbols), feed=feed_val)
        except TypeError:
            pass
    return OptionSnapshotRequest(symbol_or_symbols=list(symbols))


class _FallbackContractType:
    CALL = "call"
    PUT = "put"


ContractType = TradingContractType or getattr(alpaca_enums, "ContractType", _FallbackContractType)


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
) -> "tuple[Optional[dict], Optional[str]]":
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


def _build_occ_symbol(underlying_sym: str, expiry_date, contract_type_char: str, strike: float) -> str:
    """Build an OCC option symbol string."""
    date_str = expiry_date.strftime("%y%m%d")
    strike_int = int(round(strike * 1000))
    return f"{underlying_sym}{date_str}{contract_type_char}{strike_int:08d}"


def _is_chain_empty(chain_obj) -> bool:
    """Return True if a chain response object contains no data."""
    if chain_obj is None:
        return True
    if isinstance(chain_obj, dict):
        return len(chain_obj) == 0
    data = getattr(chain_obj, "data", None)
    if data is not None:
        try:
            return len(data) == 0
        except Exception:
            pass
    chains = getattr(chain_obj, "chains", None)
    if chains is not None:
        try:
            return len(chains) == 0
        except Exception:
            pass
    df = getattr(chain_obj, "df", None)
    if df is not None:
        try:
            return df.empty
        except Exception:
            pass
    return False


# Re-export large helpers from sim_contract_helpers2 to preserve public API
from simulation.sim_contract_helpers2 import (
    _fetch_chain_for_expiry,
    _iter_chain_symbols,
    _build_candidates_from_chain,
    _try_batch_snapshot_select,
    _try_single_strike_select,
)

