"""
simulation/sim_contract_helpers2.py
Second half of sim_contract_helpers — large chain/snapshot selection functions.
Extracted to keep sim_contract_helpers.py under 500 lines.
"""
import os
import alpaca.data.enums as alpaca_enums
from alpaca.data.requests import OptionSnapshotRequest, OptionChainRequest

from analytics.contract_logger import log_contract_attempt
from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep, get_cache, get_breaker, get_bucket

from simulation.sim_contract_helpers import (
    ALPACA_MIN_CALL_INTERVAL_SEC,
    _CHAIN_CACHE_TTL,
    _SNAPSHOT_CACHE_TTL,
    _record_error,
    _record_snapshot_probe,
    _record_success,
    _safe_float,
    _extract_snapshot,
    _get_options_feed,
    _build_snapshot_request,
    _attempt_contract_select,
    _build_occ_symbol,
)


def _fetch_chain_for_expiry(
    client,
    underlying_sym: str,
    direction: str,
    expiry_date,
    contract_type_enum,
) -> "tuple[object | None, object | None, str | None]":
    """Fetch an option chain for a single expiry date.

    Returns (chain, feed_val, error_reason).
    error_reason is non-None only if a circuit-breaker or exception prevented the fetch.
    """
    if not get_breaker().allow_request():
        return None, None, "circuit_breaker_open"
    feed_val = None
    _chain_cache_key = f"chain:{underlying_sym}:{direction}:{expiry_date.isoformat()}"
    chain = get_cache().get(_chain_cache_key)
    if chain is not None:
        return chain, feed_val, None  # cache hit
    try:
        get_bucket().acquire_wait()
        rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
        feed_val = _get_options_feed()
        chain = client.get_option_chain(
            OptionChainRequest(
                underlying_symbol=underlying_sym,
                type=contract_type_enum,
                feed=feed_val,
                expiration_date=expiry_date
            )
        )
        get_breaker().record_success()
        get_cache().set(_chain_cache_key, chain, _CHAIN_CACHE_TTL)
        try:
            sample_sym = None
            if isinstance(chain, dict) and chain:
                sample_sym = next(iter(chain.keys()))
            else:
                data = getattr(chain, "data", None)
                if isinstance(data, dict) and data:
                    sample_sym = next(iter(data.keys()))
                elif isinstance(data, list) and data:
                    sample_sym = getattr(data[0], "symbol", None) or (
                        data[0].get("symbol") if isinstance(data[0], dict) else None
                    )
            debug_log(
                "sim_chain_loaded",
                expiry=expiry_date.isoformat(),
                direction=direction,
                sample_symbol=sample_sym,
                feed=str(feed_val) if feed_val is not None else "default",
            )
        except Exception:
            pass
        return chain, feed_val, None
    except Exception as e:
        get_breaker().record_failure()
        try:
            import logging
            logging.warning("sim_contract_chain_error: %s", e)
        except Exception:
            pass
        _record_error("chain", str(e))
        return None, None, "chain_error"


def _iter_chain_symbols(chain_obj):
    """Yield all option symbols from a chain response object."""
    if isinstance(chain_obj, dict):
        yield from chain_obj.keys()
        return
    data = getattr(chain_obj, "data", None)
    if isinstance(data, dict):
        yield from data.keys()
        return
    if isinstance(data, list):
        for item in data:
            sym = getattr(item, "symbol", None) if item is not None else None
            if sym is None and isinstance(item, dict):
                sym = item.get("symbol")
            if sym:
                yield sym
        return
    chains = getattr(chain_obj, "chains", None)
    if isinstance(chains, dict):
        yield from chains.keys()
        return
    if isinstance(chains, list):
        for item in chains:
            sym = getattr(item, "symbol", None) if item is not None else None
            if sym is None and isinstance(item, dict):
                sym = item.get("symbol")
            if sym:
                yield sym
        return
    df = getattr(chain_obj, "df", None)
    if df is not None:
        try:
            if "symbol" in df.columns:
                yield from df["symbol"].dropna().tolist()
            else:
                yield from df.index.tolist()
        except Exception:
            return


def _build_candidates_from_chain(
    chain,
    underlying_price: float,
    strike_retry: list,
) -> "tuple[list, list, bool]":
    """Build (symbol_candidates, strike_candidates, chain_symbols_found) from a chain response."""
    strike_candidates = list(dict.fromkeys(strike_retry[:3]))
    chain_symbols_found = False
    symbol_candidates: list = []
    if chain is None:
        return symbol_candidates, strike_candidates, chain_symbols_found
    try:
        for sym in _iter_chain_symbols(chain):
            chain_symbols_found = True
            try:
                if isinstance(sym, str) and len(sym) >= 15:
                    strike_part = sym[-8:]
                    strike_val = int(strike_part) / 1000.0
                    symbol_candidates.append((sym, strike_val))
            except Exception:
                continue
        symbol_candidates = sorted(symbol_candidates, key=lambda s: abs(s[1] - underlying_price))
        for _s, sv in symbol_candidates[:5]:
            strike_candidates.append(int(round(sv)))
    except Exception:
        pass
    strike_candidates = strike_candidates[:8]
    symbol_candidates = symbol_candidates[:8]
    return symbol_candidates, strike_candidates, chain_symbols_found


def _try_batch_snapshot_select(
    client,
    symbol_candidates: list,
    expiry_date,
    dte: int,
    direction: str,
    underlying_price: float,
    profile: dict,
    contract_type_char: str,
    feed_val,
    reason_counts: dict,
) -> "tuple[dict | None, str | None] | None":
    """Try to select a contract via batch snapshot of chain symbols.

    Returns (result_dict, None) if a contract is found.
    Returns None if not found (updates reason_counts in-place).
    Raises nothing — all exceptions are caught internally.
    """
    if not symbol_candidates:
        return None
    try:
        batch_symbols = [s for s, _ in symbol_candidates if s]
        if not batch_symbols:
            return None
        if not get_breaker().allow_request():
            return None  # circuit breaker; caller must set last_reason
        _snap_cache_key = f"snaps:{':'.join(batch_symbols[:3])}"
        snapshots = get_cache().get(_snap_cache_key)
        if snapshots is None:
            get_bucket().acquire_wait()
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            debug_log(
                "sim_snapshot_request",
                symbols=batch_symbols[:5],
                count=len(batch_symbols),
                expiry=expiry_date.isoformat(),
                direction=direction,
            )
            snapshots = client.get_option_snapshot(
                _build_snapshot_request(batch_symbols, feed_override=feed_val)
            )
            get_breaker().record_success()
            get_cache().set(_snap_cache_key, snapshots, _SNAPSHOT_CACHE_TTL)
        _record_snapshot_probe(snapshots, batch_symbols)
        try:
            if isinstance(snapshots, dict) and len(snapshots) == 0:
                _record_error("snapshot", "empty_snapshot_response")
        except Exception:
            pass
        if isinstance(snapshots, dict) and len(snapshots) == 0 and feed_val is None:
            try:
                feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                    debug_log("sim_snapshot_retry_feed", feed="indicative", count=len(batch_symbols))
                    snapshots = client.get_option_snapshot(
                        _build_snapshot_request(batch_symbols, feed_override=feed_enum.INDICATIVE)
                    )
                    _record_snapshot_probe(snapshots, batch_symbols)
            except Exception as e:
                _record_error("snapshot", f"indicative_retry_error: {str(e)}")
        for symbol, strike in symbol_candidates:
            snap = _extract_snapshot(snapshots, symbol)
            if snap is None:
                reason_counts["no_snapshot"] = reason_counts.get("no_snapshot", 0) + 1
                debug_log("sim_snapshot_missing", symbol=symbol, expiry=expiry_date.isoformat(), strike=strike)
                try:
                    _record_error("snapshot", f"no_snapshot symbol={symbol}")
                except Exception:
                    pass
                continue
            result, reject_reason = _attempt_contract_select(
                snap, symbol, strike, underlying_price, profile,
                direction, expiry_date, dte, contract_type_char,
            )
            if reject_reason is not None:
                reason_counts[reject_reason] = reason_counts.get(reject_reason, 0) + 1
                continue
            if result is not None:
                log_contract_attempt(
                    source=f"sim:{profile.get('sim_id', 'unknown')}",
                    direction=direction, underlying_price=underlying_price,
                    expiry=expiry_date, dte=dte, strike=strike,
                    result="selected", reason="selected",
                    bid=result["bid"], ask=result["ask"], mid=result["mid"],
                    spread_pct=round(result["spread_pct"], 4),
                    iv=result["iv"], delta=result["delta"], gamma=result["gamma"],
                    theta=result["theta"], vega=result["vega"],
                )
                _record_success(symbol, result["spread_pct"], contract_type_char)
                result["selection_method"] = "chain_symbols"
                return result, None
    except Exception as e:
        get_breaker().record_failure()
        _record_error("snapshot", f"batch_snapshot_error: {str(e)}")
    return None


def _try_single_strike_select(
    client,
    symbol_item,
    expiry_date,
    dte: int,
    direction: str,
    underlying_price: float,
    profile: dict,
    contract_type_char: str,
    underlying_sym: str,
    feed_val,
    reason_counts: dict,
) -> "tuple[dict | None, str | None] | tuple[None, str]":
    """Try to select a contract by fetching a single symbol/strike snapshot.

    Returns (result_dict, None) if selected.
    Returns (None, reason_str) if rejected, no_snapshot, or error.
    """
    strike = None
    try:
        if isinstance(symbol_item, tuple):
            symbol = symbol_item[0]
            strike = symbol_item[1]
        else:
            symbol = None
            strike = symbol_item
        if not symbol:
            symbol = _build_occ_symbol(underlying_sym, expiry_date, contract_type_char, strike)
        if not get_breaker().allow_request():
            return None, "circuit_breaker_open"
        get_bucket().acquire_wait()
        rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
        debug_log(
            "sim_snapshot_request_single",
            symbol=symbol,
            expiry=expiry_date.isoformat(),
            strike=strike,
            direction=direction,
        )
        try:
            snapshots = client.get_option_snapshot(
                _build_snapshot_request([symbol], feed_override=feed_val)
            )
            get_breaker().record_success()
        except Exception as exc:
            get_breaker().record_failure()
            raise exc
        _record_snapshot_probe(snapshots, [symbol])
        if isinstance(snapshots, dict) and len(snapshots) == 0 and feed_val is None:
            try:
                feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                    debug_log("sim_snapshot_retry_feed", feed="indicative", symbol=symbol)
                    snapshots = client.get_option_snapshot(
                        _build_snapshot_request([symbol], feed_override=feed_enum.INDICATIVE)
                    )
                    _record_snapshot_probe(snapshots, [symbol])
            except Exception as e:
                _record_error("snapshot", f"indicative_retry_error: {str(e)}")
        snap = _extract_snapshot(snapshots, symbol)
        if snap is None:
            reason_counts["no_snapshot"] = reason_counts.get("no_snapshot", 0) + 1
            try:
                resp_type = type(snapshots).__name__
                keys_hint = ""
                if isinstance(snapshots, dict):
                    keys = list(snapshots.keys())[:5]
                    keys_hint = f" keys={keys}"
                _record_error("snapshot", f"no_snapshot symbol={symbol} type={resp_type}{keys_hint}")
            except Exception:
                pass
            log_contract_attempt(
                source=f"sim:{profile.get('sim_id', 'unknown')}",
                direction=direction, underlying_price=underlying_price,
                expiry=expiry_date, dte=dte, strike=strike,
                result="rejected", reason="no_snapshot",
            )
            return None, "no_snapshot"
        result, reject_reason = _attempt_contract_select(
            snap, symbol, strike, underlying_price, profile,
            direction, expiry_date, dte, contract_type_char,
        )
        if reject_reason is not None:
            reason_counts[reject_reason] = reason_counts.get(reject_reason, 0) + 1
            _greeks = getattr(snap, "greeks", None)
            _iv    = _safe_float(getattr(_greeks, "implied_volatility", None)) if _greeks else None
            _delta = _safe_float(getattr(_greeks, "delta", None)) if _greeks else None
            _gamma = _safe_float(getattr(_greeks, "gamma", None)) if _greeks else None
            _theta = _safe_float(getattr(_greeks, "theta", None)) if _greeks else None
            _vega  = _safe_float(getattr(_greeks, "vega", None)) if _greeks else None
            _quote = getattr(snap, "latest_quote", None)
            _bid = float(_quote.bid_price) if _quote and _quote.bid_price is not None else 0.0
            _ask = float(_quote.ask_price) if _quote and _quote.ask_price is not None else 0.0
            _mid = round((_bid + _ask) / 2, 4) if _bid > 0 and _ask > 0 else None
            _sp = round((_ask - _bid) / _ask, 4) if _ask > 0 else None
            log_contract_attempt(
                source=f"sim:{profile.get('sim_id', 'unknown')}",
                direction=direction, underlying_price=underlying_price,
                expiry=expiry_date, dte=dte, strike=strike,
                result="rejected", reason=reject_reason,
                bid=_bid if _bid else None, ask=_ask if _ask else None,
                mid=_mid, spread_pct=_sp,
                iv=_iv, delta=_delta, gamma=_gamma, theta=_theta, vega=_vega,
            )
            return None, reject_reason
        if result is not None:
            log_contract_attempt(
                source=f"sim:{profile.get('sim_id', 'unknown')}",
                direction=direction, underlying_price=underlying_price,
                expiry=expiry_date, dte=dte, strike=strike,
                result="selected", reason="selected",
                bid=result["bid"], ask=result["ask"], mid=result["mid"],
                spread_pct=round(result["spread_pct"], 4),
                iv=result["iv"], delta=result["delta"], gamma=result["gamma"],
                theta=result["theta"], vega=result["vega"],
            )
            _record_success(symbol, result["spread_pct"], contract_type_char)
            return result, None
        return None, "no_result"
    except Exception as e:
        reason_counts["snapshot_error"] = reason_counts.get("snapshot_error", 0) + 1
        log_contract_attempt(
            source=f"sim:{profile.get('sim_id', 'unknown')}",
            direction=direction, underlying_price=underlying_price,
            expiry=expiry_date, dte=dte, strike=strike,
            result="error", reason="snapshot_error",
        )
        try:
            import logging
            logging.warning("sim_contract_snapshot_error: %s", e)
        except Exception:
            pass
        _record_error("snapshot", str(e))
        return None, "snapshot_error"
