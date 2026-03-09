import os
import pytz
import time
from collections import deque
from datetime import datetime, timedelta, date
from typing import Optional, Any
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest, OptionChainRequest
import alpaca.data.enums as alpaca_enums
try:
    from alpaca.trading.enums import ContractType as TradingContractType
except Exception:
    TradingContractType = None

from analytics.contract_logger import log_contract_attempt
from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep, get_cache, get_breaker, get_bucket

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

_CHAIN_CACHE_TTL = 30.0    # seconds — option chains don't change often
_SNAPSHOT_CACHE_TTL = 10.0  # seconds — batch snapshot quotes

# Track chain/snapshot errors for hourly health reporting
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


def _record_success(symbol: str, spread_pct: float | None, contract_type_char: str | None) -> None:
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


def get_snapshot_probe() -> dict | None:
    return _LAST_SNAPSHOT_PROBE


def record_iv_sample(iv: float | None) -> None:
    try:
        if iv is None:
            return
        _IV_SERIES.append(float(iv))
    except Exception:
        return


def get_iv_series(window: int | None = None) -> list[float]:
    try:
        if window is None:
            return list(_IV_SERIES)
        if window <= 0:
            return []
        return list(_IV_SERIES)[-int(window):]
    except Exception:
        return []


def _record_snapshot_probe(response, symbols: list[str] | None = None) -> None:
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
    # Guard against string input (list("SPY...") -> ["S","P","Y",...])
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


def select_sim_contract_with_reason(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None
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
    today = now_et.date()

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
            return None, "cutoff_passed"
        return None, "no_candidate_expiry"

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
    # SPY options only have whole-number strikes — round to nearest integer
    # and probe ATM-2 through ATM+2 from the OTM-adjusted base.
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

    def _build_occ(expiry_date, contract_type_char: str, strike: float) -> str:
        date_str = expiry_date.strftime("%y%m%d")
        strike_int = int(round(strike * 1000))
        return f"SPY{date_str}{contract_type_char}{strike_int:08d}"

    def _is_chain_empty(chain_obj) -> bool:
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

    client = OptionHistoricalDataClient(api_key, secret_key)
    last_reason = "no_contract"

    for expiry_date, dte in candidate_dates:
        chain = None
        feed_val = None
        try:
            if not get_breaker().allow_request():
                last_reason = "circuit_breaker_open"
                break
            _chain_cache_key = f"chain:{direction}:{expiry_date.isoformat()}"
            chain = get_cache().get(_chain_cache_key)
            if chain is not None:
                pass  # cache hit — skip API call
            else:
                get_bucket().acquire_wait()
                rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
                feed_val = _get_options_feed()
                chain = client.get_option_chain(
                    OptionChainRequest(
                        underlying_symbol="SPY",
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
        except Exception as e:
            last_reason = "chain_error"
            get_breaker().record_failure()
            try:
                import logging
                logging.warning("sim_contract_chain_error: %s", e)
            except Exception:
                pass
            _record_error("chain", str(e))
            chain = None
        if chain is not None and _is_chain_empty(chain):
            last_reason = "empty_chain"
            continue

        strike_candidates = list(dict.fromkeys(strike_retry[:3]))
        chain_symbols_found = False
        symbol_candidates: list[tuple[str, float]] = []
        if chain is not None:
            try:
                def _iter_chain_symbols(chain_obj):
                    if isinstance(chain_obj, dict):
                        for sym in chain_obj.keys():
                            yield sym
                        return
                    data = getattr(chain_obj, "data", None)
                    if isinstance(data, dict):
                        for sym in data.keys():
                            yield sym
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
                        for sym in chains.keys():
                            yield sym
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
                                for sym in df["symbol"].dropna().tolist():
                                    yield sym
                            else:
                                for sym in df.index.tolist():
                                    yield sym
                        except Exception:
                            return

                for symbol in _iter_chain_symbols(chain):
                    chain_symbols_found = True
                    try:
                        if isinstance(symbol, str) and len(symbol) >= 15:
                            strike_part = symbol[-8:]
                            strike_val = int(strike_part) / 1000.0
                            symbol_candidates.append((symbol, strike_val))
                    except Exception:
                        continue
                # sort symbols by proximity to underlying
                symbol_candidates = sorted(symbol_candidates, key=lambda s: abs(s[1] - underlying_price))
                # extend strike candidates with a few closest strikes
                for _sym, s in symbol_candidates[:5]:
                    strike_candidates.append(int(round(s)))
            except Exception:
                pass

        strike_candidates = strike_candidates[:8]
        symbol_candidates = symbol_candidates[:8]
        if chain is not None and not chain_symbols_found:
            last_reason = "no_chain_symbols"

        reason_counts = {
            "no_snapshot": 0,
            "no_quote": 0,
            "invalid_quote": 0,
            "spread_too_wide": 0,
            "snapshot_error": 0,
        }
        symbol_loop = symbol_candidates if symbol_candidates else [(None, s) for s in strike_candidates]

        # Batch snapshot call when chain symbols are available
        if symbol_candidates:
            try:
                batch_symbols = [s for s, _ in symbol_candidates if s]
                if batch_symbols:
                    if not get_breaker().allow_request():
                        last_reason = "circuit_breaker_open"
                        continue
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
                    # If we got an empty response and no explicit feed, retry indicative
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
                            last_reason = "no_snapshot"
                            reason_counts["no_snapshot"] += 1
                            debug_log("sim_snapshot_missing", symbol=symbol, expiry=expiry_date.isoformat(), strike=strike)
                            try:
                                _record_error("snapshot", f"no_snapshot symbol={symbol}")
                            except Exception:
                                pass
                            continue
                        greeks = getattr(snap, "greeks", None)
                        iv    = _safe_float(getattr(greeks, "implied_volatility", None)) if greeks else None
                        delta = _safe_float(getattr(greeks, "delta", None)) if greeks else None
                        gamma = _safe_float(getattr(greeks, "gamma", None)) if greeks else None
                        theta = _safe_float(getattr(greeks, "theta", None)) if greeks else None
                        vega  = _safe_float(getattr(greeks, "vega", None)) if greeks else None

                        record_iv_sample(iv)
                        quote = getattr(snap, "latest_quote", None)
                        if quote is None:
                            last_reason = "no_quote"
                            reason_counts["no_quote"] += 1
                            continue

                        bid = float(quote.bid_price) if quote.bid_price is not None else 0.0
                        ask = float(quote.ask_price) if quote.ask_price is not None else 0.0
                        if bid <= 0 or ask <= 0:
                            last_reason = "invalid_quote"
                            reason_counts["invalid_quote"] += 1
                            continue

                        spread_pct = (ask - bid) / ask
                        mid = round((bid + ask) / 2, 4)
                        if spread_pct > float(profile["max_spread_pct"]):
                            last_reason = "spread_too_wide"
                            reason_counts["spread_too_wide"] += 1
                            continue

                        otm_pct_applied = abs(strike - underlying_price) / underlying_price
                        log_contract_attempt(
                            source=f"sim:{profile.get('sim_id', 'unknown')}",
                            direction=direction, underlying_price=underlying_price,
                            expiry=expiry_date, dte=dte, strike=strike,
                            result="selected", reason="selected",
                            bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                            iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                        )
                        _record_success(symbol, spread_pct, contract_type_char)
                        return {
                            "option_symbol": symbol,
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
                            "selection_method": "chain_symbols",
                            "iv": iv,
                            "delta": delta,
                            "gamma": gamma,
                            "theta": theta,
                            "vega": vega,
                        }, None
            except Exception as e:
                get_breaker().record_failure()
                _record_error("snapshot", f"batch_snapshot_error: {str(e)}")

        for symbol_item in symbol_loop:
            strike = None
            try:
                if isinstance(symbol_item, tuple):
                    symbol = symbol_item[0]
                    strike = symbol_item[1]
                else:
                    symbol = None
                    strike = symbol_item
                if not symbol:
                    symbol = _build_occ(expiry_date, contract_type_char, strike)
                if not get_breaker().allow_request():
                    last_reason = "circuit_breaker_open"
                    continue
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
                    last_reason = "no_snapshot"
                    reason_counts["no_snapshot"] += 1
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
                    continue

                # --- greeks capture ---
                greeks = getattr(snap, "greeks", None)
                iv    = _safe_float(getattr(greeks, "implied_volatility", None)) if greeks else None
                delta = _safe_float(getattr(greeks, "delta", None)) if greeks else None
                gamma = _safe_float(getattr(greeks, "gamma", None)) if greeks else None
                theta = _safe_float(getattr(greeks, "theta", None)) if greeks else None
                vega  = _safe_float(getattr(greeks, "vega", None)) if greeks else None

                record_iv_sample(iv)
                quote = getattr(snap, "latest_quote", None)
                if quote is None:
                    last_reason = "no_quote"
                    reason_counts["no_quote"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="no_quote",
                        iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                bid = float(quote.bid_price) if quote.bid_price is not None else 0.0
                ask = float(quote.ask_price) if quote.ask_price is not None else 0.0
                if bid <= 0 or ask <= 0:
                    last_reason = "invalid_quote"
                    reason_counts["invalid_quote"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="invalid_quote",
                        bid=bid, ask=ask, iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                spread_pct = (ask - bid) / ask
                mid = round((bid + ask) / 2, 4)
                if spread_pct > float(profile["max_spread_pct"]):
                    last_reason = "spread_too_wide"
                    reason_counts["spread_too_wide"] += 1
                    log_contract_attempt(
                        source=f"sim:{profile.get('sim_id', 'unknown')}",
                        direction=direction, underlying_price=underlying_price,
                        expiry=expiry_date, dte=dte, strike=strike,
                        result="rejected", reason="spread_too_wide",
                        bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                        iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                    )
                    continue

                otm_pct_applied = abs(strike - underlying_price) / underlying_price
                log_contract_attempt(
                    source=f"sim:{profile.get('sim_id', 'unknown')}",
                    direction=direction, underlying_price=underlying_price,
                    expiry=expiry_date, dte=dte, strike=strike,
                    result="selected", reason="selected",
                    bid=bid, ask=ask, mid=mid, spread_pct=round(spread_pct, 4),
                    iv=iv, delta=delta, gamma=gamma, theta=theta, vega=vega,
                )
                _record_success(symbol, spread_pct, contract_type_char)
                return {
                    "option_symbol": symbol,
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
                    "selection_method": "otm_pct",
                    # greeks at entry — stored on the contract dict so caller
                    # can persist them on the trade record
                    "iv": iv,
                    "delta": delta,
                    "gamma": gamma,
                    "theta": theta,
                    "vega": vega,
                }, None
            except Exception as e:
                last_reason = "snapshot_error"
                reason_counts["snapshot_error"] += 1
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
                continue

        # If we exhaust strikes without success, make the reason explicit
        total_attempts = sum(reason_counts.values())
        if total_attempts > 0:
            top_reason = max(reason_counts.items(), key=lambda x: x[1])[0]
            if reason_counts[top_reason] == total_attempts:
                last_reason = f"{top_reason}_all"
            else:
                last_reason = f"{top_reason}_most"
        if chain is not None and not chain_symbols_found and last_reason in {
            "no_snapshot_all",
            "no_snapshot_most",
            "no_quote_all",
            "no_quote_most",
            "invalid_quote_all",
            "invalid_quote_most",
            "spread_too_wide_all",
            "spread_too_wide_most",
            "snapshot_error_all",
            "snapshot_error_most",
        }:
            last_reason = f"{last_reason}_no_chain_symbols"

    return None, last_reason


def select_sim_contract(
    direction: str,
    underlying_price: float,
    profile: dict,
    now_et: datetime | None = None
) -> dict | None:
    contract, _reason = select_sim_contract_with_reason(
        direction,
        underlying_price,
        profile,
        now_et=now_et
    )
    return contract
