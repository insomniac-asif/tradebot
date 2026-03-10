import os
import time

from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest
import alpaca.data.enums as alpaca_enums

from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep, get_breaker, get_bucket

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

_LAST_OPTION_QUOTES: dict[str, dict[str, float]] = {}
_QUOTE_TTL_SECONDS = 120.0


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


def _build_snapshot_request(symbol: str, feed_override=None):
    feed_val = feed_override if feed_override is not None else _get_options_feed()
    try:
        if feed_val is not None:
            return OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=feed_val)
    except Exception:
        pass
    try:
        return OptionSnapshotRequest(symbol_or_symbols=[symbol])
    except Exception:
        return OptionSnapshotRequest(symbol_or_symbols=symbol)


def _cache_quote(symbol: str, bid: float, ask: float) -> None:
    try:
        _LAST_OPTION_QUOTES[symbol] = {
            "bid": float(bid),
            "ask": float(ask),
            "ts": time.time(),
        }
    except Exception:
        pass


def _get_cached_quote(symbol: str):
    try:
        item = _LAST_OPTION_QUOTES.get(symbol)
        if not item:
            return None
        if (time.time() - float(item.get("ts", 0))) > _QUOTE_TTL_SECONDS:
            return None
        bid = float(item.get("bid", 0))
        ask = float(item.get("ask", 0))
        if bid > 0 and ask > 0:
            return bid, ask
    except Exception:
        return None
    return None


def _get_option_quote(api_key: str, secret_key: str, symbol: str):
    if not symbol or not isinstance(symbol, str) or len(symbol) < 15:
        debug_log("option_snapshot_invalid_symbol", symbol=symbol)
        return None
    # Short-circuit if circuit breaker is open
    if not get_breaker().allow_request():
        cached = _get_cached_quote(symbol)
        return cached
    client = OptionHistoricalDataClient(api_key, secret_key)
    last_err = None
    feed_val = _get_options_feed()
    for attempt in range(2):
        try:
            get_bucket().acquire_wait()
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            debug_log("option_snapshot_request", symbol=symbol)
            req = _build_snapshot_request(symbol, feed_override=feed_val)
            start_ts = time.time()
            snapshots = client.get_option_snapshot(req)
            get_breaker().record_success()
            elapsed_ms = int((time.time() - start_ts) * 1000)
            snap = _extract_snapshot(snapshots, symbol)
            if snap is None:
                debug_log(
                    "option_snapshot_missing",
                    symbol=symbol,
                    attempt=attempt + 1,
                    response_type=type(snapshots).__name__,
                    response_time_ms=elapsed_ms,
                )
                last_err = "no_snapshot"
                # If no explicit feed set, retry indicative once before backoff
                if feed_val is None:
                    try:
                        feed_enum = getattr(alpaca_enums, "OptionsFeed", None)
                        if feed_enum is not None and hasattr(feed_enum, "INDICATIVE"):
                            debug_log("option_snapshot_retry_feed", symbol=symbol, feed="indicative")
                            req = _build_snapshot_request(symbol, feed_override=feed_enum.INDICATIVE)
                            snapshots = client.get_option_snapshot(req)
                            snap = _extract_snapshot(snapshots, symbol)
                            if snap is not None:
                                quote = getattr(snap, "latest_quote", None)
                                if quote and quote.ask_price is not None and quote.bid_price is not None:
                                    bid = quote.bid_price
                                    ask = quote.ask_price
                                    if bid is not None and ask is not None and bid > 0 and ask > 0:
                                        _cache_quote(symbol, float(bid), float(ask))
                                        return float(bid), float(ask)
                    except Exception:
                        pass
                time.sleep(0.2 * (attempt + 1))
                continue
            quote = getattr(snap, "latest_quote", None)
            if quote and quote.ask_price is not None and quote.bid_price is not None:
                bid = quote.bid_price
                ask = quote.ask_price
                if bid is not None and ask is not None and bid > 0 and ask > 0:
                    _cache_quote(symbol, float(bid), float(ask))
                    try:
                        from core.singletons import RISK_SUPERVISOR
                        _now = time.time()
                        RISK_SUPERVISOR.update_quote_freshness(_now)
                        RISK_SUPERVISOR.update_broker_health(_now)
                    except ImportError:
                        pass
                    return float(bid), float(ask)
            last_err = "no_quote"
        except Exception as e:
            last_err = str(e)
            get_breaker().record_failure()
            debug_log("option_snapshot_error", symbol=symbol, error=str(e))
            time.sleep(0.2 * (attempt + 1))
    cached = _get_cached_quote(symbol)
    if cached:
        debug_log("option_snapshot_fallback_cached", symbol=symbol)
        return cached
    if last_err:
        debug_log("option_snapshot_failed", symbol=symbol, reason=last_err)
    return None
