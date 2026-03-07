import os
import time
import asyncio
from typing import Any

from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionSnapshotRequest
import alpaca.data.enums as alpaca_enums
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, PositionIntent, OrderStatus
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

from core.debug import debug_log
from core.rate_limiter import rate_limit_sleep
from analytics.execution_logger import log_execution

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
    client = OptionHistoricalDataClient(api_key, secret_key)
    last_err = None
    feed_val = _get_options_feed()
    for attempt in range(2):
        try:
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            debug_log("option_snapshot_request", symbol=symbol)
            req = _build_snapshot_request(symbol, feed_override=feed_val)
            start_ts = time.time()
            snapshots = client.get_option_snapshot(req)
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
                    return float(bid), float(ask)
            last_err = "no_quote"
        except Exception as e:
            last_err = str(e)
            debug_log("option_snapshot_error", symbol=symbol, error=str(e))
            time.sleep(0.2 * (attempt + 1))
    cached = _get_cached_quote(symbol)
    if cached:
        debug_log("option_snapshot_fallback_cached", symbol=symbol)
        return cached
    if last_err:
        debug_log("option_snapshot_failed", symbol=symbol, reason=last_err)
    return None


async def _sleep(seconds: float) -> None:
    try:
        asyncio.get_running_loop()
        await asyncio.sleep(seconds)
    except RuntimeError:
        time.sleep(seconds)


def _increment_no_record_exit(acc, reason: str) -> None:
    if not isinstance(acc, dict):
        return
    stats = acc.get("execution_stats", {})
    if not isinstance(stats, dict):
        stats = {}
    no_record = stats.get("no_record_exits", {})
    if not isinstance(no_record, dict):
        no_record = {}
    no_record[reason] = no_record.get(reason, 0) + 1
    stats["no_record_exits"] = no_record
    acc["execution_stats"] = stats


async def execute_option_entry(option_symbol: str, quantity: int, bid: float, ask: float, ctx=None, acc=None):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        _increment_no_record_exit(acc, "missing_api_keys")
        debug_log("option_snapshot_missing_keys", symbol=option_symbol)
        return None, "missing_api_keys"

    debug_log(
        "option_entry_quote_request",
        symbol=option_symbol,
        qty=quantity,
        bid=bid,
        ask=ask,
    )
    refreshed = await asyncio.to_thread(_get_option_quote, api_key, secret_key, option_symbol)
    if refreshed is None:
        _increment_no_record_exit(acc, "quote_fetch_failed")
        return None, "quote_fetch_failed"
    bid, ask = refreshed

    mid = (bid + ask) / 2
    expected_mid = mid
    spread = ask - bid
    if mid <= 0 or spread < 0:
        _increment_no_record_exit(acc, "invalid_quote_or_spread")
        return None, "invalid_quote_or_spread"
    spread_pct = spread / ask
    if spread_pct > 0.15:
        return None, "spread_too_wide"

    client = TradingClient(api_key, secret_key, paper=True)
    first_limit = round(mid + (spread * 0.25), 2)
    order: Any = client.submit_order(
        LimitOrderRequest(
            symbol=option_symbol,
            qty=quantity,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=float(first_limit),
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
    )

    # --- first attempt: limit at mid + 25% of spread ---
    start = time.time()
    while (time.time() - start) < 5:
        current: Any = client.get_order_by_id(order.id)
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                fill_price = float(current.filled_avg_price)
                fill_ratio = 1.0
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=quantity,
                    ratio=round(fill_ratio, 3),
                    accepted=True
                )
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_mid_plus",
                    qty_requested=quantity, qty_filled=quantity,
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=fill_price, bid_at_order=bid, ask_at_order=ask,
                )
                if fill_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=quantity,
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(fill_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                return {
                    "fill_price": fill_price,
                    "filled_qty": quantity,
                    "partial": False,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
            except (TypeError, ValueError):
                return None, "limit_not_filled"
        filled_qty = getattr(current, "filled_qty", None)
        if filled_qty is not None and current.status != OrderStatus.FILLED:
            try:
                qty_val = int(float(filled_qty))
            except (TypeError, ValueError):
                qty_val = 0
            if qty_val > 0 and current.filled_avg_price is not None:
                try:
                    client.cancel_order_by_id(order.id)
                except Exception:
                    pass
                try:
                    filled_price = float(current.filled_avg_price)
                except (TypeError, ValueError):
                    return None, "limit_not_filled"
                fill_ratio = min(qty_val, quantity) / float(quantity)
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=min(qty_val, quantity),
                    ratio=round(fill_ratio, 3),
                    accepted=fill_ratio >= 0.5
                )
                if fill_ratio < 0.5:
                    try:
                        asyncio.create_task(asyncio.to_thread(close_option_position, option_symbol, min(qty_val, quantity)))
                    except Exception:
                        pass
                    if ctx is not None:
                        ctx.set_block("partial_fill_below_threshold")
                    _increment_no_record_exit(acc, "partial_fill_below_threshold")
                    return None, "partial_fill_below_threshold"
                if filled_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=min(qty_val, quantity),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(filled_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_partial",
                    qty_requested=quantity, qty_filled=min(qty_val, quantity),
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=filled_price, bid_at_order=bid, ask_at_order=ask,
                )
                return {
                    "fill_price": filled_price,
                    "filled_qty": min(qty_val, quantity),
                    "partial": True,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
        if current.status in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}:
            break
        await _sleep(1)

    try:
        client.cancel_order_by_id(order.id)
    except Exception:
        pass

    order: Any = client.submit_order(
        LimitOrderRequest(
            symbol=option_symbol,
            qty=quantity,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=float(ask),
            position_intent=PositionIntent.BUY_TO_OPEN,
        )
    )

    # --- second attempt: limit at ask ---
    start = time.time()
    while (time.time() - start) < 5:
        current: Any = client.get_order_by_id(order.id)
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                fill_price = float(current.filled_avg_price)
                fill_ratio = 1.0
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=quantity,
                    ratio=round(fill_ratio, 3),
                    accepted=True
                )
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_ask",
                    qty_requested=quantity, qty_filled=quantity,
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=fill_price, bid_at_order=bid, ask_at_order=ask,
                )
                if fill_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=quantity,
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(fill_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                return {
                    "fill_price": fill_price,
                    "filled_qty": quantity,
                    "partial": False,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
            except (TypeError, ValueError):
                return None, "limit_not_filled"
        filled_qty = getattr(current, "filled_qty", None)
        if filled_qty is not None and current.status != OrderStatus.FILLED:
            try:
                qty_val = int(float(filled_qty))
            except (TypeError, ValueError):
                qty_val = 0
            if qty_val > 0 and current.filled_avg_price is not None:
                try:
                    client.cancel_order_by_id(order.id)
                except Exception:
                    pass
                try:
                    filled_price = float(current.filled_avg_price)
                except (TypeError, ValueError):
                    return None, "limit_not_filled"
                fill_ratio = min(qty_val, quantity) / float(quantity)
                debug_log(
                    "partial_fill_ratio",
                    requested=quantity,
                    filled=min(qty_val, quantity),
                    ratio=round(fill_ratio, 3),
                    accepted=fill_ratio >= 0.5
                )
                if fill_ratio < 0.5:
                    try:
                        asyncio.create_task(asyncio.to_thread(close_option_position, option_symbol, min(qty_val, quantity)))
                    except Exception:
                        pass
                    if ctx is not None:
                        ctx.set_block("partial_fill_below_threshold")
                    _increment_no_record_exit(acc, "partial_fill_below_threshold")
                    return None, "partial_fill_below_threshold"
                if filled_price > expected_mid * 1.10:
                    try:
                        client.submit_order(
                            LimitOrderRequest(
                                symbol=option_symbol,
                                qty=min(qty_val, quantity),
                                side=OrderSide.SELL,
                                time_in_force=TimeInForce.DAY,
                                limit_price=float(filled_price),
                                position_intent=PositionIntent.SELL_TO_CLOSE,
                            )
                        )
                    except Exception:
                        pass
                    _increment_no_record_exit(acc, "slippage_guard_triggered")
                    return None, "slippage_guard_triggered"
                log_execution(
                    option_symbol=option_symbol, side="entry",
                    order_type="limit_partial",
                    qty_requested=quantity, qty_filled=min(qty_val, quantity),
                    fill_ratio=fill_ratio, expected_mid=expected_mid,
                    fill_price=filled_price, bid_at_order=bid, ask_at_order=ask,
                )
                return {
                    "fill_price": filled_price,
                    "filled_qty": min(qty_val, quantity),
                    "partial": True,
                    "expected_mid": float(expected_mid),
                    "requested_qty": quantity,
                    "fill_ratio": fill_ratio,
                }, None
        if current.status in {OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}:
            break
        await _sleep(1)

    try:
        client.cancel_order_by_id(order.id)
    except Exception:
        pass

    _increment_no_record_exit(acc, "limit_not_filled")
    return None, "limit_not_filled"


def close_option_position(option_symbol: str, quantity: int) -> dict:
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return {"ok": False, "filled_avg_price": None, "order_id": None}

    try:
        client = TradingClient(api_key, secret_key, paper=True)
        order = client.submit_order(
            MarketOrderRequest(
                symbol=option_symbol,
                qty=quantity,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                position_intent=PositionIntent.SELL_TO_CLOSE,
            )
        )
    except Exception:
        return {"ok": False, "filled_avg_price": None, "order_id": None}

    order_id = getattr(order, "id", None)
    filled_avg_price = None
    start = time.time()
    while order_id and (time.time() - start) < 10:
        try:
            current: Any = client.get_order_by_id(order_id)
        except Exception:
            break
        if current.status == OrderStatus.FILLED and current.filled_avg_price is not None:
            try:
                filled_avg_price = float(current.filled_avg_price)
            except (TypeError, ValueError):
                filled_avg_price = None
            break
        time.sleep(1)

    if filled_avg_price is not None:
        log_execution(
            option_symbol=option_symbol, side="exit",
            order_type="market",
            qty_requested=quantity, qty_filled=quantity,
            fill_ratio=1.0, expected_mid=None,
            fill_price=filled_avg_price, bid_at_order=None, ask_at_order=None,
        )

    return {"ok": True, "filled_avg_price": filled_avg_price, "order_id": order_id}


def get_option_price(option_symbol: str):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        debug_log("option_snapshot_missing_keys", symbol=option_symbol)
        return None
    try:
        client = OptionHistoricalDataClient(api_key, secret_key)
        last_err = None
        for attempt in range(2):
            rate_limit_sleep("alpaca_option_snapshot", ALPACA_MIN_CALL_INTERVAL_SEC)
            req = _build_snapshot_request(option_symbol)
            start_ts = time.time()
            snapshots = client.get_option_snapshot(req)
            elapsed_ms = int((time.time() - start_ts) * 1000)
            snap = _extract_snapshot(snapshots, option_symbol)
            if snap is None:
                debug_log(
                    "option_snapshot_missing",
                    symbol=option_symbol,
                    attempt=attempt + 1,
                    response_type=type(snapshots).__name__,
                    response_time_ms=elapsed_ms,
                )
                last_err = "no_snapshot"
                time.sleep(0.2 * (attempt + 1))
                continue
            quote = getattr(snap, "latest_quote", None)
            if quote and quote.ask_price is not None and quote.bid_price is not None:
                bid = float(quote.bid_price)
                ask = float(quote.ask_price)
                if bid > 0 and ask > 0:
                    _cache_quote(option_symbol, bid, ask)
                    return (bid + ask) / 2
            trade = getattr(snap, "latest_trade", None)
            if trade and trade.price is not None:
                try:
                    return float(trade.price)
                except (TypeError, ValueError):
                    pass
            last_err = "no_quote"
        cached = _get_cached_quote(option_symbol)
        if cached:
            debug_log("option_snapshot_fallback_cached", symbol=option_symbol)
            bid, ask = cached
            return (bid + ask) / 2
        if last_err:
            debug_log("option_snapshot_failed", symbol=option_symbol, reason=last_err)
        return None
    except Exception:
        debug_log("option_snapshot_error", symbol=option_symbol, error="exception")
        return None
    return None
