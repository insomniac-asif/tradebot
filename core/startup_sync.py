import os
import uuid
from datetime import datetime
import pytz

from core.account_repository import load_account, save_account
from core.debug import debug_log


def _parse_occ_option_symbol(symbol):
    if not isinstance(symbol, str):
        return None
    idx = 0
    while idx < len(symbol) and symbol[idx].isalpha():
        idx += 1
    if idx < 1:
        return None
    underlying = symbol[:idx]
    expected_min_length = idx + 6 + 1 + 8
    if len(symbol) != expected_min_length:
        return None
    expiry_raw = symbol[idx : idx + 6]
    contract_type = symbol[idx + 6 : idx + 7]
    strike_raw = symbol[idx + 7 : idx + 15]
    if not expiry_raw.isdigit():
        return None
    if contract_type not in {"C", "P"}:
        return None
    if not strike_raw.isdigit() or len(strike_raw) != 8:
        return None
    expiry = f"20{expiry_raw[0:2]}-{expiry_raw[2:4]}-{expiry_raw[4:6]}"
    strike = int(strike_raw) / 1000.0
    trade_type = "BULLISH" if contract_type == "C" else "BEARISH"
    return {
        "underlying": underlying,
        "expiry": expiry,
        "strike": strike,
        "contract_type": "CALL" if contract_type == "C" else "PUT",
        "type": trade_type,
    }


async def perform_startup_broker_sync(bot):
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        debug_log("startup_sync_skipped", reason="missing_api_keys")
        return

    from alpaca.trading.client import TradingClient

    acc = load_account()
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []

    client = TradingClient(api_key, secret_key, paper=True)
    try:
        positions = client.get_all_positions()
    except Exception as e:
        debug_log("startup_sync_failed", error=str(e))
        return

    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        orders_request = GetOrdersRequest(status=QueryOrderStatus.ALL, limit=50)
        orders = client.get_orders(orders_request)
    except Exception:
        orders = []

    broker_positions = {}
    for pos in positions:
        symbol = getattr(pos, "symbol", None)
        if symbol:
            broker_positions[str(symbol)] = pos

    broker_symbols = set(broker_positions.keys())
    internal_symbols = set(
        t.get("option_symbol")
        for t in open_trades
        if isinstance(t, dict) and t.get("option_symbol")
    )
    open_trade = acc.get("open_trade")
    if isinstance(open_trade, dict):
        open_symbol = open_trade.get("option_symbol")
        if open_symbol:
            internal_symbols.add(open_symbol)

    # ----------------------------
    # Reconstruct missing broker positions
    # ----------------------------
    reconstructed = []
    for symbol, pos in broker_positions.items():
        if symbol in internal_symbols:
            continue
        qty_raw = getattr(pos, "qty", None)
        avg_price_raw = getattr(pos, "avg_entry_price", None)
        if qty_raw is None or avg_price_raw is None:
            continue
        try:
            qty = float(qty_raw)
            avg_price = float(avg_price_raw)
        except (TypeError, ValueError):
            continue
        occ = _parse_occ_option_symbol(symbol) or {}
        reconstructed_trade = {
            "trade_id": uuid.uuid4().hex,
            "option_symbol": symbol,
            "quantity": int(abs(qty)),
            "entry_price": avg_price,
            "fill_price": avg_price,
            "type": occ.get("type"),
            "underlying": occ.get("underlying"),
            "expiry": occ.get("expiry"),
            "strike": occ.get("strike"),
            "contract_type": occ.get("contract_type"),
            "reconstructed": True,
            "protection_policy": {
                "mode": "emergency_stop_only",
                "max_loss_pct": 0.50,
                "min_hold_seconds": 30,
                "created_at": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            },
            "last_manage_ts": None,
            "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "risk": None,
        }
        open_trades.append(reconstructed_trade)
        reconstructed.append(reconstructed_trade)
        debug_log("startup_reconstructed_trade", symbol=symbol, qty=qty)

    if reconstructed:
        channel_id = getattr(bot, "paper_channel_id", None)
        channel = bot.get_channel(channel_id) if channel_id else None
        if channel is not None:
            await channel.send("⚠️ Reconstructed open broker position at startup.")

    # ----------------------------
    # Finalize missing internal trades
    # ----------------------------
    def _find_sell_order(symbol):
        for order in orders or []:
            order_symbol = getattr(order, "symbol", None)
            if order_symbol != symbol:
                continue
            status = getattr(order, "status", None)
            if status and str(status).lower() not in {"filled", "partially_filled"}:
                continue
            side = getattr(order, "side", None)
            intent = getattr(order, "position_intent", None)
            if intent and "SELL_TO_CLOSE" in str(intent):
                pass
            elif side and str(side).lower() == "sell":
                pass
            else:
                continue
            price = getattr(order, "filled_avg_price", None)
            if price is None:
                price = getattr(order, "limit_price", None)
            if price is None:
                continue
            qty = getattr(order, "filled_qty", None)
            if qty is None:
                qty = getattr(order, "qty", None)
            return price, qty
        return None, None

    remaining_open_trades = []
    closed_trades = []
    for t in open_trades:
        if not isinstance(t, dict):
            continue
        symbol = t.get("option_symbol")
        if not symbol:
            remaining_open_trades.append(t)
            continue
        if symbol in broker_symbols:
            remaining_open_trades.append(t)
            continue
        closed_trades.append(t)

    if isinstance(open_trade, dict):
        symbol = open_trade.get("option_symbol")
        if symbol and symbol not in broker_symbols:
            closed_trades.append(open_trade)
            acc["open_trade"] = None

    trade_log = acc.get("trade_log", [])
    if not isinstance(trade_log, list):
        trade_log = []

    for trade in closed_trades:
        symbol = trade.get("option_symbol")
        sell_price_raw, sell_qty_raw = _find_sell_order(symbol)
        try:
            sell_price = float(sell_price_raw) if sell_price_raw is not None else None
        except (TypeError, ValueError):
            sell_price = None
        try:
            sell_qty = int(float(sell_qty_raw)) if sell_qty_raw is not None else None
        except (TypeError, ValueError):
            sell_qty = None

        entry_price = trade.get("entry_price") or trade.get("fill_price")
        try:
            entry_price = float(entry_price) if entry_price is not None else None
        except (TypeError, ValueError):
            entry_price = None

        quantity = trade.get("quantity") or trade.get("size")
        try:
            quantity = int(quantity) if quantity is not None else None
        except (TypeError, ValueError):
            quantity = None

        pnl = None
        if sell_price is not None and entry_price is not None and quantity:
            pnl = (sell_price - entry_price) * quantity

        result = "unknown"
        result_reason = None
        if sell_price is None or entry_price is None or quantity is None:
            result = "closed_unknown"
            result_reason = "closed_unknown"
        elif pnl is not None:
            if pnl > 0:
                result = "win"
            elif pnl < 0:
                result = "loss"
            else:
                result = "breakeven"

        trade_record = {
            "trade_id": trade.get("trade_id"),
            "entry_time": trade.get("entry_time"),
            "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            "type": trade.get("type"),
            "style": trade.get("style", "unknown"),
            "risk": trade.get("risk"),
            "R": None,
            "regime": trade.get("regime"),
            "setup": trade.get("setup", "UNKNOWN"),
            "underlying": trade.get("underlying"),
            "strike": trade.get("strike"),
            "expiry": trade.get("expiry"),
            "option_symbol": symbol,
            "quantity": quantity,
            "confidence": trade.get("confidence", 0),
            "result": result,
            "pnl": pnl,
            "balance_after": acc.get("balance"),
            "reconstructed": trade.get("reconstructed", False),
            "offline_close": True,
            "result_reason": result_reason,
        }
        trade_log.append(trade_record)
        debug_log("startup_closed_missing_trade", symbol=symbol)

    acc["open_trades"] = remaining_open_trades
    acc["trade_log"] = trade_log

    save_account(acc)
