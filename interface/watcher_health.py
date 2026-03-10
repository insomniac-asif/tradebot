# interface/watcher_health.py
#
# Heart monitor helpers extracted from watcher_helpers.py.
# Contains: _broker_reconcile_check, _build_health_embed, _run_heart_monitor_cycle

import asyncio
import os
import uuid
import logging
import time
from datetime import datetime

import discord
import pytz

from core.debug import debug_log
from core.account_repository import save_account
from interface.fmt import ab, A


# ---------------------------------------------------------------------------
# Shared small helpers (local copy to avoid circular imports)
# ---------------------------------------------------------------------------

def _format_et(ts: datetime | None) -> str:
    if ts is None:
        return "N/A"
    eastern = pytz.timezone("America/New_York")
    if ts.tzinfo is None:
        ts = eastern.localize(ts)
    else:
        ts = ts.astimezone(eastern)
    return ts.strftime("%Y-%m-%d %H:%M:%S ET")


# ===========================================================================
# heart_monitor helpers
# ===========================================================================

async def _broker_reconcile_check(
    bot,
    acc: dict,
    channel,
    now: datetime,
    eastern,
) -> None:
    """
    Compare broker positions vs internal open trades.
    Posts mismatch/orphan-close alerts to channel.
    Mutates acc if orphan trades are finalized.
    """
    from decision.trader import _finalize_reconstructed_trade
    from execution.option_executor import close_option_position, get_option_price

    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not (api_key and secret_key):
        return

    from alpaca.trading.client import TradingClient
    client = TradingClient(api_key, secret_key, paper=True)
    positions = client.get_all_positions()

    broker_by_symbol: dict = {}
    for p in positions:
        symbol = getattr(p, "symbol", None)
        if symbol:
            broker_by_symbol[str(symbol)] = p
    broker_symbols = set(broker_by_symbol.keys())
    internal_symbols = set(
        t.get("option_symbol")
        for t in acc.get("open_trades", [])
        if isinstance(t, dict) and t.get("option_symbol")
    )
    open_trade = acc.get("open_trade")
    if isinstance(open_trade, dict):
        open_symbol = open_trade.get("option_symbol")
        if open_symbol:
            internal_symbols.add(open_symbol)

    if broker_symbols != internal_symbols:
        debug_log(
            "broker_state_mismatch",
            broker=sorted(str(s) for s in broker_symbols),
            internal=sorted(str(s) for s in internal_symbols),
        )
        last_mismatch = getattr(bot, "broker_mismatch_last_time", None)
        if last_mismatch is None or (now - last_mismatch).total_seconds() >= 300:
            from interface.watchers import _send
            await _send(
                channel,
                "⚠️ **Broker State Mismatch**\n\nBroker positions do not match internal open trades.",
            )
            bot.broker_mismatch_last_time = now

    orphan_symbols = broker_symbols.difference(internal_symbols)
    for symbol in orphan_symbols:
        position = broker_by_symbol.get(symbol)
        if position is None:
            continue
        qty_raw = getattr(position, "qty", None)
        if qty_raw is None:
            continue
        try:
            qty_val = float(qty_raw)
        except (TypeError, ValueError):
            continue
        if qty_val <= 0:
            continue

        close_result = await asyncio.to_thread(close_option_position, symbol, int(abs(qty_val)))
        if not close_result.get("ok"):
            debug_log("orphan_position_close_failed", symbol=symbol, qty=qty_val)
            continue

        filled_avg = close_result.get("filled_avg_price")
        exit_price = filled_avg if filled_avg is not None else get_option_price(symbol)
        entry_price = getattr(position, "avg_entry_price", None)
        try:
            entry_price = float(entry_price) if entry_price is not None else None
        except (TypeError, ValueError):
            entry_price = None

        pnl = 0.0
        if entry_price is not None and exit_price is not None:
            pnl = (exit_price - entry_price) * float(abs(qty_val)) * 100

        trade = {
            "trade_id": uuid.uuid4().hex,
            "option_symbol": symbol,
            "quantity": int(abs(qty_val)),
            "entry_time": datetime.now(eastern).isoformat(),
            "entry_price": entry_price,
            "emergency_exit_price": exit_price,
            "reconstructed": True,
        }
        _finalize_reconstructed_trade(acc, trade, pnl, "orphan_broker_close")

        debug_log("orphan_position_closed", symbol=symbol, qty=qty_val)
        last_times = getattr(bot, "orphan_close_last_time", {})
        last_t = last_times.get(symbol)
        if last_t is None or (now - last_t).total_seconds() >= 300:
            from interface.watchers import _send
            await _send(
                channel,
                f"⚠️ **Orphan Position Closed**\n\n"
                f"{symbol} (qty: {qty_val}) was not tracked internally.",
            )
            last_times[symbol] = now
            bot.orphan_close_last_time = last_times


def _build_health_embed(acc: dict, now: datetime, start_time: float) -> discord.Embed:
    """Build the 30-min system health embed."""
    from interface.health_monitor import check_health
    from core.market_clock import market_is_open

    status, report = check_health()
    trades = acc.get("trade_log", [])
    risk_mode = acc.get("risk_mode", "NORMAL")

    market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

    uptime_seconds = int(time.time() - start_time)
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60

    last_trade = "None"
    if trades:
        last_trade = trades[-1].get("exit_time", "Unknown")

    if status == "HEALTHY":
        color = discord.Color.green()
        status_icon = "🟢"
    else:
        color = discord.Color.red()
        status_icon = "🔴"

    embed = discord.Embed(title="🧠 SPY AI System Health", color=color)
    embed.add_field(name="🧠 System Status", value=f"{status_icon} {status}", inline=True)
    embed.add_field(name="🟢 Market", value=market_status, inline=True)
    embed.add_field(name="🧰 Risk Mode", value=risk_mode, inline=True)
    embed.add_field(name="📦 Total Trades", value=str(len(trades)), inline=True)
    embed.add_field(name="⏱ Uptime", value=f"{hours}h {minutes}m", inline=True)
    embed.add_field(
        name="🧪 Health Report",
        value=report if report else "All subsystems responding.",
        inline=False,
    )
    embed.set_footer(text=f"System Time: {_format_et(now)}")
    return embed


# ===========================================================================
# heart_monitor inner body
# ===========================================================================

async def _run_heart_monitor_cycle(
    bot,
    channel,
    acc: dict,
    now_et: datetime,
    eastern,
    start_time: float,
    last_health_emit,
    last_reconcile_time,
    _send,
) -> tuple:
    """
    Execute one heart-monitor cycle.

    Returns (last_health_emit, last_reconcile_time) — both potentially updated.
    All alerting side-effects happen inside this function.
    """
    import logs.recorder as recorder_module
    from core.market_clock import market_is_open

    if hasattr(bot, "recorder_thread"):
        if not bot.recorder_thread.is_alive():
            debug_log("recorder_thread_dead")
            last_warn = getattr(bot, "last_recorder_thread_dead_warning_time", None)
            if last_warn is None or (now_et - last_warn).total_seconds() >= 300:
                await _send(
                    channel,
                    "⚠️ **recorder_thread_dead**\n\n"
                    "Recorder background thread is not alive."
                )
                bot.last_recorder_thread_dead_warning_time = now_et

    # Recorder stall monitoring (market-open only).
    if market_is_open():
        last_saved = getattr(recorder_module, "last_saved_timestamp", None)
        if last_saved:
            try:
                last_dt = datetime.strptime(last_saved, "%Y-%m-%d %H:%M:%S")
                last_dt = eastern.localize(last_dt)
                age_seconds = (now_et - last_dt).total_seconds()
                if age_seconds > 120:
                    debug_log(
                        "recorder_stalled_warning",
                        last_saved_timestamp=last_saved,
                        age_seconds=round(age_seconds, 1)
                    )
                    was_stalled = getattr(bot, "recorder_stalled_state", False)
                    last_warn = getattr(bot, "last_recorder_stall_warning_time", None)
                    allow_warn = (not was_stalled)
                    if last_warn is None:
                        allow_warn = True
                    elif (now_et - last_warn).total_seconds() >= 300:
                        allow_warn = True
                    if allow_warn:
                        await _send(
                            channel,
                            "⚠️ **recorder_stalled_warning**\n\n"
                            f"Last saved candle: {last_saved} ET\n"
                            f"Age: {age_seconds:.0f}s\n"
                            "Recorder may not be appending new candles."
                        )
                        bot.last_recorder_stall_warning_time = now_et
                    bot.recorder_stalled_state = True
                else:
                    bot.recorder_stalled_state = False
            except Exception:
                pass

    if last_reconcile_time is None or (now_et - last_reconcile_time).total_seconds() >= 60:
        try:
            await _broker_reconcile_check(bot, acc, channel, now_et, eastern)
        except Exception:
            import logging
            logging.exception("heart_monitor_reconcile_error")
        last_reconcile_time = now_et

    open_trades = acc.get("open_trades", [])
    if isinstance(open_trades, list):
        last_times = getattr(bot, "recon_notice_last_time", {})
        updated = False
        for t in open_trades:
            if not isinstance(t, dict):
                continue
            notice = t.get("recon_notice")
            if not isinstance(notice, dict):
                continue
            symbol = notice.get("symbol") or "unknown"
            last = last_times.get(symbol)
            if last is not None and (now_et - last).total_seconds() < 300:
                continue
            ntype = notice.get("type")
            qty = notice.get("qty")
            entry = notice.get("entry")
            price = notice.get("price")
            if ntype == "emergency_stop_success":
                await _send(
                    channel,
                    f"🛑 Reconstructed position emergency-stopped: {symbol} "
                    f"qty={qty} entry={entry} price={price}"
                )
            elif ntype == "emergency_stop_failure":
                await _send(
                    channel,
                    f"⚠️ Reconstructed emergency stop failed: {symbol} qty={qty}"
                )
            else:
                continue
            last_times[symbol] = now_et
            t.pop("recon_notice", None)
            updated = True
        bot.recon_notice_last_time = last_times
        if updated:
            acc["open_trades"] = open_trades
            save_account(acc)

    if last_health_emit is None or (now_et - last_health_emit).total_seconds() >= 1800:
        embed = _build_health_embed(acc, now_et, start_time)
        await _send(channel, embed=embed)
        last_health_emit = now_et

    return last_health_emit, last_reconcile_time
