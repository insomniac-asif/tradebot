# interface/watcher_helpers.py
#
# Non-async helper functions extracted from watchers.py to reduce its size.
# All public async functions remain in watchers.py; this module contains only
# pure / synchronous (or lightly async) helpers called from those shells.
#
# Sections:
#   - auto_trader embed builders (_build_skip_embed, _build_trade_open_embed, _build_trade_close_embed)
#   - auto_trader inner body (_run_auto_trader_cycle) — re-exported from watcher_trader_cycle
#   - Decision buffer helpers (explain_block_reason, _record_decision_attempt, get_decision_buffer_snapshot)
#
# preopen_check_loop helpers → interface/watcher_preopen.py
# heart_monitor helpers      → interface/watcher_health.py
# shared small helpers       → interface/watcher_utils.py

import asyncio
import logging
from datetime import datetime

import discord
import pytz

from core.debug import debug_log
from signals.regime import get_regime
from signals.volatility import volatility_state
from interface.fmt import (
    ab, A, lbl,
    pnl_col, conf_col, dir_col, regime_col, vol_col, delta_col, ml_col,
    result_col, exit_reason_col, balance_col, wr_col, tier_col, drawdown_col, pct_col,
)
from interface.watcher_utils import (
    _format_et,
    _parse_strike_from_symbol,
    _format_contract_simple,
    _get_underlying_price,
)

# Re-export _run_auto_trader_cycle from its dedicated module (public API preserved)
from interface.watcher_trader_cycle import _run_auto_trader_cycle


# ===========================================================================
# auto_trader helpers
# ===========================================================================

def _build_skip_embed(
    reason: str,
    friendly_reason: str,
    decision_ctx,
    df,
    now: datetime,
) -> discord.Embed:
    """Build the 'Trade Skipped' embed."""
    skip_embed = discord.Embed(title="⏸️ Trade Skipped", color=0xF39C12)
    skip_embed.add_field(name="🚫 Reason Code", value=ab(A(reason, "red")), inline=False)
    skip_embed.add_field(name="📋 Explanation", value=ab(A(friendly_reason, "yellow")), inline=False)

    regime_now = get_regime(df) if df is not None else "N/A"
    vol_now = volatility_state(df) if df is not None else "N/A"
    blended_val = decision_ctx.blended_score
    threshold_val = decision_ctx.threshold
    _delta = (
        (blended_val - threshold_val)
        if blended_val is not None and threshold_val is not None
        else None
    )
    conf_val = getattr(decision_ctx, "confidence_60m", None)

    skip_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime_now)), inline=True)
    skip_embed.add_field(name="⚡ Vol", value=ab(vol_col(vol_now)), inline=True)
    skip_embed.add_field(
        name="📊 Score Δ",
        value=ab(delta_col(_delta) if _delta is not None else A("N/A", "gray")),
        inline=True,
    )
    if conf_val is not None:
        try:
            conf_pct = float(conf_val) * 100
            conf_text = f"{conf_pct:.1f}%"
        except (TypeError, ValueError):
            conf_text = "N/A"
        skip_embed.add_field(
            name="🎯 Confidence",
            value=ab(A(conf_text, "white", bold=True)),
            inline=True,
        )
    skip_embed.set_footer(text=f"Suppressed for 5m per reason | {_format_et(now)}")
    return skip_embed


def _build_trade_open_embed(trade: dict, decision_ctx, df, now: datetime) -> discord.Embed:
    """Build the 'Trade Opened' embed."""
    ml_prob = trade.get("ml_probability")
    dual_text = "YES" if decision_ctx.dual_alignment else "NO"
    conf_15 = (
        f"{decision_ctx.confidence_15m:.2f}"
        if decision_ctx.confidence_15m is not None
        else "N/A"
    )
    conf_60 = (
        f"{decision_ctx.confidence_60m:.2f}"
        if decision_ctx.confidence_60m is not None
        else "N/A"
    )
    blended_val = decision_ctx.blended_score
    threshold_val = decision_ctx.threshold
    blended_text = f"{blended_val:.2f}" if blended_val is not None else "N/A"
    threshold_text = f"{threshold_val:.2f}" if threshold_val is not None else "N/A"
    delta_text = (
        f"{(blended_val - threshold_val):+0.2f}"
        if blended_val is not None and threshold_val is not None
        else "N/A"
    )
    ml_weight_text = (
        f"{decision_ctx.ml_weight:.2f}" if decision_ctx.ml_weight is not None else "N/A"
    )
    regime_samples_text = (
        str(decision_ctx.regime_samples)
        if decision_ctx.regime_samples is not None
        else "N/A"
    )
    expectancy_samples_text = (
        str(decision_ctx.expectancy_samples)
        if decision_ctx.expectancy_samples is not None
        else "N/A"
    )

    direction_color = 0x2ECC71 if trade["type"] == "bullish" else 0xE74C3C
    direction_emoji = "🟢" if trade["type"] == "bullish" else "🔴"
    open_embed = discord.Embed(
        title=f"🤖 Trade Opened — {direction_emoji} {trade['type'].upper()}",
        color=direction_color,
    )

    qty_val = trade.get("quantity") or trade.get("qty") or "?"
    risk_val = trade.get("risk_dollars")
    risk_text = (
        ab(A(f"${risk_val:.2f}", "yellow", bold=True))
        if isinstance(risk_val, (int, float))
        else ab(A("N/A", "gray"))
    )
    _delta_num = (
        (blended_val - threshold_val)
        if blended_val is not None and threshold_val is not None
        else None
    )

    open_embed.add_field(name="💵 Entry", value=ab(A(f"${trade['entry_price']:.4f}", "white", bold=True)), inline=True)
    open_embed.add_field(name="🛑 Stop",   value=ab(A(f"${trade['stop']:.4f}", "red")), inline=True)
    open_embed.add_field(name="🎯 Target", value=ab(A(f"${trade['target']:.4f}", "green")), inline=True)
    open_embed.add_field(name="📦 Contracts", value=ab(A(str(qty_val), "white", bold=True)), inline=True)
    open_embed.add_field(name="🧾 Style", value=ab(A(trade["style"], "cyan")), inline=True)
    open_embed.add_field(name="💰 Risk", value=risk_text, inline=True)
    open_embed.add_field(name="💡 Confidence", value=ab(conf_col(trade["confidence"])), inline=True)
    open_embed.add_field(name="🤖 ML Score", value=ab(ml_col(ml_prob)), inline=True)
    open_embed.add_field(name="🧭 Regime", value=ab(regime_col(get_regime(df))), inline=True)
    open_embed.add_field(name="⚡ Volatility", value=ab(vol_col(volatility_state(df))), inline=True)
    dual_color = "green" if decision_ctx.dual_alignment else "red"
    open_embed.add_field(name="🔗 Dual Align", value=ab(A(dual_text, dual_color, bold=True)), inline=True)
    open_embed.add_field(
        name="📊 Score / Threshold",
        value=ab(
            f"{lbl('Blended')} {A(blended_text, 'white', bold=True)}  "
            f"{lbl('Thresh')} {A(threshold_text, 'white')}  "
            f"{lbl('Δ')} {delta_col(_delta_num) if _delta_num is not None else A('N/A','gray')}"
        ),
        inline=False,
    )
    open_embed.add_field(
        name="🧠 Signal Detail",
        value=ab(
            f"{lbl('15m')} {A(conf_15,'cyan')}  {lbl('60m')} {A(conf_60,'cyan')}",
            f"{lbl('ML wt')} {A(ml_weight_text,'magenta')}  {lbl('Reg samples')} {A(regime_samples_text,'white')}  {lbl('Exp samples')} {A(expectancy_samples_text,'white')}",
        ),
        inline=False,
    )

    option_sym = trade.get("option_symbol")
    expiry = trade.get("expiry")
    strike = trade.get("strike")
    contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
    contract_lines = [A(contract_label, "magenta", bold=True)]
    if option_sym:
        contract_lines.append(A(option_sym, "white"))
    open_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)

    _und = (trade.get("symbol") or trade.get("underlying", "")).upper()
    _und_price = _get_underlying_price(_und)
    if isinstance(_und_price, (int, float)):
        open_embed.add_field(
            name=f"📈 {_und} Price",
            value=ab(A(f"${_und_price:.2f}", "white", bold=True)),
            inline=True,
        )
    open_embed.set_footer(text=f"{_format_et(now)}")
    return open_embed


def _build_trade_close_embed(
    res: str,
    pnl: float,
    bal: float,
    trade,
    now: datetime,
) -> discord.Embed:
    """Build the 'Trade Closed' embed."""
    close_color = 0x2ECC71 if res == "win" else 0xE74C3C
    close_emoji = "✅" if res == "win" else "❌"
    close_embed = discord.Embed(
        title=f"{close_emoji} Trade Closed — {res.upper()}",
        color=close_color,
    )
    close_embed.add_field(name="💰 PnL", value=ab(pnl_col(pnl)), inline=True)
    close_embed.add_field(name="💵 Balance", value=ab(balance_col(bal)), inline=True)

    if isinstance(trade, dict):
        exit_reason = trade.get("exit_reason") or trade.get("result_reason") or "unknown"
        hold_secs = trade.get("time_in_trade_seconds")
        if hold_secs is not None:
            try:
                h_total = int(hold_secs)
                h_mins = h_total // 60
                h_secs = h_total % 60
                hold_text = f"{h_mins}m {h_secs}s"
            except (TypeError, ValueError):
                hold_text = "N/A"
        else:
            hold_text = "N/A"
        close_embed.add_field(name="🚪 Exit Reason", value=ab(exit_reason_col(exit_reason)), inline=True)
        close_embed.add_field(name="⏱️ Hold Time", value=ab(A(hold_text, "cyan")), inline=True)

        entry_price = trade.get("entry_price")
        exit_price = trade.get("exit_price")
        if entry_price and exit_price:
            close_embed.add_field(
                name="📍 Entry → Exit",
                value=ab(
                    f"{A(f'${entry_price:.4f}', 'white')} → {A(f'${exit_price:.4f}', 'white', bold=True)}"
                ),
                inline=True,
            )

        option_sym = trade.get("option_symbol")
        expiry = trade.get("expiry")
        strike = trade.get("strike")
        contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
        contract_lines = [A(contract_label, "magenta", bold=True)]
        if option_sym:
            contract_lines.append(A(option_sym, "white"))
        close_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)

        _und = (trade.get("symbol") or trade.get("underlying", "")).upper()
        _und_price = _get_underlying_price(_und)
        if isinstance(_und_price, (int, float)):
            close_embed.add_field(
                name=f"📈 {_und} Price",
                value=ab(A(f"${_und_price:.2f}", "white", bold=True)),
                inline=True,
            )

    close_embed.set_footer(text=f"{_format_et(now)}")
    return close_embed


# ===========================================================================
# Decision buffer helpers (moved from watchers.py)
# ===========================================================================

from decision.trader import get_ml_visibility_snapshot

_decision_buffer = {
    "attempts": 0,
    "blocked": 0,
    "opened": 0,
    "top_block_reason": {},
    "last_emit_time": None
}


def explain_block_reason(reason: str) -> str:
    mapping = {
        "regime_compression": "Regime is in compression; trend clarity is too low to trade.",
        "regime_range": "Regime is range-bound; trend conditions are not met.",
        "regime_no_data": "Regime unavailable due to insufficient data.",
        "volatility_dead": "Volatility is too low to support a trade.",
        "volatility_low": "Volatility is below minimum threshold.",
        "prediction_none": "Prediction unavailable for this cycle.",
        "direction_mismatch": "15m and 60m direction did not align.",
        "confidence": "Confidence did not meet minimum threshold.",
        "ml_threshold": "Blended score below adaptive threshold.",
        "expectancy_negative_regime": "Regime expectancy is negative.",
        "regime_low_confidence": "Regime confidence is too low.",
        "execution_plan_none": "Execution plan could not be generated.",
        "no_option_chain": "Alpaca returned no option chain for SPY (feed gap or no expiry available).",
        "no_valid_quote": "Option chain found but no contracts had valid bid/ask quotes.",
        "quantity_zero": "Position sizing rounded to zero contracts (edge compression or premium too high).",
        "signal_none": "Signal generation returned no valid setup.",
        "no_market_data": "Market data unavailable.",
        "no_latest_price": "Latest price unavailable.",
        "protection_EDGE_DECAY": "Protection layer: edge decay active.",
        "protection_EQUITY_PROTECTION": "Protection layer: equity drawdown.",
        "protection_DAILY_LIMIT": "Protection layer: daily loss limit.",
        "protection_PDT_LIMIT": "Protection layer: PDT limit reached.",
        "capital_exposure_limit": "Capital exposure limit reached.",
        "max_open_trades_reached": "Maximum open trades limit reached.",
        "order_not_filled": "Order not filled; liquidity or price unavailable.",
        "spread_too_wide": "Option spread too wide; liquidity insufficient.",
        "limit_not_filled": "Limit order not filled; liquidity insufficient.",
        "slippage_guard_triggered": "Execution slippage exceeded 10%.",
        "partial_fill_too_small": "Partial fill below 50%; position closed.",
        "partial_fill_below_threshold": "Partial fill below 50%; position closed.",
        "reconstructed_emergency_stop": "Reconstructed position hit emergency stop after restart.",
    }
    return mapping.get(reason, f"Trade skipped: {reason}")


def _record_decision_attempt(trade_result, ctx):
    _decision_buffer["attempts"] += 1

    if ctx is not None and ctx.outcome == "opened":
        _decision_buffer["opened"] += 1
        return

    _decision_buffer["blocked"] += 1
    reason = ctx.block_reason if ctx is not None else None
    reason = reason or "unknown"
    counts = _decision_buffer["top_block_reason"]
    counts[reason] = counts.get(reason, 0) + 1


def get_decision_buffer_snapshot():
    block_counts = _decision_buffer["top_block_reason"]
    top_reason = "N/A"
    if block_counts:
        top_reason = max(block_counts, key=block_counts.get)

    ml_snapshot = get_ml_visibility_snapshot()
    ml_weight = ml_snapshot.get("ml_weight")
    avg_delta = ml_snapshot.get("avg_delta")
    return {
        "attempts": _decision_buffer["attempts"],
        "opened": _decision_buffer["opened"],
        "blocked": _decision_buffer["blocked"],
        "top_block_reason": top_reason,
        "ml_weight": ml_weight,
        "avg_delta": avg_delta,
    }
