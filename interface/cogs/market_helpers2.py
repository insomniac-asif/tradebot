"""
interface/cogs/market_helpers2.py
Extracted handler functions for large market_commands.py command bodies.
No Discord decorators here — only async handler functions.
"""
import asyncio
import os
import re
import logging
import yaml

import discord

from interface.fmt import ab, lbl, A
from interface.shared_state import (
    _send_embed,
    _append_footer,
    _add_field_icons,
    _format_ts,
    _get_data_freshness_text,
)
from core.data_service import get_market_dataframe
from core.md_state import set_md_enabled, get_md_state, md_needs_warning, set_md_auto
from simulation.sim_contract import select_sim_contract_with_reason
from signals.predictor import make_prediction


def _format_contract_table(rows: list) -> str:
    header = f"{'OTM':<6} {'Status':<8} Detail"
    lines = [header]
    for row in rows:
        label = row.get("label", "")
        ok = row.get("ok", False)
        if ok:
            status = "🟢 OK"
            detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
        else:
            status = "🔴 FAIL"
            detail = row.get("reason", "unavailable")
        lines.append(f"{label:<6} {status:<8} {detail}")
    return "```\n" + "\n".join(lines) + "\n```"


def _build_predict_embed(pred: dict, timeframe_minutes: int, df) -> discord.Embed:
    """Build the embed for the !predict command result."""
    from interface.fmt import conf_col, dir_col, ab, lbl, A
    from interface.shared_state import _append_footer
    from core.session_scope import get_rth_session_view

    pred_color = (
        0x2ECC71 if pred["direction"] == "bullish"
        else 0xE74C3C if pred["direction"] == "bearish"
        else 0x95A5A6
    )
    pred_embed = discord.Embed(title=f"📊 SPY {timeframe_minutes}m Forecast", color=pred_color)
    pred_embed.add_field(name="📍 Direction", value=ab(dir_col(pred["direction"])), inline=True)
    pred_embed.add_field(name="💡 Confidence", value=ab(conf_col(pred["confidence"])), inline=True)
    pred_embed.add_field(name="🎯 Predicted High", value=ab(A(str(pred["high"]), "green", bold=True)), inline=True)
    pred_embed.add_field(name="🎯 Predicted Low", value=ab(A(str(pred["low"]), "red", bold=True)), inline=True)

    if timeframe_minutes == 30 and df is not None:
        last = df.iloc[-1]
        session_recent = get_rth_session_view(df)
        if session_recent is None or session_recent.empty:
            session_recent = df
        high_price = session_recent["high"].max()
        low_price = session_recent["low"].min()
        high_time = session_recent["high"].idxmax()
        low_time = session_recent["low"].idxmin()
        high_time_str = high_time.strftime("%H:%M") if hasattr(high_time, "strftime") else str(high_time)
        low_time_str = low_time.strftime("%H:%M") if hasattr(low_time, "strftime") else str(low_time)
        if isinstance(high_time_str, str) and "ET" not in high_time_str:
            high_time_str = f"{high_time_str} ET"
        if isinstance(low_time_str, str) and "ET" not in low_time_str:
            low_time_str = f"{low_time_str} ET"
        _pc = f"${last['close']:.2f}"
        _pv = f"${last['vwap']:.2f}"
        _pe9 = f"${last['ema9']:.2f}"
        _pe20 = f"${last['ema20']:.2f}"
        pred_embed.add_field(
            name="📍 Market Snapshot",
            value=ab(
                f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
            ),
            inline=False,
        )
        pred_embed.add_field(
            name="📈 Session Range",
            value=ab(
                f"{lbl('High')} {A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}",
                f"{lbl('Low')}  {A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}",
            ),
            inline=False,
        )

    _append_footer(pred_embed)
    return pred_embed


def _build_plan_embed(side: str, strike: float, premium: float, contracts: int, expiry: str, df) -> discord.Embed:
    """Compute plan grades and build embed. Returns embed or raises on error."""
    from signals.regime import get_regime
    from signals.volatility import volatility_state
    from signals.conviction import calculate_conviction
    from interface.shared_state import _add_field_icons, _append_footer

    regime = get_regime(df)
    vol = volatility_state(df)
    score, impulse, follow, direction = calculate_conviction(df)
    price = df.iloc[-1]["close"]
    bias_alignment = "Aligned" if (
        (side == "call" and direction == "bullish") or
        (side == "put" and direction == "bearish")
    ) else "Against Bias"
    atr = df.iloc[-1]["atr"]
    distance_from_strike = abs(price - strike)
    total_cost = premium * contracts * 100
    grade_score = (
        (1 if bias_alignment == "Aligned" else 0)
        + (1 if regime == "TREND" else 0)
        + (1 if vol in ("NORMAL", "HIGH") else 0)
        + (1 if score >= 4 else 0)
    )
    grade = "A" if grade_score >= 4 else "B" if grade_score == 3 else "C" if grade_score == 2 else "D"

    embed = discord.Embed(
        title="📋 Trade Plan Analysis",
        color=discord.Color.green() if grade in ["A", "B"] else discord.Color.orange(),
    )
    embed.add_field(name="Side", value=side.upper(), inline=True)
    embed.add_field(name=_add_field_icons("Strike"), value=strike, inline=True)
    embed.add_field(name=_add_field_icons("Premium"), value=premium, inline=True)
    embed.add_field(name=_add_field_icons("Contracts"), value=contracts, inline=True)
    embed.add_field(name=_add_field_icons("Total Cost"), value=f"${total_cost:.2f}", inline=True)
    embed.add_field(name=_add_field_icons("Expiry"), value=expiry, inline=True)
    embed.add_field(
        name=_add_field_icons("Market State"),
        value=(
            f"Regime: {regime}\n"
            f"Volatility: {vol}\n"
            f"Conviction Score: {score}\n"
            f"Impulse: {round(impulse,2)}\n"
            f"Follow Through: {round(follow*100,1)}%\n"
        ),
        inline=False,
    )
    embed.add_field(
        name=_add_field_icons("Structure Context"),
        value=(
            f"Current Price: {price:.2f}\n"
            f"Distance from Strike: {distance_from_strike:.2f}\n"
            f"ATR: {atr:.2f}\n"
            f"Bias Alignment: {bias_alignment}"
        ),
        inline=False,
    )
    embed.add_field(name=_add_field_icons("Final Grade"), value=f"🏆 {grade}", inline=False)
    _append_footer(embed)
    return embed


def _check_contracts_for_direction(direction: str, base_profile: dict, last_close_val, select_fn) -> tuple[str, bool]:
    """Run contract sanity check for one direction (BULLISH or BEARISH)."""
    rows = []
    any_ok = False
    if not base_profile:
        return "Profile unavailable", False
    if last_close_val is None:
        return "Price unavailable", False
    try:
        base_otm = float(base_profile.get("otm_pct", 0.0))
    except (TypeError, ValueError):
        base_otm = 0.0
    otm_variants = [
        ("OTM x1.0", base_otm),
        ("OTM x1.5", base_otm * 1.5),
    ]
    for label, otm_val in otm_variants:
        try:
            prof = dict(base_profile)
            prof["otm_pct"] = max(0.0, float(otm_val))
            contract, reason = select_fn(direction, last_close_val, prof)
            if contract:
                any_ok = True
                symbol = contract.get("option_symbol", "symbol")
                spread = contract.get("spread_pct")
                spread_text = f"{spread:.3f}" if isinstance(spread, (int, float)) else "N/A"
                rows.append({"label": label, "ok": True, "symbol": symbol, "spread": spread_text})
            else:
                rows.append({"label": label, "ok": False, "reason": reason or "unavailable"})
        except Exception:
            rows.append({"label": label, "ok": False, "reason": "error"})
    return _format_contract_table(rows), any_ok


async def handle_md(ctx, action, level):
    try:
        cmd = (action or "status").strip().lower()
        if cmd not in {"enable", "disable", "status", "auto"}:
            await _send_embed(ctx, "Usage: `!md enable`, `!md disable`, `!md status`, or `!md auto <low|medium|high>`")
            return

        if cmd == "enable":
            state = set_md_enabled(True)
            status_text = "ENABLED"
        elif cmd == "disable":
            state = set_md_enabled(False)
            status_text = "DISABLED"
        elif cmd == "auto":
            state = set_md_auto(level or "medium")
            status_text = "AUTO (ARMED OFF UNTIL DETECTION)"
        else:
            state = get_md_state()
            status_text = "ENABLED" if state.get("enabled") else "DISABLED"

        enabled = bool(state.get("enabled"))
        mode = str(state.get("mode", "manual")).upper()
        auto_level = str(state.get("auto_level", "medium")).upper()
        last_decay = state.get("last_decay")
        last_decay_level = state.get("last_decay_level")
        last_change = state.get("last_changed")
        market_open_prev = state.get("market_open_prev")

        embed = discord.Embed(
            title="🧭 Momentum Decay Strict Mode",
            color=0x2ECC71 if enabled else 0xE74C3C,
        )
        embed.add_field(
            name="Status",
            value=ab(A(status_text, "green" if enabled else "red", bold=True)),
            inline=False,
        )
        embed.add_field(name="Mode", value=ab(A(mode, "cyan", bold=True)), inline=True)
        if mode == "AUTO":
            embed.add_field(name="Auto Level", value=ab(A(auto_level, "yellow", bold=True)), inline=True)
            market_text = "OPEN" if market_open_prev else "CLOSED"
            embed.add_field(name="Market Session", value=ab(A(market_text, "green" if market_open_prev else "red")), inline=True)
        embed.add_field(name="Last Decay", value=ab(A(_format_ts(last_decay) if last_decay else "None", "cyan")), inline=True)
        embed.add_field(name="Last Decay Level", value=ab(A(str(last_decay_level).upper() if last_decay_level else "None", "cyan")), inline=True)
        embed.add_field(name="Last Change", value=ab(A(_format_ts(last_change) if last_change else "None", "cyan")), inline=True)

        if enabled and md_needs_warning(state):
            embed.add_field(name="⚠️ Warning", value=ab(A("MD strict is enabled but no recent decay detected. Consider disabling.", "yellow")), inline=False)

        if not enabled:
            embed.add_field(name="How It Works", value=ab(A("When ON, stop losses tighten during momentum decay.", "yellow")), inline=False)
        if mode == "AUTO":
            embed.add_field(name="Auto Rule", value=ab(A("MD stays OFF at session transitions, then turns ON only when decay is detected at/above the selected level. It turns OFF again if decay drops below level.", "yellow")), inline=False)

        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("md_error")
        await _send_embed(ctx, "md failed due to an internal error.")


async def handle_predict(ctx, minutes):
    if minutes is None:
        await _send_embed(ctx, "Usage: `!predict <minutes>`\nAllowed values: 30 or 60\nExample: `!predict 30`")
        return

    if not isinstance(minutes, str):
        await _send_embed(ctx, "Minutes must be text input.\nExample: `!predict 60`")
        return

    if not minutes.isdigit():
        await _send_embed(ctx, "Minutes must be a number.\nExample: `!predict 60`")
        return

    timeframe_minutes = int(minutes)

    if timeframe_minutes not in [30, 60]:
        await _send_embed(ctx, "Invalid timeframe.\nAllowed values: 30 or 60.")
        return

    df = await asyncio.to_thread(get_market_dataframe)

    if df is None:
        await _send_embed(ctx, "Market data unavailable.")
        return

    pred = make_prediction(timeframe_minutes, df)

    if not pred:
        await _send_embed(ctx, "Not enough graded predictions.\nNeed at least 5 graded predictions.")
        return

    await ctx.send(embed=_build_predict_embed(pred, timeframe_minutes, df))


async def handle_plan(ctx, side, strike, premium, contracts, expiry):
    if not all([side, strike, premium, contracts, expiry]):
        await _send_embed(
            ctx,
            "Usage:\n"
            "!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>\n\n"
            "Example:\n"
            "!plan call 435 1.20 2 2026-02-14"
        )
        return

    if not isinstance(side, str):
        await _send_embed(ctx, "Side must be provided as text.")
        return

    side = side.lower()

    if side not in ["call", "put"]:
        await _send_embed(ctx, "Side must be 'call' or 'put'.")
        return

    if strike is None or premium is None or contracts is None:
        await _send_embed(ctx, "Strike, premium, and contracts are required.")
        return

    if isinstance(strike, str):
        strike_text = strike.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", strike_text):
            await _send_embed(ctx, "Strike must be numeric.")
            return
        strike = float(strike_text)
    elif isinstance(strike, (int, float)):
        strike = float(strike)
    else:
        await _send_embed(ctx, "Strike must be numeric.")
        return

    if isinstance(premium, str):
        premium_text = premium.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", premium_text):
            await _send_embed(ctx, "Premium must be numeric.")
            return
        premium = float(premium_text)
    elif isinstance(premium, (int, float)):
        premium = float(premium)
    else:
        await _send_embed(ctx, "Premium must be numeric.")
        return

    if isinstance(contracts, str):
        contracts_text = contracts.strip()
        if not re.fullmatch(r"[+-]?\d+", contracts_text):
            await _send_embed(ctx, "Contracts must be a whole number.")
            return
        contracts = int(contracts_text)
    elif isinstance(contracts, int):
        contracts = int(contracts)
    else:
        await _send_embed(ctx, "Contracts must be a whole number.")
        return

    try:
        df = await asyncio.to_thread(get_market_dataframe)
        if df is None:
            await _send_embed(ctx, "Market data unavailable.")
            return
        embed = _build_plan_embed(side, strike, premium, contracts, expiry, df)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception(f"!plan failed: {e}")
        await _send_embed(ctx, "⚠️ Plan error — data still warming up.")


async def handle_preopen(ctx):
    try:
        df = await asyncio.to_thread(get_market_dataframe)
        if df is None or df.empty:
            await _send_embed(ctx, "Market data unavailable.", title="Pre-Open Check", color=0xE74C3C)
            return

        market_open = df.attrs.get("market_open")
        market_status = "OPEN" if market_open else "CLOSED"
        data_freshness = _get_data_freshness_text() or "Data age: N/A"

        last_close = df.iloc[-1].get("close") if len(df) > 0 else None
        last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
        close_text = f"{last_close_val:.2f}" if isinstance(last_close_val, (int, float)) else "N/A"

        profile_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "simulation", "sim_config.yaml")
        )
        profile = None
        try:
            with open(profile_path, "r") as f:
                profiles = yaml.safe_load(f) or {}
                profile = profiles.get("SIM03") or profiles.get("SIM01")
        except Exception:
            profile = None

        contract_status = "Not checked"
        contract_reason = None
        bull_ok = False
        bear_ok = False
        bull_text = "Not checked"
        bear_text = "Not checked"
        if profile and isinstance(last_close, (int, float)) and last_close > 0:
            bull_text, bull_ok = _check_contracts_for_direction(
                "BULLISH", profile, last_close_val, select_sim_contract_with_reason
            )
            bear_text, bear_ok = _check_contracts_for_direction(
                "BEARISH", profile, last_close_val, select_sim_contract_with_reason
            )
            if bull_ok or bear_ok:
                contract_status = "OK"
            else:
                contract_status = "Unavailable"
                contract_reason = "no_contracts_found"

        color = 0x2ECC71
        if bull_ok or bear_ok:
            color = 0x2ECC71
        elif market_status == "CLOSED":
            color = 0xF39C12
        else:
            color = 0xE74C3C

        title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
        embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
        embed.add_field(name=_add_field_icons("Market"), value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Last Price"), value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Data Freshness"), value=ab(A(data_freshness, "cyan")), inline=False)
        embed.add_field(name=_add_field_icons("Option Snapshot"), value=ab(A(contract_status, "green" if contract_status == "OK" else "yellow", bold=True)), inline=False)
        embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
        embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)
        if contract_reason:
            embed.add_field(name=_add_field_icons("Reason"), value=ab(A(contract_reason, "red", bold=True)), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception("preopen_error: %s", e)
        await _send_embed(ctx, "Pre-open check failed.", title="Pre-Open Check", color=0xE74C3C)
