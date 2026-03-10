"""
interface/shared_state.py
Shared constants, formatting helpers, and utility functions used across multiple cogs.
Extracted from bot.py during cog migration (Phase 10).
"""

import os
import asyncio
import logging
import csv
import json
import yaml
import pytz
import discord
import time as _time
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Any

from interface.fmt import (
    ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col,
    delta_col, ml_col, exit_reason_col, balance_col, wr_col,
    tier_col, drawdown_col, signed_col,
)
from core.data_service import get_market_dataframe
from core.account_repository import load_account
from core.paths import DATA_DIR
from simulation.sim_portfolio import SimPortfolio

# ── Channel IDs ───────────────────────────────────────────────────────────────
PAPER_CHANNEL_ID = 1470599150843203636
ALERT_CHANNEL_ID = 1470846800423551230
FORECAST_CHANNEL_ID = 1470931720739098774
HEART_CHANNEL_ID = 1470992071514132562
EOD_REPORT_CHANNEL_ID = 1476863964473196586

# ── Constants ─────────────────────────────────────────────────────────────────
BOT_TIMEZONE = "America/New_York"
ASK_CONTEXT_CACHE: dict[str, Any] = {}
BOT_START_TIME = datetime.now(pytz.timezone("US/Eastern"))
_CHART_TTL = 1800  # 30 minutes

STRATEGY_INTENTS = {
    "SIM00": "Live intraday trend‑pullback execution. Mirrors SIM03 logic with live routing and graduation gate.",
    "SIM01": "0DTE mean‑reversion scalp. Designed to fade short‑term extensions with short holds.",
    "SIM02": "0DTE breakout scalp. Momentum‑style entries with short holds.",
    "SIM03": "Intraday trend‑pullback. Looks for pullback entries in trend and rides continuation.",
    "SIM04": "Intraday range fade. Mean‑reversion inside range regimes, moderate holds.",
    "SIM05": "1DTE afternoon continuation. Trend‑pullback bias later session, longer holds.",
    "SIM06": "7–10 DTE short swing trend. Wider targets/stops, multi‑day holds.",
    "SIM07": "14–21 DTE swing trend. Longest holds, widest targets/stops.",
    "SIM08": "Regime‑filtered trend pullback. Only engages in TREND regime.",
    "SIM09": "Opportunity follower. Uses opportunity outputs to set direction, DTE, and hold context.",
    "SIM10": "ORB breakout. Requires features; trades breaks of opening range with volume impulse.",
    "SIM11": "Vol‑expansion trend. TREND_PULLBACK gated by ATR expansion; short swing.",
    "SIM29": "Power‑hour trend pullback. TREND_PULLBACK active only in final power hour; requires features.",
    "SIM30": "Multi‑timeframe confirm. Requires EMA alignment on both 5m and 15m bars plus price above VWAP.",
    "SIM31": "VPOC reversion. Fades displacement from the session volume point of control; requires features.",
    "SIM32": "Gap fade scalp. 0DTE fade of the opening gap back toward the pre‑gap close.",
    "SIM33": "Opening range reclaim. Enters on confirmed re‑entry into the 6‑bar opening range after a failed break.",
    "SIM34": "Vol compression breakout. Enters on the first ATR expansion bar after a compression period.",
    "SIM35": "Vol spike fade. Fades extreme ATR spikes when price is overextended from VWAP and RSI is at extremes.",
}

# ── Formatting Helpers ────────────────────────────────────────────────────────

def _infer_embed_style(title: str | None, description: str | None):
    text = f"{title or ''} {description or ''}".lower()
    error_terms = ("error", "failed", "invalid", "unknown", "missing")
    warn_terms = ("warning", "warn", "blocked", "disabled", "limit", "skip")
    success_terms = ("success", "complete", "updated", "reset", "ok", "done")
    if any(t in text for t in error_terms):
        return 0xE74C3C, "❌"
    if any(t in text for t in warn_terms):
        return 0xF39C12, "⚠️"
    if any(t in text for t in success_terms):
        return 0x2ECC71, "✅"
    return 0x3498DB, "ℹ️"


def _maybe_prefix_emoji(title: str | None, emoji: str) -> str | None:
    if not title:
        return title
    emoji_prefixes = ("✅", "❌", "⚠️", "ℹ️", "📘", "📋", "📈", "📊", "🧠", "🖥", "🤖", "📥", "📤", "🧪", "🧾")
    if title.startswith(emoji_prefixes):
        return title
    return f"{emoji} {title}"


def _add_field_icons(name: str) -> str:
    icon_map = {
        "Total Trades": "📦", "Open Trades": "📂", "Win Rate": "🎯",
        "Total PnL": "💰", "Avg Win": "📈", "Avg Loss": "📉",
        "Expectancy": "🧮", "Best Trade": "🥇", "Worst Trade": "🧯",
        "Max Drawdown": "📉", "Regime Breakdown": "🧭",
        "Time Bucket Breakdown": "🕒", "Exit Reasons": "🚪",
        "Last Trade": "🕘", "Risk / Balance": "🧰", "Details": "📋",
        "Context": "🧠", "ML": "🤖", "Reason": "⚠️", "Status": "✅",
        "Trader": "🧭", "Sims": "🧪", "Start Balance": "💵",
        "Strike": "🎯", "Premium": "💵", "Contracts": "📦",
        "Total Cost": "🧾", "Expiry": "📅", "Market State": "🧠",
        "Structure Context": "📐", "Final Grade": "🏆", "Market": "🟢",
        "System Health": "🧠", "System Diagnostics": "🧪",
        "Balance": "💰", "Trade Activity": "📒",
        "Background Systems": "⚙️", "Analytics Status": "📊",
        "Last Price": "📈", "Data Freshness": "⏱", "Option Snapshot": "🧩",
    }
    icon = icon_map.get(name)
    if icon and not name.startswith(icon):
        return f"{icon} {name}"
    return name


def _format_ts(value) -> str:
    eastern = pytz.timezone("America/New_York")
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return value
    else:
        return str(value)
    if dt.tzinfo is None:
        dt = eastern.localize(dt)
    else:
        dt = dt.astimezone(eastern)
    return dt.strftime("%Y-%m-%d %H:%M:%S ET")


def _format_pct_signed(val) -> str:
    try:
        num = float(val) * 100
        return f"{'+' if num >= 0 else '-'}{abs(num):.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def _format_duration_short(seconds) -> str:
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return "N/A"
    if total < 0:
        return "N/A"
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _get_data_freshness_text() -> str | None:
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("America/New_York")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        now = datetime.now(eastern)
        age = (now - ts).total_seconds()
        if age < 0:
            age = 0

        def _fmt_age(seconds: float) -> str:
            total = int(seconds)
            hours = total // 3600
            minutes = (total % 3600) // 60
            secs = total % 60
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)

        source = ""
        try:
            source = df.attrs.get("source")
        except Exception:
            source = ""
        source_text = f" | src: {source}" if source else ""
        market_open = None
        try:
            market_open = df.attrs.get("market_open")
        except Exception:
            market_open = None
        status_text = "Market open" if market_open else "Market closed"
        return f"{status_text} | Data age: {_fmt_age(age)} (last candle {ts.strftime('%H:%M:%S')} ET){source_text}"
    except Exception:
        return None


def _get_status_line() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        return f"Status: {risk_mode} | Last trade: {last_trade}"
    except Exception:
        return None


def _get_status_banner() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        balance = acc.get("balance")
        bal_text = f"${float(balance):,.2f}" if isinstance(balance, (int, float)) else "N/A"
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        freshness = _get_data_freshness_text() or "Data age: N/A"
        return (
            f"🧭 Risk: {risk_mode}\n"
            f"💰 Balance: {bal_text}\n"
            f"🕘 Last Trade: {last_trade}\n"
            f"⏱ {freshness}"
        )
    except Exception:
        return None


def _add_trend_arrow(val, good_when_high: bool = True) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return ""
    if num == 0:
        return "➖"
    if good_when_high:
        return "⬆️" if num > 0 else "⬇️"
    return "⬇️" if num > 0 else "⬆️"


def _tag_trade_mode(trade: dict) -> str:
    try:
        mode = (trade or {}).get("mode")
        if isinstance(mode, str) and mode:
            return mode.upper()
        if (trade or {}).get("reconstructed"):
            return "RECON"
    except Exception:
        pass
    return "SIM"


def _append_footer(embed: discord.Embed, extra: str | None = None) -> None:
    try:
        parts = []
        footer_text = embed.footer.text if embed.footer and embed.footer.text else ""
        if footer_text:
            parts.append(footer_text)
        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        if status_line and status_line not in footer_text:
            parts.append(status_line)
        if freshness_line and freshness_line not in footer_text:
            parts.append(freshness_line)
        if extra:
            parts.append(extra)
        if parts:
            footer_text = " | ".join(p for p in parts if p)
            if len(footer_text) > 2000:
                footer_text = footer_text[:2000]
            embed.set_footer(text=footer_text)
    except Exception:
        return


async def _send_embed(ctx, description: str, title: str | None = None, color: int | None = None):
    inferred_color, inferred_emoji = _infer_embed_style(title, description)
    final_color = color or inferred_color
    final_title = _maybe_prefix_emoji(title, inferred_emoji)
    final_description = description or ""
    banner = _get_status_banner()
    if banner is None:
        if "Data age:" not in final_description and "Status:" not in final_description and "🧭" not in final_description:
            status_line = _get_status_line()
            freshness_line = _get_data_freshness_text()
            context_lines = []
            if status_line:
                context_lines.append(status_line)
            if freshness_line:
                context_lines.append(freshness_line)
            if context_lines:
                appended = "\n".join(context_lines)
                if len(final_description) + len(appended) + 2 <= 3500:
                    final_description = f"{final_description}\n{appended}" if final_description else appended
    if final_title is None and description:
        if not description.startswith(("✅", "❌", "⚠️", "ℹ️")):
            final_description = f"{inferred_emoji} {description}"
    embed = discord.Embed(title=final_title, description=final_description, color=final_color)
    await ctx.send(embed=embed)


async def _schedule_delete(path: str, delay: float = _CHART_TTL):
    """Delete a file after `delay` seconds (fire-and-forget background task)."""
    await asyncio.sleep(delay)
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ── Sim Data Helpers ──────────────────────────────────────────────────────────

def _load_sim_profiles() -> dict:
    sim_config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
    )
    try:
        with open(sim_config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


from interface.shared_metrics import _collect_sim_metrics  # noqa: F401 – re-exported
