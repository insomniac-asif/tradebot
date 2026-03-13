# interface/watcher_utils.py
#
# Shared small helpers used across watcher modules.
# Extracted from watchers.py to reduce its size.
#
# Contents:
#   - _infer_embed_style
#   - _format_et
#   - _last_spy_price
#   - _get_underlying_price
#   - _parse_strike_from_symbol
#   - _format_contract_simple
#   - _get_data_age_text
#   - _send_embed_message
#   - _send

import os
import logging
from datetime import datetime

import discord
import pytz

from core.data_service import get_market_dataframe, get_symbol_csv_path


def _infer_embed_style(text: str):
    lower = (text or "").lower()
    error_terms = ("error", "failed", "invalid", "unknown", "missing")
    warn_terms = ("warning", "warn", "blocked", "disabled", "limit", "skip")
    success_terms = ("success", "complete", "updated", "reset", "ok", "done")
    if any(t in lower for t in error_terms):
        return 0xE74C3C, "❌"
    if any(t in lower for t in warn_terms):
        return 0xF39C12, "⚠️"
    if any(t in lower for t in success_terms):
        return 0x2ECC71, "✅"
    return 0x3498DB, "ℹ️"


def _format_et(ts: datetime | None) -> str:
    if ts is None:
        return "N/A"
    eastern = pytz.timezone("America/New_York")
    if ts.tzinfo is None:
        ts = eastern.localize(ts)
    else:
        ts = ts.astimezone(eastern)
    return ts.strftime("%Y-%m-%d %H:%M:%S ET")


def _last_spy_price(df) -> float | None:
    try:
        if df is None or len(df) == 0:
            return None
        last = df.iloc[-1]
        val = last.get("close")
        return float(val) if val is not None else None
    except Exception:
        return None


def _get_underlying_price(symbol: str) -> float | None:
    """Return last close price for any symbol from its registry CSV."""
    try:
        import pandas as pd
        csv_path = get_symbol_csv_path(symbol.upper())
        if not csv_path or not os.path.exists(csv_path):
            return None
        df = pd.read_csv(csv_path, usecols=lambda c: c.lower() in ("close", "c"))
        if df.empty:
            return None
        col = next((c for c in df.columns if c.lower() in ("close", "c")), None)
        if col is None:
            return None
        return float(df[col].iloc[-1])
    except Exception:
        return None


def _parse_strike_from_symbol(option_symbol: str | None) -> float | None:
    if not option_symbol or not isinstance(option_symbol, str):
        return None
    try:
        strike_part = option_symbol[-8:]
        return int(strike_part) / 1000.0
    except Exception:
        return None


def _format_contract_simple(
    option_symbol: str | None,
    direction: str | None,
    expiry: str | None,
    strike: float | None = None,
) -> str:
    cp = None
    if isinstance(direction, str):
        d = direction.lower()
        if d == "bullish":
            cp = "CALL"
        elif d == "bearish":
            cp = "PUT"
    if cp is None and isinstance(option_symbol, str) and len(option_symbol) >= 10:
        try:
            cp_char = option_symbol[9]
            if cp_char == "C":
                cp = "CALL"
            elif cp_char == "P":
                cp = "PUT"
        except Exception:
            cp = None
    if strike is None:
        strike = _parse_strike_from_symbol(option_symbol)
    expiry_text = ""
    if isinstance(expiry, str) and len(expiry) >= 10:
        expiry_text = expiry[:10]
    import re as _re_und
    _und_m = _re_und.match(r'^([A-Z]{1,6})', option_symbol or "")
    label = _und_m.group(1) if _und_m else ""
    if cp:
        label = f"{label} {cp}"
    if expiry_text:
        label = f"{label} {expiry_text}"
    if isinstance(strike, (int, float)):
        label = f"{label} {strike:g}"
    return label


def _get_data_age_text() -> str | None:
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

        market_open = None
        try:
            market_open = df.attrs.get("market_open")
        except Exception:
            market_open = None
        status_text = "Market open" if market_open else "Market closed"
        return f"{status_text} | Data age: {_fmt_age(age)} (last candle {ts.strftime('%H:%M:%S')} ET)"
    except Exception:
        return None


async def _send_embed_message(channel, message: str, title: str | None = None):
    if channel is None:
        return
    text = message if isinstance(message, str) else str(message)
    color, emoji = _infer_embed_style(text)
    if title:
        if not title.startswith(("✅", "❌", "⚠️", "ℹ️", "📘", "📋", "📈", "📊", "🧠", "🖥", "🤖", "📥", "📤")):
            title = f"{emoji} {title}"
    else:
        if text and not text.startswith(("✅", "❌", "⚠️", "ℹ️")):
            text = f"{emoji} {text}"
    embed = discord.Embed(title=title, description=text, color=color)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
    data_age = _get_data_age_text()
    if data_age:
        footer_parts.append(data_age)
    embed.set_footer(text=" | ".join(footer_parts))
    await channel.send(embed=embed)


async def _send(channel, message=None, **kwargs):
    if channel is None:
        return
    if message is None:
        message = ""
    if "embed" in kwargs:
        return await channel.send(message, **kwargs)
    if isinstance(message, discord.Embed):
        return await channel.send(embed=message)
    return await _send_embed_message(channel, message)
