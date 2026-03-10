# simulation/sim_report_helpers.py
"""
Embed builder functions and format helper functions extracted from sim_watcher.py.
"""
import re
import pytz
import discord
from datetime import datetime
from core.data_service import get_market_dataframe
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    exit_reason_col,
    balance_col,
    pct_col,
)


def _now_et() -> datetime:
    return datetime.now(pytz.timezone("US/Eastern"))


def _format_et(ts: datetime | None) -> str:
    if ts is None:
        return "N/A"
    try:
        if ts.tzinfo is None:
            ts = pytz.timezone("US/Eastern").localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("US/Eastern"))
        return ts.strftime("%Y-%m-%d %H:%M:%S ET")
    except Exception:
        return "N/A"

def _parse_strike_from_symbol(option_symbol: str | None) -> float | None:
    if not option_symbol or not isinstance(option_symbol, str):
        return None
    try:
        strike_part = option_symbol[-8:]
        return int(strike_part) / 1000.0
    except Exception:
        return None


def _format_option_symbol(symbol: str | None) -> str:
    """Convert OCC symbol (e.g. SPY260312P00680000) to human-readable form."""
    if not symbol:
        return "unknown"
    m = re.match(r'^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$', symbol)
    if not m:
        return symbol
    root, yy, mm, dd, opt_type, strike_raw = m.groups()
    strike = int(strike_raw) / 1000
    type_str = "PUT" if opt_type == "P" else "CALL"
    strike_fmt = f"${strike:.0f}" if strike == int(strike) else f"${strike:.2f}"
    return f"{root} {type_str} {strike_fmt} exp {int(mm)}/{int(dd)}"


def _format_entry_time(ts_str: str | None) -> str:
    """Format ISO entry timestamp to clean HH:MM ET."""
    if not ts_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(str(ts_str).replace(" ", "T"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(pytz.timezone("US/Eastern"))
        return dt.strftime("%H:%M ET")
    except Exception:
        try:
            return str(ts_str)[:16]
        except Exception:
            return "N/A"


def _format_context_parts(raw: str, drop_keys: set[str] | None = None) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    drop_keys = drop_keys or set()
    parts = [p.strip() for p in raw.split("|") if p and p.strip()]
    out = []
    for part in parts:
        text = part
        text = text.replace("signal_mode=", "Signal: ")
        text = text.replace("regime=", "Regime: ")
        text = text.replace("bucket=", "Time: ")
        text = text.replace("dte=", "DTE: ")
        text = text.replace("horizon=", "Horizon: ")
        text = text.replace("reason=", "Reason: ")
        text = text.replace("loss_pct=", "Loss: ")
        text = text.replace("gain_pct=", "Gain: ")
        text = text.replace("<=", "≤")
        text = text.replace(">=", "≥")
        # Drop duplicated fields (e.g., signal_mode already shown)
        lowered = text.lower()
        if any(k in lowered for k in drop_keys):
            continue
        out.append(text)
    return out


def _format_exit_context(raw: str) -> list[str]:
    if not raw or not isinstance(raw, str):
        return []
    text = raw.strip()

    # Stop loss: loss_pct=... <= -X%
    m = re.search(r"loss_pct=([^\s]+)\s*<=\s*-?([^\s]+)", text)
    if m:
        return [f"Loss {m.group(1)} vs Stop {m.group(2)}"]

    # Profit target: gain_pct=... >= X%
    m = re.search(r"gain_pct=([^\s]+)\s*>=\s*([^\s]+)", text)
    if m:
        return [f"Gain {m.group(1)} vs Target {m.group(2)}"]

    # Profit lock: gain_pct=... <= lock_pct=...
    m = re.search(r"gain_pct=([^\s]+)\s*<=\s*lock_pct=([^\s]+)", text)
    if m:
        return [f"Gain {m.group(1)} vs Lock {m.group(2)}"]

    # Trailing stop: drop_from_high=... <= -X% (high=H)
    m = re.search(r"drop_from_high=([^\s]+)\s*<=\s*-?([^\s]+)", text)
    if m:
        high = None
        mh = re.search(r"high=([^\s\)]+)", text)
        if mh:
            high = mh.group(1)
        line = f"Drop {m.group(1)} vs Trail {m.group(2)}"
        if high:
            line += f" (High {high})"
        return [line]

    # Hold max
    m = re.search(r"elapsed=([^\s]+)\s*>=\s*hold_max=([^\s]+)", text)
    if m:
        return [f"Elapsed {m.group(1)} vs Hold Max {m.group(2)}"]

    # Expiry / daytrade cutoff
    m = re.search(r"expiry=([^\s]+)\s*cutoff=([^\s]+)", text)
    if m:
        return [f"Expiry {m.group(1)} cutoff {m.group(2)}"]
    m = re.search(r"daytrade_cutoff=([^\s]+)", text)
    if m:
        return [f"Daytrade cutoff {m.group(1)}"]

    # Theta burn context
    if "remaining=" in text and "dte=" in text and "gain_pct=" in text:
        g = re.search(r"gain_pct=([^\s]+)", text)
        r = re.search(r"remaining=([^\s]+)", text)
        d = re.search(r"dte=([^\s]+)", text)
        if g and r and d:
            return [f"Gain {g.group(1)} | Time left {r.group(1)} | DTE {d.group(1)}"]

    # IV crush context
    if "iv_entry=" in text and "tightened_stop=" in text and "gain_pct=" in text:
        g = re.search(r"gain_pct=([^\s]+)", text)
        iv = re.search(r"iv_entry=([^\s]+)", text)
        ts = re.search(r"tightened_stop=([^\s]+)", text)
        if g and iv and ts:
            return [f"Gain {g.group(1)} | IV entry {iv.group(1)} | Tight stop {ts.group(1)}"]

    # Fallback: generic formatting
    return _format_context_parts(text)


def _get_data_age_text(df=None) -> str | None:
    try:
        if df is None:
            df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("US/Eastern")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        age = (_now_et() - ts).total_seconds()
        if age < 0:
            age = 0
        return f"Data age: {age:.0f}s (last candle {ts.strftime('%H:%M:%S')} ET)"
    except Exception:
        return None


def _format_skip_reason(reason: str) -> str:
    mapping = {
        "insufficient_trade_history": "Not enough closed trades to allow live execution.",
        "cutoff_passed": "0DTE cutoff passed (after 13:30 ET).",
        "no_candidate_expiry": "No valid expiries for DTE window.",
        "empty_chain": "Option chain returned empty for expiry.",
        "chain_error": "Option chain request failed (API/market data).",
        "no_snapshot": "No snapshot returned for candidate contract.",
        "no_snapshot_all": "No snapshot returned for any candidate strike.",
        "no_snapshot_most": "Most candidate strikes returned no snapshot.",
        "no_snapshot_all_no_chain_symbols": "No snapshots and chain symbols missing (likely off-hours or API issue).",
        "no_quote": "Snapshot missing bid/ask quote.",
        "no_quote_all": "All candidate snapshots missing bid/ask.",
        "no_quote_most": "Most candidate snapshots missing bid/ask.",
        "no_quote_all_no_chain_symbols": "Quotes missing and chain symbols missing (off-hours or API issue).",
        "invalid_quote": "Bid/ask invalid (likely off-hours or illiquid).",
        "invalid_quote_all": "All candidate quotes invalid (off-hours or illiquid).",
        "invalid_quote_most": "Most candidate quotes invalid.",
        "invalid_quote_all_no_chain_symbols": "Quotes invalid and chain symbols missing.",
        "spread_too_wide": "Spread exceeds max_spread_pct.",
        "spread_too_wide_all": "All candidate contracts had spreads above max_spread_pct.",
        "spread_too_wide_most": "Most candidate contracts had spreads above max_spread_pct.",
        "spread_too_wide_all_no_chain_symbols": "Spreads too wide and chain symbols missing.",
        "snapshot_error": "Snapshot request failed (API/market data).",
        "snapshot_error_all": "All snapshot requests failed.",
        "snapshot_error_most": "Most snapshot requests failed.",
        "snapshot_error_all_no_chain_symbols": "Snapshot failures and chain symbols missing.",
        "no_chain_symbols": "Chain returned no symbols (SDK format mismatch or API issue).",
        "missing_api_keys": "Missing Alpaca API keys.",
        "invalid_price": "Underlying price invalid.",
        "invalid_direction": "Signal direction invalid.",
        "no_contract": "No contract met selection rules.",
        "directional_exposure_limit": "Too many sims already open in this direction.",
        "before_entry_window": "Before entry window start time.",
        "regime_filter": "Current regime does not match required filter.",
    }
    return mapping.get(reason, "")


def _format_feature_snapshot(fs: dict | None) -> str | None:
    if not isinstance(fs, dict) or not fs:
        return None

    def _f(key, fmt="{:.3f}"):
        val = fs.get(key)
        if val is None:
            return None
        try:
            return fmt.format(float(val))
        except Exception:
            return str(val)

    parts = []
    orb_h = _f("orb_high", "{:.2f}")
    orb_l = _f("orb_low", "{:.2f}")
    if orb_h and orb_l:
        parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
    vol_z = _f("vol_z")
    if vol_z:
        parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
    atr_exp = _f("atr_expansion")
    if atr_exp:
        parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
    vwap_z = _f("vwap_z")
    if vwap_z:
        parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
    close_z = _f("close_z")
    if close_z:
        parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
    iv_rank = _f("iv_rank_proxy")
    if iv_rank:
        parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")

    if not parts:
        return None
    return "  |  ".join(parts)


def _build_circuit_breaker_embed(sim_id: str, result: dict, tripped: bool) -> "discord.Embed":
    """Build embed for circuit_breaker_tripped or circuit_breaker_recovered."""
    from interface.fmt import wr_col
    source = result.get("source_sim", "?")
    if tripped:
        embed = discord.Embed(
            title=f"🔴 {sim_id} LIVE — Circuit Breaker TRIPPED",
            color=0xE74C3C,
        )
        embed.add_field(
            name="Source Performance",
            value=ab(
                f"{lbl('Source')} {A(source, 'cyan', bold=True)}  |  "
                f"{lbl('WR')} {wr_col(result.get('source_wr'))}  |  "
                f"{lbl('Exp')} {pnl_col(result.get('source_exp'))}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Thresholds",
            value=ab(
                f"{lbl('Min WR')} {A(str(result.get('threshold_wr')), 'yellow')}  |  "
                f"{lbl('Min Exp')} {A(str(result.get('threshold_exp')), 'yellow')}  |  "
                f"{lbl('Window')} {A(str(result.get('window')), 'white')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Action",
            value=ab(A("Live execution PAUSED. Will auto-resume when source recovers.", "red", bold=True)),
            inline=False,
        )
    else:
        embed = discord.Embed(
            title=f"🟢 {sim_id} LIVE — Circuit Breaker RECOVERED",
            color=0x2ECC71,
        )
        embed.add_field(
            name="Source Performance",
            value=ab(
                f"{lbl('Source')} {A(source, 'cyan', bold=True)}  |  "
                f"{lbl('WR')} {wr_col(result.get('source_wr'))}  |  "
                f"{lbl('Exp')} {pnl_col(result.get('source_exp'))}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Action",
            value=ab(A("Live execution RESUMED.", "green", bold=True)),
            inline=False,
        )
    embed.set_footer(text=f"Time: {_format_et(_now_et())}")
    return embed


def _build_trade_history_ready_embed(
    sim_id: str,
    trade_count: int,
    min_trades: int,
    last_data_age: str | None,
) -> "discord.Embed":
    embed = discord.Embed(
        title=f"✅ {sim_id} LIVE — Trade History Threshold Reached",
        description=ab(A(
            f"Source sim has reached {trade_count} closed trades. Live execution is now unlocked.",
            "green", bold=True
        )),
        color=0x2ECC71,
    )
    embed.add_field(
        name="Trades Logged",
        value=ab(A(f"{trade_count} / {min_trades}", "green", bold=True)),
        inline=True,
    )
    footer_parts = [f"Time: {_format_et(_now_et())}"]
    if last_data_age:
        footer_parts.append(last_data_age)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


# Re-export large embed builders from sim_report_helpers2 for backward compatibility
from simulation.sim_report_helpers2 import (  # noqa: E402
    _build_entry_embed,
    _build_exit_embed,
    _build_skip_embed,
)
