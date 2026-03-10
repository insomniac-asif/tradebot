# interface/watcher_preopen.py
#
# Pre-open check loop helpers extracted from watcher_helpers.py.
# Contains: _format_contract_table, _next_trading_day, _check_contracts,
#           _build_preopen_embed, _run_preopen_checks, _get_data_age_text_helper

import asyncio
import os
import logging
from datetime import datetime, timedelta

import discord
import pytz

from core.data_service import get_symbol_csv_path
from core.debug import debug_log
from simulation.sim_contract import select_sim_contract_with_reason, get_contract_error_stats, get_snapshot_probe
from interface.fmt import (
    ab, A,
)
from interface.watcher_preopen_probe import _run_snapshot_probe


# ---------------------------------------------------------------------------
# Shared small helpers (re-exported for convenience)
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
# preopen_check_loop helpers
# ===========================================================================

def _format_contract_table(rows: list) -> str:
    header = f"{'OTM':<6} {'Status':<8} Detail"
    lines = [header]
    for row in rows:
        label = row.get("label", "")
        ok = row.get("ok", False)
        if ok and row.get("probe"):
            status = "🟡 PROBE"
            detail = row.get("detail", "next expiry")
        elif ok:
            status = "🟢 OK"
            detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
        else:
            status = "🔴 FAIL"
            detail = row.get("reason", "unavailable")
        lines.append(f"{label:<6} {status:<8} {detail}")
    return "```\n" + "\n".join(lines) + "\n```"


def _next_trading_day(d):
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:
        nd += timedelta(days=1)
    return nd


def _check_contracts(
    direction: str,
    base_profile: dict,
    last_close,
    now_et: datetime,
    expiry_notice: dict,
) -> tuple[str, bool]:
    """
    Return (table_text, any_ok).

    Parameters
    ----------
    direction      : "BULLISH" or "BEARISH"
    base_profile   : sim profile dict (from sim_config.yaml)
    last_close     : last close price (numeric or None)
    now_et         : tz-aware datetime in Eastern
    expiry_notice  : mutable dict {"flag": bool, "text": str} updated in-place
    """
    rows = []
    any_ok = False
    if not base_profile:
        return "Profile unavailable", False
    last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
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
            contract, reason = select_sim_contract_with_reason(
                direction, last_close_val, prof
            )
            if contract is None and reason in {"no_candidate_expiry", "cutoff_passed"}:
                # Probe next trading-day expiry (pre-open only)
                probe_prof = dict(prof)
                try:
                    probe_prof["dte_min"] = 1
                    probe_prof["dte_max"] = max(1, int(base_profile.get("dte_max", 1)))
                except Exception:
                    probe_prof["dte_min"] = 1
                    probe_prof["dte_max"] = 1
                probe_contract, probe_reason = select_sim_contract_with_reason(
                    direction, last_close_val, probe_prof
                )
                if probe_contract:
                    next_exp = probe_contract.get("expiry", "")
                    exp_text = next_exp[:10] if isinstance(next_exp, str) else "next expiry"
                    rows.append({
                        "label": label,
                        "ok": True,
                        "probe": True,
                        "detail": (
                            f"{probe_contract.get('option_symbol','symbol')} "
                            f"{exp_text} (next trading day)"
                        ),
                    })
                    continue
                try:
                    dte_min_val = int(base_profile.get("dte_min", 0))
                    dte_max_val = int(base_profile.get("dte_max", 0))
                except Exception:
                    dte_min_val = base_profile.get("dte_min", "?")
                    dte_max_val = base_profile.get("dte_max", "?")
                next_exp = _next_trading_day(now_et.date())
                cutoff_note = ""
                if reason == "cutoff_passed":
                    cutoff_note = " | 0DTE cutoff passed (13:30 ET)"
                expiry_notice["flag"] = True
                expiry_notice["text"] = (
                    f"dte_min={dte_min_val} dte_max={dte_max_val}{cutoff_note} "
                    f"| next expiry {next_exp.isoformat()}"
                )
            if contract:
                any_ok = True
                symbol = contract.get("option_symbol", "symbol")
                spread = contract.get("spread_pct")
                spread_text = f"{spread:.3f}" if isinstance(spread, (int, float)) else "N/A"
                rows.append({
                    "label": label,
                    "ok": True,
                    "symbol": symbol,
                    "spread": spread_text,
                })
            else:
                rows.append({
                    "label": label,
                    "ok": False,
                    "reason": reason or "unavailable",
                })
        except Exception:
            rows.append({
                "label": label,
                "ok": False,
                "reason": "error",
            })
    return _format_contract_table(rows), any_ok


def _build_preopen_embed(
    channel,
    df,
    now_et: datetime,
    alpaca_status: str,
    last_close,
    open_items: list,
    profile: dict | None,
    data_freshness: str,
    snapshot_probe,
    expiry_notice: dict,
    bull_ok: bool,
    bear_ok: bool,
    bull_text: str,
    bear_text: str,
    contract_status: str,
    contract_reason: str | None,
) -> discord.Embed:
    """Build and return the pre-open readiness embed (does NOT send it)."""
    market_open = df.attrs.get("market_open")
    market_status = "OPEN" if market_open else "CLOSED"
    close_text = f"{float(last_close):.2f}" if isinstance(last_close, (int, float)) else "N/A"

    color = (
        0x2ECC71 if (bull_ok or bear_ok)
        else (0xF39C12 if market_status == "CLOSED" else 0xE74C3C)
    )
    title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
    embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)

    alpaca_color = (
        "green" if alpaca_status == "OK"
        else "red" if "Error" in alpaca_status or "Missing" in alpaca_status
        else "yellow"
    )
    embed.add_field(
        name="Alpaca Connectivity",
        value=ab(A(alpaca_status, alpaca_color, bold=True)),
        inline=False,
    )
    embed.add_field(
        name="Market",
        value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)),
        inline=True,
    )
    embed.add_field(
        name="Last Price",
        value=ab(A(f"${close_text}", "white", bold=True)),
        inline=True,
    )
    embed.add_field(
        name="Recorder Freshness",
        value=ab(A(data_freshness, "cyan")),
        inline=False,
    )
    status_color = (
        "green" if contract_status == "OK"
        else "yellow" if contract_status == "Unavailable"
        else "red"
    )
    embed.add_field(
        name="Option Snapshot",
        value=ab(A(contract_status, status_color, bold=True)),
        inline=False,
    )
    embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
    embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)

    if open_items:
        lines = []
        for trade in open_items:
            symbol = trade.get("option_symbol") or trade.get("symbol", "unknown")
            qty = trade.get("quantity") or trade.get("qty")
            entry_price = trade.get("entry_price")
            entry_text = f"${entry_price:.2f}" if isinstance(entry_price, (int, float)) else "N/A"
            lines.append(f"{symbol} | qty {qty} | entry {entry_text}")
        live_text = "\n".join(lines)
        embed.add_field(name="📌 Live Open Trades", value=ab(A(live_text, "yellow")), inline=False)
    else:
        embed.add_field(name="📌 Live Open Trades", value=ab(A("None", "green")), inline=False)

    if expiry_notice["flag"] and expiry_notice["text"]:
        embed.add_field(
            name="Expiry Window",
            value=ab(A(expiry_notice["text"], "yellow")),
            inline=False,
        )
    if contract_reason:
        embed.add_field(
            name="Reason",
            value=ab(A(contract_reason, "red", bold=True)),
            inline=False,
        )

    try:
        stats = get_contract_error_stats(3600)
        last_snap = stats.get("last_snapshot_error")
        if (
            contract_status != "OK"
            and last_snap
            and isinstance(last_snap, (list, tuple))
            and len(last_snap) == 2
        ):
            err_msg = str(last_snap[1])
            if err_msg:
                embed.add_field(
                    name="Snapshot Debug",
                    value=ab(A(err_msg, "yellow")),
                    inline=False,
                )
    except Exception:
        pass

    if snapshot_probe:
        embed.add_field(
            name="Snapshot Probe",
            value=ab(A(snapshot_probe, "yellow")),
            inline=False,
        )

    try:
        probe = get_snapshot_probe()
        if probe and contract_status != "OK":
            keys = probe.get("keys") or []
            size = probe.get("size")
            resp_type = probe.get("response_type")
            keys_text = ", ".join([str(k) for k in keys]) if keys else "none"
            size_text = str(size) if size is not None else "N/A"
            probe_lines = [
                f"resp_type={resp_type}",
                f"size={size_text}",
                f"keys={keys_text}",
            ]
            if probe.get("data_attr"):
                probe_lines.append(f"data_attr={probe.get('data_attr')}")
            if probe.get("snapshots_attr"):
                probe_lines.append(f"snapshots_attr={probe.get('snapshots_attr')}")
            embed.add_field(
                name="Raw Snapshot Probe",
                value=ab(A(" | ".join(probe_lines), "yellow")),
                inline=False,
            )
    except Exception:
        pass

    embed.set_footer(text=f"Time: {_format_et(now_et)} | {data_freshness}")
    return embed


# ===========================================================================
# preopen_check_loop inner body
# ===========================================================================

async def _run_preopen_checks(bot, channel, df, now_et: datetime, profile: dict | None) -> None:
    """
    Execute the multi-step pre-open validation and post the readiness embed.
    Called once per invocation from the preopen_check_loop shell.
    All state (last_run_date, etc.) is managed by the outer shell.
    """
    from core.account_repository import load_account
    from core.data_service import _load_symbol_registry

    # Alpaca connectivity check (best-effort)
    alpaca_status = "OK"
    try:
        api_key = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")
        if not api_key or not secret_key:
            alpaca_status = "Missing API keys"
        else:
            from alpaca.trading.client import TradingClient
            client = TradingClient(api_key, secret_key, paper=True)
            _ = client.get_account()
    except Exception as e:
        alpaca_status = f"Error: {str(e).splitlines()[0]}"

    data_freshness = _get_data_age_text_helper(df) or "Data age: N/A"
    last_close = df.iloc[-1].get("close") if len(df) > 0 else None

    # Live open trades snapshot
    open_items = []
    try:
        acc = load_account()
        t = acc.get("open_trade")
        if isinstance(t, dict):
            open_items.append(t)
        open_trades = acc.get("open_trades")
        if isinstance(open_trades, list):
            for item in open_trades:
                if isinstance(item, dict):
                    open_items.append(item)
    except Exception:
        open_items = []

    expiry_notice = {"flag": False, "text": ""}
    contract_status = "Not checked"
    contract_reason = None
    bull_ok = False
    bear_ok = False
    bull_text = "Not checked"
    bear_text = "Not checked"
    if profile and isinstance(last_close, (int, float)) and last_close > 0:
        bull_text, bull_ok = _check_contracts("BULLISH", profile, last_close, now_et, expiry_notice)
        bear_text, bear_ok = _check_contracts("BEARISH", profile, last_close, now_et, expiry_notice)
        if bull_ok or bear_ok:
            contract_status = "OK"
        else:
            contract_status = "Unavailable"
            contract_reason = "no_contracts_found"

    snapshot_probe = None
    if contract_status != "OK":
        snapshot_probe = await _run_snapshot_probe(last_close, now_et)

    embed = _build_preopen_embed(
        channel=channel,
        df=df,
        now_et=now_et,
        alpaca_status=alpaca_status,
        last_close=last_close,
        open_items=open_items,
        profile=profile,
        data_freshness=data_freshness,
        snapshot_probe=snapshot_probe,
        expiry_notice=expiry_notice,
        bull_ok=bull_ok,
        bear_ok=bear_ok,
        bull_text=bull_text,
        bear_text=bear_text,
        contract_status=contract_status,
        contract_reason=contract_reason,
    )
    await channel.send(embed=embed)


def _get_data_age_text_helper(df) -> str | None:
    """Compute data age text from a pre-loaded dataframe (no extra fetch)."""
    try:
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
