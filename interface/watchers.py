# interface/watchers.py

import asyncio
import os
import yaml
import logging
from datetime import datetime, time as dtime, timedelta
import uuid
import pytz
import discord
try:
    import pandas_market_calendars as mcal
except ImportError:
    mcal = None
import pandas as pd
from pandas.errors import EmptyDataError

from core.market_clock import market_is_open
from core.data_service import get_market_dataframe
from core.debug import debug_log
from core.structured_logger import slog, slog_critical
from core.reconciler import write_heartbeat
from core.account_repository import save_account
from core.session_scope import get_rth_session_view
from core.data_integrity import validate_market_dataframe
from core.decision_context import DecisionContext
from core.paths import DATA_DIR
from decision.trader import (
    open_trade_if_valid,
    manage_trade,
    get_ml_visibility_snapshot,
    _finalize_reconstructed_trade,
)

from signals.conviction import calculate_conviction, momentum_is_decaying
from core.md_state import record_md_decay, is_md_enabled, get_md_state, md_needs_warning, evaluate_md_auto
from signals.opportunity import evaluate_opportunity 
from signals.environment_filter import trader_environment_filter
from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state

from analytics.prediction_stats import log_prediction
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.risk_control import dynamic_risk_percent
from analytics.feature_drift import detect_feature_drift
from analytics.grader import check_predictions
from simulation.sim_contract import select_sim_contract_with_reason, get_contract_error_stats, get_snapshot_probe
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    vol_col,
    delta_col,
    ml_col,
    result_col,
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)

DISCORD_OWNER_ID = int(os.getenv("DISCORD_OWNER_ID", "0") or "0")
_MD_TURNOFF_SUGGESTED_DATE = None


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

def _parse_strike_from_symbol(option_symbol: str | None) -> float | None:
    if not option_symbol or not isinstance(option_symbol, str):
        return None
    try:
        strike_part = option_symbol[-8:]
        return int(strike_part) / 1000.0
    except Exception:
        return None


def _format_contract_simple(option_symbol: str | None, direction: str | None, expiry: str | None, strike: float | None = None) -> str:
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
    label = "SPY"
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


async def preopen_check_loop(bot, channel_id: int):
    """
    Auto pre-open readiness check. Runs once per trading day around 09:25 ET.
    Posts a summary embed to the given channel.
    """
    last_run_date = None
    eastern = pytz.timezone("America/New_York")
    target_time = dtime(9, 25)
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            # Only run near the open window to avoid off-hours spam
            window_start = dtime(9, 10)
            window_end = dtime(9, 40)
            if now_et.time() < window_start or now_et.time() > window_end:
                await asyncio.sleep(600)
                continue
            if now_et.time() < target_time:
                await asyncio.sleep(30)
                continue
            if last_run_date == now_et.date():
                await asyncio.sleep(300)
                continue

            channel = bot.get_channel(channel_id) if bot else None
            if channel is None:
                await asyncio.sleep(300)
                continue

            df = get_market_dataframe()
            if df is None or df.empty:
                await _send_embed_message(channel, "Market data unavailable.", title="Pre-Open Check")
                last_run_date = now_et.date()
                continue

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

            market_open = df.attrs.get("market_open")
            market_status = "OPEN" if market_open else "CLOSED"
            data_freshness = _get_data_age_text() or "Data age: N/A"
            last_close = df.iloc[-1].get("close") if len(df) > 0 else None
            close_text = f"{float(last_close):.2f}" if isinstance(last_close, (int, float)) else "N/A"

            # Live open trades snapshot
            open_items = []
            try:
                from core.account_repository import load_account
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

            profile_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
            )
            profile = None
            try:
                with open(profile_path, "r") as f:
                    profiles = yaml.safe_load(f) or {}
                    profile = profiles.get("SIM03") or profiles.get("SIM01")
            except Exception:
                profile = None

            def _format_contract_table(rows):
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

            expiry_notice = {"flag": False, "text": ""}

            def _next_trading_day(d):
                nd = d + timedelta(days=1)
                while nd.weekday() >= 5:
                    nd += timedelta(days=1)
                return nd

            def _check_contracts(direction: str, base_profile: dict) -> tuple[str, bool]:
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
                                    "detail": f"{probe_contract.get('option_symbol','symbol')} {exp_text} (next trading day)",
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

            contract_status = "Not checked"
            contract_reason = None
            bull_ok = False
            bear_ok = False
            bull_text = "Not checked"
            bear_text = "Not checked"
            if profile and isinstance(last_close, (int, float)) and last_close > 0:
                bull_text, bull_ok = _check_contracts("BULLISH", profile)
                bear_text, bear_ok = _check_contracts("BEARISH", profile)
                if bull_ok or bear_ok:
                    contract_status = "OK"
                else:
                    contract_status = "Unavailable"
                    contract_reason = "no_contracts_found"

            snapshot_probe = None
            if contract_status != "OK":
                try:
                    from alpaca.data.historical import OptionHistoricalDataClient
                    from alpaca.data.requests import OptionChainRequest, OptionSnapshotRequest
                    import alpaca.data.enums as alpaca_enums
                    from core.rate_limiter import rate_limit_wait
                    api_key = os.getenv("APCA_API_KEY_ID")
                    secret_key = os.getenv("APCA_API_SECRET_KEY")
                    if api_key and secret_key and isinstance(last_close, (int, float)):
                        client = OptionHistoricalDataClient(api_key, secret_key)
                        expiry_date = now_et.date()
                        wait = rate_limit_wait("alpaca_option_chain", 0.5)
                        if wait > 0:
                            await asyncio.sleep(wait)
                        contract_type = getattr(alpaca_enums, "ContractType", None)
                        options_feed = getattr(alpaca_enums, "OptionsFeed", None)
                        feed_val = None
                        try:
                            desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
                            if options_feed is not None:
                                if desired == "opra":
                                    feed_val = options_feed.OPRA
                                elif desired == "indicative":
                                    feed_val = options_feed.INDICATIVE
                        except Exception:
                            feed_val = None
                        type_call = None
                        try:
                            if contract_type is not None and hasattr(contract_type, "CALL"):
                                type_call = contract_type.CALL
                        except Exception:
                            type_call = None
                        chain = client.get_option_chain(
                            OptionChainRequest(
                                underlying_symbol="SPY",
                                type=type_call,
                                feed=feed_val,
                                expiration_date=expiry_date
                            )
                        )
                        symbol = None
                        if isinstance(chain, dict):
                            symbol = next(iter(chain.keys()), None)
                        else:
                            data = getattr(chain, "data", None)
                            if isinstance(data, dict):
                                symbol = next(iter(data.keys()), None)
                            elif isinstance(data, list) and data:
                                symbol = getattr(data[0], "symbol", None) or (data[0].get("symbol") if isinstance(data[0], dict) else None)
                            chains = getattr(chain, "chains", None)
                            if symbol is None and isinstance(chains, dict):
                                symbol = next(iter(chains.keys()), None)
                            df_chain = getattr(chain, "df", None)
                            if symbol is None and df_chain is not None:
                                try:
                                    if "symbol" in df_chain.columns and not df_chain.empty:
                                        symbol = df_chain["symbol"].iloc[0]
                                except Exception:
                                    symbol = None
                        if symbol:
                            snapshot_probe_lines = []
                            def _snap_meta(resp, label: str):
                                try:
                                    if isinstance(resp, dict):
                                        keys = list(resp.keys())[:3]
                                        size = len(resp)
                                        keys_text = ",".join(keys) if keys else "none"
                                        snapshot_probe_lines.append(f"{label}: size={size} keys={keys_text}")
                                    else:
                                        snapshot_probe_lines.append(f"{label}: type={type(resp).__name__}")
                                except Exception:
                                    snapshot_probe_lines.append(f"{label}: error")
                            try:
                                debug_log(
                                    "preopen_snapshot_probe_request",
                                    symbol=symbol,
                                    expiry=expiry_date.isoformat(),
                                )
                            except Exception:
                                pass
                            wait = rate_limit_wait("alpaca_option_snapshot", 0.5)
                            if wait > 0:
                                await asyncio.sleep(wait)
                            try:
                                if feed_val is not None:
                                    req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=feed_val)
                                else:
                                    req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
                            except Exception:
                                req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
                            snap_resp = client.get_option_snapshot(req)
                            _snap_meta(snap_resp, "default")
                            if isinstance(snap_resp, dict) and len(snap_resp) == 0 and feed_val is None:
                                try:
                                    options_feed = getattr(alpaca_enums, "OptionsFeed", None)
                                    if options_feed is not None and hasattr(options_feed, "INDICATIVE"):
                                        debug_log("preopen_snapshot_probe_retry", symbol=symbol, feed="indicative")
                                        req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=options_feed.INDICATIVE)
                                        snap_resp = client.get_option_snapshot(req)
                                        _snap_meta(snap_resp, "indicative")
                                except Exception:
                                    pass
                            snap_type = type(snap_resp).__name__
                            keys_hint = ""
                            if isinstance(snap_resp, dict):
                                keys_hint = f" keys={list(snap_resp.keys())[:5]}"
                            snapshot_probe = f"probe_symbol={symbol} resp_type={snap_type}{keys_hint}"
                            if snapshot_probe_lines:
                                snapshot_probe = f"{snapshot_probe} | feeds: " + " / ".join(snapshot_probe_lines)
                except Exception as e:
                    snapshot_probe = f"probe_error={str(e).splitlines()[0]}"

            color = 0x2ECC71 if (bull_ok or bear_ok) else (0xF39C12 if market_status == "CLOSED" else 0xE74C3C)
            title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
            embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
            alpaca_color = "green" if alpaca_status == "OK" else "red" if "Error" in alpaca_status or "Missing" in alpaca_status else "yellow"
            embed.add_field(name="Alpaca Connectivity", value=ab(A(alpaca_status, alpaca_color, bold=True)), inline=False)
            embed.add_field(name="Market", value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
            embed.add_field(name="Last Price", value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
            embed.add_field(name="Recorder Freshness", value=ab(A(data_freshness, "cyan")), inline=False)
            status_color = "green" if contract_status == "OK" else "yellow" if contract_status == "Unavailable" else "red"
            embed.add_field(name="Option Snapshot", value=ab(A(contract_status, status_color, bold=True)), inline=False)
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
                embed.add_field(name="Expiry Window", value=ab(A(expiry_notice["text"], "yellow")), inline=False)
            if contract_reason:
                embed.add_field(name="Reason", value=ab(A(contract_reason, "red", bold=True)), inline=False)
            try:
                stats = get_contract_error_stats(3600)
                last_snap = stats.get("last_snapshot_error")
                if contract_status != "OK" and last_snap and isinstance(last_snap, (list, tuple)) and len(last_snap) == 2:
                    err_msg = str(last_snap[1])
                    if err_msg:
                        embed.add_field(name="Snapshot Debug", value=ab(A(err_msg, "yellow")), inline=False)
            except Exception:
                pass
            if snapshot_probe:
                embed.add_field(name="Snapshot Probe", value=ab(A(snapshot_probe, "yellow")), inline=False)
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
                    embed.add_field(name="Raw Snapshot Probe", value=ab(A(" | ".join(probe_lines), "yellow")), inline=False)
            except Exception:
                pass
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {data_freshness}")
            await channel.send(embed=embed)

            last_run_date = now_et.date()
        except Exception:
            logging.exception("preopen_check_loop_error")
        await asyncio.sleep(30)


async def eod_open_trade_report_loop(bot, channel_id: int):
    """
    End-of-day open trade report for live trades (main trader).
    Posts summary of open trades at 16:00 ET.
    """
    last_run_date = None
    eastern = pytz.timezone("America/New_York")
    cutoff = dtime(16, 0)
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < cutoff:
                await asyncio.sleep(30)
                continue
            if last_run_date == now_et.date():
                await asyncio.sleep(300)
                continue

            channel = bot.get_channel(channel_id) if bot else None
            if channel is None:
                await asyncio.sleep(300)
                continue

            from core.account_repository import load_account
            acc = load_account()
            open_items = []
            # main open_trade
            t = acc.get("open_trade")
            if isinstance(t, dict):
                open_items.append(t)
            # reconstructed/open list
            open_trades = acc.get("open_trades")
            if isinstance(open_trades, list):
                for item in open_trades:
                    if isinstance(item, dict):
                        open_items.append(item)

            if not open_items:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Trades (Live)",
                    description="No open live trades at end of day.",
                    color=0x2ECC71,
                )
            else:
                embed = discord.Embed(
                    title="📌 End-of-Day Open Trades (Live)",
                    description="Open trades at market close.",
                    color=0xF39C12,
                )
                for trade in open_items:
                    symbol = trade.get("option_symbol") or trade.get("symbol", "unknown")
                    qty = trade.get("quantity") or trade.get("qty")
                    entry_price = trade.get("entry_price")
                    stop = trade.get("stop")
                    target = trade.get("target")
                    entry_text = f"${entry_price:.2f}" if isinstance(entry_price, (int, float)) else "N/A"
                    stop_text = f"${stop:.2f}" if isinstance(stop, (int, float)) else "N/A"
                    target_text = f"${target:.2f}" if isinstance(target, (int, float)) else "N/A"
                    pnl_text = "N/A"
                    try:
                        current_price = get_option_price(symbol)
                        if current_price is not None and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
                            pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                            pnl_text = f"{'+' if pnl_val >= 0 else ''}${pnl_val:.2f}"
                    except Exception:
                        pnl_text = "N/A"
                    embed.add_field(
                        name=f"{symbol}",
                        value=f"Qty {qty} | Entry {entry_text} | Stop {stop_text} | Target {target_text} | PnL {pnl_text}",
                        inline=False
                    )
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {_get_data_age_text() or 'Data age: N/A'}")
            await channel.send(embed=embed)
            last_run_date = now_et.date()
        except Exception:
            logging.exception("eod_open_trade_report_error")
        await asyncio.sleep(30)


async def option_chain_health_loop(bot, channel_id: int):
    """
    Hourly during market hours: report chain/snapshot errors for sims.
    """
    last_run_hour = None
    eastern = pytz.timezone("America/New_York")
    while True:
        try:
            now_et = datetime.now(eastern)
            if now_et.weekday() > 4 or not market_is_open():
                await asyncio.sleep(300)
                continue
            # Run once per hour
            if last_run_hour == (now_et.date(), now_et.hour):
                await asyncio.sleep(30)
                continue
            if now_et.minute != 0:
                await asyncio.sleep(30)
                continue

            stats = get_contract_error_stats(3600)
            chain_errors = stats.get("chain_errors", 0)
            snapshot_errors = stats.get("snapshot_errors", 0)
            last_chain = stats.get("last_chain_error")
            last_snap = stats.get("last_snapshot_error")
            last_success = stats.get("last_success")

            color = 0x2ECC71 if (chain_errors + snapshot_errors) == 0 else 0xF39C12
            title = "✅ Option Chain Health (Last 60m)" if color == 0x2ECC71 else "⚠️ Option Chain Health (Last 60m)"
            embed = discord.Embed(title=title, color=color)
            embed.add_field(name="Chain Errors", value=ab(A(str(chain_errors), "red" if chain_errors else "green", bold=True)), inline=True)
            embed.add_field(name="Snapshot Errors", value=ab(A(str(snapshot_errors), "red" if snapshot_errors else "green", bold=True)), inline=True)
            if last_chain:
                embed.add_field(name="Last Chain Error", value=ab(A(str(last_chain[1])[:200], "red")), inline=False)
            else:
                embed.add_field(name="Last Chain Error", value=ab(A("None", "green")), inline=False)
            if last_snap:
                embed.add_field(name="Last Snapshot Error", value=ab(A(str(last_snap[1])[:200], "red")), inline=False)
            else:
                embed.add_field(name="Last Snapshot Error", value=ab(A("None", "green")), inline=False)
            if last_success:
                sym = last_success.get("symbol")
                spr = last_success.get("spread_pct")
                ct = last_success.get("contract_type")
                spr_text = f"{spr:.3f}" if isinstance(spr, (int, float)) else "N/A"
                embed.add_field(
                    name="Last Success",
                    value=ab(
                        f"{lbl('Symbol')} {A(sym or 'N/A', 'magenta')}  |  "
                        f"{lbl('Type')} {A(ct or 'N/A', 'cyan')}  |  "
                        f"{lbl('Spr')} {A(spr_text, 'yellow', bold=True)}"
                    ),
                    inline=False
                )
            else:
                embed.add_field(name="Last Success", value=ab(A("None", "yellow")), inline=False)
            embed.set_footer(text=f"Time: {_format_et(now_et)} | {_get_data_age_text() or 'Data age: N/A'}")

            if bot is not None and channel_id:
                channel = bot.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            last_run_hour = (now_et.date(), now_et.hour)
        except Exception:
            logging.exception("option_chain_health_loop_error")
        await asyncio.sleep(30)
from analytics.conviction_stats import (
    log_conviction_signal,
    update_expectancy,
    get_conviction_expectancy_stats
)
from analytics.signal_logger import log_signal_attempt
from analytics.blocked_signal_tracker import log_blocked_signal, update_blocked_outcomes
from interface.fmt import ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col, delta_col, ml_col, result_col, exit_reason_col, balance_col, wr_col, tier_col, drawdown_col, pct_col

from interface.health_monitor import check_health
from execution.option_executor import close_option_position, get_option_price


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


# =========================================================
# OPPORTUNITY WATCHER
# =========================================================
print("Opportunity watcher started")

async def opportunity_watcher(bot, alert_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(alert_channel_id)

    last_alert = None

    while not bot.is_closed():

        if not market_is_open():
            await asyncio.sleep(120)
            continue

        df = get_market_dataframe()
        if df is None:
            await asyncio.sleep(120)
            continue

        result = evaluate_opportunity(df)

        if result and result != last_alert:

            side = result[0]
            low = result[1]
            high = result[2]
            price = result[3]
            conviction_score = result[4]
            tp_low = result[5] if len(result) > 5 else None
            tp_high = result[6] if len(result) > 6 else None
            stop_loss = result[7] if len(result) > 7 else None

            vol = volatility_state(df)
            regime = get_regime(df)

            # ----- Signal Strength Tier -----
            tier_score = conviction_score

            if vol == "HIGH":
                tier_score += 1

            if regime == "TREND":
                tier_score += 1

            if tier_score >= 6:
                tier = "HIGH"
                emoji = "🔥"
            elif tier_score >= 4:
                tier = "MEDIUM"
                emoji = "⚡"
            else:
                tier = "LOW"
                emoji = "🟡"

            opp_color = 0x2ECC71 if side == "CALL" else 0xE74C3C if side == "PUT" else 0x3498DB
            if tier == "HIGH":
                opp_color = 0x27AE60 if side == "CALL" else 0xC0392B
            opp_embed = discord.Embed(
                title=f"{emoji} {tier} Strength {side} Opportunity",
                color=opp_color
            )
            side_color = "green" if side == "CALL" else "red" if side == "PUT" else "blue"
            opp_embed.add_field(name="📍 Side", value=ab(A(side, side_color, bold=True)), inline=True)
            opp_embed.add_field(name="🧭 Tier", value=ab(tier_col(tier)), inline=True)
            opp_embed.add_field(name="💰 Current Price", value=ab(A(f"${price:.2f}", "white", bold=True)), inline=True)
            opp_embed.add_field(name="📐 Entry Zone", value=ab(A(f"${low:.2f} – ${high:.2f}", "cyan")), inline=True)
            if tp_low is not None and tp_high is not None:
                opp_embed.add_field(name="🎯 Take-Profit", value=ab(A(f"${tp_low:.2f} – ${tp_high:.2f}", "green", bold=True)), inline=True)
            if stop_loss is not None:
                opp_embed.add_field(name="🛑 Stop-Loss", value=ab(A(f"${stop_loss:.2f}", "red", bold=True)), inline=True)
            opp_embed.add_field(name="🔢 Conviction", value=ab(A(str(conviction_score), "yellow", bold=True)), inline=True)
            opp_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
            opp_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vol)), inline=True)
            opp_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
            await _send(channel, embed=opp_embed)

            last_alert = result

        await asyncio.sleep(120)


# =========================================================
# AUTO TRADER (Detailed + Structured)
# =========================================================
print("Auto trader started")
async def auto_trader(bot, channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id)
    nyse_calendar = mcal.get_calendar("NYSE") if mcal is not None else None
    while not bot.is_closed():
        if not getattr(bot, "trading_enabled", True):
            if not getattr(bot, "startup_notice_sent", False):
                startup_errors = getattr(bot, "startup_errors", [])
                err_text = "\n".join(f"• {e}" for e in startup_errors) if startup_errors else "• unknown"
                await _send(channel, 
                    "🛑 **Trading Disabled – Startup Phase Gate Failed**\n\n"
                    f"{err_text}"
                )
                bot.startup_notice_sent = True
            await asyncio.sleep(60)
            continue

        eastern = pytz.timezone("America/New_York")
        now_eastern = datetime.now(eastern)
        today = now_eastern.date()
        if nyse_calendar is not None and today.weekday() < 5:
            today_schedule = nyse_calendar.schedule(
                start_date=today.isoformat(),
                end_date=today.isoformat()
            )
            if today_schedule.empty:
                notice_date = getattr(bot, "last_holiday_notice_date", None)
                if notice_date != today.isoformat():
                    await _send(channel, 
                        "🗓️ **NYSE Holiday Closure**\n\n"
                        "Market is fully closed today. Auto trading attempts paused."
                    )
                    bot.last_holiday_notice_date = today.isoformat()
                await asyncio.sleep(60)
                continue

        if not market_is_open():
            await asyncio.sleep(60)
            continue

        df = get_market_dataframe()
        if df is None:
            await asyncio.sleep(60)
            continue
        spy_price = _last_spy_price(df)

        # Strict data integrity precheck before stale-data guard.
        validation = validate_market_dataframe(df)
        if not validation["valid"]:
            eastern = pytz.timezone("America/New_York")
            now = datetime.now(eastern)
            errors = validation.get("errors", [])

            debug_log("data_integrity_block", errors="; ".join(errors))

            was_invalid = getattr(bot, "data_integrity_state", False)
            last_warn = getattr(bot, "last_integrity_warning_time", None)
            allow_warn = (not was_invalid)
            if last_warn is None:
                allow_warn = True
            elif (now - last_warn).total_seconds() >= 300:
                allow_warn = True

            if allow_warn:
                top_error = errors[0] if errors else "unknown_integrity_error"
                await _send(channel, 
                    "⚠️ **data_integrity_block**\n\n"
                    "Market data failed integrity validation.\n"
                    f"Primary Reason: {top_error}\n"
                    "Trading attempt skipped."
                )
                bot.last_integrity_warning_time = now

            bot.data_integrity_state = True
            await asyncio.sleep(60)
            continue

        bot.data_integrity_state = False

        # Strict data freshness guard: block if latest candle is older than 2 minutes.
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is not None:
            eastern = pytz.timezone("America/New_York")
            ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
            if ts.tzinfo is None:
                ts = eastern.localize(ts)
            else:
                ts = ts.astimezone(eastern)

            now = datetime.now(eastern)
            age_seconds = (now - ts).total_seconds()
            if age_seconds > 120:
                debug_log(
                    "data_stale_block",
                    candle_time=ts.isoformat(),
                    age_seconds=round(age_seconds, 1)
                )
                was_stale = getattr(bot, "data_stale_state", False)
                last_warn = getattr(bot, "last_stale_warning_time", None)
                allow_warn = (not was_stale)
                if last_warn is None:
                    allow_warn = True
                elif (now - last_warn).total_seconds() >= 300:
                    allow_warn = True
                if getattr(bot, "last_skip_reason", None) == "data_stale":
                    allow_warn = False

                if allow_warn:
                    stale_embed = discord.Embed(
                        title="⚠️ Market Data Stale — Trading Paused",
                        color=0xE67E22
                    )
                    stale_embed.add_field(name="⏱️ Data Age", value=ab(A(f"{age_seconds:.0f}s", "red", bold=True)), inline=True)
                    stale_embed.add_field(name="⚠️ Threshold", value=ab(A("120s", "yellow", bold=True)), inline=True)
                    stale_embed.add_field(name="📋 Action", value=ab(A("Bot will retry once data freshens. No trades will open until feed recovers.", "yellow")), inline=False)
                    stale_embed.set_footer(text=f"{_format_et(now)}")
                    await _send(channel, embed=stale_embed)
                    bot.last_stale_warning_time = now
                    bot.last_skip_reason = "data_stale"
                    bot.last_skip_time = now
                bot.data_stale_state = True
                await asyncio.sleep(60)
                continue
            bot.data_stale_state = False

        decision_ctx = DecisionContext()
        trade = await open_trade_if_valid(decision_ctx)
        _record_decision_attempt(trade, decision_ctx)

        # --- data collection: log every signal cycle ---
        log_signal_attempt(decision_ctx, trade)
        if decision_ctx.outcome == "blocked":
            _spy_price = df.iloc[-1]["close"] if df is not None and len(df) > 0 else None
            log_blocked_signal(decision_ctx, _spy_price)

        if decision_ctx.outcome == "blocked":
            reason = decision_ctx.block_reason or "unknown"
            eastern = pytz.timezone("America/New_York")
            now = datetime.now(eastern)
            last_reason = getattr(bot, "last_skip_reason", None)
            if last_reason != reason:
                last_time = getattr(bot, "block_reason_last_time", {}).get(reason)
                if last_time is None or (now - last_time).total_seconds() >= 300:
                    friendly_reason = explain_block_reason(reason)
                    skip_embed = discord.Embed(
                        title="⏸️ Trade Skipped",
                        color=0xF39C12
                    )
                    skip_embed.add_field(name="🚫 Reason Code", value=ab(A(reason, "red")), inline=False)
                    skip_embed.add_field(name="📋 Explanation", value=ab(A(friendly_reason, "yellow")), inline=False)
                    regime_now = get_regime(df) if df is not None else "N/A"
                    vol_now = volatility_state(df) if df is not None else "N/A"
                    blended_val = decision_ctx.blended_score
                    threshold_val = decision_ctx.threshold
                    _delta = (blended_val - threshold_val) if blended_val is not None and threshold_val is not None else None
                    conf_val = getattr(decision_ctx, "confidence_60m", None)
                    skip_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime_now)), inline=True)
                    skip_embed.add_field(name="⚡ Vol", value=ab(vol_col(vol_now)), inline=True)
                    skip_embed.add_field(name="📊 Score Δ", value=ab(delta_col(_delta) if _delta is not None else A("N/A", "gray")), inline=True)
                    if conf_val is not None:
                        try:
                            conf_pct = float(conf_val) * 100
                            conf_text = f"{conf_pct:.1f}%"
                        except (TypeError, ValueError):
                            conf_text = "N/A"
                        skip_embed.add_field(
                            name="🎯 Confidence",
                            value=ab(A(conf_text, "white", bold=True)),
                            inline=True
                        )
                    skip_embed.set_footer(text=f"Suppressed for 5m per reason | {_format_et(now)}")
                    await _send(channel, embed=skip_embed)
                    bot.block_reason_last_time[reason] = now
                    bot.last_skip_reason = reason
                    bot.last_skip_time = now

        if trade == "EQUITY_PROTECTION":
            debug_log("trade_gate", reason="EQUITY_PROTECTION")
            await asyncio.sleep(60)
            continue

        if trade == "EDGE_DECAY":
            debug_log("trade_gate", reason="EDGE_DECAY")
            await asyncio.sleep(60)
            continue


        if trade and isinstance(trade, dict):

            # ----------------------------
            # SETUP EXPECTANCY CHECK
            # ----------------------------
            setup_stats = calculate_setup_expectancy()
            current_setup = trade.get("setup")

            if setup_stats and current_setup in setup_stats:

                setup_avg_R = setup_stats[current_setup]["avg_R"]

                if setup_avg_R < 0:
                    debug_log(
                        "trade_blocked",
                        gate="setup_expectancy",
                        setup=current_setup,
                        avg_R=round(setup_avg_R, 3)
                    )
                    await asyncio.sleep(60)
                    continue

            # ----------------------------
            # RISK THROTTLE
            # ----------------------------
            risk_percent = dynamic_risk_percent()

            if risk_percent < 0.01:
                throttle_embed = discord.Embed(
                    title="⚠️ Risk Throttled",
                    description="System is in drawdown protection mode. Position sizing has been reduced.",
                    color=0xF39C12
                )
                throttle_embed.add_field(name="📉 Current Risk/Trade", value=ab(pct_col(risk_percent, good_when_high=False, multiply=True)), inline=True)
                throttle_embed.add_field(name="🛡️ Normal Risk", value=ab(A("1.0%", "green", bold=True)), inline=True)
                throttle_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=throttle_embed)

            # ----------------------------
            # ENVIRONMENT FILTER
            # ----------------------------
            expectancy = get_conviction_expectancy_stats()

            env = trader_environment_filter(
                df,
                trade["type"],
                trade["confidence"],
                expectancy,
                trade.get("regime", get_regime(df))
            )

            if not env["allow"]:
                debug_log(
                    "trade_blocked",
                    gate="environment_filter",
                    adjusted_conf=round(env["adjusted_conf"], 3),
                    reasons="; ".join(env["blocks"])
                )
            else:
                ml_prob = trade.get("ml_probability")
                debug_log(
                    "trade_opened",
                    direction=trade["type"],
                    entry=round(trade["entry_price"], 2),
                    confidence=round(trade["confidence"], 3),
                    regime=trade.get("regime")
                )

                ml_line = (
                    f"\nML Probability: {ml_prob*100:.1f}%"
                    if ml_prob is not None
                    else "\nML Probability: (warming up)"
                )
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
                    f"{decision_ctx.ml_weight:.2f}"
                    if decision_ctx.ml_weight is not None
                    else "N/A"
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
                decision_factors = (
                    "\n\n🧠 **Decision Factors:**\n"
                    f"Dual Alignment: {dual_text}\n"
                    f"15m: {conf_15} | 60m: {conf_60}\n"
                    f"Blended: {blended_text}\n"
                    f"Threshold: {threshold_text}\n"
                    f"Delta: {delta_text}\n"
                    f"ML Weight: {ml_weight_text}\n"
                    f"Regime Samples: {regime_samples_text}\n"
                    f"Expectancy Samples: {expectancy_samples_text}"
                )

                direction_color = 0x2ECC71 if trade["type"] == "bullish" else 0xE74C3C
                direction_emoji = "🟢" if trade["type"] == "bullish" else "🔴"
                open_embed = discord.Embed(
                    title=f"🤖 Trade Opened — {direction_emoji} {trade['type'].upper()}",
                    color=direction_color
                )
                qty_val = trade.get("quantity") or trade.get("qty") or "?"
                risk_val = trade.get("risk_dollars")
                risk_text = ab(A(f"${risk_val:.2f}", "yellow", bold=True)) if isinstance(risk_val, (int, float)) else ab(A("N/A", "gray"))
                _delta_num = (blended_val - threshold_val) if blended_val is not None and threshold_val is not None else None
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
                    inline=False
                )
                open_embed.add_field(
                    name="🧠 Signal Detail",
                    value=ab(
                        f"{lbl('15m')} {A(conf_15,'cyan')}  {lbl('60m')} {A(conf_60,'cyan')}",
                        f"{lbl('ML wt')} {A(ml_weight_text,'magenta')}  {lbl('Reg samples')} {A(regime_samples_text,'white')}  {lbl('Exp samples')} {A(expectancy_samples_text,'white')}",
                    ),
                    inline=False
                )
                option_sym = trade.get("option_symbol")
                expiry = trade.get("expiry")
                strike = trade.get("strike")
                contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
                contract_lines = [A(contract_label, "magenta", bold=True)]
                if option_sym:
                    contract_lines.append(A(option_sym, "white"))
                open_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)
                if isinstance(spy_price, (int, float)):
                    open_embed.add_field(name="📈 SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
                open_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=open_embed)


        result = await asyncio.to_thread(manage_trade)

        if result:
            res, pnl, bal, trade = result
            close_color = 0x2ECC71 if res == "win" else 0xE74C3C
            close_emoji = "✅" if res == "win" else "❌"
            close_embed = discord.Embed(
                title=f"{close_emoji} Trade Closed — {res.upper()}",
                color=close_color
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
                        value=ab(f"{A(f'${entry_price:.4f}', 'white')} → {A(f'${exit_price:.4f}', 'white', bold=True)}"),
                        inline=True
                    )
                option_sym = trade.get("option_symbol")
                expiry = trade.get("expiry")
                strike = trade.get("strike")
                contract_label = _format_contract_simple(option_sym, trade.get("type"), expiry, strike)
                contract_lines = [A(contract_label, "magenta", bold=True)]
                if option_sym:
                    contract_lines.append(A(option_sym, "white"))
                close_embed.add_field(name="🧾 Contract", value=ab(*contract_lines), inline=False)
                if isinstance(spy_price, (int, float)):
                    close_embed.add_field(name="📈 SPY Price", value=ab(A(f"${spy_price:.2f}", "white", bold=True)), inline=True)
            close_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
            await _send(channel, embed=close_embed)

        await asyncio.sleep(60)



# =========================================================
# PREDICTION GRADER LOOP
# =========================================================
print("Prediction grader started")
async def prediction_grader(bot, channel_id=None):

    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id) if channel_id is not None else None
    pred_file = f"{DATA_DIR}/predictions.csv"

    while not bot.is_closed():
        check_predictions()

        try:
            preds = pd.read_csv(pred_file)
        except (FileNotFoundError, EmptyDataError):
            preds = None
        except Exception:
            preds = None

        if preds is not None and not preds.empty and "checked" in preds.columns and "correct" in preds.columns:
            graded = preds[preds["checked"] == True].tail(100)
            if len(graded) > 0:
                correct_series = pd.to_numeric(graded["correct"], errors="coerce")
                if isinstance(correct_series, pd.Series):
                    current_winrate = float(correct_series.fillna(0).mean())
                else:
                    current_winrate = 0.0

                history = getattr(bot, "predictor_winrate_history", [])
                history.append(current_winrate)
                if len(history) > 20:
                    history = history[-20:]
                bot.predictor_winrate_history = history

                baseline = sum(history) / len(history) if history else current_winrate
                bot.predictor_baseline_winrate = baseline

                degraded = current_winrate < (baseline - 0.15)
                was_degraded = getattr(bot, "predictor_drift_state", False)
                now = datetime.now(pytz.timezone("America/New_York"))
                last_warn = getattr(bot, "last_predictor_drift_warning_time", None)
                allow_warn = (not was_degraded)
                if last_warn is None:
                    allow_warn = True
                elif (now - last_warn).total_seconds() >= 300:
                    allow_warn = True

                if degraded:
                    debug_log(
                        "predictor_drift_warning",
                        rolling_samples=len(graded),
                        current_winrate=round(current_winrate, 4),
                        baseline_winrate=round(baseline, 4),
                        degradation=round(baseline - current_winrate, 4)
                    )
                    if allow_warn and channel is not None:
                        color = 0xE74C3C
                        embed = discord.Embed(
                            title="⚠️ Predictor Drift Warning",
                            color=color
                        )
                        embed.add_field(name="📉 Rolling Winrate (last 100)", value=ab(wr_col(current_winrate)), inline=True)
                        embed.add_field(name="📊 Baseline Winrate", value=ab(wr_col(baseline)), inline=True)
                        embed.add_field(name="📉 Degradation", value=ab(A(f"{(baseline - current_winrate)*100:.1f}pp", "red", bold=True)), inline=True)
                        embed.add_field(name="⚡ Action Needed", value=ab(A("Model accuracy has dropped >15% vs baseline. Consider retraining with `!retrain`.", "yellow")), inline=False)
                        embed.set_footer(text=f"Samples: {len(graded)} | {_format_et(now)}")
                        await _send(channel, embed=embed)
                        bot.last_predictor_drift_warning_time = now
                    bot.predictor_drift_state = True
                else:
                    bot.predictor_drift_state = False

        await asyncio.sleep(300)


# =========================================================
# CONVICTION WATCHER (Detailed + Setup Intelligence)
# =========================================================
print("Conviction watcher started")

async def conviction_watcher(bot, alert_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(alert_channel_id)

    conviction_state = "LOW"
    drift = detect_feature_drift()

    if drift:
        severity = drift["severity"]
        features = "\n".join(drift["features"])

        await _send(channel, 
            f"⚠️ **Feature Drift Detected**\n\n"
            f"Severity: {severity}\n"
            f"{features}"
        )

    while not bot.is_closed():
        try:
            if not market_is_open():
                await asyncio.sleep(120)
                continue

            df = get_market_dataframe()
            if df is None:
                await asyncio.sleep(120)
                continue

            # ---------------------------------------
            # Update Forward Expectancy Tracking
            # ---------------------------------------
            score, impulse, follow, direction = calculate_conviction(df)
            log_conviction_signal(df, direction, impulse, follow)
            update_expectancy(df)
            update_blocked_outcomes(df)  # fill forward returns for blocked signals

            regime = get_regime(df)
            vol = volatility_state(df)

            # ---------------------------------------
            # Setup Expectancy Context
            # ---------------------------------------
            setup_stats = calculate_setup_expectancy()

            profitable_setups = []
            negative_setups = []

            if setup_stats:
                for setup_name, stats in setup_stats.items():

                    if stats["avg_R"] > 0.5:
                        profitable_setups.append(setup_name)

                    if stats["avg_R"] < 0:
                        negative_setups.append(setup_name)

            # ---------------------------------------
            # Tier Calculation
            # ---------------------------------------
            tier_score = score

            if vol == "HIGH":
                tier_score += 1

            if regime == "TREND":
                tier_score += 1

            if tier_score >= 6:
                tier = "HIGH"
                emoji = "🔥"
            elif tier_score >= 4:
                tier = "MEDIUM"
                emoji = "⚡"
            else:
                tier = "LOW"
                emoji = "🟡"

            # ---------------------------------------
            # Tier Change Alert
            # ---------------------------------------
            if tier != conviction_state and tier in ["MEDIUM", "HIGH"]:
                tier_color = 0xFF6B35 if tier == "HIGH" else 0xF39C12
                direction_color_bar = "🟢" if direction == "bullish" else "🔴" if direction == "bearish" else "⚪"
                conv_embed = discord.Embed(
                    title=f"{emoji} Conviction Upgrade — {tier}",
                    color=tier_color
                )
                conv_embed.add_field(name="📍 Direction", value=ab(dir_col(direction)), inline=True)
                conv_embed.add_field(name="🔢 Score", value=ab(A(str(score), "yellow", bold=True)), inline=True)
                conv_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                conv_embed.add_field(name="⚡ Impulse", value=ab(A(f"{impulse:.2f}×", "green" if impulse >= 1 else "yellow", bold=True)), inline=True)
                follow_color = "green" if follow >= 0.5 else "yellow" if follow >= 0.3 else "red"
                conv_embed.add_field(name="🔗 Follow-Through", value=ab(A(f"{follow*100:.0f}%", follow_color, bold=True)), inline=True)
                conv_embed.add_field(name="📊 Volatility", value=ab(vol_col(vol)), inline=True)
                if profitable_setups:
                    conv_embed.add_field(
                        name="✅ Profitable Setups",
                        value=ab(*[A(f"• {s}", "green") for s in profitable_setups]),
                        inline=False
                    )
                if negative_setups:
                    conv_embed.add_field(
                        name="⚠️ Negative Expectancy Setups",
                        value=ab(*[A(f"• {s}", "red") for s in negative_setups]),
                        inline=False
                    )
                conv_embed.set_footer(text=f"Previous: {conviction_state} → {tier} | {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=conv_embed)

                conviction_state = tier

            # ---------------------------------------
            # Momentum Decay Detection
            # ---------------------------------------
            decay_detected = momentum_is_decaying(df)
            current_tier = tier
            md_state = evaluate_md_auto(decay_detected, current_tier)
            if current_tier in ["MEDIUM", "HIGH"] and decay_detected:
                md_state = record_md_decay(level=current_tier)
                md_enabled = bool(md_state.get("enabled"))
                md_mode = str(md_state.get("mode", "manual")).upper()
                md_auto_level = str(md_state.get("auto_level", "medium")).upper()
                decay_embed = discord.Embed(
                    title="⚠️ Momentum Decay Detected",
                    description="Impulse is weakening while conviction was elevated. Risk management action recommended.",
                    color=0xE67E22
                )
                decay_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                decay_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vol)), inline=True)
                decay_embed.add_field(name="📊 Conviction Level", value=ab(tier_col(current_tier)), inline=True)
                md_text = A("ON", "green", bold=True) if md_enabled else A("OFF", "red", bold=True)
                if md_mode == "AUTO":
                    md_hint = A(f"AUTO {md_auto_level} mode", "yellow")
                else:
                    md_hint = A("Use `!md enable` to tighten stops.", "yellow") if not md_enabled else A("MD strict mode is active.", "green")
                decay_embed.add_field(name="🧰 MD Strict", value=ab(f"{md_text}  {md_hint}"), inline=False)
                decay_embed.add_field(name="💡 Suggested Action", value=ab(A("Tighten stops, reduce size, or stand aside until impulse recovers.", "yellow")), inline=False)
                decay_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                mention = f"<@{DISCORD_OWNER_ID}>" if DISCORD_OWNER_ID else None
                await _send(channel, mention, embed=decay_embed)

                conviction_state = "LOW"

            # ---------------------------------------
            # MD Turn-Off Suggestion (conditions cleared)
            # ---------------------------------------
            elif md_needs_warning():
                global _MD_TURNOFF_SUGGESTED_DATE
                today = datetime.now(pytz.timezone("America/New_York")).date()
                if _MD_TURNOFF_SUGGESTED_DATE != today:
                    _MD_TURNOFF_SUGGESTED_DATE = today
                    clear_embed = discord.Embed(
                        title="✅ Momentum Conditions Cleared",
                        description="MD strict mode is ON but no decay has been detected recently. Conditions look healthy.",
                        color=0x2ECC71,
                    )
                    clear_embed.add_field(name="💡 Suggested Action", value=ab(A("Use `!md disable` to restore normal trade filtering.", "green")), inline=False)
                    clear_embed.set_footer(text=f"{_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                    mention = f"<@{DISCORD_OWNER_ID}>" if DISCORD_OWNER_ID else None
                    await _send(channel, mention, embed=clear_embed)
        except Exception:
            logging.exception("conviction_watcher_error")
        await asyncio.sleep(120)


# =========================================================
# FORECAST WATCHER (Detailed + Clean Logging)
# =========================================================
print("Forecast watcher started")
async def forecast_watcher(bot, forecast_channel_id):

    await bot.wait_until_ready()
    channel = bot.get_channel(forecast_channel_id)

    last_logged_slot = None

    while not bot.is_closed():

        try:
            if not market_is_open():
                await asyncio.sleep(60)
                continue

            eastern = pytz.timezone("US/Eastern")
            now = datetime.now(eastern)

            slot_minute = 0 if now.minute < 30 else 30
            slot_time = now.replace(minute=slot_minute, second=0, microsecond=0)
            if last_logged_slot is None or slot_time > last_logged_slot:

                df = get_market_dataframe()
                if df is None:
                    await asyncio.sleep(60)
                    continue

                try:
                    pred = make_prediction(30, df)
                except Exception:
                    logging.exception("forecast_prediction_error")
                    await asyncio.sleep(60)
                    continue

                if pred is None:
                    logging.warning("forecast_prediction_none")
                    await asyncio.sleep(60)
                    continue

                regime = get_regime(df)
                vola = volatility_state(df)

                log_prediction(pred, regime, vola)
                try:
                    logging.info(
                        "prediction_logged",
                        extra={
                            "time": pred.get("time"),
                            "direction": pred.get("direction"),
                            "confidence": pred.get("confidence"),
                            "tf": pred.get("timeframe"),
                            "slot_time": slot_time.isoformat(),
                        },
                    )
                except Exception:
                    pass
                last_logged_slot = slot_time

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

                direction = pred["direction"]
                conf = pred["confidence"]
                if direction == "bullish":
                    fcast_color = 0x2ECC71
                    dir_emoji = "🟢"
                elif direction == "bearish":
                    fcast_color = 0xE74C3C
                    dir_emoji = "🔴"
                else:
                    fcast_color = 0x95A5A6
                    dir_emoji = "⚪"
                # Confidence tier
                if conf >= 0.65:
                    conf_label = "🔥 High"
                elif conf >= 0.52:
                    conf_label = "⚡ Medium"
                else:
                    conf_label = "🟡 Low"

                def _safe_price(val):
                    try:
                        return f"{float(val):.2f}"
                    except (TypeError, ValueError):
                        return "N/A"

                fcast_embed = discord.Embed(
                    title=f"📊 30-Minute Forecast — {dir_emoji} {direction.upper()}",
                    color=fcast_color
                )
                fcast_embed.add_field(name="📍 Direction", value=ab(dir_col(direction)), inline=True)
                fcast_embed.add_field(name="💡 Confidence", value=ab(conf_col(conf)), inline=True)
                fcast_embed.add_field(name="🧭 Regime", value=ab(regime_col(regime)), inline=True)
                fcast_embed.add_field(name="⚡ Volatility", value=ab(vol_col(vola)), inline=True)
                _ph = f"${_safe_price(pred['high'])}"
                _pl = f"${_safe_price(pred['low'])}"
                _pc = f"${_safe_price(last.get('close'))}"
                _pv = f"${_safe_price(last.get('vwap'))}"
                _pe9 = f"${_safe_price(last.get('ema9'))}"
                _pe20 = f"${_safe_price(last.get('ema20'))}"
                _psh = f"${_safe_price(high_price)}"
                _psl = f"${_safe_price(low_price)}"
                fcast_embed.add_field(name="🎯 Predicted High", value=ab(A(_ph, "green", bold=True)), inline=True)
                fcast_embed.add_field(name="🎯 Predicted Low", value=ab(A(_pl, "red", bold=True)), inline=True)
                fcast_embed.add_field(
                    name="📍 Market Snapshot",
                    value=ab(
                        f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                        f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                        f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
                    ),
                    inline=False
                )
                fcast_embed.add_field(
                    name="📈 Session Range",
                    value=ab(
                        f"{lbl('High')} {A(_psh, 'green')} @ {A(high_time_str, 'white')}",
                        f"{lbl('Low')}  {A(_psl, 'red')} @ {A(low_time_str, 'white')}",
                    ),
                    inline=False
                )
                fcast_embed.set_footer(text=f"Forecast logged | {_format_et(datetime.now(pytz.timezone('America/New_York')))}")
                await _send(channel, embed=fcast_embed)

                last_logged_slot = slot_time

            await asyncio.sleep(20)
        except Exception as _fw_exc:
            logging.exception("forecast_watcher_error")
            slog_critical("forecast_watcher_error", error=str(_fw_exc))
            await asyncio.sleep(60)


# =========================================================
# HEART MONITOR
# =========================================================

import discord
from datetime import datetime
import pytz
import os
import time

# Track uptime
START_TIME = time.time()


# Health Monitor with Embed
async def heart_monitor(bot, channel_id):
    await bot.wait_until_ready()
    channel = bot.get_channel(channel_id)
    last_health_emit = None
    last_reconcile_time = None

    while not bot.is_closed():
        try:
            from core.account_repository import load_account
            from core.market_clock import market_is_open
            from interface.health_monitor import check_health
            import logs.recorder as recorder_module

            eastern = pytz.timezone("US/Eastern")
            now = datetime.now(eastern)

            if hasattr(bot, "recorder_thread"):
                if not bot.recorder_thread.is_alive():
                    debug_log("recorder_thread_dead")
                    last_warn = getattr(bot, "last_recorder_thread_dead_warning_time", None)
                    if last_warn is None or (now - last_warn).total_seconds() >= 300:
                        await _send(channel, 
                            "⚠️ **recorder_thread_dead**\n\n"
                            "Recorder background thread is not alive."
                        )
                        bot.last_recorder_thread_dead_warning_time = now

            # Recorder stall monitoring (market-open only).
            if market_is_open():
                last_saved = getattr(recorder_module, "last_saved_timestamp", None)
                if last_saved:
                    try:
                        last_dt = datetime.strptime(last_saved, "%Y-%m-%d %H:%M:%S")
                        last_dt = eastern.localize(last_dt)
                        age_seconds = (now - last_dt).total_seconds()

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
                            elif (now - last_warn).total_seconds() >= 300:
                                allow_warn = True

                            if allow_warn:
                                await _send(channel, 
                                    "⚠️ **recorder_stalled_warning**\n\n"
                                    f"Last saved candle: {last_saved} ET\n"
                                    f"Age: {age_seconds:.0f}s\n"
                                    "Recorder may not be appending new candles."
                                )
                                bot.last_recorder_stall_warning_time = now
                            bot.recorder_stalled_state = True
                        else:
                            bot.recorder_stalled_state = False
                    except Exception:
                        pass

            acc = load_account()

            if last_reconcile_time is None or (now - last_reconcile_time).total_seconds() >= 60:
                api_key = os.getenv("APCA_API_KEY_ID")
                secret_key = os.getenv("APCA_API_SECRET_KEY")
                if api_key and secret_key:
                    from alpaca.trading.client import TradingClient
                    client = TradingClient(api_key, secret_key, paper=True)
                    positions = client.get_all_positions()

                    broker_by_symbol = {}
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
                            internal=sorted(str(s) for s in internal_symbols)
                        )
                        last_mismatch = getattr(bot, "broker_mismatch_last_time", None)
                        if last_mismatch is None or (now - last_mismatch).total_seconds() >= 300:
                            await _send(channel, 
                                "⚠️ **Broker State Mismatch**\n\n"
                                "Broker positions do not match internal open trades."
                            )
                            bot.broker_mismatch_last_time = now

                    orphan_symbols = broker_symbols.difference(internal_symbols)
                    if orphan_symbols:
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
                                debug_log(
                                    "orphan_position_close_failed",
                                    symbol=symbol,
                                    qty=qty_val
                                )
                                continue
                            filled_avg = close_result.get("filled_avg_price")
                            exit_price = None
                            if filled_avg is not None:
                                exit_price = filled_avg
                            else:
                                exit_price = get_option_price(symbol)
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
                            _finalize_reconstructed_trade(
                                acc, trade, pnl, "orphan_broker_close"
                            )

                            debug_log(
                                "orphan_position_closed",
                                symbol=symbol,
                                qty=qty_val
                            )
                            last_times = getattr(bot, "orphan_close_last_time", {})
                            last = last_times.get(symbol)
                            if last is None or (now - last).total_seconds() >= 300:
                                await _send(channel, 
                                    f"⚠️ **Orphan Position Closed**\n\n"
                                    f"{symbol} (qty: {qty_val}) was not tracked internally."
                                )
                                last_times[symbol] = now
                                bot.orphan_close_last_time = last_times
                last_reconcile_time = now

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
                    if last is not None and (now - last).total_seconds() < 300:
                        continue
                    ntype = notice.get("type")
                    qty = notice.get("qty")
                    entry = notice.get("entry")
                    price = notice.get("price")
                    if ntype == "emergency_stop_success":
                        await _send(channel, 
                            f"🛑 Reconstructed position emergency-stopped: {symbol} "
                            f"qty={qty} entry={entry} price={price}"
                        )
                    elif ntype == "emergency_stop_failure":
                        await _send(channel, 
                            f"⚠️ Reconstructed emergency stop failed: {symbol} qty={qty}"
                        )
                    else:
                        continue
                    last_times[symbol] = now
                    t.pop("recon_notice", None)
                    updated = True
                bot.recon_notice_last_time = last_times
                if updated:
                    acc["open_trades"] = open_trades
                    save_account(acc)

            if last_health_emit is None or (now - last_health_emit).total_seconds() >= 1800:
                status, report = check_health()
                trades = acc.get("trade_log", [])
                balance = acc.get("balance", 0)
                risk_mode = acc.get("risk_mode", "NORMAL")

                market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

                # Uptime calculation
                uptime_seconds = int(time.time() - START_TIME)
                hours = uptime_seconds // 3600
                minutes = (uptime_seconds % 3600) // 60

                # Last trade
                last_trade = "None"
                if trades:
                    last_trade = trades[-1].get("exit_time", "Unknown")

                # Color coding
                if status == "HEALTHY":
                    color = discord.Color.green()
                    status_icon = "🟢"
                else:
                    color = discord.Color.red()
                    status_icon = "🔴"

                embed = discord.Embed(
                    title="🧠 SPY AI System Health",
                    color=color
                )

                embed.add_field(name="🧠 System Status", value=f"{status_icon} {status}", inline=True)
                embed.add_field(name="🟢 Market", value=market_status, inline=True)
                embed.add_field(name="🧰 Risk Mode", value=risk_mode, inline=True)

                embed.add_field(name="📦 Total Trades", value=str(len(trades)), inline=True)

                embed.add_field(
                    name="⏱ Uptime",
                    value=f"{hours}h {minutes}m",
                    inline=True
                )

                embed.add_field(
                    name="🧪 Health Report",
                    value=report if report else "All subsystems responding.",
                    inline=False
                )

                embed.set_footer(text=f"System Time: {_format_et(now)}")

                await _send(channel, embed=embed)
                last_health_emit = now

        except Exception as e:
            await _send(channel, "⚠️ Health monitor encountered an error.")
            print("Health monitor error:", e)

        # Write heartbeat so crash recovery can detect unclean shutdowns
        write_heartbeat()
        await asyncio.sleep(60)
