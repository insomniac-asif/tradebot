# simulation/sim_watcher.py
import asyncio
import re
import logging
import os
import yaml
import pytz
import discord
from datetime import datetime, time
from core.data_service import get_market_dataframe
from simulation.sim_portfolio import SimPortfolio
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
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)

ENTRY_INTERVAL_SECONDS = 60
EXIT_INTERVAL_SECONDS = 30
EOD_REPORT_TIME_ET = time(16, 0)
DAILY_SUMMARY_TIME_ET = time(16, 5)
WEEKLY_LEADERBOARD_TIME_ET = time(16, 10)
SESSION_LEADERBOARD_SIMS_PER_PAGE = 4

SIM_CHANNEL_MAP = {
    "SIM00": 1477023293545648258,  # Live version of SIM03
    "SIM01": 1476794016019386460,
    "SIM02": 1478257480952975525,
    "SIM03": 1476794039067218102,
    "SIM04": 1478257645696843939,
    "SIM05": 1476794065793323120,
    "SIM06": 1478257843894485145,
    "SIM07": 1476794956826935339,
    "SIM08": 1476795166751854654,
    "SIM09": 1477017498451705968,
    "SIM10": 1478200317035417641,
    "SIM11": 1478200298466971679,
    "SIM12": 1480410929589129266,
    "SIM13": 1480410951143522325,
    "SIM14": 1480410977857175703,
    "SIM15": 1480411008928579595,
    "SIM16": 1480411067212632124,
    "SIM17": 1480411143234392154,
    "SIM18": 1480425205384871977,
    "SIM19": 1480425237639201000,
    "SIM20": 1480425269729955930,
    "SIM21": 1480425299048140922,
    "SIM22": 1480425327959474226,
    "SIM23": 1480425354840641569,
    "SIM24": 1480709591913201675,
    "SIM25": 1480709609881866454,
    "SIM26": 1480709627170525285,
    "SIM27": 1480709642735718440,
    "SIM28": 1480709656660672648,
}

_SIM_BOT = None
_SIM_LAST_SKIP_REASON = {}
_SIM_LAST_SKIP_TIME = {}
_SIM_LAST_DATA_AGE = None
_SIM_EOD_REPORT_DATE = None
_SIM_PREOPEN_REPORT_DATE = None
_SIM_DAILY_SUMMARY_DATE = None
_SIM_WEEKLY_LEADERBOARD_DATE = None
_SIM_TRADE_PROGRESS_DATE: dict = {}   # sim_id -> date of last morning progress post
_SIM_TRADE_HISTORY_UNLOCKED: set = set()  # sims announced as ready to go live
_SIM_SESSION_LEADERBOARD_DATE = None  # last date the session leaderboard was posted

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            raw = yaml.safe_load(f) or {}
        return {k: v for k, v in raw.items() if str(k).upper().startswith("SIM") and isinstance(v, dict)}
    except Exception:
        return {}


_SIM_PROFILES = _load_profiles()


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


def set_sim_bot(bot) -> None:
    global _SIM_BOT
    _SIM_BOT = bot


def get_sim_last_skip_state() -> dict:
    state = {}
    for sim_id, reason in _SIM_LAST_SKIP_REASON.items():
        state[sim_id] = {
            "reason": reason,
            "time": _SIM_LAST_SKIP_TIME.get(sim_id),
        }
    return state


async def _post_sim_event(sim_id: str, embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = SIM_CHANNEL_MAP.get(sim_id)
    if channel_id is None:
        logging.warning("post_sim_event_no_channel: sim=%s", sim_id)
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        logging.warning("post_sim_event_channel_not_found: sim=%s channel_id=%s", sim_id, channel_id)
        return
    try:
        await channel.send(embed=embed)
    except Exception:
        logging.exception("post_sim_event_send_error: sim=%s", sim_id)
        return


async def _post_main_skip(embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = getattr(_SIM_BOT, "paper_channel_id", None)
    if channel_id is None:
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        return
    try:
        main_embed = discord.Embed(
            title=embed.title,
            description=embed.description,
            color=embed.color.value if embed.color else 0xF1C40F
        )
        for field in embed.fields:
            main_embed.add_field(name=field.name, value=field.value, inline=field.inline)
        if embed.footer and embed.footer.text:
            main_embed.set_footer(text=embed.footer.text)
        await channel.send(embed=main_embed)
    except Exception:
        return


async def _post_main_embed(embed: "discord.Embed") -> None:
    if _SIM_BOT is None:
        return
    channel_id = getattr(_SIM_BOT, "paper_channel_id", None)
    if channel_id is None:
        return
    channel = _SIM_BOT.get_channel(channel_id)
    if channel is None:
        return
    try:
        await channel.send(embed=embed)
    except Exception:
        return


async def sim_eod_report_loop(channel_id: int) -> None:
    """
    End-of-day report for non-daytrading sims:
    show any open positions that will carry overnight.
    """
    global _SIM_EOD_REPORT_DATE, _SIM_PREOPEN_REPORT_DATE
    eastern = pytz.timezone("US/Eastern")
    while True:
        try:
            now_et = _now_et()
            # Weekday only
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue

            # Include same open positions snapshot in the pre-open window
            preopen_window = time(9, 10) <= now_et.time() <= time(9, 40)
            if not preopen_window and now_et.time() < EOD_REPORT_TIME_ET:
                await asyncio.sleep(30)
                continue
            if preopen_window:
                if _SIM_PREOPEN_REPORT_DATE == now_et.date():
                    await asyncio.sleep(300)
                    continue
            else:
                if _SIM_EOD_REPORT_DATE == now_et.date():
                    await asyncio.sleep(300)
                    continue

            lines_by_sim = {}
            pnl_by_sim = {}
            for sim_id, profile in _SIM_PROFILES.items():
                try:
                    # Non-daytrading sims: dte_max > 0
                    if int(profile.get("dte_max", 0)) == 0:
                        continue
                    sim = SimPortfolio(sim_id, profile)
                    sim.load()
                    if not sim.open_trades:
                        continue
                    lines = []
                    sim_pnl = 0.0
                    for t in sim.open_trades:
                        symbol = t.get("option_symbol", "unknown")
                        qty = t.get("qty")
                        entry_price = t.get("entry_price")
                        entry_time = t.get("entry_time")
                        entry_text = f"${entry_price:.2f}" if isinstance(entry_price, (int, float)) else "N/A"
                        notional_text = (
                            f"${entry_price * float(qty) * 100:.0f}"
                            if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float))
                            else "N/A"
                        )
                        pnl_text = "N/A"
                        try:
                            from execution.option_executor import get_option_price
                            current_price = get_option_price(symbol)
                            if current_price is not None and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
                                pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                                pnl_text = f"{'+' if pnl_val >= 0 else ''}${pnl_val:.2f}"
                                sim_pnl += pnl_val
                        except Exception:
                            pnl_text = "N/A"
                        readable_symbol = _format_option_symbol(symbol)
                        readable_time = _format_entry_time(entry_time)
                        lines.append(f"{readable_symbol} | qty {qty} | entry {entry_text} ({notional_text}) | pnl {pnl_text} | @{readable_time}")
                    if lines:
                        lines_by_sim[sim_id] = lines
                        pnl_by_sim[sim_id] = sim_pnl
                except Exception:
                    continue

            # Per-sim posts to each sim's channel (only if it has open trades)
            for sim_id, lines in sorted(lines_by_sim.items()):
                session_pnl = pnl_by_sim.get(sim_id, 0.0)
                if preopen_window:
                    sim_desc = "Pre-open snapshot of non-daytrade open positions."
                else:
                    sim_desc = "Open non-daytrade positions at market close."
                sim_embed = discord.Embed(
                    title=f"📌 {sim_id} Open Positions",
                    description=ab(A(sim_desc, "yellow", bold=True)),
                    color=0xF39C12,
                )
                sim_embed.add_field(name="Session PnL", value=ab(pnl_col(session_pnl)), inline=True)
                sim_embed.add_field(
                    name="Positions",
                    value=ab(*[A(line, "white") for line in lines]),
                    inline=False
                )
                footer_parts = [f"Time: {_format_et(now_et)}"]
                if _SIM_LAST_DATA_AGE:
                    footer_parts.append(_SIM_LAST_DATA_AGE)
                sim_embed.set_footer(text=" | ".join(footer_parts))
                await _post_sim_event(sim_id, sim_embed)

            if not lines_by_sim:
                embed = discord.Embed(
                    title="📌 Open Positions Snapshot",
                    description=ab(A("No open non-daytrade SIM positions.", "green", bold=True)),
                    color=0x2ECC71,
                )
            else:
                embed = discord.Embed(
                    title="📌 Open Positions Snapshot",
                    description=ab(A("Non-daytrade SIM positions carrying overnight.", "yellow", bold=True)),
                    color=0xF39C12,
                )
                for sim_id, lines in sorted(lines_by_sim.items()):
                    session_pnl = pnl_by_sim.get(sim_id, 0.0)
                    embed.add_field(
                        name=f"{sim_id} | Session PnL {session_pnl:+.2f}",
                        value=ab(*[A(line, "white") for line in lines]),
                        inline=False
                    )

            footer_parts = [f"Time: {_format_et(now_et)}"]
            if _SIM_LAST_DATA_AGE:
                footer_parts.append(_SIM_LAST_DATA_AGE)
            embed.set_footer(text=" | ".join(footer_parts))
            if _SIM_BOT is not None and channel_id:
                channel = _SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            # Post graduation progress for live sims waiting on source sim trade count
            for _grad_sim_id, _grad_profile in sorted(_SIM_PROFILES.items()):
                try:
                    if _grad_profile.get("execution_mode") != "live":
                        continue
                    _grad_source = _grad_profile.get("source_sim")
                    _grad_min = _grad_profile.get("min_source_trades", 0)
                    if not _grad_source or not _grad_min:
                        continue
                    if _grad_sim_id in _SIM_TRADE_HISTORY_UNLOCKED:
                        continue
                    _src_profile = _SIM_PROFILES.get(_grad_source, {})
                    _src_sim = SimPortfolio(_grad_source, _src_profile)
                    _src_sim.load()
                    _src_count = len(_src_sim.trade_log) if isinstance(_src_sim.trade_log, list) else 0
                    _pct = min(_src_count / _grad_min * 100, 100)
                    _filled = int(_pct / 10)
                    _bar = "█" * _filled + "░" * (10 - _filled)
                    _prog_embed = discord.Embed(
                        title=f"📊 {_grad_sim_id} — Trade History Progress",
                        description=ab(A("EOD snapshot: trades logged toward live graduation.", "yellow")),
                        color=0xF39C12,
                    )
                    _prog_embed.add_field(name="Source Sim", value=ab(A(_grad_source, "cyan", bold=True)), inline=True)
                    _prog_embed.add_field(
                        name="Progress",
                        value=ab(A(f"{_src_count} / {_grad_min}  [{_bar}]  {_pct:.0f}%", "yellow", bold=True)),
                        inline=False,
                    )
                    _prog_footer = [f"Time: {_format_et(now_et)}"]
                    if _SIM_LAST_DATA_AGE:
                        _prog_footer.append(_SIM_LAST_DATA_AGE)
                    _prog_embed.set_footer(text=" | ".join(_prog_footer))
                    await _post_sim_event(_grad_sim_id, _prog_embed)
                except Exception:
                    pass

            if preopen_window:
                _SIM_PREOPEN_REPORT_DATE = now_et.date()
            else:
                _SIM_EOD_REPORT_DATE = now_et.date()
        except Exception:
            logging.exception("sim_eod_report_error")
        await asyncio.sleep(60)


async def sim_daily_summary_loop(channel_id: int) -> None:
    """Post daily P&L summary for all sims after market close."""
    global _SIM_DAILY_SUMMARY_DATE
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < DAILY_SUMMARY_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _SIM_DAILY_SUMMARY_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            today_str = now_et.date().isoformat()
            combined_lines = []
            total_day_pnl = 0.0
            total_day_trades = 0

            for sim_id, profile in _SIM_PROFILES.items():
                if str(sim_id).startswith("_"):
                    continue
                try:
                    sim = SimPortfolio(sim_id, profile)
                    sim.load()
                    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []

                    today_trades = []
                    for t in trade_log:
                        exit_time = t.get("exit_time")
                        if not exit_time:
                            continue
                        try:
                            exit_date = datetime.fromisoformat(str(exit_time)).date().isoformat()
                        except Exception:
                            continue
                        if exit_date == today_str:
                            today_trades.append(t)

                    if not today_trades:
                        continue

                    wins = 0
                    losses = 0
                    day_pnl = 0.0
                    for t in today_trades:
                        try:
                            pnl = float(t.get("realized_pnl_dollars", 0))
                        except (TypeError, ValueError):
                            continue
                        day_pnl += pnl
                        if pnl > 0:
                            wins += 1
                        else:
                            losses += 1

                    total_day_pnl += day_pnl
                    total_day_trades += len(today_trades)
                    wr = wins / len(today_trades) if today_trades else 0
                    mode = "LIVE" if profile.get("execution_mode") == "live" and profile.get("enabled") else "SIM"

                    sim_embed = discord.Embed(
                        title=f"📊 {sim_id} Daily Summary",
                        color=0x2ECC71 if day_pnl >= 0 else 0xE74C3C,
                    )
                    sim_embed.add_field(name="Trades", value=ab(A(str(len(today_trades)), "white", bold=True)), inline=True)
                    sim_embed.add_field(name="W / L", value=ab(A(f"{wins}W", "green"), A(" / ", "gray"), A(f"{losses}L", "red")), inline=True)
                    sim_embed.add_field(name="Win Rate", value=ab(wr_col(wr)), inline=True)
                    sim_embed.add_field(name="Day PnL", value=ab(pnl_col(day_pnl)), inline=True)
                    sim_embed.add_field(name="Balance", value=ab(balance_col(sim.balance)), inline=True)
                    dd = sim.peak_balance - sim.balance if sim.peak_balance > sim.balance else 0
                    dd_pct = dd / sim.peak_balance if sim.peak_balance > 0 else 0
                    sim_embed.add_field(name="Drawdown", value=ab(drawdown_col(dd_pct)), inline=True)
                    sim_embed.set_footer(text=f"{today_str} | {mode}")
                    await _post_sim_event(sim_id, sim_embed)

                    combined_lines.append({
                        "sim_id": sim_id,
                        "mode": mode,
                        "trades": len(today_trades),
                        "wins": wins,
                        "losses": losses,
                        "wr": wr,
                        "day_pnl": day_pnl,
                        "balance": sim.balance,
                    })

                except Exception:
                    logging.exception(f"sim_daily_summary_error: {sim_id}")
                    continue

            if _SIM_BOT is not None and channel_id:
                channel = _SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    color = 0x2ECC71 if total_day_pnl >= 0 else 0xE74C3C
                    combined_embed = discord.Embed(
                        title=f"📊 Daily Summary — {today_str}",
                        color=color,
                    )
                    combined_embed.add_field(
                        name="Totals",
                        value=ab(
                            f"{lbl('Trades')} {A(str(total_day_trades), 'white', bold=True)}  |  "
                            f"{lbl('Combined PnL')} {pnl_col(total_day_pnl)}"
                        ),
                        inline=False,
                    )
                    combined_lines.sort(key=lambda x: x["day_pnl"], reverse=True)
                    for item in combined_lines:
                        mode_tag = " 🔴" if item["mode"] == "LIVE" else ""
                        combined_embed.add_field(
                            name=f"{item['sim_id']}{mode_tag}",
                            value=ab(
                                f"{lbl('PnL')} {pnl_col(item['day_pnl'])}  |  "
                                f"{lbl('W/L')} {A(str(item['wins']), 'green')}/{A(str(item['losses']), 'red')}  |  "
                                f"{lbl('WR')} {wr_col(item['wr'])}  |  "
                                f"{lbl('Bal')} {balance_col(item['balance'])}"
                            ),
                            inline=False,
                        )
                    combined_embed.set_footer(text=f"{_format_et(now_et)}")
                    await channel.send(embed=combined_embed)

            _SIM_DAILY_SUMMARY_DATE = now_et.date()
        except Exception:
            logging.exception("sim_daily_summary_loop_error")
        await asyncio.sleep(60)

def _build_entry_embed(sim_id: str, result: dict) -> "discord.Embed":
    status = result.get("status", "opened")
    live_flag = "LIVE" if status == "live_submitted" else "SIM"
    title = f"📥 {sim_id} {live_flag} Entry"
    embed = discord.Embed(title=title, color=0x2ecc71)
    option_symbol = result.get("option_symbol") or "unknown"
    expiry = result.get("expiry")
    direction = result.get("direction") or "N/A"
    raw_strike = result.get("strike")
    strike = None
    if isinstance(raw_strike, (int, float)):
        strike = float(raw_strike)
    elif isinstance(raw_strike, str):
        try:
            strike = float(raw_strike)
        except (TypeError, ValueError):
            strike = None
    if strike is None:
        strike = _parse_strike_from_symbol(option_symbol)
    call_put = "CALL" if str(direction).upper() == "BULLISH" else "PUT" if str(direction).upper() == "BEARISH" else None
    expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
    contract_label = (result.get("symbol") or "SPY").upper()
    if call_put:
        contract_label = f"{contract_label} {call_put}"
    if expiry_text:
        contract_label = f"{contract_label} {expiry_text}"
    if isinstance(strike, (int, float)):
        contract_label = f"{contract_label} {strike:g}"
    qty = result.get("qty")
    fill_price = result.get("fill_price")
    fill_text = f"${fill_price:.4f}" if isinstance(fill_price, (int, float)) else "N/A"
    direction = result.get("direction") or "N/A"
    mode = result.get("mode") or ("LIVE" if status == "live_submitted" else "SIM")
    contract_lines = [A(contract_label, "magenta", bold=True)]
    if option_symbol:
        contract_lines.append(A(option_symbol, "white"))
    embed.add_field(name="Contract", value=ab(*contract_lines), inline=False)
    embed.add_field(
        name="Order",
        value=ab(
            f"{lbl('Qty')} {A(qty if qty is not None else 'N/A', 'white', bold=True)}  |  "
            f"{lbl('Fill')} {A(fill_text, 'white', bold=True)}  |  "
            f"{lbl('Dir')} {dir_col(direction)}"
        ),
        inline=False,
    )
    embed.add_field(name="Mode", value=ab(A(f"{mode}", "cyan", bold=True)), inline=False)
    entry_price = result.get("entry_price")
    entry_text = f"${entry_price:.4f}" if isinstance(entry_price, (int, float)) else "N/A"
    notional_text = "N/A"
    try:
        if isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
            notional_text = f"${(float(entry_price) * float(qty) * 100):,.2f}"
    except Exception:
        notional_text = "N/A"
    risk_dollars = result.get("risk_dollars")
    risk_text = f"${risk_dollars:.0f}" if isinstance(risk_dollars, (int, float)) else "N/A"
    balance_val = result.get("balance")
    balance_text = f"${balance_val:,.2f}" if isinstance(balance_val, (int, float)) else "N/A"
    embed.add_field(
        name="Risk / Balance",
        value=ab(
            f"{lbl('Entry')} {A(entry_text, 'white', bold=True)}  |  "
            f"{lbl('Notional')} {A(notional_text, 'white', bold=True)}  |  "
            f"{lbl('Risk')} {A(risk_text, 'yellow', bold=True)}  |  "
            f"{lbl('Bal')} {balance_col(balance_val)}"
        ),
        inline=False
    )
    strike = result.get("strike")
    expiry = result.get("expiry")
    dte = result.get("dte")
    spread = result.get("spread_pct")
    regime = result.get("regime") or "N/A"
    time_bucket = result.get("time_bucket") or "N/A"
    details = []
    if strike is not None and expiry is not None:
        details.append(f"{strike} {expiry}")
    if dte is not None:
        details.append(f"DTE {dte}")
    if isinstance(spread, (int, float)):
        details.append(f"spr {spread:.3f}")
    detail_text = " | ".join(details) if details else "details N/A"
    embed.add_field(name="Details", value=ab(A(detail_text, "cyan")), inline=False)
    embed.add_field(name="Context", value=ab(f"{lbl('Regime')} {regime_col(regime)}  |  {lbl('Time')} {A(time_bucket, 'cyan')}"), inline=False)
    feature_snapshot = result.get("feature_snapshot")
    fs_text = _format_feature_snapshot(feature_snapshot)
    if fs_text:
        embed.add_field(name="Feature Snapshot", value=ab(fs_text), inline=False)
    entry_context = result.get("entry_context")
    signal_mode = result.get("signal_mode")
    if entry_context or signal_mode:
        ctx_lines = []
        if signal_mode:
            ctx_lines.append(f"{lbl('Signal')} {A(signal_mode, 'magenta', bold=True)}")
        if entry_context:
            parts = _format_context_parts(entry_context, drop_keys={"signal: ", "signal_mode"})
            if parts:
                ctx_lines.extend([A(p, "cyan") for p in parts])
        embed.add_field(name="Entry Context", value=ab(*ctx_lines), inline=False)
    pred_dir = result.get("predicted_direction") or "N/A"
    conf_val = result.get("prediction_confidence")
    edge_val = result.get("edge_prob")
    conf_text = f"{conf_val:.2f}" if isinstance(conf_val, (int, float)) else "N/A"
    edge_text = f"{edge_val:.2f}" if isinstance(edge_val, (int, float)) else "N/A"
    embed.add_field(
        name="ML",
        value=ab(
            f"{lbl('Pred')} {dir_col(pred_dir)}  |  "
            f"{lbl('Conf')} {conf_col(conf_val) if isinstance(conf_val, (int, float)) else A('N/A','gray')}  |  "
            f"{lbl('Edge')} {pct_col(edge_val, good_when_high=True, multiply=True) if isinstance(edge_val, (int, float)) else A('N/A','gray')}"
        ),
        inline=False
    )
    _und_sym = (result.get("symbol") or "SPY").upper()
    try:
        from core.data_service import get_symbol_dataframe
        _und_df = get_symbol_dataframe(_und_sym)
        _und_close = _und_df.iloc[-1].get("close") if _und_df is not None and len(_und_df) else None
        _und_price = float(_und_close) if _und_close is not None else None
    except Exception:
        _und_price = None
    if isinstance(_und_price, (int, float)):
        embed.add_field(name=f"{_und_sym} Price", value=ab(A(f"${_und_price:.2f}", "white", bold=True)), inline=True)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


def _build_exit_embed(sim_id: str, result: dict) -> "discord.Embed":
    pnl_val = result.get("pnl")
    mode = result.get("mode") or "SIM"
    live_flag = " LIVE" if mode == "LIVE" else ""
    badge = "🟡"
    color = 0xF39C12
    if isinstance(pnl_val, (int, float)):
        if pnl_val > 0:
            badge = "✅"
            color = 0x2ECC71
        elif pnl_val < 0:
            badge = "❌"
            color = 0xE74C3C
        else:
            badge = "⚪"
            color = 0x95A5A6
    embed = discord.Embed(title=f"{badge} {sim_id}{live_flag} Exit", color=color)
    option_symbol = result.get("option_symbol") or "unknown"
    expiry = result.get("expiry")
    direction = result.get("direction") or "N/A"
    strike = result.get("strike") or _parse_strike_from_symbol(option_symbol)
    call_put = "CALL" if str(direction).upper() == "BULLISH" else "PUT" if str(direction).upper() == "BEARISH" else None
    expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
    contract_label = (result.get("symbol") or "SPY").upper()
    if call_put:
        contract_label = f"{contract_label} {call_put}"
    if expiry_text:
        contract_label = f"{contract_label} {expiry_text}"
    if isinstance(strike, (int, float)):
        contract_label = f"{contract_label} {strike:g}"
    qty = result.get("qty")
    exit_price = result.get("exit_price")
    exit_reason = result.get("exit_reason", "unknown")
    entry_price = result.get("entry_price")
    pnl_val = result.get("pnl")
    balance_after = result.get("balance_after")
    hold_seconds = result.get("time_in_trade_seconds")
    exit_text = f"${exit_price:.4f}" if isinstance(exit_price, (int, float)) else "N/A"
    entry_text = f"${entry_price:.4f}" if isinstance(entry_price, (int, float)) else "N/A"
    pnl_text = f"${pnl_val:.2f}" if isinstance(pnl_val, (int, float)) else "N/A"
    balance_text = f"${balance_after:,.2f}" if isinstance(balance_after, (int, float)) else "N/A"
    if isinstance(hold_seconds, (int, float)):
        hold_seconds = int(hold_seconds)
        hours = hold_seconds // 3600
        minutes = (hold_seconds % 3600) // 60
        seconds = hold_seconds % 60
        if hours > 0:
            hold_text = f"{hours}h {minutes}m {seconds}s"
        else:
            hold_text = f"{minutes}m {seconds}s"
    else:
        hold_text = "N/A"
    contract_lines = [A(contract_label, "magenta", bold=True)]
    if option_symbol:
        contract_lines.append(A(option_symbol, "white"))
    embed.add_field(name="Contract", value=ab(*contract_lines), inline=False)
    embed.add_field(
        name="Exit",
        value=ab(
            f"{lbl('Qty')} {A(qty if qty is not None else 'N/A', 'white', bold=True)}  |  "
            f"{lbl('Exit')} {A(exit_text, 'white', bold=True)}  |  "
            f"{lbl('PnL')} {pnl_col(pnl_val)}"
        ),
        inline=False,
    )
    embed.add_field(name="Entry", value=ab(f"{lbl('Entry')} {A(entry_text, 'white')}  |  {lbl('Mode')} {A(mode, 'cyan', bold=True)}"), inline=False)
    embed.add_field(name="Hold / Balance", value=ab(f"{lbl('Hold')} {A(hold_text, 'cyan')}  |  {lbl('Bal')} {balance_col(balance_after)}"), inline=False)
    embed.add_field(name="Reason", value=ab(exit_reason_col(exit_reason)), inline=False)
    exit_context = result.get("exit_context")
    if exit_context:
        parts = _format_exit_context(exit_context)
        if parts:
            embed.add_field(name="Exit Context", value=ab(*[A(p, "cyan") for p in parts]), inline=False)
    feature_snapshot = result.get("feature_snapshot")
    fs_text = _format_feature_snapshot(feature_snapshot)
    if fs_text:
        embed.add_field(name="Feature Snapshot", value=ab(fs_text), inline=False)

    mae = result.get("mae")
    mfe = result.get("mfe")
    if isinstance(mae, (int, float)) or isinstance(mfe, (int, float)):
        mae_text = f"{mae:.2%}" if isinstance(mae, (int, float)) else "N/A"
        mfe_text = f"{mfe:.2%}" if isinstance(mfe, (int, float)) else "N/A"
        embed.add_field(
            name="Excursion",
            value=ab(f"{lbl('MFE')} {A(mfe_text, 'green')}  |  {lbl('MAE')} {A(mae_text, 'red')}"),
            inline=False,
        )
    pred_dir = result.get("predicted_direction") or "N/A"
    conf_val = result.get("prediction_confidence")
    edge_val = result.get("edge_prob")
    conf_text = f"{conf_val:.2f}" if isinstance(conf_val, (int, float)) else "N/A"
    edge_text = f"{edge_val:.2f}" if isinstance(edge_val, (int, float)) else "N/A"
    embed.add_field(
        name="ML",
        value=ab(
            f"{lbl('Pred')} {dir_col(pred_dir)}  |  "
            f"{lbl('Conf')} {conf_col(conf_val) if isinstance(conf_val, (int, float)) else A('N/A','gray')}  |  "
            f"{lbl('Edge')} {pct_col(edge_val, good_when_high=True, multiply=True) if isinstance(edge_val, (int, float)) else A('N/A','gray')}"
        ),
        inline=False
    )
    _und_sym2 = (result.get("symbol") or "SPY").upper()
    try:
        from core.data_service import get_symbol_dataframe
        _und_df2 = get_symbol_dataframe(_und_sym2)
        _und_close2 = _und_df2.iloc[-1].get("close") if _und_df2 is not None and len(_und_df2) else None
        _und_price2 = float(_und_close2) if _und_close2 is not None else None
    except Exception:
        _und_price2 = None
    if isinstance(_und_price2, (int, float)):
        embed.add_field(name=f"{_und_sym2} Price", value=ab(A(f"${_und_price2:.2f}", "white", bold=True)), inline=True)
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


def _build_skip_embed(sim_id: str, result: dict) -> "discord.Embed":
    reason = result.get("reason") or "unknown"
    detail = _format_skip_reason(reason)
    title = f"⏸ {sim_id} Live Skipped"
    embed = discord.Embed(title=title, color=0xF1C40F)
    embed.add_field(name="Reason", value=ab(A(reason, "yellow", bold=True)), inline=False)
    if detail:
        embed.add_field(name="Details", value=ab(A(detail, "cyan")), inline=False)
    entry_context = result.get("entry_context")
    signal_mode = result.get("signal_mode")
    if entry_context or signal_mode:
        lines = []
        if signal_mode:
            lines.append(f"{lbl('Signal')} {A(signal_mode, 'magenta', bold=True)}")
        if entry_context:
            lines.append(A(entry_context, "cyan"))
        embed.add_field(name="Context", value=ab(*lines), inline=False)
    if reason == "insufficient_trade_history":
        trade_count = result.get("trade_count")
        min_trades = result.get("min_trades_for_live")
        if isinstance(trade_count, int) and isinstance(min_trades, int):
            embed.add_field(
                name="Trade History",
                value=ab(A(f"{trade_count} / {min_trades} closed trades", "white", bold=True)),
                inline=False,
            )
    footer_parts = []
    footer_parts.append(f"Time: {_format_et(_now_et())}")
    if _SIM_LAST_DATA_AGE:
        footer_parts.append(_SIM_LAST_DATA_AGE)
    embed.set_footer(text=" | ".join(footer_parts))
    return embed


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


def _is_market_hours() -> bool:
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    return (
        now_et.weekday() <= 4
        and (now_et.hour, now_et.minute) >= (9, 30)
        and (now_et.hour, now_et.minute) <= (16, 0)
    )


# Simple heuristic regime classifier used for sim metadata (not live decisions).
def _derive_regime(df):
    """
    Classify market regime from indicator columns.
    Returns "VOLATILE", "TREND", "RANGE", or None if data insufficient.
    """
    TREND_SEPARATION_THRESHOLD = 0.001
    ATR_LOOKBACK = 50
    ATR_VOLATILE_PERCENTILE = 75
    MIN_BARS_REQUIRED = 2

    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None

        last = df.iloc[-1]
        close = float(last.get("close", 0))
        if close <= 0:
            return None

        atr_col = None
        for candidate in ["atr", "ATR", "atr14", "ATR14"]:
            if candidate in df.columns:
                atr_col = candidate
                break

        if atr_col is not None:
            lookback = min(ATR_LOOKBACK, len(df))
            atr_series = df[atr_col].iloc[-lookback:].dropna()
            if len(atr_series) >= 2:
                current_atr = float(atr_series.iloc[-1])
                pct_rank = (atr_series < current_atr).sum() / len(atr_series) * 100
                if pct_rank > ATR_VOLATILE_PERCENTILE:
                    ema9_col = None
                    for c in ["ema9", "EMA9", "ema_9"]:
                        if c in df.columns:
                            ema9_col = c
                            break
                    if ema9_col and len(df) >= 2:
                        ema9_now = float(df[ema9_col].iloc[-1])
                        ema9_prev = float(df[ema9_col].iloc[-2])
                        if ema9_now < ema9_prev:
                            return "VOLATILE"

        ema9_val = None
        ema20_val = None
        for c in ["ema9", "EMA9", "ema_9"]:
            if c in df.columns:
                ema9_val = float(last[c])
                break
        for c in ["ema20", "EMA20", "ema_20"]:
            if c in df.columns:
                ema20_val = float(last[c])
                break

        if ema9_val is not None and ema20_val is not None:
            separation = abs(ema9_val - ema20_val) / close
            if separation > TREND_SEPARATION_THRESHOLD:
                return "TREND"

        return "RANGE"

    except Exception:
        return None


# Time-of-day bucket for sim metadata.
def _get_time_of_day_bucket():
    """Classify current ET time into a trading session bucket."""
    try:
        now_et = datetime.now(pytz.timezone("US/Eastern"))
        t = now_et.time()
        if t < time(10, 30):
            return "MORNING"
        elif t < time(12, 0):
            return "MIDDAY"
        elif t < time(14, 0):
            return "AFTERNOON"
        else:
            return "CLOSE"
    except Exception:
        return None


async def sim_entry_loop() -> None:
    eastern = pytz.timezone("US/Eastern")
    while True:
        iter_start = datetime.now(eastern)
        try:
            if _is_market_hours():
                from simulation.sim_engine import run_sim_entries
                df = get_market_dataframe()
                global _SIM_LAST_DATA_AGE
                _SIM_LAST_DATA_AGE = _get_data_age_text(df)
                results = await run_sim_entries(df, regime=_derive_regime(df))
                for result in results:
                    logging.debug("sim_entry_result: %s", result)
                    try:
                        sim_id = result.get("sim_id")
                        status = result.get("status")
                        if sim_id and status in {"opened", "live_submitted"}:
                            entry_embed = _build_entry_embed(sim_id, result)
                            await _post_sim_event(sim_id, entry_embed)
                            _SIM_LAST_SKIP_REASON.pop(sim_id, None)
                        elif sim_id and status == "skipped":
                            reason = result.get("reason") or "unknown"
                            if reason == "insufficient_trade_history":
                                trade_count = result.get("trade_count", 0)
                                min_trades = result.get("min_trades_for_live", 50)
                                today = _now_et().date()
                                if isinstance(trade_count, int) and isinstance(min_trades, int) and trade_count >= min_trades:
                                    if sim_id not in _SIM_TRADE_HISTORY_UNLOCKED:
                                        _SIM_TRADE_HISTORY_UNLOCKED.add(sim_id)
                                        ready_embed = discord.Embed(
                                            title=f"✅ {sim_id} LIVE — Trade History Threshold Reached",
                                            description=ab(A(f"Source sim has reached {trade_count} closed trades. Live execution is now unlocked.", "green", bold=True)),
                                            color=0x2ECC71,
                                        )
                                        ready_embed.add_field(name="Trades Logged", value=ab(A(f"{trade_count} / {min_trades}", "green", bold=True)), inline=True)
                                        footer_parts = [f"Time: {_format_et(_now_et())}"]
                                        if _SIM_LAST_DATA_AGE:
                                            footer_parts.append(_SIM_LAST_DATA_AGE)
                                        ready_embed.set_footer(text=" | ".join(footer_parts))
                                        await _post_sim_event(sim_id, ready_embed)
                                elif _SIM_TRADE_PROGRESS_DATE.get(sim_id) != today:
                                    _SIM_TRADE_PROGRESS_DATE[sim_id] = today
                                    skip_embed = _build_skip_embed(sim_id, result)
                                    await _post_sim_event(sim_id, skip_embed)
                                # else: silently skip — already reported today
                            elif reason in {
                                "cutoff_passed",
                                "no_candidate_expiry",
                                "empty_chain",
                                "chain_error",
                                "no_snapshot",
                                "no_snapshot_all",
                                "no_snapshot_most",
                                "no_snapshot_all_no_chain_symbols",
                                "no_quote",
                                "no_quote_all",
                                "no_quote_most",
                                "no_quote_all_no_chain_symbols",
                                "invalid_quote",
                                "invalid_quote_all",
                                "invalid_quote_most",
                                "invalid_quote_all_no_chain_symbols",
                                "spread_too_wide",
                                "spread_too_wide_all",
                                "spread_too_wide_most",
                                "spread_too_wide_all_no_chain_symbols",
                                "snapshot_error",
                                "snapshot_error_all",
                                "snapshot_error_most",
                                "snapshot_error_all_no_chain_symbols",
                                "no_chain_symbols",
                                "missing_api_keys",
                                "invalid_price",
                                "invalid_direction",
                                "no_contract",
                                "before_entry_window",
                                "regime_filter",
                            }:
                                last_reason = _SIM_LAST_SKIP_REASON.get(sim_id)
                                if last_reason != reason:
                                    _SIM_LAST_SKIP_REASON[sim_id] = reason
                                    _SIM_LAST_SKIP_TIME[sim_id] = _now_et()
                                    skip_embed = _build_skip_embed(sim_id, result)
                                    await _post_sim_event(sim_id, skip_embed)
                        elif sim_id and status == "circuit_breaker_tripped":
                            cb_embed = discord.Embed(
                                title=f"🔴 {sim_id} LIVE — Circuit Breaker TRIPPED",
                                color=0xE74C3C,
                            )
                            source = result.get("source_sim", "?")
                            cb_embed.add_field(
                                name="Source Performance",
                                value=ab(
                                    f"{lbl('Source')} {A(source, 'cyan', bold=True)}  |  "
                                    f"{lbl('WR')} {wr_col(result.get('source_wr'))}  |  "
                                    f"{lbl('Exp')} {pnl_col(result.get('source_exp'))}"
                                ),
                                inline=False,
                            )
                            cb_embed.add_field(
                                name="Thresholds",
                                value=ab(
                                    f"{lbl('Min WR')} {A(str(result.get('threshold_wr')), 'yellow')}  |  "
                                    f"{lbl('Min Exp')} {A(str(result.get('threshold_exp')), 'yellow')}  |  "
                                    f"{lbl('Window')} {A(str(result.get('window')), 'white')}"
                                ),
                                inline=False,
                            )
                            cb_embed.add_field(
                                name="Action",
                                value=ab(A("Live execution PAUSED. Will auto-resume when source recovers.", "red", bold=True)),
                                inline=False,
                            )
                            cb_embed.set_footer(text=f"Time: {_format_et(_now_et())}")
                            await _post_sim_event(sim_id, cb_embed)
                        elif sim_id and status == "circuit_breaker_recovered":
                            rec_embed = discord.Embed(
                                title=f"🟢 {sim_id} LIVE — Circuit Breaker RECOVERED",
                                color=0x2ECC71,
                            )
                            source = result.get("source_sim", "?")
                            rec_embed.add_field(
                                name="Source Performance",
                                value=ab(
                                    f"{lbl('Source')} {A(source, 'cyan', bold=True)}  |  "
                                    f"{lbl('WR')} {wr_col(result.get('source_wr'))}  |  "
                                    f"{lbl('Exp')} {pnl_col(result.get('source_exp'))}"
                                ),
                                inline=False,
                            )
                            rec_embed.add_field(
                                name="Action",
                                value=ab(A("Live execution RESUMED.", "green", bold=True)),
                                inline=False,
                            )
                            rec_embed.set_footer(text=f"Time: {_format_et(_now_et())}")
                            await _post_sim_event(sim_id, rec_embed)
                    except Exception:
                        logging.exception("sim_entry_notify_error: sim=%s status=%s", result.get("sim_id"), result.get("status"))
        except Exception as e:
            logging.exception("sim_entry_loop_error: %s", e)
        elapsed = (datetime.now(eastern) - iter_start).total_seconds()
        await asyncio.sleep(max(0.0, ENTRY_INTERVAL_SECONDS - elapsed))


async def sim_exit_loop() -> None:
    eastern = pytz.timezone("US/Eastern")
    while True:
        iter_start = datetime.now(eastern)
        try:
            if not _is_market_hours():
                await asyncio.sleep(60)
                continue
            from simulation.sim_engine import run_sim_exits
            global _SIM_LAST_DATA_AGE
            _SIM_LAST_DATA_AGE = _get_data_age_text()
            results = await run_sim_exits()
            for result in results:
                logging.debug("sim_exit_result: %s", result)
                try:
                    sim_id = result.get("sim_id")
                    status = result.get("status")
                    if sim_id and status == "closed":
                        exit_embed = _build_exit_embed(sim_id, result)
                        await _post_sim_event(sim_id, exit_embed)
                        try:
                            from analytics.grader import check_predictions
                            check_predictions(result)
                        except Exception:
                            logging.exception("sim_prediction_grader_error")
                except Exception:
                    pass
        except Exception as e:
            logging.exception("sim_exit_loop_error: %s", e)
        elapsed = (datetime.now(eastern) - iter_start).total_seconds()
        await asyncio.sleep(max(0.0, EXIT_INTERVAL_SECONDS - elapsed))


async def sim_weekly_leaderboard_loop(channel_id: int) -> None:
    """Post weekly sim leaderboard every Friday after close."""
    global _SIM_WEEKLY_LEADERBOARD_DATE
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() != 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < WEEKLY_LEADERBOARD_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _SIM_WEEKLY_LEADERBOARD_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            sim_data = []
            for sim_id, profile in _SIM_PROFILES.items():
                if str(sim_id).startswith("_"):
                    continue
                try:
                    sim = SimPortfolio(sim_id, profile)
                    sim.load()
                    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                    pnl_vals = []
                    wins = 0
                    for t in trade_log:
                        pnl = t.get("realized_pnl_dollars")
                        if pnl is None:
                            continue
                        try:
                            pnl_f = float(pnl)
                        except (TypeError, ValueError):
                            continue
                        pnl_vals.append(pnl_f)
                        if pnl_f > 0:
                            wins += 1

                    total = len(pnl_vals)
                    total_pnl = sum(pnl_vals) if pnl_vals else 0
                    wr = wins / total if total > 0 else 0
                    exp = total_pnl / total if total > 0 else 0
                    win_sum = sum(p for p in pnl_vals if p > 0)
                    loss_sum = abs(sum(p for p in pnl_vals if p < 0))
                    pf = win_sum / loss_sum if loss_sum > 0 else 0
                    mode = "LIVE" if profile.get("execution_mode") == "live" and profile.get("enabled") else "SIM"

                    sim_data.append({
                        "sim_id": sim_id,
                        "name": profile.get("name", sim_id),
                        "mode": mode,
                        "trades": total,
                        "wr": wr,
                        "exp": exp,
                        "pf": pf,
                        "total_pnl": total_pnl,
                        "balance": sim.balance,
                        "sufficient": total >= 10,
                    })
                except Exception:
                    continue

            if not sim_data:
                _SIM_WEEKLY_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            eligible = [s for s in sim_data if s["sufficient"]]
            if eligible:
                for metric in ["exp", "wr", "pf"]:
                    sorted_by = sorted(eligible, key=lambda x: x[metric])
                    for rank, item in enumerate(sorted_by, 1):
                        item[f"{metric}_rank"] = rank
                n = len(eligible)
                for item in eligible:
                    exp_score = item.get("exp_rank", 0) / n if n > 0 else 0
                    wr_score = item.get("wr_rank", 0) / n if n > 0 else 0
                    pf_score = item.get("pf_rank", 0) / n if n > 0 else 0
                    item["composite"] = exp_score * 0.4 + wr_score * 0.3 + pf_score * 0.3
                eligible.sort(key=lambda x: x["composite"], reverse=True)

            embed = discord.Embed(
                title=f"🏆 Weekly Sim Leaderboard — {now_et.date().isoformat()}",
                color=0xF1C40F,
            )

            rank_lines = []
            rank = 1
            for item in eligible:
                medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                mode_tag = " 🔴" if item["mode"] == "LIVE" else ""
                pf_text = f"{item['pf']:.2f}"
                rank_lines.append(
                    f"{lbl(medal)} {A(item['sim_id'] + mode_tag, 'cyan', bold=True)}  "
                    f"{lbl('WR')} {wr_col(item['wr'])}  "
                    f"{lbl('Exp')} {pnl_col(item['exp'])}  "
                    f"{lbl('PF')} {A(pf_text, 'white')}  "
                    f"{lbl('PnL')} {pnl_col(item['total_pnl'])}"
                )
                rank += 1

            insufficient = [s for s in sim_data if not s["sufficient"]]
            for item in insufficient:
                insuff_text = f"({item['trades']} trades — insufficient data)"
                rank_lines.append(
                    f"{lbl('—')} {A(item['sim_id'], 'gray')}  "
                    f"{A(insuff_text, 'gray')}"
                )

            for i in range(0, len(rank_lines), 5):
                chunk = rank_lines[i:i + 5]
                field_name = "Rankings" if i == 0 else "​"
                embed.add_field(name=field_name, value=ab(*chunk), inline=False)

            embed.set_footer(text=f"Composite: 40% Expectancy + 30% Win Rate + 30% Profit Factor | {_format_et(now_et)}")

            if _SIM_BOT is not None and channel_id:
                channel = _SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            _SIM_WEEKLY_LEADERBOARD_DATE = now_et.date()
        except Exception:
            logging.exception("sim_weekly_leaderboard_error")
        await asyncio.sleep(60)

async def sim_session_leaderboard_loop(channel_id: int) -> None:
    """Post a paginated session leaderboard once at 16:00 ET."""
    global _SIM_SESSION_LEADERBOARD_DATE
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < time(16, 0):
                await asyncio.sleep(30)
                continue
            if _SIM_SESSION_LEADERBOARD_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            today_str = now_et.date().isoformat()
            sim_rows = []
            total_day_pnl = 0.0
            total_day_trades = 0

            for sim_id, profile in _SIM_PROFILES.items():
                if str(sim_id).startswith("_"):
                    continue
                try:
                    sim = SimPortfolio(sim_id, profile)
                    sim.load()
                    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []

                    today_trades = []
                    for t in trade_log:
                        exit_time = t.get("exit_time")
                        if not exit_time:
                            continue
                        try:
                            exit_date = datetime.fromisoformat(str(exit_time)).date().isoformat()
                        except Exception:
                            continue
                        if exit_date == today_str:
                            today_trades.append(t)

                    wins = 0
                    losses = 0
                    day_pnl = 0.0
                    for t in today_trades:
                        try:
                            pnl = float(t.get("realized_pnl_dollars", 0))
                        except (TypeError, ValueError):
                            continue
                        day_pnl += pnl
                        if pnl > 0:
                            wins += 1
                        else:
                            losses += 1

                    mode = "LIVE" if profile.get("execution_mode") == "live" else "SIM"
                    sim_rows.append({
                        "sim_id": sim_id,
                        "mode": mode,
                        "trades": len(today_trades),
                        "wins": wins,
                        "losses": losses,
                        "wr": wins / len(today_trades) if today_trades else 0.0,
                        "day_pnl": day_pnl,
                        "balance": sim.balance,
                    })
                    total_day_pnl += day_pnl
                    total_day_trades += len(today_trades)
                except Exception:
                    continue

            if not sim_rows or not channel_id or _SIM_BOT is None:
                _SIM_SESSION_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            channel = _SIM_BOT.get_channel(channel_id)
            if channel is None:
                _SIM_SESSION_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            # Sort: sims with trades first by PnL desc, no-trade sims at bottom
            sim_rows.sort(key=lambda x: (x["trades"] == 0, -x["day_pnl"]))

            pages = [
                sim_rows[i:i + SESSION_LEADERBOARD_SIMS_PER_PAGE]
                for i in range(0, len(sim_rows), SESSION_LEADERBOARD_SIMS_PER_PAGE)
            ]
            total_pages = len(pages)
            best = next((s for s in sim_rows if s["trades"] > 0), None)
            color = 0x2ECC71 if total_day_pnl >= 0 else 0xE74C3C

            for page_idx, page_sims in enumerate(pages):
                page_num = page_idx + 1
                embed = discord.Embed(
                    title=f"🏆 Session Leaderboard — {today_str}  [{page_num}/{total_pages}]",
                    color=color,
                )
                if page_idx == 0:
                    summary_lines = [
                        f"{lbl('Trades')} {A(str(total_day_trades), 'white', bold=True)}  |  "
                        f"{lbl('Total PnL')} {pnl_col(total_day_pnl)}"
                    ]
                    if best:
                        summary_lines.append(
                            f"{lbl('Best')} {A(best['sim_id'], 'cyan', bold=True)}  {pnl_col(best['day_pnl'])}"
                        )
                    embed.add_field(name="Session", value=ab(*summary_lines), inline=False)

                for item in page_sims:
                    mode_tag = " 🔴" if item["mode"] == "LIVE" else ""
                    sim_label = f"{item['sim_id']}{mode_tag}  ·  {item['mode']}"
                    if item["trades"] == 0:
                        field_val = ab(A("No trades this session", "gray"))
                    else:
                        field_val = ab(
                            f"{lbl('PnL')} {pnl_col(item['day_pnl'])}  |  "
                            f"{lbl('Trades')} {A(str(item['trades']), 'white', bold=True)}  |  "
                            f"{lbl('W/L')} {A(str(item['wins']), 'green')}/{A(str(item['losses']), 'red')}  |  "
                            f"{lbl('WR')} {wr_col(item['wr'])}"
                        )
                    embed.add_field(name=sim_label, value=field_val, inline=False)

                embed.set_footer(text=f"{_format_et(now_et)}")
                await channel.send(embed=embed)

            _SIM_SESSION_LEADERBOARD_DATE = now_et.date()
        except Exception:
            logging.exception("sim_session_leaderboard_error")
        await asyncio.sleep(30)
