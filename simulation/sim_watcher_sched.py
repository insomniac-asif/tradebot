# simulation/sim_watcher_sched.py
"""
Scheduled async loop functions extracted from sim_watcher.py.
All loops access sim_watcher globals lazily via 'from simulation import sim_watcher as _sw'.
"""
import asyncio
import logging
import pytz
import discord
from datetime import datetime, time

from simulation.sim_portfolio import SimPortfolio
from simulation.sim_report_helpers import (
    _format_et,
    _format_option_symbol,
    _format_entry_time,
)
from simulation.sim_watcher_loops import (
    _build_eod_open_positions_embed,
    _build_eod_combined_embed,
    _build_graduation_embed,
    _build_daily_sim_embed,
    _build_daily_combined_embed,
    _collect_weekly_sim_data,
    _build_weekly_leaderboard_embed,
    _collect_session_sim_rows,
    _build_session_leaderboard_page,
)
from interface.fmt import ab, A, wr_col

EOD_REPORT_TIME_ET = time(16, 0)
DAILY_SUMMARY_TIME_ET = time(16, 5)
WEEKLY_LEADERBOARD_TIME_ET = time(16, 10)
SESSION_LEADERBOARD_SIMS_PER_PAGE = 4


def _now_et() -> datetime:
    return datetime.now(pytz.timezone("US/Eastern"))


async def sim_eod_report_loop(channel_id: int) -> None:
    """
    End-of-day report for non-daytrading sims:
    show any open positions that will carry overnight.
    """
    from simulation import sim_watcher as _sw
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue

            preopen_window = time(9, 10) <= now_et.time() <= time(9, 40)
            if not preopen_window and now_et.time() < EOD_REPORT_TIME_ET:
                await asyncio.sleep(30)
                continue
            if preopen_window:
                if _sw._SIM_PREOPEN_REPORT_DATE == now_et.date():
                    await asyncio.sleep(300)
                    continue
            else:
                if _sw._SIM_EOD_REPORT_DATE == now_et.date():
                    await asyncio.sleep(300)
                    continue

            lines_by_sim = {}
            pnl_by_sim = {}
            for sim_id, profile in _sw._SIM_PROFILES.items():
                try:
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

            for sim_id, lines in sorted(lines_by_sim.items()):
                session_pnl = pnl_by_sim.get(sim_id, 0.0)
                sim_embed = _build_eod_open_positions_embed(
                    sim_id, lines, session_pnl, now_et, preopen_window, _sw._SIM_LAST_DATA_AGE
                )
                await _sw._post_sim_event(sim_id, sim_embed)

            embed = _build_eod_combined_embed(lines_by_sim, pnl_by_sim, now_et, _sw._SIM_LAST_DATA_AGE)
            if _sw._SIM_BOT is not None and channel_id:
                channel = _sw._SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            for _grad_sim_id, _grad_profile in sorted(_sw._SIM_PROFILES.items()):
                try:
                    if _grad_profile.get("execution_mode") != "live":
                        continue
                    _grad_source = _grad_profile.get("source_sim")
                    _grad_min = _grad_profile.get("min_source_trades", 0)
                    if not _grad_source or not _grad_min:
                        continue
                    if _grad_sim_id in _sw._SIM_TRADE_HISTORY_UNLOCKED:
                        continue
                    _src_profile = _sw._SIM_PROFILES.get(_grad_source, {})
                    _src_sim = SimPortfolio(_grad_source, _src_profile)
                    _src_sim.load()
                    _src_count = len(_src_sim.trade_log) if isinstance(_src_sim.trade_log, list) else 0
                    _prog_embed = _build_graduation_embed(
                        _grad_sim_id, _grad_source, _src_count, _grad_min, now_et, _sw._SIM_LAST_DATA_AGE
                    )
                    await _sw._post_sim_event(_grad_sim_id, _prog_embed)
                except Exception:
                    pass

            if preopen_window:
                _sw._SIM_PREOPEN_REPORT_DATE = now_et.date()
            else:
                _sw._SIM_EOD_REPORT_DATE = now_et.date()
        except Exception:
            logging.exception("sim_eod_report_error")
        await asyncio.sleep(60)


async def sim_daily_summary_loop(channel_id: int) -> None:
    """Post daily P&L summary for all sims after market close."""
    from simulation import sim_watcher as _sw
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < DAILY_SUMMARY_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _sw._SIM_DAILY_SUMMARY_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            today_str = now_et.date().isoformat()
            combined_lines = []
            total_day_pnl = 0.0
            total_day_trades = 0

            for sim_id, profile in _sw._SIM_PROFILES.items():
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
                    mode = "LIVE" if profile.get("execution_mode") == "live" and profile.get("enabled") else "SIM"

                    sim_embed = _build_daily_sim_embed(
                        sim_id, today_trades, wins, losses, day_pnl,
                        sim.balance, sim.peak_balance, mode, today_str
                    )
                    await _sw._post_sim_event(sim_id, sim_embed)

                    combined_lines.append({
                        "sim_id": sim_id,
                        "mode": mode,
                        "trades": len(today_trades),
                        "wins": wins,
                        "losses": losses,
                        "wr": wins / len(today_trades) if today_trades else 0,
                        "day_pnl": day_pnl,
                        "balance": sim.balance,
                    })

                except Exception:
                    logging.exception(f"sim_daily_summary_error: {sim_id}")
                    continue

            if _sw._SIM_BOT is not None and channel_id:
                channel = _sw._SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    combined_embed = _build_daily_combined_embed(
                        combined_lines, total_day_trades, total_day_pnl, today_str, now_et
                    )
                    await channel.send(embed=combined_embed)

            _sw._SIM_DAILY_SUMMARY_DATE = now_et.date()
        except Exception:
            logging.exception("sim_daily_summary_loop_error")
        await asyncio.sleep(60)


async def sim_weekly_leaderboard_loop(channel_id: int) -> None:
    """Post weekly sim leaderboard every Friday after close."""
    from simulation import sim_watcher as _sw
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() != 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < WEEKLY_LEADERBOARD_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _sw._SIM_WEEKLY_LEADERBOARD_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            sim_data = _collect_weekly_sim_data(_sw._SIM_PROFILES)

            if not sim_data:
                _sw._SIM_WEEKLY_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            embed = _build_weekly_leaderboard_embed(sim_data, now_et)

            if _sw._SIM_BOT is not None and channel_id:
                channel = _sw._SIM_BOT.get_channel(channel_id)
                if channel is not None:
                    await channel.send(embed=embed)

            _sw._SIM_WEEKLY_LEADERBOARD_DATE = now_et.date()
        except Exception:
            logging.exception("sim_weekly_leaderboard_error")
        await asyncio.sleep(60)


async def sim_weekly_behavior_report_loop(channel_id: int) -> None:
    """Post weekly behavior gap report every Friday, 5 minutes after the leaderboard."""
    from simulation import sim_watcher as _sw
    _weekly_behavior_report_date = None
    _WEEKLY_BEHAVIOR_REPORT_TIME_ET = time(16, 15)
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() != 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < _WEEKLY_BEHAVIOR_REPORT_TIME_ET:
                await asyncio.sleep(30)
                continue
            if _weekly_behavior_report_date == now_et.date():
                await asyncio.sleep(300)
                continue

            try:
                from research.behavior_divergence import generate_all_reports
                reports = generate_all_reports()
                lines = []
                for r in reports:
                    sid = r.get("sim_id", "?")
                    status = r.get("status", "error")
                    if status == "ok":
                        wr = r.get("win_rate", 0)
                        gaps = r.get("gaps", {})
                        hold = gaps.get("hold_time", {})
                        tod = gaps.get("time_of_day", {})
                        best_bucket = tod.get("best_bucket") or "?"
                        w_hold = hold.get("winners_avg_sec")
                        hold_text = f"{w_hold:.0f}s" if w_hold is not None else "?"
                        lines.append(
                            f"{A(sid, 'cyan', bold=True)}: {wr_col(wr)} WR | best: {A(best_bucket, 'white')} | winners hold {A(hold_text, 'green')}"
                        )
                    elif status == "insufficient_data":
                        lines.append(f"{A(sid, 'cyan')}: {A('<10 trades', 'gray')}")

                if not lines:
                    _weekly_behavior_report_date = now_et.date()
                    await asyncio.sleep(300)
                    continue

                chunks, current, current_len = [], [], 0
                for line in lines:
                    if current and current_len + len(line) + 1 > 3800:
                        chunks.append(current)
                        current, current_len = [], 0
                    current.append(line)
                    current_len += len(line) + 1
                if current:
                    chunks.append(current)

                if _sw._SIM_BOT is not None and channel_id:
                    channel = _sw._SIM_BOT.get_channel(channel_id)
                    if channel is not None:
                        for idx, chunk in enumerate(chunks):
                            title = f"📊 Weekly Behavior Gap Report — {now_et.date().isoformat()}" if idx == 0 else "📊 Weekly Behavior Gap Report (cont.)"
                            embed = discord.Embed(title=title, description=ab(*chunk), color=0x3498DB)
                            embed.set_footer(text=_format_et(now_et))
                            await channel.send(embed=embed)
            except Exception as e:
                logging.error("sim_weekly_behavior_report_error: %s", e)

            _weekly_behavior_report_date = now_et.date()
        except Exception:
            logging.exception("sim_weekly_behavior_report_loop_error")
        await asyncio.sleep(60)


async def sim_session_leaderboard_loop(channel_id: int) -> None:
    """Post a paginated session leaderboard once at 16:00 ET."""
    from simulation import sim_watcher as _sw
    while True:
        try:
            now_et = _now_et()
            if now_et.weekday() > 4:
                await asyncio.sleep(300)
                continue
            if now_et.time() < time(16, 0):
                await asyncio.sleep(30)
                continue
            if _sw._SIM_SESSION_LEADERBOARD_DATE == now_et.date():
                await asyncio.sleep(300)
                continue

            today_str = now_et.date().isoformat()
            sim_rows, total_day_pnl, total_day_trades = _collect_session_sim_rows(
                _sw._SIM_PROFILES, today_str
            )

            if not sim_rows or not channel_id or _sw._SIM_BOT is None:
                _sw._SIM_SESSION_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            channel = _sw._SIM_BOT.get_channel(channel_id)
            if channel is None:
                _sw._SIM_SESSION_LEADERBOARD_DATE = now_et.date()
                await asyncio.sleep(300)
                continue

            sim_rows.sort(key=lambda x: (x["trades"] == 0, -x["day_pnl"]))

            pages = [
                sim_rows[i:i + SESSION_LEADERBOARD_SIMS_PER_PAGE]
                for i in range(0, len(sim_rows), SESSION_LEADERBOARD_SIMS_PER_PAGE)
            ]
            total_pages = len(pages)

            for page_idx, page_sims in enumerate(pages):
                embed = _build_session_leaderboard_page(
                    sim_rows, total_day_pnl, total_day_trades, today_str,
                    now_et, page_idx, total_pages, page_sims,
                    SESSION_LEADERBOARD_SIMS_PER_PAGE,
                )
                await channel.send(embed=embed)

            _sw._SIM_SESSION_LEADERBOARD_DATE = now_et.date()
        except Exception:
            logging.exception("sim_session_leaderboard_error")
        await asyncio.sleep(30)
