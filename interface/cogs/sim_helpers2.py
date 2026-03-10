"""
interface/cogs/sim_helpers2.py
Extracted handler functions for large sim_commands.py command bodies.
No Discord decorators here — only async handler functions.
"""
import os
import json
import asyncio
import logging
import pytz
import discord
from datetime import datetime
from typing import Any

from interface.fmt import ab, lbl, A, pnl_col, wr_col, exit_reason_col, balance_col, drawdown_col
from interface.shared_state import (
    _send_embed, _append_footer, _add_field_icons, _format_ts, _format_pct_signed,
    _format_duration_short, _get_data_freshness_text, _load_sim_profiles,
    _collect_sim_metrics, _safe_float, _tag_trade_mode, STRATEGY_INTENTS,
)
from simulation.sim_portfolio import SimPortfolio
from execution.option_executor import get_option_price
from interface.cogs.sim_helpers import (
    _format_money, _format_signed_money, _format_drawdown, _format_pct,
    _pnl_badge, _format_duration, _extract_reason, _format_feature_snapshot,
    _sim_path, _compute_breakdown, _ansi_breakdown, _parse_page,
    _gate_parts, _paginate, _add_lines_field,
)


async def handle_simstats(ctx, sim_id):
    try:
        profiles = _load_sim_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found."); return
        if sim_id:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID."); return
            sp = _sim_path(sim_key)
            if not os.path.exists(sp):
                await _send_embed(ctx, f"No data for {sim_key} yet."); return
            sim = SimPortfolio(sim_key, profile); sim.load()
            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            total_trades = len(trade_log)
            open_count = len(sim.open_trades) if isinstance(sim.open_trades, list) else 0
            wins = 0; losses = 0; pnl_vals = []
            for t in trade_log:
                pv = _safe_float(t.get("realized_pnl_dollars"))
                if pv is None: losses += 1; continue
                pnl_vals.append(pv)
                if pv > 0: wins += 1
                else: losses += 1
            win_rate = wins / total_trades if total_trades > 0 else 0
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            avg_win = sum(p for p in pnl_vals if p > 0) / max(len([p for p in pnl_vals if p > 0]), 1) if any(p > 0 for p in pnl_vals) else 0.0
            avg_loss = sum(p for p in pnl_vals if p < 0) / max(len([p for p in pnl_vals if p < 0]), 1) if any(p < 0 for p in pnl_vals) else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            best_trade = max(pnl_vals) if pnl_vals else 0.0
            worst_trade = min(pnl_vals) if pnl_vals else 0.0
            peak_balance = _safe_float(sim.peak_balance) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0
            regime_breakdown = _compute_breakdown(trade_log, "regime_at_entry", order=["TREND","RANGE","VOLATILE","UNKNOWN"])
            time_breakdown = _compute_breakdown(trade_log, "time_of_day_bucket", order=["MORNING","MIDDAY","AFTERNOON","CLOSE","UNKNOWN"])
            exit_counts = {}
            for t in trade_log:
                reason = t.get("exit_reason", "unknown") or "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1
            if total_trades > 0 and exit_counts:
                exit_breakdown = "\n".join(f"{r}: {c} ({c/total_trades*100:.1f}%)" for r, c in sorted(exit_counts.items(), key=lambda x: x[1], reverse=True))
            else:
                exit_breakdown = "No exits recorded"
            name = profile.get("name", sim_key)
            embed_color = 0x2ECC71 if total_pnl > 0 else (0xE74C3C if total_pnl < 0 else 0x3498DB)
            embed = discord.Embed(title=f"\U0001f4ca {name} ({sim_key})", color=embed_color)
            last_trade_text = "N/A"
            if trade_log:
                last = trade_log[-1]
                last_pnl = _safe_float(last.get("realized_pnl_dollars"))
                last_trade_text = ab(
                    f"{lbl('Exit')} {A(_format_ts(last.get('exit_time','N/A')), 'white')}",
                    f"{lbl('PnL')}  {pnl_col(last_pnl) if last_pnl is not None else A('N/A','gray')}  {lbl('Reason')} {exit_reason_col(last.get('exit_reason','unknown'))}",
                    *([ f"{lbl('Hold')} {A(_format_duration(last.get('time_in_trade_seconds')), 'cyan')}" ] if _format_duration(last.get('time_in_trade_seconds')) != "N/A" else []),
                )
            embed.add_field(name=_add_field_icons("Last Trade"), value=last_trade_text, inline=False)
            embed.add_field(name=_add_field_icons("Total Trades"), value=ab(A(str(total_trades), "white", bold=True)), inline=True)
            embed.add_field(name=_add_field_icons("Open Trades"), value=ab(A(str(open_count), "cyan")), inline=True)
            embed.add_field(name=_add_field_icons("Win Rate"), value=ab(wr_col(win_rate)), inline=True)
            embed.add_field(name=_add_field_icons("Total PnL"), value=ab(pnl_col(total_pnl)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Win"), value=ab(pnl_col(avg_win)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Loss"), value=ab(pnl_col(avg_loss)), inline=True)
            embed.add_field(name=_add_field_icons("Expectancy"), value=ab(pnl_col(expectancy)), inline=True)
            embed.add_field(name=_add_field_icons("Best Trade"), value=ab(pnl_col(best_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Worst Trade"), value=ab(pnl_col(worst_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Max Drawdown"), value=ab(drawdown_col(max_drawdown)), inline=True)
            embed.add_field(name=_add_field_icons("Regime Breakdown"), value=_ansi_breakdown(regime_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Time Bucket Breakdown"), value=_ansi_breakdown(time_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Exit Reasons"), value=_ansi_breakdown(exit_breakdown), inline=False)
            gates = _gate_parts(profile)
            if gates:
                embed.add_field(name=_add_field_icons("SIM Gates"), value=ab("  |  ".join(gates)), inline=False)
            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            footer = f"Balance: {_format_money(balance)} | Start: {_format_money(start_balance)}"
            fl = _get_data_freshness_text()
            if fl: footer = f"{footer} | {fl}"
            embed.set_footer(text=footer)
            _append_footer(embed)
            await ctx.send(embed=embed); return
        # summary for all sims
        embed = discord.Embed(title="\U0001f4ca Sim Overview \u2014 All Portfolios", color=0x3498DB)
        max_abs_pnl = 0.0
        for sk, pr in profiles.items():
            try:
                sp = _sim_path(sk)
                if not os.path.exists(sp): continue
                sim = SimPortfolio(sk, pr); sim.load()
                tl = sim.trade_log if isinstance(sim.trade_log, list) else []
                pvs = [_safe_float(t.get("realized_pnl_dollars")) for t in tl]
                pvs = [p for p in pvs if p is not None]
                tp = sum(pvs) if pvs else 0.0
                max_abs_pnl = max(max_abs_pnl, abs(tp))
            except Exception: continue
        for sk, pr in profiles.items():
            try:
                sp = _sim_path(sk)
                if not os.path.exists(sp):
                    embed.add_field(name=sk, value="No data", inline=False); continue
                sim = SimPortfolio(sk, pr); sim.load()
                tl = sim.trade_log if isinstance(sim.trade_log, list) else []
                tt = len(tl); wins = 0; pvs = []; ec = {}
                for t in tl:
                    pv = _safe_float(t.get("realized_pnl_dollars"))
                    if pv is None: continue
                    pvs.append(pv)
                    if pv > 0: wins += 1
                    reason = t.get("exit_reason", "unknown") or "unknown"
                    ec[reason] = ec.get(reason, 0) + 1
                wr = wins / tt if tt > 0 else 0
                tp = sum(pvs) if pvs else 0.0
                bal = _safe_float(sim.balance) or 0.0
                te = max(ec.items(), key=lambda x: x[1])[0] if ec else "unknown"
                bar = ""
                if max_abs_pnl > 0:
                    bars = int(round(abs(tp) / max_abs_pnl * 10)); bars = max(0, min(bars, 10))
                    bar = "\u2588" * bars + "\u2591" * (10 - bars)
                else: bar = "\u2591" * 10
                gates = _gate_parts(pr)
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                lines = [
                    f"{A(str(tt), 'white', bold=True)} trades  {lbl('WR')} {wr_col(wr)}  {lbl('PnL')} {pnl_col(tp)}",
                    f"{lbl('Bal')} {balance_col(bal)}  {lbl('Exit')} {exit_reason_col(te)}  {A(bar, 'cyan')}",
                ]
                if gate_line: lines.append(gate_line)
                embed.add_field(name=sk, value=ab(*lines), inline=False)
            except Exception:
                embed.add_field(name=sk, value="No data", inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simstats_error")
        await _send_embed(ctx, "simstats failed due to an internal error.")


async def handle_simcompare(ctx):
    try:
        profiles = _load_sim_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found."); return
        rows = []; agg_pnl = 0.0; agg_pnl_count = 0
        for sk, pr in profiles.items():
            try:
                sp = _sim_path(sk)
                if not os.path.exists(sp):
                    rows.append({"sim_id": sk, "no_data": True, "balance_start": pr.get("balance_start", 0.0)}); continue
                sim = SimPortfolio(sk, pr); sim.load()
                tl = sim.trade_log if isinstance(sim.trade_log, list) else []
                tt = len(tl)
                if tt == 0:
                    rows.append({"sim_id": sk, "no_data": True, "balance_start": pr.get("balance_start", 0.0)}); continue
                wins = 0; pvs = []
                for t in tl:
                    pv = _safe_float(t.get("realized_pnl_dollars"))
                    if pv is None: continue
                    pvs.append(pv)
                    if pv > 0: wins += 1
                wr = wins / tt if tt > 0 else 0.0
                tp = sum(pvs) if pvs else 0.0
                bal = _safe_float(sim.balance) or 0.0
                pb = _safe_float(sim.peak_balance) or bal
                dd = pb - bal if pb > bal else 0.0
                exp = tp / tt if tt > 0 else 0.0
                rows.append({"sim_id": sk, "no_data": False, "total_trades": tt, "win_rate": wr, "total_pnl": tp, "balance": bal, "max_dd": dd, "expectancy": exp})
                agg_pnl += tp; agg_pnl_count += 1
            except Exception:
                rows.append({"sim_id": sk, "no_data": True, "balance_start": pr.get("balance_start", 0.0)})
        _hdr = "\u001b[30mSIM   | Trades | WR%     | PnL          | Balance      | MaxDD       | Expectancy\u001b[0m"
        lines = [_hdr]
        for row in rows:
            sk = row["sim_id"]
            if row.get("no_data"):
                tt = "--"; wr = None; tp = None; bal = _safe_float(row.get("balance_start")) or 0.0; dd = None; exp = None
            else:
                tt = row["total_trades"]; wr = row["win_rate"]; tp = row["total_pnl"]; bal = row["balance"]; dd = row["max_dd"]; exp = row["expectancy"]
            tr = f"{tt}" if isinstance(tt, str) else f"{tt:d}"
            if wr is None: wr_d = f"{'--':>7}"
            else:
                wr_r = f"{wr*100:.1f}%"; wr_d = A(f"{wr_r:>7}", "green" if wr >= 0.55 else "yellow" if wr >= 0.45 else "red", bold=True)
            if tp is None: pnl_d = f"{'--':>12}"
            else:
                pr_ = _format_signed_money(tp); pnl_d = A(f"{pr_:>12}", "green" if tp > 0 else "red" if tp < 0 else "white", bold=True)
            bal_d = f"{_format_money(bal):>12}"
            if dd is None: dd_d = f"{'--':>11}"
            else:
                dd_r = _format_signed_money(-abs(dd)) if dd > 0 else _format_signed_money(0); dd_d = A(f"{dd_r:>11}", "red" if dd > 0 else "white")
            if exp is None: exp_d = f"{'--':>10}"
            else:
                exp_r = _format_signed_money(exp); exp_d = A(f"{exp_r:>10}", "green" if exp > 0 else "red" if exp < 0 else "white")
            lines.append(f"{A(f'{sk:<5}', 'cyan')}|{tr:>7} | {wr_d} | {pnl_d} | {bal_d} | {dd_d} | {exp_d}")
        table = "```ansi\n" + "\n".join(lines) + "\n\u001b[0m```"
        color = (0x2ECC71 if agg_pnl > 0 else 0xE74C3C if agg_pnl < 0 else 0x3498DB) if agg_pnl_count > 0 else 0x95A5A6
        fl = _get_data_freshness_text()
        desc = f"{table}\n{fl}" if fl else table
        await _send_embed(ctx, desc, title="Sim Compare", color=color)
    except Exception:
        logging.exception("simcompare_error")
        await _send_embed(ctx, "simcompare failed due to an internal error.")


async def handle_simleaderboard(ctx):
    def _color_pct(val):
        try: num = float(val)
        except (TypeError, ValueError): return A("N/A", "gray")
        return A(f"{num:+.1f}%", "green" if num >= 0 else "red")
    def _pnl_or_na(val): return pnl_col(val) if val is not None else A("N/A", "gray")
    def _pick_best(items, key, prefer_high=True, filter_fn=None):
        pool = [m for m in items if (filter_fn(m) if filter_fn else True)]
        if not pool: return None
        return max(pool, key=lambda x: x.get(key, 0)) if prefer_high else min(pool, key=lambda x: x.get(key, 0))
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles: await _send_embed(ctx, "No sim profiles found."); return
        if not metrics: await _send_embed(ctx, "No sim data available yet."); return
        def _fmt(m, extra=None):
            rp = (m.get("return_pct", 0.0) or 0.0) * 100
            return ab(
                f"{lbl('Sim')} {A(m['sim_id'], 'cyan', bold=True)}  {lbl('Trades')} {A(str(m['trades']), 'white', bold=True)}",
                f"{lbl('WR')} {wr_col(m['win_rate'])}  {lbl('PnL')} {pnl_col(m['total_pnl'])}  {lbl('Return')} {_color_pct(rp)}",
                *([extra] if extra else []),
            )
        eligible = lambda m: m["trades"] >= 3
        bw = _pick_best(metrics, "win_rate", filter_fn=eligible)
        bp = _pick_best(metrics, "total_pnl", filter_fn=eligible)
        bf = _pick_best(metrics, "equity_speed", filter_fn=lambda m: eligible(m) and m["equity_speed"] is not None)
        be = _pick_best(metrics, "expectancy", filter_fn=eligible)
        bwin = _pick_best(metrics, "max_win", filter_fn=eligible)
        risky = _pick_best(metrics, "max_drawdown", filter_fn=lambda m: eligible(m) and m["total_pnl"] > 0)
        ma = _pick_best(metrics, "trades", filter_fn=lambda m: True)
        embed = discord.Embed(title="\U0001f3c1 Sim Leaderboard \u2014 Best At Each Role", color=0x3498DB)
        if bw: embed.add_field(name="\U0001f3c6 Best Win Rate", value=_fmt(bw), inline=False)
        if bp: embed.add_field(name="\U0001f4b0 Best Total PnL", value=_fmt(bp), inline=False)
        if bf: embed.add_field(name="\u26a1 Fastest Equity Growth", value=_fmt(bf, f"{lbl('Speed')} {_pnl_or_na(bf.get('equity_speed'))} {A('/day', 'cyan')}"), inline=False)
        if be: embed.add_field(name="\U0001f4c8 Best Expectancy", value=_fmt(be, f"{lbl('Expectancy')} {_pnl_or_na(be.get('expectancy'))}"), inline=False)
        if bwin: embed.add_field(name="\U0001f4a5 Biggest Winner", value=_fmt(bwin, f"{lbl('Max Win')} {_pnl_or_na(bwin.get('max_win'))}  {lbl('Max Loss')} {_pnl_or_na(bwin.get('max_loss'))}"), inline=False)
        if risky: embed.add_field(name="\u26a0\ufe0f High-Risk / High-Reward", value=_fmt(risky, f"{lbl('Drawdown')} {drawdown_col(risky.get('max_drawdown'))}  {lbl('PnL')} {pnl_col(risky.get('total_pnl'))}"), inline=False)
        if ma: embed.add_field(name="\U0001f9ee Most Active", value=_fmt(ma), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simleaderboard_error")
        await _send_embed(ctx, "simleaderboard failed due to an internal error.")


async def handle_simdte(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles: await _send_embed(ctx, "No sim profiles found."); return
        if not metrics: await _send_embed(ctx, "No sim data available yet."); return
        bucket_totals = {}
        for m in metrics:
            for k, v in (m.get("dte_stats") or {}).items():
                bucket_totals[k] = bucket_totals.get(k, 0) + v.get("total", 0)
        buckets = [b for b, _ in sorted(bucket_totals.items(), key=lambda x: x[1], reverse=True)][:8]
        if not buckets: await _send_embed(ctx, "No DTE bucket data yet."); return
        lines = []
        for bucket in buckets:
            candidates = []
            for m in metrics:
                stats = (m.get("dte_stats") or {}).get(bucket)
                if not stats or stats.get("total", 0) < 3: continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates: continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]; bottom = sorted(candidates, key=lambda x: x[0])[:3]
            def _fr(tag, sid, stats):
                w, t = stats.get("wins", 0), stats.get("total", 0)
                pf_t = A("N/A","gray"); exp_t = A("N/A","gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_t = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_t = pnl_col(exp)
                return f"{A(tag, 'cyan')} {A(sid, 'white', bold=True)} {A(f'{w}/{t}', 'white')} {wr_col(w/t)} {lbl('PF')} {pf_t} {lbl('Exp')} {exp_t}"
            lines.append(A(f"DTE {bucket} | n={bucket_totals.get(bucket, 0)}", "magenta", bold=True))
            for i, (_, sid, st) in enumerate(top, 1): lines.append(_fr(f"Top{i}", sid, st))
            for i, (_, sid, st) in enumerate(bottom, 1): lines.append(_fr(f"Bot{i}", sid, st))
        embed = discord.Embed(title="\U0001f4c6 Best by DTE Bucket (Win Rate)", description="Ranking by win rate (min 3 trades per sim+bucket). PF=profit factor, Exp=avg PnL per trade.", color=0x27AE60)
        _add_lines_field(embed, "DTE Leaders (Top/Bottom 3)", lines)
        _append_footer(embed); await ctx.send(embed=embed)
    except Exception:
        logging.exception("simdte_error")
        await _send_embed(ctx, "simdte failed due to an internal error.")


async def handle_simsetups(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles: await _send_embed(ctx, "No sim profiles found."); return
        if not metrics: await _send_embed(ctx, "No sim data available yet."); return
        setup_totals = {}
        for m in metrics:
            for k, v in (m.get("setup_stats") or {}).items():
                setup_totals[k] = setup_totals.get(k, 0) + v.get("total", 0)
        setups = [s for s, _ in sorted(setup_totals.items(), key=lambda x: x[1], reverse=True)][:8]
        if not setups: await _send_embed(ctx, "No setup data yet."); return
        lines = []
        for setup in setups:
            candidates = []
            for m in metrics:
                stats = (m.get("setup_stats") or {}).get(setup)
                if not stats or stats.get("total", 0) < 3: continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates: continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]; bottom = sorted(candidates, key=lambda x: x[0])[:3]
            def _fr(tag, sid, stats):
                w, t = stats.get("wins", 0), stats.get("total", 0)
                pf_t = A("N/A","gray"); exp_t = A("N/A","gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_t = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_t = pnl_col(exp)
                return f"{A(tag, 'cyan')} {A(sid, 'white', bold=True)} {A(f'{w}/{t}', 'white')} {wr_col(w/t)} {lbl('PF')} {pf_t} {lbl('Exp')} {exp_t}"
            lines.append(A(f"Setup {setup} | n={setup_totals.get(setup, 0)}", "magenta", bold=True))
            for i, (_, sid, st) in enumerate(top, 1): lines.append(_fr(f"Top{i}", sid, st))
            for i, (_, sid, st) in enumerate(bottom, 1): lines.append(_fr(f"Bot{i}", sid, st))
        embed = discord.Embed(title="\U0001f9e9 Best by Setup Type (Win Rate)", description="Ranking by win rate (min 3 trades per sim+setup). PF=profit factor, Exp=avg PnL per trade.", color=0xE67E22)
        _add_lines_field(embed, "Setup Leaders (Top/Bottom 3)", lines)
        _append_footer(embed); await ctx.send(embed=embed)
    except Exception:
        logging.exception("simsetups_error")
        await _send_embed(ctx, "simsetups failed due to an internal error.")


async def handle_simtrades(ctx, bot, sim_id, page):
    try:
        profiles = _load_sim_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            all_trades = []
            for sk, pr in profiles.items():
                try:
                    sp = _sim_path(sk)
                    if not os.path.exists(sp): continue
                    sim = SimPortfolio(sk, pr); sim.load()
                    tl = sim.trade_log if isinstance(sim.trade_log, list) else []
                    if not tl: continue
                    sb = _safe_float(pr.get("balance_start")) or 0.0; rb = sb
                    for t in tl:
                        pv = _safe_float(t.get("realized_pnl_dollars")) or 0.0; rb += pv
                        tc = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                        tc["sim_id"] = sk; tc["balance_after"] = rb
                        all_trades.append(tc)
                except Exception: continue
            if not all_trades: await _send_embed(ctx, "No trades recorded yet."); return
            def _pts(val):
                if val is None: return None
                if isinstance(val, datetime): return val
                try: return datetime.fromisoformat(str(val))
                except Exception: return None
            all_trades.sort(key=lambda t: _pts(t.get("exit_time")) or _pts(t.get("entry_time")) or datetime.min, reverse=True)
            trade_log = all_trades; sim_key = "ALL"
            balance_after = {str(t.get("trade_id")): t.get("balance_after") for t in trade_log}
        else:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None: await _send_embed(ctx, "Unknown sim ID."); return
            sp = _sim_path(sim_key)
            if not os.path.exists(sp): await _send_embed(ctx, f"No data for {sim_key} yet."); return
            sim = SimPortfolio(sim_key, profile); sim.load()
            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            if not trade_log: await _send_embed(ctx, "No trades recorded yet."); return
            sb = _safe_float(profile.get("balance_start")) or 0.0; rb = sb; balance_after = {}
            for t in trade_log:
                pv = _safe_float(t.get("realized_pnl_dollars")) or 0.0; rb += pv
                tid = t.get("trade_id")
                if tid: balance_after[str(tid)] = rb
        per_page = 5; total = len(trade_log); total_pages = (total + per_page - 1) // per_page
        page_num = _parse_page(page, total_pages)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simtrades {sim_key} 1` to `!simtrades {sim_key} {total_pages}`."); return
        newest_first = list(trade_log)
        def _build(pn):
            pn = max(1, min(pn, total_pages))
            start = (pn - 1) * per_page; end = start + per_page
            pt = newest_first[start:end]
            pp = sum((_safe_float(t.get("realized_pnl_dollars")) or 0) for t in pt)
            pc = 0x2ECC71 if pp > 0 else (0xE74C3C if pp < 0 else 0x3498DB)
            embed = discord.Embed(title=f"\U0001f9fe Sim Trades \u2014 {sim_key} (Page {pn}/{total_pages})", color=pc)
            for idx, t in enumerate(pt, start=start + 1):
                tid = str(t.get("trade_id", "N/A"))
                direction = str(t.get("direction", "unknown")).upper()
                ep = _safe_float(t.get("entry_price")); xp = _safe_float(t.get("exit_price"))
                pnl = _safe_float(t.get("realized_pnl_dollars")); qty = t.get("qty")
                et = _format_ts(t.get("entry_time", "N/A")); xt = _format_ts(t.get("exit_time", "N/A"))
                er = t.get("exit_reason", "unknown"); ht = _format_duration(t.get("time_in_trade_seconds"))
                bt = _format_money(balance_after.get(tid)); sl = t.get("sim_id") or sim_key
                prof = profile_map.get(sl); gates = _gate_parts(prof) if isinstance(prof, dict) else []
                gl = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                ept = f"${ep:.4f}" if ep is not None else "N/A"
                xpt = f"${xp:.4f}" if xp is not None else "N/A"
                ppv = _safe_float(t.get("realized_pnl_pct"))
                if ppv is None and ep and xp and ep != 0:
                    try: ppv = (float(xp) - float(ep)) / float(ep)
                    except Exception: ppv = None
                ppt = _format_pct_signed(ppv) if ppv is not None else "N/A"
                badge = _pnl_badge(pnl); mt = _tag_trade_mode(t)
                rt = _extract_reason(t.get("entry_context")); fst = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe")); mae = _safe_float(t.get("mae"))
                fn = f"{badge} {sl} #{idx} {direction} | {tid[:8]}"
                pct_s = f" ({A(ppt, 'cyan')})" if ppt != "N/A" else ""
                ls = [
                    f"{lbl('Mode')} {A(mt, 'magenta')}  {lbl('PnL')} {pnl_col(pnl) if pnl is not None else A('N/A','gray')}{pct_s}  {lbl('Qty')} {A(str(qty), 'white')}",
                    f"{lbl('Entry')} {A(ept, 'white')} @ {A(et, 'gray')}",
                    f"{lbl('Exit')}  {A(xpt, 'white')} @ {A(xt, 'gray')}  {lbl('Reason')} {exit_reason_col(er)}",
                    f"{lbl('Hold')} {A(ht, 'cyan')}  {lbl('Bal')} {balance_col(balance_after.get(tid))}",
                ]
                if gl: ls.append(gl)
                if rt: ls.append(f"{lbl('Signal reason')} {A(rt, 'yellow')}")
                if fst: ls.append(f"{lbl('Feature')} {A(fst, 'white')}")
                if mfe is not None or mae is not None:
                    ls.append(f"{lbl('MFE')} {A(f'{mfe:.2%}' if mfe is not None else 'N/A', 'green')}  {lbl('MAE')} {A(f'{mae:.2%}' if mae is not None else 'N/A', 'red')}")
                embed.add_field(name=fn, value=ab(*ls), inline=False)
            _append_footer(embed, extra=f"Page {pn}/{total_pages}")
            return embed
        await _paginate(ctx, bot, total_pages, _build, page_num)
    except Exception:
        logging.exception("simtrades_error")
        await _send_embed(ctx, "simtrades failed due to an internal error.")



# Re-export second-half handlers from sim_helpers3 to preserve public API
from interface.cogs.sim_helpers3 import (
    handle_simopen,
    handle_simreset,
    handle_simhealth,
    handle_siminfo,
)
