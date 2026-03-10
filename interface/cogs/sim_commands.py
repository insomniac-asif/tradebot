"""Sim commands cog -- 18 commands migrated from bot.py."""
import os, json, asyncio, logging, pytz
import discord
from discord.ext import commands
from datetime import datetime
from typing import Any

from interface.fmt import ab, lbl, A, pnl_col, wr_col, exit_reason_col, balance_col, drawdown_col
from interface.shared_state import (
    _send_embed, _append_footer, _add_field_icons, _format_ts, _format_pct_signed,
    _format_duration_short, _get_data_freshness_text, _load_sim_profiles,
    _collect_sim_metrics, _safe_float, _tag_trade_mode, STRATEGY_INTENTS,
)
from simulation.sim_portfolio import SimPortfolio
from simulation.sim_watcher import get_sim_last_skip_state
from execution.option_executor import get_option_price

_DATA_SIMS = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims"))


def _format_money(val):
    try: return f"${float(val):,.2f}"
    except (TypeError, ValueError): return "N/A"

def _format_signed_money(val):
    try:
        num = float(val)
        return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
    except (TypeError, ValueError): return "N/A"

def _format_drawdown(val):
    try: num = float(val)
    except (TypeError, ValueError): return "N/A"
    return "$0.00" if num <= 0 else f"-${abs(num):,.2f}"

def _format_pct(val):
    try: return f"{float(val) * 100:.1f}%"
    except (TypeError, ValueError): return "N/A"

def _pnl_badge(val):
    try: num = float(val)
    except (TypeError, ValueError): return "\u26aa"
    return "\u2705" if num > 0 else ("\u274c" if num < 0 else "\u26aa")

def _format_duration(seconds):
    try: total = int(seconds)
    except (TypeError, ValueError): return "N/A"
    if total < 0: return "N/A"
    h, r = divmod(total, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h > 0 else f"{m}m {s}s"

def _extract_reason(ctx_str):
    if not ctx_str or not isinstance(ctx_str, str) or "reason=" not in ctx_str: return None
    try: return ctx_str.rsplit("reason=", 1)[-1].split("|")[0].strip()
    except Exception: return None

def _format_feature_snapshot(fs):
    if not isinstance(fs, dict) or not fs: return None
    def _f(key, fmt="{:.3f}"):
        val = fs.get(key)
        if val is None: return None
        try: return fmt.format(float(val))
        except Exception: return str(val)
    parts = []
    orb_h, orb_l = _f("orb_high", "{:.2f}"), _f("orb_low", "{:.2f}")
    if orb_h and orb_l: parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
    for k, l, c in [("vol_z","Vol Z","yellow"),("atr_expansion","ATR Exp","magenta"),("vwap_z","VWAP Z","cyan"),("close_z","Close Z","cyan"),("iv_rank_proxy","IV Rank","white")]:
        v = _f(k)
        if v: parts.append(f"{lbl(l)} {A(v, c)}")
    return "  |  ".join(parts) if parts else None

def _sim_path(sim_key):
    return os.path.join(_DATA_SIMS, f"{sim_key}.json")

def _compute_breakdown(trade_log, key, order=None):
    stats = {}
    for t in trade_log:
        bucket = t.get(key) if isinstance(t, dict) else None
        bucket = bucket if bucket not in (None, "") else "UNKNOWN"
        stats.setdefault(bucket, {"wins": 0, "total": 0})
        pnl_val = _safe_float(t.get("realized_pnl_dollars"))
        stats[bucket]["total"] += 1
        if pnl_val is not None and pnl_val > 0: stats[bucket]["wins"] += 1
    lines = []
    for k in (order or sorted(stats.keys())):
        if k not in stats: continue
        total = stats[k]["total"]; wins = stats[k]["wins"]
        wr = wins / total if total > 0 else 0
        lines.append(f"{k}: {wins}/{total} ({wr * 100:.1f}%)")
    return "\n".join(lines) if lines else "N/A"

def _ansi_breakdown(text):
    if not text or text == "N/A": return ab(A("N/A", "gray"))
    return ab(*[A(line, "cyan") for line in text.splitlines()])

def _parse_page(page, total_pages):
    page_num = 1
    if isinstance(page, str):
        pt = page.strip().lower()
        if pt.startswith("page"): pt = pt.replace("page", "").strip()
        if pt.isdigit(): page_num = int(pt)
    elif isinstance(page, int):
        page_num = int(page)
    return max(1, min(page_num, total_pages))

def _gate_parts(profile):
    gates = []
    for k in ("orb_minutes", "vol_z_min", "atr_expansion_min"):
        if profile.get(k) is not None:
            gates.append(f"{lbl(k)} {A(str(profile.get(k)), 'white')}")
    return gates

async def _paginate(ctx, bot, total_pages, build_fn, start_page=1):
    page_num = max(1, min(start_page, total_pages))
    embed = build_fn(page_num) if not asyncio.iscoroutinefunction(build_fn) else await build_fn(page_num)
    message = await ctx.send(embed=embed)
    if total_pages <= 1: return
    try:
        for emoji in ("\u25c0\ufe0f", "\u25b6\ufe0f", "\u23f9\ufe0f"):
            await message.add_reaction(emoji)
    except Exception: return
    def _check(reaction, user):
        return user == ctx.author and reaction.message.id == message.id and str(reaction.emoji) in {"\u25c0\ufe0f", "\u25b6\ufe0f", "\u23f9\ufe0f"}
    while True:
        try: reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
        except asyncio.TimeoutError:
            try: await message.clear_reactions()
            except Exception: pass
            break
        emoji = str(reaction.emoji)
        if emoji == "\u23f9\ufe0f":
            try: await message.clear_reactions()
            except Exception: pass
            break
        if emoji == "\u25c0\ufe0f": page_num = total_pages if page_num == 1 else page_num - 1
        elif emoji == "\u25b6\ufe0f": page_num = 1 if page_num == total_pages else page_num + 1
        try:
            e = build_fn(page_num) if not asyncio.iscoroutinefunction(build_fn) else await build_fn(page_num)
            await message.edit(embed=e)
        except Exception: pass
        try: await message.remove_reaction(reaction.emoji, user)
        except Exception: pass


class SimCommands(commands.Cog, name="Sims"):
    def __init__(self, bot): self.bot = bot

    # ── simstats ──────────────────────────────────────────────────────────
    @commands.command(name="simstats")
    async def simstats(self, ctx, sim_id: str | None = None):
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

    # ── simcompare ────────────────────────────────────────────────────────
    @commands.command(name="simcompare")
    async def simcompare(self, ctx):
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

    # ── simleaderboard ────────────────────────────────────────────────────
    @commands.command(name="simleaderboard")
    async def simleaderboard(self, ctx):
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

    # ── simstreaks ────────────────────────────────────────────────────────
    @commands.command(name="simstreaks")
    async def simstreaks(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            if not metrics: await _send_embed(ctx, "No sim data available yet."); return
            eligible = [m for m in metrics if m.get("trades", 0) >= 3]
            if not eligible: await _send_embed(ctx, "Not enough trades to rank streaks (need 3+)."); return
            win_rank = sorted(eligible, key=lambda m: m.get("max_win_streak", 0), reverse=True)[:5]
            loss_rank = sorted(eligible, key=lambda m: m.get("max_loss_streak", 0), reverse=True)[:5]
            embed = discord.Embed(title="\U0001f501 Sim Streaks", color=0x9B59B6)
            wl = [f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_win_streak', 0)), 'green', bold=True)} {lbl('WR')} {wr_col(m.get('win_rate', 0))}" for m in win_rank]
            ll = [f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_loss_streak', 0)), 'red', bold=True)} {lbl('WR')} {wr_col(m.get('win_rate', 0))}" for m in loss_rank]
            embed.add_field(name="\u2705 Longest Win Streaks", value=ab(*wl) if wl else ab(A("N/A","gray")), inline=False)
            embed.add_field(name="\u274c Longest Loss Streaks", value=ab(*ll) if ll else ab(A("N/A","gray")), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simstreaks_error")
            await _send_embed(ctx, "simstreaks failed due to an internal error.")

    # ── simregimes ────────────────────────────────────────────────────────
    @commands.command(name="simregimes")
    async def simregimes(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            if not metrics: await _send_embed(ctx, "No sim data available yet."); return
            lines = []
            for reg in ["TREND", "RANGE", "VOLATILE", "UNKNOWN"]:
                best = None; best_wr = -1.0
                for m in metrics:
                    stats = m.get("regime_stats", {}).get(reg)
                    if not stats or stats.get("total", 0) < 3: continue
                    wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                    if wr > best_wr: best_wr = wr; best = (m["sim_id"], stats["wins"], stats["total"])
                if best:
                    sid, w, t = best
                    lines.append(f"{A(reg, 'cyan')} {A(sid, 'white', bold=True)} {A(f'{w}/{t}', 'white')} {wr_col(w/t)}")
            embed = discord.Embed(title="\U0001f9ed Best by Regime (Win Rate)", color=0x1ABC9C)
            embed.add_field(name="Regime Leaders", value=ab(*lines) if lines else ab(A("No regime data","gray")), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simregimes_error")
            await _send_embed(ctx, "simregimes failed due to an internal error.")

    # ── simtimeofday ──────────────────────────────────────────────────────
    @commands.command(name="simtimeofday")
    async def simtimeofday(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            if not metrics: await _send_embed(ctx, "No sim data available yet."); return
            lines = []
            for bucket in ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE", "UNKNOWN"]:
                best = None; best_wr = -1.0
                for m in metrics:
                    stats = m.get("time_stats", {}).get(bucket)
                    if not stats or stats.get("total", 0) < 3: continue
                    wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                    if wr > best_wr: best_wr = wr; best = (m["sim_id"], stats["wins"], stats["total"])
                if best:
                    sid, w, t = best
                    lines.append(f"{A(bucket, 'cyan')} {A(sid, 'white', bold=True)} {A(f'{w}/{t}', 'white')} {wr_col(w/t)}")
            embed = discord.Embed(title="\U0001f552 Best by Time\u2011of\u2011Day (Win Rate)", color=0x2980B9)
            embed.add_field(name="Time\u2011of\u2011Day Leaders", value=ab(*lines) if lines else ab(A("No time\u2011bucket data","gray")), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simtimeofday_error")
            await _send_embed(ctx, "simtimeofday failed due to an internal error.")

    # ── simpf ─────────────────────────────────────────────────────────────
    @commands.command(name="simpf")
    async def simpf(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("profit_factor") is not None]
            if not eligible: await _send_embed(ctx, "Not enough data for profit factor (need 3+ trades)."); return
            ranked = sorted(eligible, key=lambda m: m.get("profit_factor", 0), reverse=True)[:7]
            lines = []
            for m in ranked:
                pf = m.get("profit_factor")
                lines.append(f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('PF')} {A(f'{pf:.2f}x', 'green' if pf >= 1 else 'red', bold=True)} {lbl('WR')} {wr_col(m.get('win_rate', 0))} {lbl('PnL')} {pnl_col(m.get('total_pnl'))}")
            embed = discord.Embed(title="\U0001f9ee Profit Factor Leaders", color=0x16A085)
            embed.add_field(name="Top Profit Factors", value=ab(*lines), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simpf_error")
            await _send_embed(ctx, "simpf failed due to an internal error.")

    # ── simconsistency ────────────────────────────────────────────────────
    @commands.command(name="simconsistency")
    async def simconsistency(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("pnl_stdev") is not None]
            if not eligible: await _send_embed(ctx, "Not enough data for consistency (need 3+ trades)."); return
            ranked = sorted(eligible, key=lambda m: m.get("pnl_stdev", 0))[:7]
            lines = [f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('\u03c3')} {pnl_col(m.get('pnl_stdev'))} {lbl('Median')} {pnl_col(m.get('pnl_median'))} {lbl('WR')} {wr_col(m.get('win_rate', 0))}" for m in ranked]
            embed = discord.Embed(title="\U0001f4cf Most Consistent Sims", color=0x8E44AD)
            embed.add_field(name="Lowest PnL Volatility", value=ab(*lines), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simconsistency_error")
            await _send_embed(ctx, "simconsistency failed due to an internal error.")

    # ── simexits ──────────────────────────────────────────────────────────
    @commands.command(name="simexits")
    async def simexits(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            if not metrics: await _send_embed(ctx, "No sim data available yet."); return
            lines = []
            for reason in ["profit_target", "trailing_stop", "stop_loss", "eod_daytrade_close", "hold_max_elapsed"]:
                best = None; best_rate = -1.0
                for m in metrics:
                    total = m.get("trades", 0)
                    if total < 3: continue
                    count = m.get("exit_counts", {}).get(reason, 0)
                    rate = count / total if total > 0 else 0.0
                    if rate > best_rate: best_rate = rate; best = (m["sim_id"], count, total)
                if best:
                    sid, c, t = best
                    lines.append(f"{A(reason, 'cyan')} {A(sid, 'white', bold=True)} {A(f'{c}/{t}', 'white')} {wr_col(c/t)}")
            embed = discord.Embed(title="\U0001f3af Best Exit Hit Rates", color=0xF39C12)
            embed.add_field(name="Exit Reason Leaders", value=ab(*lines) if lines else ab(A("No exit data","gray")), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simexits_error")
            await _send_embed(ctx, "simexits failed due to an internal error.")

    # ── simhold ───────────────────────────────────────────────────────────
    @commands.command(name="simhold")
    async def simhold(self, ctx):
        try:
            metrics, profiles = _collect_sim_metrics()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("avg_hold") is not None]
            if not eligible: await _send_embed(ctx, "Not enough data for hold\u2011time stats (need 3+ trades)."); return
            fastest = sorted(eligible, key=lambda m: m.get("avg_hold", 0))[:5]
            slowest = sorted(eligible, key=lambda m: m.get("avg_hold", 0), reverse=True)[:5]
            fl = [f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}" for m in fastest]
            sl = [f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}" for m in slowest]
            embed = discord.Embed(title="\u23f1 Sim Hold\u2011Time Leaders", color=0x2C3E50)
            embed.add_field(name="Fastest Average Holds", value=ab(*fl), inline=False)
            embed.add_field(name="Slowest Average Holds", value=ab(*sl), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simhold_error")
            await _send_embed(ctx, "simhold failed due to an internal error.")

    # ── simdte ────────────────────────────────────────────────────────────
    @commands.command(name="simdte")
    async def simdte(self, ctx):
        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data","gray")), inline=False); return
            chunks, cur, cur_len = [], [], 0
            for line in lines:
                est = len(line) + 1
                if cur and cur_len + est > 900: chunks.append(cur); cur = [line]; cur_len = len(line)
                else: cur.append(line); cur_len += est
            if cur: chunks.append(cur)
            for idx, chunk in enumerate(chunks):
                embed.add_field(name=title if idx == 0 else f"{title} (cont.)", value=ab(*chunk), inline=False)
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

    # ── simsetups ─────────────────────────────────────────────────────────
    @commands.command(name="simsetups")
    async def simsetups(self, ctx):
        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data","gray")), inline=False); return
            chunks, cur, cur_len = [], [], 0
            for line in lines:
                est = len(line) + 1
                if cur and cur_len + est > 900: chunks.append(cur); cur = [line]; cur_len = len(line)
                else: cur.append(line); cur_len += est
            if cur: chunks.append(cur)
            for idx, chunk in enumerate(chunks):
                embed.add_field(name=title if idx == 0 else f"{title} (cont.)", value=ab(*chunk), inline=False)
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

    # ── simtrades ─────────────────────────────────────────────────────────
    @commands.command(name="simtrades")
    async def simtrades(self, ctx, sim_id: str | None = None, page: str | int = 1):
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
            await _paginate(ctx, self.bot, total_pages, _build, page_num)
        except Exception:
            logging.exception("simtrades_error")
            await _send_embed(ctx, "simtrades failed due to an internal error.")

    # ── simopen ───────────────────────────────────────────────────────────
    @commands.command(name="simopen")
    async def simopen(self, ctx, sim_id: str | None = None, page: str | int = 1):
        def _parse_strike_from_symbol(symbol):
            if not symbol or not isinstance(symbol, str): return None
            try: return int(symbol[-8:]) / 1000.0
            except Exception: return None
        def _contract_label(symbol, direction, expiry, strike):
            cp = None
            if isinstance(direction, str):
                d = direction.lower()
                if d == "bullish": cp = "CALL"
                elif d == "bearish": cp = "PUT"
            if cp is None and isinstance(symbol, str) and len(symbol) >= 10:
                try:
                    c = symbol[9]
                    if c == "C": cp = "CALL"
                    elif c == "P": cp = "PUT"
                except Exception: cp = None
            et = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
            label = "SPY"
            if cp: label = f"{label} {cp}"
            if et: label = f"{label} {et}"
            if strike is None: strike = _parse_strike_from_symbol(symbol)
            if isinstance(strike, (int, float)): label = f"{label} {strike:g}"
            return label
        try:
            profiles = _load_sim_profiles()
            profile_map = profiles if isinstance(profiles, dict) else {}
            trades = []
            if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
                sim_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
            else:
                sk = sim_id.strip().upper()
                if sk not in profiles: await _send_embed(ctx, "Unknown sim ID."); return
                sim_keys = [sk]
            for sk in sim_keys:
                try:
                    sp = _sim_path(sk)
                    if not os.path.exists(sp): continue
                    sim = SimPortfolio(sk, profiles.get(sk, {})); sim.load()
                    ot = sim.open_trades if isinstance(sim.open_trades, list) else []
                    for t in ot:
                        tc = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                        tc["sim_id"] = sk; trades.append(tc)
                except Exception: continue
            if not trades: await _send_embed(ctx, "No open sim trades."); return
            def _pts(val):
                if val is None: return None
                if isinstance(val, datetime): return val
                try: return datetime.fromisoformat(str(val))
                except Exception: return None
            trades.sort(key=lambda t: _pts(t.get("entry_time")) or datetime.min, reverse=True)
            per_page = 5; total = len(trades); total_pages = (total + per_page - 1) // per_page
            page_num = _parse_page(page, total_pages)
            if page_num < 1 or page_num > total_pages:
                await _send_embed(ctx, f"Invalid page. Use `!simopen {sim_id or 'all'} 1` to `!simopen {sim_id or 'all'} {total_pages}`."); return
            async def _build(pn):
                pn = max(1, min(pn, total_pages))
                start = (pn - 1) * per_page; end = start + per_page
                pt = trades[start:end]
                embed = discord.Embed(title=f"\U0001f4cc Open Sim Trades (Page {pn}/{total_pages})", color=0x3498DB)
                now_et = datetime.now(pytz.timezone("US/Eastern"))
                for idx, t in enumerate(pt, start=start + 1):
                    tid = str(t.get("trade_id", "N/A")); sl = t.get("sim_id") or "SIM"
                    prof = profile_map.get(sl); gates = _gate_parts(prof) if isinstance(prof, dict) else []
                    gl = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                    direction = str(t.get("direction") or t.get("type") or "unknown").upper()
                    osym = t.get("option_symbol"); expiry = t.get("expiry")
                    strike = _safe_float(t.get("strike")); qty = t.get("qty") or t.get("quantity")
                    ep = _safe_float(t.get("entry_price"))
                    en = _safe_float(t.get("entry_notional"))
                    if en is None and ep is not None and qty is not None:
                        try: en = float(ep) * float(qty) * 100
                        except Exception: en = None
                    et = _format_ts(t.get("entry_time", "N/A"))
                    hs = None
                    try:
                        dt = _pts(t.get("entry_time"))
                        if dt is not None:
                            eastern = pytz.timezone("America/New_York")
                            dt = eastern.localize(dt) if dt.tzinfo is None else dt.astimezone(eastern)
                            hs = (now_et - dt).total_seconds()
                    except Exception: hs = None
                    cp = None
                    if osym:
                        try: cp = await asyncio.to_thread(get_option_price, osym)
                        except Exception: cp = None
                    pv = None
                    if cp is not None and ep is not None and qty is not None:
                        try: pv = (float(cp) - float(ep)) * float(qty) * 100
                        except Exception: pv = None
                    ept = f"${ep:.4f}" if ep is not None else "N/A"
                    nt = f"${float(cp):.4f}" if cp is not None else "N/A"
                    ct = _format_money(en) if en is not None else "N/A"
                    ht = _format_duration(hs); cl = _contract_label(osym, direction, expiry, strike)
                    rt = _extract_reason(t.get("entry_context")); fst = _format_feature_snapshot(t.get("feature_snapshot"))
                    mfe = _safe_float(t.get("mfe_pct")); mae = _safe_float(t.get("mae_pct"))
                    fn = f"\U0001f7e1 {sl} #{idx} {direction} | {tid[:8]}"
                    ls = [
                        f"{lbl('Contract')} {A(cl, 'magenta', bold=True)}",
                        f"{lbl('Qty')} {A(str(qty), 'white')}  {lbl('Entry')} {A(ept, 'white')}  {lbl('Cost')} {A(ct, 'white')}",
                        f"{lbl('Now')} {A(nt, 'white')}  {lbl('PnL')} {pnl_col(pv) if pv is not None else A('N/A','gray')}",
                        f"{lbl('Hold')} {A(ht, 'cyan')}  {lbl('Entry Time')} {A(et, 'gray')}",
                    ]
                    if gl: ls.append(gl)
                    if rt: ls.append(f"{lbl('Signal reason')} {A(rt, 'yellow')}")
                    if fst: ls.append(f"{lbl('Feature')} {A(fst, 'white')}")
                    if mfe is not None or mae is not None:
                        ls.append(f"{lbl('MFE')} {A(f'{mfe:.2%}' if mfe is not None else 'N/A', 'green')}  {lbl('MAE')} {A(f'{mae:.2%}' if mae is not None else 'N/A', 'red')}")
                    embed.add_field(name=fn, value=ab(*ls), inline=False)
                _append_footer(embed, extra=f"Page {pn}/{total_pages}")
                return embed
            await _paginate(ctx, self.bot, total_pages, _build, page_num)
        except Exception:
            logging.exception("simopen_error")
            await _send_embed(ctx, "simopen failed due to an internal error.")

    # ── simreset ──────────────────────────────────────────────────────────
    @commands.command(name="simreset")
    async def simreset(self, ctx, sim_id: str | None = None):
        if sim_id is None:
            await _send_embed(ctx, "Usage: `!simreset SIM03`, `!simreset all`, or `!simreset live`"); return
        try:
            profiles = _load_sim_profiles()
            sim_key = sim_id.strip().upper()
            def _reset_one(sk, pr):
                try:
                    sp = _sim_path(sk)
                    if os.path.exists(sp):
                        try: os.remove(sp)
                        except Exception: logging.exception("simreset_remove_failed")
                    sim = SimPortfolio(sk, pr); sim.load(); sim.save()
                    return True, "reset"
                except Exception:
                    logging.exception("simreset_one_failed"); return False, "error"
            if sim_key == "ALL":
                target_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
            elif sim_key == "LIVE":
                target_keys = sorted([k for k, p in profiles.items() if p.get("execution_mode") == "live"])
            else:
                if profiles.get(sim_key) is None: await _send_embed(ctx, "Unknown sim ID."); return
                target_keys = [sim_key]
            if not target_keys: await _send_embed(ctx, "No sims matched your reset request."); return
            results = [(k, *_reset_one(k, profiles.get(k, {}))) for k in target_keys]
            title = f"\u2705 Sim Reset \u2014 {sim_key}" if sim_key in {"ALL", "LIVE"} else f"\u2705 Sim Reset \u2014 {target_keys[0]}"
            embed = discord.Embed(title=title, color=0x2ECC71)
            ok_keys = [k for k, ok, _ in results if ok]; fail_keys = [k for k, ok, _ in results if not ok]
            for k, ok, status in results:
                sb = profiles.get(k, {}).get("balance_start", 0.0)
                embed.add_field(name=_add_field_icons(k), value=f"{'Reset to starting balance.' if ok else 'Reset failed.'} Start: ${float(sb):,.2f}", inline=False)
            if len(results) > 1:
                parts = []
                if ok_keys: parts.append(f"Reset: {', '.join(ok_keys)}")
                if fail_keys: parts.append(f"Failed: {', '.join(fail_keys)}")
                if parts: embed.add_field(name=_add_field_icons("Summary"), value=" | ".join(parts), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("simreset_error")
            await _send_embed(ctx, "simreset failed due to an internal error.")

    # ── simhealth ─────────────────────────────────────────────────────────
    @commands.command(name="simhealth")
    async def simhealth(self, ctx, page: str | int | None = None):
        required_keys = ["signal_mode","dte_min","dte_max","balance_start","risk_per_trade_pct","daily_loss_limit_pct","max_open_trades","exposure_cap_pct","max_spread_pct","cutoff_time_et"]
        try:
            profiles = _load_sim_profiles()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            sim_items = [(k, v) for k, v in profiles.items() if k.startswith("SIM")]
            validator_summary = None; validator_details = []
            try:
                from simulation.sim_validator import collect_sim_validation
                errors, total_errors = collect_sim_validation()
                validator_summary = "OK" if total_errors == 0 else f"FAIL ({total_errors} issues)"
                if total_errors > 0: validator_details = errors[:3]
            except Exception: validator_summary = "FAIL (validator error)"
            def _build(pn):
                pt = max(1, (len(sim_items) + 2) // 3)
                pi = max(1, min(pn, pt)) - 1; s = pi * 3; e = s + 3
                embed = discord.Embed(title=f"\U0001f9ea Sim Health Check \u2014 Page {pi+1}/{pt}", color=0x3498DB)
                if validator_summary:
                    vl = [A(validator_summary, "green" if validator_summary.startswith("OK") else "red", bold=True)]
                    for line in validator_details:
                        sev = "red" if any(tok in line for tok in ("missing:", "cutoff_format_invalid", "orb_requires_features")) else "yellow"
                        vl.append(A(line, sev))
                    embed.add_field(name="SIM Validator", value=ab(*vl), inline=False)
                embed.add_field(name="SIM Profiles Loaded", value=ab(A(str(len(sim_items)), "white", bold=True)), inline=True)
                for sid, profile in sim_items[s:e]:
                    try:
                        sp = _sim_path(sid); fe = os.path.exists(sp)
                        fs = "\u2705" if fe else "\u274c"
                        mk = [k for k in required_keys if k not in profile]
                        mt = ", ".join(mk) if mk else "None"
                        if not fe:
                            embed.add_field(name=sid, value=ab(f"{lbl('File')} {A(fs, 'red', bold=True)}", f"{A('Not initialized', 'yellow')}", f"{lbl('Missing keys')} {A(mt, 'cyan')}"), inline=False); continue
                        try:
                            with open(sp, "r", encoding="utf-8") as f: data = json.load(f)
                        except Exception:
                            embed.add_field(name=sid, value=ab(f"{lbl('File')} {A(fs, 'green', bold=True)}", f"{lbl('Schema')} {A('\u26a0\ufe0f', 'yellow', bold=True)}", f"{lbl('Missing keys')} {A(mt, 'cyan')}"), inline=False); continue
                        so = "\u2705" if data.get("schema_version") is not None else "\u26a0\ufe0f"
                        ot = data.get("open_trades"); tl = data.get("trade_log")
                        oc = len(ot) if isinstance(ot, list) else 0; tc = len(tl) if isinstance(tl, list) else 0
                        bv = _safe_float(data.get("balance")); pb = _safe_float(data.get("peak_balance"))
                        dlv = _safe_float(data.get("daily_loss"))
                        dp = None
                        try:
                            today = datetime.now(pytz.timezone("US/Eastern")).date()
                            total = 0.0; sb = _safe_float(profile.get("balance_start")) or 0.0; rb = sb; cpk = sb
                            for t in (tl if isinstance(tl, list) else []):
                                pv = _safe_float(t.get("realized_pnl_dollars")) or 0.0; rb += pv
                                if rb > cpk: cpk = rb
                                xt = t.get("exit_time")
                                if not xt: continue
                                dt = datetime.fromisoformat(str(xt))
                                if dt.tzinfo is None: dt = pytz.timezone("US/Eastern").localize(dt)
                                if dt.date() == today: total += pv
                            dp = total
                            pb = max(pb, cpk) if pb is not None else cpk
                        except Exception: dp = None
                        lr = "N/A"
                        try:
                            ctx_val = None
                            if isinstance(tl, list):
                                for t in reversed(tl):
                                    if isinstance(t, dict) and t.get("entry_context"): ctx_val = t.get("entry_context"); break
                            if ctx_val is None and isinstance(ot, list):
                                for t in reversed(ot):
                                    if isinstance(t, dict) and t.get("entry_context"): ctx_val = t.get("entry_context"); break
                            if ctx_val and "reason=" in ctx_val: lr = ctx_val.split("reason=")[-1].split("|")[0].strip()
                        except Exception: lr = "N/A"
                        gates = _gate_parts(profile)
                        gt = "  |  ".join(gates) if gates else None
                        vlines = [
                            f"{lbl('File')} {A(fs, 'green', bold=True)}",
                            f"{lbl('Schema')} {A(so, 'green' if so == '\u2705' else 'yellow', bold=True)}",
                            f"{lbl('Open trades')} {A(str(oc), 'white')}",
                            f"{lbl('Trade log')} {A(str(tc), 'white')}",
                            f"{lbl('Balance')} {balance_col(bv)}",
                            f"{lbl('Peak balance')} {balance_col(pb)}",
                            f"{lbl('Daily PnL')} {pnl_col(dp) if dp is not None else A('N/A','gray')}",
                            f"{lbl('Daily loss')} {pnl_col(-abs(dlv)) if dlv is not None else A('N/A','gray')}",
                            f"{lbl('Missing keys')} {A(mt, 'cyan')}",
                            f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                            f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                        ]
                        if gt: vlines.append(f"{lbl('gates')} {A(gt, 'white')}")
                        vlines.append(f"{lbl('last_reason')} {A(lr, 'yellow')}")
                        embed.add_field(name=sid, value=ab(*vlines), inline=False)
                    except Exception:
                        embed.add_field(name=sid, value="Error reading sim data", inline=False)
                embed.set_footer(text=f"Checked: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}")
                _append_footer(embed)
                return embed
            pages_count = max(1, (len(sim_items) + 2) // 3)
            pn = _parse_page(page, pages_count) if page is not None else 1
            await _paginate(ctx, self.bot, pages_count, _build, pn)
        except Exception:
            logging.exception("simhealth_error")
            await _send_embed(ctx, "simhealth failed due to an internal error.")

    # ── siminfo ───────────────────────────────────────────────────────────
    @commands.command(name="siminfo")
    async def siminfo(self, ctx, sim_id: str | int | None = None):
        def _norm(raw):
            if raw is None: return None
            text = str(raw).strip().upper()
            if not text: return None
            if text.startswith("SIM"):
                suffix = text.replace("SIM", "").strip()
                return f"SIM{int(suffix):02d}" if suffix.isdigit() else text
            return f"SIM{int(text):02d}" if text.isdigit() else None
        def _fmt_secs(seconds):
            try: s = int(seconds)
            except (TypeError, ValueError): return "N/A"
            if s < 60: return f"{s}s"
            if s < 3600: return f"{s // 60}m"
            if s < 86400: return f"{s / 3600:.1f}h"
            return f"{s / 86400:.1f}d"
        def _dte_tier(dmin, dmax):
            try: dmin, dmax = int(dmin), int(dmax)
            except (TypeError, ValueError): return "DTE: N/A"
            if dmin == 0 and dmax == 0: return "0DTE only"
            if dmin == 0 and dmax == 1: return "0\u20131 DTE intraday"
            if dmin == 1 and dmax == 1: return "1 DTE intraday"
            if dmin == 1 and dmax == 3: return "1\u20133 DTE intraday"
            if dmin >= 7 and dmax <= 10: return "7\u201310 DTE swing"
            if dmin >= 14 and dmax <= 21: return "14\u201321 DTE swing"
            return f"DTE range {dmin}\u2013{dmax}"
        try:
            profiles = _load_sim_profiles()
            if not profiles: await _send_embed(ctx, "No sim profiles found."); return
            sid = _norm(sim_id)
            if sid is None: await _send_embed(ctx, "Usage: `!siminfo 0-11` or `!siminfo SIM03`."); return
            profile = profiles.get(sid)
            if not profile: await _send_embed(ctx, f"{sid} not found in sim_config.yaml."); return
            name = profile.get("name", sid); mode = str(profile.get("signal_mode", "N/A"))
            horizon = str(profile.get("horizon", "N/A"))
            dmin = profile.get("dte_min"); dmax = profile.get("dte_max")
            hmin = _fmt_secs(profile.get("hold_min_seconds")); hmax = _fmt_secs(profile.get("hold_max_seconds"))
            cutoff = profile.get("cutoff_time_et", "N/A"); fe = profile.get("features_enabled", False)
            em = profile.get("execution_mode", "sim").upper()
            def _pct_text(key):
                try: return f"{float(profile.get(key, 0)) * 100:.2f}%"
                except (TypeError, ValueError): return "N/A"
            rpt = _pct_text("risk_per_trade_pct"); dlpt = _pct_text("daily_loss_limit_pct")
            try: ept = f"{float(profile.get('exposure_cap_pct', 0)) * 100:.1f}%"
            except (TypeError, ValueError): ept = "N/A"
            try: spt = f"{float(profile.get('stop_loss_pct', 0)) * 100:.1f}%"
            except (TypeError, ValueError): spt = "N/A"
            try: tpt = f"{float(profile.get('profit_target_pct', 0)) * 100:.1f}%"
            except (TypeError, ValueError): tpt = "N/A"
            try: slip = f"in {profile.get('entry_slippage', 'N/A')} / out {profile.get('exit_slippage', 'N/A')}"
            except Exception: slip = "N/A"
            gates = []
            if profile.get("regime_filter"): gates.append(f"{lbl('regime')} {A(str(profile.get('regime_filter')), 'yellow')}")
            for k in ("orb_minutes", "vol_z_min", "atr_expansion_min"):
                if profile.get(k) is not None: gates.append(f"{lbl(k)} {A(str(profile.get(k)), 'white')}")
            gt = "  |  ".join(gates) if gates else "None"
            embed = discord.Embed(title=f"\U0001f9e0 {sid} \u2014 {name}", color=0x3498DB)
            embed.add_field(name="Strategy Intent", value=ab(A(STRATEGY_INTENTS.get(sid, "Configured strategy profile."), "white")), inline=False)
            embed.add_field(name="Profile", value=ab(f"{lbl('signal_mode')} {A(mode, 'magenta', bold=True)}", f"{lbl('horizon')} {A(horizon, 'cyan')}", f"{lbl('execution')} {A(em, 'yellow')}", f"{lbl('features_enabled')} {A(str(fe), 'cyan')}", f"{lbl('dte_tier')} {A(_dte_tier(dmin, dmax), 'white')}"), inline=False)
            embed.add_field(name="Timing", value=ab(f"{lbl('DTE range')} {A(f'{dmin}\u2013{dmax}', 'white')}", f"{lbl('hold_min')} {A(hmin, 'white')}  |  {lbl('hold_max')} {A(hmax, 'white')}", f"{lbl('cutoff')} {A(str(cutoff), 'white')}"), inline=False)
            embed.add_field(name="Risk / Exposure", value=ab(f"{lbl('risk/trade')} {A(rpt, 'white')}", f"{lbl('daily_loss')} {A(dlpt, 'white')}", f"{lbl('max_open')} {A(str(profile.get('max_open_trades', 'N/A')), 'white')}", f"{lbl('exposure_cap')} {A(ept, 'white')}"), inline=False)
            embed.add_field(name="Stops / Targets", value=ab(f"{lbl('stop_loss')} {A(spt, 'red')}", f"{lbl('profit_target')} {A(tpt, 'green')}", f"{lbl('trail_activate')} {A(str(profile.get('trailing_stop_activate_pct', 'N/A')), 'white')}", f"{lbl('trail_pct')} {A(str(profile.get('trailing_stop_trail_pct', 'N/A')), 'white')}"), inline=False)
            embed.add_field(name="Entry / Selection", value=ab(f"{lbl('otm_pct')} {A(str(profile.get('otm_pct', 'N/A')), 'white')}", f"{lbl('max_spread')} {A(str(profile.get('max_spread_pct', 'N/A')), 'white')}", f"{lbl('slippage')} {A(slip, 'white')}", f"{lbl('gates')} {A(gt, 'white')}"), inline=False)
            embed.set_footer(text=f"Loaded: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}")
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("siminfo_error")
            await _send_embed(ctx, "siminfo failed due to an internal error.")

    # ── lastskip ──────────────────────────────────────────────────────────
    @commands.command(name="lastskip")
    async def lastskip(self, ctx):
        try:
            reason = getattr(self.bot, "last_skip_reason", None)
            ts = getattr(self.bot, "last_skip_time", None)
            ts_text = _format_ts(ts) if ts is not None else "N/A"
            sim_state = get_sim_last_skip_state()
            sim_lines = []
            for sid in sorted(sim_state.keys()):
                item = sim_state.get(sid, {})
                sr = item.get("reason") or "N/A"
                st = item.get("time")
                st_text = _format_ts(st) if st is not None else "N/A"
                sim_lines.append(f"{sid}: {sr} ({st_text})")
            sim_text = "\n".join(sim_lines) if sim_lines else "None"
            embed = discord.Embed(title="\u23f8 Last Skip Reasons", color=0xF39C12)
            trader_text = f"{reason} ({ts_text})" if reason else "None"
            embed.add_field(name=_add_field_icons("Trader"), value=ab(A(trader_text, "yellow")), inline=False)
            embed.add_field(name=_add_field_icons("Sims"), value=ab(A(sim_text, "yellow")), inline=False)
            _append_footer(embed); await ctx.send(embed=embed)
        except Exception:
            logging.exception("lastskip_error")
            await _send_embed(ctx, "lastskip failed due to an internal error.")


async def setup(bot):
    await bot.add_cog(SimCommands(bot))
