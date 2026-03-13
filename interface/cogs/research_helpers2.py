"""
interface/cogs/research_helpers2.py
Additional helper implementations for research_commands.py.
Extracted from research_commands.py to reduce file size.
No Discord decorators here — only async handler logic.
"""

import os
import re
import io
import json
import pytz
import discord
from datetime import datetime, timedelta

from interface.shared_state import _send_embed, _append_footer, _load_sim_profiles
from interface.fmt import ab, lbl, A, wr_col


# ---------------------------------------------------------------------------
# !ask <option_symbol> — trade chart + AI analysis
# ---------------------------------------------------------------------------

async def handle_ask_trade(ctx, symbol: str):
    """
    !ask <option_symbol>  — Find and analyze any trade matching the contract symbol.
    Example: !ask SPY260303C00670000
    """
    try:
        symbol_upper = symbol.strip().upper()

        # Locate sim data dir and config
        _bot_dir   = os.path.dirname(os.path.abspath(__file__))
        _base_dir  = os.path.dirname(os.path.dirname(_bot_dir))
        _sims_dir  = os.path.join(_base_dir, "data", "sims")
        _cfg_path  = os.path.join(_base_dir, "simulation", "sim_config.yaml")

        # Load sim config
        import asyncio
        import yaml as _yaml
        def _load_yaml():
            with open(_cfg_path) as f:
                return _yaml.safe_load(f) or {}
        sim_cfg = await asyncio.to_thread(_load_yaml)

        # Discover all sim IDs dynamically
        all_sim_ids = [k for k, v in sim_cfg.items()
                       if not str(k).startswith("_") and isinstance(v, dict)]

        # Search all sims for matching trades
        matches = []  # list of (sim_id, trade_dict, profile)
        today_symbols = set()

        for sid in all_sim_ids:
            path = os.path.join(_sims_dir, f"{sid}.json")
            if not os.path.exists(path):
                continue
            try:
                def _load_json(p=path):
                    with open(p) as f:
                        return json.load(f)
                data = await asyncio.to_thread(_load_json)
            except Exception:
                continue
            profile = sim_cfg.get(sid, {})
            for t in (data.get("trade_log") or []):
                opt = (t.get("option_symbol") or "").upper()
                today_symbols.add(opt)
                if opt == symbol_upper:
                    matches.append((sid, t, profile))

        if not matches:
            # Fuzzy: find symbols with same ticker+expiry prefix
            prefix = symbol_upper[:9] if len(symbol_upper) >= 9 else symbol_upper
            similar = sorted({s for s in today_symbols if s.startswith(prefix)})[:8]
            suggestion = (f"\nSimilar contracts: `{'`, `'.join(similar)}`" if similar
                          else "\nNo similar contracts found in trade logs.")
            await ctx.send(f"No trades found for `{symbol_upper}`.{suggestion}")
            return

        # Candle window helper — symbol-aware via get_candle_data
        import pandas as _pd
        from core.data_service import get_candle_data as _get_candle_data

        def _get_window(entry_str, exit_str, symbol=None):
            try:
                e_dt = _pd.to_datetime(entry_str).tz_localize(None) if hasattr(_pd.to_datetime(entry_str), "tz") and _pd.to_datetime(entry_str).tzinfo else _pd.to_datetime(entry_str)
                x_dt = _pd.to_datetime(exit_str).tz_localize(None)  if exit_str and hasattr(_pd.to_datetime(exit_str), "tz") and _pd.to_datetime(exit_str).tzinfo  else (_pd.to_datetime(exit_str) if exit_str else e_dt)
                # Strip tz if present (get_candle_data expects naive ET)
                if hasattr(e_dt, "tzinfo") and e_dt.tzinfo:
                    import pytz as _pytz
                    e_dt = e_dt.astimezone(_pytz.timezone("US/Eastern")).replace(tzinfo=None)
                if hasattr(x_dt, "tzinfo") and x_dt.tzinfo:
                    import pytz as _pytz
                    x_dt = x_dt.astimezone(_pytz.timezone("US/Eastern")).replace(tzinfo=None)
                start = e_dt.to_pydatetime() - timedelta(minutes=30)
                end   = x_dt.to_pydatetime() + timedelta(minutes=10)
                return _get_candle_data(symbol, start, end)
            except Exception:
                return []

        # Send one embed per matching trade
        for (sid, trade, profile) in matches:
            pnl_d    = float(trade.get("realized_pnl_dollars") or 0)
            pnl_pct  = round((trade.get("realized_pnl_pct") or 0) * 100, 1)
            is_win   = pnl_d >= 0
            direction = (trade.get("direction") or "").upper()
            entry_p  = trade.get("entry_price")
            exit_p   = trade.get("exit_price")
            signal   = trade.get("signal_mode") or profile.get("signal_mode", "\u2014")
            exit_rsn = trade.get("exit_reason", "\u2014")

            # Format times
            def _fmt_t(ts):
                if not ts: return "\u2014"
                try:
                    from datetime import datetime as _dt
                    return _dt.fromisoformat(ts).strftime("%H:%M ET")
                except Exception: return str(ts)[:16]

            color = 0x3fb950 if is_win else 0xf85149
            pnl_sign = "+" if pnl_d >= 0 else ""

            # Resolve underlying symbol from trade dict or OCC prefix
            _opt_sym_ask = (trade.get("option_symbol") or "").upper()
            _undl = trade.get("symbol") or ""
            if not _undl:
                _m = re.match(r'^([A-Z]{1,6})', _opt_sym_ask)
                _undl = _m.group(1) if _m else ""

            # Try to get cached narrative or generate new one
            candle_data = _get_window(trade.get("entry_time",""), trade.get("exit_time",""), symbol=_undl)
            narrative = None
            try:
                from analytics.trade_narrator import narrate_trade
                narrative = await narrate_trade(trade, candle_data, profile)
            except Exception:
                pass

            grade   = narrative.get("grade", "") if narrative else ""
            summary = narrative.get("strategy_summary", "") if narrative else ""
            tags    = narrative.get("tags", []) if narrative else []

            # Build embed
            title = f"\U0001f4ca Trade Analysis: {sid} \u2014 {symbol_upper}"
            embed = discord.Embed(title=title, color=color)
            embed.add_field(name="Underlying", value=_undl or "\u2014",     inline=True)
            embed.add_field(name="Strategy",   value=signal,           inline=True)
            embed.add_field(name="Direction",  value=direction or "\u2014", inline=True)
            embed.add_field(name="Grade",      value=grade or "\u2014",     inline=True)
            embed.add_field(name="\U0001f4e5 Entry",
                            value=f"${entry_p:.4f}" if entry_p else "\u2014",
                            inline=True)
            embed.add_field(name="\U0001f4e4 Exit",
                            value=f"${exit_p:.4f} ({exit_rsn})" if exit_p else "\u2014",
                            inline=True)
            embed.add_field(name="\U0001f4b0 P&L",
                            value=f"`{pnl_sign}${pnl_d:.2f} ({pnl_sign}{pnl_pct}%)`",
                            inline=True)
            embed.add_field(name="\u23f1\ufe0f Time",
                            value=f"{_fmt_t(trade.get('entry_time'))} \u2192 {_fmt_t(trade.get('exit_time'))}",
                            inline=True)
            if tags:
                embed.add_field(name="\U0001f3f7\ufe0f Tags", value=", ".join(tags), inline=False)
            if summary:
                embed.description = f"**{summary}**"

            _append_footer(embed)

            # Generate chart and attach
            chart_file = None
            try:
                from charts.trade_chart import generate_trade_chart
                png = generate_trade_chart(trade, candle_data, narrative=narrative,
                                           size=(1000, 500))
                if isinstance(png, bytes):
                    chart_file = discord.File(io.BytesIO(png), filename="trade_chart.png")
                    embed.set_image(url="attachment://trade_chart.png")
            except Exception:
                pass

            if chart_file:
                await ctx.send(embed=embed, file=chart_file)
            else:
                await ctx.send(embed=embed)

            # Follow-up message with full narrative text (avoid embed field limits)
            if narrative:
                entry_r  = narrative.get("entry_reasoning", "")
                exit_r   = narrative.get("exit_reasoning", "")
                outcome  = narrative.get("outcome_analysis", "")
                parts = []
                if entry_r:  parts.append(f"**\U0001f4e5 Entry Reasoning**\n{entry_r}")
                if exit_r:   parts.append(f"**\U0001f4e4 Exit Reasoning**\n{exit_r}")
                if outcome:  parts.append(f"**\U0001f4ca Outcome Analysis**\n{outcome}")
                if parts:
                    followup = "\n\n".join(parts)
                    if len(followup) > 1950:
                        followup = followup[:1950] + "\u2026"
                    await ctx.send(followup)
            elif not narrative or narrative.get("entry_reasoning") == "Analysis unavailable \u2014 GPT service unreachable or API key not set.":
                await ctx.send("*AI analysis unavailable \u2014 OPENAI_API_KEY not set or GPT call failed.*")

    except Exception as e:
        await ctx.send(f"\u274c Error running !ask: {e}")


# ---------------------------------------------------------------------------
# !research subcommand handlers
# ---------------------------------------------------------------------------

async def handle_research_gaps(ctx, args, find_behavior_gaps, generate_all_reports):
    """Handle !research gaps <SIM_ID|all>"""
    if not args:
        embed = discord.Embed(
            title="Usage",
            description=ab(A("!research gaps <SIM_ID>", "cyan"), A("!research gaps all", "cyan")),
            color=0x3498DB,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)
        return

    target = args[0].upper()

    if target == "ALL":
        try:
            reports = generate_all_reports()
            lines = []
            for r in reports:
                sid = r.get("sim_id", "?")
                status = r.get("status", "error")
                if status == "ok":
                    wr = r.get("win_rate", 0)
                    best = (r.get("gaps", {}).get("time_of_day", {}).get("best_bucket") or "?")
                    lines.append(f"{A(sid, 'cyan', bold=True)}: {wr_col(wr)} WR | best: {A(best, 'white')}")
                elif status == "insufficient_data":
                    lines.append(f"{A(sid, 'cyan')}: {A('<10 trades', 'gray')}")
                else:
                    lines.append(f"{A(sid, 'cyan')}: {A('error', 'red')}")

            # Split into <=4096-char embeds
            chunks, current, current_len = [], [], 0
            for line in lines:
                if current and current_len + len(line) + 1 > 3800:
                    chunks.append(current)
                    current, current_len = [], 0
                current.append(line)
                current_len += len(line) + 1
            if current:
                chunks.append(current)

            for idx, chunk in enumerate(chunks):
                title = "Behavior Gap Summary \u2014 All Sims" if idx == 0 else "Behavior Gap Summary (cont.)"
                embed = discord.Embed(title=title, description=ab(*chunk), color=0x3498DB)
                _append_footer(embed)
                await ctx.send(embed=embed)
        except Exception as e:
            embed = discord.Embed(title="Research Error", description=str(e), color=0xE74C3C)
            _append_footer(embed)
            await ctx.send(embed=embed)
        return

    # Single sim gap report
    try:
        report = find_behavior_gaps(target)
        status = report.get("status", "error")
        if status == "insufficient_data":
            embed = discord.Embed(
                title=f"Behavior Gap Report \u2014 {target}",
                description=ab(A(f"Need 10+ closed trades to analyze {target}.", "gray")),
                color=0x95A5A6,
            )
            _append_footer(embed)
            await ctx.send(embed=embed)
            return
        if status == "error":
            embed = discord.Embed(
                title=f"Behavior Gap Report \u2014 {target}",
                description=ab(A(f"Error analyzing {target}.", "red")),
                color=0xE74C3C,
            )
            _append_footer(embed)
            await ctx.send(embed=embed)
            return

        wr = report.get("win_rate", 0)
        gaps = report.get("gaps", {})
        hold = gaps.get("hold_time", {})
        tod  = gaps.get("time_of_day", {})
        exits = gaps.get("exit_reason", {})

        w_hold = hold.get("winners_avg_sec")
        l_hold = hold.get("losers_avg_sec")
        best_bucket   = tod.get("best_bucket") or "\u2014"
        worst_bucket  = tod.get("worst_bucket") or "\u2014"
        winner_exit   = exits.get("winner_dominant") or "\u2014"
        loser_exit    = exits.get("loser_dominant") or "\u2014"

        color = 0x27AE60 if wr > 0.5 else (0xE74C3C if wr < 0.4 else 0xF39C12)
        embed = discord.Embed(title=f"Behavior Gap Report \u2014 {target}", color=color)
        embed.add_field(
            name="Performance",
            value=ab(
                f"{lbl('Win Rate')} {wr_col(wr)}",
                f"{lbl('Trades')} {A(str(report.get('trade_count', '?')), 'white', bold=True)}",
            ),
            inline=False,
        )
        embed.add_field(
            name="Hold Time",
            value=ab(
                f"{lbl('Winner Avg Hold')} {A(f'{w_hold:.0f}s' if w_hold is not None else '\u2014', 'green')}",
                f"{lbl('Loser Avg Hold')} {A(f'{l_hold:.0f}s' if l_hold is not None else '\u2014', 'red')}",
            ),
            inline=False,
        )
        embed.add_field(
            name="Time of Day",
            value=ab(
                f"{lbl('Best Bucket')} {A(best_bucket, 'green')}",
                f"{lbl('Worst Bucket')} {A(worst_bucket, 'red')}",
            ),
            inline=False,
        )
        embed.add_field(
            name="Exit Reasons",
            value=ab(
                f"{lbl('Winner Exit')} {A(winner_exit, 'green')}",
                f"{lbl('Loser Exit')} {A(loser_exit, 'red')}",
            ),
            inline=False,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="Research Error", description=str(e), color=0xE74C3C)
        _append_footer(embed)
        await ctx.send(embed=embed)


async def handle_research_hypotheses(ctx, load_hypotheses):
    """Handle !research hypotheses"""
    try:
        hyps = load_hypotheses()
        if not hyps:
            embed = discord.Embed(
                title="Research Hypotheses",
                description=ab(A("No hypotheses recorded yet.", "gray")),
                color=0x9B59B6,
            )
            _append_footer(embed)
            await ctx.send(embed=embed)
            return
        lines = []
        for h in hyps:
            hid    = h.get("id", "?")
            status = h.get("status", "?")
            claim  = str(h.get("claim", ""))[:60]
            lines.append(f"{A(hid, 'cyan', bold=True)} {A(f'[{status}]', 'white')} \u2014 {claim}")
        embed = discord.Embed(title="Research Hypotheses", description=ab(*lines), color=0x9B59B6)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="Research Error", description=str(e), color=0xE74C3C)
        _append_footer(embed)
        await ctx.send(embed=embed)


async def handle_research_create(ctx, args, build_hypothesis, save_hypothesis, suggest_signal_mode):
    """Handle !research create"""
    if len(args) < 4:
        embed = discord.Embed(
            title="Usage",
            description=ab(
                A('!research create "<source>" "<claim>" "<counter>" "<features>"', "cyan"),
                A("Features are comma-separated: volatility,compression", "gray"),
            ),
            color=0x3498DB,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)
        return
    try:
        source, claim, counter, features_raw = args[0], args[1], args[2], args[3]
        features = [f.strip() for f in features_raw.split(",") if f.strip()]
        hyp = build_hypothesis(source, claim, counter, features)
        save_hypothesis(hyp)
        signal_mode = suggest_signal_mode(features)
        embed = discord.Embed(title="Hypothesis Created", color=0x27AE60)
        embed.add_field(
            name="Details",
            value=ab(
                f"{lbl('ID')} {A(hyp['id'], 'cyan', bold=True)}",
                f"{lbl('Source')} {A(source, 'white')}",
                f"{lbl('Status')} {A(hyp['status'], 'yellow')}",
                f"{lbl('Features')} {A(', '.join(features) or '\u2014', 'white')}",
                f"{lbl('Suggested Mode')} {A(signal_mode, 'green')}",
            ),
            inline=False,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="Research Error", description=str(e), color=0xE74C3C)
        _append_footer(embed)
        await ctx.send(embed=embed)


async def handle_research_compare(ctx, args, find_behavior_gaps):
    """Handle !research compare <SIM_A> <SIM_B>"""
    if len(args) < 2:
        embed = discord.Embed(
            title="Usage",
            description=ab(A("!research compare <SIM_A> <SIM_B>", "cyan")),
            color=0x3498DB,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)
        return
    try:
        sim_a = args[0].upper()
        sim_b = args[1].upper()
        rep_a = find_behavior_gaps(sim_a)
        rep_b = find_behavior_gaps(sim_b)

        for sid, rep in ((sim_a, rep_a), (sim_b, rep_b)):
            if rep.get("status") == "insufficient_data":
                embed = discord.Embed(
                    title=f"Behavior Comparison \u2014 {sim_a} vs {sim_b}",
                    description=ab(A(f"Need 10+ closed trades to analyze {sid}.", "gray")),
                    color=0x95A5A6,
                )
                _append_footer(embed)
                await ctx.send(embed=embed)
                return
            if rep.get("status") == "error":
                embed = discord.Embed(
                    title=f"Behavior Comparison \u2014 {sim_a} vs {sim_b}",
                    description=ab(A(f"Error analyzing {sid}.", "red")),
                    color=0xE74C3C,
                )
                _append_footer(embed)
                await ctx.send(embed=embed)
                return

        def _g(rep, *keys):
            obj = rep.get("gaps", {})
            for k in keys:
                obj = obj.get(k) if isinstance(obj, dict) else None
            return obj

        def _sec(v):
            return f"{v:.0f}s" if v is not None else "\u2014"

        wr_a = rep_a.get("win_rate", 0)
        wr_b = rep_b.get("win_rate", 0)
        wh_a = _g(rep_a, "hold_time", "winners_avg_sec")
        wh_b = _g(rep_b, "hold_time", "winners_avg_sec")
        lh_a = _g(rep_a, "hold_time", "losers_avg_sec")
        lh_b = _g(rep_b, "hold_time", "losers_avg_sec")
        bb_a = _g(rep_a, "time_of_day", "best_bucket") or "\u2014"
        bb_b = _g(rep_b, "time_of_day", "best_bucket") or "\u2014"
        we_a = _g(rep_a, "exit_reason", "winner_dominant") or "\u2014"
        we_b = _g(rep_b, "exit_reason", "winner_dominant") or "\u2014"
        le_a = _g(rep_a, "exit_reason", "loser_dominant") or "\u2014"
        le_b = _g(rep_b, "exit_reason", "loser_dominant") or "\u2014"
        tc_a = rep_a.get("trade_count", "?")
        tc_b = rep_b.get("trade_count", "?")

        embed = discord.Embed(title=f"Behavior Comparison \u2014 {sim_a} vs {sim_b}", color=0x3498DB)
        embed.add_field(name=sim_a, value=ab(
            f"{lbl('Win Rate')} {wr_col(wr_a)}",
            f"{lbl('Winner Hold')} {A(_sec(wh_a), 'green')}",
            f"{lbl('Loser Hold')} {A(_sec(lh_a), 'red')}",
            f"{lbl('Best Bucket')} {A(bb_a, 'white')}",
            f"{lbl('Winner Exit')} {A(we_a, 'green')}",
            f"{lbl('Loser Exit')} {A(le_a, 'red')}",
        ), inline=True)
        embed.add_field(name=sim_b, value=ab(
            f"{lbl('Win Rate')} {wr_col(wr_b)}",
            f"{lbl('Winner Hold')} {A(_sec(wh_b), 'green')}",
            f"{lbl('Loser Hold')} {A(_sec(lh_b), 'red')}",
            f"{lbl('Best Bucket')} {A(bb_b, 'white')}",
            f"{lbl('Winner Exit')} {A(we_b, 'green')}",
            f"{lbl('Loser Exit')} {A(le_b, 'red')}",
        ), inline=True)
        embed.set_footer(text=f"{sim_a}: {tc_a} trades  |  {sim_b}: {tc_b} trades")
        await ctx.send(embed=embed)
    except Exception as e:
        embed = discord.Embed(title="Research Error", description=str(e), color=0xE74C3C)
        _append_footer(embed)
        await ctx.send(embed=embed)
