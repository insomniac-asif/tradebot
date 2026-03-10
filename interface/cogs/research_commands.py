"""
interface/cogs/research_commands.py
AI coach and research Discord commands.

Commands: !ask  !askmore  !research (gaps, hypotheses, create, compare)
"""

import logging
import os
import re
import json
import pytz
import discord
from datetime import datetime, timedelta
from discord.ext import commands

from interface.shared_state import (
    _send_embed,
    _append_footer,
    _load_sim_profiles,
    ASK_CONTEXT_CACHE,
    STRATEGY_INTENTS,
)
from interface.fmt import ab, lbl, A, wr_col
from interface.ai_assistant import ask_ai
from analytics.performance import get_paper_stats, get_career_stats
from core.account_repository import load_account

try:
    from research.behavior_divergence import find_behavior_gaps, generate_all_reports
    from research.hypothesis_builder import build_hypothesis, save_hypothesis, load_hypotheses
    from research.sim_generator import suggest_signal_mode
    _RESEARCH_AVAILABLE = True
except ImportError:
    _RESEARCH_AVAILABLE = False


class ResearchCommands(commands.Cog, name="Research"):
    """AI coach and research pipeline commands."""

    def __init__(self, bot):
        self.bot = bot

    # ── helper ────────────────────────────────────────────────────────────────

    async def _ask_trade_impl(self, ctx, symbol: str):
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
            import yaml as _yaml
            with open(_cfg_path) as f:
                sim_cfg = _yaml.safe_load(f) or {}

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
                    with open(path) as f:
                        data = json.load(f)
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

            def _get_window(entry_str, exit_str, symbol="SPY"):
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
                trade_id = trade.get("trade_id", "")

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
                    _undl = _m.group(1) if _m else "SPY"

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
                        import io as _io
                        chart_file = discord.File(_io.BytesIO(png), filename="trade_chart.png")
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

    # ── !ask ──────────────────────────────────────────────────────────────────

    @commands.command(name="ask")
    async def cmd_ask(self, ctx, *, question=None):

        if not question:
            await _send_embed(ctx,
                "Usage:\n"
                "\u2022 `!ask <option_contract>` \u2014 trade chart + AI analysis  "
                "(e.g. `!ask SPY260310C00592000`)\n"
                "\u2022 `!ask <question>` \u2014 AI reviews your performance  "
                "(e.g. `!ask Did I overtrade?`)"
            )
            return

        # If the argument looks like an OCC option symbol, route to trade analysis
        if re.match(r'^[A-Z]{1,6}\d{6}[CP]\d+$', question.strip().upper()):
            await self._ask_trade_impl(ctx, question.strip().upper())
            return

        def _norm_sim_id(raw: str) -> str | None:
            text = str(raw).strip().upper()
            if not text:
                return None
            if text.startswith("SIM"):
                suffix = text.replace("SIM", "").strip()
                if suffix.isdigit():
                    return f"SIM{int(suffix):02d}"
                return text
            if text.isdigit():
                return f"SIM{int(text):02d}"
            return None

        def _fmt_pct(val, decimals=1) -> str:
            try:
                return f"{float(val) * 100:.{decimals}f}%"
            except (TypeError, ValueError):
                return "N/A"

        def _fmt_secs(seconds) -> str:
            try:
                s = int(seconds)
            except (TypeError, ValueError):
                return "N/A"
            if s < 60:
                return f"{s}s"
            if s < 3600:
                return f"{s // 60}m"
            if s < 86400:
                return f"{s / 3600:.1f}h"
            return f"{s / 86400:.1f}d"

        strategy_intents = STRATEGY_INTENTS

        def _build_sim_context(question_text: str) -> str | None:
            q = question_text.lower()
            if "sim" not in q:
                return None

            profiles = _load_sim_profiles()
            if not profiles:
                return None

            found = set()
            for match in re.findall(r"\bSIM\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
                norm = _norm_sim_id(match)
                if norm:
                    found.add(norm)
            for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
                norm = _norm_sim_id(match.replace("sim", "SIM"))
                if norm:
                    found.add(norm)

            include_all = ("all sims" in q) or ("all sim" in q) or (len(found) == 0)
            sim_ids = sorted([k for k in profiles.keys() if k.startswith("SIM")]) if include_all else sorted(found)

            lines = []
            for sid in sim_ids:
                profile = profiles.get(sid)
                if not isinstance(profile, dict):
                    continue
                name = profile.get("name", sid)
                mode = profile.get("signal_mode", "N/A")
                horizon = profile.get("horizon", "N/A")
                exec_mode = str(profile.get("execution_mode", "sim")).upper()
                dte_min = profile.get("dte_min", "N/A")
                dte_max = profile.get("dte_max", "N/A")
                hold_min = _fmt_secs(profile.get("hold_min_seconds"))
                hold_max = _fmt_secs(profile.get("hold_max_seconds"))
                cutoff = profile.get("cutoff_time_et", "N/A")
                stop_pct = _fmt_pct(profile.get("stop_loss_pct"))
                target_pct = _fmt_pct(profile.get("profit_target_pct"))
                risk_pct = _fmt_pct(profile.get("risk_per_trade_pct"), 2)
                daily_loss = _fmt_pct(profile.get("daily_loss_limit_pct"), 2)
                max_open = profile.get("max_open_trades", "N/A")
                max_spread = profile.get("max_spread_pct", "N/A")
                features = profile.get("features_enabled", False)

                gates = []
                if profile.get("regime_filter"):
                    gates.append(f"regime={profile.get('regime_filter')}")
                if profile.get("orb_minutes") is not None:
                    gates.append(f"orb_minutes={profile.get('orb_minutes')}")
                if profile.get("vol_z_min") is not None:
                    gates.append(f"vol_z_min={profile.get('vol_z_min')}")
                if profile.get("atr_expansion_min") is not None:
                    gates.append(f"atr_expansion_min={profile.get('atr_expansion_min')}")
                gate_text = ("gates: " + ", ".join(gates)) if gates else "gates: none"

                # Sim state (if available)
                state_line = None
                try:
                    sim_path = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
                    )
                    if os.path.exists(sim_path):
                        with open(sim_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        open_count = len(data.get("open_trades", [])) if isinstance(data.get("open_trades"), list) else 0
                        trade_count = len(data.get("trade_log", [])) if isinstance(data.get("trade_log"), list) else 0
                        balance = data.get("balance")
                        peak = data.get("peak_balance")
                        state_line = f"state: open={open_count} trades={trade_count} balance={balance} peak={peak}"
                except Exception:
                    state_line = None

                lines.append(f"{sid} \u2014 {name}")
                lines.append(f"strategy: {strategy_intents.get(sid, 'Configured strategy profile.')}")
                lines.append(f"mode={mode} horizon={horizon} exec={exec_mode} features={features}")
                lines.append(f"DTE {dte_min}-{dte_max} | hold {hold_min}-{hold_max} | cutoff {cutoff}")
                lines.append(f"risk {risk_pct} | daily_loss {daily_loss} | max_open {max_open} | max_spread {max_spread}")
                lines.append(f"stop {stop_pct} | target {target_pct}")
                lines.append(gate_text)
                if state_line:
                    lines.append(state_line)
                lines.append("\u2014")

            context = "\n".join(lines).strip()
            if len(context) > 2500:
                context = context[:2500] + "\n\u2026(truncated)"
            return context

        def _extract_target_sims(question_text: str) -> list[str]:
            found = set()
            for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
                norm = _norm_sim_id(match)
                if norm:
                    found.add(norm)
            return sorted(found)

        def _build_sim_fallback_answer(question_text: str) -> str | None:
            profiles = _load_sim_profiles()
            targets = _extract_target_sims(question_text)
            if not targets:
                return None

            sid = targets[0]
            profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
            )
            if not os.path.exists(sim_path):
                return f"{sid}: no sim state file found yet. It may not have initialized trades yet."

            try:
                with open(sim_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                return f"{sid}: unable to read sim state, so I cannot analyze its performance yet."

            trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
            open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
            eastern = pytz.timezone("US/Eastern")

            def _parse_trade_dt(raw):
                if not raw:
                    return None
                try:
                    dt = datetime.fromisoformat(str(raw))
                    if dt.tzinfo is None:
                        dt = eastern.localize(dt)
                    else:
                        dt = dt.astimezone(eastern)
                    return dt
                except Exception:
                    return None

            def _parse_strike(symbol):
                if not symbol or not isinstance(symbol, str):
                    return None
                try:
                    return int(symbol[-8:]) / 1000.0
                except Exception:
                    return None

            closed = []
            for t in trade_log:
                try:
                    pnl = float(t.get("realized_pnl_dollars"))
                except (TypeError, ValueError):
                    continue
                dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
                closed.append({"trade": t, "pnl": pnl, "dt": dt})

            if not closed:
                mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
                dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
                dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
                return (
                    f"{sid} has no closed trades yet (open: {len(open_trades)}). "
                    f"Configured mode={mode}, DTE={dte_min}-{dte_max}. "
                    "No performance diagnosis is possible until more closes are logged."
                )

            q = question_text.lower()
            scope = closed
            if "yesterday" in q:
                yday = (datetime.now(eastern).date() - timedelta(days=1))
                scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
                if scoped:
                    scope = scoped

            total = len(scope)
            wins = sum(1 for x in scope if x["pnl"] > 0)
            losses = total - wins
            wr = (wins / total) * 100 if total else 0.0
            total_pnl = sum(x["pnl"] for x in scope)
            avg = total_pnl / total if total else 0.0
            avg_win = (sum(x["pnl"] for x in scope if x["pnl"] > 0) / wins) if wins else 0.0
            avg_loss = (sum(x["pnl"] for x in scope if x["pnl"] <= 0) / losses) if losses else 0.0

            exit_counts = {}
            for item in scope:
                t = item["trade"]
                reason = (t.get("exit_reason") or "unknown").strip()
                exit_counts[reason] = exit_counts.get(reason, 0) + 1
            top_exit = sorted(exit_counts.items(), key=lambda x: x[1], reverse=True)[:2]
            top_exit_text = ", ".join([f"{k}:{v}" for k, v in top_exit]) if top_exit else "none"

            mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
            stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
            target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
            dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
            dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
            strategy_text = strategy_intents.get(sid, "Configured strategy profile.")

            scope_text = "yesterday" if ("yesterday" in q and scope is not closed) else "selected period"

            diagnosis = []
            if wr >= 70 and "trailing_stop" in exit_counts:
                diagnosis.append("entries aligned with trend continuation and exits locked gains via trailing stops")
            if wr < 45:
                diagnosis.append("entry quality or regime alignment is weak in this period")
            if avg_win > 0 and abs(avg_loss) > avg_win:
                diagnosis.append("losses are larger than wins; stop/position sizing pressure remains")
            if any(k in {"stop_loss", "iv_crush_stop", "theta_burn"} for k, _ in top_exit):
                diagnosis.append("protective exits are active, likely from pullback or premium decay pressure")
            if not diagnosis:
                diagnosis.append("results are positive, but edge may be regime- and timing-dependent")

            recent = sorted(scope, key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC), reverse=True)[:3]
            trade_lines = []
            for item in recent:
                t = item["trade"]
                pnl = item["pnl"]
                dt = item["dt"]
                dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
                direction = (t.get("direction") or "N/A")
                strike = t.get("strike")
                if not isinstance(strike, (int, float)):
                    strike = _parse_strike(t.get("option_symbol"))
                strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
                entry = t.get("entry_price")
                exit_px = t.get("exit_price")
                entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
                exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
                reason = t.get("exit_reason") or "unknown"
                hold = _fmt_secs(t.get("time_in_trade_seconds"))
                trade_lines.append(
                    f"{dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | {reason} | hold {hold}"
                )

            lines = [
                f"Assessment: {sid} performed strongly in {scope_text} with WR {wr:.1f}% ({wins}W/{losses}L) and PnL ${total_pnl:.2f}.",
                f"Strategy: {strategy_text} (mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}).",
                f"Evidence: avg/trade ${avg:.2f}, avg win ${avg_win:.2f}, avg loss ${avg_loss:.2f}, top exits {top_exit_text}.",
            ]
            if trade_lines:
                lines.append("Recent trades:")
                lines.extend(trade_lines)
            lines.append(f"Likely Causes: {'; '.join(diagnosis)}.")
            lines.append("Next step: use `!askmore break down each trade with entry context and regime` for deeper trade-by-trade analysis.")
            return "\n".join(lines)

        paper = get_paper_stats()
        career = get_career_stats()
        acc = load_account()
        sim_context = _build_sim_context(question)

        try:
            answer = ask_ai(
                question,
                "Live market snapshot",
                paper,
                career,
                acc.get("trade_log", [])[-5:],
                sim_context,
                prior_context=None,
            )
        except Exception:
            answer = None

        # If AI returns an empty/weak/non-sim-specific reply for SIM questions, provide deterministic local analysis.
        answer_text = str(answer).strip() if answer is not None else ""
        weak_for_sim = (
            sim_context is not None
            and (
                not answer_text
                or len(answer_text) < 120
                or "sim" not in answer_text.lower()
            )
        )
        if weak_for_sim:
            fallback = _build_sim_fallback_answer(question)
            if fallback:
                answer = fallback

        if answer is None or not str(answer).strip():
            answer = "No response available."
        else:
            answer = str(answer).strip()
            if "!askmore" not in answer.lower():
                answer = (
                    f"{answer}\n\n"
                    "Follow-up: use `!askmore <question>` to continue from this exact context."
                )
            # Discord embed description hard limit is 4096; keep headroom for status lines.
            if len(answer) > 3400:
                answer = answer[:3400].rstrip() + "\n\n[truncated]"

        ASK_CONTEXT_CACHE[str(ctx.author.id)] = {
            "question": str(question),
            "answer": str(answer),
            "sim_context": sim_context,
            "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        }

        await _send_embed(ctx, answer, title="AI Coach")

    # ── !askmore ──────────────────────────────────────────────────────────────

    @commands.command(name="askmore")
    async def cmd_askmore(self, ctx, *, question=None):
        if not question:
            await _send_embed(ctx, "Usage: `!askmore <follow-up question>`")
            return

        cache_key = str(ctx.author.id)
        last_ctx = ASK_CONTEXT_CACHE.get(cache_key)
        if not isinstance(last_ctx, dict):
            await _send_embed(ctx, "No prior `!ask` context found. Use `!ask` first.")
            return

        paper = get_paper_stats()
        career = get_career_stats()
        acc = load_account()

        prev_q = str(last_ctx.get("question") or "")
        prev_a = str(last_ctx.get("answer") or "")
        prev_ts = str(last_ctx.get("timestamp") or "N/A")
        sim_context = last_ctx.get("sim_context")

        def _fmt_pct(val, decimals=1) -> str:
            try:
                return f"{float(val) * 100:.{decimals}f}%"
            except (TypeError, ValueError):
                return "N/A"

        def _fmt_secs(seconds) -> str:
            try:
                s = int(seconds)
            except (TypeError, ValueError):
                return "N/A"
            if s < 60:
                return f"{s}s"
            if s < 3600:
                return f"{s // 60}m"
            if s < 86400:
                return f"{s / 3600:.1f}h"
            return f"{s / 86400:.1f}d"

        def _parse_trade_dt(raw):
            if not raw:
                return None
            try:
                eastern = pytz.timezone("US/Eastern")
                dt = datetime.fromisoformat(str(raw))
                if dt.tzinfo is None:
                    dt = eastern.localize(dt)
                else:
                    dt = dt.astimezone(eastern)
                return dt
            except Exception:
                return None

        def _parse_strike(symbol):
            if not symbol or not isinstance(symbol, str):
                return None
            try:
                return int(symbol[-8:]) / 1000.0
            except Exception:
                return None

        def _extract_reason(entry_context):
            if not entry_context or "reason=" not in str(entry_context):
                return None
            try:
                return str(entry_context).split("reason=", 1)[1].split("|")[0].strip()
            except Exception:
                return None

        def _resolve_sim_id(q_text: str) -> str | None:
            # 1) explicit SIM reference in follow-up
            m = re.search(r"\bsim\s*(\d{1,2})\b", q_text, flags=re.IGNORECASE)
            if m:
                return f"SIM{int(m.group(1)):02d}"
            # 2) first SIM id from prior question
            m = re.search(r"\bsim\s*(\d{1,2})\b", prev_q, flags=re.IGNORECASE)
            if m:
                return f"SIM{int(m.group(1)):02d}"
            # 3) first SIM id in cached sim_context text
            if isinstance(sim_context, str):
                m = re.search(r"\bSIM\d{2}\b", sim_context)
                if m:
                    return m.group(0)
            return None

        def _build_askmore_sim_fallback(q_text: str) -> str | None:
            sid = _resolve_sim_id(q_text)
            if not sid:
                return None
            profiles = _load_sim_profiles()
            profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
            )
            if not os.path.exists(sim_path):
                return f"{sid}: sim state file not found."

            try:
                with open(sim_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                return f"{sid}: unable to read sim state file."

            trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
            open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
            closed = []
            for t in trade_log:
                try:
                    pnl = float(t.get("realized_pnl_dollars"))
                except (TypeError, ValueError):
                    continue
                dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
                closed.append({"trade": t, "pnl": pnl, "dt": dt})
            if not closed:
                return f"{sid}: no closed trades available yet (open={len(open_trades)})."

            q = q_text.lower()
            n = 3
            m = re.search(r"last\s+(\d+)\s+trades", q)
            if m:
                try:
                    n = max(1, min(20, int(m.group(1))))
                except Exception:
                    n = 3

            scope = closed
            if "yesterday" in q:
                yday = (datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(days=1))
                scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
                if scoped:
                    scope = scoped

            scope_sorted = sorted(
                scope,
                key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC),
                reverse=True,
            )
            picks = scope_sorted[:n]

            wins = sum(1 for x in scope if x["pnl"] > 0)
            total = len(scope)
            total_pnl = sum(x["pnl"] for x in scope)
            wr = (wins / total) * 100 if total else 0.0

            stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
            target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
            mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
            dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
            dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"

            lines = [
                f"Assessment: {sid} follow-up for {len(picks)} trade(s). Period WR={wr:.1f}% ({wins}/{total}) and PnL={total_pnl:+.2f}.",
                f"Strategy frame: mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}.",
                "Trade breakdown:",
            ]

            want_context = any(k in q for k in ["entry context", "regime", "context", "why"])
            for item in picks:
                t = item["trade"]
                dt = item["dt"]
                dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
                pnl = item["pnl"]
                direction = t.get("direction") or "N/A"
                strike = t.get("strike")
                if not isinstance(strike, (int, float)):
                    strike = _parse_strike(t.get("option_symbol"))
                strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
                entry = t.get("entry_price")
                exit_px = t.get("exit_price")
                entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
                exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
                exit_reason = t.get("exit_reason") or "unknown"
                hold = _fmt_secs(t.get("time_in_trade_seconds"))
                lines.append(
                    f"- {dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | exit={exit_reason} | hold={hold}"
                )
                if want_context:
                    regime = t.get("regime_at_entry") or "N/A"
                    bucket = t.get("time_of_day_bucket") or "N/A"
                    ectx = t.get("entry_context") or "N/A"
                    reason = _extract_reason(ectx) or "N/A"
                    lines.append(f"  context: regime={regime}, bucket={bucket}, signal_reason={reason}")
                    if "entry context" in q:
                        lines.append(f"  entry_context: {ectx}")

            lines.append("Next step: use `!askmore compare winners vs losers for this SIM by regime/time bucket`.")
            return "\n".join(lines)

        prior_context = (
            f"Previous question: {prev_q}\n"
            f"Previous answer: {prev_a}\n"
            f"Previous timestamp ET: {prev_ts}\n"
            "Continue from this context and directly answer the follow-up."
        )

        try:
            answer = ask_ai(
                question,
                "Live market snapshot",
                paper,
                career,
                acc.get("trade_log", [])[-5:],
                sim_context,
                prior_context=prior_context,
            )
        except Exception:
            answer = None

        answer_text = str(answer).strip() if answer is not None else ""
        weak_for_sim = (
            sim_context is not None
            and (
                not answer_text
                or len(answer_text) < 120
                or "sim" not in answer_text.lower()
            )
        )

        if weak_for_sim:
            fallback = _build_askmore_sim_fallback(question)
            if fallback:
                answer = fallback
            else:
                answer = (
                    "I could not produce a full continuation from model output this turn. "
                    f"Prior question was: `{prev_q}`. "
                    "Try `!askmore break down the last 3 trades with entry context and regime`."
                )
        elif answer is None or not str(answer).strip() or len(str(answer).strip()) < 40:
            answer = (
                "I could not produce a full continuation from model output this turn. "
                f"Prior question was: `{prev_q}`. "
                "Try a narrower follow-up like `!askmore break down the last 3 trades and exit reasons`."
            )
        else:
            answer = str(answer).strip()
            if "!askmore" not in answer.lower():
                answer = (
                    f"{answer}\n\n"
                    "Follow-up: use `!askmore <question>` to keep this same context."
                )
            if len(answer) > 3400:
                answer = answer[:3400].rstrip() + "\n\n[truncated]"

        ASK_CONTEXT_CACHE[cache_key] = {
            "question": str(question),
            "answer": str(answer),
            "sim_context": sim_context,
            "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        }
        await _send_embed(ctx, answer, title="AI Coach")

    # ── !research ─────────────────────────────────────────────────────────────

    @commands.command(name="research")
    async def cmd_research(self, ctx, subcmd: str | None = None, *args):
        if not _RESEARCH_AVAILABLE:
            embed = discord.Embed(title="Research Unavailable", description="Research modules failed to import.", color=0xE74C3C)
            _append_footer(embed)
            await ctx.send(embed=embed)
            return

        subcmd = (subcmd or "").lower()

        # ── gaps ──────────────────────────────────────────────────────────────
        if subcmd == "gaps":
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

        # ── hypotheses ────────────────────────────────────────────────────────
        elif subcmd == "hypotheses":
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

        # ── create ────────────────────────────────────────────────────────────
        elif subcmd == "create":
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

        # ── compare ───────────────────────────────────────────────────────────
        elif subcmd == "compare":
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

                def _sec(v):
                    return f"{v:.0f}s" if v is not None else "\u2014"

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

        # ── unknown / no subcommand ───────────────────────────────────────────
        else:
            embed = discord.Embed(
                title="!research \u2014 Subcommands",
                description=ab(
                    f"{A('!research gaps <SIM_ID>', 'cyan')} \u2014 behavior gap analysis for one sim",
                    f"{A('!research gaps all', 'cyan')} \u2014 gap summary for all sims",
                    f"{A('!research compare <SIM_A> <SIM_B>', 'cyan')} \u2014 side-by-side behavior comparison",
                    f"{A('!research hypotheses', 'cyan')} \u2014 list saved hypotheses",
                    f'{A("!research create", "cyan")} <source> <claim> <counter> <features> \u2014 create hypothesis',
                ),
                color=0x3498DB,
            )
            _append_footer(embed)
            await ctx.send(embed=embed)


    @commands.command(name="stratperf")
    async def cmd_stratperf(self, ctx, strategy: str = None, regime: str = None):
        """Strategy performance breakdown. Usage: !stratperf <strategy> [regime]"""
        if strategy is None:
            await _send_embed(ctx, "Usage: `!stratperf <strategy> [regime]`\nExample: `!stratperf TREND_PULLBACK` or `!stratperf TREND_PULLBACK TREND`")
            return
        try:
            from analytics.strategy_performance import PERF_STORE
            strat = strategy.upper()
            if regime:
                reg = regime.upper()
                data = PERF_STORE._data.get(strat, {}).get(reg, {})
                if not data:
                    await _send_embed(ctx, f"No data for `{strat}` in regime `{reg}` yet.")
                    return
                embed = discord.Embed(title=f"{strat} / {reg}", color=0x3498DB)
                lines = []
                for bucket, stats in sorted(data.items()):
                    wr = stats["wins"] / stats["trades"] * 100 if stats["trades"] else 0
                    _pnl = stats["total_pnl"]
                    _pnl_str = f"${_pnl:.2f}"
                    lines.append(
                        f"{lbl(bucket)} {A(str(stats['trades']), 'white', bold=True)} trades  "
                        f"{A(f'{wr:.0f}%', 'green' if wr >= 50 else 'red')} WR  "
                        f"{A(_pnl_str, 'green' if _pnl >= 0 else 'red')}"
                    )
                embed.add_field(name="Bucket breakdown", value=ab(*lines) if lines else "_none_", inline=False)
            else:
                summary = PERF_STORE.get_strategy_summary(strat)
                if not summary:
                    await _send_embed(ctx, f"No data for `{strat}` yet.")
                    return
                embed = discord.Embed(title=f"{strat} — Performance Summary", color=0x3498DB)
                embed.add_field(name="Trades",    value=str(summary["trades"]),                               inline=True)
                embed.add_field(name="Win Rate",  value=f"{summary['win_rate'] * 100:.0f}%",                  inline=True)
                embed.add_field(name="Total PnL", value=f"${summary['total_pnl']:.2f}",                       inline=True)
                embed.add_field(name="Regimes",   value=", ".join(summary["regimes"]) or "—",                 inline=False)
            _append_footer(embed)
            await ctx.send(embed=embed)
        except Exception as exc:
            logging.exception("!stratperf failed: %s", exc)
            await _send_embed(ctx, "⚠️ Error loading strategy performance data.")

    @commands.command(name="candidates")
    async def cmd_candidates(self, ctx, date: str = None):
        """Candidate signal summary for a day. Usage: !candidates [YYYY-MM-DD]"""
        try:
            date_str = date or datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d")
            path = os.path.join("data", "candidates", f"{date_str}.jsonl")
            if not os.path.exists(path):
                await _send_embed(ctx, f"No candidate data for `{date_str}`.")
                return

            fired = blocked = traded = total = 0
            by_strategy: dict = {}
            with open(path) as f:
                for line in f:
                    try:
                        c = json.loads(line)
                    except Exception:
                        continue
                    total += 1
                    if c.get("fired"):     fired += 1
                    if c.get("blocked"):   blocked += 1
                    if c.get("traded"):    traded += 1
                    s = c.get("strategy") or "?"
                    entry = by_strategy.setdefault(s, {"fired": 0, "blocked": 0, "traded": 0})
                    if c.get("fired"):   entry["fired"] += 1
                    if c.get("blocked"): entry["blocked"] += 1
                    if c.get("traded"):  entry["traded"] += 1

            embed = discord.Embed(title=f"📊 Candidates — {date_str}", color=0x3498DB)
            embed.add_field(name="Evaluations", value=str(total),   inline=True)
            embed.add_field(name="Fired",        value=str(fired),   inline=True)
            embed.add_field(name="Blocked",      value=str(blocked), inline=True)
            embed.add_field(name="Traded",       value=str(traded),  inline=True)

            top = sorted(by_strategy.items(), key=lambda x: x[1]["fired"], reverse=True)[:8]
            lines = [
                f"{lbl(s)} {A(str(d['fired']), 'white', bold=True)} fired  "
                f"{A(str(d['blocked']), 'red')} blocked  "
                f"{A(str(d['traded']), 'green')} traded"
                for s, d in top
            ]
            if lines:
                embed.add_field(name="By Strategy (top 8)", value=ab(*lines), inline=False)
            _append_footer(embed)
            await ctx.send(embed=embed)
        except Exception as exc:
            logging.exception("!candidates failed: %s", exc)
            await _send_embed(ctx, "⚠️ Error loading candidate data.")


async def setup(bot):
    await bot.add_cog(ResearchCommands(bot))
