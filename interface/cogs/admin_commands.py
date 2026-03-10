"""
interface/cogs/admin_commands.py
Discord commands for bot administration and operational maintenance.

Commands: help, helpplan, system, ratelimit, backfill, query,
          conviction_fix, features_reset, pred_reset, retrain
"""

import os
import csv
import asyncio
import logging

import discord
from discord.ext import commands

from interface.shared_state import (
    _send_embed,
    _append_footer,
    _add_field_icons,
    _format_ts,
)
from core.paths import DATA_DIR


class AdminCommands(commands.Cog, name="Admin"):
    """Bot administration and maintenance commands."""

    def __init__(self, bot):
        self.bot = bot

    # ── help ──────────────────────────────────────────────────────────────────

    @commands.command(name="help")
    async def help_command(self, ctx, command_name: str | int | None = None):

        command_levels = {
            "spy": "basic",
            "predict": "basic",
            "regime": "basic",
            "conviction": "basic",
            "opportunity": "basic",
            "plan": "basic",
            "trades": "basic",
            "conviction_fix": "advanced",
            "features_reset": "advanced",
            "pred_reset": "advanced",
            "analysis": "advanced",
            "attempts": "advanced",
            "run": "advanced",
            "paperstats": "advanced",
            "career": "advanced",
            "equity": "advanced",
            "risk": "advanced",
            "expectancy": "advanced",
            "regimes": "advanced",
            "accuracy": "advanced",
            "mlstats": "advanced",
            "retrain": "advanced",
            "importance": "advanced",
            "md": "advanced",
            "simstats": "advanced",
            "simcompare": "advanced",
            "simtrades": "advanced",
            "simopen": "advanced",
            "simreset": "advanced",
            "simleaderboard": "advanced",
            "simstreaks": "advanced",
            "simregimes": "advanced",
            "simtimeofday": "advanced",
            "simpf": "advanced",
            "simconsistency": "advanced",
            "simexits": "advanced",
            "simhold": "advanced",
            "simdte": "advanced",
            "simsetups": "advanced",
            "simhealth": "advanced",
            "siminfo": "advanced",
            "preopen": "advanced",
            "lastskip": "advanced",
            "system": "advanced",
            "replay": "advanced",
            "helpplan": "advanced",
            "ask": "advanced",
            "askmore": "advanced",
            "backfill": "advanced",
            "query": "advanced",
            "ratelimit": "advanced",
        }

        def _send_help_page(page_num: int):
            pages = [
                {
                    "title": "📘 Help — Page 1/3 (Market + Core)",
                    "color": 0x3498DB,
                    "fields": [
                        ("🟢 Market", "`!spy`, `!predict`, `!regime`, `!conviction`, `!opportunity`, `!plan`"),
                        ("🟦 Core Performance", "`!trades`, `!analysis`, `!attempts`, `!run`"),
                        ("🟣 Risk + Expectancy", "`!risk`, `!expectancy`, `!regimes`, `!accuracy`, `!md`"),
                        ("🧭 MD Controls", "`!md status`, `!md enable`, `!md disable`, `!md auto <low|medium|high>`"),
                    ],
                },
                {
                    "title": "📗 Help — Page 2/3 (ML + Sims)",
                    "color": 0x2ECC71,
                    "fields": [
                        ("🧠 ML", "`!mlstats`, `!retrain`, `!importance`"),
                        ("🧪 Sims", "`!simstats`, `!simcompare`, `!simtrades`, `!simopen`, `!simleaderboard`, `!simstreaks`, `!simregimes`, `!simtimeofday`, `!simdte`, `!simsetups`, `!simpf`, `!simconsistency`, `!simexits`, `!simhold`, `!simreset`, `!simhealth`, `!siminfo`"),
                        ("⏸ Skip Status", "`!lastskip`, `!preopen`"),
                    ],
                },
                {
                    "title": "📙 Help — Page 3/3 (System + AI)",
                    "color": 0xF39C12,
                    "fields": [
                        ("🖥 System", "`!system`, `!ratelimit`, `!backfill [days] [sym|all]`, `!query`, `!replay`, `!helpplan`"),
                        ("🧭 Momentum Decay", "`!md status`, `!md enable`, `!md disable`, `!md auto <low|medium|high>`"),
                        ("🤖 AI Coach", "`!ask <contract>` — chart + narrative  |  `!askmore`"),
                        ("🧰 Maintenance", "`!conviction_fix`, `!features_reset`, `!pred_reset`"),
                    ],
                },
            ]
            page_index = max(1, min(page_num, len(pages))) - 1
            page = pages[page_index]
            embed = discord.Embed(title=page["title"], color=page["color"])
            embed.description = "Use `!help <command>` for detailed usage. Use `!help 1|2|3` for pages."
            for name, value in page["fields"]:
                embed.add_field(name=name, value=value, inline=False)
            _append_footer(embed, extra=f"Page {page_index + 1}/{len(pages)}")
            return embed

        async def _send_help_paginated(start_page: int):
            pages_count = 3
            page = max(1, min(start_page, pages_count))
            message = await ctx.send(embed=_send_help_page(page))
            if pages_count <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await self.bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page = pages_count if page == 1 else page - 1
                elif emoji == "▶️":
                    page = 1 if page == pages_count else page + 1

                try:
                    await message.edit(embed=_send_help_page(page))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        if command_name is None:
            await _send_help_paginated(1)
            return
        if isinstance(command_name, int):
            await _send_help_paginated(command_name)
            return
        if isinstance(command_name, str):
            page_text = command_name.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                await _send_help_paginated(int(page_text))
                return

        if not isinstance(command_name, str):
            await _send_embed(ctx, "Command name must be text.")
            return

        command_name = command_name.lower()

        command_guides = {
            "plan": """
`!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>`

Analyzes a proposed options trade using:

• Market Regime
• Volatility State
• Conviction Score
• Structure Alignment
• ATR Context
• Bias Alignment

Example:
`!plan call 435 1.20 2 2026-02-14`

Outputs:
• Market Diagnostics
• Strike Context
• Exposure Size
• AI Grade (A–D)

This does NOT place a trade.
It evaluates the idea against your engine.
""",
            "predict": """
`!predict <minutes>`

Forecasts SPY direction.

Allowed values:
30 or 60

Examples:
`!predict 30`
`!predict 60`
""",

            "risk": """
`!risk`

Displays:
• Avg R
• Avg Win R
• Avg Loss R
• Max R
• Drawdown

Requires:
Minimum 10 closed trades.
""",

            "expectancy": """
`!expectancy`

Displays rolling expectancy (R-based).

Requires:
Minimum 10 closed trades.
""",
            "spy": """
`!spy` / `!qqq` / `!iwm` / `!vxx` / `!tsla` / `!aapl` / `!nvda` / `!msft`
Or: `!quote <SYMBOL>` for any symbol

Shows price snapshot for that symbol:
• Price, VWAP, EMA9, EMA20
• Session high/low with timestamps
• Sends a chart image

Note: non-SPY symbols need data — run `!backfill 5 <symbol>` first.
""",
            "regime": """
`!regime`

Displays current market regime label.
""",
            "conviction": """
`!conviction`

Displays:
• Conviction score
• Direction
• Impulse
• Follow-through
""",
            "conviction_fix": """
`!conviction_fix`

Forces a backfill of conviction expectancy:
• Fills fwd_5m / fwd_10m where possible
• Adds price/time metadata and status markers
""",
            "features_reset": """
`!features_reset`

Resets trade_features.csv to a clean header.
Use when the feature file is malformed or legacy.
""",
            "pred_reset": """
`!pred_reset`

Resets predictions.csv to a clean header.
Use when old/stale predictions are present.
""",
            "opportunity": """
`!opportunity`

Returns current opportunity zone if available.
""",
            "run": """
`!run`

Shows runtime stats:
• Trades
• Wins/Losses
• Balance
""",
            "paperstats": """
`!paperstats`

Shows paper account stats:
• Balance
• PnL
• Winrate
""",
            "career": """
`!career`

Shows career stats:
• Total trades
• Winrate
• Best balance
""",
            "equity": """
`!equity`

Sends equity curve chart (requires closed trades).
""",
            "accuracy": """
`!accuracy`

Shows prediction accuracy (requires graded predictions).
""",
            "analysis": """
`!analysis`

Decision analysis summary:
• Trades analyzed
• Corr Delta vs R
• Corr Blended vs R
• Execution no-record exits (if present)
""",
            "attempts": """
`!attempts`

Decision attempt summary (runtime):
• Attempts / Opened / Blocked
• Top block reason
• ML weight
• Avg blended vs threshold
""",
            "trades": """
`!trades <page>`

Shows paginated trade log (5 per page).
Example: `!trades 2`
""",
            "simstats": """
`!simstats` or `!simstats SIM03`

Shows sim performance stats:
• Total trades, win rate, total PnL
• Avg win/loss, expectancy, drawdown
• Best/worst trade
• Regime/time-of-day breakdowns
""",
            "simcompare": """
`!simcompare`

Side-by-side sim comparison table.
""",
            "simleaderboard": """
`!simleaderboard`

Ranks sims by key performance metrics:
• Best win rate
• Best total return / PnL
• Fastest equity growth
• Best expectancy
• Biggest winner
• High-risk / high-reward
""",
            "simstreaks": """
`!simstreaks`

Win/loss streak leaders across sims.
""",
            "simregimes": """
`!simregimes`

Best sim by regime (win rate).
""",
            "simtimeofday": """
`!simtimeofday`

Best sim by time-of-day bucket (win rate).
""",
            "simpf": """
`!simpf`

Profit factor leaderboard.
""",
            "simconsistency": """
`!simconsistency`

Most consistent sims (lowest PnL volatility).
""",
            "simexits": """
`!simexits`

Best exit reason hit rates.
""",
            "simhold": """
`!simhold`

Fastest/slowest average hold time.
""",
            "md": """
`!md status`
`!md enable`
`!md disable`
`!md auto <low|medium|high>`

Toggles Momentum Decay strict mode:
• Enabled = tighter stops during decay
• Status shows last decay + warnings
• Auto mode: OFF at session transitions, ON only when detected decay meets/exceeds level
""",
            "simdte": """
`!simdte`

Best sim by DTE bucket (win rate).
""",
            "simsetups": """
`!simsetups`

Best sim by setup type (win rate).
""",
            "siminfo": """
`!siminfo 0-11`
`!siminfo SIM03`

Shows one sim's detailed strategy/config:
• Strategy intent + signal mode
• DTE/hold/cutoff profile
• Risk, stops, targets
• Optional gates (ORB/vol_z/atr_expansion/regime)
""",
            "preopen": """
`!preopen`

Runs a pre-open readiness check:
• Market open/closed status
• Data age + source
• Latest SPY close
• Option snapshot sanity (call/put + 3 OTM variants)
""",
            "simtrades": """
`!simtrades SIM03 [page]`

Shows paginated sim trade history.
""",
            "simopen": """
`!simopen` or `!simopen SIM03 [page]`

Shows open sim trades:
• Hold time
• SPY CALL/PUT expiry strike
• Entry cost + current PnL
""",
            "simreset": """
`!simreset SIM03`
`!simreset all`
`!simreset live`

Resets a sim to starting balance and clears trade history.
""",
            "lastskip": """
`!lastskip`

Shows the most recent skip reason
for trade attempts.
""",
            "regimes": """
`!regimes`

Regime expectancy stats (R-multiple).
""",
            "system": """
`!system`

Displays:
• Market status
• System health
• Active background systems
""",
            "replay": """
`!replay [symbol]`

Sends recorded session chart and live chart for the given symbol.
Defaults to SPY if no symbol given.

Examples:
  `!replay` → SPY session
  `!replay iwm` → IWM session
  `!replay tsla` → TSLA session
""",
            "helpplan": """
`!helpplan`

Quick reference for `!plan` usage.
""",

            "mlstats": """
`!mlstats`

Displays rolling ML accuracy (last 30 trades).

Requires:
At least 30 ML-evaluated trades.
""",

            "retrain": """
`!retrain`

Retrains:
• Direction model
• Edge model

Requires:
Minimum 50 logged trades in feature file.
""",

            "importance": """
`!importance`

Displays feature importance from Edge ML model.

Model must be trained first.
""",

            "system": """
`!system`

Displays:
• Market status
• System health
• Active background systems
""",

            "backfill": """
`!backfill [days] [symbol|all]`

Fetches historical 1-min candles from Alpaca and merges into the symbol's CSV.

Examples:
`!backfill`              — SPY, 30 days
`!backfill 60`           — SPY, 60 days
`!backfill 30 QQQ`       — QQQ, 30 days
`!backfill 7 all`        — all registered symbols, 7 days

Registered symbols: SPY, QQQ, IWM, VXX, TSLA, AAPL, NVDA, MSFT
""",
            "ask": """
`!ask <option_contract>`  — Trade chart + AI analysis
`!ask <question>`         — AI reviews your performance

**Trade analysis** (OCC contract format):
`!ask SPY260321C00565000`
`!ask QQQ260321P00480000`

• Searches all sims for trades on that contract
• Generates annotated chart (entry/exit, EMAs, VWAP, RSI panel)
• GPT narrative: entry reasoning, exit quality, grade (A–F), tags
• Posts one embed per matching sim (compare strategies side-by-side)

**Performance review** (free-text question):
`!ask Did I overtrade?`
`!ask Why are my mean reversion trades losing?`

Use `!askmore` for follow-up questions.
""",
            "askmore": """
`!askmore <follow-up question>`

Continues from your previous `!ask` context.

Examples:
`!askmore break down the last 3 trades`
`!askmore include entry context and regime`
"""
        }

        if command_name in command_guides:
            await _send_embed(ctx, command_guides[command_name], title=f"!{command_name}")
        else:
            await _send_embed(
                ctx,
                "Unknown command.\n"
                "Type `!help` to view available commands.\n"
                "Type `!help <command>` for detailed usage."
            )

    # ── helpplan ──────────────────────────────────────────────────────────────

    @commands.command(name="helpplan")
    async def help_plan(self, ctx):
        await _send_embed(ctx, """
📋 **!plan Usage**

!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>

Example:
!plan call 435 1.20 2 2026-02-14

Analyzes:
• Market structure
• Volatility
• Statistical alignment
• AI model grade (A–D)
""", title="Help")

    # ── system ────────────────────────────────────────────────────────────────

    @commands.command(name="system")
    async def system(self, ctx):

        import pytz
        from datetime import datetime
        from core.account_repository import load_account
        from interface.health_monitor import check_health
        from core.market_clock import market_is_open

        eastern = pytz.timezone("US/Eastern")
        now = datetime.now(eastern)

        # ---------------------------
        # Safe Account Load
        # ---------------------------
        try:
            acc = load_account()
        except:
            acc = {}

        trade_log = acc.get("trade_log", [])

        # ---------------------------
        # Safe Trade Count
        # ---------------------------
        total_trades = len(trade_log)

        if total_trades == 0:
            trade_status = "No closed trades yet."
        else:
            trade_status = f"{total_trades} closed trades"

        # ---------------------------
        # Health Check (Safe)
        # ---------------------------
        try:
            status, report = check_health()
        except:
            status = "UNKNOWN"
            report = "Health monitor unavailable."

        market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

        # ---------------------------
        # Embed
        # ---------------------------
        embed = discord.Embed(
            title="🧠 SPY AI Control Center",
            color=discord.Color.green() if status == "HEALTHY" else discord.Color.orange()
        )
        embed.add_field(name=_add_field_icons("Market"), value=market_status, inline=True)
        embed.add_field(name=_add_field_icons("System Health"), value=status, inline=True)
        embed.add_field(name=_add_field_icons("System Diagnostics"), value=f"```\n{report}\n```", inline=False)

        embed.add_field(
            name=_add_field_icons("Trade Activity"),
            value=f"{trade_status}",
            inline=False
        )

        embed.add_field(
            name=_add_field_icons("Background Systems"),
            value=(
                "Auto Trader: Running\n"
                "Forecast Engine: Active\n"
                "Conviction Watcher: Active\n"
                "Prediction Grader: Active\n"
                "Heart Monitor: Active"
            ),
            inline=False
        )

        if total_trades < 10:
            embed.add_field(
                name=_add_field_icons("Analytics Status"),
                value=(
                    "⚠️ Not enough trade data for:\n"
                    "• Expectancy\n"
                    "• Risk Metrics\n"
                    "• Edge Stability\n"
                    "System is collecting data."
                ),
                inline=False
            )

        embed.set_footer(text=f"System time: {_format_ts(now)}")
        _append_footer(embed)

        await ctx.send(embed=embed)

    # ── ratelimit ─────────────────────────────────────────────────────────────

    @commands.command(name="ratelimit")
    async def ratelimit(self, ctx):
        """Show API rate-limiter / circuit-breaker / cache stats."""
        from core.api_resilience import resilience_stats

        stats = resilience_stats()
        bucket = stats["bucket"]
        cache = stats["cache"]
        breaker = stats["breaker"]

        cb_state = breaker["state"]
        cb_color = 0x00CC44 if cb_state == "closed" else (0xFF8800 if cb_state == "half_open" else 0xFF3333)

        age_str = ""
        if breaker.get("opened_age_seconds") is not None:
            age_str = f" (open {breaker['opened_age_seconds']}s ago)"

        embed = discord.Embed(title="API Resilience Status", color=cb_color)
        embed.add_field(
            name="Token Bucket",
            value=(
                f"Available: **{bucket['tokens_available']}** / {int(bucket['max_tokens'])}\n"
                f"Refill rate: {bucket['refill_rate']}/s"
            ),
            inline=True,
        )
        embed.add_field(
            name="Response Cache",
            value=(
                f"Valid entries: **{cache['cached_entries']}**\n"
                f"Total slots: {cache['total_entries']}"
            ),
            inline=True,
        )
        cb_icon = "🟢" if cb_state == "closed" else ("🟡" if cb_state == "half_open" else "🔴")
        embed.add_field(
            name="Circuit Breaker",
            value=(
                f"{cb_icon} **{cb_state.upper()}**{age_str}\n"
                f"Failures: {breaker['failures']} / {breaker['threshold']}"
            ),
            inline=False,
        )
        _append_footer(embed)
        await ctx.send(embed=embed)

    # ── backfill ──────────────────────────────────────────────────────────────

    @commands.command(name="backfill")
    async def backfill(self, ctx, days: int = 30, symbol: str = "SPY"):
        """
        Backfill historical 1m candles from Alpaca.
        Usage: !backfill [days] [SPY|QQQ|all]
        Examples: !backfill 30 SPY  |  !backfill 7 QQQ  |  !backfill 30 all
        """
        from core.backfill import (
            run_backfill_async,
            run_backfill_all_symbols_async,
            backfill_status,
            _load_registered_symbols as _backfill_get_symbols,
        )

        if days < 1 or days > 365:
            await _send_embed(ctx, "Days must be between 1 and 365.")
            return

        backfill_all = symbol.lower() == "all"
        sym = symbol.upper()

        if backfill_all:
            registry = _backfill_get_symbols()
            sym_list = list(registry.keys()) if registry else ["SPY"]
            status_lines = []
            for s in sym_list:
                st = backfill_status(s)
                status_lines.append(f"**{s}**: {st['rows']:,} rows ({st['latest'] or 'no data'})")
            embed = discord.Embed(
                title=f"Backfilling {days} days — ALL symbols ({len(sym_list)})",
                description="\n".join(status_lines) + "\n\nRunning in background…",
                color=0x4444FF,
            )
        else:
            status = backfill_status(sym)
            embed = discord.Embed(
                title=f"Backfilling {days} days — {sym}",
                description=(
                    f"Current: **{status['rows']:,}** rows\n"
                    f"Earliest: {status['earliest'] or 'N/A'}\n"
                    f"Latest: {status['latest'] or 'N/A'}\n\n"
                    "Running in background — this may take a minute."
                ),
                color=0x4444FF,
            )
        _append_footer(embed)
        await ctx.send(embed=embed)

        messages: list[str] = []
        def progress(msg: str):
            messages.append(msg)

        if backfill_all:
            result = await run_backfill_all_symbols_async(days_back=days, progress_cb=progress)
        else:
            result = await run_backfill_async(days_back=days, symbol=sym, progress_cb=progress)

        if result.get("ok"):
            if backfill_all:
                lines = []
                for s, r in result.get("results", {}).items():
                    if r.get("ok"):
                        lines.append(f"**{s}**: +{r['added_rows']:,} rows (total {r['total_rows']:,}, {r['errors']} err)")
                    else:
                        lines.append(f"**{s}**: ❌ {r.get('error','failed')}")
                embed2 = discord.Embed(
                    title="Backfill Complete — All Symbols",
                    description="\n".join(lines) if lines else "No new rows added.",
                    color=0x00CC44,
                )
                embed2.add_field(
                    name="Totals",
                    value=f"Rows added: **{result['total_added']:,}** | Day-errors: {result['total_errors']}",
                    inline=False,
                )
            else:
                embed2 = discord.Embed(title=f"Backfill Complete — {sym}", color=0x00CC44)
                embed2.add_field(
                    name="Results",
                    value=(
                        f"Days fetched: **{result['fetched_days']}**\n"
                        f"Rows added: **{result['added_rows']:,}**\n"
                        f"Total rows: **{result['total_rows']:,}**\n"
                        f"Day-errors: {result['errors']}"
                    ),
                    inline=False,
                )
        else:
            embed2 = discord.Embed(
                title="Backfill Failed",
                description=result.get("error", "unknown error"),
                color=0xFF3333,
            )
        _append_footer(embed2)
        await ctx.send(embed=embed2)

    # ── query ─────────────────────────────────────────────────────────────────

    @commands.command(name="query")
    async def query(self, ctx, sim_id: str = None, page: int = 1):
        """Query trade journal DB. Usage: !query [SIM03] [page]"""
        from core.trade_db import query_trades, trade_count

        per_page = 10
        offset = (max(1, page) - 1) * per_page
        sid = sim_id.upper() if sim_id else None

        trades = query_trades(sim_id=sid, limit=per_page, offset=offset)
        total = trade_count(sim_id=sid)
        total_pages = max(1, (total + per_page - 1) // per_page)

        title = f"Trade Journal — {sid or 'ALL SIMS'} (page {page}/{total_pages})"
        embed = discord.Embed(title=title, color=0x4444AA)
        embed.description = f"Total records: **{total:,}**"

        if not trades:
            embed.description += "\nNo trades found."
        else:
            lines = []
            for t in trades:
                pnl = t.get("realized_pnl_dollars")
                pnl_str = f"${pnl:+.2f}" if pnl is not None else "?"
                sym = t.get("option_symbol") or "?"
                xt = (t.get("exit_time") or "")[:16]
                reason = t.get("exit_reason") or "?"
                sim = t.get("sim_id") or "?"
                lines.append(f"`{sim}` {sym} | {pnl_str} | {reason} | {xt}")
            embed.add_field(name="Trades", value="\n".join(lines), inline=False)

        if total_pages > 1:
            embed.set_footer(text=f"Use !query {sim_id or ''} {page + 1} for next page")
        else:
            _append_footer(embed)
        await ctx.send(embed=embed)

    # ── conviction_fix ────────────────────────────────────────────────────────

    @commands.command(name="conviction_fix")
    async def conviction_fix(self, ctx):
        try:
            from core.data_service import get_market_dataframe
            from analytics.conviction_stats import update_expectancy

            df = get_market_dataframe()
            update_expectancy(df)
            await _send_embed(
                ctx,
                "Conviction expectancy backfill complete.\n"
                "fwd_5m / fwd_10m now updated where possible with price/time metadata.",
                title="Conviction Fix",
                color=0x2ecc71,
            )
        except Exception as e:
            logging.exception("conviction_fix_error: %s", e)
            await _send_embed(
                ctx,
                f"Conviction fix failed: {e}",
                title="Conviction Fix",
                color=0xe74c3c,
            )

    # ── features_reset ────────────────────────────────────────────────────────

    @commands.command(name="features_reset")
    async def features_reset(self, ctx):
        try:
            from analytics.feature_logger import FEATURE_FILE, FEATURE_HEADERS

            with open(FEATURE_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(FEATURE_HEADERS)
            await _send_embed(
                ctx,
                "trade_features.csv reset to clean header.",
                title="Features Reset",
                color=0x2ecc71,
            )
        except Exception as e:
            logging.exception("features_reset_error: %s", e)
            await _send_embed(
                ctx,
                f"Features reset failed: {e}",
                title="Features Reset",
                color=0xe74c3c,
            )

    # ── pred_reset ────────────────────────────────────────────────────────────

    @commands.command(name="pred_reset")
    async def pred_reset(self, ctx):
        try:
            from analytics.prediction_stats import PRED_FILE, PRED_HEADERS

            with open(PRED_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(PRED_HEADERS)
            await _send_embed(
                ctx,
                "predictions.csv reset to clean header.",
                title="Predictions Reset",
                color=0x2ecc71,
            )
        except Exception as e:
            logging.exception("pred_reset_error: %s", e)
            await _send_embed(
                ctx,
                f"Predictions reset failed: {e}",
                title="Predictions Reset",
                color=0xe74c3c,
            )

    # ── retrain ───────────────────────────────────────────────────────────────

    @commands.command(name="retrain")
    async def retrain(self, ctx):

        feature_file = os.path.join(DATA_DIR, "trade_features.csv")

        if not os.path.exists(feature_file):
            await _send_embed(
                ctx,
                "No trade feature data found.\n"
                "You need at least 50 logged trades before retraining."
            )
            return

        await _send_embed(ctx, "🔄 Retraining models...")

        from research.train_ai import train_direction_model, train_edge_model
        train_direction_model()
        train_edge_model()

        await _send_embed(ctx, "✅ Models retrained successfully.")


async def setup(bot):
    await bot.add_cog(AdminCommands(bot))
