"""
interface/cogs/market_commands.py
Market analysis, symbol snapshots, and account performance commands.

Commands:
  Symbol snapshots: !spy !qqq !iwm !vxx !tsla !aapl !nvda !msft !quote
  Market analysis:  !risk !regime !regimes !conviction !opportunity !predict
                    !plan !mlstats !importance !expectancy !replay !preopen !md
  Account perf:     !run !paperstats !career !equity !accuracy !analysis !attempts !trades
"""

import os
import re
import asyncio
import logging
import time as _time
import yaml

import discord
from discord.ext import commands
import matplotlib.pyplot as plt

from interface.fmt import (
    ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col,
    delta_col, ml_col, exit_reason_col, balance_col, wr_col,
    tier_col, drawdown_col, signed_col,
)
from interface.shared_state import (
    _send_embed,
    _append_footer,
    _add_field_icons,
    _format_ts,
    _format_pct_signed,
    _get_data_freshness_text,
    _get_status_line,
    _get_status_banner,
    _add_trend_arrow,
    _tag_trade_mode,
    _schedule_delete,
    _CHART_TTL,
    _safe_float,
)

from core.data_service import get_market_dataframe, get_symbol_csv_path
from core.account_repository import load_account
from core.paths import DATA_DIR
from core.session_scope import get_rth_session_view
from core.md_state import set_md_enabled, get_md_state, md_needs_warning, set_md_auto

from signals.conviction import calculate_conviction
from signals.opportunity import evaluate_opportunity
from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state

from analytics.prediction_stats import calculate_accuracy
from analytics.run_stats import get_run_stats
from analytics.performance import get_paper_stats, get_career_stats
from analytics.equity_curve import generate_equity_curve
from analytics.risk_metrics import calculate_r_metrics, calculate_drawdown
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.expectancy import calculate_expectancy
from analytics.feature_importance import get_feature_importance
from analytics.ml_accuracy import ml_rolling_accuracy
from analytics.decision_analysis import analyze_decision_quality

from interface.charting import generate_chart, generate_live_chart
from interface.watchers import get_decision_buffer_snapshot
from simulation.sim_contract import select_sim_contract_with_reason


class MarketCommands(commands.Cog, name="Market"):
    """Symbol snapshots, market analysis, and account performance commands."""

    def __init__(self, bot):
        self.bot = bot

    # ── Helper ────────────────────────────────────────────────────────────────

    async def _symbol_snapshot(self, ctx, symbol: str):
        """Generic symbol snapshot: price, indicators, chart. Used by !spy, !iwm, !tsla, etc."""
        symbol = symbol.upper()
        import pandas_ta as _ta
        try:
            # Load candle data from CSV via registry
            from datetime import date as _date
            import pandas as _pd
            csv_path = get_symbol_csv_path(symbol)
            df = None
            if csv_path and os.path.exists(csv_path):
                df = _pd.read_csv(csv_path)
                df.columns = [c.lower() for c in df.columns]
                ts_col = next((c for c in ("timestamp", "time", "datetime") if c in df.columns), None)
                if ts_col:
                    df[ts_col] = _pd.to_datetime(df[ts_col], errors="coerce")
                    df = df.dropna(subset=[ts_col])
                    if df[ts_col].dt.tz is not None:
                        df[ts_col] = df[ts_col].dt.tz_convert("US/Eastern").dt.tz_localize(None)
                    df = df.set_index(ts_col).sort_index()
                    # Filter to today only
                    today = _pd.Timestamp(_date.today())
                    df = df[df.index.date == today.date()]
            # For SPY fall back to get_market_dataframe
            if (df is None or df.empty) and symbol == "SPY":
                df = get_market_dataframe()
            if df is None or df.empty:
                await _send_embed(ctx, f"No data for {symbol} — run `!backfill 5 {symbol.lower()}` first.")
                return

            # Compute indicators if missing
            if "ema9" not in df.columns and len(df) >= 9:
                df["ema9"] = df["close"].ewm(span=9).mean()
            if "ema20" not in df.columns and len(df) >= 20:
                df["ema20"] = df["close"].ewm(span=20).mean()
            if "vwap" not in df.columns and "volume" in df.columns:
                try:
                    df["vwap"] = _ta.vwap(df["high"], df["low"], df["close"], df["volume"])
                except Exception:
                    df["vwap"] = df["close"]

            last = df.iloc[-1]
            recent = df.tail(120)
            high_price = recent["high"].max()
            low_price  = recent["low"].min()
            high_time  = recent["high"].idxmax()
            low_time   = recent["low"].idxmin()
            high_time_str = high_time.strftime('%H:%M') if hasattr(high_time, "strftime") else str(high_time)
            low_time_str  = low_time.strftime('%H:%M')  if hasattr(low_time,  "strftime") else str(low_time)

            # Generate chart (skip if cached copy is < 30 min old)
            os.makedirs("charts", exist_ok=True)
            chart_file = f"charts/snap_{symbol.lower()}.png"
            if not (os.path.exists(chart_file) and _time.time() - os.path.getmtime(chart_file) < _CHART_TTL):
                plt.figure(figsize=(8, 4))
                n = min(120, len(df))
                plt.plot(df.index[-n:], df["close"].iloc[-n:], label="Price")
                if "ema9"  in df.columns: plt.plot(df.index[-n:], df["ema9"].iloc[-n:],  label="EMA9")
                if "ema20" in df.columns: plt.plot(df.index[-n:], df["ema20"].iloc[-n:], label="EMA20")
                if "vwap"  in df.columns: plt.plot(df.index[-n:], df["vwap"].iloc[-n:],  label="VWAP")
                plt.legend(); plt.xticks(rotation=45); plt.tight_layout()
                plt.savefig(chart_file); plt.close()

            def _v(col): return f"${last[col]:.2f}" if col in last.index and last[col] == last[col] else "N/A"
            embed = discord.Embed(title=f"📡 {symbol} Snapshot", color=0x3498DB)
            embed.add_field(name="💰 Price",         value=ab(A(_v("close"), "white", bold=True)), inline=True)
            embed.add_field(name="📊 VWAP",          value=ab(A(_v("vwap"),  "cyan")),             inline=True)
            embed.add_field(name="📈 EMA9",          value=ab(A(_v("ema9"),  "yellow")),           inline=True)
            embed.add_field(name="📉 EMA20",         value=ab(A(_v("ema20"), "yellow")),           inline=True)
            embed.add_field(name="⬆️ Session High",  value=ab(f"{A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}"), inline=True)
            embed.add_field(name="⬇️ Session Low",   value=ab(f"{A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}"),   inline=True)
            _append_footer(embed)
            await ctx.send(embed=embed)
            if os.path.exists(chart_file):
                await ctx.send(file=discord.File(chart_file))
                asyncio.create_task(_schedule_delete(chart_file))
        except Exception as e:
            logging.exception(f"!{symbol.lower()} snapshot failed: {e}")
            await _send_embed(ctx, f"⚠️ Chart error for {symbol} — data may still be warming up.")

    # ── Symbol Snapshot Commands ──────────────────────────────────────────────

    @commands.command()
    async def spy(self, ctx):
        await self._symbol_snapshot(ctx, "SPY")

    @commands.command()
    async def qqq(self, ctx):
        await self._symbol_snapshot(ctx, "QQQ")

    @commands.command()
    async def iwm(self, ctx):
        await self._symbol_snapshot(ctx, "IWM")

    @commands.command()
    async def vxx(self, ctx):
        await self._symbol_snapshot(ctx, "VXX")

    @commands.command()
    async def tsla(self, ctx):
        await self._symbol_snapshot(ctx, "TSLA")

    @commands.command()
    async def aapl(self, ctx):
        await self._symbol_snapshot(ctx, "AAPL")

    @commands.command()
    async def nvda(self, ctx):
        await self._symbol_snapshot(ctx, "NVDA")

    @commands.command()
    async def msft(self, ctx):
        await self._symbol_snapshot(ctx, "MSFT")

    @commands.command(name="quote")
    async def quote(self, ctx, symbol: str | None = None):
        """Generic: !quote <SYMBOL>"""
        if not symbol:
            await _send_embed(ctx, "Usage: `!quote <SYMBOL>` — e.g. `!quote tsla`")
            return
        await self._symbol_snapshot(ctx, symbol)

    # ── Market Analysis Commands ──────────────────────────────────────────────

    @commands.command()
    async def risk(self, ctx):

        r = calculate_r_metrics()
        dd = calculate_drawdown()

        if not r:
            await _send_embed(
                ctx,
                "⚠️ Need at least 10 closed trades to calculate R metrics.\n"
                "Close more trades before evaluating performance."
            )
            return

        if not dd:
            await _send_embed(
                ctx,
                "⚠️ Drawdown metrics unavailable.\n"
                "Close at least 1 trade to calculate drawdown."
            )
            return

        risk_embed = discord.Embed(title="📊 Risk Metrics", color=0x3498DB)
        risk_embed.add_field(name="📊 Avg R",       value=ab(A(str(r['avg_R']), "yellow", bold=True)), inline=True)
        risk_embed.add_field(name="✅ Avg Win R",   value=ab(A(str(r['avg_win_R']), "green", bold=True)), inline=True)
        risk_embed.add_field(name="❌ Avg Loss R",  value=ab(A(str(r['avg_loss_R']), "red", bold=True)), inline=True)
        risk_embed.add_field(name="🏆 Max R",       value=ab(A(str(r['max_R']), "green")), inline=True)
        risk_embed.add_field(name="💀 Min R",       value=ab(A(str(r['min_R']), "red")), inline=True)
        risk_embed.add_field(name="📉 Max Drawdown", value=ab(drawdown_col(dd['max_drawdown_dollars'])), inline=True)
        _append_footer(risk_embed)
        await ctx.send(embed=risk_embed)

    @commands.command()
    async def md(self, ctx, action: str | None = None, level: str | None = None):
        try:
            cmd = (action or "status").strip().lower()
            if cmd not in {"enable", "disable", "status", "auto"}:
                await _send_embed(ctx, "Usage: `!md enable`, `!md disable`, `!md status`, or `!md auto <low|medium|high>`")
                return

            if cmd == "enable":
                state = set_md_enabled(True)
                status_text = "ENABLED"
            elif cmd == "disable":
                state = set_md_enabled(False)
                status_text = "DISABLED"
            elif cmd == "auto":
                state = set_md_auto(level or "medium")
                status_text = "AUTO (ARMED OFF UNTIL DETECTION)"
            else:
                state = get_md_state()
                status_text = "ENABLED" if state.get("enabled") else "DISABLED"

            enabled = bool(state.get("enabled"))
            mode = str(state.get("mode", "manual")).upper()
            auto_level = str(state.get("auto_level", "medium")).upper()
            last_decay = state.get("last_decay")
            last_decay_level = state.get("last_decay_level")
            last_change = state.get("last_changed")
            market_open_prev = state.get("market_open_prev")

            embed = discord.Embed(
                title="🧭 Momentum Decay Strict Mode",
                color=0x2ECC71 if enabled else 0xE74C3C,
            )
            embed.add_field(
                name="Status",
                value=ab(A(status_text, "green" if enabled else "red", bold=True)),
                inline=False,
            )
            embed.add_field(
                name="Mode",
                value=ab(A(mode, "cyan", bold=True)),
                inline=True,
            )
            if mode == "AUTO":
                embed.add_field(
                    name="Auto Level",
                    value=ab(A(auto_level, "yellow", bold=True)),
                    inline=True,
                )
                market_text = "OPEN" if market_open_prev else "CLOSED"
                embed.add_field(
                    name="Market Session",
                    value=ab(A(market_text, "green" if market_open_prev else "red")),
                    inline=True,
                )
            embed.add_field(
                name="Last Decay",
                value=ab(A(_format_ts(last_decay) if last_decay else "None", "cyan")),
                inline=True,
            )
            embed.add_field(
                name="Last Decay Level",
                value=ab(A(str(last_decay_level).upper() if last_decay_level else "None", "cyan")),
                inline=True,
            )
            embed.add_field(
                name="Last Change",
                value=ab(A(_format_ts(last_change) if last_change else "None", "cyan")),
                inline=True,
            )

            if enabled and md_needs_warning(state):
                embed.add_field(
                    name="⚠️ Warning",
                    value=ab(A("MD strict is enabled but no recent decay detected. Consider disabling.", "yellow")),
                    inline=False,
                )

            if not enabled:
                embed.add_field(
                    name="How It Works",
                    value=ab(A("When ON, stop losses tighten during momentum decay.", "yellow")),
                    inline=False,
                )
            if mode == "AUTO":
                embed.add_field(
                    name="Auto Rule",
                    value=ab(A("MD stays OFF at session transitions, then turns ON only when decay is detected at/above the selected level. It turns OFF again if decay drops below level.", "yellow")),
                    inline=False,
                )

            _append_footer(embed)
            await ctx.send(embed=embed)
        except Exception:
            logging.exception("md_error")
            await _send_embed(ctx, "md failed due to an internal error.")

    @commands.command()
    async def expectancy(self, ctx):

        stats = calculate_expectancy()

        if not stats:
            await _send_embed(ctx, "Need at least 10 closed trades to calculate expectancy.")
            return

        exp_embed = discord.Embed(title="📊 Rolling Expectancy", color=0x3498DB)
        exp_embed.add_field(name="📊 Avg R",      value=ab(A(str(stats['avg_R']), "yellow", bold=True)), inline=True)
        exp_embed.add_field(name="🎯 Win Rate",   value=ab(wr_col(stats['winrate'] / 100.0)), inline=True)
        exp_embed.add_field(name="💰 Expectancy", value=ab(pnl_col(stats['expectancy'])), inline=True)
        exp_embed.add_field(name="📦 Trades",     value=ab(A(str(stats['samples']), "white")), inline=True)
        _append_footer(exp_embed)
        await ctx.send(embed=exp_embed)

    @commands.command(name="replay")
    async def replay(self, ctx, symbol: str | None = None):
        sym = (symbol or "SPY").upper()
        try:
            chart_path = generate_chart(sym)

            if chart_path:
                await _send_embed(ctx, f"📊 {sym} Recorded Session:")
                await ctx.send(file=discord.File(chart_path))
                asyncio.create_task(_schedule_delete(chart_path))
            else:
                await _send_embed(ctx, f"No recorded session data for {sym} — run `!backfill 5 {sym.lower()}` first.")

            live_path = generate_live_chart(sym)

            if live_path and os.path.exists(live_path):
                await _send_embed(ctx, f"📈 {sym} Live Market:")
                await ctx.send(file=discord.File(live_path))
                asyncio.create_task(_schedule_delete(live_path))
            else:
                await _send_embed(ctx, f"Market closed or no live data for {sym}.")

        except Exception as e:
            logging.exception(f"Replay error: {e}")
            await _send_embed(ctx, "Replay failed — check logs.")

    @commands.command()
    async def importance(self, ctx):

        data = get_feature_importance()

        if not data:
            await _send_embed(
                ctx,
                "Model not trained yet.\n"
                "Run `!retrain` after 50+ trades."
            )
            return

        message = "📊 Feature Importance (Edge Model)\n\n"

        for name, score in data:
            message += f"{name}: {round(score, 3)}\n"

        await _send_embed(ctx, message, title="Feature Importance")

    @commands.command()
    async def regime(self, ctx):
        df = get_market_dataframe()
        if df is None:
            await _send_embed(ctx, "No data.")
            return
        await _send_embed(ctx, f"Market Regime: {get_regime(df)}", title="Regime")

    @commands.command()
    async def regimes(self, ctx):

        data = calculate_regime_expectancy()

        if not data:
            await _send_embed(ctx, "Need at least 10 closed trades to calculate meaningful expextancy metrics.")
            return

        message = "📊 Regime Expectancy (R-Multiple)\n\n"

        for regimes, stats in data.items():
            message += (
                f"{regimes}\n"
                f"Trades: {stats['trades']}\n"
                f"Avg R: {stats['avg_R']}\n"
                f"Winrate: {stats['winrate']}%\n\n"
            )

        await _send_embed(ctx, message, title="Regime Expectancy")

    @commands.command()
    async def conviction(self, ctx):
        df = get_market_dataframe()
        if df is None:
            await _send_embed(ctx, "No data.")
            return

        score, impulse, follow, direction = calculate_conviction(df)

        await _send_embed(
            ctx,
            (
                f"Conviction Score: {score}\n"
                f"Direction: {direction}\n"
                f"Impulse: {impulse:.2f}\n"
                f"Follow: {follow*100:.0f}%"
            ),
            title="Conviction"
        )

    @commands.command()
    async def opportunity(self, ctx):
        df = get_market_dataframe()
        if df is None:
            await _send_embed(ctx, "No data.")
            return

        result = evaluate_opportunity(df)

        if result:
            if not isinstance(result, (list, tuple)) or len(result) < 5:
                await _send_embed(ctx, "No opportunity right now.")
                return
            side = result[0]
            low = result[1]
            high = result[2]
            price = result[3]
            tp_low = tp_high = stop_loss = None
            if len(result) >= 8:
                tp_low = result[5]
                tp_high = result[6]
                stop_loss = result[7]
            lines = [
                f"{side} setup",
                f"Zone: {low:.2f}-{high:.2f}",
                f"Current: ${price:.2f}",
            ]
            if tp_low is not None and tp_high is not None:
                lines.append(f"Take-Profit: {tp_low:.2f}-{tp_high:.2f}")
            if stop_loss is not None:
                lines.append(f"Stop-Loss: {stop_loss:.2f}")
            await _send_embed(
                ctx,
                "\n".join(lines),
                title="Opportunity"
            )
        else:
            await _send_embed(ctx, "No opportunity right now.")

    @commands.command()
    async def predict(self, ctx, minutes: str | None = None):

        if minutes is None:
            await _send_embed(
                ctx,
                "Usage: `!predict <minutes>`\n"
                "Allowed values: 30 or 60\n"
                "Example: `!predict 30`"
            )
            return

        if not isinstance(minutes, str):
            await _send_embed(
                ctx,
                "Minutes must be text input.\n"
                "Example: `!predict 60`"
            )
            return

        if not minutes.isdigit():
            await _send_embed(
                ctx,
                "Minutes must be a number.\n"
                "Example: `!predict 60`"
            )
            return

        timeframe_minutes = int(minutes)

        if timeframe_minutes not in [30, 60]:
            await _send_embed(
                ctx,
                "Invalid timeframe.\n"
                "Allowed values: 30 or 60."
            )
            return

        df = get_market_dataframe()

        if df is None:
            await _send_embed(ctx, "Market data unavailable.")
            return

        pred = make_prediction(timeframe_minutes, df)

        if not pred:
            await _send_embed(
                ctx,
                "Not enough graded predictions.\n"
                "Need at least 5 graded predictions."
            )
            return

        pred_color = 0x2ECC71 if pred['direction'] == "bullish" else 0xE74C3C if pred['direction'] == "bearish" else 0x95A5A6
        pred_embed = discord.Embed(title=f"📊 SPY {timeframe_minutes}m Forecast", color=pred_color)
        pred_embed.add_field(name="📍 Direction",   value=ab(dir_col(pred['direction'])), inline=True)
        pred_embed.add_field(name="💡 Confidence",  value=ab(conf_col(pred['confidence'])), inline=True)
        pred_embed.add_field(name="🎯 Predicted High", value=ab(A(str(pred['high']), "green", bold=True)), inline=True)
        pred_embed.add_field(name="🎯 Predicted Low",  value=ab(A(str(pred['low']), "red", bold=True)), inline=True)

        if timeframe_minutes == 30:
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
            if isinstance(high_time_str, str) and "ET" not in high_time_str:
                high_time_str = f"{high_time_str} ET"
            if isinstance(low_time_str, str) and "ET" not in low_time_str:
                low_time_str = f"{low_time_str} ET"

            _pc = f"${last['close']:.2f}"
            _pv = f"${last['vwap']:.2f}"
            _pe9 = f"${last['ema9']:.2f}"
            _pe20 = f"${last['ema20']:.2f}"
            pred_embed.add_field(
                name="📍 Market Snapshot",
                value=ab(
                    f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                    f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                    f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
                ),
                inline=False
            )
            pred_embed.add_field(
                name="📈 Session Range",
                value=ab(
                    f"{lbl('High')} {A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}",
                    f"{lbl('Low')}  {A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}",
                ),
                inline=False
            )

        _append_footer(pred_embed)
        await ctx.send(embed=pred_embed)

    @commands.command()
    async def plan(self, ctx, side=None, strike=None, premium=None, contracts=None, expiry=None):

        if not all([side, strike, premium, contracts, expiry]):
            await _send_embed(
                ctx,
                "Usage:\n"
                "!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>\n\n"
                "Example:\n"
                "!plan call 435 1.20 2 2026-02-14"
            )
            return

        if not isinstance(side, str):
            await _send_embed(ctx, "Side must be provided as text.")
            return

        side = side.lower()

        if side not in ["call", "put"]:
            await _send_embed(ctx, "Side must be 'call' or 'put'.")
            return

        if strike is None or premium is None or contracts is None:
            await _send_embed(ctx, "Strike, premium, and contracts are required.")
            return

        if isinstance(strike, str):
            strike_text = strike.strip()
            if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", strike_text):
                await _send_embed(ctx, "Strike must be numeric.")
                return
            strike = float(strike_text)
        elif isinstance(strike, (int, float)):
            strike = float(strike)
        else:
            await _send_embed(ctx, "Strike must be numeric.")
            return

        if isinstance(premium, str):
            premium_text = premium.strip()
            if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", premium_text):
                await _send_embed(ctx, "Premium must be numeric.")
                return
            premium = float(premium_text)
        elif isinstance(premium, (int, float)):
            premium = float(premium)
        else:
            await _send_embed(ctx, "Premium must be numeric.")
            return

        if isinstance(contracts, str):
            contracts_text = contracts.strip()
            if not re.fullmatch(r"[+-]?\d+", contracts_text):
                await _send_embed(ctx, "Contracts must be a whole number.")
                return
            contracts = int(contracts_text)
        elif isinstance(contracts, int):
            contracts = int(contracts)
        else:
            await _send_embed(ctx, "Contracts must be a whole number.")
            return
        try:
            df = get_market_dataframe()
            if df is None:
                await _send_embed(ctx, "Market data unavailable.")
                return

            regime = get_regime(df)
            vol = volatility_state(df)
            score, impulse, follow, direction = calculate_conviction(df)

            price = df.iloc[-1]["close"]

            # Determine bias alignment
            bias_alignment = "Aligned" if (
                (side == "call" and direction == "bullish") or
                (side == "put" and direction == "bearish")
            ) else "Against Bias"

            # ATR context
            atr = df.iloc[-1]["atr"]
            distance_from_strike = abs(price - strike)

            # Basic risk math
            total_cost = premium * contracts * 100

            # Basic grade logic
            grade_score = 0

            if bias_alignment == "Aligned":
                grade_score += 1
            if regime == "TREND":
                grade_score += 1
            if vol == "NORMAL" or vol == "HIGH":
                grade_score += 1
            if score >= 4:
                grade_score += 1

            if grade_score >= 4:
                grade = "A"
            elif grade_score == 3:
                grade = "B"
            elif grade_score == 2:
                grade = "C"
            else:
                grade = "D"

            embed = discord.Embed(
                title="📋 Trade Plan Analysis",
                color=discord.Color.green() if grade in ["A", "B"] else discord.Color.orange()
            )

            embed.add_field(name="Side", value=side.upper(), inline=True)
            embed.add_field(name=_add_field_icons("Strike"), value=strike, inline=True)
            embed.add_field(name=_add_field_icons("Premium"), value=premium, inline=True)

            embed.add_field(name=_add_field_icons("Contracts"), value=contracts, inline=True)
            embed.add_field(name=_add_field_icons("Total Cost"), value=f"${total_cost:.2f}", inline=True)
            embed.add_field(name=_add_field_icons("Expiry"), value=expiry, inline=True)

            embed.add_field(
                name=_add_field_icons("Market State"),
                value=(
                    f"Regime: {regime}\n"
                    f"Volatility: {vol}\n"
                    f"Conviction Score: {score}\n"
                    f"Impulse: {round(impulse,2)}\n"
                    f"Follow Through: {round(follow*100,1)}%\n"
                ),
                inline=False
            )

            embed.add_field(
                name=_add_field_icons("Structure Context"),
                value=(
                    f"Current Price: {price:.2f}\n"
                    f"Distance from Strike: {distance_from_strike:.2f}\n"
                    f"ATR: {atr:.2f}\n"
                    f"Bias Alignment: {bias_alignment}"
                ),
                inline=False
            )

            embed.add_field(name=_add_field_icons("Final Grade"), value=f"🏆 {grade}", inline=False)
            _append_footer(embed)

            await ctx.send(embed=embed)
        except Exception as e:
            logging.exception(f"!plan failed: {e}")
            await _send_embed(ctx, "⚠️ Plan error — data still warming up.")

    @commands.command()
    async def mlstats(self, ctx):

        acc = ml_rolling_accuracy()

        if acc is None:
            await _send_embed(
                ctx,
                "Not enough ML trade data.\n"
                "Need at least 30 ML-evaluated trades."
            )
        else:
            await _send_embed(ctx, f"🧠 ML Rolling Accuracy (Last 30 Trades): {acc}%")

    @commands.command(name="preopen")
    async def preopen(self, ctx):
        """
        Pre-open readiness check:
        - Market status
        - Data freshness
        - Option contract snapshot sanity (best-effort)
        """
        try:
            df = get_market_dataframe()
            if df is None or df.empty:
                await _send_embed(ctx, "Market data unavailable.", title="Pre-Open Check", color=0xE74C3C)
                return

            market_open = df.attrs.get("market_open")
            market_status = "OPEN" if market_open else "CLOSED"
            data_freshness = _get_data_freshness_text() or "Data age: N/A"

            last_close = df.iloc[-1].get("close") if len(df) > 0 else None
            last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
            close_text = f"{last_close_val:.2f}" if isinstance(last_close_val, (int, float)) else "N/A"

            profile_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "simulation", "sim_config.yaml")
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
                    if ok:
                        status = "🟢 OK"
                        detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
                    else:
                        status = "🔴 FAIL"
                        detail = row.get("reason", "unavailable")
                    lines.append(f"{label:<6} {status:<8} {detail}")
                return "```\n" + "\n".join(lines) + "\n```"

            def _check_contracts(direction: str, base_profile: dict) -> tuple[str, bool]:
                rows = []
                any_ok = False
                if not base_profile:
                    return "Profile unavailable", False
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
                        contract, reason = select_sim_contract_with_reason(direction, last_close_val, prof)
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

            color = 0x2ECC71
            if bull_ok or bear_ok:
                color = 0x2ECC71
            elif market_status == "CLOSED":
                color = 0xF39C12
            else:
                color = 0xE74C3C

            title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
            embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
            embed.add_field(name=_add_field_icons("Market"), value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
            embed.add_field(name=_add_field_icons("Last Price"), value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
            embed.add_field(name=_add_field_icons("Data Freshness"), value=ab(A(data_freshness, "cyan")), inline=False)
            embed.add_field(name=_add_field_icons("Option Snapshot"), value=ab(A(contract_status, "green" if contract_status == "OK" else "yellow", bold=True)), inline=False)
            embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
            embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)
            if contract_reason:
                embed.add_field(name=_add_field_icons("Reason"), value=ab(A(contract_reason, "red", bold=True)), inline=False)
            _append_footer(embed)
            await ctx.send(embed=embed)
        except Exception as e:
            logging.exception("preopen_error: %s", e)
            await _send_embed(ctx, "Pre-open check failed.", title="Pre-Open Check", color=0xE74C3C)

    # ── Account Performance Commands ──────────────────────────────────────────

    @commands.command()
    async def run(self, ctx):
        s = get_run_stats()
        await _send_embed(
            ctx,
            (
                f"Trades: {s['trades']}\n"
                f"Wins: {s['wins']}\n"
                f"Losses: {s['losses']}\n"
                f"Balance: ${s['current']}"
            ),
            title="Run Stats"
        )

    @commands.command()
    async def paperstats(self, ctx):
        s = get_paper_stats()
        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        pnl_badge = "⚪"
        try:
            pnl_val = float(s["pnl"])
            if pnl_val > 0:
                pnl_badge = "✅"
            elif pnl_val < 0:
                pnl_badge = "❌"
        except (TypeError, ValueError, KeyError):
            pnl_badge = "⚪"
        lines = [
            f"💰 Balance: ${s['balance']:.2f}",
            f"📈 PnL: {pnl_badge} ${s['pnl']:.2f}",
            f"🎯 Winrate: {s['winrate']}%",
        ]
        if status_line:
            lines.append(status_line)
        if freshness_line:
            lines.append(freshness_line)
        await _send_embed(
            ctx,
            "\n".join(lines),
            title="Paper Stats"
        )

    @commands.command()
    async def career(self, ctx):
        c = get_career_stats()
        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        win_badge = "⚪"
        try:
            winrate_val = float(c["winrate"])
            if winrate_val >= 50:
                win_badge = "✅"
            else:
                win_badge = "❌"
        except (TypeError, ValueError, KeyError):
            win_badge = "⚪"
        lines = [
            f"📦 Total Trades: {c['total_trades']}",
            f"🎯 Winrate: {win_badge} {c['winrate']}%",
            f"🏆 Best Balance: ${c['best_balance']:.2f}",
        ]
        if status_line:
            lines.append(status_line)
        if freshness_line:
            lines.append(freshness_line)
        await _send_embed(
            ctx,
            "\n".join(lines),
            title="Career Stats"
        )

    @commands.command()
    async def equity(self, ctx):
        chart = generate_equity_curve()
        if chart:
            embed = discord.Embed(title="📈 Equity Curve", color=0x3498DB)
            _append_footer(embed)
            await ctx.send(embed=embed, file=discord.File(chart))
        else:
            await _send_embed(ctx, "Need at least 1 closed trade(s) to see equity.")

    @commands.command()
    async def accuracy(self, ctx):
        stats = calculate_accuracy()
        if stats:
            await _send_embed(ctx, str(stats), title="Accuracy")
        else:
            await _send_embed(ctx, "Need at least 5 graded predictions before accuracy can be calculated.")

    @commands.command()
    async def analysis(self, ctx):
        try:
            acc = load_account()
        except Exception:
            await _send_embed(ctx, "Could not load account data.")
            return

        results = analyze_decision_quality(acc.get("trade_log", []))
        total = results.get("total_trades_analyzed", 0)
        corr_delta = results.get("corr_threshold_delta_vs_R")
        corr_blended = results.get("corr_blended_vs_R")

        corr_delta_text = "N/A" if corr_delta is None else f"{corr_delta:.4f}"
        corr_blended_text = "N/A" if corr_blended is None else f"{corr_blended:.4f}"

        lines = [
            "📊 Decision Analysis Summary",
            f"📦 Trades Analyzed: {total}",
            f"📈 Corr Delta vs R: {corr_delta_text}",
            f"🧠 Corr Blended vs R: {corr_blended_text}",
        ]

        exec_stats = acc.get("execution_stats")
        no_record = exec_stats.get("no_record_exits") if isinstance(exec_stats, dict) else None
        if isinstance(no_record, dict) and no_record:
            no_record_lines = []
            for key in sorted(no_record.keys()):
                no_record_lines.append(f"{key}: {no_record.get(key)}")
            stats_text = "\n".join(no_record_lines)
            lines.append("Execution No-Record Exits:")
            lines.append(stats_text)

        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        if status_line:
            lines.append(status_line)
        if freshness_line:
            lines.append(freshness_line)

        await _send_embed(ctx, "\n".join(lines)[:1500], title="Analysis")

    @commands.command()
    async def attempts(self, ctx):
        snap = get_decision_buffer_snapshot()
        avg_delta = snap.get("avg_delta")
        avg_delta_text = "N/A" if avg_delta is None else f"{avg_delta:.3f}"
        ml_weight = snap.get("ml_weight")
        ml_weight_text = "N/A" if ml_weight is None else f"{ml_weight:.3f}"

        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        lines = [
            "📊 **Decision Attempts (Runtime)**",
            f"Attempts: {snap.get('attempts', 0)}",
            f"Opened: {snap.get('opened', 0)}",
            f"Blocked: {snap.get('blocked', 0)}",
            f"Top Block Reason: {snap.get('top_block_reason', 'N/A')}",
            f"ML Weight: {ml_weight_text}",
            f"Avg Blended vs Threshold (Last 20): {avg_delta_text}",
        ]
        if status_line:
            lines.append(status_line)
        if freshness_line:
            lines.append(freshness_line)

        await _send_embed(
            ctx,
            "\n".join(lines),
            title="Attempts"
        )

    @commands.command()
    async def trades(self, ctx, page: str | int = 1):
        def _safe_money(val, decimals=2):
            try:
                return f"${float(val):.{decimals}f}"
            except (TypeError, ValueError):
                return "N/A"

        def _safe_float_local(val):
            try:
                if val is None:
                    return None
                return float(val)
            except (TypeError, ValueError):
                return None

        def _safe_r(val):
            try:
                return f"{float(val):.3f}R"
            except (TypeError, ValueError):
                return "N/A"

        def _badge_from_pnl(val):
            try:
                num = float(val)
            except (TypeError, ValueError):
                return "⚪"
            if num > 0:
                return "✅"
            if num < 0:
                return "❌"
            return "⚪"

        def _format_duration(seconds):
            try:
                total = int(seconds)
            except (TypeError, ValueError):
                return "N/A"
            if total < 0:
                return "N/A"
            hours = total // 3600
            minutes = (total % 3600) // 60
            secs = total % 60
            if hours > 0:
                return f"{hours}h {minutes}m {secs}s"
            return f"{minutes}m {secs}s"

        try:
            acc = load_account()
        except Exception:
            await _send_embed(ctx, "Could not load account data.")
            return

        trade_log = acc.get("trade_log", [])
        if not trade_log:
            await _send_embed(ctx, "No closed trades yet.")
            return

        per_page = 5
        total = len(trade_log)
        total_pages = (total + per_page - 1) // per_page

        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!trades 1` to `!trades {total_pages}`.")
            return

        # Show newest trades first
        newest_first = list(reversed(trade_log))

        def _build_trades_embed(page_num: int) -> "discord.Embed":
            page_num = max(1, min(page_num, total_pages))
            start = (page_num - 1) * per_page
            end = start + per_page
            page_trades = newest_first[start:end]

            lines = [f"📒 **Trade Log** (Page {page_num}/{total_pages})"]
            page_pnl = 0.0
            page_pnl_count = 0
            for idx, t in enumerate(page_trades, start=start + 1):
                trade_type = str(t.get("type", "unknown")).upper()
                style = t.get("style", "unknown")
                result = str(t.get("result", "unknown")).upper()
                entry_time = _format_ts(t.get("entry_time", "N/A"))
                exit_time = _format_ts(t.get("exit_time", "N/A"))
                exit_reason = t.get("result_reason") or t.get("exit_reason") or "N/A"

                risk = t.get("risk")
                pnl = t.get("pnl")
                r_mult = t.get("R")
                balance_after = t.get("balance_after")
                hold_text = _format_duration(t.get("time_in_trade_seconds"))
                mode_tag = _tag_trade_mode(t)
                pnl_arrow = _add_trend_arrow(pnl, good_when_high=True)

                risk_text = _safe_money(risk)
                pnl_text = _safe_money(pnl)
                r_text = _safe_r(r_mult)
                bal_text = _safe_money(balance_after)
                badge = _badge_from_pnl(pnl)
                pnl_pct_text = "N/A"
                entry_price = _safe_float_local(t.get("entry_price"))
                exit_price = _safe_float_local(t.get("exit_price"))
                try:
                    if entry_price is not None and exit_price is not None and entry_price != 0:
                        pnl_pct = (float(exit_price) - float(entry_price)) / float(entry_price)
                        pnl_pct_text = _format_pct_signed(pnl_pct)
                except (TypeError, ValueError):
                    pnl_pct_text = "N/A"

                snapshot = t.get("decision_snapshot", {})
                delta = snapshot.get("threshold_delta") if isinstance(snapshot, dict) else None
                blended = snapshot.get("blended_score") if isinstance(snapshot, dict) else None
                delta_text = None
                try:
                    if delta is not None and blended is not None:
                        delta_val = float(delta)
                        blended_val = float(blended)
                        delta_text = f"Delta: {delta_val:+.4f} | Blended: {blended_val:.4f}"
                except (TypeError, ValueError):
                    delta_text = None

                symbol = t.get("option_symbol")
                strike = t.get("strike")
                expiry = t.get("expiry")
                qty = t.get("quantity")

                contract_line = ""
                if symbol:
                    contract_line = f"Contract: {symbol}\n"
                elif strike and expiry:
                    contract_line = f"{strike} | Exp: {expiry} | Qty: {qty}\n"

                lines.append(
                    f"\n{badge} **#{idx}** {trade_type} ({style}) - {result} | {mode_tag}\n"
                    f"{contract_line}"
                    f"Risk: {risk_text} | PnL: {pnl_text} ({pnl_pct_text}) {pnl_arrow} | R: {r_text}\n"
                    f"{delta_text + chr(10) if delta_text else ''}"
                    f"Entry: {entry_time}\n"
                    f"Exit: {exit_time}\n"
                    f"Hold: {hold_text} | Reason: {exit_reason}\n"
                    f"Balance After: {bal_text}"
                )

                try:
                    pnl_val = float(pnl) if pnl is not None else None
                except (TypeError, ValueError):
                    pnl_val = None
                if pnl_val is not None:
                    page_pnl += pnl_val
                    page_pnl_count += 1

            final_message = "\n".join(lines)
            if page_pnl_count > 0:
                if page_pnl > 0:
                    color = 0x2ECC71
                elif page_pnl < 0:
                    color = 0xE74C3C
                else:
                    color = 0x3498DB
            else:
                color = 0x3498DB

            embed = discord.Embed(title=f"📒 Trade Log (Page {page_num}/{total_pages})", description=final_message, color=color)
            banner = _get_status_banner()
            if banner:
                embed.add_field(name="🧭 Status Banner", value=banner, inline=False)
            _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
            return embed

        async def _send_trades_paginated(start_page: int):
            page_num = max(1, min(start_page, total_pages))
            message = await ctx.send(embed=_build_trades_embed(page_num))
            if total_pages <= 1:
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
                    page_num = total_pages if page_num == 1 else page_num - 1
                elif emoji == "▶️":
                    page_num = 1 if page_num == total_pages else page_num + 1

                try:
                    await message.edit(embed=_build_trades_embed(page_num))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        await _send_trades_paginated(page_num)


async def setup(bot):
    await bot.add_cog(MarketCommands(bot))
