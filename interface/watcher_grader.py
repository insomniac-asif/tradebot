# interface/watcher_grader.py
#
# Prediction grader cycle helper extracted from watchers.py.
# Contains: _run_grader_cycle

import logging
from datetime import datetime

import discord
import pandas as pd
import pytz

from core.debug import debug_log
from interface.fmt import ab, A, wr_col


async def _run_grader_cycle(bot, channel, preds) -> None:
    """
    Process graded predictions and emit a drift warning if winrate degrades.
    preds: a pandas DataFrame or None.
    """
    if preds is None or preds.empty:
        return
    if "checked" not in preds.columns or "correct" not in preds.columns:
        return

    graded = preds[preds["checked"] == True].tail(100)
    if len(graded) == 0:
        return

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
            from interface.watcher_utils import _format_et
            embed = discord.Embed(title="⚠️ Predictor Drift Warning", color=0xE74C3C)
            embed.add_field(name="📉 Rolling Winrate (last 100)", value=ab(wr_col(current_winrate)), inline=True)
            embed.add_field(name="📊 Baseline Winrate", value=ab(wr_col(baseline)), inline=True)
            embed.add_field(name="📉 Degradation", value=ab(A(f"{(baseline - current_winrate)*100:.1f}pp", "red", bold=True)), inline=True)
            embed.add_field(name="⚡ Action Needed", value=ab(A("Model accuracy has dropped >15% vs baseline. Consider retraining with `!retrain`.", "yellow")), inline=False)
            embed.set_footer(text=f"Samples: {len(graded)} | {_format_et(now)}")
            from interface.watcher_utils import _send
            await _send(channel, embed=embed)
            bot.last_predictor_drift_warning_time = now
        bot.predictor_drift_state = True
    else:
        bot.predictor_drift_state = False
