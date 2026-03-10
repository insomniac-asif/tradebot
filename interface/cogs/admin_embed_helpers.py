"""
interface/cogs/admin_embed_helpers.py
--------------------------------------
Embed-building helpers extracted from admin_commands.py to reduce its line count.
"""

import discord
from interface.shared_state import _append_footer, _add_field_icons, _format_ts


def _build_system_status_embed(acc, status, report, market_status, now, total_trades) -> discord.Embed:
    """Build the !system command embed. Returns a discord.Embed."""
    embed = discord.Embed(
        title="🧠 SPY AI Control Center",
        color=discord.Color.green() if status == "HEALTHY" else discord.Color.orange()
    )
    embed.add_field(name=_add_field_icons("Market"), value=market_status, inline=True)
    embed.add_field(name=_add_field_icons("System Health"), value=status, inline=True)
    embed.add_field(name=_add_field_icons("System Diagnostics"), value=f"```\n{report}\n```", inline=False)

    if total_trades == 0:
        trade_status = "No closed trades yet."
    else:
        trade_status = f"{total_trades} closed trades"

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
    return embed
