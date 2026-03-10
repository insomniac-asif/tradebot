# interface/watcher_loops.py
#
# Helpers for periodic report loops extracted from watchers.py.
# Contains: _run_eod_report, _run_chain_health_embed

import discord

from simulation.sim_contract import get_contract_error_stats
from interface.fmt import ab, A, lbl


def _run_eod_report(acc: dict) -> discord.Embed:
    """Build the end-of-day open trades embed from the given account dict."""
    open_items = []
    t = acc.get("open_trade")
    if isinstance(t, dict):
        open_items.append(t)
    open_trades = acc.get("open_trades")
    if isinstance(open_trades, list):
        for item in open_trades:
            if isinstance(item, dict):
                open_items.append(item)

    if not open_items:
        embed = discord.Embed(
            title="📌 End-of-Day Open Trades (Live)",
            description="No open live trades at end of day.",
            color=0x2ECC71,
        )
    else:
        embed = discord.Embed(
            title="📌 End-of-Day Open Trades (Live)",
            description="Open trades at market close.",
            color=0xF39C12,
        )
        for trade in open_items:
            symbol = trade.get("option_symbol") or trade.get("symbol", "unknown")
            qty = trade.get("quantity") or trade.get("qty")
            entry_price = trade.get("entry_price")
            stop = trade.get("stop")
            target = trade.get("target")
            entry_text = f"${entry_price:.2f}" if isinstance(entry_price, (int, float)) else "N/A"
            stop_text = f"${stop:.2f}" if isinstance(stop, (int, float)) else "N/A"
            target_text = f"${target:.2f}" if isinstance(target, (int, float)) else "N/A"
            pnl_text = "N/A"
            try:
                from execution.option_executor import get_option_price
                current_price = get_option_price(symbol)
                if current_price is not None and isinstance(entry_price, (int, float)) and isinstance(qty, (int, float)):
                    pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                    pnl_text = f"{'+' if pnl_val >= 0 else ''}${pnl_val:.2f}"
            except Exception:
                pnl_text = "N/A"
            embed.add_field(
                name=f"{symbol}",
                value=f"Qty {qty} | Entry {entry_text} | Stop {stop_text} | Target {target_text} | PnL {pnl_text}",
                inline=False
            )
    return embed


def _build_chain_health_embed(now_et) -> discord.Embed:
    """Build the option chain health embed for the last 60 minutes."""
    stats = get_contract_error_stats(3600)
    chain_errors = stats.get("chain_errors", 0)
    snapshot_errors = stats.get("snapshot_errors", 0)
    last_chain = stats.get("last_chain_error")
    last_snap = stats.get("last_snapshot_error")
    last_success = stats.get("last_success")

    color = 0x2ECC71 if (chain_errors + snapshot_errors) == 0 else 0xF39C12
    title = "✅ Option Chain Health (Last 60m)" if color == 0x2ECC71 else "⚠️ Option Chain Health (Last 60m)"
    embed = discord.Embed(title=title, color=color)
    embed.add_field(name="Chain Errors", value=ab(A(str(chain_errors), "red" if chain_errors else "green", bold=True)), inline=True)
    embed.add_field(name="Snapshot Errors", value=ab(A(str(snapshot_errors), "red" if snapshot_errors else "green", bold=True)), inline=True)
    if last_chain:
        embed.add_field(name="Last Chain Error", value=ab(A(str(last_chain[1])[:200], "red")), inline=False)
    else:
        embed.add_field(name="Last Chain Error", value=ab(A("None", "green")), inline=False)
    if last_snap:
        embed.add_field(name="Last Snapshot Error", value=ab(A(str(last_snap[1])[:200], "red")), inline=False)
    else:
        embed.add_field(name="Last Snapshot Error", value=ab(A("None", "green")), inline=False)
    if last_success:
        sym = last_success.get("symbol")
        spr = last_success.get("spread_pct")
        ct = last_success.get("contract_type")
        spr_text = f"{spr:.3f}" if isinstance(spr, (int, float)) else "N/A"
        embed.add_field(
            name="Last Success",
            value=ab(
                f"{lbl('Symbol')} {A(sym or 'N/A', 'magenta')}  |  "
                f"{lbl('Type')} {A(ct or 'N/A', 'cyan')}  |  "
                f"{lbl('Spr')} {A(spr_text, 'yellow', bold=True)}"
            ),
            inline=False
        )
    else:
        embed.add_field(name="Last Success", value=ab(A("None", "yellow")), inline=False)
    return embed
