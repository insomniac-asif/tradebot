# analytics/run_stats.py

from core.account_repository import load_account


def get_run_stats():

    acc = load_account()

    trades = acc.get("trade_log", [])

    total_trades = len(trades)
    wins = acc.get("wins", 0)
    losses = acc.get("losses", 0)

    starting = acc.get("starting_balance", 0)
    current = acc.get("balance", 0)

    pnl = current - starting

    # Equity curve balances
    balances = [starting]
    for t in trades:
        balances.append(t["balance_after"])

    peak = balances[0]
    max_peak = balances[0]
    max_drawdown = 0

    for b in balances:
        if b > max_peak:
            max_peak = b
        drawdown = max_peak - b
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "trades": total_trades,
        "wins": wins,
        "losses": losses,
        "pnl": pnl,
        "current": current,
        "start": starting,
        "peak": max_peak,
        "drawdown": max_drawdown
    }