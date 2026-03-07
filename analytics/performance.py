# analytics/performance.py

from core.account_repository import load_account, load_career


def get_paper_stats():

    acc = load_account()

    trades = acc["wins"] + acc["losses"]

    if trades == 0:
        winrate = 0
    else:
        winrate = (acc["wins"] / trades) * 100

    pnl = acc["balance"] - acc["starting_balance"]

    return {
        "balance": acc["balance"],
        "pnl": pnl,
        "wins": acc["wins"],
        "losses": acc["losses"],
        "winrate": round(winrate, 2)
    }


def get_career_stats():

    c = load_career()

    total = c["total_trades_all_time"]

    if total == 0:
        winrate = 0
    else:
        winrate = (c["total_wins_all_time"] / total) * 100

    return {
        "total_trades": total,
        "wins": c["total_wins_all_time"],
        "losses": c["total_losses_all_time"],
        "winrate": round(winrate, 2),
        "best_balance": c["best_balance"]
    }