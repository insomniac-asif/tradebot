# analytics/risk_metrics.py

import json
import os
from core.paths import DATA_DIR

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")


def calculate_r_metrics():

    if not os.path.exists(ACCOUNT_FILE):
        return None

    with open(ACCOUNT_FILE, "r") as f:
        acc = json.load(f)

    trades = acc.get("trade_log", [])

    if not trades:
        return None

    r_values = []

    for t in trades:

        risk = t.get("risk", 0)

        if risk == 0:
            continue

        r = t["pnl"] / risk
        r_values.append(r)

    if not r_values:
        return None

    avg_r = sum(r_values) / len(r_values)
    win_r = [r for r in r_values if r > 0]
    loss_r = [r for r in r_values if r <= 0]

    return {
        "total_trades": len(r_values),
        "avg_R": round(avg_r, 2),
        "avg_win_R": round(sum(win_r)/len(win_r), 2) if win_r else 0,
        "avg_loss_R": round(sum(loss_r)/len(loss_r), 2) if loss_r else 0,
        "max_R": round(max(r_values), 2),
        "min_R": round(min(r_values), 2),
    }

def calculate_drawdown():

    if not os.path.exists(ACCOUNT_FILE):
        return None

    with open(ACCOUNT_FILE, "r") as f:
        acc = json.load(f)

    trades = acc.get("trade_log", [])

    if not trades:
        return None

    balances = [t["balance_after"] for t in trades]

    peak = balances[0]
    max_drawdown = 0

    for b in balances:
        if b > peak:
            peak = b

        drawdown = peak - b

        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "max_drawdown_dollars": round(max_drawdown, 2),
        "max_drawdown_percent": round(
            (max_drawdown / peak) * 100 if peak != 0 else 0,
            2
        )
    }
