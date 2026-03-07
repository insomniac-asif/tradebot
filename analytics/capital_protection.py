# analytics/capital_protection.py

from core.account_repository import load_account
from analytics.edge_stability import calculate_edge_stability


def get_capital_mode():

    acc = load_account()

    balance = acc.get("balance", 0)
    starting = acc.get("starting_balance", balance)
    daily_loss = acc.get("daily_loss", 0)
    max_daily = acc.get("max_daily_loss", 1)

    drawdown = 0
    if starting > 0:
        drawdown = (starting - balance) / starting

    stability_data = calculate_edge_stability()
    stability = stability_data["stability"] if stability_data else 0.5

    # ---------------------------------
    # MODE LOGIC
    # ---------------------------------

    # 🚨 LOCKDOWN
    if drawdown >= 0.20:
        return {
            "mode": "LOCKDOWN",
            "risk_multiplier": 0.0,
            "threshold_buffer": 0.15
        }

    # 🔴 CRITICAL
    if drawdown >= 0.12 or daily_loss >= max_daily:
        return {
            "mode": "CRITICAL",
            "risk_multiplier": 0.4,
            "threshold_buffer": 0.08
        }

    # 🟡 DEFENSIVE
    if drawdown >= 0.07 or stability < 0.45:
        return {
            "mode": "DEFENSIVE",
            "risk_multiplier": 0.7,
            "threshold_buffer": 0.04
        }

    # 🟢 NORMAL
    return {
        "mode": "NORMAL",
        "risk_multiplier": 1.0,
        "threshold_buffer": 0.0
    }
