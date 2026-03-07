import json
import matplotlib.pyplot as plt
from core.account_repository import load_account
from core.paths import DATA_DIR

def generate_equity_curve():

    acc = load_account()

    trades = acc.get("trade_log", [])

    if len(trades) < 2:
        return None

    balances = [t["balance_after"] for t in trades]
    x = list(range(1, len(balances)+1))

    plt.figure(figsize=(8,4))
    plt.plot(x, balances)
    plt.xlabel("Trade Number")
    plt.ylabel("Account Balance ($)")
    plt.title("AI Trader Equity Curve")
    plt.tight_layout()
    plt.savefig("equity.png")
    plt.close()

    return "equity.png"
