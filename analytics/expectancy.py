from core.account_repository import load_account

def calculate_expectancy():

    acc = load_account()
    trades = acc.get("trade_log", [])

    if len(trades) < 10:
        return None

    Rs = [t["R"] for t in trades if "R" in t]

    avg_R = sum(Rs) / len(Rs)

    win_Rs = [r for r in Rs if r > 0]
    loss_Rs = [r for r in Rs if r < 0]

    winrate = len(win_Rs) / len(Rs)

    avg_win = sum(win_Rs)/len(win_Rs) if win_Rs else 0
    avg_loss = sum(loss_Rs)/len(loss_Rs) if loss_Rs else 0

    expectancy = (winrate * avg_win) + ((1-winrate) * avg_loss)

    return {
        "avg_R": round(avg_R, 3),
        "winrate": round(winrate*100, 1),
        "expectancy": round(expectancy, 3),
        "samples": len(Rs)
    }
