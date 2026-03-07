# analytics/career_updater.py

from core.account_repository import load_career, save_career
from signals.session_classifier import classify_session


def update_career_after_trade(trade, result, pnl, new_balance):

    career = load_career()

    career["total_trades_all_time"] += 1

    if result == "win":
        career["total_wins_all_time"] += 1
    else:
        career["total_losses_all_time"] += 1

    # ----- Best Balance
    if new_balance > career["best_balance"]:
        career["best_balance"] = new_balance

    # ----- Time of Day
    session = classify_session(trade["entry_time"])
    if session in career["time_of_day"]:
        if result == "win":
            career["time_of_day"][session]["wins"] += 1
        else:
            career["time_of_day"][session]["losses"] += 1
        career["time_of_day"][session]["pnl"] += pnl

    # ----- Setup Tracking
    setup = trade.get("setup", "UNKNOWN")
    if setup in career["setups"]:
        if result == "win":
            career["setups"][setup]["wins"] += 1
        else:
            career["setups"][setup]["losses"] += 1

    # ----- Confidence Calibration
    conf = trade.get("confidence", 0) * 100

    bucket = None
    if 50 <= conf < 60:
        bucket = "50-60"
    elif 60 <= conf < 70:
        bucket = "60-70"
    elif 70 <= conf < 80:
        bucket = "70-80"
    elif conf >= 80:
        bucket = "80-100"

    if bucket:
        career["confidence"][bucket]["total"] += 1
        if result == "win":
            career["confidence"][bucket]["correct"] += 1

    save_career(career)
