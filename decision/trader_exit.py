# decision/trader_exit.py
#
# Trade exit/management helpers extracted from decision/trader.py.
# Covers: check_expectancy_exit, check_partial_logic, check_exit_conditions,
#         calculate_pnl, finalize_trade.
#
# All function signatures are identical to their originals in trader.py.
# ─────────────────────────────────────────────────────────────────────────────

from analytics.career_updater import update_career_after_trade
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.feature_logger import log_trade_features
from analytics.review_engine import review_trade

from core.account_repository import save_account, load_account

from datetime import datetime
import pytz
import os

from decision.trader_utils import maybe_retrain_models


def check_expectancy_exit(acc, trade, price):
    """
    price must be the current option price (not stock price).
    Uses the same R-based P&L formula as calculate_pnl so that balance
    updates and R_multiple are consistent across all exit paths.
    """
    setup_stats = calculate_setup_expectancy()
    current_setup = trade.get("setup")

    if setup_stats and current_setup in setup_stats:

        avg_R = setup_stats[current_setup]["avg_R"]

        if avg_R < -0.25:
            entry = trade.get("entry_price")
            initial_stop = trade.get("initial_stop") or trade.get("stop")
            risk_amount = trade.get("risk", 0)

            if not entry or not initial_stop or entry == initial_stop or not risk_amount:
                return None

            risk_per = abs(entry - initial_stop)
            move = (price - entry) if trade["type"] == "bullish" else (entry - price)
            move_ratio = move / risk_per
            pnl = risk_amount * move_ratio

            return finalize_trade(acc, trade, "edge_exit", pnl)

    return None


def check_partial_logic(acc, trade, price):

    if trade["style"] != "momentum":
        return None

    if trade["partial_taken"]:
        return None

    hit_target = (
        trade["type"] == "bullish" and price >= trade["target"]
    ) or (
        trade["type"] == "bearish" and price <= trade["target"]
    )

    if not hit_target:
        return None

    move_ratio = abs(price - trade["entry_price"]) / abs(
        trade["entry_price"] - trade["initial_stop"]
    )

    partial_pnl = trade["risk"] * move_ratio * 0.5

    acc["balance"] += partial_pnl

    trade["partial_taken"] = True
    trade["runner_active"] = True
    trade["stop"] = trade["entry_price"]

    acc["open_trade"] = trade
    save_account(acc)

    return True


def check_exit_conditions(trade, price):

    if trade["type"] == "bullish":

        if price <= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price >= trade["target"]:
            return "win"

    if trade["type"] == "bearish":

        if price >= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price <= trade["target"]:
            return "win"

    return None


def calculate_pnl(trade, result, price):

    risk_amount = trade["risk"]

    if trade["style"] == "momentum" and trade["partial_taken"]:
        return 0

    if result == "win":

        move_ratio = abs(price - trade["entry_price"]) / abs(
            trade["entry_price"] - trade["initial_stop"]
        )

        return risk_amount * move_ratio

    return -risk_amount


def finalize_trade(acc, trade, result, pnl):

    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    if result == "win":
        acc["wins"] += 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    if trade["risk"] > 0:
        R_multiple = round(pnl / trade["risk"], 3)
    else:
        R_multiple = 0

    if not trade.get("option_symbol"):
        raise RuntimeError("Missing option metadata")
    if trade.get("quantity") is None or trade.get("quantity") <= 0:
        raise RuntimeError("Invalid quantity for option trade")

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "entry_time": trade["entry_time"],
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "type": trade["type"],
        "style": trade.get("style", "unknown"),
        "risk": trade["risk"],
        "R": R_multiple,
        "regime": trade.get("regime"),
        "setup": trade.get("setup", "UNKNOWN"),
        "underlying": trade.get("underlying"),
        "strike": trade.get("strike"),
        "expiry": trade.get("expiry"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "confidence": trade.get("confidence", 0),
        "result": result,
        "pnl": pnl,
        "balance_after": acc["balance"],
    }
    if trade_record.get("R") is not None:
        if trade_record["R"] > 0:
            trade_record["result"] = "win"
        elif trade_record["R"] < 0:
            trade_record["result"] = "loss"
        else:
            trade_record["result"] = "breakeven"
    review = review_trade(trade_record, result)
    print(review)
    acc["trade_log"].append(trade_record)
    acc["day_trades"].append(datetime.now(pytz.timezone("US/Eastern")).isoformat())
    acc["open_trade"] = None

    save_account(acc)
    # FIX: Trigger periodic ML retraining in background (non-blocking)
    maybe_retrain_models()

    return result, pnl, acc["balance"], trade


# =========================
# RECONSTRUCTED TRADE HELPERS (extracted from trader.py)
# =========================

def _finalize_reconstructed_trade(acc, trade, pnl, result_reason):
    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    result = "win" if pnl > 0 else "loss"
    if result == "win":
        acc["wins"] = acc.get("wins", 0) + 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "entry_time": trade.get("entry_time"),
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("emergency_exit_price"),
        "pnl": pnl,
        "result": result,
        "result_reason": result_reason,
        "reconstructed": True,
        "R": None,
        "risk_unknown": True,
        "balance_after": acc["balance"],
    }

    trade_log = acc.get("trade_log", [])
    if not isinstance(trade_log, list):
        trade_log = []
    trade_log.append(trade_record)
    acc["trade_log"] = trade_log

    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []
    acc["open_trades"] = [
        t for t in open_trades if not (isinstance(t, dict) and t.get("trade_id") == trade.get("trade_id"))
    ]

    save_account(acc)
    return result, pnl, acc["balance"], trade


def _manage_reconstructed_trades(acc):
    from execution.option_executor import get_option_price, close_option_position

    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list) or not open_trades:
        return None

    now = datetime.now(pytz.timezone("US/Eastern"))
    for trade in open_trades:
        if not isinstance(trade, dict) or not trade.get("reconstructed"):
            continue
        if trade.get("stop") is not None or trade.get("target") is not None:
            continue
        policy = trade.get("protection_policy", {})
        max_loss_pct = policy.get("max_loss_pct", 0.50)
        min_hold_seconds = policy.get("min_hold_seconds", 0)
        created_at = policy.get("created_at")
        if created_at:
            try:
                created_dt = datetime.fromisoformat(created_at)
                if created_dt.tzinfo is None:
                    created_dt = pytz.timezone("US/Eastern").localize(created_dt)
                if (now - created_dt).total_seconds() < float(min_hold_seconds):
                    continue
            except Exception:
                pass

        entry_price = trade.get("entry_price")
        option_symbol = trade.get("option_symbol")
        qty = trade.get("quantity")
        if entry_price is None or option_symbol is None or qty is None:
            continue
        try:
            entry_price = float(entry_price)
            qty = int(qty)
        except (TypeError, ValueError):
            continue
        if qty <= 0 or entry_price <= 0:
            continue

        current_price = get_option_price(option_symbol)
        if current_price is None:
            continue

        trade["last_manage_ts"] = now.isoformat()

        if current_price <= entry_price * (1 - float(max_loss_pct)):
            close_result = close_option_position(option_symbol, qty)
            filled_avg = close_result.get("filled_avg_price")
            if close_result.get("ok"):
                exit_price = None
                source = "estimated_mid"
                if filled_avg is not None:
                    exit_price = filled_avg
                    source = "broker_fill"
                else:
                    exit_price = current_price
                trade["emergency_exit_price"] = exit_price
                trade["emergency_exit_price_source"] = source
                pnl = (exit_price - entry_price) * qty * 100
                trade["result_reason"] = "reconstructed_emergency_stop"
                trade["recon_notice"] = {
                    "type": "emergency_stop_success",
                    "symbol": option_symbol,
                    "qty": qty,
                    "entry": entry_price,
                    "price": exit_price,
                    "ts": now.isoformat(),
                }
                return _finalize_reconstructed_trade(
                    acc, trade, pnl, "reconstructed_emergency_stop"
                )

            trade["emergency_stop_failed"] = True
            trade["recon_notice"] = {
                "type": "emergency_stop_failure",
                "symbol": option_symbol,
                "qty": qty,
                "entry": entry_price,
                "price": current_price,
                "ts": now.isoformat(),
            }
            save_account(acc)
            return None

    return None


RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED = False


def _manage_reconstructed_advanced(acc):
    if not RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED:
        return None
    try:
        from core.paths import DATA_DIR
        import json
        stats_path = os.path.join(DATA_DIR, "career_stats.json")
        with open(stats_path, "r", encoding="utf-8") as f:
            stats = json.load(f)
        closed_count = int(stats.get("total_trades", 0))
    except Exception:
        closed_count = 0
    if closed_count < 20:
        return None
    # TODO: trailing stop / take-profit for reconstructed trades.
    return None
    return result, pnl, acc["balance"], trade
