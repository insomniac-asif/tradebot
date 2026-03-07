# signal/opportunity.py

from core.data_service import get_market_dataframe
from signals.conviction import calculate_conviction, momentum_is_decaying
from signals.volatility import volatility_state


def evaluate_opportunity(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return None

    last = df.iloc[-1]

    try:
        price = float(last["close"])
        vwap = float(last["vwap"])
        ema9 = float(last["ema9"])
        ema20 = float(last["ema20"])
        rsi = float(last["rsi"])
    except Exception:
        return None

    # -----------------------------------
    # CONVICTION FIRST
    # -----------------------------------

    conviction_score, impulse, follow, direction = calculate_conviction(df)

    if conviction_score < 3:
        return None

    # -----------------------------------
    # VOLATILITY FILTER
    # -----------------------------------

    vol_state = volatility_state(df)

    if vol_state == "DEAD":
        return None

    if conviction_score >= 4 and vol_state == "LOW":
        return None

    if vol_state == "LOW":
        conviction_score -= 1

    if vol_state == "HIGH":
        conviction_score += 1

    # -----------------------------------
    # MOMENTUM HEALTH
    # -----------------------------------

    if momentum_is_decaying(df):
        return None

    # -----------------------------------
    # STRUCTURE SCORE
    # -----------------------------------

    structure_score = 0

    if price > vwap:
        structure_score += 1

    if ema9 > ema20:
        structure_score += 1

    if direction == "bullish" and 45 < rsi < 70:
        structure_score += 1

    if direction == "bearish" and 30 < rsi < 55:
        structure_score += 1

    try:
        atr = float(last["atr"])
    except Exception:
        return None
    if atr <= 0:
        return None

    distance_from_ema = abs(price - ema9)

    extended = distance_from_ema > (atr * 0.5)

    # -----------------------------------
    # FINAL ALIGNMENT
    # -----------------------------------

    if direction == "bullish" and structure_score >= 2 and not extended:
        entry_low = ema9 - (atr * 0.2)
        entry_high = ema9 + (atr * 0.2)
        entry_mid = (entry_low + entry_high) / 2
        tp_mult = 1.5
        sl_mult = 0.5
        if vol_state == "HIGH":
            tp_mult += 0.5
            sl_mult += 0.2
        elif vol_state == "LOW":
            tp_mult -= 0.3
            sl_mult -= 0.1
        if conviction_score >= 5:
            tp_mult += 0.3
            sl_mult += 0.1
        elif conviction_score <= 3:
            tp_mult -= 0.2
            sl_mult -= 0.1
        tp_mult = max(0.8, tp_mult)
        sl_mult = max(0.2, sl_mult)
        take_profit_low = entry_mid + (atr * tp_mult * 0.9)
        take_profit_high = entry_mid + (atr * tp_mult * 1.1)
        stop_loss = entry_mid - (atr * sl_mult)
        return (
            "CALLS",
            entry_low,
            entry_high,
            price,
            conviction_score,
            take_profit_low,
            take_profit_high,
            stop_loss,
        )

    if direction == "bearish" and structure_score >= 2 and not extended:
        entry_low = ema9 - (atr * 0.2)
        entry_high = ema9 + (atr * 0.2)
        entry_mid = (entry_low + entry_high) / 2
        tp_mult = 1.5
        sl_mult = 0.5
        if vol_state == "HIGH":
            tp_mult += 0.5
            sl_mult += 0.2
        elif vol_state == "LOW":
            tp_mult -= 0.3
            sl_mult -= 0.1
        if conviction_score >= 5:
            tp_mult += 0.3
            sl_mult += 0.1
        elif conviction_score <= 3:
            tp_mult -= 0.2
            sl_mult -= 0.1
        tp_mult = max(0.8, tp_mult)
        sl_mult = max(0.2, sl_mult)
        take_profit_high = entry_mid - (atr * tp_mult * 0.9)
        take_profit_low = entry_mid - (atr * tp_mult * 1.1)
        stop_loss = entry_mid + (atr * sl_mult)
        return (
            "PUTS",
            entry_low,
            entry_high,
            price,
            conviction_score,
            take_profit_low,
            take_profit_high,
            stop_loss,
        )

    return None
