# signals/predictor.py

from datetime import datetime
import pytz
import math
from core.data_service import get_market_dataframe


def make_prediction(minutes=60, df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return None

    recent = df.tail(30)

    last = recent.iloc[-1]

    # FIX: defensive numeric parsing to avoid crashing the forecast loop
    try:
        price = float(last["close"])
    except Exception:
        return None

    vwap = last.get("vwap", price)
    try:
        vwap = float(vwap)
    except Exception:
        vwap = price
    if not math.isfinite(vwap):
        vwap = price

    try:
        high30 = float(recent["high"].max())
        low30 = float(recent["low"].min())
    except Exception:
        return None

    try:
        trend = float(recent["close"].iloc[-1]) - float(recent["close"].iloc[0])
    except Exception:
        return None

    vol = (high30 - low30) / price if price != 0 else 0

    # Deterministic evidence model:
    # turn structural features into directional evidence scores,
    # then map scores to probabilities via temperature-scaled softmax.
    bullish_score = 0.15
    bearish_score = 0.15
    range_score = 0.15
    reasons = []

    vwap_dist = (price - vwap) / price if price else 0
    loc_strength = min(abs(vwap_dist) / 0.004, 1.0)
    trend_strength = min(abs(trend) / price / 0.004, 1.0) if price else 0.0

    # Location vs VWAP
    if price > vwap:
        bullish_score += 0.9 * loc_strength
        reasons.append("Price above VWAP")
    else:
        bearish_score += 0.9 * loc_strength
        reasons.append("Price below VWAP")

    # Momentum
    if trend > 0:
        bullish_score += 0.9 * trend_strength
        reasons.append("Upward momentum")
    elif trend < 0:
        bearish_score += 0.9 * trend_strength
        reasons.append("Downward momentum")
    else:
        range_score += 0.15
        reasons.append("Flat momentum")

    # Volatility
    if vol < 0.0018:
        range_score += 1.0
        bullish_score += 0.1
        bearish_score += 0.1
        reasons.append("Low volatility (range favored)")
    elif vol > 0.004:
        bullish_score += 0.3
        bearish_score += 0.3
        reasons.append("High volatility (directional expansion)")
    else:
        bullish_score += 0.15
        bearish_score += 0.15
        range_score += 0.05
        reasons.append("Normal volatility")

    # Temperature keeps probabilities realistic and avoids overconfidence.
    temperature = 1.35
    exp_bull = math.exp(bullish_score / temperature)
    exp_bear = math.exp(bearish_score / temperature)
    exp_range = math.exp(range_score / temperature)
    total = exp_bull + exp_bear + exp_range
    bullish = exp_bull / total
    bearish = exp_bear / total
    range_prob = exp_range / total

    expected_move = price * (vol * (minutes / 30))
    pred_high = price + expected_move / 2
    pred_low = price - expected_move / 2

    direction = max(
        [("bullish", bullish), ("bearish", bearish), ("range", range_prob)],
        key=lambda x: x[1]
    )[0]

    confidence = max(bullish, bearish, range_prob)

    return {
        "time": datetime.now(pytz.timezone("US/Eastern")),
        "timeframe": minutes,
        "direction": direction,
        "confidence": round(confidence, 3),
        "high": round(pred_high, 2),
        "low": round(pred_low, 2),
        "reasons": reasons
    }
