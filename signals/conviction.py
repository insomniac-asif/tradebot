# signals/conviction.py
import pandas as pd
from core.data_service import get_market_dataframe


def calculate_conviction(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return 0, 0, 0, "neutral"

    try:
        recent = df.tail(30).copy()

        # Force numeric safety
        recent["high"] = pd.to_numeric(recent["high"], errors="coerce")
        recent["low"] = pd.to_numeric(recent["low"], errors="coerce")
        recent["close"] = pd.to_numeric(recent["close"], errors="coerce")
        recent["ema9"] = pd.to_numeric(recent["ema9"], errors="coerce")

        recent["range"] = recent["high"] - recent["low"]

        baseline = recent.head(25)
        avg_range = baseline["range"].mean()

        last5 = recent.tail(5)

        price_change = last5["close"].iloc[-1] - last5["close"].iloc[0]

        if price_change > 0:
            direction = "bullish"
        elif price_change < 0:
            direction = "bearish"
        else:
            direction = "neutral"

        impulse = last5["range"].mean() / avg_range if avg_range != 0 else 0

        closes = last5["close"].values
        direction_moves = sum(
            1 for i in range(1, len(closes))
            if (closes[i] - closes[i - 1]) * price_change > 0
        )

        follow_through = direction_moves / 4

        if "ema9" not in df.columns:
            return 0, 0, 0, "neutral"

        price = float(df["close"].iloc[-1])
        ema9 = float(df["ema9"].iloc[-1])

        pullback_depth = abs(price - ema9) / price if price != 0 else 0

        score = 0

        if impulse > 1.6:
            score += 2
        if follow_through > 0.6:
            score += 2
        if pullback_depth < 0.0025:
            score += 2

        return score, impulse, follow_through, direction

    except Exception:
        return 0, 0, 0, "neutral"


def momentum_is_decaying(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 10:
        return False

    recent = df.tail(10).copy()
    recent["range"] = recent["high"] - recent["low"]

    last5 = recent.tail(5)
    first5 = recent.head(5)

    last_impulse = last5["range"].mean()
    first_impulse = first5["range"].mean()

    if first_impulse == 0:
        return False

    decay_ratio = last_impulse / first_impulse

    return decay_ratio < 0.7


def scalp_context_valid(df, direction):

    if df is None:
        return False

    last = df.iloc[-1]
    price = last["close"]
    vwap = last["vwap"]

    if direction == "bullish" and price < vwap:
        return False
    if direction == "bearish" and price > vwap:
        return False

    return True