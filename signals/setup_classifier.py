# signals/setup_classifier.py

from core.data_service import get_market_dataframe


def classify_trade(entry_price, direction):

    df = get_market_dataframe()

    if df is None or len(df) < 25:
        return "UNKNOWN"

    recent = df.tail(20)

    recent_high = recent["high"].max()
    recent_low = recent["low"].min()

    last_close = recent["close"].iloc[-1]
    first_close = recent["close"].iloc[0]

    trend_up = last_close > first_close

    # Breakout detection
    if direction == "bullish" and entry_price >= recent_high * 0.999:
        return "BREAKOUT"

    if direction == "bearish" and entry_price <= recent_low * 1.001:
        return "BREAKOUT"

    # Pullback detection
    if direction == "bullish" and trend_up:
        return "PULLBACK"

    if direction == "bearish" and not trend_up:
        return "PULLBACK"

    return "REVERSAL"
