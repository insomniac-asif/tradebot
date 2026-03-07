# signals/regime.py
from core.data_service import get_market_dataframe

def get_regime(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 60:
        return "NO_DATA"

    recent = df.tail(60)

    price_change = recent["close"].iloc[-1] - recent["close"].iloc[0]
    high = recent["high"].max()
    low = recent["low"].min()

    total_range = high - low
    avg_candle = (recent["high"] - recent["low"]).mean()

    vwap = recent["vwap"].mean() if "vwap" in recent.columns else recent["close"].mean()

    above_vwap = (recent["close"] > vwap).sum()
    below_vwap = (recent["close"] < vwap).sum()

    directionality = abs(price_change) / total_range if total_range != 0 else 0

    if avg_candle < 0.08:
        return "COMPRESSION"

    if directionality > 0.6 and abs(above_vwap - below_vwap) > 30:
        return "TREND"

    if total_range > 1.2:
        return "VOLATILE"

    return "RANGE"
