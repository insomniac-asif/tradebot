# signals/volatility.py
from core.data_service import get_market_dataframe


def get_intraday_volatility(df=None):

    if df is None:
        df = get_market_dataframe()

    if df is None or len(df) < 30:
        return 0

    recent = df.tail(30)

    high = recent["high"].max()
    low = recent["low"].min()

    return round(high - low, 3)


def volatility_state(df=None):

    if df is None:
        df = get_market_dataframe()

    vol = get_intraday_volatility(df)

    # Use price-relative thresholds so classification works for any symbol
    try:
        mid_price = float(df.tail(30)["close"].mean()) if df is not None and len(df) >= 30 else 0
    except Exception:
        mid_price = 0

    if mid_price > 0:
        vol_pct = vol / mid_price
        if vol_pct < 0.00053:    # ~0.35/666 for SPY
            return "DEAD"
        if vol_pct < 0.00113:    # ~0.75/666 for SPY
            return "LOW"
        if vol_pct < 0.00225:    # ~1.5/666 for SPY
            return "NORMAL"
        return "HIGH"

    # Fallback to absolute thresholds if price unavailable
    if vol < 0.35:
        return "DEAD"

    if vol < 0.75:
        return "LOW"

    if vol < 1.5:
        return "NORMAL"

    return "HIGH"