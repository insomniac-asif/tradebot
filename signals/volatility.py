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

    vol = get_intraday_volatility(df)

    if vol < 0.35:
        return "DEAD"

    if vol < 0.75:
        return "LOW"

    if vol < 1.5:
        return "NORMAL"

    return "HIGH"