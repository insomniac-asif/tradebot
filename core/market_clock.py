# core/market_clock.py

from datetime import datetime
import pytz


def market_is_open():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    if now.weekday() >= 5:
        return False

    minutes = now.hour * 60 + now.minute
    return 570 <= minutes < 960  # 9:30–4:00