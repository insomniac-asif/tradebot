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


def get_time_bucket() -> str:
    """Classify current ET time into a trading session bucket."""
    et = datetime.now(pytz.timezone("US/Eastern"))
    t = et.hour * 60 + et.minute
    if t < 570:   return "PREMARKET"      # before 9:30
    if t < 630:   return "OPENING_HOUR"   # 9:30–10:30
    if t < 780:   return "MIDDAY"         # 10:30–13:00
    if t < 900:   return "POWER_HOUR"     # 13:00–15:00
    return "CLOSING"                       # 15:00–16:00