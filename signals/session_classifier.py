# signals/session_classifier.py

from datetime import datetime
import pytz

def classify_session(timestamp_iso):

    if not timestamp_iso:
        return "UNKNOWN"

    eastern = pytz.timezone("US/Eastern")

    try:
        t = datetime.fromisoformat(timestamp_iso)

        # If naive datetime → assume Eastern
        if t.tzinfo is None:
            t = eastern.localize(t)
        else:
            t = t.astimezone(eastern)

        minutes = t.hour * 60 + t.minute

        # 9:30 – 10:30
        if 570 <= minutes < 630:
            return "OPEN"

        # 10:30 – 1:30
        elif 630 <= minutes < 810:
            return "MIDDAY"

        # 1:30 – 3:00
        elif 810 <= minutes < 900:
            return "AFTERNOON"

        # 3:00 – 4:00
        elif 900 <= minutes < 960:
            return "POWER"

        else:
            return "UNKNOWN"

    except Exception:
        return "UNKNOWN"
