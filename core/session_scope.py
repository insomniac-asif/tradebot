import pandas as pd
from typing import cast
import pytz


EASTERN_TZ = pytz.timezone("US/Eastern")


def get_rth_session_view(df):
    """
    Return current-session Regular Trading Hours (09:30-16:00 ET) slice.
    Handles both tz-aware and naive indices.
    """
    if df is None or df.empty:
        return df

    if not isinstance(df.index, pd.DatetimeIndex):
        return df

    idx = df.index

    # Normalize index to US/Eastern.
    # If naive, this project's data convention is local ET timestamps.
    if idx.tz is None:
        idx_eastern = idx.tz_localize(EASTERN_TZ)
    else:
        idx_eastern = idx.tz_convert(EASTERN_TZ)

    df_eastern = df.copy()
    df_eastern.index = idx_eastern

    # Current session date from latest bar.
    last_ts = cast(pd.Timestamp, df_eastern.index[-1])
    session_date = last_ts.date()
    session_mask = df_eastern.index.to_series().dt.date == session_date
    session_df = df_eastern[session_mask]
    if session_df.empty:
        return df

    # Strict regular trading hours only.
    rth_df = session_df.between_time("09:30", "16:00")
    if rth_df.empty:
        rth_df = session_df

    return rth_df
