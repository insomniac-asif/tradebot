import math
from datetime import datetime, timedelta, time as dt_time
import pandas as pd
import pytz


def _safe_float(val):
    try:
        if val is None:
            return None
        out = float(val)
        if math.isfinite(out):
            return out
    except (TypeError, ValueError):
        return None
    return None


def _index_to_et_naive(idx):
    if not isinstance(idx, pd.DatetimeIndex) or len(idx) == 0:
        return None
    eastern = pytz.timezone("US/Eastern")
    try:
        if idx.tz is not None:
            return idx.tz_convert(eastern).tz_localize(None)
    except Exception:
        pass
    return idx


def _session_open_ts(idx_local: pd.DatetimeIndex):
    try:
        last_ts = idx_local[-1]
        session_date = last_ts.date()
        return datetime.combine(session_date, dt_time(9, 30))
    except Exception:
        return None


def opening_range(df, minutes: int = 15):
    """
    Compute opening range high/low for the current ET session.
    Returns (high, low) or (None, None).
    """
    if df is None or df.empty or "high" not in df.columns or "low" not in df.columns:
        return None, None
    idx_local = _index_to_et_naive(df.index)
    if idx_local is None:
        return None, None
    session_open = _session_open_ts(idx_local)
    if session_open is None:
        return None, None
    window_end = session_open + timedelta(minutes=minutes)
    try:
        # Filter to current session date before applying ORB window
        session_mask = idx_local.date == session_open.date()
        if not session_mask.any():
            return None, None
        session_df = df.loc[session_mask]
        session_idx = idx_local[session_mask]
        window = session_df[(session_idx >= session_open) & (session_idx < window_end)]
        if window.empty:
            return None, None
        return _safe_float(window["high"].max()), _safe_float(window["low"].min())
    except Exception:
        return None, None


def compute_indicators(df, orb_minutes: int = 15) -> dict:
    """
    Pull most recent indicator values from the dataframe.
    Uses existing columns created by core.data_service._prepare_dataframe().
    """
    if df is None or df.empty:
        return {}
    last = df.iloc[-1]
    close = _safe_float(last.get("close"))
    ema9 = _safe_float(last.get("ema9"))
    ema20 = _safe_float(last.get("ema20"))
    rsi = _safe_float(last.get("rsi"))
    atr = _safe_float(last.get("atr"))
    vwap = _safe_float(last.get("vwap"))
    orb_high, orb_low = opening_range(df, minutes=orb_minutes)

    vwap_dist = None
    ema_spread = None
    if close and vwap and close > 0:
        vwap_dist = (close - vwap) / close
    if ema9 and ema20:
        ema_spread = (ema9 - ema20) / ema20

    return {
        "close": close,
        "ema9": ema9,
        "ema20": ema20,
        "rsi": rsi,
        "atr": atr,
        "vwap": vwap,
        "vwap_dist": vwap_dist,
        "ema_spread": ema_spread,
        "orb_high": orb_high,
        "orb_low": orb_low,
    }


def compute_zscores(df, window: int = 30) -> dict:
    """
    Additive z-scores for SIM analytics (no decision impact).
    """
    if df is None or df.empty:
        return {}
    if len(df) < (window + 2):
        return {}
    out = {}
    try:
        close = pd.to_numeric(df["close"], errors="coerce")
        vwap = pd.to_numeric(df["vwap"], errors="coerce") if "vwap" in df.columns else None
        volume = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else None
        atr = pd.to_numeric(df["atr"], errors="coerce") if "atr" in df.columns else None

        tail = close.tail(window)
        if tail.notna().sum() >= 5:
            sma = tail.mean()
            std = tail.std()
            if std and std > 0:
                out["close_z"] = _safe_float((close.iloc[-1] - sma) / std)

        if vwap is not None:
            dev = (close - vwap).tail(window)
            if dev.notna().sum() >= 5:
                std = dev.std()
                if std and std > 0:
                    out["vwap_z"] = _safe_float(dev.iloc[-1] / std)

        if volume is not None:
            vol_tail = volume.tail(window)
            if vol_tail.notna().sum() >= 5:
                sma = vol_tail.mean()
                std = vol_tail.std()
                if std and std > 0:
                    out["vol_z"] = _safe_float((volume.iloc[-1] - sma) / std)

        if atr is not None:
            atr_tail = atr.tail(window)
            if atr_tail.notna().sum() >= 5:
                sma = atr_tail.mean()
                if sma and sma > 0:
                    out["atr_expansion"] = _safe_float(atr.iloc[-1] / sma)
    except Exception:
        return out
    return out
