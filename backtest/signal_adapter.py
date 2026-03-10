"""
backtest/signal_adapter.py
Thin synchronous wrapper around simulation/sim_signals.py derive_sim_signal().

Accepts a rolling DataFrame of 1-minute bars (same format data_service returns:
DatetimeIndex (ET naive), columns: open, high, low, close, volume, ema9, ema20, rsi, atr, vwap)
Returns (direction, price, meta) or (None, None, meta).
"""
from __future__ import annotations
import logging
import pandas as pd
import pandas_ta as ta


def _prepare_df_with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the DataFrame has the indicators expected by signal functions.
    Mirrors what data_service._prepare_dataframe() does.
    """
    if df is None or df.empty:
        return df

    # Ensure DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        return df

    df = df.copy()

    # EMA
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()

    # RSI
    try:
        df["rsi"] = ta.rsi(df["close"], length=14)
    except Exception:
        df["rsi"] = None

    # ATR
    try:
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    except Exception:
        df["atr"] = None

    # VWAP
    try:
        if len(df) > 10:
            df["vwap"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        else:
            df["vwap"] = None
    except Exception:
        df["vwap"] = None

    return df


def get_signal(
    df: pd.DataFrame,
    signal_mode: str,
    profile: dict,
    feature_snapshot: dict | None = None,
) -> tuple:
    """
    Call derive_sim_signal with the correct arguments derived from reading sim_signals.py.

    Signature of derive_sim_signal:
        derive_sim_signal(df, signal_mode, context=None, feature_snapshot=None,
                          profile=None, signal_params=None)

    Returns (direction, price, meta).
    direction: "BULLISH", "BEARISH", or None.
    price: float or None.
    meta: dict or None.
    """
    from simulation.sim_signals import derive_sim_signal

    # Build context dict with profile-level tuning params (matching sim_entry_runner.py)
    context = {
        "trade_count": 0,
        "atr_expansion_min": profile.get("atr_expansion_min"),
        "vol_z_min": profile.get("vol_z_min"),
        "require_trend_bias": profile.get("require_trend_bias"),
        "iv_rank_max": profile.get("iv_rank_max"),
        "vwap_z_min": profile.get("vwap_z_min"),
        "close_z_min": profile.get("close_z_min"),
    }
    # Remove None values so signal functions fall back to their own defaults
    context = {k: v for k, v in context.items() if v is not None}

    signal_params = profile.get("signal_params") or {}

    try:
        result = derive_sim_signal(
            df,
            signal_mode,
            context=context,
            feature_snapshot=feature_snapshot,
            profile=profile,
            signal_params=signal_params,
        )
        if isinstance(result, tuple) and len(result) >= 2:
            direction = result[0]
            price = result[1]
            meta = result[2] if len(result) >= 3 else {}
            return direction, price, meta
        return None, None, {"reason": "unexpected_result_format"}
    except Exception as e:
        logging.debug("signal_adapter_error: mode=%s err=%s", signal_mode, e)
        return None, None, {"reason": f"signal_error:{e}"}


def compute_features_for_backtest(
    df: pd.DataFrame,
    profile: dict,
    signal_mode: str,
) -> dict | None:
    """
    Compute feature snapshot for signals that require features_enabled=True.
    Uses analytics.sim_features.compute_sim_features (same as live engine).
    Returns dict or None if features not available / not needed.
    """
    if not profile.get("features_enabled"):
        return None

    try:
        from analytics.sim_features import compute_sim_features
        from simulation.sim_contract import get_iv_series

        context = {
            "direction": None,
            "price": None,
            "regime": None,
            "signal_mode": signal_mode,
            "horizon": profile.get("horizon"),
            "dte_min": profile.get("dte_min"),
            "dte_max": profile.get("dte_max"),
            "orb_minutes": profile.get("orb_minutes", 15),
            "zscore_window": profile.get("zscore_window", 30),
            "iv_series": get_iv_series(profile.get("iv_series_window", 200)),
        }
        return compute_sim_features(df, context)
    except Exception as e:
        logging.debug("compute_features_for_backtest_error: %s", e)
        return None
