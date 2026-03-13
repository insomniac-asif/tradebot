"""
Volatility Filter — "Is a big move coming?"

ML model predicts trending (|move| > 0.2% in 30 bars) vs range-bound.
72.8% CV accuracy, 67.1% holdout. Gates SIM00 and SIM09 only.

Usage:
    from signals.volatility_filter import predict_trending
    result = predict_trending(df)
    if result["trending"]:
        # big move likely — allow entry
"""
import json
import logging
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

MODEL_PATH = Path("data/volatility_filter_model.pkl")
CONFIG_PATH = Path("data/volatility_filter_config.json")

_model_cache = None
_model_mtime = 0.0

DEFAULT_CONFIG = {
    "enabled": True,
    "probability_threshold": 0.50,
    "gated_sims": ["SIM00", "SIM09"],
}


def _load_config():
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                stored = json.load(f)
            return {**DEFAULT_CONFIG, **stored}
        except (json.JSONDecodeError, OSError):
            pass
    return DEFAULT_CONFIG.copy()


def _load_model():
    global _model_cache, _model_mtime
    if not MODEL_PATH.exists():
        return None, None
    try:
        mtime = os.path.getmtime(MODEL_PATH)
        if mtime != _model_mtime or _model_cache is None:
            bundle = joblib.load(MODEL_PATH)
            _model_cache = bundle
            _model_mtime = mtime
        return _model_cache.get("model"), _model_cache.get("metadata")
    except Exception:
        logging.exception("volatility_filter_load_error")
        return None, None


def _compute_features(df):
    """Same 26 features as training script."""
    try:
        import pandas_ta as ta
    except ImportError:
        return None

    feat = pd.DataFrame(index=df.index)
    feat["ema9"] = ta.ema(df["close"], length=9)
    feat["ema20"] = ta.ema(df["close"], length=20)
    feat["rsi"] = ta.rsi(df["close"], length=14)
    feat["volume"] = df["volume"]

    if "vwap" in df.columns:
        feat["vwap"] = df["vwap"]
    else:
        tp = (df["high"] + df["low"] + df["close"]) / 3
        feat["vwap"] = (tp * df["volume"]).rolling(30).sum() / df["volume"].rolling(30).sum()

    feat["price_vs_vwap"] = (df["close"] - feat["vwap"]) / feat["vwap"].clip(lower=0.01)
    feat["price_vs_ema9"] = (df["close"] - feat["ema9"]) / feat["ema9"].clip(lower=0.01)
    feat["price_vs_ema20"] = (df["close"] - feat["ema20"]) / feat["ema20"].clip(lower=0.01)
    feat["ema_spread"] = (feat["ema9"] - feat["ema20"]) / feat["ema20"].clip(lower=0.01)

    feat["returns_5m"] = df["close"].pct_change(5)
    feat["returns_15m"] = df["close"].pct_change(15)
    feat["returns_30m"] = df["close"].pct_change(30)

    pct_chg = df["close"].pct_change()
    feat["volatility_15m"] = pct_chg.rolling(15).std()
    feat["volatility_60m"] = pct_chg.rolling(60).std()
    feat["vol_ratio"] = feat["volatility_15m"] / feat["volatility_60m"].clip(lower=1e-8)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    feat["atr"] = tr.rolling(14).mean()
    feat["atr_expansion"] = feat["atr"] / feat["atr"].rolling(30).mean().clip(lower=1e-8)

    vol_mean = df["volume"].rolling(60).mean()
    vol_std = df["volume"].rolling(60).std().clip(lower=1e-8)
    feat["volume_zscore"] = (df["volume"] - vol_mean) / vol_std

    feat["rsi_momentum"] = feat["rsi"].diff(5)
    feat["price_momentum"] = df["close"].pct_change(10)

    minutes_into = (df.index.hour - 9) * 60 + df.index.minute - 30
    minutes_into = np.clip(minutes_into, 0, 390)
    feat["time_sin"] = np.sin(2 * np.pi * minutes_into / 390)
    feat["time_cos"] = np.cos(2 * np.pi * minutes_into / 390)

    body = df["close"] - df["open"]
    total_range = (df["high"] - df["low"]).clip(lower=1e-8)
    feat["body_ratio"] = body / total_range
    feat["upper_wick_ratio"] = (df["high"] - df[["open", "close"]].max(axis=1)) / total_range
    feat["lower_wick_ratio"] = (df[["open", "close"]].min(axis=1) - df["low"]) / total_range

    rolling_high = df["high"].rolling(30).max()
    rolling_low = df["low"].rolling(30).min()
    hl_range = (rolling_high - rolling_low).clip(lower=1e-8)
    feat["price_position"] = (df["close"] - rolling_low) / hl_range

    return feat


def predict_trending(df):
    """
    Predict whether a big move is coming.

    Returns dict:
        trending: bool — True if model predicts trending (big move likely)
        probability: float — P(trending), 0-1
        pass_filter: bool — True if prediction passes threshold
        reason: str — human-readable explanation
    """
    config = _load_config()

    if not config.get("enabled", True):
        return {
            "trending": True,
            "probability": 0.5,
            "pass_filter": True,
            "reason": "volatility_filter_disabled",
        }

    model, metadata = _load_model()
    if model is None:
        return {
            "trending": True,
            "probability": 0.5,
            "pass_filter": True,
            "reason": "no_model_loaded",
        }

    features = _compute_features(df)
    if features is None or len(features) == 0:
        return {
            "trending": True,
            "probability": 0.5,
            "pass_filter": True,
            "reason": "feature_computation_failed",
        }

    # Use the last row (current bar)
    feature_cols = metadata.get("features", list(features.columns))
    row = features[feature_cols].iloc[[-1]].copy()

    if row.isna().any(axis=1).iloc[0]:
        return {
            "trending": True,
            "probability": 0.5,
            "pass_filter": True,
            "reason": "features_contain_nan",
        }

    try:
        proba = model.predict_proba(row)
        p_trending = float(proba[0, 1]) if proba.shape[1] > 1 else float(proba[0, 0])
    except Exception:
        logging.exception("volatility_filter_predict_error")
        return {
            "trending": True,
            "probability": 0.5,
            "pass_filter": True,
            "reason": "prediction_error",
        }

    threshold = config.get("probability_threshold", 0.50)
    is_trending = p_trending >= threshold

    return {
        "trending": is_trending,
        "probability": round(p_trending, 4),
        "pass_filter": is_trending,
        "reason": f"P(trending)={p_trending:.3f} {'≥' if is_trending else '<'} {threshold:.2f}",
    }


def should_gate_sim(sim_id):
    """Check if a sim should be gated by the volatility filter."""
    config = _load_config()
    gated = config.get("gated_sims", ["SIM00", "SIM09"])
    return sim_id in gated
