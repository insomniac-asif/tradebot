import os
import joblib
import logging
import threading
from datetime import datetime

from core.paths import DATA_DIR
from analytics.feature_logger import log_trade_features, FEATURE_FILE
from research.train_ai import train_direction_model, train_edge_model
from signals.conviction import calculate_conviction
from signals.regime import get_regime
from signals.volatility import volatility_state
from signals.setup_classifier import classify_trade
from signals.session_classifier import classify_session


DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

_direction_model = None
_edge_model = None
_TRAINING = False
_LAST_TRAIN_COUNT = 0
_TRADE_COUNT = None


REGIME_MAP = {
    "TREND": 1,
    "RANGE": 2,
    "VOLATILE": 3,
    "COMPRESSION": 4,
    "NO_DATA": 0,
}

VOL_MAP = {
    "DEAD": 0,
    "LOW": 1,
    "NORMAL": 2,
    "HIGH": 3,
}

SETUP_MAP = {
    "BREAKOUT": 1,
    "PULLBACK": 2,
    "REVERSAL": 3,
    "UNKNOWN": 0,
}

STYLE_MAP = {
    "scalp": 1,
    "mini_swing": 2,
    "momentum": 3,
}


def _load_models():
    global _direction_model, _edge_model
    if _direction_model is None and os.path.exists(DIR_MODEL_FILE):
        try:
            _direction_model = joblib.load(DIR_MODEL_FILE)
        except Exception:
            _direction_model = None
    if _edge_model is None and os.path.exists(EDGE_MODEL_FILE):
        try:
            _edge_model = joblib.load(EDGE_MODEL_FILE)
        except Exception:
            _edge_model = None
    return _direction_model, _edge_model


def _style_from_horizon(horizon: str | None) -> str:
    if horizon == "scalp":
        return "scalp"
    if horizon == "swing":
        return "momentum"
    return "mini_swing"


def predict_sim_trade(df, context: dict) -> dict:
    """
    Generates ML predictions for sim trades without altering trade logic.
    Returns prediction fields + feature context for logging/grading.
    """
    direction_model, edge_model = _load_models()

    predicted_direction = None
    direction_prob = None
    prediction_confidence = None
    edge_prob = None
    direction_ready = False
    edge_ready = False

    conviction_score, impulse, follow_through, _ = calculate_conviction(df)
    regime = context.get("regime") or get_regime(df)
    volatility = volatility_state(df)
    horizon = context.get("horizon")
    style = _style_from_horizon(horizon)

    entry_price = context.get("price")
    trade_direction = context.get("direction")
    setup = "UNKNOWN"
    try:
        setup = classify_trade(entry_price, (trade_direction or "").lower())
    except Exception:
        setup = "UNKNOWN"

    timestamp = context.get("timestamp") or datetime.now().isoformat()
    session = classify_session(timestamp)

    # Direction model: uses market features (ema9, ema20, rsi, vwap, volume)
    if direction_model is not None and df is not None and not df.empty:
        try:
            last = df.iloc[-1]
            ema9 = float(last.get("ema9"))
            ema20 = float(last.get("ema20"))
            rsi = float(last.get("rsi"))
            vwap = float(last.get("vwap"))
            volume = float(last.get("volume"))
            features = [[ema9, ema20, rsi, vwap, volume]]
            direction_prob = float(direction_model.predict_proba(features)[0][1])
            predicted_direction = "BULLISH" if direction_prob >= 0.5 else "BEARISH"
            prediction_confidence = max(direction_prob, 1 - direction_prob)
            direction_ready = True
        except Exception:
            predicted_direction = None
            direction_prob = None
            prediction_confidence = None

    # Edge model: uses trade-quality features
    if edge_model is not None:
        try:
            regime_encoded = REGIME_MAP.get(regime, 0)
            vol_encoded = VOL_MAP.get(volatility, 0)
            setup_encoded = SETUP_MAP.get(setup, 0)
            style_encoded = STYLE_MAP.get(style, 0)
            session_encoded = {
                "OPEN": 1,
                "MIDDAY": 2,
                "AFTERNOON": 3,
                "POWER": 4,
                "UNKNOWN": 0,
            }.get(session, 0)
            confidence_val = prediction_confidence if prediction_confidence is not None else 0.5
            feature_vec = [[
                regime_encoded,
                vol_encoded,
                conviction_score,
                impulse,
                follow_through,
                setup_encoded,
                session_encoded,
                confidence_val,
                style_encoded,
                0,
                0,
            ]]
            edge_prob = float(edge_model.predict_proba(feature_vec)[0][1])
            edge_ready = True
        except Exception:
            edge_prob = None

    if predicted_direction is None:
        if trade_direction in {"BULLISH", "BEARISH"}:
            predicted_direction = trade_direction

    if predicted_direction is not None:
        if direction_prob is None:
            direction_prob = 0.5
        if prediction_confidence is None:
            prediction_confidence = 0.5
        if edge_prob is None:
            edge_prob = 0.5

    if prediction_confidence is None and direction_prob is not None:
        prediction_confidence = max(direction_prob, 1 - direction_prob)

    if prediction_confidence is not None and edge_prob is not None:
        prediction_confidence = (prediction_confidence * 0.6) + (edge_prob * 0.4)

    return {
        "predicted_direction": predicted_direction,
        "prediction_confidence": prediction_confidence,
        "direction_prob": direction_prob,
        "edge_prob": edge_prob,
        "ml_ready": direction_ready or edge_ready,
        "regime": regime,
        "volatility": volatility,
        "conviction_score": conviction_score,
        "impulse": impulse,
        "follow_through": follow_through,
        "setup": setup,
        "style": style,
        "confidence": prediction_confidence,
    }


def _init_trade_count() -> int:
    try:
        if os.path.exists(FEATURE_FILE):
            with open(FEATURE_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            count = max(0, len(lines) - 1)
            return count
    except Exception:
        return 0
    return 0


def maybe_retrain_models(trade_count: int, min_trades: int = 50) -> bool:
    global _TRAINING, _LAST_TRAIN_COUNT
    if trade_count is None or trade_count < min_trades:
        return False
    if _TRAINING:
        return False
    if trade_count - _LAST_TRAIN_COUNT < min_trades:
        return False

    _TRAINING = True

    def _run(count: int):
        global _TRAINING, _LAST_TRAIN_COUNT
        try:
            train_direction_model()
            train_edge_model()
            _LAST_TRAIN_COUNT = count
        except Exception:
            logging.exception("sim_ml_retrain_failed")
        finally:
            _TRAINING = False

    threading.Thread(target=_run, args=(trade_count,), daemon=True).start()
    return True


def record_sim_trade_close(trade: dict, pnl: float | None) -> None:
    global _TRADE_COUNT
    if not isinstance(trade, dict):
        return
    if _TRADE_COUNT is None:
        _TRADE_COUNT = _init_trade_count()
    result = None
    if pnl is not None:
        result = "win" if pnl > 0 else "loss"
    try:
        if result is not None:
            log_trade_features(trade, result, pnl)
    except Exception:
        logging.exception("sim_ml_log_trade_features_failed")
    _TRADE_COUNT += 1
    maybe_retrain_models(_TRADE_COUNT)
