# decision/trader_utils.py
#
# Utility functions extracted from decision/trader.py.
# Covers: daily stats, gate recording, ML model management, feature building,
# day-trade limit, style selection, and the ML visibility snapshot.
#
# All function signatures are identical to their originals in trader.py.
# ─────────────────────────────────────────────────────────────────────────────

from analytics.feature_logger import FEATURE_FILE
from analytics.ml_loader import load_edge_model, build_feature_vector  # noqa: F401 (re-exported)
from analytics.progressive_influence import get_ml_weight
from signals.volatility import volatility_state
from research.train_ai import train_direction_model, train_edge_model
from core.debug import debug_log
from core.paths import DATA_DIR

from datetime import datetime, timedelta
import pytz
import os
import time
import threading
import logging

import joblib
from collections import deque


DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

MODEL_RETRAIN_INTERVAL_MINUTES = float(os.getenv("ML_RETRAIN_INTERVAL_MINUTES", "1440"))
MODEL_MAX_AGE_HOURS = float(
    os.getenv("ML_MODEL_MAX_AGE_HOURS", str(MODEL_RETRAIN_INTERVAL_MINUTES / 60.0))
)
MODEL_RETRAIN_MIN_TRADES = int(os.getenv("ML_RETRAIN_MIN_TRADES", "50"))

# Module-level state (mirrors trader.py globals)
direction_model = None
edge_model = None
_TRAINING = False
_LAST_RETRAIN_TS = 0.0

# ==============================
# CONFIDENCE DISTRIBUTION STATS
# ==============================
_conf_stats = {
    "samples": 0,
    "conf_15m_values": [],
    "conf_60m_values": [],
    "conf_15m_ge_055": 0,
    "conf_60m_ge_055": 0,
    "dual_alignment_ge_055": 0
}

_blend_history = deque(maxlen=20)
_threshold_history = deque(maxlen=20)

_gate_stats = {
    "attempts": 0,
    "blocked": {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    }
}

_daily_stats = {
    "date": None,
    "summary_logged": False,
    "attempts": 0,
    "trades_opened": 0,
    "blocks": {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    },
    "conf_15m_values": [],
    "conf_60m_values": [],
    "dual_alignment_ge_055": 0,
    "blended_scores": [],
    "thresholds": [],
    "misaligned_warning_logged": False
}


def _reset_daily_stats(for_date):
    _daily_stats["date"] = for_date
    _daily_stats["summary_logged"] = False
    _daily_stats["attempts"] = 0
    _daily_stats["trades_opened"] = 0
    _daily_stats["blocks"] = {
        "regime": 0,
        "volatility": 0,
        "confidence": 0,
        "ml_threshold": 0,
        "expectancy": 0,
        "protection_layer": 0
    }
    _daily_stats["conf_15m_values"] = []
    _daily_stats["conf_60m_values"] = []
    _daily_stats["dual_alignment_ge_055"] = 0
    _daily_stats["blended_scores"] = []
    _daily_stats["thresholds"] = []
    _daily_stats["misaligned_warning_logged"] = False


def _get_daily_trade_stats(acc, day):
    trades = acc.get("trade_log", [])
    day_trades = []
    for trade in trades:
        exit_time = trade.get("exit_time")
        if not exit_time:
            continue
        try:
            exit_dt = datetime.fromisoformat(exit_time)
        except Exception:
            continue
        if exit_dt.date() == day:
            day_trades.append(trade)

    if not day_trades:
        return None, None

    wins = sum(1 for t in day_trades if t.get("result") == "win")
    winrate = (wins / len(day_trades)) * 100 if day_trades else None

    r_values = [
        float(t.get("R"))
        for t in day_trades
        if t.get("R") is not None
    ]
    avg_r = (sum(r_values) / len(r_values)) if r_values else None
    return winrate, avg_r


def _emit_daily_summary(acc, day):
    if _daily_stats["summary_logged"]:
        return

    attempts = _daily_stats["attempts"]
    blocks = _daily_stats["blocks"]

    def pct(count):
        return round((count / attempts) * 100, 1) if attempts > 0 else 0.0

    avg_15 = (
        sum(_daily_stats["conf_15m_values"]) / len(_daily_stats["conf_15m_values"])
        if _daily_stats["conf_15m_values"]
        else None
    )
    avg_60 = (
        sum(_daily_stats["conf_60m_values"]) / len(_daily_stats["conf_60m_values"])
        if _daily_stats["conf_60m_values"]
        else None
    )
    avg_blended = (
        sum(_daily_stats["blended_scores"]) / len(_daily_stats["blended_scores"])
        if _daily_stats["blended_scores"]
        else None
    )
    avg_threshold = (
        sum(_daily_stats["thresholds"]) / len(_daily_stats["thresholds"])
        if _daily_stats["thresholds"]
        else None
    )
    winrate, avg_r = _get_daily_trade_stats(acc, day)

    debug_log(
        "daily_calibration_summary",
        day=str(day),
        total_signal_attempts=attempts,
        total_trades_opened=_daily_stats["trades_opened"],
        block_pct_regime=pct(blocks["regime"]),
        block_pct_volatility=pct(blocks["volatility"]),
        block_pct_confidence=pct(blocks["confidence"]),
        block_pct_ml_threshold=pct(blocks["ml_threshold"]),
        block_pct_expectancy=pct(blocks["expectancy"]),
        block_pct_protection_layer=pct(blocks["protection_layer"]),
        avg_15m_confidence=round(avg_15, 3) if avg_15 is not None else "N/A",
        avg_60m_confidence=round(avg_60, 3) if avg_60 is not None else "N/A",
        avg_blended_score=round(avg_blended, 3) if avg_blended is not None else "N/A",
        avg_threshold=round(avg_threshold, 3) if avg_threshold is not None else "N/A",
        winrate=round(winrate, 1) if winrate is not None else "N/A",
        avg_R_multiple=round(avg_r, 3) if avg_r is not None else "N/A"
    )
    _daily_stats["summary_logged"] = True


def _roll_daily_summary_if_needed(acc, now_eastern):
    today = now_eastern.date()

    if _daily_stats["date"] is None:
        _reset_daily_stats(today)
        return

    if today != _daily_stats["date"]:
        _emit_daily_summary(acc, _daily_stats["date"])
        _reset_daily_stats(today)


def _record_signal_attempt():
    _gate_stats["attempts"] += 1
    _daily_stats["attempts"] += 1
    attempts = _gate_stats["attempts"]
    if attempts % 25 == 0:
        blocked = _gate_stats["blocked"]
        debug_log(
            "gate_breakdown_summary",
            attempts=attempts,
            blocked_regime=blocked["regime"],
            blocked_volatility=blocked["volatility"],
            blocked_confidence=blocked["confidence"],
            blocked_ml_threshold=blocked["ml_threshold"],
            blocked_expectancy=blocked["expectancy"],
            blocked_protection_layer=blocked["protection_layer"]
        )


def _record_gate_block(category):
    if category in _gate_stats["blocked"]:
        _gate_stats["blocked"][category] += 1
    if category in _daily_stats["blocks"]:
        _daily_stats["blocks"][category] += 1


def _category_for_block_reason(reason):
    if not reason:
        return None
    if reason.startswith("protection_"):
        return "protection_layer"
    if reason.startswith("regime_"):
        return "regime"
    if reason.startswith("volatility_"):
        return "volatility"
    if reason in {"confidence", "direction_mismatch"}:
        return "confidence"
    if reason in {"ml_threshold"}:
        return "ml_threshold"
    if reason in {"expectancy_negative_regime"}:
        return "expectancy"
    return None


def get_ml_visibility_snapshot():
    avg_blended = None
    avg_threshold = None
    if _blend_history:
        avg_blended = sum(_blend_history) / len(_blend_history)
    if _threshold_history:
        avg_threshold = sum(_threshold_history) / len(_threshold_history)

    delta = None
    if avg_blended is not None and avg_threshold is not None:
        delta = avg_blended - avg_threshold

    return {
        "ml_weight": get_ml_weight(),
        "avg_blended": avg_blended,
        "avg_threshold": avg_threshold,
        "avg_delta": delta,
    }


def _track_confidence_distribution(bias, trigger):
    bias_conf = float(bias.get("confidence", 0))
    trigger_conf = float(trigger.get("confidence", 0))
    aligned_direction = bias.get("direction") == trigger.get("direction")
    dual_conf_aligned = (
        bias_conf >= 0.55 and trigger_conf >= 0.55 and aligned_direction
    )

    _conf_stats["samples"] += 1
    _conf_stats["conf_60m_values"].append(bias_conf)
    _conf_stats["conf_15m_values"].append(trigger_conf)
    _daily_stats["conf_60m_values"].append(bias_conf)
    _daily_stats["conf_15m_values"].append(trigger_conf)

    if bias_conf >= 0.55:
        _conf_stats["conf_60m_ge_055"] += 1
    if trigger_conf >= 0.55:
        _conf_stats["conf_15m_ge_055"] += 1
    if dual_conf_aligned:
        _conf_stats["dual_alignment_ge_055"] += 1
        _daily_stats["dual_alignment_ge_055"] += 1

    samples = _conf_stats["samples"]
    if samples % 30 == 0:
        pct_15 = (_conf_stats["conf_15m_ge_055"] / samples) * 100
        pct_60 = (_conf_stats["conf_60m_ge_055"] / samples) * 100
        pct_dual = (_conf_stats["dual_alignment_ge_055"] / samples) * 100

        debug_log(
            "confidence_distribution_summary",
            samples=samples,
            pct_15m_ge_055=round(pct_15, 1),
            pct_60m_ge_055=round(pct_60, 1),
            pct_dual_alignment_ge_055=round(pct_dual, 1)
        )

    daily_samples = len(_daily_stats["conf_15m_values"])
    if (
        daily_samples >= 100
        and not _daily_stats["misaligned_warning_logged"]
    ):
        dual_pct = (_daily_stats["dual_alignment_ge_055"] / daily_samples) * 100
        if dual_pct < 2:
            debug_log(
                "confidence_threshold_misaligned",
                samples=daily_samples,
                dual_alignment_ge_055_pct=round(dual_pct, 2)
            )
            _daily_stats["misaligned_warning_logged"] = True


def _model_is_fresh(path: str) -> bool:
    try:
        if not os.path.exists(path):
            return False
        if MODEL_MAX_AGE_HOURS <= 0:
            return True
        age_sec = time.time() - os.path.getmtime(path)
        return age_sec <= (MODEL_MAX_AGE_HOURS * 3600)
    except Exception:
        return False


def load_models():
    global direction_model, edge_model
    if direction_model is None and os.path.exists(DIR_MODEL_FILE):
        if _model_is_fresh(DIR_MODEL_FILE):
            try:
                direction_model = joblib.load(DIR_MODEL_FILE)
            except Exception:
                direction_model = None
        else:
            debug_log("ml_model_stale", model="direction_model")

    if edge_model is None and os.path.exists(EDGE_MODEL_FILE):
        if _model_is_fresh(EDGE_MODEL_FILE):
            try:
                edge_model = joblib.load(EDGE_MODEL_FILE)
            except Exception:
                edge_model = None
        else:
            debug_log("ml_model_stale", model="edge_model")


def _feature_trade_count() -> int:
    try:
        if not os.path.exists(FEATURE_FILE):
            return 0
        with open(FEATURE_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return max(0, len(lines) - 1)
    except Exception:
        return 0


def maybe_retrain_models() -> None:
    global _TRAINING, _LAST_RETRAIN_TS, direction_model, edge_model
    if _TRAINING:
        return
    # Only retrain if we have enough data
    trade_count = _feature_trade_count()
    if trade_count < MODEL_RETRAIN_MIN_TRADES:
        return
    # Enforce time-based retrain interval
    now = time.time()
    if _LAST_RETRAIN_TS and (now - _LAST_RETRAIN_TS) < (MODEL_RETRAIN_INTERVAL_MINUTES * 60):
        return

    _TRAINING = True

    def _run():
        global _TRAINING, _LAST_RETRAIN_TS, direction_model, edge_model
        try:
            train_direction_model()
            train_edge_model()
            # Reset loaded models so next call reloads fresh models
            direction_model = None
            edge_model = None
            _LAST_RETRAIN_TS = time.time()
            debug_log("ml_retrain_completed", trade_count=trade_count)
        except Exception:
            logging.exception("ml_retrain_failed")
        finally:
            _TRAINING = False

    threading.Thread(target=_run, daemon=True).start()


def build_ml_features(df, trade, conviction_score, impulse, follow):
    """
    Build ML feature vector from current state.
    Must match training feature order exactly.
    """

    last = df.iloc[-1]

    regime_map = {"TREND": 1, "RANGE": 2, "VOLATILE": 3, "COMPRESSION": 4}
    vol_map = {"DEAD": 1, "LOW": 2, "NORMAL": 3, "HIGH": 4}

    regime_encoded = regime_map.get(trade["regime"], 0)
    volatility_encoded = vol_map.get(volatility_state(df), 0)

    features = [[
        regime_encoded,
        volatility_encoded,
        conviction_score,
        impulse,
        follow,
        trade["confidence"]
    ]]

    return features


# =========================
# DAY TRADE LIMIT CHECK
# =========================

def can_day_trade(acc):
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    acc["day_trades"] = [
        t for t in acc["day_trades"]
        if datetime.fromisoformat(t) > now - timedelta(days=5)
    ]

    return len(acc["day_trades"]) < 3


# =========================
# STYLE SELECTION
# =========================

def select_style(regime, volatility, conviction_score):

    if regime == "TREND" and conviction_score >= 5:
        return "momentum"

    if regime == "TREND" and conviction_score >= 3:
        return "mini_swing"

    if regime == "RANGE":
        return "scalp"

    if volatility == "HIGH" and conviction_score >= 4:
        return "momentum"

    return "scalp"
