# decision/trader.py
from analytics.career_updater import update_career_after_trade
from analytics.risk_control import get_dynamic_risk_percent
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.edge_decay import edge_decay_status
from core.md_state import is_md_enabled
from analytics.feature_logger import log_trade_features, FEATURE_FILE
from analytics.ml_loader import load_edge_model, build_feature_vector
from analytics.adaptive_threshold import adaptive_ml_threshold
from analytics.progressive_influence import get_ml_weight
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.review_engine import review_trade
from analytics.stability_mode import get_stability_mode
from analytics.setup_intelligence import get_setup_intelligence
from analytics.edge_compression import get_edge_compression
from analytics.regime_transition import detect_regime_transition
from analytics.regime_persistence import calculate_regime_persistence
from analytics.regime_memory import get_regime_memory

from datetime import datetime, timedelta, date
import pytz
import os
import uuid
import time
import threading
import logging

from core.account_repository import (
    load_account,
    save_account,
)

from core.data_service import (
    get_latest_price,
    get_market_dataframe
)
from core.market_clock import market_is_open
from core.debug import debug_log
from core.decision_context import DecisionContext
from research.train_ai import train_direction_model, train_edge_model

from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state
from signals.setup_classifier import classify_trade
from signals.conviction import calculate_conviction
from signals.signal_evaluator import grade_trade
from signals.session_classifier import classify_session

import joblib
from collections import deque
from core.paths import DATA_DIR

from execution.ml_gate import ml_probability_gate
from execution.option_executor import execute_option_entry, close_option_position, get_option_price
from core.rate_limiter import rate_limit_sleep
from alpaca.data.historical import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest
from alpaca.trading.enums import ContractType

from analytics.contract_logger import log_contract_attempt


DIR_MODEL_FILE = os.path.join(DATA_DIR, "direction_model.pkl")
EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")
ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))
MODEL_RETRAIN_INTERVAL_MINUTES = float(os.getenv("ML_RETRAIN_INTERVAL_MINUTES", "1440"))
# If ML_MODEL_MAX_AGE_HOURS is not set, align model staleness with retrain interval
MODEL_MAX_AGE_HOURS = float(
    os.getenv("ML_MODEL_MAX_AGE_HOURS", str(MODEL_RETRAIN_INTERVAL_MINUTES / 60.0))
)
MODEL_RETRAIN_MIN_TRADES = int(os.getenv("ML_RETRAIN_MIN_TRADES", "50"))

direction_model = None
edge_model = None
_TRAINING = False
_LAST_RETRAIN_TS = 0.0

# ==============================
# ML WARMUP CONFIG
# ==============================
MIN_TRADES_FOR_ML = 50  # change this if you want
MAX_OPEN_TRADES = 3
RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED = False


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


def _get_option_client():
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        return None
    return OptionHistoricalDataClient(api_key, secret_key)


def _parse_option_symbol(symbol):
    # OCC format: SPYyymmddC/P######## (strike with 3 decimals)
    if not symbol or len(symbol) < 15:
        return None
    if not symbol.startswith("SPY"):
        return None
    tail = symbol[3:]
    if len(tail) < 15:
        return None
    date_part = tail[:6]
    cp = tail[6:7]
    strike_part = tail[7:]
    if not (date_part.isdigit() and strike_part.isdigit() and cp in {"C", "P"}):
        return None
    yy = int(date_part[0:2])
    mm = int(date_part[2:4])
    dd = int(date_part[4:6])
    expiry = date(2000 + yy, mm, dd)
    strike = int(strike_part) / 1000.0
    return expiry, cp, strike


def _select_option_contract(direction, underlying_price):
    client = _get_option_client()
    if client is None or underlying_price is None:
        return None, None

    eastern = pytz.timezone("US/Eastern")
    today = datetime.now(eastern).date()
    contract_type = ContractType.CALL if direction == "bullish" else ContractType.PUT

    # Prefer same-day expiry when market is open.
    expiry_date = today if market_is_open() else None

    request = OptionChainRequest(
        underlying_symbol="SPY",
        type=contract_type,
        expiration_date=expiry_date,
        strike_price_gte=underlying_price * 0.9,
        strike_price_lte=underlying_price * 1.1,
    )

    rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
    chain = client.get_option_chain(request)
    if not chain:
        request = OptionChainRequest(
            underlying_symbol="SPY",
            type=contract_type,
            expiration_date_gte=today,
            strike_price_gte=underlying_price * 0.9,
            strike_price_lte=underlying_price * 1.1,
        )
        rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
        chain = client.get_option_chain(request)

    if not chain:
        return None, "no_option_chain"

    # Alpaca SDK may return a custom mapping type rather than a plain dict on
    # some SDK versions.  Normalise to dict so .items() is guaranteed to work.
    if not isinstance(chain, dict):
        try:
            chain = dict(chain)
        except (TypeError, ValueError):
            return None, "no_option_chain"
    if not chain:
        return None, "no_option_chain"

    candidates = []
    for symbol, snap in chain.items():
        parsed = _parse_option_symbol(symbol)
        if not parsed:
            continue
        exp, cp, strike = parsed
        if (cp == "C" and contract_type != ContractType.CALL) or (
            cp == "P" and contract_type != ContractType.PUT
        ):
            continue
        quote = getattr(snap, "latest_quote", None)
        bid = quote.bid_price if quote is not None else None
        ask = quote.ask_price if quote is not None else None
        if bid is None or ask is None:
            continue
        if bid <= 0 or ask <= 0:
            continue
        entry_price = (bid + ask) / 2
        candidates.append((symbol, exp, strike, bid, ask, entry_price))

    if not candidates:
        return None, "no_valid_quote"

    # Prefer nearest expiry (same-day already filtered if available), then closest ATM strike.
    candidates.sort(key=lambda x: (abs((x[1] - today).days), abs(x[2] - underlying_price)))
    attempts = 0
    for symbol, exp, strike, bid, ask, entry_price in candidates:
        if attempts >= 3:
            break
        attempts += 1
        spread = ask - bid
        if ask <= 0 or spread < 0:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="invalid_quote", bid=bid, ask=ask,
            )
            continue
        spread_pct = spread / ask
        if spread_pct > 0.15:
            log_contract_attempt(
                source="main", direction=direction, underlying_price=underlying_price,
                expiry=exp, dte=abs((exp - today).days), strike=strike,
                result="rejected", reason="spread_too_wide",
                bid=bid, ask=ask, spread_pct=spread_pct,
                mid=round((bid + ask) / 2, 4),
            )
            continue
        log_contract_attempt(
            source="main", direction=direction, underlying_price=underlying_price,
            expiry=exp, dte=abs((exp - today).days), strike=strike,
            result="selected", reason="selected",
            bid=bid, ask=ask, spread_pct=spread_pct,
            mid=round((bid + ask) / 2, 4),
        )
        return {
            "symbol": symbol,
            "expiry": exp.isoformat(),
            "strike": strike,
            "entry_price": float(entry_price),
            "bid": float(bid),
            "ask": float(ask),
        }, None

    return None, "spread_too_wide"
def apply_ml_and_edge_filters(
    acc,
    df,
    regime,
    vol_state,
    direction,
    confidence,
    score,
    impulse,
    follow,
    setup_type,
    ctx
):

    style = select_style(regime, vol_state, score)
    threshold = None

    total_trades = len(acc.get("trade_log", []))
    ml_weight_current = get_ml_weight()
    ctx.ml_weight = ml_weight_current
    
    # ----------------------------------
    # HARD ML WARMUP BYPASS
    # ----------------------------------
    if total_trades < MIN_TRADES_FOR_ML:
        style = select_style(regime, vol_state, score)

        conviction_norm = min(score / 6, 1.0)
        ctx.blended_score = conviction_norm
        ctx.threshold = None
        debug_log(
            "trade_filter_pass",
            layer="ml_warmup_bypass",
            threshold="N/A",
            blended_score=round(conviction_norm, 3),
            total_trades=total_trades,
            ml_weight=round(ml_weight_current, 3)
        )

        return True, conviction_norm, style

    # ------------------------------
    # Load models if needed
    # ------------------------------
    load_models()

    # ------------------------------
    # Get ML probability
    # ------------------------------
    allow_ml, ml_probability = ml_probability_gate(
        df,
        regime,
        score,
        impulse,
        follow,
        confidence,
        total_trades,
        direction_model,
        edge_model
    )

    # ------------------------------
    # Progressive Influence
    # ------------------------------
    if ml_probability is None:
        blended_score = confidence
    else:
        ml_weight = get_ml_weight()

        conviction_norm = min(score / 6, 1.0)

        blended_score = (
            conviction_norm * (1 - ml_weight)
            + ml_probability * ml_weight
        )

    # ------------------------------
    # Setup Intelligence Layer
    # ------------------------------

    intelligence = get_setup_intelligence(
        setup_type,
        regime,
        ml_probability
    )

    intelligence_score = intelligence["score"]

    # Blend intelligence with blended_score
    blended_score = (blended_score * 0.7) + (intelligence_score * 0.3)

    transition_data = detect_regime_transition()
    ctx.regime_transition = transition_data["transition"]
    ctx.regime_transition_severity = transition_data["severity"]
    # ------------------------------
    # Adaptive Threshold
    # ------------------------------
    threshold = adaptive_ml_threshold(total_trades)

    if transition_data["transition"]:
        threshold += transition_data["severity"] * 0.05

    # ------------------------------
    # Regime Stability Influence
    # ------------------------------

    persistence_data = calculate_regime_persistence()
    memory_data = get_regime_memory()

    # If persistence low → tighten
    if persistence_data["persistence"] < 0.6:
        threshold += 0.03

    # If new regime → distrust slightly
    threshold += (1 - memory_data["trust"]) * 0.05


    regime_stats = calculate_regime_expectancy()

    if regime_stats and regime in regime_stats:

        regime_conf = regime_stats[regime]["confidence"]
        regime_avg_R = regime_stats[regime]["avg_R"]
        regime_samples = regime_stats[regime].get(
            "regime_sample_count",
            regime_stats[regime].get("trades", 0)
        )
        ctx.regime_samples = regime_samples

        # Penalize negative expectancy regimes
        if regime_samples >= 20 and regime_avg_R < 0:
            ctx.set_block("expectancy_negative_regime")
            debug_log(
                "trade_blocked",
                gate="apply_ml_and_edge_filters",
                reason="negative_regime_expectancy",
                regime_samples=regime_samples,
                threshold=round(threshold, 3),
                blended_score=round(blended_score, 3)
            )
            return False, None, None

        # Tighten threshold if regime unstable
        if regime_samples >= 20 and regime_conf < 0.3:
            ctx.set_block("regime_low_confidence")
            debug_log(
                "trade_blocked",
                gate="apply_ml_and_edge_filters",
                reason="low_regime_confidence",
                regime_samples=regime_samples,
                threshold=round(threshold, 3),
                blended_score=round(blended_score, 3)
            )
            return False, None, None

    # Early stage forgiveness
    confidence_decay = 1 - get_ml_weight()
    threshold -= 0.05 * confidence_decay
    # ------------------------------
    # Stability Mode Tightening
    # ------------------------------
    mode = get_stability_mode()

    threshold += mode["threshold_buffer"]

    debug_log(
        "ml_visibility",
        total_trades=total_trades,
        ml_weight=round(ml_weight_current, 3),
        threshold=round(threshold, 3),
        blended_score=round(blended_score, 3)
    )
    ctx.blended_score = blended_score
    ctx.threshold = threshold
    _daily_stats["blended_scores"].append(float(blended_score))
    _daily_stats["thresholds"].append(float(threshold))
    _blend_history.append(float(blended_score))
    _threshold_history.append(float(threshold))
    if len(_blend_history) == 20 and len(_threshold_history) == 20:
        avg_blended_last20 = sum(_blend_history) / 20
        avg_threshold_last20 = sum(_threshold_history) / 20
        debug_log(
            "ml_window_summary",
            samples=20,
            avg_blended_score_last20=round(avg_blended_last20, 3),
            avg_threshold_last20=round(avg_threshold_last20, 3)
        )

    if blended_score < threshold:
        ctx.set_block("ml_threshold")
        debug_log(
            "trade_blocked",
            gate="apply_ml_and_edge_filters",
            reason="blended_below_threshold",
            threshold=round(threshold, 3),
            blended_score=round(blended_score, 3)
        )
        return False, None, None

    # ------------------------------
    # Setup Expectancy Influence
    # ------------------------------
    setup_stats = calculate_setup_expectancy()

    if setup_stats and setup_type in setup_stats:
        ctx.expectancy_samples = setup_stats[setup_type].get(
            "trades",
            setup_stats[setup_type].get("count")
        )

        avg_R = setup_stats[setup_type]["avg_R"]

        if avg_R < 0:
            style = "scalp"
        elif avg_R > 1.0:
            style = "momentum"

    debug_log(
        "trade_filter_pass",
        layer="apply_ml_and_edge_filters",
        threshold=round(threshold, 3),
        blended_score=round(blended_score, 3),
        style=style
    )
    return True, blended_score, style
# =========================
# OPEN TRADE ENGINE
# =========================

async def open_trade_if_valid(ctx=None):
    if ctx is None:
        ctx = DecisionContext()

    _record_signal_attempt()

    acc = load_account()
    eastern = pytz.timezone("US/Eastern")
    now_eastern = datetime.now(eastern)
    _roll_daily_summary_if_needed(acc, now_eastern)

    # ----------------------------
    # 1️⃣ Pre-Trade Protection Layer
    # ----------------------------
    protection = pre_trade_checks(acc, ctx)
    if protection is not None:
        if ctx.block_reason is None:
            ctx.set_block(f"protection_{protection}")
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return protection

    # ----------------------------
    # 2️⃣ Signal Generation Layer
    # ----------------------------
    signal = generate_signal(acc, ctx)
    if signal is None:
        if ctx.block_reason is None:
            ctx.set_block("signal_none")
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return None

    df, regime, vol_state, direction, confidence, score, impulse, follow, price, setup_type = signal

    # ----------------------------
    # 3️⃣ ML + Edge Filtering Layer
    # ----------------------------
    allow, blended_score, style = apply_ml_and_edge_filters(
        acc,
        df,
        regime,
        vol_state,
        direction,
        confidence,
        score,
        impulse,
        follow,
        setup_type,
        ctx
    )
    
    if not allow:
        category = _category_for_block_reason(ctx.block_reason)
        if category:
            _record_gate_block(category)
        return None

    # ----------------------------
    # 4️⃣ Execution Plan Layer
    # ----------------------------
    execution = build_execution_plan(
        acc,
        df,
        regime,
        vol_state,
        direction,
        style,
        price,
        setup_type
    )

    if execution is None:
        ctx.set_block("execution_plan_none")
        return None
    if isinstance(execution, dict) and execution.get("block_reason"):
        ctx.set_block(execution["block_reason"])
        return None
    if not isinstance(execution, tuple) or len(execution) != 4:
        ctx.set_block("execution_plan_none")
        return None

    risk_dollars, trade_size, option, target_R = execution
    try:
        risk_dollars = float(risk_dollars)
        trade_size = int(trade_size)
        target_R = float(target_R)
    except (TypeError, ValueError):
        ctx.set_block("execution_plan_none")
        return None
    if not isinstance(option, dict):
        ctx.set_block("execution_plan_none")
        return None

    virtual_cap = acc.get("virtual_capital_limit", acc["balance"])
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []

    open_trades_updated = False
    for t in open_trades:
        if isinstance(t, dict) and t.get("trade_id") is None:
            t["trade_id"] = str(uuid.uuid4())
            open_trades_updated = True
    if open_trades_updated:
        acc["open_trades"] = open_trades
        save_account(acc)

    unique_ids = {
        t.get("trade_id")
        for t in open_trades
        if isinstance(t, dict) and t.get("trade_id") is not None
    }

    if len(unique_ids) >= MAX_OPEN_TRADES:
        ctx.set_block("max_open_trades_reached")
        return None
    unique_risk = {}
    for t in open_trades:
        if isinstance(t, dict):
            trade_id = t.get("trade_id")
            if trade_id:
                unique_risk[trade_id] = float(t.get("risk", 0))

    open_trade = acc.get("open_trade")
    if isinstance(open_trade, dict):
        trade_id = open_trade.get("trade_id")
        if trade_id and trade_id not in unique_risk:
            unique_risk[trade_id] = float(open_trade.get("risk", 0))

    total_open_risk = sum(unique_risk.values())
    if total_open_risk + risk_dollars > virtual_cap:
        ctx.set_block("capital_exposure_limit")
        return None

    option_symbol = option.get("symbol") if option else None
    bid = option.get("bid") if option else None
    ask = option.get("ask") if option else None
    if not option_symbol or bid is None or ask is None:
        ctx.set_block("execution_plan_none")
        return None
    try:
        bid = float(bid)
        ask = float(ask)
    except (TypeError, ValueError):
        ctx.set_block("execution_plan_none")
        return None
    if ask <= 0 or bid < 0:
        ctx.set_block("execution_plan_none")
        return None
    spread = ask - bid
    if spread < 0:
        ctx.set_block("execution_plan_none")
        return None
    spread_pct = spread / ask
    if spread_pct > 0.15:
        ctx.set_block("spread_too_wide")
        return None

    fill_result, exec_block = await execute_option_entry(option_symbol, trade_size, bid, ask, ctx=ctx, acc=acc)
    if fill_result is None:
        ctx.set_block(exec_block or "limit_not_filled")
        return None
    fill_price = fill_result.get("fill_price")
    filled_qty = fill_result.get("filled_qty")
    requested_qty = fill_result.get("requested_qty")
    fill_ratio = fill_result.get("fill_ratio")
    if fill_price is None or filled_qty is None or requested_qty is None:
        ctx.set_block("limit_not_filled")
        return None
    try:
        filled_qty = int(filled_qty)
        requested_qty = int(requested_qty)
        fill_ratio = float(fill_ratio) if fill_ratio is not None else None
    except (TypeError, ValueError):
        ctx.set_block("limit_not_filled")
        return None
    if filled_qty <= 0:
        ctx.set_block("limit_not_filled")
        return None
    if requested_qty <= 0:
        ctx.set_block("limit_not_filled")
        return None
    if fill_ratio is None:
        fill_ratio = filled_qty / requested_qty
    if fill_ratio < 0.5:
        ctx.set_block("partial_fill_below_threshold")
        return None
    if filled_qty < requested_qty:
        trade_size = filled_qty
        risk_dollars = risk_dollars * fill_ratio

    stop_loss_frac = 0.5
    if is_md_enabled():
        stop_loss_frac = 0.35
    stop = fill_price - (fill_price * stop_loss_frac)
    risk_per_contract = fill_price - stop              # option price units (per share)
    target = fill_price + (risk_per_contract * target_R)
    risk = trade_size * risk_per_contract * 100        # dollars: qty × $/share × 100 shares/contract

    # ----------------------------
    # 5️⃣ Create Trade Object
    # ----------------------------
    trade = create_trade_object(
        direction,
        style,
        fill_price,
        stop,
        target,
        risk,
        trade_size,
        confidence,
        regime,
        vol_state,
        score,
        impulse,
        follow,
        setup_type,
        blended_score,
        ctx
    )
    if option:
        trade["underlying"] = "SPY"
        trade["option_symbol"] = option.get("symbol")
        trade["strike"] = option.get("strike")
        trade["expiry"] = option.get("expiry")
        trade["quantity"] = trade_size
        trade["entry_price"] = fill_price
        trade["stop"] = stop
        trade["initial_stop"] = stop
        trade["target"] = target
        trade["stop_price"] = stop
        trade["target_price"] = target

    acc["open_trade"] = trade
    save_account(acc)
    _daily_stats["trades_opened"] += 1
    ctx.set_opened()
    debug_log(
        "trade_opened",
        direction=trade["type"],
        entry=round(trade["entry_price"], 2),
        confidence=round(trade["confidence"], 3),
        blended=trade.get("ml_probability")
    )

    return trade


def build_execution_plan(
    acc,
    df,
    regime,
    vol_state,
    direction,
    style,
    price,
    setup_type
):
    option, selection_block = _select_option_contract(direction, price)
    if selection_block:
        return {"block_reason": selection_block}
    if option is None:
        return {"block_reason": "no_option_chain"}

    entry_price = option["entry_price"]
    # One options contract = 100 shares.  risk_per_contract is the dollar
    # loss if the position hits the 25%-of-premium stop.
    risk_per_contract = entry_price * 100 * 0.25  # e.g. $1.50 mid → $37.50 risk/contract
    if risk_per_contract <= 0:
        return {"block_reason": "no_valid_quote"}

    # ----------------------------
    # Target Based on Style
    # ----------------------------
    if style == "momentum":
        target_R = 2.5
    elif style == "mini_swing":
        target_R = 2.0
    else:
        target_R = 1.2

    # ----------------------------
    # Position Sizing
    # ----------------------------
    risk_percent = get_dynamic_risk_percent(acc)
    debug_log("risk_percent_update", percent=risk_percent)
    effective_balance = min(
        acc["balance"],
        acc.get("virtual_capital_limit", acc["balance"])
    )
    risk_dollars = effective_balance * risk_percent
    if risk_dollars < 50:
        risk_dollars = 50

    quantity = int(risk_dollars // risk_per_contract)
    if quantity <= 0:
        return {"block_reason": "quantity_zero"}

    compression = get_edge_compression()
    quantity = int(quantity * compression["position_multiplier"])
    if quantity <= 0:
        return {"block_reason": "quantity_zero"}

    return risk_dollars, quantity, option, target_R

# =========================
# TRADE MANAGEMENT ENGINE
# =========================
def _finalize_reconstructed_trade(acc, trade, pnl, result_reason):
    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    result = "win" if pnl > 0 else "loss"
    if result == "win":
        acc["wins"] = acc.get("wins", 0) + 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "entry_time": trade.get("entry_time"),
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "entry_price": trade.get("entry_price"),
        "exit_price": trade.get("emergency_exit_price"),
        "pnl": pnl,
        "result": result,
        "result_reason": result_reason,
        "reconstructed": True,
        "R": None,
        "risk_unknown": True,
        "balance_after": acc["balance"],
    }

    trade_log = acc.get("trade_log", [])
    if not isinstance(trade_log, list):
        trade_log = []
    trade_log.append(trade_record)
    acc["trade_log"] = trade_log

    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list):
        open_trades = []
    acc["open_trades"] = [
        t for t in open_trades if not (isinstance(t, dict) and t.get("trade_id") == trade.get("trade_id"))
    ]

    save_account(acc)
    return result, pnl, acc["balance"], trade


def _manage_reconstructed_trades(acc):
    open_trades = acc.get("open_trades", [])
    if not isinstance(open_trades, list) or not open_trades:
        return None

    now = datetime.now(pytz.timezone("US/Eastern"))
    for trade in open_trades:
        if not isinstance(trade, dict) or not trade.get("reconstructed"):
            continue
        if trade.get("stop") is not None or trade.get("target") is not None:
            continue
        policy = trade.get("protection_policy", {})
        max_loss_pct = policy.get("max_loss_pct", 0.50)
        min_hold_seconds = policy.get("min_hold_seconds", 0)
        created_at = policy.get("created_at")
        if created_at:
            try:
                created_dt = datetime.fromisoformat(created_at)
                if created_dt.tzinfo is None:
                    created_dt = pytz.timezone("US/Eastern").localize(created_dt)
                if (now - created_dt).total_seconds() < float(min_hold_seconds):
                    continue
            except Exception:
                pass

        entry_price = trade.get("entry_price")
        option_symbol = trade.get("option_symbol")
        qty = trade.get("quantity")
        if entry_price is None or option_symbol is None or qty is None:
            continue
        try:
            entry_price = float(entry_price)
            qty = int(qty)
        except (TypeError, ValueError):
            continue
        if qty <= 0 or entry_price <= 0:
            continue

        current_price = get_option_price(option_symbol)
        if current_price is None:
            continue

        trade["last_manage_ts"] = now.isoformat()

        if current_price <= entry_price * (1 - float(max_loss_pct)):
            close_result = close_option_position(option_symbol, qty)
            filled_avg = close_result.get("filled_avg_price")
            if close_result.get("ok"):
                exit_price = None
                source = "estimated_mid"
                if filled_avg is not None:
                    exit_price = filled_avg
                    source = "broker_fill"
                else:
                    exit_price = current_price
                trade["emergency_exit_price"] = exit_price
                trade["emergency_exit_price_source"] = source
                pnl = (exit_price - entry_price) * qty * 100
                trade["result_reason"] = "reconstructed_emergency_stop"
                trade["recon_notice"] = {
                    "type": "emergency_stop_success",
                    "symbol": option_symbol,
                    "qty": qty,
                    "entry": entry_price,
                    "price": exit_price,
                    "ts": now.isoformat(),
                }
                return _finalize_reconstructed_trade(
                    acc, trade, pnl, "reconstructed_emergency_stop"
                )

            trade["emergency_stop_failed"] = True
            trade["recon_notice"] = {
                "type": "emergency_stop_failure",
                "symbol": option_symbol,
                "qty": qty,
                "entry": entry_price,
                "price": current_price,
                "ts": now.isoformat(),
            }
            save_account(acc)
            return None

    return None


def _manage_reconstructed_advanced(acc):
    if not RECONSTRUCTED_ADVANCED_MANAGEMENT_ENABLED:
        return None
    try:
        from core.account_repository import load_account
        from core.paths import DATA_DIR
        import json
        stats_path = os.path.join(DATA_DIR, "career_stats.json")
        with open(stats_path, "r", encoding="utf-8") as f:
            stats = json.load(f)
        closed_count = int(stats.get("total_trades", 0))
    except Exception:
        closed_count = 0
    if closed_count < 20:
        return None
    # TODO: trailing stop / take-profit for reconstructed trades.
    return None


def pre_trade_checks(acc, ctx):

    decay = edge_decay_status()

    if decay["status"] == "DISABLE":
        ctx.set_block("protection_EDGE_DECAY")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="EDGE_DECAY")
        return "EDGE_DECAY"

    if acc["balance"] <= acc["starting_balance"] * 0.85:
        ctx.set_block("protection_EQUITY_PROTECTION")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="EQUITY_PROTECTION")
        return "EQUITY_PROTECTION"

    if acc["daily_loss"] >= acc["max_daily_loss"]:
        ctx.set_block("protection_DAILY_LIMIT")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="DAILY_LIMIT")
        return "DAILY_LIMIT"

    if acc["open_trade"] is not None:
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="OPEN_TRADE_EXISTS")
        return "OPEN_TRADE_EXISTS"

    if not can_day_trade(acc):
        ctx.set_block("protection_PDT_LIMIT")
        debug_log("trade_gate_exit", gate="pre_trade_checks", reason="PDT_LIMIT")
        return "PDT_LIMIT"

    return None
def generate_signal(acc, ctx):

    df = get_market_dataframe()
    if df is None:
        ctx.set_block("no_market_data")
        debug_log("trade_gate_exit", gate="generate_signal", reason="NO_MARKET_DATA")
        return None

    regime = get_regime(df)
    ctx.regime = regime
    if regime in ["COMPRESSION", "RANGE", "NO_DATA"]:
        ctx.set_block(f"regime_{regime.lower()}")
        debug_log("trade_gate_exit", gate="generate_signal", reason=f"REGIME_{regime}")
        return None

    vol_state = volatility_state(df)
    ctx.volatility = vol_state
    if vol_state in ["DEAD", "LOW"]:
        ctx.set_block(f"volatility_{vol_state.lower()}")
        debug_log("trade_gate_exit", gate="generate_signal", reason=f"VOL_{vol_state}")
        return None

    bias = make_prediction(60, df)
    trigger = make_prediction(15, df)

    if bias is None or trigger is None:
        ctx.set_block("prediction_none")
        debug_log("trade_gate_exit", gate="generate_signal", reason="PREDICTION_NONE")
        return None

    _track_confidence_distribution(bias, trigger)
    ctx.direction_60m = bias.get("direction")
    ctx.confidence_60m = bias.get("confidence")
    ctx.direction_15m = trigger.get("direction")
    ctx.confidence_15m = trigger.get("confidence")
    ctx.dual_alignment = bias.get("direction") == trigger.get("direction")

    if bias["direction"] != trigger["direction"]:
        ctx.set_block("direction_mismatch")
        debug_log(
            "trade_gate_exit",
            gate="generate_signal",
            reason="DIRECTION_MISMATCH",
            bias=bias["direction"],
            trigger=trigger["direction"]
        )
        return None

    if bias["confidence"] < 0.55 or trigger["confidence"] < 0.55:
        ctx.set_block("confidence")
        debug_log(
            "trade_gate_exit",
            gate="generate_signal",
            reason="CONFIDENCE_BELOW_THRESHOLD",
            bias_conf=bias["confidence"],
            trigger_conf=trigger["confidence"]
        )
        return None

    direction = bias["direction"]
    confidence = bias["confidence"]

    price = get_latest_price()
    if price is None:
        ctx.set_block("no_latest_price")
        debug_log("trade_gate_exit", gate="generate_signal", reason="NO_LATEST_PRICE")
        return None

    ctx.spy_price = price
    setup_type = classify_trade(price, direction)

    score, impulse, follow, _ = calculate_conviction(df)
    ctx.conviction_score = score
    ctx.impulse = impulse
    ctx.follow = follow
    debug_log(
        "signal_generated",
        direction=direction,
        confidence=round(confidence, 3),
        regime=regime,
        volatility=vol_state,
        conviction=score
    )

    return (
        df,
        regime,
        vol_state,
        direction,
        confidence,
        score,
        impulse,
        follow,
        price,
        setup_type
    )

def create_trade_object(
    direction,
    style,
    price,
    stop,
    target,
    risk,
    trade_size,
    confidence,
    regime,
    vol_state,
    score,
    impulse,
    follow,
    setup_type,
    blended_score,
    ctx
):

    trade_id = str(uuid.uuid4())
    return {
        "trade_id": trade_id,
        "type": direction,
        "style": style,
        "entry_price": price,
        "size": trade_size,
        "risk": risk,
        "confidence": confidence,
        "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "stop": stop,
        "initial_stop": stop,
        "target": target,
        "regime": regime,
        "volatility": vol_state,
        "conviction_score": score,
        "impulse": impulse,
        "follow_through": follow,
        "ml_probability": round(blended_score, 3) if blended_score else None,
        "setup": setup_type,
        "underlying": None,
        "strike": None,
        "expiry": None,
        "option_symbol": None,
        "quantity": None,
        "decision_snapshot": {
            "timestamp": ctx.timestamp.isoformat(),
            "regime": ctx.regime,
            "volatility": ctx.volatility,
            "direction_60m": ctx.direction_60m,
            "confidence_60m": ctx.confidence_60m,
            "direction_15m": ctx.direction_15m,
            "confidence_15m": ctx.confidence_15m,
            "dual_alignment": ctx.dual_alignment,
            "conviction_score": ctx.conviction_score,
            "impulse": ctx.impulse,
            "follow": ctx.follow,
            "blended_score": ctx.blended_score,
            "threshold": ctx.threshold,
            "threshold_delta": (
                round(ctx.blended_score - ctx.threshold, 6)
                if ctx.blended_score is not None and ctx.threshold is not None
                else None
            ),
            "ml_weight": ctx.ml_weight,
            "regime_samples": ctx.regime_samples,
            "expectancy_samples": ctx.expectancy_samples
        },
        "runner_active": False,
        "partial_taken": False,
        "regime_transition_at_entry": getattr(ctx, "regime_transition", None),
        "regime_transition_severity": getattr(ctx, "regime_transition_severity", None),
    }
def manage_trade():

    acc = load_account()

    recon_result = _manage_reconstructed_trades(acc)
    if recon_result:
        return recon_result

    advanced_result = _manage_reconstructed_advanced(acc)
    if advanced_result:
        return advanced_result

    if acc["open_trade"] is None:
        return None

    trade = acc["open_trade"]

    # Use the live option price for all stop/target comparisons.
    # stop and target are expressed in option-price units, so comparing
    # them to the underlying stock price (get_latest_price) is wrong.
    option_symbol = trade.get("option_symbol") if isinstance(trade, dict) else None
    if not option_symbol:
        return None
    price = get_option_price(option_symbol)
    if price is None:
        return None

    # Expiry handling: close same-day expiry positions 5 minutes before close
    try:
        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        expiry_raw = trade.get("expiry")
        expiry_date = None
        if isinstance(expiry_raw, str):
            expiry_date = datetime.fromisoformat(expiry_raw).date()
        if expiry_date == now_et.date() and now_et.time() >= datetime.strptime("15:55", "%H:%M").time():
            result = "win" if price >= trade["entry_price"] else "loss"
            pnl = calculate_pnl(trade, result, price)
            return finalize_trade(acc, trade, "expiry_close", pnl)
    except Exception:
        pass

    # 1️⃣ Expectancy Protection
    expectancy_exit = check_expectancy_exit(acc, trade, price)
    if expectancy_exit:
        return expectancy_exit

    # 2️⃣ Partial Logic
    partial = check_partial_logic(acc, trade, price)
    if partial is not None:
        return None

    # 3️⃣ Hard Exit Conditions
    exit_result = check_exit_conditions(trade, price)
    if exit_result is None:
        return None

    result = exit_result

    # 4️⃣ Calculate PnL
    pnl = calculate_pnl(trade, result, price)

    # 5️⃣ Finalize Trade
    return finalize_trade(acc, trade, result, pnl)

def check_expectancy_exit(acc, trade, price):
    """
    price must be the current option price (not stock price).
    Uses the same R-based P&L formula as calculate_pnl so that balance
    updates and R_multiple are consistent across all exit paths.
    """
    setup_stats = calculate_setup_expectancy()
    current_setup = trade.get("setup")

    if setup_stats and current_setup in setup_stats:

        avg_R = setup_stats[current_setup]["avg_R"]

        if avg_R < -0.25:
            entry = trade.get("entry_price")
            initial_stop = trade.get("initial_stop") or trade.get("stop")
            risk_amount = trade.get("risk", 0)

            if not entry or not initial_stop or entry == initial_stop or not risk_amount:
                return None

            risk_per = abs(entry - initial_stop)
            move = (price - entry) if trade["type"] == "bullish" else (entry - price)
            move_ratio = move / risk_per
            pnl = risk_amount * move_ratio

            return finalize_trade(acc, trade, "edge_exit", pnl)

    return None
def check_partial_logic(acc, trade, price):

    if trade["style"] != "momentum":
        return None

    if trade["partial_taken"]:
        return None

    hit_target = (
        trade["type"] == "bullish" and price >= trade["target"]
    ) or (
        trade["type"] == "bearish" and price <= trade["target"]
    )

    if not hit_target:
        return None

    move_ratio = abs(price - trade["entry_price"]) / abs(
        trade["entry_price"] - trade["initial_stop"]
    )

    partial_pnl = trade["risk"] * move_ratio * 0.5

    acc["balance"] += partial_pnl

    trade["partial_taken"] = True
    trade["runner_active"] = True
    trade["stop"] = trade["entry_price"]

    acc["open_trade"] = trade
    save_account(acc)

    return True
def check_exit_conditions(trade, price):

    if trade["type"] == "bullish":

        if price <= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price >= trade["target"]:
            return "win"

    if trade["type"] == "bearish":

        if price >= trade["stop"]:
            return "loss" if not trade["partial_taken"] else "win"

        if not trade["partial_taken"] and price <= trade["target"]:
            return "win"

    return None
def calculate_pnl(trade, result, price):

    risk_amount = trade["risk"]

    if trade["style"] == "momentum" and trade["partial_taken"]:
        return 0

    if result == "win":

        move_ratio = abs(price - trade["entry_price"]) / abs(
            trade["entry_price"] - trade["initial_stop"]
        )

        return risk_amount * move_ratio

    return -risk_amount
def finalize_trade(acc, trade, result, pnl):

    if pnl < 0:
        acc["daily_loss"] += abs(pnl)

    acc["balance"] += pnl
    if acc["balance"] > acc.get("peak_balance", 0):
        acc["peak_balance"] = acc["balance"]

    if result == "win":
        acc["wins"] += 1
    else:
        acc["losses"] += 1

    update_career_after_trade(trade, result, pnl, acc["balance"])
    log_trade_features(trade, result, pnl)

    if trade["risk"] > 0:
        R_multiple = round(pnl / trade["risk"], 3)
    else:
        R_multiple = 0

    if not trade.get("option_symbol"):
        raise RuntimeError("Missing option metadata")
    if trade.get("quantity") is None or trade.get("quantity") <= 0:
        raise RuntimeError("Invalid quantity for option trade")

    trade_record = {
        "trade_id": trade.get("trade_id"),
        "entry_time": trade["entry_time"],
        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
        "type": trade["type"],
        "style": trade.get("style", "unknown"),
        "risk": trade["risk"],
        "R": R_multiple,
        "regime": trade.get("regime"),
        "setup": trade.get("setup", "UNKNOWN"),
        "underlying": trade.get("underlying"),
        "strike": trade.get("strike"),
        "expiry": trade.get("expiry"),
        "option_symbol": trade.get("option_symbol"),
        "quantity": trade.get("quantity"),
        "confidence": trade.get("confidence", 0),
        "result": result,
        "pnl": pnl,
        "balance_after": acc["balance"],
    }
    if trade_record.get("R") is not None:
        if trade_record["R"] > 0:
            trade_record["result"] = "win"
        elif trade_record["R"] < 0:
            trade_record["result"] = "loss"
        else:
            trade_record["result"] = "breakeven"
    review = review_trade(trade_record, result)
    print(review)
    acc["trade_log"].append(trade_record)
    acc["day_trades"].append(datetime.now(pytz.timezone("US/Eastern")).isoformat())
    acc["open_trade"] = None

    save_account(acc)
    # FIX: Trigger periodic ML retraining in background (non-blocking)
    maybe_retrain_models()

    return result, pnl, acc["balance"], trade
