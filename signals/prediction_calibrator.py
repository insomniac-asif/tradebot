"""
Prediction Confidence Calibrator

Wraps make_prediction() to filter low-quality predictions based on
empirical accuracy analysis. Key findings from calibration study:

1. Confidence is ANTI-CORRELATED with accuracy (lower conf = better)
2. Hour 9 (first 30 min) has 46% accuracy (below random) → filter out
3. Power hour (15:00) is best at 66.5%
4. RANGE regime is best (67.3%), VOLATILE worst (59.8%)
5. Overall directional accuracy is 61.8% — the predictor works

Usage:
    from signals.prediction_calibrator import calibrated_prediction
    result = calibrated_prediction(minutes=60, df=df, regime=regime, hour=14)
    if result["pass_filter"]:
        direction = result["direction"]
"""
import json
from pathlib import Path

from signals.predictor import make_prediction

CALIBRATION_PATH = Path("data/calibration_config.json")

DEFAULT_CONFIG = {
    "blocked_hours": [9],
    "min_confidence": 0.0,
    "regime_adjustments": {},
    "enabled": True,
}


def load_config():
    """Load calibration config, merging with defaults."""
    if CALIBRATION_PATH.exists():
        try:
            with open(CALIBRATION_PATH) as f:
                stored = json.load(f)
            return {**DEFAULT_CONFIG, **stored}
        except (json.JSONDecodeError, OSError):
            pass
    return DEFAULT_CONFIG.copy()


def calibrated_prediction(minutes=60, df=None, regime=None, hour=None):
    """
    Wraps make_prediction() with empirical quality filtering.

    Returns dict with original prediction fields plus:
        raw_confidence: float — original confidence
        pass_filter: bool — True if prediction passes quality checks
        reason: str — human-readable pass/fail explanation
        calibrated: bool — True (went through calibrator)
    """
    config = load_config()

    raw = make_prediction(minutes=minutes, df=df)

    direction = raw.get("direction", "range")
    confidence = raw.get("confidence", 0.0)

    if not config.get("enabled", True):
        return {
            **raw,
            "raw_confidence": confidence,
            "pass_filter": True,
            "calibrated": False,
            "reason": "calibrator_disabled",
        }

    # Check blocked hours (hour 9 = first 30 min, 46% accuracy)
    blocked_hours = config.get("blocked_hours", [9])
    if hour is not None and hour in blocked_hours:
        return {
            **raw,
            "raw_confidence": confidence,
            "pass_filter": False,
            "calibrated": True,
            "reason": f"hour={hour} is blocked (low accuracy)",
        }

    # Check minimum confidence floor
    min_conf = config.get("min_confidence", 0.0)
    if confidence < min_conf:
        return {
            **raw,
            "raw_confidence": confidence,
            "pass_filter": False,
            "calibrated": True,
            "reason": f"conf={confidence:.3f} < min={min_conf:.3f}",
        }

    # High confidence = least reliable (anti-correlated, r=-0.065)
    max_conf = config.get("max_confidence", 0.70)
    if max_conf and confidence >= max_conf:
        return {
            **raw,
            "raw_confidence": confidence,
            "pass_filter": False,
            "calibrated": True,
            "reason": f"conf={confidence:.3f} >= max={max_conf:.3f} (anti-correlated)",
        }

    # Range predictions are almost always wrong (3% accuracy)
    if direction == "range":
        return {
            **raw,
            "raw_confidence": confidence,
            "pass_filter": False,
            "calibrated": True,
            "reason": "range predictions have <3% accuracy",
        }

    return {
        **raw,
        "raw_confidence": confidence,
        "pass_filter": True,
        "calibrated": True,
        "reason": f"passed: dir={direction}, conf={confidence:.3f}, hour={hour}",
    }
