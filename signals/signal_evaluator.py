# signals/signal_evaluator.py

from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state


def grade_trade(direction):

    pred = make_prediction(60)
    regime = get_regime()
    vol = volatility_state()

    pred_direction = pred.get("direction") if isinstance(pred, dict) else None
    pred_conf = pred.get("confidence") if isinstance(pred, dict) else None

    if pred_direction is None or pred_conf is None:
        return {
            "grade": "N/A",
            "score": 0,
            "confidence": None,
            "model_direction": pred_direction,
            "regime": regime,
            "volatility": vol,
            "reasons": ["Prediction unavailable"],
        }

    score = 0
    reasons = []

    # Alignment with model
    if direction == pred_direction:
        score += 2
        reasons.append("Aligned with model prediction")
    else:
        reasons.append("Against model prediction")

    # Confidence weighting
    confidence = round(float(pred_conf) * 100, 1)

    if confidence >= 70:
        score += 1
        reasons.append("High statistical confidence")
    elif confidence < 55:
        reasons.append("Low statistical confidence")

    # Regime filter
    if regime == "TREND":
        score += 1
        reasons.append("Trending market conditions")
    elif regime == "RANGE":
        reasons.append("Range market")

    # Volatility filter
    if vol in ["NORMAL", "HIGH"]:
        score += 1
        reasons.append("Sufficient volatility")
    else:
        reasons.append("Low volatility")

    grade = (
        "A" if score >= 4
        else "B" if score == 3
        else "C" if score == 2
        else "D"
    )

    return {
        "grade": grade,
        "score": score,
        "confidence": confidence,
        "model_direction": pred_direction,
        "regime": regime,
        "volatility": vol,
        "reasons": reasons
    }
