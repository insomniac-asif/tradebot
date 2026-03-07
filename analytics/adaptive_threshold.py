from analytics.edge_stability import calculate_edge_stability
from analytics.ml_accuracy import ml_rolling_accuracy
from core.account_repository import load_account


def adaptive_ml_threshold(total_trades):
    """
    Institutional-grade adaptive ML threshold.

    Adjusted by:
    - Sample size
    - Edge stability
    - Current drawdown pressure
    """

    # ========================
    # 1️⃣ Base Threshold Curve
    # ========================

    if total_trades < 20:
        base = 0.55
    elif total_trades < 50:
        base = 0.58
    elif total_trades < 100:
        base = 0.60
    else:
        base = 0.62

    # ========================
    # 2️⃣ Edge Stability Adjustment
    # ========================

    stability_data = calculate_edge_stability()

    stability_adjustment = 0

    if stability_data:

        stability = stability_data["stability"]

        # Scale adjustment strength by sample size
        sample_scale = min(total_trades / 100, 1)

        # Stability centered at 0.5
        stability_shift = (stability - 0.5)

        # Max ±0.06 adjustment when fully scaled
        stability_adjustment = stability_shift * 0.06 * sample_scale
    dynamic_threshold = base - stability_adjustment

    ml_stats = ml_rolling_accuracy()
    if ml_stats:
        acc = ml_stats.get("accuracy")
        if acc is not None:
            # If ML performing poorly → tighten threshold
            if acc < 52:
                dynamic_threshold += 0.03

            # If ML strong → slightly loosen
            if acc > 60:
                dynamic_threshold -= 0.02

    # ========================
    # 3️⃣ Drawdown Pressure
    # ========================

    acc = load_account()

    peak = acc.get("peak_balance", acc["starting_balance"])
    balance = acc.get("balance", 0)

    if peak > 0:
        drawdown = (peak - balance) / peak
    else:
        drawdown = 0

    # If >5% drawdown → tighten
    if drawdown > 0.05:
        drawdown_adjustment = min(drawdown * 0.15, 0.05)
    else:
        drawdown_adjustment = 0

    # ========================
    # 4️⃣ Final Threshold
    # ========================

    dynamic_threshold = dynamic_threshold + drawdown_adjustment

    # Hard clamp safety
    dynamic_threshold = max(0.50, min(dynamic_threshold, 0.72))

    return round(dynamic_threshold, 3)
