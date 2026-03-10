# decision/trader_filters.py
#
# ML + edge filter layer extracted from decision/trader.py.
# Contains apply_ml_and_edge_filters and its direct helpers.
#
# All function signatures are identical to their originals in trader.py.
# ─────────────────────────────────────────────────────────────────────────────

from analytics.ml_loader import load_edge_model, build_feature_vector  # noqa: F401
from analytics.adaptive_threshold import adaptive_ml_threshold
from analytics.progressive_influence import get_ml_weight
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.setup_intelligence import get_setup_intelligence
from analytics.stability_mode import get_stability_mode
from analytics.regime_transition import detect_regime_transition
from analytics.regime_persistence import calculate_regime_persistence
from analytics.regime_memory import get_regime_memory
from execution.ml_gate import ml_probability_gate
from core.debug import debug_log

# Import shared state from trader_utils so all modules share ONE copy of the
# module-level globals (direction_model, edge_model, _blend_history, etc.)
import decision.trader_utils as _utils

MIN_TRADES_FOR_ML = 50  # must match trader.py / trader_utils.py


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
    from decision.trader_utils import select_style  # local to avoid circular at module level

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
    _utils.load_models()

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
        _utils.direction_model,
        _utils.edge_model
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
    _utils._daily_stats["blended_scores"].append(float(blended_score))
    _utils._daily_stats["thresholds"].append(float(threshold))
    _utils._blend_history.append(float(blended_score))
    _utils._threshold_history.append(float(threshold))
    if len(_utils._blend_history) == 20 and len(_utils._threshold_history) == 20:
        avg_blended_last20 = sum(_utils._blend_history) / 20
        avg_threshold_last20 = sum(_utils._threshold_history) / 20
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
