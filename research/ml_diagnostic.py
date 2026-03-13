"""
ML Predictor Diagnostic — Accuracy Decomposition
Run: cd /home/asif420/qqqbot && python -m research.ml_diagnostic
"""
import sqlite3
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

DB_PATH = Path("data/analytics.db")
WEIGHTS_PATH = Path("data/predictor_weights.json")
DIR_MODEL_PATH = Path("data/direction_model.pkl")
EDGE_MODEL_PATH = Path("data/edge_model.pkl")

SEP = "=" * 70


def load_data():
    """Load predictions and trade_features from SQLite."""
    conn = sqlite3.connect(DB_PATH)
    predictions = pd.read_sql("SELECT * FROM predictions WHERE checked = 1", conn)
    predictions["time"] = pd.to_datetime(predictions["time"])
    trades = pd.read_sql("SELECT * FROM trade_features", conn)
    conn.close()
    print(f"Loaded {len(predictions):,} graded predictions "
          f"({predictions['time'].min().date()} to {predictions['time'].max().date()})")
    print(f"Loaded {len(trades):,} closed trades from trade_features")
    print(f"Symbols: {sorted(predictions['symbol'].unique())}")
    return predictions, trades


# ── 1A: Overall accuracy + class distribution ──────────────────────────

def section_1a(pred):
    print(f"\n{SEP}")
    print("1A — OVERALL ACCURACY & CLASS DISTRIBUTION")
    print(SEP)
    overall_acc = pred["correct"].mean()
    print(f"Overall 3-class accuracy: {overall_acc:.4f} ({overall_acc:.1%})")
    print(f"Baseline (random 3-class):  0.3333 (33.3%)")
    print(f"Lift over random: {(overall_acc - 0.3333)*100:+.1f}pp")

    print("\nPredicted class distribution:")
    pred_dist = pred["direction"].value_counts(normalize=True).sort_index()
    for cls, pct in pred_dist.items():
        n = (pred["direction"] == cls).sum()
        print(f"  {cls:>10s}: {pct:.1%}  (n={n:,})")

    print("\nActual outcome distribution:")
    act_dist = pred["actual"].value_counts(normalize=True).sort_index()
    for cls, pct in act_dist.items():
        n = (pred["actual"] == cls).sum()
        print(f"  {cls:>10s}: {pct:.1%}  (n={n:,})")


# ── 1B: Accuracy by predicted class ─────────────────────────────────

def section_1b(pred):
    print(f"\n{SEP}")
    print("1B — ACCURACY BY PREDICTED CLASS")
    print(SEP)
    for cls in sorted(pred["direction"].unique()):
        mask = pred["direction"] == cls
        n = mask.sum()
        acc = pred.loc[mask, "correct"].mean()
        print(f"  Predicted {cls:>10s}: {acc:.1%} accuracy  (n={n:,})")


# ── 1C: Accuracy by time of day ─────────────────────────────────────

def section_1c(pred):
    print(f"\n{SEP}")
    print("1C — ACCURACY BY TIME OF DAY")
    print(SEP)
    pred = pred.copy()
    pred["hour"] = pred["time"].dt.hour
    pred["session_bucket"] = pd.cut(
        pred["hour"],
        bins=[0, 10, 11, 14, 16, 24],
        labels=["PRE/OPEN (≤10)", "MORNING (10-11)", "MIDDAY (11-14)",
                "AFTERNOON (14-16)", "AFTER (16+)"],
        right=False,
    )
    for bucket in pred["session_bucket"].cat.categories:
        mask = pred["session_bucket"] == bucket
        n = mask.sum()
        if n < 50:
            continue
        acc = pred.loc[mask, "correct"].mean()
        print(f"  {bucket:>25s}: {acc:.1%}  (n={n:,})")


# ── 1D: Accuracy by regime, session, volatility ─────────────────────

def section_1d(pred):
    print(f"\n{SEP}")
    print("1D — ACCURACY BY REGIME / SESSION / VOLATILITY")
    print(SEP)
    for col_name, col in [("Regime", "regime"), ("Session", "session"),
                           ("Volatility", "volatility")]:
        print(f"\n  {col_name}:")
        for val in sorted(pred[col].dropna().unique()):
            mask = pred[col] == val
            n = mask.sum()
            if n < 50:
                continue
            acc = pred.loc[mask, "correct"].mean()
            print(f"    {val:>15s}: {acc:.1%}  (n={n:,})")


# ── 1E: Accuracy by confidence band ─────────────────────────────────

def section_1e(pred):
    print(f"\n{SEP}")
    print("1E — ACCURACY BY CONFIDENCE BAND")
    print(SEP)
    # Use existing confidence_band column
    for band in sorted(pred["confidence_band"].dropna().unique()):
        mask = pred["confidence_band"] == band
        n = mask.sum()
        if n < 50:
            continue
        acc = pred.loc[mask, "correct"].mean()
        avg_conf = pred.loc[mask, "confidence"].mean()
        print(f"  Band '{band:>10s}': {acc:.1%} accuracy, "
              f"avg conf={avg_conf:.3f}  (n={n:,})")

    # Also bucket by raw confidence
    print("\n  Raw confidence quartiles:")
    pred = pred.copy()
    pred["conf_q"] = pd.qcut(pred["confidence"], q=4, duplicates="drop")
    for q in sorted(pred["conf_q"].dropna().unique()):
        mask = pred["conf_q"] == q
        n = mask.sum()
        if n < 50:
            continue
        acc = pred.loc[mask, "correct"].mean()
        print(f"    {str(q):>25s}: {acc:.1%}  (n={n:,})")


# ── 1F: Accuracy by month ───────────────────────────────────────────

def section_1f(pred):
    print(f"\n{SEP}")
    print("1F — ACCURACY BY MONTH (temporal drift)")
    print(SEP)
    pred = pred.copy()
    pred["month"] = pred["time"].dt.to_period("M")
    for month in sorted(pred["month"].unique()):
        mask = pred["month"] == month
        n = mask.sum()
        if n < 50:
            continue
        acc = pred.loc[mask, "correct"].mean()
        print(f"  {str(month):>10s}: {acc:.1%}  (n={n:,})")


# ── 1G: Confusion matrix ────────────────────────────────────────────

def section_1g(pred):
    print(f"\n{SEP}")
    print("1G — CONFUSION MATRIX")
    print(SEP)
    from sklearn.metrics import confusion_matrix, classification_report

    # Include all classes including 'both' in actual
    labels = sorted(set(pred["direction"].unique()) | set(pred["actual"].unique()))
    cm = confusion_matrix(pred["actual"], pred["direction"], labels=labels)
    print(f"\nLabels: {labels}")
    print(f"Rows = actual, Cols = predicted\n")
    # Header
    header = "           " + "".join(f"{l:>10s}" for l in labels)
    print(header)
    for i, row_label in enumerate(labels):
        row_str = f"{row_label:>10s} " + "".join(f"{cm[i, j]:>10d}" for j in range(len(labels)))
        print(row_str)

    # Classification report (predicted vs actual)
    print("\nClassification Report (actual includes 'both'):")
    print(classification_report(
        pred["actual"], pred["direction"],
        labels=labels, zero_division=0
    ))


# ── 1H: The "both" problem ──────────────────────────────────────────

def section_1h(pred):
    print(f"\n{SEP}")
    print('1H — THE "BOTH" PROBLEM')
    print(SEP)
    both_mask = pred["actual"] == "both"
    both_count = both_mask.sum()
    both_pct = both_count / len(pred)
    print(f'Predictions with actual="both": {both_count:,} ({both_pct:.1%})')
    print(f'These are ALWAYS counted as wrong (correct=0)')

    # What if we reclassify "both" as directionally correct?
    # If predicted bullish and high_hit=1 → correct
    # If predicted bearish and low_hit=1 → correct
    directional_correct = (
        ((pred["direction"] == "bullish") & (pred["high_hit"] == 1)) |
        ((pred["direction"] == "bearish") & (pred["low_hit"] == 1))
    )
    directional_acc = directional_correct.mean()
    print(f"\nDirectional accuracy (bullish→high_hit, bearish→low_hit):")
    print(f"  {directional_acc:.1%} (vs {pred['correct'].mean():.1%} original)")
    print(f"  Lift: {(directional_acc - pred['correct'].mean())*100:+.1f}pp")

    # Among "both" outcomes, what was predicted?
    both_pred = pred.loc[both_mask, "direction"].value_counts(normalize=True)
    print(f"\nAmong 'both' outcomes, predictions were:")
    for cls, pct in both_pred.items():
        print(f"  {cls:>10s}: {pct:.1%}")

    # Binary accuracy: up vs down (exclude range predictions)
    binary_mask = pred["direction"].isin(["bullish", "bearish"])
    binary_pred = pred[binary_mask]
    binary_correct = (
        ((binary_pred["direction"] == "bullish") & (binary_pred["high_hit"] == 1)) |
        ((binary_pred["direction"] == "bearish") & (binary_pred["low_hit"] == 1))
    )
    binary_acc = binary_correct.mean()
    print(f"\nBinary accuracy (bullish/bearish only, directional):")
    print(f"  {binary_acc:.1%}  (n={len(binary_pred):,})")

    # What if we simply exclude "both" from grading?
    no_both = pred[~both_mask]
    no_both_acc = no_both["correct"].mean()
    print(f"\nAccuracy excluding 'both' outcomes:")
    print(f"  {no_both_acc:.1%}  (n={len(no_both):,})")


# ── 1I: Direction model analysis ─────────────────────────────────────

def section_1i():
    print(f"\n{SEP}")
    print("1I — DIRECTION MODEL ANALYSIS")
    print(SEP)
    if not DIR_MODEL_PATH.exists():
        print("  ⚠️  direction_model.pkl not found")
        return

    import joblib
    model = joblib.load(DIR_MODEL_PATH)
    print(f"  Type: {type(model).__name__}")
    print(f"  N features: {model.n_features_in_}")
    if hasattr(model, "feature_names_in_"):
        print(f"  Features: {list(model.feature_names_in_)}")
    print(f"  N estimators: {model.n_estimators}")
    print(f"  Max depth: {model.max_depth}")
    print(f"  Max leaf nodes per tree: {2**model.max_depth}")
    total_capacity = model.n_estimators * (2 ** model.max_depth)
    print(f"  Total capacity: {total_capacity:,} leaf nodes")

    if hasattr(model, "feature_importances_"):
        print("\n  Feature importances:")
        names = (list(model.feature_names_in_) if hasattr(model, "feature_names_in_")
                 else [f"f{i}" for i in range(model.n_features_in_)])
        for name, imp in sorted(zip(names, model.feature_importances_),
                                key=lambda x: -x[1]):
            print(f"    {name:>15s}: {imp:.4f}")


# ── 1J: Edge model analysis ─────────────────────────────────────────

def section_1j(trades):
    print(f"\n{SEP}")
    print("1J — EDGE MODEL ANALYSIS")
    print(SEP)
    if not EDGE_MODEL_PATH.exists():
        print("  ⚠️  edge_model.pkl not found")
        return

    import joblib
    model = joblib.load(EDGE_MODEL_PATH)
    print(f"  Type: {type(model).__name__}")
    print(f"  N features: {model.n_features_in_}")
    print(f"  N estimators: {model.n_estimators}")
    print(f"  Max depth: {model.max_depth}")
    total_capacity = model.n_estimators * (2 ** model.max_depth)
    print(f"  Total capacity: {total_capacity:,} leaf nodes")
    print(f"  Training samples: {len(trades)}")
    print(f"  ⚠️  Capacity/samples ratio: {total_capacity / max(len(trades), 1):.1f}x "
          f"({'SEVERE OVERFITTING RISK' if total_capacity > len(trades) * 5 else 'OK'})")

    if hasattr(model, "feature_importances_"):
        print("\n  Feature importances:")
        names = (list(model.feature_names_in_) if hasattr(model, "feature_names_in_")
                 else [f"f{i}" for i in range(model.n_features_in_)])
        for name, imp in sorted(zip(names, model.feature_importances_),
                                key=lambda x: -x[1]):
            print(f"    {name:>20s}: {imp:.4f}")

    # Win rate in training data
    if "won" in trades.columns:
        wr = trades["won"].mean()
        print(f"\n  Trade win rate: {wr:.1%} (n={len(trades)})")


# ── 1K: Predictor weights analysis ──────────────────────────────────

def section_1k():
    print(f"\n{SEP}")
    print("1K — PREDICTOR WEIGHTS (BIAS CORRECTIONS)")
    print(SEP)
    if not WEIGHTS_PATH.exists():
        print("  ⚠️  predictor_weights.json not found")
        return

    with open(WEIGHTS_PATH) as f:
        weights = json.load(f)

    meta = weights.get("meta", {})
    print(f"  Total samples: {meta.get('samples', '?'):,}")
    print(f"  Overall win rate: {meta.get('overall_wr', '?')}")

    all_biases = []
    for section in ["regime", "session", "volatility", "regime_session"]:
        data = weights.get(section, {})
        if not data:
            continue
        print(f"\n  {section.upper()} biases:")
        for key, biases in sorted(data.items()):
            vals = list(biases.values()) if isinstance(biases, dict) else [biases]
            all_biases.extend(vals)
            if isinstance(biases, dict):
                parts = [f"{d}={b:+.3f}" for d, b in biases.items()]
                print(f"    {key:>25s}: {', '.join(parts)}")
            else:
                print(f"    {key:>25s}: {biases:+.3f}")

    if all_biases:
        arr = np.array(all_biases)
        print(f"\n  Bias statistics:")
        print(f"    Mean: {arr.mean():+.4f}")
        print(f"    Std:  {arr.std():.4f}")
        print(f"    Min:  {arr.min():+.4f}")
        print(f"    Max:  {arr.max():+.4f}")
        print(f"    Positive biases: {(arr > 0).sum()}/{len(arr)}")
        print(f"    Negative biases: {(arr < 0).sum()}/{len(arr)}")
        print(f"    Near-zero (|b|<0.05): {(np.abs(arr) < 0.05).sum()}/{len(arr)}")


# ── 1L: Consolidated report ──────────────────────────────────────────

def section_1l(pred, trades):
    print(f"\n{SEP}")
    print("ML PREDICTOR DIAGNOSTIC REPORT")
    print(SEP)

    overall_acc = pred["correct"].mean()
    directional_correct = (
        ((pred["direction"] == "bullish") & (pred["high_hit"] == 1)) |
        ((pred["direction"] == "bearish") & (pred["low_hit"] == 1))
    )
    dir_acc = directional_correct.mean()
    both_pct = (pred["actual"] == "both").mean()

    print(f"\n📊 Predictions: {len(pred):,} graded")
    print(f"📊 3-class accuracy: {overall_acc:.1%} (baseline: 33.3%)")
    print(f"📊 Directional accuracy: {dir_acc:.1%} (bullish→high_hit, bearish→low_hit)")
    print(f'📊 "both" outcomes: {both_pct:.1%} (always graded wrong)')
    print(f"📊 Edge model: {len(trades)} training samples")

    print(f"\n🔴 CRITICAL ISSUES:")
    print(f"   1. 3-class accuracy ({overall_acc:.1%}) ≈ random chance (33.3%)")
    print(f'   2. "both" outcomes ({both_pct:.1%}) are unresolvable in 3-class grading')
    print(f"   3. Direction model uses only 5 of 100+ available features")
    if EDGE_MODEL_PATH.exists():
        import joblib
        em = joblib.load(EDGE_MODEL_PATH)
        cap = em.n_estimators * (2 ** em.max_depth)
        print(f"   4. Edge model: {cap:,} leaf nodes for {len(trades)} samples ({cap/max(len(trades),1):.0f}x overfit)")
    print(f"   5. Random train/test split on time-series data = lookahead bias")
    print(f"   6. Prediction horizon mismatch (30-bar train vs 10-min live)")

    # Confidence discrimination
    if "confidence" in pred.columns:
        high_conf = pred[pred["confidence"] >= pred["confidence"].quantile(0.75)]
        low_conf = pred[pred["confidence"] <= pred["confidence"].quantile(0.25)]
        print(f"\n📊 Confidence discrimination:")
        print(f"   Top-25% confidence accuracy: {high_conf['correct'].mean():.1%}")
        print(f"   Bottom-25% confidence accuracy: {low_conf['correct'].mean():.1%}")
        spread = high_conf["correct"].mean() - low_conf["correct"].mean()
        print(f"   Spread: {spread*100:+.1f}pp ({'NO EDGE' if abs(spread) < 0.02 else 'some signal'})")

    print(f"\n💡 RECOMMENDED REBUILD ORDER:")
    print(f"   Phase 2: Retrain direction model with 25+ features + time-series CV")
    print(f"   Phase 3: Switch to HistGradientBoosting + proper regularization")
    print(f"   Phase 4: Fix prediction horizon alignment")
    print(f"   Phase 5: Accumulate more trade data for edge model (need 2000+ samples)")


def main():
    predictions, trades = load_data()

    section_1a(predictions)
    section_1b(predictions)
    section_1c(predictions)
    section_1d(predictions)
    section_1e(predictions)
    section_1f(predictions)
    section_1g(predictions)
    section_1h(predictions)
    section_1i()
    section_1j(trades)
    section_1k()
    section_1l(predictions, trades)


if __name__ == "__main__":
    main()
