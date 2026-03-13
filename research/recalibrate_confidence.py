"""
Confidence Recalibration Analysis
Run: cd /home/asif420/qqqbot && python -m research.recalibrate_confidence
"""
import warnings

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

warnings.filterwarnings("ignore")

import sqlite3

DB_PATH = "data/analytics.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT confidence, correct, direction as predicted, actual, time as timestamp
        FROM predictions
        WHERE direction IN ('bullish', 'bearish') AND checked = 1
        ORDER BY time
    """, conn)
    conn.close()

    print(f"Loaded {len(df):,} directional predictions")
    print(f"Overall accuracy: {df['correct'].mean():.1%}")

    # ── Analysis 1: Confidence decile accuracy ───────────────
    df["conf_decile"] = pd.qcut(df["confidence"], 10, labels=False, duplicates="drop")
    decile_acc = df.groupby("conf_decile").agg(
        accuracy=("correct", "mean"),
        mean_conf=("confidence", "mean"),
        n=("correct", "count"),
    )
    print("\n=== CONFIDENCE DECILE ACCURACY ===")
    print(decile_acc.to_string())

    corr = df["confidence"].corr(df["correct"])
    print(f"\nPearson correlation (confidence <-> correct): {corr:.4f}")
    print(f"Direction: {'ANTI-CORRELATED' if corr < 0 else 'POSITIVE'}")

    # ── Analysis 2: High vs low confidence ───────────────────
    print("\n=== HIGH CONFIDENCE (>=0.65) ===")
    high = df[df["confidence"] >= 0.65]
    print(f"  N={len(high):,}, Accuracy={high['correct'].mean():.1%}")

    print("\n=== LOW CONFIDENCE (0.35-0.42) ===")
    low = df[(df["confidence"] >= 0.35) & (df["confidence"] <= 0.42)]
    print(f"  N={len(low):,}, Accuracy={low['correct'].mean():.1%}")

    # ── Analysis 3: Simple inversion test ────────────────────
    df["inverted"] = 1 - df["confidence"]
    inv_corr = df["inverted"].corr(df["correct"])
    print(f"\n=== INVERTED CONFIDENCE TEST ===")
    print(f"Correlation (inverted <-> correct): {inv_corr:.4f}")

    print(f"\n{'Threshold':<12} {'Accuracy':<10} {'Coverage':<10} {'N':<8}")
    print("-" * 40)
    for t in np.arange(0.50, 0.80, 0.05):
        mask = df["inverted"] >= t
        if mask.sum() < 100:
            continue
        acc = df.loc[mask, "correct"].mean()
        cov = mask.mean()
        print(f"{t:<12.2f} {acc:<10.1%} {cov:<10.1%} {mask.sum():<8}")

    # ── Analysis 4: Isotonic regression calibration ──────────
    split = int(len(df) * 0.8)
    train_df = df.iloc[:split]
    test_df = df.iloc[split:].copy()

    iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip")
    iso.fit(train_df["confidence"].values, train_df["correct"].values)

    test_df["calibrated"] = iso.predict(test_df["confidence"].values)

    cal_corr = test_df["calibrated"].corr(test_df["correct"])
    print(f"\n=== ISOTONIC CALIBRATION ===")
    print(f"Original correlation: {corr:.4f}")
    print(f"Calibrated correlation: {cal_corr:.4f}")
    print(f"Improvement: {'YES' if cal_corr > corr else 'NO'}")

    # Show what isotonic does to confidence values
    print("\n  Sample mappings (raw -> calibrated):")
    for raw_val in [0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
        cal_val = iso.predict([raw_val])[0]
        print(f"    {raw_val:.2f} -> {cal_val:.3f}")

    print(f"\n{'Cal Threshold':<14} {'Accuracy':<10} {'Coverage':<10} {'N':<8}")
    print("-" * 42)
    for t in np.arange(0.50, 0.80, 0.05):
        mask = test_df["calibrated"] >= t
        if mask.sum() < 50:
            continue
        acc = test_df.loc[mask, "correct"].mean()
        cov = mask.mean()
        print(f"{t:<14.2f} {acc:<10.1%} {cov:<10.1%} {mask.sum():<8}")

    # ── Recommendation ───────────────────────────────────────
    print(f"\n=== RECOMMENDATION ===")
    if inv_corr > 0.02:
        print("Option 1: INVERT confidence (1 - conf). Simple, effective.")
    if cal_corr > 0.02:
        print("Option 2: ISOTONIC calibration. More principled.")
    print("Option 3: Just use all predictions (no filtering). Overall 61.8% already strong.")

    # Save isotonic model
    if cal_corr > abs(corr) * 0.5:  # only if it improves things
        joblib.dump(iso, "data/isotonic_calibrator.pkl")
        print(f"\nSaved isotonic calibrator to data/isotonic_calibrator.pkl")
    else:
        print("\nIsotonic calibration did not improve enough. NOT saving.")

    # ── Final analysis: is filtering worth it? ───────────────
    print(f"\n=== IS FILTERING WORTH IT? ===")
    print(f"Overall accuracy (all predictions): {df['correct'].mean():.1%}")
    print(f"If we filter out hour 9 only: ", end="")
    # Parse hour from timestamp
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    no_h9 = df[df["hour"] != 9]
    print(f"{no_h9['correct'].mean():.1%} (n={len(no_h9):,})")
    h9_only = df[df["hour"] == 9]
    print(f"Hour 9 only: {h9_only['correct'].mean():.1%} (n={len(h9_only):,})")

    # Best simple strategy
    print(f"\nBest simple strategies:")
    print(f"  1. Use all predictions:        {df['correct'].mean():.1%}")
    print(f"  2. Skip hour 9:                {no_h9['correct'].mean():.1%}")
    print(f"  3. Skip conf >= 0.70:          {df[df['confidence'] < 0.70]['correct'].mean():.1%}")
    print(f"  4. Skip hour 9 + conf >= 0.70: {no_h9[no_h9['confidence'] < 0.70]['correct'].mean():.1%}")


if __name__ == "__main__":
    main()
