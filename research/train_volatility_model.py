"""
Train the trending-vs-range volatility filter model.
Reproduces the 67-73% accuracy from Phase 3 Experiment B.

Run: cd /home/asif420/qqqbot && python -m research.train_volatility_model
"""
import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pandas_ta as ta
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import TimeSeriesSplit

warnings.filterwarnings("ignore")

CSV_PATH = Path("data/qqq_1m.csv")
MODEL_PATH = Path("data/volatility_filter_model.pkl")

# Best parameters from Phase 3 Experiment B:
# Horizon 30, threshold >0.2% → 72.8% mean CV accuracy
HORIZON = 30
THRESHOLD_PCT = 0.2


def load_and_prepare():
    """Load SPY 1-min bars, filter to RTH."""
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower() for c in df.columns]
    if "timestamp" in df.columns:
        df = df.rename(columns={"timestamp": "time"})
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time").sort_index()

    rth_mask = (
        ((df.index.hour == 9) & (df.index.minute >= 30))
        | ((df.index.hour >= 10) & (df.index.hour < 16))
    )
    df = df[rth_mask]
    print(f"  RTH bars: {len(df):,}")
    return df


def compute_features(df):
    """26 features — same as train_ai_v2.py and ml_binary_experiments.py."""
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


def make_model():
    return HistGradientBoostingClassifier(
        max_iter=200, max_depth=4, learning_rate=0.05,
        min_samples_leaf=20, l2_regularization=1.0, max_bins=128,
        early_stopping=True, n_iter_no_change=20,
        validation_fraction=0.15, random_state=42,
    )


def main():
    print("=" * 60)
    print(f"VOLATILITY FILTER MODEL — Trending vs Range")
    print(f"Horizon: {HORIZON} bars, Threshold: >{THRESHOLD_PCT}%")
    print("=" * 60)

    df = load_and_prepare()
    features = compute_features(df)

    # Target: 1 if |move| > threshold, 0 if range-bound
    returns = (df["close"].shift(-HORIZON) - df["close"]) / df["close"]
    target = (returns.abs() > THRESHOLD_PCT / 100).astype(float)

    combined = features.copy()
    combined["target"] = target
    combined = combined.dropna()
    y = combined["target"].astype(int)
    X = combined.drop(columns=["target"])
    feature_columns = list(X.columns)

    print(f"\n  Samples: {len(X):,}")
    print(f"  Features: {len(feature_columns)}")
    trend_pct = y.mean()
    print(f"  Class balance: {trend_pct:.1%} trending, {1-trend_pct:.1%} range")

    # Walk-forward CV
    print(f"\n{'='*60}")
    print("WALK-FORWARD CV (5 folds)")
    print("=" * 60)
    tscv = TimeSeriesSplit(n_splits=5)
    fold_accs = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        model = make_model()
        model.fit(X.iloc[train_idx], y.iloc[train_idx])
        y_pred = model.predict(X.iloc[test_idx])
        acc = accuracy_score(y.iloc[test_idx], y_pred)
        fold_accs.append(acc)
        print(f"  Fold {fold}: {acc:.1%} (train={len(train_idx):,}, test={len(test_idx):,})")

    mean_acc = np.mean(fold_accs)
    print(f"\nMean CV accuracy: {mean_acc:.1%} ± {np.std(fold_accs):.1%}")

    if mean_acc < 0.60:
        print("FAIL: Accuracy below 60%. NOT saving.")
        return

    # Final model on all-but-last-30-days
    print(f"\n{'='*60}")
    print("FINAL MODEL (holdout = last 30 days)")
    print("=" * 60)
    timestamps = pd.Series(X.index, index=X.index)
    holdout_cutoff = timestamps.max() - pd.Timedelta(days=30)
    train_mask = timestamps < holdout_cutoff
    holdout_mask = timestamps >= holdout_cutoff

    X_train, y_train = X[train_mask], y[train_mask]
    X_holdout, y_holdout = X[holdout_mask], y[holdout_mask]

    print(f"  Train: {len(X_train):,}, Holdout: {len(X_holdout):,}")

    final_model = make_model()
    final_model.fit(X_train, y_train)

    y_pred_h = final_model.predict(X_holdout)
    holdout_acc = accuracy_score(y_holdout, y_pred_h)
    print(f"  Holdout accuracy: {holdout_acc:.1%}")

    print(f"\n  Classification report (holdout):")
    print(classification_report(y_holdout, y_pred_h, target_names=["range", "trending"], digits=3))

    # Probability threshold sweep
    y_prob_h = final_model.predict_proba(X_holdout)
    if y_prob_h.shape[1] > 1:
        p_trending = y_prob_h[:, 1]
    else:
        p_trending = y_prob_h[:, 0]

    print("  Probability threshold sweep:")
    for t in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
        pred_t = (p_trending >= t).astype(int)
        acc_t = accuracy_score(y_holdout, pred_t)
        cov_t = (p_trending >= t).mean()
        print(f"    P(trending) >= {t:.2f}: acc={acc_t:.1%}, coverage={cov_t:.1%}")

    # Save
    print(f"\n{'='*60}")
    print("SAVE")
    print("=" * 60)
    metadata = {
        "horizon": HORIZON,
        "threshold_pct": THRESHOLD_PCT,
        "features": feature_columns,
        "cv_accuracy": round(float(mean_acc), 4),
        "holdout_accuracy": round(float(holdout_acc), 4),
        "n_training_samples": int(len(X_train)),
        "model_type": "HistGradientBoostingClassifier",
        "class_labels": ["range", "trending"],
    }
    joblib.dump({"model": final_model, "metadata": metadata}, MODEL_PATH)
    print(f"  Saved to {MODEL_PATH}")
    print(f"  CV accuracy: {mean_acc:.1%}")
    print(f"  Holdout accuracy: {holdout_acc:.1%}")

    # Feature importance via permutation (quick, on holdout subset)
    from sklearn.inspection import permutation_importance
    n_samp = min(5000, len(X_holdout))
    result = permutation_importance(
        final_model, X_holdout.iloc[:n_samp], y_holdout.iloc[:n_samp],
        n_repeats=5, random_state=42, n_jobs=-1,
    )
    imp_df = pd.DataFrame({
        "feature": feature_columns,
        "importance": result.importances_mean,
    }).sort_values("importance", ascending=False)
    print(f"\n  Top 10 features:")
    for _, row in imp_df.head(10).iterrows():
        print(f"    {row['feature']:>20s}: {row['importance']:+.4f}")

    # Save metadata separately for easy reading
    meta_path = Path("data/volatility_filter_meta.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"\n  Metadata saved to {meta_path}")


if __name__ == "__main__":
    main()
