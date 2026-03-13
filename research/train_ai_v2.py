"""
ML Direction Model v2 — Binary classifier with time-series CV
Run: cd /home/asif420/qqqbot && python -m research.train_ai_v2

Key insight from Phase 1 diagnostic:
  - 3-class grading was broken ("both" outcomes = 29%, always wrong)
  - Directional accuracy already 61.8% with current 5-feature model
  - This rebuild: binary (up/down), 25+ features, time-series CV, HistGBM

Saves to data/direction_model_v2.pkl — does NOT overwrite production model.
"""
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

DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "qqq_1m.csv"
OUTPUT_PATH = DATA_DIR / "direction_model_v2.pkl"

# ── Feature engineering ──────────────────────────────────────────────

def load_and_prepare_data():
    """Load SPY 1-min bars and compute features."""
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)
    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    if "timestamp" in df.columns:
        df = df.rename(columns={"timestamp": "time"})
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index("time").sort_index()
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()

    print(f"  Raw bars: {len(df):,}  ({df.index.min()} to {df.index.max()})")

    # Filter to RTH only (9:30-16:00 ET)
    if hasattr(df.index, "hour"):
        rth_mask = (
            ((df.index.hour == 9) & (df.index.minute >= 30)) |
            ((df.index.hour >= 10) & (df.index.hour < 16))
        )
        df = df[rth_mask]
        print(f"  RTH bars: {len(df):,}")

    return df


def compute_features(df):
    """Compute 25+ features from OHLCV bars."""
    print("Computing features...")
    feat = pd.DataFrame(index=df.index)

    # ── Existing features (from current model) ───────────────────
    feat["ema9"] = ta.ema(df["close"], length=9)
    feat["ema20"] = ta.ema(df["close"], length=20)
    feat["rsi"] = ta.rsi(df["close"], length=14)
    feat["volume"] = df["volume"]

    # VWAP (session-based) — approximate with rolling if no session boundaries
    if "vwap" in df.columns:
        feat["vwap"] = df["vwap"]
    else:
        # Rolling VWAP proxy (30-bar)
        tp = (df["high"] + df["low"] + df["close"]) / 3
        feat["vwap"] = (tp * df["volume"]).rolling(30).sum() / df["volume"].rolling(30).sum()

    # ── Relative position features ───────────────────────────────
    feat["price_vs_vwap"] = (df["close"] - feat["vwap"]) / feat["vwap"].clip(lower=0.01)
    feat["price_vs_ema9"] = (df["close"] - feat["ema9"]) / feat["ema9"].clip(lower=0.01)
    feat["price_vs_ema20"] = (df["close"] - feat["ema20"]) / feat["ema20"].clip(lower=0.01)
    feat["ema_spread"] = (feat["ema9"] - feat["ema20"]) / feat["ema20"].clip(lower=0.01)

    # ── Rolling returns ──────────────────────────────────────────
    feat["returns_5m"] = df["close"].pct_change(5)
    feat["returns_15m"] = df["close"].pct_change(15)
    feat["returns_30m"] = df["close"].pct_change(30)

    # ── Volatility features ──────────────────────────────────────
    pct_chg = df["close"].pct_change()
    feat["volatility_15m"] = pct_chg.rolling(15).std()
    feat["volatility_60m"] = pct_chg.rolling(60).std()
    feat["vol_ratio"] = feat["volatility_15m"] / feat["volatility_60m"].clip(lower=1e-8)

    # ATR
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    feat["atr"] = tr.rolling(14).mean()
    atr_sma = feat["atr"].rolling(30).mean()
    feat["atr_expansion"] = feat["atr"] / atr_sma.clip(lower=1e-8)

    # ── Volume context ───────────────────────────────────────────
    vol_mean = df["volume"].rolling(60).mean()
    vol_std = df["volume"].rolling(60).std().clip(lower=1e-8)
    feat["volume_zscore"] = (df["volume"] - vol_mean) / vol_std

    # ── Momentum features ────────────────────────────────────────
    feat["rsi_momentum"] = feat["rsi"].diff(5)
    feat["price_momentum"] = df["close"].pct_change(10)

    # ── Time features (cyclical encoding) ────────────────────────
    if hasattr(df.index, "hour"):
        minutes_into_session = (df.index.hour - 9) * 60 + df.index.minute - 30
        minutes_into_session = np.clip(minutes_into_session, 0, 390)  # 6.5 hours = 390 min
        feat["time_sin"] = np.sin(2 * np.pi * minutes_into_session / 390)
        feat["time_cos"] = np.cos(2 * np.pi * minutes_into_session / 390)
    else:
        feat["time_sin"] = 0.0
        feat["time_cos"] = 0.0

    # ── Candlestick features ─────────────────────────────────────
    body = df["close"] - df["open"]
    total_range = (df["high"] - df["low"]).clip(lower=1e-8)
    feat["body_ratio"] = body / total_range  # +1 = full bull candle, -1 = full bear
    feat["upper_wick_ratio"] = (df["high"] - df[["open", "close"]].max(axis=1)) / total_range
    feat["lower_wick_ratio"] = (df[["open", "close"]].min(axis=1) - df["low"]) / total_range

    # ── High/low position ────────────────────────────────────────
    rolling_high = df["high"].rolling(30).max()
    rolling_low = df["low"].rolling(30).min()
    hl_range = (rolling_high - rolling_low).clip(lower=1e-8)
    feat["price_position"] = (df["close"] - rolling_low) / hl_range  # 0=at low, 1=at high

    print(f"  Features computed: {len(feat.columns)}")
    return feat


def compute_target(df, horizon=30):
    """Binary target: 1 if close rises over next `horizon` bars, 0 otherwise."""
    future_close = df["close"].shift(-horizon)
    target = (future_close > df["close"]).astype(int)
    return target


# ── Training ─────────────────────────────────────────────────────────

def train_with_cv(X, y, timestamps):
    """Walk-forward time-series cross-validation."""
    print("\n" + "=" * 70)
    print("WALK-FORWARD CROSS-VALIDATION (TimeSeriesSplit, 5 folds)")
    print("=" * 70)

    tscv = TimeSeriesSplit(n_splits=5)
    fold_results = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        model = HistGradientBoostingClassifier(
            max_iter=200,
            max_depth=4,
            learning_rate=0.05,
            min_samples_leaf=20,
            l2_regularization=1.0,
            max_bins=128,
            early_stopping=True,
            n_iter_no_change=20,
            validation_fraction=0.15,
            random_state=42,
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)
        acc = accuracy_score(y_test, y_pred)

        # Per-class accuracy
        for cls in sorted(y_test.unique()):
            mask = y_test == cls
            cls_acc = (y_pred[mask] == y_test[mask]).mean()
            cls_label = "UP" if cls == 1 else "DOWN"
            n_cls = mask.sum()
            pct = n_cls / len(y_test) * 100
            print(f"  Fold {fold} | {cls_label}: {cls_acc:.1%} (n={n_cls:,}, {pct:.0f}%)")

        # Confidence analysis
        max_prob = y_prob.max(axis=1)
        high_conf_mask = max_prob >= 0.55
        if high_conf_mask.sum() > 0:
            high_conf_acc = (y_pred[high_conf_mask] == y_test.values[high_conf_mask]).mean()
            coverage = high_conf_mask.mean()
            print(f"  Fold {fold} | Confident (≥0.55): {high_conf_acc:.1%} "
                  f"(coverage={coverage:.0%}, n={high_conf_mask.sum():,})")

        train_dates = f"{timestamps.iloc[train_idx[0]].date()} to {timestamps.iloc[train_idx[-1]].date()}"
        test_dates = f"{timestamps.iloc[test_idx[0]].date()} to {timestamps.iloc[test_idx[-1]].date()}"

        fold_results.append({
            "fold": fold,
            "train_size": len(train_idx),
            "test_size": len(test_idx),
            "accuracy": acc,
            "train_dates": train_dates,
            "test_dates": test_dates,
        })
        print(f"  Fold {fold}: train={len(train_idx):,} ({train_dates}), "
              f"test={len(test_idx):,} ({test_dates}), accuracy={acc:.1%}")
        print()

    mean_acc = np.mean([r["accuracy"] for r in fold_results])
    std_acc = np.std([r["accuracy"] for r in fold_results])
    print(f"Mean CV accuracy: {mean_acc:.1%} ± {std_acc:.1%}")
    return fold_results, mean_acc


def train_final_model(X, y, timestamps):
    """Train final model on all data except last 30 days as holdout."""
    print("\n" + "=" * 70)
    print("FINAL MODEL TRAINING (holdout = last 30 days)")
    print("=" * 70)

    holdout_cutoff = timestamps.max() - pd.Timedelta(days=30)
    train_mask = timestamps < holdout_cutoff
    holdout_mask = timestamps >= holdout_cutoff

    X_train = X[train_mask]
    y_train = y[train_mask]
    X_holdout = X[holdout_mask]
    y_holdout = y[holdout_mask]

    print(f"  Train: {len(X_train):,} bars (up to {holdout_cutoff.date()})")
    print(f"  Holdout: {len(X_holdout):,} bars (last 30 days)")

    model = HistGradientBoostingClassifier(
        max_iter=200,
        max_depth=4,
        learning_rate=0.05,
        min_samples_leaf=20,
        l2_regularization=1.0,
        max_bins=128,
        early_stopping=True,
        n_iter_no_change=20,
        validation_fraction=0.15,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_holdout)
    holdout_acc = accuracy_score(y_holdout, y_pred)

    print(f"\n  Holdout accuracy: {holdout_acc:.1%}")
    print(f"  Gate (must be > 50%): {'PASS' if holdout_acc > 0.50 else 'FAIL'}")

    # Detailed holdout report
    print(f"\n  Holdout classification report:")
    print(classification_report(
        y_holdout, y_pred,
        target_names=["DOWN", "UP"],
        digits=3,
    ))

    # Confidence sweep on holdout
    y_prob = model.predict_proba(X_holdout)
    max_prob = y_prob.max(axis=1)
    print("  Confidence threshold sweep (holdout):")
    for threshold in [0.50, 0.52, 0.55, 0.58, 0.60, 0.65]:
        mask = max_prob >= threshold
        if mask.sum() < 50:
            continue
        t_acc = (y_pred[mask] == y_holdout.values[mask]).mean()
        coverage = mask.mean()
        print(f"    ≥{threshold:.2f}: {t_acc:.1%} accuracy, "
              f"{coverage:.0%} coverage (n={mask.sum():,})")

    return model, holdout_acc


def print_feature_importance(model, feature_names, X_test, y_test):
    """Print feature importance via permutation importance."""
    from sklearn.inspection import permutation_importance

    print("\n" + "=" * 70)
    print("FEATURE IMPORTANCE (permutation, 5 repeats)")
    print("=" * 70)

    # Use a subsample for speed (max 5000 rows)
    n = min(5000, len(X_test))
    result = permutation_importance(
        model, X_test.iloc[:n], y_test.iloc[:n],
        n_repeats=5, random_state=42, n_jobs=-1,
    )

    imp_df = pd.DataFrame({
        "feature": feature_names,
        "importance_mean": result.importances_mean,
        "importance_std": result.importances_std,
    }).sort_values("importance_mean", ascending=False)

    print("\n  TOP 15 FEATURES (higher = more important):")
    for _, row in imp_df.head(15).iterrows():
        bar_len = max(0, int(row["importance_mean"] / max(imp_df["importance_mean"].max(), 1e-8) * 30))
        bar = "█" * bar_len
        print(f"    {row['feature']:>20s}: {row['importance_mean']:+.4f} ±{row['importance_std']:.4f}  {bar}")

    print("\n  BOTTOM 10 FEATURES (candidates for removal):")
    for _, row in imp_df.tail(10).iterrows():
        print(f"    {row['feature']:>20s}: {row['importance_mean']:+.4f} ±{row['importance_std']:.4f}")


def experiment_magnitude_filtered(df, features, timestamps):
    """Try predicting only significant moves (>0.1% or >0.2%)."""
    print("\n" + "=" * 70)
    print("EXPERIMENT: MAGNITUDE-FILTERED TARGETS")
    print("=" * 70)

    future_close = df["close"].shift(-30)
    returns = (future_close - df["close"]) / df["close"]

    for threshold in [0.001, 0.002, 0.003]:
        # Only keep bars where the future move exceeds threshold
        sig_mask = returns.abs() > threshold
        sig_returns = returns[sig_mask]
        sig_target = (sig_returns > 0).astype(int)

        # Align with features
        common_idx = features.index.intersection(sig_target.dropna().index)
        X_sig = features.loc[common_idx]
        y_sig = sig_target.loc[common_idx]
        ts_sig = pd.Series(common_idx, index=common_idx)

        pct_kept = len(X_sig) / len(features) * 100
        up_pct = y_sig.mean() * 100

        if len(X_sig) < 10000:
            print(f"\n  Threshold >{threshold:.1%}: too few samples ({len(X_sig):,}), skipping")
            continue

        print(f"\n  Threshold >{threshold:.1%}: {len(X_sig):,} samples "
              f"({pct_kept:.0f}% of data), UP={up_pct:.0f}%/DOWN={100-up_pct:.0f}%")

        # Quick 3-fold CV
        tscv = TimeSeriesSplit(n_splits=3)
        accs = []
        for fold, (train_idx, test_idx) in enumerate(tscv.split(X_sig)):
            model = HistGradientBoostingClassifier(
                max_iter=200, max_depth=4, learning_rate=0.05,
                min_samples_leaf=20, l2_regularization=1.0,
                early_stopping=True, n_iter_no_change=20,
                validation_fraction=0.15, random_state=42,
            )
            model.fit(X_sig.iloc[train_idx], y_sig.iloc[train_idx])
            y_pred = model.predict(X_sig.iloc[test_idx])
            acc = (y_pred == y_sig.iloc[test_idx]).mean()
            accs.append(acc)
            print(f"    Fold {fold}: {acc:.1%}")

        print(f"    Mean: {np.mean(accs):.1%} ± {np.std(accs):.1%}")


def experiment_horizons(df, features, timestamps):
    """Try different prediction horizons."""
    print("\n" + "=" * 70)
    print("EXPERIMENT: DIFFERENT PREDICTION HORIZONS")
    print("=" * 70)

    for horizon in [10, 20, 30, 60, 120]:
        target = compute_target(df, horizon=horizon)
        common_idx = features.index.intersection(target.dropna().index)
        X_h = features.loc[common_idx]
        y_h = target.loc[common_idx]

        if len(X_h) < 10000:
            continue

        # Quick 3-fold CV
        tscv = TimeSeriesSplit(n_splits=3)
        accs = []
        for fold, (train_idx, test_idx) in enumerate(tscv.split(X_h)):
            model = HistGradientBoostingClassifier(
                max_iter=200, max_depth=4, learning_rate=0.05,
                min_samples_leaf=20, l2_regularization=1.0,
                early_stopping=True, n_iter_no_change=20,
                validation_fraction=0.15, random_state=42,
            )
            model.fit(X_h.iloc[train_idx], y_h.iloc[train_idx])
            y_pred = model.predict(X_h.iloc[test_idx])
            acc = (y_pred == y_h.iloc[test_idx]).mean()
            accs.append(acc)

        mean_acc = np.mean(accs)
        print(f"  Horizon {horizon:>3d} bars: {mean_acc:.1%} ± {np.std(accs):.1%}  "
              f"(UP={y_h.mean():.1%})")


def main():
    print("=" * 70)
    print("ML DIRECTION MODEL v2 — Binary (UP/DOWN)")
    print("HistGradientBoostingClassifier + TimeSeriesSplit")
    print("=" * 70)

    # Load data
    df = load_and_prepare_data()

    # Compute features and target
    features = compute_features(df)
    target = compute_target(df, horizon=30)

    # Align and drop NaN
    combined = features.copy()
    combined["target"] = target
    combined = combined.dropna()
    print(f"\n  Clean samples after dropping NaN: {len(combined):,}")

    X = combined.drop(columns=["target"])
    y = combined["target"]
    timestamps = pd.Series(combined.index, index=combined.index)

    feature_names = list(X.columns)
    print(f"  Features: {len(feature_names)}")
    print(f"  Target distribution: UP={y.mean():.1%}, DOWN={1-y.mean():.1%}")

    # Cross-validation
    fold_results, mean_cv_acc = train_with_cv(X, y, timestamps)

    # Final model with holdout
    model, holdout_acc = train_final_model(X, y, timestamps)

    # Feature importance (on holdout subset)
    holdout_cutoff = timestamps.max() - pd.Timedelta(days=30)
    holdout_mask = timestamps >= holdout_cutoff
    print_feature_importance(model, feature_names, X[holdout_mask], y[holdout_mask])

    # ── Experiments ──────────────────────────────────────────────
    experiment_horizons(df, X, timestamps)
    experiment_magnitude_filtered(df, X, timestamps)

    # ── Save decision ────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("SAVE DECISION")
    print("=" * 70)
    if holdout_acc > 0.50:
        joblib.dump(model, OUTPUT_PATH)
        print(f"  Model saved to {OUTPUT_PATH}")
        print(f"  Holdout accuracy: {holdout_acc:.1%}")
        print(f"  Mean CV accuracy: {mean_cv_acc:.1%}")
        print(f"  Features: {len(feature_names)}")

        # Also save feature list for inference
        import json
        meta = {
            "features": feature_names,
            "holdout_accuracy": round(holdout_acc, 4),
            "cv_accuracy": round(mean_cv_acc, 4),
            "n_features": len(feature_names),
            "horizon": 30,
            "model_type": "HistGradientBoostingClassifier",
            "target": "binary_up_down",
        }
        meta_path = DATA_DIR / "direction_model_v2_meta.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"  Metadata saved to {meta_path}")
    else:
        print(f"  Model did NOT pass holdout test ({holdout_acc:.1%} <= 50%)")
        print(f"  NOT saving. Review diagnostic output.")


if __name__ == "__main__":
    main()
