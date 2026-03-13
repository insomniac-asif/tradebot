"""
Phase 3 Binary ML Experiments — the missing tests.
Run AFTER Part A grading fix so correct field uses binary directional logic.
Run: cd /home/asif420/qqqbot && python -m research.ml_binary_experiments
"""
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_ta as ta
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import TimeSeriesSplit

warnings.filterwarnings("ignore")

CSV_PATH = Path("data/qqq_1m.csv")


def load_data():
    """Load SPY 1-min bars, filter to RTH, compute features."""
    print("Loading data...")
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower() for c in df.columns]
    if "timestamp" in df.columns:
        df = df.rename(columns={"timestamp": "time"})
    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time").sort_index()

    # RTH only
    rth_mask = (
        ((df.index.hour == 9) & (df.index.minute >= 30))
        | ((df.index.hour >= 10) & (df.index.hour < 16))
    )
    df = df[rth_mask]
    print(f"  RTH bars: {len(df):,}")
    return df


def compute_features(df):
    """Same 26 features as train_ai_v2.py."""
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


def run_cv(X, y, n_splits=5):
    """Run time-series CV, return list of fold accuracies."""
    tscv = TimeSeriesSplit(n_splits=n_splits)
    accs = []
    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        model = make_model()
        model.fit(X.iloc[train_idx], y.iloc[train_idx])
        y_pred = model.predict(X.iloc[test_idx])
        acc = accuracy_score(y.iloc[test_idx], y_pred)
        accs.append(acc)
        print(f"    Fold {fold}: {acc:.1%} (train={len(train_idx):,}, test={len(test_idx):,})")
    return accs


# ── Experiment A: Bullish vs Not-Bullish ─────────────────────────────

def experiment_a(df, X):
    print("\n" + "=" * 60)
    print("EXPERIMENT A: Bullish vs Not-Bullish")
    print("  Target: 1 if close rises, 0 otherwise")
    print("=" * 60)

    results = {}
    for horizon in [10, 30, 60]:
        target = (df["close"].shift(-horizon) > df["close"]).astype(float)
        combined = X.copy()
        combined["target"] = target
        combined = combined.dropna()
        y = combined["target"].astype(int)
        X_exp = combined.drop(columns=["target"])

        print(f"\n  --- Horizon: {horizon} bars ---")
        print(f"  Class balance: {y.mean():.1%} bullish, {1-y.mean():.1%} not-bullish")
        print(f"  Samples: {len(X_exp):,}")

        accs = run_cv(X_exp, y)
        mean_acc = np.mean(accs)
        std_acc = np.std(accs)
        verdict = "ABOVE 50%" if mean_acc > 0.50 else "AT OR BELOW 50%"
        print(f"    MEAN: {mean_acc:.1%} ± {std_acc:.1%}  → {verdict}")
        results[horizon] = mean_acc
    return results


# ── Experiment B: Trending vs Range ──────────────────────────────────

def experiment_b(df, X):
    print("\n" + "=" * 60)
    print("EXPERIMENT B: Trending vs Range")
    print("  Target: 1 if |move| > threshold, 0 otherwise")
    print("=" * 60)

    results = {}
    for horizon in [10, 30, 60]:
        for threshold_pct in [0.1, 0.2, 0.3]:
            returns = (df["close"].shift(-horizon) - df["close"]) / df["close"]
            target = (returns.abs() > threshold_pct / 100).astype(float)
            combined = X.copy()
            combined["target"] = target
            combined = combined.dropna()
            y = combined["target"].astype(int)
            X_exp = combined.drop(columns=["target"])

            trend_pct = y.mean()
            if trend_pct < 0.20 or trend_pct > 0.80:
                print(f"\n  --- Horizon {horizon}, threshold >{threshold_pct}% ---")
                print(f"    SKIPPED: class balance too extreme ({trend_pct:.1%} trending)")
                continue

            print(f"\n  --- Horizon {horizon}, threshold >{threshold_pct}% ---")
            print(f"  Class balance: {trend_pct:.1%} trending, {1-trend_pct:.1%} range")

            accs = run_cv(X_exp, y)
            mean_acc = np.mean(accs)
            std_acc = np.std(accs)
            verdict = "ABOVE 50%" if mean_acc > 0.50 else "AT OR BELOW 50%"
            print(f"    MEAN: {mean_acc:.1%} ± {std_acc:.1%}  → {verdict}")
            results[(horizon, threshold_pct)] = mean_acc
    return results


# ── Experiment C: Systematic Abstention ──────────────────────────────

def experiment_c(df, X):
    print("\n" + "=" * 60)
    print("EXPERIMENT C: Prediction with Abstention")
    print("  Train on first 80%, predict on last 20%")
    print("  Sweep confidence thresholds for accuracy/coverage tradeoff")
    print("=" * 60)

    horizon = 30
    target = (df["close"].shift(-horizon) > df["close"]).astype(float)
    combined = X.copy()
    combined["target"] = target
    combined = combined.dropna()
    y = combined["target"].astype(int)
    X_all = combined.drop(columns=["target"])

    split_idx = int(len(X_all) * 0.8)
    X_train, X_test = X_all.iloc[:split_idx], X_all.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print(f"  Train: {len(X_train):,}, Test: {len(X_test):,}")

    model = make_model()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)
    max_probs = y_prob.max(axis=1)

    print(f"\n  {'Threshold':<12} {'Accuracy':<10} {'Coverage':<10} {'N preds':<10} {'Edge vs 50%':<12}")
    print("  " + "-" * 54)

    results = []
    for threshold in np.arange(0.50, 0.80, 0.01):
        mask = max_probs >= threshold
        n_preds = mask.sum()
        if n_preds < 50:
            continue
        acc = accuracy_score(y_test.values[mask], y_pred[mask])
        coverage = mask.mean()
        edge = acc - 0.50
        print(f"  {threshold:<12.2f} {acc:<10.1%} {coverage:<10.1%} {n_preds:<10} {edge:<12.1%}")
        results.append({
            "threshold": threshold, "accuracy": acc,
            "coverage": coverage, "n": n_preds, "edge": edge,
        })

    if not results:
        print("  No viable thresholds found.")
        return {}

    rdf = pd.DataFrame(results)

    for min_cov, label in [(0.20, "≥20%"), (0.40, "≥40%"), (0.60, "≥60%")]:
        viable = rdf[rdf["coverage"] >= min_cov]
        if len(viable) > 0:
            best = viable.loc[viable["accuracy"].idxmax()]
            print(f"\n  🎯 BEST ({label} coverage): threshold={best['threshold']:.2f}, "
                  f"accuracy={best['accuracy']:.1%}, coverage={best['coverage']:.1%} "
                  f"(n={best['n']:.0f})")

    return results


def main():
    df = load_data()
    features = compute_features(df)
    X = features.dropna()
    # Align df to match X's index
    df = df.loc[X.index]
    print(f"  Clean samples: {len(X):,}, Features: {len(X.columns)}")

    results_a = experiment_a(df, X)
    results_b = experiment_b(df, X)
    results_c = experiment_c(df, X)

    # Summary
    print("\n" + "=" * 60)
    print("PHASE 3 EXPERIMENT SUMMARY")
    print("=" * 60)
    print(f"\n  Experiment A (Bullish vs Not-Bullish):")
    for h, acc in results_a.items():
        print(f"    Horizon {h:>3d}: {acc:.1%}")
    print(f"\n  Experiment B (Trending vs Range):")
    for (h, t), acc in results_b.items():
        print(f"    Horizon {h:>3d}, threshold >{t}%: {acc:.1%}")
    if results_c:
        rdf = pd.DataFrame(results_c)
        best_all = rdf.loc[rdf["accuracy"].idxmax()]
        print(f"\n  Experiment C (Abstention):")
        print(f"    Best overall: threshold={best_all['threshold']:.2f}, "
              f"accuracy={best_all['accuracy']:.1%}, coverage={best_all['coverage']:.1%}")

    print("\n  Baseline: 50% (random binary)")
    print("  Conclusion: see results above for which framing (if any) breaks 50%")


if __name__ == "__main__":
    main()
