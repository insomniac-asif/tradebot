"""
analytics/predictor_optimizer.py

Analyzes historical graded predictions to learn regime/session/volatility
bias corrections for the predictor.  Saves learned weights to
data/predictor_weights.json which signals/predictor.py loads at runtime.

How it works
------------
For each context slice (regime, session, volatility) and each predicted
direction (bullish / bearish / range), we compute a Laplace-smoothed
win rate.  The win rate is compared against the random-chance baseline
(1/2 for binary directional) and converted into an additive pre-softmax
score adjustment:

    bias = (smoothed_wr - BASELINE) * SCALE

A positive bias boosts that direction in that context; negative penalises it.
The bias is applied BEFORE the softmax temperature scaling, so it shifts the
probability distribution rather than just clamping confidence.

Usage
-----
    python analytics/predictor_optimizer.py          # print report & save
    python analytics/predictor_optimizer.py --dry-run  # print only
    from analytics.predictor_optimizer import update_predictor_weights
    update_predictor_weights()                       # call from grader/backfill
"""

import argparse
import json
import os

import pandas as pd

from core.paths import DATA_DIR

WEIGHTS_FILE = os.path.join(DATA_DIR, "predictor_weights.json")
PRED_FILE    = os.path.join(DATA_DIR, "predictions.csv")  # legacy constant

# ── tuning knobs ─────────────────────────────────────────────────────────────
BASELINE        = 1 / 2          # random-chance win rate for binary directional
PRIOR_COUNT     = 8              # Laplace smoothing: equivalent to 8 balanced samples
SCALE_REGIME    = 1.2            # pre-softmax bias strength for regime signal
SCALE_SESSION   = 0.8            # pre-softmax bias strength for session signal
SCALE_VOLATILITY = 0.6          # pre-softmax bias strength for volatility signal
SCALE_COMBO     = 1.4            # regime × session combined (more specific → higher weight)
MIN_SAMPLES     = 15             # ignore slices with fewer than this many samples
MIN_SAMPLES_COMBO = 25           # higher threshold for combined slice


# ── core math ────────────────────────────────────────────────────────────────

def _bias(wins: int, total: int, scale: float) -> float:
    """Laplace-smoothed win-rate converted to a pre-softmax bias score."""
    smoothed_wr = (wins + PRIOR_COUNT * BASELINE) / (total + PRIOR_COUNT)
    return round((smoothed_wr - BASELINE) * scale * 3.0, 4)
    # × 3 stretches the signal: WR=0.55 → ~0.24 bias, WR=0.175 → ~-0.14 bias


def _slice_biases(group: pd.DataFrame, scale: float) -> dict:
    """Compute {direction: bias} for a prediction slice."""
    out = {}
    for direction, dg in group.groupby("direction"):
        n    = len(dg)
        wins = int(dg["correct"].sum())
        out[direction] = _bias(wins, n, scale)
    return out


# ── main weight computation ───────────────────────────────────────────────────

def compute_weights(df: pd.DataFrame) -> dict:
    """
    Compute full bias weight dict from a graded predictions dataframe.
    Returns dict ready to JSON-serialize.
    """
    required = {"direction", "correct", "regime", "session", "volatility"}
    if not required.issubset(df.columns):
        return {}

    df = df[df["correct"].notna() & df["direction"].notna()].copy()
    df["correct"] = pd.to_numeric(df["correct"], errors="coerce").fillna(0)

    if len(df) < MIN_SAMPLES:
        return {}

    weights: dict = {
        "meta": {
            "samples":    int(len(df)),
            "overall_wr": round(float(df["correct"].mean()), 4),
        },
        "regime":      {},
        "session":     {},
        "volatility":  {},
        "regime_session": {},
    }

    # Per-regime
    for ctx, grp in df.groupby("regime"):
        if len(grp) < MIN_SAMPLES:
            continue
        weights["regime"][ctx] = _slice_biases(grp, SCALE_REGIME)

    # Per-session
    for ctx, grp in df.groupby("session"):
        if len(grp) < MIN_SAMPLES:
            continue
        weights["session"][ctx] = _slice_biases(grp, SCALE_SESSION)

    # Per-volatility
    for ctx, grp in df.groupby("volatility"):
        if len(grp) < MIN_SAMPLES:
            continue
        weights["volatility"][ctx] = _slice_biases(grp, SCALE_VOLATILITY)

    # Combined regime × session (most specific — highest weight)
    for (regime, session), grp in df.groupby(["regime", "session"]):
        if len(grp) < MIN_SAMPLES_COMBO:
            continue
        key = f"{regime}_{session}"
        weights["regime_session"][key] = _slice_biases(grp, SCALE_COMBO)

    return weights


def update_predictor_weights(dry_run: bool = False) -> dict:
    """
    Load predictions from SQLite, recompute weights, optionally save to JSON.
    Returns the weights dict (empty dict on failure).
    """
    try:
        from core.analytics_db import read_df
        df = read_df("SELECT * FROM predictions WHERE checked = 1")
    except Exception:
        return {}

    if df.empty:
        return {}

    weights = compute_weights(df)

    if not weights:
        return {}

    if not dry_run:
        try:
            with open(WEIGHTS_FILE, "w") as f:
                json.dump(weights, f, indent=2)
        except Exception:
            pass

    return weights


# ── CLI report ───────────────────────────────────────────────────────────────

def _fmt_bias(b: float) -> str:
    sign = "+" if b >= 0 else ""
    return f"{sign}{b:.3f}"


def _print_report(weights: dict) -> None:
    meta = weights.get("meta", {})
    print(f"\nTraining samples : {meta.get('samples', '?')}")
    print(f"Overall win rate : {meta.get('overall_wr', '?'):.1%}")
    print(f"Baseline (random): {BASELINE:.1%}")

    sections = [
        ("Regime biases",          "regime"),
        ("Session biases",         "session"),
        ("Volatility biases",      "volatility"),
        ("Regime × Session biases","regime_session"),
    ]
    for title, key in sections:
        data = weights.get(key, {})
        if not data:
            continue
        print(f"\n── {title}:")
        for ctx, biases in sorted(data.items()):
            parts = "  ".join(f"{d}: {_fmt_bias(b)}" for d, b in sorted(biases.items()))
            print(f"   {ctx:<28s} {parts}")


def main():
    parser = argparse.ArgumentParser(description="Compute predictor bias weights from historical predictions")
    parser.add_argument("--dry-run", action="store_true", help="Print weights without saving")
    args = parser.parse_args()

    weights = update_predictor_weights(dry_run=args.dry_run)

    if not weights:
        print("Not enough graded predictions to compute weights.")
        return

    _print_report(weights)

    if args.dry_run:
        print("\n[dry-run] weights NOT saved.")
    else:
        print(f"\nWeights saved → {WEIGHTS_FILE}")


if __name__ == "__main__":
    main()
