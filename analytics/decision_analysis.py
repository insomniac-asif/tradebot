import pandas as pd
from typing import cast


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def analyze_decision_quality(trade_log):
    rows = []

    for trade in trade_log or []:
        snapshot = trade.get("decision_snapshot")
        if not isinstance(snapshot, dict):
            continue

        r_val = trade.get("R")
        if r_val is None:
            continue

        row = {
            "R": _safe_float(r_val),
            "blended_score": _safe_float(snapshot.get("blended_score")),
            "threshold": _safe_float(snapshot.get("threshold")),
            "threshold_delta": _safe_float(snapshot.get("threshold_delta")),
            "ml_weight": _safe_float(snapshot.get("ml_weight")),
            "regime_samples": _safe_float(snapshot.get("regime_samples")),
            "expectancy_samples": _safe_float(snapshot.get("expectancy_samples")),
            "dual_alignment": bool(snapshot.get("dual_alignment")) if snapshot.get("dual_alignment") is not None else None,
            "confidence_15m": _safe_float(snapshot.get("confidence_15m")),
            "confidence_60m": _safe_float(snapshot.get("confidence_60m")),
            "spread_pct": None,
            "slippage_pct": None,
            "win": None,
        }

        bid = _safe_float(trade.get("bid"))
        ask = _safe_float(trade.get("ask"))
        if bid is not None and ask is not None and ask > 0 and ask >= bid:
            row["spread_pct"] = (ask - bid) / ask

        expected_mid = _safe_float(trade.get("expected_mid"))
        if expected_mid is None and bid is not None and ask is not None:
            expected_mid = (bid + ask) / 2

        fill_price = _safe_float(trade.get("entry_price"))
        if fill_price is None:
            fill_price = _safe_float(trade.get("fill_price"))

        if expected_mid is not None and fill_price is not None and expected_mid > 0:
            row["slippage_pct"] = (fill_price - expected_mid) / expected_mid

        result = trade.get("result")
        if result is not None:
            row["win"] = str(result).lower() == "win"
        else:
            row["win"] = bool(row["R"] is not None and row["R"] > 0)

        rows.append(row)

    if not rows:
        return {
            "total_trades_analyzed": 0,
            "corr_threshold_delta_vs_R": None,
            "corr_blended_vs_R": None,
            "avg_spread_at_entry": None,
            "avg_slippage_vs_mid": None,
            "winrate_by_delta_quartile": {},
            "winrate_by_ml_weight_quartile": {},
            "winrate_by_spread_quartile": {},
            "regime_maturity_comparison": {},
        }

    df: pd.DataFrame = pd.DataFrame(rows)
    df = df.dropna(subset=["R"])

    if df.empty:
        return {
            "total_trades_analyzed": 0,
            "corr_threshold_delta_vs_R": None,
            "corr_blended_vs_R": None,
            "avg_spread_at_entry": None,
            "avg_slippage_vs_mid": None,
            "winrate_by_delta_quartile": {},
            "winrate_by_ml_weight_quartile": {},
            "winrate_by_spread_quartile": {},
            "regime_maturity_comparison": {},
        }

    def _safe_corr(x_col, y_col):
        sub = df[[x_col, y_col]].dropna()
        if len(sub) < 2:
            return None
        x_series = cast(pd.Series, sub[x_col])
        y_series = cast(pd.Series, sub[y_col])
        val = x_series.corr(y_series)
        if val is None or pd.isna(val):
            return None
        return float(val)

    corr_delta_r = _safe_corr("threshold_delta", "R")
    corr_blended_r = _safe_corr("blended_score", "R")

    spread_sub = df[["spread_pct"]].dropna()
    avg_spread_at_entry = None
    if not spread_sub.empty:
        avg_spread_at_entry = float(spread_sub["spread_pct"].mean())

    slippage_sub = df[["slippage_pct"]].dropna()
    avg_slippage_vs_mid = None
    if not slippage_sub.empty:
        avg_slippage_vs_mid = float(slippage_sub["slippage_pct"].mean())

    def _quartile_winrate(col, label_prefix):
        sub = df[[col, "win"]].dropna()
        if len(sub) < 4:
            return {}
        try:
            q = pd.qcut(sub[col], 4, labels=False, duplicates="drop")
            q_series = cast(pd.Series, q)
            q_values = [int(v) for v in q_series.dropna().unique().tolist()]
        except Exception:
            return {}

        out = {}
        for idx in sorted(q_values):
            mask = q_series == idx
            bucket_df = sub.loc[mask, ["win"]]
            bucket = [bool(v) for v in bucket_df["win"]]
            if len(bucket) == 0:
                continue
            out[f"{label_prefix}_q{int(idx) + 1}"] = {
                "trades": int(len(bucket)),
                "winrate": float(sum(bucket) / len(bucket)),
            }
        return out

    winrate_by_delta_quartile = _quartile_winrate("threshold_delta", "delta")
    winrate_by_ml_weight_quartile = _quartile_winrate("ml_weight", "ml_weight")
    winrate_by_spread_quartile = _quartile_winrate("spread_pct", "spread")

    maturity_sub = df[["regime_samples", "win"]].dropna()
    regime_maturity_comparison = {}
    if not maturity_sub.empty:
        mature_df = maturity_sub.loc[maturity_sub["regime_samples"] >= 20, ["win"]]
        immature_df = maturity_sub.loc[maturity_sub["regime_samples"] < 20, ["win"]]
        mature = [bool(v) for v in mature_df["win"]]
        immature = [bool(v) for v in immature_df["win"]]

        regime_maturity_comparison = {
            "regime_samples_ge_20": {
                "trades": int(len(mature)),
                "winrate": float(sum(mature) / len(mature)) if len(mature) > 0 else None,
            },
            "regime_samples_lt_20": {
                "trades": int(len(immature)),
                "winrate": float(sum(immature) / len(immature)) if len(immature) > 0 else None,
            },
        }

    return {
        "total_trades_analyzed": int(len(df)),
        "corr_threshold_delta_vs_R": corr_delta_r,
        "corr_blended_vs_R": corr_blended_r,
        "avg_spread_at_entry": avg_spread_at_entry,
        "avg_slippage_vs_mid": avg_slippage_vs_mid,
        "winrate_by_delta_quartile": winrate_by_delta_quartile,
        "winrate_by_ml_weight_quartile": winrate_by_ml_weight_quartile,
        "winrate_by_spread_quartile": winrate_by_spread_quartile,
        "regime_maturity_comparison": regime_maturity_comparison,
    }
