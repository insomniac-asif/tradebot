import os
import pandas as pd
from core.paths import DATA_DIR
from core.analytics_db import read_df

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")


def detect_feature_drift():
    """
    Detect statistical feature drift using
    rolling Z-score deviation.

    Returns:
        None (if no drift)
        OR structured drift report dict
    """

    try:
        df = read_df("SELECT * FROM trade_features")
    except Exception:
        return None

    if df.empty or len(df) < 80:
        return None

    numeric_cols = df.select_dtypes(include="number").columns
    # Exclude the 'id' column from drift checks
    numeric_cols = [c for c in numeric_cols if c != "id"]

    if len(numeric_cols) == 0:
        return None

    # ==========================
    # Split Data
    # ==========================

    recent_window = 40
    baseline_window = len(df) - recent_window

    if baseline_window < 40:
        return None

    recent = df.tail(recent_window)
    baseline = df.head(baseline_window)

    drift_flags = []
    severity_score = 0

    # ==========================
    # Z-score Drift Check
    # ==========================

    for col in numeric_cols:

        try:
            base_mean = float(baseline[col].mean())
            base_std = float(baseline[col].std())
        except Exception:
            continue

        if base_std == 0 or pd.isna(base_std):
            continue

        try:
            recent_mean = float(recent[col].mean())
        except Exception:
            continue

        z_score = abs(recent_mean - base_mean) / base_std

        if z_score > 2.0:
            drift_flags.append(f"{col} Z={round(z_score,2)}")
            severity_score += min(z_score / 4, 1.0)

    if not drift_flags:
        return None

    # Normalize severity
    severity_score = min(severity_score / len(numeric_cols), 1.0)

    return {
        "features": drift_flags,
        "severity": round(severity_score, 3)
    }
