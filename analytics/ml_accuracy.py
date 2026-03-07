import os
import pandas as pd
from pandas.errors import EmptyDataError
from core.paths import DATA_DIR

FILE = os.path.join(DATA_DIR, "trade_features.csv")


def ml_rolling_accuracy(lookback=30):
    """
    Returns advanced rolling ML performance metrics.
    """

    if not os.path.exists(FILE):
        return None

    if os.path.getsize(FILE) == 0:
        return None

    try:
        df = pd.read_csv(FILE)
    except EmptyDataError:
        return None
    except Exception:
        return None

    required_cols = ["won", "predicted_won"]

    for col in required_cols:
        if col not in df.columns:
            return None

    # Remove incomplete rows
    df = df.dropna(subset=required_cols)

    if len(df) < lookback:
        return None

    recent = df.tail(lookback)

    accuracy = (recent["won"] == recent["predicted_won"]).mean()

    # Optional: probability calibration check
    calibration_score = None
    if "ml_probability" in recent.columns:

        confident = recent[recent["ml_probability"] > 0.65]

        if len(confident) >= 5:
            confident_acc = (confident["won"] == confident["predicted_won"]).mean()
            calibration_score = round(confident_acc * 100, 2)

    return {
        "accuracy": round(accuracy * 100, 2),
        "samples": len(recent),
        "confident_accuracy": calibration_score
    }
