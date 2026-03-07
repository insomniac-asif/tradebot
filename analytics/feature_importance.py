import os
import joblib
import pandas as pd
from core.paths import DATA_DIR

EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

FEATURE_NAMES = [
    "regime",
    "volatility",
    "conviction_score",
    "impulse",
    "follow_through",
    "confidence"
]

def get_feature_importance():

    if not os.path.exists(EDGE_MODEL_FILE):
        return None

    model = joblib.load(EDGE_MODEL_FILE)

    if not hasattr(model, "feature_importances_"):
        return None

    importances = model.feature_importances_

    data = sorted(
        zip(FEATURE_NAMES, importances),
        key=lambda x: x[1],
        reverse=True
    )

    return data
