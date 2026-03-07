import os
import joblib
from core.paths import DATA_DIR

EDGE_MODEL_FILE = os.path.join(DATA_DIR, "edge_model.pkl")

def load_edge_model():
    if not os.path.exists(EDGE_MODEL_FILE):
        return None
    return joblib.load(EDGE_MODEL_FILE)
def build_feature_vector(trade, regime_encoded, volatility_encoded,
                         conviction_score, impulse, follow_through,
                         setup_encoded, session_encoded):

    return [[
        regime_encoded,
        volatility_encoded,
        conviction_score,
        impulse,
        follow_through,
        setup_encoded,
        session_encoded,
        trade["confidence"]
    ]]
