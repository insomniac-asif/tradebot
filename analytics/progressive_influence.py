import os
import pandas as pd
from core.paths import DATA_DIR
from analytics.edge_momentum import calculate_edge_momentum

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")

def get_ml_weight(max_trades=200):
    """
    Gradually increases ML influence as trade count grows.
    """

    if not os.path.exists(FEATURE_FILE):
        return 0.0

    try:
        df = pd.read_csv(FEATURE_FILE)
    except:
        return 0.0
    trade_count = len(df)

    weight = min(trade_count / max_trades, 1.0)
    momentum_data = calculate_edge_momentum()

    if momentum_data:
        momentum = momentum_data.get("momentum")
        if momentum is not None:
            if momentum > 0.2:
                weight += 0.05
            elif momentum < -0.2:
                weight -= 0.05

    weight = max(0.0, min(weight, 1.0))

    return round(weight, 3)
