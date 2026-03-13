import os
import pandas as pd
from core.paths import DATA_DIR
from core.analytics_db import read_df, row_count
from analytics.edge_momentum import calculate_edge_momentum

FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")

def get_ml_weight(max_trades=200):
    """
    Gradually increases ML influence as trade count grows.
    """

    try:
        trade_count = row_count("trade_features")
    except Exception:
        return 0.0

    if trade_count == 0:
        return 0.0

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
