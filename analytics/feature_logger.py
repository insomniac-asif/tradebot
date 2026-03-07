import os
import csv
from core.paths import DATA_DIR
from analytics.setup_expectancy import calculate_setup_expectancy
from analytics.regime_expectancy import calculate_regime_expectancy
from signals.session_classifier import classify_session


FEATURE_FILE = os.path.join(DATA_DIR, "trade_features.csv")
FEATURE_HEADERS = [
    "regime_encoded",
    "volatility_encoded",
    "conviction_score",
    "impulse",
    "follow_through",
    "confidence",
    "style_encoded",
    "setup_encoded",
    "session_encoded",
    "setup_raw_avg_R",
    "regime_raw_avg_R",
    "ml_probability",
    "predicted_won",
    "won"
]


def ensure_feature_file(reset_if_invalid: bool = False):
    if os.path.exists(FEATURE_FILE) and os.path.getsize(FEATURE_FILE) > 0:
        try:
            with open(FEATURE_FILE, "r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            if not rows:
                raise ValueError("empty_file")
            header = rows[0]
            if header != FEATURE_HEADERS:
                if reset_if_invalid:
                    with open(FEATURE_FILE, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(FEATURE_HEADERS)
                return
        except Exception:
            if reset_if_invalid:
                with open(FEATURE_FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(FEATURE_HEADERS)
        return

    with open(FEATURE_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FEATURE_HEADERS)


# ----------------------------
# Encoders
# ----------------------------

REGIME_MAP = {
    "TREND": 1,
    "RANGE": 2,
    "VOLATILE": 3,
    "COMPRESSION": 4,
    "NO_DATA": 0
}

VOL_MAP = {
    "DEAD": 0,
    "LOW": 1,
    "NORMAL": 2,
    "HIGH": 3
}

SETUP_MAP = {
    "BREAKOUT": 1,
    "PULLBACK": 2,
    "REVERSAL": 3,
    "UNKNOWN": 0
}

STYLE_MAP = {
    "scalp": 1,
    "mini_swing": 2,
    "momentum": 3
}

SESSION_MAP = {
    "OPEN": 1,
    "MIDDAY": 2,
    "AFTERNOON": 3,
    "POWER": 4,
    "UNKNOWN": 0
}


# ----------------------------
# Main Logger
# ----------------------------

def log_trade_features(trade, result, pnl):
    ensure_feature_file(reset_if_invalid=True)

    with open(FEATURE_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        # ----------------------------
        # Encoded values
        # ----------------------------

        regime_encoded = REGIME_MAP.get(trade.get("regime"), 0)
        vol_encoded = VOL_MAP.get(trade.get("volatility"), 0)
        setup_encoded = SETUP_MAP.get(trade.get("setup"), 0)
        style_encoded = STYLE_MAP.get(trade.get("style"), 0)

        timestamp = trade.get("entry_time")
        session = classify_session(timestamp)
        session_encoded = SESSION_MAP.get(session, 0)

        # ----------------------------
        # Expectancy Intelligence
        # ----------------------------

        setup_stats = calculate_setup_expectancy()
        regime_stats = calculate_regime_expectancy()

        setup_raw_avg_R = 0
        regime_raw_avg_R = 0

        if setup_stats:
            s = setup_stats.get(trade.get("setup"))
            if s:
                setup_raw_avg_R = s.get("raw_avg_R", 0)

        if regime_stats:
            r = regime_stats.get(trade.get("regime"))
            if r:
                regime_raw_avg_R = r.get("avg_R", 0)

        # ----------------------------
        # ML + Result
        # ----------------------------

        ml_prob = trade.get("ml_probability")
        predicted_won = 1 if ml_prob and ml_prob >= 0.5 else 0
        won = 1 if result == "win" else 0

        # ----------------------------
        # Write Row
        # ----------------------------

        writer.writerow([
            regime_encoded,
            vol_encoded,
            trade.get("conviction_score"),
            trade.get("impulse"),
            trade.get("follow_through"),
            trade.get("confidence"),
            style_encoded,
            setup_encoded,
            session_encoded,
            setup_raw_avg_R,
            regime_raw_avg_R,
            ml_prob,
            predicted_won,
            won
        ])
