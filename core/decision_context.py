from datetime import datetime
import pytz


class DecisionContext:
    def __init__(self):
        self.timestamp = datetime.now(pytz.timezone("US/Eastern"))
        self.regime = None
        self.volatility = None
        self.direction_60m = None
        self.confidence_60m = None
        self.direction_15m = None
        self.confidence_15m = None
        self.dual_alignment = None
        self.conviction_score = None
        self.impulse = None
        self.follow = None
        self.blended_score = None
        self.threshold = None
        self.ml_weight = None
        self.regime_samples = None
        self.expectancy_samples = None
        self.block_reason = None
        self.outcome = "blocked"
        # --- data collection extensions ---
        self.spy_price = None                   # SPY close at decision time
        self.regime_transition = None           # bool: transition detected?
        self.regime_transition_severity = None  # float severity 0–1

    def set_block(self, reason):
        self.block_reason = reason
        self.outcome = "blocked"

    def set_opened(self):
        self.block_reason = None
        self.outcome = "opened"

    def snapshot_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "regime": self.regime,
            "volatility": self.volatility,
            "direction_60m": self.direction_60m,
            "confidence_60m": self.confidence_60m,
            "direction_15m": self.direction_15m,
            "confidence_15m": self.confidence_15m,
            "dual_alignment": self.dual_alignment,
            "conviction_score": self.conviction_score,
            "impulse": self.impulse,
            "follow": self.follow,
            "blended_score": self.blended_score,
            "threshold": self.threshold,
            "ml_weight": self.ml_weight,
            "regime_samples": self.regime_samples,
            "expectancy_samples": self.expectancy_samples,
            "block_reason": self.block_reason,
            "outcome": self.outcome,
            "spy_price": self.spy_price,
            "regime_transition": self.regime_transition,
            "regime_transition_severity": self.regime_transition_severity,
        }
