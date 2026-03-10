from dataclasses import dataclass, field, asdict
from typing import Optional, Dict
import time


@dataclass
class Candidate:
    """One strategy's output for one cycle. Logged whether or not a trade was taken."""
    timestamp: float = field(default_factory=time.time)
    sim_id: str = ""
    strategy: str = ""           # signal_mode
    symbol: str = ""
    direction: Optional[str] = None  # BULLISH/BEARISH/None
    fired: bool = False          # did the signal produce a direction?
    entry_ref: Optional[float] = None
    regime: str = ""
    time_bucket: str = ""        # PREMARKET/OPENING_HOUR/MIDDAY/POWER_HOUR/CLOSING
    conviction: Optional[int] = None
    grade: Optional[str] = None
    spread_pct: Optional[float] = None
    blocked: bool = False        # signal fired but was blocked by a gate
    block_reason: str = ""
    traded: bool = False         # did this actually become a live/paper trade?
    trade_id: Optional[str] = None
    signal_params: Dict = field(default_factory=dict)
    meta: Dict = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)
