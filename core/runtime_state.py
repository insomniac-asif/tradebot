"""
core/runtime_state.py — System state machine for safe trading lifecycle management.

States:
  BOOTING          → process just started, loading config
  RECONCILING      → comparing broker state vs internal state
  READY            → all checks passed, not yet in market hours
  TRADING_ENABLED  → market open, all systems go, new entries allowed
  DEGRADED         → stale data or partial failure — exits allowed, entries blocked
  EXIT_ONLY        → explicit lock — manage open positions only
  PANIC_LOCKDOWN   → emergency — close everything, block all new activity

Allowed transitions (enforced by transition()):
  BOOTING          → RECONCILING
  RECONCILING      → READY | EXIT_ONLY | PANIC_LOCKDOWN
  READY            → TRADING_ENABLED | DEGRADED | EXIT_ONLY | PANIC_LOCKDOWN
  TRADING_ENABLED  → DEGRADED | EXIT_ONLY | PANIC_LOCKDOWN | READY
  DEGRADED         → TRADING_ENABLED | EXIT_ONLY | PANIC_LOCKDOWN | READY
  EXIT_ONLY        → READY | RECONCILING | PANIC_LOCKDOWN
  PANIC_LOCKDOWN   → RECONCILING  (manual recovery only)
"""

import logging
import threading
import time
from enum import Enum

logger = logging.getLogger(__name__)


class SystemState(Enum):
    BOOTING = "BOOTING"
    RECONCILING = "RECONCILING"
    READY = "READY"
    TRADING_ENABLED = "TRADING_ENABLED"
    DEGRADED = "DEGRADED"
    EXIT_ONLY = "EXIT_ONLY"
    PANIC_LOCKDOWN = "PANIC_LOCKDOWN"


_ALLOWED_TRANSITIONS: dict[SystemState, set[SystemState]] = {
    SystemState.BOOTING:         {SystemState.RECONCILING},
    SystemState.RECONCILING:     {SystemState.READY, SystemState.EXIT_ONLY, SystemState.PANIC_LOCKDOWN},
    SystemState.READY:           {SystemState.TRADING_ENABLED, SystemState.DEGRADED, SystemState.EXIT_ONLY, SystemState.PANIC_LOCKDOWN},
    SystemState.TRADING_ENABLED: {SystemState.DEGRADED, SystemState.EXIT_ONLY, SystemState.PANIC_LOCKDOWN, SystemState.READY},
    SystemState.DEGRADED:        {SystemState.TRADING_ENABLED, SystemState.EXIT_ONLY, SystemState.PANIC_LOCKDOWN, SystemState.READY},
    SystemState.EXIT_ONLY:       {SystemState.READY, SystemState.RECONCILING, SystemState.PANIC_LOCKDOWN},
    SystemState.PANIC_LOCKDOWN:  {SystemState.RECONCILING},
}


class RuntimeState:
    """
    Single source of truth for the bot's operating mode.
    Thread-safe. Every component checks this before acting.
    Use the module-level RUNTIME singleton.
    """

    def __init__(self) -> None:
        self._state = SystemState.BOOTING
        self._lock = threading.Lock()
        self._reason = "system starting"
        self._state_history: list[tuple] = []   # (timestamp, old, new, reason)
        self._degradation_reasons: set[str] = set()

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def state(self) -> SystemState:
        with self._lock:
            return self._state

    @property
    def reason(self) -> str:
        with self._lock:
            return self._reason

    # ── Degradation tracking ───────────────────────────────────────────────

    def add_degradation(self, reason: str) -> None:
        with self._lock:
            self._degradation_reasons.add(reason)

    def clear_degradation(self, reason: str) -> None:
        with self._lock:
            self._degradation_reasons.discard(reason)

    def degradation_reasons(self) -> set[str]:
        with self._lock:
            return set(self._degradation_reasons)

    # ── Convenience predicates ─────────────────────────────────────────────

    def can_enter_trades(self) -> bool:
        """True only when new trade entries are permitted."""
        return self._state == SystemState.TRADING_ENABLED

    # Alias used in Phase 1 wire-ins
    def can_enter(self) -> bool:
        return self.can_enter_trades()

    def can_exit_trades(self) -> bool:
        """True when existing positions may be managed (exits, stops)."""
        return self._state in {
            SystemState.TRADING_ENABLED,
            SystemState.DEGRADED,
            SystemState.EXIT_ONLY,
            SystemState.PANIC_LOCKDOWN,
        }

    # Alias used in Phase 1 wire-ins
    def can_manage_exits(self) -> bool:
        return self.can_exit_trades()

    def can_run_paper_sims(self) -> bool:
        """True when paper sims may attempt entries."""
        return self._state in {
            SystemState.TRADING_ENABLED,
            SystemState.DEGRADED,
            SystemState.READY,
        }

    def is_live_blocked(self) -> bool:
        """True when live order submission must be hard-blocked."""
        return self._state in {SystemState.PANIC_LOCKDOWN}

    # ── Transitions ────────────────────────────────────────────────────────

    def transition(self, new_state: SystemState, reason: str = "") -> bool:
        """
        Attempt a graph-validated state transition.
        Returns True if the transition was applied, False if rejected.
        """
        with self._lock:
            allowed = _ALLOWED_TRANSITIONS.get(self._state, set())
            if new_state not in allowed:
                logger.warning(
                    "runtime_state_invalid_transition: %s → %s (reason=%r)",
                    self._state.value, new_state.value, reason,
                )
                return False
            old = self._state
            self._state = new_state
            self._reason = reason or f"{old.value} → {new_state.value}"
            self._state_history.append((time.time(), old, new_state, self._reason))
            if len(self._state_history) > 100:
                self._state_history = self._state_history[-100:]
            logger.info(
                "runtime_state_transition: %s → %s (reason=%r)",
                old.value, new_state.value, self._reason,
            )
            return True

    def force_transition(self, new_state: SystemState, reason: str = "") -> None:
        """Emergency override — bypasses the transition graph."""
        with self._lock:
            old = self._state
            self._state = new_state
            self._reason = reason or f"FORCED: {old.value} → {new_state.value}"
            self._state_history.append((time.time(), old, new_state, self._reason))
            if len(self._state_history) > 100:
                self._state_history = self._state_history[-100:]
            logger.error(
                "runtime_state_forced: %s → %s (reason=%r)",
                old.value, new_state.value, self._reason,
            )

    def get_status_dict(self) -> dict:
        with self._lock:
            return {
                "state": self._state.value,
                "reason": self._reason,
                "degradation_reasons": list(self._degradation_reasons),
                "history": [
                    {"time": t, "from": o.value, "to": n.value, "reason": r}
                    for t, o, n, r in self._state_history[-10:]
                ],
            }

    def __repr__(self) -> str:
        return f"RuntimeState(state={self._state.value!r}, reason={self._reason!r})"


# Module-level singleton — import this everywhere.
RUNTIME = RuntimeState()
