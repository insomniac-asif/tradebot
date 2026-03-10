"""
core/singletons.py — Single import point for global singletons.

Import from here to avoid circular dependencies:
    from core.singletons import RUNTIME, RISK_SUPERVISOR, SystemState
"""

from core.runtime_state import RUNTIME, SystemState  # noqa: F401
from core.live_risk_supervisor import LiveRiskSupervisor

RISK_SUPERVISOR = LiveRiskSupervisor(RUNTIME)
