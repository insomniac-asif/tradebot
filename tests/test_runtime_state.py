"""Tests for core/runtime_state.py — system state machine."""
import threading
from core.runtime_state import RuntimeState, SystemState


def _fresh():
    return RuntimeState()


class TestInitialState:
    def test_starts_in_booting(self):
        rt = _fresh()
        assert rt.state == SystemState.BOOTING

    def test_initial_reason(self):
        rt = _fresh()
        assert "starting" in rt.reason


class TestTransitions:
    def test_booting_to_reconciling(self):
        rt = _fresh()
        assert rt.transition(SystemState.RECONCILING, "startup")

    def test_booting_to_ready_rejected(self):
        rt = _fresh()
        assert not rt.transition(SystemState.READY, "nope")

    def test_full_happy_path(self):
        rt = _fresh()
        assert rt.transition(SystemState.RECONCILING, "startup")
        assert rt.transition(SystemState.READY, "reconciled")
        assert rt.transition(SystemState.TRADING_ENABLED, "market open")
        assert rt.state == SystemState.TRADING_ENABLED

    def test_trading_to_degraded_and_back(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.TRADING_ENABLED)
        assert rt.transition(SystemState.DEGRADED, "stale bars")
        assert rt.state == SystemState.DEGRADED
        assert rt.transition(SystemState.TRADING_ENABLED, "bars refreshed")

    def test_degraded_to_exit_only(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.DEGRADED)
        assert rt.transition(SystemState.EXIT_ONLY, "manual kill")

    def test_panic_lockdown_only_to_reconciling(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.force_transition(SystemState.PANIC_LOCKDOWN, "emergency")
        assert not rt.transition(SystemState.READY)
        assert not rt.transition(SystemState.TRADING_ENABLED)
        assert rt.transition(SystemState.RECONCILING, "recovery")

    def test_exit_only_to_ready(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.EXIT_ONLY)
        assert rt.transition(SystemState.READY, "unkill")

    def test_invalid_transition_returns_false(self):
        rt = _fresh()
        assert not rt.transition(SystemState.TRADING_ENABLED)
        assert rt.state == SystemState.BOOTING  # unchanged


class TestForceTransition:
    def test_force_bypasses_graph(self):
        rt = _fresh()
        rt.force_transition(SystemState.PANIC_LOCKDOWN, "test")
        assert rt.state == SystemState.PANIC_LOCKDOWN

    def test_force_records_reason(self):
        rt = _fresh()
        rt.force_transition(SystemState.EXIT_ONLY, "manual override")
        assert "manual override" in rt.reason


class TestPredicates:
    def test_can_enter_only_when_trading_enabled(self):
        rt = _fresh()
        assert not rt.can_enter_trades()
        rt.transition(SystemState.RECONCILING)
        assert not rt.can_enter_trades()
        rt.transition(SystemState.READY)
        assert not rt.can_enter_trades()
        rt.transition(SystemState.TRADING_ENABLED)
        assert rt.can_enter_trades()

    def test_can_enter_alias(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.TRADING_ENABLED)
        assert rt.can_enter() == rt.can_enter_trades()

    def test_can_exit_in_multiple_states(self):
        rt = _fresh()
        assert not rt.can_exit_trades()  # BOOTING
        rt.transition(SystemState.RECONCILING)
        assert not rt.can_exit_trades()
        rt.transition(SystemState.READY)
        assert not rt.can_exit_trades()
        rt.transition(SystemState.TRADING_ENABLED)
        assert rt.can_exit_trades()
        rt.transition(SystemState.DEGRADED)
        assert rt.can_exit_trades()
        rt.transition(SystemState.EXIT_ONLY)
        assert rt.can_exit_trades()
        rt.force_transition(SystemState.PANIC_LOCKDOWN)
        assert rt.can_exit_trades()

    def test_can_run_paper_sims(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        assert rt.can_run_paper_sims()
        rt.transition(SystemState.TRADING_ENABLED)
        assert rt.can_run_paper_sims()
        rt.transition(SystemState.DEGRADED)
        assert rt.can_run_paper_sims()
        rt.transition(SystemState.EXIT_ONLY)
        assert not rt.can_run_paper_sims()

    def test_is_live_blocked_only_in_panic(self):
        rt = _fresh()
        assert not rt.is_live_blocked()
        rt.force_transition(SystemState.PANIC_LOCKDOWN)
        assert rt.is_live_blocked()


class TestDegradation:
    def test_add_and_clear(self):
        rt = _fresh()
        rt.add_degradation("stale_bars")
        rt.add_degradation("broker_error")
        assert rt.degradation_reasons() == {"stale_bars", "broker_error"}
        rt.clear_degradation("stale_bars")
        assert rt.degradation_reasons() == {"broker_error"}

    def test_clear_nonexistent_is_safe(self):
        rt = _fresh()
        rt.clear_degradation("nonexistent")  # no error


class TestStatusDict:
    def test_contains_required_keys(self):
        rt = _fresh()
        d = rt.get_status_dict()
        assert "state" in d
        assert "reason" in d
        assert "degradation_reasons" in d
        assert "history" in d

    def test_history_after_transitions(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        d = rt.get_status_dict()
        assert len(d["history"]) == 2


class TestThreadSafety:
    def test_concurrent_transitions(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.TRADING_ENABLED)

        errors = []

        def toggle(n):
            for _ in range(n):
                rt.transition(SystemState.DEGRADED)
                rt.transition(SystemState.TRADING_ENABLED)

        threads = [threading.Thread(target=toggle, args=(50,)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # After all threads complete, state should be valid
        assert rt.state in {SystemState.TRADING_ENABLED, SystemState.DEGRADED}


class TestHistoryLimit:
    def test_history_capped_at_100(self):
        rt = _fresh()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        rt.transition(SystemState.TRADING_ENABLED)
        for _ in range(120):
            rt.transition(SystemState.DEGRADED)
            rt.transition(SystemState.TRADING_ENABLED)
        assert len(rt._state_history) <= 100
