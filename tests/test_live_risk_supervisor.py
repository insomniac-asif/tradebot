"""Tests for core/live_risk_supervisor.py — entry/exit authorization."""
import time
from core.runtime_state import RuntimeState, SystemState
from core.live_risk_supervisor import LiveRiskSupervisor


def _make_trading_rt():
    """Create a RuntimeState already in TRADING_ENABLED."""
    rt = RuntimeState()
    rt.transition(SystemState.RECONCILING)
    rt.transition(SystemState.READY)
    rt.transition(SystemState.TRADING_ENABLED)
    return rt


def _make_ready_supervisor():
    """Supervisor with fresh timestamps and TRADING_ENABLED state."""
    rt = _make_trading_rt()
    sup = LiveRiskSupervisor(rt)
    now = time.time()
    sup.update_bar_freshness(now)
    sup.update_quote_freshness(now)
    sup.update_account_freshness(now)
    sup.update_broker_health(now)
    return sup


class TestAuthorizeEntry:
    def test_all_fresh_authorized(self):
        sup = _make_ready_supervisor()
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is True
        assert reason == "authorized"

    def test_emergency_kill_blocks(self):
        sup = _make_ready_supervisor()
        sup.emergency_kill("manual test")
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "emergency_kill" in reason

    def test_wrong_runtime_state_blocks(self):
        rt = RuntimeState()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        # READY, not TRADING_ENABLED
        sup = LiveRiskSupervisor(rt)
        now = time.time()
        sup.update_bar_freshness(now)
        sup.update_quote_freshness(now)
        sup.update_account_freshness(now)
        sup.update_broker_health(now)
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "runtime_state" in reason

    def test_stale_bars_blocks(self):
        sup = _make_ready_supervisor()
        sup.update_bar_freshness(time.time() - 200)  # way past threshold
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "stale_bars" in reason

    def test_stale_quotes_blocks(self):
        sup = _make_ready_supervisor()
        sup.update_quote_freshness(time.time() - 200)
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "stale_quotes" in reason

    def test_stale_account_blocks(self):
        sup = _make_ready_supervisor()
        sup.update_account_freshness(time.time() - 300)
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "stale_account" in reason

    def test_broker_unreachable_blocks(self):
        sup = _make_ready_supervisor()
        sup.update_broker_health(time.time() - 600)
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "broker_unreachable" in reason

    def test_no_timestamps_blocks(self):
        rt = _make_trading_rt()
        sup = LiveRiskSupervisor(rt)
        # No freshness updates → all stale
        ok, reason = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is False
        assert "stale" in reason


class TestAuthorizeExit:
    def test_exit_allowed_in_trading_enabled(self):
        sup = _make_ready_supervisor()
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is True

    def test_exit_allowed_in_degraded(self):
        rt = _make_trading_rt()
        rt.transition(SystemState.DEGRADED)
        sup = LiveRiskSupervisor(rt)
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is True

    def test_exit_allowed_in_exit_only(self):
        rt = _make_trading_rt()
        rt.transition(SystemState.EXIT_ONLY)
        sup = LiveRiskSupervisor(rt)
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is True

    def test_exit_allowed_in_panic(self):
        rt = RuntimeState()
        rt.force_transition(SystemState.PANIC_LOCKDOWN)
        sup = LiveRiskSupervisor(rt)
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is True

    def test_exit_blocked_in_booting(self):
        rt = RuntimeState()
        sup = LiveRiskSupervisor(rt)
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is False

    def test_exit_blocked_in_ready(self):
        rt = RuntimeState()
        rt.transition(SystemState.RECONCILING)
        rt.transition(SystemState.READY)
        sup = LiveRiskSupervisor(rt)
        ok, reason = sup.authorize_exit("SIM00")
        assert ok is False


class TestKillSwitch:
    def test_kill_and_clear(self):
        sup = _make_ready_supervisor()
        assert not sup.is_killed
        sup.emergency_kill("test")
        assert sup.is_killed
        sup.clear_kill()
        assert not sup.is_killed
        ok, _ = sup.authorize_entry("SIM00", "BULLISH", "SPY", 100.0)
        assert ok is True

    def test_kill_reason_in_status(self):
        sup = _make_ready_supervisor()
        sup.emergency_kill("margin call")
        status = sup.get_status()
        assert status["emergency_kill"] is True
        assert status["kill_reason"] == "margin call"


class TestStatus:
    def test_status_keys(self):
        sup = _make_ready_supervisor()
        status = sup.get_status()
        assert "emergency_kill" in status
        assert "kill_reason" in status
        assert "bar_age_seconds" in status
        assert "quote_age_seconds" in status
        assert "account_age_seconds" in status
        assert "broker_age_seconds" in status

    def test_age_none_when_never_updated(self):
        rt = _make_trading_rt()
        sup = LiveRiskSupervisor(rt)
        status = sup.get_status()
        assert status["bar_age_seconds"] is None
