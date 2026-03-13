"""Tests for simulation/correlation_guard.py"""
import json
import os
import tempfile
import pytest

from simulation.correlation_guard import (
    get_correlation_group,
    is_inverse,
    effective_direction,
    count_correlated_exposure,
    check_correlation_limit,
)


# ── Unit tests for helpers ────────────────────────────────────────────────

def test_get_correlation_group_equity_index():
    assert get_correlation_group("SPY") == "equity_index"
    assert get_correlation_group("QQQ") == "equity_index"
    assert get_correlation_group("IWM") == "equity_index"


def test_get_correlation_group_mega_tech():
    assert get_correlation_group("TSLA") == "mega_tech"
    assert get_correlation_group("AAPL") == "mega_tech"
    assert get_correlation_group("NVDA") == "mega_tech"
    assert get_correlation_group("MSFT") == "mega_tech"


def test_get_correlation_group_vxx_maps_to_equity():
    assert get_correlation_group("VXX") == "equity_index"


def test_get_correlation_group_unknown_symbol():
    assert get_correlation_group("XYZ") == "XYZ"


def test_is_inverse():
    assert is_inverse("VXX") is True
    assert is_inverse("SPY") is False
    assert is_inverse("AAPL") is False


def test_effective_direction_normal():
    assert effective_direction("SPY", "BULLISH") == "BULLISH"
    assert effective_direction("QQQ", "BEARISH") == "BEARISH"


def test_effective_direction_inverse_vxx():
    assert effective_direction("VXX", "BULLISH") == "BEARISH"
    assert effective_direction("VXX", "BEARISH") == "BULLISH"


# ── Integration tests with SimPortfolio files ─────────────────────────────

def _make_sim_file(tmpdir, sim_id, open_trades):
    """Write a minimal sim JSON file and return the path."""
    path = os.path.join(tmpdir, f"{sim_id}.json")
    data = {
        "balance": 3000,
        "open_trades": open_trades,
        "trade_log": [],
    }
    with open(path, "w") as f:
        json.dump(data, f)
    return path


@pytest.fixture
def sim_tmpdir(monkeypatch, tmp_path):
    """Point SimPortfolio at a temp directory."""
    sims_dir = str(tmp_path / "sims")
    os.makedirs(sims_dir, exist_ok=True)
    monkeypatch.setattr("simulation.sim_portfolio.SIM_DIR", sims_dir)
    return sims_dir


def test_count_correlated_no_open_trades(sim_tmpdir):
    """No open trades → count is 0."""
    _make_sim_file(sim_tmpdir, "SIM01", [])
    _make_sim_file(sim_tmpdir, "SIM02", [])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
    }
    assert count_correlated_exposure("BULLISH", "SPY", profiles) == 0


def test_count_correlated_same_group(sim_tmpdir):
    """SPY and QQQ bullish trades count together as equity_index."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "QQQ", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM03", [{"symbol": "IWM", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
        "SIM03": {"signal_mode": "MEAN_REVERSION"},
    }
    count = count_correlated_exposure("BULLISH", "SPY", profiles)
    assert count == 3


def test_count_correlated_opposite_direction_not_counted(sim_tmpdir):
    """Bearish trades don't count toward bullish exposure."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BEARISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "QQQ", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
    }
    count = count_correlated_exposure("BULLISH", "SPY", profiles)
    assert count == 1  # only SIM02


def test_count_correlated_vxx_inverse(sim_tmpdir):
    """VXX BULLISH = BEARISH equity_index exposure."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "VXX", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "SPY", "direction": "BEARISH"}])
    profiles = {
        "SIM01": {"signal_mode": "VOL_SPIKE_FADE"},
        "SIM02": {"signal_mode": "MEAN_REVERSION"},
    }
    # Both are effectively BEARISH equity_index
    count = count_correlated_exposure("BEARISH", "SPY", profiles)
    assert count == 2

    # VXX bullish should NOT count toward BULLISH equity_index
    count_bull = count_correlated_exposure("BULLISH", "SPY", profiles)
    assert count_bull == 0


def test_count_correlated_vxx_entry_direction_flip(sim_tmpdir):
    """Entering VXX BEARISH should count against BULLISH equity exposure."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "QQQ", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
    }
    # VXX BEARISH → effective BULLISH equity_index → should see 2 existing
    count = count_correlated_exposure("BEARISH", "VXX", profiles)
    assert count == 2


def test_count_correlated_mega_tech_group(sim_tmpdir):
    """TSLA and NVDA are in the same mega_tech group."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "TSLA", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "NVDA", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM03", [{"symbol": "SPY", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
        "SIM03": {"signal_mode": "MEAN_REVERSION"},
    }
    # AAPL would join mega_tech group
    count = count_correlated_exposure("BULLISH", "AAPL", profiles)
    assert count == 2  # TSLA + NVDA, not SPY


def test_count_correlated_cross_group_isolation(sim_tmpdir):
    """equity_index and mega_tech are separate groups."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "TSLA", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
    }
    count_eq = count_correlated_exposure("BULLISH", "QQQ", profiles)
    assert count_eq == 1  # only SPY

    count_tech = count_correlated_exposure("BULLISH", "NVDA", profiles)
    assert count_tech == 1  # only TSLA


def test_count_correlated_exclude_sim(sim_tmpdir):
    """exclude_sim skips the specified sim."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    _make_sim_file(sim_tmpdir, "SIM02", [{"symbol": "QQQ", "direction": "BULLISH"}])
    profiles = {
        "SIM01": {"signal_mode": "BREAKOUT"},
        "SIM02": {"signal_mode": "TREND_PULLBACK"},
    }
    count = count_correlated_exposure("BULLISH", "IWM", profiles, exclude_sim="SIM01")
    assert count == 1  # only SIM02


def test_count_correlated_per_sim_dedup(sim_tmpdir):
    """A sim with 2 open trades in the group still counts as 1."""
    _make_sim_file(sim_tmpdir, "SIM01", [
        {"symbol": "SPY", "direction": "BULLISH"},
        {"symbol": "QQQ", "direction": "BULLISH"},
    ])
    profiles = {"SIM01": {"signal_mode": "BREAKOUT"}}
    count = count_correlated_exposure("BULLISH", "IWM", profiles)
    assert count == 1


def test_check_correlation_limit_blocked(sim_tmpdir):
    """Should return block info when limit is reached."""
    for i in range(3):
        _make_sim_file(sim_tmpdir, f"SIM0{i+1}", [{"symbol": "SPY", "direction": "BULLISH"}])
    profiles = {f"SIM0{i+1}": {"signal_mode": "BREAKOUT"} for i in range(3)}

    result = check_correlation_limit("BULLISH", "QQQ", profiles, max_correlated=3)
    assert result is not None
    assert result["reason"] == "correlated_exposure_limit"
    assert result["correlation_group"] == "equity_index"
    assert result["current_count"] == 3


def test_check_correlation_limit_allowed(sim_tmpdir):
    """Should return None when under the limit."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    profiles = {"SIM01": {"signal_mode": "BREAKOUT"}}

    result = check_correlation_limit("BULLISH", "QQQ", profiles, max_correlated=3)
    assert result is None


def test_global_key_skipped(sim_tmpdir):
    """Profiles starting with _ should be skipped."""
    _make_sim_file(sim_tmpdir, "SIM01", [{"symbol": "SPY", "direction": "BULLISH"}])
    profiles = {
        "_global": {"cross_sim_guard_enabled": True},
        "SIM01": {"signal_mode": "BREAKOUT"},
    }
    count = count_correlated_exposure("BULLISH", "QQQ", profiles)
    assert count == 1  # _global not counted
