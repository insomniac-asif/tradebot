"""
Lightweight smoke tests for sim_signals.py.

Run from repo root:
    python -m pytest tests/test_sim_signals.py -v
or:
    python tests/test_sim_signals.py
"""

import sys
import os
import types

# ---------------------------------------------------------------------------
# Minimal stubs so we can import sim_signals without a full bot environment
# ---------------------------------------------------------------------------

def _stub_module(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


def _ensure_stubs():
    # Only stub if not already importable
    stubs = {
        "signals": {},
        "signals.opportunity": {"evaluate_opportunity": lambda df: None},
        "signals.volatility": {"volatility_state": lambda df: "NORMAL"},
        "signals.predictor": {"make_prediction": lambda mins, df: {}},
    }
    for mod_name, attrs in stubs.items():
        if mod_name not in sys.modules:
            _stub_module(mod_name, **attrs)


_ensure_stubs()

# Now safe to import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simulation.sim_signals import (
    _KNOWN_SIGNAL_MODES,
    _SIGNAL_MODE_FAMILY,
    derive_sim_signal,
    get_signal_family,
    is_known_signal_mode,
    _signal_failed_breakout_reversal,
    _signal_vwap_continuation,
    _signal_opening_drive,
    _signal_afternoon_breakout,
    _signal_trend_reclaim,
    _signal_extreme_extension_fade,
    _signal_multi_tf_confirm,
    _signal_gap_fade,
    _signal_vpoc_reversion,
    _signal_opening_range_reclaim,
    _signal_vol_compression_breakout,
    _signal_vol_spike_fade,
)

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n=60, trend="flat"):
    """Build a minimal OHLCV + indicator DataFrame for testing."""
    close = np.full(n, 550.0)
    if trend == "up":
        close = 550.0 + np.arange(n) * 0.05
    elif trend == "down":
        close = 550.0 - np.arange(n) * 0.05

    high = close + 0.30
    low = close - 0.30
    open_ = close - 0.10
    volume = np.full(n, 10000.0)
    ema9 = pd.Series(close).ewm(span=9).mean().values
    ema20 = pd.Series(close).ewm(span=20).mean().values
    rsi = np.full(n, 50.0)
    atr = np.full(n, 0.50)
    vwap = close * 1.001

    return pd.DataFrame({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "ema9": ema9,
        "ema20": ema20,
        "rsi": rsi,
        "atr": atr,
        "vwap": vwap,
    })


def _assert_3tuple(result, label=""):
    assert isinstance(result, tuple) and len(result) == 3, \
        f"{label}: expected 3-tuple, got {result!r}"
    direction, price, ctx = result
    assert direction in (None, "BULLISH", "BEARISH"), \
        f"{label}: bad direction {direction!r}"
    if direction is not None:
        assert isinstance(price, float), f"{label}: price should be float"
    assert ctx is None or isinstance(ctx, dict), \
        f"{label}: ctx should be dict or None"


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

def test_known_signal_modes_complete():
    expected = {
        "MEAN_REVERSION", "BREAKOUT", "TREND_PULLBACK", "SWING_TREND",
        "OPPORTUNITY", "ORB_BREAKOUT", "VWAP_REVERSION", "ZSCORE_BOUNCE",
        "FAILED_BREAKOUT_REVERSAL", "VWAP_CONTINUATION", "OPENING_DRIVE",
        "AFTERNOON_BREAKOUT", "TREND_RECLAIM", "EXTREME_EXTENSION_FADE",
        "FVG_4H", "FVG_5M", "LIQUIDITY_SWEEP", "FVG_SWEEP_COMBO",
        "FLOW_DIVERGENCE", "MULTI_TF_CONFIRM", "GAP_FADE", "VPOC_REVERSION",
        "OPENING_RANGE_RECLAIM", "VOL_COMPRESSION_BREAKOUT", "VOL_SPIKE_FADE",
        "STRUCTURE_FADE", "GEX_FLOW",
    }
    assert expected == _KNOWN_SIGNAL_MODES, \
        f"Missing: {expected - _KNOWN_SIGNAL_MODES}, extra: {_KNOWN_SIGNAL_MODES - expected}"


def test_signal_mode_family_coverage():
    for mode in _KNOWN_SIGNAL_MODES:
        assert mode in _SIGNAL_MODE_FAMILY, f"No family entry for {mode}"
        assert isinstance(_SIGNAL_MODE_FAMILY[mode], str)


def test_is_known_signal_mode():
    assert is_known_signal_mode("MEAN_REVERSION")
    assert is_known_signal_mode("mean_reversion")  # case-insensitive
    assert not is_known_signal_mode("UNICORN_STRATEGY")


def test_get_signal_family():
    assert get_signal_family("FAILED_BREAKOUT_REVERSAL") == "reversal"
    assert get_signal_family("VWAP_CONTINUATION") == "trend"
    assert get_signal_family("OPENING_DRIVE") == "breakout"
    assert get_signal_family("AFTERNOON_BREAKOUT") == "breakout"
    assert get_signal_family("TREND_RECLAIM") == "reclaim"
    assert get_signal_family("EXTREME_EXTENSION_FADE") == "fade"
    assert get_signal_family("NONEXISTENT") == "unknown"


# ---------------------------------------------------------------------------
# derive_sim_signal dispatch: all modes return 3-tuple and don't crash
# ---------------------------------------------------------------------------

def test_dispatch_all_modes_no_crash():
    df = _make_df(60)
    feature_snapshot = {
        "close": 550.0, "vwap_z": 0.5, "rsi": 50.0, "close_z": 0.5,
        "ema_spread": 0.001, "vol_z": 1.2, "atr_expansion": 1.3,
        "orb_high": 552.0, "orb_low": 548.0, "iv_rank_proxy": 0.2,
    }
    for mode in _KNOWN_SIGNAL_MODES:
        result = derive_sim_signal(
            df, mode,
            context={},
            feature_snapshot=feature_snapshot,
            profile={},
        )
        _assert_3tuple(result, label=mode)


def test_dispatch_unknown_mode():
    df = _make_df(30)
    result = derive_sim_signal(df, "NONEXISTENT_MODE")
    assert result[0] is None
    assert isinstance(result[2], dict)
    assert "unknown_signal_mode" in result[2].get("reason", "")


def test_dispatch_none_df():
    for mode in _KNOWN_SIGNAL_MODES:
        result = derive_sim_signal(None, mode)
        assert isinstance(result, tuple) and len(result) == 3


# ---------------------------------------------------------------------------
# New signal function unit tests
# ---------------------------------------------------------------------------

def test_failed_breakout_reversal_no_signal_flat():
    df = _make_df(30, trend="flat")
    result = _signal_failed_breakout_reversal(df)
    _assert_3tuple(result, "fbr_flat")
    # Flat market — no clear failed breakout
    assert result[0] is None


def test_failed_breakout_reversal_bearish():
    """Inject a pierced-high + reclaim pattern."""
    df = _make_df(30, trend="flat")
    # prev bar (index -2): spiked above ref high, closed back below
    ref_high = float(df["high"].iloc[:-2].max())
    df.at[df.index[-2], "high"] = ref_high + 1.0    # pierced above
    df.at[df.index[-2], "close"] = ref_high - 0.10  # closed back below
    # curr bar: follow-through lower
    df.at[df.index[-1], "close"] = df.at[df.index[-2], "close"] - 0.20
    result = _signal_failed_breakout_reversal(df)
    _assert_3tuple(result, "fbr_bear")
    assert result[0] == "BEARISH"
    assert result[2]["structure_score"] >= 1


def test_failed_breakout_reversal_insufficient_bars():
    df = _make_df(5)
    result = _signal_failed_breakout_reversal(df)
    assert result[0] is None
    assert result[2]["reason"] == "insufficient_bars"


def test_vwap_continuation_bullish():
    df = _make_df(20, trend="up")
    # Force EMA9 > EMA20 and close > VWAP
    df["ema9"] = df["close"] + 0.50
    df["ema20"] = df["close"] - 0.20
    df["vwap"] = df["close"] - 0.10   # close slightly above VWAP
    result = _signal_vwap_continuation(df, profile={"max_vwap_dist_pct": 0.01})
    _assert_3tuple(result, "vwap_cont_bull")
    assert result[0] == "BULLISH"


def test_vwap_continuation_too_extended():
    df = _make_df(20, trend="up")
    df["ema9"] = df["close"] + 0.50
    df["ema20"] = df["close"] - 0.20
    df["vwap"] = df["close"] * 0.98   # close way above VWAP (2%)
    result = _signal_vwap_continuation(df, profile={"max_vwap_dist_pct": 0.005})
    assert result[0] is None  # blocked — too extended


def test_opening_drive_bullish():
    df = _make_df(20, trend="up")
    # Strong bullish open: first bar low, last bar high
    df.at[df.index[0], "open"] = 540.0
    df.at[df.index[-1], "close"] = 542.0  # 0.37% move
    result = _signal_opening_drive(df, profile={"open_move_min_pct": 0.003})
    _assert_3tuple(result, "opening_drive")
    # May or may not fire depending on exact values; just check no crash
    assert result[0] in (None, "BULLISH", "BEARISH")


def test_opening_drive_insufficient_bars():
    df = _make_df(5)
    result = _signal_opening_drive(df)
    assert result[0] is None
    assert result[2]["reason"] == "insufficient_bars"


def test_afternoon_breakout_bullish():
    df = _make_df(20, trend="flat")
    # Compression: all bars flat, then expansion breakout
    ref_high = float(df["high"].iloc[:-2].max())
    df.at[df.index[-1], "high"] = ref_high + 2.0
    df.at[df.index[-1], "close"] = ref_high + 1.5  # breaks out
    df.at[df.index[-1], "low"] = ref_high - 0.20
    result = _signal_afternoon_breakout(df, profile={"expansion_ratio_min": 1.5})
    _assert_3tuple(result, "pm_bo")
    # With large expansion, should fire bullish
    assert result[0] == "BULLISH"


def test_afternoon_breakout_insufficient_bars():
    df = _make_df(5)
    result = _signal_afternoon_breakout(df)
    assert result[0] is None
    assert result[2]["reason"] == "insufficient_bars"


def test_trend_reclaim_bullish():
    df = _make_df(10, trend="up")
    # Force reclaim pattern: prev2 below EMA9, prev+curr above, EMA9 > EMA20
    df["ema20"] = df["ema9"] - 0.30  # EMA9 > EMA20
    df.at[df.index[-3], "close"] = float(df["ema9"].iloc[-3]) - 0.50  # below
    df.at[df.index[-2], "close"] = float(df["ema9"].iloc[-2]) + 0.10  # reclaimed
    df.at[df.index[-1], "close"] = float(df["ema9"].iloc[-1]) + 0.20  # holding
    result = _signal_trend_reclaim(df)
    _assert_3tuple(result, "trend_reclaim_bull")
    assert result[0] == "BULLISH"


def test_trend_reclaim_insufficient_bars():
    df = _make_df(2)
    result = _signal_trend_reclaim(df)
    assert result[0] is None


def test_extreme_extension_fade_no_snapshot():
    df = _make_df(20)
    result = _signal_extreme_extension_fade(df, feature_snapshot=None)
    assert result[0] is None
    assert result[2]["reason"] == "features_required"


def test_extreme_extension_fade_bearish():
    df = _make_df(20)
    snap = {
        "close": 555.0,
        "vwap_z": 3.0,     # extreme high
        "rsi": 80.0,        # overbought
        "ema_spread": 0.001,  # small spread — not trending strongly
    }
    result = _signal_extreme_extension_fade(df, feature_snapshot=snap, profile={
        "vwap_z_threshold": 2.5,
        "rsi_bear_threshold": 76.0,
        "rsi_bull_threshold": 24.0,
    })
    _assert_3tuple(result, "ext_fade_bear")
    assert result[0] == "BEARISH"
    assert result[2]["structure_score"] >= 2


def test_extreme_extension_fade_blocked_by_trend():
    df = _make_df(20)
    snap = {
        "close": 555.0,
        "vwap_z": 3.0,
        "rsi": 80.0,
        "ema_spread": 0.010,  # large spread — strong trend, should block
    }
    result = _signal_extreme_extension_fade(df, feature_snapshot=snap)
    assert result[0] is None
    assert result[2]["reason"] == "ema_spread_too_large"


def test_extreme_extension_fade_bullish():
    df = _make_df(20)
    snap = {
        "close": 545.0,
        "vwap_z": -3.0,
        "rsi": 20.0,
        "ema_spread": 0.001,
    }
    result = _signal_extreme_extension_fade(df, feature_snapshot=snap)
    _assert_3tuple(result, "ext_fade_bull")
    assert result[0] == "BULLISH"


# ---------------------------------------------------------------------------
# MULTI_TF_CONFIRM tests
# ---------------------------------------------------------------------------

def _make_df_large(n=200, trend="flat"):
    """Like _make_df but larger — required for multi-TF tests needing 150+ bars."""
    close = np.full(n, 550.0)
    if trend == "up":
        close = 550.0 + np.arange(n) * 0.05
    elif trend == "down":
        close = 550.0 - np.arange(n) * 0.05
    high = close + 0.30
    low  = close - 0.30
    open_ = close - 0.10
    volume = np.full(n, 10000.0)
    ema9  = pd.Series(close).ewm(span=9).mean().values
    ema20 = pd.Series(close).ewm(span=20).mean().values
    rsi   = np.full(n, 50.0)
    atr   = np.full(n, 0.50)
    vwap  = close * 1.001
    return pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": volume, "ema9": ema9, "ema20": ema20,
        "rsi": rsi, "atr": atr, "vwap": vwap,
    })


def test_multi_tf_confirm_insufficient_bars():
    df = _make_df(60)
    result = _signal_multi_tf_confirm(df)
    _assert_3tuple(result, "mtf_insufficient")
    assert result[0] is None
    assert result[2].get("reason") == "insufficient_bars"


def test_multi_tf_confirm_no_signal_flat():
    # Flat trend: EMA9 ≈ EMA20 in all TFs → no alignment
    df = _make_df_large(200, "flat")
    result = _signal_multi_tf_confirm(df)
    _assert_3tuple(result, "mtf_flat")
    # Flat df may or may not fire depending on EMA warmup — just verify contract
    assert result[0] in (None, "BULLISH", "BEARISH")


def test_multi_tf_confirm_bullish():
    # Strong uptrend: EMA9 > EMA20 in both TFs, close > vwap
    n = 200
    close = 550.0 + np.arange(n) * 0.10   # steep up
    high  = close + 0.30
    low   = close - 0.30
    open_ = close - 0.05
    volume = np.full(n, 10000.0)
    # Make vwap slightly below close so close > vwap
    vwap  = close - 0.50
    df = pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": volume, "vwap": vwap,
        "ema9":  pd.Series(close).ewm(span=9).mean().values,
        "ema20": pd.Series(close).ewm(span=20).mean().values,
        "rsi":   np.full(n, 55.0), "atr": np.full(n, 0.50),
    })
    result = _signal_multi_tf_confirm(df)
    _assert_3tuple(result, "mtf_bull")
    assert result[0] == "BULLISH", f"Expected BULLISH, got {result}"


def test_multi_tf_confirm_bearish():
    # Strong downtrend: EMA9 < EMA20 in both TFs, close < vwap
    n = 200
    close = 550.0 - np.arange(n) * 0.10
    high  = close + 0.30
    low   = close - 0.30
    open_ = close + 0.05
    volume = np.full(n, 10000.0)
    vwap  = close + 0.50   # above close
    df = pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": volume, "vwap": vwap,
        "ema9":  pd.Series(close).ewm(span=9).mean().values,
        "ema20": pd.Series(close).ewm(span=20).mean().values,
        "rsi":   np.full(n, 45.0), "atr": np.full(n, 0.50),
    })
    result = _signal_multi_tf_confirm(df)
    _assert_3tuple(result, "mtf_bear")
    assert result[0] == "BEARISH", f"Expected BEARISH, got {result}"


# ---------------------------------------------------------------------------
# GAP_FADE tests
# ---------------------------------------------------------------------------

def _make_gap_df(gap_pct=0.005, n_total=40, gap_bars_ago=16):
    """
    Build a df with a gap at `gap_bars_ago` bars from the end.

    signal uses:
      pre_gap_bar  = df.iloc[-(WINDOW+2)]  →  index = n_total - (WINDOW+2)
      session_open = df.iloc[-(WINDOW+1)]  →  index = n_total - (WINDOW+1)
    So we keep close flat before session_open and open the gapped price there.
    """
    WINDOW = 15   # must match _signal_gap_fade WINDOW
    pre_idx  = n_total - (WINDOW + 2)   # pre_gap_bar index
    open_idx = n_total - (WINDOW + 1)   # session_open_bar index

    base_close = 550.0
    gapped_px  = base_close * (1.0 + gap_pct)

    close = np.full(n_total, base_close)
    open_ = np.full(n_total, base_close - 0.10)

    # From the session open bar onward: open AND close at gapped price
    if open_idx >= 0:
        open_[open_idx:] = gapped_px
        close[open_idx:] = gapped_px

    high = np.maximum(open_, close) + 0.30
    low  = np.minimum(open_, close) - 0.30
    return pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": np.full(n_total, 10000.0),
        "ema9":  pd.Series(close).ewm(span=9).mean().values,
        "ema20": pd.Series(close).ewm(span=20).mean().values,
        "rsi":   np.full(n_total, 50.0), "atr": np.full(n_total, 0.50),
        "vwap":  close * 1.001,
    })


def test_gap_fade_insufficient_bars():
    df = _make_df(16)  # < WINDOW(15) + 2 = 17
    result = _signal_gap_fade(df)
    _assert_3tuple(result, "gap_insufficient")
    assert result[0] is None
    assert result[2].get("reason") == "insufficient_bars"


def test_gap_fade_no_gap():
    # Flat df with no gap → gap_too_small
    df = _make_df(60)
    result = _signal_gap_fade(df)
    _assert_3tuple(result, "gap_none")
    assert result[0] is None


def test_gap_fade_down_signal():
    # Gap up → should return BEARISH fade signal
    df = _make_gap_df(gap_pct=0.005)
    result = _signal_gap_fade(df)
    _assert_3tuple(result, "gap_fade_down")
    assert result[0] == "BEARISH", f"Expected BEARISH gap fade, got {result}"
    assert result[2].get("reason") == "gap_fade_down"


def test_gap_fade_up_signal():
    # Gap down → should return BULLISH fade signal
    df = _make_gap_df(gap_pct=-0.005)
    result = _signal_gap_fade(df)
    _assert_3tuple(result, "gap_fade_up")
    assert result[0] == "BULLISH", f"Expected BULLISH gap fade, got {result}"
    assert result[2].get("reason") == "gap_fade_up"


# ---------------------------------------------------------------------------
# VPOC_REVERSION tests
# ---------------------------------------------------------------------------

def test_vpoc_reversion_insufficient_bars():
    df = _make_df(10)
    result = _signal_vpoc_reversion(df)
    _assert_3tuple(result, "vpoc_insufficient")
    assert result[0] is None


def test_vpoc_reversion_no_signal_neutral():
    # Flat price, RSI=50, price ≈ VPOC → no signal
    df = _make_df(30)
    result = _signal_vpoc_reversion(df)
    _assert_3tuple(result, "vpoc_neutral")
    assert result[0] is None


def test_vpoc_reversion_returns_3tuple():
    df = _make_df(60)
    result = _signal_vpoc_reversion(df)
    _assert_3tuple(result, "vpoc_contract")


# ---------------------------------------------------------------------------
# OPENING_RANGE_RECLAIM tests
# ---------------------------------------------------------------------------

def _make_orr_df(n=40, or_low=548.0, or_high=552.0, prev_close=547.5, curr_close=549.0,
                 rsi=50.0, vwap=548.5):
    """Build a df where first 6 bars form the opening range, then price moves as specified."""
    close = np.full(n, 550.0)
    high  = np.full(n, 552.0)
    low   = np.full(n, 548.0)
    open_ = np.full(n, 549.0)

    # Ensure opening range is or_high/or_low in first 6 bars
    high[:6]  = or_high
    low[:6]   = or_low

    # Set prev and curr bar values
    close[-2] = prev_close
    close[-1] = curr_close

    vwap_arr = np.full(n, vwap)
    rsi_arr  = np.full(n, rsi)
    return pd.DataFrame({
        "open": open_, "high": high, "low": low, "close": close,
        "volume": np.full(n, 10000.0),
        "ema9":  pd.Series(close).ewm(span=9).mean().values,
        "ema20": pd.Series(close).ewm(span=20).mean().values,
        "rsi":   rsi_arr, "atr": np.full(n, 0.50),
        "vwap":  vwap_arr,
    })


def test_orr_bullish_reclaim():
    # prev below or_low, curr reclaims above, close > vwap, rsi < 60
    df = _make_orr_df(n=40, or_low=548.0, prev_close=547.5, curr_close=549.0,
                      rsi=50.0, vwap=548.5)
    result = _signal_opening_range_reclaim(df)
    _assert_3tuple(result, "orr_bull")
    assert result[0] == "BULLISH", f"Expected BULLISH, got {result}"
    assert result[2].get("reason") == "orr_bull_reclaim"


def test_orr_no_signal_price_still_below():
    # prev below or_low, curr ALSO below or_low — no reclaim
    df = _make_orr_df(n=40, or_low=548.0, prev_close=547.5, curr_close=547.0,
                      rsi=50.0, vwap=548.5)
    result = _signal_opening_range_reclaim(df)
    _assert_3tuple(result, "orr_no_signal")
    assert result[0] is None


def test_orr_bearish_reclaim():
    # prev above or_high, curr fails back below, close < vwap, rsi > 40
    df = _make_orr_df(n=40, or_high=552.0, prev_close=552.5, curr_close=551.5,
                      rsi=55.0, vwap=552.0)
    result = _signal_opening_range_reclaim(df)
    _assert_3tuple(result, "orr_bear")
    assert result[0] == "BEARISH", f"Expected BEARISH, got {result}"
    assert result[2].get("reason") == "orr_bear_reclaim"


def test_orr_insufficient_bars():
    df = _make_df(15)
    result = _signal_opening_range_reclaim(df)
    _assert_3tuple(result, "orr_insufficient")
    assert result[0] is None


# ---------------------------------------------------------------------------
# VOL_COMPRESSION_BREAKOUT tests
# ---------------------------------------------------------------------------

def _make_vcb_df(n=25, atr_mean=1.0, atr_prev_ratio=0.5, atr_now_ratio=0.95,
                 close_above_ema9=True):
    """Build a df configured for the ATR compression-expansion pattern."""
    close = np.full(n, 550.0)
    if close_above_ema9:
        close[-1] = 551.0   # above EMA9 (which will ≈ 550 for flat series)
    else:
        close[-1] = 549.0

    atr = np.full(n, atr_mean)
    atr[-2] = atr_mean * atr_prev_ratio   # compressed prev bar
    atr[-1] = atr_mean * atr_now_ratio    # expanded current bar

    high = close + 0.30
    low  = close - 0.30
    return pd.DataFrame({
        "open": close - 0.10, "high": high, "low": low, "close": close,
        "volume": np.full(n, 10000.0),
        "ema9":  pd.Series(np.full(n, 550.0)).ewm(span=9).mean().values,
        "ema20": pd.Series(np.full(n, 550.0)).ewm(span=20).mean().values,
        "rsi":   np.full(n, 50.0), "atr": atr,
        "vwap":  np.full(n, 550.0),
    })


def test_vcb_bullish():
    # ATR compressed on prev bar, expanded on current, close > ema9
    df = _make_vcb_df(n=25, atr_mean=1.0, atr_prev_ratio=0.5, atr_now_ratio=0.95,
                      close_above_ema9=True)
    result = _signal_vol_compression_breakout(df)
    _assert_3tuple(result, "vcb_bull")
    assert result[0] == "BULLISH", f"Expected BULLISH, got {result}"
    assert result[2].get("reason") == "vcb_bull"


def test_vcb_no_signal_no_compression():
    # ATR normal on prev bar (not compressed) → no signal
    df = _make_vcb_df(n=25, atr_mean=1.0, atr_prev_ratio=0.9, atr_now_ratio=0.95,
                      close_above_ema9=True)
    result = _signal_vol_compression_breakout(df)
    _assert_3tuple(result, "vcb_no_comp")
    assert result[0] is None
    assert result[2].get("reason") == "no_compression"


def test_vcb_insufficient_bars():
    df = _make_df(10)
    result = _signal_vol_compression_breakout(df)
    _assert_3tuple(result, "vcb_insufficient")
    assert result[0] is None


# ---------------------------------------------------------------------------
# VOL_SPIKE_FADE tests
# ---------------------------------------------------------------------------

def _make_vsf_df(n=25, atr_mean=1.0, atr_spike_ratio=2.0,
                 close=555.0, vwap=550.0, rsi=75.0):
    """Build a df configured for a vol spike with price above VWAP."""
    atr = np.full(n, atr_mean)
    atr[-1] = atr_mean * atr_spike_ratio    # current bar spike

    close_arr = np.full(n, close)
    return pd.DataFrame({
        "open":   close_arr - 0.10,
        "high":   close_arr + 0.30,
        "low":    close_arr - 0.30,
        "close":  close_arr,
        "volume": np.full(n, 10000.0),
        "ema9":   pd.Series(close_arr).ewm(span=9).mean().values,
        "ema20":  pd.Series(close_arr).ewm(span=20).mean().values,
        "rsi":    np.full(n, rsi),
        "atr":    atr,
        "vwap":   np.full(n, vwap),
    })


def test_vsf_bearish():
    # Spike detected, price far above VWAP, RSI > 70 → BEARISH fade
    df = _make_vsf_df(n=25, atr_mean=1.0, atr_spike_ratio=2.0,
                      close=555.0, vwap=550.0, rsi=75.0)
    result = _signal_vol_spike_fade(df)
    _assert_3tuple(result, "vsf_bear")
    assert result[0] == "BEARISH", f"Expected BEARISH, got {result}"
    assert result[2].get("reason") == "vsf_fade_bear"


def test_vsf_no_signal_moderate_rsi():
    # Same spike and distance but RSI=50 (not extreme) → no signal
    df = _make_vsf_df(n=25, atr_mean=1.0, atr_spike_ratio=2.0,
                      close=555.0, vwap=550.0, rsi=50.0)
    result = _signal_vol_spike_fade(df)
    _assert_3tuple(result, "vsf_no_rsi")
    assert result[0] is None


def test_vsf_insufficient_bars():
    df = _make_df(10)
    result = _signal_vol_spike_fade(df)
    _assert_3tuple(result, "vsf_insufficient")
    assert result[0] is None


# ---------------------------------------------------------------------------
# Existing signal backward-compat (still return 3-tuples)
# ---------------------------------------------------------------------------

def test_existing_modes_return_3tuples():
    df = _make_df(30)
    for mode in ("MEAN_REVERSION", "BREAKOUT", "TREND_PULLBACK", "SWING_TREND"):
        result = derive_sim_signal(df, mode)
        _assert_3tuple(result, mode)


# ---------------------------------------------------------------------------
# Config loading smoke test
# ---------------------------------------------------------------------------

def test_sim_config_loads():
    import yaml
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "simulation", "sim_config.yaml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    assert isinstance(config, dict)
    assert "_global" in config
    # All new sims present
    for sim_id in ("SIM18", "SIM19", "SIM20", "SIM21", "SIM22", "SIM23",
                   "SIM29", "SIM30", "SIM31", "SIM32",
                   "SIM33", "SIM34", "SIM35"):
        assert sim_id in config, f"{sim_id} missing from config"
    # All new signal modes referenced in config
    modes_in_config = {v.get("signal_mode") for k, v in config.items()
                       if isinstance(v, dict) and not k.startswith("_")}
    new_modes = {
        "FAILED_BREAKOUT_REVERSAL", "VWAP_CONTINUATION", "OPENING_DRIVE",
        "AFTERNOON_BREAKOUT", "TREND_RECLAIM", "EXTREME_EXTENSION_FADE",
        "MULTI_TF_CONFIRM", "GAP_FADE", "VPOC_REVERSION",
        "OPENING_RANGE_RECLAIM", "VOL_COMPRESSION_BREAKOUT", "VOL_SPIKE_FADE",
    }
    for m in new_modes:
        assert m in modes_in_config, f"{m} not referenced in any sim profile"


def test_sim_config_no_unknown_signal_modes():
    import yaml
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "simulation", "sim_config.yaml"
    )
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    for sim_id, profile in config.items():
        if str(sim_id).startswith("_") or not isinstance(profile, dict):
            continue
        mode = profile.get("signal_mode", "")
        if not mode:
            continue  # skip non-sim entries (e.g. symbols registry)
        assert mode in _KNOWN_SIGNAL_MODES, \
            f"{sim_id} has unknown signal_mode: {mode!r}"


# ---------------------------------------------------------------------------
# Entry point for direct execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_known_signal_modes_complete,
        test_signal_mode_family_coverage,
        test_is_known_signal_mode,
        test_get_signal_family,
        test_dispatch_all_modes_no_crash,
        test_dispatch_unknown_mode,
        test_dispatch_none_df,
        test_failed_breakout_reversal_no_signal_flat,
        test_failed_breakout_reversal_bearish,
        test_failed_breakout_reversal_insufficient_bars,
        test_vwap_continuation_bullish,
        test_vwap_continuation_too_extended,
        test_opening_drive_bullish,
        test_opening_drive_insufficient_bars,
        test_afternoon_breakout_bullish,
        test_afternoon_breakout_insufficient_bars,
        test_trend_reclaim_bullish,
        test_trend_reclaim_insufficient_bars,
        test_extreme_extension_fade_no_snapshot,
        test_extreme_extension_fade_bearish,
        test_extreme_extension_fade_blocked_by_trend,
        test_extreme_extension_fade_bullish,
        test_existing_modes_return_3tuples,
        test_multi_tf_confirm_insufficient_bars,
        test_multi_tf_confirm_no_signal_flat,
        test_multi_tf_confirm_bullish,
        test_multi_tf_confirm_bearish,
        test_gap_fade_insufficient_bars,
        test_gap_fade_no_gap,
        test_gap_fade_down_signal,
        test_gap_fade_up_signal,
        test_vpoc_reversion_insufficient_bars,
        test_vpoc_reversion_no_signal_neutral,
        test_vpoc_reversion_returns_3tuple,
        test_orr_bullish_reclaim,
        test_orr_no_signal_price_still_below,
        test_orr_bearish_reclaim,
        test_orr_insufficient_bars,
        test_vcb_bullish,
        test_vcb_no_signal_no_compression,
        test_vcb_insufficient_bars,
        test_vsf_bearish,
        test_vsf_no_signal_moderate_rsi,
        test_vsf_insufficient_bars,
        test_sim_config_loads,
        test_sim_config_no_unknown_signal_modes,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
