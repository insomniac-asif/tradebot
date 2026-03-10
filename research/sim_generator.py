# research/sim_generator.py
#
# Generates sim config YAML blocks from hypothesis dicts.
# Uses ACTUAL field names from sim_config.yaml (balance_start, risk_per_trade_pct)
# NOT the incorrect names from some spec templates (starting_balance, risk_per_trade).

import json
import logging
import os
from datetime import datetime
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# Template defaults keyed by timeframe.
# All field names match what sim_engine.py / SimPortfolio actually reads.
_TEMPLATES = {
    "scalp": {
        "horizon":              "scalp",
        "hold_min_seconds":     60,
        "hold_max_seconds":     1800,
        "stop_loss_pct":        0.20,
        "profit_target_pct":    0.40,
        "trailing_stop_activate_pct": None,
        "trailing_stop_trail_pct":    None,
        "balance_start":        1000,
        "risk_per_trade_pct":   0.10,
        "daily_loss_limit_pct": 0.02,
        "dte_min":              0,
        "dte_max":              0,
        "otm_pct":              0.008,
        "cutoff_time_et":       "11:00",
        "max_open_trades":      1,
        "exposure_cap_pct":     0.10,
        "max_spread_pct":       0.15,
        "entry_slippage":       0.01,
        "exit_slippage":        0.01,
    },
    "intraday": {
        "horizon":              "intraday",
        "hold_min_seconds":     300,
        "hold_max_seconds":     14400,
        "stop_loss_pct":        0.25,
        "profit_target_pct":    0.50,
        "trailing_stop_activate_pct": 0.12,
        "trailing_stop_trail_pct":    0.06,
        "balance_start":        1000,
        "risk_per_trade_pct":   0.10,
        "daily_loss_limit_pct": 0.02,
        "dte_min":              0,
        "dte_max":              1,
        "otm_pct":              0.008,
        "cutoff_time_et":       "15:00",
        "max_open_trades":      2,
        "exposure_cap_pct":     0.15,
        "max_spread_pct":       0.15,
        "entry_slippage":       0.01,
        "exit_slippage":        0.01,
    },
    "swing": {
        "horizon":              "swing",
        "hold_min_seconds":     3600,
        "hold_max_seconds":     86400,
        "stop_loss_pct":        0.30,
        "profit_target_pct":    0.80,
        "trailing_stop_activate_pct": 0.15,
        "trailing_stop_trail_pct":    0.08,
        "balance_start":        1000,
        "risk_per_trade_pct":   0.10,
        "daily_loss_limit_pct": 0.03,
        "dte_min":              1,
        "dte_max":              7,
        "otm_pct":              0.008,
        "cutoff_time_et":       "15:30",
        "max_open_trades":      2,
        "exposure_cap_pct":     0.15,
        "max_spread_pct":       0.18,
        "entry_slippage":       0.01,
        "exit_slippage":        0.01,
    },
}

# Maps feature keywords (lowercased) to signal modes.
FEATURE_MAP = {
    "opening_range":   "OPENING_RANGE_RECLAIM",
    "flush":           "OPENING_RANGE_RECLAIM",
    "reclaim":         "OPENING_RANGE_RECLAIM",
    "compression":     "VOL_COMPRESSION_BREAKOUT",
    "vol_spike":       "VOL_SPIKE_FADE",
    "fade":            "VOL_SPIKE_FADE",
    "fvg":             "FVG_5M",
    "fair_value_gap":  "FVG_4H",
    "liquidity":       "LIQUIDITY_SWEEP",
    "sweep":           "LIQUIDITY_SWEEP",
    "mean_reversion":  "MEAN_REVERSION",
    "breakout":        "BREAKOUT",
    "trend":           "TREND_PULLBACK",
    "pullback":        "TREND_PULLBACK",
    "swing":           "SWING_TREND",
    "vpoc":            "VPOC_REVERSION",
    "gap":             "GAP_FADE",
    "multi_timeframe": "MULTI_TF_CONFIRM",
    "flow":            "FLOW_DIVERGENCE",
    "volatility":      "VOL_COMPRESSION_BREAKOUT",
}


def suggest_signal_mode(features: list) -> str:
    """
    Return the first matching signal mode for the given feature list.
    Falls back to TREND_PULLBACK if no keyword matches.
    """
    for feat in features:
        mode = FEATURE_MAP.get(str(feat).lower())
        if mode:
            return mode
    return "TREND_PULLBACK"


def generate_sim_config(
    hyp: dict,
    sim_id: str,
    base_template: str = "intraday",
) -> str:
    """
    Generate a YAML sim config block from a hypothesis dict.
    Uses suggest_signal_mode() to pick the signal mode from hyp['features'].
    Returns a YAML string ready to paste into sim_config.yaml.
    """
    template    = dict(_TEMPLATES.get(base_template, _TEMPLATES["intraday"]))
    signal_mode = suggest_signal_mode(hyp.get("features", []))
    # Truncate claim to keep the name field reasonable
    name        = (hyp.get("claim") or f"Generated: {sim_id}")[:60]

    config = {
        sim_id: {
            "name":            name,
            "signal_mode":     signal_mode,
            "execution_mode":  "paper",
            "features_enabled": False,
            "hypothesis_id":   hyp.get("id"),
            "symbols":         ["SPY", "QQQ", "IWM"],
            **template,
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)
