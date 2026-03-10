import logging
from signals.opportunity import evaluate_opportunity
from signals.volatility import volatility_state
from signals.predictor import make_prediction

# ---------------------------------------------------------------------------
# Signal mode registry
# ---------------------------------------------------------------------------

_KNOWN_SIGNAL_MODES = frozenset({
    "MEAN_REVERSION",
    "BREAKOUT",
    "TREND_PULLBACK",
    "SWING_TREND",
    "OPPORTUNITY",
    "ORB_BREAKOUT",
    "VWAP_REVERSION",
    "ZSCORE_BOUNCE",
    "FAILED_BREAKOUT_REVERSAL",
    "VWAP_CONTINUATION",
    "OPENING_DRIVE",
    "AFTERNOON_BREAKOUT",
    "TREND_RECLAIM",
    "EXTREME_EXTENSION_FADE",
    "FVG_4H",
    "FVG_5M",
    "LIQUIDITY_SWEEP",
    "FVG_SWEEP_COMBO",
    "FLOW_DIVERGENCE",
    "MULTI_TF_CONFIRM",
    "GAP_FADE",
    "VPOC_REVERSION",
    "OPENING_RANGE_RECLAIM",
    "VOL_COMPRESSION_BREAKOUT",
    "VOL_SPIKE_FADE",
})

_SIGNAL_MODE_FAMILY = {
    "MEAN_REVERSION":           "reversal",
    "BREAKOUT":                 "breakout",
    "TREND_PULLBACK":           "trend",
    "SWING_TREND":              "trend",
    "OPPORTUNITY":              "adaptive",
    "ORB_BREAKOUT":             "breakout",
    "VWAP_REVERSION":           "reversal",
    "ZSCORE_BOUNCE":            "reversal",
    "FAILED_BREAKOUT_REVERSAL": "reversal",
    "VWAP_CONTINUATION":        "trend",
    "OPENING_DRIVE":            "breakout",
    "AFTERNOON_BREAKOUT":       "breakout",
    "TREND_RECLAIM":            "reclaim",
    "EXTREME_EXTENSION_FADE":   "fade",
    "FVG_4H":                   "reversal",
    "FVG_5M":                   "reversal",
    "LIQUIDITY_SWEEP":          "reversal",
    "FVG_SWEEP_COMBO":          "reversal",
    "FLOW_DIVERGENCE":          "fade",
    "MULTI_TF_CONFIRM":         "trend",
    "GAP_FADE":                 "fade",
    "VPOC_REVERSION":           "reversal",
    "OPENING_RANGE_RECLAIM":    "structure",
    "VOL_COMPRESSION_BREAKOUT": "volatility",
    "VOL_SPIKE_FADE":           "volatility",
}


def get_signal_family(signal_mode: str) -> str:
    """Return the strategy family for a given signal mode."""
    return _SIGNAL_MODE_FAMILY.get(str(signal_mode).upper(), "unknown")


def is_known_signal_mode(signal_mode: str) -> bool:
    return str(signal_mode).upper() in _KNOWN_SIGNAL_MODES


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _find_col(df, candidates):
    if df is None:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _ctx_float(context, key, default):
    if not isinstance(context, dict):
        return default
    val = context.get(key)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Existing signal functions (preserved, updated to 3-tuple returns)
# ---------------------------------------------------------------------------

def _signal_mean_reversion(df) -> tuple:
    RSI_OVERSOLD = 30
    RSI_OVERBOUGHT = 70
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None, {"reason": "insufficient_bars"}
        close_col = _find_col(df, ["close", "Close"])
        rsi_col = _find_col(df, ["rsi", "RSI", "rsi14", "RSI14"])
        vwap_col = _find_col(df, ["vwap", "VWAP"])
        if close_col is None or rsi_col is None or vwap_col is None:
            return None, None, {"reason": "missing_cols"}
        last = df.iloc[-1]
        close = float(last[close_col])
        rsi = float(last[rsi_col])
        vwap = float(last[vwap_col])
        if rsi < RSI_OVERSOLD and close < vwap:
            return "BULLISH", close, {"reason": "mean_reversion_long", "rsi": rsi}
        if rsi > RSI_OVERBOUGHT and close > vwap:
            return "BEARISH", close, {"reason": "mean_reversion_short", "rsi": rsi}
        return None, None, {"reason": "no_mean_reversion"}
    except Exception:
        return None, None, {"reason": "mean_reversion_error"}


def _signal_breakout(df) -> tuple:
    BREAKOUT_LOOKBACK = 20
    try:
        if df is None or len(df) < BREAKOUT_LOOKBACK + 1:
            return None, None, {"reason": "insufficient_bars"}
        close_col = _find_col(df, ["close", "Close"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])
        if close_col is None or high_col is None or low_col is None:
            return None, None, {"reason": "missing_cols"}
        close = float(df.iloc[-1][close_col])
        highs = df[high_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        lows = df[low_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        if len(highs) < 1 or len(lows) < 1:
            return None, None, {"reason": "insufficient_data"}
        recent_high = max(highs)
        recent_low = min(lows)
        if close > recent_high:
            return "BULLISH", close, {"reason": "breakout_high", "ref_high": recent_high}
        if close < recent_low:
            return "BEARISH", close, {"reason": "breakout_low", "ref_low": recent_low}
        return None, None, {"reason": "no_breakout"}
    except Exception:
        return None, None, {"reason": "breakout_error"}


def _signal_trend_pullback(df) -> tuple:
    PULLBACK_TOLERANCE = 0.001
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None, {"reason": "insufficient_bars"}
        close_col = _find_col(df, ["close", "Close"])
        ema9_col = _find_col(df, ["ema9", "EMA9", "ema_9"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema9_col is None or ema20_col is None:
            return None, None, {"reason": "missing_cols"}
        last = df.iloc[-1]
        close = float(last[close_col])
        ema9 = float(last[ema9_col])
        ema20 = float(last[ema20_col])
        if ema9 > ema20:
            if close <= ema9 * (1 + PULLBACK_TOLERANCE) and close >= ema9 * (1 - PULLBACK_TOLERANCE):
                return "BULLISH", close, {"reason": "trend_pullback_bull"}
        if ema9 < ema20:
            if close >= ema9 * (1 - PULLBACK_TOLERANCE) and close <= ema9 * (1 + PULLBACK_TOLERANCE):
                return "BEARISH", close, {"reason": "trend_pullback_bear"}
        return None, None, {"reason": "no_pullback"}
    except Exception:
        return None, None, {"reason": "trend_pullback_error"}


def _signal_orb_breakout(feature_snapshot: dict, profile: dict) -> tuple:
    try:
        close = feature_snapshot.get("close")
        orb_high = feature_snapshot.get("orb_high")
        orb_low = feature_snapshot.get("orb_low")
        vol_z = feature_snapshot.get("vol_z")
        ema_spread = feature_snapshot.get("ema_spread")
        if close is None or orb_high is None or orb_low is None:
            return None, None, "orb_unavailable"
        vol_z_min = float(profile.get("vol_z_min", 0.0))
        if vol_z is None or float(vol_z) < vol_z_min:
            return None, None, "vol_z_filter"
        require_trend_bias = bool(profile.get("require_trend_bias", False))
        if close > orb_high:
            if require_trend_bias and (ema_spread is None or float(ema_spread) <= 0.0):
                return None, None, "trend_bias_fail"
            return "BULLISH", float(close), "orb_break_high"
        if close < orb_low:
            if require_trend_bias and (ema_spread is None or float(ema_spread) >= 0.0):
                return None, None, "trend_bias_fail"
            return "BEARISH", float(close), "orb_break_low"
        return None, None, "no_orb_break"
    except Exception:
        return None, None, "orb_error"


def _signal_swing_trend(df) -> tuple:
    SWING_SLOPE_LOOKBACK = 10
    try:
        if df is None or len(df) < SWING_SLOPE_LOOKBACK + 1:
            return None, None, {"reason": "insufficient_bars"}
        close_col = _find_col(df, ["close", "Close"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema20_col is None:
            return None, None, {"reason": "missing_cols"}
        last = df.iloc[-1]
        close = float(last[close_col])
        ema20_now = float(last[ema20_col])
        ema20_past = float(df[ema20_col].iloc[-(SWING_SLOPE_LOOKBACK + 1)])
        ema20_slope_positive = ema20_now > ema20_past
        if ema20_slope_positive and close > ema20_now:
            return "BULLISH", close, {"reason": "swing_trend_bull"}
        if (not ema20_slope_positive) and close < ema20_now:
            return "BEARISH", close, {"reason": "swing_trend_bear"}
        return None, None, {"reason": "no_swing_trend"}
    except Exception:
        return None, None, {"reason": "swing_trend_error"}


def _pick_opportunity_horizon(conviction_score, vol_state):
    try:
        score = float(conviction_score) if conviction_score is not None else 0.0
    except (TypeError, ValueError):
        score = 0.0
    if score >= 6:
        return "WEEKLY"
    if score <= 3:
        return "DAYTRADE"
    if vol_state == "HIGH":
        return "DAYTRADE"
    if vol_state == "LOW":
        return "WEEKLY"
    return "SWING"


def _signal_opportunity(df, context: dict | None = None):
    try:
        result = evaluate_opportunity(df)
        if not result:
            return None, None, None
        side = result[0]
        entry_low = result[1]
        entry_high = result[2]
        price = result[3]
        conviction_score = result[4]
        tp_low = result[5] if len(result) > 5 else None
        tp_high = result[6] if len(result) > 6 else None
        stop_loss = result[7] if len(result) > 7 else None

        direction = "BULLISH" if str(side).upper() in {"CALL", "CALLS"} else "BEARISH"
        underlying_price = float(price)
        vol_state = volatility_state(df)
        horizon_type = _pick_opportunity_horizon(conviction_score, vol_state)

        trade_count = 0
        if isinstance(context, dict):
            try:
                trade_count = int(context.get("trade_count", 0))
            except (TypeError, ValueError):
                trade_count = 0
        optimize_ready = trade_count >= 50

        pred_minutes_map = {
            "DAYTRADE": 30,
            "SWING": 120,
            "WEEKLY": 390,
        }
        pred_minutes = pred_minutes_map.get(horizon_type, 60)
        pred = make_prediction(pred_minutes, df)
        pred_dir = pred.get("direction") if isinstance(pred, dict) else None
        pred_conf = pred.get("confidence") if isinstance(pred, dict) else None
        pred_dir_up = pred_dir.upper() if isinstance(pred_dir, str) else None

        if optimize_ready:
            if pred_dir_up in {"BULLISH", "BEARISH"} and pred_conf is not None:
                try:
                    if float(pred_conf) >= 0.6:
                        if (direction == "BULLISH" and pred_dir_up == "BEARISH") or (
                            direction == "BEARISH" and pred_dir_up == "BULLISH"
                        ):
                            return None, None, None
                except (TypeError, ValueError):
                    pass
            if pred_dir_up == "RANGE" and pred_conf is not None:
                try:
                    if float(pred_conf) >= 0.6:
                        return None, None, None
                except (TypeError, ValueError):
                    pass

        horizon_map = {
            "DAYTRADE": {
                "dte_min": 0,
                "dte_max": 0,
                "hold_min_seconds": 300,
                "hold_max_seconds": 3600,
                "horizon": "scalp",
                "cutoff_time_et": "15:30",
            },
            "SWING": {
                "dte_min": 1,
                "dte_max": 5,
                "hold_min_seconds": 1800,
                "hold_max_seconds": 86400,
                "horizon": "intraday",
                "cutoff_time_et": "15:45",
            },
            "WEEKLY": {
                "dte_min": 7,
                "dte_max": 21,
                "hold_min_seconds": 3600,
                "hold_max_seconds": 604800,
                "horizon": "swing",
                "cutoff_time_et": "15:45",
            },
        }
        meta = dict(horizon_map.get(horizon_type, {}))
        if optimize_ready:
            try:
                meta["hold_max_seconds"] = max(int(meta["hold_min_seconds"]), int(pred_minutes * 60))
            except Exception:
                pass
        entry_ctx = []
        try:
            entry_ctx.append(f"opp_entry={float(entry_low):.2f}-{float(entry_high):.2f}")
        except Exception:
            pass
        if tp_low is not None and tp_high is not None:
            try:
                entry_ctx.append(f"tp={float(tp_low):.2f}-{float(tp_high):.2f}")
            except Exception:
                pass
        if stop_loss is not None:
            try:
                entry_ctx.append(f"sl={float(stop_loss):.2f}")
            except Exception:
                pass
        if horizon_type:
            entry_ctx.append(f"horizon={horizon_type}")
        if pred_dir_up:
            entry_ctx.append(f"pred={pred_dir_up}@{pred_minutes}m")
        if pred_conf is not None:
            try:
                entry_ctx.append(f"pred_conf={float(pred_conf):.2f}")
            except (TypeError, ValueError):
                pass
        entry_ctx.append(f"opt_ready={int(optimize_ready)}")
        entry_ctx.append(f"cutoff={meta.get('cutoff_time_et', 'N/A')}")
        if entry_ctx:
            meta["entry_context"] = " | ".join(entry_ctx)
        meta["opportunity_type"] = horizon_type
        meta["take_profit_low"] = tp_low
        meta["take_profit_high"] = tp_high
        meta["stop_loss"] = stop_loss
        meta["horizon_type"] = horizon_type
        meta["predicted_direction"] = pred_dir_up
        meta["prediction_confidence"] = pred_conf
        meta["prediction_timeframe"] = pred_minutes
        meta["optimize_ready"] = optimize_ready

        return direction, underlying_price, meta
    except Exception:
        return None, None, None


def _signal_vwap_reversion(df, context: dict | None = None, feature_snapshot: dict | None = None) -> tuple:
    try:
        if feature_snapshot is None:
            return None, None, {"reason": "features_required"}
        close = feature_snapshot.get("close")
        vwap_z = feature_snapshot.get("vwap_z")
        rsi = feature_snapshot.get("rsi")
        if close is None or vwap_z is None:
            return None, None, {"reason": "vwap_z_unavailable"}
        vwap_z_min = _ctx_float(context, "vwap_z_min", 2.0)
        vwap_z_val = float(vwap_z)
        if vwap_z_val <= -vwap_z_min:
            if rsi is not None:
                try:
                    if float(rsi) > 60:
                        return None, None, {"reason": "rsi_conflict"}
                except (TypeError, ValueError):
                    pass
            return "BULLISH", float(close), {
                "reason": "vwap_reversion_long",
                "vwap_z": vwap_z_val,
                "entry_context": f"vwap_z={vwap_z_val:.2f} threshold={vwap_z_min}",
            }
        if vwap_z_val >= vwap_z_min:
            if rsi is not None:
                try:
                    if float(rsi) < 40:
                        return None, None, {"reason": "rsi_conflict"}
                except (TypeError, ValueError):
                    pass
            return "BEARISH", float(close), {
                "reason": "vwap_reversion_short",
                "vwap_z": vwap_z_val,
                "entry_context": f"vwap_z={vwap_z_val:.2f} threshold={vwap_z_min}",
            }
        return None, None, {"reason": "vwap_z_below_threshold"}
    except Exception:
        return None, None, {"reason": "vwap_reversion_error"}


def _signal_zscore_bounce(df, context: dict | None = None, feature_snapshot: dict | None = None) -> tuple:
    try:
        if feature_snapshot is None:
            return None, None, {"reason": "features_required"}
        close = feature_snapshot.get("close")
        close_z = feature_snapshot.get("close_z")
        rsi = feature_snapshot.get("rsi")
        ema_spread = feature_snapshot.get("ema_spread")
        if close is None or close_z is None:
            return None, None, {"reason": "close_z_unavailable"}
        close_z_min = _ctx_float(context, "close_z_min", 2.0)
        close_z_val = float(close_z)
        if close_z_val <= -close_z_min:
            if rsi is not None:
                try:
                    if float(rsi) > 55:
                        return None, None, {"reason": "rsi_divergence"}
                except (TypeError, ValueError):
                    pass
            if ema_spread is not None:
                try:
                    if float(ema_spread) < -0.003:
                        return None, None, {"reason": "strong_downtrend"}
                except (TypeError, ValueError):
                    pass
            return "BULLISH", float(close), {
                "reason": "zscore_bounce_long",
                "close_z": close_z_val,
                "entry_context": f"close_z={close_z_val:.2f} threshold={close_z_min}",
            }
        if close_z_val >= close_z_min:
            if rsi is not None:
                try:
                    if float(rsi) < 45:
                        return None, None, {"reason": "rsi_divergence"}
                except (TypeError, ValueError):
                    pass
            if ema_spread is not None:
                try:
                    if float(ema_spread) > 0.003:
                        return None, None, {"reason": "strong_uptrend"}
                except (TypeError, ValueError):
                    pass
            return "BEARISH", float(close), {
                "reason": "zscore_bounce_short",
                "close_z": close_z_val,
                "entry_context": f"close_z={close_z_val:.2f} threshold={close_z_min}",
            }
        return None, None, {"reason": "close_z_below_threshold"}
    except Exception:
        return None, None, {"reason": "zscore_bounce_error"}


# ---------------------------------------------------------------------------
# New signal functions
# ---------------------------------------------------------------------------

def _signal_failed_breakout_reversal(df) -> tuple:
    """
    Detects a failed breakout/breakdown: previous bar pierced a reference
    level but closed back inside it; current bar confirms the reversal.
    No feature_snapshot required.
    """
    LOOKBACK = 20
    try:
        if df is None or len(df) < LOOKBACK + 2:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])
        volume_col = _find_col(df, ["volume", "Volume"])

        if close_col is None or high_col is None or low_col is None:
            return None, None, {"reason": "missing_cols"}

        # Reference window: bars before the last two
        ref_window = df.iloc[-(LOOKBACK + 2):-2]
        ref_high = float(ref_window[high_col].max())
        ref_low = float(ref_window[low_col].min())

        prev = df.iloc[-2]
        curr = df.iloc[-1]

        prev_high = float(prev[high_col])
        prev_low = float(prev[low_col])
        prev_close = float(prev[close_col])
        curr_close = float(curr[close_col])

        # Volume ratio vs average of reference window
        vol_ratio = 1.0
        if volume_col is not None:
            try:
                avg_vol = float(df[volume_col].iloc[-(LOOKBACK + 2):-2].mean())
                curr_vol = float(curr[volume_col])
                if avg_vol > 0:
                    vol_ratio = curr_vol / avg_vol
            except Exception:
                pass

        bar_range = prev_high - prev_low

        # Bearish failed breakout: prev pierced above ref_high, closed back below, curr follows
        if prev_high > ref_high and prev_close <= ref_high and curr_close < prev_close:
            fail_mag = (prev_high - ref_high) / ref_high if ref_high > 0 else 0.0
            wick_rej = (prev_high - prev_close) / bar_range if bar_range > 0 else 0.0
            structure_score = int(fail_mag > 0.001) + int(wick_rej > 0.5) + int(vol_ratio > 1.1) + int(curr_close < prev_close * 0.9995)
            return "BEARISH", curr_close, {
                "reason": "failed_breakout_bear",
                "structure_score": structure_score,
                "fail_mag": round(fail_mag, 5),
                "wick_rej": round(wick_rej, 3),
                "vol_ratio": round(vol_ratio, 2),
                "entry_context": f"failed_bo ref_high={ref_high:.2f} wick={wick_rej:.2f} vol_ratio={vol_ratio:.2f}",
            }

        # Bullish failed breakdown: prev pierced below ref_low, closed back above, curr follows
        if prev_low < ref_low and prev_close >= ref_low and curr_close > prev_close:
            fail_mag = (ref_low - prev_low) / ref_low if ref_low > 0 else 0.0
            wick_rej = (prev_close - prev_low) / bar_range if bar_range > 0 else 0.0
            structure_score = int(fail_mag > 0.001) + int(wick_rej > 0.5) + int(vol_ratio > 1.1) + int(curr_close > prev_close * 1.0005)
            return "BULLISH", curr_close, {
                "reason": "failed_breakdown_bull",
                "structure_score": structure_score,
                "fail_mag": round(fail_mag, 5),
                "wick_rej": round(wick_rej, 3),
                "vol_ratio": round(vol_ratio, 2),
                "entry_context": f"failed_bd ref_low={ref_low:.2f} wick={wick_rej:.2f} vol_ratio={vol_ratio:.2f}",
            }

        return None, None, {"reason": "no_failed_breakout"}
    except Exception:
        return None, None, {"reason": "failed_bo_error"}


def _signal_vwap_continuation(df, profile: dict | None = None) -> tuple:
    """
    Trend-following entry on a VWAP touch/bounce in the direction of
    EMA alignment. Blocked if price is too extended from VWAP.
    No feature_snapshot required.
    """
    try:
        if df is None or len(df) < 10:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        ema9_col = _find_col(df, ["ema9", "EMA9", "ema_9"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        vwap_col = _find_col(df, ["vwap", "VWAP"])
        low_col = _find_col(df, ["low", "Low"])

        if close_col is None or ema9_col is None or ema20_col is None or vwap_col is None:
            return None, None, {"reason": "missing_cols"}

        last = df.iloc[-1]
        close = float(last[close_col])
        ema9 = float(last[ema9_col])
        ema20 = float(last[ema20_col])
        vwap = float(last[vwap_col])

        max_vwap_dist = 0.005
        if isinstance(profile, dict):
            max_vwap_dist = _safe_float(profile.get("max_vwap_dist_pct"), 0.005)

        ema_spread = (ema9 - ema20) / ema20 if ema20 != 0 else 0.0
        vwap_dist = (close - vwap) / vwap if vwap != 0 else 0.0

        # Detect recent VWAP touch (within last 5 bars)
        had_vwap_touch = False
        if low_col is not None and len(df) >= 7:
            try:
                recent_lows = df[low_col].iloc[-6:-1]
                recent_vwaps = df[vwap_col].iloc[-6:-1]
                for i in range(len(recent_lows)):
                    if float(recent_lows.iloc[i]) <= float(recent_vwaps.iloc[i]) * 1.002:
                        had_vwap_touch = True
                        break
            except Exception:
                pass

        # Bullish: EMA9 > EMA20, close > VWAP, not too extended above VWAP
        if ema_spread > 0 and close > vwap and 0 < vwap_dist <= max_vwap_dist:
            structure_score = int(ema_spread > 0.001) + int(had_vwap_touch) + int(vwap_dist > 0.0005) + int(close > ema9)
            return "BULLISH", close, {
                "reason": "vwap_continuation_bull",
                "structure_score": structure_score,
                "ema_spread": round(ema_spread, 5),
                "vwap_dist": round(vwap_dist, 5),
                "had_vwap_touch": had_vwap_touch,
                "entry_context": f"vwap_cont ema_spread={ema_spread:.4f} vwap_dist={vwap_dist:.4f}",
            }

        # Bearish: EMA9 < EMA20, close < VWAP, not too extended below VWAP
        if ema_spread < 0 and close < vwap and -max_vwap_dist <= vwap_dist < 0:
            structure_score = int(ema_spread < -0.001) + int(had_vwap_touch) + int(vwap_dist < -0.0005) + int(close < ema9)
            return "BEARISH", close, {
                "reason": "vwap_continuation_bear",
                "structure_score": structure_score,
                "ema_spread": round(ema_spread, 5),
                "vwap_dist": round(vwap_dist, 5),
                "had_vwap_touch": had_vwap_touch,
                "entry_context": f"vwap_cont ema_spread={ema_spread:.4f} vwap_dist={vwap_dist:.4f}",
            }

        return None, None, {"reason": "vwap_cont_no_signal"}
    except Exception:
        return None, None, {"reason": "vwap_cont_error"}


def _signal_opening_drive(df, profile: dict | None = None) -> tuple:
    """
    Detects a sustained directional move from the open over a 15-bar window
    with optional mid-session pullback confirmation. No feature_snapshot required.
    """
    WINDOW = 15
    try:
        if df is None or len(df) < WINDOW + 1:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        open_col = _find_col(df, ["open", "Open"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])

        if close_col is None or open_col is None:
            return None, None, {"reason": "missing_cols"}

        min_move = 0.003
        if isinstance(profile, dict):
            min_move = _safe_float(profile.get("open_move_min_pct"), 0.003)

        window = df.iloc[-(WINDOW + 1):]
        first_open = float(window.iloc[0][open_col])
        curr_close = float(window.iloc[-1][close_col])
        move_pct = (curr_close - first_open) / first_open if first_open != 0 else 0.0

        # Exhaustion check: last 3 bars moving counter to drive direction
        exhausted = False
        if len(window) >= 4:
            try:
                c1 = float(window.iloc[-1][close_col])
                c2 = float(window.iloc[-2][close_col])
                c3 = float(window.iloc[-3][close_col])
                if move_pct > 0 and c1 < c2 < c3:
                    exhausted = True
                elif move_pct < 0 and c1 > c2 > c3:
                    exhausted = True
            except Exception:
                pass

        if exhausted:
            return None, None, {"reason": "opening_drive_exhausted"}

        if move_pct >= min_move:
            pullback_detected = False
            if low_col is not None and high_col is not None and len(window) >= 13:
                try:
                    mid_bars = window.iloc[4:13]
                    mid_low = float(mid_bars[low_col].min())
                    anchor_close = float(window.iloc[2][close_col])
                    if mid_low < anchor_close:
                        pullback_detected = True
                except Exception:
                    pass
            prev_close = float(window.iloc[-2][close_col]) if len(window) >= 2 else curr_close
            structure_score = int(move_pct > min_move * 1.5) + int(pullback_detected) + int(curr_close > prev_close) + int(abs(move_pct) > 0.005)
            return "BULLISH", curr_close, {
                "reason": "opening_drive_bull",
                "structure_score": structure_score,
                "move_pct": round(move_pct, 5),
                "pullback": pullback_detected,
                "entry_context": f"opening_drive move={move_pct:.3%} pullback={pullback_detected}",
            }

        if move_pct <= -min_move:
            pullback_detected = False
            if high_col is not None and len(window) >= 13:
                try:
                    mid_bars = window.iloc[4:13]
                    mid_high = float(mid_bars[high_col].max())
                    anchor_close = float(window.iloc[2][close_col])
                    if mid_high > anchor_close:
                        pullback_detected = True
                except Exception:
                    pass
            prev_close = float(window.iloc[-2][close_col]) if len(window) >= 2 else curr_close
            structure_score = int(abs(move_pct) > min_move * 1.5) + int(pullback_detected) + int(curr_close < prev_close) + int(abs(move_pct) > 0.005)
            return "BEARISH", curr_close, {
                "reason": "opening_drive_bear",
                "structure_score": structure_score,
                "move_pct": round(move_pct, 5),
                "pullback": pullback_detected,
                "entry_context": f"opening_drive move={move_pct:.3%} pullback={pullback_detected}",
            }

        return None, None, {"reason": "opening_drive_insufficient_move"}
    except Exception:
        return None, None, {"reason": "opening_drive_error"}


def _signal_afternoon_breakout(df, profile: dict | None = None) -> tuple:
    """
    Detects a range compression followed by an expansion breakout.
    Requires current bar range > expansion_ratio_min × avg compression bar range.
    No feature_snapshot required.
    """
    COMP_LOOKBACK = 12
    try:
        if df is None or len(df) < COMP_LOOKBACK + 2:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])
        atr_col = _find_col(df, ["atr", "ATR"])

        if close_col is None or high_col is None or low_col is None:
            return None, None, {"reason": "missing_cols"}

        expansion_ratio_min = 1.5
        if isinstance(profile, dict):
            expansion_ratio_min = _safe_float(profile.get("expansion_ratio_min"), 1.5)

        comp_window = df.iloc[-(COMP_LOOKBACK + 2):-2]
        comp_high = float(comp_window[high_col].max())
        comp_low = float(comp_window[low_col].min())

        comp_bar_ranges = comp_window[high_col] - comp_window[low_col]
        avg_bar_range = float(comp_bar_ranges.mean()) if len(comp_bar_ranges) > 0 else 0.0

        if avg_bar_range <= 0:
            return None, None, {"reason": "zero_avg_range"}

        curr = df.iloc[-1]
        curr_close = float(curr[close_col])
        curr_high = float(curr[high_col])
        curr_low = float(curr[low_col])
        curr_bar_range = curr_high - curr_low
        expansion_ratio = curr_bar_range / avg_bar_range

        atr_bonus = False
        if atr_col is not None:
            try:
                atr = float(curr[atr_col])
                if atr > 0 and curr_bar_range > atr * 0.8:
                    atr_bonus = True
            except Exception:
                pass

        if curr_close > comp_high and expansion_ratio >= expansion_ratio_min:
            structure_score = int(expansion_ratio > expansion_ratio_min * 1.3) + int(atr_bonus) + int(curr_close > comp_high * 1.001) + int(expansion_ratio > 2.0)
            return "BULLISH", curr_close, {
                "reason": "afternoon_breakout_bull",
                "structure_score": structure_score,
                "expansion_ratio": round(expansion_ratio, 2),
                "comp_high": round(comp_high, 2),
                "entry_context": f"pm_bo comp_high={comp_high:.2f} exp_ratio={expansion_ratio:.2f}",
            }

        if curr_close < comp_low and expansion_ratio >= expansion_ratio_min:
            structure_score = int(expansion_ratio > expansion_ratio_min * 1.3) + int(atr_bonus) + int(curr_close < comp_low * 0.999) + int(expansion_ratio > 2.0)
            return "BEARISH", curr_close, {
                "reason": "afternoon_breakout_bear",
                "structure_score": structure_score,
                "expansion_ratio": round(expansion_ratio, 2),
                "comp_low": round(comp_low, 2),
                "entry_context": f"pm_bo comp_low={comp_low:.2f} exp_ratio={expansion_ratio:.2f}",
            }

        return None, None, {"reason": "no_afternoon_breakout"}
    except Exception:
        return None, None, {"reason": "afternoon_breakout_error"}


def _signal_trend_reclaim(df, profile: dict | None = None) -> tuple:
    """
    Detects an EMA9 cross reclaim event: price was on the wrong side of EMA9
    two bars ago, then reclaimed EMA9 on prev bar and holds it on current bar,
    with EMA9/EMA20 trending in the correct direction.
    No feature_snapshot required.
    """
    try:
        if df is None or len(df) < 4:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        ema9_col = _find_col(df, ["ema9", "EMA9", "ema_9"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        vwap_col = _find_col(df, ["vwap", "VWAP"])

        if close_col is None or ema9_col is None or ema20_col is None:
            return None, None, {"reason": "missing_cols"}

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        prev2 = df.iloc[-3]

        curr_close = float(curr[close_col])
        curr_ema9 = float(curr[ema9_col])
        curr_ema20 = float(curr[ema20_col])
        prev_close = float(prev[close_col])
        prev_ema9 = float(prev[ema9_col])
        prev2_close = float(prev2[close_col])
        prev2_ema9 = float(prev2[ema9_col])

        ema_spread_min = 0.0
        if isinstance(profile, dict):
            ema_spread_min = _safe_float(profile.get("ema_spread_min"), 0.0)

        ema_spread = (curr_ema9 - curr_ema20) / curr_ema20 if curr_ema20 != 0 else 0.0

        vwap_aligned = False
        if vwap_col is not None:
            try:
                vwap = float(curr[vwap_col])
                if curr_ema9 > curr_ema20 and curr_close > vwap:
                    vwap_aligned = True
                elif curr_ema9 < curr_ema20 and curr_close < vwap:
                    vwap_aligned = True
            except Exception:
                pass

        # Bullish reclaim: prev2 was below EMA9, prev+curr reclaimed and held, EMA9 > EMA20
        bull_reclaim = (
            prev2_close < prev2_ema9
            and prev_close >= prev_ema9
            and curr_close >= curr_ema9
            and curr_ema9 > curr_ema20
            and abs(ema_spread) >= ema_spread_min
        )
        if bull_reclaim:
            structure_score = int(ema_spread > 0.001) + int(vwap_aligned) + int(curr_close > prev_close) + int(abs(ema_spread) > 0.002)
            return "BULLISH", curr_close, {
                "reason": "ema9_reclaim_bull",
                "structure_score": structure_score,
                "ema_spread": round(ema_spread, 5),
                "vwap_aligned": vwap_aligned,
                "entry_context": f"ema9_reclaim ema_spread={ema_spread:.4f} vwap_aligned={vwap_aligned}",
            }

        # Bearish reclaim: prev2 was above EMA9, prev+curr dropped below and held, EMA9 < EMA20
        bear_reclaim = (
            prev2_close > prev2_ema9
            and prev_close <= prev_ema9
            and curr_close <= curr_ema9
            and curr_ema9 < curr_ema20
            and abs(ema_spread) >= ema_spread_min
        )
        if bear_reclaim:
            structure_score = int(ema_spread < -0.001) + int(vwap_aligned) + int(curr_close < prev_close) + int(abs(ema_spread) > 0.002)
            return "BEARISH", curr_close, {
                "reason": "ema9_reclaim_bear",
                "structure_score": structure_score,
                "ema_spread": round(ema_spread, 5),
                "vwap_aligned": vwap_aligned,
                "entry_context": f"ema9_reclaim ema_spread={ema_spread:.4f} vwap_aligned={vwap_aligned}",
            }

        return None, None, {"reason": "no_trend_reclaim"}
    except Exception:
        return None, None, {"reason": "trend_reclaim_error"}


def _signal_extreme_extension_fade(
    df, context: dict | None = None, profile: dict | None = None, feature_snapshot: dict | None = None
) -> tuple:
    """
    Fades extreme VWAP extensions confirmed by overbought/oversold RSI.
    Requires features_enabled: true (feature_snapshot mandatory).
    Blocked when EMA spread is too large (strong trending environment).
    """
    try:
        if feature_snapshot is None:
            return None, None, {"reason": "features_required"}

        close = feature_snapshot.get("close")
        vwap_z = feature_snapshot.get("vwap_z")
        rsi = feature_snapshot.get("rsi")
        ema_spread = feature_snapshot.get("ema_spread")

        if close is None or vwap_z is None or rsi is None:
            return None, None, {"reason": "missing_features"}

        vwap_z_val = float(vwap_z)
        rsi_val = float(rsi)
        close_val = float(close)

        # Thresholds — profile-configurable
        vwap_z_threshold = 2.5
        rsi_bear = 76.0
        rsi_bull = 24.0
        max_ema_spread = 0.005

        if isinstance(profile, dict):
            vwap_z_threshold = _safe_float(profile.get("vwap_z_threshold"), 2.5)
            rsi_bear = _safe_float(profile.get("rsi_bear_threshold"), 76.0)
            rsi_bull = _safe_float(profile.get("rsi_bull_threshold"), 24.0)

        # Block in strong-trend environment
        if ema_spread is not None:
            try:
                if abs(float(ema_spread)) > max_ema_spread:
                    return None, None, {"reason": "ema_spread_too_large", "ema_spread": round(float(ema_spread), 5)}
            except (TypeError, ValueError):
                pass

        # Bearish fade: extreme extension above VWAP + overbought RSI
        if vwap_z_val >= vwap_z_threshold and rsi_val >= rsi_bear:
            structure_score = (
                int(vwap_z_val >= vwap_z_threshold * 1.2)
                + int(rsi_val >= rsi_bear + 4)
                + int(ema_spread is not None and abs(float(ema_spread)) < max_ema_spread * 0.5)
                + int(vwap_z_val >= 3.0)
            )
            return "BEARISH", close_val, {
                "reason": "extreme_extension_fade_bear",
                "structure_score": structure_score,
                "vwap_z": round(vwap_z_val, 3),
                "rsi": round(rsi_val, 1),
                "entry_context": f"ext_fade vwap_z={vwap_z_val:.2f} rsi={rsi_val:.1f}",
            }

        # Bullish fade: extreme extension below VWAP + oversold RSI
        if vwap_z_val <= -vwap_z_threshold and rsi_val <= rsi_bull:
            structure_score = (
                int(vwap_z_val <= -vwap_z_threshold * 1.2)
                + int(rsi_val <= rsi_bull - 4)
                + int(ema_spread is not None and abs(float(ema_spread)) < max_ema_spread * 0.5)
                + int(vwap_z_val <= -3.0)
            )
            return "BULLISH", close_val, {
                "reason": "extreme_extension_fade_bull",
                "structure_score": structure_score,
                "vwap_z": round(vwap_z_val, 3),
                "rsi": round(rsi_val, 1),
                "entry_context": f"ext_fade vwap_z={vwap_z_val:.2f} rsi={rsi_val:.1f}",
            }

        return None, None, {"reason": "no_extreme_extension"}
    except Exception:
        return None, None, {"reason": "extreme_extension_fade_error"}


# ---------------------------------------------------------------------------
# Primary dispatch
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SMC signal helpers — shared state, aggregation, FVG detection, cooldowns
# ---------------------------------------------------------------------------

# Per-session cooldown state: resets on process restart (intentional)
_signal_cooldowns: dict = {}


def _check_cooldown(mode: str, direction: str, current_bar_count: int, cooldown_bars: int) -> bool:
    key = (mode, direction)
    last = _signal_cooldowns.get(key, -999999)
    return (current_bar_count - last) >= cooldown_bars


def _set_cooldown(mode: str, direction: str, current_bar_count: int) -> None:
    _signal_cooldowns[(mode, direction)] = current_bar_count


def _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, n_minutes):
    """Aggregate 1m OHLCV into n-minute bars. Returns list of dicts."""
    rows = len(df)
    bars = []
    for i in range(0, rows - n_minutes + 1, n_minutes):
        chunk = df.iloc[i:i + n_minutes]
        bar = {
            "open":      float(chunk[open_col].iloc[0]),
            "high":      float(chunk[high_col].max()),
            "low":       float(chunk[low_col].min()),
            "close":     float(chunk[close_col].iloc[-1]),
            "volume":    float(chunk[vol_col].sum()) if vol_col else 0.0,
            "bar_index": i + n_minutes - 1,
        }
        bars.append(bar)
    return bars


def _detect_fvg_zones(bars, min_gap_pct, max_age_bars):
    """Scan aggregated bars for FVG patterns. Returns list of unfilled FVG zone dicts."""
    zones = []
    if len(bars) < 3:
        return zones
    scan_start = max(0, len(bars) - 3 - max_age_bars)
    for i in range(scan_start, len(bars) - 2):
        c1, c2, c3 = bars[i], bars[i + 1], bars[i + 2]
        mid_price = c2["close"]
        if mid_price <= 0:
            continue
        # Bullish FVG: gap above candle 1's high, below candle 3's low
        if c1["high"] < c3["low"]:
            gap_size = (c3["low"] - c1["high"]) / mid_price
            if gap_size >= min_gap_pct:
                zones.append({
                    "direction":   "BULLISH",
                    "zone_top":    c3["low"],
                    "zone_bottom": c1["high"],
                    "bar_index":   c2["bar_index"],
                    "filled":      False,
                })
        # Bearish FVG: gap below candle 1's low, above candle 3's high
        if c1["low"] > c3["high"]:
            gap_size = (c1["low"] - c3["high"]) / mid_price
            if gap_size >= min_gap_pct:
                zones.append({
                    "direction":   "BEARISH",
                    "zone_top":    c1["low"],
                    "zone_bottom": c3["high"],
                    "bar_index":   c2["bar_index"],
                    "filled":      False,
                })
    # Mark filled zones
    bar_index_map = {b["bar_index"]: bi for bi, b in enumerate(bars)}
    for zone in zones:
        zone_pos = bar_index_map.get(zone["bar_index"])
        if zone_pos is not None:
            for b in bars[zone_pos + 2:]:
                if b["high"] >= zone["zone_top"] and b["low"] <= zone["zone_bottom"]:
                    zone["filled"] = True
                    break
    return [z for z in zones if not z["filled"]]


# ---------------------------------------------------------------------------
# SMC Signal constants
# ---------------------------------------------------------------------------

FVG_4H_AGG_MINUTES   = 240
FVG_4H_MIN_GAP_PCT   = 0.0005
FVG_4H_MAX_AGE_BARS  = 12
FVG_4H_COOLDOWN_BARS = 60

FVG_5M_AGG_MINUTES      = 5
FVG_5M_MIN_GAP_PCT      = 0.0002
FVG_5M_MAX_AGE_BARS     = 20
FVG_5M_COOLDOWN_BARS    = 10
FVG_5M_VOL_MULTIPLIER   = 1.0

SWEEP_SWING_LOOKBACK      = 5
SWEEP_CONFIRMATION_BARS   = 3
SWEEP_MIN_BODY_RATIO      = 0.5
SWEEP_COOLDOWN_BARS       = 15
SWEEP_MIN_BREAK_PCT       = 0.0001

COMBO_SWEEP_WINDOW   = 3
COMBO_COOLDOWN_BARS  = 20

FLOW_VWAP_SLOPE_LOOKBACK   = 10
FLOW_VOL_SMA_LOOKBACK      = 20
FLOW_VOL_SPIKE_THRESHOLD   = 1.5
FLOW_MOMENTUM_LOOKBACK     = 10
FLOW_MOMENTUM_MIN_ATR      = 0.5
FLOW_COOLDOWN_BARS         = 30


# ---------------------------------------------------------------------------
# SMC Signal functions
# ---------------------------------------------------------------------------

def _signal_fvg_4h(df) -> tuple:
    try:
        if len(df) < FVG_4H_AGG_MINUTES * 4:
            return None, None, {"reason": "insufficient_data"}
        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high", "High"])
        low_col   = _find_col(df, ["low", "Low"])
        open_col  = _find_col(df, ["open", "Open"])
        vol_col   = _find_col(df, ["volume", "Volume", "vol"])
        if not all([close_col, high_col, low_col, open_col]):
            return None, None, {"reason": "missing_columns"}
        bars = _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, FVG_4H_AGG_MINUTES)
        if len(bars) < 3:
            return None, None, {"reason": "insufficient_bars"}
        zones = _detect_fvg_zones(bars, FVG_4H_MIN_GAP_PCT, FVG_4H_MAX_AGE_BARS)
        current_close = float(df[close_col].iloc[-1])
        n = len(df)
        for zone in reversed(zones):
            if zone["zone_bottom"] <= current_close <= zone["zone_top"]:
                d = zone["direction"]
                if _check_cooldown("FVG_4H", d, n, FVG_4H_COOLDOWN_BARS):
                    _set_cooldown("FVG_4H", d, n)
                    return d, current_close, {"signal": "fvg_4h", "zone": zone["direction"]}
        return None, None, {"reason": "no_fvg_zone"}
    except Exception:
        return None, None, {"reason": "fvg_4h_error"}


def _signal_fvg_5m(df) -> tuple:
    try:
        if len(df) < FVG_5M_AGG_MINUTES * 6:
            return None, None, {"reason": "insufficient_data"}
        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high", "High"])
        low_col   = _find_col(df, ["low", "Low"])
        open_col  = _find_col(df, ["open", "Open"])
        vol_col   = _find_col(df, ["volume", "Volume", "vol"])
        if not all([close_col, high_col, low_col, open_col]):
            return None, None, {"reason": "missing_columns"}
        bars = _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, FVG_5M_AGG_MINUTES)
        if len(bars) < 3:
            return None, None, {"reason": "insufficient_bars"}
        vol_sma = sum(b["volume"] for b in bars[-20:]) / min(len(bars), 20) if vol_col else 0.0
        zones = _detect_fvg_zones(bars, FVG_5M_MIN_GAP_PCT, FVG_5M_MAX_AGE_BARS)
        if vol_col and vol_sma > 0:
            bar_vol_map = {b["bar_index"]: b["volume"] for b in bars}
            zones = [z for z in zones if bar_vol_map.get(z["bar_index"], 0) >= vol_sma * FVG_5M_VOL_MULTIPLIER]
        current_close = float(df[close_col].iloc[-1])
        n = len(df)
        for zone in reversed(zones):
            if zone["zone_bottom"] <= current_close <= zone["zone_top"]:
                d = zone["direction"]
                if _check_cooldown("FVG_5M", d, n, FVG_5M_COOLDOWN_BARS):
                    _set_cooldown("FVG_5M", d, n)
                    return d, current_close, {"signal": "fvg_5m", "zone": zone["direction"]}
        return None, None, {"reason": "no_fvg_zone"}
    except Exception:
        return None, None, {"reason": "fvg_5m_error"}


def _signal_liquidity_sweep(df) -> tuple:
    try:
        if len(df) < 60:
            return None, None, {"reason": "insufficient_data"}
        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high", "High"])
        low_col   = _find_col(df, ["low", "Low"])
        open_col  = _find_col(df, ["open", "Open"])
        if not all([close_col, high_col, low_col, open_col]):
            return None, None, {"reason": "missing_columns"}
        closes = df[close_col].values
        highs  = df[high_col].values
        lows   = df[low_col].values
        opens  = df[open_col].values
        n = len(df)
        # Identify swing highs/lows in last 60 bars
        swing_highs, swing_lows = [], []
        scan_start = max(SWEEP_SWING_LOOKBACK, n - 60)
        for i in range(scan_start, n - SWEEP_SWING_LOOKBACK):
            left_h  = highs[i - SWEEP_SWING_LOOKBACK:i]
            right_h = highs[i + 1:i + 1 + SWEEP_SWING_LOOKBACK]
            if len(left_h) > 0 and len(right_h) > 0:
                if highs[i] > max(left_h) and highs[i] > max(right_h):
                    swing_highs.append((i, float(highs[i])))
            left_l  = lows[i - SWEEP_SWING_LOOKBACK:i]
            right_l = lows[i + 1:i + 1 + SWEEP_SWING_LOOKBACK]
            if len(left_l) > 0 and len(right_l) > 0:
                if lows[i] < min(left_l) and lows[i] < min(right_l):
                    swing_lows.append((i, float(lows[i])))
        # Check buyside sweep: broke above swing high, closed back below
        for sh_idx, sh_level in reversed(swing_highs):
            window_start = max(sh_idx + 1, n - SWEEP_CONFIRMATION_BARS - 2)
            for j in range(window_start, n):
                if float(highs[j]) > sh_level * (1 + SWEEP_MIN_BREAK_PCT):
                    for k in range(j + 1, min(j + SWEEP_CONFIRMATION_BARS + 1, n)):
                        if float(closes[k]) < sh_level:
                            body = abs(float(closes[k]) - float(opens[k]))
                            rng  = float(highs[k]) - float(lows[k])
                            if rng > 0 and body / rng >= SWEEP_MIN_BODY_RATIO:
                                if _check_cooldown("SWEEP", "BEARISH", n, SWEEP_COOLDOWN_BARS):
                                    _set_cooldown("SWEEP", "BEARISH", n)
                                    return "BEARISH", float(closes[-1]), {"signal": "sweep_buyside"}
                    break
        # Check sellside sweep: broke below swing low, closed back above
        for sl_idx, sl_level in reversed(swing_lows):
            window_start = max(sl_idx + 1, n - SWEEP_CONFIRMATION_BARS - 2)
            for j in range(window_start, n):
                if float(lows[j]) < sl_level * (1 - SWEEP_MIN_BREAK_PCT):
                    for k in range(j + 1, min(j + SWEEP_CONFIRMATION_BARS + 1, n)):
                        if float(closes[k]) > sl_level:
                            body = abs(float(closes[k]) - float(opens[k]))
                            rng  = float(highs[k]) - float(lows[k])
                            if rng > 0 and body / rng >= SWEEP_MIN_BODY_RATIO:
                                if _check_cooldown("SWEEP", "BULLISH", n, SWEEP_COOLDOWN_BARS):
                                    _set_cooldown("SWEEP", "BULLISH", n)
                                    return "BULLISH", float(closes[-1]), {"signal": "sweep_sellside"}
                    break
        return None, None, {"reason": "no_sweep"}
    except Exception:
        return None, None, {"reason": "sweep_error"}


def _signal_fvg_sweep_combo(df) -> tuple:
    try:
        sweep_dir, sweep_price, sweep_ctx = _signal_liquidity_sweep(df)
        if sweep_dir is None:
            return None, None, {"reason": "no_sweep_for_combo"}
        if len(df) < FVG_5M_AGG_MINUTES * 6:
            return None, None, {"reason": "insufficient_data"}
        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high", "High"])
        low_col   = _find_col(df, ["low", "Low"])
        open_col  = _find_col(df, ["open", "Open"])
        vol_col   = _find_col(df, ["volume", "Volume", "vol"])
        if not all([close_col, high_col, low_col, open_col]):
            return None, None, {"reason": "missing_columns"}
        bars  = _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, FVG_5M_AGG_MINUTES)
        zones = _detect_fvg_zones(bars, FVG_5M_MIN_GAP_PCT, FVG_5M_MAX_AGE_BARS)
        current_close = float(df[close_col].iloc[-1])
        n = len(df)
        for zone in reversed(zones):
            if zone["direction"] == sweep_dir and zone["zone_bottom"] <= current_close <= zone["zone_top"]:
                if _check_cooldown("COMBO", sweep_dir, n, COMBO_COOLDOWN_BARS):
                    _set_cooldown("COMBO", sweep_dir, n)
                    return sweep_dir, current_close, {"signal": "fvg_sweep_combo"}
        return None, None, {"reason": "no_fvg_confluence"}
    except Exception:
        return None, None, {"reason": "combo_error"}


def _signal_flow_divergence(df) -> tuple:
    try:
        min_rows = max(FLOW_VOL_SMA_LOOKBACK, FLOW_MOMENTUM_LOOKBACK) + 5
        if len(df) < min_rows:
            return None, None, {"reason": "insufficient_data"}
        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high", "High"])
        low_col   = _find_col(df, ["low", "Low"])
        vol_col   = _find_col(df, ["volume", "Volume", "vol"])
        vwap_col  = _find_col(df, ["vwap", "VWAP"])
        if not all([close_col, high_col, low_col]):
            return None, None, {"reason": "missing_columns"}
        closes = df[close_col].values
        highs  = df[high_col].values
        lows   = df[low_col].values
        n = len(df)
        current_close = float(closes[-1])
        # Get baseline signal direction
        std_dir, _, _ = _signal_trend_pullback(df)
        if std_dir is None:
            return None, None, {"reason": "no_baseline_signal"}
        # VWAP slope
        vwap_slope_bullish = None
        if vwap_col:
            vwaps = df[vwap_col].values
            if len(vwaps) >= FLOW_VWAP_SLOPE_LOOKBACK + 1:
                vwap_slope_bullish = float(vwaps[-1]) > float(vwaps[-FLOW_VWAP_SLOPE_LOOKBACK - 1])
        # Volume spike
        vol_spike = False
        if vol_col:
            vols = df[vol_col].values
            vol_current = float(vols[-1])
            vol_sma = sum(float(v) for v in vols[-FLOW_VOL_SMA_LOOKBACK:]) / FLOW_VOL_SMA_LOOKBACK
            if vol_sma > 0:
                vol_spike = vol_current >= vol_sma * FLOW_VOL_SPIKE_THRESHOLD
        # Price momentum normalized by ATR
        price_momentum = float(closes[-1]) - float(closes[-FLOW_MOMENTUM_LOOKBACK - 1])
        atr_vals = [float(highs[i]) - float(lows[i]) for i in range(n - 14, n)]
        atr = sum(atr_vals) / len(atr_vals) if atr_vals else 1.0
        norm_momentum = price_momentum / atr if atr > 0 else 0.0
        momentum_bullish = norm_momentum > FLOW_MOMENTUM_MIN_ATR
        momentum_bearish = norm_momentum < -FLOW_MOMENTUM_MIN_ATR
        # Divergence: standard says BULLISH but flow says BEARISH
        if std_dir == "BULLISH" and momentum_bearish and vol_spike:
            vwap_ok = (vwap_slope_bullish is None) or (not vwap_slope_bullish)
            if vwap_ok and _check_cooldown("FLOW", "BEARISH", n, FLOW_COOLDOWN_BARS):
                _set_cooldown("FLOW", "BEARISH", n)
                return "BEARISH", current_close, {"signal": "flow_divergence", "override": "bearish"}
        # Divergence: standard says BEARISH but flow says BULLISH
        if std_dir == "BEARISH" and momentum_bullish and vol_spike:
            vwap_ok = (vwap_slope_bullish is None) or vwap_slope_bullish
            if vwap_ok and _check_cooldown("FLOW", "BULLISH", n, FLOW_COOLDOWN_BARS):
                _set_cooldown("FLOW", "BULLISH", n)
                return "BULLISH", current_close, {"signal": "flow_divergence", "override": "bullish"}
        return None, None, {"reason": "no_divergence"}
    except Exception:
        return None, None, {"reason": "flow_divergence_error"}


def _signal_multi_tf_confirm(df) -> tuple:
    """
    Multi-timeframe EMA alignment: 5m EMA9>EMA20 AND 15m EMA9>EMA20 AND close>VWAP
    for BULLISH; reverse for BEARISH.  EMAs are computed from aggregated 1m bars.
    Requires at least 150 1m bars (10 × 15m bars with EMA warmup).
    """
    MIN_BARS = 150
    MIN_5M_BARS = 20   # need 20 5m bars for EMA20
    MIN_15M_BARS = 10  # need 10 15m bars minimum

    def _ema_last(prices, span):
        k = 2.0 / (span + 1)
        val = float(prices[0])
        for p in prices[1:]:
            val = float(p) * k + val * (1 - k)
        return val

    try:
        if df is None or len(df) < MIN_BARS:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high",  "High"])
        low_col   = _find_col(df, ["low",   "Low"])
        open_col  = _find_col(df, ["open",  "Open"])
        vol_col   = _find_col(df, ["volume", "Volume"])
        vwap_col  = _find_col(df, ["vwap",  "VWAP"])

        if close_col is None or vwap_col is None:
            return None, None, {"reason": "missing_cols"}
        if high_col is None or low_col is None or open_col is None:
            return None, None, {"reason": "missing_ohlc_cols"}

        close = float(df.iloc[-1][close_col])
        vwap  = float(df.iloc[-1][vwap_col])

        bars_5m  = _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, 5)
        bars_15m = _aggregate_bars(df, close_col, high_col, low_col, open_col, vol_col, 15)

        if len(bars_5m) < MIN_5M_BARS or len(bars_15m) < MIN_15M_BARS:
            return None, None, {"reason": "insufficient_tf_bars"}

        closes_5m  = [b["close"] for b in bars_5m]
        closes_15m = [b["close"] for b in bars_15m]

        ema9_5m   = _ema_last(closes_5m,  9)
        ema20_5m  = _ema_last(closes_5m,  20)
        ema9_15m  = _ema_last(closes_15m, 9)
        ema20_15m = _ema_last(closes_15m, 20)

        bull_5m  = ema9_5m  > ema20_5m
        bull_15m = ema9_15m > ema20_15m

        if bull_5m and bull_15m and close > vwap:
            return "BULLISH", close, {
                "reason": "multi_tf_bull",
                "ema9_5m":  round(ema9_5m,  4),
                "ema20_5m": round(ema20_5m, 4),
                "ema9_15m": round(ema9_15m,  4),
            }

        if (not bull_5m) and (not bull_15m) and close < vwap:
            return "BEARISH", close, {
                "reason": "multi_tf_bear",
                "ema9_5m":  round(ema9_5m,  4),
                "ema20_5m": round(ema20_5m, 4),
                "ema9_15m": round(ema9_15m,  4),
            }

        return None, None, {"reason": "no_multi_tf_align"}
    except Exception:
        return None, None, {"reason": "multi_tf_confirm_error"}


def _signal_gap_fade(df) -> tuple:
    """
    Fade an opening gap: if the bar 30 bars ago opened >0.3% away from the
    bar before it, and price has not yet filled the gap, signal a reversion.
    Fires only when price is still within ~30 bars of the gap event.
    """
    GAP_MIN_PCT = 0.003
    WINDOW = 30

    try:
        if df is None or len(df) < WINDOW + 2:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        open_col  = _find_col(df, ["open",  "Open"])

        if close_col is None or open_col is None:
            return None, None, {"reason": "missing_cols"}

        # Bar at session open (WINDOW bars ago) and the bar just before it
        session_open_bar = df.iloc[-(WINDOW + 1)]
        pre_gap_bar      = df.iloc[-(WINDOW + 2)]
        curr             = df.iloc[-1]

        prev_close        = float(pre_gap_bar[close_col])
        session_open_px   = float(session_open_bar[open_col])
        curr_close        = float(curr[close_col])

        if prev_close <= 0:
            return None, None, {"reason": "invalid_prev_close"}

        gap_pct = (session_open_px - prev_close) / prev_close

        if abs(gap_pct) < GAP_MIN_PCT:
            return None, None, {"reason": "gap_too_small"}

        # Gap up → fade bearish (price hasn't yet returned below prev_close)
        if gap_pct > GAP_MIN_PCT and curr_close > prev_close:
            return "BEARISH", curr_close, {
                "reason":       "gap_fade_down",
                "gap_pct":      round(gap_pct, 4),
                "prev_close":   round(prev_close, 2),
                "session_open": round(session_open_px, 2),
            }

        # Gap down → fade bullish (price hasn't yet returned above prev_close)
        if gap_pct < -GAP_MIN_PCT and curr_close < prev_close:
            return "BULLISH", curr_close, {
                "reason":       "gap_fade_up",
                "gap_pct":      round(gap_pct, 4),
                "prev_close":   round(prev_close, 2),
                "session_open": round(session_open_px, 2),
            }

        return None, None, {"reason": "gap_already_filled"}
    except Exception:
        return None, None, {"reason": "gap_fade_error"}


def _signal_vpoc_reversion(df) -> tuple:
    """
    Volume Point of Control (VPOC) reversion: build a $0.50-bucket volume
    profile over the last 30 bars, find the highest-volume price level,
    then signal reversion when price is >0.5% from VPOC and RSI diverges.
    """
    BUCKET_SIZE      = 0.50
    MIN_BARS         = 30
    RSI_OVERSOLD     = 40
    RSI_OVERBOUGHT   = 60
    DIST_THRESHOLD   = 0.005   # 0.5% away from VPOC

    try:
        if df is None or len(df) < MIN_BARS:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        vol_col   = _find_col(df, ["volume", "Volume"])
        rsi_col   = _find_col(df, ["rsi", "RSI", "rsi14", "RSI14"])

        if close_col is None or vol_col is None or rsi_col is None:
            return None, None, {"reason": "missing_cols"}

        recent = df.tail(MIN_BARS)

        # Build $0.50-bucket volume profile
        vol_by_bucket: dict = {}
        for _, row in recent.iterrows():
            bucket = round(float(row[close_col]) / BUCKET_SIZE) * BUCKET_SIZE
            vol_by_bucket[bucket] = vol_by_bucket.get(bucket, 0.0) + float(row[vol_col])

        if not vol_by_bucket:
            return None, None, {"reason": "no_volume_data"}

        vpoc = max(vol_by_bucket, key=vol_by_bucket.__getitem__)

        curr_close = float(recent.iloc[-1][close_col])
        curr_rsi   = float(recent.iloc[-1][rsi_col])

        if vpoc <= 0:
            return None, None, {"reason": "invalid_vpoc"}

        dist_pct = (curr_close - vpoc) / vpoc

        if dist_pct > DIST_THRESHOLD and curr_rsi > RSI_OVERBOUGHT:
            return "BEARISH", curr_close, {
                "reason":   "vpoc_reversion_short",
                "vpoc":     round(vpoc, 2),
                "dist_pct": round(dist_pct, 4),
                "rsi":      round(curr_rsi, 1),
            }

        if dist_pct < -DIST_THRESHOLD and curr_rsi < RSI_OVERSOLD:
            return "BULLISH", curr_close, {
                "reason":   "vpoc_reversion_long",
                "vpoc":     round(vpoc, 2),
                "dist_pct": round(dist_pct, 4),
                "rsi":      round(curr_rsi, 1),
            }

        return None, None, {"reason": "no_vpoc_reversion"}
    except Exception:
        return None, None, {"reason": "vpoc_reversion_error"}


def _signal_opening_range_reclaim(df) -> tuple:
    """
    Detects a reclaim of the opening range after a flush-through.
    - Bullish: prev bar closed below or_low, current bar reclaims above it,
      close > vwap, and RSI < 60 (hasn't already ripped).
    - Bearish: prev bar closed above or_high, current bar fails back inside,
      close < vwap, and RSI > 40.
    Opening range = first 6 bars of the dataframe.  Requires 30+ bars total.
    """
    try:
        if df is None or len(df) < 30:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close", "Close"])
        high_col  = _find_col(df, ["high",  "High"])
        low_col   = _find_col(df, ["low",   "Low"])
        rsi_col   = _find_col(df, ["rsi",   "RSI", "rsi14", "RSI14"])
        vwap_col  = _find_col(df, ["vwap",  "VWAP"])

        if None in (close_col, high_col, low_col, rsi_col, vwap_col):
            return None, None, {"reason": "missing_cols"}

        or_high = float(df[high_col].iloc[:6].max())
        or_low  = float(df[low_col].iloc[:6].min())

        # Don't evaluate while the opening range is still forming
        if len(df) <= 6:
            return None, None, {"reason": "in_range_formation"}

        prev_close = float(df.iloc[-2][close_col])
        curr       = df.iloc[-1]
        close      = float(curr[close_col])
        rsi        = float(curr[rsi_col])
        vwap       = float(curr[vwap_col])

        if prev_close < or_low and close > or_low and close > vwap and rsi < 60:
            return "BULLISH", close, {
                "reason": "orr_bull_reclaim",
                "or_low": round(or_low, 2),
                "rsi":    round(rsi, 1),
            }

        if prev_close > or_high and close < or_high and close < vwap and rsi > 40:
            return "BEARISH", close, {
                "reason": "orr_bear_reclaim",
                "or_high": round(or_high, 2),
                "rsi":     round(rsi, 1),
            }

        return None, None, {"reason": "no_orr_signal"}
    except Exception:
        return None, None, {"reason": "opening_range_reclaim_error"}


def _signal_vol_compression_breakout(df) -> tuple:
    """
    Detects a volatility compression-to-expansion event.
    Previous bar's ATR must be below 60% of the 20-bar ATR mean (compression),
    and current bar's ATR must have recovered to 90%+ of the mean (expansion).
    Direction is determined by close vs EMA9.
    """
    COMPRESSION_LOOKBACK  = 20
    COMPRESSION_THRESHOLD = 0.6

    try:
        if df is None or len(df) < COMPRESSION_LOOKBACK + 2:
            return None, None, {"reason": "insufficient_bars"}

        atr_col = _find_col(df, ["atr", "ATR"])
        if atr_col is None:
            return None, None, {"reason": "missing_cols"}

        atr_vals  = df[atr_col].values
        window    = atr_vals[-(COMPRESSION_LOOKBACK + 1):-1]
        atr_mean  = float(sum(window) / len(window))
        atr_prev  = float(atr_vals[-2])
        atr_now   = float(atr_vals[-1])

        if atr_mean <= 0:
            return None, None, {"reason": "zero_atr_mean"}

        if not (atr_prev < atr_mean * COMPRESSION_THRESHOLD):
            return None, None, {"reason": "no_compression"}

        if not (atr_now > atr_mean * 0.9):
            return None, None, {"reason": "no_expansion"}

        close_col = _find_col(df, ["close", "Close"])
        ema9_col  = _find_col(df, ["ema9",  "EMA9", "ema_9"])

        if close_col is None or ema9_col is None:
            return None, None, {"reason": "missing_cols"}

        close = float(df.iloc[-1][close_col])
        ema9  = float(df.iloc[-1][ema9_col])

        if close > ema9:
            return "BULLISH", close, {
                "reason":    "vcb_bull",
                "atr_mean":  round(atr_mean, 4),
                "atr_ratio": round(atr_now / atr_mean, 3),
            }

        if close < ema9:
            return "BEARISH", close, {
                "reason":    "vcb_bear",
                "atr_mean":  round(atr_mean, 4),
                "atr_ratio": round(atr_now / atr_mean, 3),
            }

        return None, None, {"reason": "no_direction"}
    except Exception:
        return None, None, {"reason": "vol_compression_breakout_error"}


def _signal_vol_spike_fade(df) -> tuple:
    """
    Fades intraday volatility spikes when RSI is stretched and price is far
    from VWAP.  Complements VOL_COMPRESSION_BREAKOUT: that signal trades
    WITH the expansion; this one trades AGAINST it when it's overextended.
    - Current ATR must be 1.5x the 19-bar ATR mean (spike detected).
    - abs(close - vwap) must exceed 1.5x the ATR mean (meaningful dislocation).
    - Direction: close > vwap + RSI > 70 → BEARISH; close < vwap + RSI < 30 → BULLISH.
    """
    SPIKE_ATR_MULTIPLIER   = 1.5
    VWAP_DIST_ATR_MULT     = 1.5

    try:
        if df is None or len(df) < 20:
            return None, None, {"reason": "insufficient_bars"}

        close_col = _find_col(df, ["close",  "Close"])
        vwap_col  = _find_col(df, ["vwap",   "VWAP"])
        atr_col   = _find_col(df, ["atr",    "ATR"])
        rsi_col   = _find_col(df, ["rsi",    "RSI", "rsi14", "RSI14"])

        if None in (close_col, vwap_col, atr_col, rsi_col):
            return None, None, {"reason": "missing_cols"}

        atr_vals  = df[atr_col].values
        window    = atr_vals[-20:-1]          # 19 bars, excluding current
        atr_mean  = float(sum(window) / len(window))
        atr_now   = float(atr_vals[-1])

        if atr_mean <= 0:
            return None, None, {"reason": "zero_atr_mean"}

        if atr_now <= atr_mean * SPIKE_ATR_MULTIPLIER:
            return None, None, {"reason": "no_vol_spike"}

        curr  = df.iloc[-1]
        close = float(curr[close_col])
        vwap  = float(curr[vwap_col])
        rsi   = float(curr[rsi_col])

        vwap_dist = abs(close - vwap)
        if vwap_dist <= atr_mean * VWAP_DIST_ATR_MULT:
            return None, None, {"reason": "price_near_vwap"}

        if close > vwap and rsi > 70:
            return "BEARISH", close, {
                "reason":    "vsf_fade_bear",
                "rsi":       round(rsi, 1),
                "atr_ratio": round(atr_now / atr_mean, 3),
            }

        if close < vwap and rsi < 30:
            return "BULLISH", close, {
                "reason":    "vsf_fade_bull",
                "rsi":       round(rsi, 1),
                "atr_ratio": round(atr_now / atr_mean, 3),
            }

        return None, None, {"reason": "no_vsf_signal"}
    except Exception:
        return None, None, {"reason": "vol_spike_fade_error"}


def derive_sim_signal(
    df,
    signal_mode,
    context: dict | None = None,
    feature_snapshot: dict | None = None,
    profile: dict | None = None,
):
    """
    Dispatch entry-signal generation by mode.

    Always returns a 3-tuple: (direction, price, context_dict).
    direction is "BULLISH", "BEARISH", or None.
    price is float or None.
    context_dict is a dict or None.
    """
    try:
        mode = str(signal_mode).upper()

        if mode == "MEAN_REVERSION":
            return _signal_mean_reversion(df)

        elif mode == "BREAKOUT":
            direction, price, ctx = _signal_breakout(df)
            if direction is None:
                return None, None, ctx
            vol_z_min = _ctx_float(context, "vol_z_min", None)
            if vol_z_min is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                vol_z = feature_snapshot.get("vol_z")
                try:
                    if vol_z is None or float(vol_z) < float(vol_z_min):
                        return None, None, {"reason": "vol_z_filter"}
                except Exception:
                    return None, None, {"reason": "vol_z_invalid"}
            return direction, price, ctx

        elif mode == "TREND_PULLBACK":
            direction, price, ctx = _signal_trend_pullback(df)
            if direction is None:
                return None, None, ctx
            min_exp = _ctx_float(context, "atr_expansion_min", None)
            if min_exp is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                atr_exp = feature_snapshot.get("atr_expansion")
                try:
                    if atr_exp is None or float(atr_exp) < min_exp:
                        return None, None, {"reason": "atr_expansion_filter"}
                except Exception:
                    return None, None, {"reason": "atr_expansion_invalid"}
            vol_z_min = _ctx_float(context, "vol_z_min", None)
            if vol_z_min is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                vol_z = feature_snapshot.get("vol_z")
                try:
                    if vol_z is None or float(vol_z) < vol_z_min:
                        return None, None, {"reason": "vol_z_filter"}
                except Exception:
                    return None, None, {"reason": "vol_z_invalid"}
            iv_rank_max = _ctx_float(context, "iv_rank_max", None)
            if iv_rank_max is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                iv_rank = feature_snapshot.get("iv_rank_proxy")
                try:
                    if iv_rank is None or float(iv_rank) > iv_rank_max:
                        return None, None, {"reason": "iv_rank_filter"}
                except Exception:
                    return None, None, {"reason": "iv_rank_invalid"}
            return direction, price, ctx

        elif mode == "SWING_TREND":
            return _signal_swing_trend(df)

        elif mode == "OPPORTUNITY":
            return _signal_opportunity(df, context)

        elif mode == "ORB_BREAKOUT":
            if feature_snapshot is None:
                return None, None, {"reason": "features_required"}
            direction, price, reason = _signal_orb_breakout(feature_snapshot, context or {})
            if direction is None:
                return None, None, {"reason": reason or "orb_no_signal"}
            return direction, price, {"reason": reason or "orb_break"}

        elif mode == "VWAP_REVERSION":
            return _signal_vwap_reversion(df, context, feature_snapshot)

        elif mode == "ZSCORE_BOUNCE":
            return _signal_zscore_bounce(df, context, feature_snapshot)

        elif mode == "FAILED_BREAKOUT_REVERSAL":
            return _signal_failed_breakout_reversal(df)

        elif mode == "VWAP_CONTINUATION":
            return _signal_vwap_continuation(df, profile)

        elif mode == "OPENING_DRIVE":
            return _signal_opening_drive(df, profile)

        elif mode == "AFTERNOON_BREAKOUT":
            return _signal_afternoon_breakout(df, profile)

        elif mode == "TREND_RECLAIM":
            return _signal_trend_reclaim(df, profile)

        elif mode == "EXTREME_EXTENSION_FADE":
            return _signal_extreme_extension_fade(df, context, profile, feature_snapshot)

        elif mode == "FVG_4H":
            return _signal_fvg_4h(df)

        elif mode == "FVG_5M":
            return _signal_fvg_5m(df)

        elif mode == "LIQUIDITY_SWEEP":
            return _signal_liquidity_sweep(df)

        elif mode == "FVG_SWEEP_COMBO":
            return _signal_fvg_sweep_combo(df)

        elif mode == "FLOW_DIVERGENCE":
            return _signal_flow_divergence(df)

        elif mode == "MULTI_TF_CONFIRM":
            return _signal_multi_tf_confirm(df)

        elif mode == "GAP_FADE":
            return _signal_gap_fade(df)

        elif mode == "VPOC_REVERSION":
            return _signal_vpoc_reversion(df)

        elif mode == "OPENING_RANGE_RECLAIM":
            return _signal_opening_range_reclaim(df)

        elif mode == "VOL_COMPRESSION_BREAKOUT":
            return _signal_vol_compression_breakout(df)

        elif mode == "VOL_SPIKE_FADE":
            return _signal_vol_spike_fade(df)

        else:
            return None, None, {"reason": f"unknown_signal_mode:{signal_mode}"}

    except Exception:
        return None, None, {"reason": "dispatch_error"}
