# simulation/sim_signal_funcs2.py
#
# Second half of classic signal functions, split from sim_signal_funcs.py
# to keep both files under 500 lines.
# Imports shared helpers from sim_signal_funcs to avoid duplication.

from simulation.sim_signal_funcs import _find_col, _ctx_float, _safe_float


def _signal_failed_breakout_reversal(df, context=None) -> tuple:
    """
    Detects a failed breakout/breakdown: previous bar pierced a reference
    level but closed back inside it; current bar confirms the reversal.
    No feature_snapshot required.
    """
    LOOKBACK = int(_ctx_float(context, "fbr_lookback", 20))
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
