import logging
from signals.opportunity import evaluate_opportunity
from signals.volatility import volatility_state
from signals.predictor import make_prediction


def _find_col(df, candidates):
    if df is None:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _signal_mean_reversion(df) -> tuple[str | None, float | None]:
    RSI_OVERSOLD = 30
    RSI_OVERBOUGHT = 70
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        rsi_col = _find_col(df, ["rsi", "RSI", "rsi14", "RSI14"])
        vwap_col = _find_col(df, ["vwap", "VWAP"])
        if close_col is None or rsi_col is None or vwap_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        rsi = float(last[rsi_col])
        vwap = float(last[vwap_col])
        if rsi < RSI_OVERSOLD and close < vwap:
            return "BULLISH", close
        if rsi > RSI_OVERBOUGHT and close > vwap:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_breakout(df) -> tuple[str | None, float | None]:
    BREAKOUT_LOOKBACK = 20
    try:
        if df is None or len(df) < BREAKOUT_LOOKBACK + 1:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        high_col = _find_col(df, ["high", "High"])
        low_col = _find_col(df, ["low", "Low"])
        if close_col is None or high_col is None or low_col is None:
            return None, None
        close = float(df.iloc[-1][close_col])
        highs = df[high_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        lows = df[low_col].iloc[-(BREAKOUT_LOOKBACK + 1):-1].dropna()
        if len(highs) < 1 or len(lows) < 1:
            return None, None
        recent_high = max(highs)
        recent_low = min(lows)
        if close > recent_high:
            return "BULLISH", close
        if close < recent_low:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_trend_pullback(df) -> tuple[str | None, float | None]:
    PULLBACK_TOLERANCE = 0.001
    MIN_BARS_REQUIRED = 2
    try:
        if df is None or len(df) < MIN_BARS_REQUIRED:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        ema9_col = _find_col(df, ["ema9", "EMA9", "ema_9"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema9_col is None or ema20_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        ema9 = float(last[ema9_col])
        ema20 = float(last[ema20_col])
        if ema9 > ema20:
            if close <= ema9 * (1 + PULLBACK_TOLERANCE) and close >= ema9 * (1 - PULLBACK_TOLERANCE):
                return "BULLISH", close
        if ema9 < ema20:
            if close >= ema9 * (1 - PULLBACK_TOLERANCE) and close <= ema9 * (1 + PULLBACK_TOLERANCE):
                return "BEARISH", close
        return None, None
    except Exception:
        return None, None


def _signal_orb_breakout(feature_snapshot: dict, profile: dict) -> tuple[str | None, float | None, str | None]:
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


def _signal_swing_trend(df) -> tuple[str | None, float | None]:
    SWING_SLOPE_LOOKBACK = 10
    try:
        if df is None or len(df) < SWING_SLOPE_LOOKBACK + 1:
            return None, None
        close_col = _find_col(df, ["close", "Close"])
        ema20_col = _find_col(df, ["ema20", "EMA20", "ema_20"])
        if close_col is None or ema20_col is None:
            return None, None
        last = df.iloc[-1]
        close = float(last[close_col])
        ema20_now = float(last[ema20_col])
        ema20_past = float(df[ema20_col].iloc[-(SWING_SLOPE_LOOKBACK + 1)])
        ema20_slope_positive = ema20_now > ema20_past
        if ema20_slope_positive and close > ema20_now:
            return "BULLISH", close
        if (not ema20_slope_positive) and close < ema20_now:
            return "BEARISH", close
        return None, None
    except Exception:
        return None, None


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

        # Use wider prediction horizons to decide whether to trade and how long to hold
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

        # Safety gate: only enforce after enough trades
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


def derive_sim_signal(df, signal_mode, context: dict | None = None, feature_snapshot: dict | None = None):
    try:
        if signal_mode == "MEAN_REVERSION":
            return _signal_mean_reversion(df)
        elif signal_mode == "BREAKOUT":
            return _signal_breakout(df)
        elif signal_mode == "TREND_PULLBACK":
            direction, price = _signal_trend_pullback(df)
            if direction is None or price is None:
                return None, None, None
            min_exp = context.get("atr_expansion_min") if isinstance(context, dict) else None
            if min_exp is not None:
                if feature_snapshot is None:
                    return None, None, {"reason": "features_required"}
                atr_exp = feature_snapshot.get("atr_expansion")
                try:
                    if atr_exp is None or float(atr_exp) < float(min_exp):
                        return None, None, {"reason": "atr_expansion_filter"}
                except Exception:
                    return None, None, {"reason": "atr_expansion_invalid"}
            return direction, price, {"reason": "trend_pullback"}
        elif signal_mode == "SWING_TREND":
            return _signal_swing_trend(df)
        elif signal_mode == "OPPORTUNITY":
            return _signal_opportunity(df, context)
        elif signal_mode == "ORB_BREAKOUT":
            if feature_snapshot is None:
                return None, None, {"reason": "features_required"}
            direction, price, reason = _signal_orb_breakout(feature_snapshot, context or {})
            if direction is None or price is None:
                return None, None, {"reason": reason or "orb_no_signal"}
            return direction, price, {"reason": reason or "orb_break"}
        else:
            return None, None
    except Exception:
        return None, None
