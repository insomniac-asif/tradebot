import os
import asyncio
import uuid
import yaml
import pytz
import math
import logging
import time as _time
from datetime import datetime, time
from simulation.sim_portfolio import SimPortfolio
from simulation.sim_executor import sim_try_fill, sim_compute_risk_dollars, sim_should_trade_now
from simulation.sim_contract import select_sim_contract, select_sim_contract_with_reason, get_iv_series
from simulation.sim_live_router import sim_live_router, manage_live_exit
from execution.option_executor import get_option_price
from simulation.sim_watcher import _get_time_of_day_bucket
from simulation.sim_signals import derive_sim_signal
from simulation.sim_ml import predict_sim_trade, record_sim_trade_close
from analytics.sim_features import compute_sim_features
from core.md_state import is_md_enabled
from signals.volatility import volatility_state


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


_PROFILES = _load_profiles()
_LAST_CHAIN_CALL_TS = 0.0
_CHAIN_CALL_MIN_INTERVAL = 1.0  # seconds between Alpaca snapshot calls
DAYTRADE_EOD_CUTOFF = time(15, 55)  # ET cutoff for day-trading sims to flatten
EXPIRY_EOD_CUTOFF = time(15, 55)    # ET cutoff for same-day expiries


def _trade_grade(tr) -> float | None:
    """Best available confidence/edge score for a trade."""
    candidates = []
    for key in ("edge_prob", "prediction_confidence", "confidence", "ml_probability"):
        val = tr.get(key)
        if isinstance(val, (int, float)):
            candidates.append(float(val))
    return max(candidates) if candidates else None


def _count_directional_exposure(direction: str) -> int:
    """Count how many sims currently have open trades in the given direction."""
    count = 0
    for sid, prof in _PROFILES.items():
        if str(sid).startswith("_"):
            continue
        try:
            s = SimPortfolio(sid, prof)
            s.load()
            for t in s.open_trades:
                if isinstance(t, dict) and t.get("direction") == direction:
                    count += 1
                    break
        except Exception:
            continue
    return count


async def run_sim_entries(
    df,
    regime: str | None = None
) -> list[dict]:
    global _LAST_CHAIN_CALL_TS
    results = []
    if not _PROFILES:
        return [{"sim_id": None, "status": "error", "reason": "no_profiles_loaded"}]
    time_of_day_bucket = _get_time_of_day_bucket()
    for sim_id, profile in _PROFILES.items():
        if str(sim_id).startswith("_"):
            continue
        try:
            sim = SimPortfolio(sim_id, profile)
            sim.load()

            signal_mode = sim.profile.get("signal_mode", "TREND_PULLBACK")
            trade_count = len(sim.trade_log) if isinstance(sim.trade_log, list) else 0
            signal_meta = None
            direction = None
            underlying_price = None

            effective_profile = dict(profile)

            # ── 1. Derive signal FIRST ──────────────────────────────
            feature_snapshot = None
            if profile.get("features_enabled"):
                try:
                    feature_snapshot = compute_sim_features(
                        df,
                        {
                            "direction": None,
                            "price": None,
                            "regime": regime,
                            "signal_mode": signal_mode,
                            "horizon": effective_profile.get("horizon", profile.get("horizon")),
                            "dte_min": effective_profile.get("dte_min"),
                            "dte_max": effective_profile.get("dte_max"),
                            "orb_minutes": effective_profile.get("orb_minutes", profile.get("orb_minutes", 15)),
                            "zscore_window": effective_profile.get("zscore_window", profile.get("zscore_window", 30)),
                            "iv_series": get_iv_series(profile.get("iv_series_window", 200)),
                        },
                    )
                except Exception:
                    feature_snapshot = None

            sig = derive_sim_signal(
                df,
                signal_mode,
                {
                    "trade_count": trade_count,
                    "atr_expansion_min": profile.get("atr_expansion_min"),
                    "vol_z_min": profile.get("vol_z_min"),
                    "require_trend_bias": profile.get("require_trend_bias"),
                    "iv_rank_max": profile.get("iv_rank_max"),
                    "vwap_z_min": profile.get("vwap_z_min"),
                    "close_z_min": profile.get("close_z_min"),
                },
                feature_snapshot=feature_snapshot,
            )
            if isinstance(sig, tuple):
                if len(sig) >= 2:
                    direction = sig[0]
                    underlying_price = sig[1]
                if len(sig) >= 3:
                    signal_meta = sig[2]

            # ── 2. Apply signal_meta overrides ──────────────────────
            if isinstance(signal_meta, dict):
                for k in [
                    "dte_min",
                    "dte_max",
                    "hold_min_seconds",
                    "hold_max_seconds",
                    "horizon",
                    "orb_minutes",
                    "zscore_window",
                ]:
                    if signal_meta.get(k) is not None:
                        effective_profile[k] = signal_meta.get(k)

            # ── 3. Build entry_context AFTER signal_meta ────────────
            entry_context = f"signal_mode={signal_mode} | regime={regime or 'N/A'} | bucket={time_of_day_bucket or 'N/A'}"
            if isinstance(signal_meta, dict) and signal_meta.get("entry_context"):
                entry_context = f"{entry_context} | {signal_meta.get('entry_context')}"
            if isinstance(signal_meta, dict) and signal_meta.get("reason"):
                entry_context = f"{entry_context} | reason={signal_meta.get('reason')}"

            # ── 4. Early exit if no signal ─────────────────────────
            if direction is None or underlying_price is None:
                if isinstance(signal_meta, dict) and signal_meta.get("reason"):
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": signal_meta.get("reason"),
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                continue

            # ── Cross-sim directional exposure guard ─────────────────
            global_config = _PROFILES.get("_global", {})
            if global_config.get("cross_sim_guard_enabled", False):
                try:
                    max_dir_sims = int(global_config.get("max_directional_sims", 4))
                except (TypeError, ValueError):
                    max_dir_sims = 4
                current_dir_count = _count_directional_exposure(direction)
                if current_dir_count >= max_dir_sims:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": "directional_exposure_limit",
                        "direction": direction,
                        "current_count": current_dir_count,
                        "max_allowed": max_dir_sims,
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue

            # ── 5. ML prediction with real direction/price ─────────
            ml_context = {
                "direction": direction,
                "price": underlying_price,
                "regime": regime,
                "horizon": effective_profile.get("horizon"),
                "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
            }
            ml_prediction = predict_sim_trade(df, ml_context)

            # ── 6. Continue with regime_filter, execution_mode, etc. ─
            regime_filter = sim.profile.get("regime_filter")
            if regime_filter is not None:
                filtered_out = False
                if isinstance(regime_filter, list):
                    filtered_out = regime not in regime_filter
                elif regime_filter == "TREND_ONLY":
                    filtered_out = regime != "TREND"
                elif regime_filter == "RANGE_ONLY":
                    filtered_out = regime != "RANGE"
                elif regime_filter == "VOLATILE_ONLY":
                    filtered_out = regime != "VOLATILE"
                if filtered_out:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": "regime_filter",
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue

            execution_mode = sim.profile.get("execution_mode")
            if execution_mode == "live":
                # Graduation gate: this sim activates only after a source sim
                # (e.g. SIM03) has logged enough closed paper trades.
                source_sim_id = sim.profile.get("source_sim")
                min_source_trades = 0
                try:
                    min_source_trades = int(sim.profile.get("min_source_trades", 0))
                except (TypeError, ValueError):
                    min_source_trades = 0
                if source_sim_id and min_source_trades > 0:
                    src_trade_count = 0
                    try:
                        src_profile = _PROFILES.get(source_sim_id, {})
                        src_sim = SimPortfolio(source_sim_id, src_profile)
                        src_sim.load()
                        src_trade_count = len(src_sim.trade_log) if isinstance(src_sim.trade_log, list) else 0
                    except Exception:
                        src_trade_count = 0
                    if src_trade_count < min_source_trades:
                        results.append({
                            "sim_id": sim_id,
                            "status": "skipped",
                            "reason": "insufficient_trade_history",
                            "trade_count": src_trade_count,
                            "min_trades_for_live": min_source_trades,
                            "entry_context": entry_context,
                            "signal_mode": signal_mode,
                        })
                        continue

                # ── Circuit breaker: continuous performance check ──
                cb_config = sim.profile.get("circuit_breaker")
                if isinstance(cb_config, dict) and cb_config.get("enabled"):
                    cb_source_id = cb_config.get("source_sim")
                    cb_window = int(cb_config.get("rolling_window", 20))
                    cb_min_wr = float(cb_config.get("min_win_rate", 0.35))
                    cb_min_exp = float(cb_config.get("min_expectancy", -50.0))
                    cb_recover_wr = float(cb_config.get("recovery_win_rate", 0.45))
                    cb_recover_exp = float(cb_config.get("recovery_expectancy", 0.0))

                    if cb_source_id:
                        try:
                            cb_src_profile = _PROFILES.get(cb_source_id, {})
                            cb_src_sim = SimPortfolio(cb_source_id, cb_src_profile)
                            cb_src_sim.load()
                            cb_log = cb_src_sim.trade_log if isinstance(cb_src_sim.trade_log, list) else []
                            cb_recent = cb_log[-cb_window:] if len(cb_log) >= cb_window else cb_log

                            if len(cb_recent) >= cb_window:
                                cb_wins = 0
                                cb_pnl_total = 0.0
                                for t in cb_recent:
                                    pnl = t.get("realized_pnl_dollars")
                                    if pnl is None:
                                        continue
                                    try:
                                        pnl_f = float(pnl)
                                    except (TypeError, ValueError):
                                        continue
                                    cb_pnl_total += pnl_f
                                    if pnl_f > 0:
                                        cb_wins += 1
                                cb_wr = cb_wins / len(cb_recent) if cb_recent else 0
                                cb_exp = cb_pnl_total / len(cb_recent) if cb_recent else 0

                                was_tripped = sim.profile.get("_circuit_breaker_tripped", False)

                                if not was_tripped:
                                    if cb_wr < cb_min_wr or cb_exp < cb_min_exp:
                                        results.append({
                                            "sim_id": sim_id,
                                            "status": "circuit_breaker_tripped",
                                            "reason": "source_performance_degraded",
                                            "source_sim": cb_source_id,
                                            "source_wr": round(cb_wr, 3),
                                            "source_exp": round(cb_exp, 2),
                                            "threshold_wr": cb_min_wr,
                                            "threshold_exp": cb_min_exp,
                                            "window": cb_window,
                                        })
                                        sim.profile["_circuit_breaker_tripped"] = True
                                        continue
                                else:
                                    if cb_wr >= cb_recover_wr and cb_exp >= cb_recover_exp:
                                        results.append({
                                            "sim_id": sim_id,
                                            "status": "circuit_breaker_recovered",
                                            "source_sim": cb_source_id,
                                            "source_wr": round(cb_wr, 3),
                                            "source_exp": round(cb_exp, 2),
                                        })
                                        sim.profile["_circuit_breaker_tripped"] = False
                                    else:
                                        results.append({
                                            "sim_id": sim_id,
                                            "status": "circuit_breaker_held",
                                            "reason": "source_still_degraded",
                                            "source_sim": cb_source_id,
                                            "source_wr": round(cb_wr, 3),
                                            "source_exp": round(cb_exp, 2),
                                        })
                                        continue
                        except Exception:
                            logging.exception("circuit_breaker_check_error")

                if not sim.profile.get("enabled"):
                    results.append({"sim_id": sim_id, "status": "skipped", "reason": "live_disabled"})
                    continue
                ok, reason = sim_should_trade_now(effective_profile)
                if not ok:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": reason,
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue
                ok, reason = sim.can_trade()
                if not ok:
                    results.append({
                        "sim_id": sim_id,
                        "status": "skipped",
                        "reason": reason,
                        "entry_context": entry_context,
                        "signal_mode": signal_mode,
                    })
                    continue
                elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
                if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                    await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
                _LAST_CHAIN_CALL_TS = _time.monotonic()
                live_result = await sim_live_router(
                    sim_id=sim_id,
                    direction=direction,
                    price=underlying_price,
                    ml_prediction=ml_prediction,
                    regime=regime,
                    time_of_day_bucket=time_of_day_bucket,
                    signal_mode=signal_mode,
                    entry_context=entry_context,
                    feature_snapshot=feature_snapshot,
                )
                if not isinstance(live_result, dict) or live_result.get("status") != "success":
                    results.append({
                        "sim_id": sim_id,
                        "status": "error",
                        "reason": (live_result or {}).get("message", "live_order_failed")
                    })
                    continue
                results.append({
                    "sim_id": sim_id,
                    "status": "live_submitted",
                    "option_symbol": live_result.get("option_symbol"),
                    "qty": live_result.get("qty"),
                    "fill_price": live_result.get("fill_price"),
                    "entry_price": live_result.get("fill_price"),
                    "direction": direction,
                    "risk_dollars": live_result.get("risk_dollars"),
                    "strike": live_result.get("strike"),
                    "expiry": live_result.get("expiry"),
                    "dte": live_result.get("dte"),
                    "spread_pct": live_result.get("spread_pct"),
                    "regime": regime,
                    "time_bucket": time_of_day_bucket,
                    "mode": "LIVE",
                    "balance": live_result.get("balance_after"),
                    "entry_context": live_result.get("entry_context") or entry_context,
                    "signal_mode": live_result.get("signal_mode") or signal_mode,
                    "predicted_direction": live_result.get("predicted_direction") or ml_prediction.get("predicted_direction"),
                    "prediction_confidence": live_result.get("prediction_confidence") or ml_prediction.get("prediction_confidence"),
                    "edge_prob": live_result.get("edge_prob") or ml_prediction.get("edge_prob"),
                    "direction_prob": live_result.get("direction_prob") or ml_prediction.get("direction_prob"),
                })
                continue

            ok, reason = sim_should_trade_now(effective_profile)
            if not ok:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": reason,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            ok, reason = sim.can_trade()
            if not ok:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": reason,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
            if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
            _LAST_CHAIN_CALL_TS = _time.monotonic()

            contract, contract_reason = select_sim_contract_with_reason(direction, underlying_price, {**effective_profile, "sim_id": sim_id})
            if contract is None:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": contract_reason or "no_contract",
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            bid = contract["bid"]
            ask = contract["ask"]

            fill_result, err = sim_try_fill(
                contract["option_symbol"],
                qty=1,
                bid=bid,
                ask=ask,
                profile=profile,
                side="entry"
            )
            if err or fill_result is None:
                results.append({
                    "sim_id": sim_id,
                    "status": "skipped",
                    "reason": err,
                    "entry_context": entry_context,
                    "signal_mode": signal_mode,
                })
                continue

            fill_price = fill_result["fill_price"]
            risk_dollars = sim_compute_risk_dollars(sim.balance, profile)
            qty = max(1, math.floor(risk_dollars / (fill_price * 100)))

            fill_result, err = sim_try_fill(
                contract["option_symbol"],
                qty=qty,
                bid=bid,
                ask=ask,
                profile=profile,
                side="entry"
            )
            if err or fill_result is None:
                results.append({"sim_id": sim_id, "status": "skipped", "reason": err})
                continue

            trade = {
                "trade_id": f"{sim_id}__{uuid.uuid4()}",
                "sim_id": sim_id,
                "option_symbol": contract["option_symbol"],
                "entry_price": fill_result["fill_price"],
                "qty": qty,
                "entry_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                "horizon": effective_profile.get("horizon", profile.get("horizon")),
                "dte_bucket": str(contract["dte"]),
                "otm_pct": contract["otm_pct_applied"],
                "direction": direction,
                "strike": contract["strike"],
                "expiry": contract["expiry"],
                "contract_type": contract["contract_type"],
                "hold_min_seconds": int(effective_profile.get("hold_min_seconds", profile.get("hold_min_seconds"))),
                "hold_max_seconds": int(effective_profile.get("hold_max_seconds", profile.get("hold_max_seconds"))),
                "entry_price_source": fill_result.get("price_source", "mid_plus_slippage"),
            }
            if feature_snapshot:
                trade["feature_snapshot"] = feature_snapshot
            if sim_id == "SIM09" or str(signal_mode).upper() == "OPPORTUNITY":
                vol_state = volatility_state(df)
                vol_stop_map = {
                    "DEAD": 0.03,
                    "LOW": 0.04,
                    "NORMAL": 0.06,
                    "HIGH": 0.08,
                }
                dynamic_stop = vol_stop_map.get(str(vol_state).upper(), 0.06)
                trade["stop_loss_pct"] = min(0.10, max(0.01, float(dynamic_stop)))
            trade["regime_at_entry"] = regime
            trade["time_of_day_bucket"] = time_of_day_bucket
            trade["signal_mode"] = signal_mode
            trade["entry_context"] = entry_context
            # greeks at entry (from contract snapshot)
            trade["iv_at_entry"] = contract.get("iv")
            trade["delta_at_entry"] = contract.get("delta")
            trade["gamma_at_entry"] = contract.get("gamma")
            trade["theta_at_entry"] = contract.get("theta")
            trade["vega_at_entry"] = contract.get("vega")
            if isinstance(ml_prediction, dict):
                trade["predicted_direction"] = ml_prediction.get("predicted_direction")
                trade["prediction_confidence"] = ml_prediction.get("prediction_confidence")
                trade["direction_prob"] = ml_prediction.get("direction_prob")
                trade["edge_prob"] = ml_prediction.get("edge_prob")
                trade["regime"] = ml_prediction.get("regime")
                trade["volatility"] = ml_prediction.get("volatility")
                trade["conviction_score"] = ml_prediction.get("conviction_score")
                trade["impulse"] = ml_prediction.get("impulse")
                trade["follow_through"] = ml_prediction.get("follow_through")
                trade["setup"] = ml_prediction.get("setup")
                trade["style"] = ml_prediction.get("style")
                trade["confidence"] = ml_prediction.get("confidence")
                trade["ml_probability"] = ml_prediction.get("edge_prob")

            sim.record_open(trade)
            sim.save()

            results.append({
                "sim_id": sim_id,
                "status": "opened",
                "trade_id": trade["trade_id"],
                "fill_price": trade["entry_price"],
                "entry_price": trade["entry_price"],
                "qty": qty,
                "option_symbol": trade["option_symbol"],
                "expiry": contract["expiry"],
                "strike": contract["strike"],
                "dte": contract["dte"],
                "spread_pct": contract["spread_pct"],
                "direction": direction,
                "risk_dollars": risk_dollars,
                "regime": regime,
                "time_bucket": time_of_day_bucket,
                "mode": "SIM",
                "balance": sim.balance,
                "entry_context": entry_context,
                "signal_mode": signal_mode,
                "predicted_direction": trade.get("predicted_direction"),
                "prediction_confidence": trade.get("prediction_confidence"),
                "edge_prob": trade.get("edge_prob"),
                "direction_prob": trade.get("direction_prob"),
            })
        except Exception as e:
            logging.exception("run_sim_entries_error: %s", e)
            results.append({"sim_id": sim_id, "status": "error", "reason": str(e)})
    return results


async def run_sim_exits() -> list[dict]:
    global _LAST_CHAIN_CALL_TS
    results = []
    if not _PROFILES:
        return [{"sim_id": None, "status": "error", "reason": "no_profiles_loaded"}]
    eastern = pytz.timezone("US/Eastern")

    for sim_id, profile in _PROFILES.items():
        try:
            sim = SimPortfolio(sim_id, profile)
            sim.load()
        except Exception as e:
            logging.exception("run_sim_exits_load_error: %s", e)
            results.append({
                "sim_id": sim_id,
                "status": "error",
                "reason": str(e)
            })
            continue
        if profile.get("execution_mode") == "live" and profile.get("enabled"):
            for trade in list(sim.open_trades):
                try:
                    live_result = await manage_live_exit(sim, trade)
                    if live_result and live_result.get("status") == "success":
                        results.append({
                            "sim_id": sim_id,
                            "trade_id": trade.get("trade_id"),
                            "status": "closed",
                            "exit_price": live_result.get("exit_price"),
                        "exit_reason": live_result.get("exit_reason"),
                        "exit_context": live_result.get("exit_context"),
                        "option_symbol": live_result.get("option_symbol"),
                            "strike": live_result.get("strike") or trade.get("strike"),
                            "expiry": live_result.get("expiry") or trade.get("expiry"),
                            "direction": live_result.get("direction") or trade.get("direction"),
                            "qty": live_result.get("qty"),
                            "entry_price": live_result.get("entry_price"),
                            "pnl": live_result.get("pnl"),
                            "mode": "LIVE",
                            "balance_after": live_result.get("balance_after"),
                            "time_in_trade_seconds": live_result.get("time_in_trade_seconds"),
                            "predicted_direction": live_result.get("predicted_direction"),
                            "prediction_confidence": live_result.get("prediction_confidence"),
                            "edge_prob": live_result.get("edge_prob"),
                        "direction_prob": live_result.get("direction_prob"),
                    })
                except Exception as e:
                    logging.exception("run_sim_exits_live_error: %s", e)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade.get("trade_id"),
                        "status": "error",
                        "reason": str(e)
                    })
            continue
        for trade in list(sim.open_trades):
            try:
                now_et = datetime.now(eastern)
                entry_time_et = datetime.fromisoformat(trade["entry_time"])
                if entry_time_et.tzinfo is None:
                    entry_time_et = eastern.localize(entry_time_et)
                elapsed_seconds = (now_et - entry_time_et).total_seconds()
                elapsed = _time.monotonic() - _LAST_CHAIN_CALL_TS
                if elapsed < _CHAIN_CALL_MIN_INTERVAL:
                    await asyncio.sleep(_CHAIN_CALL_MIN_INTERVAL - elapsed)
                _LAST_CHAIN_CALL_TS = _time.monotonic()
                current_price = get_option_price(trade["option_symbol"])
                if current_price is None:
                    logging.warning("sim_exit_missing_quote: %s", trade["trade_id"])
                    continue
                sim.update_open_trade_excursion(trade["trade_id"], current_price)
                should_exit = False
                exit_reason = None
                exit_context = None
                spread_guard_bypass = False

                # Force exit for same-day expiry (all sims) before market close
                expiry_date = None
                try:
                    expiry_raw = trade.get("expiry")
                    if isinstance(expiry_raw, str):
                        expiry_date = datetime.fromisoformat(expiry_raw).date()
                except Exception:
                    expiry_date = None
                if expiry_date == now_et.date() and now_et.time() >= EXPIRY_EOD_CUTOFF:
                    should_exit = True
                    exit_reason = "expiry_close"
                    spread_guard_bypass = True
                    expiry_text = expiry_date.isoformat() if expiry_date else "unknown"
                    exit_context = f"expiry={expiry_text} cutoff={EXPIRY_EOD_CUTOFF.strftime('%H:%M')}"

                # Force exit for day-trading sims before market close
                is_daytrade = int(profile.get("dte_max", 0)) == 0
                if not should_exit and is_daytrade and now_et.time() >= DAYTRADE_EOD_CUTOFF:
                    should_exit = True
                    exit_reason = "eod_daytrade_close"
                    spread_guard_bypass = True
                    exit_context = f"daytrade_cutoff={DAYTRADE_EOD_CUTOFF.strftime('%H:%M')}"
                if not should_exit and elapsed_seconds < trade["hold_min_seconds"]:
                    continue

                stop_loss_pct = trade.get("stop_loss_pct", profile.get("stop_loss_pct"))
                if stop_loss_pct is not None and is_md_enabled() and sim_id != "SIM09":
                    try:
                        stop_loss_pct = max(float(stop_loss_pct) * 0.7, 0.05)
                    except (TypeError, ValueError):
                        pass
                if stop_loss_pct is not None:
                    try:
                        entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            # Track peak price for profit-lock SL
                            peak_price = float(trade.get("peak_price") or 0)
                            if current_price > entry_price and current_price > peak_price:
                                trade["peak_price"] = current_price
                                sim.save()
                                peak_price = current_price

                            loss_pct = (current_price - entry_price) / entry_price
                            effective_sl_pct = abs(float(stop_loss_pct))
                            original_sl_price = entry_price * (1 - effective_sl_pct)

                            # Profit-lock: once trade has been profitable, SL floors at breakeven
                            effective_sl_price = max(original_sl_price, entry_price) if peak_price > entry_price else original_sl_price

                            if current_price <= effective_sl_price:
                                should_exit = True
                                spread_guard_bypass = True
                                if effective_sl_price > original_sl_price:
                                    exit_reason = "breakeven_stop"
                                    exit_context = f"price={current_price:.4f} locked_floor={effective_sl_price:.4f} peak={peak_price:.4f}"
                                else:
                                    exit_reason = "stop_loss"
                                    exit_context = f"loss_pct={loss_pct:.3%} <= -{effective_sl_pct:.3%}"
                    except (TypeError, ValueError):
                        pass

                profit_target_pct = profile.get("profit_target_pct")
                entry_price = None
                gain_pct = None
                try:
                    entry_price = float(trade.get("entry_price", 0))
                    if entry_price > 0:
                        gain_pct = (current_price - entry_price) / entry_price
                except (TypeError, ValueError):
                    gain_pct = None

                # ── Greeks-aware exit: theta burn acceleration ────────
                if not should_exit and gain_pct is not None and gain_pct <= 0.02:
                    dte_at_entry = trade.get("dte_bucket")
                    try:
                        dte_val = int(dte_at_entry) if dte_at_entry is not None else None
                    except (TypeError, ValueError):
                        dte_val = None
                    if dte_val is not None and dte_val <= 1:
                        try:
                            expiry_raw = trade.get("expiry")
                            if isinstance(expiry_raw, str):
                                expiry_date = datetime.fromisoformat(expiry_raw).date()
                                if expiry_date == now_et.date():
                                    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
                                    remaining_seconds = (market_close - now_et).total_seconds()
                                    if 0 < remaining_seconds < 7200 and gain_pct <= 0.02:
                                        should_exit = True
                                        exit_reason = "theta_burn"
                                        exit_context = f"gain_pct={gain_pct:.3%} remaining={remaining_seconds:.0f}s dte={dte_val}"
                                        spread_guard_bypass = True
                        except Exception:
                            pass

                # ── Greeks-aware exit: IV crush detection ─────────────
                if not should_exit and gain_pct is not None and gain_pct < 0:
                    iv_at_entry = trade.get("iv_at_entry")
                    if isinstance(iv_at_entry, (int, float)) and float(iv_at_entry) > 0:
                        try:
                            iv_entry = float(iv_at_entry)
                            stop_pct = float(profile.get("stop_loss_pct", 0.30))
                            if abs(gain_pct) > stop_pct * 0.3:
                                tightened_stop = stop_pct * 0.6
                                if gain_pct <= -abs(tightened_stop):
                                    should_exit = True
                                    exit_reason = "iv_crush_stop"
                                    exit_context = f"gain_pct={gain_pct:.3%} iv_entry={iv_entry:.3f} tightened_stop={tightened_stop:.3%}"
                                    spread_guard_bypass = True
                        except (TypeError, ValueError):
                            pass

                # Near-TP adaptive lock + TP2 (optional)
                if not should_exit and profit_target_pct is not None and gain_pct is not None:
                    try:
                        base_target = abs(float(profit_target_pct))
                        if base_target > 0 and profile.get("tp2_enabled", True):
                            near_ratio = float(profile.get("near_tp_trigger_ratio", 0.85))
                            grade_min = float(profile.get("near_tp_grade_min", 0.6))
                            tp2_mult = float(profile.get("tp2_multiplier", 1.3))
                            grade = _trade_grade(trade)
                            if grade is not None and grade >= grade_min and gain_pct >= base_target * near_ratio:
                                if not trade.get("tp2_activated"):
                                    lock_pct = trade.get("lock_profit_pct")
                                    if lock_pct is None:
                                        # Lock ~50% of the base target, minimum 5%
                                        lock_pct = max(0.05, min(base_target * 0.5, base_target - 0.02))
                                    trade["lock_profit_pct"] = float(lock_pct)
                                    trade["tp2_target_pct"] = float(base_target * tp2_mult)
                                    trade["tp2_activated"] = True
                                    sim.save()
                    except (TypeError, ValueError):
                        pass

                # Profit lock: exit if retrace below locked profit level after TP2 activation
                lock_pct = trade.get("lock_profit_pct")
                if not should_exit and gain_pct is not None and trade.get("tp2_activated") and isinstance(lock_pct, (int, float)):
                    try:
                        if gain_pct <= float(lock_pct):
                            should_exit = True
                            exit_reason = "profit_lock"
                            exit_context = f"gain_pct={gain_pct:.3%} <= lock_pct={float(lock_pct):.3%}"
                    except (TypeError, ValueError):
                        pass

                effective_target = profit_target_pct
                if trade.get("tp2_activated") and trade.get("tp2_target_pct") is not None:
                    effective_target = trade.get("tp2_target_pct")

                if not should_exit and effective_target is not None:
                    try:
                        if entry_price is None:
                            entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            if gain_pct is None:
                                gain_pct = (current_price - entry_price) / entry_price
                            target_val = abs(float(effective_target))
                            if gain_pct >= target_val:
                                should_exit = True
                                exit_reason = "profit_target_2" if trade.get("tp2_activated") else "profit_target"
                                exit_context = f"gain_pct={gain_pct:.3%} >= {target_val:.3%}"
                    except (TypeError, ValueError):
                        pass

                trailing_activate = profile.get("trailing_stop_activate_pct")
                trailing_trail = profile.get("trailing_stop_trail_pct")
                if not should_exit and trailing_activate is not None and trailing_trail is not None:
                    try:
                        entry_price = float(trade.get("entry_price", 0))
                        if entry_price > 0:
                            gain_pct = (current_price - entry_price) / entry_price
                            trailing_activate_f = abs(float(trailing_activate))
                            trailing_trail_f = abs(float(trailing_trail))
                            if not trade.get("trailing_stop_activated", False):
                                if gain_pct >= trailing_activate_f:
                                    trade["trailing_stop_activated"] = True
                                    trade["trailing_stop_high"] = current_price
                                    sim.save()
                            else:
                                if current_price > trade.get("trailing_stop_high", 0):
                                    trade["trailing_stop_high"] = current_price
                                    sim.save()
                                trail_high = float(trade.get("trailing_stop_high", 0))
                                if trail_high > 0:
                                    drop_from_high = (current_price - trail_high) / trail_high
                                    if drop_from_high <= -trailing_trail_f:
                                        should_exit = True
                                        exit_reason = "trailing_stop"
                                        exit_context = f"drop_from_high={drop_from_high:.3%} <= -{trailing_trail_f:.3%} (high={trail_high:.4f})"
                    except (TypeError, ValueError):
                        pass

                if should_exit and exit_reason:
                    bid = current_price * 0.99
                    ask = current_price * 1.01
                    fill_result, err = sim_try_fill(
                        trade["option_symbol"],
                        qty=trade["qty"],
                        bid=bid,
                        ask=ask,
                        profile=profile,
                        side="exit"
                    )
                    if err and not spread_guard_bypass:
                        continue
                    if err and spread_guard_bypass:
                        fill_result = None
                    exit_price = fill_result["fill_price"] if fill_result else current_price
                    exit_price_source = (
                        fill_result.get("price_source", "mid_minus_slippage")
                        if fill_result
                        else "market_raw_price"
                    )
                    exit_data = {
                        "exit_price": exit_price,
                        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                        "exit_reason": exit_reason,
                        "exit_context": exit_context,
                        "exit_price_source": exit_price_source,
                        "exit_quote_model": "market_raw_price" if (spread_guard_bypass and fill_result is None) else "synthetic_1pct",
                        "entry_price_source": trade.get("entry_price_source", "mid_plus_slippage"),
                        "time_in_trade_seconds": int(elapsed_seconds),
                        "spread_guard_bypassed": spread_guard_bypass and fill_result is None,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "regime_at_entry": trade.get("regime_at_entry"),
                        "time_of_day_bucket": trade.get("time_of_day_bucket"),
                    }
                    sim.record_close(trade["trade_id"], exit_data)
                    sim.save()
                    entry_price = trade.get("entry_price")
                    qty_val = trade.get("qty")
                    pnl_val = None
                    try:
                        entry_price_f = float(entry_price)
                        qty_f = float(qty_val)
                        pnl_val = (exit_price - entry_price_f) * qty_f * 100
                    except (TypeError, ValueError):
                        pnl_val = None
                    record_sim_trade_close(trade, pnl_val)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade["trade_id"],
                        "status": "closed",
                        "exit_price": exit_price,
                        "exit_reason": exit_data["exit_reason"],
                        "exit_context": exit_data.get("exit_context"),
                        "option_symbol": trade.get("option_symbol"),
                        "strike": trade.get("strike"),
                        "expiry": trade.get("expiry"),
                        "direction": trade.get("direction"),
                        "qty": qty_val,
                        "entry_price": entry_price,
                        "pnl": pnl_val,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "feature_snapshot": trade.get("feature_snapshot"),
                        "mode": "SIM",
                        "balance_after": sim.balance,
                        "time_in_trade_seconds": exit_data.get("time_in_trade_seconds"),
                        "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
                        "prediction_confidence": trade.get("prediction_confidence"),
                        "edge_prob": trade.get("edge_prob"),
                        "direction_prob": trade.get("direction_prob"),
                    })
                    continue
                # Hold-max force exit (final fallback only)
                if elapsed_seconds >= trade["hold_max_seconds"]:
                    bid = current_price * 0.99
                    ask = current_price * 1.01
                    fill_result, err = sim_try_fill(
                        trade["option_symbol"],
                        qty=trade["qty"],
                        bid=bid,
                        ask=ask,
                        profile=profile,
                        side="exit"
                    )
                    if err:
                        fill_result = None
                    exit_price = fill_result["fill_price"] if fill_result else current_price
                    exit_price_source = (
                        fill_result.get("price_source", "mid_minus_slippage")
                        if fill_result
                        else "market_raw_price"
                    )
                    exit_context = f"elapsed={int(elapsed_seconds)}s >= hold_max={int(trade['hold_max_seconds'])}s"
                    exit_data = {
                        "exit_price": exit_price,
                        "exit_time": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
                        "exit_reason": "hold_max_elapsed",
                        "exit_context": exit_context,
                        "exit_price_source": exit_price_source,
                        "exit_quote_model": "market_raw_price" if fill_result is None else "synthetic_1pct",
                        "entry_price_source": trade.get("entry_price_source", "mid_plus_slippage"),
                        "time_in_trade_seconds": int(elapsed_seconds),
                        "spread_guard_bypassed": fill_result is None,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "regime_at_entry": trade.get("regime_at_entry"),
                        "time_of_day_bucket": trade.get("time_of_day_bucket"),
                    }
                    sim.record_close(trade["trade_id"], exit_data)
                    sim.save()
                    entry_price = trade.get("entry_price")
                    qty_val = trade.get("qty")
                    pnl_val = None
                    try:
                        entry_price_f = float(entry_price)
                        qty_f = float(qty_val)
                        pnl_val = (exit_price - entry_price_f) * qty_f * 100
                    except (TypeError, ValueError):
                        pnl_val = None
                    record_sim_trade_close(trade, pnl_val)
                    results.append({
                        "sim_id": sim_id,
                        "trade_id": trade["trade_id"],
                        "status": "closed",
                        "exit_price": exit_price,
                        "exit_reason": "hold_max_elapsed",
                        "exit_context": exit_context,
                        "option_symbol": trade.get("option_symbol"),
                        "strike": trade.get("strike"),
                        "expiry": trade.get("expiry"),
                        "direction": trade.get("direction"),
                        "qty": qty_val,
                        "entry_price": entry_price,
                        "pnl": pnl_val,
                        "mae": trade.get("mae_pct"),
                        "mfe": trade.get("mfe_pct"),
                        "feature_snapshot": trade.get("feature_snapshot"),
                        "mode": "SIM",
                        "balance_after": sim.balance,
                        "time_in_trade_seconds": exit_data.get("time_in_trade_seconds"),
                        "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
                        "prediction_confidence": trade.get("prediction_confidence"),
                        "edge_prob": trade.get("edge_prob"),
                        "direction_prob": trade.get("direction_prob"),
                    })
            except Exception as e:
                logging.exception("run_sim_exits_error: %s", e)
                results.append({
                    "sim_id": sim_id,
                    "trade_id": trade.get("trade_id"),
                    "status": "error",
                    "reason": str(e)
                })
    return results
