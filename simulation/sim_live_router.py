# LIVE PATH AUDIT
# Two paths can place live orders:
#   1. sim_live_router.py → SIM00 (sim graduation path) — ACTIVE
#   2. decision/trader.py → open_trade_if_valid() (auto_trader) — PAPER ONLY
#      (auto_trader routes through open_trade_if_valid which calls execute_option_entry,
#       but no sim with execution_mode=live feeds through trader.py in production.
#       Both paths are gated by core/singletons.py::RISK_SUPERVISOR.authorize_entry())
# Resolution: sim_live_router.py is the canonical live path for SIM00.
#             decision/trader.py manages the paper auto_trader account only.

import os
import asyncio
import yaml
import math
import logging
import pytz
from datetime import datetime

from simulation.sim_portfolio import SimPortfolio
from simulation.sim_contract import select_sim_contract, select_sim_contract_with_reason
from simulation.sim_ml import record_sim_trade_close
from execution.option_executor import execute_option_entry, close_option_position, get_option_price
from core.md_state import is_md_enabled


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            raw = yaml.safe_load(f) or {}
        return {k: v for k, v in raw.items() if str(k).upper().startswith("SIM") and isinstance(v, dict)}
    except Exception:
        return {}


_PROFILES = _load_profiles()
EOD_CUTOFF = datetime.strptime("15:55", "%H:%M").time()


def _now_et_iso() -> str:
    return datetime.now(pytz.timezone("US/Eastern")).isoformat()


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _trade_grade(tr) -> float | None:
    """Best available confidence/edge score for a trade."""
    candidates = []
    for key in ("edge_prob", "prediction_confidence", "confidence", "ml_probability"):
        val = tr.get(key)
        if isinstance(val, (int, float)):
            candidates.append(float(val))
    return max(candidates) if candidates else None


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


async def sim_live_router(sim_id, direction, price, ml_prediction=None, regime=None, time_of_day_bucket=None, signal_mode=None, entry_context=None, feature_snapshot=None):
    try:
        try:
            from core.runtime_state import RUNTIME, SystemState
            _rs = RUNTIME.state
            if _rs == SystemState.PANIC_LOCKDOWN:
                return {"status": "error", "message": "panic_lockdown"}
            if _rs == SystemState.OFFLINE:
                return {"status": "error", "message": "system_offline"}
            if _rs == SystemState.EXIT_ONLY:
                return {"status": "error", "message": "exit_only_mode"}
            if not RUNTIME.can_enter():
                return {"status": "error", "message": f"runtime_{_rs.value.lower()}"}
        except ImportError:
            pass

        if direction not in {"BULLISH", "BEARISH"}:
            return {"status": "error", "message": "invalid_direction"}
        try:
            price_val = float(price)
        except (TypeError, ValueError):
            return {"status": "error", "message": "invalid_price"}
        if price_val <= 0:
            return {"status": "error", "message": "invalid_price"}

        profiles = _PROFILES or _load_profiles()
        live_active = [
            key
            for key, prof in profiles.items()
            if isinstance(prof, dict)
            and prof.get("execution_mode") == "live"
            and prof.get("enabled")
        ]
        if len(live_active) > 1:
            logging.error("sim_live_router_multi_live_enabled: %s", live_active)
            return {"status": "error", "message": "multiple_live_sims"}

        profile = profiles.get(sim_id)
        if not isinstance(profile, dict):
            return {"status": "error", "message": "invalid_sim_id"}
        if profile.get("execution_mode") != "live":
            return {"status": "error", "message": "sim_not_live"}
        if not profile.get("enabled"):
            return {"status": "error", "message": "sim_disabled"}

        sim = SimPortfolio(sim_id, profile)
        sim.load()

        daily_loss_limit = profile.get("daily_loss_limit")
        if daily_loss_limit is not None:
            try:
                if float(sim.daily_loss) >= float(daily_loss_limit):
                    return {"status": "error", "message": "daily_loss_limit"}
            except (TypeError, ValueError):
                pass

        max_open_trades = profile.get("max_open_trades")
        if max_open_trades is not None:
            try:
                if int(max_open_trades) > 0 and len(sim.open_trades) >= int(max_open_trades):
                    logging.info("sim_live_router_blocked_max_open_trades: %s", sim_id)
                    return {"status": "error", "message": "max_open_trades"}
            except (TypeError, ValueError):
                pass

        capital_limit = profile.get("capital_limit_dollars", 25000)
        capital_limit_val = _safe_float(capital_limit)
        if capital_limit_val is None or capital_limit_val <= 0:
            return {"status": "error", "message": "invalid_capital_limit"}

        _trade_symbol = profile.get("symbol", "SPY")
        contract, contract_reason = select_sim_contract_with_reason(direction, price_val, profile, symbol=_trade_symbol)
        if contract is None:
            return {"status": "error", "message": contract_reason or "no_contract"}

        mid = contract.get("mid")
        mid_val = _safe_float(mid)
        if mid_val is None or mid_val <= 0:
            return {"status": "error", "message": "invalid_mid"}

        risk_pct = profile.get("risk_per_trade_pct")
        risk_pct_val = _safe_float(risk_pct)
        if risk_pct_val is None:
            return {"status": "error", "message": "invalid_risk_pct"}

        effective_balance = min(sim.balance, capital_limit_val)
        risk_dollars = effective_balance * risk_pct_val
        if risk_dollars < 50.0:
            risk_dollars = 50.0

        qty = max(1, math.floor(risk_dollars / (mid_val * 100)))

        open_exposure = 0.0
        for t in sim.open_trades:
            if isinstance(t, dict):
                try:
                    entry_price = float(t.get("entry_price", 0.0))
                    qty_val = float(t.get("qty", 0.0))
                    open_exposure += entry_price * qty_val * 100
                except (TypeError, ValueError):
                    pass
        est_exposure = mid_val * qty * 100
        if open_exposure + est_exposure > capital_limit_val:
            return {"status": "error", "message": "capital_limit_reached"}

        try:
            from core.singletons import RISK_SUPERVISOR
            notional = mid_val * qty * 100
            _allowed, _deny_reason = RISK_SUPERVISOR.authorize_entry(
                sim_id=sim_id, direction=direction,
                symbol=_trade_symbol, notional=notional,
            )
            if not _allowed:
                logging.info("sim_live_router_blocked_supervisor: %s", _deny_reason)
                return {"status": "error", "message": f"risk_supervisor: {_deny_reason}"}
        except ImportError:
            pass

        fill_result, block = await execute_option_entry(
            contract["option_symbol"],
            qty,
            contract["bid"],
            contract["ask"],
        )
        if fill_result is None:
            return {"status": "error", "message": block or "order_not_filled"}

        trade = {
            "trade_id": f"{sim_id}__{os.urandom(8).hex()}",
            "sim_id": sim_id,
            "option_symbol": contract["option_symbol"],
            "entry_price": fill_result.get("fill_price"),
            "qty": qty,
            "entry_time": _now_et_iso(),
            "horizon": profile.get("horizon"),
            "dte_bucket": str(contract.get("dte")),
            "otm_pct": contract.get("otm_pct_applied"),
            "direction": direction,
            "strike": contract.get("strike"),
            "expiry": contract.get("expiry"),
            "contract_type": contract.get("contract_type"),
            "hold_min_seconds": int(profile.get("hold_min_seconds", 0)),
            "hold_max_seconds": int(profile.get("hold_max_seconds", 0)),
            "entry_price_source": "live_fill",
            "execution_mode": "live",
        }
        if feature_snapshot:
            trade["feature_snapshot"] = feature_snapshot
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

        return {
            "status": "success",
            "message": "live_trade_submitted",
            "option_symbol": contract["option_symbol"],
            "qty": qty,
            "fill_price": fill_result.get("fill_price"),
            "risk_dollars": risk_dollars,
            "strike": contract.get("strike"),
            "expiry": contract.get("expiry"),
            "dte": contract.get("dte"),
            "spread_pct": contract.get("spread_pct"),
            "balance_after": sim.balance,
            "entry_context": entry_context,
            "signal_mode": signal_mode,
            "predicted_direction": trade.get("predicted_direction"),
            "prediction_confidence": trade.get("prediction_confidence"),
            "edge_prob": trade.get("edge_prob"),
            "direction_prob": trade.get("direction_prob"),
        }
    except Exception as e:
        logging.exception("sim_live_router_error: %s", e)
        return {"status": "error", "message": str(e)}


async def manage_live_exit(sim, trade):
    try:
        if not isinstance(trade, dict):
            return {"status": "error", "message": "invalid_trade"}
        option_symbol = trade.get("option_symbol")
        if not option_symbol:
            return {"status": "error", "message": "missing_symbol"}
        entry_time = trade.get("entry_time")
        if not entry_time:
            return {"status": "error", "message": "missing_entry_time"}

        eastern = pytz.timezone("US/Eastern")
        now_et = datetime.now(eastern)
        entry_time_et = datetime.fromisoformat(entry_time)
        if entry_time_et.tzinfo is None:
            entry_time_et = eastern.localize(entry_time_et)
        elapsed_seconds = (now_et - entry_time_et).total_seconds()

        # ── Forced exits that bypass hold_min ────────────────────
        force_exit = False
        force_reason = None
        force_context = None
        try:
            expiry_raw = trade.get("expiry")
            if isinstance(expiry_raw, str):
                expiry_date = datetime.fromisoformat(expiry_raw).date()
                if expiry_date == now_et.date() and now_et.time() >= EOD_CUTOFF:
                    force_exit = True
                    force_reason = "expiry_close"
                    force_context = f"expiry={expiry_date.isoformat()} cutoff=15:55"
        except Exception:
            pass

        profile = sim.profile
        is_daytrade = int(profile.get("dte_max", 0)) == 0
        if not force_exit and is_daytrade and now_et.time() >= EOD_CUTOFF:
            force_exit = True
            force_reason = "eod_daytrade_close"
            force_context = "daytrade_cutoff=15:55"

        hold_min_seconds = int(trade.get("hold_min_seconds", 0))
        if not force_exit and elapsed_seconds < hold_min_seconds:
            return {"status": "skipped", "message": "hold_min"}

        current_price = get_option_price(option_symbol)
        if current_price is None:
            logging.warning("sim_live_exit_missing_price: %s", trade.get("trade_id"))
            return {"status": "error", "message": "missing_price"}

        sim.update_open_trade_excursion(trade.get("trade_id"), current_price)

        should_exit = force_exit
        exit_reason = force_reason
        exit_context = force_context

        stop_loss_pct = profile.get("stop_loss_pct")
        if stop_loss_pct is not None and is_md_enabled() and sim.sim_id != "SIM09":
            try:
                stop_loss_pct = max(float(stop_loss_pct) * 0.7, 0.05)
            except (TypeError, ValueError):
                pass
        if stop_loss_pct is not None:
            try:
                entry_price = float(trade.get("entry_price", 0))
                if entry_price > 0:
                    loss_pct = (current_price - entry_price) / entry_price
                    if loss_pct <= -abs(float(stop_loss_pct)):
                        should_exit = True
                        exit_reason = "stop_loss"
                        exit_context = f"loss_pct={loss_pct:.3%} <= -{abs(float(stop_loss_pct)):.3%}"
            except (TypeError, ValueError):
                pass

        profit_target_pct = profile.get("profit_target_pct")
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
                except (TypeError, ValueError):
                    pass

        # Near-TP adaptive lock + TP2 (match sim_engine logic)
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
                                lock_pct = max(0.05, min(base_target * 0.5, base_target - 0.02))
                            trade["lock_profit_pct"] = float(lock_pct)
                            trade["tp2_target_pct"] = float(base_target * tp2_mult)
                            trade["tp2_activated"] = True
                            sim.save()
            except (TypeError, ValueError):
                pass

        # Profit lock check
        lock_pct = trade.get("lock_profit_pct")
        if not should_exit and gain_pct is not None and trade.get("tp2_activated") and isinstance(lock_pct, (int, float)):
            try:
                if gain_pct <= float(lock_pct):
                    should_exit = True
                    exit_reason = "profit_lock"
                    exit_context = f"gain_pct={gain_pct:.3%} <= lock_pct={float(lock_pct):.3%}"
            except (TypeError, ValueError):
                pass

        # Effective target (TP2 overrides base)
        effective_target = profit_target_pct
        if trade.get("tp2_activated") and trade.get("tp2_target_pct") is not None:
            effective_target = trade.get("tp2_target_pct")

        if not should_exit and effective_target is not None:
            try:
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

        if not should_exit:
            hold_max_seconds = int(trade.get("hold_max_seconds", 0))
            if hold_max_seconds > 0 and elapsed_seconds >= hold_max_seconds:
                should_exit = True
                exit_reason = "hold_max_elapsed"
                exit_context = f"elapsed={int(elapsed_seconds)}s >= hold_max={int(hold_max_seconds)}s"

        if not should_exit or not exit_reason:
            return {"status": "skipped", "message": "no_exit"}

        qty = trade.get("qty")
        qty_val = _safe_int(qty)
        if qty_val is None:
            return {"status": "error", "message": "invalid_qty"}
        if qty_val <= 0:
            return {"status": "error", "message": "invalid_qty"}

        close_result = await asyncio.to_thread(close_option_position, option_symbol, qty_val)
        if not close_result.get("ok"):
            logging.warning("sim_live_exit_failed: %s", trade.get("trade_id"))
            return {"status": "error", "message": "exit_failed"}

        filled_avg_price = close_result.get("filled_avg_price")
        exit_price = filled_avg_price if filled_avg_price is not None else current_price
        exit_price_source = "broker_fill" if filled_avg_price is not None else "estimated_mid"

        exit_data = {
            "exit_price": exit_price,
            "exit_time": _now_et_iso(),
            "exit_reason": exit_reason,
            "exit_context": exit_context,
            "exit_price_source": exit_price_source,
            "exit_quote_model": "live_market",
            "entry_price_source": trade.get("entry_price_source", "live_fill"),
            "time_in_trade_seconds": int(elapsed_seconds),
            "spread_guard_bypassed": exit_price_source == "estimated_mid",
            "mae": trade.get("mae_pct"),
            "mfe": trade.get("mfe_pct"),
            "regime_at_entry": trade.get("regime_at_entry"),
            "time_of_day_bucket": trade.get("time_of_day_bucket"),
        }
        sim.record_close(trade.get("trade_id"), exit_data)
        sim.save()
        pnl_val = None
        try:
            entry_price_val = float(trade.get("entry_price", 0))
            if entry_price_val > 0:
                pnl_val = (exit_price - entry_price_val) * qty_val * 100
        except (TypeError, ValueError):
            pnl_val = None
        record_sim_trade_close(trade, pnl_val)

        return {
            "status": "success",
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "exit_context": exit_context,
            "option_symbol": option_symbol,
            "strike": trade.get("strike"),
            "expiry": trade.get("expiry"),
            "direction": trade.get("direction"),
            "qty": qty_val,
            "entry_price": trade.get("entry_price"),
            "pnl": pnl_val,
            "mae": trade.get("mae_pct"),
            "mfe": trade.get("mfe_pct"),
            "feature_snapshot": trade.get("feature_snapshot"),
            "balance_after": sim.balance,
            "time_in_trade_seconds": int(elapsed_seconds),
            "predicted_direction": trade.get("predicted_direction") or trade.get("direction"),
            "prediction_confidence": trade.get("prediction_confidence"),
            "edge_prob": trade.get("edge_prob"),
            "direction_prob": trade.get("direction_prob"),
        }
    except Exception as e:
        logging.exception("sim_live_exit_exception: %s", e)
        return {"status": "error", "message": str(e)}
