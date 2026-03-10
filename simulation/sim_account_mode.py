"""simulation/sim_account_mode.py

Small-account compounding framework for QQQBot simulators.

Centralises:
  - Account phase detection (MICRO / EARLY_GROWTH / SCALING)
  - Position sizing with small-account constraints
  - Account death detection and quarantine
  - Post-mortem report generation

All paper sims call compute_small_account_qty() instead of the raw
risk_dollars / (mid * 100) formula so that sizing always respects the
current balance rather than a fantasy $25k assumption.

Config keys read from each sim's profile (all have hardcoded defaults):
  risk_per_trade_pct   – fraction of balance to risk per trade (default 0.02)
  max_position_pct     – max single-trade notional as fraction of balance (default 0.15)
  death_threshold      – balance at or below which sim is declared dead (default 25.0)
"""

import json
import logging
import math
import os
from datetime import datetime

import pytz

# ── Account phase thresholds (USD balance) ───────────────────────────────────
PHASE_MICRO       = "MICRO"        # $0 – < $1,000
PHASE_EARLY       = "EARLY_GROWTH" # $1,000 – < $2,500
PHASE_SCALING     = "SCALING"      # $2,500+

_PHASE_BOUNDS = [
    (0,     1_000, PHASE_MICRO),
    (1_000, 2_500, PHASE_EARLY),
    (2_500, 1e9,   PHASE_SCALING),
]

# ── Sizing defaults (overridable per-sim via profile keys) ───────────────────
DEFAULT_BALANCE_START    = 500.0
DEFAULT_DEATH_THRESHOLD  = 25.0
DEFAULT_RISK_PCT         = 0.02    # 2 % of current balance per trade
DEFAULT_MAX_POSITION_PCT = 0.15    # 15 % of balance max per single trade
DEFAULT_MAX_DAILY_DD_PCT = 0.06    # 6 % daily drawdown cap

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POSTMORTEM_DIR = os.path.join(BASE_DIR, "data", "postmortems")


# ── Phase detection ──────────────────────────────────────────────────────────

def get_account_phase(balance: float) -> str:
    """Return the growth phase label for the current balance."""
    for lo, hi, phase in _PHASE_BOUNDS:
        if lo <= balance < hi:
            return phase
    return PHASE_SCALING


# ── Position sizing ──────────────────────────────────────────────────────────

def compute_small_account_qty(
    balance: float,
    fill_price: float,
    profile: dict,
) -> tuple[int, float, str | None]:
    """
    Compute how many option contracts to buy, respecting small-account limits.

    Returns (qty, risk_dollars, block_reason).
    When block_reason is not None the caller should skip the trade entirely.

    Sizing rules:
      1. Balance at or below death_threshold → block (account is blown)
      2. One contract notional > max_position_pct * balance → block (too expensive)
      3. risk_dollars = balance * risk_per_trade_pct, floored at 1 % of balance
      4. qty = floor(risk_dollars / (fill_price * 100)), min 1
      5. Reduce qty until notional fits within max_position_pct * balance
    """
    if fill_price <= 0:
        return 0, 0.0, "invalid_fill_price"

    death_threshold = float(profile.get("death_threshold", DEFAULT_DEATH_THRESHOLD))
    if balance <= death_threshold:
        return 0, 0.0, "balance_below_death_threshold"

    risk_pct     = float(profile.get("risk_per_trade_pct", DEFAULT_RISK_PCT))
    max_pos_pct  = float(profile.get("max_position_pct", DEFAULT_MAX_POSITION_PCT))

    one_contract_cost = fill_price * 100
    max_notional      = balance * max_pos_pct

    # One contract alone exceeds max position limit – skip
    if one_contract_cost > max_notional:
        return 0, 0.0, "contract_too_expensive_for_account"

    risk_dollars = balance * risk_pct
    # Minimum risk = 1 % of balance (never below $3 to avoid micro-noise)
    min_risk     = max(3.0, balance * 0.01)
    risk_dollars = max(risk_dollars, min_risk)

    qty = max(1, math.floor(risk_dollars / one_contract_cost))

    # Trim qty so total notional stays within max_position_pct
    while qty > 1 and (fill_price * qty * 100) > max_notional:
        qty -= 1

    return qty, risk_dollars, None


# ── Death detection & quarantine ─────────────────────────────────────────────

def check_and_handle_death(sim) -> bool:
    """
    Inspect sim.balance against its death_threshold.

    Returns True if the sim is (or just became) dead.
    On first death detection:
      - Sets sim.is_dead = True, sim.death_time, sim.death_balance
      - Increments sim.reset_count
      - Writes a post-mortem JSON report to data/postmortems/
      - Emits an ERROR-level log so it appears in system.log

    Callers must still call sim.save() after this to persist the dead flag.
    """
    if getattr(sim, "is_dead", False):
        return True  # already dead

    profile         = sim.profile or {}
    death_threshold = float(profile.get("death_threshold", DEFAULT_DEATH_THRESHOLD))

    if sim.balance > death_threshold:
        return False

    # ── First death ──────────────────────────────────────────────────────────
    sim.is_dead      = True
    sim.death_time   = _now_et_iso()
    sim.death_balance = round(sim.balance, 4)
    sim.reset_count  = getattr(sim, "reset_count", 0)  # preserve existing count

    _write_postmortem(sim)

    logging.error(
        "sim_account_death: sim_id=%s balance=%.4f threshold=%.2f resets=%d",
        sim.sim_id, sim.balance, death_threshold,
        getattr(sim, "reset_count", 0),
    )
    return True


# ── Post-mortem report ───────────────────────────────────────────────────────

def _write_postmortem(sim) -> None:
    """Write a structured JSON post-mortem when a sim account is blown."""
    try:
        os.makedirs(POSTMORTEM_DIR, exist_ok=True)

        trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
        profile   = sim.profile or {}

        wins   = [t for t in trade_log if isinstance(t, dict) and float(t.get("realized_pnl_dollars") or 0) > 0]
        losses = [t for t in trade_log if isinstance(t, dict) and float(t.get("realized_pnl_dollars") or 0) <= 0]

        total      = len(trade_log)
        win_rate   = len(wins) / total if total else 0
        avg_win    = (sum(float(t.get("realized_pnl_dollars") or 0) for t in wins)   / len(wins))   if wins   else 0
        avg_loss   = (sum(float(t.get("realized_pnl_dollars") or 0) for t in losses) / len(losses)) if losses else 0
        biggest_loss = min((float(t.get("realized_pnl_dollars") or 0) for t in trade_log), default=0)
        biggest_win  = max((float(t.get("realized_pnl_dollars") or 0) for t in trade_log), default=0)

        # Gross profit / gross loss → profit factor
        gross_profit = sum(float(t.get("realized_pnl_dollars") or 0) for t in wins)
        gross_loss   = abs(sum(float(t.get("realized_pnl_dollars") or 0) for t in losses))
        profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else None

        # Exit reason breakdown
        exit_reasons: dict[str, int] = {}
        for t in trade_log:
            r = t.get("exit_reason") or "unknown"
            exit_reasons[r] = exit_reasons.get(r, 0) + 1

        # Regime breakdown
        regimes: dict[str, int] = {}
        for t in trade_log:
            r = t.get("regime_at_entry") or "unknown"
            regimes[r] = regimes.get(r, 0) + 1

        # Signal mode breakdown (loss count)
        signal_losses: dict[str, int] = {}
        for t in losses:
            s = t.get("signal_mode") or "unknown"
            signal_losses[s] = signal_losses.get(s, 0) + 1

        start_balance   = float(profile.get("balance_start", DEFAULT_BALANCE_START))
        total_return    = (sim.death_balance - start_balance) / start_balance if start_balance > 0 else 0

        # Max drawdown from equity curve (reconstructed from trade_log)
        max_drawdown    = _compute_max_drawdown(trade_log, start_balance)

        # Consecutive loss stats
        max_consec_loss = _max_consecutive_losses(trade_log)

        summary = {
            "sim_id":             sim.sim_id,
            "strategy_name":      profile.get("name", ""),
            "signal_mode":        profile.get("signal_mode", ""),
            "death_time":         sim.death_time,
            "reset_count":        getattr(sim, "reset_count", 0),
            # Balance journey
            "start_balance":      start_balance,
            "peak_balance":       round(sim.peak_balance, 4),
            "death_balance":      round(sim.death_balance, 4),
            "total_return_pct":   round(total_return * 100, 2),
            "max_drawdown_pct":   round(max_drawdown * 100, 2),
            # Trade stats
            "total_trades":       total,
            "win_count":          len(wins),
            "loss_count":         len(losses),
            "win_rate_pct":       round(win_rate * 100, 2),
            "avg_win_dollars":    round(avg_win, 4),
            "avg_loss_dollars":   round(avg_loss, 4),
            "biggest_win_dollars": round(biggest_win, 4),
            "biggest_loss_dollars": round(biggest_loss, 4),
            "profit_factor":      profit_factor,
            "max_consecutive_losses": max_consec_loss,
            # Breakdowns for later analysis
            "exit_reason_breakdown":  exit_reasons,
            "regime_breakdown":       regimes,
            "signal_loss_breakdown":  signal_losses,
            # Diagnosis
            "likely_cause":       _diagnose_failure(trade_log, profile),
        }

        ts       = datetime.now(pytz.timezone("US/Eastern")).strftime("%Y%m%d_%H%M%S")
        filename = f"{sim.sim_id}_postmortem_{ts}.json"
        path     = os.path.join(POSTMORTEM_DIR, filename)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=4)

        logging.error("sim_postmortem_written: sim_id=%s path=%s", sim.sim_id, path)

    except Exception:
        logging.exception("sim_postmortem_write_error: sim_id=%s", getattr(sim, "sim_id", "?"))


def _compute_max_drawdown(trade_log: list, start_balance: float) -> float:
    running = start_balance
    peak    = start_balance
    max_dd  = 0.0
    for t in trade_log:
        if not isinstance(t, dict):
            continue
        pnl = float(t.get("realized_pnl_dollars") or 0)
        running += pnl
        peak     = max(peak, running)
        dd       = (peak - running) / peak if peak > 0 else 0.0
        max_dd   = max(max_dd, dd)
    return max_dd


def _max_consecutive_losses(trade_log: list) -> int:
    max_streak = 0
    streak     = 0
    for t in trade_log:
        if not isinstance(t, dict):
            continue
        pnl = float(t.get("realized_pnl_dollars") or 0)
        if pnl < 0:
            streak  += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def _diagnose_failure(trade_log: list, profile: dict) -> str:
    """Return a plain-English diagnosis string for the post-mortem."""
    if not trade_log:
        return "no_trades_taken"

    total  = len(trade_log)
    if total < 3:
        return "too_few_trades_for_diagnosis"

    wins   = [t for t in trade_log if isinstance(t, dict) and float(t.get("realized_pnl_dollars") or 0) > 0]
    losses = [t for t in trade_log if isinstance(t, dict) and float(t.get("realized_pnl_dollars") or 0) < 0]

    win_rate  = len(wins)   / total
    avg_loss  = abs(sum(float(t.get("realized_pnl_dollars") or 0) for t in losses) / len(losses)) if losses else 0
    avg_win   = sum(float(t.get("realized_pnl_dollars") or 0) for t in wins)   / len(wins)   if wins else 0

    reasons = []

    if win_rate < 0.35:
        reasons.append("low_win_rate")

    if losses and wins and avg_loss > avg_win * 1.8:
        reasons.append("unfavorable_risk_reward")

    stop_exits = [t for t in trade_log if isinstance(t.get("exit_reason"), str) and "stop" in t["exit_reason"]]
    if len(stop_exits) / total > 0.55:
        reasons.append("frequent_stop_outs")

    # Worst regime
    regime_loss: dict[str, int] = {}
    for t in losses:
        r = t.get("regime_at_entry") or "UNKNOWN"
        regime_loss[r] = regime_loss.get(r, 0) + 1
    if regime_loss:
        worst_r = max(regime_loss, key=lambda r: regime_loss[r])
        if regime_loss[worst_r] / max(len(losses), 1) > 0.55:
            reasons.append(f"regime_mismatch:{worst_r}")

    # Sizing symptom: if avg loss is much larger than max_position_pct implies
    max_pos_pct = float(profile.get("max_position_pct", DEFAULT_MAX_POSITION_PCT))
    start_bal   = float(profile.get("balance_start", DEFAULT_BALANCE_START))
    implied_max = start_bal * max_pos_pct
    if avg_loss > implied_max * 1.5:
        reasons.append("oversizing_symptom")

    return ", ".join(reasons) if reasons else "gradual_capital_erosion"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _now_et_iso() -> str:
    return datetime.now(pytz.timezone("US/Eastern")).isoformat()
