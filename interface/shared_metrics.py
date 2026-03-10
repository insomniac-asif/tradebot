"""
interface/shared_metrics.py
----------------------------
Extracted from interface/shared_state.py to keep that file under 500 lines.
Contains _collect_sim_metrics, a large helper used by sim stat commands.
"""

import os
from datetime import datetime

from simulation.sim_portfolio import SimPortfolio


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _load_sim_profiles() -> dict:
    import yaml
    sim_config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
    )
    try:
        with open(sim_config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _collect_sim_metrics():
    def _parse_ts(val):
        try:
            if not val:
                return None
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    profiles = _load_sim_profiles()
    if not profiles:
        return [], {}

    metrics = []
    for sim_key, profile in profiles.items():
        try:
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                continue
            sim = SimPortfolio(sim_key, profile)
            sim.load()
            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            pnl_vals = []
            wins = 0
            win_loss_seq = []
            regime_stats = {}
            time_stats = {}
            dte_stats = {}
            setup_stats = {}
            exit_counts = {}
            hold_times = []
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    continue
                pnl_vals.append(pnl_val)
                if pnl_val > 0:
                    wins += 1
                    win_loss_seq.append(1)
                else:
                    win_loss_seq.append(0)

                regime_key = t.get("regime_at_entry") or "UNKNOWN"
                regime_stats.setdefault(regime_key, {"wins": 0, "total": 0})
                regime_stats[regime_key]["total"] += 1
                if pnl_val > 0:
                    regime_stats[regime_key]["wins"] += 1

                time_key = t.get("time_of_day_bucket") or "UNKNOWN"
                time_stats.setdefault(time_key, {"wins": 0, "total": 0})
                time_stats[time_key]["total"] += 1
                if pnl_val > 0:
                    time_stats[time_key]["wins"] += 1

                dte_key = t.get("dte_bucket") or "UNKNOWN"
                dte_stats.setdefault(
                    dte_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                dte_stats[dte_key]["total"] += 1
                dte_stats[dte_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    dte_stats[dte_key]["wins"] += 1
                    dte_stats[dte_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    dte_stats[dte_key]["pnl_neg"] += abs(pnl_val)

                setup_key = t.get("setup") or t.get("setup_type") or "UNKNOWN"
                setup_stats.setdefault(
                    setup_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                setup_stats[setup_key]["total"] += 1
                setup_stats[setup_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    setup_stats[setup_key]["wins"] += 1
                    setup_stats[setup_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    setup_stats[setup_key]["pnl_neg"] += abs(pnl_val)

                reason = t.get("exit_reason", "unknown") or "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1

                hold_sec = _safe_float(t.get("time_in_trade_seconds"))
                if hold_sec is not None:
                    hold_times.append(hold_sec)

            total_trades = len(trade_log)
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            win_rate = wins / total_trades if total_trades > 0 else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            max_win = max(pnl_vals) if pnl_vals else 0.0
            max_loss = min(pnl_vals) if pnl_vals else 0.0
            win_sum = sum(p for p in pnl_vals if p > 0)
            loss_sum = abs(sum(p for p in pnl_vals if p < 0))
            profit_factor = win_sum / loss_sum if loss_sum > 0 else None

            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            return_pct = (total_pnl / start_balance) if start_balance > 0 else 0.0
            peak_balance = _safe_float(getattr(sim, "peak_balance", None)) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0

            times = []
            for t in trade_log:
                ts = _parse_ts(t.get("exit_time")) or _parse_ts(t.get("entry_time"))
                if ts:
                    times.append(ts)
            if len(times) >= 2:
                days_active = max((max(times) - min(times)).total_seconds() / 86400.0, 1 / 24)
            else:
                days_active = 1.0
            equity_speed = total_pnl / days_active if days_active else None

            max_win_streak = 0
            max_loss_streak = 0
            cur_win = 0
            cur_loss = 0
            for outcome in win_loss_seq:
                if outcome == 1:
                    cur_win += 1
                    cur_loss = 0
                else:
                    cur_loss += 1
                    cur_win = 0
                max_win_streak = max(max_win_streak, cur_win)
                max_loss_streak = max(max_loss_streak, cur_loss)

            avg_hold = sum(hold_times) / len(hold_times) if hold_times else None
            pnl_stdev = None
            pnl_median = None
            try:
                if len(pnl_vals) >= 2:
                    import statistics
                    pnl_stdev = statistics.pstdev(pnl_vals)
                    pnl_median = statistics.median(pnl_vals)
            except Exception:
                pnl_stdev = None
                pnl_median = None

            metrics.append({
                "sim_id": sim_key,
                "name": profile.get("name", sim_key),
                "trades": total_trades,
                "wins": wins,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "return_pct": return_pct,
                "expectancy": expectancy,
                "max_win": max_win,
                "max_loss": max_loss,
                "profit_factor": profit_factor,
                "max_drawdown": max_drawdown,
                "equity_speed": equity_speed,
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "avg_hold": avg_hold,
                "pnl_stdev": pnl_stdev,
                "pnl_median": pnl_median,
                "regime_stats": regime_stats,
                "time_stats": time_stats,
                "dte_stats": dte_stats,
                "setup_stats": setup_stats,
                "exit_counts": exit_counts,
                "start_balance": start_balance,
                "balance": balance,
            })
        except Exception:
            continue

    return metrics, profiles
