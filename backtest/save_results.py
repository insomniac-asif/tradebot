"""
backtest/save_results.py
Save backtest results to JSON in two formats:
1. Per-sim: backtest/results/{profile_id}_summary.json
2. Combined dashboard format: backtest/results/dashboard_data.json
"""
from __future__ import annotations
import json
import os
from backtest.results import BacktestSummary

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


def _summary_to_dict(summary: BacktestSummary) -> dict:
    """Convert BacktestSummary dataclass to a plain dict."""
    runs_out = []
    for r in summary.runs:
        if isinstance(r, dict):
            runs_out.append(r)
        else:
            # Dataclass instance
            runs_out.append({
                "sim_profile": r.sim_profile,
                "signal_mode": r.signal_mode,
                "symbol": r.symbol,
                "run_number": r.run_number,
                "start_date": r.start_date,
                "end_date": r.end_date,
                "starting_balance": r.starting_balance,
                "final_balance": r.final_balance,
                "peak_balance": r.peak_balance,
                "outcome": r.outcome,
                "hit_target": r.hit_target,
                "target_hit_date": r.target_hit_date,
                "total_trades": r.total_trades,
                "wins": r.wins,
                "losses": r.losses,
                "win_rate": r.win_rate,
                "total_pnl": r.total_pnl,
                "avg_win": r.avg_win,
                "avg_loss": r.avg_loss,
                "profit_factor": r.profit_factor,
                "max_drawdown_pct": r.max_drawdown_pct,
                "max_drawdown_dollars": r.max_drawdown_dollars,
                "days_active": r.days_active,
                "trades": r.trades if hasattr(r, "trades") else [],
                "equity_curve": r.equity_curve if hasattr(r, "equity_curve") else [],
            })

    return {
        "sim_profile": summary.sim_profile,
        "signal_mode": summary.signal_mode,
        "symbol": summary.symbol,
        "total_runs": summary.total_runs,
        "blown_count": summary.blown_count,
        "target_hit_count": summary.target_hit_count,
        "best_run_number": summary.best_run_number,
        "worst_run_number": summary.worst_run_number,
        "avg_trades_per_run": summary.avg_trades_per_run,
        "avg_win_rate": summary.avg_win_rate,
        "avg_max_drawdown": summary.avg_max_drawdown,
        "runs": runs_out,
    }


def save_sim_summary(summary: BacktestSummary) -> str:
    """
    Save per-sim summary to backtest/results/{profile_id}_summary.json.
    Returns the file path.
    """
    data = _summary_to_dict(summary)
    path = os.path.join(RESULTS_DIR, f"{summary.sim_profile}_summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def build_win_rate_chart_data(runs: list) -> dict:
    """
    Build win_rate_chart data in the same format the live dashboard uses.
    Each run produces a series of {trade_num, win_rate} points.
    The last run is marked is_current=True (matches dashboard renderWinRateChart).
    """
    chart_runs = []
    for i, r in enumerate(runs):
        if isinstance(r, dict):
            trades = r.get("trades") or []
            run_num = r.get("run_number", i + 1)
        else:
            trades = r.trades if hasattr(r, "trades") else []
            run_num = r.run_number

        if not trades:
            continue

        points = []
        wins = 0
        for j, t in enumerate(trades, 1):
            if isinstance(t, dict):
                pnl = float(t.get("realized_pnl_dollars") or t.get("pnl") or 0)
            else:
                pnl = float(getattr(t, "pnl", 0))
            if pnl > 0:
                wins += 1
            wr = (wins / j) * 100
            points.append({"trade_num": j, "win_rate": round(wr, 1)})

        is_current = (i == len(runs) - 1)
        chart_runs.append({
            "run_number": run_num,
            "is_current": is_current,
            "points": points,
        })

    return {"runs": chart_runs}


def save_dashboard_data(summaries: list[BacktestSummary]) -> str:
    """
    Save combined dashboard data to backtest/results/dashboard_data.json.

    Format:
    {
      "SIM01": {
        "summary": {...},
        "win_rate_chart": {"runs": [...]},
        "equity_curves": [...],
        "trade_log": [...]  -- all trades across all runs, flattened
      },
      ...
    }
    """
    output = {}
    for summary in summaries:
        sim_id = summary.sim_profile.upper()
        data = _summary_to_dict(summary)

        # Build win rate chart data
        win_rate_chart = build_win_rate_chart_data(data.get("runs") or [])

        # Flatten equity curves: list of {timestamp, balance, run_number}
        equity_curves = []
        for r in (data.get("runs") or []):
            for pt in (r.get("equity_curve") or []):
                equity_curves.append({
                    "timestamp": pt.get("timestamp"),
                    "balance": pt.get("balance"),
                    "run_number": r.get("run_number"),
                })

        # Flatten all trades across runs — use live sim field naming
        # so existing dashboard charts can reuse them
        trade_log = []
        for r in (data.get("runs") or []):
            run_num = r.get("run_number", 1)
            for t in (r.get("trades") or []):
                trade_dict = dict(t)
                # Ensure live-sim-compatible field names
                trade_dict.setdefault("realized_pnl_dollars", trade_dict.get("pnl"))
                trade_dict.setdefault("balance_after_trade", trade_dict.get("balance_after"))
                trade_dict["_run_number"] = run_num
                trade_log.append(trade_dict)

        output[sim_id] = {
            "summary": data,
            "win_rate_chart": win_rate_chart,
            "equity_curves": equity_curves,
            "trade_log": trade_log,
        }

    path = os.path.join(RESULTS_DIR, "dashboard_data.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)
    return path
