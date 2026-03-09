import os
import math
import yaml
import logging

from simulation.sim_portfolio import SimPortfolio


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")


def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return {k: v for k, v in raw.items() if str(k).upper().startswith("SIM") and isinstance(v, dict)}
    except Exception:
        return {}


_PROFILES = _load_profiles()


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _summarize_trade_log(trade_log: list[dict]) -> dict:
    total = len(trade_log)
    pnl_vals = []
    wins = 0
    for t in trade_log:
        pnl = _safe_float(t.get("realized_pnl_dollars"))
        if pnl is None:
            pnl = _safe_float(t.get("pnl"))
        if pnl is None:
            continue
        pnl_vals.append(pnl)
        if pnl > 0:
            wins += 1

    total_pnl = sum(pnl_vals) if pnl_vals else 0.0
    win_rate = (wins / total) if total > 0 else 0.0
    expectancy = (total_pnl / total) if total > 0 else 0.0

    mean_pnl = (sum(pnl_vals) / len(pnl_vals)) if pnl_vals else 0.0
    variance = 0.0
    if len(pnl_vals) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnl_vals) / (len(pnl_vals) - 1)
    std_pnl = math.sqrt(variance) if variance > 0 else 0.0
    sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0

    return {
        "total_trades": total,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "expectancy": expectancy,
        "sharpe": sharpe,
        "volatility": std_pnl,
    }


def _regime_breakdown(trade_log: list[dict]) -> dict:
    buckets: dict[str, list[float]] = {}
    for t in trade_log:
        regime = t.get("regime_at_entry") or "UNKNOWN"
        pnl = _safe_float(t.get("realized_pnl_dollars"))
        if pnl is None:
            pnl = _safe_float(t.get("pnl"))
        if pnl is None:
            continue
        buckets.setdefault(regime, []).append(pnl)

    out = {}
    for regime, pnls in buckets.items():
        total = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        total_pnl = sum(pnls)
        win_rate = (wins / total) if total > 0 else 0.0
        expectancy = (total_pnl / total) if total > 0 else 0.0
        out[regime] = {
            "total_trades": total,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "expectancy": expectancy,
        }
    return out


def _confidence_distribution(trade_log: list[dict]) -> dict:
    buckets = {"low": 0, "medium": 0, "high": 0, "unknown": 0}
    for t in trade_log:
        conf = _safe_float(t.get("prediction_confidence"))
        if conf is None:
            conf = _safe_float(t.get("confidence"))
        if conf is None:
            buckets["unknown"] += 1
        elif conf < 0.6:
            buckets["low"] += 1
        elif conf < 0.75:
            buckets["medium"] += 1
        else:
            buckets["high"] += 1

    total = sum(buckets.values())
    dist = {}
    for key, count in buckets.items():
        pct = (count / total) if total > 0 else 0.0
        dist[key] = {"count": count, "pct": pct}
    return dist


def get_sim_performance_profile(sim_id: str) -> dict:
    profile = _PROFILES.get(sim_id, {})
    sim = SimPortfolio(sim_id, profile)
    try:
        sim.load()
    except Exception:
        logging.exception("sim_metrics_load_failed: %s", sim_id)
        return {"sim_id": sim_id, "error": "load_failed"}

    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
    summary = _summarize_trade_log(trade_log)
    regime_stats = _regime_breakdown(trade_log)
    confidence_stats = _confidence_distribution(trade_log)

    return {
        "sim_id": sim_id,
        "profile_name": profile.get("name"),
        "balance": sim.balance,
        "peak_balance": sim.peak_balance,
        "open_trades": len(sim.open_trades) if isinstance(sim.open_trades, list) else 0,
        "summary": summary,
        "regime_stats": regime_stats,
        "confidence_stats": confidence_stats,
    }


def compare_sim_performance(sim_a: str, sim_b: str) -> dict:
    a = get_sim_performance_profile(sim_a)
    b = get_sim_performance_profile(sim_b)
    a_sum = a.get("summary", {})
    b_sum = b.get("summary", {})

    a_score = (a_sum.get("expectancy", 0.0), a_sum.get("sharpe", 0.0))
    b_score = (b_sum.get("expectancy", 0.0), b_sum.get("sharpe", 0.0))

    if a_score > b_score:
        winner = sim_a
    elif b_score > a_score:
        winner = sim_b
    else:
        winner = "tie"

    return {
        "sim_a": sim_a,
        "sim_b": sim_b,
        "winner": winner,
        "sim_a_summary": a_sum,
        "sim_b_summary": b_sum,
    }
