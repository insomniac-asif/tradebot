"""
dashboard/app_helpers2.py
--------------------------
Overflow helpers for dashboard/app.py — extracted route bodies too large to
fit in app_helpers.py without exceeding the 500-line limit.
"""

from dashboard.app_helpers import (
    _load_config,
    _load_sim,
    _compute_stats,
    _safe_float,
    _parse_underlying,
)


def _handle_get_sim(sim_id: str) -> dict:
    """Body of GET /api/sim/{sim_id}. Returns the full sim detail dict."""
    config = _load_config()
    profile = config.get(sim_id)
    if not isinstance(profile, dict):
        return None  # caller raises 404

    data = _load_sim(sim_id)
    if data is None:
        bs = float(profile.get("balance_start", 25000))
        _cfg_syms = profile.get("symbols") or ([profile.get("symbol")] if profile.get("symbol") else ["SPY"])
        stub_stats = {
            "sim_id": sim_id,
            "name": profile.get("name", sim_id),
            "signal_mode": profile.get("signal_mode", ""),
            "strategy_family": profile.get("signal_mode", "").lower().replace("_", " ").title(),
            "features_enabled": bool(profile.get("features_enabled")),
            "horizon": profile.get("horizon", ""),
            "dte_min": profile.get("dte_min"),
            "dte_max": profile.get("dte_max"),
            "symbols": _cfg_syms,
            "balance": bs,
            "balance_start": bs,
            "pnl_dollars": 0.0,
            "pnl_pct": 0.0,
            "total_trades": 0,
            "win_rate": None,
            "avg_pnl": None,
            "total_pnl": 0.0,
            "daily_loss": 0.0,
            "open_count": 0,
            "open_trade": None,
            "open_trades": [],
            "best_trade": None,
            "worst_trade": None,
            "max_drawdown_pct": 0.0,
            "symbol_stats": {},
            "session": {"trades": 0, "open": 0, "pnl": 0.0, "win_rate": None, "best": None, "worst": None},
            "streak": None,
        }
        return {
            "sim_id": sim_id,
            "name": profile.get("name", sim_id),
            "profile": {k: v for k, v in profile.items() if not str(k).startswith("_")},
            "stats": stub_stats,
            "open_trades": [],
            "recent_trades": [],
            "balance_history": [],
        }

    stats = _compute_stats(sim_id, data, profile)

    # Balance history (running cumulative)
    trade_log = data.get("trade_log") or []
    balance_history = []
    running = float(profile.get("balance_start", 25000))
    for t in trade_log:
        pnl = t.get("realized_pnl_dollars")
        if pnl is None:
            continue
        try:
            running += float(pnl)
            balance_history.append({
                "time": t.get("exit_time", ""),
                "balance": round(running, 2),
                "pnl": round(float(pnl), 2),
                "exit_reason": t.get("exit_reason", ""),
            })
        except Exception:
            pass

    # SL/TP pcts from profile (for display)
    _sl_pct = _safe_float(profile.get("stop_loss_pct"), 0)
    _tp_pct = _safe_float(profile.get("profit_target_pct"), 0)

    # Recent trades (newest first, last 30)
    recent_trades = []
    for t in reversed(trade_log[-30:]):
        ep = t.get("entry_price")
        sl_price = round(ep * (1 - _sl_pct), 4) if ep and _sl_pct else None
        tp_price = round(ep * (1 + _tp_pct), 4) if ep and _tp_pct else None
        recent_trades.append({
            "trade_id": (t.get("trade_id") or "")[-8:],
            "symbol": (t.get("symbol") or _parse_underlying(t.get("option_symbol", ""))).upper(),
            "entry_time": t.get("entry_time", ""),
            "exit_time": t.get("exit_time", ""),
            "direction": t.get("direction", ""),
            "option_symbol": t.get("option_symbol", ""),
            "strike": t.get("strike"),
            "expiry": t.get("expiry"),
            "contract_type": t.get("contract_type"),
            "entry_price": ep,
            "exit_price": t.get("exit_price"),
            "sl_price": sl_price,
            "tp_price": tp_price,
            "qty": t.get("qty"),
            "pnl": t.get("realized_pnl_dollars"),
            "pnl_pct": t.get("realized_pnl_pct"),
            "exit_reason": t.get("exit_reason", ""),
            "exit_context": t.get("exit_context", ""),
            "regime": t.get("regime_at_entry", ""),
            "time_bucket": t.get("time_of_day_bucket", ""),
            "structure_score": t.get("structure_score"),
            "strategy_family": t.get("strategy_family", ""),
        })

    return {
        "sim_id": sim_id,
        "name": profile.get("name", sim_id),
        "profile": {k: v for k, v in profile.items() if not str(k).startswith("_")},
        "stats": stats,
        "open_trades": data.get("open_trades") or [],
        "recent_trades": recent_trades,
        "balance_history": balance_history,
    }
