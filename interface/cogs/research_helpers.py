"""
interface/cogs/research_helpers.py
Pure helper/utility functions for research_commands.py.
No Discord decorators here — only formatting, parsing, and analysis logic.
"""

import os
import re
import json
import pytz
from datetime import datetime, timedelta


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _norm_sim_id(raw: str) -> "str | None":
    text = str(raw).strip().upper()
    if not text:
        return None
    if text.startswith("SIM"):
        suffix = text.replace("SIM", "").strip()
        if suffix.isdigit():
            return f"SIM{int(suffix):02d}"
        return text
    if text.isdigit():
        return f"SIM{int(text):02d}"
    return None


def _fmt_pct(val, decimals=1) -> str:
    try:
        return f"{float(val) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_secs(seconds) -> str:
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return "N/A"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s / 3600:.1f}h"
    return f"{s / 86400:.1f}d"


def _parse_trade_dt(raw):
    if not raw:
        return None
    try:
        eastern = pytz.timezone("US/Eastern")
        dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is None:
            dt = eastern.localize(dt)
        else:
            dt = dt.astimezone(eastern)
        return dt
    except Exception:
        return None


def _parse_strike(symbol):
    if not symbol or not isinstance(symbol, str):
        return None
    try:
        return int(symbol[-8:]) / 1000.0
    except Exception:
        return None


def _extract_reason(entry_context):
    if not entry_context or "reason=" not in str(entry_context):
        return None
    try:
        return str(entry_context).split("reason=", 1)[1].split("|")[0].strip()
    except Exception:
        return None


# ── Sim context builders ───────────────────────────────────────────────────────

def _extract_target_sims(question_text: str) -> "list[str]":
    found = set()
    for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
        norm = _norm_sim_id(match)
        if norm:
            found.add(norm)
    return sorted(found)


def build_sim_context(question_text: str, load_sim_profiles_fn, strategy_intents: dict) -> "str | None":
    """Build a context string for AI queries mentioning SIM IDs."""
    q = question_text.lower()
    if "sim" not in q:
        return None

    profiles = load_sim_profiles_fn()
    if not profiles:
        return None

    found = set()
    for match in re.findall(r"\bSIM\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
        norm = _norm_sim_id(match)
        if norm:
            found.add(norm)
    for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
        norm = _norm_sim_id(match.replace("sim", "SIM"))
        if norm:
            found.add(norm)

    include_all = ("all sims" in q) or ("all sim" in q) or (len(found) == 0)
    sim_ids = sorted([k for k in profiles.keys() if k.startswith("SIM")]) if include_all else sorted(found)

    lines = []
    for sid in sim_ids:
        profile = profiles.get(sid)
        if not isinstance(profile, dict):
            continue
        name = profile.get("name", sid)
        mode = profile.get("signal_mode", "N/A")
        horizon = profile.get("horizon", "N/A")
        exec_mode = str(profile.get("execution_mode", "sim")).upper()
        dte_min = profile.get("dte_min", "N/A")
        dte_max = profile.get("dte_max", "N/A")
        hold_min = _fmt_secs(profile.get("hold_min_seconds"))
        hold_max = _fmt_secs(profile.get("hold_max_seconds"))
        cutoff = profile.get("cutoff_time_et", "N/A")
        stop_pct = _fmt_pct(profile.get("stop_loss_pct"))
        target_pct = _fmt_pct(profile.get("profit_target_pct"))
        risk_pct = _fmt_pct(profile.get("risk_per_trade_pct"), 2)
        daily_loss = _fmt_pct(profile.get("daily_loss_limit_pct"), 2)
        max_open = profile.get("max_open_trades", "N/A")
        max_spread = profile.get("max_spread_pct", "N/A")
        features = profile.get("features_enabled", False)

        gates = []
        if profile.get("regime_filter"):
            gates.append(f"regime={profile.get('regime_filter')}")
        if profile.get("orb_minutes") is not None:
            gates.append(f"orb_minutes={profile.get('orb_minutes')}")
        if profile.get("vol_z_min") is not None:
            gates.append(f"vol_z_min={profile.get('vol_z_min')}")
        if profile.get("atr_expansion_min") is not None:
            gates.append(f"atr_expansion_min={profile.get('atr_expansion_min')}")
        gate_text = ("gates: " + ", ".join(gates)) if gates else "gates: none"

        # Sim state (if available)
        state_line = None
        try:
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
            )
            if os.path.exists(sim_path):
                with open(sim_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                open_count = len(data.get("open_trades", [])) if isinstance(data.get("open_trades"), list) else 0
                trade_count = len(data.get("trade_log", [])) if isinstance(data.get("trade_log"), list) else 0
                balance = data.get("balance")
                peak = data.get("peak_balance")
                state_line = f"state: open={open_count} trades={trade_count} balance={balance} peak={peak}"
        except Exception:
            state_line = None

        lines.append(f"{sid} \u2014 {name}")
        lines.append(f"strategy: {strategy_intents.get(sid, 'Configured strategy profile.')}")
        lines.append(f"mode={mode} horizon={horizon} exec={exec_mode} features={features}")
        lines.append(f"DTE {dte_min}-{dte_max} | hold {hold_min}-{hold_max} | cutoff {cutoff}")
        lines.append(f"risk {risk_pct} | daily_loss {daily_loss} | max_open {max_open} | max_spread {max_spread}")
        lines.append(f"stop {stop_pct} | target {target_pct}")
        lines.append(gate_text)
        if state_line:
            lines.append(state_line)
        lines.append("\u2014")

    context = "\n".join(lines).strip()
    if len(context) > 2500:
        context = context[:2500] + "\n\u2026(truncated)"
    return context


def build_sim_fallback_answer(question_text: str, load_sim_profiles_fn, strategy_intents: dict) -> "str | None":
    """Build a deterministic local answer for SIM-specific questions when AI gives a weak reply."""
    profiles = load_sim_profiles_fn()
    targets = _extract_target_sims(question_text)
    if not targets:
        return None

    sid = targets[0]
    profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
    sim_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
    )
    if not os.path.exists(sim_path):
        return f"{sid}: no sim state file found yet. It may not have initialized trades yet."

    try:
        with open(sim_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return f"{sid}: unable to read sim state, so I cannot analyze its performance yet."

    trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
    open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
    eastern = pytz.timezone("US/Eastern")

    closed = []
    for t in trade_log:
        try:
            pnl = float(t.get("realized_pnl_dollars"))
        except (TypeError, ValueError):
            continue
        dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
        closed.append({"trade": t, "pnl": pnl, "dt": dt})

    if not closed:
        mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
        dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
        dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
        return (
            f"{sid} has no closed trades yet (open: {len(open_trades)}). "
            f"Configured mode={mode}, DTE={dte_min}-{dte_max}. "
            "No performance diagnosis is possible until more closes are logged."
        )

    q = question_text.lower()
    scope = closed
    if "yesterday" in q:
        yday = (datetime.now(eastern).date() - timedelta(days=1))
        scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
        if scoped:
            scope = scoped

    total = len(scope)
    wins = sum(1 for x in scope if x["pnl"] > 0)
    losses = total - wins
    wr = (wins / total) * 100 if total else 0.0
    total_pnl = sum(x["pnl"] for x in scope)
    avg = total_pnl / total if total else 0.0
    avg_win = (sum(x["pnl"] for x in scope if x["pnl"] > 0) / wins) if wins else 0.0
    avg_loss = (sum(x["pnl"] for x in scope if x["pnl"] <= 0) / losses) if losses else 0.0

    exit_counts = {}
    for item in scope:
        t = item["trade"]
        reason = (t.get("exit_reason") or "unknown").strip()
        exit_counts[reason] = exit_counts.get(reason, 0) + 1
    top_exit = sorted(exit_counts.items(), key=lambda x: x[1], reverse=True)[:2]
    top_exit_text = ", ".join([f"{k}:{v}" for k, v in top_exit]) if top_exit else "none"

    mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
    stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
    target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
    dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
    dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
    strategy_text = strategy_intents.get(sid, "Configured strategy profile.")

    scope_text = "yesterday" if ("yesterday" in q and scope is not closed) else "selected period"

    diagnosis = []
    if wr >= 70 and "trailing_stop" in exit_counts:
        diagnosis.append("entries aligned with trend continuation and exits locked gains via trailing stops")
    if wr < 45:
        diagnosis.append("entry quality or regime alignment is weak in this period")
    if avg_win > 0 and abs(avg_loss) > avg_win:
        diagnosis.append("losses are larger than wins; stop/position sizing pressure remains")
    if any(k in {"stop_loss", "iv_crush_stop", "theta_burn"} for k, _ in top_exit):
        diagnosis.append("protective exits are active, likely from pullback or premium decay pressure")
    if not diagnosis:
        diagnosis.append("results are positive, but edge may be regime- and timing-dependent")

    recent = sorted(scope, key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC), reverse=True)[:3]
    trade_lines = []
    for item in recent:
        t = item["trade"]
        pnl = item["pnl"]
        dt = item["dt"]
        dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
        direction = (t.get("direction") or "N/A")
        strike = t.get("strike")
        if not isinstance(strike, (int, float)):
            strike = _parse_strike(t.get("option_symbol"))
        strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
        entry = t.get("entry_price")
        exit_px = t.get("exit_price")
        entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
        exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
        reason = t.get("exit_reason") or "unknown"
        hold = _fmt_secs(t.get("time_in_trade_seconds"))
        trade_lines.append(
            f"{dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | {reason} | hold {hold}"
        )

    lines = [
        f"Assessment: {sid} performed strongly in {scope_text} with WR {wr:.1f}% ({wins}W/{losses}L) and PnL ${total_pnl:.2f}.",
        f"Strategy: {strategy_text} (mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}).",
        f"Evidence: avg/trade ${avg:.2f}, avg win ${avg_win:.2f}, avg loss ${avg_loss:.2f}, top exits {top_exit_text}.",
    ]
    if trade_lines:
        lines.append("Recent trades:")
        lines.extend(trade_lines)
    lines.append(f"Likely Causes: {'; '.join(diagnosis)}.")
    lines.append("Next step: use `!askmore break down each trade with entry context and regime` for deeper trade-by-trade analysis.")
    return "\n".join(lines)


def resolve_sim_id(q_text: str, prev_q: str, sim_context) -> "str | None":
    """Resolve SIM ID from follow-up question, prior question, or cached context."""
    import re
    # 1) explicit SIM reference in follow-up
    m = re.search(r"\bsim\s*(\d{1,2})\b", q_text, flags=re.IGNORECASE)
    if m:
        return f"SIM{int(m.group(1)):02d}"
    # 2) first SIM id from prior question
    m = re.search(r"\bsim\s*(\d{1,2})\b", prev_q, flags=re.IGNORECASE)
    if m:
        return f"SIM{int(m.group(1)):02d}"
    # 3) first SIM id in cached sim_context text
    if isinstance(sim_context, str):
        m = re.search(r"\bSIM\d{2}\b", sim_context)
        if m:
            return m.group(0)
    return None


def build_askmore_sim_fallback(
    q_text: str,
    prev_q: str,
    sim_context,
    load_sim_profiles_fn,
) -> "str | None":
    """Build detailed follow-up answer for SIM questions in !askmore."""
    sid = resolve_sim_id(q_text, prev_q, sim_context)
    if not sid:
        return None
    profiles = load_sim_profiles_fn()
    profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
    sim_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims", f"{sid}.json")
    )
    if not os.path.exists(sim_path):
        return f"{sid}: sim state file not found."

    try:
        with open(sim_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return f"{sid}: unable to read sim state file."

    trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
    open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
    closed = []
    for t in trade_log:
        try:
            pnl = float(t.get("realized_pnl_dollars"))
        except (TypeError, ValueError):
            continue
        dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
        closed.append({"trade": t, "pnl": pnl, "dt": dt})
    if not closed:
        return f"{sid}: no closed trades available yet (open={len(open_trades)})."

    import re as _re
    q = q_text.lower()
    n = 3
    m = _re.search(r"last\s+(\d+)\s+trades", q)
    if m:
        try:
            n = max(1, min(20, int(m.group(1))))
        except Exception:
            n = 3

    scope = closed
    if "yesterday" in q:
        yday = (datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(days=1))
        scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
        if scoped:
            scope = scoped

    scope_sorted = sorted(
        scope,
        key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC),
        reverse=True,
    )
    picks = scope_sorted[:n]

    wins = sum(1 for x in scope if x["pnl"] > 0)
    total = len(scope)
    total_pnl = sum(x["pnl"] for x in scope)
    wr = (wins / total) * 100 if total else 0.0

    stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
    target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
    mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
    dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
    dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"

    lines = [
        f"Assessment: {sid} follow-up for {len(picks)} trade(s). Period WR={wr:.1f}% ({wins}/{total}) and PnL={total_pnl:+.2f}.",
        f"Strategy frame: mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}.",
        "Trade breakdown:",
    ]

    want_context = any(k in q for k in ["entry context", "regime", "context", "why"])
    for item in picks:
        t = item["trade"]
        dt = item["dt"]
        dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
        pnl = item["pnl"]
        direction = t.get("direction") or "N/A"
        strike = t.get("strike")
        if not isinstance(strike, (int, float)):
            strike = _parse_strike(t.get("option_symbol"))
        strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
        entry = t.get("entry_price")
        exit_px = t.get("exit_price")
        entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
        exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
        exit_reason = t.get("exit_reason") or "unknown"
        hold = _fmt_secs(t.get("time_in_trade_seconds"))
        lines.append(
            f"- {dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | exit={exit_reason} | hold={hold}"
        )
        if want_context:
            regime = t.get("regime_at_entry") or "N/A"
            bucket = t.get("time_of_day_bucket") or "N/A"
            ectx = t.get("entry_context") or "N/A"
            reason = _extract_reason(ectx) or "N/A"
            lines.append(f"  context: regime={regime}, bucket={bucket}, signal_reason={reason}")
            if "entry context" in q:
                lines.append(f"  entry_context: {ectx}")

    lines.append("Next step: use `!askmore compare winners vs losers for this SIM by regime/time bucket`.")
    return "\n".join(lines)
