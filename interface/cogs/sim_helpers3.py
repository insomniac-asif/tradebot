"""
interface/cogs/sim_helpers3.py
Second half of sim_helpers2 — extracted handler functions for simopen, simreset,
simhealth, siminfo commands.
"""
import os
import json
import asyncio
import logging
import pytz
import discord
from datetime import datetime

from interface.fmt import ab, lbl, A, pnl_col, balance_col
from interface.shared_state import (
    _send_embed, _append_footer, _add_field_icons, _format_ts,
    _load_sim_profiles, _safe_float, STRATEGY_INTENTS,
)
from simulation.sim_portfolio import SimPortfolio
from execution.option_executor import get_option_price
from interface.cogs.sim_helpers import (
    _format_money, _format_duration, _extract_reason, _format_feature_snapshot,
    _sim_path, _parse_page, _gate_parts, _paginate,
)


def _parse_strike_from_symbol(symbol):
    if not symbol or not isinstance(symbol, str): return None
    try: return int(symbol[-8:]) / 1000.0
    except Exception: return None


def _contract_label(symbol, direction, expiry, strike):
    cp = None
    if isinstance(direction, str):
        d = direction.lower()
        if d == "bullish": cp = "CALL"
        elif d == "bearish": cp = "PUT"
    if cp is None and isinstance(symbol, str) and len(symbol) >= 10:
        try:
            c = symbol[9]
            if c == "C": cp = "CALL"
            elif c == "P": cp = "PUT"
        except Exception: cp = None
    et = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
    import re as _re_und
    _und_m = _re_und.match(r'^([A-Z]{1,6})', symbol or "")
    label = _und_m.group(1) if _und_m else ""
    if cp: label = f"{label} {cp}"
    if et: label = f"{label} {et}"
    if strike is None: strike = _parse_strike_from_symbol(symbol)
    if isinstance(strike, (int, float)): label = f"{label} {strike:g}"
    return label


async def handle_simopen(ctx, bot, sim_id, page):
    try:
        profiles = _load_sim_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        trades = []
        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            sim_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        else:
            sk = sim_id.strip().upper()
            if sk not in profiles: await _send_embed(ctx, "Unknown sim ID."); return
            sim_keys = [sk]
        for sk in sim_keys:
            try:
                sp = _sim_path(sk)
                if not os.path.exists(sp): continue
                sim = SimPortfolio(sk, profiles.get(sk, {})); await asyncio.to_thread(sim.load)
                ot = sim.open_trades if isinstance(sim.open_trades, list) else []
                for t in ot:
                    tc = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                    tc["sim_id"] = sk; trades.append(tc)
            except Exception: continue
        if not trades: await _send_embed(ctx, "No open sim trades."); return
        def _pts(val):
            if val is None: return None
            if isinstance(val, datetime): return val
            try: return datetime.fromisoformat(str(val))
            except Exception: return None
        trades.sort(key=lambda t: _pts(t.get("entry_time")) or datetime.min, reverse=True)
        per_page = 5; total = len(trades); total_pages = (total + per_page - 1) // per_page
        page_num = _parse_page(page, total_pages)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simopen {sim_id or 'all'} 1` to `!simopen {sim_id or 'all'} {total_pages}`."); return
        async def _build(pn):
            pn = max(1, min(pn, total_pages))
            start = (pn - 1) * per_page; end = start + per_page
            pt = trades[start:end]
            embed = discord.Embed(title=f"\U0001f4cc Open Sim Trades (Page {pn}/{total_pages})", color=0x3498DB)
            now_et = datetime.now(pytz.timezone("US/Eastern"))
            for idx, t in enumerate(pt, start=start + 1):
                tid = str(t.get("trade_id", "N/A")); sl = t.get("sim_id") or "SIM"
                prof = profile_map.get(sl); gates = _gate_parts(prof) if isinstance(prof, dict) else []
                gl = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                direction = str(t.get("direction") or t.get("type") or "unknown").upper()
                osym = t.get("option_symbol"); expiry = t.get("expiry")
                strike = _safe_float(t.get("strike")); qty = t.get("qty") or t.get("quantity")
                ep = _safe_float(t.get("entry_price"))
                en = _safe_float(t.get("entry_notional"))
                if en is None and ep is not None and qty is not None:
                    try: en = float(ep) * float(qty) * 100
                    except Exception: en = None
                et = _format_ts(t.get("entry_time", "N/A"))
                hs = None
                try:
                    dt = _pts(t.get("entry_time"))
                    if dt is not None:
                        eastern = pytz.timezone("America/New_York")
                        dt = eastern.localize(dt) if dt.tzinfo is None else dt.astimezone(eastern)
                        hs = (now_et - dt).total_seconds()
                except Exception: hs = None
                cp = None
                if osym:
                    try: cp = await asyncio.to_thread(get_option_price, osym)
                    except Exception: cp = None
                pv = None
                if cp is not None and ep is not None and qty is not None:
                    try: pv = (float(cp) - float(ep)) * float(qty) * 100
                    except Exception: pv = None
                ept = f"${ep:.4f}" if ep is not None else "N/A"
                nt = f"${float(cp):.4f}" if cp is not None else "N/A"
                ct = _format_money(en) if en is not None else "N/A"
                ht = _format_duration(hs); cl = _contract_label(osym, direction, expiry, strike)
                rt = _extract_reason(t.get("entry_context")); fst = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe_pct")); mae = _safe_float(t.get("mae_pct"))
                fn = f"\U0001f7e1 {sl} #{idx} {direction} | {tid[:8]}"
                ls = [
                    f"{lbl('Contract')} {A(cl, 'magenta', bold=True)}",
                    f"{lbl('Qty')} {A(str(qty), 'white')}  {lbl('Entry')} {A(ept, 'white')}  {lbl('Cost')} {A(ct, 'white')}",
                    f"{lbl('Now')} {A(nt, 'white')}  {lbl('PnL')} {pnl_col(pv) if pv is not None else A('N/A','gray')}",
                    f"{lbl('Hold')} {A(ht, 'cyan')}  {lbl('Entry Time')} {A(et, 'gray')}",
                ]
                if gl: ls.append(gl)
                if rt: ls.append(f"{lbl('Signal reason')} {A(rt, 'yellow')}")
                if fst: ls.append(f"{lbl('Feature')} {A(fst, 'white')}")
                if mfe is not None or mae is not None:
                    ls.append(f"{lbl('MFE')} {A(f'{mfe:.2%}' if mfe is not None else 'N/A', 'green')}  {lbl('MAE')} {A(f'{mae:.2%}' if mae is not None else 'N/A', 'red')}")
                embed.add_field(name=fn, value=ab(*ls), inline=False)
            _append_footer(embed, extra=f"Page {pn}/{total_pages}")
            return embed
        await _paginate(ctx, bot, total_pages, _build, page_num)
    except Exception:
        logging.exception("simopen_error")
        await _send_embed(ctx, "simopen failed due to an internal error.")


async def handle_simreset(ctx, sim_id):
    if sim_id is None:
        await _send_embed(ctx, "Usage: `!simreset SIM03`, `!simreset all`, or `!simreset live`"); return
    try:
        profiles = _load_sim_profiles()
        sim_key = sim_id.strip().upper()
        def _reset_one(sk, pr):
            try:
                sp = _sim_path(sk)
                if os.path.exists(sp):
                    try: os.remove(sp)
                    except Exception: logging.exception("simreset_remove_failed")
                sim = SimPortfolio(sk, pr); sim.load(); sim.save()
                return True, "reset"
            except Exception:
                logging.exception("simreset_one_failed"); return False, "error"
        if sim_key == "ALL":
            target_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        elif sim_key == "LIVE":
            target_keys = sorted([k for k, p in profiles.items() if p.get("execution_mode") == "live"])
        else:
            if profiles.get(sim_key) is None: await _send_embed(ctx, "Unknown sim ID."); return
            target_keys = [sim_key]
        if not target_keys: await _send_embed(ctx, "No sims matched your reset request."); return
        results = [(k, *_reset_one(k, profiles.get(k, {}))) for k in target_keys]
        title = f"\u2705 Sim Reset \u2014 {sim_key}" if sim_key in {"ALL", "LIVE"} else f"\u2705 Sim Reset \u2014 {target_keys[0]}"
        ok_keys   = [k for k, ok, _ in results if ok]
        fail_keys = [k for k, ok, _ in results if not ok]
        embed = discord.Embed(title=title, color=0x2ECC71 if not fail_keys else 0xE74C3C)
        if len(results) == 1:
            # Single-sim reset: one descriptive field
            k, ok, _ = results[0]
            sb = profiles.get(k, {}).get("balance_start", 0.0)
            embed.add_field(
                name=_add_field_icons(k),
                value=f"{'Reset to starting balance.' if ok else 'Reset failed.'} Start: ${float(sb):,.2f}",
                inline=False,
            )
        else:
            # Multi-sim reset: compact summary to stay under Discord's 25-field limit
            sb_sample = profiles.get(target_keys[0], {}).get("balance_start", 0.0)
            embed.add_field(
                name=_add_field_icons("Summary"),
                value=f"Reset {len(ok_keys)}/{len(results)} sims to ${float(sb_sample):,.2f} starting balance.",
                inline=False,
            )
            if ok_keys:
                # Chunk into groups of ~15 to avoid value length limits
                chunk_size = 15
                for i in range(0, len(ok_keys), chunk_size):
                    chunk = ok_keys[i:i+chunk_size]
                    embed.add_field(name="\u2705 Reset OK", value="  ".join(chunk), inline=False)
            if fail_keys:
                embed.add_field(name="\u274c Failed", value="  ".join(fail_keys), inline=False)
        _append_footer(embed); await ctx.send(embed=embed)
    except Exception:
        logging.exception("simreset_error")
        await _send_embed(ctx, "simreset failed due to an internal error.")


async def handle_simhealth(ctx, bot, page):
    required_keys = ["signal_mode","dte_min","dte_max","balance_start","risk_per_trade_pct","daily_loss_limit_pct","max_open_trades","exposure_cap_pct","max_spread_pct","cutoff_time_et"]
    try:
        profiles = _load_sim_profiles()
        if not profiles: await _send_embed(ctx, "No sim profiles found."); return
        sim_items = [(k, v) for k, v in profiles.items() if k.startswith("SIM")]
        validator_summary = None; validator_details = []
        try:
            from simulation.sim_validator import collect_sim_validation
            errors, total_errors = collect_sim_validation()
            validator_summary = "OK" if total_errors == 0 else f"FAIL ({total_errors} issues)"
            if total_errors > 0: validator_details = errors[:3]
        except Exception: validator_summary = "FAIL (validator error)"
        def _build(pn):
            pt = max(1, (len(sim_items) + 2) // 3)
            pi = max(1, min(pn, pt)) - 1; s = pi * 3; e = s + 3
            embed = discord.Embed(title=f"\U0001f9ea Sim Health Check \u2014 Page {pi+1}/{pt}", color=0x3498DB)
            if validator_summary:
                vl = [A(validator_summary, "green" if validator_summary.startswith("OK") else "red", bold=True)]
                for line in validator_details:
                    sev = "red" if any(tok in line for tok in ("missing:", "cutoff_format_invalid", "orb_requires_features")) else "yellow"
                    vl.append(A(line, sev))
                embed.add_field(name="SIM Validator", value=ab(*vl), inline=False)
            embed.add_field(name="SIM Profiles Loaded", value=ab(A(str(len(sim_items)), "white", bold=True)), inline=True)
            for sid, profile in sim_items[s:e]:
                try:
                    sp = _sim_path(sid); fe = os.path.exists(sp)
                    fs = "\u2705" if fe else "\u274c"
                    mk = [k for k in required_keys if k not in profile]
                    mt = ", ".join(mk) if mk else "None"
                    if not fe:
                        embed.add_field(name=sid, value=ab(f"{lbl('File')} {A(fs, 'red', bold=True)}", f"{A('Not initialized', 'yellow')}", f"{lbl('Missing keys')} {A(mt, 'cyan')}"), inline=False); continue
                    try:
                        with open(sp, "r", encoding="utf-8") as f: data = json.load(f)
                    except Exception:
                        embed.add_field(name=sid, value=ab(f"{lbl('File')} {A(fs, 'green', bold=True)}", f"{lbl('Schema')} {A('\u26a0\ufe0f', 'yellow', bold=True)}", f"{lbl('Missing keys')} {A(mt, 'cyan')}"), inline=False); continue
                    so = "\u2705" if data.get("schema_version") is not None else "\u26a0\ufe0f"
                    ot = data.get("open_trades"); tl = data.get("trade_log")
                    oc = len(ot) if isinstance(ot, list) else 0; tc = len(tl) if isinstance(tl, list) else 0
                    bv = _safe_float(data.get("balance")); pb = _safe_float(data.get("peak_balance"))
                    dlv = _safe_float(data.get("daily_loss"))
                    dp = None
                    try:
                        today = datetime.now(pytz.timezone("US/Eastern")).date()
                        total = 0.0; sb = _safe_float(profile.get("balance_start")) or 0.0; rb = sb; cpk = sb
                        for t in (tl if isinstance(tl, list) else []):
                            pv = _safe_float(t.get("realized_pnl_dollars")) or 0.0; rb += pv
                            if rb > cpk: cpk = rb
                            xt = t.get("exit_time")
                            if not xt: continue
                            dt = datetime.fromisoformat(str(xt))
                            if dt.tzinfo is None: dt = pytz.timezone("US/Eastern").localize(dt)
                            if dt.date() == today: total += pv
                        dp = total
                        pb = max(pb, cpk) if pb is not None else cpk
                    except Exception: dp = None
                    lr = "N/A"
                    try:
                        ctx_val = None
                        if isinstance(tl, list):
                            for t in reversed(tl):
                                if isinstance(t, dict) and t.get("entry_context"): ctx_val = t.get("entry_context"); break
                        if ctx_val is None and isinstance(ot, list):
                            for t in reversed(ot):
                                if isinstance(t, dict) and t.get("entry_context"): ctx_val = t.get("entry_context"); break
                        if ctx_val and "reason=" in ctx_val: lr = ctx_val.split("reason=")[-1].split("|")[0].strip()
                    except Exception: lr = "N/A"
                    gates = _gate_parts(profile)
                    gt = "  |  ".join(gates) if gates else None
                    vlines = [
                        f"{lbl('File')} {A(fs, 'green', bold=True)}",
                        f"{lbl('Schema')} {A(so, 'green' if so == '\u2705' else 'yellow', bold=True)}",
                        f"{lbl('Open trades')} {A(str(oc), 'white')}",
                        f"{lbl('Trade log')} {A(str(tc), 'white')}",
                        f"{lbl('Balance')} {balance_col(bv)}",
                        f"{lbl('Peak balance')} {balance_col(pb)}",
                        f"{lbl('Daily PnL')} {pnl_col(dp) if dp is not None else A('N/A','gray')}",
                        f"{lbl('Daily loss')} {pnl_col(-abs(dlv)) if dlv is not None else A('N/A','gray')}",
                        f"{lbl('Missing keys')} {A(mt, 'cyan')}",
                        f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                        f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                    ]
                    if gt: vlines.append(f"{lbl('gates')} {A(gt, 'white')}")
                    vlines.append(f"{lbl('last_reason')} {A(lr, 'yellow')}")
                    embed.add_field(name=sid, value=ab(*vlines), inline=False)
                except Exception:
                    embed.add_field(name=sid, value="Error reading sim data", inline=False)
            embed.set_footer(text=f"Checked: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}")
            _append_footer(embed)
            return embed
        pages_count = max(1, (len(sim_items) + 2) // 3)
        pn = _parse_page(page, pages_count) if page is not None else 1
        await _paginate(ctx, bot, pages_count, _build, pn)
    except Exception:
        logging.exception("simhealth_error")
        await _send_embed(ctx, "simhealth failed due to an internal error.")


async def handle_siminfo(ctx, sim_id):
    def _norm(raw):
        if raw is None: return None
        text = str(raw).strip().upper()
        if not text: return None
        if text.startswith("SIM"):
            suffix = text.replace("SIM", "").strip()
            return f"SIM{int(suffix):02d}" if suffix.isdigit() else text
        return f"SIM{int(text):02d}" if text.isdigit() else None
    def _fmt_secs(seconds):
        try: s = int(seconds)
        except (TypeError, ValueError): return "N/A"
        if s < 60: return f"{s}s"
        if s < 3600: return f"{s // 60}m"
        if s < 86400: return f"{s / 3600:.1f}h"
        return f"{s / 86400:.1f}d"
    def _dte_tier(dmin, dmax):
        try: dmin, dmax = int(dmin), int(dmax)
        except (TypeError, ValueError): return "DTE: N/A"
        if dmin == 0 and dmax == 0: return "0DTE only"
        if dmin == 0 and dmax == 1: return "0\u20131 DTE intraday"
        if dmin == 1 and dmax == 1: return "1 DTE intraday"
        if dmin == 1 and dmax == 3: return "1\u20133 DTE intraday"
        if dmin >= 7 and dmax <= 10: return "7\u201310 DTE swing"
        if dmin >= 14 and dmax <= 21: return "14\u201321 DTE swing"
        return f"DTE range {dmin}\u2013{dmax}"
    try:
        profiles = _load_sim_profiles()
        if not profiles: await _send_embed(ctx, "No sim profiles found."); return
        sid = _norm(sim_id)
        if sid is None: await _send_embed(ctx, "Usage: `!siminfo 0-11` or `!siminfo SIM03`."); return
        profile = profiles.get(sid)
        if not profile: await _send_embed(ctx, f"{sid} not found in sim_config.yaml."); return
        name = profile.get("name", sid); mode = str(profile.get("signal_mode", "N/A"))
        horizon = str(profile.get("horizon", "N/A"))
        dmin = profile.get("dte_min"); dmax = profile.get("dte_max")
        hmin = _fmt_secs(profile.get("hold_min_seconds")); hmax = _fmt_secs(profile.get("hold_max_seconds"))
        cutoff = profile.get("cutoff_time_et", "N/A"); fe = profile.get("features_enabled", False)
        em = profile.get("execution_mode", "sim").upper()
        def _pct_text(key):
            try: return f"{float(profile.get(key, 0)) * 100:.2f}%"
            except (TypeError, ValueError): return "N/A"
        rpt = _pct_text("risk_per_trade_pct"); dlpt = _pct_text("daily_loss_limit_pct")
        try: ept = f"{float(profile.get('exposure_cap_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError): ept = "N/A"
        try: spt = f"{float(profile.get('stop_loss_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError): spt = "N/A"
        try: tpt = f"{float(profile.get('profit_target_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError): tpt = "N/A"
        try: slip = f"in {profile.get('entry_slippage', 'N/A')} / out {profile.get('exit_slippage', 'N/A')}"
        except Exception: slip = "N/A"
        gates = []
        if profile.get("regime_filter"): gates.append(f"{lbl('regime')} {A(str(profile.get('regime_filter')), 'yellow')}")
        for k in ("orb_minutes", "vol_z_min", "atr_expansion_min"):
            if profile.get(k) is not None: gates.append(f"{lbl(k)} {A(str(profile.get(k)), 'white')}")
        gt = "  |  ".join(gates) if gates else "None"
        embed = discord.Embed(title=f"\U0001f9e0 {sid} \u2014 {name}", color=0x3498DB)
        embed.add_field(name="Strategy Intent", value=ab(A(STRATEGY_INTENTS.get(sid, "Configured strategy profile."), "white")), inline=False)
        embed.add_field(name="Profile", value=ab(f"{lbl('signal_mode')} {A(mode, 'magenta', bold=True)}", f"{lbl('horizon')} {A(horizon, 'cyan')}", f"{lbl('execution')} {A(em, 'yellow')}", f"{lbl('features_enabled')} {A(str(fe), 'cyan')}", f"{lbl('dte_tier')} {A(_dte_tier(dmin, dmax), 'white')}"), inline=False)
        embed.add_field(name="Timing", value=ab(f"{lbl('DTE range')} {A(f'{dmin}\u2013{dmax}', 'white')}", f"{lbl('hold_min')} {A(hmin, 'white')}  |  {lbl('hold_max')} {A(hmax, 'white')}", f"{lbl('cutoff')} {A(str(cutoff), 'white')}"), inline=False)
        embed.add_field(name="Risk / Exposure", value=ab(f"{lbl('risk/trade')} {A(rpt, 'white')}", f"{lbl('daily_loss')} {A(dlpt, 'white')}", f"{lbl('max_open')} {A(str(profile.get('max_open_trades', 'N/A')), 'white')}", f"{lbl('exposure_cap')} {A(ept, 'white')}"), inline=False)
        embed.add_field(name="Stops / Targets", value=ab(f"{lbl('stop_loss')} {A(spt, 'red')}", f"{lbl('profit_target')} {A(tpt, 'green')}", f"{lbl('trail_activate')} {A(str(profile.get('trailing_stop_activate_pct', 'N/A')), 'white')}", f"{lbl('trail_pct')} {A(str(profile.get('trailing_stop_trail_pct', 'N/A')), 'white')}"), inline=False)
        embed.add_field(name="Entry / Selection", value=ab(f"{lbl('otm_pct')} {A(str(profile.get('otm_pct', 'N/A')), 'white')}", f"{lbl('max_spread')} {A(str(profile.get('max_spread_pct', 'N/A')), 'white')}", f"{lbl('slippage')} {A(slip, 'white')}", f"{lbl('gates')} {A(gt, 'white')}"), inline=False)
        embed.set_footer(text=f"Loaded: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}")
        _append_footer(embed); await ctx.send(embed=embed)
    except Exception:
        logging.exception("siminfo_error")
        await _send_embed(ctx, "siminfo failed due to an internal error.")
