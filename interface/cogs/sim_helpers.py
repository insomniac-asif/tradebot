"""
interface/cogs/sim_helpers.py
Module-level helper functions for sim_commands.py.
Extracted to reduce file size; no logic changes.
"""
import os
import asyncio

from interface.fmt import ab, lbl, A
from interface.shared_state import _safe_float

_DATA_SIMS = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "sims"))


def _format_money(val):
    try: return f"${float(val):,.2f}"
    except (TypeError, ValueError): return "N/A"

def _format_signed_money(val):
    try:
        num = float(val)
        return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
    except (TypeError, ValueError): return "N/A"

def _format_drawdown(val):
    try: num = float(val)
    except (TypeError, ValueError): return "N/A"
    return "$0.00" if num <= 0 else f"-${abs(num):,.2f}"

def _format_pct(val):
    try: return f"{float(val) * 100:.1f}%"
    except (TypeError, ValueError): return "N/A"

def _pnl_badge(val):
    try: num = float(val)
    except (TypeError, ValueError): return "\u26aa"
    return "\u2705" if num > 0 else ("\u274c" if num < 0 else "\u26aa")

def _format_duration(seconds):
    try: total = int(seconds)
    except (TypeError, ValueError): return "N/A"
    if total < 0: return "N/A"
    h, r = divmod(total, 3600); m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h > 0 else f"{m}m {s}s"

def _extract_reason(ctx_str):
    if not ctx_str or not isinstance(ctx_str, str) or "reason=" not in ctx_str: return None
    try: return ctx_str.rsplit("reason=", 1)[-1].split("|")[0].strip()
    except Exception: return None

def _format_feature_snapshot(fs):
    if not isinstance(fs, dict) or not fs: return None
    def _f(key, fmt="{:.3f}"):
        val = fs.get(key)
        if val is None: return None
        try: return fmt.format(float(val))
        except Exception: return str(val)
    parts = []
    orb_h, orb_l = _f("orb_high", "{:.2f}"), _f("orb_low", "{:.2f}")
    if orb_h and orb_l: parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
    for k, l, c in [("vol_z","Vol Z","yellow"),("atr_expansion","ATR Exp","magenta"),("vwap_z","VWAP Z","cyan"),("close_z","Close Z","cyan"),("iv_rank_proxy","IV Rank","white")]:
        v = _f(k)
        if v: parts.append(f"{lbl(l)} {A(v, c)}")
    return "  |  ".join(parts) if parts else None

def _sim_path(sim_key):
    return os.path.join(_DATA_SIMS, f"{sim_key}.json")

def _compute_breakdown(trade_log, key, order=None):
    stats = {}
    for t in trade_log:
        bucket = t.get(key) if isinstance(t, dict) else None
        bucket = bucket if bucket not in (None, "") else "UNKNOWN"
        stats.setdefault(bucket, {"wins": 0, "total": 0})
        pnl_val = _safe_float(t.get("realized_pnl_dollars"))
        stats[bucket]["total"] += 1
        if pnl_val is not None and pnl_val > 0: stats[bucket]["wins"] += 1
    lines = []
    for k in (order or sorted(stats.keys())):
        if k not in stats: continue
        total = stats[k]["total"]; wins = stats[k]["wins"]
        wr = wins / total if total > 0 else 0
        lines.append(f"{k}: {wins}/{total} ({wr * 100:.1f}%)")
    return "\n".join(lines) if lines else "N/A"

def _ansi_breakdown(text):
    if not text or text == "N/A": return ab(A("N/A", "gray"))
    return ab(*[A(line, "cyan") for line in text.splitlines()])

def _parse_page(page, total_pages):
    page_num = 1
    if isinstance(page, str):
        pt = page.strip().lower()
        if pt.startswith("page"): pt = pt.replace("page", "").strip()
        if pt.isdigit(): page_num = int(pt)
    elif isinstance(page, int):
        page_num = int(page)
    return max(1, min(page_num, total_pages))

def _gate_parts(profile):
    gates = []
    for k in ("orb_minutes", "vol_z_min", "atr_expansion_min"):
        if profile.get(k) is not None:
            gates.append(f"{lbl(k)} {A(str(profile.get(k)), 'white')}")
    return gates

def _add_lines_field(embed, title, lines):
    """Add embed fields for a list of lines, splitting into chunks to stay within field limits."""
    if not lines:
        embed.add_field(name=title, value=ab(A("No data", "gray")), inline=False)
        return
    chunks, cur, cur_len = [], [], 0
    for line in lines:
        est = len(line) + 1
        if cur and cur_len + est > 900:
            chunks.append(cur)
            cur = [line]
            cur_len = len(line)
        else:
            cur.append(line)
            cur_len += est
    if cur:
        chunks.append(cur)
    for idx, chunk in enumerate(chunks):
        embed.add_field(name=title if idx == 0 else f"{title} (cont.)", value=ab(*chunk), inline=False)


async def _paginate(ctx, bot, total_pages, build_fn, start_page=1):
    page_num = max(1, min(start_page, total_pages))
    embed = build_fn(page_num) if not asyncio.iscoroutinefunction(build_fn) else await build_fn(page_num)
    message = await ctx.send(embed=embed)
    if total_pages <= 1: return
    try:
        for emoji in ("\u25c0\ufe0f", "\u25b6\ufe0f", "\u23f9\ufe0f"):
            await message.add_reaction(emoji)
    except Exception: return
    def _check(reaction, user):
        return user == ctx.author and reaction.message.id == message.id and str(reaction.emoji) in {"\u25c0\ufe0f", "\u25b6\ufe0f", "\u23f9\ufe0f"}
    while True:
        try: reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
        except asyncio.TimeoutError:
            try: await message.clear_reactions()
            except Exception: pass
            break
        emoji = str(reaction.emoji)
        if emoji == "\u23f9\ufe0f":
            try: await message.clear_reactions()
            except Exception: pass
            break
        if emoji == "\u25c0\ufe0f": page_num = total_pages if page_num == 1 else page_num - 1
        elif emoji == "\u25b6\ufe0f": page_num = 1 if page_num == total_pages else page_num + 1
        try:
            e = build_fn(page_num) if not asyncio.iscoroutinefunction(build_fn) else await build_fn(page_num)
            await message.edit(embed=e)
        except Exception: pass
        try: await message.remove_reaction(reaction.emoji, user)
        except Exception: pass
