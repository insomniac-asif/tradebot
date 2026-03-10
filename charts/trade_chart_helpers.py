"""
Pure helper functions for trade_chart.py.
Extracted to keep trade_chart.py concise.
"""

import io
import re
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ---------------------------------------------------------------------------
BASE_DIR      = Path(__file__).resolve().parent.parent
CACHE_DIR     = BASE_DIR / "data" / "trade_charts"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_MAX_AGE = 1800  # 30 minutes

_log = logging.getLogger("trade_chart")

# THEME is imported from trade_chart by callers that need it;
# _render_placeholder/_render_error also need it, so define it here too.
THEME = {
    "bg":         "#0d1117",
    "bg2":        "#161b22",
    "grid":       "#21262d",
    "text":       "#c9d1d9",
    "text_dim":   "#8b949e",
    "candle_up":  "#3fb950",
    "candle_dn":  "#f85149",
    "wick_up":    "#3fb950",
    "wick_dn":    "#f85149",
    "volume_up":  "#1a4a1a",
    "volume_dn":  "#4a1a1a",
    "entry_col":  "#58a6ff",
    "exit_win":   "#3fb950",
    "exit_loss":  "#f85149",
    "sl_col":     "#f85149",
    "tp_col":     "#3fb950",
    "sr_col":     "#79c0ff",
    "shade":      "#58a6ff",
    "pnl_win_bg": "#1a3a1a",
    "pnl_los_bg": "#3a1a1a",
}

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_path(sim_id: str, trade_id: str) -> Path:
    safe = re.sub(r"[^\w\-]", "_", trade_id)
    return CACHE_DIR / f"{sim_id}_{safe}.png"


def _load_cached_chart(cache_p: Path) -> bytes | None:
    """Load cached PNG if it exists and is < CACHE_MAX_AGE seconds old."""
    if not cache_p.exists():
        return None
    age = time.time() - cache_p.stat().st_mtime
    if age > CACHE_MAX_AGE:
        try:
            cache_p.unlink()
        except Exception:
            pass
        return None
    return cache_p.read_bytes()


# ---------------------------------------------------------------------------
# Timezone / timestamp helpers
# ---------------------------------------------------------------------------

_ET_TZ = None


def _get_et():
    global _ET_TZ
    if _ET_TZ is None:
        import pytz
        _ET_TZ = pytz.timezone("US/Eastern")
    return _ET_TZ


def _parse_ts(ts_str: str | None) -> datetime | None:
    """Parse timestamp to naive Eastern Time datetime."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_str))
        if dt.tzinfo is not None:
            dt = dt.astimezone(_get_et()).replace(tzinfo=None)
        return dt
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Candle loading
# ---------------------------------------------------------------------------

def _load_candles(candle_data: list[dict]) -> tuple[list, list, list, list, list, list]:
    """Parse candle_data into parallel lists: times, opens, highs, lows, closes, volumes."""
    times, opens, highs, lows, closes, vols = [], [], [], [], [], []
    for c in candle_data:
        t_raw = c.get("t") or c.get("timestamp") or c.get("time")
        ts = _parse_ts(str(t_raw)) if t_raw else None
        if ts is None:
            continue
        try:
            times.append(ts)
            opens.append(float(c.get("o") or c.get("open") or 0))
            highs.append(float(c.get("h") or c.get("high") or 0))
            lows.append(float(c.get("l") or c.get("low") or 0))
            closes.append(float(c.get("c") or c.get("close") or 0))
            vols.append(float(c.get("v") or c.get("volume") or 0))
        except Exception:
            pass
    return times, opens, highs, lows, closes, vols


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%H:%M")


def _fmt_pnl(trade: dict) -> str:
    pnl_d = trade.get("realized_pnl_dollars") or 0
    pnl_p = (trade.get("realized_pnl_pct") or 0) * 100
    sign  = "+" if pnl_d >= 0 else ""
    return f"{sign}${pnl_d:.2f} ({sign}{pnl_p:.1f}%)"


# ---------------------------------------------------------------------------
# Indicator helpers
# ---------------------------------------------------------------------------

def _compute_ema(values, span):
    if not values:
        return []
    alpha = 2.0 / (span + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result


def _compute_vwap(highs, lows, closes, volumes):
    """Session VWAP (cumulative)."""
    result = []
    cum_pv = 0.0
    cum_v = 0.0
    for h, l, c, v in zip(highs, lows, closes, volumes):
        tp = (h + l + c) / 3
        cum_pv += tp * v
        cum_v += v
        result.append(cum_pv / cum_v if cum_v > 0 else tp)
    return result


def _compute_rsi(closes, period=14):
    if len(closes) < period + 2:
        return [50.0] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rsi_vals = [50.0] * (period + 1)
    rs = avg_g / avg_l if avg_l > 0 else 100.0
    rsi_vals.append(100 - 100 / (1 + rs))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 0 else 100.0
        rsi_vals.append(100 - 100 / (1 + rs))
    return rsi_vals


def _resample_candles(times, opens, highs, lows, closes, vols, max_bars=300):
    """Downsample 1-min candles to at most max_bars by aggregating into larger buckets."""
    n = len(times)
    if n <= max_bars:
        return times, opens, highs, lows, closes, vols

    total_mins = (times[-1] - times[0]).total_seconds() / 60
    bucket_mins = 1
    for bm in [5, 15, 30, 60, 120, 240]:
        if total_mins / bm <= max_bars:
            bucket_mins = bm
            break
    else:
        bucket_mins = 240

    t0 = times[0]
    bucket_map: dict[int, dict] = {}
    for i in range(n):
        idx = int((times[i] - t0).total_seconds() // (bucket_mins * 60))
        if idx not in bucket_map:
            bucket_map[idx] = {
                "t": t0 + timedelta(minutes=idx * bucket_mins),
                "o": opens[i], "h": highs[i], "l": lows[i], "c": closes[i], "v": vols[i],
            }
        else:
            b = bucket_map[idx]
            b["h"] = max(b["h"], highs[i])
            b["l"] = min(b["l"], lows[i])
            b["c"] = closes[i]
            b["v"] += vols[i]

    keys = sorted(bucket_map)
    return (
        [bucket_map[k]["t"] for k in keys],
        [bucket_map[k]["o"] for k in keys],
        [bucket_map[k]["h"] for k in keys],
        [bucket_map[k]["l"] for k in keys],
        [bucket_map[k]["c"] for k in keys],
        [bucket_map[k]["v"] for k in keys],
    )


def _compute_sma(values, period):
    result = []
    for i in range(len(values)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(sum(values[i - period + 1:i + 1]) / period)
    return result


# ---------------------------------------------------------------------------
# Placeholder / Error renderers
# ---------------------------------------------------------------------------

def _render_placeholder(msg: str, size: tuple) -> bytes:
    fig, ax = plt.subplots(figsize=(size[0]/100, size[1]/100), dpi=100)
    fig.patch.set_facecolor(THEME["bg"])
    ax.set_facecolor(THEME["bg"])
    ax.text(0.5, 0.5, msg, transform=ax.transAxes,
            color=THEME["text_dim"], ha="center", va="center", fontsize=10,
            wrap=True, fontfamily="monospace")
    ax.axis("off")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, facecolor=THEME["bg"])
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _render_error(msg: str, size: tuple) -> bytes:
    """Return a simple error PNG."""
    return _render_placeholder(f"Chart error:\n{msg}", size)


# ---------------------------------------------------------------------------
# Panel-drawing helpers (extracted from _render in trade_chart.py)
# ---------------------------------------------------------------------------
import matplotlib.dates as mdates
import matplotlib.ticker as mticker


def _draw_rsi_panel(ax_rsi, times, rsi_vals, entry_dt, exit_dt, is_win):
    """Draw the RSI sub-panel onto ax_rsi."""
    T = THEME
    try:
        ax_rsi.plot(times, rsi_vals[:len(times)], color="#ffffff", linewidth=1.0, zorder=3)
        ax_rsi.axhline(70, color="#f85149", linestyle="--", linewidth=0.7, alpha=0.6)
        ax_rsi.axhline(30, color="#3fb950", linestyle="--", linewidth=0.7, alpha=0.6)

        rsi_arr = np.array(rsi_vals[:len(times)])
        times_arr = np.array(times)
        ax_rsi.fill_between(times_arr, 70, rsi_arr, where=(rsi_arr > 70),
                            color="#f85149", alpha=0.15, interpolate=True)
        ax_rsi.fill_between(times_arr, rsi_arr, 30, where=(rsi_arr < 30),
                            color="#3fb950", alpha=0.15, interpolate=True)

        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_yticks([30, 50, 70])

        if entry_dt and times:
            entry_idx = min(range(len(times)),
                            key=lambda i: abs((times[i] - entry_dt).total_seconds()))
            entry_rsi = rsi_vals[entry_idx] if entry_idx < len(rsi_vals) else 50.0
            ax_rsi.scatter([times[entry_idx]], [entry_rsi], color=T["entry_col"],
                           s=30, zorder=5)
            ax_rsi.text(mdates.date2num(times[entry_idx]), entry_rsi,
                        f" RSI {entry_rsi:.0f}",
                        color=T["entry_col"], fontsize=6, va="center",
                        ha="left", transform=ax_rsi.transData)

        if exit_dt and times:
            exit_idx = min(range(len(times)),
                           key=lambda i: abs((times[i] - exit_dt).total_seconds()))
            exit_rsi = rsi_vals[exit_idx] if exit_idx < len(rsi_vals) else 50.0
            exit_col = T["exit_win"] if is_win else T["exit_loss"]
            ax_rsi.scatter([times[exit_idx]], [exit_rsi], color=exit_col, s=30, zorder=5)

        ax_rsi.text(0.01, 0.90, "RSI", transform=ax_rsi.transAxes,
                    color=T["text_dim"], fontsize=7, va="top", ha="left")
    except Exception:
        pass


def _draw_volume_panel(ax_vol, times, opens, closes, vols, bar_width_days):
    """Draw the volume sub-panel onto ax_vol."""
    T = THEME
    if ax_vol is None or not times:
        return
    try:
        avg_vol = (np.mean([v for v in vols if v > 0])
                   if any(v > 0 for v in vols) else 1)
        vol_formatter = mticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K")
        ax_vol.yaxis.set_major_formatter(vol_formatter)

        for i, ts in enumerate(times):
            try:
                is_up = closes[i] >= opens[i]
                bright = vols[i] > 2 * avg_vol
                if is_up:
                    v_col = "#2ea043" if bright else T["volume_up"]
                else:
                    v_col = "#da3633" if bright else T["volume_dn"]
                ax_vol.bar(ts, vols[i], width=timedelta(days=bar_width_days),
                           color=v_col, edgecolor="none", zorder=2)
            except Exception:
                pass

        ax_vol.axhline(avg_vol, color="#555555", linestyle="--",
                       linewidth=0.7, alpha=0.7, zorder=3)
        ax_vol.text(0.01, 0.90, "VOL", transform=ax_vol.transAxes,
                    color=T["text_dim"], fontsize=7, va="top", ha="left")
    except Exception:
        pass


def _format_x_axis(bottom_ax, times):
    """Apply adaptive x-axis date formatting to the bottom panel."""
    T = THEME
    try:
        if times:
            span_mins = (times[-1] - times[0]).total_seconds() / 60
            if span_mins <= 240:
                bottom_ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 10)))
                bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=0)
            elif span_mins <= 480:
                bottom_ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
                bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=0)
            elif span_mins <= 1440:
                bottom_ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
                bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
                plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
            elif span_mins <= 4320:
                bottom_ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
                bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
                plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
            else:
                bottom_ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                bottom_ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
                plt.setp(bottom_ax.xaxis.get_majorticklabels(), rotation=0)
        bottom_ax.tick_params(axis="x", colors=T["text_dim"], labelsize=7)
    except Exception:
        pass
