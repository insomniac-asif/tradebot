"""
Trade Chart Generator — shared between dashboard and Discord !ask command.
Generates annotated OHLC candlestick charts for closed trades.

Usage:
    from charts.trade_chart import generate_trade_chart
    png_bytes = generate_trade_chart(trade, candle_data)
    # or save to file:
    path = generate_trade_chart(trade, candle_data, output_path="trade.png")
"""

import io
import re
import logging
from datetime import timedelta
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec

from charts.trade_chart_helpers import (
    CACHE_DIR,
    THEME,
    _cache_path,
    _load_cached_chart,
    _parse_ts,
    _load_candles,
    _fmt_pnl,
    _compute_ema,
    _compute_vwap,
    _compute_rsi,
    _resample_candles,
    _compute_sma,
    _render_placeholder,
    _render_error,
    _draw_rsi_panel,
    _draw_volume_panel,
    _format_x_axis,
)

# ---------------------------------------------------------------------------
_log = logging.getLogger("trade_chart")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_trade_chart(
    trade: dict,
    candle_data: list[dict],
    narrative: dict | None = None,
    output_path: str | None = None,
    size: tuple = (1200, 700),
    theme: str = "dark",
    force_refresh: bool = False,
) -> bytes | str:
    """
    Generate an annotated trade chart.

    Returns PNG bytes if output_path is None, else saves to path and returns path string.
    Caches output to data/trade_charts/ if sim_id and trade_id are in trade dict.
    """
    sim_id   = trade.get("sim_id", "UNKNOWN")
    trade_id = trade.get("trade_id", "")

    # Check cache (respects 30-min expiry)
    if trade_id and not force_refresh and output_path is None:
        data = _load_cached_chart(_cache_path(sim_id, trade_id))
        if data is not None:
            return data

    try:
        png = _render(trade, candle_data, narrative, size)
    except Exception as e:
        _log.error("Chart render failed for %s/%s: %s", sim_id, trade_id, e)
        png = _render_error(str(e), size)

    # Cache
    if trade_id and output_path is None:
        try:
            _cache_path(sim_id, trade_id).write_bytes(png)
        except Exception:
            pass

    if output_path:
        try:
            Path(output_path).write_bytes(png)
            return output_path
        except Exception as e:
            _log.error("Failed to save chart to %s: %s", output_path, e)
            return output_path

    return png


def _render(trade: dict, candle_data: list[dict], narrative: dict | None, size: tuple) -> bytes:
    T = THEME
    w, h = size[0] / 100, size[1] / 100  # inches at 100 dpi

    # Parse candles
    times, opens, highs, lows, closes, vols = _load_candles(candle_data)
    times, opens, highs, lows, closes, vols = _resample_candles(
        times, opens, highs, lows, closes, vols, max_bars=300
    )

    # Bug 1 fix: insufficient candle data guard
    if len(times) < 5:
        underlying_fb = trade.get("symbol") or ""
        if not underlying_fb:
            _m = re.match(r'^([A-Z]{1,6})', (trade.get("option_symbol", "") or "").upper())
            underlying_fb = _m.group(1) if _m else "?"
        return _render_placeholder(
            f"Insufficient candle data — {len(times)} candles loaded for {underlying_fb}",
            size
        )

    _log.debug("Chart render: %d candles for %s [%s → %s]", len(times),
               trade.get("symbol", "?"),
               times[0] if times else 'N/A',
               times[-1] if times else 'N/A')

    has_volume = any(v > 0 for v in vols)

    # Parse trade times
    entry_dt = _parse_ts(trade.get("entry_time"))
    exit_dt  = _parse_ts(trade.get("exit_time"))

    # Option prices (for annotation text only — NOT used for chart Y positions)
    entry_opt_p = float(trade.get("entry_price") or 0)
    exit_opt_p  = float(trade.get("exit_price")  or 0)

    # Find underlying price from candle data at entry/exit time
    def _candle_price_at(dt):
        if dt is None or not times:
            return None
        idx = min(range(len(times)), key=lambda i: abs((times[i] - dt).total_seconds()))
        if abs((times[idx] - dt).total_seconds()) < 7200:
            return closes[idx]
        return None

    pnl_d     = float(trade.get("realized_pnl_dollars") or 0)
    is_win    = pnl_d >= 0
    direction = (trade.get("direction") or "BULLISH").upper()
    signal_mode = trade.get("signal_mode") or "—"
    sim_id    = trade.get("sim_id", "")
    opt_sym   = trade.get("option_symbol", "")
    underlying = trade.get("symbol") or ""
    if not underlying:
        _m = re.match(r'^([A-Z]{1,6})', opt_sym.upper())
        underlying = _m.group(1) if _m else ""

    # Underlying prices from candle data at entry/exit (used for marker Y positions)
    entry_p = _candle_price_at(entry_dt) or closes[-1]
    exit_p  = _candle_price_at(exit_dt)  or closes[-1]

    # Support/resistance from narrative
    support_p = resistance_p = None
    if narrative and isinstance(narrative.get("key_levels"), dict):
        kl = narrative["key_levels"]
        support_p    = kl.get("support")
        resistance_p = kl.get("resistance")
        if support_p:
            try: support_p = float(support_p)
            except: support_p = None
        if resistance_p:
            try: resistance_p = float(resistance_p)
            except: resistance_p = None

    # Bug 2 fix: dynamic bar width
    if len(times) >= 2:
        span_days = (times[-1] - times[0]).total_seconds() / 86400
        bar_width_days = span_days / len(times) * 0.75
    else:
        bar_width_days = 0.0005

    # ---------------------------------------------------------------------------
    # 3-Panel Layout via GridSpec
    # ---------------------------------------------------------------------------
    if has_volume:
        height_ratios = [6, 1.5, 2]
        n_rows = 3
    else:
        height_ratios = [7.5, 2.5]
        n_rows = 2

    fig = plt.figure(figsize=(w, h), dpi=100)
    fig.patch.set_facecolor(T["bg"])

    gs = gridspec.GridSpec(n_rows, 1, figure=fig,
                           height_ratios=height_ratios, hspace=0.04)

    ax     = fig.add_subplot(gs[0])
    ax_rsi = fig.add_subplot(gs[1], sharex=ax)
    ax_vol = fig.add_subplot(gs[2], sharex=ax) if has_volume else None

    # Apply common styling to all axes
    def _style_ax(a):
        a.set_facecolor(T["bg2"])
        a.tick_params(colors=T["text_dim"], labelsize=7)
        for spine in a.spines.values():
            spine.set_edgecolor(T["grid"])
        a.grid(True, color=T["grid"], linewidth=0.4, alpha=0.6)

    _style_ax(ax)
    _style_ax(ax_rsi)
    if ax_vol:
        _style_ax(ax_vol)

    # Hide x-tick labels on main and RSI panels
    plt.setp(ax.xaxis.get_majorticklabels(), visible=False)
    plt.setp(ax_rsi.xaxis.get_majorticklabels(), visible=False)

    x_min = times[0]
    x_max = times[-1]

    # ---------------------------------------------------------------------------
    # Compute indicators
    # ---------------------------------------------------------------------------
    vwap_vals = _compute_vwap(highs, lows, closes, vols) if has_volume else _compute_vwap(highs, lows, closes, [1.0]*len(closes))
    ema9_vals  = _compute_ema(closes, 9)
    ema21_vals = _compute_ema(closes, 21)
    sma50_vals = _compute_sma(closes, 50) if len(closes) >= 50 else None
    rsi_vals   = _compute_rsi(closes, 14)

    # ---------------------------------------------------------------------------
    # Main panel — draw in z-order
    # ---------------------------------------------------------------------------

    # 1. Trade region shade
    try:
        if entry_dt and exit_dt:
            shade_col = T["exit_win"] if is_win else T["exit_loss"]
            ax.axvspan(entry_dt, exit_dt, alpha=0.07, color=shade_col, zorder=1)
    except Exception:
        pass

    # 2. Candlesticks (wicks + Rectangle bodies)
    for i, ts in enumerate(times):
        try:
            o, h_, l, c = opens[i], highs[i], lows[i], closes[i]
            bull = c >= o
            body_col = T["candle_up"] if bull else T["candle_dn"]
            wick_col = T["wick_up"]   if bull else T["wick_dn"]
            # Wick
            ax.plot([ts, ts], [l, h_], color=wick_col, linewidth=0.7, zorder=2)
            # Body
            bh = abs(c - o) if abs(c - o) > 0.001 else 0.01
            rect = mpatches.Rectangle(
                (mdates.date2num(ts) - bar_width_days / 2, min(o, c)),
                bar_width_days, bh,
                color=body_col, zorder=3,
            )
            ax.add_patch(rect)
        except Exception:
            pass

    # 3. VWAP
    try:
        ax.plot(times, vwap_vals, color="#f0c040", linewidth=1.2, alpha=0.85,
                zorder=4, label="VWAP")
    except Exception:
        pass

    # 4. EMA 9
    try:
        ax.plot(times, ema9_vals, color="#00bcd4", linewidth=0.9, alpha=0.75,
                zorder=4, label="E9")
    except Exception:
        pass

    # 5. EMA 21
    try:
        ax.plot(times, ema21_vals, color="#ff9800", linewidth=0.9, alpha=0.75,
                zorder=4, label="E21")
    except Exception:
        pass

    # 6. SMA 50
    try:
        if sma50_vals is not None and len(times) >= 50:
            sma50_times = [times[i] for i in range(len(times)) if sma50_vals[i] is not None]
            sma50_clean = [v for v in sma50_vals if v is not None]
            if sma50_times:
                ax.plot(sma50_times, sma50_clean, color="#ffffff", linestyle="--",
                        linewidth=0.7, alpha=0.5, zorder=4, label="SMA50")
    except Exception:
        pass

    # Indicator legend (top-left, compact)
    try:
        ax.legend(loc="upper left", fontsize=6, framealpha=0.5,
                  facecolor=T["bg"], edgecolor=T["grid"],
                  labelcolor=T["text_dim"], handlelength=1.2, borderpad=0.4,
                  ncol=4)
    except Exception:
        pass

    # 7. Stop loss — skip (option price; not meaningful on underlying chart)

    # 8. Take profit — skip (option price; not meaningful on underlying chart)

    # 9. Support / Resistance
    try:
        if support_p:
            ax.axhline(support_p, color=T["sr_col"], linestyle=":", linewidth=0.9,
                       alpha=0.6, zorder=4)
            ax.text(mdates.date2num(x_min), support_p, f"S ${support_p:.2f} ",
                    color=T["sr_col"], fontsize=7, va="bottom", ha="left",
                    transform=ax.transData, style="italic")
    except Exception:
        pass
    try:
        if resistance_p:
            ax.axhline(resistance_p, color=T["sr_col"], linestyle=":", linewidth=0.9,
                       alpha=0.6, zorder=4)
            ax.text(mdates.date2num(x_min), resistance_p, f"R ${resistance_p:.2f} ",
                    color=T["sr_col"], fontsize=7, va="bottom", ha="left",
                    transform=ax.transData, style="italic")
    except Exception:
        pass

    # 10. MAE / 11. MFE — omitted: stored as option pct, not translatable to underlying price

    # 12. Entry marker (placed at underlying price from candles)
    try:
        if entry_dt and entry_p:
            entry_marker = "^" if direction == "BULLISH" else "v"
            entry_label = (narrative or {}).get("entry_label") or "ENTRY"
            opt_str = f"opt ${entry_opt_p:.2f}" if entry_opt_p else ""
            ax.scatter([entry_dt], [entry_p], marker=entry_marker, color=T["entry_col"],
                       s=150, zorder=6)
            y_offset = 12 if direction == "BULLISH" else -24
            ax.annotate(f" {entry_label}\n {opt_str}", xy=(entry_dt, entry_p),
                        xytext=(8, y_offset), textcoords="offset points",
                        color=T["entry_col"], fontsize=7.5, fontweight="bold",
                        arrowprops=dict(arrowstyle="-", color=T["entry_col"], lw=0.8))
    except Exception:
        pass

    # 13. Exit marker (placed at underlying price from candles)
    try:
        if exit_dt and exit_p:
            exit_col    = T["exit_win"] if is_win else T["exit_loss"]
            exit_marker = "v" if direction == "BULLISH" else "^"
            exit_label  = ((narrative or {}).get("exit_label")
                           or trade.get("exit_reason")
                           or "EXIT")
            opt_str = f"opt ${exit_opt_p:.2f}" if exit_opt_p else ""
            ax.scatter([exit_dt], [exit_p], marker=exit_marker, color=exit_col,
                       s=150, zorder=6)
            y_offset = -24 if direction == "BULLISH" else 12
            ax.annotate(f" {exit_label}\n {opt_str}", xy=(exit_dt, exit_p),
                        xytext=(8, y_offset), textcoords="offset points",
                        color=exit_col, fontsize=7.5, fontweight="bold",
                        arrowprops=dict(arrowstyle="-", color=exit_col, lw=0.8))
    except Exception:
        pass

    # 14. Connecting line entry → exit (on underlying price)
    try:
        if entry_dt and exit_dt and entry_p and exit_p:
            line_col = T["exit_win"] if is_win else T["exit_loss"]
            ax.plot([entry_dt, exit_dt], [entry_p, exit_p],
                    linestyle="--", color=line_col, linewidth=0.8, alpha=0.5, zorder=5)
    except Exception:
        pass

    # 15. PnL box (top-right)
    try:
        pnl_str   = _fmt_pnl(trade)
        grade_str = (narrative or {}).get("grade", "")
        pnl_bg    = T["pnl_win_bg"] if is_win else T["pnl_los_bg"]
        pnl_fc    = T["exit_win"]   if is_win else T["exit_loss"]
        pnl_label = pnl_str + (f"  [{grade_str}]" if grade_str and grade_str != "N/A" else "")
        ax.text(0.99, 0.97, pnl_label, transform=ax.transAxes, fontsize=9, fontweight="bold",
                color=pnl_fc, ha="right", va="top",
                bbox=dict(facecolor=pnl_bg, edgecolor=pnl_fc,
                          boxstyle="round,pad=0.3", alpha=0.9))
    except Exception:
        pass

    # 16. Signal mode label (top-left)
    try:
        ax.text(0.01, 0.97, signal_mode, transform=ax.transAxes, fontsize=8,
                color=T["text_dim"], ha="left", va="top",
                bbox=dict(facecolor=T["bg"], edgecolor=T["grid"],
                          boxstyle="round,pad=0.2", alpha=0.85))
    except Exception:
        pass

    # 17. Title + subtitle
    try:
        exit_reason = trade.get("exit_reason", "")
        date_str = entry_dt.strftime("%b %d") if entry_dt else ""
        title = f"{sim_id} | {underlying} | {opt_sym} | {signal_mode} | {date_str}"
        ax.set_title(title, color=T["text"], fontsize=9, pad=6, loc="center")
        strategy_summary = (narrative or {}).get("strategy_summary")
        if strategy_summary:
            ax.text(0.5, 1.01, strategy_summary, transform=ax.transAxes,
                    color=T["text_dim"], fontsize=7, ha="center", va="bottom",
                    style="italic")
    except Exception:
        pass

    # ---------------------------------------------------------------------------
    # RSI, Volume, and X-axis panels (delegated to helpers)
    # ---------------------------------------------------------------------------
    _draw_rsi_panel(ax_rsi, times, rsi_vals, entry_dt, exit_dt, is_win)
    _draw_volume_panel(ax_vol, times, opens, closes, vols, bar_width_days)
    bottom_ax = ax_vol if ax_vol else ax_rsi
    _format_x_axis(bottom_ax, times)

    # X limits
    try:
        if times:
            x_pad = timedelta(minutes=1)
            ax.set_xlim(times[0] - x_pad, times[-1] + x_pad)
    except Exception:
        pass

    # Y limits — zoom to actual price range with a small padding
    try:
        price_min = min(lows)
        price_max = max(highs)
        pad = (price_max - price_min) * 0.15 or price_min * 0.005
        ax.set_ylim(price_min - pad, price_max + pad)
    except Exception:
        pass

    plt.tight_layout(pad=0.6)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight",
                facecolor=T["bg"], edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
