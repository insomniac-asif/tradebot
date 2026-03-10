# interface/charting.py

import io
import logging
import os
import re as _re
import time as _time_mod
import pandas as pd
from typing import cast
import pandas_ta as ta
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed

from core.paths import DATA_DIR, CHART_DIR
from core.data_service import get_market_dataframe
from core.rate_limiter import rate_limit_sleep

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

DATA_FILE = os.path.join(DATA_DIR, "qqq_1m.csv")
from core.data_service import get_client, get_symbol_csv_path

client = get_client()

def generate_chart(symbol: str = "SPY"):
    symbol = symbol.upper()
    df = None

    # Try per-symbol CSV first (via registry), then SPY default CSV
    csv_path = get_symbol_csv_path(symbol) or (DATA_FILE if symbol == "SPY" else None)
    if csv_path and os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            df = None

    if df is None or df.empty:
        # Fallback to Alpaca via data_service (works for SPY)
        df = get_market_dataframe()
        if df is None or df.empty:
            print("No data available (CSV missing + Alpaca fallback failed)")
            return False

    if df.empty or "timestamp" not in df.columns:
        # data_service returns index-based DF; normalize
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns and "timestamp" not in df.columns:
                df.rename(columns={"index": "timestamp"}, inplace=True)
        if "timestamp" not in df.columns:
            return False

    # ---------- Timestamp Cleanup ----------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df.set_index("timestamp", inplace=True)

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    if len(df) < 2:
        return False  # not enough candles to plot

    # ---------- Indicator Safe Zone ----------
    df["ema9_r"] = None
    df["ema20_r"] = None
    df["vwap_r"] = None

    try:
        if len(df) >= 9:
            close = cast(pd.Series, df["close"])
            df["ema9_r"] = ta.ema(close, length=9)

        if len(df) >= 20:
            close = cast(pd.Series, df["close"])
            df["ema20_r"] = ta.ema(close, length=20)

        if len(df) >= 15:
            high = cast(pd.Series, df["high"])
            low = cast(pd.Series, df["low"])
            close = cast(pd.Series, df["close"])
            volume = cast(pd.Series, df["volume"])
            df["vwap_r"] = ta.vwap(high, low, close, volume)
    except Exception as e:
        print("Indicator calculation skipped:", e)

    df = df.tail(200)

    os.makedirs(CHART_DIR, exist_ok=True)
    filepath = os.path.join(CHART_DIR, f"chart_{symbol.lower()}.png")

    # Return cached file if < 30 min old
    if os.path.exists(filepath) and _time_mod.time() - os.path.getmtime(filepath) < 1800:
        return filepath

    apds = []

    ema9 = cast(pd.Series, df["ema9_r"])
    if ema9.notna().any():
        apds.append(mpf.make_addplot(ema9, color="yellow"))

    ema20 = cast(pd.Series, df["ema20_r"])
    if ema20.notna().any():
        apds.append(mpf.make_addplot(ema20, color="purple"))

    vwap = cast(pd.Series, df["vwap_r"])
    if vwap.notna().any():
        apds.append(mpf.make_addplot(vwap, color="blue"))

    try:
        mpf.plot(
            df,
            type="candle",
            style="yahoo",
            addplot=apds if apds else None,
            volume=True,
            savefig=filepath
        )
    except Exception as e:
        print("Chart plotting failed:", e)
        return False

    return filepath

def generate_live_chart(symbol: str = "SPY"):
    symbol = symbol.upper()
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # Set start and end times properly in Eastern Time zone
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)

    # If before open → no chart
    if now < market_open:
        return False

    start_utc = market_open.astimezone(pytz.UTC)
    end_utc = now.astimezone(pytz.UTC)

    if client is None:
        return False

    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(1, TimeFrameUnit("Min")),
        start=start_utc,
        end=end_utc,
        feed=DataFeed.IEX
    )

    try:
        rate_limit_sleep("alpaca_stock_bars", ALPACA_MIN_CALL_INTERVAL_SEC)
        bars = client.get_stock_bars(request)
        df = getattr(bars, "df", None)
    except Exception as e:
        print("Alpaca fetch failed:", e)
        return False

    if not isinstance(df, pd.DataFrame) or df.empty:
        return False

    # Drop symbol level if multi-index
    if isinstance(df.index, pd.MultiIndex):
        df.index = df.index.get_level_values("timestamp")

    # Convert timezone properly
    dt_index = pd.DatetimeIndex(pd.to_datetime(df.index))
    if dt_index.tz is None:
        dt_index = dt_index.tz_localize("UTC")
    dt_index = dt_index.tz_convert("US/Eastern").tz_localize(None)
    df.index = dt_index

    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    # Ensure index is ordered
    df = df.sort_index()
    if len(df) < 5:
        return False

    df = df.tail(400)

    # Calculate indicators
    try:
        df["ema9"] = df["close"].ewm(span=9).mean()
        df["ema20"] = df["close"].ewm(span=20).mean()
        if "volume" in df.columns:
            high = cast(pd.Series, df["high"])
            low = cast(pd.Series, df["low"])
            close = cast(pd.Series, df["close"])
            volume = cast(pd.Series, df["volume"])
            df["vwap"] = ta.vwap(high, low, close, volume)
    except Exception:
        pass

    # --- PLOTTING ---
    fig, (ax1, ax2) = plt.subplots(
        nrows=2,
        ncols=1,
        sharex=True,
        figsize=(14, 10),
        gridspec_kw={"height_ratios": [3, 1]},
    )

    # PRICE (top axis)
    ax1.plot(df.index, df["close"], label="Price", linewidth=2)
    ax1.plot(df.index, df["ema9"], label="EMA 9", linewidth=2)
    ax1.plot(df.index, df["ema20"], label="EMA 20", linewidth=2)
    if "vwap" in df.columns:
        ax1.plot(df.index, df["vwap"], label="VWAP", linewidth=2)

    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left")
    ax1.grid(True, axis='y', linestyle='--', alpha=0.3)

    # VOLUME (bottom axis)
    colors = ["green" if df["close"].iloc[i] >= df["open"].iloc[i] else "red"
        for i in range(len(df))]

    ax2.bar(df.index, df["volume"], color=colors, alpha=0.4, width=((1/1440) * 8))
    ax2.set_ylabel("Volume")
    ax2.grid(True, axis='y', linestyle='--', alpha=0.3)

    ax1.set_title(f"{symbol} Live Session (Alpaca)")

    # =====================
    # FINAL PLOTTING
    # =====================
    plt.xticks(df.index[::5], rotation=45)  # Show every 5th tick for readability

    # Add gridlines
    ax1.grid(True, axis='y', linestyle='--', alpha=0.3)

    # Ensure that the date formatting and session times are correct
    session_date = cast(pd.Timestamp, df.index[0]).date()

    market_open = datetime.combine(session_date, datetime.min.time()).replace(hour=9, minute=30)
    market_close = datetime.combine(session_date, datetime.min.time()).replace(hour=16, minute=0)

    ax1.set_xlim(float(mdates.date2num(market_open)), float(mdates.date2num(market_close)))

    # Format x-axis to display time properly (removes date)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Make sure the ticks appear at 30-minute intervals
    ax1.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))

    plt.tight_layout()
    os.makedirs(CHART_DIR, exist_ok=True)
    filepath = os.path.join(CHART_DIR, f"live_{symbol.lower()}.png")

    # Return cached if < 30 min old
    if os.path.exists(filepath) and _time_mod.time() - os.path.getmtime(filepath) < 1800:
        plt.close()
        return filepath

    plt.savefig(filepath)
    plt.close()
    return filepath


def generate_trade_replay(
    sim_id: str,
    trade: dict,
    output_path: str = None,
) -> str | None:
    """
    Generate a mini price chart for a closed trade.
    Fetches 1-minute underlying bars around the trade window, then delegates
    rendering to charts.trade_chart.generate_trade_chart().
    Returns the file path to the saved PNG, or None if data is unavailable.
    """
    try:
        from core.data_service import get_candle_data
        from core.paths import CHART_DIR
        from charts.trade_chart import generate_trade_chart

        entry_str = trade.get("entry_time")
        exit_str  = trade.get("exit_time")
        if not entry_str:
            return None

        # Resolve underlying symbol (e.g. "SPY260303C00689000" → "SPY")
        opt_sym = trade.get("option_symbol") or ""
        symbol  = trade.get("symbol") or ""
        if not symbol:
            m = _re.match(r'^([A-Z]{1,6})', opt_sym.upper())
            symbol = m.group(1) if m else "SPY"

        # Parse ISO-8601 timestamps to naive Eastern Time
        def _to_et_naive(ts_str):
            if not ts_str:
                return None
            try:
                dt = datetime.fromisoformat(str(ts_str))
                if dt.tzinfo is not None:
                    dt = dt.astimezone(pytz.timezone("US/Eastern")).replace(tzinfo=None)
                return dt
            except Exception:
                return None

        entry_dt = _to_et_naive(entry_str)
        exit_dt  = _to_et_naive(exit_str)
        if entry_dt is None:
            return None
        if exit_dt is None:
            exit_dt = entry_dt

        # Window: 15-min buffer each side; short trades (<30 min) get 1-hour
        # window centered on the trade midpoint
        trade_secs = max(0.0, (exit_dt - entry_dt).total_seconds())
        if trade_secs < 1800:
            mid   = entry_dt + timedelta(seconds=trade_secs / 2)
            start = mid - timedelta(minutes=30)
            end   = mid + timedelta(minutes=30)
        else:
            start = entry_dt - timedelta(minutes=15)
            end   = exit_dt  + timedelta(minutes=15)

        candle_data = get_candle_data(symbol, start, end)
        if not candle_data:
            logging.warning("generate_trade_replay: no candle data for %s %s [%s → %s]",
                            sim_id, symbol, start, end)
            return None

        if output_path is None:
            os.makedirs(CHART_DIR, exist_ok=True)
            ts_tag = entry_dt.strftime("%Y%m%d_%H%M")
            output_path = os.path.join(CHART_DIR, f"replay_{sim_id}_{ts_tag}.png")

        # Inject sim_id so chart title/caching uses the right sim
        trade_copy = dict(trade, sim_id=sim_id)
        result = generate_trade_chart(trade_copy, candle_data, output_path=output_path)
        return output_path if result else None

    except Exception:
        logging.exception("generate_trade_replay failed for %s", sim_id)
        return None
