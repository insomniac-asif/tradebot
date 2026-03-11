"""
analytics/cross_asset_context.py
Cross-asset context: index divergence, VXX fear gauge,
megacap correlations, multi-timeframe momentum.

Takes a dict of DataFrames keyed by symbol.
No state, no mutation, no side effects.
"""
import logging
import math
from typing import Optional


def _safe_return(series, lookback: int) -> Optional[float]:
    """Compute return over last `lookback` bars. Returns None on failure."""
    try:
        if series is None or len(series) < lookback + 1:
            return None
        old = float(series.iloc[-(lookback + 1)])
        new = float(series.iloc[-1])
        if old == 0:
            return None
        return (new - old) / old
    except Exception:
        return None


def _get_close(df):
    """Extract close series from a df, trying multiple column names."""
    if df is None:
        return None
    for c in ("close", "Close"):
        if c in df.columns:
            return df[c]
    return None


def compute_index_divergence(dfs: dict, lookback: int = 30) -> dict:
    """Compare SPY vs QQQ vs IWM momentum."""
    try:
        spy_close = _get_close(dfs.get("SPY"))
        qqq_close = _get_close(dfs.get("QQQ"))
        iwm_close = _get_close(dfs.get("IWM"))

        spy_ret = _safe_return(spy_close, lookback)
        qqq_ret = _safe_return(qqq_close, lookback)
        iwm_ret = _safe_return(iwm_close, lookback)

        result = {
            "spy_return_30": spy_ret,
            "qqq_return_30": qqq_ret,
            "iwm_return_30": iwm_ret,
        }

        if spy_ret is not None and qqq_ret is not None:
            result["spy_qqq_divergence"] = round(spy_ret - qqq_ret, 6)
        else:
            result["spy_qqq_divergence"] = None

        if spy_ret is not None and iwm_ret is not None:
            result["spy_iwm_divergence"] = round(spy_ret - iwm_ret, 6)
        else:
            result["spy_iwm_divergence"] = None

        # Breadth signal
        rets = [r for r in (spy_ret, qqq_ret, iwm_ret) if r is not None]
        if len(rets) == 3:
            signs = [1 if r > 0 else -1 for r in rets]
            if signs[0] == signs[1] == signs[2]:
                result["breadth_signal"] = "aligned"
            elif len(set(signs)) == 2:
                result["breadth_signal"] = "mixed"
            else:
                result["breadth_signal"] = "divergent"
        else:
            result["breadth_signal"] = None

        # Tech leading / risk_on
        if qqq_ret is not None and spy_ret is not None and iwm_ret is not None:
            result["tech_leading"] = qqq_ret > spy_ret > iwm_ret
            result["risk_on"] = iwm_ret > spy_ret
        else:
            result["tech_leading"] = None
            result["risk_on"] = None

        return result
    except Exception as exc:
        logging.debug("compute_index_divergence error: %s", exc)
        return {}


def compute_vxx_context(df_vxx, lookback_short: int = 10, lookback_long: int = 30) -> dict:
    """VXX-based fear/volatility context."""
    try:
        close = _get_close(df_vxx)
        if close is None or len(close) < lookback_long + 1:
            return {}

        vxx_ret_short = _safe_return(close, lookback_short)
        vxx_ret_long = _safe_return(close, lookback_long)

        if vxx_ret_short is None or vxx_ret_long is None:
            return {}

        # Acceleration: is short-term move outsized vs average rate?
        avg_rate_short = vxx_ret_long * lookback_short / lookback_long
        vxx_accel = vxx_ret_short - avg_rate_short

        # Linear regression slope over last lookback_short bars
        vals = [float(close.iloc[-(lookback_short - i)]) for i in range(lookback_short)]
        n = len(vals)
        sum_x = n * (n - 1) / 2
        sum_x2 = n * (n - 1) * (2 * n - 1) / 6
        sum_y = sum(vals)
        sum_xy = sum(i * vals[i] for i in range(n))
        denom = n * sum_x2 - sum_x * sum_x
        slope = (n * sum_xy - sum_x * sum_y) / denom if denom != 0 else 0.0

        # VXX elevated vs 20-bar SMA
        sma20 = float(close.iloc[-20:].mean()) if len(close) >= 20 else float(close.mean())
        vxx_current = float(close.iloc[-1])

        return {
            "vxx_return_short": round(vxx_ret_short, 6),
            "vxx_return_long": round(vxx_ret_long, 6),
            "vxx_acceleration": round(vxx_accel, 6),
            "fear_rising": vxx_ret_short > 0.005,
            "fear_fading": vxx_ret_short < -0.005,
            "vxx_slope": round(slope, 6),
            "vxx_elevated": vxx_current > sma20,
        }
    except Exception as exc:
        logging.debug("compute_vxx_context error: %s", exc)
        return {}


def compute_megacap_correlation(dfs: dict, lookback: int = 30) -> dict:
    """Check if mega-caps move in lockstep or diverge from SPY."""
    try:
        spy_close = _get_close(dfs.get("SPY"))
        spy_ret = _safe_return(spy_close, lookback)

        megacaps = ["AAPL", "NVDA", "MSFT", "TSLA"]
        returns = {}
        for sym in megacaps:
            c = _get_close(dfs.get(sym))
            ret = _safe_return(c, lookback)
            if ret is not None:
                returns[sym] = ret

        if not returns:
            return {}

        # Alignment with SPY
        aligned = 0
        if spy_ret is not None:
            spy_sign = 1 if spy_ret >= 0 else -1
            for sym, ret in returns.items():
                if (1 if ret >= 0 else -1) == spy_sign:
                    aligned += 1

        total = len(returns)
        concentration = aligned / total if total > 0 else 0.0

        strongest = max(returns, key=returns.get)
        weakest = min(returns, key=returns.get)
        spread = returns[strongest] - returns[weakest]

        return {
            "aligned_count": aligned,
            "sector_concentration": round(concentration, 4),
            "strongest_megacap": strongest,
            "weakest_megacap": weakest,
            "megacap_spread": round(spread, 6),
            "megacap_spread_pct": round(abs(spread) * 100, 4),
        }
    except Exception as exc:
        logging.debug("compute_megacap_correlation error: %s", exc)
        return {}


def compute_intraday_momentum(df_spy, windows: Optional[list] = None) -> dict:
    """Multi-timeframe momentum for SPY."""
    try:
        if windows is None:
            windows = [5, 10, 30, 60]

        close = _get_close(df_spy)
        if close is None:
            return {}

        result = {}
        positive_count = 0
        valid_count = 0

        for w in windows:
            ret = _safe_return(close, w)
            result[f"return_{w}"] = round(ret, 6) if ret is not None else None
            if ret is not None:
                result[f"momentum_positive_{w}"] = ret > 0
                valid_count += 1
                if ret > 0:
                    positive_count += 1

        result["momentum_alignment"] = round(positive_count / valid_count, 4) if valid_count > 0 else None

        # Accelerating: short-term move outsized vs medium-term
        r5 = result.get("return_5")
        r10 = result.get("return_10")
        if r5 is not None and r10 is not None:
            result["momentum_accelerating"] = abs(r5) > abs(r10) * 0.5
        else:
            result["momentum_accelerating"] = None

        return result
    except Exception as exc:
        logging.debug("compute_intraday_momentum error: %s", exc)
        return {}


def compute_all_cross_asset(dfs: dict) -> dict:
    """Aggregate all cross-asset computations into one flat dict."""
    try:
        if not dfs or not isinstance(dfs, dict):
            return {}

        result = {}

        # Index divergence (needs SPY + at least one other index)
        try:
            data = compute_index_divergence(dfs)
            if isinstance(data, dict):
                result.update(data)
        except Exception:
            pass

        # VXX context
        try:
            vxx_df = dfs.get("VXX")
            if vxx_df is not None:
                data = compute_vxx_context(vxx_df)
                if isinstance(data, dict):
                    result.update(data)
        except Exception:
            pass

        # Megacap correlation
        try:
            data = compute_megacap_correlation(dfs)
            if isinstance(data, dict):
                result.update(data)
        except Exception:
            pass

        # Intraday momentum (SPY)
        try:
            spy_df = dfs.get("SPY")
            if spy_df is not None:
                data = compute_intraday_momentum(spy_df)
                if isinstance(data, dict):
                    result.update(data)
        except Exception:
            pass

        return result
    except Exception as exc:
        logging.debug("compute_all_cross_asset error: %s", exc)
        return {}
