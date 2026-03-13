"""
analytics/statistical_trailing_stop.py
Statistical trailing stop that computes stop levels from the realized
volatility distribution of recent price bars.
"""
import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


def _col(df, candidates):
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def compute_statistical_stop(
    df,
    direction: str,
    group_size: int = 10,
    distribution_length: int = 100,
    level: int = 1,
) -> dict:
    """Compute a volatility-distribution-based trailing stop.

    Parameters
    ----------
    df : DataFrame
        1-min OHLCV bars.
    direction : str
        "BULLISH" or "BEARISH".
    group_size : int
        Number of bars per volatility sample.
    distribution_length : int
        Number of volatility samples to use for the distribution.
    level : int
        0 = mu (tightest), 1 = mu+sigma, 2 = mu+2*sigma, 3 = mu+3*sigma.

    Returns
    -------
    dict with keys: stat_stop, stat_width, vol_mu, vol_sigma.
    """
    _none_result: dict = {
        "stat_stop": None,
        "stat_width": None,
        "vol_mu": None,
        "vol_sigma": None,
    }
    try:
        if df is None:
            return _none_result

        close_col = _col(df, ["close", "Close", "CLOSE"])
        if close_col is None:
            return _none_result

        closes = df[close_col].values
        n = len(closes)

        # Minimum bars: ideal is group_size * distribution_length,
        # but accept as few as group_size * 10
        min_bars = group_size * 10
        if n < min_bars + 1:
            return _none_result

        # Compute log returns
        log_returns = []
        for i in range(1, n):
            prev = float(closes[i - 1])
            curr = float(closes[i])
            if prev > 0 and curr > 0:
                log_returns.append(math.log(curr / prev))
            else:
                log_returns.append(0.0)

        if len(log_returns) < group_size:
            return _none_result

        # Segment into groups and compute per-group stdev
        num_groups = len(log_returns) // group_size
        group_vols = []
        for g in range(num_groups):
            start = g * group_size
            end = start + group_size
            group = log_returns[start:end]
            mean_g = sum(group) / len(group)
            var_g = sum((x - mean_g) ** 2 for x in group) / len(group)
            group_vols.append(math.sqrt(var_g))

        # Take last distribution_length groups (or all available)
        if len(group_vols) == 0:
            return _none_result
        dist = group_vols[-distribution_length:]

        # Compute mu and sigma of the volatility distribution
        mu = sum(dist) / len(dist)
        if len(dist) >= 2:
            sigma = math.sqrt(sum((v - mu) ** 2 for v in dist) / len(dist))
        else:
            sigma = 0.0

        # Compute stop width based on level
        level = max(0, min(3, level))
        width = mu + level * sigma

        if width <= 0:
            return _none_result

        current_close = float(closes[-1])
        if current_close <= 0:
            return _none_result

        # Compute stop price
        if direction == "BULLISH":
            stat_stop = current_close - (width * current_close)
        elif direction == "BEARISH":
            stat_stop = current_close + (width * current_close)
        else:
            return _none_result

        return {
            "stat_stop": float(stat_stop),
            "stat_width": float(width),
            "vol_mu": float(mu),
            "vol_sigma": float(sigma),
        }
    except Exception:
        logger.exception("compute_statistical_stop error")
        return _none_result
