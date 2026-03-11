"""
analytics/market_structure.py
Pure functions for computing market structure levels:
swing highs/lows, floor pivots, VWAP bands, volume profile,
round-number proximity, and previous-day levels.

No state, no mutation, no side effects. Every public function
returns a dict and is wrapped in try/except.
"""
import logging
import math
from typing import Optional


def _col(df, *candidates):
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _get_col(df, *candidates):
    """Return the Series for the first matching column, or None."""
    name = _col(df, *candidates)
    return df[name] if name is not None else None


def compute_swing_levels(df, lookback: int = 50, prominence: int = 3) -> dict:
    """Identify swing highs and swing lows using fractal pivots."""
    try:
        high_s = _get_col(df, "high", "High")
        low_s = _get_col(df, "low", "Low")
        close_s = _get_col(df, "close", "Close")
        if high_s is None or low_s is None or close_s is None:
            return {}
        if len(df) < lookback:
            lookback = len(df)
        if lookback < 2 * prominence + 1:
            return {}

        highs = high_s.values
        lows = low_s.values
        n = len(highs)
        current_close = float(close_s.iloc[-1])

        # Scan window: last `lookback` bars, excluding most recent `prominence`
        start_idx = max(0, n - lookback)
        end_idx = n - prominence  # can't confirm more recent bars

        swing_highs = []
        swing_lows = []

        for i in range(start_idx + prominence, end_idx):
            # Swing high check
            is_high = True
            for j in range(1, prominence + 1):
                if highs[i] <= highs[i - j] or highs[i] <= highs[i + j]:
                    is_high = False
                    break
            if is_high:
                # Count touches: how many bars after i came within 0.1% of this level
                price = float(highs[i])
                touches = 0
                threshold = price * 0.001
                for k in range(i + 1, n):
                    if abs(float(highs[k]) - price) <= threshold or abs(float(lows[k]) - price) <= threshold:
                        touches += 1
                swing_highs.append({
                    "price": price,
                    "type": "swing_high",
                    "bars_ago": n - 1 - i,
                    "touches": touches,
                })

            # Swing low check
            is_low = True
            for j in range(1, prominence + 1):
                if lows[i] >= lows[i - j] or lows[i] >= lows[i + j]:
                    is_low = False
                    break
            if is_low:
                price = float(lows[i])
                touches = 0
                threshold = price * 0.001
                for k in range(i + 1, n):
                    if abs(float(highs[k]) - price) <= threshold or abs(float(lows[k]) - price) <= threshold:
                        touches += 1
                swing_lows.append({
                    "price": price,
                    "type": "swing_low",
                    "bars_ago": n - 1 - i,
                    "touches": touches,
                })

        # Sort by recency (lowest bars_ago first), take top 5
        swing_highs.sort(key=lambda x: x["bars_ago"])
        swing_lows.sort(key=lambda x: x["bars_ago"])
        swing_highs = swing_highs[:5]
        swing_lows = swing_lows[:5]

        # Nearest resistance = closest swing_high above current close
        above = [s for s in swing_highs if s["price"] > current_close]
        nearest_resistance = min(above, key=lambda x: x["price"])["price"] if above else None

        # Nearest support = closest swing_low below current close
        below = [s for s in swing_lows if s["price"] < current_close]
        nearest_support = max(below, key=lambda x: x["price"])["price"] if below else None

        dist_res = (nearest_resistance - current_close) / current_close if nearest_resistance and current_close else None
        dist_sup = (current_close - nearest_support) / current_close if nearest_support and current_close else None

        return {
            "swing_highs": swing_highs,
            "swing_lows": swing_lows,
            "nearest_resistance": nearest_resistance,
            "nearest_support": nearest_support,
            "distance_to_resistance_pct": dist_res,
            "distance_to_support_pct": dist_sup,
        }
    except Exception as exc:
        logging.debug("compute_swing_levels error: %s", exc)
        return {}


def compute_pivot_levels(df) -> dict:
    """Classic floor trader pivot points from previous session."""
    try:
        close_s = _get_col(df, "close", "Close")
        high_s = _get_col(df, "high", "High")
        low_s = _get_col(df, "low", "Low")
        if close_s is None or high_s is None or low_s is None:
            return {}

        # Group by date
        idx = df.index
        if hasattr(idx, "date"):
            dates = idx.date
        else:
            # Try to parse from a timestamp column
            ts_col = _col(df, "timestamp", "Timestamp", "time")
            if ts_col is None:
                return {}
            import pandas as pd
            dates = pd.to_datetime(df[ts_col]).dt.date.values

        unique_dates = sorted(set(dates))
        if len(unique_dates) < 2:
            return {}

        prev_date = unique_dates[-2]
        mask = [d == prev_date for d in dates]

        prev_high = float(high_s[mask].max())
        prev_low = float(low_s[mask].min())
        prev_close = float(close_s[mask].iloc[-1])
        current_close = float(close_s.iloc[-1])

        pivot = (prev_high + prev_low + prev_close) / 3
        r1 = 2 * pivot - prev_low
        s1 = 2 * pivot - prev_high
        r2 = pivot + (prev_high - prev_low)
        s2 = pivot - (prev_high - prev_low)
        r3 = prev_high + 2 * (pivot - prev_low)
        s3 = prev_low - 2 * (prev_high - pivot)

        # Determine pivot zone
        if current_close > r3:
            zone = "above_R3"
        elif current_close > r2:
            zone = "R2_R3"
        elif current_close > r1:
            zone = "R1_R2"
        elif current_close > pivot:
            zone = "Pivot_R1"
        elif current_close > s1:
            zone = "S1_Pivot"
        elif current_close > s2:
            zone = "S2_S1"
        elif current_close > s3:
            zone = "S3_S2"
        else:
            zone = "below_S3"

        dist_to_pivot = (current_close - pivot) / pivot if pivot else 0.0

        return {
            "pivot": round(pivot, 4),
            "r1": round(r1, 4),
            "r2": round(r2, 4),
            "r3": round(r3, 4),
            "s1": round(s1, 4),
            "s2": round(s2, 4),
            "s3": round(s3, 4),
            "pivot_zone": zone,
            "distance_to_pivot_pct": round(dist_to_pivot, 6),
        }
    except Exception as exc:
        logging.debug("compute_pivot_levels error: %s", exc)
        return {}


def compute_vwap_bands(df) -> dict:
    """VWAP with standard deviation bands for current intraday session."""
    try:
        close_s = _get_col(df, "close", "Close")
        high_s = _get_col(df, "high", "High")
        low_s = _get_col(df, "low", "Low")
        vol_s = _get_col(df, "volume", "Volume")
        if close_s is None or vol_s is None:
            return {}

        # Filter to today's bars
        idx = df.index
        if hasattr(idx, "date"):
            today = idx[-1].date()
            mask = idx.date == today
        else:
            ts_col = _col(df, "timestamp", "Timestamp", "time")
            if ts_col is None:
                mask = [True] * len(df)
            else:
                import pandas as pd
                dts = pd.to_datetime(df[ts_col])
                today = dts.iloc[-1].date()
                mask = dts.dt.date == today

        c = close_s[mask].values
        v = vol_s[mask].values

        if high_s is not None and low_s is not None:
            h = high_s[mask].values
            l = low_s[mask].values
            tp = (h + l + c) / 3
        else:
            tp = c

        if len(tp) < 2 or sum(v) == 0:
            return {}

        # Cumulative VWAP
        cum_tp_vol = 0.0
        cum_tp2_vol = 0.0
        cum_vol = 0.0
        for i in range(len(tp)):
            cum_tp_vol += float(tp[i]) * float(v[i])
            cum_tp2_vol += float(tp[i]) ** 2 * float(v[i])
            cum_vol += float(v[i])

        if cum_vol == 0:
            return {}

        vwap = cum_tp_vol / cum_vol
        variance = cum_tp2_vol / cum_vol - vwap ** 2
        std = math.sqrt(max(0, variance))

        upper_1 = vwap + std
        lower_1 = vwap - std
        upper_2 = vwap + 2 * std
        lower_2 = vwap - 2 * std

        current_close = float(c[-1])

        if current_close > upper_2:
            position = "above_2sd"
        elif current_close > upper_1:
            position = "1sd_to_2sd_above"
        elif current_close > vwap:
            position = "within_1sd_above"
        elif current_close > lower_1:
            position = "within_1sd_below"
        elif current_close > lower_2:
            position = "1sd_to_2sd_below"
        else:
            position = "below_2sd"

        dist = (current_close - vwap) / vwap if vwap else 0.0

        return {
            "vwap": round(vwap, 4),
            "upper_1sd": round(upper_1, 4),
            "lower_1sd": round(lower_1, 4),
            "upper_2sd": round(upper_2, 4),
            "lower_2sd": round(lower_2, 4),
            "vwap_position": position,
            "distance_to_vwap_pct": round(dist, 6),
        }
    except Exception as exc:
        logging.debug("compute_vwap_bands error: %s", exc)
        return {}


def compute_volume_profile(df, num_bins: int = 20) -> dict:
    """Approximate volume profile (price-at-volume) from intraday bars."""
    try:
        close_s = _get_col(df, "close", "Close")
        high_s = _get_col(df, "high", "High")
        low_s = _get_col(df, "low", "Low")
        vol_s = _get_col(df, "volume", "Volume")
        if close_s is None or high_s is None or low_s is None or vol_s is None:
            return {}

        # Filter to today
        idx = df.index
        if hasattr(idx, "date"):
            today = idx[-1].date()
            mask = idx.date == today
        else:
            ts_col = _col(df, "timestamp", "Timestamp", "time")
            if ts_col is None:
                mask = [True] * len(df)
            else:
                import pandas as pd
                dts = pd.to_datetime(df[ts_col])
                today = dts.iloc[-1].date()
                mask = dts.dt.date == today

        h = high_s[mask].values
        l = low_s[mask].values
        v = vol_s[mask].values
        c = close_s[mask].values

        if len(h) < 2:
            return {}

        day_low = float(min(l))
        day_high = float(max(h))
        if day_high <= day_low:
            return {}

        bin_size = (day_high - day_low) / num_bins
        bins = [0.0] * num_bins

        # Distribute each bar's volume across overlapping bins
        for i in range(len(h)):
            bar_lo = float(l[i])
            bar_hi = float(h[i])
            bar_vol = float(v[i])
            if bar_hi <= bar_lo or bar_vol <= 0:
                continue
            for b in range(num_bins):
                bin_lo = day_low + b * bin_size
                bin_hi = bin_lo + bin_size
                overlap = max(0, min(bar_hi, bin_hi) - max(bar_lo, bin_lo))
                bar_range = bar_hi - bar_lo
                if bar_range > 0 and overlap > 0:
                    bins[b] += bar_vol * (overlap / bar_range)

        total_vol = sum(bins)
        if total_vol == 0:
            return {}

        # POC = bin with highest volume
        poc_bin = max(range(num_bins), key=lambda b: bins[b])
        poc = day_low + (poc_bin + 0.5) * bin_size

        # Value Area: contiguous range of bins containing 70% of volume
        # Start from POC bin, expand outward
        va_bins = {poc_bin}
        va_vol = bins[poc_bin]
        lo_ptr = poc_bin - 1
        hi_ptr = poc_bin + 1
        target = total_vol * 0.70

        while va_vol < target and (lo_ptr >= 0 or hi_ptr < num_bins):
            lo_vol = bins[lo_ptr] if lo_ptr >= 0 else -1
            hi_vol = bins[hi_ptr] if hi_ptr < num_bins else -1
            if lo_vol >= hi_vol and lo_ptr >= 0:
                va_bins.add(lo_ptr)
                va_vol += lo_vol
                lo_ptr -= 1
            elif hi_ptr < num_bins:
                va_bins.add(hi_ptr)
                va_vol += hi_vol
                hi_ptr += 1
            else:
                break

        va_low = day_low + min(va_bins) * bin_size
        va_high = day_low + (max(va_bins) + 1) * bin_size

        current_close = float(c[-1])
        if current_close > va_high:
            position = "above"
        elif current_close < va_low:
            position = "below"
        else:
            position = "inside"

        poc_dist = (current_close - poc) / poc if poc else 0.0

        return {
            "poc": round(poc, 4),
            "va_high": round(va_high, 4),
            "va_low": round(va_low, 4),
            "value_area_position": position,
            "poc_distance_pct": round(poc_dist, 6),
        }
    except Exception as exc:
        logging.debug("compute_volume_profile error: %s", exc)
        return {}


def compute_round_number_proximity(price: float, tick: float = 5.0) -> dict:
    """Check proximity to round numbers."""
    try:
        if not price or price <= 0:
            return {}

        nearest_round5 = round(price / tick) * tick
        nearest_round1 = round(price)

        r5_dist = abs(price - nearest_round5) / price
        r1_dist = abs(price - nearest_round1) / price

        return {
            "nearest_round5": nearest_round5,
            "nearest_round1": float(nearest_round1),
            "round5_distance_pct": round(r5_dist, 6),
            "round1_distance_pct": round(r1_dist, 6),
            "near_round5": r5_dist < 0.001,
            "near_round1": r1_dist < 0.0005,
        }
    except Exception as exc:
        logging.debug("compute_round_number_proximity error: %s", exc)
        return {}


def compute_prev_day_levels(df) -> dict:
    """Previous day's high/low/close and today's opening range."""
    try:
        close_s = _get_col(df, "close", "Close")
        high_s = _get_col(df, "high", "High")
        low_s = _get_col(df, "low", "Low")
        if close_s is None or high_s is None or low_s is None:
            return {}

        idx = df.index
        if hasattr(idx, "date"):
            dates = idx.date
        else:
            ts_col = _col(df, "timestamp", "Timestamp", "time")
            if ts_col is None:
                return {}
            import pandas as pd
            dates = pd.to_datetime(df[ts_col]).dt.date.values

        unique_dates = sorted(set(dates))
        if len(unique_dates) < 2:
            return {}

        prev_date = unique_dates[-2]
        today_date = unique_dates[-1]
        prev_mask = [d == prev_date for d in dates]
        today_mask = [d == today_date for d in dates]

        prev_high = float(high_s[prev_mask].max())
        prev_low = float(low_s[prev_mask].min())
        prev_close = float(close_s[prev_mask].iloc[-1])

        current_close = float(close_s.iloc[-1])

        result = {
            "prev_high": round(prev_high, 4),
            "prev_low": round(prev_low, 4),
            "prev_close": round(prev_close, 4),
            "above_prev_high": current_close > prev_high,
            "below_prev_low": current_close < prev_low,
        }

        # Today's opening range
        today_highs = high_s[today_mask].values
        today_lows = low_s[today_mask].values
        today_closes = close_s[today_mask].values

        if len(today_highs) >= 5:
            or5_high = float(max(today_highs[:5]))
            or5_low = float(min(today_lows[:5]))
            result["or5_high"] = round(or5_high, 4)
            result["or5_low"] = round(or5_low, 4)
            if current_close > or5_high:
                result["or5_breakout"] = "above"
            elif current_close < or5_low:
                result["or5_breakout"] = "below"
            else:
                result["or5_breakout"] = "inside"

        if len(today_highs) >= 15:
            or15_high = float(max(today_highs[:15]))
            or15_low = float(min(today_lows[:15]))
            result["or15_high"] = round(or15_high, 4)
            result["or15_low"] = round(or15_low, 4)
            if current_close > or15_high:
                result["or15_breakout"] = "above"
            elif current_close < or15_low:
                result["or15_breakout"] = "below"
            else:
                result["or15_breakout"] = "inside"

        return result
    except Exception as exc:
        logging.debug("compute_prev_day_levels error: %s", exc)
        return {}


def compute_all_structure(df, current_price: Optional[float] = None) -> dict:
    """Aggregate all market structure computations into one flat dict."""
    try:
        if df is None or len(df) == 0:
            return {}

        if current_price is None:
            close_s = _get_col(df, "close", "Close")
            if close_s is not None and len(close_s) > 0:
                current_price = float(close_s.iloc[-1])

        result = {}
        for fn in (
            lambda: compute_swing_levels(df),
            lambda: compute_pivot_levels(df),
            lambda: compute_vwap_bands(df),
            lambda: compute_volume_profile(df),
            lambda: compute_round_number_proximity(current_price) if current_price else {},
            lambda: compute_prev_day_levels(df),
        ):
            try:
                data = fn()
                if isinstance(data, dict):
                    for k, v in data.items():
                        # Skip list values for the flat dict (swing_highs/swing_lows)
                        if not isinstance(v, (list, dict)):
                            result[k] = v
            except Exception:
                pass
        return result
    except Exception as exc:
        logging.debug("compute_all_structure error: %s", exc)
        return {}
