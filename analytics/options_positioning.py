"""
analytics/options_positioning.py
Options positioning analytics: GEX, max pain, OI walls, liquidation zones.

Fetches SPY options chain from Alpaca, caches for 5 minutes.
"""
import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pytz

from core.rate_limiter import rate_limit_sleep

ALPACA_MIN_CALL_INTERVAL_SEC = float(os.getenv("ALPACA_MIN_CALL_INTERVAL_SEC", "0.5"))

_OPTIONS_CACHE = {}  # keyed by symbol: {"data": ..., "timestamp": ...}
_OPTIONS_CACHE_TTL = 300  # 5 minutes


def _get_trading_client():
    """Get Alpaca TradingClient using env credentials (same pattern as rest of codebase)."""
    try:
        from alpaca.trading.client import TradingClient
        api_key = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")
        if not api_key or not secret_key:
            return None
        return TradingClient(api_key, secret_key, paper=True)
    except Exception:
        return None


def _get_option_data_client():
    """Get Alpaca OptionHistoricalDataClient."""
    try:
        from alpaca.data.historical import OptionHistoricalDataClient
        api_key = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")
        if not api_key or not secret_key:
            return None
        return OptionHistoricalDataClient(api_key, secret_key)
    except Exception:
        return None


def _fetch_options_chain(underlying: str = "SPY") -> list[dict]:
    """Fetch option chain data with OI from contracts + greeks from snapshots.

    Returns list of dicts with keys: symbol, type, strike_price, expiration_date,
    open_interest, close_price, delta, gamma.
    """
    try:
        # Check per-symbol cache
        now = time.time()
        cached = _OPTIONS_CACHE.get(underlying)
        if (
            cached is not None
            and cached.get("data") is not None
            and now - cached.get("timestamp", 0) < _OPTIONS_CACHE_TTL
        ):
            return cached["data"]

        eastern = pytz.timezone("US/Eastern")
        today = datetime.now(eastern).date()
        expiry_max = today + timedelta(days=7)

        # Step 1: Get contracts (OI, strike, type)
        trading_client = _get_trading_client()
        if trading_client is None:
            return []

        from alpaca.trading.requests import GetOptionContractsRequest

        rate_limit_sleep("alpaca_option_contracts", ALPACA_MIN_CALL_INTERVAL_SEC)
        contracts_resp = trading_client.get_option_contracts(
            GetOptionContractsRequest(
                underlying_symbols=[underlying],
                status="active",
                expiration_date_gte=today,
                expiration_date_lte=expiry_max,
                strike_price_gte=None,
                strike_price_lte=None,
            )
        )

        # Normalize response — may be list or have .option_contracts attr
        raw_contracts = []
        if isinstance(contracts_resp, list):
            raw_contracts = contracts_resp
        elif hasattr(contracts_resp, "option_contracts"):
            raw_contracts = contracts_resp.option_contracts or []
        else:
            try:
                raw_contracts = list(contracts_resp)
            except (TypeError, ValueError):
                raw_contracts = []

        if not raw_contracts:
            return []

        # Build contract data dict keyed by symbol
        contracts_by_sym = {}
        for c in raw_contracts:
            sym = getattr(c, "symbol", None)
            if not sym:
                continue
            oi = getattr(c, "open_interest", None)
            strike = getattr(c, "strike_price", None)
            ctype = getattr(c, "type", None)
            close_px = getattr(c, "close_price", None)
            exp = getattr(c, "expiration_date", None)

            contracts_by_sym[sym] = {
                "symbol": sym,
                "type": str(ctype).lower() if ctype else None,
                "strike_price": float(strike) if strike is not None else None,
                "expiration_date": str(exp) if exp else None,
                "open_interest": int(oi) if oi is not None else 0,
                "close_price": float(close_px) if close_px is not None else None,
                "delta": None,
                "gamma": None,
            }

        # Step 2: Get greeks from option chain snapshots
        data_client = _get_option_data_client()
        if data_client is not None:
            try:
                from alpaca.data.requests import OptionChainRequest

                rate_limit_sleep("alpaca_option_chain", ALPACA_MIN_CALL_INTERVAL_SEC)
                chain = data_client.get_option_chain(
                    OptionChainRequest(
                        underlying_symbol=underlying,
                        expiration_date_gte=today,
                        expiration_date_lte=expiry_max,
                    )
                )

                # Normalize chain to dict
                if not isinstance(chain, dict):
                    try:
                        chain = dict(chain)
                    except (TypeError, ValueError):
                        chain = {}

                for sym, snap in chain.items():
                    if sym not in contracts_by_sym:
                        continue
                    greeks = getattr(snap, "greeks", None)
                    if greeks:
                        delta = getattr(greeks, "delta", None)
                        gamma = getattr(greeks, "gamma", None)
                        if delta is not None:
                            contracts_by_sym[sym]["delta"] = float(delta)
                        if gamma is not None:
                            contracts_by_sym[sym]["gamma"] = float(gamma)
            except Exception as exc:
                logging.debug("options_chain_greeks_fetch_error: %s", exc)

        result = list(contracts_by_sym.values())

        # Update per-symbol cache
        _OPTIONS_CACHE[underlying] = {"data": result, "timestamp": now}

        return result
    except Exception as exc:
        logging.debug("fetch_options_chain error (%s): %s", underlying, exc)
        return []


def compute_gamma_exposure(chain: list[dict], spot_price: float) -> dict:
    """Compute GEX (Gamma Exposure) by strike."""
    try:
        if not chain or not spot_price or spot_price <= 0:
            return {}

        # Filter to contracts with gamma
        has_gamma = [c for c in chain if c.get("gamma") is not None and c.get("open_interest", 0) > 0]
        if not has_gamma:
            return {}

        gex_by_strike = {}
        for c in has_gamma:
            strike = c.get("strike_price")
            gamma = c.get("gamma", 0)
            oi = c.get("open_interest", 0)
            ctype = c.get("type", "")

            if strike is None or gamma is None:
                continue

            contract_gex = abs(gamma) * oi * 100 * spot_price ** 2 * 0.01

            # Dealer convention: short calls → positive GEX, short puts → negative GEX
            if "call" in str(ctype).lower():
                gex_contribution = contract_gex
            else:
                gex_contribution = -contract_gex

            gex_by_strike[strike] = gex_by_strike.get(strike, 0) + gex_contribution

        if not gex_by_strike:
            return {}

        total_gex = sum(gex_by_strike.values())

        # Max positive GEX strike (strongest magnet)
        max_gex_strike = max(gex_by_strike, key=gex_by_strike.get)

        # GEX flip: strike where cumulative GEX crosses zero (scanning low to high)
        sorted_strikes = sorted(gex_by_strike.keys())
        gex_flip_strike = None
        cumulative = 0
        for s in sorted_strikes:
            prev_cum = cumulative
            cumulative += gex_by_strike[s]
            if prev_cum <= 0 < cumulative:
                gex_flip_strike = s
                break
        # If no flip found scanning up, try finding where it goes negative
        if gex_flip_strike is None:
            cumulative = 0
            for s in sorted_strikes:
                prev_cum = cumulative
                cumulative += gex_by_strike[s]
                if prev_cum >= 0 > cumulative:
                    gex_flip_strike = s
                    break

        above_flip = spot_price > gex_flip_strike if gex_flip_strike else None
        dist_to_flip = (
            (gex_flip_strike - spot_price) / spot_price
            if gex_flip_strike and spot_price
            else None
        )

        return {
            "total_gex": round(total_gex, 2),
            "gex_flip_strike": gex_flip_strike,
            "max_gex_strike": max_gex_strike,
            "gex_positive": total_gex > 0,
            "above_gex_flip": above_flip,
            "distance_to_gex_flip_pct": round(dist_to_flip, 6) if dist_to_flip is not None else None,
        }
    except Exception as exc:
        logging.debug("compute_gamma_exposure error: %s", exc)
        return {}


def compute_max_pain(chain: list[dict], spot_price: float) -> dict:
    """Find the max pain strike (minimizes total intrinsic value)."""
    try:
        if not chain or not spot_price:
            return {}

        # Get all unique strikes
        strikes = sorted(set(
            c["strike_price"] for c in chain
            if c.get("strike_price") is not None
        ))
        if not strikes:
            return {}

        calls = [c for c in chain if "call" in str(c.get("type", "")).lower() and c.get("open_interest", 0) > 0]
        puts = [c for c in chain if "put" in str(c.get("type", "")).lower() and c.get("open_interest", 0) > 0]

        if not calls and not puts:
            return {}

        min_pain = float("inf")
        max_pain_strike = strikes[0]

        for test_strike in strikes:
            total_pain = 0
            for c in calls:
                k = c["strike_price"]
                oi = c.get("open_interest", 0)
                if test_strike > k:
                    total_pain += (test_strike - k) * oi * 100
            for p in puts:
                k = p["strike_price"]
                oi = p.get("open_interest", 0)
                if test_strike < k:
                    total_pain += (k - test_strike) * oi * 100

            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = test_strike

        dist = (max_pain_strike - spot_price) / spot_price if spot_price else 0

        return {
            "max_pain_strike": max_pain_strike,
            "distance_to_max_pain_pct": round(dist, 6),
            "price_above_max_pain": spot_price > max_pain_strike,
        }
    except Exception as exc:
        logging.debug("compute_max_pain error: %s", exc)
        return {}


def compute_oi_walls(chain: list[dict], spot_price: float, num_strikes: int = 5) -> dict:
    """Find strikes with highest call/put OI — resistance/support magnets."""
    try:
        if not chain or not spot_price:
            return {}

        calls = [c for c in chain if "call" in str(c.get("type", "")).lower() and c.get("open_interest", 0) > 0]
        puts = [c for c in chain if "put" in str(c.get("type", "")).lower() and c.get("open_interest", 0) > 0]

        result = {}

        # Top call walls (resistance)
        calls_sorted = sorted(calls, key=lambda c: c.get("open_interest", 0), reverse=True)
        top_calls = calls_sorted[:num_strikes]
        top_call_strikes = sorted([c["strike_price"] for c in top_calls if c.get("strike_price")])
        result["top_call_walls"] = top_call_strikes

        # Top put walls (support)
        puts_sorted = sorted(puts, key=lambda c: c.get("open_interest", 0), reverse=True)
        top_puts = puts_sorted[:num_strikes]
        top_put_strikes = sorted([p["strike_price"] for p in top_puts if p.get("strike_price")])
        result["top_put_walls"] = top_put_strikes

        # Nearest call wall above spot
        calls_above = [s for s in top_call_strikes if s >= spot_price]
        nearest_call = min(calls_above) if calls_above else None
        result["nearest_call_wall"] = nearest_call
        result["call_wall_distance_pct"] = (
            round((nearest_call - spot_price) / spot_price, 6)
            if nearest_call and spot_price else None
        )

        # Nearest put wall below spot
        puts_below = [s for s in top_put_strikes if s <= spot_price]
        nearest_put = max(puts_below) if puts_below else None
        result["nearest_put_wall"] = nearest_put
        result["put_wall_distance_pct"] = (
            round((spot_price - nearest_put) / spot_price, 6)
            if nearest_put and spot_price else None
        )

        # OI totals above/below spot
        call_oi_above = sum(c.get("open_interest", 0) for c in calls if c.get("strike_price", 0) > spot_price)
        put_oi_below = sum(p.get("open_interest", 0) for p in puts if p.get("strike_price", 0) < spot_price)
        result["call_oi_above"] = call_oi_above
        result["put_oi_below"] = put_oi_below
        result["put_call_oi_ratio"] = (
            round(put_oi_below / call_oi_above, 4)
            if call_oi_above > 0 else None
        )

        return result
    except Exception as exc:
        logging.debug("compute_oi_walls error: %s", exc)
        return {}


def compute_liquidation_zones(chain: list[dict], spot_price: float) -> dict:
    """Estimate strikes where stop-cluster / forced-liquidation flows concentrate."""
    try:
        if not chain or not spot_price or spot_price <= 0:
            return {}

        lower_bound = spot_price * 0.95
        upper_bound = spot_price * 1.05

        # Filter to contracts near spot with gamma
        nearby = [
            c for c in chain
            if c.get("strike_price") is not None
            and lower_bound <= c["strike_price"] <= upper_bound
        ]

        # Compute flow impact per strike
        flow_by_strike = {}
        for c in nearby:
            strike = c["strike_price"]
            gamma = abs(c.get("gamma", 0) or 0)
            oi = c.get("open_interest", 0)
            impact = gamma * oi * 100
            flow_by_strike[strike] = flow_by_strike.get(strike, 0) + impact

        if not flow_by_strike:
            return {}

        # Split into above/below spot, sort by impact
        above = [(s, v) for s, v in flow_by_strike.items() if s > spot_price]
        below = [(s, v) for s, v in flow_by_strike.items() if s < spot_price]

        above.sort(key=lambda x: x[1], reverse=True)
        below.sort(key=lambda x: x[1], reverse=True)

        upside_wires = [s for s, _ in above[:3]]
        downside_wires = [s for s, _ in below[:3]]

        nearest_up = min(upside_wires) if upside_wires else None
        nearest_down = max(downside_wires) if downside_wires else None

        # In low gamma zone = not near any high-impact strike
        in_low_gamma = True
        if flow_by_strike:
            for strike, impact in flow_by_strike.items():
                if abs(strike - spot_price) / spot_price < 0.003 and impact > 0:
                    in_low_gamma = False
                    break

        return {
            "upside_trip_wires": sorted(upside_wires),
            "downside_trip_wires": sorted(downside_wires, reverse=True),
            "nearest_upside_trigger": nearest_up,
            "nearest_downside_trigger": nearest_down,
            "nearest_upside_trigger_dist_pct": (
                round((nearest_up - spot_price) / spot_price, 6)
                if nearest_up and spot_price else None
            ),
            "nearest_downside_trigger_dist_pct": (
                round((spot_price - nearest_down) / spot_price, 6)
                if nearest_down and spot_price else None
            ),
            "in_low_gamma_zone": in_low_gamma,
        }
    except Exception as exc:
        logging.debug("compute_liquidation_zones error: %s", exc)
        return {}


def compute_all_options_positioning(spot_price: float, symbol: str = "SPY") -> dict:
    """Aggregate all options positioning computations for a given symbol."""
    try:
        if not spot_price or spot_price <= 0:
            return {"options_data_available": False}

        chain = _fetch_options_chain(symbol)
        if not chain:
            return {"options_data_available": False}

        result = {"options_data_available": True}

        for fn in (
            lambda: compute_gamma_exposure(chain, spot_price),
            lambda: compute_max_pain(chain, spot_price),
            lambda: compute_oi_walls(chain, spot_price),
            lambda: compute_liquidation_zones(chain, spot_price),
        ):
            try:
                data = fn()
                if isinstance(data, dict):
                    for k, v in data.items():
                        # Skip list values for the flat dict
                        if not isinstance(v, (list, dict)):
                            result[k] = v
            except Exception:
                pass

        return result
    except Exception as exc:
        logging.debug("compute_all_options_positioning error: %s", exc)
        return {"options_data_available": False}
