"""
scripts/prefetch_options.py
Pre-download option bar data for all contracts the backtest engine might need,
so engine runs hit parquet cache instead of Alpaca API.

Usage:
    python scripts/prefetch_options.py --start 2024-06-01 --end 2024-12-31
    python scripts/prefetch_options.py --start 2024-06-01 --end 2024-12-31 --sims SIM03 SIM05
    python scripts/prefetch_options.py --start 2024-06-01 --end 2024-12-31 --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path so we can import backtest modules
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backtest.data_fetcher import (  # noqa: E402
    fetch_stock_bars,
    fetch_option_bars,
    build_occ_symbol,
    CACHE_DIR,
)

SIM_CONFIG_PATH = PROJECT_ROOT / "simulation" / "sim_config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prefetch")


# ---------------------------------------------------------------------------
# Helpers (duplicated from backtest/engine.py to avoid importing the full engine)
# ---------------------------------------------------------------------------

def _select_expiry(trade_date: date, dte_min: int, dte_max: int) -> date | None:
    """Nearest valid weekday expiry within [dte_min, dte_max] trading days."""
    candidates = []
    trading_dte = 0
    expiry_date = trade_date
    while trading_dte <= dte_max + 2:
        if expiry_date.weekday() < 5:
            if dte_min <= trading_dte <= dte_max:
                candidates.append((expiry_date, trading_dte))
            trading_dte += 1
        expiry_date += timedelta(days=1)
    return candidates[0][0] if candidates else None


def _select_strike(price: float, direction: str, otm_pct: float) -> float:
    if direction == "BULLISH":
        return float(round(price * (1 + otm_pct)))
    else:
        return float(round(price * (1 - otm_pct)))


def _trading_days(start: date, end: date) -> list[date]:
    """Return all weekdays between start and end (inclusive)."""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def _hold_max_days(profile: dict) -> int:
    """Max calendar days a trade can be open (from hold_max_seconds)."""
    secs = float(profile.get("hold_max_seconds", 3600))
    return max(1, int(secs / 86400) + 1)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_sim_profiles(sim_filter: list[str] | None = None) -> dict[str, dict]:
    """Load sim profiles from sim_config.yaml, optionally filtered."""
    with open(SIM_CONFIG_PATH) as f:
        raw = yaml.safe_load(f)

    global_cfg = raw.get("_global", {})
    profiles = {}
    for key, val in raw.items():
        if key.startswith("_") or key == "symbol_registry":
            continue
        if not isinstance(val, dict):
            continue
        if sim_filter and key not in sim_filter:
            continue
        # Merge global defaults under profile
        merged = {**global_cfg, **val}
        profiles[key] = merged
    return profiles


def resolve_symbols(profile: dict) -> list[str]:
    """Extract tradeable symbol list from a profile."""
    TRADEABLE = {"SPY", "QQQ", "IWM", "VXX", "TSLA", "AAPL", "NVDA", "MSFT"}
    syms = profile.get("symbols")
    if syms and isinstance(syms, list):
        syms = [str(s).upper() for s in syms]
    elif profile.get("symbol"):
        syms = [str(profile["symbol"]).upper()]
    else:
        syms = []
    return [s for s in syms if s in TRADEABLE] or syms[:1]


def sample_prices_per_day(
    stock_df: pd.DataFrame,
) -> dict[date, list[float]]:
    """
    From a stock bar DataFrame, extract representative prices per trading day.
    Samples at ~30-min intervals to capture the price range the underlying trades at.
    """
    if stock_df.empty:
        return {}

    result: dict[date, list[float]] = {}
    # Group by date
    stock_df = stock_df.copy()
    stock_df["_date"] = stock_df.index.date
    for d, group in stock_df.groupby("_date"):
        prices = set()
        # Sample open of day, close of day, and every ~30 bars (~30 min)
        closes = group["close"].dropna()
        if closes.empty:
            continue
        prices.add(float(closes.iloc[0]))
        prices.add(float(closes.iloc[-1]))
        for i in range(0, len(closes), 30):
            prices.add(float(closes.iloc[i]))
        # Also add high/low of day for wider strike coverage
        prices.add(float(group["high"].max()))
        prices.add(float(group["low"].min()))
        result[d] = sorted(prices)
    return result


def enumerate_contracts(
    profiles: dict[str, dict],
    start_date: str,
    end_date: str,
) -> tuple[set[tuple[str, str]], dict[str, pd.DataFrame]]:
    """
    Enumerate all (occ_contract, date_str) pairs needed across all sims.
    Also returns the stock DataFrames so they don't need to be re-fetched.

    Returns:
        (set of (contract, date_str), dict of {symbol: stock_df})
    """
    # Collect unique symbol sets and their param combos
    # Key: symbol -> list of (dte_min, dte_max, otm_pct, hold_days)
    symbol_params: dict[str, list[tuple[int, int, float, int]]] = {}
    for pid, prof in profiles.items():
        syms = resolve_symbols(prof)
        dte_min = int(prof.get("dte_min", 0))
        dte_max = int(prof.get("dte_max", 1))
        otm_pct = float(prof.get("otm_pct", 0.005))
        hold_days = _hold_max_days(prof)
        for s in syms:
            if s not in symbol_params:
                symbol_params[s] = []
            combo = (dte_min, dte_max, otm_pct, hold_days)
            if combo not in symbol_params[s]:
                symbol_params[s].append(combo)

    log.info(
        "Symbols to process: %s (%d param combos total)",
        ", ".join(sorted(symbol_params)),
        sum(len(v) for v in symbol_params.values()),
    )

    # Fetch stock bars for all symbols
    stock_dfs: dict[str, pd.DataFrame] = {}
    for sym in sorted(symbol_params):
        log.info("Fetching stock bars: %s %s -> %s", sym, start_date, end_date)
        df = fetch_stock_bars(sym, start_date, end_date)
        if df is not None and not df.empty:
            stock_dfs[sym] = df
            log.info("  %s: %d bars", sym, len(df))
        else:
            log.warning("  %s: NO DATA", sym)

    # Enumerate contracts
    all_pairs: set[tuple[str, str]] = set()
    t_days = _trading_days(
        date.fromisoformat(start_date),
        date.fromisoformat(end_date),
    )

    for sym, params_list in symbol_params.items():
        if sym not in stock_dfs:
            continue
        prices_by_day = sample_prices_per_day(stock_dfs[sym])
        log.info("Enumerating contracts for %s (%d trading days with data)", sym, len(prices_by_day))

        for trade_day in t_days:
            if trade_day not in prices_by_day:
                continue
            sample_prices = prices_by_day[trade_day]

            for dte_min, dte_max, otm_pct, hold_days in params_list:
                # All possible expiry dates for this day
                expiries = set()
                trading_dte = 0
                check_date = trade_day
                while trading_dte <= dte_max + 2:
                    if check_date.weekday() < 5:
                        if dte_min <= trading_dte <= dte_max:
                            expiries.add(check_date)
                        trading_dte += 1
                    check_date += timedelta(days=1)

                for price in sample_prices:
                    if price <= 0:
                        continue
                    for direction in ("BULLISH", "BEARISH"):
                        strike = _select_strike(price, direction, otm_pct)
                        if strike <= 0:
                            continue
                        # Also include ±1 strike for rounding edge cases
                        strikes = {strike, strike - 1, strike + 1}
                        for s in strikes:
                            if s <= 0:
                                continue
                            for expiry in expiries:
                                contract = build_occ_symbol(sym, expiry, direction, s)
                                # Need bars on trade_day (entry)
                                all_pairs.add((contract, str(trade_day)))
                                # Need bars on subsequent days for exit (multi-day holds)
                                if hold_days > 1 or dte_max > 0:
                                    extra_days = min(hold_days, (expiry - trade_day).days + 1, 5)
                                    d = trade_day + timedelta(days=1)
                                    added = 0
                                    while added < extra_days and d <= expiry:
                                        if d.weekday() < 5:
                                            all_pairs.add((contract, str(d)))
                                            added += 1
                                        d += timedelta(days=1)

    return all_pairs, stock_dfs


def check_cache(pairs: set[tuple[str, str]]) -> tuple[list[tuple[str, str]], int]:
    """
    Check which (contract, date) pairs are already cached.
    Returns (needs_fetch list, already_cached count).
    """
    needs_fetch = []
    cached = 0
    for contract, date_str in pairs:
        safe = contract.replace("/", "_")
        path = os.path.join(CACHE_DIR, f"option_bars_{safe}_{date_str}.parquet")
        if os.path.exists(path):
            cached += 1
        else:
            needs_fetch.append((contract, date_str))
    # Sort by date then contract for Alpaca-friendly ordering
    needs_fetch.sort(key=lambda x: (x[1], x[0]))
    return needs_fetch, cached


def fetch_all(
    needs_fetch: list[tuple[str, str]],
    already_cached: int,
) -> tuple[int, int]:
    """
    Fetch all uncached option bars with rate limiting and retry.
    Returns (success_count, fail_count).
    """
    total = len(needs_fetch)
    if total == 0:
        return 0, 0

    success = 0
    failed = 0
    start_time = time.time()

    log.info(
        "Starting fetch: %d contracts to download (%d already cached)",
        total,
        already_cached,
    )

    for i, (contract, date_str) in enumerate(needs_fetch):
        # Progress every 50 fetches or first/last
        if i % 50 == 0 or i == total - 1:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (total - i - 1) / rate if rate > 0 else 0
            eta_min = remaining / 60
            log.info(
                "[%d / %d] %.0f%% | ok=%d fail=%d | %.1f/s | ETA: %.0fm",
                i + 1,
                total,
                (i + 1) / total * 100,
                success,
                failed,
                rate,
                eta_min,
            )

        # Retry loop (fetch_option_bars already sleeps 0.35s)
        ok = False
        for attempt in range(3):
            try:
                df = fetch_option_bars(contract, date_str)
                ok = True
                break
            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate" in err_str or "500" in err_str:
                    wait = [2, 5, 15][attempt]
                    log.warning(
                        "  Rate limited / server error on %s %s (attempt %d), "
                        "waiting %ds: %s",
                        contract,
                        date_str,
                        attempt + 1,
                        wait,
                        e,
                    )
                    time.sleep(wait)
                else:
                    log.warning("  Failed %s %s: %s", contract, date_str, e)
                    break

        if ok:
            success += 1
        else:
            failed += 1

    return success, failed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pre-download option bars for backtest cache",
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--sims",
        nargs="*",
        default=None,
        help="Sim IDs to include (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just count contracts, don't fetch",
    )
    parser.add_argument(
        "--skip-live",
        action="store_true",
        default=True,
        help="Skip SIM00 (default: true)",
    )
    args = parser.parse_args()

    t0 = time.time()

    # Load profiles
    sim_filter = [s.upper() for s in args.sims] if args.sims else None
    profiles = load_sim_profiles(sim_filter)
    if args.skip_live:
        profiles.pop("SIM00", None)

    if not profiles:
        log.error("No sim profiles found")
        sys.exit(1)

    log.info(
        "Loaded %d sim profiles: %s",
        len(profiles),
        ", ".join(sorted(profiles)),
    )

    # Enumerate all contracts
    log.info("Enumerating contracts for %s -> %s ...", args.start, args.end)
    all_pairs, stock_dfs = enumerate_contracts(profiles, args.start, args.end)
    log.info("Total unique (contract, date) pairs: %d", len(all_pairs))

    # Check cache
    needs_fetch, already_cached = check_cache(all_pairs)
    log.info("Already cached: %d", already_cached)
    log.info("Need to fetch:  %d", len(needs_fetch))

    if args.dry_run:
        est_minutes = len(needs_fetch) * 0.4 / 60  # ~0.4s per fetch with overhead
        log.info("Estimated fetch time: %.0f minutes", est_minutes)
        log.info("Dry run — exiting without fetching.")
        return

    if not needs_fetch:
        log.info("Nothing to fetch — cache is complete!")
        return

    # Estimate time
    est_minutes = len(needs_fetch) * 0.4 / 60
    log.info("Estimated fetch time: ~%.0f minutes", est_minutes)

    # Fetch
    success, failed = fetch_all(needs_fetch, already_cached)

    elapsed = time.time() - t0
    log.info("=" * 60)
    log.info("Prefetch complete:")
    log.info("  Sims scanned:      %d", len(profiles))
    log.info("  Symbols:           %s", ", ".join(sorted(stock_dfs)))
    log.info("  Unique contracts:  %d", len(all_pairs))
    log.info("  Already cached:    %d", already_cached)
    log.info("  Fetched:           %d", success)
    log.info("  Failed/empty:      %d", failed)
    log.info("  Time elapsed:      %dm %ds", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()
