"""
scripts/prefetch_options.py
Pre-download option bar data for all contracts the backtest engine might need,
so engine runs hit parquet cache instead of Alpaca API.

Usage:
    # Simple date-range mode (original):
    python scripts/prefetch_options.py --start 2024-02-01 --end 2026-03-13
    python scripts/prefetch_options.py --start 2024-02-01 --end 2026-03-13 --sims SIM03 SIM05
    python scripts/prefetch_options.py --start 2024-02-01 --end 2026-03-13 --dry-run

    # Optimizer-config mode (NEW):
    #   Reads sim_config.yaml to enumerate all profiles, uses a default date window of
    #   2024-02-01 → today, mirrors the optimizer's walk-forward fold structure, and
    #   overrides hold_max with the optimizer sweep's maximum (240 min) so every
    #   param combo has the exit-day bars it needs.
    python scripts/prefetch_options.py --optimizer-config simulation/sim_config.yaml
    python scripts/prefetch_options.py --optimizer-config simulation/sim_config.yaml --sims SIM03 SIM22 --dry-run
    python scripts/prefetch_options.py --optimizer-config simulation/sim_config.yaml --start 2024-06-01 --end 2026-03-13
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import logging
from datetime import date, datetime, timedelta
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
    fetch_option_bars_batch,
    build_occ_symbol,
    CACHE_DIR,
)

SIM_CONFIG_PATH = PROJECT_ROOT / "simulation" / "sim_config.yaml"

# Optimizer sweep constants — must match backtest/optimizer.py exactly.
# These are the hold_max values the grid search will test; we cache enough
# exit-day bars to cover the largest one.
_OPTIMIZER_HOLD_MAX_MINUTES = [15, 30, 60, 120, 240]
_OPTIMIZER_HOLD_MAX_SECONDS = max(_OPTIMIZER_HOLD_MAX_MINUTES) * 60  # 14400 s = 4 h = 1 day

# Default date window for --optimizer-config mode
_OPTIMIZER_DEFAULT_START = "2024-02-01"
_OPTIMIZER_DEFAULT_END = datetime.today().strftime("%Y-%m-%d")  # today

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


def _hold_max_days(profile: dict, override_hold_max_seconds: int | None = None) -> int:
    """Max calendar days a trade can be open."""
    secs = float(override_hold_max_seconds or profile.get("hold_max_seconds", 3600))
    return max(1, int(secs / 86400) + 1)


# ---------------------------------------------------------------------------
# Optimizer fold logic (mirrors backtest/optimizer.py SimOptimizer._build_folds)
# ---------------------------------------------------------------------------

def _build_optimizer_folds(start_date: str, end_date: str) -> list[dict]:
    """
    Mirror SimOptimizer._build_folds (3-fold walk-forward).
    Returns list of fold dicts with train_start/train_end/test_start/test_end.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")
    total_days = (end - start).days
    chunk = timedelta(days=total_days // 6)

    folds = [
        {
            "fold": 1,
            "train_start": start.strftime("%Y-%m-%d"),
            "train_end":   (start + 4 * chunk).strftime("%Y-%m-%d"),
            "test_start":  (start + 4 * chunk).strftime("%Y-%m-%d"),
            "test_end":    (start + 6 * chunk).strftime("%Y-%m-%d"),
        },
        {
            "fold": 2,
            "train_start": (start + chunk).strftime("%Y-%m-%d"),
            "train_end":   (start + 5 * chunk).strftime("%Y-%m-%d"),
            "test_start":  (start + 5 * chunk).strftime("%Y-%m-%d"),
            "test_end":    end.strftime("%Y-%m-%d"),
        },
        {
            "fold": 3,
            "train_start": (start + 2 * chunk).strftime("%Y-%m-%d"),
            "train_end":   (end - chunk).strftime("%Y-%m-%d"),
            "test_start":  (end - chunk).strftime("%Y-%m-%d"),
            "test_end":    end.strftime("%Y-%m-%d"),
        },
    ]
    return folds


def _union_date_range(folds: list[dict]) -> tuple[str, str]:
    """
    Compute the earliest train_start and latest test_end across all folds.
    This is the total date window we need to cache.
    """
    starts = [f["train_start"] for f in folds]
    ends   = [f["test_end"]    for f in folds]
    return min(starts), max(ends)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_sim_profiles(
    config_path: Path,
    sim_filter: list[str] | None = None,
) -> dict[str, dict]:
    """Load sim profiles from config YAML, optionally filtered."""
    with open(config_path) as f:
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
    stock_df = stock_df.copy()
    stock_df["_date"] = stock_df.index.date
    for d, group in stock_df.groupby("_date"):
        prices = set()
        closes = group["close"].dropna()
        if closes.empty:
            continue
        prices.add(float(closes.iloc[0]))
        prices.add(float(closes.iloc[-1]))
        for i in range(0, len(closes), 30):
            prices.add(float(closes.iloc[i]))
        prices.add(float(group["high"].max()))
        prices.add(float(group["low"].min()))
        result[d] = sorted(prices)
    return result


def enumerate_contracts(
    profiles: dict[str, dict],
    start_date: str,
    end_date: str,
    optimizer_mode: bool = False,
) -> tuple[set[tuple[str, str]], dict[str, pd.DataFrame]]:
    """
    Enumerate all (occ_contract, date_str) pairs needed across all sims.

    In optimizer_mode, hold_max_seconds is overridden to the optimizer sweep's
    maximum (240 min) so that exit-day bars are cached for every param combo.

    Returns:
        (set of (contract, date_str), dict of {symbol: stock_df})
    """
    hold_override = _OPTIMIZER_HOLD_MAX_SECONDS if optimizer_mode else None

    # Collect unique symbol sets and their param combos
    symbol_params: dict[str, list[tuple[int, int, float, int]]] = {}
    for pid, prof in profiles.items():
        syms = resolve_symbols(prof)
        dte_min  = int(prof.get("dte_min", 0))
        dte_max  = int(prof.get("dte_max", 1))
        otm_pct  = float(prof.get("otm_pct", 0.005))
        hold_days = _hold_max_days(prof, hold_override)
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


# Number of contracts to fetch per API call.
# Alpaca's limit is 10,000 bars per page. With 390 min/day, 100 contracts
# = 39,000 bars ≈ 4 pages per request. Keeping batches small reduces
# pagination overhead and lets more parallel workers finish quickly.
BATCH_SIZE = 100


def fetch_all(
    needs_fetch: list[tuple[str, str]],
    already_cached: int,
) -> tuple[int, int]:
    """
    Batch-fetch all uncached option bars with concurrency, rate limiting, and retry.
    Contracts are grouped by date; up to BATCH_SIZE are fetched per API call.
    Runs multiple batches concurrently using ThreadPoolExecutor.
    Returns (success_count, fail_count).
    """
    total = len(needs_fetch)
    if total == 0:
        return 0, 0

    from collections import defaultdict
    import concurrent.futures
    import threading

    # Group uncached pairs by date
    by_date: dict[str, list[str]] = defaultdict(list)
    for contract, date_str in needs_fetch:
        by_date[date_str].append(contract)

    # Build list of all (batch, date_str) jobs
    all_jobs = []
    for date_str in sorted(by_date):
        contracts = by_date[date_str]
        for batch_start in range(0, len(contracts), BATCH_SIZE):
            batch = contracts[batch_start: batch_start + BATCH_SIZE]
            all_jobs.append((batch, date_str))

    total_batches = len(all_jobs)
    success = 0
    failed = 0
    batch_num = 0
    start_time = time.time()
    
    # Thread-safe lock for shared progress counters
    progress_lock = threading.Lock()

    # N_WORKERS=1: Alpaca rate-limits concurrent connections, so more workers
    # actually makes things slower. Single-threaded sequential fetching is optimal.
    N_WORKERS = 1

    log.info(
        "Starting fetch: %d contracts in ~%d batches of %d (%d already cached). Using %d concurrent workers.",
        total, total_batches, BATCH_SIZE, already_cached, N_WORKERS,
    )

    def process_batch(job_tuple, b_num):
        batch, trade_date = job_tuple
        batch_success = 0
        batch_failed = 0
        ok = False

        for attempt in range(3):
            try:
                results = fetch_option_bars_batch(batch, trade_date)
                batch_success = len(results)
                ok = True
                break
            except (PermissionError, ConnectionAbortedError, ConnectionResetError, OSError) as e:
                wait = [15, 30, 60][attempt]
                log.warning("  Connection blocked on batch %d attempt %d, waiting %ds: %s", b_num, attempt + 1, wait, e)
                time.sleep(wait)
            except Exception as e:
                err_str = str(e).lower()
                if any(k in err_str for k in ("permission denied", "connection aborted", "connection reset", "broken pipe")):
                    wait = [15, 30, 60][attempt]
                    time.sleep(wait)
                elif "429" in err_str or "rate" in err_str or "500" in err_str:
                    wait = [2, 5, 10][attempt]
                    time.sleep(wait)
                else:
                    log.warning("  Batch %d failed: %s", b_num, e)
                    break

        if not ok:
            batch_failed = len(batch)

        return batch_success, batch_failed

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
            # Submit all jobs. ThreadPoolExecutor internally queues them and
            # only runs N_WORKERS concurrently — no flooding, no wave starvation.
            future_to_num = {
                executor.submit(process_batch, job, j + 1): j + 1
                for j, job in enumerate(all_jobs)
            }

            for future in concurrent.futures.as_completed(future_to_num):
                b_num = future_to_num[future]
                try:
                    b_ok, b_fail = future.result()
                    with progress_lock:
                        success += b_ok
                        failed += b_fail
                        batch_num += 1

                        if batch_num % 10 == 0 or batch_num == total_batches:
                            elapsed = time.time() - start_time
                            rate = batch_num / elapsed if elapsed > 0 else 0
                            remaining = (total_batches - batch_num) / rate if rate > 0 else 0
                            log.info(
                                "[batch %d / %d] ok=%d fail=%d | %.1f batches/s | ETA: %.0fm",
                                batch_num, total_batches, success, failed, rate, remaining / 60,
                            )
                except Exception as exc:
                    log.warning("Batch %d generated exception: %s", b_num, exc)
                    with progress_lock:
                        batch_num += 1

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        log.info("")
        log.info("Interrupted by user (Ctrl+C). Progress so far:")
        log.info("  Fetched:       %d", success)
        log.info("  Failed/empty:  %d", failed)
        log.info("  Batches done:  %d / %d", batch_num, total_batches)
        log.info("  Time elapsed:  %dm %ds", int(elapsed) // 60, int(elapsed) % 60)
        log.info("Re-run the same command to resume — already-cached files are skipped.")

    return success, failed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pre-download option bars for backtest / optimizer cache",
    )

    # ── Optimizer-config mode (NEW) ────────────────────────────────────────
    parser.add_argument(
        "--optimizer-config",
        metavar="PATH",
        default=None,
        help=(
            "Path to sim_config.yaml used by the optimizer. "
            "Enables optimizer mode: overrides hold_max to the sweep maximum "
            "(240 min), prints walk-forward fold windows, and defaults "
            "--start/--end to %s / %s."
            % (_OPTIMIZER_DEFAULT_START, _OPTIMIZER_DEFAULT_END)
        ),
    )

    # ── Standard args ──────────────────────────────────────────────────────
    parser.add_argument(
        "--start",
        default=None,
        help="Start date YYYY-MM-DD (default: %s in optimizer mode)" % _OPTIMIZER_DEFAULT_START,
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD (default: today in optimizer mode)",
    )
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

    # ── Resolve mode and config path ───────────────────────────────────────
    optimizer_mode = args.optimizer_config is not None

    if optimizer_mode:
        config_path = Path(args.optimizer_config)
        if not config_path.is_absolute():
            config_path = PROJECT_ROOT / config_path
        if not config_path.exists():
            log.error("optimizer-config not found: %s", config_path)
            sys.exit(1)
        start_date = args.start or _OPTIMIZER_DEFAULT_START
        end_date   = args.end   or _OPTIMIZER_DEFAULT_END
    else:
        config_path = SIM_CONFIG_PATH
        if not args.start or not args.end:
            parser.error("--start and --end are required when not using --optimizer-config")
        start_date = args.start
        end_date   = args.end

    t0 = time.time()

    # ── Load profiles ──────────────────────────────────────────────────────
    sim_filter = [s.upper() for s in args.sims] if args.sims else None
    profiles = load_sim_profiles(config_path, sim_filter)
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

    # ── Optimizer mode: show fold windows ──────────────────────────────────
    if optimizer_mode:
        folds = _build_optimizer_folds(start_date, end_date)
        fold_start, fold_end = _union_date_range(folds)

        log.info("")
        log.info("═" * 62)
        log.info("  OPTIMIZER MODE")
        log.info("  Config:         %s", config_path)
        log.info("  Overall window: %s → %s", fold_start, fold_end)
        log.info("  Hold-max used:  %d min (optimizer sweep max)", max(_OPTIMIZER_HOLD_MAX_MINUTES))
        log.info("  Walk-forward folds:")
        for f in folds:
            log.info(
                "    Fold %d: train [%s → %s]  test [%s → %s]",
                f["fold"], f["train_start"], f["train_end"],
                f["test_start"], f["test_end"],
            )
        log.info("═" * 62)
        log.info("")

        # The cache must cover the full union window
        start_date = fold_start
        end_date   = fold_end

    # ── Enumerate all contracts ────────────────────────────────────────────
    log.info("Enumerating contracts for %s -> %s ...", start_date, end_date)
    all_pairs, stock_dfs = enumerate_contracts(
        profiles, start_date, end_date, optimizer_mode=optimizer_mode
    )
    log.info("Total unique (contract, date) pairs: %d", len(all_pairs))

    # ── Check cache ────────────────────────────────────────────────────────
    needs_fetch, already_cached = check_cache(all_pairs)
    log.info("Already cached: %d", already_cached)
    log.info("Need to fetch:  %d", len(needs_fetch))

    if args.dry_run:
        est_minutes = len(needs_fetch) * 0.4 / 60
        log.info("Cache directory: %s", CACHE_DIR)
        log.info("Estimated fetch time: %.0f minutes", est_minutes)
        log.info("Dry run — exiting without fetching.")
        return

    if not needs_fetch:
        log.info("Nothing to fetch — cache is complete!")
        return

    # ── Estimate time ──────────────────────────────────────────────────────
    est_minutes = len(needs_fetch) * 0.4 / 60
    log.info("Cache directory:     %s", CACHE_DIR)
    log.info("Estimated fetch time: ~%.0f minutes", est_minutes)

    # ── Fetch ──────────────────────────────────────────────────────────────
    success, failed = fetch_all(needs_fetch, already_cached)

    elapsed = time.time() - t0
    log.info("=" * 60)
    log.info("Prefetch complete:")
    log.info("  Mode:              %s", "OPTIMIZER" if optimizer_mode else "STANDARD")
    log.info("  Sims scanned:      %d", len(profiles))
    log.info("  Symbols:           %s", ", ".join(sorted(stock_dfs)))
    log.info("  Unique contracts:  %d", len(all_pairs))
    log.info("  Already cached:    %d", already_cached)
    log.info("  Fetched:           %d", success)
    log.info("  Failed/empty:      %d", failed)
    log.info("  Time elapsed:      %dm %ds", elapsed // 60, elapsed % 60)


if __name__ == "__main__":
    main()
