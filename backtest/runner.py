"""
backtest/runner.py
CLI runner for the backtesting engine.

Usage examples:
  # Run all sims for a date range
  python -m backtest.runner --start 2024-01-01 --end 2024-12-31

  # Run specific sims
  python -m backtest.runner --sims SIM03 SIM11 --start 2024-06-01 --end 2024-12-31

  # Run a single sim, single symbol override
  python -m backtest.runner --sims SIM03 --symbol SPY --start 2024-01-01 --end 2024-12-31

  # Run with custom max_runs and be verbose
  python -m backtest.runner --sims SIM03 --max-runs 100 --start 2024-01-01 --end 2024-12-31 --verbose

  # Pre-fetch all required stock data first (useful for large date ranges)
  python -m backtest.runner --prefetch --start 2024-01-01 --end 2024-12-31

Options:
  --start         Start date (YYYY-MM-DD), required
  --end           End date (YYYY-MM-DD), required
  --sims          Sim IDs to run (e.g. SIM03 SIM11). Default: all sims.
  --symbol        Override symbol (default: use profile's first symbol)
  --max-runs      Maximum runs per sim (default: 50)
  --prefetch      Pre-download all stock data before running
  --no-verbose    Suppress per-bar progress output
"""
from __future__ import annotations
import argparse
import os
import sys
import time

# Ensure project root is in sys.path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _load_profiles() -> dict:
    """Load sim profiles from sim_config.yaml, skipping _global and non-SIM keys."""
    import yaml
    cfg_path = os.path.join(_PROJECT_ROOT, "simulation", "sim_config.yaml")
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    profiles = {}
    for key, val in cfg.items():
        if str(key).startswith("_"):
            continue
        if not isinstance(val, dict):
            continue
        k = str(key).upper()
        if k.startswith("SIM") and k[3:].isdigit():
            profiles[k] = val
    return profiles


def _get_symbols_for_profile(profile: dict) -> list:
    """Return the list of tradeable symbols for a profile."""
    symbols_raw = profile.get("symbols")
    if symbols_raw and isinstance(symbols_raw, list):
        return [str(s).upper() for s in symbols_raw]
    if profile.get("symbol"):
        return [str(profile["symbol"]).upper()]
    if profile.get("underlying"):
        return [str(profile["underlying"]).upper()]
    return ["SPY"]


def main():
    parser = argparse.ArgumentParser(description="QQQBot Historical Backtest Runner")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--sims", nargs="*", help="Sim IDs to run (default: all)")
    parser.add_argument("--symbol", help="Override symbol (default: profile's first symbol)")
    parser.add_argument("--max-runs", type=int, default=0, help="Max runs per sim (0 = unlimited)")
    parser.add_argument("--prefetch", action="store_true", help="Pre-download stock data first")
    parser.add_argument("--no-verbose", action="store_true", help="Suppress verbose output")
    parser.add_argument("--skip-live", action="store_true", default=True,
                        help="Skip SIM00 (live execution sim). Default: True.")
    parser.add_argument("--adaptive", action="store_true",
                        help="Enable adaptive optimization (learn from each run)")
    args = parser.parse_args()

    verbose = not args.no_verbose
    start_date = args.start
    end_date = args.end

    mode_str = "ADAPTIVE" if args.adaptive else "STATIC"
    print(f"QQQBot Backtest Runner ({mode_str})")
    print(f"  Period: {start_date} -> {end_date}")
    print(f"  Max runs per sim: {'unlimited' if args.max_runs == 0 else args.max_runs}")
    if args.adaptive:
        print(f"  Adaptive mode: ON — filters evolve between runs")

    # Load profiles
    try:
        all_profiles = _load_profiles()
    except Exception as e:
        print(f"ERROR loading sim_config.yaml: {e}")
        sys.exit(1)

    # Filter to requested sims
    if args.sims:
        requested = [s.upper() for s in args.sims]
        profiles = {k: v for k, v in all_profiles.items() if k in requested}
        missing = [s for s in requested if s not in profiles]
        if missing:
            print(f"WARNING: Sims not found in config: {missing}")
    else:
        profiles = all_profiles

    # Skip live sim by default
    if args.skip_live and "SIM00" in profiles:
        print("  Skipping SIM00 (live sim). Use --sims to include it explicitly.")
        del profiles["SIM00"]

    if not profiles:
        print("No sims to run.")
        sys.exit(0)

    print(f"  Running {len(profiles)} sims: {list(profiles.keys())}")

    # Pre-fetch stock data if requested
    if args.prefetch:
        from backtest.data_fetcher import prefetch_stock_data
        # Collect all unique symbols
        all_symbols = set()
        for pid, prof in profiles.items():
            syms = _get_symbols_for_profile(prof)
            all_symbols.update(syms[:3])  # Only fetch first 3 per sim to limit API calls
        if args.symbol:
            all_symbols = {args.symbol.upper()}
        print(f"\nPre-fetching stock data for: {sorted(all_symbols)}")
        prefetch_stock_data(sorted(all_symbols), start_date, end_date)
        print("Pre-fetch complete.\n")

    from backtest.engine import BacktestEngine
    from backtest.save_results import save_sim_summary, save_dashboard_data

    all_summaries = []
    total_start = time.time()

    for sim_id, profile in profiles.items():
        t0 = time.time()
        print(f"\n{'=' * 60}")
        print(f"Running {sim_id}: {profile.get('name', sim_id)}")
        print(f"  Signal mode: {profile.get('signal_mode', 'TREND_PULLBACK')}")

        # Apply symbol override if specified
        if args.symbol:
            profile = dict(profile)
            profile["symbols"] = [args.symbol.upper()]

        try:
            engine = BacktestEngine(
                profile_id=sim_id,
                profile=profile,
                start_date=start_date,
                end_date=end_date,
                max_runs=args.max_runs,
                verbose=verbose,
                adaptive=args.adaptive,
            )
            summary = engine.run()
            all_summaries.append(summary)

            # Save per-sim file
            sim_path = save_sim_summary(summary)
            elapsed = time.time() - t0

            print(f"\n  Results for {sim_id}:")
            print(f"    Runs completed: {summary.total_runs}")
            print(f"    Blown accounts: {summary.blown_count}")
            print(f"    Target hits:    {summary.target_hit_count}")
            print(f"    Avg win rate:   {summary.avg_win_rate * 100:.1f}%")
            print(f"    Avg drawdown:   {summary.avg_max_drawdown * 100:.1f}%")
            print(f"    Saved to:       {sim_path}")
            print(f"    Time:           {elapsed:.1f}s")

            if args.adaptive and engine.adapt_filters:
                af = engine.adapt_filters
                dn = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
                print(f"    Adaptive gen:   {af.generation} | Best peak: ${af.best_run_peak:.0f}")
                if af.blocked_hours: print(f"    Blocked hours:  {sorted(af.blocked_hours)}")
                if af.allowed_hours is not None: print(f"    Allowed hours:  {sorted(af.allowed_hours)}")
                if af.blocked_days: print(f"    Blocked days:   {[dn.get(d,d) for d in sorted(af.blocked_days)]}")
                if af.allowed_days is not None: print(f"    Allowed days:   {[dn.get(d,d) for d in sorted(af.allowed_days)]}")
                if af.blocked_direction: print(f"    Blocked dir:    {af.blocked_direction}")
                if af.required_direction: print(f"    Required dir:   {af.required_direction}")
                if af.max_hold_seconds: print(f"    Max hold:       {af.max_hold_seconds}s ({af.max_hold_seconds//60}min)")

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception as e:
            print(f"  ERROR running {sim_id}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Save combined dashboard data
    if all_summaries:
        try:
            dashboard_path = save_dashboard_data(all_summaries)
            print(f"\n{'=' * 60}")
            print(f"Dashboard data saved to: {dashboard_path}")
            print(f"View on dashboard (Backtest tab)")
        except Exception as e:
            print(f"ERROR saving dashboard data: {e}")
            import traceback
            traceback.print_exc()

    total_elapsed = time.time() - total_start
    print(f"\nTotal backtest time: {total_elapsed:.1f}s")
    print("Done.")


if __name__ == "__main__":
    main()
