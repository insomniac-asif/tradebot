#!/usr/bin/env python3
"""
Standalone backfill script — run from the repo root:

    python scripts/backfill_candles.py [--days N] [--symbol SPY] [--all]

Fetches N calendar days of historical 1m bars from Alpaca and merges into
the correct per-symbol CSV file.  Defaults to SPY, 30 days.

Examples:
    python scripts/backfill_candles.py                  # SPY, 30 days
    python scripts/backfill_candles.py --days 60        # SPY, 60 days
    python scripts/backfill_candles.py --symbol QQQ     # QQQ, 30 days
    python scripts/backfill_candles.py --all            # all registered symbols, 30 days
    python scripts/backfill_candles.py --all --days 7   # all symbols, 7 days
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.backfill import (
    run_backfill,
    run_backfill_all_symbols,
    backfill_status,
    _load_registered_symbols,
)


def main():
    parser = argparse.ArgumentParser(description="Backfill 1m candles from Alpaca")
    parser.add_argument("--days",   type=int, default=30,  help="Calendar days to look back (default: 30)")
    parser.add_argument("--symbol", type=str, default="all", help="Symbol to backfill (default: all)")
    parser.add_argument("--all",    action="store_true",   help="Backfill ALL registered symbols")
    args = parser.parse_args()

    def progress(msg: str):
        print(f"  {msg}", flush=True)

    if args.all:
        registry = _load_registered_symbols()
        symbols  = list(registry.keys()) if registry else []
        print(f"Backfilling ALL symbols: {', '.join(symbols)}")
        print(f"Days: {args.days}\n")

        result = run_backfill_all_symbols(days_back=args.days, progress_cb=progress)
        print(f"\n{'═'*50}")
        print(f"All-symbol backfill complete.")
        print(f"  Total rows added : {result['total_added']:,}")
        print(f"  Total day-errors : {result['total_errors']}")
        print()
        for sym, r in result["results"].items():
            if r.get("ok"):
                print(f"  {sym:<6} +{r['added_rows']:>5} rows  (total {r['total_rows']:,},  {r['errors']} errors)")
            else:
                print(f"  {sym:<6} FAILED: {r.get('error')}")
    else:
        sym = args.symbol.upper()
        status = backfill_status(sym)
        print(f"Symbol : {sym}")
        print(f"File   : {status['file']}")
        print(f"Current: {status['rows']:,} rows  ({status['earliest'] or 'N/A'} → {status['latest'] or 'N/A'})")
        print(f"Backfilling {args.days} days…\n")

        result = run_backfill(days_back=args.days, symbol=sym, progress_cb=progress)
        if result.get("ok"):
            print(
                f"\nDone. Fetched {result['fetched_days']} days, "
                f"added {result['added_rows']:,} rows, "
                f"total {result['total_rows']:,} rows "
                f"({result['errors']} day-errors)."
            )
        else:
            print(f"\nFailed: {result.get('error')}")
            sys.exit(1)


if __name__ == "__main__":
    main()
