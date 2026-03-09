#!/usr/bin/env python3
"""
Standalone backfill script — run from the repo root:

    python scripts/backfill_candles.py [--days N]

Fetches N calendar days of historical 1m SPY bars from Alpaca and
merges them into data/qqq_1m.csv.  Defaults to 30 days.
"""

import argparse
import sys
import os

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.backfill import run_backfill, backfill_status


def main():
    parser = argparse.ArgumentParser(description="Backfill SPY 1m candles from Alpaca")
    parser.add_argument("--days", type=int, default=30, help="Calendar days to look back (default 30)")
    args = parser.parse_args()

    print(f"Current CSV status: {backfill_status()}")
    print(f"Backfilling {args.days} days…")

    def progress(msg: str):
        print(f"  {msg}", flush=True)

    result = run_backfill(days_back=args.days, progress_cb=progress)
    if result.get("ok"):
        print(
            f"\nDone. Fetched {result['fetched_days']} days, "
            f"added {result['added_rows']} rows, "
            f"total {result['total_rows']} rows "
            f"({result['errors']} day-errors)."
        )
    else:
        print(f"\nFailed: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
