import os
import json
import csv
import argparse
import logging
from datetime import datetime, timedelta
import pytz


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIM_DIR = os.path.join(BASE_DIR, "data", "sims")

REGIME_BUCKETS = {"TREND", "RANGE", "VOLATILE"}
TIME_BUCKETS = {"MORNING", "MIDDAY", "AFTERNOON", "CLOSE"}


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            eastern = pytz.timezone("US/Eastern")
            dt = eastern.localize(dt)
        return dt
    except Exception:
        return None


def _init_group():
    return {
        "total": 0,
        "wins": 0,
        "pnl_sum": 0.0,
        "pnl_vals": [],
        "cum_pnl": 0.0,
        "peak": 0.0,
        "max_dd": 0.0,
    }


def _update_group(group, pnl):
    group["total"] += 1
    group["pnl_sum"] += pnl
    group["pnl_vals"].append(pnl)
    if pnl > 0:
        group["wins"] += 1
    group["cum_pnl"] += pnl
    if group["cum_pnl"] > group["peak"]:
        group["peak"] = group["cum_pnl"]
    dd = group["peak"] - group["cum_pnl"]
    if dd > group["max_dd"]:
        group["max_dd"] = dd


def _compute_stats(group):
    total = group["total"]
    wins = group["wins"]
    win_rate = wins / total if total > 0 else 0.0
    pnl_sum = group["pnl_sum"]
    expectancy = pnl_sum / total if total > 0 else 0.0
    pos = [p for p in group["pnl_vals"] if p > 0]
    neg = [p for p in group["pnl_vals"] if p < 0]
    avg_win = sum(pos) / len(pos) if pos else 0.0
    avg_loss = sum(neg) / len(neg) if neg else 0.0
    return {
        "total": total,
        "win_rate": win_rate,
        "pnl_sum": pnl_sum,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "max_dd": group["max_dd"],
    }


def _format_pct(val):
    return f"{val * 100:.1f}%"


def _format_money(val):
    return f"${val:,.2f}"


def _load_sim_files():
    if not os.path.exists(SIM_DIR):
        return []
    sims = []
    for name in sorted(os.listdir(SIM_DIR)):
        if not name.endswith(".json"):
            continue
        sim_id = os.path.splitext(name)[0]
        sims.append((sim_id, os.path.join(SIM_DIR, name)))
    return sims


def _filter_trade_by_date(trade, start_dt, end_dt):
    ts = trade.get("exit_time") or trade.get("entry_time")
    dt = _parse_iso(ts)
    if dt is None:
        return False
    if start_dt and dt < start_dt:
        return False
    if end_dt and dt > end_dt:
        return False
    return True


def evaluate_sims(start_dt=None, end_dt=None, csv_path=None, plot=False):
    sims = _load_sim_files()
    if not sims:
        print("No sim data files found.")
        return

    regime_stats = {}
    time_stats = {}
    sim_stats = {}
    trades_skipped = 0

    for sim_id, path in sims:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            print(f"Failed to load {sim_id}.json")
            continue

        trade_log = data.get("trade_log", [])
        if not trade_log:
            print(f"No data recorded for {sim_id}")
            continue

        sim_group = _init_group()

        for trade in trade_log:
            if not isinstance(trade, dict):
                continue
            if not _filter_trade_by_date(trade, start_dt, end_dt):
                continue

            regime = trade.get("regime_at_entry")
            bucket = trade.get("time_of_day_bucket")
            if not regime or not bucket:
                logging.warning("sim_trade_missing_meta: %s", sim_id)
                trades_skipped += 1
                continue
            if regime not in REGIME_BUCKETS:
                logging.warning("sim_trade_bad_regime: %s", regime)
                trades_skipped += 1
                continue
            if bucket not in TIME_BUCKETS:
                logging.warning("sim_trade_bad_bucket: %s", bucket)
                trades_skipped += 1
                continue

            pnl_val = trade.get("realized_pnl_dollars")
            try:
                pnl = float(pnl_val) if pnl_val is not None else None
            except (TypeError, ValueError):
                pnl = None
            if pnl is None:
                trades_skipped += 1
                continue

            sim_group_key = sim_id
            if sim_group_key not in sim_stats:
                sim_stats[sim_group_key] = _init_group()
            _update_group(sim_stats[sim_group_key], pnl)

            if regime not in regime_stats:
                regime_stats[regime] = _init_group()
            _update_group(regime_stats[regime], pnl)

            if bucket not in time_stats:
                time_stats[bucket] = _init_group()
            _update_group(time_stats[bucket], pnl)

    print("\n=== Regime Breakdown ===")
    for regime in sorted(regime_stats.keys()):
        stats = _compute_stats(regime_stats[regime])
        print(
            f"{regime:<8} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | "
            f"Avg Win: {_format_money(stats['avg_win'])} | Avg Loss: {_format_money(stats['avg_loss'])} | "
            f"Exp: {_format_money(stats['expectancy'])} | MaxDD: {_format_money(stats['max_dd'])}"
        )

    print("\n=== Time Bucket Breakdown ===")
    for bucket in ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE"]:
        if bucket not in time_stats:
            continue
        stats = _compute_stats(time_stats[bucket])
        print(
            f"{bucket:<9} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | "
            f"Avg Win: {_format_money(stats['avg_win'])} | Avg Loss: {_format_money(stats['avg_loss'])} | "
            f"Exp: {_format_money(stats['expectancy'])} | MaxDD: {_format_money(stats['max_dd'])}"
        )

    print("\n=== Per-Sim Summary ===")
    for sim_id in sorted(sim_stats.keys()):
        stats = _compute_stats(sim_stats[sim_id])
        print(
            f"{sim_id:<5} | Trades: {stats['total']:<4} | WR: {_format_pct(stats['win_rate'])} | "
            f"PnL: {_format_money(stats['pnl_sum'])} | MaxDD: {_format_money(stats['max_dd'])} | "
            f"Exp: {_format_money(stats['expectancy'])}"
        )

    if trades_skipped:
        print(f"\nSkipped trades (missing/invalid metadata or PnL): {trades_skipped}")

    if csv_path:
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["category_type", "category", "trades", "win_rate", "pnl", "avg_win", "avg_loss", "expectancy", "max_drawdown"])
                for regime, group in regime_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "regime", regime, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
                for bucket, group in time_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "time_bucket", bucket, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
                for sim_id, group in sim_stats.items():
                    stats = _compute_stats(group)
                    writer.writerow([
                        "sim", sim_id, stats["total"], stats["win_rate"],
                        stats["pnl_sum"], stats["avg_win"], stats["avg_loss"],
                        stats["expectancy"], stats["max_dd"]
                    ])
            print(f"\nCSV exported to {csv_path}")
        except Exception:
            logging.exception("sim_evaluation_csv_export_failed")

    if plot:
        try:
            import matplotlib.pyplot as plt
            regimes = sorted(regime_stats.keys())
            regime_pnls = [_compute_stats(regime_stats[r])["pnl_sum"] for r in regimes]
            buckets = [b for b in ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE"] if b in time_stats]
            bucket_pnls = [_compute_stats(time_stats[b])["pnl_sum"] for b in buckets]

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            axes[0].bar(regimes, regime_pnls)
            axes[0].set_title("PnL by Regime")
            axes[0].set_ylabel("PnL ($)")
            axes[1].bar(buckets, bucket_pnls)
            axes[1].set_title("PnL by Time Bucket")
            axes[1].set_ylabel("PnL ($)")
            plt.tight_layout()
            plt.show()
        except Exception:
            logging.exception("sim_evaluation_plot_failed")


def main():
    parser = argparse.ArgumentParser(description="Sim performance evaluation by regime and time bucket.")
    parser.add_argument("--since-days", type=int, default=None)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--csv", type=str, default=None)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()

    eastern = pytz.timezone("US/Eastern")
    start_dt = None
    end_dt = None
    if args.since_days is not None:
        end_dt = datetime.now(eastern)
        start_dt = end_dt - timedelta(days=int(args.since_days))
    if args.start_date:
        try:
            start_dt = eastern.localize(datetime.fromisoformat(args.start_date))
        except Exception:
            start_dt = None
    if args.end_date:
        try:
            end_dt = eastern.localize(datetime.fromisoformat(args.end_date))
        except Exception:
            end_dt = None

    evaluate_sims(start_dt=start_dt, end_dt=end_dt, csv_path=args.csv, plot=args.plot)


if __name__ == "__main__":
    main()
