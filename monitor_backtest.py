"""
monitor_backtest.py — Live progress monitor with ETA for backtest/optimizer runs.

Usage (PowerShell):
    python monitor_backtest.py
    python monitor_backtest.py --refresh 5

Reads from:
    backtest/backtest_progress.log   (live output)
    backtest/backtest_status.txt     (one-line status)
    backtest/results/*_summary.json  (completed sim results)
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGFILE = os.path.join(SCRIPT_DIR, "backtest", "backtest_progress.log")
STATUSFILE = os.path.join(SCRIPT_DIR, "backtest", "backtest_status.txt")
RESULTS_DIR = os.path.join(SCRIPT_DIR, "backtest", "results")

C = {
    "green": "\033[92m", "yellow": "\033[93m", "red": "\033[91m",
    "cyan": "\033[96m", "dim": "\033[90m", "bold": "\033[1m",
    "white": "\033[97m", "reset": "\033[0m",
}


def _bar(current, total, width=40):
    if total == 0:
        return "░" * width
    filled = int(width * current / total)
    return f"{C['green']}{'█' * filled}{C['dim']}{'░' * (width - filled)}{C['reset']}"


def _ftime(seconds):
    if seconds is None or seconds < 0:
        return "???"
    h, m, s = int(seconds) // 3600, (int(seconds) % 3600) // 60, int(seconds) % 60
    if h > 0:
        return f"{h}h {m:02d}m"
    if m > 0:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _parse_log():
    """Parse backtest_progress.log into structured state."""
    if not os.path.exists(LOGFILE):
        return None

    with open(LOGFILE, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    lines = content.splitlines()
    if not lines:
        return None

    state = {
        "mode": "backtest",
        "total_sims": 0,
        "sims_list": [],
        "completed": [],      # [(sim_id, seconds, "ok"/"fail")]
        "current_sim": None,
        "current_sim_start": None,
        "optimizer_done": 0,   # runs done within current sim
        "optimizer_total": 0,  # total runs for current sim
        "started_at": None,
        "finished": False,
    }

    for line in lines:
        # Start time
        m = re.match(r"\w+ started: (.+)", line)
        if m:
            for fmt in ["%a, %b %d, %Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                try:
                    state["started_at"] = datetime.strptime(m.group(1).strip(), fmt)
                    break
                except ValueError:
                    pass

        # Sim list
        if line.startswith("Sims:"):
            sims = line.replace("Sims:", "").strip().split()
            state["sims_list"] = sims
            state["total_sims"] = len(sims)

        # Mode detection
        if line.startswith("Flags:"):
            flags = line.lower()
            if "--optimize" in flags:
                state["mode"] = "optimize"
            if "--patterns" in flags:
                state["mode"] += "+patterns"
            if "--growth" in flags:
                state["mode"] += "+growth"

        # Sim start
        m = re.match(r">>> Starting (SIM\d+) at (\d+:\d+:\d+)", line)
        if m:
            state["current_sim"] = m.group(1)
            state["current_sim_start"] = m.group(2)
            state["optimizer_done"] = 0
            state["optimizer_total"] = 0

        # Grid size
        m = re.search(r"= (\d+) engine runs", line)
        if m:
            state["optimizer_total"] = int(m.group(1))

        # Optimizer progress
        m = re.search(r"Progress: (\d+)/(\d+) runs complete", line)
        if m:
            state["optimizer_done"] = int(m.group(1))
            state["optimizer_total"] = int(m.group(2))

        # Sim finish
        m = re.match(r">>> Finished (SIM\d+) at .+ \((\d+)s\)", line)
        if m:
            state["completed"].append((m.group(1), int(m.group(2)), "ok"))

        m = re.match(r">>> FAILED (SIM\d+) at .+ \((\d+)s\)", line)
        if m:
            state["completed"].append((m.group(1), int(m.group(2)), "fail"))

        if "ALL DONE" in line:
            state["finished"] = True
            state["current_sim"] = None

    return state


def _load_sim_result(sim_id):
    """Load a completed sim's result summary."""
    path = os.path.join(RESULTS_DIR, f"{sim_id}_summary.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _load_optimizer_result(sim_id):
    """Load optimizer result for a sim."""
    path = os.path.join(RESULTS_DIR, f"optimizer_{sim_id}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def render(state, refresh):
    os.system("cls" if os.name == "nt" else "clear")

    mode = state["mode"]
    total_sims = state["total_sims"]
    done_count = len(state["completed"])
    current = state["current_sim"]
    is_optimize = "optimize" in mode

    # Header
    print(f"\n{C['bold']}{'═' * 64}")
    print(f"  QQQBot {mode.upper()} Monitor")
    print(f"{'═' * 64}{C['reset']}")

    # Overall progress
    print(f"\n  Sims: {_bar(done_count, total_sims)} {C['bold']}{done_count}/{total_sims}{C['reset']}", end="")
    if current:
        print(f"  {C['cyan']}▸ {current}{C['reset']}", end="")
    elif state["finished"]:
        print(f"  {C['green']}COMPLETE{C['reset']}", end="")
    print()

    # Current sim optimizer progress
    if current and is_optimize and state["optimizer_total"] > 0:
        od = state["optimizer_done"]
        ot = state["optimizer_total"]
        pct = od / ot * 100 if ot > 0 else 0
        print(f"  Runs: {_bar(od, ot, 35)} {C['bold']}{od}/{ot}{C['reset']} ({pct:.0f}%)", end="")

        # ETA for current sim
        if od > 0 and state["current_sim_start"]:
            try:
                now = datetime.now()
                h, m, s = state["current_sim_start"].split(":")
                sim_start = now.replace(hour=int(h), minute=int(m), second=int(s), microsecond=0)
                if sim_start > now:
                    sim_start -= timedelta(days=1)
                elapsed = (now - sim_start).total_seconds()
                rate = od / elapsed
                remaining = (ot - od) / rate if rate > 0 else 0
                print(f"  {C['dim']}~{_ftime(remaining)} left @ {rate:.1f}/s{C['reset']}", end="")
            except (ValueError, IndexError):
                pass
        print()

    # Completed sims table
    if state["completed"]:
        print(f"\n  {C['bold']}{'SIM':<8} {'Time':>8}", end="")
        if is_optimize:
            print(f"  {'Verdict':<20} {'Best TP':>7} {'Best SL':>7} {'OOS Score':>9}", end="")
        else:
            print(f"  {'Trades':>7} {'WR':>6} {'Final $':>9} {'Peak $':>9}", end="")
        print(f"{C['reset']}")
        print(f"  {'─' * 62}")

        for sim_id, secs, status in state["completed"]:
            time_str = _ftime(secs)
            status_color = C["green"] if status == "ok" else C["red"]

            if is_optimize:
                opt = _load_optimizer_result(sim_id)
                if opt:
                    verdict = opt.get("verdict", "?")
                    v_color = C["green"] if verdict == "VIABLE" else (
                        C["yellow"] if verdict == "MARGINAL" else C["red"])
                    top = opt.get("top_10", [{}])
                    if top:
                        p = top[0].get("params", {})
                        score = top[0].get("avg_test_score", 0)
                        print(f"  {sim_id:<8} {status_color}{time_str:>8}{C['reset']}"
                              f"  {v_color}{verdict:<20}{C['reset']}"
                              f"  {p.get('tp', 0):>6.2f}  {p.get('sl', 0):>6.2f}  {score:>9.2f}")
                    else:
                        print(f"  {sim_id:<8} {status_color}{time_str:>8}{C['reset']}"
                              f"  {v_color}{verdict:<20}{C['reset']}")
                else:
                    print(f"  {sim_id:<8} {status_color}{time_str:>8}{C['reset']}"
                          f"  {C['dim']}no result file{C['reset']}")
            else:
                res = _load_sim_result(sim_id)
                if res:
                    trades = sum(r.get("total_trades", 0) for r in res.get("runs", []))
                    wr = res.get("avg_win_rate", 0) * 100
                    runs = res.get("runs", [])
                    final = runs[-1].get("final_balance", 0) if runs else 0
                    peak = max((r.get("peak_balance", 0) for r in runs), default=0)
                    f_color = C["green"] if final >= 3000 else C["red"]
                    print(f"  {sim_id:<8} {status_color}{time_str:>8}{C['reset']}"
                          f"  {trades:>7}  {wr:>5.1f}%"
                          f"  {f_color}${final:>8,.0f}{C['reset']}  ${peak:>8,.0f}")
                else:
                    print(f"  {sim_id:<8} {status_color}{time_str:>8}{C['reset']}"
                          f"  {C['dim']}(loading...){C['reset']}")

    # Remaining sims
    completed_ids = {s[0] for s in state["completed"]}
    remaining = [s for s in state["sims_list"]
                 if s not in completed_ids and s != current]
    if remaining and not state["finished"]:
        show = remaining[:8]
        extra = len(remaining) - len(show)
        print(f"\n  {C['dim']}Queued: {', '.join(show)}"
              f"{'...' + f' (+{extra} more)' if extra > 0 else ''}{C['reset']}")

    # ETA
    if done_count > 0 and not state["finished"]:
        times = [s for _, s, _ in state["completed"]]
        avg_time = sum(times) / len(times)
        remaining_count = total_sims - done_count

        # Account for current sim progress
        current_remaining = 0
        if current and is_optimize and state["optimizer_done"] > 0 and state["optimizer_total"] > 0:
            try:
                now = datetime.now()
                h, m, s = state["current_sim_start"].split(":")
                sim_start = now.replace(hour=int(h), minute=int(m), second=int(s), microsecond=0)
                if sim_start > now:
                    sim_start -= timedelta(days=1)
                elapsed = (now - sim_start).total_seconds()
                rate = state["optimizer_done"] / elapsed
                current_remaining = (state["optimizer_total"] - state["optimizer_done"]) / rate if rate > 0 else 0
                remaining_count -= 1  # don't double-count current
            except (ValueError, IndexError):
                pass

        total_remaining = current_remaining + (remaining_count * avg_time)
        eta = datetime.now() + timedelta(seconds=total_remaining)

        print(f"\n  {C['bold']}Avg/sim: {_ftime(avg_time)} | "
              f"Remaining: ~{_ftime(total_remaining)} | "
              f"ETA: {eta.strftime('%H:%M:%S')}{C['reset']}")

    elif state["finished"]:
        total_time = sum(s for _, s, _ in state["completed"])
        ok = sum(1 for _, _, st in state["completed"] if st == "ok")
        fail = sum(1 for _, _, st in state["completed"] if st == "fail")
        print(f"\n  {C['green']}{C['bold']}ALL COMPLETE{C['reset']} in {_ftime(total_time)}"
              f" — {ok} ok, {fail} failed")

    # Elapsed
    if state["started_at"]:
        elapsed = (datetime.now() - state["started_at"]).total_seconds()
        print(f"  {C['dim']}Elapsed: {_ftime(elapsed)} | "
              f"Updated: {datetime.now().strftime('%H:%M:%S')} | "
              f"Refresh: {refresh}s | Ctrl+C to stop{C['reset']}")

    print()


if __name__ == "__main__":
    refresh = 3
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--refresh" and i < len(sys.argv) - 1:
            try:
                refresh = int(sys.argv[i + 1])
            except ValueError:
                pass

    try:
        while True:
            state = _parse_log()
            if state:
                render(state, refresh)
            else:
                os.system("cls" if os.name == "nt" else "clear")
                print("\n  Waiting for backtest/backtest_progress.log...")
            time.sleep(refresh)
    except KeyboardInterrupt:
        print("\n  Monitor stopped.")
