#!/usr/bin/env bash
# run_backtest.sh — Run backtests/optimizer on sims with progress tracking
#
# Monitor progress anytime (PowerShell):
#   Get-Content backtest/backtest_progress.log -Tail 30 -Wait
#   Get-Content backtest/backtest_status.txt
#
# Monitor progress (bash):
#   tail -f backtest/backtest_progress.log
#   cat backtest/backtest_status.txt

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Activate venv
if [ -f venv/Scripts/activate ]; then
    source venv/Scripts/activate
elif [ -f venv/bin/activate ]; then
    source venv/bin/activate
fi

LOGFILE="backtest/backtest_progress.log"
STATUSFILE="backtest/backtest_status.txt"

# Defaults
SIMS=(SIM01 SIM02 SIM03 SIM04 SIM05 SIM06 SIM07 SIM08 SIM09 SIM10 SIM12 SIM13 SIM14 SIM15 SIM16 SIM17 SIM18 SIM19 SIM20 SIM21 SIM22 SIM23 SIM24 SIM25 SIM26 SIM27 SIM28 SIM32 SIM33 SIM34 SIM35 SIM36 SIM40 SIM41 SIM42 SIM43)
START="2024-01-01"
END="2024-12-31"
EXTRA_FLAGS=""

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sims)
            shift; SIMS=()
            while [[ $# -gt 0 && "${1:-}" != "--"* ]]; do
                SIMS+=("$1"); shift
            done;;
        --start) START="$2"; shift 2;;
        --end)   END="$2"; shift 2;;
        *)       EXTRA_FLAGS="$EXTRA_FLAGS $1"; shift;;
    esac
done

SIM_COUNT=${#SIMS[@]}
MODE="backtest"
[[ "$EXTRA_FLAGS" == *"--optimize"* ]] && MODE="optimize"

echo "$MODE started: $(date)" > "$LOGFILE"
echo "Sims: ${SIMS[*]}" >> "$LOGFILE"
echo "Period: $START -> $END" >> "$LOGFILE"
echo "Flags: $EXTRA_FLAGS" >> "$LOGFILE"
echo "========================================" >> "$LOGFILE"

echo "RUNNING | 0/$SIM_COUNT complete | Started $(date '+%H:%M:%S') | $MODE" > "$STATUSFILE"

DONE=0
FAILED=0
START_SEC=$SECONDS

for SIM in "${SIMS[@]}"; do
    SIM_START=$SECONDS
    echo "" >> "$LOGFILE"
    echo ">>> Starting $SIM at $(date '+%H:%M:%S') ($DONE/$SIM_COUNT done)" >> "$LOGFILE"
    echo "RUNNING $SIM | $DONE/$SIM_COUNT complete | Started $(date '+%H:%M:%S') | $MODE" > "$STATUSFILE"

    if PYTHONUNBUFFERED=1 python -m backtest.runner --sims "$SIM" --start "$START" --end "$END" $EXTRA_FLAGS 2>&1 >> "$LOGFILE"; then
        SIM_ELAPSED=$(( SECONDS - SIM_START ))
        DONE=$((DONE + 1))
        echo ">>> Finished $SIM at $(date '+%H:%M:%S') (${SIM_ELAPSED}s) — $DONE/$SIM_COUNT done" >> "$LOGFILE"
        echo "LAST DONE: $SIM (${SIM_ELAPSED}s) | $DONE/$SIM_COUNT complete | $(date '+%H:%M:%S') | $MODE" > "$STATUSFILE"
    else
        SIM_ELAPSED=$(( SECONDS - SIM_START ))
        DONE=$((DONE + 1))
        FAILED=$((FAILED + 1))
        echo ">>> FAILED $SIM at $(date '+%H:%M:%S') (${SIM_ELAPSED}s) — $DONE/$SIM_COUNT done" >> "$LOGFILE"
        echo "LAST FAILED: $SIM (${SIM_ELAPSED}s) | $DONE/$SIM_COUNT complete | $(date '+%H:%M:%S') | $MODE" > "$STATUSFILE"
    fi
done

TOTAL_ELAPSED=$(( SECONDS - START_SEC ))
HOURS=$((TOTAL_ELAPSED / 3600))
MINS=$(( (TOTAL_ELAPSED % 3600) / 60 ))
SECS=$((TOTAL_ELAPSED % 60))

echo "" >> "$LOGFILE"
echo "========================================" >> "$LOGFILE"
echo "ALL DONE at $(date) — ${HOURS}h ${MINS}m ${SECS}s total, $FAILED failures" >> "$LOGFILE"
echo "COMPLETE | $SIM_COUNT/$SIM_COUNT | ${HOURS}h ${MINS}m ${SECS}s | $FAILED failures | $(date '+%H:%M:%S') | $MODE" > "$STATUSFILE"
