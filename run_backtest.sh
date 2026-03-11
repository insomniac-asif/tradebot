#!/usr/bin/env bash
# run_backtest.sh — Standalone backtest runner with logging and memory tracking
#
# Usage:
#   ./run_backtest.sh --start 2024-06-01 --end 2024-12-31
#   ./run_backtest.sh --start 2024-06-01 --end 2024-12-31 --sims SIM03 SIM11
#   ./run_backtest.sh --start 2024-01-01 --end 2024-12-31 --all-sims --adaptive
#
# Available backtest modules:
#   backtest.runner          Main backtest engine (--start, --end, --sims, --symbol, etc.)
#   backtest.optimizer       Parameter optimizer (--sim SIM_ID or --all)
#   research.walk_forward    Walk-forward analysis (--sim SIM_ID, --sweep)
#   research.pattern_pipeline Pattern discovery (--sim SIM_ID)
#
# To use a different module:
#   BACKTEST_MODULE=backtest.optimizer ./run_backtest.sh --sim SIM03
#   BACKTEST_MODULE=research.walk_forward ./run_backtest.sh --sim SIM03 --sweep

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
else
    echo "ERROR: venv not found at $SCRIPT_DIR/venv"
    echo "Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Load .env if present
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Select module (default: backtest.runner)
MODULE="${BACKTEST_MODULE:-backtest.runner}"

# Create logs directory
mkdir -p logs
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOGFILE="logs/backtest_${TIMESTAMP}.log"

echo "═══════════════════════════════════════════════════════════"
echo "  QQQBot Backtest Runner"
echo "  Module:  $MODULE"
echo "  Args:    $*"
echo "  Log:     $LOGFILE"
echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "═══════════════════════════════════════════════════════════"

START_SEC=$SECONDS

# Run with /usr/bin/time for memory tracking (if available)
if command -v /usr/bin/time &>/dev/null; then
    /usr/bin/time -v python -m "$MODULE" "$@" 2>&1 | tee "$LOGFILE"
    # /usr/bin/time output goes to stderr, capture it
    {
        echo ""
        echo "═══════════════════════════════════════════════════════════"
        ELAPSED=$(( SECONDS - START_SEC ))
        printf "  Elapsed: %dh %dm %ds\n" $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60))
        echo "  Log:     $LOGFILE"
        echo "═══════════════════════════════════════════════════════════"
    } | tee -a "$LOGFILE"
else
    python -m "$MODULE" "$@" 2>&1 | tee "$LOGFILE"
    {
        echo ""
        echo "═══════════════════════════════════════════════════════════"
        ELAPSED=$(( SECONDS - START_SEC ))
        printf "  Elapsed: %dh %dm %ds\n" $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60))
        echo "  Log:     $LOGFILE"
        echo "  Note:    Install GNU time for memory stats: sudo apt install time"
        echo "═══════════════════════════════════════════════════════════"
    } | tee -a "$LOGFILE"
fi
