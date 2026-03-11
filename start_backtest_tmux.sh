#!/usr/bin/env bash
# start_backtest_tmux.sh — Launch a backtest inside a tmux session
#
# Usage:
#   ./start_backtest_tmux.sh --start 2024-06-01 --end 2024-12-31
#   ./start_backtest_tmux.sh --start 2024-01-01 --end 2024-12-31 --sims SIM03 SIM22
#
# The backtest runs inside a tmux session called "backtest".
# You can detach (Ctrl+B, D) and reattach later:
#   tmux attach -t backtest
#
# To use a different module:
#   BACKTEST_MODULE=backtest.optimizer ./start_backtest_tmux.sh --sim SIM03

set -euo pipefail

SESSION="backtest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -eq 0 ]; then
    echo "QQQBot Backtest (tmux launcher)"
    echo ""
    echo "Usage: ./start_backtest_tmux.sh [backtest args...]"
    echo ""
    echo "Examples:"
    echo "  ./start_backtest_tmux.sh --start 2024-06-01 --end 2024-12-31"
    echo "  ./start_backtest_tmux.sh --start 2024-01-01 --end 2024-12-31 --sims SIM03 SIM11"
    echo "  BACKTEST_MODULE=backtest.optimizer ./start_backtest_tmux.sh --all"
    echo "  BACKTEST_MODULE=research.walk_forward ./start_backtest_tmux.sh --sim SIM03 --sweep"
    echo ""
    echo "After launching, detach with Ctrl+B then D."
    echo "Reattach with: tmux attach -t $SESSION"
    exit 0
fi

# Check tmux
if ! command -v tmux &>/dev/null; then
    echo "tmux not found. Install with: sudo apt install tmux"
    exit 1
fi

# Build the command to run inside tmux
MODULE_ENV=""
if [ -n "${BACKTEST_MODULE:-}" ]; then
    MODULE_ENV="BACKTEST_MODULE=$BACKTEST_MODULE "
fi
CMD="${MODULE_ENV}${SCRIPT_DIR}/run_backtest.sh $*; echo ''; echo 'Backtest finished. Press Enter to close.'; read"

# Kill existing session if present
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Create new session and run
tmux new-session -d -s "$SESSION" -c "$SCRIPT_DIR" "$CMD"

echo "Backtest launched in tmux session '$SESSION'"
echo ""
echo "  Attach:  tmux attach -t $SESSION"
echo "  Detach:  Ctrl+B, then D"
echo "  Kill:    tmux kill-session -t $SESSION"
echo ""

# Attach immediately
tmux attach -t "$SESSION"
