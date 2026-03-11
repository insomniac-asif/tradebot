#!/usr/bin/env bash
# sync_from_laptop.sh — Pull latest code and optionally rsync data files
#
# Usage:
#   ./sync_from_laptop.sh                      # git pull only
#   ./sync_from_laptop.sh user@laptop-ip       # git pull + rsync data/ from laptop
#   ./sync_from_laptop.sh user@192.168.1.50    # same with explicit IP

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "═══════════════════════════════════════════════════════════"
echo "  QQQBot Sync"
echo "═══════════════════════════════════════════════════════════"

# Step 1: git pull
echo ""
echo "→ Pulling latest code from git..."
git pull --ff-only origin main
echo "  Done."

# Step 2: rsync data if remote host provided
if [ $# -ge 1 ]; then
    REMOTE="$1"
    REMOTE_PATH="${2:-~/qqqbot}"

    echo ""
    echo "→ Syncing data/ from $REMOTE:$REMOTE_PATH/data/ ..."
    rsync -avz --progress \
        --include='*.csv' \
        --include='*.json' \
        --include='*/' \
        --exclude='*' \
        "$REMOTE:$REMOTE_PATH/data/" "$SCRIPT_DIR/data/"
    echo "  Done."

    # Also sync research patterns if they exist
    echo ""
    echo "→ Syncing research/patterns/ (if exists)..."
    rsync -avz --progress \
        "$REMOTE:$REMOTE_PATH/research/patterns/" "$SCRIPT_DIR/research/patterns/" 2>/dev/null || echo "  No research/patterns/ on remote (skipped)"

    # Sync sim state files
    echo ""
    echo "→ Syncing data/sims/ ..."
    rsync -avz --progress \
        "$REMOTE:$REMOTE_PATH/data/sims/" "$SCRIPT_DIR/data/sims/" 2>/dev/null || echo "  No data/sims/ on remote (skipped)"
else
    echo ""
    echo "Tip: To also sync data files from your laptop:"
    echo "  ./sync_from_laptop.sh user@laptop-ip"
    echo "  ./sync_from_laptop.sh user@laptop-ip ~/path/to/qqqbot"
fi

echo ""
echo "Sync complete."
