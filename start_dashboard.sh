#!/usr/bin/env bash
# start_dashboard.sh — Launch dashboard + temporary public URL tunnel
#
# Usage:
#   ./start_dashboard.sh              # default port 8090
#   DASHBOARD_PORT=8080 ./start_dashboard.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${DASHBOARD_PORT:-8090}"
DASHBOARD_PID=""
TUNNEL_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    [ -n "$TUNNEL_PID" ] && kill "$TUNNEL_PID" 2>/dev/null && echo "  Tunnel stopped"
    [ -n "$DASHBOARD_PID" ] && kill "$DASHBOARD_PID" 2>/dev/null && echo "  Dashboard stopped"
    exit 0
}
trap cleanup INT TERM

# Activate venv
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
else
    echo "ERROR: venv not found. Run setup first."
    exit 1
fi

# Load .env if present
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# Export port for dashboard
export DASHBOARD_PORT="$PORT"

echo "═══════════════════════════════════════════════════════════"
echo "  QQQBot Dashboard Launcher"
echo "  Port: $PORT"
echo "═══════════════════════════════════════════════════════════"

# Start dashboard in background
echo "Starting dashboard on port $PORT..."
python -m dashboard.app &
DASHBOARD_PID=$!
sleep 3

# Check it started
if ! kill -0 "$DASHBOARD_PID" 2>/dev/null; then
    echo "ERROR: Dashboard failed to start. Check logs."
    exit 1
fi
echo "Dashboard running (PID $DASHBOARD_PID)"
echo "  Local:  http://localhost:$PORT"
echo ""

# Start tunnel
if command -v cloudflared &>/dev/null; then
    echo "Starting Cloudflare tunnel..."
    cloudflared tunnel --url "http://localhost:$PORT" 2>&1 &
    TUNNEL_PID=$!
    # cloudflared prints the URL to stderr after a moment
    sleep 5
    echo ""
    echo "Look above for your *.trycloudflare.com URL"
elif command -v ngrok &>/dev/null; then
    echo "Starting ngrok tunnel..."
    ngrok http "$PORT" --log=stdout &
    TUNNEL_PID=$!
    sleep 3
    # Try to get the URL from ngrok API
    NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['tunnels'][0]['public_url'])" 2>/dev/null || echo "")
    if [ -n "$NGROK_URL" ]; then
        echo ""
        echo "═══════════════════════════════════════════════════════════"
        echo "  PUBLIC URL: $NGROK_URL"
        echo "═══════════════════════════════════════════════════════════"
    else
        echo "ngrok started — check http://localhost:4040 for the URL"
    fi
else
    echo "WARNING: Neither cloudflared nor ngrok found."
    echo "Install cloudflared:"
    echo "  curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared"
    echo "  chmod +x /usr/local/bin/cloudflared"
    echo ""
    echo "Dashboard is running locally at http://localhost:$PORT"
    echo "Press Ctrl+C to stop."
fi

echo ""
echo "Press Ctrl+C to stop dashboard and tunnel."

# Wait for either process to exit
wait
