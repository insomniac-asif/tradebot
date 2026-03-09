#!/bin/bash
CRASH_LOG="logs/crash.log"
mkdir -p logs

while true
do
    echo "Starting bot... "
    python -m interface.bot
    EXIT_CODE=$?
    CRASH_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    echo "$CRASH_TS exit_code=$EXIT_CODE" >> "$CRASH_LOG"
    echo "Bot exited (code $EXIT_CODE) at $CRASH_TS. Restarting in 5 seconds... "
    sleep 5
done
