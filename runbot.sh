#!/bin/bash
while true
do
    echo "Starting bot... "
    python -m interface.bot
    echo "Bot crashed. Restarting in 5 seconds... "
    sleep 5
done
