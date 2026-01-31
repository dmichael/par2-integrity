#!/bin/sh
set -e

RUN_MODE="${RUN_MODE:-cron}"
# Derive default schedule from Python config (single source of truth)
CRON_SCHEDULE="${CRON_SCHEDULE:-$(python -c 'from par2integrity.config import Config; print(Config().cron_schedule)')}"

if [ "$RUN_MODE" = "cron" ]; then
    echo "=== PAR2 Integrity: cron mode ==="
    echo "Schedule: $CRON_SCHEDULE"

    # Create crontab entry
    echo "$CRON_SCHEDULE python -m par2integrity.main scan >> /proc/1/fd/1 2>&1" > /etc/crontabs/root

    # Run initial scan on startup
    echo "Running initial scan..."
    python -m par2integrity.main scan || true

    echo "Starting cron daemon..."
    exec crond -f -l 2
elif [ "$RUN_MODE" = "manual" ]; then
    # Pass all arguments through to main.py
    exec python -m par2integrity.main "$@"
else
    echo "Unknown RUN_MODE: $RUN_MODE (expected 'cron' or 'manual')"
    exit 1
fi
