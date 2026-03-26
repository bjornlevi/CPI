#!/usr/bin/env bash
set -euo pipefail

# Resolve project dir (where this script lives)
DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
VENV="$DIR/.venv"
PY="$VENV/bin/python"
MODULE="cpi_app.jobs.fetch_all"
LOG_DIR="$DIR/logs"
LOG_FILE="$LOG_DIR/cpi.log"
LOCK_FILE="/tmp/cpi.lock"

mkdir -p "$LOG_DIR"

# Sanity checks
[ -x "$PY" ] || { echo "ERROR: $PY not found/executable. Create venv first."; exit 1; }
[ -d "$DIR/cpi_app/jobs" ] || { echo "ERROR: $DIR/cpi_app/jobs not found."; exit 1; }

# Run once (no overlap) and log everything
flock -n "$LOCK_FILE" bash -c "
  echo \"--- \$(date -Iseconds) starting $MODULE ---\" >> \"$LOG_FILE\"
  \"$PY\" -m \"$MODULE\" >> \"$LOG_FILE\" 2>&1
  echo \"--- \$(date -Iseconds) finished $MODULE ---\" >> \"$LOG_FILE\"
"
