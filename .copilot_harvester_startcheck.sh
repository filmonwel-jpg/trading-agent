#!/bin/zsh
set -euo pipefail

cd /Users/filmonghezehey/trading-agent
log_file=/tmp/copilot_harvester_startcheck.log
: > "$log_file"

./harvester.py > "$log_file" 2>&1 &
pid=$!

sleep 6

if kill -0 "$pid" 2>/dev/null; then
  echo "STARTCHECK_PID=$pid"
  head -n 80 "$log_file"
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
else
  echo "STARTCHECK_EXITED=$pid"
  cat "$log_file"
fi

