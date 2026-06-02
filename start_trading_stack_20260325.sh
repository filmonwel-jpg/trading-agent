#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
mkdir -p "$runtime_dir"

run_stamp="$(date '+%Y%m%d_%H%M%S')"
launcher_log="$runtime_dir/morning-launch-$run_stamp.log"
exec >>"$launcher_log" 2>&1

export PATH="/Users/filmonghezehey/miniforge3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1

log() {
  printf '[MORNING][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

is_port_listening() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

start_harvester() {
  local harvester_log="$runtime_dir/harvester-scheduled-$run_stamp.log"
  if pgrep -f "$repo_root/harvester.py" >/dev/null 2>&1; then
    log "harvester already running; skipping start"
    return
  fi
  log "starting harvester.py -> $harvester_log"
  nohup "$repo_root/harvester.py" >>"$harvester_log" 2>&1 &
  log "harvester pid=$!"
}

start_symbol() {
  local symbol="$1"
  local port="$2"
  local symbol_lower
  symbol_lower="$(printf '%s' "$symbol" | tr '[:upper:]' '[:lower:]')"
  local launch_log="$runtime_dir/${symbol_lower}-scheduled-$run_stamp.log"

  if is_port_listening "$port"; then
    log "$symbol already listening on port $port; skipping start"
    return
  fi

  log "starting $symbol via run_symbol.sh --start --tee-db -> $launch_log"
  nohup "$repo_root/run_symbol.sh" "$symbol" --start --tee-db >>"$launch_log" 2>&1 &
  log "$symbol launcher pid=$!"
}

log "morning stack launcher started"
start_harvester
sleep 15

start_symbol TSLA 8081
sleep 10
start_symbol NVDA 8082
sleep 10
start_symbol AMD 8083
sleep 10
start_symbol AMZN 8084

log "morning stack launcher finished scheduling child processes"

