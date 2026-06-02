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
HARVESTER_PROCESS_PATTERN='harvester\.py'

log() {
  printf '[MORNING][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

send_notification() {
  local status="$1"
  local title="$2"
  local message="$3"
  local details="${4:-}"
  "$repo_root/send_stack_notification.sh" \
    --event=morning-startup \
    --status="$status" \
    --title="$title" \
    --message="$message" \
    --details="$details" >/dev/null 2>&1 || true
}

ensure_ibkr_ready() {
  log "ensuring IBKR workstation / gateway API is reachable before starting services"
  if "$repo_root/ensure_ibkr_workstation.sh"; then
    log "IBKR preflight passed"
    return 0
  fi
  log "IBKR preflight failed; aborting morning launch before starting harvester or symbol bots"
  send_notification "fail" "Trading stack startup blocked" "IBKR preflight failed before morning startup." "Check $launcher_log"
  return 1
}

is_port_listening() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

start_harvester() {
  local harvester_log="$runtime_dir/harvester-scheduled-$run_stamp.log"
  if pgrep -f "$HARVESTER_PROCESS_PATTERN" >/dev/null 2>&1; then
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

start_generated_bots() {
  log "starting generated Databento bot fleet via start_all_databento_bots.sh"
  "$repo_root/start_all_databento_bots.sh" --start --tee-db --stagger-seconds=10 --skip-ibkr-preflight
}

log "weekday trading stack launcher started"
ensure_ibkr_ready
start_harvester
sleep 15

start_generated_bots

sleep 20
log "running post-start validation via check_morning_stack.py"
if "$repo_root/check_morning_stack.py"; then
  send_notification "ok" "Trading stack startup OK" "Harvester, symbol bots, and DB validation passed." "Log: $launcher_log"
else
  send_notification "fail" "Trading stack startup needs attention" "Morning launcher finished, but post-start validation did not pass." "Run ./check_morning_stack.py and inspect $launcher_log"
  log "weekday trading stack launcher finished with validation failure"
  exit 1
fi

log "weekday trading stack launcher finished successfully"

