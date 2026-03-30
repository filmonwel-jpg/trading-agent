#!/usr/bin/env bash
set -euo pipefail

repo_root="/Users/filmonghezehey/trading-agent"
runtime_dir="$repo_root/runtime"
state_dir="$runtime_dir/schedule_state"
mkdir -p "$runtime_dir" "$state_dir"

export PATH="/Users/filmonghezehey/miniforge3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1
HARVESTER_PROCESS_PATTERN='harvester\.py'

HTTP_CONNECT_TIMEOUT_SECONDS=2
HTTP_MAX_TIME_SECONDS=6
HTTP_RETRY_COUNT=2

TARGET_TZ="America/Denver"
TARGET_HOUR="14"
TARGET_MINUTE_START=15
TARGET_MINUTE_END=19
state_file="$state_dir/afternoon_flatten_mt_$(TZ="$TARGET_TZ" date '+%Y%m%d').done"
log_file=""

log() {
  printf '[AFTERNOON][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

control_url() {
  local port="$1"
  local endpoint="$2"
  printf 'http://127.0.0.1:%s%s' "$port" "$endpoint"
}

http_get() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    "$url"
}

http_post() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    -X POST \
    "$url"
}

send_notification() {
  local status="$1"
  local title="$2"
  local message="$3"
  local details="${4:-}"
  "$repo_root/send_stack_notification.sh" \
    --event=afternoon-shutdown \
    --status="$status" \
    --title="$title" \
    --message="$message" \
    --details="$details" >/dev/null 2>&1 || true
}

if [[ "${FORCE_RUN:-0}" != "1" ]]; then
  weekday="$(TZ="$TARGET_TZ" date '+%u')"
  hour="$(TZ="$TARGET_TZ" date '+%H')"
  minute="$(TZ="$TARGET_TZ" date '+%M')"
  minute_num=$((10#$minute))

  if [[ "$weekday" -gt 5 ]]; then
    exit 0
  fi

  if [[ "$hour" != "$TARGET_HOUR" || $minute_num -lt $TARGET_MINUTE_START || $minute_num -gt $TARGET_MINUTE_END ]]; then
    exit 0
  fi
fi

if [[ -f "$state_file" && "${FORCE_RUN:-0}" != "1" ]]; then
  exit 0
fi

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  log "dry-run: would flatten and shut down bots + harvester for MT afternoon window"
  exit 0
fi

run_stamp="$(date '+%Y%m%d_%H%M%S')"
log_file="$runtime_dir/afternoon-flatten-$run_stamp.log"
exec >>"$log_file" 2>&1

json_field() {
  local port="$1"
  local field="$2"
  http_get "$(control_url "$port" '/api/control/status')" 2>/dev/null | python3 -c 'import json,sys; data=json.load(sys.stdin); val=data.get(sys.argv[1], data.get("position") if sys.argv[1]=="currentPosition" else ""); print(val)' "$field"
}

post_control() {
  local port="$1"
  local endpoint="$2"
  local url
  url="$(control_url "$port" "$endpoint")"
  local attempt
  for attempt in $(seq 1 "$HTTP_RETRY_COUNT"); do
    if http_post "$url" >/dev/null 2>&1; then
      log "$port $endpoint request succeeded attempt=$attempt"
      return 0
    fi
    log "$port $endpoint request failed attempt=$attempt timeout=${HTTP_MAX_TIME_SECONDS}s"
    sleep 1
  done
  return 1
}

is_port_listening() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

terminate_port_process() {
  local port="$1"
  local label="$2"
  local pid
  pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN | head -n 1 || true)"
  if [[ -z "$pid" ]]; then
    log "$label no listening process found on port $port"
    return 0
  fi
  log "$label sending TERM to pid=$pid on port $port"
  kill "$pid" >/dev/null 2>&1 || true
  for _ in {1..10}; do
    if ! ps -p "$pid" >/dev/null 2>&1; then
      log "$label process pid=$pid exited cleanly"
      return 0
    fi
    sleep 1
  done
  log "$label forcing KILL to pid=$pid"
  kill -9 "$pid" >/dev/null 2>&1 || true
}

stop_harvester() {
  local pids
  pids="$(pgrep -f "$HARVESTER_PROCESS_PATTERN" || true)"
  if [[ -z "$pids" ]]; then
    log "harvester not running"
    return 0
  fi
  log "stopping harvester pid(s): $(echo "$pids" | tr '\n' ' ')"
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    kill "$pid" >/dev/null 2>&1 || true
  done <<< "$pids"

  local remaining=""
  for _ in {1..10}; do
    remaining="$(pgrep -f "$HARVESTER_PROCESS_PATTERN" || true)"
    if [[ -z "$remaining" ]]; then
      log "harvester exited cleanly"
      return 0
    fi
    sleep 1
  done

  log "forcing harvester pid(s): $(echo "$remaining" | tr '\n' ' ')"
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    kill -9 "$pid" >/dev/null 2>&1 || true
  done <<< "$remaining"

  sleep 1
  remaining="$(pgrep -f "$HARVESTER_PROCESS_PATTERN" || true)"
  if [[ -n "$remaining" ]]; then
    log "harvester still running after forced stop pid(s): $(echo "$remaining" | tr '\n' ' ')"
    return 1
  fi

  log "harvester force-stop completed"
  return 0
}

shutdown_ibkr() {
  log "stopping IBKR workstation / gateway as final shutdown step"
  if "$repo_root/ensure_ibkr_workstation.sh" --shutdown; then
    log "IBKR shutdown confirmed"
    return 0
  fi
  log "IBKR shutdown failed; leaving schedule incomplete for retry/manual follow-up"
  return 1
}

flatten_and_stop_symbol() {
  local symbol="$1"
  local port="$2"

  if ! is_port_listening "$port"; then
    log "$symbol not listening on port $port; skipping"
    return 0
  fi

  if ! http_get "$(control_url "$port" '/api/control/status')" >/dev/null 2>&1; then
    log "$symbol port $port is listening but control endpoint is unreachable; leaving process alone"
    return 1
  fi

  local current_position open_orders
  current_position="$(json_field "$port" currentPosition || true)"
  open_orders="$(json_field "$port" openOrders || true)"
  log "$symbol pre-flatten status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"

  if ! post_control "$port" "/api/control/pause"; then
    log "$symbol pause request failed"
  fi

  current_position="$(json_field "$port" currentPosition || true)"
  open_orders="$(json_field "$port" openOrders || true)"
  log "$symbol post-pause status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"

  if [[ "${current_position:-x}" == "0" && "${open_orders:-x}" == "0" ]]; then
    log "$symbol already flat and order-free after pause; enabling kill-switch and stopping process without cancel/flatten"
    post_control "$port" "/api/control/kill-switch/true" || log "$symbol kill-switch request failed"
    terminate_port_process "$port" "$symbol"
    return 0
  fi

  if [[ -n "${open_orders:-}" && "${open_orders:-0}" != "0" ]]; then
    if ! post_control "$port" "/api/control/cancel-open-orders"; then
      log "$symbol cancel-open-orders request failed"
    fi
  else
    log "$symbol has no open orders; skipping cancel-open-orders request"
  fi

  if [[ "${current_position:-0}" != "0" && -n "${current_position:-}" ]]; then
    if ! post_control "$port" "/api/control/flatten"; then
      log "$symbol flatten request failed"
    fi
  else
    log "$symbol already flat or position unavailable; no flatten order sent"
  fi

  local flat_confirmed=0
  for _ in {1..24}; do
    sleep 5
    current_position="$(json_field "$port" currentPosition || true)"
    open_orders="$(json_field "$port" openOrders || true)"
    log "$symbol poll status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"
    if [[ "${current_position:-x}" == "0" && "${open_orders:-x}" == "0" ]]; then
      flat_confirmed=1
      break
    fi
  done

  if [[ $flat_confirmed -eq 1 ]]; then
    log "$symbol flat confirmed; enabling kill-switch then stopping process"
    post_control "$port" "/api/control/kill-switch/true" || log "$symbol kill-switch request failed"
    terminate_port_process "$port" "$symbol"
    return 0
  else
    log "$symbol did not confirm flat in time; leaving process running for manual follow-up"
    return 1
  fi
}

log "afternoon flatten/shutdown started"
any_failure=0
if ! flatten_and_stop_symbol TSLA 8081; then any_failure=1; fi
if ! flatten_and_stop_symbol NVDA 8082; then any_failure=1; fi
if ! flatten_and_stop_symbol AMD 8083; then any_failure=1; fi
if ! flatten_and_stop_symbol AMZN 8084; then any_failure=1; fi
if ! stop_harvester; then any_failure=1; fi
if [[ $any_failure -eq 0 ]]; then
  if ! shutdown_ibkr; then any_failure=1; fi
else
  log "skipping IBKR shutdown because one or more symbols did not flatten cleanly"
fi
log "afternoon flatten/shutdown completed"
if [[ $any_failure -eq 0 ]]; then
  touch "$state_file"
  send_notification "ok" "Trading stack shutdown OK" "Bots flattened, harvester stopped, and IBKR shutdown completed." "Log: $log_file"
else
  log "afternoon flatten/shutdown left one or more symbols unconfirmed; not marking schedule complete so it can retry within the MT window"
  send_notification "fail" "Trading stack shutdown needs attention" "One or more symbols did not flatten or IBKR did not shut down cleanly." "Check $log_file"
  exit 1
fi


