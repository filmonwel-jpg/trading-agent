#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start_shared_ibkr_gateway.sh [--host=HOST] [--port=PORT] [--python-bin=PATH] [--dry-run] [--foreground] [--force-restart] [--wait-timeout-seconds=N]

Behavior:
  - Starts the shared IBKR gateway JSON socket server used by bulk-launched symbol bots.
  - Uses runtime/databento.env and databento_ibkr_bridge/.env when present.
  - Writes a worktree-owned pid file to runtime/databento/state/shared-ibkr-gateway.pid.
  - Writes combined stdout/stderr to runtime/databento/logs/shared-ibkr-gateway.log.
  - If the configured host/port already serves the expected JSON protocol and reports connected=true, the script exits successfully without taking ownership of that listener.
  - A bare TCP listener is not treated as healthy; startup waits for a real protocol ping+snapshot handshake.

Examples:
  ./start_shared_ibkr_gateway.sh
  ./start_shared_ibkr_gateway.sh --dry-run
  ./start_shared_ibkr_gateway.sh --host=127.0.0.1 --port=9910
  ./start_shared_ibkr_gateway.sh --foreground
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
bridge_root="$repo_root/databento_ibkr_bridge"
bridge_runner="$bridge_root/src/databento_ibkr_bridge/bridge_runner.py"
gateway_probe_script="$bridge_root/src/databento_ibkr_bridge/shared_ibkr_gateway_health.py"
runtime_env_file="$repo_root/runtime/databento.env"
bridge_env_file="$bridge_root/.env"
state_dir="$repo_root/runtime/databento/state"
log_dir="$repo_root/runtime/databento/logs"
pid_file="$state_dir/shared-ibkr-gateway.pid"
log_file="$log_dir/shared-ibkr-gateway.log"
process_pattern='bridge_runner\.py --shared-ibkr-gateway-server'

host="${IBKR_SHARED_GATEWAY_HOST:-127.0.0.1}"
port="${IBKR_SHARED_GATEWAY_PORT:-9910}"
python_bin="${IBKR_SHARED_GATEWAY_PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-}}"
dry_run=0
foreground=0
force_restart=0
wait_timeout_seconds=15
probe_connect_timeout_seconds="3.000"
probe_ack_timeout_seconds="5.000"

log() {
  printf '[SHARED-IBKR-GATEWAY][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

trim_spaces() {
  printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
}

millis_to_seconds() {
  local millis="$1"
  local fallback="$2"
  case "$millis" in
    ''|*[!0-9]*)
      printf '%s' "$fallback"
      ;;
    *)
      awk -v ms="$millis" 'BEGIN { printf "%.3f", ((ms + 0.0) / 1000.0) }'
      ;;
  esac
}

load_env_file() {
  local path="$1"
  local overwrite="$2"
  local line key value
  [[ -f "$path" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(trim_spaces "$line")"
    [[ -n "$line" ]] || continue
    if [[ "${line#\#}" != "$line" ]]; then
      continue
    fi
    [[ "$line" == *=* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    key="$(trim_spaces "$key")"
    value="$(trim_spaces "$value")"
    value="${value#\"}"
    value="${value%\"}"
    value="${value#\'}"
    value="${value%\'}"
    [[ -n "$key" ]] || continue
    if [[ "$overwrite" -eq 1 || -z "${!key-}" ]]; then
      export "$key=$value"
    fi
  done < "$path"
}

resolve_python_bin() {
  local candidate="$1"
  if [[ -z "$candidate" ]]; then
    return 1
  fi
  if [[ -x "$candidate" ]]; then
    printf '%s' "$candidate"
    return 0
  fi
  candidate="$(command -v "$candidate" 2>/dev/null || true)"
  if [[ -n "$candidate" && -x "$candidate" ]]; then
    printf '%s' "$candidate"
    return 0
  fi
  return 1
}

python_can_run_bridge() {
  local candidate="$1"
  BRIDGE_SRC="$bridge_root/src" "$candidate" -c 'import os, sys; sys.path.insert(0, os.environ["BRIDGE_SRC"]); import databento_ibkr_bridge.bridge_runner' >/dev/null 2>&1
}

choose_python_bin() {
  local resolved
  local -a candidates=()
  if [[ -n "$python_bin" ]]; then
    candidates+=("$python_bin")
  fi
  candidates+=(
    "$bridge_root/.venv/bin/python"
    "$HOME/miniforge3/bin/python3"
    python3
    /opt/homebrew/bin/python3
    /usr/local/bin/python3
    /usr/bin/python3
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    resolved="$(resolve_python_bin "$candidate" || true)"
    if [[ -n "$resolved" ]] && python_can_run_bridge "$resolved"; then
      printf '%s' "$resolved"
      return 0
    fi
  done
  return 1
}

pid_is_alive() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  ps -p "$pid" >/dev/null 2>&1
}

port_reachable() {
  local check_host="$1"
  local check_port="$2"
  nc -z "$check_host" "$check_port" >/dev/null 2>&1
}

gateway_protocol_healthy() {
  [[ -n "${python_bin:-}" ]] || return 1
  [[ -f "$gateway_probe_script" ]] || return 1
  "$python_bin" "$gateway_probe_script" \
    --host="$host" \
    --port="$port" \
    --connect-timeout-seconds="$probe_connect_timeout_seconds" \
    --ack-timeout-seconds="$probe_ack_timeout_seconds" \
    --quiet >/dev/null 2>&1
}

read_pid_file() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file" 2>/dev/null || true
  fi
}

stop_owned_pid_if_requested() {
  local existing_pid="$1"
  [[ -n "$existing_pid" ]] || return 0
  if [[ $force_restart -eq 0 ]]; then
    return 0
  fi
  if ! pid_is_alive "$existing_pid"; then
    rm -f "$pid_file"
    return 0
  fi
  log "force-restart stopping existing owned gateway pid=$existing_pid"
  kill -TERM "$existing_pid" >/dev/null 2>&1 || true
  sleep 1
  if pid_is_alive "$existing_pid"; then
    kill -KILL "$existing_pid" >/dev/null 2>&1 || true
    sleep 1
  fi
  rm -f "$pid_file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --host=*)
      host="${1#--host=}"
      ;;
    --port=*)
      port="${1#--port=}"
      ;;
    --python-bin=*)
      python_bin="${1#--python-bin=}"
      ;;
    --dry-run)
      dry_run=1
      ;;
    --foreground)
      foreground=1
      ;;
    --force-restart)
      force_restart=1
      ;;
    --wait-timeout-seconds=*)
      wait_timeout_seconds="${1#--wait-timeout-seconds=}"
      ;;
    *)
      echo "[SHARED-IBKR-GATEWAY][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

case "$port" in
  ''|*[!0-9]*)
    echo "[SHARED-IBKR-GATEWAY][ERROR] --port must be a whole number." >&2
    exit 1
    ;;
esac

case "$wait_timeout_seconds" in
  ''|*[!0-9]*)
    echo "[SHARED-IBKR-GATEWAY][ERROR] --wait-timeout-seconds must be a whole number." >&2
    exit 1
    ;;
esac

if [[ ! -f "$bridge_runner" ]]; then
  echo "[SHARED-IBKR-GATEWAY][ERROR] Missing bridge runner: $bridge_runner" >&2
  exit 1
fi

if [[ ! -f "$gateway_probe_script" ]]; then
  echo "[SHARED-IBKR-GATEWAY][ERROR] Missing gateway protocol probe: $gateway_probe_script" >&2
  exit 1
fi

mkdir -p "$state_dir" "$log_dir"

load_env_file "$runtime_env_file" 0
load_env_file "$bridge_env_file" 1

probe_connect_timeout_seconds="$(millis_to_seconds "${IBKR_SHARED_GATEWAY_CONNECT_TIMEOUT_MS:-3000}" "3.000")"
probe_ack_timeout_seconds="$(millis_to_seconds "${IBKR_SHARED_GATEWAY_ACK_TIMEOUT_MS:-5000}" "5.000")"

export SHARED_IBKR_GATEWAY_HOST="$host"
export SHARED_IBKR_GATEWAY_PORT="$port"
export BRIDGE_MODE="shared_ibkr_gateway_server"
if [[ $dry_run -eq 1 ]]; then
  export DRY_RUN=true
else
  export DRY_RUN=false
fi

python_bin="$(choose_python_bin || true)"
if [[ -z "$python_bin" ]]; then
  echo "[SHARED-IBKR-GATEWAY][ERROR] Could not find a Python interpreter that can import databento_ibkr_bridge.bridge_runner." >&2
  echo "[SHARED-IBKR-GATEWAY][ERROR] Try creating databento_ibkr_bridge/.venv or pass --python-bin=/path/to/python3." >&2
  exit 1
fi

existing_pid="$(read_pid_file)"
stop_owned_pid_if_requested "$existing_pid"
existing_pid="$(read_pid_file)"
if [[ -n "$existing_pid" ]]; then
  if ! pid_is_alive "$existing_pid"; then
    log "removing stale pid file pid=$existing_pid"
    rm -f "$pid_file"
    existing_pid=""
  fi
fi

if gateway_protocol_healthy; then
  if [[ -n "$existing_pid" ]]; then
    if pid_is_alive "$existing_pid"; then
      log "gateway already protocol-healthy pid=$existing_pid host=$host port=$port"
    else
      log "gateway already protocol-healthy at $host:$port; leaving external listener untouched"
    fi
  else
    log "gateway already protocol-healthy at $host:$port; leaving external listener untouched"
  fi
  exit 0
fi

if port_reachable "$host" "$port"; then
  if [[ -n "$existing_pid" ]] && pid_is_alive "$existing_pid"; then
    echo "[SHARED-IBKR-GATEWAY][ERROR] Existing owned gateway pid=$existing_pid is listening on $host:$port but the JSON protocol health probe failed." >&2
    echo "[SHARED-IBKR-GATEWAY][ERROR] Check $log_file or re-run with --force-restart after investigating the blocking stage." >&2
  else
    echo "[SHARED-IBKR-GATEWAY][ERROR] Port $host:$port is occupied by a listener that does not pass the shared IBKR gateway protocol health probe." >&2
    echo "[SHARED-IBKR-GATEWAY][ERROR] Free the port or inspect the existing listener before retrying." >&2
  fi
  exit 1
fi

cmd=("$python_bin" "$bridge_runner" --shared-ibkr-gateway-server)
if [[ $dry_run -eq 1 ]]; then
  cmd+=(--dry-run)
fi

log "python_bin=$python_bin"
log "host=$host port=$port dry_run=$dry_run foreground=$foreground log_file=$log_file probe_connect_timeout_seconds=$probe_connect_timeout_seconds probe_ack_timeout_seconds=$probe_ack_timeout_seconds"
log "command=$(printf '%q ' "${cmd[@]}")"

if [[ $foreground -eq 1 ]]; then
  exec "${cmd[@]}"
fi

: > "$log_file"
nohup "${cmd[@]}" >>"$log_file" 2>&1 &
started_pid="$!"
printf '%s\n' "$started_pid" > "$pid_file"
log "started pid=$started_pid"

elapsed=0
while [[ $elapsed -lt $wait_timeout_seconds ]]; do
  if gateway_protocol_healthy; then
    log "gateway protocol-healthy host=$host port=$port pid=$started_pid"
    exit 0
  fi
  if ! pid_is_alive "$started_pid"; then
    echo "[SHARED-IBKR-GATEWAY][ERROR] Gateway process exited before becoming protocol-healthy. See $log_file" >&2
    exit 1
  fi
  if port_reachable "$host" "$port"; then
    log "gateway listener is open but protocol not healthy yet host=$host port=$port pid=$started_pid elapsed=${elapsed}s"
  fi
  sleep 1
  elapsed=$((elapsed + 1))
done

echo "[SHARED-IBKR-GATEWAY][ERROR] Timed out waiting for gateway protocol health at $host:$port. See $log_file" >&2
exit 1



