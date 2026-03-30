#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
app_props="$repo_root/src/main/resources/application.properties"
launch_env="$runtime_dir/ibkr-launch.env"

usage() {
  cat <<'EOF'
Usage:
  ./ensure_ibkr_workstation.sh [--wait-seconds=N] [--poll-seconds=N] [--host=HOST] [--port=PORT] [--status-only]
  ./ensure_ibkr_workstation.sh --shutdown [--wait-seconds=N] [--poll-seconds=N] [--host=HOST] [--port=PORT]

Behavior:
  - Checks whether the IBKR API endpoint is reachable.
  - If unreachable, tries to launch Trader Workstation / IB Gateway.
  - Waits for the API port to become reachable before returning success.
  - In shutdown mode, quits Trader Workstation / IB Gateway and waits for the API/processes to stop.
  - Loads optional overrides from runtime/ibkr-launch.env.

Recommended config file:
  runtime/ibkr-launch.env

Important:
  - This script launches the IBKR app, but it does not hardcode username/password automation.
  - A local Keychain-backed wrapper such as ./ibkr_keychain_launch.sh can be used via IBKR_LAUNCH_CMD.
  - If you later adopt an external launcher such as IBC, set IBKR_LAUNCH_CMD to that wrapper command.
  - For shutdown, you can optionally set IBKR_SHUTDOWN_CMD or IBKR_FORCE_KILL_ON_SHUTDOWN=1.
EOF
}

get_prop() {
  local path="$1"
  local key="$2"
  if [[ ! -f "$path" ]]; then
    return 0
  fi
  awk -F= -v search_key="$key" '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    {
      current=$1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current)
      if (current == search_key) {
        value=substr($0, index($0, "=") + 1)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        print value
        exit
      }
    }
  ' "$path"
}

log() {
  printf '[IBKR-PREFLIGHT][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

append_unique() {
  local value="$1"
  [[ -n "$value" ]] || return 0
  local existing
  for existing in "${APP_CANDIDATES[@]:-}"; do
    [[ "$existing" == "$value" ]] && return 0
  done
  APP_CANDIDATES+=("$value")
}

derive_app_name_from_path() {
  local path="$1"
  local base
  base="$(basename "$path")"
  base="${base%.app}"
  printf '%s' "$base"
}

ibkr_process_running() {
  pgrep -fi 'Trader Workstation|IB Gateway|jts' >/dev/null 2>&1
}

build_app_candidates() {
  APP_CANDIDATES=()
  if [[ -n "${IBKR_APP_NAME:-}" ]]; then
    append_unique "$IBKR_APP_NAME"
  fi
  if [[ -n "${IBKR_APP_PATH:-}" ]]; then
    append_unique "$(derive_app_name_from_path "$IBKR_APP_PATH")"
  fi
  append_unique "Trader Workstation"
  append_unique "IB Gateway"
}

port_reachable() {
  local host="$1"
  local port="$2"
  python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(1.5)
    try:
        sock.connect((host, port))
    except OSError:
        raise SystemExit(1)
raise SystemExit(0)
PY
}

launch_ibkr_app() {
  local launched=1

  if [[ "${IBKR_AUTO_LAUNCH:-1}" != "1" ]]; then
    log "auto-launch disabled via IBKR_AUTO_LAUNCH=${IBKR_AUTO_LAUNCH:-0}"
    return 1
  fi

  if [[ -n "${IBKR_LAUNCH_CMD:-}" ]]; then
    log "launching IBKR via IBKR_LAUNCH_CMD"
    /bin/bash -lc "$IBKR_LAUNCH_CMD" && launched=0 || launched=$?
    return "$launched"
  fi

  if [[ -n "${IBKR_APP_PATH:-}" ]]; then
    log "launching IBKR app path: $IBKR_APP_PATH"
    open "$IBKR_APP_PATH" && launched=0 || launched=$?
    return "$launched"
  fi

  local candidate
  build_app_candidates
  for candidate in "${APP_CANDIDATES[@]}"; do
    if open -a "$candidate" >/dev/null 2>&1; then
      log "launch command accepted for app: $candidate"
      return 0
    fi
  done

  log "no launchable IBKR app was found via IBKR_APP_NAME or default macOS app names"
  return 1
}

quit_ibkr_app() {
  local quit_requested=1

  if [[ -n "${IBKR_SHUTDOWN_CMD:-}" ]]; then
    log "running IBKR shutdown command from IBKR_SHUTDOWN_CMD"
    /bin/bash -lc "$IBKR_SHUTDOWN_CMD" && return 0 || return $?
  fi

  build_app_candidates
  local candidate
  for candidate in "${APP_CANDIDATES[@]}"; do
    if osascript -e "tell application \"$candidate\" to quit" >/dev/null 2>&1; then
      log "quit requested for app: $candidate"
      quit_requested=0
    fi
  done

  if [[ $quit_requested -eq 0 ]]; then
    return 0
  fi

  if ibkr_process_running; then
    log "IBKR processes are running but no macOS quit command succeeded"
    return 1
  fi

  log "IBKR app is already stopped"
  return 0
}

force_kill_ibkr() {
  log "forcing IBKR process shutdown via pkill"
  pkill -fi 'Trader Workstation|IB Gateway|jts' >/dev/null 2>&1 || true
}

shutdown_ibkr() {
  log "requesting IBKR shutdown host=$ib_host port=$ib_port wait_seconds=$wait_seconds poll_seconds=$poll_seconds"

  if ! port_reachable "$ib_host" "$ib_port" && ! ibkr_process_running; then
    log "IBKR API and processes are already stopped"
    return 0
  fi

  if ! quit_ibkr_app; then
    log "IBKR graceful shutdown request did not fully succeed"
  fi

  local elapsed=0
  while (( elapsed < wait_seconds )); do
    if ! port_reachable "$ib_host" "$ib_port" && ! ibkr_process_running; then
      log "IBKR shutdown confirmed after ${elapsed}s"
      return 0
    fi
    sleep "$poll_seconds"
    elapsed=$((elapsed + poll_seconds))
  done

  if [[ "${IBKR_FORCE_KILL_ON_SHUTDOWN:-0}" == "1" ]]; then
    force_kill_ibkr
    sleep 2
    if ! port_reachable "$ib_host" "$ib_port" && ! ibkr_process_running; then
      log "IBKR shutdown confirmed after force-kill fallback"
      return 0
    fi
  fi

  log "timed out waiting for IBKR shutdown"
  return 1
}

wait_seconds="${IBKR_WAIT_SECONDS:-180}"
poll_seconds="${IBKR_POLL_SECONDS:-5}"
status_only=0
shutdown_mode=0
host_override=""
port_override=""

if [[ -f "$launch_env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$launch_env"
  set +a
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --wait-seconds=*)
      wait_seconds="${1#--wait-seconds=}"
      ;;
    --poll-seconds=*)
      poll_seconds="${1#--poll-seconds=}"
      ;;
    --host=*)
      host_override="${1#--host=}"
      ;;
    --port=*)
      port_override="${1#--port=}"
      ;;
    --status-only)
      status_only=1
      ;;
    --shutdown|--quit)
      shutdown_mode=1
      ;;
    *)
      echo "[IBKR-PREFLIGHT][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

ib_host="${host_override:-${IBKR_HOST:-$(get_prop "$app_props" trading.host)}}"
ib_port="${port_override:-${IBKR_PORT:-$(get_prop "$app_props" trading.port)}}"
ib_host="${ib_host:-127.0.0.1}"
ib_port="${ib_port:-7497}"

if ! [[ "$wait_seconds" =~ ^[0-9]+$ && "$poll_seconds" =~ ^[0-9]+$ && "$ib_port" =~ ^[0-9]+$ ]]; then
  echo "[IBKR-PREFLIGHT][ERROR] wait/poll seconds and port must be integers." >&2
  exit 1
fi

log "checking IBKR API host=$ib_host port=$ib_port wait_seconds=$wait_seconds poll_seconds=$poll_seconds"

if [[ "$shutdown_mode" -eq 1 ]]; then
  shutdown_ibkr
  exit $?
fi

if port_reachable "$ib_host" "$ib_port"; then
  log "IBKR API already reachable"
  exit 0
fi

if [[ "$status_only" -eq 1 ]]; then
  log "IBKR API is not reachable"
  exit 1
fi

if launch_ibkr_app; then
  log "IBKR launch requested; waiting for API port"
else
  log "IBKR launch step failed or is unavailable; waiting anyway in case login is already in progress"
fi

elapsed=0
warned_login_pending=0
while (( elapsed < wait_seconds )); do
  if port_reachable "$ib_host" "$ib_port"; then
    log "IBKR API reachable after ${elapsed}s"
    exit 0
  fi
  if (( warned_login_pending == 0 )) && pgrep -fi 'Trader Workstation|IB Gateway|jts' >/dev/null 2>&1; then
    log "IBKR process is running but API port is still closed; login / 2FA may still be pending"
    warned_login_pending=1
  fi
  sleep "$poll_seconds"
  elapsed=$((elapsed + poll_seconds))
done

log "timed out waiting for IBKR API on $ib_host:$ib_port"
log "Tip: if you need unattended login, use runtime/ibkr-launch.env with IBKR_LAUNCH_CMD pointing to a secure external launcher (for example IBC), or keep TWS/Gateway logged in before 07:00 MT."
exit 1

