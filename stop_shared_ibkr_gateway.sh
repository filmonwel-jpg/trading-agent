#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./stop_shared_ibkr_gateway.sh [--dry-run] [--force-after-seconds=N]

Behavior:
  - Stops the worktree-owned shared IBKR gateway started by start_shared_ibkr_gateway.sh.
  - Uses runtime/databento/state/shared-ibkr-gateway.pid as the ownership marker.
  - Will not kill an externally managed listener when no owned pid file is present.

Examples:
  ./stop_shared_ibkr_gateway.sh
  ./stop_shared_ibkr_gateway.sh --dry-run
  ./stop_shared_ibkr_gateway.sh --force-after-seconds=3
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
pid_file="$repo_root/runtime/databento/state/shared-ibkr-gateway.pid"
log_file="$repo_root/runtime/databento/logs/shared-ibkr-gateway.log"
force_after_seconds=10
dry_run=0

log() {
  printf '[SHARED-IBKR-GATEWAY-STOP][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

pid_is_alive() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  ps -p "$pid" >/dev/null 2>&1
}

wait_for_exit() {
  local pid="$1"
  local timeout="$2"
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    if ! pid_is_alive "$pid"; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  ! pid_is_alive "$pid"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --dry-run)
      dry_run=1
      ;;
    --force-after-seconds=*)
      force_after_seconds="${1#--force-after-seconds=}"
      ;;
    *)
      echo "[SHARED-IBKR-GATEWAY-STOP][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

case "$force_after_seconds" in
  ''|*[!0-9]*)
    echo "[SHARED-IBKR-GATEWAY-STOP][ERROR] --force-after-seconds must be a whole number." >&2
    exit 1
    ;;
esac

if [[ ! -f "$pid_file" ]]; then
  log "no owned pid file present; leaving any external gateway listener untouched"
  exit 0
fi

pid="$(tr -d '[:space:]' < "$pid_file" 2>/dev/null || true)"
if [[ -z "$pid" ]]; then
  rm -f "$pid_file"
  log "removed empty pid file"
  exit 0
fi

if ! pid_is_alive "$pid"; then
  rm -f "$pid_file"
  log "removed stale pid file pid=$pid"
  exit 0
fi

log "target pid=$pid log_file=$log_file"
if [[ $dry_run -eq 1 ]]; then
  exit 0
fi

kill -TERM "$pid" >/dev/null 2>&1 || true
if wait_for_exit "$pid" "$force_after_seconds"; then
  rm -f "$pid_file"
  log "gateway exited after SIGTERM"
  exit 0
fi

log "forcing pid=$pid"
kill -KILL "$pid" >/dev/null 2>&1 || true
sleep 1
if pid_is_alive "$pid"; then
  echo "[SHARED-IBKR-GATEWAY-STOP][ERROR] Failed to stop pid=$pid" >&2
  exit 1
fi

rm -f "$pid_file"
log "gateway stopped"

