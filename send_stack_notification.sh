#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
config_file="$runtime_dir/notifications.env"
log_file="$runtime_dir/stack-notifications.log"
status_file="$runtime_dir/stack-last-status.json"
mkdir -p "$runtime_dir"

usage() {
  cat <<'EOF'
Usage:
  ./send_stack_notification.sh --event=EVENT --status=STATUS --title=TITLE --message=MESSAGE [--details=TEXT]

Behavior:
  - Always appends a local notification log entry.
  - Always updates runtime/stack-last-status.json.
  - Optionally posts to a configured local webhook or ntfy topic.

Config file:
  runtime/notifications.env
EOF
}

json_escape() {
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

url_encode() {
  python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))'
}

if [[ -f "$config_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$config_file"
  set +a
fi

event=""
status=""
title=""
message=""
details=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --event=*)
      event="${1#--event=}"
      ;;
    --status=*)
      status="${1#--status=}"
      ;;
    --title=*)
      title="${1#--title=}"
      ;;
    --message=*)
      message="${1#--message=}"
      ;;
    --details=*)
      details="${1#--details=}"
      ;;
    *)
      echo "[STACK-NOTIFY][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ -z "$event" || -z "$status" || -z "$title" || -z "$message" ]]; then
  echo "[STACK-NOTIFY][ERROR] event, status, title, and message are required." >&2
  usage
  exit 1
fi

notify_enabled="${STACK_NOTIFY_ENABLED:-1}"
notify_timeout="${STACK_NOTIFY_TIMEOUT_SECONDS:-5}"
host_name="${STACK_NOTIFY_HOSTNAME:-$(hostname)}"
timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

printf '[STACK-NOTIFY][%s] event=%s status=%s title=%s message=%s\n' "$timestamp" "$event" "$status" "$title" "$message" >>"$log_file"
if [[ -n "$details" ]]; then
  printf '[STACK-NOTIFY][%s] details=%s\n' "$timestamp" "$details" >>"$log_file"
fi

python3 - "$status_file" "$timestamp" "$event" "$status" "$title" "$message" "$details" "$host_name" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
payload = {
    "timestamp": sys.argv[2],
    "event": sys.argv[3],
    "status": sys.argv[4],
    "title": sys.argv[5],
    "message": sys.argv[6],
    "details": sys.argv[7],
    "host": sys.argv[8],
}
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY

if [[ "$notify_enabled" != "1" ]]; then
  exit 0
fi

payload="{\"timestamp\":$(printf '%s' "$timestamp" | json_escape),\"event\":$(printf '%s' "$event" | json_escape),\"status\":$(printf '%s' "$status" | json_escape),\"title\":$(printf '%s' "$title" | json_escape),\"message\":$(printf '%s' "$message" | json_escape),\"details\":$(printf '%s' "$details" | json_escape),\"host\":$(printf '%s' "$host_name" | json_escape)}"
combined_message="$title\n$message"
if [[ -n "$details" ]]; then
  combined_message="$combined_message\n$details"
fi

if [[ -n "${STACK_NOTIFY_WEBHOOK_URL:-}" ]]; then
  curl --silent --show-error --max-time "$notify_timeout" \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    -X POST "$STACK_NOTIFY_WEBHOOK_URL" >/dev/null 2>&1 || true
fi

if [[ -n "${STACK_NOTIFY_NTFY_URL:-}" ]]; then
  priority="default"
  case "$status" in
    ok|success)
      priority="default"
      ;;
    warn|warning)
      priority="high"
      ;;
    fail|error)
      priority="urgent"
      ;;
  esac
  curl --silent --show-error --max-time "$notify_timeout" \
    -H "Title: $title" \
    -H "Priority: $priority" \
    -H "Tags: trading-agent,$event,$status" \
    -d "$combined_message" \
    -X POST "$STACK_NOTIFY_NTFY_URL" >/dev/null 2>&1 || true
fi

