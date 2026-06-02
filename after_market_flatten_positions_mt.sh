#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./after_market_flatten_positions_mt.sh [options]

Purpose:
  Flatten selected Databento positions during the after-market window without
  stopping bots, harvesters, the shared IBKR gateway, TWS, or IB Gateway.

Options:
  --symbols=CSV                 Only flatten these symbols, e.g. --symbols=AAPL,NVDA
  --exclude=CSV                 Exclude these symbols
  --mode=gateway|bot            gateway = direct shared IBKR gateway broker-position flatten (default)
                                bot     = call each bot's /api/control/flatten endpoint
  --force-run                   Ignore weekday/time-window and once-per-day state checks
  --dry-run                     Preview actions only; do not submit live flatten requests
  --list                        List selected targets and exit
  --help                        Show this help

Environment overrides:
  TARGET_TZ                     Default: America/Denver
  TARGET_HOUR                   Default: 14
  TARGET_MINUTE_START           Default: 5
  TARGET_MINUTE_END             Default: 30
  AFTER_MARKET_FLATTEN_MODE     Default: gateway
  AFTER_MARKET_LIMIT_OFFSET_PCT Default: 1.0
  AFTER_MARKET_LIMIT_TIF        Default: DAY
  AFTER_MARKET_FLATTEN_ARGS     Extra args appended to the chosen flatten command
  FORCE_RUN=1                   Same as --force-run
  DRY_RUN=1                     Same as --dry-run

Examples:
  ./after_market_flatten_positions_mt.sh --dry-run
  ./after_market_flatten_positions_mt.sh --force-run --symbols=AAPL,NVDA
  ./after_market_flatten_positions_mt.sh --force-run --mode=bot

Notes:
  - Default time is 14:05-14:30 America/Denver, roughly after the regular U.S.
    equities close while this host is on Mountain time.
  - In gateway mode this script submits outside-RTH LMT flatten orders:
      BUY to cover short = current ask + AFTER_MARKET_LIMIT_OFFSET_PCT
      SELL long position = current bid - AFTER_MARKET_LIMIT_OFFSET_PCT
  - In bot mode this script calls flatten_all_databento_bots.sh and waits for flat status.
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
state_dir="$runtime_dir/schedule_state"
mkdir -p "$runtime_dir" "$state_dir"

export PATH="/Users/filmonghezehey/miniforge3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1

TARGET_TZ="${TARGET_TZ:-America/Denver}"
TARGET_HOUR="${TARGET_HOUR:-14}"
TARGET_MINUTE_START="${TARGET_MINUTE_START:-5}"
TARGET_MINUTE_END="${TARGET_MINUTE_END:-30}"
AFTER_MARKET_LIMIT_OFFSET_PCT="${AFTER_MARKET_LIMIT_OFFSET_PCT:-1.0}"
AFTER_MARKET_LIMIT_TIF="${AFTER_MARKET_LIMIT_TIF:-DAY}"
mode="${AFTER_MARKET_FLATTEN_MODE:-gateway}"
symbols_csv=""
exclude_csv=""
force_run="${FORCE_RUN:-0}"
dry_run="${DRY_RUN:-0}"
list_only=0

log_file=""
state_file="$state_dir/after_market_flatten_positions_mt_$(TZ="$TARGET_TZ" date '+%Y%m%d').done"

log() {
  printf '[AFTER-MARKET-FLATTEN][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

send_notification() {
  local status="$1"
  local title="$2"
  local message="$3"
  local details="${4:-}"
  if [[ -x "$repo_root/send_stack_notification.sh" ]]; then
    "$repo_root/send_stack_notification.sh" \
      --event=after-market-flatten \
      --status="$status" \
      --title="$title" \
      --message="$message" \
      --details="$details" >/dev/null 2>&1 || true
  fi
}

validate_non_negative_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be a non-negative integer; got '$value'" >&2
      exit 2
      ;;
  esac
}

validate_mode() {
  case "$mode" in
    gateway|bot) ;;
    *)
      echo "--mode must be gateway or bot; got '$mode'" >&2
      exit 2
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbols=*) symbols_csv="${1#*=}" ;;
    --exclude=*) exclude_csv="${1#*=}" ;;
    --mode=*) mode="${1#*=}" ;;
    --force-run) force_run=1 ;;
    --dry-run) dry_run=1 ;;
    --list) list_only=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

validate_non_negative_integer TARGET_HOUR "$TARGET_HOUR"
validate_non_negative_integer TARGET_MINUTE_START "$TARGET_MINUTE_START"
validate_non_negative_integer TARGET_MINUTE_END "$TARGET_MINUTE_END"
validate_mode

target_hour_num=$((10#$TARGET_HOUR))
target_minute_start_num=$((10#$TARGET_MINUTE_START))
target_minute_end_num=$((10#$TARGET_MINUTE_END))

if [[ "$target_hour_num" -gt 23 ]]; then
  echo "TARGET_HOUR must be between 0 and 23" >&2
  exit 2
fi
if [[ "$target_minute_start_num" -gt 59 || "$target_minute_end_num" -gt 59 || "$target_minute_start_num" -gt "$target_minute_end_num" ]]; then
  echo "TARGET_MINUTE_START/TARGET_MINUTE_END must be ordered minutes between 0 and 59" >&2
  exit 2
fi

if [[ "$force_run" != "1" && $list_only -eq 0 ]]; then
  weekday="$(TZ="$TARGET_TZ" date '+%u')"
  hour="$(TZ="$TARGET_TZ" date '+%H')"
  minute="$(TZ="$TARGET_TZ" date '+%M')"
  minute_num=$((10#$minute))

  if [[ "$weekday" -gt 5 ]]; then
    exit 0
  fi

  if [[ "$hour" != "$(printf '%02d' "$target_hour_num")" || $minute_num -lt "$target_minute_start_num" || $minute_num -gt "$target_minute_end_num" ]]; then
    exit 0
  fi

  if [[ -f "$state_file" ]]; then
    exit 0
  fi
fi

run_stamp="$(date '+%Y%m%d_%H%M%S')"
log_file="$runtime_dir/after-market-flatten-positions-$run_stamp.log"
exec > >(tee -a "$log_file") 2>&1

log "after-market flatten started mode=$mode dryRun=$dry_run forceRun=$force_run log=$log_file"

cmd=()
case "$mode" in
  gateway)
    cmd=(
      python3 "$repo_root/flatten_all_via_shared_ibkr_gateway.py"
      --continue-on-failure
      --after-hours-limit
      --allow-after-hours
      --limit-offset-pct="$AFTER_MARKET_LIMIT_OFFSET_PCT"
      --after-hours-limit-tif="$AFTER_MARKET_LIMIT_TIF"
    )
    if [[ $list_only -eq 1 ]]; then
      cmd+=(--list)
    elif [[ "$dry_run" == "1" ]]; then
      cmd+=(--dry-run)
    else
      cmd+=(--execute)
    fi
    ;;
  bot)
    cmd=("$repo_root/flatten_all_databento_bots.sh" --continue-on-failure)
    if [[ $list_only -eq 1 ]]; then
      cmd+=(--list)
    fi
    if [[ "$dry_run" == "1" ]]; then
      cmd+=(--dry-run)
    fi
    ;;
esac

if [[ -n "$symbols_csv" ]]; then
  cmd+=(--symbols="$symbols_csv")
fi
if [[ -n "$exclude_csv" ]]; then
  cmd+=(--exclude="$exclude_csv")
fi

if [[ -n "${AFTER_MARKET_FLATTEN_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  extra_args=(${AFTER_MARKET_FLATTEN_ARGS})
  cmd+=("${extra_args[@]}")
fi

printf -v printable_cmd '%q ' "${cmd[@]}"
log "running: ${printable_cmd% }"

if "${cmd[@]}"; then
  if [[ $list_only -eq 1 ]]; then
    log "after-market flatten target listing completed"
    exit 0
  fi
  if [[ "$dry_run" == "1" ]]; then
    log "after-market flatten dry-run completed; no state marker written"
    exit 0
  fi
  touch "$state_file"
  log "after-market flatten completed successfully; wrote state=$state_file"
  send_notification "ok" "After-market flatten OK" "Selected positions were flattened or already flat." "Mode: $mode; Log: $log_file"
  exit 0
else
  status=$?
fi

log "after-market flatten failed status=$status; no state marker written so schedule can retry/manual follow-up can run"
send_notification "fail" "After-market flatten needs attention" "One or more selected positions did not flatten cleanly." "Mode: $mode; Log: $log_file"
exit "$status"



