#!/usr/bin/env bash
set -euo pipefail

repo_root="/Users/filmonghezehey/trading-agent"
runtime_dir="$repo_root/runtime"
state_dir="$runtime_dir/schedule_state"
mkdir -p "$state_dir"

export PATH="/Users/filmonghezehey/miniforge3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1

TARGET_TZ="America/Denver"
TARGET_HOUR="07"
TARGET_MINUTE_START=0
TARGET_MINUTE_END=4
state_file="$state_dir/morning_mt_$(TZ="$TARGET_TZ" date '+%Y%m%d').done"

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
  echo "[SCHEDULE][MORNING] dry-run: would start weekday stack for MT morning window"
  exit 0
fi

if "$repo_root/start_trading_stack_weekdays.sh"; then
  touch "$state_file"
else
  echo "[SCHEDULE][MORNING] startup failed; leaving schedule state incomplete for retry/manual follow-up" >&2
  exit 1
fi

