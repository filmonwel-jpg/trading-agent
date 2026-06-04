#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
FINISH_LOG_DIR="$ROOT/runtime/logs/finish_20260523_outputs"
FINISH_PID_FILE="$FINISH_LOG_DIR/finish_from_chunks.pid"
RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="$ROOT/runtime/logs/continue_20260523_after_finish/$RUN_ID"
LOG="$LOG_DIR/continue.log"
PIPELINE_LOG_DIR="$ROOT/runtime/logs/20260523_pipeline_after_bars/queued_after_finish_$RUN_ID"

mkdir -p "$LOG_DIR" "$PIPELINE_LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "continue_20260523_after_finish START"
log "ROOT=$ROOT"
log "LOG=$LOG"
log "PIPELINE_LOG_DIR=$PIPELINE_LOG_DIR"

finish_pid=""
if [[ -f "$FINISH_PID_FILE" ]]; then
  finish_pid="$(cat "$FINISH_PID_FILE" 2>/dev/null || true)"
fi
if [[ -n "$finish_pid" ]] && kill -0 "$finish_pid" 2>/dev/null; then
  log "Waiting for finish process pid=$finish_pid"
  while kill -0 "$finish_pid" 2>/dev/null; do
    log "Finish process still running pid=$finish_pid disk=$(df -h "$ROOT" | tail -1 | awk '{print $4 " available, " $5 " used"}')"
    sleep "${FINISH_POLL_SECONDS:-300}"
  done
  latest_finish_log="$(ls -t "$FINISH_LOG_DIR"/finish_from_chunks_*.log 2>/dev/null | head -1 || true)"
  if [[ -z "$latest_finish_log" ]] || ! grep -q 'finish_20260523_outputs_from_chunks COMPLETE' "$latest_finish_log"; then
    log "Finish process exited, but success marker was not found. latest_finish_log=$latest_finish_log"
    [[ -n "$latest_finish_log" ]] && tail -120 "$latest_finish_log" || true
    exit 1
  fi
  log "Finish process completed successfully. latest_finish_log=$latest_finish_log"
else
  log "No active finish process found (pid=${finish_pid:-none}); validating existing outputs."
fi

log "Running finish-output status check"
"$ROOT/scripts/check_20260523_finish_outputs_status.py"

log "Running official retrain status check"
bash "$ROOT/scripts/check_retrain_20260523_status.sh"

log "Starting after-bars pipeline"
(
  cd "$ROOT"
  CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-300}" \
  RUN_COMPARE_FINALIZE="${RUN_COMPARE_FINALIZE:-1}" \
  RUN_LIFECYCLE_MICRO="${RUN_LIFECYCLE_MICRO:-1}" \
  RUN_MAVEN_VERIFY="${RUN_MAVEN_VERIFY:-1}" \
  RUN_BACKTEST="${RUN_BACKTEST:-0}" \
  AUTO_APPLY_COMPARE="${AUTO_APPLY_COMPARE:-1}" \
  PYTHONUNBUFFERED=1 \
  LOG_DIR="$PIPELINE_LOG_DIR" \
  bash "$ROOT/scripts/run_20260523_pipeline_after_bars.sh"
)
rc=$?
log "after-bars pipeline exited rc=$rc"
exit "$rc"


