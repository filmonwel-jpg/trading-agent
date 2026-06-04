#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
PYTHON_BIN="${PYTHON_BIN:-python3}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-300}"
MAX_RECOVERY_ATTEMPTS="${MAX_RECOVERY_ATTEMPTS:-3}"
MAX_BAR_BUILD_RESTARTS="${MAX_BAR_BUILD_RESTARTS:-1}"
MIN_FREE_GB_FOR_BAR_RESTART="${MIN_FREE_GB_FOR_BAR_RESTART:-150}"

RUN_COMPARE_FINALIZE="${RUN_COMPARE_FINALIZE:-1}"
RUN_LIFECYCLE_MICRO="${RUN_LIFECYCLE_MICRO:-1}"
RUN_MAVEN_VERIFY="${RUN_MAVEN_VERIFY:-1}"
RUN_BACKTEST="${RUN_BACKTEST:-0}"
AUTO_APPLY_COMPARE="${AUTO_APPLY_COMPARE:-1}"

DATA_30S="$ROOT/training_data/databento_30s_20260523"
COMBINED_30S="$ROOT/training_data/databento_30s_20260523_combined.csv"
COMBINED_5S="$ROOT/training_data/databento_5s_20260523_combined.csv"
COMBINED_1S="$ROOT/training_data/databento_1s_20260523_combined.csv"
PLAN_CSV="$DATA_30S/symbol_model_plan.csv"
AFTER_BARS_LOCK="$ROOT/runtime/locks/20260523_pipeline_after_bars.lock/pid"
RECOVERY_LOCK_ROOT="$ROOT/runtime/locks"
RECOVERY_LOCK_DIR="$RECOVERY_LOCK_ROOT/20260523_pipeline_recovery.lock"
RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="${LOG_DIR:-$ROOT/runtime/logs/20260523_pipeline_recovery/$RUN_ID}"
MASTER_LOG="$LOG_DIR/recovery.log"
BAR_RESTART_COUNT_FILE="$LOG_DIR/bar_build_restart_count"

mkdir -p "$RECOVERY_LOCK_ROOT" "$LOG_DIR"
if mkdir "$RECOVERY_LOCK_DIR" 2>/dev/null; then
  echo "$$" > "$RECOVERY_LOCK_DIR/pid"
else
  existing_pid="$(cat "$RECOVERY_LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "Another 20260523 recovery watchdog is already running with pid=$existing_pid" >&2
    exit 1
  fi
  rm -rf "$RECOVERY_LOCK_DIR"
  mkdir "$RECOVERY_LOCK_DIR"
  echo "$$" > "$RECOVERY_LOCK_DIR/pid"
fi
trap 'rm -rf "$RECOVERY_LOCK_DIR"' EXIT

exec > >(tee -a "$MASTER_LOG") 2>&1

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

builder_pids() {
  pgrep -f 'run_parallel_databento_build_20260523\.sh|build_30s_from_5s_csv\.py' || true
}

after_bars_pid() {
  local lock_pid=""
  if [[ -f "$AFTER_BARS_LOCK" ]]; then
    lock_pid="$(cat "$AFTER_BARS_LOCK" 2>/dev/null || true)"
    if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
      echo "$lock_pid"
      return 0
    fi
  fi
  pgrep -f 'run_20260523_pipeline_after_bars\.sh' | head -1 || true
}

latest_after_bars_log() {
  ls -t "$ROOT"/runtime/logs/20260523_pipeline_after_bars/*/pipeline.log 2>/dev/null | head -1 || true
}

pipeline_complete() {
  local log_path
  log_path="$(latest_after_bars_log)"
  [[ -n "$log_path" ]] && grep -q 'PIPELINE_COMPLETE' "$log_path"
}

required_bar_outputs_exist() {
  [[ -s "$COMBINED_30S" && -s "$COMBINED_5S" && -s "$COMBINED_1S" && -s "$PLAN_CSV" ]]
}

free_gb() {
  df -g "$ROOT" | awk 'NR==2 {print $4}'
}

diagnose_failure() {
  local label="$1"
  local diag_stamp
  diag_stamp="$(date '+%Y%m%d_%H%M%S')"
  local diag_log="$LOG_DIR/diagnostics_${label}_${diag_stamp}.log"
  log "Capturing diagnostics label=$label log=$diag_log"
  {
    echo "=== date ==="
    date
    echo
    echo "=== disk ==="
    df -h "$ROOT"
    echo
    echo "=== processes ==="
    pgrep -af 'run_20260523_pipeline_after_bars|recover_20260523_pipeline|run_parallel_databento_build_20260523|build_30s_from_5s_csv.py|caffeinate -dimsu -w' || true
    echo
    echo "=== retrain status ==="
    bash "$ROOT/scripts/check_retrain_20260523_status.sh" || true
    echo
    echo "=== latest after-bars log tail ==="
    local latest_log
    latest_log="$(latest_after_bars_log)"
    echo "latest_log=$latest_log"
    [[ -n "$latest_log" ]] && tail -200 "$latest_log" || true
    echo
    echo "=== recent failure markers ==="
    if [[ -d "$ROOT/runtime/logs/20260523_pipeline_after_bars" ]]; then
      grep -RniE 'FAILED step=|Traceback|No space left on device|Exception|ERROR|BUILD_COMPLETE|PIPELINE_COMPLETE' "$ROOT/runtime/logs/20260523_pipeline_after_bars" | tail -200 || true
    fi
  } | tee -a "$diag_log"
}

run_bar_build_restart_if_needed() {
  if required_bar_outputs_exist; then
    log "Required bar outputs already exist; no bar rebuild needed."
    return 0
  fi

  local pids
  pids="$(builder_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -n "$pids" ]]; then
    log "Bar builder is still running pids=[$pids]; not starting another build."
    return 0
  fi

  local restart_count=0
  [[ -f "$BAR_RESTART_COUNT_FILE" ]] && restart_count="$(cat "$BAR_RESTART_COUNT_FILE" 2>/dev/null || echo 0)"
  if [[ "$restart_count" -ge "$MAX_BAR_BUILD_RESTARTS" ]]; then
    log "Bar outputs are missing, but max bar-build restarts reached: $restart_count/$MAX_BAR_BUILD_RESTARTS"
    return 1
  fi

  local available_gb
  available_gb="$(free_gb)"
  if [[ "$available_gb" -lt "$MIN_FREE_GB_FOR_BAR_RESTART" ]]; then
    log "Bar outputs are missing, but free disk is too low for safe rebuild: ${available_gb}GiB < ${MIN_FREE_GB_FOR_BAR_RESTART}GiB"
    return 1
  fi

  restart_count=$((restart_count + 1))
  echo "$restart_count" > "$BAR_RESTART_COUNT_FILE"
  local build_log="$LOG_DIR/bar_build_restart_${restart_count}.log"
  log "Restarting full 20260523 bar build attempt=$restart_count/$MAX_BAR_BUILD_RESTARTS log=$build_log"
  set +e
  (
    cd "$ROOT"
    MAX_DAYS=0 CHUNK_COUNT=4 PYTHONUNBUFFERED=1 bash "$ROOT/scripts/run_parallel_databento_build_20260523.sh"
  ) 2>&1 | tee -a "$build_log"
  local rc=${PIPESTATUS[0]}
  set -e
  if [[ "$rc" -ne 0 ]]; then
    log "Bar build restart failed rc=$rc"
    return "$rc"
  fi
  log "Bar build restart completed successfully."
}

run_after_bars_attempt() {
  local attempt="$1"
  local attempt_stamp
  attempt_stamp="$(date '+%Y%m%d_%H%M%S')"
  local attempt_log_dir="$ROOT/runtime/logs/20260523_pipeline_after_bars/recovery_attempt_${attempt}_${attempt_stamp}"
  log "Starting after-bars pipeline recovery attempt=$attempt log_dir=$attempt_log_dir"
  set +e
  (
    cd "$ROOT"
    CHECK_INTERVAL_SECONDS="$CHECK_INTERVAL_SECONDS" \
    RUN_COMPARE_FINALIZE="$RUN_COMPARE_FINALIZE" \
    RUN_LIFECYCLE_MICRO="$RUN_LIFECYCLE_MICRO" \
    RUN_MAVEN_VERIFY="$RUN_MAVEN_VERIFY" \
    RUN_BACKTEST="$RUN_BACKTEST" \
    AUTO_APPLY_COMPARE="$AUTO_APPLY_COMPARE" \
    PYTHON_BIN="$PYTHON_BIN" \
    PYTHONUNBUFFERED=1 \
    LOG_DIR="$attempt_log_dir" \
    bash "$ROOT/scripts/run_20260523_pipeline_after_bars.sh"
  )
  local rc=$?
  set -e
  log "After-bars pipeline recovery attempt=$attempt exited rc=$rc"
  return "$rc"
}

monitor_existing_after_bars() {
  while true; do
    if pipeline_complete; then
      log "Detected PIPELINE_COMPLETE in latest after-bars log. Recovery watchdog complete."
      return 0
    fi
    local pid
    pid="$(after_bars_pid)"
    if [[ -z "$pid" ]]; then
      log "No active after-bars supervisor detected."
      return 1
    fi
    log "Monitoring active after-bars supervisor pid=$pid disk=$(df -h "$ROOT" | tail -1 | awk '{print $4 " available, " $5 " used"}')"
    sleep "$CHECK_INTERVAL_SECONDS"
  done
}

main() {
  cd "$ROOT"
  log "20260523 recovery watchdog started."
  log "MASTER_LOG=$MASTER_LOG"
  log "Settings: MAX_RECOVERY_ATTEMPTS=$MAX_RECOVERY_ATTEMPTS MAX_BAR_BUILD_RESTARTS=$MAX_BAR_BUILD_RESTARTS MIN_FREE_GB_FOR_BAR_RESTART=$MIN_FREE_GB_FOR_BAR_RESTART"

  local attempt=0
  while [[ "$attempt" -le "$MAX_RECOVERY_ATTEMPTS" ]]; do
    if monitor_existing_after_bars; then
      return 0
    fi

    if pipeline_complete; then
      log "After-bars pipeline completed while recovery loop was evaluating."
      return 0
    fi

    attempt=$((attempt + 1))
    if [[ "$attempt" -gt "$MAX_RECOVERY_ATTEMPTS" ]]; then
      diagnose_failure "attempts_exhausted"
      log "Recovery attempts exhausted. Manual intervention required."
      return 1
    fi

    diagnose_failure "attempt_${attempt}_before_recovery"
    if ! run_bar_build_restart_if_needed; then
      diagnose_failure "attempt_${attempt}_bar_recovery_failed"
      log "Unable to recover bar outputs safely on attempt=$attempt."
      return 1
    fi

    if run_after_bars_attempt "$attempt"; then
      if pipeline_complete; then
        log "Recovery attempt=$attempt completed full pipeline successfully."
        return 0
      fi
      log "Recovery attempt=$attempt exited 0 but PIPELINE_COMPLETE marker was not found; continuing diagnostics."
    fi

    diagnose_failure "attempt_${attempt}_after_recovery"
    log "Sleeping $CHECK_INTERVAL_SECONDS seconds before next recovery attempt."
    sleep "$CHECK_INTERVAL_SECONDS"
  done
}

main "$@"

