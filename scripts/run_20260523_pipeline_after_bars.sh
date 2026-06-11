#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
PYTHON_BIN="${PYTHON_BIN:-/Users/filmonghezehey/miniforge3/bin/python3}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-300}"
MIN_30S_SYMBOL_CSVS="${MIN_30S_SYMBOL_CSVS:-99}"
MIN_5S_SYMBOL_CSVS="${MIN_5S_SYMBOL_CSVS:-99}"
MIN_1S_SYMBOL_CSVS="${MIN_1S_SYMBOL_CSVS:-99}"
MIN_30S_ROWS_PER_SYMBOL="${MIN_30S_ROWS_PER_SYMBOL:-150000}"
MIN_UNIQUE_DAYS_PER_SYMBOL="${MIN_UNIQUE_DAYS_PER_SYMBOL:-200}"
RUN_COMPARE_FINALIZE="${RUN_COMPARE_FINALIZE:-1}"
RUN_LIFECYCLE_MICRO="${RUN_LIFECYCLE_MICRO:-1}"
RUN_MAVEN_VERIFY="${RUN_MAVEN_VERIFY:-1}"
RUN_BACKTEST="${RUN_BACKTEST:-0}"
AUTO_APPLY_COMPARE="${AUTO_APPLY_COMPARE:-1}"
BACKTEST_SYMBOLS="${BACKTEST_SYMBOLS:-TSLA}"
BACKTEST_DRY_RUN="${BACKTEST_DRY_RUN:-true}"

DATA_30S="$ROOT/training_data/databento_30s_20260523"
DATA_5S="$ROOT/training_data/databento_5s_20260523"
DATA_1S="$ROOT/training_data/databento_1s_20260523"
COMBINED_30S="$ROOT/training_data/databento_30s_20260523_combined.csv"
COMBINED_5S="$ROOT/training_data/databento_5s_20260523_combined.csv"
COMBINED_1S="$ROOT/training_data/databento_1s_20260523_combined.csv"
PLAN_CSV="$DATA_30S/symbol_model_plan.csv"
COMPARE_ROOT="$ROOT/training_data/compare_runs_20260523_meta_ab"
ROUTING_CSV="$ROOT/runtime/databento/model-routing-20260523.csv"
LIFECYCLE_DIR="$ROOT/model_exports/lifecycle_micro_20260523"
LOCK_ROOT="$ROOT/runtime/locks"
LOCK_DIR="$LOCK_ROOT/20260523_pipeline_after_bars.lock"
RUN_ID="$(date '+%Y%m%d_%H%M%S')"
LOG_DIR="${LOG_DIR:-$ROOT/runtime/logs/20260523_pipeline_after_bars/$RUN_ID}"
MASTER_LOG="$LOG_DIR/pipeline.log"

mkdir -p "$LOCK_ROOT" "$LOG_DIR"
if mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "$$" > "$LOCK_DIR/pid"
else
  existing_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "Another 20260523 after-bars pipeline is already running with pid=$existing_pid" >&2
    exit 1
  fi
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
  echo "$$" > "$LOCK_DIR/pid"
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

exec > >(tee -a "$MASTER_LOG") 2>&1

step_index=0

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

chunk_error_detected() {
  local chunk_root="$ROOT/training_data/databento_30s_20260523_build_chunks"
  [[ -d "$chunk_root" ]] || return 1
  grep -RliE 'Traceback|No space left on device|One or more chunk builds failed|OSError|Exception' "$chunk_root"/chunk_*/build.log >/dev/null 2>&1
}

required_bar_outputs_exist() {
  [[ -s "$COMBINED_30S" && -s "$COMBINED_5S" && -s "$COMBINED_1S" && -s "$PLAN_CSV" ]]
}

count_csvs() {
  local dir="$1"
  local pattern="$2"
  if [[ -d "$dir" ]]; then
    find "$dir" -maxdepth 1 -name "$pattern" -type f | wc -l | tr -d ' '
  else
    echo 0
  fi
}

run_step() {
  local name="$1"
  shift
  step_index=$((step_index + 1))
  local step_prefix
  step_prefix="$(printf '%02d' "$step_index")"
  local step_log="$LOG_DIR/${step_prefix}_${name}.log"
  log "START step=$name log=$step_log"
  set +e
  "$@" 2>&1 | tee -a "$step_log"
  local rc=${PIPESTATUS[0]}
  set -e
  if [[ "$rc" -ne 0 ]]; then
    log "FAILED step=$name rc=$rc"
    exit "$rc"
  fi
  log "DONE step=$name"
}

wait_for_current_bar_build() {
  log "Waiting for the active 20260523 bar build to finish before continuing."
  log "Logs: $LOG_DIR"
  while true; do
    local pids
    pids="$(builder_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    if chunk_error_detected; then
      log "Detected an error marker in chunk build logs. Stopping before training."
      bash "$ROOT/scripts/check_retrain_20260523_status.sh" || true
      exit 1
    fi
    if [[ -n "$pids" ]]; then
      log "Bar build still running pids=[$pids] disk=$(df -h "$ROOT" | tail -1 | awk '{print $4 " available, " $5 " used"}')"
      bash "$ROOT/scripts/check_retrain_20260523_status.sh" | tail -80 || true
      sleep "$CHECK_INTERVAL_SECONDS"
      continue
    fi
    if required_bar_outputs_exist; then
      log "Bar build process has exited and required output files exist."
      break
    fi
    log "No 20260523 build process is running, but required outputs are missing. Stopping."
    bash "$ROOT/scripts/check_retrain_20260523_status.sh" || true
    exit 1
  done
}

validate_bar_outputs() {
  log "Validating 20260523 bar artifacts before training."
  "$PYTHON_BIN" - <<PY
from pathlib import Path
import pandas as pd

root = Path("$ROOT")
data_30s = Path("$DATA_30S")
data_5s = Path("$DATA_5S")
data_1s = Path("$DATA_1S")
combined_30s = Path("$COMBINED_30S")
combined_5s = Path("$COMBINED_5S")
combined_1s = Path("$COMBINED_1S")
plan_csv = Path("$PLAN_CSV")

required = [combined_30s, combined_5s, combined_1s, plan_csv]
missing = [str(path) for path in required if not path.is_file() or path.stat().st_size <= 0]
if missing:
    raise SystemExit("Missing or empty required bar output(s): " + "; ".join(missing))

count_30s = len(list(data_30s.glob("*_30s_training.csv")))
count_5s = len(list(data_5s.glob("*_5s_training.csv")))
count_1s = len(list(data_1s.glob("*_1s_training.csv")))
if count_30s < int("$MIN_30S_SYMBOL_CSVS"):
    raise SystemExit(f"Too few 30s symbol CSVs: {count_30s} < $MIN_30S_SYMBOL_CSVS")
if count_5s < int("$MIN_5S_SYMBOL_CSVS"):
    raise SystemExit(f"Too few 5s symbol CSVs: {count_5s} < $MIN_5S_SYMBOL_CSVS")
if count_1s < int("$MIN_1S_SYMBOL_CSVS"):
    raise SystemExit(f"Too few 1s symbol CSVs: {count_1s} < $MIN_1S_SYMBOL_CSVS")

plan = pd.read_csv(plan_csv)
if len(plan) < int("$MIN_30S_SYMBOL_CSVS"):
    raise SystemExit(f"Plan has too few rows: {len(plan)} < $MIN_30S_SYMBOL_CSVS")

bad = []
all_dates = set()
for csv_path in sorted(data_30s.glob("*_30s_training.csv")):
    frame = pd.read_csv(csv_path, usecols=lambda col: col in {"Date", "Timestamp"})
    if "Date" in frame.columns:
        dates = set(frame["Date"].astype(str))
    elif "Timestamp" in frame.columns:
        raw_dates = frame["Timestamp"].astype(str).str.split().str[0]
        dates = set(pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce").dt.date.dropna().astype(str))
    else:
        dates = set()
    all_dates.update(dates)
    if len(frame) < int("$MIN_30S_ROWS_PER_SYMBOL") or len(dates) < int("$MIN_UNIQUE_DAYS_PER_SYMBOL"):
        bad.append((csv_path.name, len(frame), len(dates)))
if bad:
    preview = "; ".join(f"{name}: rows={rows} days={days}" for name, rows, days in bad[:10])
    raise SystemExit(
        "Coverage validation failed; refusing downstream training. "
        f"Require rows>=$MIN_30S_ROWS_PER_SYMBOL days>=$MIN_UNIQUE_DAYS_PER_SYMBOL. Examples: {preview}"
    )

print(
    "BAR_VALIDATION_OK "
    f"30s_csvs={count_30s} 5s_csvs={count_5s} 1s_csvs={count_1s} "
    f"plan_rows={len(plan)} unique_30s_dates={len(all_dates)} "
    f"first_date={min(all_dates) if all_dates else ''} last_date={max(all_dates) if all_dates else ''}"
)
PY
}

validate_compare_outputs() {
  [[ -s "$COMPARE_ROOT/final_comparison_report.json" ]] || { log "Missing final comparison report."; exit 1; }
  [[ -s "$ROUTING_CSV" ]] || { log "Missing versioned routing CSV."; exit 1; }
  "$PYTHON_BIN" - <<PY
from pathlib import Path
import pandas as pd

routing = Path("$ROUTING_CSV")
frame = pd.read_csv(routing)
if frame.empty:
    raise SystemExit(f"Routing CSV is empty: {routing}")
if "variant" not in frame.columns:
    raise SystemExit(f"Routing CSV is missing required variant column: {routing}")
print(f"COMPARE_VALIDATION_OK routing_rows={len(frame)} variants={frame['variant'].value_counts(dropna=False).to_dict()}")
PY
}

validate_lifecycle_outputs() {
  local required=(
    "$LIFECYCLE_DIR/long_exit_lifecycle.onnx"
    "$LIFECYCLE_DIR/short_exit_lifecycle.onnx"
    "$LIFECYCLE_DIR/long_micro_entry_5s.onnx"
    "$LIFECYCLE_DIR/short_micro_entry_5s.onnx"
    "$LIFECYCLE_DIR/long_micro_exit_guard_5s.onnx"
    "$LIFECYCLE_DIR/short_micro_exit_guard_5s.onnx"
    "$LIFECYCLE_DIR/lifecycle_micro_scorecard.csv"
    "$LIFECYCLE_DIR/lifecycle_micro_route_manifest.json"
  )
  local missing=()
  for path in "${required[@]}"; do
    [[ -s "$path" ]] || missing+=("$path")
  done
  if [[ "${#missing[@]}" -gt 0 ]]; then
    printf 'Missing lifecycle/micro output: %s\n' "${missing[@]}" >&2
    exit 1
  fi
  log "LIFECYCLE_VALIDATION_OK output_dir=$LIFECYCLE_DIR"
}

run_optional_backtests() {
  IFS=',' read -r -a symbols <<< "$BACKTEST_SYMBOLS"
  for symbol in "${symbols[@]}"; do
    symbol="$(printf '%s' "$symbol" | tr '[:lower:]' '[:upper:]' | xargs)"
    [[ -n "$symbol" ]] || continue
    run_step "backtest_${symbol}" env \
      PYTHON_BIN="$PYTHON_BIN" \
      UPGRADED_ROUTE_REQUIRED=true \
      LIFECYCLE_EXIT_ENABLED=true \
      MICRO_ENTRY_ENABLED=true \
      MICRO_EXIT_GUARD_ENABLED=true \
      MODEL_DIR="$LIFECYCLE_DIR" \
      DRY_RUN="$BACKTEST_DRY_RUN" \
      bash "$ROOT/scripts/run_databento_historical_streaming_backtest_20260523.sh" "$symbol"
  done
}

main() {
  cd "$ROOT"
  log "20260523 after-bars automation started."
  log "MASTER_LOG=$MASTER_LOG"
  log "Settings: RUN_COMPARE_FINALIZE=$RUN_COMPARE_FINALIZE RUN_LIFECYCLE_MICRO=$RUN_LIFECYCLE_MICRO RUN_MAVEN_VERIFY=$RUN_MAVEN_VERIFY RUN_BACKTEST=$RUN_BACKTEST AUTO_APPLY_COMPARE=$AUTO_APPLY_COMPARE"

  wait_for_current_bar_build
  run_step "bar_output_validation" validate_bar_outputs
  run_step "audit_retrain_runs" "$PYTHON_BIN" "$ROOT/scripts/audit_databento_retrain_runs.py"

  if truthy "$RUN_COMPARE_FINALIZE"; then
    run_step "finalize_compare" env PYTHONUNBUFFERED=1 bash "$ROOT/scripts/finalize_databento_compare_20260523.sh"
    run_step "compare_output_validation" validate_compare_outputs
  else
    log "Skipping compare/finalize because RUN_COMPARE_FINALIZE=$RUN_COMPARE_FINALIZE"
  fi

  if truthy "$RUN_LIFECYCLE_MICRO"; then
    run_step "lifecycle_micro_training" bash "$ROOT/scripts/run_lifecycle_micro_training_20260523.sh"
    run_step "lifecycle_output_validation" validate_lifecycle_outputs
  else
    log "Skipping lifecycle/micro training because RUN_LIFECYCLE_MICRO=$RUN_LIFECYCLE_MICRO"
  fi

  if truthy "$RUN_MAVEN_VERIFY"; then
    run_step "maven_compile" "$ROOT/mvnw" -q -DskipTests compile
    run_step "maven_targeted_tests" "$ROOT/mvnw" -q -Dtest=PingPongStrategyFeatureSchemaTest,PingPongStrategyAiEvaluationTest,PingPongStrategyBucketFinalizationTest test
  else
    log "Skipping Maven verification because RUN_MAVEN_VERIFY=$RUN_MAVEN_VERIFY"
  fi

  if truthy "$RUN_BACKTEST"; then
    run_optional_backtests
  else
    log "Skipping historical streaming backtest because RUN_BACKTEST=$RUN_BACKTEST"
  fi

  if truthy "$AUTO_APPLY_COMPARE"; then
    run_step "apply_compare" bash "$ROOT/scripts/apply_databento_compare_20260523.sh"
  else
    log "Skipping runtime apply because AUTO_APPLY_COMPARE=$AUTO_APPLY_COMPARE"
  fi

  log "PIPELINE_COMPLETE log_dir=$LOG_DIR"
}

main "$@"

