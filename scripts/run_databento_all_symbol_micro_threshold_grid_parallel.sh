#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
SYMBOLS_CSV=""
SYMBOLS_FILE="runtime/symbols_100.txt"
START_DATE="${START_DATE:-${CAL_START:-}}"
END_DATE="${END_DATE:-${CAL_END:-}}"
OUTPUT_BASE="${OUTPUT_BASE:-/tmp/trading-agent-all-symbol-micro-threshold-grid-low-${RUN_STAMP}}"
MODEL_DIR="${MODEL_DIR:-model_exports/20260524_022652}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-model_exports/lifecycle_micro_20260523}"
PYTHON_BIN="${PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-/tmp/trading-agent-databento-venv/bin/python3}}"
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
PREVIOUS_CLOSE_LOOKBACK_DAYS="${DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS:-14}"
MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
MAX_SHARE_CAP="${BACKTEST_MAX_SHARE_CAP:-2000}"
LONG_THRESHOLDS="0.10 0.11 0.12 0.13 0.14 0.15 0.16 0.17"
SHORT_THRESHOLDS="0.08 0.09 0.10 0.11 0.12 0.13 0.14"
JOBS="${GRID_JOBS:-8}"
RUN_DEFAULT_BASELINE="true"
RUN_DISABLED_BASELINE="false"
RESUME="true"
DRY_RUN="false"
SKIP_BUILD="false"
CLASSPATH_FILE="${CLASSPATH_FILE:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_databento_all_symbol_micro_threshold_grid_parallel.sh --start YYYY-MM-DD --end YYYY-MM-DD [options]

Runs the same one-symbol micro-threshold grid used for TSLA across many symbols,
in parallel, with one output folder per symbol and per-symbol driver logs.

Defaults match the low TSLA calibration grid:
  long:  0.10 0.11 0.12 0.13 0.14 0.15 0.16 0.17
  short: 0.08 0.09 0.10 0.11 0.12 0.13 0.14
  plus --default-baseline, no disabled baseline, and --resume.

Required unless already exported:
  --start YYYY-MM-DD              First replay day. Also reads START_DATE or CAL_START.
  --end YYYY-MM-DD                Last replay day. Also reads END_DATE or CAL_END.

Core options:
  --symbols CSV                   Comma/space-separated symbols.
  --symbols-file FILE             Default: runtime/symbols_100.txt
  --output-base DIR               Default: /tmp/trading-agent-all-symbol-micro-threshold-grid-low-<timestamp>
  --model-dir DIR                 Default: model_exports/20260524_022652
  --lifecycle-model-dir DIR       Default: model_exports/lifecycle_micro_20260523
  --python-bin PATH               Default: /tmp/trading-agent-databento-venv/bin/python3
  --databento-env-file FILE       Env file containing DATABENTO_API_KEY.
  --classpath-file FILE           Shared Java classpath cache. Default: <output-base>/databento_ibkr_sim_backtest_cp.txt
  --timeout-seconds N             Per threshold-run timeout. Default: 3600
  --previous-close-lookback-days N Default: 14
  --max-trades N                  Default: 2000
  --max-share-cap N               Default: 500

Parallel/grid options:
  --jobs N                        Concurrent symbol grids. Default: GRID_JOBS or 8
  --long-thresholds "LIST"         Space/comma-separated long thresholds.
  --short-thresholds "LIST"        Space/comma-separated short thresholds.
  --thresholds "LIST"              Use same list for both sides.
  --default-baseline              Include default scorecard baseline. Default: on
  --no-default-baseline           Disable default baseline.
  --disabled-baseline             Include disabled lifecycle/micro baseline. Default: off
  --resume                        Skip completed symbol threshold dirs. Default: on
  --no-resume                     Re-run completed threshold dirs.
  --dry-run                       Validate wiring without downloading Databento data.
  --skip-build                    Do not run mvn package; still builds classpath if missing.

Example aiming for ~12 hours on a capable machine/network:
  scripts/run_databento_all_symbol_micro_threshold_grid_parallel.sh \
    --start 2026-05-18 --end 2026-05-22 \
    --output-base "$RUN_ROOT" \
    --jobs 10 \
    --skip-build
USAGE
}

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

normalize_list() {
  printf '%s' "$1" | tr ',' ' '
}

safe_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr -cs '[:alnum:].-' '_'
}

append_symbol() {
  local raw="$1" symbol existing
  symbol="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]' | xargs)"
  [[ -n "$symbol" ]] || return 0
  if [[ "${#SYMBOLS[@]}" -gt 0 ]]; then
    for existing in "${SYMBOLS[@]}"; do
      [[ "$existing" == "$symbol" ]] && return 0
    done
  fi
  SYMBOLS+=("$symbol")
}

load_symbols_text() {
  local text="$1" symbol
  while IFS= read -r symbol; do
    append_symbol "$symbol"
  done < <(printf '%s' "$text" | tr ',[:space:]' '\n' | awk 'NF {print toupper($0)}')
}

run_symbol_grid() {
  local symbol="$1" safe out_dir log status status_file
  safe="$(safe_symbol "$symbol")"
  out_dir="$OUTPUT_BASE/$safe"
  log="$LOG_DIR/${safe}.driver.log"
  status_file="$out_dir/driver.exit_status"
  mkdir -p "$out_dir" "$LOG_DIR"

  {
    echo "[PARALLEL-GRID] symbol=$symbol started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "[PARALLEL-GRID] output_base=$out_dir"
    echo "[PARALLEL-GRID] classpath_file=$CLASSPATH_FILE"

    local args=(
      scripts/run_databento_micro_threshold_grid_search.sh
      --symbol "$symbol"
      --start "$START_DATE"
      --end "$END_DATE"
      --output-base "$out_dir"
      --model-dir "$MODEL_DIR"
      --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR"
      --python-bin "$PYTHON_BIN"
      --classpath-file "$CLASSPATH_FILE"
      --timeout-seconds "$TIMEOUT_SECONDS"
      --previous-close-lookback-days "$PREVIOUS_CLOSE_LOOKBACK_DAYS"
      --max-trades "$MAX_TRADES"
      --max-share-cap "$MAX_SHARE_CAP"
      --long-thresholds "$LONG_THRESHOLDS"
      --short-thresholds "$SHORT_THRESHOLDS"
      --skip-build
    )
    [[ -n "$DATABENTO_ENV_FILE" ]] && args+=(--databento-env-file "$DATABENTO_ENV_FILE")
    truthy "$RUN_DEFAULT_BASELINE" && args+=(--default-baseline)
    truthy "$RUN_DISABLED_BASELINE" && args+=(--disabled-baseline)
    truthy "$RESUME" && args+=(--resume)
    truthy "$DRY_RUN" && args+=(--dry-run)

    printf '[PARALLEL-GRID] command:'
    printf ' %q' "${args[@]}"
    printf '\n'

    set +e
    "${args[@]}"
    status=$?
    set -e
    printf '%s\n' "$status" > "$status_file"
    echo "[PARALLEL-GRID] symbol=$symbol finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) status=$status"
    exit "$status"
  } > "$log" 2>&1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbols) SYMBOLS_CSV="$2"; shift 2 ;;
    --symbols=*) SYMBOLS_CSV="${1#--symbols=}"; shift ;;
    --symbols-file) SYMBOLS_FILE="$2"; shift 2 ;;
    --symbols-file=*) SYMBOLS_FILE="${1#--symbols-file=}"; shift ;;
    --start|--start-date) START_DATE="$2"; shift 2 ;;
    --start=*|--start-date=*) START_DATE="${1#*=}"; shift ;;
    --end|--end-date) END_DATE="$2"; shift 2 ;;
    --end=*|--end-date=*) END_DATE="${1#*=}"; shift ;;
    --output-base) OUTPUT_BASE="$2"; shift 2 ;;
    --output-base=*) OUTPUT_BASE="${1#--output-base=}"; shift ;;
    --model-dir) MODEL_DIR="$2"; shift 2 ;;
    --model-dir=*) MODEL_DIR="${1#--model-dir=}"; shift ;;
    --lifecycle-model-dir) LIFECYCLE_MODEL_DIR="$2"; shift 2 ;;
    --lifecycle-model-dir=*) LIFECYCLE_MODEL_DIR="${1#--lifecycle-model-dir=}"; shift ;;
    --python-bin) PYTHON_BIN="$2"; shift 2 ;;
    --python-bin=*) PYTHON_BIN="${1#--python-bin=}"; shift ;;
    --databento-env-file) DATABENTO_ENV_FILE="$2"; shift 2 ;;
    --databento-env-file=*) DATABENTO_ENV_FILE="${1#--databento-env-file=}"; shift ;;
    --classpath-file) CLASSPATH_FILE="$2"; shift 2 ;;
    --classpath-file=*) CLASSPATH_FILE="${1#--classpath-file=}"; shift ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --timeout-seconds=*) TIMEOUT_SECONDS="${1#--timeout-seconds=}"; shift ;;
    --previous-close-lookback-days) PREVIOUS_CLOSE_LOOKBACK_DAYS="$2"; shift 2 ;;
    --previous-close-lookback-days=*) PREVIOUS_CLOSE_LOOKBACK_DAYS="${1#--previous-close-lookback-days=}"; shift ;;
    --max-trades) MAX_TRADES="$2"; shift 2 ;;
    --max-trades=*) MAX_TRADES="${1#--max-trades=}"; shift ;;
    --max-share-cap) MAX_SHARE_CAP="$2"; shift 2 ;;
    --max-share-cap=*) MAX_SHARE_CAP="${1#--max-share-cap=}"; shift ;;
    --jobs) JOBS="$2"; shift 2 ;;
    --jobs=*) JOBS="${1#--jobs=}"; shift ;;
    --long-thresholds) LONG_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --long-thresholds=*) LONG_THRESHOLDS="$(normalize_list "${1#--long-thresholds=}")"; shift ;;
    --short-thresholds) SHORT_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --short-thresholds=*) SHORT_THRESHOLDS="$(normalize_list "${1#--short-thresholds=}")"; shift ;;
    --thresholds) LONG_THRESHOLDS="$(normalize_list "$2")"; SHORT_THRESHOLDS="$LONG_THRESHOLDS"; shift 2 ;;
    --thresholds=*) LONG_THRESHOLDS="$(normalize_list "${1#--thresholds=}")"; SHORT_THRESHOLDS="$LONG_THRESHOLDS"; shift ;;
    --default-baseline) RUN_DEFAULT_BASELINE="true"; shift ;;
    --no-default-baseline) RUN_DEFAULT_BASELINE="false"; shift ;;
    --disabled-baseline) RUN_DISABLED_BASELINE="true"; shift ;;
    --resume) RESUME="true"; shift ;;
    --no-resume) RESUME="false"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "[PARALLEL-GRID][ERROR] Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$START_DATE" ]] || { echo "[PARALLEL-GRID][ERROR] --start is required" >&2; exit 2; }
[[ -n "$END_DATE" ]] || { echo "[PARALLEL-GRID][ERROR] --end is required" >&2; exit 2; }
[[ -n "$LONG_THRESHOLDS" ]] || { echo "[PARALLEL-GRID][ERROR] at least one long threshold is required" >&2; exit 2; }
[[ -n "$SHORT_THRESHOLDS" ]] || { echo "[PARALLEL-GRID][ERROR] at least one short threshold is required" >&2; exit 2; }
[[ "$JOBS" =~ ^[0-9]+$ && "$JOBS" -ge 1 ]] || { echo "[PARALLEL-GRID][ERROR] --jobs must be >= 1" >&2; exit 2; }

python3 - "$START_DATE" "$END_DATE" <<'PY'
import datetime as dt
import sys
try:
    start = dt.date.fromisoformat(sys.argv[1])
    end = dt.date.fromisoformat(sys.argv[2])
except ValueError as exc:
    raise SystemExit(f"[PARALLEL-GRID][ERROR] invalid date: {exc}")
if end < start:
    raise SystemExit("[PARALLEL-GRID][ERROR] --end must be on/after --start")
weekdays = sum(1 for i in range((end - start).days + 1) if (start + dt.timedelta(days=i)).weekday() < 5)
print(f"[PARALLEL-GRID] date_window={start}..{end} weekdays={weekdays}")
PY

[[ "$OUTPUT_BASE" != /* ]] && OUTPUT_BASE="$ROOT/$OUTPUT_BASE"
[[ "$SYMBOLS_FILE" != /* ]] && SYMBOLS_FILE="$ROOT/$SYMBOLS_FILE"
[[ "$LIFECYCLE_MODEL_DIR" != /* ]] && LIFECYCLE_MODEL_DIR="$ROOT/$LIFECYCLE_MODEL_DIR"
CLASSPATH_FILE="${CLASSPATH_FILE:-$OUTPUT_BASE/databento_ibkr_sim_backtest_cp.txt}"
[[ "$CLASSPATH_FILE" != /* ]] && CLASSPATH_FILE="$ROOT/$CLASSPATH_FILE"

mkdir -p "$OUTPUT_BASE"
LOG_DIR="$OUTPUT_BASE/_driver_logs"
mkdir -p "$LOG_DIR" "$(dirname "$CLASSPATH_FILE")"

SYMBOLS=()
if [[ -n "$SYMBOLS_CSV" ]]; then
  load_symbols_text "$SYMBOLS_CSV"
else
  [[ -f "$SYMBOLS_FILE" ]] || { echo "[PARALLEL-GRID][ERROR] symbols file not found: $SYMBOLS_FILE" >&2; exit 2; }
  load_symbols_text "$(cat "$SYMBOLS_FILE")"
fi
[[ "${#SYMBOLS[@]}" -gt 0 ]] || { echo "[PARALLEL-GRID][ERROR] no symbols resolved" >&2; exit 2; }
printf '%s\n' "${SYMBOLS[@]}" > "$OUTPUT_BASE/symbols.resolved.txt"

LONG_COUNT="$(wc -w <<< "$LONG_THRESHOLDS" | tr -d ' ')"
SHORT_COUNT="$(wc -w <<< "$SHORT_THRESHOLDS" | tr -d ' ')"
COMBOS=$((LONG_COUNT * SHORT_COUNT))
BASELINES=0
truthy "$RUN_DEFAULT_BASELINE" && BASELINES=$((BASELINES + 1))
truthy "$RUN_DISABLED_BASELINE" && BASELINES=$((BASELINES + 1))
RUNS_PER_SYMBOL=$((COMBOS + BASELINES))
TOTAL_RUNS=$((${#SYMBOLS[@]} * RUNS_PER_SYMBOL))

DRIVER_LOG="$OUTPUT_BASE/all-symbol-grid-parallel-driver.log"
cat <<SUMMARY | tee "$DRIVER_LOG"
[PARALLEL-GRID] run_root=$OUTPUT_BASE
[PARALLEL-GRID] symbols=${#SYMBOLS[@]} jobs=$JOBS runs_per_symbol=$RUNS_PER_SYMBOL total_symbol_threshold_runs=$TOTAL_RUNS
[PARALLEL-GRID] start=$START_DATE end=$END_DATE
[PARALLEL-GRID] model_dir=$MODEL_DIR
[PARALLEL-GRID] lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[PARALLEL-GRID] python_bin=$PYTHON_BIN
[PARALLEL-GRID] classpath_file=$CLASSPATH_FILE
[PARALLEL-GRID] thresholds long=[$LONG_THRESHOLDS] short=[$SHORT_THRESHOLDS]
[PARALLEL-GRID] default_baseline=$RUN_DEFAULT_BASELINE disabled_baseline=$RUN_DISABLED_BASELINE resume=$RESUME dry_run=$DRY_RUN
SUMMARY

if ! truthy "$SKIP_BUILD"; then
  echo "[PARALLEL-GRID] building once before launching workers" | tee -a "$DRIVER_LOG"
  "$ROOT/mvnw" -q -DskipTests package
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
elif [[ ! -f "$CLASSPATH_FILE" ]]; then
  echo "[PARALLEL-GRID] skip_build=true but classpath cache is missing; building dependency classpath only" | tee -a "$DRIVER_LOG"
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
else
  echo "[PARALLEL-GRID] skip_build=true; reusing classpath cache" | tee -a "$DRIVER_LOG"
fi

cleanup_interrupted_workers() {
  local active
  echo "[PARALLEL-GRID][WARN] interrupted; terminating active workers" >&2
  active="$(jobs -pr)"
  if [[ -n "$active" ]]; then
    kill $active 2>/dev/null || true
  fi
  exit 130
}
trap cleanup_interrupted_workers INT TERM

PIDS=()
PID_SYMBOLS=()
for symbol in "${SYMBOLS[@]}"; do
  while [[ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$JOBS" ]]; do
    sleep 5
  done
  run_symbol_grid "$symbol" &
  pid="$!"
  PIDS+=("$pid")
  PID_SYMBOLS+=("$symbol")
  echo "[PARALLEL-GRID] launched symbol=$symbol pid=$pid active=$(jobs -rp | wc -l | tr -d ' ') log=$LOG_DIR/$(safe_symbol "$symbol").driver.log" | tee -a "$DRIVER_LOG"
done

failures=0
for i in "${!PIDS[@]}"; do
  pid="${PIDS[$i]}"
  symbol="${PID_SYMBOLS[$i]}"
  if wait "$pid"; then
    echo "[PARALLEL-GRID] completed symbol=$symbol pid=$pid" | tee -a "$DRIVER_LOG"
  else
    failures=$((failures + 1))
    echo "[PARALLEL-GRID][ERROR] failed symbol=$symbol pid=$pid log=$LOG_DIR/$(safe_symbol "$symbol").driver.log" | tee -a "$DRIVER_LOG" >&2
  fi
done

GRID_RESULTS_COUNT="$(find "$OUTPUT_BASE" -mindepth 2 -maxdepth 2 -name grid_results.csv -type f 2>/dev/null | wc -l | tr -d ' ')"
BEST_ENV_COUNT="$(find "$OUTPUT_BASE" -mindepth 2 -maxdepth 2 -name best_threshold.env -type f 2>/dev/null | wc -l | tr -d ' ')"
cat <<SUMMARY | tee -a "$DRIVER_LOG"
[PARALLEL-GRID] finished run_root=$OUTPUT_BASE failures=$failures
[PARALLEL-GRID] grid_results_count=$GRID_RESULTS_COUNT expected_symbols=${#SYMBOLS[@]}
[PARALLEL-GRID] best_threshold_env_count=$BEST_ENV_COUNT expected_symbols=${#SYMBOLS[@]}
[PARALLEL-GRID] driver_logs=$LOG_DIR
SUMMARY

if [[ "$failures" -gt 0 ]]; then
  exit 1
fi
