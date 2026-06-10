#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SYMBOLS_FILE="runtime/symbols_100.txt"
SYMBOLS_CSV=""
START_DATE=""
END_DATE=""
OUTPUT_BASE="/tmp/trading-agent-backtests-local-calibration"
MODEL_DIR="model_exports/20260524_022652"
PYTHON_BIN="/tmp/trading-agent-databento-venv/bin/python3"
TIMEOUT_SECONDS="3600"
BATCH_SIZE="5"
START_BATCH="1"
MAX_BATCHES="0"
MODE="both"
DRY_RUN="false"
SKIP_BUILD="false"
PREVIOUS_CLOSE_LOOKBACK_DAYS="14"
MICRO_LONG_ENTRY_THRESHOLD=""
MICRO_SHORT_ENTRY_THRESHOLD=""
LIFECYCLE_LONG_EXIT_THRESHOLD=""
LIFECYCLE_SHORT_EXIT_THRESHOLD=""
MICRO_LONG_EXIT_GUARD_THRESHOLD=""
MICRO_SHORT_EXIT_GUARD_THRESHOLD=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_databento_calibration_batches.sh --start YYYY-MM-DD --end YYYY-MM-DD [options]

Runs Databento historical IBKR simulation backtests in batches of at least 5 symbols.
By default it runs both lifecycle/micro enabled and disabled legs and writes under /tmp.

Options:
  --symbols CSV              Comma-separated symbols. If omitted, --symbols-file is used.
  --symbols-file FILE        Newline/comma-separated symbols. Default: runtime/symbols_100.txt
  --start YYYY-MM-DD         First replay day; should cover at least 5 trading days with --end.
  --end YYYY-MM-DD           Last replay day.
  --output-base DIR          Base output directory. Default: /tmp/trading-agent-backtests-local-calibration
  --model-dir DIR            30s model dir passed to launcher. Default: model_exports/20260524_022652
  --python-bin PATH          Python with databento+pandas. Default: /tmp/trading-agent-databento-venv/bin/python3
  --timeout-seconds N        Per-symbol timeout. Default: 3600
  --batch-size N             Target batch size. Default: 5. Final short remainder is merged into prior batch.
  --enabled-only             Run only lifecycle/micro enabled leg.
  --disabled-only            Run only lifecycle/micro disabled leg.
  --start-batch N            1-based batch number to start/resume from. Default: 1
  --max-batches N            Stop after N batches from --start-batch. Default: 0 (all)
  --previous-close-lookback-days N  Export DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS. Default: 14
  --micro-long-entry-threshold P    Override 5s long micro-entry threshold for enabled legs.
  --micro-short-entry-threshold P   Override 5s short micro-entry threshold for enabled legs.
  --lifecycle-exit-threshold P         Override both long/short lifecycle-exit thresholds for enabled legs.
  --lifecycle-long-exit-threshold P    Override 30s long lifecycle-exit threshold for enabled legs.
  --lifecycle-short-exit-threshold P   Override 30s short lifecycle-exit threshold for enabled legs.
  --micro-exit-guard-threshold P       Override both long/short 5s micro-exit guard thresholds for enabled legs.
  --micro-long-exit-guard-threshold P  Override 5s long micro-exit guard threshold for enabled legs.
  --micro-short-exit-guard-threshold P Override 5s short micro-exit guard threshold for enabled legs.
  --dry-run                  Validate wiring without downloading Databento data.
  --skip-build               Pass --skip-build to the launcher.
  --help                     Show this help.

Examples:
  scripts/run_databento_calibration_batches.sh \
    --symbols TSLA,NVDA,AAPL,MSFT,AMZN \
    --start 2026-05-18 --end 2026-05-22 \
    --skip-build

  scripts/run_databento_calibration_batches.sh \
    --symbols-file runtime/symbols_100.txt \
    --start 2026-05-18 --end 2026-05-22 \
    --batch-size 5 --start-batch 1 --skip-build
USAGE
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

append_symbol() {
  local raw normalized existing
  raw="$1"
  normalized="$(trim "$raw" | tr '[:lower:]' '[:upper:]')"
  [[ -n "$normalized" ]] || return 0
  for existing in "${SYMBOLS[@]-}"; do
    [[ "$existing" == "$normalized" ]] && return 0
  done
  SYMBOLS+=("$normalized")
}

parse_symbols_csv() {
  local raw="$1" old_ifs token
  old_ifs="$IFS"
  IFS=','
  for token in $raw; do
    append_symbol "$token"
  done
  IFS="$old_ifs"
}

load_symbols_file() {
  local file="$1" line token
  [[ -f "$file" ]] || { echo "[CALIBRATION][ERROR] symbols file not found: $file" >&2; exit 2; }
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="${line//,/ }"
    for token in $line; do
      append_symbol "$token"
    done
  done < "$file"
}

join_csv() {
  local old_ifs="$IFS"
  IFS=','
  printf '%s' "$*"
  IFS="$old_ifs"
}

slugify_symbols() {
  printf '%s' "$1" | tr '[:upper:],.' '[:lower:]---' | tr -cs '[:alnum:]-' '-'
}

validate_window() {
  python3 - "$START_DATE" "$END_DATE" <<'PY'
import datetime as dt
import sys
start = dt.date.fromisoformat(sys.argv[1])
end = dt.date.fromisoformat(sys.argv[2])
if end < start:
    raise SystemExit("[CALIBRATION][ERROR] --end must be on/after --start")
weekdays = sum(1 for i in range((end - start).days + 1) if (start + dt.timedelta(days=i)).weekday() < 5)
if weekdays < 5:
    raise SystemExit(f"[CALIBRATION][ERROR] date window has only {weekdays} weekdays; use at least 5 trading days")
print(f"[CALIBRATION] date_window={start}..{end} weekdays={weekdays}")
PY
}

make_batches() {
  local total="${#SYMBOLS[@]}" i end chunk
  BATCHES=()
  i=0
  while [[ "$i" -lt "$total" ]]; do
    end=$((i + BATCH_SIZE))
    (( end > total )) && end="$total"
    chunk="$(join_csv "${SYMBOLS[@]:i:end-i}")"
    BATCHES+=("$chunk")
    i="$end"
  done
  if [[ "${#BATCHES[@]}" -gt 1 ]]; then
    local last_index=$(( ${#BATCHES[@]} - 1 ))
    local last="${BATCHES[$last_index]}"
    local last_count
    last_count="$(awk -F, '{print NF}' <<< "$last")"
    if [[ "$last_count" -lt 5 ]]; then
      local prev_index=$(( last_index - 1 ))
      BATCHES[$prev_index]="${BATCHES[$prev_index]},$last"
      unset 'BATCHES[$last_index]'
      BATCHES=("${BATCHES[@]}")
    fi
  fi
}

run_leg() {
  local leg="$1" batch_number="$2" symbols_csv="$3" slug out_dir run_id log status
  slug="$(slugify_symbols "$symbols_csv")"
  out_dir="$OUTPUT_BASE/${leg}-batch-$(printf '%03d' "$batch_number")-${slug}"
  mkdir -p "$out_dir"
  run_id="$(date +%Y%m%d_%H%M%S)"
  log="$out_dir/${leg}-batch-$(printf '%03d' "$batch_number")-${START_DATE}-to-${END_DATE}-${run_id}.log"

  echo "[CALIBRATION] leg=$leg batch=$batch_number symbols=$symbols_csv output_dir=$out_dir log=$log"

  local launcher_args=(
    --symbols "$symbols_csv"
    --start "$START_DATE"
    --end "$END_DATE"
    --timeout-seconds "$TIMEOUT_SECONDS"
    --python-bin "$PYTHON_BIN"
    --model-dir "$MODEL_DIR"
    --output-dir "$out_dir"
  )
  [[ "$DRY_RUN" == "true" ]] && launcher_args+=(--dry-run)
  [[ "$SKIP_BUILD" == "true" ]] && launcher_args+=(--skip-build)
  if [[ "$leg" != "disabled" ]]; then
    [[ -n "$MICRO_LONG_ENTRY_THRESHOLD" ]] && launcher_args+=(--micro-long-entry-threshold "$MICRO_LONG_ENTRY_THRESHOLD")
    [[ -n "$MICRO_SHORT_ENTRY_THRESHOLD" ]] && launcher_args+=(--micro-short-entry-threshold "$MICRO_SHORT_ENTRY_THRESHOLD")
    [[ -n "$LIFECYCLE_LONG_EXIT_THRESHOLD" ]] && launcher_args+=(--lifecycle-long-exit-threshold "$LIFECYCLE_LONG_EXIT_THRESHOLD")
    [[ -n "$LIFECYCLE_SHORT_EXIT_THRESHOLD" ]] && launcher_args+=(--lifecycle-short-exit-threshold "$LIFECYCLE_SHORT_EXIT_THRESHOLD")
    [[ -n "$MICRO_LONG_EXIT_GUARD_THRESHOLD" ]] && launcher_args+=(--micro-long-exit-guard-threshold "$MICRO_LONG_EXIT_GUARD_THRESHOLD")
    [[ -n "$MICRO_SHORT_EXIT_GUARD_THRESHOLD" ]] && launcher_args+=(--micro-short-exit-guard-threshold "$MICRO_SHORT_EXIT_GUARD_THRESHOLD")
  fi
  [[ "$leg" == "disabled" ]] && launcher_args+=(--disable-lifecycle-micro)

  local filter='\[BACKTEST\] symbol=|historical-api-symbol-begin|historical-api-symbol-complete|databento_api_key_source|model_dir=|micro_entry_thresholds|lifecycle_exit_thresholds|micro_exit_guard_thresholds|YESTERDAY_CLOSE_AVAILABLE=FAIL|YESTERDAY_CLOSE_AVAILABLE=PASS|PREVIOUS_CLOSE_AVAILABLE=FAIL|PREVIOUS_CLOSE_AVAILABLE=PASS|AI_PREDICTS_ENTRY=PASS|Armed long micro-entry|Armed short micro-entry|MICRO_ENTRY_CONFIRMS=PASS|MICRO_ENTRY_CONFIRMS=FAIL|reason=expired|simulated orderId|submitted orderId|Total trades|Total PnL|arms_total|arm_confirmations|arm_expirations|BACKTEST.NO_TRADE_DIAG|closest_setup rank=1|completed=|failed=|model directory not found|Traceback|401|CERTIFICATE_VERIFY_FAILED|ERROR|timed out'

  set +e
  DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS="$PREVIOUS_CLOSE_LOOKBACK_DAYS" \
    scripts/run_databento_historical_ibkr_sim_backtest.sh "${launcher_args[@]}" \
    2>&1 | tee "$log" | grep --line-buffered -E "$filter"
  status=${PIPESTATUS[0]}
  set -e

  if [[ -x scripts/summarize_databento_calibration_logs.py ]]; then
    local label
    label="$(printf '%s' "$leg" | tr '[:lower:]' '[:upper:]')"
    scripts/summarize_databento_calibration_logs.py --symbols "$symbols_csv" "$label=$log" || true
  fi
  return "$status"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbols) SYMBOLS_CSV="$2"; shift 2 ;;
    --symbols=*) SYMBOLS_CSV="${1#--symbols=}"; shift ;;
    --symbols-file) SYMBOLS_FILE="$2"; shift 2 ;;
    --symbols-file=*) SYMBOLS_FILE="${1#--symbols-file=}"; shift ;;
    --start|--start-date) START_DATE="$2"; shift 2 ;;
    --end|--end-date) END_DATE="$2"; shift 2 ;;
    --output-base) OUTPUT_BASE="$2"; shift 2 ;;
    --output-base=*) OUTPUT_BASE="${1#--output-base=}"; shift ;;
    --model-dir) MODEL_DIR="$2"; shift 2 ;;
    --model-dir=*) MODEL_DIR="${1#--model-dir=}"; shift ;;
    --python-bin) PYTHON_BIN="$2"; shift 2 ;;
    --python-bin=*) PYTHON_BIN="${1#--python-bin=}"; shift ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --timeout-seconds=*) TIMEOUT_SECONDS="${1#--timeout-seconds=}"; shift ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --batch-size=*) BATCH_SIZE="${1#--batch-size=}"; shift ;;
    --start-batch) START_BATCH="$2"; shift 2 ;;
    --start-batch=*) START_BATCH="${1#--start-batch=}"; shift ;;
    --max-batches) MAX_BATCHES="$2"; shift 2 ;;
    --max-batches=*) MAX_BATCHES="${1#--max-batches=}"; shift ;;
    --previous-close-lookback-days) PREVIOUS_CLOSE_LOOKBACK_DAYS="$2"; shift 2 ;;
    --previous-close-lookback-days=*) PREVIOUS_CLOSE_LOOKBACK_DAYS="${1#--previous-close-lookback-days=}"; shift ;;
    --micro-long-entry-threshold) MICRO_LONG_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-long-entry-threshold=*) MICRO_LONG_ENTRY_THRESHOLD="${1#--micro-long-entry-threshold=}"; shift ;;
    --micro-short-entry-threshold) MICRO_SHORT_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-short-entry-threshold=*) MICRO_SHORT_ENTRY_THRESHOLD="${1#--micro-short-entry-threshold=}"; shift ;;
    --lifecycle-exit-threshold) LIFECYCLE_LONG_EXIT_THRESHOLD="$2"; LIFECYCLE_SHORT_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-exit-threshold=*) LIFECYCLE_LONG_EXIT_THRESHOLD="${1#--lifecycle-exit-threshold=}"; LIFECYCLE_SHORT_EXIT_THRESHOLD="${1#--lifecycle-exit-threshold=}"; shift ;;
    --lifecycle-long-exit-threshold) LIFECYCLE_LONG_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-long-exit-threshold=*) LIFECYCLE_LONG_EXIT_THRESHOLD="${1#--lifecycle-long-exit-threshold=}"; shift ;;
    --lifecycle-short-exit-threshold) LIFECYCLE_SHORT_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-short-exit-threshold=*) LIFECYCLE_SHORT_EXIT_THRESHOLD="${1#--lifecycle-short-exit-threshold=}"; shift ;;
    --micro-exit-guard-threshold) MICRO_LONG_EXIT_GUARD_THRESHOLD="$2"; MICRO_SHORT_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-exit-guard-threshold=*) MICRO_LONG_EXIT_GUARD_THRESHOLD="${1#--micro-exit-guard-threshold=}"; MICRO_SHORT_EXIT_GUARD_THRESHOLD="${1#--micro-exit-guard-threshold=}"; shift ;;
    --micro-long-exit-guard-threshold) MICRO_LONG_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-long-exit-guard-threshold=*) MICRO_LONG_EXIT_GUARD_THRESHOLD="${1#--micro-long-exit-guard-threshold=}"; shift ;;
    --micro-short-exit-guard-threshold) MICRO_SHORT_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-short-exit-guard-threshold=*) MICRO_SHORT_EXIT_GUARD_THRESHOLD="${1#--micro-short-exit-guard-threshold=}"; shift ;;
    --enabled-only) MODE="enabled"; shift ;;
    --disabled-only) MODE="disabled"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "[CALIBRATION][ERROR] Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "[CALIBRATION][ERROR] --start and --end are required" >&2
  usage >&2
  exit 2
fi
if [[ "$BATCH_SIZE" -lt 5 ]]; then
  echo "[CALIBRATION][ERROR] --batch-size must be >= 5" >&2
  exit 2
fi
if [[ "$SYMBOLS_FILE" != /* ]]; then SYMBOLS_FILE="$ROOT/$SYMBOLS_FILE"; fi
if [[ "$OUTPUT_BASE" != /* ]]; then OUTPUT_BASE="$ROOT/$OUTPUT_BASE"; fi

validate_window
mkdir -p "$OUTPUT_BASE"

SYMBOLS=()
if [[ -n "$SYMBOLS_CSV" ]]; then
  parse_symbols_csv "$SYMBOLS_CSV"
else
  load_symbols_file "$SYMBOLS_FILE"
fi
if [[ "${#SYMBOLS[@]}" -lt 5 ]]; then
  echo "[CALIBRATION][ERROR] Need at least 5 symbols; got ${#SYMBOLS[@]}" >&2
  exit 2
fi
make_batches

echo "[CALIBRATION] symbols_count=${#SYMBOLS[@]} batch_count=${#BATCHES[@]} batch_size=$BATCH_SIZE mode=$MODE dry_run=$DRY_RUN"
failures=0
ran=0
for index in "${!BATCHES[@]}"; do
  batch_number=$((index + 1))
  [[ "$batch_number" -lt "$START_BATCH" ]] && continue
  if [[ "$MAX_BATCHES" -gt 0 && "$ran" -ge "$MAX_BATCHES" ]]; then
    break
  fi
  symbols_csv="${BATCHES[$index]}"
  echo "[CALIBRATION] ------------------------------------------------------------"
  echo "[CALIBRATION] batch=$batch_number/${#BATCHES[@]} symbols=$symbols_csv"
  if [[ "$MODE" == "enabled" || "$MODE" == "both" ]]; then
    if ! run_leg enabled "$batch_number" "$symbols_csv"; then
      failures=$((failures + 1))
      echo "[CALIBRATION][ERROR] enabled batch=$batch_number failed" >&2
    fi
  fi
  if [[ "$MODE" == "disabled" || "$MODE" == "both" ]]; then
    if ! run_leg disabled "$batch_number" "$symbols_csv"; then
      failures=$((failures + 1))
      echo "[CALIBRATION][ERROR] disabled batch=$batch_number failed" >&2
    fi
  fi
  ran=$((ran + 1))
done

echo "[CALIBRATION] completed_batches=$ran failures=$failures output_base=$OUTPUT_BASE"
exit "$failures"
