#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SYMBOL="${BACKTEST_SYMBOL:-TSLA}"
START_DATE="${START_DATE:-${CAL_START:-}}"
END_DATE="${END_DATE:-${CAL_END:-}}"
RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_BASE="${OUTPUT_BASE:-/tmp/trading-agent-exit-threshold-grid-${RUN_STAMP}}"
MODEL_DIR="${MODEL_DIR:-model_exports/20260524_022652}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-model_exports/lifecycle_micro_20260523}"
PYTHON_BIN="${PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-/tmp/trading-agent-databento-venv/bin/python3}}"
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
PREVIOUS_CLOSE_LOOKBACK_DAYS="${DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS:-14}"
MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
MAX_SHARE_CAP="${BACKTEST_MAX_SHARE_CAP:-500}"
MICRO_LONG_ENTRY_THRESHOLD="${MICRO_LONG_ENTRY_THRESHOLD:-${STRATEGY_MICRO_LONG_ENTRY_THRESHOLD:-}}"
MICRO_SHORT_ENTRY_THRESHOLD="${MICRO_SHORT_ENTRY_THRESHOLD:-${STRATEGY_MICRO_SHORT_ENTRY_THRESHOLD:-}}"
LIFECYCLE_EXIT_THRESHOLDS="0.45 0.50 0.55 0.60 0.65 0.70"
MICRO_EXIT_GUARD_THRESHOLDS="0.50 0.55 0.60 0.65 0.70 0.75"
DRY_RUN="false"
SKIP_BUILD="false"
RESUME="false"
RUN_DEFAULT_BASELINE="true"
SCALE_IF_PROFITABLE="false"
SCALE_SYMBOLS=""
SCALE_SYMBOLS_FILE=""
SCALE_OUTPUT_BASE=""
SCALE_BATCH_SIZE="5"
SCALE_TIMEOUT_SECONDS=""
SCALE_START_BATCH="1"
SCALE_MAX_BATCHES="0"
MIN_PNL_TO_SCALE="0.0"
MIN_TRADES_TO_SCALE="1"
CLASSPATH_FILE="${CLASSPATH_FILE:-}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_databento_exit_threshold_grid_search.sh --symbol SYMBOL --start YYYY-MM-DD --end YYYY-MM-DD [options]

Runs a one-symbol Databento historical IBKR simulation grid search for lifecycle-exit
and 5s micro-exit-guard thresholds while keeping the selected micro-entry thresholds
fixed. The default sweep is symmetric: one lifecycle threshold is used for both long
and short lifecycle exits, and one guard threshold is used for both long and short
micro-exit guards.

Default exit grid:
  lifecycle exit:   0.45 0.50 0.55 0.60 0.65 0.70
  micro exit guard: 0.50 0.55 0.60 0.65 0.70 0.75

Required unless already exported:
  --start YYYY-MM-DD              First replay day. Also reads START_DATE or CAL_START.
  --end YYYY-MM-DD                Last replay day. Also reads END_DATE or CAL_END.

Core options:
  --symbol SYMBOL                 Pilot symbol. Default: TSLA or BACKTEST_SYMBOL.
  --output-base DIR               Output root. Default: /tmp/trading-agent-exit-threshold-grid-<timestamp>
  --model-dir DIR                 30s model dir. Default: model_exports/20260524_022652
  --lifecycle-model-dir DIR       Lifecycle/micro model dir. Default: model_exports/lifecycle_micro_20260523
  --python-bin PATH               Python with databento+pandas. Default: /tmp/trading-agent-databento-venv/bin/python3
  --databento-env-file FILE       Env file containing DATABENTO_API_KEY, passed to the launcher.
  --classpath-file FILE           Shared Java classpath cache. Default: <output-base>/databento_ibkr_sim_backtest_cp.txt
  --timeout-seconds N             Per-run Databento timeout. Default: 3600
  --previous-close-lookback-days N Export DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS. Default: 14
  --max-trades N                  Strategy max trades during replay. Default: 2000
  --max-share-cap N               Simulated broker max shares per order. Default: 500

Fixed entry thresholds:
  --micro-long-entry-threshold P   Fixed 5s long micro-entry threshold. If omitted, launcher uses scorecard/default.
  --micro-short-entry-threshold P  Fixed 5s short micro-entry threshold. If omitted, launcher uses scorecard/default.

Exit grid options:
  --lifecycle-exit-thresholds "LIST"    Space/comma-separated symmetric lifecycle-exit thresholds.
  --micro-exit-guard-thresholds "LIST"  Space/comma-separated symmetric micro-exit-guard thresholds.
  --resume                              Skip combo dirs with completed successful logs.
  --default-baseline                    Also run lifecycle/micro enabled once with scorecard/default exit thresholds. Default: on.
  --no-default-baseline                 Disable the default baseline run.
  --dry-run                             Validate wiring without downloading Databento data.
  --skip-build                          Reuse existing target/classes and classpath.

Optional scale-up:
  --scale-if-profitable           If the best pilot combo is profitable, run enabled calibration batches.
  --scale-symbols CSV             Symbols for scale-up.
  --scale-symbols-file FILE       Symbols file for scale-up. Example: runtime/symbols_100.txt
  --scale-output-base DIR         Scale-up output root. Default: <output-base>/scale-best
  --scale-batch-size N            Batch size for scale-up. Default: 5
  --scale-timeout-seconds N       Per-symbol timeout for scale-up. Default: --timeout-seconds
  --scale-start-batch N           Resume scale-up from 1-based batch. Default: 1
  --scale-max-batches N           Stop scale-up after N batches. Default: 0 (all)
  --min-pnl-to-scale PNL          Required best pilot PnL for scale-up. Default: 0.0
  --min-trades-to-scale N         Required best pilot trade count for scale-up. Default: 1

Examples:
  scripts/run_databento_exit_threshold_grid_search.sh \
    --symbol TSLA \
    --start 2026-05-18 \
    --end 2026-05-22 \
    --micro-long-entry-threshold 0.12 \
    --micro-short-entry-threshold 0.10 \
    --skip-build

  scripts/run_databento_exit_threshold_grid_search.sh \
    --symbol TSLA \
    --start "$CAL_START" \
    --end "$CAL_END" \
    --micro-long-entry-threshold "$BEST_LONG_THRESHOLD" \
    --micro-short-entry-threshold "$BEST_SHORT_THRESHOLD" \
    --scale-if-profitable \
    --scale-symbols-file runtime/symbols_100.txt \
    --skip-build
USAGE
}

die() {
  echo "[EXIT-GRID][ERROR] $*" >&2
  exit 2
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

threshold_slug() {
  printf '%s' "$1" | tr '.' 'p' | tr -cs '[:alnum:]p+-' '-'
}

safe_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr -cs '[:alnum:].-' '_'
}

completed_successfully() {
  local dir="$1" log
  log="$(find "$dir" -maxdepth 1 -type f -name '*.log' -print 2>/dev/null | sort | tail -1 || true)"
  [[ -n "$log" ]] || return 1
  grep -q '\[BACKTEST\] completed=1 failed=0 requested=1' "$log"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbol) SYMBOL="$2"; shift 2 ;;
    --symbol=*) SYMBOL="${1#--symbol=}"; shift ;;
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
    --micro-long-entry-threshold) MICRO_LONG_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-long-entry-threshold=*) MICRO_LONG_ENTRY_THRESHOLD="${1#--micro-long-entry-threshold=}"; shift ;;
    --micro-short-entry-threshold) MICRO_SHORT_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-short-entry-threshold=*) MICRO_SHORT_ENTRY_THRESHOLD="${1#--micro-short-entry-threshold=}"; shift ;;
    --lifecycle-exit-thresholds) LIFECYCLE_EXIT_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --lifecycle-exit-thresholds=*) LIFECYCLE_EXIT_THRESHOLDS="$(normalize_list "${1#--lifecycle-exit-thresholds=}")"; shift ;;
    --micro-exit-guard-thresholds) MICRO_EXIT_GUARD_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --micro-exit-guard-thresholds=*) MICRO_EXIT_GUARD_THRESHOLDS="$(normalize_list "${1#--micro-exit-guard-thresholds=}")"; shift ;;
    --default-baseline) RUN_DEFAULT_BASELINE="true"; shift ;;
    --no-default-baseline) RUN_DEFAULT_BASELINE="false"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --resume) RESUME="true"; shift ;;
    --scale-if-profitable) SCALE_IF_PROFITABLE="true"; shift ;;
    --scale-symbols) SCALE_SYMBOLS="$2"; shift 2 ;;
    --scale-symbols=*) SCALE_SYMBOLS="${1#--scale-symbols=}"; shift ;;
    --scale-symbols-file) SCALE_SYMBOLS_FILE="$2"; shift 2 ;;
    --scale-symbols-file=*) SCALE_SYMBOLS_FILE="${1#--scale-symbols-file=}"; shift ;;
    --scale-output-base) SCALE_OUTPUT_BASE="$2"; shift 2 ;;
    --scale-output-base=*) SCALE_OUTPUT_BASE="${1#--scale-output-base=}"; shift ;;
    --scale-batch-size) SCALE_BATCH_SIZE="$2"; shift 2 ;;
    --scale-batch-size=*) SCALE_BATCH_SIZE="${1#--scale-batch-size=}"; shift ;;
    --scale-timeout-seconds) SCALE_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --scale-timeout-seconds=*) SCALE_TIMEOUT_SECONDS="${1#--scale-timeout-seconds=}"; shift ;;
    --scale-start-batch) SCALE_START_BATCH="$2"; shift 2 ;;
    --scale-start-batch=*) SCALE_START_BATCH="${1#--scale-start-batch=}"; shift ;;
    --scale-max-batches) SCALE_MAX_BATCHES="$2"; shift 2 ;;
    --scale-max-batches=*) SCALE_MAX_BATCHES="${1#--scale-max-batches=}"; shift ;;
    --min-pnl-to-scale) MIN_PNL_TO_SCALE="$2"; shift 2 ;;
    --min-pnl-to-scale=*) MIN_PNL_TO_SCALE="${1#--min-pnl-to-scale=}"; shift ;;
    --min-trades-to-scale) MIN_TRADES_TO_SCALE="$2"; shift 2 ;;
    --min-trades-to-scale=*) MIN_TRADES_TO_SCALE="${1#--min-trades-to-scale=}"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done

[[ -n "$SYMBOL" ]] || die "--symbol is required"
[[ -n "$START_DATE" ]] || die "--start is required, or export START_DATE/CAL_START"
[[ -n "$END_DATE" ]] || die "--end is required, or export END_DATE/CAL_END"
[[ -n "$LIFECYCLE_EXIT_THRESHOLDS" ]] || die "at least one lifecycle exit threshold is required"
[[ -n "$MICRO_EXIT_GUARD_THRESHOLDS" ]] || die "at least one micro exit guard threshold is required"

python3 - "$START_DATE" "$END_DATE" "$MIN_PNL_TO_SCALE" "$MIN_TRADES_TO_SCALE" <<'PY'
import datetime as dt
import sys
try:
    start = dt.date.fromisoformat(sys.argv[1])
    end = dt.date.fromisoformat(sys.argv[2])
except ValueError as exc:
    raise SystemExit(f"[EXIT-GRID][ERROR] invalid date: {exc}")
if end < start:
    raise SystemExit("[EXIT-GRID][ERROR] --end must be on/after --start")
try:
    float(sys.argv[3])
    int(sys.argv[4])
except ValueError as exc:
    raise SystemExit(f"[EXIT-GRID][ERROR] invalid scale gate: {exc}")
weekdays = sum(1 for i in range((end - start).days + 1) if (start + dt.timedelta(days=i)).weekday() < 5)
print(f"[EXIT-GRID] date_window={start}..{end} weekdays={weekdays}")
PY

SYMBOL="$(printf '%s' "$SYMBOL" | tr '[:lower:]' '[:upper:]')"
[[ "$OUTPUT_BASE" != /* ]] && OUTPUT_BASE="$ROOT/$OUTPUT_BASE"
[[ "$LIFECYCLE_MODEL_DIR" != /* ]] && LIFECYCLE_MODEL_DIR="$ROOT/$LIFECYCLE_MODEL_DIR"
[[ -d "$LIFECYCLE_MODEL_DIR" ]] || die "Lifecycle/micro model directory not found: $LIFECYCLE_MODEL_DIR"
mkdir -p "$OUTPUT_BASE"
CLASSPATH_FILE="${CLASSPATH_FILE:-$OUTPUT_BASE/databento_ibkr_sim_backtest_cp.txt}"
[[ "$CLASSPATH_FILE" != /* ]] && CLASSPATH_FILE="$ROOT/$CLASSPATH_FILE"

if [[ -z "$SCALE_TIMEOUT_SECONDS" ]]; then
  SCALE_TIMEOUT_SECONDS="$TIMEOUT_SECONDS"
fi
if [[ -z "$SCALE_OUTPUT_BASE" ]]; then
  SCALE_OUTPUT_BASE="$OUTPUT_BASE/scale-best"
fi

LIFECYCLE_COUNT="$(wc -w <<< "$LIFECYCLE_EXIT_THRESHOLDS" | tr -d ' ')"
GUARD_COUNT="$(wc -w <<< "$MICRO_EXIT_GUARD_THRESHOLDS" | tr -d ' ')"
TOTAL_COMBOS=$((LIFECYCLE_COUNT * GUARD_COUNT))
SAFE_SYMBOL="$(safe_symbol "$SYMBOL")"

cat <<SUMMARY
[EXIT-GRID] symbol=$SYMBOL start=$START_DATE end=$END_DATE dry_run=$DRY_RUN
[EXIT-GRID] output_base=$OUTPUT_BASE
[EXIT-GRID] model_dir=$MODEL_DIR
[EXIT-GRID] lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[EXIT-GRID] python_bin=$PYTHON_BIN
[EXIT-GRID] fixed_micro_entry_thresholds long=${MICRO_LONG_ENTRY_THRESHOLD:-<scorecard/default>} short=${MICRO_SHORT_ENTRY_THRESHOLD:-<scorecard/default>}
[EXIT-GRID] lifecycle_exit_thresholds=[$LIFECYCLE_EXIT_THRESHOLDS] micro_exit_guard_thresholds=[$MICRO_EXIT_GUARD_THRESHOLDS] combos=$TOTAL_COMBOS
SUMMARY

if [[ -z "$MICRO_LONG_ENTRY_THRESHOLD" || -z "$MICRO_SHORT_ENTRY_THRESHOLD" ]]; then
  echo "[EXIT-GRID][WARN] fixed micro-entry threshold(s) not supplied; launcher will use scorecard/default entry thresholds. For clean exit calibration, pass the best entry thresholds from the prior grid." >&2
fi

if ! truthy "$SKIP_BUILD"; then
  echo "[EXIT-GRID] building once before grid search"
  "$ROOT/mvnw" -q -DskipTests package
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
else
  echo "[EXIT-GRID] skip_build=true; launcher will reuse target/classes and classpath when possible"
fi

FILTER='\[BACKTEST\] symbol=|historical-api-symbol-begin|historical-api-symbol-complete|databento_api_key_source|model_dir=|micro_entry_thresholds|lifecycle_exit_thresholds|micro_exit_guard_thresholds|LIFECYCLE_EXIT_TRIGGERS=PASS|MICRO_EXIT_GUARD_TRIGGERS=PASS|simulated orderId|submitted orderId|Total trades|Total PnL|arms_total|arm_confirmations|guard_fires|lifecycle_exits|hard_risk_exits|eod_exits|completed=|failed=|model directory not found|Traceback|401|CERTIFICATE_VERIFY_FAILED|ERROR|timed out'

run_launcher() {
  local label="$1" lifecycle_threshold="$2" guard_threshold="$3" mode="$4" out_dir="$5" log="$6" status
  mkdir -p "$out_dir"

  local launcher_args=(
    --symbol "$SYMBOL"
    --start "$START_DATE"
    --end "$END_DATE"
    --timeout-seconds "$TIMEOUT_SECONDS"
    --python-bin "$PYTHON_BIN"
    --model-dir "$MODEL_DIR"
    --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR"
    --output-dir "$out_dir"
    --classpath-file "$CLASSPATH_FILE"
    --max-trades "$MAX_TRADES"
    --max-share-cap "$MAX_SHARE_CAP"
    --skip-build
  )
  [[ -n "$DATABENTO_ENV_FILE" ]] && launcher_args+=(--databento-env-file "$DATABENTO_ENV_FILE")
  [[ -n "$MICRO_LONG_ENTRY_THRESHOLD" ]] && launcher_args+=(--micro-long-entry-threshold "$MICRO_LONG_ENTRY_THRESHOLD")
  [[ -n "$MICRO_SHORT_ENTRY_THRESHOLD" ]] && launcher_args+=(--micro-short-entry-threshold "$MICRO_SHORT_ENTRY_THRESHOLD")
  truthy "$DRY_RUN" && launcher_args+=(--dry-run)
  if [[ "$mode" == "grid" ]]; then
    launcher_args+=(--lifecycle-long-exit-threshold "$lifecycle_threshold")
    launcher_args+=(--lifecycle-short-exit-threshold "$lifecycle_threshold")
    launcher_args+=(--micro-long-exit-guard-threshold "$guard_threshold")
    launcher_args+=(--micro-short-exit-guard-threshold "$guard_threshold")
  fi

  echo "[EXIT-GRID] ------------------------------------------------------------"
  echo "[EXIT-GRID] run=$label mode=$mode output_dir=$out_dir log=$log"
  if [[ "$mode" == "grid" ]]; then
    echo "[EXIT-GRID] thresholds lifecycle_exit=$lifecycle_threshold micro_exit_guard=$guard_threshold"
  fi

  set +e
  DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS="$PREVIOUS_CLOSE_LOOKBACK_DAYS" \
    scripts/run_databento_historical_ibkr_sim_backtest.sh "${launcher_args[@]}" \
    2>&1 | tee "$log" | grep --line-buffered -E "$FILTER"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$status" > "$out_dir/launcher.exit_status"
  if [[ "$status" -ne 0 ]]; then
    echo "[EXIT-GRID][WARN] run=$label failed status=$status log=$log" >&2
  fi
  return 0
}

if truthy "$RUN_DEFAULT_BASELINE"; then
  BASE_DIR="$OUTPUT_BASE/baseline-default"
  if truthy "$RESUME" && completed_successfully "$BASE_DIR"; then
    echo "[EXIT-GRID] resume=true; skipping completed baseline-default"
  else
    run_launcher "baseline-default" "" "" "default" "$BASE_DIR" "$BASE_DIR/${SAFE_SYMBOL}-${START_DATE}-to-${END_DATE}-baseline-default-${RUN_STAMP}.log"
  fi
fi

combo_index=0
for LE in $LIFECYCLE_EXIT_THRESHOLDS; do
  for GE in $MICRO_EXIT_GUARD_THRESHOLDS; do
    combo_index=$((combo_index + 1))
    LE_SLUG="$(threshold_slug "$LE")"
    GE_SLUG="$(threshold_slug "$GE")"
    COMBO_DIR="$OUTPUT_BASE/LE${LE_SLUG}-GE${GE_SLUG}"
    if truthy "$RESUME" && completed_successfully "$COMBO_DIR"; then
      echo "[EXIT-GRID] resume=true; skipping completed combo=$combo_index/$TOTAL_COMBOS lifecycle_exit=$LE micro_exit_guard=$GE"
      continue
    fi
    run_launcher "combo-$combo_index-of-$TOTAL_COMBOS" "$LE" "$GE" "grid" "$COMBO_DIR" "$COMBO_DIR/${SAFE_SYMBOL}-${START_DATE}-to-${END_DATE}-LE${LE_SLUG}-GE${GE_SLUG}-${RUN_STAMP}.log"
  done
done

echo "[EXIT-GRID] summarizing grid results"
python3 - "$OUTPUT_BASE" "$SYMBOL" "$MIN_PNL_TO_SCALE" "$MIN_TRADES_TO_SCALE" <<'PY'
from __future__ import annotations

import csv
import math
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
symbol = sys.argv[2]
min_pnl = float(sys.argv[3])
min_trades = int(sys.argv[4])

combo_re = re.compile(r"^LE(?P<lifecycle>[0-9p+-]+)-GE(?P<guard>[0-9p+-]+)$")
completed_re = re.compile(r"\[BACKTEST\] completed=(\d+) failed=(\d+) requested=(\d+)")
entry_threshold_re = re.compile(r"micro_entry_thresholds long=([^ ]+) short=([^\s]+)")
lifecycle_threshold_re = re.compile(r"lifecycle_exit_thresholds long=([^ ]+) short=([^\s]+)")
guard_threshold_re = re.compile(r"micro_exit_guard_thresholds long=([^ ]+) short=([^\s]+)")
total_trades_re = re.compile(r"Total trades:\s*([-+0-9.]+)")
total_pnl_re = re.compile(r"Total PnL:\s*([-+0-9.]+)")
error_re = re.compile(r"\[ERROR\]|\[BACKTEST\]\[ERROR\]|\bTraceback\b|\b401\b|CERTIFICATE_VERIFY_FAILED|timed out", re.IGNORECASE)


def de_slug(value: str) -> str:
    return value.replace("p", ".").strip("-")


def as_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def as_int(value: str | None, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except ValueError:
        return default


def latest_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return matches[-1] if matches else None


def read_lifecycle(directory: Path) -> dict[str, float | int | str]:
    path = latest_file(directory, "*-trade-lifecycle-summary.csv")
    result: dict[str, float | int | str] = {"lifecycle_csv": str(path or "")}
    if not path:
        return result
    with path.open(newline="", errors="replace") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return result
    closed = [row for row in rows if (row.get("ClosedTradeIndex") or "").strip()]
    pnls = [as_float(row.get("TradePnL")) for row in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    first = rows[0]
    result.update({
        "trades": len(closed),
        "pnl": sum(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(closed)) if closed else 0.0,
        "avg_pnl_per_trade": (sum(pnls) / len(closed)) if closed else 0.0,
        "best_trade": max(pnls) if pnls else 0.0,
        "worst_trade": min(pnls) if pnls else 0.0,
        "guard_evals": as_int(first.get("GuardEvaluations")),
        "guard_fires": as_int(first.get("GuardFires")),
        "lifecycle_exits": as_int(first.get("LifecycleExits")),
        "hard_risk_exits": as_int(first.get("HardRiskExits")),
        "eod_exits": as_int(first.get("EodExits")),
        "avg_mfe_r": as_float(first.get("AvgMfeR")),
        "avg_mae_r": as_float(first.get("AvgMaeR")),
        "exit_reason_distribution": first.get("ExitReasonDistribution", ""),
    })
    reason_counts: dict[str, int] = {}
    for row in closed:
        reason = (row.get("ExitReason") or "other").strip() or "other"
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    result["closed_lifecycle_exits"] = reason_counts.get("lifecycle", 0)
    result["closed_guard_exits"] = reason_counts.get("guard", 0)
    result["closed_hard_stop_exits"] = reason_counts.get("hard_stop", 0)
    result["closed_hard_risk_exits"] = reason_counts.get("hard_risk", 0)
    result["closed_eod_exits"] = reason_counts.get("eod", 0)
    return result


def read_log(directory: Path) -> dict[str, str | int]:
    path = latest_file(directory, "*.log")
    result: dict[str, str | int] = {"log": str(path or ""), "completed": "", "failed": "", "requested": "", "errors": 0}
    if not path:
        return result
    errors = 0
    with path.open(errors="replace") as handle:
        for line in handle:
            m = completed_re.search(line)
            if m:
                result["completed"], result["failed"], result["requested"] = m.groups()
            t = entry_threshold_re.search(line)
            if t:
                result["resolved_entry_long"], result["resolved_entry_short"] = t.groups()
            t = lifecycle_threshold_re.search(line)
            if t:
                result["resolved_lifecycle_long"], result["resolved_lifecycle_short"] = t.groups()
            t = guard_threshold_re.search(line)
            if t:
                result["resolved_guard_long"], result["resolved_guard_short"] = t.groups()
            if total_trades_re.search(line):
                result["log_total_trades"] = total_trades_re.search(line).group(1)  # type: ignore[union-attr]
            if total_pnl_re.search(line):
                result["log_total_pnl"] = total_pnl_re.search(line).group(1)  # type: ignore[union-attr]
            if error_re.search(line):
                errors += 1
    result["errors"] = errors
    return result


rows: list[dict[str, object]] = []
for directory in sorted(p for p in root.iterdir() if p.is_dir()):
    match = combo_re.match(directory.name)
    if match:
        lifecycle_threshold = de_slug(match.group("lifecycle"))
        guard_threshold = de_slug(match.group("guard"))
        kind = "grid"
    elif directory.name == "baseline-default":
        lifecycle_threshold = "default"
        guard_threshold = "default"
        kind = "baseline-default"
    else:
        continue

    row: dict[str, object] = {
        "kind": kind,
        "symbol": symbol,
        "lifecycle_exit_threshold": lifecycle_threshold,
        "micro_exit_guard_threshold": guard_threshold,
        "directory": str(directory),
    }
    row.update(read_lifecycle(directory))
    row.update(read_log(directory))
    if "trades" not in row:
        row["trades"] = as_int(str(row.get("log_total_trades", "") or ""))
    if "pnl" not in row:
        row["pnl"] = as_float(str(row.get("log_total_pnl", "") or ""))
    row["completed_ok"] = str(row.get("completed", "")) == "1" and str(row.get("failed", "")) == "0"
    row["profitable"] = bool(row["completed_ok"]) and as_float(str(row.get("pnl", "0"))) > 0 and as_int(str(row.get("trades", "0"))) > 0
    rows.append(row)

fields = [
    "rank", "kind", "symbol", "lifecycle_exit_threshold", "micro_exit_guard_threshold", "completed_ok", "profitable",
    "trades", "pnl", "avg_pnl_per_trade", "wins", "losses", "win_rate", "best_trade", "worst_trade",
    "guard_evals", "guard_fires", "lifecycle_exits", "hard_risk_exits", "eod_exits",
    "closed_lifecycle_exits", "closed_guard_exits", "closed_hard_stop_exits", "closed_hard_risk_exits", "closed_eod_exits",
    "avg_mfe_r", "avg_mae_r", "resolved_entry_long", "resolved_entry_short",
    "resolved_lifecycle_long", "resolved_lifecycle_short", "resolved_guard_long", "resolved_guard_short",
    "completed", "failed", "requested", "errors", "exit_reason_distribution", "directory", "log", "lifecycle_csv",
]

grid_rows = [row for row in rows if row.get("kind") == "grid"]
ranked_grid = sorted(
    grid_rows,
    key=lambda r: (
        bool(r.get("completed_ok")),
        as_float(str(r.get("pnl", "0"))),
        as_float(str(r.get("avg_pnl_per_trade", "0"))),
        -as_int(str(r.get("closed_hard_stop_exits", "0"))) - as_int(str(r.get("closed_hard_risk_exits", "0"))),
        as_int(str(r.get("trades", "0"))),
        -as_int(str(r.get("errors", "0"))),
    ),
    reverse=True,
)
rank_by_key = {
    (row.get("kind"), row.get("lifecycle_exit_threshold"), row.get("micro_exit_guard_threshold")): index
    for index, row in enumerate(ranked_grid, start=1)
}

results_csv = root / "exit_grid_results.csv"
with results_csv.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in sorted(rows, key=lambda r: (r.get("kind") != "grid", rank_by_key.get((r.get("kind"), r.get("lifecycle_exit_threshold"), r.get("micro_exit_guard_threshold")), 999999), str(r.get("kind")))):
        row = dict(row)
        row["rank"] = rank_by_key.get((row.get("kind"), row.get("lifecycle_exit_threshold"), row.get("micro_exit_guard_threshold")), "")
        writer.writerow(row)

top_tsv = root / "top_exit_thresholds.tsv"
with top_tsv.open("w") as handle:
    header = ["rank", "lifecycle_exit", "micro_exit_guard", "pnl", "trades", "avg_pnl", "win_rate", "guard_exits", "lifecycle_exits", "hard_stop", "hard_risk", "completed_ok", "errors"]
    handle.write("\t".join(header) + "\n")
    for index, row in enumerate(ranked_grid[:20], start=1):
        handle.write("\t".join([
            str(index),
            str(row.get("lifecycle_exit_threshold", "")),
            str(row.get("micro_exit_guard_threshold", "")),
            f"{as_float(str(row.get('pnl', '0'))):.2f}",
            str(as_int(str(row.get("trades", "0")))),
            f"{as_float(str(row.get('avg_pnl_per_trade', '0'))):.2f}",
            f"{as_float(str(row.get('win_rate', '0'))):.2%}",
            str(as_int(str(row.get("closed_guard_exits", "0")))),
            str(as_int(str(row.get("closed_lifecycle_exits", "0")))),
            str(as_int(str(row.get("closed_hard_stop_exits", "0")))),
            str(as_int(str(row.get("closed_hard_risk_exits", "0")))),
            str(row.get("completed_ok", "")),
            str(as_int(str(row.get("errors", "0")))),
        ]) + "\n")

successful_grid = [row for row in ranked_grid if row.get("completed_ok")]
best = successful_grid[0] if successful_grid else None
best_env = root / "best_exit_threshold.env"
with best_env.open("w") as handle:
    if best:
        best_pnl = as_float(str(best.get("pnl", "0")))
        best_trades = as_int(str(best.get("trades", "0")))
        qualifies = bool(best.get("completed_ok")) and best_pnl > min_pnl and best_trades >= min_trades
        lifecycle_threshold = best.get("lifecycle_exit_threshold", "")
        guard_threshold = best.get("micro_exit_guard_threshold", "")
        handle.write(f"BEST_LIFECYCLE_EXIT_THRESHOLD={lifecycle_threshold}\n")
        handle.write(f"BEST_MICRO_EXIT_GUARD_THRESHOLD={guard_threshold}\n")
        handle.write(f"BEST_PNL={best_pnl:.6f}\n")
        handle.write(f"BEST_TRADES={best_trades}\n")
        handle.write(f"BEST_COMPLETED_OK={str(bool(best.get('completed_ok'))).lower()}\n")
        handle.write(f"BEST_QUALIFIES_FOR_SCALE={str(qualifies).lower()}\n")
    else:
        handle.write("BEST_LIFECYCLE_EXIT_THRESHOLD=\nBEST_MICRO_EXIT_GUARD_THRESHOLD=\nBEST_PNL=0.000000\nBEST_TRADES=0\nBEST_COMPLETED_OK=false\nBEST_QUALIFIES_FOR_SCALE=false\n")

print(f"[EXIT-GRID] wrote_results_csv={results_csv}")
print(f"[EXIT-GRID] wrote_top_thresholds={top_tsv}")
print(f"[EXIT-GRID] wrote_best_env={best_env}")
print("\n[EXIT-GRID] TOP EXIT THRESHOLDS")
print(top_tsv.read_text())
if best:
    print(
        "[EXIT-GRID] best "
        f"lifecycle_exit={best.get('lifecycle_exit_threshold')} micro_exit_guard={best.get('micro_exit_guard_threshold')} "
        f"pnl={as_float(str(best.get('pnl', '0'))):.2f} trades={as_int(str(best.get('trades', '0')))} "
        f"completed_ok={best.get('completed_ok')}"
    )
else:
    print("[EXIT-GRID][WARN] no successful completed grid rows found")
PY

BEST_ENV="$OUTPUT_BASE/best_exit_threshold.env"
if [[ -f "$BEST_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$BEST_ENV"
else
  BEST_LIFECYCLE_EXIT_THRESHOLD=""
  BEST_MICRO_EXIT_GUARD_THRESHOLD=""
  BEST_PNL="0"
  BEST_TRADES="0"
  BEST_QUALIFIES_FOR_SCALE="false"
fi

if [[ -z "${BEST_LIFECYCLE_EXIT_THRESHOLD:-}" || -z "${BEST_MICRO_EXIT_GUARD_THRESHOLD:-}" ]]; then
  echo "[EXIT-GRID][WARN] no successful exit threshold pair completed; inspect $OUTPUT_BASE/exit_grid_results.csv before scaling"
fi

if truthy "$SCALE_IF_PROFITABLE"; then
  if ! truthy "${BEST_QUALIFIES_FOR_SCALE:-false}"; then
    echo "[EXIT-GRID] scale skipped: best threshold did not pass gates min_pnl=$MIN_PNL_TO_SCALE min_trades=$MIN_TRADES_TO_SCALE best_pnl=${BEST_PNL:-0} best_trades=${BEST_TRADES:-0}"
  elif [[ -z "$SCALE_SYMBOLS" && -z "$SCALE_SYMBOLS_FILE" ]]; then
    echo "[EXIT-GRID] scale skipped: provide --scale-symbols CSV or --scale-symbols-file FILE"
  else
    mkdir -p "$SCALE_OUTPUT_BASE"
    scale_args=(
      --start "$START_DATE"
      --end "$END_DATE"
      --output-base "$SCALE_OUTPUT_BASE"
      --model-dir "$MODEL_DIR"
      --python-bin "$PYTHON_BIN"
      --batch-size "$SCALE_BATCH_SIZE"
      --start-batch "$SCALE_START_BATCH"
      --max-batches "$SCALE_MAX_BATCHES"
      --timeout-seconds "$SCALE_TIMEOUT_SECONDS"
      --previous-close-lookback-days "$PREVIOUS_CLOSE_LOOKBACK_DAYS"
      --lifecycle-long-exit-threshold "$BEST_LIFECYCLE_EXIT_THRESHOLD"
      --lifecycle-short-exit-threshold "$BEST_LIFECYCLE_EXIT_THRESHOLD"
      --micro-long-exit-guard-threshold "$BEST_MICRO_EXIT_GUARD_THRESHOLD"
      --micro-short-exit-guard-threshold "$BEST_MICRO_EXIT_GUARD_THRESHOLD"
      --enabled-only
      --skip-build
    )
    [[ -n "$MICRO_LONG_ENTRY_THRESHOLD" ]] && scale_args+=(--micro-long-entry-threshold "$MICRO_LONG_ENTRY_THRESHOLD")
    [[ -n "$MICRO_SHORT_ENTRY_THRESHOLD" ]] && scale_args+=(--micro-short-entry-threshold "$MICRO_SHORT_ENTRY_THRESHOLD")
    [[ -n "$SCALE_SYMBOLS" ]] && scale_args+=(--symbols "$SCALE_SYMBOLS")
    [[ -n "$SCALE_SYMBOLS_FILE" ]] && scale_args+=(--symbols-file "$SCALE_SYMBOLS_FILE")
    truthy "$DRY_RUN" && scale_args+=(--dry-run)
    echo "[EXIT-GRID] scaling best exit thresholds lifecycle_exit=$BEST_LIFECYCLE_EXIT_THRESHOLD micro_exit_guard=$BEST_MICRO_EXIT_GUARD_THRESHOLD output_base=$SCALE_OUTPUT_BASE"
    scripts/run_databento_calibration_batches.sh "${scale_args[@]}"
  fi
else
  if [[ -n "${BEST_LIFECYCLE_EXIT_THRESHOLD:-}" && -n "${BEST_MICRO_EXIT_GUARD_THRESHOLD:-}" ]]; then
    cat <<NEXT
[EXIT-GRID] scale not requested. If the top row looks sane, scale it manually, for example:

scripts/run_databento_calibration_batches.sh \\
  --symbols-file runtime/symbols_100.txt \\
  --start "$START_DATE" \\
  --end "$END_DATE" \\
  --output-base "$OUTPUT_BASE/scale-best" \\
  --model-dir "$MODEL_DIR" \\
  --python-bin "$PYTHON_BIN" \\
  --batch-size 5 \\
  --enabled-only \\
  --micro-long-entry-threshold "${MICRO_LONG_ENTRY_THRESHOLD:-<best-long-entry>}" \\
  --micro-short-entry-threshold "${MICRO_SHORT_ENTRY_THRESHOLD:-<best-short-entry>}" \\
  --lifecycle-long-exit-threshold "${BEST_LIFECYCLE_EXIT_THRESHOLD:-}" \\
  --lifecycle-short-exit-threshold "${BEST_LIFECYCLE_EXIT_THRESHOLD:-}" \\
  --micro-long-exit-guard-threshold "${BEST_MICRO_EXIT_GUARD_THRESHOLD:-}" \\
  --micro-short-exit-guard-threshold "${BEST_MICRO_EXIT_GUARD_THRESHOLD:-}" \\
  --timeout-seconds "$TIMEOUT_SECONDS" \\
  --skip-build
NEXT
  else
    echo "[EXIT-GRID] scale not requested and no successful best threshold is available yet. Fix failed runs, then rerun with --resume."
  fi
fi

echo "[EXIT-GRID] done output_base=$OUTPUT_BASE"
