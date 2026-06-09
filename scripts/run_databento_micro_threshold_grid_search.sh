#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SYMBOL="${BACKTEST_SYMBOL:-TSLA}"
START_DATE="${START_DATE:-${CAL_START:-}}"
END_DATE="${END_DATE:-${CAL_END:-}}"
RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_BASE="${OUTPUT_BASE:-/tmp/trading-agent-micro-threshold-grid-${RUN_STAMP}}"
MODEL_DIR="${MODEL_DIR:-model_exports/20260524_022652}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-model_exports/lifecycle_micro_20260523}"
PYTHON_BIN="${PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-/tmp/trading-agent-databento-venv/bin/python3}}"
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"
PREVIOUS_CLOSE_LOOKBACK_DAYS="${DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS:-14}"
MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
MAX_SHARE_CAP="${BACKTEST_MAX_SHARE_CAP:-500}"
LONG_THRESHOLDS="0.32 0.35 0.38 0.40 0.42 0.45"
SHORT_THRESHOLDS="0.32 0.35 0.38 0.40 0.42 0.45"
DRY_RUN="false"
SKIP_BUILD="false"
RESUME="false"
RUN_DISABLED_BASELINE="false"
RUN_DEFAULT_BASELINE="false"
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
  scripts/run_databento_micro_threshold_grid_search.sh --symbol SYMBOL --start YYYY-MM-DD --end YYYY-MM-DD [options]

Runs a one-symbol Databento historical IBKR simulation grid search for lifecycle/micro
entry thresholds, ranks all long/short threshold pairs, and optionally scales the best
profitable pair through scripts/run_databento_calibration_batches.sh.

Default grid:
  long:  0.32 0.35 0.38 0.40 0.42 0.45
  short: 0.32 0.35 0.38 0.40 0.42 0.45

Required unless already exported:
  --start YYYY-MM-DD              First replay day. Also reads START_DATE or CAL_START.
  --end YYYY-MM-DD                Last replay day. Also reads END_DATE or CAL_END.

Core options:
  --symbol SYMBOL                 Pilot symbol. Default: TSLA or BACKTEST_SYMBOL.
  --output-base DIR               Output root. Default: /tmp/trading-agent-micro-threshold-grid-<timestamp>
  --model-dir DIR                 30s model dir. Default: model_exports/20260524_022652
  --lifecycle-model-dir DIR       Lifecycle/micro model dir. Default: model_exports/lifecycle_micro_20260523
  --python-bin PATH               Python with databento+pandas. Default: /tmp/trading-agent-databento-venv/bin/python3
  --databento-env-file FILE       Env file containing DATABENTO_API_KEY, passed to the launcher.
  --classpath-file FILE           Shared Java classpath cache. Default: <output-base>/databento_ibkr_sim_backtest_cp.txt
  --timeout-seconds N             Per-run Databento timeout. Default: 3600
  --previous-close-lookback-days N Export DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS. Default: 14
  --max-trades N                  Strategy max trades during replay. Default: 2000
  --max-share-cap N               Simulated broker max shares per order. Default: 500

Grid options:
  --long-thresholds "LIST"         Space/comma-separated long thresholds.
  --short-thresholds "LIST"        Space/comma-separated short thresholds.
  --thresholds "LIST"              Use the same space/comma-separated list for both sides.
  --resume                        Skip a combo directory that already has a completed successful log.
  --dry-run                       Validate wiring without downloading Databento data.
  --skip-build                    Reuse existing target/classes and classpath.
  --disabled-baseline             Also run lifecycle/micro disabled once.
  --default-baseline              Also run lifecycle/micro enabled once with scorecard/default thresholds.

Optional scale-up:
  --scale-if-profitable           If the best pilot pair is profitable, run enabled calibration batches.
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
  scripts/run_databento_micro_threshold_grid_search.sh \
    --symbol TSLA \
    --start 2026-05-18 \
    --end 2026-05-22 \
    --skip-build

  scripts/run_databento_micro_threshold_grid_search.sh \
    --symbol TSLA \
    --start "$CAL_START" \
    --end "$CAL_END" \
    --scale-if-profitable \
    --scale-symbols-file runtime/symbols_100.txt \
    --skip-build
USAGE
}

die() {
  echo "[GRID][ERROR] $*" >&2
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
    --long-thresholds) LONG_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --long-thresholds=*) LONG_THRESHOLDS="$(normalize_list "${1#--long-thresholds=}")"; shift ;;
    --short-thresholds) SHORT_THRESHOLDS="$(normalize_list "$2")"; shift 2 ;;
    --short-thresholds=*) SHORT_THRESHOLDS="$(normalize_list "${1#--short-thresholds=}")"; shift ;;
    --thresholds) LONG_THRESHOLDS="$(normalize_list "$2")"; SHORT_THRESHOLDS="$LONG_THRESHOLDS"; shift 2 ;;
    --thresholds=*) LONG_THRESHOLDS="$(normalize_list "${1#--thresholds=}")"; SHORT_THRESHOLDS="$LONG_THRESHOLDS"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --resume) RESUME="true"; shift ;;
    --disabled-baseline) RUN_DISABLED_BASELINE="true"; shift ;;
    --default-baseline) RUN_DEFAULT_BASELINE="true"; shift ;;
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
[[ -n "$LONG_THRESHOLDS" ]] || die "at least one long threshold is required"
[[ -n "$SHORT_THRESHOLDS" ]] || die "at least one short threshold is required"

python3 - "$START_DATE" "$END_DATE" "$MIN_PNL_TO_SCALE" "$MIN_TRADES_TO_SCALE" <<'PY'
import datetime as dt
import sys
try:
    start = dt.date.fromisoformat(sys.argv[1])
    end = dt.date.fromisoformat(sys.argv[2])
except ValueError as exc:
    raise SystemExit(f"[GRID][ERROR] invalid date: {exc}")
if end < start:
    raise SystemExit("[GRID][ERROR] --end must be on/after --start")
try:
    float(sys.argv[3])
    int(sys.argv[4])
except ValueError as exc:
    raise SystemExit(f"[GRID][ERROR] invalid scale gate: {exc}")
weekdays = sum(1 for i in range((end - start).days + 1) if (start + dt.timedelta(days=i)).weekday() < 5)
print(f"[GRID] date_window={start}..{end} weekdays={weekdays}")
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

LONG_COUNT="$(wc -w <<< "$LONG_THRESHOLDS" | tr -d ' ')"
SHORT_COUNT="$(wc -w <<< "$SHORT_THRESHOLDS" | tr -d ' ')"
TOTAL_COMBOS=$((LONG_COUNT * SHORT_COUNT))
SAFE_SYMBOL="$(safe_symbol "$SYMBOL")"

cat <<SUMMARY
[GRID] symbol=$SYMBOL start=$START_DATE end=$END_DATE dry_run=$DRY_RUN
[GRID] output_base=$OUTPUT_BASE
[GRID] model_dir=$MODEL_DIR
[GRID] lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[GRID] python_bin=$PYTHON_BIN
[GRID] thresholds long=[$LONG_THRESHOLDS] short=[$SHORT_THRESHOLDS] combos=$TOTAL_COMBOS
SUMMARY

if ! truthy "$SKIP_BUILD"; then
  echo "[GRID] building once before grid search"
  "$ROOT/mvnw" -q -DskipTests package
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
else
  echo "[GRID] skip_build=true; launcher will reuse target/classes and classpath when possible"
fi

FILTER='\[BACKTEST\] symbol=|historical-api-symbol-begin|historical-api-symbol-complete|databento_api_key_source|model_dir=|micro_entry_thresholds|YESTERDAY_CLOSE_AVAILABLE=FAIL|YESTERDAY_CLOSE_AVAILABLE=PASS|PREVIOUS_CLOSE_AVAILABLE=FAIL|PREVIOUS_CLOSE_AVAILABLE=PASS|AI_PREDICTS_ENTRY=PASS|Armed long micro-entry|Armed short micro-entry|MICRO_ENTRY_CONFIRMS=PASS|MICRO_ENTRY_CONFIRMS=FAIL|reason=expired|simulated orderId|submitted orderId|Total trades|Total PnL|arms_total|arm_confirmations|arm_expirations|BACKTEST.NO_TRADE_DIAG|closest_setup rank=1|completed=|failed=|model directory not found|Traceback|401|CERTIFICATE_VERIFY_FAILED|ERROR|timed out'

run_launcher() {
  local label="$1" long_threshold="$2" short_threshold="$3" mode="$4" out_dir="$5" log="$6" status
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
  truthy "$DRY_RUN" && launcher_args+=(--dry-run)
  if [[ "$mode" == "disabled" ]]; then
    launcher_args+=(--disable-lifecycle-micro)
  elif [[ -n "$long_threshold" && -n "$short_threshold" ]]; then
    launcher_args+=(--micro-long-entry-threshold "$long_threshold")
    launcher_args+=(--micro-short-entry-threshold "$short_threshold")
  fi

  echo "[GRID] ------------------------------------------------------------"
  echo "[GRID] run=$label mode=$mode output_dir=$out_dir log=$log"
  if [[ -n "$long_threshold" && -n "$short_threshold" ]]; then
    echo "[GRID] thresholds long=$long_threshold short=$short_threshold"
  fi

  set +e
  DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS="$PREVIOUS_CLOSE_LOOKBACK_DAYS" \
    scripts/run_databento_historical_ibkr_sim_backtest.sh "${launcher_args[@]}" \
    2>&1 | tee "$log" | grep --line-buffered -E "$FILTER"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$status" > "$out_dir/launcher.exit_status"
  if [[ "$status" -ne 0 ]]; then
    echo "[GRID][WARN] run=$label failed status=$status log=$log" >&2
  fi
  return 0
}

if truthy "$RUN_DISABLED_BASELINE"; then
  BASE_DIR="$OUTPUT_BASE/baseline-disabled"
  if truthy "$RESUME" && completed_successfully "$BASE_DIR"; then
    echo "[GRID] resume=true; skipping completed baseline-disabled"
  else
    run_launcher "baseline-disabled" "" "" "disabled" "$BASE_DIR" "$BASE_DIR/${SAFE_SYMBOL}-${START_DATE}-to-${END_DATE}-baseline-disabled-${RUN_STAMP}.log"
  fi
fi

if truthy "$RUN_DEFAULT_BASELINE"; then
  BASE_DIR="$OUTPUT_BASE/baseline-default"
  if truthy "$RESUME" && completed_successfully "$BASE_DIR"; then
    echo "[GRID] resume=true; skipping completed baseline-default"
  else
    run_launcher "baseline-default" "" "" "default" "$BASE_DIR" "$BASE_DIR/${SAFE_SYMBOL}-${START_DATE}-to-${END_DATE}-baseline-default-${RUN_STAMP}.log"
  fi
fi

combo_index=0
for L in $LONG_THRESHOLDS; do
  for S in $SHORT_THRESHOLDS; do
    combo_index=$((combo_index + 1))
    L_SLUG="$(threshold_slug "$L")"
    S_SLUG="$(threshold_slug "$S")"
    COMBO_DIR="$OUTPUT_BASE/L${L_SLUG}-S${S_SLUG}"
    if truthy "$RESUME" && completed_successfully "$COMBO_DIR"; then
      echo "[GRID] resume=true; skipping completed combo=$combo_index/$TOTAL_COMBOS long=$L short=$S"
      continue
    fi
    run_launcher "combo-$combo_index-of-$TOTAL_COMBOS" "$L" "$S" "enabled" "$COMBO_DIR" "$COMBO_DIR/${SAFE_SYMBOL}-${START_DATE}-to-${END_DATE}-L${L_SLUG}-S${S_SLUG}-${RUN_STAMP}.log"
  done
done

echo "[GRID] summarizing grid results"
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

combo_re = re.compile(r"^L(?P<long>[0-9p+-]+)-S(?P<short>[0-9p+-]+)$")
completed_re = re.compile(r"\[BACKTEST\] completed=(\d+) failed=(\d+) requested=(\d+)")
threshold_re = re.compile(r"micro_entry_thresholds long=([^ ]+) short=([^\s]+)")
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
        "arms_total": as_int(first.get("ArmsTotal")),
        "arms_long": as_int(first.get("ArmsLong")),
        "arms_short": as_int(first.get("ArmsShort")),
        "confirmations": as_int(first.get("ArmConfirmations")),
        "expirations": as_int(first.get("ArmExpirations")),
        "conversion_rate": as_float(first.get("ArmConversionRate")),
        "guard_evals": as_int(first.get("GuardEvaluations")),
        "guard_fires": as_int(first.get("GuardFires")),
        "lifecycle_exits": as_int(first.get("LifecycleExits")),
        "hard_risk_exits": as_int(first.get("HardRiskExits")),
        "eod_exits": as_int(first.get("EodExits")),
        "avg_setup_to_fill_seconds": as_float(first.get("AvgSetupToFillSeconds")),
        "avg_mfe_r": as_float(first.get("AvgMfeR")),
        "avg_mae_r": as_float(first.get("AvgMaeR")),
    })
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
            t = threshold_re.search(line)
            if t:
                result["resolved_long"], result["resolved_short"] = t.groups()
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
        long_threshold = de_slug(match.group("long"))
        short_threshold = de_slug(match.group("short"))
        kind = "grid"
    elif directory.name == "baseline-disabled":
        long_threshold = ""
        short_threshold = ""
        kind = "baseline-disabled"
    elif directory.name == "baseline-default":
        long_threshold = "default"
        short_threshold = "default"
        kind = "baseline-default"
    else:
        continue

    row: dict[str, object] = {
        "kind": kind,
        "symbol": symbol,
        "long_threshold": long_threshold,
        "short_threshold": short_threshold,
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
    "rank", "kind", "symbol", "long_threshold", "short_threshold", "completed_ok", "profitable",
    "trades", "pnl", "avg_pnl_per_trade", "wins", "losses", "win_rate", "best_trade", "worst_trade",
    "arms_total", "arms_long", "arms_short", "confirmations", "expirations", "conversion_rate",
    "guard_evals", "guard_fires", "lifecycle_exits", "hard_risk_exits", "eod_exits",
    "avg_setup_to_fill_seconds", "avg_mfe_r", "avg_mae_r", "resolved_long", "resolved_short",
    "completed", "failed", "requested", "errors", "directory", "log", "lifecycle_csv",
]

grid_rows = [row for row in rows if row.get("kind") == "grid"]
ranked_grid = sorted(
    grid_rows,
    key=lambda r: (
        bool(r.get("completed_ok")),
        as_float(str(r.get("pnl", "0"))),
        as_float(str(r.get("avg_pnl_per_trade", "0"))),
        as_int(str(r.get("trades", "0"))),
        -as_int(str(r.get("errors", "0"))),
    ),
    reverse=True,
)
rank_by_key = {
    (row.get("kind"), row.get("long_threshold"), row.get("short_threshold")): index
    for index, row in enumerate(ranked_grid, start=1)
}

results_csv = root / "grid_results.csv"
with results_csv.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in sorted(rows, key=lambda r: (r.get("kind") != "grid", rank_by_key.get((r.get("kind"), r.get("long_threshold"), r.get("short_threshold")), 999999), str(r.get("kind")))):
        row = dict(row)
        row["rank"] = rank_by_key.get((row.get("kind"), row.get("long_threshold"), row.get("short_threshold")), "")
        writer.writerow(row)

top_tsv = root / "top_thresholds.tsv"
with top_tsv.open("w") as handle:
    header = ["rank", "long", "short", "pnl", "trades", "avg_pnl", "win_rate", "arms", "confirmations", "conversion", "guard_fires", "completed_ok", "errors"]
    handle.write("\t".join(header) + "\n")
    for index, row in enumerate(ranked_grid[:20], start=1):
        handle.write("\t".join([
            str(index),
            str(row.get("long_threshold", "")),
            str(row.get("short_threshold", "")),
            f"{as_float(str(row.get('pnl', '0'))):.2f}",
            str(as_int(str(row.get("trades", "0")))),
            f"{as_float(str(row.get('avg_pnl_per_trade', '0'))):.2f}",
            f"{as_float(str(row.get('win_rate', '0'))):.2%}",
            str(as_int(str(row.get("arms_total", "0")))),
            str(as_int(str(row.get("confirmations", "0")))),
            f"{as_float(str(row.get('conversion_rate', '0'))):.2%}",
            str(as_int(str(row.get("guard_fires", "0")))),
            str(row.get("completed_ok", "")),
            str(as_int(str(row.get("errors", "0")))),
        ]) + "\n")

successful_grid = [row for row in ranked_grid if row.get("completed_ok")]
best = successful_grid[0] if successful_grid else None
best_env = root / "best_threshold.env"
with best_env.open("w") as handle:
    if best:
        best_pnl = as_float(str(best.get("pnl", "0")))
        best_trades = as_int(str(best.get("trades", "0")))
        qualifies = bool(best.get("completed_ok")) and best_pnl > min_pnl and best_trades >= min_trades
        handle.write(f"BEST_LONG_THRESHOLD={best.get('long_threshold', '')}\n")
        handle.write(f"BEST_SHORT_THRESHOLD={best.get('short_threshold', '')}\n")
        handle.write(f"BEST_PNL={best_pnl:.6f}\n")
        handle.write(f"BEST_TRADES={best_trades}\n")
        handle.write(f"BEST_COMPLETED_OK={str(bool(best.get('completed_ok'))).lower()}\n")
        handle.write(f"BEST_QUALIFIES_FOR_SCALE={str(qualifies).lower()}\n")
    else:
        handle.write("BEST_LONG_THRESHOLD=\nBEST_SHORT_THRESHOLD=\nBEST_PNL=0.000000\nBEST_TRADES=0\nBEST_COMPLETED_OK=false\nBEST_QUALIFIES_FOR_SCALE=false\n")

print(f"[GRID] wrote_results_csv={results_csv}")
print(f"[GRID] wrote_top_thresholds={top_tsv}")
print(f"[GRID] wrote_best_env={best_env}")
print("\n[GRID] TOP THRESHOLDS")
print(top_tsv.read_text())
if best:
    print(
        "[GRID] best "
        f"long={best.get('long_threshold')} short={best.get('short_threshold')} "
        f"pnl={as_float(str(best.get('pnl', '0'))):.2f} trades={as_int(str(best.get('trades', '0')))} "
        f"completed_ok={best.get('completed_ok')}"
    )
else:
    print("[GRID][WARN] no successful completed grid rows found")
PY

BEST_ENV="$OUTPUT_BASE/best_threshold.env"
if [[ -f "$BEST_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$BEST_ENV"
else
  BEST_LONG_THRESHOLD=""
  BEST_SHORT_THRESHOLD=""
  BEST_PNL="0"
  BEST_TRADES="0"
  BEST_QUALIFIES_FOR_SCALE="false"
fi

if [[ -z "${BEST_LONG_THRESHOLD:-}" || -z "${BEST_SHORT_THRESHOLD:-}" ]]; then
  echo "[GRID][WARN] no successful threshold pair completed; inspect $OUTPUT_BASE/grid_results.csv before scaling"
fi

if truthy "$SCALE_IF_PROFITABLE"; then
  if ! truthy "${BEST_QUALIFIES_FOR_SCALE:-false}"; then
    echo "[GRID] scale skipped: best threshold did not pass gates min_pnl=$MIN_PNL_TO_SCALE min_trades=$MIN_TRADES_TO_SCALE best_pnl=${BEST_PNL:-0} best_trades=${BEST_TRADES:-0}"
  elif [[ -z "$SCALE_SYMBOLS" && -z "$SCALE_SYMBOLS_FILE" ]]; then
    echo "[GRID] scale skipped: provide --scale-symbols CSV or --scale-symbols-file FILE"
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
      --micro-long-entry-threshold "$BEST_LONG_THRESHOLD"
      --micro-short-entry-threshold "$BEST_SHORT_THRESHOLD"
      --enabled-only
      --skip-build
    )
    [[ -n "$SCALE_SYMBOLS" ]] && scale_args+=(--symbols "$SCALE_SYMBOLS")
    [[ -n "$SCALE_SYMBOLS_FILE" ]] && scale_args+=(--symbols-file "$SCALE_SYMBOLS_FILE")
    truthy "$DRY_RUN" && scale_args+=(--dry-run)
    echo "[GRID] scaling best thresholds long=$BEST_LONG_THRESHOLD short=$BEST_SHORT_THRESHOLD output_base=$SCALE_OUTPUT_BASE"
    scripts/run_databento_calibration_batches.sh "${scale_args[@]}"
  fi
else
  if [[ -n "${BEST_LONG_THRESHOLD:-}" && -n "${BEST_SHORT_THRESHOLD:-}" ]]; then
    cat <<NEXT
[GRID] scale not requested. If the top row looks sane, scale it manually, for example:

scripts/run_databento_calibration_batches.sh \\
  --symbols-file runtime/symbols_100.txt \\
  --start "$START_DATE" \\
  --end "$END_DATE" \\
  --output-base "$OUTPUT_BASE/scale-best" \\
  --model-dir "$MODEL_DIR" \\
  --python-bin "$PYTHON_BIN" \\
  --batch-size 5 \\
  --enabled-only \\
  --micro-long-entry-threshold "${BEST_LONG_THRESHOLD:-}" \\
  --micro-short-entry-threshold "${BEST_SHORT_THRESHOLD:-}" \\
  --timeout-seconds "$TIMEOUT_SECONDS" \\
  --skip-build
NEXT
  else
    echo "[GRID] scale not requested and no successful best threshold is available yet. Fix failed runs, then rerun with --resume."
  fi
fi

echo "[GRID] done output_base=$OUTPUT_BASE"
