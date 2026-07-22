#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SOURCE_EVENTS="${SOURCE_EVENTS:-runtime/replay/databento-20260523-core5.ndjson.gz}"
START="${START:-auto}"
END="${END:-auto}"
SYMBOLS="${SYMBOLS:-NVDA,QQQ,SPY,TQQQ,TSLA}"
SLICE_SYMBOLS="${SLICE_SYMBOLS:-TSLA,TQQQ,NVDA,SPY,QQQ}"
INPUT_30S_CSV="${INPUT_30S_CSV:?Set INPUT_30S_CSV to combined_30s.csv or a directory of *_30s_training.csv files.}"
ROUTE_MANIFEST="${ROUTE_MANIFEST:-runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json}"
SETUP_MODEL_DIR="${SETUP_MODEL_DIR:-runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854}"
LIFECYCLE_MODEL_DIR="${LIFECYCLE_MODEL_DIR:-runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports}"
OUT_ROOT="${OUT_ROOT:-runtime/local-backtests}"
RUN_NOTRADE="${RUN_NOTRADE:-1}"
RUN_TRADE="${RUN_TRADE:-1}"
REBUILD_EXISTING="${REBUILD_EXISTING:-0}"

for required in "$SOURCE_EVENTS" "$INPUT_30S_CSV" "$ROUTE_MANIFEST" "$SETUP_MODEL_DIR" "$LIFECYCLE_MODEL_DIR"; do
  if [[ ! -e "$required" ]]; then
    echo "[WHOLE_RANGE][ERROR] missing required path: $required" >&2
    exit 2
  fi
done

if [[ "$START" == "auto" || "$END" == "auto" ]]; then
  mapfile -t AUTO_RANGE < <("$PYTHON_BIN" - "$SOURCE_EVENTS" <<'PY'
import gzip
import re
import sys
from datetime import date

source = sys.argv[1]
day_re = re.compile(r"\bday=(\d{8})\b")
days = set()
open_fn = gzip.open if source.endswith(".gz") else open
with open_fn(source, "rt", encoding="utf-8", errors="ignore") as stream:
    for line in stream:
        match = day_re.search(line)
        if not match:
            continue
        raw = match.group(1)
        days.add(date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])))
if len(days) < 2:
    raise SystemExit(f"not enough session markers in {source}: {len(days)}")
ordered = sorted(days)
# Use the second session by default so the builder can observe a prior close for START.
print(ordered[1].isoformat())
print(ordered[-1].isoformat())
PY
  )
  [[ "$START" == "auto" ]] && START="${AUTO_RANGE[0]}"
  [[ "$END" == "auto" ]] && END="${AUTO_RANGE[1]}"
fi

START_TAG="$(printf '%s' "$START" | tr -d '-')"
END_TAG="$(printf '%s' "$END" | tr -d '-')"
BASE="${BASE:-$OUT_ROOT/databento-core5-whole-${START_TAG}-${END_TAG}-event-carried}"
SLICE="$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.ndjson.gz"
SLICE_MANIFEST="$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.manifest.json"
RUN_DIR="$BASE/run"
RUN_TS="${RUN_TS:-$(date +%Y%m%d_%H%M%S)}"
RUN_LOG="$RUN_DIR/databento-core5-whole-${START_TAG}-${END_TAG}-${RUN_TS}.log"
CF_DIR="$BASE/setup_micro_counterfactual_$RUN_TS"
SIDECAR="$CF_DIR/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv"
ENRICHED_EVENTS="$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz"
NO_TRADE_OUT="$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade"
TRADE_OUT="$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl"
SUMMARY_PREFIX="$TRADE_OUT/pnl_period_summary_${START_TAG}_${END_TAG}"

mkdir -p "$BASE" "$RUN_DIR"

TRADE_JAVA_TOOL_OPTIONS="-Dbacktest.strategy.tradeAmount=60000 -Dtrading.trade-amount=60000 -Dtrading.risk.max-order-notional=70000 -Dbacktest.strategy.maxOrderNotional=70000"
if [[ " ${JAVA_TOOL_OPTIONS:-} " != *" -Dbacktest.strategy.tradeAmount="* ]]; then
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }$TRADE_JAVA_TOOL_OPTIONS"
fi

cat <<SUMMARY
[WHOLE_RANGE] root=$ROOT
[WHOLE_RANGE] source_events=$SOURCE_EVENTS
[WHOLE_RANGE] start=$START end=$END
[WHOLE_RANGE] base=$BASE
[WHOLE_RANGE] input_30s_csv=$INPUT_30S_CSV
[WHOLE_RANGE] route_manifest=$ROUTE_MANIFEST
[WHOLE_RANGE] setup_model_dir=$SETUP_MODEL_DIR
[WHOLE_RANGE] lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[WHOLE_RANGE] java_tool_options=${JAVA_TOOL_OPTIONS:-}
SUMMARY

run_backtest() {
  local log_file="$1"
  shift
  set +e
  PYTHON_BIN="$PYTHON_BIN" SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh "$@" > "$log_file" 2>&1
  local rc=$?
  echo "BACKTEST_RC=$rc" | tee -a "$log_file"
  set -e
  return "$rc"
}

echo "[WHOLE_RANGE] building Java once"
./mvnw -q -DskipTests package
./mvnw -q dependency:build-classpath -Dmdep.outputFile=runtime/backtests/databento_ibkr_sim_backtest_cp.txt

if [[ "$REBUILD_EXISTING" == "1" || ! -f "$SLICE" || ! -f "$SLICE_MANIFEST" ]]; then
  echo "[WHOLE_RANGE] building slice: $SLICE"
  "$PYTHON_BIN" runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/build_4week_slice_with_daily_prevclose.py \
    --root "$ROOT" \
    --input-events "$SOURCE_EVENTS" \
    --out-dir "$BASE" \
    --output-events "$SLICE" \
    --manifest "$SLICE_MANIFEST" \
    --symbols "$SLICE_SYMBOLS" \
    --start "$START" \
    --end "$END" \
    > "$BASE/build_slice.log" 2>&1
else
  echo "[WHOLE_RANGE] reusing slice: $SLICE"
fi

echo "[WHOLE_RANGE] validating slice"
"$PYTHON_BIN" runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/validate_4week_slice.py \
  --slice "$SLICE" \
  --manifest "$SLICE_MANIFEST" \
  --symbols "$SLICE_SYMBOLS" \
  --first-events 0 \
  > "$BASE/validate_slice.log" 2>&1

if [[ "$REBUILD_EXISTING" == "1" || ! -f "$SIDECAR" ]]; then
  mkdir -p "$CF_DIR"
  echo "[WHOLE_RANGE] running normal replay for counterfactual labels: $RUN_LOG"
  run_backtest "$RUN_LOG" \
    --source ndjson \
    --recorded-events "$SLICE" \
    --symbols "$SYMBOLS" \
    --start "$START" \
    --end "$END" \
    --output-dir "$RUN_DIR" \
    --model-dir "$SETUP_MODEL_DIR" \
    --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR" \
    --timeout-seconds 0 \
    --max-trades 2000

  echo "[WHOLE_RANGE] generating setup/micro counterfactual labels"
  "$PYTHON_BIN" -u scripts/generate_setup_micro_counterfactual_report.py \
    --log "$RUN_LOG" \
    --recorded-events "$SLICE" \
    --output-dir "$CF_DIR" \
    --label-min-micro-prob 0.30 \
    --min-expected-net-r 0.0 \
    --micro-threshold-grid "0.20 0.25 0.30 0.35 0.40 0.45 0.50 0.52 0.55 0.57 0.58 0.60 0.62 0.64" \
    --horizon-seconds 600 \
    --profit-pct 0.0035 \
    --risk-pct 0.0025 \
    --entry-slippage-bps 2.0 \
    --exit-slippage-bps 2.0 \
    --default-spread-bps 0.0 \
    --round-trip-commission-bps 0.0 \
    --fill-probability 0.98 \
    --partial-fill-penalty-r 0.02 \
    --missed-fill-penalty-r 0.05 \
    > "$CF_DIR/generate_setup_micro_counterfactual_report.log" 2>&1

  echo "[WHOLE_RANGE] building downstream setup training rows sidecar"
  "$PYTHON_BIN" -u scripts/build_downstream_setup_training_rows.py \
    --input-30s-csv "$INPUT_30S_CSV" \
    --labels-csv "$CF_DIR/setup_downstream_confirmable_labels_v1.csv" \
    --output-dir "$CF_DIR/downstream_setup_training_rows" \
    --join-tolerance-seconds 31 \
    > "$CF_DIR/build_downstream_setup_training_rows.log" 2>&1
else
  echo "[WHOLE_RANGE] reusing sidecar: $SIDECAR"
fi

if [[ ! -f "$SIDECAR" ]]; then
  echo "[WHOLE_RANGE][ERROR] missing sidecar: $SIDECAR" >&2
  exit 3
fi

if [[ "$REBUILD_EXISTING" == "1" || ! -f "$ENRICHED_EVENTS" ]]; then
  echo "[WHOLE_RANGE] injecting event-carried snapshots: $ENRICHED_EVENTS"
  "$PYTHON_BIN" scripts/inject_downstream_setup_features_into_ndjson.py \
    --input-events "$SLICE" \
    --sidecar-csv "$SIDECAR" \
    --manifest "$ROUTE_MANIFEST" \
    --output-events "$ENRICHED_EVENTS" \
    --snapshot-source setup_downstream_training_rows_v1 \
    > "$BASE/inject_event_snapshots.log" 2>&1
else
  echo "[WHOLE_RANGE] reusing enriched events: $ENRICHED_EVENTS"
fi

if [[ "$RUN_NOTRADE" == "1" ]]; then
  echo "[WHOLE_RANGE] running no-trade event-carried validation"
  rm -rf "$NO_TRADE_OUT"
  mkdir -p "$NO_TRADE_OUT"
  run_backtest "$NO_TRADE_OUT/controlled_java_replay.log" \
    --symbols "$SYMBOLS" \
    --source ndjson \
    --recorded-events "$ENRICHED_EVENTS" \
    --output-dir "$NO_TRADE_OUT" \
    --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
    --micro-long-entry-threshold 0.30 \
    --micro-short-entry-threshold 0.30 \
    --micro-entry-research-no-trade \
    --timeout-seconds 0

  "$PYTHON_BIN" scripts/compare_event_snapshot_replay_drift.py \
    --event-log "$NO_TRADE_OUT/controlled_java_replay.log" \
    --sidecar-csv "$SIDECAR" \
    --output-dir "$NO_TRADE_OUT/event_snapshot_replay_drift" \
    --strict-no-trade \
    --require-sidecar-disabled \
    --min-feature-snapshot-hit-rate 1.0 \
    --fail-on-no-go \
    > "$NO_TRADE_OUT/event_snapshot_replay_drift.log" 2>&1
fi

if [[ "$RUN_TRADE" == "1" ]]; then
  echo "[WHOLE_RANGE] running trade-enabled event-carried replay"
  rm -rf "$TRADE_OUT"
  mkdir -p "$TRADE_OUT"
  run_backtest "$TRADE_OUT/controlled_java_replay.log" \
    --symbols "$SYMBOLS" \
    --source ndjson \
    --recorded-events "$ENRICHED_EVENTS" \
    --output-dir "$TRADE_OUT" \
    --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
    --micro-long-entry-threshold 0.30 \
    --micro-short-entry-threshold 0.30 \
    --timeout-seconds 0 \
    --max-trades 2000

  echo "[WHOLE_RANGE] summarizing PnL periods"
  "$PYTHON_BIN" scripts/summarize_trade_lifecycle_periods.py \
    --output-dir "$TRADE_OUT" \
    --log-file "$TRADE_OUT/controlled_java_replay.log" \
    --out-prefix "$SUMMARY_PREFIX" \
    --title "Whole-range event-carried trade PnL $START to $END" \
    > "$TRADE_OUT/summarize_trade_lifecycle_periods.log" 2>&1
fi

cat <<DONE
[WHOLE_RANGE] DONE
[WHOLE_RANGE] base=$BASE
[WHOLE_RANGE] slice=$SLICE
[WHOLE_RANGE] sidecar=$SIDECAR
[WHOLE_RANGE] enriched_events=$ENRICHED_EVENTS
[WHOLE_RANGE] notrade_out=$NO_TRADE_OUT
[WHOLE_RANGE] trade_out=$TRADE_OUT
[WHOLE_RANGE] summary_prefix=$SUMMARY_PREFIX
DONE

if [[ -f "$SUMMARY_PREFIX.json" ]]; then
  "$PYTHON_BIN" - <<PY
import json
from pathlib import Path
p = Path("$SUMMARY_PREFIX.json")
r = json.loads(p.read_text())
print("[WHOLE_RANGE] summary_json", p)
print("[WHOLE_RANGE] all", r.get("all", []))
print("[WHOLE_RANGE] by_symbol", r.get("by_symbol", []))
PY
fi

