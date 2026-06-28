#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
cd "$ROOT"

OUT_BASE="$ROOT/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent"
RUN_DIR="$OUT_BASE/run"
RECORDED_EVENTS="$OUT_BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz"
SETUP_DIR="$ROOT/runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854"
LIFECYCLE_DIR="$ROOT/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports"
SYMBOLS_FILE="$ROOT/config/databento_core_5_symbols.txt"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$RUN_DIR/databento-core5-4week-recent-$RUN_TS.log"
LATEST_ENV="$RUN_DIR/latest_run.env"

mkdir -p "$RUN_DIR"

# Match the existing recorded-replay sizing/risk environment used in the prior local checks.
TRADE_JAVA_TOOL_OPTIONS="-Dbacktest.strategy.tradeAmount=60000 -Dtrading.trade-amount=60000 -Dtrading.risk.max-order-notional=70000 -Dbacktest.strategy.maxOrderNotional=70000"
if [[ " ${JAVA_TOOL_OPTIONS:-} " != *" -Dbacktest.strategy.tradeAmount="* ]]; then
  export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }$TRADE_JAVA_TOOL_OPTIONS"
fi

cat > "$LATEST_ENV" <<ENV
RUN_TS=$RUN_TS
RUN_LOG=$RUN_LOG
RUN_DIR=$RUN_DIR
RECORDED_EVENTS=$RECORDED_EVENTS
SETUP_DIR=$SETUP_DIR
SETUP_THRESHOLDS_FILE=$SETUP_DIR/setup_runtime_thresholds.properties
LIFECYCLE_DIR=$LIFECYCLE_DIR
SYMBOLS_FILE=$SYMBOLS_FILE
START_DATE=2026-04-27
END_DATE=2026-05-22
JAVA_TOOL_OPTIONS=$JAVA_TOOL_OPTIONS
ENV

set -o pipefail
{
  echo "[RUN4W] root=$ROOT"
  echo "[RUN4W] run_log=$RUN_LOG"
  echo "[RUN4W] recorded_events=$RECORDED_EVENTS"
  echo "[RUN4W] setup_dir=$SETUP_DIR"
  echo "[RUN4W] lifecycle_dir=$LIFECYCLE_DIR"
  echo "[RUN4W] latest_env=$LATEST_ENV"
  scripts/run_databento_historical_ibkr_sim_backtest.sh \
    --source ndjson \
    --recorded-events "$RECORDED_EVENTS" \
    --symbols-file "$SYMBOLS_FILE" \
    --start 2026-04-27 \
    --end 2026-05-22 \
    --output-dir "$RUN_DIR" \
    --model-dir "$SETUP_DIR" \
    --lifecycle-model-dir "$LIFECYCLE_DIR" \
    --timeout-seconds 0 \
    --max-trades 2000
  rc=$?
  echo "BACKTEST_RC=$rc"
  exit "$rc"
} 2>&1 | tee "$RUN_LOG"
exit "${PIPESTATUS[0]}"

