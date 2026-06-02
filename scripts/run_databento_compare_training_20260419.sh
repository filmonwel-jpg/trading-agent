#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
DATA_DIR="$ROOT/training_data/databento_30s_20260419"
PLAN_CSV="$DATA_DIR/symbol_model_plan.csv"
COMPARE_ROOT="$ROOT/training_data/compare_runs_20260419_meta_ab"
PYTHON_BIN="/Users/filmonghezehey/miniforge3/bin/python3"
MODEL_FAMILY="random_forest"
REGIME_MODEL_FAMILY="random_forest"

mkdir -p "$COMPARE_ROOT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] compare run root: $COMPARE_ROOT"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] data dir: $DATA_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] plan csv: $PLAN_CSV"

run_variant() {
  local variant_name="$1"
  shift

  local work_dir="$COMPARE_ROOT/$variant_name"
  local console_log="$work_dir/run_console.log"
  mkdir -p "$work_dir"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START $variant_name"
  (
    cd "$ROOT"
    export PYTHONUNBUFFERED=1
    export MODEL_EXPORTS_ROOT="$work_dir/model_exports"
    export UPDATE_CANONICAL_MODEL_ALIASES=0
    "$PYTHON_BIN" "$ROOT/prepare_databento_training.py" \
      --data-dir "$DATA_DIR" \
      --plan-csv "$PLAN_CSV" \
      --work-dir "$work_dir" \
      --run-ready \
      --model-family "$MODEL_FAMILY" \
      --regime-model-family "$REGIME_MODEL_FAMILY" \
      --python-bin "$PYTHON_BIN" \
      "$@"
  ) | tee "$console_log"
  local rc=${PIPESTATUS[0]}
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] END $variant_name rc=$rc"
  return "$rc"
}

run_variant "baseline_no_generated_meta" \
  --use-meta-producer-features 0 \
  --timesfm-backend off \
  --sequence-backend off

run_variant "with_timesfm_and_sequence_proxy" \
  --use-meta-producer-features 1 \
  --timesfm-backend proxy \
  --sequence-backend proxy

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ALL_COMPARE_RUNS_COMPLETE"

