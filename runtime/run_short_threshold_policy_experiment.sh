#!/usr/bin/env zsh
set -euo pipefail

cd /Users/FXG06FA/trading-agent-main

export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets"
export ENRICHED_30S_ROOT="$LAKE_ROOT/pilot_10d_six_source_enriched_30s_20260617_220849"
export INPUT_CSV="$ENRICHED_30S_ROOT/combined/combined_30s.csv"

if [[ ! -r "$INPUT_CSV" ]]; then
  echo "ERROR: input CSV is not readable: $INPUT_CSV" >&2
  exit 2
fi

export RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
export POLICY_ROOT="/Users/FXG06FA/trading-agent-main/runtime/databento_short_threshold_policy_no_onnx_$RUN_STAMP"
mkdir -p "$POLICY_ROOT"

export MODEL_FAMILY=random_forest
export REGIME_MODEL_FAMILY=random_forest
export TRAIN_LEGACY_30S_EXIT_MODELS=0
export USE_DATABENTO_SILVER_FEATURES=1
export USE_NEWS_BAR_FEATURES=1
export USE_REGIME_PROB_FEATURES=1
export USE_META_PRODUCER_FEATURES=0
export UPDATE_CANONICAL_MODEL_ALIASES=0
export RESEARCH_SHORT_THRESHOLD_FLOOR_ENABLED=1
export RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_RATE=0.005
export RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_COUNT=20
export RESEARCH_SHORT_THRESHOLD_MIN_PRECISION=0.20

{
  printf 'POLICY_ROOT=%s\n' "$POLICY_ROOT"
  printf 'INPUT_CSV=%s\n' "$INPUT_CSV"
  printf 'MODEL_FAMILY=%s\n' "$MODEL_FAMILY"
  printf 'REGIME_MODEL_FAMILY=%s\n' "$REGIME_MODEL_FAMILY"
  printf 'TRAIN_LEGACY_30S_EXIT_MODELS=%s\n' "$TRAIN_LEGACY_30S_EXIT_MODELS"
  printf 'RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_RATE=%s\n' "$RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_RATE"
  printf 'RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_COUNT=%s\n' "$RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_COUNT"
  printf 'RESEARCH_SHORT_THRESHOLD_MIN_PRECISION=%s\n' "$RESEARCH_SHORT_THRESHOLD_MIN_PRECISION"
} | tee "$POLICY_ROOT/run_config.log"

for preset in liquidity equs; do
  export DATABENTO_SILVER_FEATURE_SET="$preset"
  export RUN_ROOT="$POLICY_ROOT/$preset"
  mkdir -p "$RUN_ROOT"
  echo "== START preset=$preset run_root=$RUN_ROOT $(date -u +%Y-%m-%dT%H:%M:%SZ) ==" | tee "$RUN_ROOT/run_marker.log"
  python3 train_30s_models.py \
    --input-csv "$INPUT_CSV" \
    --output-dir "$RUN_ROOT" \
    --no-onnx \
    2>&1 | tee "$RUN_ROOT/train_30s_no_onnx.log"
  run_rc=${pipestatus[1]}
  echo "== DONE preset=$preset status=$run_rc $(date -u +%Y-%m-%dT%H:%M:%SZ) ==" | tee -a "$RUN_ROOT/run_marker.log"
  if [[ "$run_rc" -ne 0 ]]; then
    echo "FAILED preset=$preset status=$run_rc" | tee -a "$POLICY_ROOT/run_config.log"
    exit "$run_rc"
  fi
done

printf '\nPOLICY_ROOT=%s\n' "$POLICY_ROOT"
