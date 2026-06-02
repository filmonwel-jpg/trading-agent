#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
CHUNK_ROOT="$ROOT/training_data/databento_30s_20260419_build_chunks"
FINAL_DATA_DIR="$ROOT/training_data/databento_30s_20260419"
COMPARE_ROOT="$ROOT/training_data/compare_runs_20260419_meta_ab"
PIPELINE_LOG="$ROOT/runtime/databento/logs/retrain_20260419_pipeline.log"

echo "=== pipeline log tail ==="
if [[ -f "$PIPELINE_LOG" ]]; then
  tail -20 "$PIPELINE_LOG"
else
  echo "missing: $PIPELINE_LOG"
fi

echo
for chunk in 01 02 03 04; do
  log="$CHUNK_ROOT/chunk_${chunk}/build.log"
  echo "=== chunk_${chunk} ==="
  if [[ -f "$log" ]]; then
    tail -5 "$log"
  else
    echo "missing: $log"
  fi
  echo
 done

echo "=== final dataset outputs ==="
find "$FINAL_DATA_DIR" -maxdepth 1 -name '*_30s_training.csv' | wc -l | awk '{print "csv_count=" $1}'
[[ -f "$FINAL_DATA_DIR/symbol_model_plan.csv" ]] && echo "symbol_model_plan=present" || echo "symbol_model_plan=missing"
[[ -f "$ROOT/training_data/databento_30s_20260419_combined.csv" ]] && echo "combined_csv=present" || echo "combined_csv=missing"

echo
if [[ -d "$COMPARE_ROOT" ]]; then
  echo "=== compare outputs ==="
  find "$COMPARE_ROOT" -maxdepth 2 \( -name 'training_manifest.csv' -o -name 'training_manifest_summary.json' -o -name 'final_comparison_report.json' \) -print | sort
else
  echo "compare_root_not_created_yet"
fi

