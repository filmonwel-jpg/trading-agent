#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
CHUNK_ROOT="$ROOT/training_data/databento_30s_20260523_build_chunks"
DATA_30S="$ROOT/training_data/databento_30s_20260523"
DATA_5S="$ROOT/training_data/databento_5s_20260523"
DATA_1S="$ROOT/training_data/databento_1s_20260523"
COMBINED_30S="$ROOT/training_data/databento_30s_20260523_combined.csv"
COMBINED_5S="$ROOT/training_data/databento_5s_20260523_combined.csv"
COMBINED_1S="$ROOT/training_data/databento_1s_20260523_combined.csv"
PLAN_CSV="$DATA_30S/symbol_model_plan.csv"

count_csvs() {
  local dir="$1"
  local pattern="$2"
  if [[ -d "$dir" ]]; then
    find "$dir" -maxdepth 1 -name "$pattern" -type f | wc -l | tr -d ' '
  else
    echo 0
  fi
}

line_count() {
  local file="$1"
  if [[ -f "$file" ]]; then
    wc -l < "$file" | tr -d ' '
  else
    echo 0
  fi
}

echo "DATABENTO 20260523 BUILD STATUS"
echo "ROOT=$ROOT"
echo "CHUNK_ROOT=$CHUNK_ROOT"
echo "30S_SYMBOL_CSVS=$(count_csvs "$DATA_30S" '*_30s_training.csv') lines_combined=$(line_count "$COMBINED_30S") path=$COMBINED_30S"
echo "5S_SYMBOL_CSVS=$(count_csvs "$DATA_5S" '*_5s_training.csv') lines_combined=$(line_count "$COMBINED_5S") path=$COMBINED_5S"
echo "1S_SYMBOL_CSVS=$(count_csvs "$DATA_1S" '*_1s_training.csv') lines_combined=$(line_count "$COMBINED_1S") path=$COMBINED_1S"
echo "PLAN_EXISTS=$([[ -f "$PLAN_CSV" ]] && echo yes || echo no) path=$PLAN_CSV"

if [[ -d "$CHUNK_ROOT" ]]; then
  echo
  echo "CHUNK LOG SUMMARY"
  for log in "$CHUNK_ROOT"/chunk_*/build.log; do
    [[ -f "$log" ]] || continue
    echo "--- $log"
    if grep -qiE 'error|traceback|failed|exception' "$log"; then
      echo "STATUS=ERROR_DETECTED"
      grep -iE 'error|traceback|failed|exception' "$log" | tail -20 || true
    else
      echo "STATUS=OK_OR_RUNNING"
      tail -5 "$log" || true
    fi
  done
fi

