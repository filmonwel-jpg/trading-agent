#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
PYTHON_BIN="${PYTHON_BIN:-/Users/filmonghezehey/miniforge3/bin/python3}"
INPUT_30S_CSV="${INPUT_30S_CSV:-$ROOT/training_data/databento_30s_20260523}"
INPUT_5S_CSV="${INPUT_5S_CSV:-$ROOT/training_data/databento_5s_20260523}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT/model_exports/lifecycle_micro_20260523}"
STAGING_DIR="${STAGING_DIR:-$ROOT/runtime/lifecycle_micro_20260523_staging}"
MAX_TRAIN_ROWS_PER_MODEL="${MAX_TRAIN_ROWS_PER_MODEL:-750000}"
MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL="${MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL:-20000}"
MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE="${MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE:-500}"
MAX_ENTRY_EVENTS="${MAX_ENTRY_EVENTS:-0}"
MIN_ROWS="${MIN_ROWS:-200}"

if [[ ! -e "$INPUT_30S_CSV" ]]; then
  echo "Missing INPUT_30S_CSV/dir: $INPUT_30S_CSV" >&2
  echo "Run scripts/run_parallel_databento_build_20260523.sh first." >&2
  exit 1
fi
if [[ ! -e "$INPUT_5S_CSV" ]]; then
  echo "Missing INPUT_5S_CSV/dir: $INPUT_5S_CSV" >&2
  echo "Run scripts/run_parallel_databento_build_20260523.sh first." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR" "$STAGING_DIR"
cd "$ROOT"

"$PYTHON_BIN" train_lifecycle_micro_models.py \
  --input-30s-csv "$INPUT_30S_CSV" \
  --input-5s-csv "$INPUT_5S_CSV" \
  --output-dir "$OUTPUT_DIR" \
  --staging-dir "$STAGING_DIR" \
  --max-train-rows-per-model "$MAX_TRAIN_ROWS_PER_MODEL" \
  --max-staged-rows-per-symbol-per-model "$MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL" \
  --max-entry-events-per-symbol-side "$MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE" \
  --max-entry-events "$MAX_ENTRY_EVENTS" \
  --min-rows "$MIN_ROWS"

echo "WROTE $OUTPUT_DIR/lifecycle_micro_scorecard.csv"
echo "WROTE $OUTPUT_DIR/lifecycle_micro_route_manifest.json"

