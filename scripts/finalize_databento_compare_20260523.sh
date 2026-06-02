#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
COMPARE_ROOT="${COMPARE_ROOT:-$ROOT/training_data/compare_runs_20260523_meta_ab}"
PYTHON_BIN="${PYTHON_BIN:-/Users/filmonghezehey/miniforge3/bin/python3}"
ROUTING_CSV="${ROUTING_CSV:-$ROOT/runtime/databento/model-routing-20260523.csv}"

cd "$ROOT"

bash "$ROOT/scripts/run_databento_compare_training_20260523.sh"

"$PYTHON_BIN" "$ROOT/runtime/reconcile_compare_variant_manifest_20260407.py" \
  --variant-dir "$COMPARE_ROOT/baseline_no_generated_meta" \
  --baseline-template-manifest "$COMPARE_ROOT/baseline_no_generated_meta/training_manifest.csv" \
  --write

"$PYTHON_BIN" "$ROOT/runtime/reconcile_compare_variant_manifest_20260407.py" \
  --variant-dir "$COMPARE_ROOT/with_timesfm_and_sequence_proxy" \
  --baseline-template-manifest "$COMPARE_ROOT/baseline_no_generated_meta/training_manifest.csv" \
  --write

"$PYTHON_BIN" "$ROOT/runtime/generate_compare_report_20260407.py" \
  --compare-root "$COMPARE_ROOT"

"$PYTHON_BIN" "$ROOT/runtime/build_mixed_model_routing_20260407.py" \
  --compare-root "$COMPARE_ROOT" \
  --output-csv "$ROUTING_CSV"

echo "FINALIZED_COMPARE_ROOT=$COMPARE_ROOT"
echo "FINALIZED_ROUTING_CSV=$ROUTING_CSV"

