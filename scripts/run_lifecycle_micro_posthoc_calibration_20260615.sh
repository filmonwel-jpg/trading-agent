#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
PILOT_BUILD_ROOT="${PILOT_BUILD_ROOT:-$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446}"
SETUP_OUT_DIR="${SETUP_OUT_DIR:-$LAKE_ROOT/model_training_sets/setup_30s_fixed_quality_20260615_144107}"
RUN_ID="${RUN_ID:-lifecycle_micro_posthoc_calibration_$(date +%Y%m%d_%H%M%S)}"
LIFECYCLE_OUT_DIR="${LIFECYCLE_OUT_DIR:-$LAKE_ROOT/model_training_sets/$RUN_ID}"

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "ERROR: missing required file: $path" >&2
    exit 1
  fi
}

echo "repo_root=$repo_root"
echo "LAKE_ROOT=$LAKE_ROOT"
echo "PILOT_BUILD_ROOT=$PILOT_BUILD_ROOT"
echo "SETUP_OUT_DIR=$SETUP_OUT_DIR"
echo "LIFECYCLE_OUT_DIR=$LIFECYCLE_OUT_DIR"

require_file "$PILOT_BUILD_ROOT/combined/combined_30s.csv"
require_file "$PILOT_BUILD_ROOT/combined/combined_5s.csv"
require_file "$SETUP_OUT_DIR/oof_setup_predictions.csv"

python3 - "$SETUP_OUT_DIR/oof_setup_predictions.csv" <<'PY'
import sys
import pandas as pd

path = sys.argv[1]
required = {
    "Symbol",
    "Timestamp",
    "f_long_setup_prob",
    "f_short_setup_prob",
    "long_setup_fold_id",
    "short_setup_fold_id",
    "is_oof_setup_prediction",
}
cols = set(pd.read_csv(path, nrows=5).columns)
missing = sorted(required - cols)
if missing:
    raise SystemExit(f"ERROR: setup OOF CSV is not lifecycle-compatible; missing columns: {missing}\npath={path}")
print("OOF schema OK:", path)
PY

mkdir -p "$LIFECYCLE_OUT_DIR"
export LIFECYCLE_OUT_DIR

python3 -u train_lifecycle_micro_models.py \
  --input-30s-csv "$PILOT_BUILD_ROOT/combined/combined_30s.csv" \
  --input-5s-csv "$PILOT_BUILD_ROOT/combined/combined_5s.csv" \
  --setup-predictions-csv "$SETUP_OUT_DIR/oof_setup_predictions.csv" \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --posthoc-calibration both \
  --posthoc-calibration-frac 0.20 \
  --frozen-holdout-frac 0.20 \
  --min-frozen-holdout-rows 500 \
  --min-holdout-predictions 20 \
  --max-day-dominance-frac 0.40 \
  --no-onnx 2>&1 | tee "$LIFECYCLE_OUT_DIR/train.log"

ls -la "$LIFECYCLE_OUT_DIR"

python3 - <<'PY'
import json
import os
import pandas as pd

out = os.environ["LIFECYCLE_OUT_DIR"]
manifest_path = os.path.join(out, "calibration_manifest.json")
comparison_path = os.path.join(out, "posthoc_calibration_comparison.csv")
calibrators_path = os.path.join(out, "posthoc_calibrators.json")

manifest = json.load(open(manifest_path))
comparison = pd.read_csv(comparison_path)

print("output_dir:", out)
print("errors:", manifest.get("errors"))
print("model_count:", len(manifest.get("models", [])))
print("posthoc_calibrators_exists:", os.path.exists(calibrators_path))

cols = [
    "model",
    "calibration_method",
    "brier_score",
    "ece",
    "threshold",
    "predicted_positive_count",
    "max_predicted_day_fraction",
]
print(comparison[cols].to_string(index=False))
PY

echo "DONE: $LIFECYCLE_OUT_DIR"
