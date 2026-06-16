#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
PILOT_BUILD_ROOT="${PILOT_BUILD_ROOT:-$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446}"
SETUP_OUT_DIR="${SETUP_OUT_DIR:-$LAKE_ROOT/model_training_sets/setup_30s_fixed_quality_20260615_144107}"
INPUT_30S="${INPUT_30S:-$PILOT_BUILD_ROOT/combined/combined_30s.csv}"
INPUT_5S="${INPUT_5S:-$PILOT_BUILD_ROOT/combined/combined_5s.csv}"
SETUP_PREDICTIONS="${SETUP_PREDICTIONS:-$SETUP_OUT_DIR/oof_setup_predictions.csv}"
RUN_ID="${RUN_ID:-lifecycle_micro_posthoc_threshold_stability_$(date +%Y%m%d_%H%M%S)}"
LIFECYCLE_OUT_DIR="${LIFECYCLE_OUT_DIR:-$LAKE_ROOT/model_training_sets/$RUN_ID}"
MIN_FROZEN_HOLDOUT_ROWS="${MIN_FROZEN_HOLDOUT_ROWS:-500}"
MIN_HOLDOUT_PREDICTIONS="${MIN_HOLDOUT_PREDICTIONS:-20}"
MAX_DAY_DOMINANCE_FRAC="${MAX_DAY_DOMINANCE_FRAC:-0.40}"
MIN_STABLE_THRESHOLD_POINTS="${MIN_STABLE_THRESHOLD_POINTS:-3}"

require_path() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    echo "ERROR: missing required path: $path" >&2
    exit 1
  fi
}

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
echo "INPUT_30S=$INPUT_30S"
echo "INPUT_5S=$INPUT_5S"
echo "SETUP_PREDICTIONS=$SETUP_PREDICTIONS"
echo "LIFECYCLE_OUT_DIR=$LIFECYCLE_OUT_DIR"
echo "MIN_STABLE_THRESHOLD_POINTS=$MIN_STABLE_THRESHOLD_POINTS"

require_path "$INPUT_30S"
require_path "$INPUT_5S"
require_file "$SETUP_PREDICTIONS"

python3 - "$SETUP_PREDICTIONS" <<'PY'
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
  --input-30s-csv "$INPUT_30S" \
  --input-5s-csv "$INPUT_5S" \
  --setup-predictions-csv "$SETUP_PREDICTIONS" \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --posthoc-calibration both \
  --posthoc-calibration-frac 0.20 \
  --frozen-holdout-frac 0.20 \
  --min-frozen-holdout-rows "$MIN_FROZEN_HOLDOUT_ROWS" \
  --min-holdout-predictions "$MIN_HOLDOUT_PREDICTIONS" \
  --max-day-dominance-frac "$MAX_DAY_DOMINANCE_FRAC" \
  --min-stable-threshold-points "$MIN_STABLE_THRESHOLD_POINTS" \
  --no-onnx 2>&1 | tee "$LIFECYCLE_OUT_DIR/train.log"

python3 scripts/check_lifecycle_posthoc_gates.py \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --min-frozen-holdout-rows "$MIN_FROZEN_HOLDOUT_ROWS" \
  --min-predicted-positive-count "$MIN_HOLDOUT_PREDICTIONS" \
  --max-day-dominance-frac "$MAX_DAY_DOMINANCE_FRAC" \
  --min-stable-threshold-points "$MIN_STABLE_THRESHOLD_POINTS"

ls -la "$LIFECYCLE_OUT_DIR"

python3 - <<'PY'
import json
import os
import pandas as pd

out = os.environ["LIFECYCLE_OUT_DIR"]
manifest = json.load(open(os.path.join(out, "calibration_manifest.json")))
scorecard = pd.read_csv(os.path.join(out, "lifecycle_micro_scorecard.csv"))
stability_report_path = os.path.join(out, "posthoc_threshold_stability_report.json")
gate_report_path = os.path.join(out, "posthoc_promotion_gate_report.json")
stability_report = json.load(open(stability_report_path)) if os.path.exists(stability_report_path) else {"models": []}
gate_report = json.load(open(gate_report_path)) if os.path.exists(gate_report_path) else {}

print("output_dir:", out)
print("errors:", manifest.get("errors"))
print("model_count:", len(manifest.get("models", [])))
print("posthoc_threshold_stability_report_exists:", os.path.exists(stability_report_path))
print("posthoc_promotion_gate_report_exists:", os.path.exists(gate_report_path))
print("promotion_ready:", gate_report.get("promotion_ready"))

score_cols = [
    "model",
    "posthoc_selected_method",
    "posthoc_brier_score",
    "posthoc_ece",
    "posthoc_threshold",
    "posthoc_calibration_rows",
]
print("\nselected methods:")
print(scorecard[score_cols].to_string(index=False))

stability_rows = pd.DataFrame(stability_report.get("models", []))
if not stability_rows.empty:
    cols = [
        "model",
        "calibration_method",
        "selected_threshold",
        "stable_island_points",
        "stable_island_threshold_min",
        "stable_island_threshold_max",
        "pass_stable_threshold_island",
    ]
    cols = [col for col in cols if col in stability_rows.columns]
    print("\nthreshold stability:")
    print(stability_rows[cols].to_string(index=False))
PY

echo "DONE: $LIFECYCLE_OUT_DIR"
