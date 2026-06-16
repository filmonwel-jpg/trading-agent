#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
PILOT_BUILD_ROOT="${PILOT_BUILD_ROOT:-$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446}"
INPUT_30S="${INPUT_30S:-$PILOT_BUILD_ROOT/combined/combined_30s.csv}"
RUN_ID="${RUN_ID:-setup_cost_aware_30s_$(date +%Y%m%d_%H%M%S)}"
SETUP_OUT_DIR="${SETUP_OUT_DIR:-$LAKE_ROOT/model_training_sets/$RUN_ID}"
RUNNER_PREFLIGHT_ONLY="${RUNNER_PREFLIGHT_ONLY:-0}"

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
echo "INPUT_30S=$INPUT_30S"
echo "SETUP_OUT_DIR=$SETUP_OUT_DIR"
echo "RUNNER_PREFLIGHT_ONLY=$RUNNER_PREFLIGHT_ONLY"

require_file "$INPUT_30S"

case "$RUNNER_PREFLIGHT_ONLY" in
  1|true|TRUE|yes|YES|on|ON)
    echo "PREFLIGHT_ONLY: validated setup input path; skipping training."
    exit 0
    ;;
esac

mkdir -p "$SETUP_OUT_DIR"
export SETUP_OUT_DIR

TRAIN_LEGACY_30S_EXIT_MODELS="${TRAIN_LEGACY_30S_EXIT_MODELS:-0}" \
UPDATE_CANONICAL_MODEL_ALIASES="${UPDATE_CANONICAL_MODEL_ALIASES:-0}" \
ENTRY_FILL_MODE="${ENTRY_FILL_MODE:-next_open_with_slippage}" \
COST_AWARE_LABELS="${COST_AWARE_LABELS:-1}" \
COST_AWARE_FILL_PROBABILITY="${COST_AWARE_FILL_PROBABILITY:-0.98}" \
COST_AWARE_PARTIAL_FILL_PENALTY_R="${COST_AWARE_PARTIAL_FILL_PENALTY_R:-0.02}" \
COST_AWARE_MISSED_FILL_PENALTY_R="${COST_AWARE_MISSED_FILL_PENALTY_R:-0.05}" \
python3 -u train_30s_models.py \
  --input-csv "$INPUT_30S" \
  --output-dir "$SETUP_OUT_DIR" \
  --no-onnx 2>&1 | tee "$SETUP_OUT_DIR/train.log"

ls -la "$SETUP_OUT_DIR"

python3 - <<'PY'
import json
import os
import pandas as pd

out = os.environ["SETUP_OUT_DIR"]
setup_manifest = json.load(open(os.path.join(out, "setup_manifest.json")))
label_manifest = json.load(open(os.path.join(out, "cost_aware_label_manifest.json")))
scorecard = pd.read_csv(os.path.join(out, "setup_scorecard.csv"))
oof = pd.read_csv(os.path.join(out, "oof_setup_predictions.csv"))

print("output_dir:", out)
print("setup_errors:", setup_manifest.get("errors"))
print("label_errors:", label_manifest.get("errors"))
print("label_cost_aware:", setup_manifest.get("label_info", {}).get("cost_aware"))
print("label_type:", setup_manifest.get("label_info", {}).get("type"))
print("oof_rows:", len(oof))
print("paired_oof_rows:", int(oof.get("is_oof_setup_prediction", pd.Series(dtype=int)).sum()))

print("\nscorecard:")
cols = [col for col in ["model", "avg_precision", "avg_threshold", "threshold_std", "brier_score", "ece", "folds_used"] if col in scorecard.columns]
print(scorecard[cols].to_string(index=False))

print("\nlabel summary:")
rows = []
for side, summary in (label_manifest.get("summary") or {}).items():
    row = {"side": side}
    row.update(summary)
    rows.append(row)
if rows:
    labels = pd.DataFrame(rows)
    cols = [
        "side",
        "rows",
        "finite_expected_net_r_rows",
        "positive_cost_aware_labels",
        "positive_cost_aware_rate",
        "positive_legacy_tp_before_sl_labels",
        "label_delta_count",
        "expected_net_r_mean",
        "expected_net_r_p50",
    ]
    cols = [col for col in cols if col in labels.columns]
    print(labels[cols].to_string(index=False))
PY

echo "DONE: $SETUP_OUT_DIR"
