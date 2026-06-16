#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

first_existing_path() {
  local path
  for path in "$@"; do
    if [[ -e "$path" ]]; then
      printf '%s\n' "$path"
      return 0
    fi
  done
  return 1
}

require_path() {
  local path="$1"
  local label="$2"
  if [[ ! -e "$path" ]]; then
    echo "ERROR: missing required $label: $path" >&2
    exit 1
  fi
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    echo "ERROR: missing required $label: $path" >&2
    exit 1
  fi
}

LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
OFFLOAD_ROOT="${OFFLOAD_ROOT:-${LAKE_ROOT%/data_lake_v2}}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$LAKE_ROOT/model_training_sets}"

default_source_30s="$(first_existing_path \
  "$OFFLOAD_ROOT/training_data/databento_30s_20260523_combined.csv" \
  "$repo_root/training_data/databento_30s_20260523_combined.csv" \
  "$OFFLOAD_ROOT/training_data/databento_30s_20260523" \
  "$repo_root/training_data/databento_30s_20260523" \
  || true)"
default_source_5s="$(first_existing_path \
  "$OFFLOAD_ROOT/training_data/databento_5s_20260523_combined.csv" \
  "$repo_root/training_data/databento_5s_20260523_combined.csv" \
  "$OFFLOAD_ROOT/training_data/databento_5s_20260523" \
  "$repo_root/training_data/databento_5s_20260523" \
  || true)"

SOURCE_30S="${SOURCE_30S:-$default_source_30s}"
SOURCE_5S="${SOURCE_5S:-$default_source_5s}"
SYMBOLS="${SYMBOLS:-TSLA,TQQQ,NVDA,SPY,QQQ}"
START_DATE="${START_DATE:-2025-07-21}"
END_DATE_EXCLUSIVE="${END_DATE_EXCLUSIVE:-2026-05-23}"

CHAIN_TS="${CHAIN_TS:-$(date +%Y%m%d_%H%M%S)}"
CHAIN_RUN_ID="${CHAIN_RUN_ID:-broader_full_window_cost_aware_$CHAIN_TS}"
CHAIN_ROOT="${CHAIN_ROOT:-$OUTPUT_ROOT/$CHAIN_RUN_ID}"
STAGED_INPUT_DIR="${STAGED_INPUT_DIR:-$CHAIN_ROOT/input_slice}"
SETUP_OUT_DIR="${SETUP_OUT_DIR:-$CHAIN_ROOT/setup_cost_aware_full_window}"
LIFECYCLE_OUT_DIR="${LIFECYCLE_OUT_DIR:-$CHAIN_ROOT/lifecycle_micro_full_window_cost_aware}"
LIFECYCLE_STAGING_DIR="${LIFECYCLE_STAGING_DIR:-$CHAIN_ROOT/lifecycle_staging}"

RUN_STAGE_INPUTS="${RUN_STAGE_INPUTS:-1}"
RUN_SETUP_STAGE="${RUN_SETUP_STAGE:-1}"
RUN_LIFECYCLE_STAGE="${RUN_LIFECYCLE_STAGE:-1}"
RUNNER_PREFLIGHT_ONLY="${RUNNER_PREFLIGHT_ONLY:-0}"
ALLOW_EXTERNAL_SETUP_OUT_DIR="${ALLOW_EXTERNAL_SETUP_OUT_DIR:-0}"

STAGE_CHUNK_SIZE="${STAGE_CHUNK_SIZE:-200000}"
MIN_UNIQUE_DAYS="${MIN_UNIQUE_DAYS:-100}"
MIN_STAGED_30S_ROWS="${MIN_STAGED_30S_ROWS:-100000}"
MIN_STAGED_5S_ROWS="${MIN_STAGED_5S_ROWS:-600000}"
MIN_SETUP_PAIRED_OOF_ROWS="${MIN_SETUP_PAIRED_OOF_ROWS:-25000}"
MIN_OUTPUT_FREE_GB="${MIN_OUTPUT_FREE_GB:-50}"

MIN_FROZEN_HOLDOUT_ROWS="${MIN_FROZEN_HOLDOUT_ROWS:-500}"
MIN_HOLDOUT_PREDICTIONS="${MIN_HOLDOUT_PREDICTIONS:-20}"
MAX_DAY_DOMINANCE_FRAC="${MAX_DAY_DOMINANCE_FRAC:-0.40}"
MIN_STABLE_THRESHOLD_POINTS="${MIN_STABLE_THRESHOLD_POINTS:-3}"
LIFECYCLE_MAX_ENTRY_EVENTS="${LIFECYCLE_MAX_ENTRY_EVENTS:-0}"
LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE="${LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE:-0}"
LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL="${LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL:-0}"
LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL="${LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL:-1000000}"

SETUP_INPUT_30S="$STAGED_INPUT_DIR/combined_30s.csv"
LIFECYCLE_INPUT_30S="$STAGED_INPUT_DIR/data_30s"
LIFECYCLE_INPUT_5S="$STAGED_INPUT_DIR/data_5s"
SETUP_PREDICTIONS="${SETUP_PREDICTIONS:-$SETUP_OUT_DIR/oof_setup_predictions.csv}"
export CHAIN_ROOT SETUP_OUT_DIR LIFECYCLE_OUT_DIR

echo "repo_root=$repo_root"
echo "LAKE_ROOT=$LAKE_ROOT"
echo "OFFLOAD_ROOT=$OFFLOAD_ROOT"
echo "OUTPUT_ROOT=$OUTPUT_ROOT"
echo "SOURCE_30S=$SOURCE_30S"
echo "SOURCE_5S=$SOURCE_5S"
echo "SYMBOLS=$SYMBOLS"
echo "START_DATE=$START_DATE"
echo "END_DATE_EXCLUSIVE=$END_DATE_EXCLUSIVE"
echo "CHAIN_ROOT=$CHAIN_ROOT"
echo "STAGED_INPUT_DIR=$STAGED_INPUT_DIR"
echo "SETUP_OUT_DIR=$SETUP_OUT_DIR"
echo "LIFECYCLE_OUT_DIR=$LIFECYCLE_OUT_DIR"
echo "LIFECYCLE_STAGING_DIR=$LIFECYCLE_STAGING_DIR"
echo "RUN_STAGE_INPUTS=$RUN_STAGE_INPUTS"
echo "RUN_SETUP_STAGE=$RUN_SETUP_STAGE"
echo "RUN_LIFECYCLE_STAGE=$RUN_LIFECYCLE_STAGE"
echo "RUNNER_PREFLIGHT_ONLY=$RUNNER_PREFLIGHT_ONLY"
echo "ALLOW_EXTERNAL_SETUP_OUT_DIR=$ALLOW_EXTERNAL_SETUP_OUT_DIR"
echo "MIN_UNIQUE_DAYS=$MIN_UNIQUE_DAYS"
echo "MIN_STAGED_30S_ROWS=$MIN_STAGED_30S_ROWS"
echo "MIN_STAGED_5S_ROWS=$MIN_STAGED_5S_ROWS"
echo "MIN_SETUP_PAIRED_OOF_ROWS=$MIN_SETUP_PAIRED_OOF_ROWS"
echo "LIFECYCLE_MAX_ENTRY_EVENTS=$LIFECYCLE_MAX_ENTRY_EVENTS"
echo "LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE=$LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE"
echo "LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL=$LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL"
echo "LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL=$LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL"

if [[ -z "$SOURCE_30S" || -z "$SOURCE_5S" ]]; then
  cat >&2 <<'EOF'
ERROR: full/broader source inputs were not auto-detected.
Set SOURCE_30S and SOURCE_5S explicitly, for example:

  export SOURCE_30S="/Volumes/DatabentoVault/trading-agent-offload/databento/training_data/databento_30s_20260523_combined.csv"
  export SOURCE_5S="/Volumes/DatabentoVault/trading-agent-offload/databento/training_data/databento_5s_20260523_combined.csv"

This runner intentionally does not default to the 10-day pilot dataset.
EOF
  exit 1
fi

if truthy "$RUN_SETUP_STAGE" && ! truthy "$ALLOW_EXTERNAL_SETUP_OUT_DIR"; then
  case "$SETUP_OUT_DIR" in
    "$CHAIN_ROOT"/*) ;;
    *)
      cat >&2 <<EOF
ERROR: RUN_SETUP_STAGE=1 but SETUP_OUT_DIR is outside CHAIN_ROOT.
This usually means a stale SETUP_OUT_DIR is still exported from an earlier run.
Unset SETUP_OUT_DIR, or set ALLOW_EXTERNAL_SETUP_OUT_DIR=1 only when intentionally writing setup artifacts outside the chain directory.
CHAIN_ROOT=$CHAIN_ROOT
SETUP_OUT_DIR=$SETUP_OUT_DIR
EOF
      exit 1
      ;;
  esac
fi

require_path "$SOURCE_30S" "30s source input"
require_path "$SOURCE_5S" "5s source input"

if [[ "$OUTPUT_ROOT" != /Volumes/DatabentoVault/* ]] && ! truthy "${ALLOW_LOCAL_OUTPUT:-0}"; then
  echo "ERROR: OUTPUT_ROOT is not on /Volumes/DatabentoVault: $OUTPUT_ROOT" >&2
  echo "Set ALLOW_LOCAL_OUTPUT=1 only for tiny local tests, not for full-window training." >&2
  exit 1
fi

python3 - "$SOURCE_30S" "$SOURCE_5S" "$OUTPUT_ROOT" "$MIN_OUTPUT_FREE_GB" <<'PY'
import shutil
import sys
from pathlib import Path

import pandas as pd

source_30s = Path(sys.argv[1])
source_5s = Path(sys.argv[2])
output_root = Path(sys.argv[3])
min_free_gb = float(sys.argv[4])

def validate_input(path: Path, cadence: str) -> None:
    if path.is_dir():
        matches = list(path.glob(f"*_{cadence}_training.csv"))
        if not matches:
            raise SystemExit(f"ERROR: no *_{cadence}_training.csv files found under {path}")
        print(f"INPUT_SCHEMA_OK cadence={cadence} mode=directory files={len(matches)} path={path}")
        return
    cols = set(pd.read_csv(path, nrows=5).columns)
    if "Timestamp" not in cols and "Date" not in cols:
        raise SystemExit(f"ERROR: {cadence} source lacks Timestamp/Date columns: {path}")
    if "Symbol" not in cols:
        print(f"WARN {cadence} source has no Symbol column; staging will treat it as SINGLE: {path}")
    print(f"INPUT_SCHEMA_OK cadence={cadence} mode=file path={path}")

validate_input(source_30s, "30s")
validate_input(source_5s, "5s")

probe = output_root
while not probe.exists() and probe.parent != probe:
    probe = probe.parent
usage = shutil.disk_usage(probe)
free_gb = usage.free / (1024 ** 3)
print(f"OUTPUT_DISK_FREE_GB={free_gb:.2f} path={probe}")
if free_gb < min_free_gb:
    raise SystemExit(f"ERROR: insufficient free space at {probe}: {free_gb:.2f}GB < {min_free_gb:.2f}GB")
PY

if truthy "$RUNNER_PREFLIGHT_ONLY"; then
  echo "PREFLIGHT_ONLY: validated source paths/schema, output root, and free-space threshold; skipping staging/training."
  exit 0
fi

mkdir -p "$CHAIN_ROOT"
cat > "$CHAIN_ROOT/chain_config.env" <<EOF
LAKE_ROOT=$LAKE_ROOT
OFFLOAD_ROOT=$OFFLOAD_ROOT
OUTPUT_ROOT=$OUTPUT_ROOT
SOURCE_30S=$SOURCE_30S
SOURCE_5S=$SOURCE_5S
SYMBOLS=$SYMBOLS
START_DATE=$START_DATE
END_DATE_EXCLUSIVE=$END_DATE_EXCLUSIVE
CHAIN_ROOT=$CHAIN_ROOT
STAGED_INPUT_DIR=$STAGED_INPUT_DIR
SETUP_OUT_DIR=$SETUP_OUT_DIR
LIFECYCLE_OUT_DIR=$LIFECYCLE_OUT_DIR
LIFECYCLE_STAGING_DIR=$LIFECYCLE_STAGING_DIR
ALLOW_EXTERNAL_SETUP_OUT_DIR=$ALLOW_EXTERNAL_SETUP_OUT_DIR
MIN_UNIQUE_DAYS=$MIN_UNIQUE_DAYS
MIN_STAGED_30S_ROWS=$MIN_STAGED_30S_ROWS
MIN_STAGED_5S_ROWS=$MIN_STAGED_5S_ROWS
MIN_SETUP_PAIRED_OOF_ROWS=$MIN_SETUP_PAIRED_OOF_ROWS
EOF

if truthy "$RUN_STAGE_INPUTS"; then
  python3 -u scripts/stage_broader_window_inputs.py \
    --input-30s "$SOURCE_30S" \
    --input-5s "$SOURCE_5S" \
    --output-dir "$STAGED_INPUT_DIR" \
    --symbols "$SYMBOLS" \
    --start-date "$START_DATE" \
    --end-date-exclusive "$END_DATE_EXCLUSIVE" \
    --chunk-size "$STAGE_CHUNK_SIZE" \
    --min-unique-days "$MIN_UNIQUE_DAYS" \
    --min-rows-30s "$MIN_STAGED_30S_ROWS" \
    --min-rows-5s "$MIN_STAGED_5S_ROWS" \
    --force 2>&1 | tee "$CHAIN_ROOT/stage_inputs.log"
else
  echo "Skipping input staging because RUN_STAGE_INPUTS=$RUN_STAGE_INPUTS"
fi

require_file "$SETUP_INPUT_30S" "staged setup 30s CSV"
require_path "$LIFECYCLE_INPUT_30S" "staged lifecycle 30s directory"
require_path "$LIFECYCLE_INPUT_5S" "staged lifecycle 5s directory"

if truthy "$RUN_SETUP_STAGE"; then
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
    --input-csv "$SETUP_INPUT_30S" \
    --output-dir "$SETUP_OUT_DIR" \
    --no-onnx 2>&1 | tee "$SETUP_OUT_DIR/train.log"
else
  echo "Skipping setup training because RUN_SETUP_STAGE=$RUN_SETUP_STAGE"
fi

require_file "$SETUP_PREDICTIONS" "setup OOF predictions"
python3 - "$SETUP_OUT_DIR" "$SETUP_PREDICTIONS" "$MIN_SETUP_PAIRED_OOF_ROWS" <<'PY'
import json
import sys
from pathlib import Path

import pandas as pd

setup_dir = Path(sys.argv[1])
oof_path = Path(sys.argv[2])
min_paired = int(sys.argv[3])
manifest = json.loads((setup_dir / "setup_manifest.json").read_text())
label_manifest = json.loads((setup_dir / "cost_aware_label_manifest.json").read_text())
required = {
    "Symbol",
    "Timestamp",
    "f_long_setup_prob",
    "f_short_setup_prob",
    "long_setup_fold_id",
    "short_setup_fold_id",
    "is_oof_setup_prediction",
    "Label_Long_Entry_ExpectedNetRAfterCosts",
    "Label_Short_Entry_ExpectedNetRAfterCosts",
}
header = set(pd.read_csv(oof_path, nrows=5).columns)
missing = sorted(required - header)
if missing:
    raise SystemExit(f"ERROR: setup OOF CSV missing required columns: {missing} path={oof_path}")
oof = pd.read_csv(oof_path, usecols=["is_oof_setup_prediction"])
paired = int(pd.to_numeric(oof["is_oof_setup_prediction"], errors="coerce").fillna(0).sum())
if paired < min_paired:
    raise SystemExit(f"ERROR: paired setup OOF rows too low for broader/full-window run: {paired} < {min_paired}")
if manifest.get("label_info", {}).get("cost_aware") is not True:
    raise SystemExit("ERROR: setup manifest does not report cost-aware labels")
if label_manifest.get("errors"):
    raise SystemExit(f"ERROR: cost-aware label manifest errors={label_manifest.get('errors')}")
print("SETUP_OOF_VALIDATION_OK", f"paired_oof_rows={paired}", f"path={oof_path}")
PY

if truthy "$RUN_LIFECYCLE_STAGE"; then
  mkdir -p "$LIFECYCLE_OUT_DIR"
  export LIFECYCLE_OUT_DIR
  python3 -u train_lifecycle_micro_models.py \
    --input-30s-csv "$LIFECYCLE_INPUT_30S" \
    --input-5s-csv "$LIFECYCLE_INPUT_5S" \
    --setup-predictions-csv "$SETUP_PREDICTIONS" \
    --output-dir "$LIFECYCLE_OUT_DIR" \
    --staging-dir "$LIFECYCLE_STAGING_DIR" \
    --max-entry-events "$LIFECYCLE_MAX_ENTRY_EVENTS" \
    --max-entry-events-per-symbol-side "$LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE" \
    --max-staged-rows-per-symbol-per-model "$LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL" \
    --max-train-rows-per-model "$LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL" \
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
else
  echo "Skipping lifecycle/micro training because RUN_LIFECYCLE_STAGE=$RUN_LIFECYCLE_STAGE"
fi

python3 - <<'PY'
import json
import os
from pathlib import Path

import pandas as pd

chain_root = Path(os.environ.get("CHAIN_ROOT", ""))
setup_out = Path(os.environ.get("SETUP_OUT_DIR", ""))
lifecycle_out = Path(os.environ.get("LIFECYCLE_OUT_DIR", ""))
print("chain_root:", chain_root)
if (setup_out / "setup_manifest.json").exists():
    setup_manifest = json.loads((setup_out / "setup_manifest.json").read_text())
    print("setup_output_dir:", setup_out)
    print("setup_errors:", setup_manifest.get("errors"))
    print("setup_cost_aware:", setup_manifest.get("label_info", {}).get("cost_aware"))
    print("setup_oof_paired_rows:", setup_manifest.get("oof_predictions", {}).get("paired_rows"))
if (lifecycle_out / "posthoc_promotion_gate_report.json").exists():
    gate = json.loads((lifecycle_out / "posthoc_promotion_gate_report.json").read_text())
    print("lifecycle_output_dir:", lifecycle_out)
    print("promotion_ready:", gate.get("promotion_ready"))
    rows_path = lifecycle_out / "posthoc_promotion_gate_rows.csv"
    if rows_path.exists():
        rows = pd.read_csv(rows_path)
        cols = [c for c in ["model", "selected_method", "predicted_positive_count", "max_predicted_day_fraction", "stable_threshold_island_points", "gate_status"] if c in rows.columns]
        print("\npromotion gate rows:")
        print(rows[cols].to_string(index=False))
PY

echo "DONE: $CHAIN_ROOT"

