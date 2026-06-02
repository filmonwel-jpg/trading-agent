#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
PYTHON_BIN="${PYTHON_BIN:-/Users/filmonghezehey/miniforge3/bin/python3}"
DATA_DIR="${DATA_DIR:-$ROOT/training_data/databento_30s_20260523}"
PLAN_CSV="${PLAN_CSV:-$DATA_DIR/symbol_model_plan.csv}"
COMPARE_ROOT="${COMPARE_ROOT:-$ROOT/training_data/compare_runs_20260523_meta_ab}"
INCLUDE_COHORTS="${INCLUDE_COHORTS:-mega_liquid,liquid,specialized}"
MODEL_FAMILY="${MODEL_FAMILY:-random_forest}"
REGIME_MODEL_FAMILY="${REGIME_MODEL_FAMILY:-$MODEL_FAMILY}"
MIN_ROWS_PER_SYMBOL="${MIN_ROWS_PER_SYMBOL:-100000}"
MIN_UNIQUE_DAYS="${MIN_UNIQUE_DAYS:-100}"
SKIP_LEGACY_30S_EXIT_MODELS="${SKIP_LEGACY_30S_EXIT_MODELS:-1}"

if [[ ! -d "$DATA_DIR" ]]; then
  echo "Missing DATA_DIR: $DATA_DIR" >&2
  echo "Run scripts/run_parallel_databento_build_20260523.sh first." >&2
  exit 1
fi
if [[ ! -f "$PLAN_CSV" ]]; then
  echo "Missing PLAN_CSV: $PLAN_CSV" >&2
  echo "Run scripts/run_parallel_databento_build_20260523.sh first." >&2
  exit 1
fi

"$PYTHON_BIN" - <<PY
from pathlib import Path
import pandas as pd
data_dir = Path("$DATA_DIR")
min_rows = int("$MIN_ROWS_PER_SYMBOL")
min_days = int("$MIN_UNIQUE_DAYS")
csv_paths = sorted(data_dir.glob("*_30s_training.csv"))
if not csv_paths:
    raise SystemExit(f"No per-symbol 30s CSVs found in {data_dir}")
bad = []
for path in csv_paths:
    frame = pd.read_csv(path, usecols=lambda col: col in {"Timestamp", "Date"})
    row_count = len(frame)
    if "Date" in frame.columns:
        unique_days = frame["Date"].astype(str).nunique()
    elif "Timestamp" in frame.columns:
        raw_dates = frame["Timestamp"].astype(str).str.split().str[0]
        unique_days = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce").dt.date.nunique()
    else:
        unique_days = 0
    if (min_rows > 0 and row_count < min_rows) or (min_days > 0 and unique_days < min_days):
        bad.append((path.name, row_count, unique_days))
if bad:
    preview = "; ".join(f"{name}: rows={rows} days={days}" for name, rows, days in bad[:10])
    raise SystemExit(
        "Coverage preflight failed; refusing to train on a likely capped/incomplete 20260523 dataset. "
        f"Require rows>={min_rows} and days>={min_days}. Examples: {preview}. "
        "Set MIN_ROWS_PER_SYMBOL=0 MIN_UNIQUE_DAYS=0 only for an intentional smoke/bounded run."
    )
print(f"Coverage preflight OK: files={len(csv_paths)} min_rows={min_rows} min_days={min_days}")
PY

mkdir -p "$COMPARE_ROOT"
cd "$ROOT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] compare run root: $COMPARE_ROOT"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] data dir: $DATA_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] plan csv: $PLAN_CSV"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip legacy 30s exit models: $SKIP_LEGACY_30S_EXIT_MODELS"

skip_legacy_exit_args=()
skip_legacy_exit_models_normalized="$(printf '%s' "$SKIP_LEGACY_30S_EXIT_MODELS" | tr '[:upper:]' '[:lower:]')"
case "$skip_legacy_exit_models_normalized" in
  1|true|yes|on)
    skip_legacy_exit_args+=(--skip-legacy-30s-exit-models)
    ;;
esac

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
      --include-cohorts "$INCLUDE_COHORTS" \
      --run-ready \
      --model-family "$MODEL_FAMILY" \
      --regime-model-family "$REGIME_MODEL_FAMILY" \
      --python-bin "$PYTHON_BIN" \
      "${skip_legacy_exit_args[@]}" \
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
echo "COMPARE_ROOT=$COMPARE_ROOT"

