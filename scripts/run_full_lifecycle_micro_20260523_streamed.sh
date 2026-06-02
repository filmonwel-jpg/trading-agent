#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
RUN_ID="${RUN_ID:-full_lifecycle_micro_$(date '+%Y%m%d_%H%M%S')}"
LOG_DIR="$ROOT/runtime/logs/lifecycle_micro_20260523/$RUN_ID"
LOG="$LOG_DIR/train.log"
OUT_TMP="$ROOT/model_exports/lifecycle_micro_20260523_full_$RUN_ID"
STAGING="$ROOT/runtime/lifecycle_micro_20260523_staging_$RUN_ID"
CANONICAL="$ROOT/model_exports/lifecycle_micro_20260523"
BACKUP="$ROOT/model_exports/lifecycle_micro_20260523_backup_before_full_$RUN_ID"

mkdir -p "$LOG_DIR" "$OUT_TMP" "$STAGING"
exec > >(tee -a "$LOG") 2>&1
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "START full lifecycle/micro retrain RUN_ID=$RUN_ID"
log "OUTPUT_TMP=$OUT_TMP"
log "STAGING=$STAGING"

cd "$ROOT"
PYTHONUNBUFFERED=1 \
OUTPUT_DIR="$OUT_TMP" \
STAGING_DIR="$STAGING" \
MAX_ENTRY_EVENTS="${MAX_ENTRY_EVENTS:-0}" \
MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL="${MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL:-5000}" \
MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE="${MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE:-500}" \
MAX_TRAIN_ROWS_PER_MODEL="${MAX_TRAIN_ROWS_PER_MODEL:-500000}" \
MIN_ROWS="${MIN_ROWS:-200}" \
bash "$ROOT/scripts/run_lifecycle_micro_training_20260523.sh"

required=(
  long_exit_lifecycle.onnx
  short_exit_lifecycle.onnx
  long_micro_entry_5s.onnx
  short_micro_entry_5s.onnx
  long_micro_exit_guard_5s.onnx
  short_micro_exit_guard_5s.onnx
  lifecycle_micro_scorecard.csv
  lifecycle_micro_route_manifest.json
)
for file in "${required[@]}"; do
  test -s "$OUT_TMP/$file"
done

if [[ -d "$CANONICAL" ]]; then
  cp -a "$CANONICAL" "$BACKUP"
fi
mkdir -p "$CANONICAL"
cp -f "$OUT_TMP"/* "$CANONICAL/"
python3 - "$CANONICAL" "$OUT_TMP" <<'PY'
from pathlib import Path
import sys

canonical = Path(sys.argv[1])
out_tmp = sys.argv[2]
for name in ["lifecycle_micro_scorecard.csv", "lifecycle_micro_route_manifest.json"]:
    path = canonical / name
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace(out_tmp, str(canonical)), encoding="utf-8")
PY

log "PROMOTED_FULL_LIFECYCLE output=$OUT_TMP backup=$BACKUP canonical=$CANONICAL"
log "FULL_LIFECYCLE_RETRAIN_COMPLETE"



