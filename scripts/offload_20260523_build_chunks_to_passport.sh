#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
SRC="$ROOT/training_data/databento_30s_20260523_build_chunks"
VAULT="/Volumes/DatabentoVault"
DEST_PARENT="$VAULT/trading-agent-offload/databento/training_data"
DEST="$DEST_PARENT/databento_30s_20260523_build_chunks"
LOG_DIR="$VAULT/trading-agent-offload/databento/logs"
LOCAL_LOG_DIR="$ROOT/runtime/logs/external_offload"
STAMP="$(date '+%Y%m%d_%H%M%S')"
LOG="$LOG_DIR/offload_build_chunks_$STAMP.log"
LOCAL_LOG="$LOCAL_LOG_DIR/offload_build_chunks_$STAMP.log"
PID_FILE="$LOCAL_LOG_DIR/offload_build_chunks.pid"

mkdir -p "$DEST_PARENT" "$LOG_DIR" "$LOCAL_LOG_DIR"

if [[ ! -d "$SRC" ]]; then
  echo "ERROR: source missing: $SRC" >&2
  exit 1
fi

if [[ ! -d "$VAULT" ]]; then
  echo "ERROR: external vault is not mounted: $VAULT" >&2
  exit 1
fi

if [[ -e "$DEST" ]]; then
  echo "ERROR: destination already exists: $DEST" >&2
  exit 1
fi

if pgrep -f 'run_20260523_pipeline_after_bars|recover_20260523_pipeline|run_parallel_databento_build_20260523|build_30s_from_5s_csv.py' >/dev/null 2>&1; then
  echo "ERROR: relevant pipeline/build process appears to be running; refusing offload copy." >&2
  pgrep -af 'run_20260523_pipeline_after_bars|recover_20260523_pipeline|run_parallel_databento_build_20260523|build_30s_from_5s_csv.py' >&2 || true
  exit 1
fi

cat > "$LOG" <<EOF
Databento build chunks offload started
start=$(date)
source=$SRC
dest=$DEST
mode=copy_only_no_internal_delete
EOF
ln -sf "$LOG" "$LOCAL_LOG"

(
  echo "=== rsync start $(date) ==="
  echo "source=$SRC/"
  echo "dest=$DEST/"
  mkdir -p "$DEST"
  rsync -aE --partial --stats "$SRC/" "$DEST/"
  rc=$?
  echo "=== rsync exit rc=$rc $(date) ==="
  echo "=== source size ==="
  du -xsh "$SRC" || true
  echo "=== dest size ==="
  du -xsh "$DEST" || true
  echo "=== source files ==="
  find "$SRC" -xdev -type f | wc -l | tr -d ' '
  echo
  echo "=== dest files ==="
  find "$DEST" -xdev -type f | wc -l | tr -d ' '
  echo
  exit "$rc"
) >> "$LOG" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

cat <<EOF
STARTED_OFFLOAD_COPY_PID=$PID
LOG=$LOG
LOCAL_LOG_SYMLINK=$LOCAL_LOG
PID_FILE=$PID_FILE
SOURCE=$SRC
DEST=$DEST
EOF

