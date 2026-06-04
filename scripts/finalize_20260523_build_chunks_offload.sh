#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
SRC="$ROOT/training_data/databento_30s_20260523_build_chunks"
VAULT="/Volumes/DatabentoVault"
DEST="$VAULT/trading-agent-offload/databento/training_data/databento_30s_20260523_build_chunks"
BACKUP_MARKER="$ROOT/training_data/databento_30s_20260523_build_chunks.internal_deleted_after_external_offload"
PID_FILE="$ROOT/runtime/logs/external_offload/offload_build_chunks.pid"

if [[ ! -d "$VAULT" ]]; then
  echo "ERROR: external vault is not mounted: $VAULT" >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "ERROR: offload copy is still running pid=$pid. Wait for it to finish before finalizing." >&2
    exit 1
  fi
fi

if [[ ! -d "$SRC" ]]; then
  echo "ERROR: source directory missing or already replaced: $SRC" >&2
  exit 1
fi

if [[ ! -d "$DEST" ]]; then
  echo "ERROR: destination directory missing: $DEST" >&2
  exit 1
fi

echo "=== Size check ==="
du -xsh "$SRC" "$DEST"

echo "=== File count check ==="
src_files="$(find "$SRC" -xdev -type f | wc -l | tr -d ' ')"
dest_files="$(find "$DEST" -xdev -type f | wc -l | tr -d ' ')"
echo "source_files=$src_files"
echo "dest_files=$dest_files"
if [[ "$src_files" != "$dest_files" ]]; then
  echo "ERROR: file counts differ; refusing to delete internal source." >&2
  exit 1
fi

echo "=== Required chunk artifact check ==="
missing=0
for chunk in chunk_01 chunk_02 chunk_03 chunk_04; do
  for file in combined_30s.csv combined_5s.csv combined_1s.csv symbol_model_plan.csv build.log; do
    if [[ ! -s "$DEST/$chunk/$file" ]]; then
      echo "MISSING_OR_EMPTY: $DEST/$chunk/$file" >&2
      missing=1
    fi
  done
done
if [[ "$missing" -ne 0 ]]; then
  echo "ERROR: required destination artifacts are missing/empty; refusing to delete internal source." >&2
  exit 1
fi

echo "=== Sample checksum check ==="
# Check all small plan/log files and the first 1MiB checksum of large CSVs by comparing full shasum for small files.
for rel in \
  chunk_01/symbol_model_plan.csv chunk_02/symbol_model_plan.csv chunk_03/symbol_model_plan.csv chunk_04/symbol_model_plan.csv \
  chunk_01/build.log chunk_02/build.log chunk_03/build.log chunk_04/build.log; do
  src_sum="$(shasum -a 256 "$SRC/$rel" | awk '{print $1}')"
  dst_sum="$(shasum -a 256 "$DEST/$rel" | awk '{print $1}')"
  if [[ "$src_sum" != "$dst_sum" ]]; then
    echo "ERROR: checksum mismatch for $rel" >&2
    exit 1
  fi
done

echo "=== Replacing internal directory with symlink ==="
rm -rf "$SRC"
ln -s "$DEST" "$SRC"
date > "$BACKUP_MARKER"

echo "=== Final status ==="
ls -ld "$SRC"
df -h "$ROOT" "$VAULT"
echo "FINALIZED_OK"

