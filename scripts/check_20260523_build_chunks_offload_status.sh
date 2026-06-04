#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
SRC="$ROOT/training_data/databento_30s_20260523_build_chunks"
VAULT="/Volumes/DatabentoVault"
DEST="$VAULT/trading-agent-offload/databento/training_data/databento_30s_20260523_build_chunks"
PID_FILE="$ROOT/runtime/logs/external_offload/offload_build_chunks.pid"
LOG_LINKS=("$ROOT"/runtime/logs/external_offload/offload_build_chunks_*.log)

echo "=== Disk ==="
df -h "$ROOT" "$VAULT" 2>/dev/null || true

echo
echo "=== Process ==="
if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "RUNNING pid=$pid"
    ps -p "$pid" -o pid,ppid,etime,command || true
    pgrep -P "$pid" -lf . || true
  else
    echo "NOT_RUNNING last_pid=${pid:-unknown}"
  fi
else
  echo "NO_PID_FILE"
fi

echo
echo "=== Size ==="
[[ -e "$SRC" ]] && du -xsh "$SRC" || echo "MISSING source=$SRC"
[[ -e "$DEST" ]] && du -xsh "$DEST" || echo "MISSING dest=$DEST"

echo
echo "=== File counts ==="
if [[ -d "$SRC" ]]; then echo -n "source_files="; find "$SRC" -xdev -type f | wc -l | tr -d ' '; echo; fi
if [[ -d "$DEST" ]]; then echo -n "dest_files="; find "$DEST" -xdev -type f | wc -l | tr -d ' '; echo; fi

echo
echo "=== Latest log tail ==="
latest=""
if compgen -G "$ROOT/runtime/logs/external_offload/offload_build_chunks_*.log" >/dev/null; then
  latest="$(ls -t "$ROOT"/runtime/logs/external_offload/offload_build_chunks_*.log | head -1)"
fi
if [[ -n "$latest" ]]; then
  echo "log=$latest"
  tail -40 "$latest" || true
else
  echo "No offload log found."
fi

