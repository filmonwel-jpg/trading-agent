#!/usr/bin/env bash
set -eo pipefail

# Move large generated artifacts out of the repo working tree, preserving local paths
# as symlinks to an external vault. Defaults to dry-run.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VAULT_ROOT="${DATABENTO_VAULT:-/Volumes/DatabentoVault}"
DEST_BASE="${VAULT_ROOT}/trading-agent-offload/databento"
DRY_RUN=1
ALLOW_TRACKED=0

usage() {
  cat <<'USAGE'
Usage: scripts/offload_large_artifacts_to_vault.sh [--execute] [--vault PATH] [--dest-base PATH] [--allow-tracked] [PATH ...]

Moves generated artifacts to the external Databento vault and replaces each local
path with a symlink to the offloaded copy. By default this is a dry-run.

Defaults, when no PATH is supplied, target common generated artifact locations:
  training_data/databento_*
  training_data/compare_runs_*
  training_data/databento_training_runs_*
  runtime/backtests
  runtime/logs
  runtime/databento/snapshots
  runtime/databento/promotions
  runtime/lifecycle_micro_*
  runtime/tmp_*
  runtime/*_live_trade_logs.txt
  runtime/*.log
  model_exports/20*
  model_exports/lifecycle_micro_*

Safety:
  - Refuses to move Git-tracked paths unless --allow-tracked is passed.
  - Refuses to overwrite a non-empty destination unless it already exists as a
    merge/update target for the same relative path.
  - Uses rsync, verifies rsync success, then removes the source and creates a
    symlink.

Examples:
  scripts/offload_large_artifacts_to_vault.sh
  scripts/offload_large_artifacts_to_vault.sh --execute training_data/databento_1s_20260523
  DATABENTO_VAULT=/Volumes/DatabentoVault scripts/offload_large_artifacts_to_vault.sh --execute
USAGE
}

log() { printf '%s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
fatal() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_vault() {
  [[ -d "$VAULT_ROOT" ]] || fatal "vault is not mounted: $VAULT_ROOT"
  mkdir -p "$DEST_BASE"
  local check_file="$DEST_BASE/.offload-write-check.$$"
  : > "$check_file" || fatal "vault is not writable: $DEST_BASE"
  rm -f "$check_file"
}

directory_contains_only_symlinks_to_dest() {
  local src_dir="$1"
  local dest_dir="$2"
  local entry target count=0

  [[ -d "$src_dir" ]] || return 1
  shopt -s nullglob dotglob
  for entry in "$src_dir"/*; do
    [[ "$(basename "$entry")" == "." || "$(basename "$entry")" == ".." ]] && continue
    count=$((count + 1))
    [[ -L "$entry" ]] || { shopt -u nullglob dotglob; return 1; }
    target="$(readlink "$entry")"
    [[ "$target" == "$dest_dir" || "$target" == "$dest_dir"/* ]] || { shopt -u nullglob dotglob; return 1; }
  done
  shopt -u nullglob dotglob
  [[ "$count" -ge 0 ]]
}

is_tracked() {
  local rel="$1"
  git -C "$REPO_ROOT" ls-files --error-unmatch -- "$rel" >/dev/null 2>&1
}

has_tracked_children() {
  local rel="$1"
  git -C "$REPO_ROOT" ls-files -- "$rel" | grep -q .
}

relativize() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    python3 - "$REPO_ROOT" "$path" <<'PY'
import os, sys
root, path = sys.argv[1], sys.argv[2]
print(os.path.relpath(path, root))
PY
  else
    printf '%s\n' "$path"
  fi
}

add_default_paths() {
  local patterns=(
    'training_data/databento_*'
    'training_data/compare_runs_*'
    'training_data/databento_training_runs_*'
    'runtime/backtests'
    'runtime/logs'
    'runtime/databento/snapshots'
    'runtime/databento/promotions'
    'runtime/lifecycle_micro_*'
    'runtime/tmp_*'
    'runtime/*_live_trade_logs.txt'
    'runtime/*.log'
    'model_exports/20*'
    'model_exports/lifecycle_micro_*'
  )
  local pattern match
  shopt -s nullglob
  for pattern in "${patterns[@]}"; do
    for match in "$REPO_ROOT"/$pattern; do
      [[ -e "$match" || -L "$match" ]] || continue
      printf '%s\0' "$(relativize "$match")"
    done
  done
  shopt -u nullglob
}

paths=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute)
      DRY_RUN=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --allow-tracked)
      ALLOW_TRACKED=1
      shift
      ;;
    --vault)
      [[ $# -ge 2 ]] || fatal "--vault requires a path"
      VAULT_ROOT="$2"
      DEST_BASE="${VAULT_ROOT}/trading-agent-offload/databento"
      shift 2
      ;;
    --dest-base)
      [[ $# -ge 2 ]] || fatal "--dest-base requires a path"
      DEST_BASE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do paths+=("$(relativize "$1")"); shift; done
      ;;
    -*)
      fatal "unknown option: $1"
      ;;
    *)
      paths+=("$(relativize "$1")")
      shift
      ;;
  esac
done

cd "$REPO_ROOT"
require_vault

if [[ ${#paths[@]} -eq 0 ]]; then
  while IFS= read -r -d '' rel; do
    paths+=("$rel")
  done < <(add_default_paths)
fi

# De-duplicate while preserving order.
unique_paths=()
for rel in "${paths[@]}"; do
  rel="${rel#./}"
  [[ -n "$rel" ]] || continue
  duplicate=0
  for existing in "${unique_paths[@]}"; do
    if [[ "$existing" == "$rel" ]]; then
      duplicate=1
      break
    fi
  done
  [[ "$duplicate" -eq 1 ]] && continue
  unique_paths+=("$rel")
done

if [[ ${#unique_paths[@]} -eq 0 ]]; then
  log "No matching artifact paths found."
  exit 0
fi

log "Repo:      $REPO_ROOT"
log "Dest base: $DEST_BASE"
if [[ "$DRY_RUN" -eq 1 ]]; then
  log "Mode:      dry-run (pass --execute to move)"
else
  log "Mode:      execute"
fi

for rel in "${unique_paths[@]}"; do
  src="$REPO_ROOT/$rel"
  dest="$DEST_BASE/$rel"

  if [[ -L "$src" ]]; then
    log "SKIP symlink: $rel -> $(readlink "$src")"
    continue
  fi
  if [[ ! -e "$src" ]]; then
    warn "missing: $rel"
    continue
  fi
  if [[ "$ALLOW_TRACKED" -ne 1 ]]; then
    if is_tracked "$rel" || has_tracked_children "$rel"; then
      warn "skip tracked path (use --allow-tracked only if intentional): $rel"
      continue
    fi
  fi

  size="$(du -sh "$src" 2>/dev/null | awk '{print $1}')"
  log "OFFLOAD $rel ($size) -> $dest"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    continue
  fi

  require_vault
  mkdir -p "$(dirname "$dest")"
  if [[ -d "$src" ]]; then
    mkdir -p "$dest"
    if directory_contains_only_symlinks_to_dest "$src" "$dest"; then
      log "  source contains only vault symlinks; replacing parent directory with one symlink"
      require_vault
      rm -rf "$src"
      ln -s "$dest" "$src"
      log "  linked: $rel -> $dest"
      continue
    fi
    mkdir -p "$dest"
    rsync -a --partial "$src"/ "$dest"/
    require_vault
    rm -rf "$src"
  else
    rsync -a --partial "$src" "$dest"
    require_vault
    rm -f "$src"
  fi
  ln -s "$dest" "$src"
  log "  linked: $rel -> $dest"
done





