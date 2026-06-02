#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./move_snapshot_to_databento_disk.sh [SNAPSHOT_FOLDER] [options]
  ./move_snapshot_to_databento_disk.sh --all [options]

Moves one folder under runtime/databento/snapshots to the external Databento
vault and replaces the local folder with a symlink to the external copy.
After the external copy is verified and the symlink is created, the original
local directory data is deleted to free internal disk space.

When SNAPSHOT_FOLDER is omitted, the script selects the latest local snapshot
folder whose name starts with a date, for example:
  2026-05-28_pre-stop_20260528T131819
  20260528_pre-stop_131819

Options:
  --all                  Move every local dated snapshot folder that is not already a symlink.
  --dry-run              Print what would be moved without changing files.
  --vault PATH           External disk mount. Default: $DATABENTO_VAULT or /Volumes/DatabentoVault
  --snapshot-base PATH   Local snapshots directory. Default: runtime/databento/snapshots
  --dest-base PATH       External snapshots directory. Default:
                         $DATABENTO_SNAPSHOT_OFFLOAD_DIR or
                         <vault>/trading-agent-offload/databento/runtime/databento/snapshots
  -h, --help             Show this help.

Examples:
  ./move_snapshot_to_databento_disk.sh --dry-run
  ./move_snapshot_to_databento_disk.sh --all
  ./move_snapshot_to_databento_disk.sh 2026-05-28_pre-stop_20260528T131819
  DATABENTO_VAULT=/Volumes/DatabentoVault ./move_snapshot_to_databento_disk.sh
EOF
}

log() {
  printf '[SNAPSHOT-OFFLOAD][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
vault="${DATABENTO_VAULT:-/Volumes/DatabentoVault}"
snapshot_base="$repo_root/runtime/databento/snapshots"
dest_base=""
snapshot_name=""
dry_run=0
all_mode=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      all_mode=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --vault)
      [[ $# -ge 2 ]] || die "--vault requires a path"
      vault="$2"
      shift 2
      ;;
    --vault=*)
      vault="${1#--vault=}"
      shift
      ;;
    --snapshot-base)
      [[ $# -ge 2 ]] || die "--snapshot-base requires a path"
      snapshot_base="$2"
      shift 2
      ;;
    --snapshot-base=*)
      snapshot_base="${1#--snapshot-base=}"
      shift
      ;;
    --dest-base)
      [[ $# -ge 2 ]] || die "--dest-base requires a path"
      dest_base="$2"
      shift 2
      ;;
    --dest-base=*)
      dest_base="${1#--dest-base=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      die "unknown option: $1"
      ;;
    *)
      [[ -z "$snapshot_name" ]] || die "only one SNAPSHOT_FOLDER may be supplied"
      snapshot_name="$1"
      shift
      ;;
  esac
done

snapshot_base="${snapshot_base%/}"
vault="${vault%/}"
if [[ -z "$dest_base" ]]; then
  dest_base="${DATABENTO_SNAPSHOT_OFFLOAD_DIR:-$vault/trading-agent-offload/databento/runtime/databento/snapshots}"
fi
dest_base="${dest_base%/}"

is_dated_snapshot_name() {
  local name="$1"
  [[ "$name" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}($|[_-]) || "$name" =~ ^[0-9]{8}($|[_-]) ]]
}

select_latest_snapshot() {
  local latest=""
  local path name

  [[ -d "$snapshot_base" ]] || die "snapshot base directory does not exist: $snapshot_base"

  while IFS= read -r -d '' path; do
    name="$(basename "$path")"
    if is_dated_snapshot_name "$name"; then
      if [[ -z "$latest" || "$name" > "$latest" ]]; then
        latest="$name"
      fi
    fi
  done < <(find "$snapshot_base" -mindepth 1 -maxdepth 1 -type d -print0)

  [[ -n "$latest" ]] || die "no local dated snapshot folders found under: $snapshot_base"
  printf '%s\n' "$latest"
}

select_all_snapshots() {
  local path name

  [[ -d "$snapshot_base" ]] || die "snapshot base directory does not exist: $snapshot_base"

  while IFS= read -r path; do
    name="$(basename "$path")"
    if is_dated_snapshot_name "$name"; then
      printf '%s\n' "$name"
    fi
  done < <(find "$snapshot_base" -mindepth 1 -maxdepth 1 -type d -print | sort)
}

verify_copy() {
  local src="$1"
  local dst="$2"
  SRC_DIR="$src" DST_DIR="$dst" python3 - <<'PY'
import os
import sys
import hashlib
from pathlib import Path

src = Path(os.environ["SRC_DIR"])
dst = Path(os.environ["DST_DIR"])

def file_sha256(path: Path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def manifest(root: Path):
    entries = {}
    for path in root.rglob("*"):
        rel = path.relative_to(root).as_posix()
        try:
            if path.is_symlink():
                entries[rel] = ("symlink", os.readlink(path))
            elif path.is_file():
                stat = path.stat()
                entries[rel] = ("file", stat.st_size, file_sha256(path))
            elif path.is_dir():
                entries[rel] = ("dir", None)
            else:
                entries[rel] = ("other", None)
        except FileNotFoundError:
            print(f"Source changed during verification: {path}", file=sys.stderr)
            sys.exit(2)
    return entries

src_manifest = manifest(src)
dst_manifest = manifest(dst)

missing = sorted(set(src_manifest) - set(dst_manifest))
extra = sorted(set(dst_manifest) - set(src_manifest))
mismatch = sorted(
    rel for rel in (set(src_manifest) & set(dst_manifest))
    if src_manifest[rel] != dst_manifest[rel]
)

print(f"source_entries={len(src_manifest)}")
print(f"dest_entries={len(dst_manifest)}")
print(f"missing_in_dest={len(missing)}")
print(f"extra_in_dest={len(extra)}")
print(f"metadata_mismatch={len(mismatch)}")

if missing:
    print("missing sample:", missing[:10], file=sys.stderr)
if extra:
    print("extra sample:", extra[:10], file=sys.stderr)
if mismatch:
    print("mismatch sample:", mismatch[:10], file=sys.stderr)

sys.exit(0 if not missing and not extra and not mismatch else 1)
PY
}

replace_source_with_symlink_and_delete_local_data() {
  local src="$1"
  local dest="$2"
  local backup_src="$3"

  log "replacing local snapshot directory with symlink"
  mv "$src" "$backup_src"
  if ! ln -s "$dest" "$src"; then
    mv "$backup_src" "$src"
    die "failed to create symlink; restored original source directory"
  fi

  log "deleting local source data to free space: $backup_src"
  rm -rf "$backup_src"
}

offload_snapshot() {
  local snapshot_name="$1"
  local src dest stamp tmp_dest backup_src

  [[ -n "$snapshot_name" ]] || die "empty snapshot folder name"
  [[ "$snapshot_name" != */* ]] || die "SNAPSHOT_FOLDER must be a folder name under $snapshot_base, not a path"
  [[ "$snapshot_name" != "." && "$snapshot_name" != ".." ]] || die "invalid snapshot folder name: $snapshot_name"

  src="$snapshot_base/$snapshot_name"
  dest="$dest_base/$snapshot_name"
  stamp="$(date '+%Y%m%dT%H%M%S')"
  tmp_dest="$dest.incomplete.$stamp.$$"
  backup_src="$src.internal-before-symlink.$stamp.$$"

  [[ -e "$src" || -L "$src" ]] || die "source snapshot does not exist: $src"
  if [[ -L "$src" ]]; then
    if [[ "$(readlink "$src")" == "$dest" ]]; then
      log "already offloaded; local entry is already the expected symlink: $src -> $dest"
      return 0
    fi
    die "source snapshot is already a symlink to a different target: $src -> $(readlink "$src")"
  fi
  [[ -d "$src" ]] || die "source snapshot is not a directory: $src"
  [[ ! -e "$tmp_dest" ]] || die "temporary destination already exists: $tmp_dest"
  [[ ! -e "$backup_src" ]] || die "temporary local backup path already exists: $backup_src"

  cat <<EOF
SNAPSHOT_FOLDER=$snapshot_name
SOURCE=$src
DESTINATION=$dest
LOCAL_SYMLINK=$src -> $dest
VAULT=$vault
DRY_RUN=$dry_run
EOF

  if [[ "$dry_run" -eq 1 ]]; then
    if [[ -e "$dest" ]]; then
      log "dry run only; would verify existing external destination, replace local directory with symlink, then delete local source data"
    else
      log "dry run only; would copy to external disk, verify, replace local directory with symlink, then delete local source data"
    fi
    return 0
  fi

  log "creating destination parent: $dest_base"
  mkdir -p "$dest_base"

  if [[ -e "$dest" ]]; then
    [[ -d "$dest" ]] || die "destination exists but is not a directory: $dest"
    log "destination already exists; verifying it matches local source before deleting local data"
    verify_copy "$src" "$dest"
  else
    log "copying snapshot to temporary external directory"
    mkdir -p "$tmp_dest"
    if ! rsync -aE --partial --stats "$src/" "$tmp_dest/"; then
      rm -rf "$tmp_dest"
      die "rsync failed; local source was left unchanged"
    fi

    log "verifying copied entries"
    if ! verify_copy "$src" "$tmp_dest"; then
      rm -rf "$tmp_dest"
      die "verification failed; local source was left unchanged"
    fi

    log "promoting external copy into final location"
    mv "$tmp_dest" "$dest"
  fi

  replace_source_with_symlink_and_delete_local_data "$src" "$dest" "$backup_src"

  log "offload complete"
  ls -ld "$src" "$dest"
  df -h "$snapshot_base" "$vault" 2>/dev/null || true
}

[[ -d "$snapshot_base" ]] || die "snapshot base directory does not exist: $snapshot_base"
[[ -d "$vault" ]] || die "external Databento vault is not mounted or is not a directory: $vault"
command -v rsync >/dev/null 2>&1 || die "rsync is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required for copy verification"

if [[ "$all_mode" -eq 1 && -n "$snapshot_name" ]]; then
  die "use either --all or one SNAPSHOT_FOLDER, not both"
fi

snapshot_names=()
if [[ "$all_mode" -eq 1 ]]; then
  while IFS= read -r name; do
    snapshot_names+=("$name")
  done < <(select_all_snapshots)
  if [[ "${#snapshot_names[@]}" -eq 0 ]]; then
    log "no remaining local dated snapshot directories found under: $snapshot_base"
    exit 0
  fi
elif [[ -z "$snapshot_name" ]]; then
  snapshot_name="$(select_latest_snapshot)"
  log "selected latest local dated snapshot: $snapshot_name"
  snapshot_names+=("$snapshot_name")
else
  snapshot_names+=("$snapshot_name")
fi

for name in "${snapshot_names[@]}"; do
  offload_snapshot "$name"
done

