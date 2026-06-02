#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./prepare_local_databento_startup_data.sh [--apply] [--symbols=CSV] [--exclude=CSV] [--vault=PATH] [--offload-root=PATH] [--postgres-source=PATH] [--model-routing-source=PATH] [--install-python-deps] [--no-update-configs]

Copies the minimum Databento startup artifacts from the external disk into local repo paths so
./start_all_databento_bots.sh can start without the external disk mounted later.

Default mode is a dry run. Pass --apply to copy files and rewrite selected bot configs.

Copies/creates:
  - runtime/postgres-local.properties (copies --postgres-source if supplied; otherwise creates a minimal local placeholder)
  - model_exports/lifecycle_micro_20260523 (required lifecycle/micro artifacts)
  - training_data/databento_30s_20260523/symbol_model_plan.csv
  - runtime/databento/model-routing.csv (from --model-routing-source or latest external promotion backup)
  - selected unique 30s model bundle dirs into runtime/databento/model-bundles/...
  - selected runtime/databento/bots/trading-*.properties trading.model.dir values rewritten to local copied bundles

Examples:
  ./prepare_local_databento_startup_data.sh
  ./prepare_local_databento_startup_data.sh --apply --symbols=AMD,AMZN,NVDA,TSLA
  ./prepare_local_databento_startup_data.sh --apply --install-python-deps
  ./prepare_local_databento_startup_data.sh --apply --vault=/Volumes/DatabentoVault
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
bots_dir="$repo_root/runtime/databento/bots"
default_vault="/Volumes/DatabentoVault"
vault="$default_vault"
offload_root=""
symbols_csv=""
exclude_csv=""
apply_mode=0
update_configs=1
install_python_deps=0
postgres_source=""
model_routing_source=""

required_30s_models=(
  long_entry.onnx
  short_entry.onnx
  regime_classifier.onnx
  choppy_long_entry.onnx
  choppy_short_entry.onnx
  trend_long_entry.onnx
  trend_short_entry.onnx
  volatile_long_entry.onnx
  volatile_short_entry.onnx
  open30_long_entry.onnx
  open30_short_entry.onnx
)

log() { printf '[LOCAL-PREP] %s\n' "$*"; }
warn() { printf '[LOCAL-PREP][WARN] %s\n' "$*" >&2; }
error() { printf '[LOCAL-PREP][ERROR] %s\n' "$*" >&2; }

run_or_show() {
  if [[ $apply_mode -eq 1 ]]; then
    "$@"
  else
    printf '[LOCAL-PREP][DRY-RUN]'
    printf ' %q' "$@"
    printf '\n'
  fi
}

trim_spaces() { printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'; }
normalize_symbol() { printf '%s' "$1" | tr '[:lower:]' '[:upper:]'; }
lower_symbol() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

contains_symbol() {
  local needle="$1" item
  shift || true
  for item in "$@"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

parse_csv_symbols() {
  local raw="$1" old_ifs="$IFS" entry trimmed
  IFS=','
  for entry in $raw; do
    trimmed="$(trim_spaces "$entry")"
    if [[ -n "$trimmed" ]]; then
      normalize_symbol "$trimmed"
      printf '\n'
    fi
  done
  IFS="$old_ifs"
}

extract_prop() {
  local path="$1" key="$2"
  [[ -f "$path" ]] || return 0
  awk -F= -v search_key="$key" '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    {
      current=$1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current)
      if (current == search_key) {
        value=substr($0, index($0, "=") + 1)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        print value
        exit
      }
    }
  ' "$path"
}

discover_symbols() {
  local path filename symbol
  shopt -s nullglob
  for path in "$bots_dir"/trading-*.properties; do
    filename="$(basename "$path")"
    [[ "$filename" == "trading-databento-template.properties" ]] && continue
    symbol="$(extract_prop "$path" trading.symbol)"
    if [[ -z "$symbol" ]]; then
      symbol="${filename#trading-}"
      symbol="${symbol%.properties}"
    fi
    symbol="$(normalize_symbol "$symbol")"
    [[ -n "$symbol" ]] && printf '%s\n' "$symbol"
  done
  shopt -u nullglob
}

resolve_properties_path() {
  local symbol_lower="$1"
  local candidate
  for candidate in "$bots_dir/trading-${symbol_lower}.properties" "$repo_root/runtime/trading-${symbol_lower}.properties"; do
    if [[ -f "$candidate" ]]; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

relative_training_path_from_model_dir() {
  local model_dir="$1"
  case "$model_dir" in
    */training_data/*) printf '%s' "${model_dir#*/training_data/}" ;;
    *) return 1 ;;
  esac
}

local_bundle_dest_for_rel() {
  local rel="$1"
  printf '%s/runtime/databento/model-bundles/%s' "$repo_root" "$rel"
}

replace_symlink_parent_with_dir() {
  local path="$1"
  local parent
  parent="$(dirname "$path")"
  if [[ -L "$parent" ]]; then
    run_or_show rm "$parent"
  fi
  run_or_show mkdir -p "$parent"
}

copy_dir_filtered() {
  local src="$1" dest="$2" label="$3"
  if [[ ! -d "$src" ]]; then
    error "Missing source $label: $src"
    return 1
  fi
  if [[ -L "$dest" ]]; then
    run_or_show rm "$dest"
  fi
  run_or_show mkdir -p "$dest"
  run_or_show rsync -a --delete --include='*/' --include='*.onnx' --include='*.json' --include='*.csv' --exclude='*' "$src/" "$dest/"
}

copy_file_to_local() {
  local src="$1" dest="$2" label="$3"
  if [[ ! -f "$src" ]]; then
    error "Missing source $label: $src"
    return 1
  fi
  replace_symlink_parent_with_dir "$dest"
  run_or_show cp "$src" "$dest"
}

create_minimal_postgres_placeholder() {
  local dest="$repo_root/runtime/postgres-local.properties"
  replace_symlink_parent_with_dir "$dest"
  if [[ $apply_mode -eq 1 ]]; then
    cat > "$dest" <<'EOF'
# Local startup placeholder.
# The default datasource values are defined in src/main/resources/application.properties.
# Override spring.datasource.* here only if this computer uses different local PostgreSQL settings.
EOF
  else
    printf '[LOCAL-PREP][DRY-RUN] create minimal %q\n' "$dest"
  fi
}

patch_model_dir_property() {
  local properties_path="$1" new_model_dir="$2" backup_path
  backup_path="$properties_path.local-prep.bak"
  if [[ $apply_mode -eq 0 ]]; then
    printf '[LOCAL-PREP][DRY-RUN] update %q trading.model.dir=%q\n' "$properties_path" "$new_model_dir"
    return 0
  fi
  cp "$properties_path" "$backup_path"
  python3 - "$properties_path" "$new_model_dir" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
model_dir = sys.argv[2]
lines = path.read_text().splitlines()
out = []
replaced = False
for line in lines:
    stripped = line.strip()
    if stripped.startswith("trading.model.dir=") and not stripped.startswith("#"):
        out.append(f"trading.model.dir={model_dir}")
        replaced = True
    else:
        out.append(line)
if not replaced:
    out.append(f"trading.model.dir={model_dir}")
path.write_text("\n".join(out) + "\n")
PY
}

prepare_tee_log_target() {
  local symbol="$1"
  local symbol_lower tee_path
  symbol_lower="$(lower_symbol "$symbol")"
  tee_path="$repo_root/runtime/${symbol_lower}_live_trade_logs.txt"
  if [[ -L "$tee_path" ]]; then
    run_or_show rm "$tee_path"
  fi
}

install_databento_python_deps() {
  local venv_dir="$repo_root/runtime/databento/python-venv"
  local python_bin=""
  local candidate resolved
  for candidate in /usr/local/bin/python3 /opt/homebrew/bin/python3 python3 /usr/bin/python3; do
    resolved="$(command -v "$candidate" 2>/dev/null || true)"
    [[ -z "$resolved" && -x "$candidate" ]] && resolved="$candidate"
    if [[ -n "$resolved" ]]; then
      python_bin="$resolved"
      break
    fi
  done
  if [[ -z "$python_bin" ]]; then
    error "Cannot install Python deps because python3 was not found."
    return 1
  fi
  run_or_show "$python_bin" -m venv "$venv_dir"
  run_or_show "$venv_dir/bin/python" -m pip install --upgrade pip
  run_or_show "$venv_dir/bin/python" -m pip install 'databento>=0.43.0' 'databento-dbn>=0.21.0'
  if [[ $apply_mode -eq 1 ]]; then
    "$venv_dir/bin/python" - <<'PY'
import databento, databento_dbn
print("[LOCAL-PREP] Databento Python venv import check OK")
PY
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --apply) apply_mode=1 ;;
    --symbols=*) symbols_csv="${1#--symbols=}" ;;
    --exclude=*) exclude_csv="${1#--exclude=}" ;;
    --vault=*) vault="${1#--vault=}" ;;
    --offload-root=*) offload_root="${1#--offload-root=}" ;;
    --postgres-source=*) postgres_source="${1#--postgres-source=}" ;;
    --model-routing-source=*) model_routing_source="${1#--model-routing-source=}" ;;
    --install-python-deps) install_python_deps=1 ;;
    --no-update-configs) update_configs=0 ;;
    *) error "Unknown option: $1"; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ -z "$offload_root" ]]; then
  if [[ -d "$vault/trading-agent-offload/databento" ]]; then
    offload_root="$vault/trading-agent-offload/databento"
  else
    offload_root="$vault"
  fi
fi

if [[ ! -d "$offload_root" ]]; then
  error "External offload root not found: $offload_root"
  exit 1
fi
if [[ ! -d "$bots_dir" ]]; then
  error "Missing bots directory: $bots_dir"
  exit 1
fi

all_symbols=()
while IFS= read -r symbol; do [[ -n "$symbol" ]] && all_symbols+=("$symbol"); done < <(discover_symbols | LC_ALL=C sort -u)

include_symbols=()
if [[ -n "$symbols_csv" ]]; then
  while IFS= read -r symbol; do [[ -n "$symbol" ]] && include_symbols+=("$symbol"); done < <(parse_csv_symbols "$symbols_csv")
fi
exclude_symbols=()
if [[ -n "$exclude_csv" ]]; then
  while IFS= read -r symbol; do [[ -n "$symbol" ]] && exclude_symbols+=("$symbol"); done < <(parse_csv_symbols "$exclude_csv")
fi

selected_symbols=()
if [[ ${#include_symbols[@]} -gt 0 ]]; then
  for symbol in "${include_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${all_symbols[@]}"; then
      error "Requested symbol not found in generated bot configs: $symbol"
      exit 1
    fi
    contains_symbol "$symbol" "${selected_symbols[@]-}" || selected_symbols+=("$symbol")
  done
else
  selected_symbols=("${all_symbols[@]}")
fi
if [[ ${#exclude_symbols[@]} -gt 0 ]]; then
  filtered=()
  for symbol in "${selected_symbols[@]}"; do
    contains_symbol "$symbol" "${exclude_symbols[@]}" || filtered+=("$symbol")
  done
  selected_symbols=("${filtered[@]}")
fi
if [[ ${#selected_symbols[@]} -eq 0 ]]; then
  error "No symbols selected."
  exit 1
fi

log "mode=$([[ $apply_mode -eq 1 ]] && printf APPLY || printf DRY-RUN) offload_root=$offload_root selected=${#selected_symbols[@]}"

mkdir_targets=(
  "$repo_root/runtime/databento/logs"
  "$repo_root/runtime/databento/output"
  "$repo_root/runtime/databento/state"
  "$repo_root/runtime/databento/launchers"
)
for d in "${mkdir_targets[@]}"; do
  if [[ -L "$d" ]]; then run_or_show rm "$d"; fi
  run_or_show mkdir -p "$d"
done

if [[ -n "$postgres_source" ]]; then
  copy_file_to_local "$postgres_source" "$repo_root/runtime/postgres-local.properties" "postgres-local.properties"
elif [[ -f "$offload_root/runtime/postgres-local.properties" ]]; then
  copy_file_to_local "$offload_root/runtime/postgres-local.properties" "$repo_root/runtime/postgres-local.properties" "postgres-local.properties"
else
  warn "No postgres-local.properties found on external disk; creating minimal local placeholder."
  create_minimal_postgres_placeholder
fi

copy_dir_filtered \
  "$offload_root/model_exports/lifecycle_micro_20260523" \
  "$repo_root/model_exports/lifecycle_micro_20260523" \
  "lifecycle/micro model bundle"

copy_file_to_local \
  "$offload_root/training_data/databento_30s_20260523/symbol_model_plan.csv" \
  "$repo_root/training_data/databento_30s_20260523/symbol_model_plan.csv" \
  "symbol_model_plan.csv"

if [[ -z "$model_routing_source" ]]; then
  if [[ -f "$offload_root/runtime/databento/model-routing.csv" ]]; then
    model_routing_source="$offload_root/runtime/databento/model-routing.csv"
  else
    model_routing_source="$(find "$offload_root/runtime/databento/promotions" -name model-routing.csv -type f 2>/dev/null | LC_ALL=C sort | tail -1 || true)"
  fi
fi
if [[ -n "$model_routing_source" ]]; then
  copy_file_to_local "$model_routing_source" "$repo_root/runtime/databento/model-routing.csv" "model-routing.csv"
else
  warn "No model-routing.csv source found. Startup can fall back to default model dirs, but routing may differ."
fi

map_file="$(mktemp)"
trap 'rm -f "$map_file"' EXIT

for symbol in "${selected_symbols[@]}"; do
  symbol_lower="$(lower_symbol "$symbol")"
  properties_path="$(resolve_properties_path "$symbol_lower" || true)"
  if [[ -z "$properties_path" ]]; then
    error "Missing properties for $symbol"
    exit 1
  fi
  configured_model_dir="$(extract_prop "$properties_path" trading.model.dir)"
  if [[ -z "$configured_model_dir" ]]; then
    configured_model_dir="$repo_root/runtime/models/$symbol"
  fi
  if ! rel="$(relative_training_path_from_model_dir "$configured_model_dir")"; then
    warn "Cannot map $symbol model dir to external training_data path; keeping: $configured_model_dir"
    prepare_tee_log_target "$symbol"
    continue
  fi
  src_model_dir="$offload_root/training_data/$rel"
  dest_model_dir="$(local_bundle_dest_for_rel "$rel")"
  printf '%s\t%s\n' "$properties_path" "$dest_model_dir" >> "$map_file"
  if [[ ! -f "$dest_model_dir/.local-prep-copied" ]]; then
    copy_dir_filtered "$src_model_dir" "$dest_model_dir" "30s model bundle for $symbol"
    if [[ $apply_mode -eq 1 ]]; then
      touch "$dest_model_dir/.local-prep-copied"
    fi
  fi
  for artifact in "${required_30s_models[@]}"; do
    if [[ $apply_mode -eq 1 && ! -f "$dest_model_dir/$artifact" ]]; then
      error "Copied model dir for $symbol is missing required artifact: $dest_model_dir/$artifact"
      exit 1
    fi
  done
  prepare_tee_log_target "$symbol"
done

if [[ $update_configs -eq 1 ]]; then
  sort -u "$map_file" | while IFS=$'\t' read -r properties_path dest_model_dir; do
    [[ -n "$properties_path" && -n "$dest_model_dir" ]] || continue
    patch_model_dir_property "$properties_path" "$dest_model_dir"
  done
else
  warn "Skipping bot config rewrites because --no-update-configs was passed."
fi

if [[ $install_python_deps -eq 1 ]]; then
  install_databento_python_deps
fi

log "Prepared local startup artifacts for ${#selected_symbols[@]} selected symbol(s)."
if [[ $apply_mode -eq 0 ]]; then
  log "Dry run only. Re-run with --apply to copy and rewrite configs."
else
  log "Next verification command: ./verify_databento_local_startup_data.sh --symbols=$(printf '%s,' "${selected_symbols[@]}" | sed 's/,$//')"
  if [[ $install_python_deps -eq 1 ]]; then
    log "Use Databento Python override: -- --trading.databento.python-bin=runtime/databento/python-venv/bin/python"
  fi
fi



