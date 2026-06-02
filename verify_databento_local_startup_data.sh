#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./verify_databento_local_startup_data.sh [--symbols=CSV] [--exclude=CSV] [--tee] [--allow-external-symlinks] [-- <extra java args...>]

Verifies the local startup data needed by ./start_all_databento_bots.sh / ./run_symbol.sh:
  - packaged jar
  - runtime/postgres-local.properties
  - selected symbol bot configs
  - selected symbol 30s ONNX model directories and required route artifacts
  - lifecycle/micro model directory and required artifacts, unless TRADING_LIFECYCLE_MICRO_ENABLED=false
  - configured Databento model-routing/symbol-plan CSVs when present in bot configs
  - local tee log targets when --tee is used

The verifier fails on missing paths and on symlink components that resolve to /Volumes/... unless
--allow-external-symlinks is passed.
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
bots_dir="$repo_root/runtime/databento/bots"
jar_path="$repo_root/target/trading-agent-0.0.1-SNAPSHOT.jar"

symbols_csv=""
exclude_csv=""
tee_mode=0
allow_external_symlinks=0
extra_args=()
failures=0
warnings=0

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

required_lifecycle_models=(
  long_exit_lifecycle.onnx
  short_exit_lifecycle.onnx
  long_micro_entry_5s.onnx
  short_micro_entry_5s.onnx
  long_micro_exit_guard_5s.onnx
  short_micro_exit_guard_5s.onnx
  lifecycle_micro_scorecard.csv
  lifecycle_micro_route_manifest.json
)

trim_spaces() {
  printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
}

normalize_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
}

lower_symbol() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

is_truthy() {
  local lowered
  lowered="$(trim_spaces "$1" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

contains_symbol() {
  local needle="$1"
  shift || true
  local item
  for item in "$@"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

parse_csv_symbols() {
  local raw="$1"
  local old_ifs="$IFS"
  local entry trimmed
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

extract_prop_from_file() {
  local path="$1"
  local key="$2"
  if [[ ! -f "$path" ]]; then
    return 0
  fi
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

resolve_extra_arg_override() {
  local key="$1"
  local arg value=""
  for arg in "${extra_args[@]-}"; do
    if [[ "$arg" == --"$key"=* ]]; then
      value="${arg#--$key=}"
    fi
  done
  printf '%s' "$value"
}

resolve_symbol_properties_path() {
  local symbol_lower="$1"
  local candidate
  for candidate in \
    "$bots_dir/trading-${symbol_lower}.properties" \
    "$repo_root/runtime/trading-${symbol_lower}.properties"; do
    if [[ -f "$candidate" ]]; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

discover_symbols() {
  local path filename symbol
  shopt -s nullglob
  for path in "$bots_dir"/trading-*.properties; do
    filename="$(basename "$path")"
    [[ "$filename" == "trading-databento-template.properties" ]] && continue
    symbol="$(extract_prop_from_file "$path" "trading.symbol")"
    if [[ -z "$symbol" ]]; then
      symbol="${filename#trading-}"
      symbol="${symbol%.properties}"
    fi
    symbol="$(normalize_symbol "$symbol")"
    [[ -n "$symbol" ]] && printf '%s\n' "$symbol"
  done
  shopt -u nullglob
}

absolute_path() {
  local raw="$1"
  case "$raw" in
    /*) printf '%s' "$raw" ;;
    *) printf '%s/%s' "$repo_root" "$raw" ;;
  esac
}

path_symlink_issue() {
  local raw="$1"
  local abs current component target resolved
  abs="$(absolute_path "$raw")"
  case "$abs" in
    /Volumes/*)
      printf 'path is under /Volumes: %s' "$abs"
      return 0
      ;;
  esac

  current=""
  IFS='/' read -r -a components <<< "$abs"
  for component in "${components[@]}"; do
    [[ -z "$component" ]] && continue
    current="$current/$component"
    if [[ -L "$current" ]]; then
      target="$(readlink "$current")"
      case "$target" in
        /*) resolved="$target" ;;
        *) resolved="$(cd "$(dirname "$current")" 2>/dev/null && pwd -P)/$target" ;;
      esac
      case "$resolved" in
        /Volumes/*)
          if [[ $allow_external_symlinks -eq 0 ]]; then
            printf 'symlink component uses external volume: %s -> %s' "$current" "$target"
            return 0
          fi
          ;;
      esac
      if [[ ! -e "$current" ]]; then
        printf 'broken symlink component: %s -> %s' "$current" "$target"
        return 0
      fi
    fi
  done
  return 1
}

record_failure() {
  failures=$((failures + 1))
  printf '[LOCAL-DATA][ERROR] %s\n' "$*" >&2
}

record_warning() {
  warnings=$((warnings + 1))
  printf '[LOCAL-DATA][WARN] %s\n' "$*" >&2
}

check_required_file() {
  local path="$1"
  local label="$2"
  local issue abs
  abs="$(absolute_path "$path")"
  issue="$(path_symlink_issue "$path" || true)"
  if [[ -n "$issue" ]]; then
    record_failure "$label is not local-only: $path ($issue)"
    return 1
  fi
  if [[ ! -f "$abs" ]]; then
    record_failure "Missing $label: $path"
    return 1
  fi
  return 0
}

check_required_dir() {
  local path="$1"
  local label="$2"
  local issue abs
  abs="$(absolute_path "$path")"
  issue="$(path_symlink_issue "$path" || true)"
  if [[ -n "$issue" ]]; then
    record_failure "$label is not local-only: $path ($issue)"
    return 1
  fi
  if [[ ! -d "$abs" ]]; then
    record_failure "Missing $label: $path"
    return 1
  fi
  return 0
}

check_optional_config_file() {
  local path="$1"
  local label="$2"
  local issue abs
  [[ -n "$path" ]] || return 0
  abs="$(absolute_path "$path")"
  issue="$(path_symlink_issue "$path" || true)"
  if [[ -n "$issue" ]]; then
    record_failure "$label is not local-only: $path ($issue)"
    return 1
  fi
  if [[ ! -f "$abs" ]]; then
    record_warning "Configured $label is missing locally: $path"
    return 0
  fi
  return 0
}

check_writable_target_parent() {
  local path="$1"
  local label="$2"
  local parent issue abs_parent
  [[ -n "$path" ]] || return 0
  if [[ -L "$(absolute_path "$path")" ]]; then
    issue="$(path_symlink_issue "$path" || true)"
    if [[ -n "$issue" ]]; then
      record_failure "$label is not local-only: $path ($issue)"
      return 1
    fi
  fi
  parent="$(dirname "$path")"
  abs_parent="$(absolute_path "$parent")"
  issue="$(path_symlink_issue "$parent" || true)"
  if [[ -n "$issue" ]]; then
    record_failure "$label parent is not local-only: $parent ($issue)"
    return 1
  fi
  if [[ ! -d "$abs_parent" ]]; then
    record_warning "$label parent directory does not exist yet and will need to be created: $parent"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --symbols=*)
      symbols_csv="${1#--symbols=}"
      ;;
    --exclude=*)
      exclude_csv="${1#--exclude=}"
      ;;
    --tee)
      tee_mode=1
      ;;
    --allow-external-symlinks)
      allow_external_symlinks=1
      ;;
    --)
      shift
      extra_args=("$@")
      break
      ;;
    *)
      record_failure "Unknown option: $1"
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -d "$bots_dir" ]]; then
  record_failure "Missing Databento bots directory: $bots_dir"
fi

all_symbols=()
if [[ -d "$bots_dir" ]]; then
  while IFS= read -r symbol; do
    [[ -n "$symbol" ]] && all_symbols+=("$symbol")
  done < <(discover_symbols | LC_ALL=C sort -u)
fi

include_symbols=()
if [[ -n "$symbols_csv" ]]; then
  while IFS= read -r symbol; do
    [[ -n "$symbol" ]] && include_symbols+=("$symbol")
  done < <(parse_csv_symbols "$symbols_csv")
fi

exclude_symbols=()
if [[ -n "$exclude_csv" ]]; then
  while IFS= read -r symbol; do
    [[ -n "$symbol" ]] && exclude_symbols+=("$symbol")
  done < <(parse_csv_symbols "$exclude_csv")
fi

selected_symbols=()
if [[ ${#include_symbols[@]} -gt 0 ]]; then
  for symbol in "${include_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${all_symbols[@]-}"; then
      record_failure "Requested symbol not found in generated bot configs: $symbol"
      continue
    fi
    if ! contains_symbol "$symbol" "${selected_symbols[@]-}"; then
      selected_symbols+=("$symbol")
    fi
  done
else
  selected_symbols=("${all_symbols[@]}")
fi

if [[ ${#exclude_symbols[@]} -gt 0 ]]; then
  filtered_symbols=()F
  for symbol in "${selected_symbols[@]-}"; do
    if ! contains_symbol "$symbol" "${exclude_symbols[@]}"; then
      filtered_symbols+=("$symbol")
    fi
  done
  selected_symbols=("${filtered_symbols[@]}")
fi

if [[ ${#selected_symbols[@]} -eq 0 ]]; then
  record_failure "No symbols selected for local startup data verification."
fi

printf '[LOCAL-DATA] verifying selected_symbols=%s\n' "$(printf '%s,' "${selected_symbols[@]-}" | sed 's/,$//')"

check_required_file "$jar_path" "packaged jar" || true
check_required_file "$repo_root/runtime/postgres-local.properties" "runtime postgres properties" || true

lifecycle_micro_enabled="${TRADING_LIFECYCLE_MICRO_ENABLED:-true}"
if is_truthy "$lifecycle_micro_enabled"; then
  lifecycle_model_dir="${TRADING_LIFECYCLE_MODEL_DIR:-$repo_root/model_exports/lifecycle_micro_20260523}"
  if check_required_dir "$lifecycle_model_dir" "lifecycle/micro model directory"; then
    for artifact in "${required_lifecycle_models[@]}"; do
      check_required_file "$lifecycle_model_dir/$artifact" "lifecycle/micro artifact $artifact" || true
    done
  fi
else
  printf '[LOCAL-DATA] lifecycle/micro verification skipped because TRADING_LIFECYCLE_MICRO_ENABLED=%s\n' "$lifecycle_micro_enabled"
fi

model_dir_override="$(resolve_extra_arg_override trading.model.dir)"
model_routing_override="$(resolve_extra_arg_override trading.databento.model-routing-csv)"
symbol_plan_override="$(resolve_extra_arg_override trading.databento.symbol-plan-csv)"
tee_file_override="$(resolve_extra_arg_override trading.log.file)"
app_log_override="$(resolve_extra_arg_override logging.file.name)"

for symbol in "${selected_symbols[@]-}"; do
  symbol_lower="$(lower_symbol "$symbol")"
  properties_path="$(resolve_symbol_properties_path "$symbol_lower" || true)"
  if [[ -z "$properties_path" ]]; then
    record_failure "Missing symbol runtime properties for $symbol"
    continue
  fi
  check_required_file "$properties_path" "symbol runtime properties for $symbol" || true

  model_dir="${model_dir_override:-$(extract_prop_from_file "$properties_path" trading.model.dir)}"
  model_dir="${model_dir:-$repo_root/runtime/models/$symbol}"
  if check_required_dir "$model_dir" "30s model directory for $symbol"; then
    for artifact in "${required_30s_models[@]}"; do
      check_required_file "$model_dir/$artifact" "30s route model for $symbol: $artifact" || true
    done
  fi

  model_routing_csv="${model_routing_override:-$(extract_prop_from_file "$properties_path" trading.databento.model-routing-csv)}"
  symbol_plan_csv="${symbol_plan_override:-$(extract_prop_from_file "$properties_path" trading.databento.symbol-plan-csv)}"
  check_optional_config_file "$model_routing_csv" "Databento model-routing CSV for $symbol" || true
  check_optional_config_file "$symbol_plan_csv" "Databento symbol-plan CSV for $symbol" || true

  trade_log_file="${tee_file_override:-$(extract_prop_from_file "$properties_path" trading.log.file)}"
  app_log_file="${app_log_override:-$(extract_prop_from_file "$properties_path" logging.file.name)}"
  check_writable_target_parent "$trade_log_file" "trade log file for $symbol" || true
  check_writable_target_parent "$app_log_file" "application log file for $symbol" || true
  if [[ $tee_mode -eq 1 ]]; then
    check_writable_target_parent "$repo_root/runtime/${symbol_lower}_live_trade_logs.txt" "tee log file for $symbol" || true
  fi
done

if [[ $warnings -gt 0 ]]; then
  printf '[LOCAL-DATA] warnings=%s (warnings do not block startup verification unless listed as ERROR)\n' "$warnings" >&2
fi

if [[ $failures -gt 0 ]]; then
  cat >&2 <<EOF
[LOCAL-DATA][ERROR] Local startup data verification failed with $failures error(s).
[LOCAL-DATA][ERROR] Copy the missing model/config data from the external disk into local repo paths, then update bot configs or pass overrides.
[LOCAL-DATA][ERROR] Recommended local targets:
[LOCAL-DATA][ERROR]   - lifecycle bundle: $repo_root/model_exports/lifecycle_micro_20260523
[LOCAL-DATA][ERROR]   - per-symbol 30s models: $repo_root/runtime/models/<SYMBOL>
[LOCAL-DATA][ERROR]   - postgres properties: $repo_root/runtime/postgres-local.properties
[LOCAL-DATA][ERROR]   - routing CSV: $repo_root/runtime/databento/model-routing.csv
[LOCAL-DATA][ERROR]   - symbol plan CSV: $repo_root/training_data/databento_30s_20260523/symbol_model_plan.csv
EOF
  exit 1
fi

printf '[LOCAL-DATA] OK: required startup data for %s symbol(s) is present and local-only.\n' "${#selected_symbols[@]}"

