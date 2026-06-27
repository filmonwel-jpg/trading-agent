#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SYMBOLS_CSV="${BACKTEST_SYMBOLS:-${BACKTEST_SYMBOL:-}}"
SYMBOLS_FILE="${BACKTEST_SYMBOLS_FILE:-runtime/symbols_100.txt}"
EXCLUDE_SYMBOLS_CSV="${BACKTEST_EXCLUDE_SYMBOLS:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
OUTPUT_DIR="${BACKTEST_OUTPUT_DIR:-runtime/backtests}"
DEFAULT_SETUP_MODEL_DIR="runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854"
DEFAULT_LIFECYCLE_MODEL_DIR="runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports"
MODEL_DIR="${MODEL_DIR:-${TRADING_MODEL_DIR:-${TRADING_SETUP_MODEL_DIR:-$DEFAULT_SETUP_MODEL_DIR}}}"
PYTHON_BIN="${PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-}}"
DEFAULT_DATABENTO_ENV_FILE="runtime/databento.env"
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-}}"
DATABENTO_API_KEY_SOURCE="environment"
BRIDGE_DATABENTO_ENV_FILE="databento_ibkr_bridge/.env"
DRY_RUN="${DRY_RUN:-false}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-0}"
BACKTEST_MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
BACKTEST_MAX_SHARE_CAP="${BACKTEST_MAX_SHARE_CAP:-500}"
BACKTEST_DATABENTO_SOURCE="${BACKTEST_DATABENTO_SOURCE:-api}"
BACKTEST_RECORDED_EVENTS_FILE="${BACKTEST_RECORDED_EVENTS_FILE:-}"
SKIP_BUILD="${SKIP_BUILD:-false}"
ROUTING_CSV="${TRADING_DATABENTO_MODEL_ROUTING_CSV:-runtime/databento/model-routing.csv}"
CLASSPATH_FILE="${BACKTEST_CLASSPATH_FILE:-runtime/backtests/databento_ibkr_sim_backtest_cp.txt}"
LIFECYCLE_MICRO_ENABLED="${TRADING_LIFECYCLE_MICRO_ENABLED:-true}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-$DEFAULT_LIFECYCLE_MODEL_DIR}"
SETUP_THRESHOLDS_FILE="${TRADING_SETUP_THRESHOLDS_FILE:-}"
SETUP_THRESHOLDS_FILE_EXPLICIT="false"
[[ -n "$SETUP_THRESHOLDS_FILE" ]] && SETUP_THRESHOLDS_FILE_EXPLICIT="true"
BACKTEST_PREVIOUS_CLOSE="${BACKTEST_PREVIOUS_CLOSE:-}"
MICRO_LONG_ENTRY_THRESHOLD="${MICRO_LONG_ENTRY_THRESHOLD:-${STRATEGY_MICRO_LONG_ENTRY_THRESHOLD:-}}"
MICRO_SHORT_ENTRY_THRESHOLD="${MICRO_SHORT_ENTRY_THRESHOLD:-${STRATEGY_MICRO_SHORT_ENTRY_THRESHOLD:-}}"
LIFECYCLE_LONG_EXIT_THRESHOLD="${LIFECYCLE_LONG_EXIT_THRESHOLD:-${STRATEGY_LIFECYCLE_LONG_EXIT_THRESHOLD:-}}"
LIFECYCLE_SHORT_EXIT_THRESHOLD="${LIFECYCLE_SHORT_EXIT_THRESHOLD:-${STRATEGY_LIFECYCLE_SHORT_EXIT_THRESHOLD:-}}"
MICRO_LONG_EXIT_GUARD_THRESHOLD="${MICRO_LONG_EXIT_GUARD_THRESHOLD:-${STRATEGY_MICRO_LONG_EXIT_GUARD_THRESHOLD:-}}"
MICRO_SHORT_EXIT_GUARD_THRESHOLD="${MICRO_SHORT_EXIT_GUARD_THRESHOLD:-${STRATEGY_MICRO_SHORT_EXIT_GUARD_THRESHOLD:-}}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_databento_historical_ibkr_sim_backtest.sh [options]

Options:
  --symbol SYMBOL          Underlying to replay. Can be repeated. If omitted, --symbols-file is used.
  --symbols CSV            Comma-separated underlyings to replay, e.g. TSLA,NVDA,AAPL.
  --symbols-file FILE      Newline/comma-separated default universe. Default: runtime/symbols_100.txt
  --exclude CSV            Comma-separated symbols to skip after resolving the selected/default universe.
  --list-symbols           Print the resolved symbols and exit before build/API work.
  --start YYYY-MM-DD       First replay day. Date-only values replay 09:30 America/New_York onward.
  --end YYYY-MM-DD         Last replay day. Date-only values replay through 16:00 America/New_York.
  --output-dir DIR         Directory for trade/order CSV outputs. Default: runtime/backtests
  --model-dir DIR          30s ONNX setup bundle directory. Default: runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
  --setup-thresholds-file F Setup threshold properties. Default: <model-dir>/setup_runtime_thresholds.properties when present.
  --python-bin PATH        Python with databento installed. If omitted, common python3 locations are searched.
  --databento-env-file F   Env file containing DATABENTO_API_KEY. Default: runtime/databento.env
  --source api|ndjson     Stream source for the Java backtester. Default: api
  --recorded-events FILE  Recorded normalized NDJSON/NDJSON.GZ file for --source ndjson. Can be comma-separated.
  --dry-run                Validate wiring without downloading Databento data.
  --timeout-seconds N      Kill the Databento stream if it has not completed after N seconds. Default: 0 (no timeout)
  --max-trades N           Strategy max trades during replay. Default: 2000
  --max-share-cap N        Simulated broker max shares per order. Default: 500
  --lifecycle-model-dir D  Lifecycle/micro ONNX bundle. Default: runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
  --micro-long-entry-threshold P   Override 5s long micro-entry threshold from lifecycle scorecard.
  --micro-short-entry-threshold P  Override 5s short micro-entry threshold from lifecycle scorecard.
  --lifecycle-exit-threshold P     Override both long/short lifecycle-exit thresholds.
  --lifecycle-long-exit-threshold P   Override 30s long lifecycle-exit threshold from lifecycle scorecard.
  --lifecycle-short-exit-threshold P  Override 30s short lifecycle-exit threshold from lifecycle scorecard.
  --micro-exit-guard-threshold P   Override both long/short 5s micro-exit guard thresholds.
  --micro-long-exit-guard-threshold P   Override 5s long micro-exit guard threshold from lifecycle scorecard.
  --micro-short-exit-guard-threshold P  Override 5s short micro-exit guard threshold from lifecycle scorecard.
  --disable-lifecycle-micro Disable lifecycle exit and 5s micro entry/exit guard routes.
  --previous-close PRICE  Override previous close injected into the strategy before replay bars.
  --classpath-file FILE    Maven runtime classpath cache. Default: runtime/backtests/databento_ibkr_sim_backtest_cp.txt
  --skip-build             Reuse target/classes and the cached Maven classpath.
  --help                   Show this help.

Outputs:
  <output-dir>/<symbol>-<start>-to-<end>-<timestamp>-trades.csv
  <output-dir>/<symbol>-<start>-to-<end>-<timestamp>-orders.csv

Default symbol behavior:
  If neither --symbol nor --symbols is supplied, the script replays every symbol in --symbols-file.
  BACKTEST_SYMBOLS or BACKTEST_SYMBOL can also be used from the environment.
USAGE
}

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

normalize_symbol() {
  trim "$1" | tr '[:lower:]' '[:upper:]'
}

append_symbol() {
  local symbol normalized existing
  symbol="$1"
  normalized="$(normalize_symbol "$symbol")"
  [[ -n "$normalized" ]] || return 0
  for existing in "${symbols[@]-}"; do
    [[ "$existing" == "$normalized" ]] && return 0
  done
  symbols+=("$normalized")
}

parse_symbols_csv_into() {
  local raw="$1" old_ifs entry
  old_ifs="$IFS"
  IFS=','
  for entry in $raw; do
    append_symbol "$entry"
  done
  IFS="$old_ifs"
}

load_symbols_file_into() {
  local path="$1" line token
  [[ -f "$path" ]] || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="${line//,/ }"
    for token in $line; do
      append_symbol "$token"
    done
  done < "$path"
}

contains_symbol() {
  local needle="$1" item
  shift || true
  for item in "$@"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

strip_quotes() {
  local value="$1"
  if [[ ${#value} -ge 2 ]]; then
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value#\"}"; value="${value%\"}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value#\'}"; value="${value%\'}"
    fi
  fi
  printf '%s' "$value"
}

load_env_file() {
  local path="$1" line key value
  [[ -f "$path" ]] || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(trim "$line")"
    [[ -n "$line" && "${line#\#}" == "$line" && "$line" == *=* ]] || continue
    [[ "$line" == export\ * ]] && line="$(trim "${line#export }")"
    key="$(trim "${line%%=*}")"
    value="$(strip_quotes "$(trim "${line#*=}")")"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ && -n "$value" ]] || continue
    if [[ "$key" == "DATABENTO_API_KEY" ]]; then
      if ! usable_databento_key "${DATABENTO_API_KEY:-}" || usable_databento_key "$value"; then
        export "$key=$value"
      fi
    elif [[ -z "${!key-}" ]]; then
      export "$key=$value"
    fi
  done < "$path"
}

get_prop() {
  local key="$1" file="$2"
  [[ -f "$file" ]] || return 0
  awk -F= -v search_key="$key" '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    $1 ~ /^[[:space:]]*[^=]+[[:space:]]*$/ {
      current=$1
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current)
      if (current == search_key) {
        value=substr($0, index($0, "=") + 1)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        found=value
      }
    }
    END {if (found != "") print found}
  ' "$file"
}

configured_databento_env_file() {
  local symbol="$1" lower_symbol prop
  lower_symbol="$(printf '%s' "$symbol" | tr '[:upper:]' '[:lower:]')"
  prop="$(get_prop trading.databento.env-file "$ROOT/runtime/databento/bots/trading-${lower_symbol}.properties" || true)"
  if [[ -n "$prop" ]]; then
    printf '%s' "$prop"
    return 0
  fi
  prop="$(get_prop trading.databento.env-file "$ROOT/src/main/resources/application.properties" || true)"
  if [[ -n "$prop" ]]; then
    printf '%s' "$prop"
    return 0
  fi
  printf '%s' "$DEFAULT_DATABENTO_ENV_FILE"
}

load_launchctl_databento_key() {
  local value
  command -v launchctl >/dev/null 2>&1 || return 1
  value="$(launchctl getenv DATABENTO_API_KEY 2>/dev/null || true)"
  if usable_databento_key "$value"; then
    export DATABENTO_API_KEY="$value"
    return 0
  fi
  return 1
}

load_application_databento_credentials() {
  local app_props="$ROOT/src/main/resources/application.properties"
  local api_key api_userid trimmed_userid
  [[ -f "$app_props" ]] || return 1
  api_key="$(get_prop trading.databento.api.key "$app_props" || true)"
  api_userid="$(get_prop trading.databento.api.userid "$app_props" || true)"
  if ! usable_databento_key "$api_key"; then
    return 1
  fi
  export DATABENTO_API_KEY="$api_key"
  trimmed_userid="$(trim "$api_userid")"
  if [[ -n "$trimmed_userid" ]]; then
    export DATABENTO_API_USERID="$trimmed_userid"
    export DATABENTO_USERID="$trimmed_userid"
  fi
  return 0
}

configure_python_ca_bundle() {
  local existing_bundle ca_dir ca_bundle tmp_bundle certifi_bundle keychain
  existing_bundle="${REQUESTS_CA_BUNDLE:-${SSL_CERT_FILE:-}}"
  if [[ -n "$existing_bundle" && -f "$existing_bundle" ]]; then
    export REQUESTS_CA_BUNDLE="$existing_bundle"
    export SSL_CERT_FILE="$existing_bundle"
    export CURL_CA_BUNDLE="${CURL_CA_BUNDLE:-$existing_bundle}"
    printf '[BACKTEST] python_ca_bundle=%s\n' "$existing_bundle"
    return 0
  fi
  [[ "$(uname -s 2>/dev/null || true)" == "Darwin" ]] || return 0
  command -v security >/dev/null 2>&1 || return 0
  ca_dir="$ROOT/runtime/databento/certs"
  ca_bundle="$ca_dir/macos-keychain-ca-bundle.pem"
  mkdir -p "$ca_dir"
  tmp_bundle="$ca_bundle.tmp.$$"
  : > "$tmp_bundle"
  certifi_bundle="$($PYTHON_BIN -c 'import certifi; print(certifi.where())' 2>/dev/null || true)"
  if [[ -n "$certifi_bundle" && -f "$certifi_bundle" ]]; then
    cat "$certifi_bundle" >> "$tmp_bundle"
  fi
  for keychain in \
    /System/Library/Keychains/SystemRootCertificates.keychain \
    /Library/Keychains/System.keychain \
    "$HOME/Library/Keychains/login.keychain-db"; do
    [[ -e "$keychain" ]] || continue
    security find-certificate -a -p "$keychain" >> "$tmp_bundle" 2>/dev/null || true
  done
  if grep -q 'BEGIN CERTIFICATE' "$tmp_bundle"; then
    mv "$tmp_bundle" "$ca_bundle"
    export REQUESTS_CA_BUNDLE="$ca_bundle"
    export SSL_CERT_FILE="$ca_bundle"
    export CURL_CA_BUNDLE="$ca_bundle"
    printf '[BACKTEST] python_ca_bundle=%s\n' "$ca_bundle"
  else
    rm -f "$tmp_bundle"
  fi
}

usable_databento_key() {
  local value lowered
  value="$(trim "${1:-}")"
  lowered="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    ""|replace_me|paste_your_api_key_here|\<your-key\>|\<your_databento_api_key\>|changeme) return 1 ;;
    *) return 0 ;;
  esac
}

resolve_python_bin() {
  local configured="$1" candidate resolved
  local -a candidates=()
  [[ -n "$configured" ]] && candidates+=("$configured")
  candidates+=("$ROOT/.venv/bin/python" "$ROOT/.venv/bin/python3" "$ROOT/runtime/databento/python-venv/bin/python3" "$HOME/miniforge3/bin/python3" python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 /usr/bin/python3)
  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate" ]]; then
      resolved="$candidate"
    else
      resolved="$(command -v "$candidate" 2>/dev/null || true)"
    fi
    if [[ -n "$resolved" ]] && "$resolved" -c 'import databento, pandas' >/dev/null 2>&1; then
      printf '%s' "$resolved"
      return 0
    fi
  done
  return 1
}

resolve_basic_python_bin() {
  local configured="$1" candidate resolved
  local -a candidates=()
  [[ -n "$configured" ]] && candidates+=("$configured")
  candidates+=("$ROOT/.venv/bin/python" "$ROOT/.venv/bin/python3" "$ROOT/runtime/databento/python-venv/bin/python3" "$HOME/miniforge3/bin/python3" python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 /usr/bin/python3)
  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate" ]]; then
      resolved="$candidate"
    else
      resolved="$(command -v "$candidate" 2>/dev/null || true)"
    fi
    if [[ -n "$resolved" ]] && "$resolved" -c 'import pandas' >/dev/null 2>&1; then
      printf '%s' "$resolved"
      return 0
    fi
  done
  return 1
}

route_model_dir() {
  local symbol="$1" routing_csv="$2"
  [[ -f "$routing_csv" ]] || return 0
  awk -F, -v symbol="$symbol" 'NR > 1 && toupper($1) == symbol {print $4; exit}' "$routing_csv"
}

csv_threshold() {
  local model_name="$1" default_value="$2" scorecard="$3"
  if [[ -f "$scorecard" ]]; then
    awk -F, -v model="$model_name" -v fallback="$default_value" '
      function trim(s) { gsub(/^[[:space:]\r\n]+|[[:space:]\r\n]+$/, "", s); return s }
      function field(name) { return idx[name] ? trim($(idx[name])) : "" }
      NR == 1 {
        for (i = 1; i <= NF; i++) idx[trim($i)] = i
        next
      }
      field("model") == model {
        value = fallback
        raw_threshold = field("threshold")
        posthoc_threshold = field("posthoc_threshold")
        if (raw_threshold != "") value = raw_threshold
        if (posthoc_threshold != "") value = posthoc_threshold
        printf "%.4f", value + 0
        found=1
        exit
      }
      END {if (!found) printf "%.4f", fallback + 0}
    ' "$scorecard"
  else
    printf '%.4f' "$default_value"
  fi
}

append_setup_threshold_props() {
  local threshold_file="$1" raw_key raw_value key value
  while IFS='=' read -r raw_key raw_value || [[ -n "${raw_key:-}" ]]; do
    key="$(trim "${raw_key:-}")"
    value="$(trim "${raw_value:-}")"
    [[ -z "$key" || "$key" == \#* ]] && continue
    JAVA_PROPS+=("-D$key=$value")
  done < "$threshold_file"
}

symbols=()
exclude_symbols=()
LIST_SYMBOLS_ONLY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbol) append_symbol "$2"; shift 2 ;;
    --symbol=*) append_symbol "${1#--symbol=}"; shift ;;
    --symbols) parse_symbols_csv_into "$2"; shift 2 ;;
    --symbols=*) parse_symbols_csv_into "${1#--symbols=}"; shift ;;
    --symbols-file) SYMBOLS_FILE="$2"; shift 2 ;;
    --symbols-file=*) SYMBOLS_FILE="${1#--symbols-file=}"; shift ;;
    --exclude) EXCLUDE_SYMBOLS_CSV="$2"; shift 2 ;;
    --exclude=*) EXCLUDE_SYMBOLS_CSV="${1#--exclude=}"; shift ;;
    --list-symbols) LIST_SYMBOLS_ONLY="true"; shift ;;
    --start|--start-date) START_DATE="$2"; shift 2 ;;
    --end|--end-date) END_DATE="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --model-dir) MODEL_DIR="$2"; shift 2 ;;
    --setup-thresholds-file) SETUP_THRESHOLDS_FILE="$2"; SETUP_THRESHOLDS_FILE_EXPLICIT="true"; shift 2 ;;
    --setup-thresholds-file=*) SETUP_THRESHOLDS_FILE="${1#--setup-thresholds-file=}"; SETUP_THRESHOLDS_FILE_EXPLICIT="true"; shift ;;
    --python-bin) PYTHON_BIN="$2"; shift 2 ;;
    --databento-env-file) DATABENTO_ENV_FILE="$2"; shift 2 ;;
    --source) BACKTEST_DATABENTO_SOURCE="$2"; shift 2 ;;
    --source=*) BACKTEST_DATABENTO_SOURCE="${1#--source=}"; shift ;;
    --recorded-events|--input-file) BACKTEST_RECORDED_EVENTS_FILE="$2"; shift 2 ;;
    --recorded-events=*|--input-file=*) BACKTEST_RECORDED_EVENTS_FILE="${1#*=}"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --max-trades) BACKTEST_MAX_TRADES="$2"; shift 2 ;;
    --max-share-cap) BACKTEST_MAX_SHARE_CAP="$2"; shift 2 ;;
    --lifecycle-model-dir) LIFECYCLE_MODEL_DIR="$2"; shift 2 ;;
    --lifecycle-model-dir=*) LIFECYCLE_MODEL_DIR="${1#--lifecycle-model-dir=}"; shift ;;
    --micro-long-entry-threshold) MICRO_LONG_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-long-entry-threshold=*) MICRO_LONG_ENTRY_THRESHOLD="${1#--micro-long-entry-threshold=}"; shift ;;
    --micro-short-entry-threshold) MICRO_SHORT_ENTRY_THRESHOLD="$2"; shift 2 ;;
    --micro-short-entry-threshold=*) MICRO_SHORT_ENTRY_THRESHOLD="${1#--micro-short-entry-threshold=}"; shift ;;
    --lifecycle-exit-threshold) LIFECYCLE_LONG_EXIT_THRESHOLD="$2"; LIFECYCLE_SHORT_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-exit-threshold=*) LIFECYCLE_LONG_EXIT_THRESHOLD="${1#--lifecycle-exit-threshold=}"; LIFECYCLE_SHORT_EXIT_THRESHOLD="${1#--lifecycle-exit-threshold=}"; shift ;;
    --lifecycle-long-exit-threshold) LIFECYCLE_LONG_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-long-exit-threshold=*) LIFECYCLE_LONG_EXIT_THRESHOLD="${1#--lifecycle-long-exit-threshold=}"; shift ;;
    --lifecycle-short-exit-threshold) LIFECYCLE_SHORT_EXIT_THRESHOLD="$2"; shift 2 ;;
    --lifecycle-short-exit-threshold=*) LIFECYCLE_SHORT_EXIT_THRESHOLD="${1#--lifecycle-short-exit-threshold=}"; shift ;;
    --micro-exit-guard-threshold) MICRO_LONG_EXIT_GUARD_THRESHOLD="$2"; MICRO_SHORT_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-exit-guard-threshold=*) MICRO_LONG_EXIT_GUARD_THRESHOLD="${1#--micro-exit-guard-threshold=}"; MICRO_SHORT_EXIT_GUARD_THRESHOLD="${1#--micro-exit-guard-threshold=}"; shift ;;
    --micro-long-exit-guard-threshold) MICRO_LONG_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-long-exit-guard-threshold=*) MICRO_LONG_EXIT_GUARD_THRESHOLD="${1#--micro-long-exit-guard-threshold=}"; shift ;;
    --micro-short-exit-guard-threshold) MICRO_SHORT_EXIT_GUARD_THRESHOLD="$2"; shift 2 ;;
    --micro-short-exit-guard-threshold=*) MICRO_SHORT_EXIT_GUARD_THRESHOLD="${1#--micro-short-exit-guard-threshold=}"; shift ;;
    --disable-lifecycle-micro) LIFECYCLE_MICRO_ENABLED="false"; shift ;;
    --previous-close) BACKTEST_PREVIOUS_CLOSE="$2"; shift 2 ;;
    --previous-close=*) BACKTEST_PREVIOUS_CLOSE="${1#--previous-close=}"; shift ;;
    --classpath-file) CLASSPATH_FILE="$2"; shift 2 ;;
    --classpath-file=*) CLASSPATH_FILE="${1#--classpath-file=}"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "[BACKTEST][ERROR] Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

BACKTEST_DATABENTO_SOURCE="$(printf '%s' "$BACKTEST_DATABENTO_SOURCE" | tr '[:upper:]' '[:lower:]')"
if [[ "$BACKTEST_DATABENTO_SOURCE" != "api" && "$BACKTEST_DATABENTO_SOURCE" != "ndjson" ]]; then
  echo "[BACKTEST][ERROR] --source must be api or ndjson." >&2
  exit 2
fi
if ! truthy "$LIST_SYMBOLS_ONLY" && [[ "$BACKTEST_DATABENTO_SOURCE" == "api" && ( -z "$START_DATE" || -z "$END_DATE" ) ]]; then
  echo "[BACKTEST][ERROR] --start and --end are required." >&2
  usage >&2
  exit 2
fi
if ! truthy "$LIST_SYMBOLS_ONLY" && [[ "$BACKTEST_DATABENTO_SOURCE" == "ndjson" && -z "$BACKTEST_RECORDED_EVENTS_FILE" ]]; then
  echo "[BACKTEST][ERROR] --source ndjson requires --recorded-events." >&2
  usage >&2
  exit 2
fi

[[ "$OUTPUT_DIR" != /* ]] && OUTPUT_DIR="$ROOT/$OUTPUT_DIR"
[[ "$ROUTING_CSV" != /* ]] && ROUTING_CSV="$ROOT/$ROUTING_CSV"
[[ "$SYMBOLS_FILE" != /* ]] && SYMBOLS_FILE="$ROOT/$SYMBOLS_FILE"
[[ "$LIFECYCLE_MODEL_DIR" != /* ]] && LIFECYCLE_MODEL_DIR="$ROOT/$LIFECYCLE_MODEL_DIR"
[[ "$CLASSPATH_FILE" != /* ]] && CLASSPATH_FILE="$ROOT/$CLASSPATH_FILE"

CLASSPATH_PARENT="$(dirname "$CLASSPATH_FILE")"
mkdir -p "$CLASSPATH_PARENT" 2>/dev/null || true
if [[ ! -d "$CLASSPATH_PARENT" || ! -w "$CLASSPATH_PARENT" ]]; then
  FALLBACK_CLASSPATH_FILE="$ROOT/runtime/databento_ibkr_sim_backtest_cp.txt"
  echo "[BACKTEST][WARN] Classpath cache directory is not writable: $CLASSPATH_PARENT; using $FALLBACK_CLASSPATH_FILE" >&2
  CLASSPATH_FILE="$FALLBACK_CLASSPATH_FILE"
  mkdir -p "$(dirname "$CLASSPATH_FILE")"
fi

if [[ ${#symbols[@]} -eq 0 && -n "$SYMBOLS_CSV" ]]; then
  parse_symbols_csv_into "$SYMBOLS_CSV"
fi
if [[ ${#symbols[@]} -eq 0 ]]; then
  if ! load_symbols_file_into "$SYMBOLS_FILE"; then
    echo "[BACKTEST][ERROR] No symbols supplied and default symbols file is missing: $SYMBOLS_FILE" >&2
    exit 2
  fi
fi
if [[ -n "$EXCLUDE_SYMBOLS_CSV" ]]; then
  old_symbols=("${symbols[@]}")
  symbols=()
  parse_symbols_csv_into "$EXCLUDE_SYMBOLS_CSV"
  exclude_symbols=("${symbols[@]}")
  symbols=()
  for symbol in "${old_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${exclude_symbols[@]-}"; then
      symbols+=("$symbol")
    fi
  done
fi
if [[ ${#symbols[@]} -eq 0 ]]; then
  echo "[BACKTEST][ERROR] No symbols remain after applying --symbol/--symbols/--symbols-file/--exclude." >&2
  exit 2
fi

if [[ -z "$DATABENTO_ENV_FILE" ]]; then
  DATABENTO_ENV_FILE="$(configured_databento_env_file "${symbols[0]}")"
fi
[[ "$DATABENTO_ENV_FILE" != /* ]] && DATABENTO_ENV_FILE="$ROOT/$DATABENTO_ENV_FILE"

if truthy "$LIST_SYMBOLS_ONLY"; then
  printf '[BACKTEST] symbols=%s\n' "$(printf '%s,' "${symbols[@]}" | sed 's/,$//')" >&2
  printf '[BACKTEST] symbols_count=%s symbols_file=%s\n' "${#symbols[@]}" "$SYMBOLS_FILE" >&2
  printf '%s\n' "${symbols[@]}"
  exit 0
fi
if ! mkdir -p "$OUTPUT_DIR"; then
  echo "[BACKTEST][ERROR] Could not create output directory: $OUTPUT_DIR" >&2
  echo "[BACKTEST][ERROR] If this points at /Volumes/DatabentoVault, remount the external disk read/write, run on the write-capable computer, or pass --output-dir/CORE5_OUTPUT_DIR/BACKTEST_OUTPUT_DIR to a writable directory." >&2
  exit 1
fi
if [[ ! -d "$OUTPUT_DIR" || ! -w "$OUTPUT_DIR" ]]; then
  echo "[BACKTEST][ERROR] Output directory is not writable: $OUTPUT_DIR" >&2
  echo "[BACKTEST][ERROR] If this points at /Volumes/DatabentoVault, this computer may have the disk mounted read-only; run on the write-capable computer or override --output-dir." >&2
  exit 1
fi
printf '[BACKTEST] symbols=%s\n' "$(printf '%s,' "${symbols[@]}" | sed 's/,$//')"
printf '[BACKTEST] symbols_count=%s symbols_file=%s\n' "${#symbols[@]}" "$SYMBOLS_FILE"
printf '[BACKTEST] classpath_file=%s\n' "$CLASSPATH_FILE"
printf '[BACKTEST] source=%s recorded_events=%s\n' "$BACKTEST_DATABENTO_SOURCE" "${BACKTEST_RECORDED_EVENTS_FILE:-<none>}"

if usable_databento_key "${DATABENTO_API_KEY:-}"; then
  DATABENTO_API_KEY_SOURCE="environment"
elif load_launchctl_databento_key; then
  DATABENTO_API_KEY_SOURCE="launchctl:DATABENTO_API_KEY"
else
  load_env_file "$DATABENTO_ENV_FILE" || true
  if usable_databento_key "${DATABENTO_API_KEY:-}"; then
    DATABENTO_API_KEY_SOURCE="env-file:$DATABENTO_ENV_FILE"
  elif [[ -f "$ROOT/$BRIDGE_DATABENTO_ENV_FILE" ]] && load_env_file "$ROOT/$BRIDGE_DATABENTO_ENV_FILE" && usable_databento_key "${DATABENTO_API_KEY:-}"; then
    DATABENTO_API_KEY_SOURCE="env-file:$ROOT/$BRIDGE_DATABENTO_ENV_FILE"
  elif load_application_databento_credentials; then
    DATABENTO_API_KEY_SOURCE="application.properties:trading.databento.api.key"
  else
    DATABENTO_API_KEY_SOURCE="missing"
  fi
fi
printf '[BACKTEST] databento_api_key_source=%s\n' "$DATABENTO_API_KEY_SOURCE"
if [[ "$BACKTEST_DATABENTO_SOURCE" == "api" ]] && ! truthy "$DRY_RUN" && ! usable_databento_key "${DATABENTO_API_KEY:-}"; then
  echo "[BACKTEST][ERROR] Missing valid DATABENTO_API_KEY for non-dry-run historical API streaming." >&2
  echo "[BACKTEST][ERROR] Set it in the parent environment or in $DATABENTO_ENV_FILE, matching the live Databento sidecar path." >&2
  exit 1
fi

if [[ "$BACKTEST_DATABENTO_SOURCE" == "ndjson" ]]; then
  PYTHON_BIN="$(resolve_basic_python_bin "$PYTHON_BIN" || true)"
else
  PYTHON_BIN="$(resolve_python_bin "$PYTHON_BIN" || true)"
fi
if [[ -z "$PYTHON_BIN" ]]; then
  echo "[BACKTEST][ERROR] Could not find Python with required replay packages installed." >&2
  echo "[BACKTEST][ERROR] Try: python3 -m pip install -r requirements.txt" >&2
  exit 1
fi
configure_python_ca_bundle

CONFIG_MODEL_DIR="$MODEL_DIR"
if [[ -n "$CONFIG_MODEL_DIR" && "$CONFIG_MODEL_DIR" != /* ]]; then
  CONFIG_MODEL_DIR="$ROOT/$CONFIG_MODEL_DIR"
fi

if truthy "$LIFECYCLE_MICRO_ENABLED"; then
  if [[ ! -d "$LIFECYCLE_MODEL_DIR" ]]; then
    echo "[BACKTEST][ERROR] Lifecycle/micro model directory not found: $LIFECYCLE_MODEL_DIR" >&2
    exit 1
  fi
  for required_lifecycle_model in \
    long_exit_lifecycle.onnx \
    short_exit_lifecycle.onnx \
    long_micro_entry_5s.onnx \
    short_micro_entry_5s.onnx \
    long_micro_exit_guard_5s.onnx \
    short_micro_exit_guard_5s.onnx \
    lifecycle_micro_scorecard.csv \
    lifecycle_micro_route_manifest.json; do
    if [[ ! -f "$LIFECYCLE_MODEL_DIR/$required_lifecycle_model" ]]; then
      echo "[BACKTEST][ERROR] Missing lifecycle/micro artifact: $LIFECYCLE_MODEL_DIR/$required_lifecycle_model" >&2
      exit 1
    fi
  done
fi

if ! truthy "$SKIP_BUILD"; then
  "$ROOT/mvnw" -q -DskipTests package
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
elif [[ ! -f "$CLASSPATH_FILE" ]]; then
  "$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"
fi

RUN_TS="$(date +%Y%m%d_%H%M%S)"
SAFE_START="$(printf '%s' "${START_DATE:-recorded}" | tr -c '[:alnum:]' '_')"
SAFE_END="$(printf '%s' "${END_DATE:-recorded}" | tr -c '[:alnum:]' '_')"
CLASSPATH="target/classes:$(cat "$CLASSPATH_FILE")"

failures=0
completed=0
for SYMBOL in "${symbols[@]}"; do
  MODEL_DIR="$CONFIG_MODEL_DIR"
  if [[ -z "$MODEL_DIR" ]]; then
    MODEL_DIR="$(route_model_dir "$SYMBOL" "$ROUTING_CSV" || true)"
  fi
  if [[ -n "$MODEL_DIR" && "$MODEL_DIR" != /* ]]; then
    MODEL_DIR="$ROOT/$MODEL_DIR"
  fi
  LOCAL_MODEL_DIR="$ROOT/runtime/models/$SYMBOL"
  if [[ -n "$MODEL_DIR" && ! -d "$MODEL_DIR" ]]; then
    if [[ -d "$LOCAL_MODEL_DIR" ]]; then
      echo "[BACKTEST][WARN] symbol=$SYMBOL model directory not found: $MODEL_DIR; using local fallback $LOCAL_MODEL_DIR" >&2
      MODEL_DIR="$LOCAL_MODEL_DIR"
    else
      echo "[BACKTEST][ERROR] symbol=$SYMBOL model directory not found: $MODEL_DIR" >&2
      failures=$((failures + 1))
      continue
    fi
  fi
  if [[ -z "$MODEL_DIR" ]]; then
    if [[ -d "$LOCAL_MODEL_DIR" ]]; then
      echo "[BACKTEST][WARN] symbol=$SYMBOL no model directory resolved; using local fallback $LOCAL_MODEL_DIR" >&2
      MODEL_DIR="$LOCAL_MODEL_DIR"
    else
      echo "[BACKTEST][WARN] symbol=$SYMBOL no model directory resolved; strategy will initialize with AI disabled/fallback behavior." >&2
    fi
  fi

  TRADE_LOG="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-trades.csv"
  ORDER_HISTORY="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-orders.csv"
  TRADE_LIFECYCLE_SUMMARY="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-trade-lifecycle-summary.csv"
  STREAM_SANITY_REPORT="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-stream-sanity.json"
  JAVA_PROPS=(
    "-Dbacktest.symbol=$SYMBOL"
    "-Dbacktest.ibkrSimulation=true"
    "-Dbacktest.databento.python=$PYTHON_BIN"
    "-Dbacktest.databento.streamer=scripts/databento_historical_streamer.py"
    "-Dbacktest.databento.source=$BACKTEST_DATABENTO_SOURCE"
    "-Dbacktest.databento.start=$START_DATE"
    "-Dbacktest.databento.end=$END_DATE"
    "-Dbacktest.databento.equityDataset=${DATABENTO_EQUITY_DATASET:-EQUS.MINI}"
    "-Dbacktest.databento.equitySchema=${DATABENTO_EQUITY_SCHEMA:-tbbo}"
    "-Dbacktest.databento.equityStypeIn=${DATABENTO_EQUITY_STYPE_IN:-raw_symbol}"
    "-Dbacktest.databento.optionsDataset=${DATABENTO_OPTIONS_DATASET:-OPRA.PILLAR}"
    "-Dbacktest.databento.optionsSchema=${DATABENTO_OPTIONS_SCHEMA:-ohlcv-1s}"
    "-Dbacktest.databento.optionsStypeIn=${DATABENTO_OPTIONS_STYPE_IN:-parent}"
    "-Dbacktest.databento.dryRun=$DRY_RUN"
    "-Dbacktest.databento.timeoutSeconds=$TIMEOUT_SECONDS"
    "-Dbacktest.strategy.maxTrades=$BACKTEST_MAX_TRADES"
    "-Dbacktest.strategy.maxShareCap=$BACKTEST_MAX_SHARE_CAP"
    "-Dbacktest.tradeLogFile=$TRADE_LOG"
    "-Dbacktest.orderHistoryFile=$ORDER_HISTORY"
    "-Dbacktest.tradeLifecycleSummaryFile=$TRADE_LIFECYCLE_SUMMARY"
    "-Dbacktest.streamSanityReportFile=$STREAM_SANITY_REPORT"
  )
  [[ -n "$MODEL_DIR" ]] && JAVA_PROPS+=("-Dtrading.model.dir=$MODEL_DIR")
  SETUP_THRESHOLDS_FILE_RESOLVED="$SETUP_THRESHOLDS_FILE"
  if [[ -z "$SETUP_THRESHOLDS_FILE_RESOLVED" && -n "$MODEL_DIR" && -f "$MODEL_DIR/setup_runtime_thresholds.properties" ]]; then
    SETUP_THRESHOLDS_FILE_RESOLVED="$MODEL_DIR/setup_runtime_thresholds.properties"
  fi
  if [[ -n "$SETUP_THRESHOLDS_FILE_RESOLVED" ]]; then
    [[ "$SETUP_THRESHOLDS_FILE_RESOLVED" != /* ]] && SETUP_THRESHOLDS_FILE_RESOLVED="$ROOT/$SETUP_THRESHOLDS_FILE_RESOLVED"
    if [[ ! -f "$SETUP_THRESHOLDS_FILE_RESOLVED" ]]; then
      if truthy "$SETUP_THRESHOLDS_FILE_EXPLICIT"; then
        echo "[BACKTEST][ERROR] Setup thresholds file not found: $SETUP_THRESHOLDS_FILE_RESOLVED" >&2
        failures=$((failures + 1))
        continue
      fi
      SETUP_THRESHOLDS_FILE_RESOLVED=""
    else
      append_setup_threshold_props "$SETUP_THRESHOLDS_FILE_RESOLVED"
    fi
  fi
  [[ -n "$BACKTEST_PREVIOUS_CLOSE" ]] && JAVA_PROPS+=("-Dbacktest.previousClose=$BACKTEST_PREVIOUS_CLOSE")
  [[ -n "$BACKTEST_RECORDED_EVENTS_FILE" ]] && JAVA_PROPS+=("-Dbacktest.databento.inputFile=$BACKTEST_RECORDED_EVENTS_FILE")
  MICRO_LONG_ENTRY_THRESHOLD_RESOLVED=""
  MICRO_SHORT_ENTRY_THRESHOLD_RESOLVED=""
  LIFECYCLE_LONG_EXIT_THRESHOLD_RESOLVED=""
  LIFECYCLE_SHORT_EXIT_THRESHOLD_RESOLVED=""
  MICRO_LONG_EXIT_GUARD_THRESHOLD_RESOLVED=""
  MICRO_SHORT_EXIT_GUARD_THRESHOLD_RESOLVED=""
  if truthy "$LIFECYCLE_MICRO_ENABLED"; then
    LIFECYCLE_SCORECARD="$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv"
    MICRO_LONG_ENTRY_THRESHOLD_RESOLVED="${MICRO_LONG_ENTRY_THRESHOLD:-$(csv_threshold longMicroEntryAi 0.58 "$LIFECYCLE_SCORECARD")}"
    MICRO_SHORT_ENTRY_THRESHOLD_RESOLVED="${MICRO_SHORT_ENTRY_THRESHOLD:-$(csv_threshold shortMicroEntryAi 0.58 "$LIFECYCLE_SCORECARD")}"
    LIFECYCLE_LONG_EXIT_THRESHOLD_RESOLVED="${LIFECYCLE_LONG_EXIT_THRESHOLD:-$(csv_threshold longExitLifecycleAi 0.60 "$LIFECYCLE_SCORECARD")}"
    LIFECYCLE_SHORT_EXIT_THRESHOLD_RESOLVED="${LIFECYCLE_SHORT_EXIT_THRESHOLD:-$(csv_threshold shortExitLifecycleAi 0.60 "$LIFECYCLE_SCORECARD")}"
    MICRO_LONG_EXIT_GUARD_THRESHOLD_RESOLVED="${MICRO_LONG_EXIT_GUARD_THRESHOLD:-$(csv_threshold longMicroExitGuardAi 0.70 "$LIFECYCLE_SCORECARD")}"
    MICRO_SHORT_EXIT_GUARD_THRESHOLD_RESOLVED="${MICRO_SHORT_EXIT_GUARD_THRESHOLD:-$(csv_threshold shortMicroExitGuardAi 0.70 "$LIFECYCLE_SCORECARD")}"
    JAVA_PROPS+=(
      "-Dstrategy.model.upgradedRouteRequired=true"
      "-Dstrategy.exit.legacy30sEnabled=false"
      "-Dstrategy.exit.lifecycleEnabled=true"
      "-Dstrategy.micro.entryEnabled=true"
      "-Dstrategy.micro.exitGuardEnabled=true"
      "-Dstrategy.lifecycle.modelDir=$LIFECYCLE_MODEL_DIR"
      "-Dstrategy.micro.modelDir=$LIFECYCLE_MODEL_DIR"
      "-Dstrategy.exit.lifecycle.longThreshold=$LIFECYCLE_LONG_EXIT_THRESHOLD_RESOLVED"
      "-Dstrategy.exit.lifecycle.shortThreshold=$LIFECYCLE_SHORT_EXIT_THRESHOLD_RESOLVED"
      "-Dstrategy.micro.longEntryThreshold=$MICRO_LONG_ENTRY_THRESHOLD_RESOLVED"
      "-Dstrategy.micro.shortEntryThreshold=$MICRO_SHORT_ENTRY_THRESHOLD_RESOLVED"
      "-Dstrategy.micro.longExitGuardThreshold=$MICRO_LONG_EXIT_GUARD_THRESHOLD_RESOLVED"
      "-Dstrategy.micro.shortExitGuardThreshold=$MICRO_SHORT_EXIT_GUARD_THRESHOLD_RESOLVED"
    )
  else
    JAVA_PROPS+=(
      "-Dstrategy.exit.lifecycleEnabled=${STRATEGY_LIFECYCLE_EXIT_ENABLED:-false}"
      "-Dstrategy.micro.entryEnabled=${STRATEGY_MICRO_ENTRY_ENABLED:-false}"
      "-Dstrategy.micro.exitGuardEnabled=${STRATEGY_MICRO_EXIT_GUARD_ENABLED:-false}"
    )
  fi

  cat <<SUMMARY
[BACKTEST] ------------------------------------------------------------
[BACKTEST] symbol=$SYMBOL start=${START_DATE:-<recorded>} end=${END_DATE:-<recorded>} dry_run=$DRY_RUN ibkr_simulation=true source=$BACKTEST_DATABENTO_SOURCE
[BACKTEST] python_bin=$PYTHON_BIN
[BACKTEST] model_dir=${MODEL_DIR:-<none>}
[BACKTEST] setup_thresholds_file=${SETUP_THRESHOLDS_FILE_RESOLVED:-<none>}
[BACKTEST] lifecycle_micro_enabled=$LIFECYCLE_MICRO_ENABLED lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[BACKTEST] micro_entry_thresholds long=${MICRO_LONG_ENTRY_THRESHOLD_RESOLVED:-<disabled>} short=${MICRO_SHORT_ENTRY_THRESHOLD_RESOLVED:-<disabled>}
[BACKTEST] lifecycle_exit_thresholds long=${LIFECYCLE_LONG_EXIT_THRESHOLD_RESOLVED:-<disabled>} short=${LIFECYCLE_SHORT_EXIT_THRESHOLD_RESOLVED:-<disabled>}
[BACKTEST] micro_exit_guard_thresholds long=${MICRO_LONG_EXIT_GUARD_THRESHOLD_RESOLVED:-<disabled>} short=${MICRO_SHORT_EXIT_GUARD_THRESHOLD_RESOLVED:-<disabled>}
[BACKTEST] trade_log=$TRADE_LOG
[BACKTEST] order_history=$ORDER_HISTORY
[BACKTEST] trade_lifecycle_summary=$TRADE_LIFECYCLE_SUMMARY
[BACKTEST] stream_sanity_report=$STREAM_SANITY_REPORT
SUMMARY

  if java -cp "$CLASSPATH" \
    "${JAVA_PROPS[@]}" \
    com.calgary.fili.trader.testers.DatabentoHistoricalStreamingBacktester \
    "$SYMBOL"; then
    completed=$((completed + 1))
  else
    failures=$((failures + 1))
    echo "[BACKTEST][ERROR] symbol=$SYMBOL replay failed." >&2
  fi
done

printf '[BACKTEST] completed=%s failed=%s requested=%s output_dir=%s\n' "$completed" "$failures" "${#symbols[@]}" "$OUTPUT_DIR"
if [[ "$failures" -gt 0 ]]; then
  exit 1
fi








