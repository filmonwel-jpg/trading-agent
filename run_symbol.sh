#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_symbol.sh <SYMBOL> [--start] [--max-trades=N] [--max-share-cap=N] [--trade-amount=N] [--max-order-notional=N] [--per-trade-notional=N] [--tee[=FILE]] [--tee-db] [--skip-ibkr-preflight] [--require-prebuilt-jar] [-- <extra java args...>]

Examples:
  ./run_symbol.sh TSLA
  ./run_symbol.sh NVDA --start
  ./run_symbol.sh AMD --start --max-trades=0
  ./run_symbol.sh TSLA --start --max-trades=2 --per-trade-notional=5000 --max-share-cap=25
  ./run_symbol.sh TSLA --start --tee
  ./run_symbol.sh NVDA --start --tee=runtime/nvda_live_trade_logs.txt -- --trading.risk.max-order-notional=90000
  ./run_symbol.sh AMD --start --tee --tee-db

Behavior:
  - Preview mode is the default. The script prints the resolved config and launch command.
  - Use --start to actually launch the packaged jar.
  - In --start mode, the script auto-rebuilds the packaged jar if src/, pom.xml, resources, or libs/ are newer than target/trading-agent-0.0.1-SNAPSHOT.jar, unless --require-prebuilt-jar is set.
  - With --require-prebuilt-jar, preview/start both fail if the packaged jar is missing or stale.
  - In --start mode, the script automatically waits for the configured IBKR API endpoint before launching unless you pass --skip-ibkr-preflight.
  - For Databento market data, the script validates the configured Python interpreter and automatically falls back to a local interpreter that has databento/databento_dbn installed.
  - Calibrated per-symbol micro-entry thresholds are loaded from config/databento_calibrated_micro_entry_thresholds.csv when present.
  - Use --max-trades=0 for a safe startup verification that blocks new entries.
  - Use --max-share-cap=N to set the final broker-side maximum shares per order before an order is sent.
  - Use --trade-amount=N to set the dollar amount used by the strategy to calculate share quantity.
  - Use --max-order-notional=N to set the risk cap checked before opening orders.
  - Use --per-trade-notional=N to set both --trade-amount and --max-order-notional to the same value.
  - Use --tee or --tee=FILE to append combined output to a log file.
  - Use --tee-db to persist the combined live process log stream to PostgreSQL.
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

repo_root="$(cd "$(dirname "$0")" && pwd)"
jar_path="$repo_root/target/trading-agent-0.0.1-SNAPSHOT.jar"
runtime_dir="$repo_root/runtime"
mvnw_path="$repo_root/mvnw"
ensure_ibkr_script="$repo_root/ensure_ibkr_workstation.sh"

symbol_input=""
start_mode=0
tee_mode=0
tee_db_mode=0
tee_path=""
max_trades_override=""
trade_amount_cli_override=""
max_notional_cli_override=""
max_share_cap_cli_override=""
skip_ibkr_preflight=0
require_prebuilt_jar=0
extra_args=()

case "${RUN_SYMBOL_REQUIRE_PREBUILT_JAR:-}" in
  1|true|TRUE|yes|YES|on|ON)
    require_prebuilt_jar=1
    ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --start)
      start_mode=1
      ;;
    --tee)
      tee_mode=1
      ;;
    --tee=*)
      tee_mode=1
      tee_path="${1#--tee=}"
      ;;
    --tee-db)
      tee_db_mode=1
      ;;
    --max-trades=*)
      max_trades_override="${1#--max-trades=}"
      ;;
    --trade-amount=*)
      trade_amount_cli_override="${1#--trade-amount=}"
      ;;
    --max-order-notional=*)
      max_notional_cli_override="${1#--max-order-notional=}"
      ;;
    --max-share-cap=*)
      max_share_cap_cli_override="${1#--max-share-cap=}"
      ;;
    --per-trade-notional=*|--trade-notional=*)
      trade_amount_cli_override="${1#*=}"
      max_notional_cli_override="${1#*=}"
      ;;
    --skip-ibkr-preflight|--no-ensure-ibkr)
      skip_ibkr_preflight=1
      ;;
    --require-prebuilt-jar)
      require_prebuilt_jar=1
      ;;
    --)
      shift
      extra_args=("$@")
      break
      ;;
    -*)
      echo "[RUN][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      if [[ -z "$symbol_input" ]]; then
        symbol_input="$1"
      else
        extra_args+=("$1")
      fi
      ;;
  esac
  shift
done

if [[ -z "$symbol_input" ]]; then
  echo "[RUN][ERROR] Missing symbol." >&2
  usage
  exit 1
fi

symbol_upper="$(printf '%s' "$symbol_input" | tr '[:lower:]' '[:upper:]')"
symbol_lower="$(printf '%s' "$symbol_input" | tr '[:upper:]' '[:lower:]')"

if [[ -n "$trade_amount_cli_override" && ! "$trade_amount_cli_override" =~ ^[1-9][0-9]*$ ]]; then
  echo "[RUN][ERROR] --trade-amount/--per-trade-notional must be a positive whole-dollar amount." >&2
  exit 1
fi

if [[ -n "$max_notional_cli_override" ]] && ! awk -v value="$max_notional_cli_override" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/ && value + 0 > 0) }'; then
  echo "[RUN][ERROR] --max-order-notional/--per-trade-notional must be a positive number." >&2
  exit 1
fi

if [[ -n "$max_share_cap_cli_override" && ! "$max_share_cap_cli_override" =~ ^[1-9][0-9]*$ ]]; then
  echo "[RUN][ERROR] --max-share-cap must be a positive whole-share quantity." >&2
  exit 1
fi

resolve_properties_file() {
  local symbol_lower_local="$1"
  local candidates=(
    "$runtime_dir/databento/bots/trading-${symbol_lower_local}.properties"
    "$runtime_dir/trading-${symbol_lower_local}.properties"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

properties_file="$(resolve_properties_file "$symbol_lower" || true)"

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    echo "[RUN][ERROR] Missing ${label}: $path" >&2
    exit 1
  fi
}

require_dir() {
  local path="$1"
  local label="$2"
  if [[ ! -d "$path" ]]; then
    echo "[RUN][ERROR] Missing ${label}: $path" >&2
    exit 1
  fi
}

jar_status="unknown"

jar_is_stale() {
  if [[ ! -f "$jar_path" ]]; then
    return 0
  fi

  if [[ "$repo_root/pom.xml" -nt "$jar_path" ]]; then
    return 0
  fi

  local newer_file=""
  newer_file="$(find \
    "$repo_root/src/main" \
    "$repo_root/src/main/resources" \
    "$repo_root/libs" \
    -type f -newer "$jar_path" -print -quit 2>/dev/null || true)"
  [[ -n "$newer_file" ]]
}

ensure_packaged_jar_current() {
  if [[ -f "$jar_path" ]] && ! jar_is_stale; then
    jar_status="fresh"
    return 0
  fi

  jar_status="$([[ -f "$jar_path" ]] && printf 'stale' || printf 'missing')"
  if [[ $require_prebuilt_jar -eq 1 ]]; then
    if [[ "$jar_status" == "stale" ]]; then
      echo "[RUN][ERROR] Packaged jar is stale and --require-prebuilt-jar is enabled: $jar_path" >&2
      echo "[RUN][ERROR] Rebuild it first: cd $repo_root && ./mvnw -DskipTests package" >&2
      return 1
    fi
    echo "[RUN][ERROR] Missing packaged jar and --require-prebuilt-jar is enabled: $jar_path" >&2
    echo "[RUN][ERROR] Build it first: cd $repo_root && ./mvnw -DskipTests package" >&2
    return 1
  fi

  if [[ $start_mode -eq 0 ]]; then
    if [[ "$jar_status" == "stale" ]]; then
      echo "[RUN][WARN] Packaged jar is stale: $jar_path" >&2
      echo "[RUN][WARN] Rebuild before production launch: cd $repo_root && ./mvnw -DskipTests package" >&2
      return 0
    fi
    echo "[RUN][ERROR] Missing packaged jar: $jar_path" >&2
    echo "[RUN][ERROR] Build it first: cd $repo_root && ./mvnw -DskipTests package" >&2
    return 1
  fi

  if [[ ! -x "$mvnw_path" ]]; then
    echo "[RUN][ERROR] Cannot refresh packaged jar because mvnw is missing or not executable: $mvnw_path" >&2
    return 1
  fi

  echo "[RUN] Detected $jar_status packaged jar. Rebuilding before launch..."
  if ! "$mvnw_path" -DskipTests package; then
    echo "[RUN][ERROR] Maven package failed; refusing to launch stale jar." >&2
    return 1
  fi

  if [[ ! -f "$jar_path" ]]; then
    echo "[RUN][ERROR] Build completed but packaged jar is still missing: $jar_path" >&2
    return 1
  fi

  jar_status="fresh"
}

get_prop_from_file() {
  local key="$1"
  local file="$2"
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
    END {
      if (found != "") print found
    }
  ' "$file"
}

get_prop() {
  get_prop_from_file "$1" "$properties_file"
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

extra_arg_is_managed_override() {
  local arg="$1"
  case "$arg" in
    --server.port=*|\
    --trading.client-id=*|\
    --trading.market-data-request-id=*|\
    --trading.trade-amount=*|\
    --trading.risk.max-order-notional=*|\
    --trading.risk.max-share-cap=*|\
    --trading.model.dir=*|\
    --trading.state.file=*|\
    --trading.log.file=*|\
    --logging.file.name=*|\
    --trading.ai.long-entry-threshold=*|\
    --trading.ai.short-entry-threshold=*|\
    --trading.ai.long-exit-threshold=*|\
    --trading.ai.short-exit-threshold=*|\
    --trading.ai.regime-threshold=*|\
    --trading.ai.entry-threshold-raise-percent=*|\
    --trading.ai.open30.long-entry-threshold=*|\
    --trading.ai.open30.short-entry-threshold=*|\
    --trading.ai.regime.choppy.long-entry-threshold=*|\
    --trading.ai.regime.choppy.short-entry-threshold=*|\
    --trading.ai.regime.trend.long-entry-threshold=*|\
    --trading.ai.regime.trend.short-entry-threshold=*|\
    --trading.ai.regime.volatile.long-entry-threshold=*|\
    --trading.ai.regime.volatile.short-entry-threshold=*|\
    --trading.micro.long-entry-threshold=*|\
    --trading.micro.short-entry-threshold=*|\
    --trading.shared-capital.enabled=*|\
    --trading.shared-capital.file=*|\
    --trading.shared-capital.total-notional=*|\
    --trading.databento.python-bin=*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

resolve_python_bin() {
  local candidate="$1"
  if [[ -z "$candidate" ]]; then
    return 1
  fi
  if [[ -x "$candidate" ]]; then
    printf '%s' "$candidate"
    return 0
  fi
  candidate="$(command -v "$candidate" 2>/dev/null || true)"
  if [[ -n "$candidate" && -x "$candidate" ]]; then
    printf '%s' "$candidate"
    return 0
  fi
  return 1
}

python_can_import_databento() {
  local python_bin="$1"
  "$python_bin" -c 'import databento, databento_dbn' >/dev/null 2>&1
}

python_can_import_psycopg() {
  local python_bin="$1"
  "$python_bin" -c 'import psycopg' >/dev/null 2>&1
}

resolve_databento_python_bin() {
  local configured_bin="$1"
  local -a candidates=()
  local candidate resolved

  if [[ -n "$configured_bin" ]]; then
    candidates+=("$configured_bin")
  fi
  if [[ -n "${DATABENTO_PYTHON_BIN:-}" ]]; then
    candidates+=("$DATABENTO_PYTHON_BIN")
  fi
  candidates+=(
    "$repo_root/.venv/bin/python"
    "$HOME/miniforge3/bin/python3"
    python3
    /opt/homebrew/bin/python3
    /usr/local/bin/python3
    /usr/bin/python3
  )

  for candidate in "${candidates[@]}"; do
    resolved="$(resolve_python_bin "$candidate" || true)"
    if [[ -n "$resolved" ]] && python_can_import_databento "$resolved"; then
      printf '%s' "$resolved"
      return 0
    fi
  done

  return 1
}

resolve_tee_db_python_bin() {
  local configured_bin="$1"
  local databento_bin="$2"
  local -a candidates=()
  local candidate resolved

  if [[ -n "$configured_bin" ]]; then
    candidates+=("$configured_bin")
  fi
  if [[ -n "$databento_bin" ]]; then
    candidates+=("$databento_bin")
  fi
  if [[ -n "${RUN_SYMBOL_DB_TEE_PYTHON_BIN:-}" ]]; then
    candidates+=("$RUN_SYMBOL_DB_TEE_PYTHON_BIN")
  fi
  if [[ -n "${DATABENTO_PYTHON_BIN:-}" ]]; then
    candidates+=("$DATABENTO_PYTHON_BIN")
  fi
  candidates+=(
    python3
    "$HOME/miniforge3/bin/python3"
    /opt/homebrew/bin/python3
    /usr/local/bin/python3
    /usr/bin/python3
  )

  for candidate in "${candidates[@]}"; do
    resolved="$(resolve_python_bin "$candidate" || true)"
    if [[ -n "$resolved" ]] && python_can_import_psycopg "$resolved"; then
      printf '%s' "$resolved"
      return 0
    fi
  done

  return 1
}

require_file "$properties_file" "symbol runtime properties"
require_file "$runtime_dir/postgres-local.properties" "runtime postgres properties"

resolve_java_bin() {
  local candidate_home

  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
    printf '%s' "$JAVA_HOME/bin/java"
    return 0
  fi

  if [[ -x "/usr/libexec/java_home" ]]; then
    candidate_home="$(/usr/libexec/java_home 2>/dev/null || true)"
    if [[ -n "$candidate_home" && -x "$candidate_home/bin/java" ]]; then
      export JAVA_HOME="$candidate_home"
      export PATH="$JAVA_HOME/bin:$PATH"
      printf '%s' "$JAVA_HOME/bin/java"
      return 0
    fi
  fi

  for candidate_home in \
    "$HOME"/.jdks/*/Contents/Home \
    "$HOME"/Library/Java/JavaVirtualMachines/*/Contents/Home \
    /Library/Java/JavaVirtualMachines/*/Contents/Home \
    /opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home \
    /usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home; do
    [[ -d "$candidate_home" ]] || continue
    if [[ -x "$candidate_home/bin/java" ]]; then
      export JAVA_HOME="$candidate_home"
      export PATH="$JAVA_HOME/bin:$PATH"
      printf '%s' "$candidate_home/bin/java"
      return 0
    fi
  done

  if command -v java >/dev/null 2>&1; then
    printf '%s' "$(command -v java)"
    return 0
  fi

  return 1
}

java_bin="$(resolve_java_bin || true)"
if [[ -z "$java_bin" ]]; then
  echo "[RUN][ERROR] java is not available. Set JAVA_HOME or install a JDK under ~/.jdks, /Library/Java/JavaVirtualMachines, or Homebrew openjdk." >&2
  exit 1
fi

if ! "$java_bin" -version >/dev/null 2>&1; then
  echo "[RUN][ERROR] Java binary is present but unusable: $java_bin" >&2
  echo "[RUN][ERROR] launchd often misses JAVA_HOME on macOS; current JAVA_HOME=${JAVA_HOME:-<unset>}" >&2
  exit 1
fi

ensure_packaged_jar_current
require_file "$jar_path" "packaged jar"

if [[ $tee_db_mode -eq 1 && ! -f "$repo_root/stream_live_logs_to_db.py" ]]; then
  echo "[RUN][ERROR] Missing DB tee helper: $repo_root/stream_live_logs_to_db.py" >&2
  exit 1
fi

server_port="$(get_prop server.port)"
client_id="$(get_prop trading.client-id)"
market_data_request_id="$(get_prop trading.market-data-request-id)"
trade_amount="$(get_prop trading.trade-amount)"
max_notional="$(get_prop trading.risk.max-order-notional)"
max_share_cap="$(get_prop trading.risk.max-share-cap)"
ai_long_entry_threshold="$(get_prop trading.ai.long-entry-threshold)"
ai_short_entry_threshold="$(get_prop trading.ai.short-entry-threshold)"
ai_long_exit_threshold="$(get_prop trading.ai.long-exit-threshold)"
ai_short_exit_threshold="$(get_prop trading.ai.short-exit-threshold)"
ai_regime_threshold="$(get_prop trading.ai.regime-threshold)"
ai_entry_threshold_raise_percent="$(get_prop trading.ai.entry-threshold-raise-percent)"
ai_open30_long_entry_threshold="$(get_prop trading.ai.open30.long-entry-threshold)"
ai_open30_short_entry_threshold="$(get_prop trading.ai.open30.short-entry-threshold)"
ai_choppy_long_entry_threshold="$(get_prop trading.ai.regime.choppy.long-entry-threshold)"
ai_choppy_short_entry_threshold="$(get_prop trading.ai.regime.choppy.short-entry-threshold)"
ai_trend_long_entry_threshold="$(get_prop trading.ai.regime.trend.long-entry-threshold)"
ai_trend_short_entry_threshold="$(get_prop trading.ai.regime.trend.short-entry-threshold)"
ai_volatile_long_entry_threshold="$(get_prop trading.ai.regime.volatile.long-entry-threshold)"
ai_volatile_short_entry_threshold="$(get_prop trading.ai.regime.volatile.short-entry-threshold)"
shared_capital_enabled="$(get_prop trading.shared-capital.enabled)"
shared_capital_file="$(get_prop trading.shared-capital.file)"
shared_capital_total_notional="$(get_prop trading.shared-capital.total-notional)"
ib_host="$(get_prop trading.host)"
ib_port="$(get_prop trading.port)"
market_data_provider="$(get_prop trading.market-data.provider)"
databento_python_bin="$(get_prop trading.databento.python-bin)"
databento_shared_feed_enabled="$(get_prop trading.databento.shared-feed.enabled)"
model_dir_prop="$(get_prop trading.model.dir)"
state_file="$(get_prop trading.state.file)"
trade_log_file="$(get_prop trading.log.file)"
app_log_file="$(get_prop logging.file.name)"
default_catboost_setup_model_dir="$repo_root/runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854"
default_lifecycle_micro_model_dir="$repo_root/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports"
model_dir_env_override="${TRADING_MODEL_DIR:-${TRADING_SETUP_MODEL_DIR:-}}"
if [[ -n "$model_dir_env_override" ]]; then
  model_dir="$model_dir_env_override"
elif [[ -d "$default_catboost_setup_model_dir" ]]; then
  model_dir="$default_catboost_setup_model_dir"
else
  model_dir="${model_dir_prop:-$runtime_dir/models/$symbol_upper}"
fi
setup_thresholds_file="${TRADING_SETUP_THRESHOLDS_FILE:-$model_dir/setup_runtime_thresholds.properties}"
setup_thresholds_file_lower="$(printf '%s' "$setup_thresholds_file" | tr '[:upper:]' '[:lower:]')"
if [[ "$setup_thresholds_file_lower" == "none" ]]; then
  setup_thresholds_file=""
elif [[ -n "$setup_thresholds_file" && "$setup_thresholds_file" != /* ]]; then
  setup_thresholds_file="$repo_root/$setup_thresholds_file"
fi
setup_thresholds_source=""
lifecycle_model_dir="${TRADING_LIFECYCLE_MODEL_DIR:-$default_lifecycle_micro_model_dir}"
lifecycle_scorecard="$lifecycle_model_dir/lifecycle_micro_scorecard.csv"
calibrated_micro_thresholds_file="${TRADING_CALIBRATED_MICRO_THRESHOLDS_FILE:-$repo_root/config/databento_calibrated_micro_entry_thresholds.csv}"
[[ "$calibrated_micro_thresholds_file" != /* ]] && calibrated_micro_thresholds_file="$repo_root/$calibrated_micro_thresholds_file"
micro_long_entry_threshold="$(get_prop trading.micro.long-entry-threshold)"
micro_short_entry_threshold="$(get_prop trading.micro.short-entry-threshold)"
micro_long_entry_threshold_source=""
micro_short_entry_threshold_source=""
if [[ -n "$micro_long_entry_threshold" ]]; then
  micro_long_entry_threshold_source="properties:$properties_file"
fi
if [[ -n "$micro_short_entry_threshold" ]]; then
  micro_short_entry_threshold_source="properties:$properties_file"
fi

truthy_env() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

csv_threshold() {
  local model_name="$1"
  local default_value="$2"
  if [[ -f "$lifecycle_scorecard" ]]; then
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
    ' "$lifecycle_scorecard"
  else
    printf '%.4f' "$default_value"
  fi
}

csv_symbol_value() {
  local file_path="$1"
  local target_symbol="$2"
  local target_key="$3"
  [[ -f "$file_path" ]] || return 0
  awk -F, -v target_symbol="$target_symbol" -v target_key="$target_key" '
    function trim(s) { gsub(/^[[:space:]\r\n]+|[[:space:]\r\n]+$/, "", s); return s }
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        key = trim($i)
        idx[key] = i
      }
      symbol_idx = idx["symbol"]
      value_idx = idx[target_key]
      if (!symbol_idx || !value_idx) exit 0
      next
    }
    symbol_idx && value_idx {
      current_symbol = toupper(trim($(symbol_idx)))
      if (current_symbol == toupper(target_symbol)) {
        print trim($(value_idx))
        exit 0
      }
    }
  ' "$file_path"
}

apply_setup_threshold_default() {
  local property_key="$1"
  local variable_name="$2"
  local value=""
  [[ -n "$setup_thresholds_file" && -f "$setup_thresholds_file" ]] || return 0
  value="$(get_prop_from_file "$property_key" "$setup_thresholds_file")"
  if [[ -n "$value" ]]; then
    printf -v "$variable_name" '%s' "$value"
    setup_thresholds_source="properties:$setup_thresholds_file"
  fi
}

apply_env_override() {
  local env_name="$1"
  local variable_name="$2"
  if [[ -n "${!env_name:-}" ]]; then
    printf -v "$variable_name" '%s' "${!env_name}"
  fi
}

apply_setup_threshold_default trading.ai.long-entry-threshold ai_long_entry_threshold
apply_setup_threshold_default trading.ai.short-entry-threshold ai_short_entry_threshold
apply_setup_threshold_default trading.ai.regime-threshold ai_regime_threshold
apply_setup_threshold_default trading.ai.entry-threshold-raise-percent ai_entry_threshold_raise_percent
apply_setup_threshold_default trading.ai.open30.long-entry-threshold ai_open30_long_entry_threshold
apply_setup_threshold_default trading.ai.open30.short-entry-threshold ai_open30_short_entry_threshold
apply_setup_threshold_default trading.ai.regime.choppy.long-entry-threshold ai_choppy_long_entry_threshold
apply_setup_threshold_default trading.ai.regime.choppy.short-entry-threshold ai_choppy_short_entry_threshold
apply_setup_threshold_default trading.ai.regime.trend.long-entry-threshold ai_trend_long_entry_threshold
apply_setup_threshold_default trading.ai.regime.trend.short-entry-threshold ai_trend_short_entry_threshold
apply_setup_threshold_default trading.ai.regime.volatile.long-entry-threshold ai_volatile_long_entry_threshold
apply_setup_threshold_default trading.ai.regime.volatile.short-entry-threshold ai_volatile_short_entry_threshold

apply_env_override TRADING_AI_LONG_ENTRY_THRESHOLD ai_long_entry_threshold
apply_env_override TRADING_AI_SHORT_ENTRY_THRESHOLD ai_short_entry_threshold
apply_env_override TRADING_AI_LONG_EXIT_THRESHOLD ai_long_exit_threshold
apply_env_override TRADING_AI_SHORT_EXIT_THRESHOLD ai_short_exit_threshold
apply_env_override TRADING_AI_REGIME_THRESHOLD ai_regime_threshold
apply_env_override TRADING_AI_ENTRY_THRESHOLD_RAISE_PERCENT ai_entry_threshold_raise_percent
apply_env_override TRADING_AI_OPEN30_LONG_ENTRY_THRESHOLD ai_open30_long_entry_threshold
apply_env_override TRADING_AI_OPEN30_SHORT_ENTRY_THRESHOLD ai_open30_short_entry_threshold
apply_env_override TRADING_AI_REGIME_CHOPPY_LONG_ENTRY_THRESHOLD ai_choppy_long_entry_threshold
apply_env_override TRADING_AI_REGIME_CHOPPY_SHORT_ENTRY_THRESHOLD ai_choppy_short_entry_threshold
apply_env_override TRADING_AI_REGIME_TREND_LONG_ENTRY_THRESHOLD ai_trend_long_entry_threshold
apply_env_override TRADING_AI_REGIME_TREND_SHORT_ENTRY_THRESHOLD ai_trend_short_entry_threshold
apply_env_override TRADING_AI_REGIME_VOLATILE_LONG_ENTRY_THRESHOLD ai_volatile_long_entry_threshold
apply_env_override TRADING_AI_REGIME_VOLATILE_SHORT_ENTRY_THRESHOLD ai_volatile_short_entry_threshold

server_port_override="$(resolve_extra_arg_override server.port)"
client_id_override="$(resolve_extra_arg_override trading.client-id)"
market_data_request_id_override="$(resolve_extra_arg_override trading.market-data-request-id)"
trade_amount_extra_override="$(resolve_extra_arg_override trading.trade-amount)"
max_notional_extra_override="$(resolve_extra_arg_override trading.risk.max-order-notional)"
max_share_cap_extra_override="$(resolve_extra_arg_override trading.risk.max-share-cap)"
trade_amount_override="${trade_amount_extra_override:-$trade_amount_cli_override}"
max_notional_override="${max_notional_extra_override:-$max_notional_cli_override}"
max_share_cap_override="${max_share_cap_extra_override:-$max_share_cap_cli_override}"
model_dir_prop_override="$(resolve_extra_arg_override trading.model.dir)"
state_file_override="$(resolve_extra_arg_override trading.state.file)"
trade_log_file_override="$(resolve_extra_arg_override trading.log.file)"
app_log_file_override="$(resolve_extra_arg_override logging.file.name)"
ai_long_entry_threshold_override="$(resolve_extra_arg_override trading.ai.long-entry-threshold)"
ai_short_entry_threshold_override="$(resolve_extra_arg_override trading.ai.short-entry-threshold)"
ai_long_exit_threshold_override="$(resolve_extra_arg_override trading.ai.long-exit-threshold)"
ai_short_exit_threshold_override="$(resolve_extra_arg_override trading.ai.short-exit-threshold)"
ai_regime_threshold_override="$(resolve_extra_arg_override trading.ai.regime-threshold)"
ai_entry_threshold_raise_percent_override="$(resolve_extra_arg_override trading.ai.entry-threshold-raise-percent)"
ai_open30_long_entry_threshold_override="$(resolve_extra_arg_override trading.ai.open30.long-entry-threshold)"
ai_open30_short_entry_threshold_override="$(resolve_extra_arg_override trading.ai.open30.short-entry-threshold)"
ai_choppy_long_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.choppy.long-entry-threshold)"
ai_choppy_short_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.choppy.short-entry-threshold)"
ai_trend_long_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.trend.long-entry-threshold)"
ai_trend_short_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.trend.short-entry-threshold)"
ai_volatile_long_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.volatile.long-entry-threshold)"
ai_volatile_short_entry_threshold_override="$(resolve_extra_arg_override trading.ai.regime.volatile.short-entry-threshold)"
micro_long_entry_threshold_override="$(resolve_extra_arg_override trading.micro.long-entry-threshold)"
micro_short_entry_threshold_override="$(resolve_extra_arg_override trading.micro.short-entry-threshold)"
shared_capital_enabled_override="$(resolve_extra_arg_override trading.shared-capital.enabled)"
shared_capital_file_override="$(resolve_extra_arg_override trading.shared-capital.file)"
shared_capital_total_notional_override="$(resolve_extra_arg_override trading.shared-capital.total-notional)"
ib_host_override="$(resolve_extra_arg_override trading.host)"
ib_port_override="$(resolve_extra_arg_override trading.port)"
databento_python_bin_override="$(resolve_extra_arg_override trading.databento.python-bin)"

if [[ -n "$server_port_override" ]]; then
  server_port="$server_port_override"
fi
if [[ -n "$client_id_override" ]]; then
  client_id="$client_id_override"
fi
if [[ -n "$market_data_request_id_override" ]]; then
  market_data_request_id="$market_data_request_id_override"
fi
if [[ -n "$trade_amount_override" ]]; then
  trade_amount="$trade_amount_override"
fi
if [[ -n "$max_notional_override" ]]; then
  max_notional="$max_notional_override"
fi
if [[ -n "$max_share_cap_override" ]]; then
  max_share_cap="$max_share_cap_override"
fi
if [[ -n "$max_share_cap" && ! "$max_share_cap" =~ ^[1-9][0-9]*$ ]]; then
  echo "[RUN][ERROR] Effective trading.risk.max-share-cap must be a positive whole-share quantity." >&2
  exit 1
fi
if [[ -n "$model_dir_prop_override" ]]; then
  model_dir_prop="$model_dir_prop_override"
  model_dir="$model_dir_prop_override"
fi
if [[ -n "$state_file_override" ]]; then
  state_file="$state_file_override"
fi
if [[ -n "$trade_log_file_override" ]]; then
  trade_log_file="$trade_log_file_override"
fi
if [[ -n "$app_log_file_override" ]]; then
  app_log_file="$app_log_file_override"
fi
if [[ -n "$ai_long_entry_threshold_override" ]]; then
  ai_long_entry_threshold="$ai_long_entry_threshold_override"
fi
if [[ -n "$ai_short_entry_threshold_override" ]]; then
  ai_short_entry_threshold="$ai_short_entry_threshold_override"
fi
if [[ -n "$ai_long_exit_threshold_override" ]]; then
  ai_long_exit_threshold="$ai_long_exit_threshold_override"
fi
if [[ -n "$ai_short_exit_threshold_override" ]]; then
  ai_short_exit_threshold="$ai_short_exit_threshold_override"
fi
if [[ -n "$ai_regime_threshold_override" ]]; then
  ai_regime_threshold="$ai_regime_threshold_override"
fi
if [[ -n "$ai_entry_threshold_raise_percent_override" ]]; then
  ai_entry_threshold_raise_percent="$ai_entry_threshold_raise_percent_override"
fi
if [[ -n "$ai_open30_long_entry_threshold_override" ]]; then
  ai_open30_long_entry_threshold="$ai_open30_long_entry_threshold_override"
fi
if [[ -n "$ai_open30_short_entry_threshold_override" ]]; then
  ai_open30_short_entry_threshold="$ai_open30_short_entry_threshold_override"
fi
if [[ -n "$ai_choppy_long_entry_threshold_override" ]]; then
  ai_choppy_long_entry_threshold="$ai_choppy_long_entry_threshold_override"
fi
if [[ -n "$ai_choppy_short_entry_threshold_override" ]]; then
  ai_choppy_short_entry_threshold="$ai_choppy_short_entry_threshold_override"
fi
if [[ -n "$ai_trend_long_entry_threshold_override" ]]; then
  ai_trend_long_entry_threshold="$ai_trend_long_entry_threshold_override"
fi
if [[ -n "$ai_trend_short_entry_threshold_override" ]]; then
  ai_trend_short_entry_threshold="$ai_trend_short_entry_threshold_override"
fi
if [[ -n "$ai_volatile_long_entry_threshold_override" ]]; then
  ai_volatile_long_entry_threshold="$ai_volatile_long_entry_threshold_override"
fi
if [[ -n "$ai_volatile_short_entry_threshold_override" ]]; then
  ai_volatile_short_entry_threshold="$ai_volatile_short_entry_threshold_override"
fi
ai_entry_threshold_raise_percent="${ai_entry_threshold_raise_percent:-10.0}"
if [[ -n "${TRADING_MICRO_LONG_ENTRY_THRESHOLD:-}" ]]; then
  micro_long_entry_threshold="$TRADING_MICRO_LONG_ENTRY_THRESHOLD"
  micro_long_entry_threshold_source="env:TRADING_MICRO_LONG_ENTRY_THRESHOLD"
fi
if [[ -n "${TRADING_MICRO_SHORT_ENTRY_THRESHOLD:-}" ]]; then
  micro_short_entry_threshold="$TRADING_MICRO_SHORT_ENTRY_THRESHOLD"
  micro_short_entry_threshold_source="env:TRADING_MICRO_SHORT_ENTRY_THRESHOLD"
fi
if [[ -n "$micro_long_entry_threshold_override" ]]; then
  micro_long_entry_threshold="$micro_long_entry_threshold_override"
  micro_long_entry_threshold_source="extra-arg:trading.micro.long-entry-threshold"
fi
if [[ -n "$micro_short_entry_threshold_override" ]]; then
  micro_short_entry_threshold="$micro_short_entry_threshold_override"
  micro_short_entry_threshold_source="extra-arg:trading.micro.short-entry-threshold"
fi
if [[ -z "$micro_long_entry_threshold" ]]; then
  csv_micro_long_entry_threshold="$(csv_symbol_value "$calibrated_micro_thresholds_file" "$symbol_upper" micro_long_entry_threshold || true)"
  if [[ -n "$csv_micro_long_entry_threshold" ]]; then
    micro_long_entry_threshold="$csv_micro_long_entry_threshold"
    micro_long_entry_threshold_source="csv:$calibrated_micro_thresholds_file"
  fi
fi
if [[ -z "$micro_short_entry_threshold" ]]; then
  csv_micro_short_entry_threshold="$(csv_symbol_value "$calibrated_micro_thresholds_file" "$symbol_upper" micro_short_entry_threshold || true)"
  if [[ -n "$csv_micro_short_entry_threshold" ]]; then
    micro_short_entry_threshold="$csv_micro_short_entry_threshold"
    micro_short_entry_threshold_source="csv:$calibrated_micro_thresholds_file"
  fi
fi
if [[ -n "$shared_capital_enabled_override" ]]; then
  shared_capital_enabled="$shared_capital_enabled_override"
fi
if [[ -n "$shared_capital_file_override" ]]; then
  shared_capital_file="$shared_capital_file_override"
fi
if [[ -n "$shared_capital_total_notional_override" ]]; then
  shared_capital_total_notional="$shared_capital_total_notional_override"
fi
if [[ -n "$ib_host_override" ]]; then
  ib_host="$ib_host_override"
fi
if [[ -n "$ib_port_override" ]]; then
  ib_port="$ib_port_override"
fi
if [[ -n "$databento_python_bin_override" ]]; then
  databento_python_bin="$databento_python_bin_override"
fi

ib_host="${ib_host:-127.0.0.1}"
ib_port="${ib_port:-7497}"

market_data_provider_normalized="$(printf '%s' "${market_data_provider:-}" | tr '[:upper:]' '[:lower:]')"
uses_databento_sidecar=0
if [[ "$market_data_provider_normalized" == "databento" ]]; then
  uses_databento_sidecar=1
fi

resolved_databento_python_bin=""
if [[ $uses_databento_sidecar -eq 1 ]]; then
  resolved_databento_python_bin="$(resolve_databento_python_bin "$databento_python_bin" || true)"
  if [[ -z "$resolved_databento_python_bin" ]]; then
    echo "[RUN][ERROR] Could not find a usable Python interpreter with both databento and databento_dbn installed." >&2
    if [[ -n "$databento_python_bin" ]]; then
      echo "[RUN][ERROR] Configured trading.databento.python-bin=$databento_python_bin is not usable on this machine." >&2
    fi
    echo "[RUN][ERROR] Install the Databento sidecar dependencies, e.g.: python3 -m pip install -r $repo_root/requirements.txt" >&2
    exit 1
  fi
  if [[ -n "$databento_python_bin" && "$resolved_databento_python_bin" != "$databento_python_bin" ]]; then
    echo "[RUN][WARN] Configured Databento python is unavailable or missing packages: $databento_python_bin" >&2
    echo "[RUN][WARN] Falling back to: $resolved_databento_python_bin" >&2
  fi
  databento_python_bin="$resolved_databento_python_bin"
fi

tee_db_python_bin=""
if [[ $tee_db_mode -eq 1 ]]; then
  tee_db_python_bin="$(resolve_tee_db_python_bin "${RUN_SYMBOL_DB_TEE_PYTHON_BIN:-}" "$databento_python_bin" || true)"
  if [[ -z "$tee_db_python_bin" ]]; then
    echo "[RUN][ERROR] Could not find a usable Python interpreter with psycopg installed for --tee-db." >&2
    echo "[RUN][ERROR] Set RUN_SYMBOL_DB_TEE_PYTHON_BIN or install psycopg, e.g.: python3 -m pip install 'psycopg[binary]>=3.2'" >&2
    exit 1
  fi
fi

require_dir "$model_dir" "symbol model directory"

lifecycle_micro_enabled="${TRADING_LIFECYCLE_MICRO_ENABLED:-true}"
if truthy_env "$lifecycle_micro_enabled"; then
  require_dir "$lifecycle_model_dir" "lifecycle/micro model directory"
  for required_lifecycle_model in \
    long_exit_lifecycle.onnx \
    short_exit_lifecycle.onnx \
    long_micro_entry_5s.onnx \
    short_micro_entry_5s.onnx \
    long_micro_exit_guard_5s.onnx \
    short_micro_exit_guard_5s.onnx \
    lifecycle_micro_scorecard.csv \
    lifecycle_micro_route_manifest.json; do
    require_file "$lifecycle_model_dir/$required_lifecycle_model" "lifecycle/micro artifact $required_lifecycle_model"
  done
fi

java_opts_raw="${TRADING_AGENT_JAVA_OPTS:-}"
if [[ -z "$java_opts_raw" ]]; then
  java_opts=(
    -Xms32m
    -Xmx192m
    -XX:+UseSerialGC
    -Dspring.main.lazy-initialization=true
  )
else
  read -r -a java_opts <<< "$java_opts_raw"
fi

lifecycle_long_exit_threshold_resolved="$(csv_threshold longExitLifecycleAi 0.60)"
lifecycle_short_exit_threshold_resolved="$(csv_threshold shortExitLifecycleAi 0.60)"
micro_long_entry_threshold_resolved="${micro_long_entry_threshold:-$(csv_threshold longMicroEntryAi 0.58)}"
micro_short_entry_threshold_resolved="${micro_short_entry_threshold:-$(csv_threshold shortMicroEntryAi 0.58)}"
micro_long_exit_guard_threshold_resolved="$(csv_threshold longMicroExitGuardAi 0.70)"
micro_short_exit_guard_threshold_resolved="$(csv_threshold shortMicroExitGuardAi 0.70)"
micro_long_entry_threshold_source="${micro_long_entry_threshold_source:-scorecard:$lifecycle_scorecard}"
micro_short_entry_threshold_source="${micro_short_entry_threshold_source:-scorecard:$lifecycle_scorecard}"

if truthy_env "$lifecycle_micro_enabled"; then
  java_opts+=(
    -Dstrategy.model.upgradedRouteRequired=true
    -Dstrategy.exit.legacy30sEnabled=false
    -Dstrategy.exit.lifecycleEnabled=true
    -Dstrategy.micro.entryEnabled=true
    -Dstrategy.micro.exitGuardEnabled=true
    "-Dstrategy.lifecycle.modelDir=$lifecycle_model_dir"
    "-Dstrategy.micro.modelDir=$lifecycle_model_dir"
    "-Dstrategy.exit.lifecycle.longThreshold=$lifecycle_long_exit_threshold_resolved"
    "-Dstrategy.exit.lifecycle.shortThreshold=$lifecycle_short_exit_threshold_resolved"
    "-Dstrategy.micro.longEntryThreshold=$micro_long_entry_threshold_resolved"
    "-Dstrategy.micro.shortEntryThreshold=$micro_short_entry_threshold_resolved"
    "-Dstrategy.micro.longExitGuardThreshold=$micro_long_exit_guard_threshold_resolved"
    "-Dstrategy.micro.shortExitGuardThreshold=$micro_short_exit_guard_threshold_resolved"
  )
fi

onnx_count="$(find "$model_dir" -maxdepth 1 -type f -name '*.onnx' | wc -l | tr -d ' ')"

if [[ -z "$server_port" ]]; then
  echo "[RUN][ERROR] server.port is missing in $properties_file" >&2
  exit 1
fi

for required_30s_model in \
  long_entry.onnx \
  short_entry.onnx \
  regime_classifier.onnx \
  choppy_long_entry.onnx \
  choppy_short_entry.onnx \
  trend_long_entry.onnx \
  trend_short_entry.onnx \
  volatile_long_entry.onnx \
  volatile_short_entry.onnx \
  open30_long_entry.onnx \
  open30_short_entry.onnx; do
  require_file "$model_dir/$required_30s_model" "30s route model $required_30s_model"
done

if lsof -nP -iTCP:"$server_port" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[RUN][WARN] Port $server_port is already in use. Startup will likely fail unless you stop the existing process first."
  port_busy=1
else
  port_busy=0
fi

if [[ $tee_mode -eq 1 && -z "$tee_path" ]]; then
  tee_path="$runtime_dir/${symbol_lower}_live_trade_logs.txt"
fi

cmd=(
  "$java_bin"
  "${java_opts[@]}"
  -jar
  "$jar_path"
  "--spring.config.additional-location=file:$properties_file"
)

if [[ -n "$server_port" ]]; then
  cmd+=("--server.port=$server_port")
fi
if [[ -n "$client_id" ]]; then
  cmd+=("--trading.client-id=$client_id")
fi
if [[ -n "$market_data_request_id" ]]; then
  cmd+=("--trading.market-data-request-id=$market_data_request_id")
fi
if [[ -n "$trade_amount" ]]; then
  cmd+=("--trading.trade-amount=$trade_amount")
fi
if [[ -n "$max_notional" ]]; then
  cmd+=("--trading.risk.max-order-notional=$max_notional")
fi
if [[ -n "$max_share_cap" ]]; then
  cmd+=("--trading.risk.max-share-cap=$max_share_cap")
fi
if [[ -n "$model_dir" ]]; then
  cmd+=("--trading.model.dir=$model_dir")
fi
if [[ -n "$state_file" ]]; then
  cmd+=("--trading.state.file=$state_file")
fi
if [[ -n "$trade_log_file" ]]; then
  cmd+=("--trading.log.file=$trade_log_file")
fi
if [[ -n "$app_log_file" ]]; then
  cmd+=("--logging.file.name=$app_log_file")
fi

if [[ -n "$ai_long_entry_threshold" ]]; then
  cmd+=("--trading.ai.long-entry-threshold=$ai_long_entry_threshold")
fi
if [[ -n "$ai_short_entry_threshold" ]]; then
  cmd+=("--trading.ai.short-entry-threshold=$ai_short_entry_threshold")
fi
if [[ -n "$ai_long_exit_threshold" ]]; then
  cmd+=("--trading.ai.long-exit-threshold=$ai_long_exit_threshold")
fi
if [[ -n "$ai_short_exit_threshold" ]]; then
  cmd+=("--trading.ai.short-exit-threshold=$ai_short_exit_threshold")
fi
if [[ -n "$ai_regime_threshold" ]]; then
  cmd+=("--trading.ai.regime-threshold=$ai_regime_threshold")
fi
if [[ -n "$ai_entry_threshold_raise_percent" ]]; then
  cmd+=("--trading.ai.entry-threshold-raise-percent=$ai_entry_threshold_raise_percent")
fi
if [[ -n "$ai_open30_long_entry_threshold" ]]; then
  cmd+=("--trading.ai.open30.long-entry-threshold=$ai_open30_long_entry_threshold")
fi
if [[ -n "$ai_open30_short_entry_threshold" ]]; then
  cmd+=("--trading.ai.open30.short-entry-threshold=$ai_open30_short_entry_threshold")
fi
if [[ -n "$ai_choppy_long_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.choppy.long-entry-threshold=$ai_choppy_long_entry_threshold")
fi
if [[ -n "$ai_choppy_short_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.choppy.short-entry-threshold=$ai_choppy_short_entry_threshold")
fi
if [[ -n "$ai_trend_long_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.trend.long-entry-threshold=$ai_trend_long_entry_threshold")
fi
if [[ -n "$ai_trend_short_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.trend.short-entry-threshold=$ai_trend_short_entry_threshold")
fi
if [[ -n "$ai_volatile_long_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.volatile.long-entry-threshold=$ai_volatile_long_entry_threshold")
fi
if [[ -n "$ai_volatile_short_entry_threshold" ]]; then
  cmd+=("--trading.ai.regime.volatile.short-entry-threshold=$ai_volatile_short_entry_threshold")
fi
if [[ -n "$shared_capital_enabled" ]]; then
  cmd+=("--trading.shared-capital.enabled=$shared_capital_enabled")
fi
if [[ -n "$shared_capital_file" ]]; then
  cmd+=("--trading.shared-capital.file=$shared_capital_file")
fi
if [[ -n "$shared_capital_total_notional" ]]; then
  cmd+=("--trading.shared-capital.total-notional=$shared_capital_total_notional")
fi
if [[ -n "$databento_python_bin" ]]; then
  cmd+=("--trading.databento.python-bin=$databento_python_bin")
fi

if [[ -n "$max_trades_override" ]]; then
  cmd+=("--trading.max-trades=$max_trades_override")
fi

if [[ ${#extra_args[@]} -gt 0 ]]; then
  filtered_extra_args=()
  for extra_arg in "${extra_args[@]}"; do
    if extra_arg_is_managed_override "$extra_arg"; then
      continue
    fi
    filtered_extra_args+=("$extra_arg")
  done
  if [[ ${#filtered_extra_args[@]} -gt 0 ]]; then
    cmd+=("${filtered_extra_args[@]}")
  fi
fi

printf '[RUN] symbol=%s\n' "$symbol_upper"
printf '[RUN] java_bin=%s java_home=%s\n' "$java_bin" "${JAVA_HOME:-<unset>}"
printf '[RUN] java_opts=%s\n' "${java_opts[*]}"
printf '[RUN] properties=%s\n' "$properties_file"
printf '[RUN] jar_path=%s jar_status=%s\n' "$jar_path" "$jar_status"
printf '[RUN] require_prebuilt_jar=%s\n' "$require_prebuilt_jar"
printf '[RUN] model_dir=%s\n' "$model_dir"
printf '[RUN] configured_model_dir=%s\n' "$model_dir_prop"
printf '[RUN] setup_thresholds_file=%s source=%s\n' "${setup_thresholds_file:-disabled}" "${setup_thresholds_source:-default/properties}"
printf '[RUN] onnx_count=%s\n' "$onnx_count"
printf '[RUN] lifecycle_micro_enabled=%s lifecycle_model_dir=%s\n' "$lifecycle_micro_enabled" "$lifecycle_model_dir"
if truthy_env "$lifecycle_micro_enabled"; then
  printf '[RUN] lifecycle_exit_thresholds long=%s short=%s\n' "$lifecycle_long_exit_threshold_resolved" "$lifecycle_short_exit_threshold_resolved"
  printf '[RUN] micro_entry_thresholds long=%s source=%s short=%s source=%s\n' \
    "$micro_long_entry_threshold_resolved" \
    "$micro_long_entry_threshold_source" \
    "$micro_short_entry_threshold_resolved" \
    "$micro_short_entry_threshold_source"
  printf '[RUN] micro_exit_guard_thresholds long=%s short=%s\n' "$micro_long_exit_guard_threshold_resolved" "$micro_short_exit_guard_threshold_resolved"
fi
printf '[RUN] server_port=%s client_id=%s market_data_request_id=%s\n' "$server_port" "$client_id" "$market_data_request_id"
printf '[RUN] trade_amount=%s max_order_notional=%s max_share_cap=%s\n' "$trade_amount" "$max_notional" "${max_share_cap:-500}"
printf '[RUN] ibkr_host=%s ibkr_port=%s\n' "$ib_host" "$ib_port"
printf '[RUN] ai_thresholds longEntry=%s shortEntry=%s longExit=%s shortExit=%s regime=%s\n' \
  "${ai_long_entry_threshold:-default}" \
  "${ai_short_entry_threshold:-default}" \
  "${ai_long_exit_threshold:-default}" \
  "${ai_short_exit_threshold:-default}" \
  "${ai_regime_threshold:-default}"
printf '[RUN] ai_entry_threshold_raise_percent=%s\n' "${ai_entry_threshold_raise_percent:-default}"
printf '[RUN] ai_variant_entry_thresholds open30Long=%s open30Short=%s choppyLong=%s choppyShort=%s trendLong=%s trendShort=%s volatileLong=%s volatileShort=%s\n' \
  "${ai_open30_long_entry_threshold:-default}" \
  "${ai_open30_short_entry_threshold:-default}" \
  "${ai_choppy_long_entry_threshold:-default}" \
  "${ai_choppy_short_entry_threshold:-default}" \
  "${ai_trend_long_entry_threshold:-default}" \
  "${ai_trend_short_entry_threshold:-default}" \
  "${ai_volatile_long_entry_threshold:-default}" \
  "${ai_volatile_short_entry_threshold:-default}"
if [[ $uses_databento_sidecar -eq 1 ]]; then
  printf '[RUN] databento_python_bin=%s\n' "$databento_python_bin"
fi
if [[ $tee_db_mode -eq 1 ]]; then
  printf '[RUN] tee_db_python_bin=%s\n' "$tee_db_python_bin"
fi
printf '[RUN] shared_capital enabled=%s file=%s total_notional=%s\n' \
  "${shared_capital_enabled:-default}" \
  "${shared_capital_file:-default}" \
  "${shared_capital_total_notional:-default}"
printf '[RUN] state_file=%s\n' "$state_file"
printf '[RUN] trade_log_file=%s\n' "$trade_log_file"
printf '[RUN] app_log_file=%s\n' "$app_log_file"
printf '[RUN] mode=%s\n' "$([[ $start_mode -eq 1 ]] && printf 'START' || printf 'PREVIEW')"

printf '[RUN] command='
printf '%q ' "${cmd[@]}"
printf '\n'

if [[ $tee_mode -eq 1 ]]; then
  printf '[RUN] tee=%s\n' "$tee_path"
fi

if [[ $tee_db_mode -eq 1 ]]; then
  printf '[RUN] tee_db=postgres enabled\n'
fi

if [[ $start_mode -eq 0 ]]; then
  echo "[RUN] Preview complete. Re-run with --start to launch."
  exit 0
fi

if [[ $port_busy -eq 1 ]]; then
  echo "[RUN][ERROR] Refusing to launch because port $server_port is already in use." >&2
  exit 1
fi

if [[ $skip_ibkr_preflight -eq 0 ]]; then
  if [[ ! -x "$ensure_ibkr_script" ]]; then
    echo "[RUN][ERROR] Missing or non-executable IBKR preflight script: $ensure_ibkr_script" >&2
    exit 1
  fi
  echo "[RUN] ensuring_ibkr host=$ib_host port=$ib_port"
  "$ensure_ibkr_script" --host="$ib_host" --port="$ib_port"
fi

if [[ $tee_mode -eq 1 || $tee_db_mode -eq 1 ]]; then
  run_id="${symbol_upper}_$(date '+%Y%m%d_%H%M%S')"
  set -o pipefail
  if [[ $tee_db_mode -eq 1 ]]; then
    helper_cmd=(
      "$tee_db_python_bin"
      "$repo_root/stream_live_logs_to_db.py"
      --symbol "$symbol_upper"
      --run-id "$run_id"
      --source "run_symbol.sh"
    )
    if [[ $tee_mode -eq 1 ]]; then
      mkdir -p "$(dirname "$tee_path")"
      helper_cmd+=(--tee-file "$tee_path")
    fi
    "${cmd[@]}" 2>&1 | "${helper_cmd[@]}"
  else
    mkdir -p "$(dirname "$tee_path")"
    "${cmd[@]}" 2>&1 | tee -a "$tee_path"
  fi
else
  exec "${cmd[@]}"
fi



