#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DEFAULT_CATBOOST_SETUP_MODEL_DIR="$ROOT/runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854"
DEFAULT_LIFECYCLE_MICRO_MODEL_DIR="$ROOT/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports"
SYMBOL="$(printf '%s' "${1:-${BACKTEST_SYMBOL:-TSLA}}" | tr '[:lower:]' '[:upper:]')"
START_DATE="${START_DATE:-2026-05-21}"
END_DATE="${END_DATE:-2026-05-21}"
DRY_RUN="${DRY_RUN:-false}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-0}"
BACKTEST_MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
LIFECYCLE_MICRO_ENABLED="${TRADING_LIFECYCLE_MICRO_ENABLED:-true}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-$DEFAULT_LIFECYCLE_MICRO_MODEL_DIR}"
LIFECYCLE_SCORECARD="$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv"
PROPERTIES_FILE="$ROOT/runtime/databento/bots/trading-$(printf '%s' "$SYMBOL" | tr '[:upper:]' '[:lower:]').properties"
ROUTING_CSV="${TRADING_DATABENTO_MODEL_ROUTING_CSV:-$ROOT/runtime/databento/model-routing.csv}"
BACKTEST_WORK_DIR="${TRADING_BACKTEST_WORK_DIR:-$ROOT/runtime/backtests}"
if [[ -z "${TRADING_BACKTEST_WORK_DIR:-}" && -L "$BACKTEST_WORK_DIR" && ! -e "$BACKTEST_WORK_DIR" ]]; then
  BACKTEST_WORK_DIR="$ROOT/runtime/local-backtests"
fi
CLASSPATH_FILE="$BACKTEST_WORK_DIR/databento_backtest_cp.txt"
DEFAULT_DATABENTO_ENV_FILE="$ROOT/runtime/databento.env"
DATABENTO_API_KEY_SOURCE="environment"

mkdir -p "$BACKTEST_WORK_DIR"

truthy_env() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

trim_spaces() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

strip_env_quotes() {
  local value="$1"
  if [[ ${#value} -ge 2 ]]; then
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value#\"}"
      value="${value%\"}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value#\'}"
      value="${value%\'}"
    fi
  fi
  printf '%s' "$value"
}

usable_databento_api_key() {
  local api_key="$(trim_spaces "${1:-}")"
  local lowered
  lowered="$(printf '%s' "$api_key" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    ""|replace_me|paste_your_api_key_here|\<your-key\>|\<your_databento_api_key\>|changeme)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

load_databento_env_file() {
  local path="$1"
  local line key value parent_databento_api_key_present
  [[ -f "$path" ]] || return 1
  parent_databento_api_key_present="${DATABENTO_API_KEY+x}"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(trim_spaces "$line")"
    [[ -n "$line" ]] || continue
    [[ "${line#\#}" == "$line" ]] || continue
    if [[ "$line" == export\ * ]]; then
      line="$(trim_spaces "${line#export }")"
    fi
    [[ "$line" == *=* ]] || continue
    key="$(trim_spaces "${line%%=*}")"
    value="$(strip_env_quotes "$(trim_spaces "${line#*=}")")"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    [[ -n "$value" ]] || continue
    if [[ "$key" == "DATABENTO_API_KEY" && -z "$parent_databento_api_key_present" ]]; then
      export "$key=$value"
    elif [[ -z "${!key-}" ]]; then
      export "$key=$value"
    fi
  done < "$path"
  return 0
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    echo "[BACKTEST][ERROR] Missing ${label}: $path" >&2
    exit 1
  fi
}

require_dir() {
  local path="$1"
  local label="$2"
  if [[ ! -d "$path" ]]; then
    echo "[BACKTEST][ERROR] Missing ${label}: $path" >&2
    exit 1
  fi
}

get_prop() {
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
    END {if (found != "") print found}
  ' "$file"
}

route_model_dir() {
  local symbol="$1"
  local routing_csv="$2"
  [[ -f "$routing_csv" ]] || return 0
  awk -F, -v symbol="$symbol" 'NR > 1 && $1 == symbol {print $4; exit}' "$routing_csv"
}

resolve_python_bin() {
  local configured="$1"
  local -a candidates=()
  local candidate resolved
  [[ -n "$configured" ]] && candidates+=("$configured")
  [[ -n "${DATABENTO_PYTHON_BIN:-}" ]] && candidates+=("$DATABENTO_PYTHON_BIN")
  candidates+=("$HOME/miniforge3/bin/python3" python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 /usr/bin/python3)
  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate" ]]; then
      resolved="$candidate"
    else
      resolved="$(command -v "$candidate" 2>/dev/null || true)"
    fi
    if [[ -n "$resolved" ]] && "$resolved" -c 'import databento, databento_dbn' >/dev/null 2>&1; then
      printf '%s' "$resolved"
      return 0
    fi
  done
  return 1
}

csv_threshold() {
  local model_name="$1"
  local default_value="$2"
  if [[ -f "$LIFECYCLE_SCORECARD" ]]; then
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
    ' "$LIFECYCLE_SCORECARD"
  else
    printf '%.4f' "$default_value"
  fi
}

setup_threshold() {
  local property_key="$1"
  local default_value="$2"
  local value=""
  if [[ -n "${SETUP_THRESHOLDS_FILE:-}" && -f "$SETUP_THRESHOLDS_FILE" ]]; then
    value="$(get_prop "$property_key" "$SETUP_THRESHOLDS_FILE")"
  fi
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$default_value"
  fi
}

if [[ -f "$PROPERTIES_FILE" ]]; then
  CONFIG_MODEL_DIR="$(get_prop trading.model.dir "$PROPERTIES_FILE")"
  CONFIG_PYTHON_BIN="$(get_prop trading.databento.python-bin "$PROPERTIES_FILE")"
  CONFIG_DATABENTO_ENV_FILE="$(get_prop trading.databento.env-file "$PROPERTIES_FILE")"
else
  CONFIG_MODEL_DIR=""
  CONFIG_PYTHON_BIN=""
  CONFIG_DATABENTO_ENV_FILE=""
fi

MODEL_DIR="${MODEL_DIR:-${TRADING_MODEL_DIR:-${TRADING_SETUP_MODEL_DIR:-}}}"
if [[ -z "$MODEL_DIR" ]]; then
  if [[ -d "$DEFAULT_CATBOOST_SETUP_MODEL_DIR" ]]; then
    MODEL_DIR="$DEFAULT_CATBOOST_SETUP_MODEL_DIR"
  else
    MODEL_DIR="${CONFIG_MODEL_DIR:-$(route_model_dir "$SYMBOL" "$ROUTING_CSV")}"
  fi
fi
SETUP_THRESHOLDS_FILE="${TRADING_SETUP_THRESHOLDS_FILE:-$MODEL_DIR/setup_runtime_thresholds.properties}"
SETUP_THRESHOLDS_FILE_LOWER="$(printf '%s' "$SETUP_THRESHOLDS_FILE" | tr '[:upper:]' '[:lower:]')"
if [[ "$SETUP_THRESHOLDS_FILE_LOWER" == "none" ]]; then
  SETUP_THRESHOLDS_FILE=""
elif [[ -n "$SETUP_THRESHOLDS_FILE" && "$SETUP_THRESHOLDS_FILE" != /* ]]; then
  SETUP_THRESHOLDS_FILE="$ROOT/$SETUP_THRESHOLDS_FILE"
fi
SETUP_THRESHOLDS_SOURCE="default"
if [[ -n "$SETUP_THRESHOLDS_FILE" && -f "$SETUP_THRESHOLDS_FILE" ]]; then
  SETUP_THRESHOLDS_SOURCE="properties:$SETUP_THRESHOLDS_FILE"
fi
PYTHON_BIN="$(resolve_python_bin "${PYTHON_BIN:-$CONFIG_PYTHON_BIN}" || true)"
if [[ -z "$PYTHON_BIN" ]] && truthy_env "$DRY_RUN"; then
  PYTHON_BIN="$(command -v python3 || command -v python || true)"
fi
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-${CONFIG_DATABENTO_ENV_FILE:-$DEFAULT_DATABENTO_ENV_FILE}}}"
if [[ "$DATABENTO_ENV_FILE" != /* ]]; then
  DATABENTO_ENV_FILE="$ROOT/$DATABENTO_ENV_FILE"
fi

if ! usable_databento_api_key "${DATABENTO_API_KEY:-}"; then
  if load_databento_env_file "$DATABENTO_ENV_FILE" && usable_databento_api_key "${DATABENTO_API_KEY:-}"; then
    DATABENTO_API_KEY_SOURCE="env-file:$DATABENTO_ENV_FILE"
  else
    DATABENTO_API_KEY_SOURCE="missing"
  fi
fi

if ! truthy_env "$DRY_RUN" && ! usable_databento_api_key "${DATABENTO_API_KEY:-}"; then
  echo "[BACKTEST][ERROR] Missing valid DATABENTO_API_KEY for non-dry-run historical API streaming." >&2
  echo "[BACKTEST][ERROR] Set it in the parent environment or in $DATABENTO_ENV_FILE, matching the live Databento sidecar path." >&2
  exit 1
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "[BACKTEST][ERROR] Could not find a Python interpreter with databento and databento_dbn installed." >&2
  echo "[BACKTEST][ERROR] Set PYTHON_BIN or DATABENTO_PYTHON_BIN, or install dependencies from requirements.txt." >&2
  exit 1
fi

require_dir "$MODEL_DIR" "30s route model directory"
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
  require_file "$MODEL_DIR/$required_30s_model" "30s route model $required_30s_model"
done

if truthy_env "$LIFECYCLE_MICRO_ENABLED"; then
  require_dir "$LIFECYCLE_MODEL_DIR" "lifecycle/micro model directory"
  for required_lifecycle_model in \
    long_exit_lifecycle.onnx \
    short_exit_lifecycle.onnx \
    long_micro_entry_5s.onnx \
    short_micro_entry_5s.onnx \
    long_micro_exit_guard_5s.onnx \
    short_micro_exit_guard_5s.onnx \
    lifecycle_micro_scorecard.csv \
    lifecycle_micro_route_manifest.json; do
    require_file "$LIFECYCLE_MODEL_DIR/$required_lifecycle_model" "lifecycle/micro artifact $required_lifecycle_model"
  done
fi

"$ROOT/mvnw" -q -DskipTests package
"$ROOT/mvnw" -q dependency:build-classpath -Dmdep.outputFile="$CLASSPATH_FILE"

BACKTEST_RUN_TIMESTAMP="${BACKTEST_RUN_TIMESTAMP:-$(date +%Y%m%d_%H%M%S)}"
BACKTEST_FILE_PREFIX="$BACKTEST_WORK_DIR/databento-stream-$(printf '%s' "$SYMBOL" | tr '[:upper:]' '[:lower:]')-$BACKTEST_RUN_TIMESTAMP"

SETUP_LONG_ENTRY_THRESHOLD="${TRADING_AI_LONG_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.long-entry-threshold 0.68)}"
SETUP_SHORT_ENTRY_THRESHOLD="${TRADING_AI_SHORT_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.short-entry-threshold 0.63)}"
SETUP_LONG_EXIT_THRESHOLD="${TRADING_AI_LONG_EXIT_THRESHOLD:-$(setup_threshold trading.ai.long-exit-threshold 0.58)}"
SETUP_SHORT_EXIT_THRESHOLD="${TRADING_AI_SHORT_EXIT_THRESHOLD:-$(setup_threshold trading.ai.short-exit-threshold 0.60)}"
SETUP_REGIME_THRESHOLD="${TRADING_AI_REGIME_THRESHOLD:-$(setup_threshold trading.ai.regime-threshold 0.50)}"
SETUP_ENTRY_THRESHOLD_RAISE_PERCENT="${TRADING_AI_ENTRY_THRESHOLD_RAISE_PERCENT:-$(setup_threshold trading.ai.entry-threshold-raise-percent 0.0)}"
SETUP_OPEN30_LONG_ENTRY_THRESHOLD="${TRADING_AI_OPEN30_LONG_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.open30.long-entry-threshold "$SETUP_LONG_ENTRY_THRESHOLD")}"
SETUP_OPEN30_SHORT_ENTRY_THRESHOLD="${TRADING_AI_OPEN30_SHORT_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.open30.short-entry-threshold "$SETUP_SHORT_ENTRY_THRESHOLD")}"
SETUP_CHOPPY_LONG_ENTRY_THRESHOLD="${TRADING_AI_REGIME_CHOPPY_LONG_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.choppy.long-entry-threshold "$SETUP_LONG_ENTRY_THRESHOLD")}"
SETUP_CHOPPY_SHORT_ENTRY_THRESHOLD="${TRADING_AI_REGIME_CHOPPY_SHORT_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.choppy.short-entry-threshold "$SETUP_SHORT_ENTRY_THRESHOLD")}"
SETUP_TREND_LONG_ENTRY_THRESHOLD="${TRADING_AI_REGIME_TREND_LONG_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.trend.long-entry-threshold "$SETUP_LONG_ENTRY_THRESHOLD")}"
SETUP_TREND_SHORT_ENTRY_THRESHOLD="${TRADING_AI_REGIME_TREND_SHORT_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.trend.short-entry-threshold "$SETUP_SHORT_ENTRY_THRESHOLD")}"
SETUP_VOLATILE_LONG_ENTRY_THRESHOLD="${TRADING_AI_REGIME_VOLATILE_LONG_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.volatile.long-entry-threshold "$SETUP_LONG_ENTRY_THRESHOLD")}"
SETUP_VOLATILE_SHORT_ENTRY_THRESHOLD="${TRADING_AI_REGIME_VOLATILE_SHORT_ENTRY_THRESHOLD:-$(setup_threshold trading.ai.regime.volatile.short-entry-threshold "$SETUP_SHORT_ENTRY_THRESHOLD")}"

JAVA_PROPS=(
  "-Dtrading.model.dir=$MODEL_DIR"
  "-Dtrading.ai.long-entry-threshold=$SETUP_LONG_ENTRY_THRESHOLD"
  "-Dtrading.ai.short-entry-threshold=$SETUP_SHORT_ENTRY_THRESHOLD"
  "-Dtrading.ai.long-exit-threshold=$SETUP_LONG_EXIT_THRESHOLD"
  "-Dtrading.ai.short-exit-threshold=$SETUP_SHORT_EXIT_THRESHOLD"
  "-Dtrading.ai.regime-threshold=$SETUP_REGIME_THRESHOLD"
  "-Dtrading.ai.entry-threshold-raise-percent=$SETUP_ENTRY_THRESHOLD_RAISE_PERCENT"
  "-Dtrading.ai.open30.long-entry-threshold=$SETUP_OPEN30_LONG_ENTRY_THRESHOLD"
  "-Dtrading.ai.open30.short-entry-threshold=$SETUP_OPEN30_SHORT_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.choppy.long-entry-threshold=$SETUP_CHOPPY_LONG_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.choppy.short-entry-threshold=$SETUP_CHOPPY_SHORT_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.trend.long-entry-threshold=$SETUP_TREND_LONG_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.trend.short-entry-threshold=$SETUP_TREND_SHORT_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.volatile.long-entry-threshold=$SETUP_VOLATILE_LONG_ENTRY_THRESHOLD"
  "-Dtrading.ai.regime.volatile.short-entry-threshold=$SETUP_VOLATILE_SHORT_ENTRY_THRESHOLD"
  "-Dbacktest.symbol=$SYMBOL"
  "-Dbacktest.ibkrSimulation=true"
  "-Dbacktest.tradeLogFile=$BACKTEST_FILE_PREFIX.csv"
  "-Dbacktest.orderHistoryFile=$BACKTEST_FILE_PREFIX-orders.csv"
  "-Dbacktest.tradeLifecycleSummaryFile=$BACKTEST_FILE_PREFIX-trade-lifecycle-summary.csv"
  "-Dbacktest.streamSanityReportFile=$BACKTEST_FILE_PREFIX-sanity.json"
  "-Dbacktest.databento.python=$PYTHON_BIN"
  "-Dbacktest.databento.streamer=scripts/databento_historical_streamer.py"
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
)

if truthy_env "$LIFECYCLE_MICRO_ENABLED"; then
  JAVA_PROPS+=(
    "-Dstrategy.model.upgradedRouteRequired=true"
    "-Dstrategy.exit.legacy30sEnabled=false"
    "-Dstrategy.exit.lifecycleEnabled=true"
    "-Dstrategy.micro.entryEnabled=true"
    "-Dstrategy.micro.exitGuardEnabled=true"
    "-Dstrategy.lifecycle.modelDir=$LIFECYCLE_MODEL_DIR"
    "-Dstrategy.micro.modelDir=$LIFECYCLE_MODEL_DIR"
    "-Dstrategy.exit.lifecycle.longThreshold=$(csv_threshold longExitLifecycleAi 0.60)"
    "-Dstrategy.exit.lifecycle.shortThreshold=$(csv_threshold shortExitLifecycleAi 0.60)"
    "-Dstrategy.micro.longEntryThreshold=$(csv_threshold longMicroEntryAi 0.58)"
    "-Dstrategy.micro.shortEntryThreshold=$(csv_threshold shortMicroEntryAi 0.58)"
    "-Dstrategy.micro.longExitGuardThreshold=$(csv_threshold longMicroExitGuardAi 0.70)"
    "-Dstrategy.micro.shortExitGuardThreshold=$(csv_threshold shortMicroExitGuardAi 0.70)"
  )
else
  JAVA_PROPS+=(
    "-Dstrategy.exit.lifecycleEnabled=${LIFECYCLE_EXIT_ENABLED:-false}"
    "-Dstrategy.micro.entryEnabled=${MICRO_ENTRY_ENABLED:-false}"
    "-Dstrategy.micro.exitGuardEnabled=${MICRO_EXIT_GUARD_ENABLED:-false}"
  )
fi

printf '[BACKTEST] symbol=%s dry_run=%s ibkr_simulation=true\n' "$SYMBOL" "$DRY_RUN"
printf '[BACKTEST] model_dir=%s\n' "$MODEL_DIR"
printf '[BACKTEST] setup_thresholds_file=%s source=%s\n' "${SETUP_THRESHOLDS_FILE:-disabled}" "$SETUP_THRESHOLDS_SOURCE"
printf '[BACKTEST] setup_thresholds long=%s short=%s regime=%s raisePercent=%s\n' "$SETUP_LONG_ENTRY_THRESHOLD" "$SETUP_SHORT_ENTRY_THRESHOLD" "$SETUP_REGIME_THRESHOLD" "$SETUP_ENTRY_THRESHOLD_RAISE_PERCENT"
printf '[BACKTEST] setup_variant_entry_thresholds open30Long=%s open30Short=%s choppyLong=%s choppyShort=%s trendLong=%s trendShort=%s volatileLong=%s volatileShort=%s\n' \
  "$SETUP_OPEN30_LONG_ENTRY_THRESHOLD" \
  "$SETUP_OPEN30_SHORT_ENTRY_THRESHOLD" \
  "$SETUP_CHOPPY_LONG_ENTRY_THRESHOLD" \
  "$SETUP_CHOPPY_SHORT_ENTRY_THRESHOLD" \
  "$SETUP_TREND_LONG_ENTRY_THRESHOLD" \
  "$SETUP_TREND_SHORT_ENTRY_THRESHOLD" \
  "$SETUP_VOLATILE_LONG_ENTRY_THRESHOLD" \
  "$SETUP_VOLATILE_SHORT_ENTRY_THRESHOLD"
printf '[BACKTEST] lifecycle_micro_enabled=%s lifecycle_model_dir=%s\n' "$LIFECYCLE_MICRO_ENABLED" "$LIFECYCLE_MODEL_DIR"
printf '[BACKTEST] python_bin=%s\n' "$PYTHON_BIN"
printf '[BACKTEST] databento_api_key_source=%s\n' "$DATABENTO_API_KEY_SOURCE"
printf '[BACKTEST] date_window=%s..%s timeout_seconds=%s max_trades=%s\n' "$START_DATE" "$END_DATE" "$TIMEOUT_SECONDS" "$BACKTEST_MAX_TRADES"

java -cp "target/classes:$(cat "$CLASSPATH_FILE")" \
  "${JAVA_PROPS[@]}" \
  com.calgary.fili.trader.testers.DatabentoHistoricalStreamingBacktester \
  "$SYMBOL"

