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
MODEL_DIR="${MODEL_DIR:-}"
PYTHON_BIN="${PYTHON_BIN:-${DATABENTO_PYTHON_BIN:-}}"
DATABENTO_ENV_FILE="${BACKTEST_DATABENTO_ENV_FILE:-${TRADING_DATABENTO_ENV_FILE:-runtime/databento.env}}"
DRY_RUN="${DRY_RUN:-false}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-0}"
BACKTEST_MAX_TRADES="${BACKTEST_MAX_TRADES:-2000}"
BACKTEST_MAX_SHARE_CAP="${BACKTEST_MAX_SHARE_CAP:-500}"
SKIP_BUILD="${SKIP_BUILD:-false}"
ROUTING_CSV="${TRADING_DATABENTO_MODEL_ROUTING_CSV:-runtime/databento/model-routing.csv}"
CLASSPATH_FILE="runtime/backtests/databento_ibkr_sim_backtest_cp.txt"
LIFECYCLE_MICRO_ENABLED="${TRADING_LIFECYCLE_MICRO_ENABLED:-true}"
LIFECYCLE_MODEL_DIR="${TRADING_LIFECYCLE_MODEL_DIR:-model_exports/lifecycle_micro_20260523}"

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
  --model-dir DIR          30s ONNX model bundle directory. If omitted, runtime/databento/model-routing.csv is used when possible.
  --python-bin PATH        Python with databento installed. If omitted, common python3 locations are searched.
  --databento-env-file F   Env file containing DATABENTO_API_KEY. Default: runtime/databento.env
  --dry-run                Validate wiring without downloading Databento data.
  --timeout-seconds N      Kill the Databento stream if it has not completed after N seconds. Default: 0 (no timeout)
  --max-trades N           Strategy max trades during replay. Default: 2000
  --max-share-cap N        Simulated broker max shares per order. Default: 500
  --lifecycle-model-dir D  Lifecycle/micro ONNX bundle. Default: model_exports/lifecycle_micro_20260523
  --disable-lifecycle-micro Disable lifecycle exit and 5s micro entry/exit guard routes.
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
  candidates+=("$HOME/miniforge3/bin/python3" python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 /usr/bin/python3)
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

route_model_dir() {
  local symbol="$1" routing_csv="$2"
  [[ -f "$routing_csv" ]] || return 0
  awk -F, -v symbol="$symbol" 'NR > 1 && toupper($1) == symbol {print $4; exit}' "$routing_csv"
}

csv_threshold() {
  local model_name="$1" default_value="$2" scorecard="$3"
  if [[ -f "$scorecard" ]]; then
    awk -F, -v model="$model_name" -v fallback="$default_value" '
      NR > 1 && $1 == model {printf "%.4f", $6 + 0; found=1; exit}
      END {if (!found) printf "%.4f", fallback + 0}
    ' "$scorecard"
  else
    printf '%.4f' "$default_value"
  fi
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
    --python-bin) PYTHON_BIN="$2"; shift 2 ;;
    --databento-env-file) DATABENTO_ENV_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --max-trades) BACKTEST_MAX_TRADES="$2"; shift 2 ;;
    --max-share-cap) BACKTEST_MAX_SHARE_CAP="$2"; shift 2 ;;
    --lifecycle-model-dir) LIFECYCLE_MODEL_DIR="$2"; shift 2 ;;
    --lifecycle-model-dir=*) LIFECYCLE_MODEL_DIR="${1#--lifecycle-model-dir=}"; shift ;;
    --disable-lifecycle-micro) LIFECYCLE_MICRO_ENABLED="false"; shift ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "[BACKTEST][ERROR] Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! truthy "$LIST_SYMBOLS_ONLY" && [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "[BACKTEST][ERROR] --start and --end are required." >&2
  usage >&2
  exit 2
fi

[[ "$OUTPUT_DIR" != /* ]] && OUTPUT_DIR="$ROOT/$OUTPUT_DIR"
[[ "$DATABENTO_ENV_FILE" != /* ]] && DATABENTO_ENV_FILE="$ROOT/$DATABENTO_ENV_FILE"
[[ "$ROUTING_CSV" != /* ]] && ROUTING_CSV="$ROOT/$ROUTING_CSV"
[[ "$SYMBOLS_FILE" != /* ]] && SYMBOLS_FILE="$ROOT/$SYMBOLS_FILE"
[[ "$LIFECYCLE_MODEL_DIR" != /* ]] && LIFECYCLE_MODEL_DIR="$ROOT/$LIFECYCLE_MODEL_DIR"
mkdir -p "$OUTPUT_DIR" runtime/backtests

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

if truthy "$LIST_SYMBOLS_ONLY"; then
  printf '[BACKTEST] symbols=%s\n' "$(printf '%s,' "${symbols[@]}" | sed 's/,$//')" >&2
  printf '[BACKTEST] symbols_count=%s symbols_file=%s\n' "${#symbols[@]}" "$SYMBOLS_FILE" >&2
  printf '%s\n' "${symbols[@]}"
  exit 0
fi
printf '[BACKTEST] symbols=%s\n' "$(printf '%s,' "${symbols[@]}" | sed 's/,$//')"
printf '[BACKTEST] symbols_count=%s symbols_file=%s\n' "${#symbols[@]}" "$SYMBOLS_FILE"

if ! usable_databento_key "${DATABENTO_API_KEY:-}"; then
  load_env_file "$DATABENTO_ENV_FILE" || true
fi
if ! truthy "$DRY_RUN" && ! usable_databento_key "${DATABENTO_API_KEY:-}"; then
  echo "[BACKTEST][ERROR] Missing valid DATABENTO_API_KEY. Export it or put it in $DATABENTO_ENV_FILE." >&2
  exit 1
fi

PYTHON_BIN="$(resolve_python_bin "$PYTHON_BIN" || true)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "[BACKTEST][ERROR] Could not find Python with databento and pandas installed." >&2
  echo "[BACKTEST][ERROR] Try: python3 -m pip install -r requirements.txt" >&2
  exit 1
fi

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
SAFE_START="$(printf '%s' "$START_DATE" | tr -c '[:alnum:]' '_')"
SAFE_END="$(printf '%s' "$END_DATE" | tr -c '[:alnum:]' '_')"
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
  if [[ -n "$MODEL_DIR" && ! -d "$MODEL_DIR" ]]; then
    echo "[BACKTEST][ERROR] symbol=$SYMBOL model directory not found: $MODEL_DIR" >&2
    failures=$((failures + 1))
    continue
  fi
  if [[ -z "$MODEL_DIR" ]]; then
    echo "[BACKTEST][WARN] symbol=$SYMBOL no model directory resolved; strategy will initialize with AI disabled/fallback behavior." >&2
  fi

  TRADE_LOG="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-trades.csv"
  ORDER_HISTORY="$OUTPUT_DIR/${SYMBOL}-${SAFE_START}-to-${SAFE_END}-${RUN_TS}-orders.csv"
  JAVA_PROPS=(
    "-Dbacktest.symbol=$SYMBOL"
    "-Dbacktest.ibkrSimulation=true"
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
    "-Dbacktest.strategy.maxShareCap=$BACKTEST_MAX_SHARE_CAP"
    "-Dbacktest.tradeLogFile=$TRADE_LOG"
    "-Dbacktest.orderHistoryFile=$ORDER_HISTORY"
  )
  [[ -n "$MODEL_DIR" ]] && JAVA_PROPS+=("-Dtrading.model.dir=$MODEL_DIR")
  if truthy "$LIFECYCLE_MICRO_ENABLED"; then
    LIFECYCLE_SCORECARD="$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv"
    JAVA_PROPS+=(
      "-Dstrategy.model.upgradedRouteRequired=true"
      "-Dstrategy.exit.legacy30sEnabled=false"
      "-Dstrategy.exit.lifecycleEnabled=true"
      "-Dstrategy.micro.entryEnabled=true"
      "-Dstrategy.micro.exitGuardEnabled=true"
      "-Dstrategy.lifecycle.modelDir=$LIFECYCLE_MODEL_DIR"
      "-Dstrategy.micro.modelDir=$LIFECYCLE_MODEL_DIR"
      "-Dstrategy.exit.lifecycle.longThreshold=$(csv_threshold longExitLifecycleAi 0.60 "$LIFECYCLE_SCORECARD")"
      "-Dstrategy.exit.lifecycle.shortThreshold=$(csv_threshold shortExitLifecycleAi 0.60 "$LIFECYCLE_SCORECARD")"
      "-Dstrategy.micro.longEntryThreshold=$(csv_threshold longMicroEntryAi 0.58 "$LIFECYCLE_SCORECARD")"
      "-Dstrategy.micro.shortEntryThreshold=$(csv_threshold shortMicroEntryAi 0.58 "$LIFECYCLE_SCORECARD")"
      "-Dstrategy.micro.longExitGuardThreshold=$(csv_threshold longMicroExitGuardAi 0.70 "$LIFECYCLE_SCORECARD")"
      "-Dstrategy.micro.shortExitGuardThreshold=$(csv_threshold shortMicroExitGuardAi 0.70 "$LIFECYCLE_SCORECARD")"
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
[BACKTEST] symbol=$SYMBOL start=$START_DATE end=$END_DATE dry_run=$DRY_RUN ibkr_simulation=true
[BACKTEST] python_bin=$PYTHON_BIN
[BACKTEST] model_dir=${MODEL_DIR:-<none>}
[BACKTEST] lifecycle_micro_enabled=$LIFECYCLE_MICRO_ENABLED lifecycle_model_dir=$LIFECYCLE_MODEL_DIR
[BACKTEST] trade_log=$TRADE_LOG
[BACKTEST] order_history=$ORDER_HISTORY
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








