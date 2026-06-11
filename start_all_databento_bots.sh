#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start_all_databento_bots.sh [--start] [--symbols=CSV] [--exclude=CSV] [--max-trades=N] [--max-share-cap=N] [--trade-amount=N] [--max-order-notional=N] [--per-trade-notional=N] [--tee] [--tee-db] [--stagger-seconds=N] [--ibkr-client-cap=N] [--allow-over-ibkr-client-cap] [--ensure-ibkr] [--skip-ibkr-preflight] [--skip-local-data-preflight] [--ibkr-shared-gateway] [--no-ibkr-shared-gateway] [--ibkr-shared-gateway-host=HOST] [--ibkr-shared-gateway-port=PORT] [--ibkr-shared-gateway-skip-direct-connection] [--ibkr-shared-gateway-allow-direct-fallback] [--startup-history-seconds=N] [--startup-history-schema=SCHEMA] [--list] [-- <extra run_symbol args...>]

Behavior:
  - Discovers Databento bot configs from runtime/databento/bots/trading-*.properties.
  - Preview mode is the default: runs the existing single-symbol launcher in preview mode for every selected symbol.
  - Use --start to actually launch all selected bots in the background.
  - In --start mode, the script first runs a preflight preview for every selected symbol and aborts if any one fails.
  - Bulk preview/start always require an already-built packaged jar; they never trigger a Maven rebuild.
  - Bulk preview/start first verify startup-critical model/config data is present locally and not hidden behind broken external-disk symlinks.
  - In --start mode, the script automatically waits for IBKR to be reachable before launch unless you pass --skip-ibkr-preflight.
  - Bulk launch enables the shared IBKR gateway by default and points every symbol bot at the same gateway host/port.
  - By default the bulk launcher also sets trading.ibkr.shared-gateway.skip-direct-connection=true, so symbol bots do not consume one direct IBKR API client each.
  - In shared-only mode, the script automatically starts the companion shared IBKR gateway server if needed, then verifies reachability before launching bots.
  - Launches each bot via ./run_symbol.sh so the existing per-symbol validation remains authoritative.
  - In --start mode, direct-IBKR launches are capped by default to a conservative IBKR client budget of 32 unless you explicitly override it.

Examples:
  ./start_all_databento_bots.sh
  ./start_all_databento_bots.sh --list
  ./start_all_databento_bots.sh --symbols=SPY,AAPL,NVDA
  ./start_all_databento_bots.sh --start --max-trades=0
  ./start_all_databento_bots.sh --start --symbols=TSLA,TQQQ --tee --startup-history-seconds=0 --max-trades=2 --per-trade-notional=5000 --max-share-cap=25
  ./start_all_databento_bots.sh --start --symbols=SPY,AAPL,NVDA --tee --stagger-seconds=1
  ./start_all_databento_bots.sh --start --exclude=SPY,QQQ --tee-db
  ./start_all_databento_bots.sh --start --tee --tee-db --startup-history-seconds=0
  ./start_all_databento_bots.sh --start --ibkr-client-cap=20
  ./start_all_databento_bots.sh --start --allow-over-ibkr-client-cap
  ./start_all_databento_bots.sh --start --ibkr-shared-gateway-host=127.0.0.1 --ibkr-shared-gateway-port=9910
  ./start_all_databento_bots.sh --start --no-ibkr-shared-gateway
  ./start_all_databento_bots.sh --start --tee --tee-db -- --trading.databento.startup-history-seconds=120 --trading.databento.startup-history-schema=ohlcv-1s
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
bots_dir="$repo_root/runtime/databento/bots"
launch_dir="$repo_root/runtime/databento/launchers"
gateway_probe_script="$repo_root/databento_ibkr_bridge/src/databento_ibkr_bridge/shared_ibkr_gateway_health.py"
run_symbol_script="$repo_root/run_symbol.sh"
ensure_ibkr_script="$repo_root/ensure_ibkr_workstation.sh"
start_shared_ibkr_gateway_script="$repo_root/start_shared_ibkr_gateway.sh"
local_startup_data_verifier_script="$repo_root/verify_databento_local_startup_data.sh"
jar_path="$repo_root/target/trading-agent-0.0.1-SNAPSHOT.jar"
default_direct_ibkr_client_cap=32

start_mode=0
tee_mode=0
tee_db_mode=0
ensure_ibkr_mode="auto"
local_data_preflight_enabled=1
list_mode=0
symbols_csv=""
exclude_csv=""
max_trades_override=""
trade_amount_override=""
max_notional_override=""
max_share_cap_override=""
stagger_seconds="0"
startup_history_seconds_override=""
startup_history_schema_override=""
ibkr_client_cap="${IBKR_CLIENT_CAP:-$default_direct_ibkr_client_cap}"
allow_over_ibkr_client_cap=0
ibkr_shared_gateway_enabled=1
ibkr_shared_gateway_host="${IBKR_SHARED_GATEWAY_HOST:-127.0.0.1}"
ibkr_shared_gateway_port="${IBKR_SHARED_GATEWAY_PORT:-9910}"
ibkr_shared_gateway_connect_timeout_ms="${IBKR_SHARED_GATEWAY_CONNECT_TIMEOUT_MS:-3000}"
ibkr_shared_gateway_ack_timeout_ms="${IBKR_SHARED_GATEWAY_ACK_TIMEOUT_MS:-5000}"
ibkr_shared_gateway_skip_direct_connection=1
databento_shared_feed_expected_client_count="${DATABENTO_SHARED_FEED_EXPECTED_CLIENT_COUNT:-}"
databento_shared_feed_client_wait_timeout_ms="${DATABENTO_SHARED_FEED_CLIENT_WAIT_TIMEOUT_MS:-60000}"
extra_args=()

normalize_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
}

lower_symbol() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

trim_spaces() {
  printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
}

is_truthy() {
  local raw lowered
  raw="$(trim_spaces "$1")"
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$lowered" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

extract_prop() {
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

contains_symbol() {
  local needle="$1"
  shift || true
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
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

discover_symbols() {
  local path filename symbol
  shopt -s nullglob
  for path in "$bots_dir"/trading-*.properties; do
    filename="$(basename "$path")"
    if [[ "$filename" == "trading-databento-template.properties" ]]; then
      continue
    fi
    symbol="$(extract_prop "$path" "trading.symbol")"
    if [[ -z "$symbol" ]]; then
      symbol="${filename#trading-}"
      symbol="${symbol%.properties}"
    fi
    symbol="$(normalize_symbol "$symbol")"
    if [[ -n "$symbol" ]]; then
      printf '%s\n' "$symbol"
    fi
  done
  shopt -u nullglob
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

symbol_uses_direct_ibkr_connection() {
  local symbol="$1"
  if [[ -n "$symbol" ]]; then
    :
  fi
  if [[ $ibkr_shared_gateway_enabled -eq 1 && $ibkr_shared_gateway_skip_direct_connection -eq 1 ]]; then
    return 1
  fi
  return 0
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

resolve_symbol_shared_feed_target() {
  local symbol="$1"
  local symbol_lower properties_path relay_host relay_port
  symbol_lower="$(lower_symbol "$symbol")"
  properties_path="$(resolve_symbol_properties_path "$symbol_lower" || true)"
  relay_host="127.0.0.1"
  relay_port="9800"
  if [[ -n "$properties_path" ]]; then
    relay_host="$(extract_prop "$properties_path" "trading.databento.shared-feed.host")"
    relay_port="$(extract_prop "$properties_path" "trading.databento.shared-feed.port")"
  fi
  relay_host="${relay_host:-127.0.0.1}"
  relay_port="${relay_port:-9800}"
  printf '%s:%s' "$relay_host" "$relay_port"
}

count_selected_symbols_for_shared_feed_target() {
  local target="$1"
  local count=0 symbol symbol_target
  for symbol in "${selected_symbols[@]-}"; do
    symbol_target="$(resolve_symbol_shared_feed_target "$symbol")"
    if [[ "$symbol_target" == "$target" ]]; then
      count=$((count + 1))
    fi
  done
  printf '%s' "$count"
}

port_reachable() {
  local host="$1"
  local port="$2"
  nc -z "$host" "$port" >/dev/null 2>&1
}

millis_to_seconds() {
  local millis="$1"
  local fallback="$2"
  case "$millis" in
    ''|*[!0-9]*)
      printf '%s' "$fallback"
      ;;
    *)
      awk -v ms="$millis" 'BEGIN { printf "%.3f", ((ms + 0.0) / 1000.0) }'
      ;;
  esac
}

resolve_gateway_probe_python() {
  local candidate resolved
  for candidate in \
    "${DATABENTO_PYTHON_BIN:-}" \
    python3 \
    /opt/homebrew/bin/python3 \
    /usr/local/bin/python3 \
    /usr/bin/python3; do
    [[ -n "$candidate" ]] || continue
    if [[ -x "$candidate" ]]; then
      printf '%s' "$candidate"
      return 0
    fi
    resolved="$(command -v "$candidate" 2>/dev/null || true)"
    if [[ -n "$resolved" && -x "$resolved" ]]; then
      printf '%s' "$resolved"
      return 0
    fi
  done
  return 1
}

shared_gateway_protocol_healthy() {
  local probe_python
  local connect_timeout_seconds ack_timeout_seconds
  [[ -f "$gateway_probe_script" ]] || return 1
  probe_python="$(resolve_gateway_probe_python || true)"
  [[ -n "$probe_python" ]] || return 1
  connect_timeout_seconds="$(millis_to_seconds "$ibkr_shared_gateway_connect_timeout_ms" "3.000")"
  ack_timeout_seconds="$(millis_to_seconds "$ibkr_shared_gateway_ack_timeout_ms" "5.000")"
  "$probe_python" "$gateway_probe_script" \
    --host="$ibkr_shared_gateway_host" \
    --port="$ibkr_shared_gateway_port" \
    --connect-timeout-seconds="$connect_timeout_seconds" \
    --ack-timeout-seconds="$ack_timeout_seconds" \
    --quiet >/dev/null 2>&1
}

build_symbol_cmd() {
  local symbol="$1"
  local mode="$2"
  local -a cmd gateway_args feed_args
  local shared_feed_target shard_selected_count
  cmd=("$run_symbol_script" "$symbol")
  cmd+=(--require-prebuilt-jar)
  gateway_args=()
  feed_args=()
  if [[ "$mode" == "start" ]]; then
    cmd+=(--start)
    if [[ "$ensure_ibkr_mode" == "no" || ${should_run_ensure_ibkr:-0} -eq 1 ]]; then
      cmd+=(--skip-ibkr-preflight)
    fi
  fi
  if [[ -n "$max_trades_override" ]]; then
    cmd+=("--max-trades=$max_trades_override")
  fi
  if [[ -n "$trade_amount_override" ]]; then
    cmd+=("--trade-amount=$trade_amount_override")
  fi
  if [[ -n "$max_notional_override" ]]; then
    cmd+=("--max-order-notional=$max_notional_override")
  fi
  if [[ -n "$max_share_cap_override" ]]; then
    cmd+=("--max-share-cap=$max_share_cap_override")
  fi
  if [[ $tee_mode -eq 1 ]]; then
    cmd+=(--tee)
  fi
  if [[ $tee_db_mode -eq 1 ]]; then
    cmd+=(--tee-db)
  fi
  if [[ $ibkr_shared_gateway_enabled -eq 1 ]]; then
    gateway_args+=("--trading.ibkr.shared-gateway.enabled=true")
    gateway_args+=("--trading.ibkr.shared-gateway.host=$ibkr_shared_gateway_host")
    gateway_args+=("--trading.ibkr.shared-gateway.port=$ibkr_shared_gateway_port")
    gateway_args+=("--trading.ibkr.shared-gateway.connect-timeout-ms=$ibkr_shared_gateway_connect_timeout_ms")
    gateway_args+=("--trading.ibkr.shared-gateway.ack-timeout-ms=$ibkr_shared_gateway_ack_timeout_ms")
    if [[ $ibkr_shared_gateway_skip_direct_connection -eq 1 ]]; then
      gateway_args+=("--trading.ibkr.shared-gateway.skip-direct-connection=true")
    else
      gateway_args+=("--trading.ibkr.shared-gateway.skip-direct-connection=false")
    fi
  else
    gateway_args+=("--trading.ibkr.shared-gateway.enabled=false")
    gateway_args+=("--trading.ibkr.shared-gateway.skip-direct-connection=false")
  fi
  shared_feed_target="$(resolve_symbol_shared_feed_target "$symbol")"
  shard_selected_count="$(count_selected_symbols_for_shared_feed_target "$shared_feed_target")"
  if [[ -n "$databento_shared_feed_expected_client_count" ]]; then
    feed_args+=("--trading.databento.shared-feed.expected-client-count=$databento_shared_feed_expected_client_count")
  elif [[ -n "$shard_selected_count" && "$shard_selected_count" -gt 0 ]]; then
    feed_args+=("--trading.databento.shared-feed.expected-client-count=$shard_selected_count")
  fi
  if [[ -n "$databento_shared_feed_client_wait_timeout_ms" ]]; then
    feed_args+=("--trading.databento.shared-feed.client-wait-timeout-ms=$databento_shared_feed_client_wait_timeout_ms")
  fi
  if [[ ${#gateway_args[@]} -gt 0 || ${#feed_args[@]} -gt 0 || ${#extra_args[@]} -gt 0 ]]; then
    cmd+=(--)
    if [[ ${#gateway_args[@]} -gt 0 ]]; then
      cmd+=("${gateway_args[@]}")
    fi
    if [[ ${#feed_args[@]} -gt 0 ]]; then
      cmd+=("${feed_args[@]}")
    fi
    if [[ ${#extra_args[@]} -gt 0 ]]; then
    cmd+=("${extra_args[@]}")
    fi
  fi
  printf '%q ' "${cmd[@]}"
}

log() {
  printf '[BULK-LAUNCH][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

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

ensure_bulk_shared_prereqs() {
  if [[ ! -f "$jar_path" ]]; then
    echo "[BULK-LAUNCH][ERROR] Missing packaged jar required by bulk preview/start: $jar_path" >&2
    echo "[BULK-LAUNCH][ERROR] Build it first: cd $repo_root && ./mvnw -DskipTests package" >&2
    exit 1
  fi

  if jar_is_stale; then
    echo "[BULK-LAUNCH][ERROR] Packaged jar is stale and bulk preview/start requires a prebuilt jar: $jar_path" >&2
    echo "[BULK-LAUNCH][ERROR] Rebuild it first: cd $repo_root && ./mvnw -DskipTests package" >&2
    exit 1
  fi

  if [[ ! -f "$repo_root/runtime/postgres-local.properties" ]]; then
    echo "[BULK-LAUNCH][ERROR] Missing shared runtime postgres properties: $repo_root/runtime/postgres-local.properties" >&2
    exit 1
  fi

  if [[ $tee_db_mode -eq 1 && ! -f "$repo_root/stream_live_logs_to_db.py" ]]; then
    echo "[BULK-LAUNCH][ERROR] Missing DB tee helper required by --tee-db: $repo_root/stream_live_logs_to_db.py" >&2
    exit 1
  fi

}

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
    --tee-db)
      tee_db_mode=1
      ;;
    --ensure-ibkr)
      ensure_ibkr_mode="yes"
      ;;
    --skip-ibkr-preflight|--no-ensure-ibkr)
      ensure_ibkr_mode="no"
      ;;
    --skip-local-data-preflight)
      local_data_preflight_enabled=0
      ;;
    --list)
      list_mode=1
      ;;
    --symbols=*)
      symbols_csv="${1#--symbols=}"
      ;;
    --exclude=*)
      exclude_csv="${1#--exclude=}"
      ;;
    --max-trades=*)
      max_trades_override="${1#--max-trades=}"
      ;;
    --trade-amount=*)
      trade_amount_override="${1#--trade-amount=}"
      ;;
    --max-order-notional=*)
      max_notional_override="${1#--max-order-notional=}"
      ;;
    --max-share-cap=*)
      max_share_cap_override="${1#--max-share-cap=}"
      ;;
    --per-trade-notional=*|--trade-notional=*)
      trade_amount_override="${1#*=}"
      max_notional_override="${1#*=}"
      ;;
    --stagger-seconds=*)
      stagger_seconds="${1#--stagger-seconds=}"
      ;;
    --startup-history-seconds=*)
      startup_history_seconds_override="${1#--startup-history-seconds=}"
      ;;
    --startup-history-schema=*)
      startup_history_schema_override="${1#--startup-history-schema=}"
      ;;
    --ibkr-client-cap=*)
      ibkr_client_cap="${1#--ibkr-client-cap=}"
      ;;
    --allow-over-ibkr-client-cap)
      allow_over_ibkr_client_cap=1
      ;;
    --ibkr-shared-gateway)
      ibkr_shared_gateway_enabled=1
      ;;
    --no-ibkr-shared-gateway|--skip-ibkr-shared-gateway)
      ibkr_shared_gateway_enabled=0
      ;;
    --ibkr-shared-gateway-host=*)
      ibkr_shared_gateway_host="${1#--ibkr-shared-gateway-host=}"
      ;;
    --ibkr-shared-gateway-port=*)
      ibkr_shared_gateway_port="${1#--ibkr-shared-gateway-port=}"
      ;;
    --ibkr-shared-gateway-connect-timeout-ms=*)
      ibkr_shared_gateway_connect_timeout_ms="${1#--ibkr-shared-gateway-connect-timeout-ms=}"
      ;;
    --ibkr-shared-gateway-ack-timeout-ms=*)
      ibkr_shared_gateway_ack_timeout_ms="${1#--ibkr-shared-gateway-ack-timeout-ms=}"
      ;;
    --ibkr-shared-gateway-skip-direct-connection)
      ibkr_shared_gateway_skip_direct_connection=1
      ;;
    --ibkr-shared-gateway-allow-direct-fallback)
      ibkr_shared_gateway_skip_direct_connection=0
      ;;
    --)
      shift
      extra_args=("$@")
      break
      ;;
    *)
      echo "[BULK-LAUNCH][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ ! -d "$bots_dir" ]]; then
  echo "[BULK-LAUNCH][ERROR] Missing bots directory: $bots_dir" >&2
  exit 1
fi

if [[ ! -x "$run_symbol_script" ]]; then
  echo "[BULK-LAUNCH][ERROR] Missing or non-executable launcher: $run_symbol_script" >&2
  exit 1
fi

case "$stagger_seconds" in
  ''|*[!0-9.]* )
    echo "[BULK-LAUNCH][ERROR] --stagger-seconds must be numeric." >&2
    exit 1
    ;;
esac

if [[ -n "$trade_amount_override" && ! "$trade_amount_override" =~ ^[1-9][0-9]*$ ]]; then
  echo "[BULK-LAUNCH][ERROR] --trade-amount/--per-trade-notional must be a positive whole-dollar amount." >&2
  exit 1
fi

if [[ -n "$max_notional_override" ]] && ! awk -v value="$max_notional_override" 'BEGIN { exit !(value ~ /^[0-9]+([.][0-9]+)?$/ && value + 0 > 0) }'; then
  echo "[BULK-LAUNCH][ERROR] --max-order-notional/--per-trade-notional must be a positive number." >&2
  exit 1
fi

if [[ -n "$max_share_cap_override" && ! "$max_share_cap_override" =~ ^[1-9][0-9]*$ ]]; then
  echo "[BULK-LAUNCH][ERROR] --max-share-cap must be a positive whole-share quantity." >&2
  exit 1
fi

if [[ -n "$startup_history_seconds_override" ]]; then
  case "$startup_history_seconds_override" in
    ''|*[!0-9.]* )
      echo "[BULK-LAUNCH][ERROR] --startup-history-seconds must be numeric." >&2
      exit 1
      ;;
  esac
fi

if [[ -n "$startup_history_seconds_override" ]]; then
  extra_args+=("--trading.databento.startup-history-seconds=$startup_history_seconds_override")
fi
if [[ -n "$startup_history_schema_override" ]]; then
  extra_args+=("--trading.databento.startup-history-schema=$startup_history_schema_override")
fi

case "$ibkr_client_cap" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] --ibkr-client-cap must be a whole number." >&2
    exit 1
    ;;
esac

case "$ibkr_shared_gateway_port" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] --ibkr-shared-gateway-port must be a whole number." >&2
    exit 1
    ;;
esac

case "$ibkr_shared_gateway_connect_timeout_ms" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] --ibkr-shared-gateway-connect-timeout-ms must be a whole number." >&2
    exit 1
    ;;
esac

case "$ibkr_shared_gateway_ack_timeout_ms" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] --ibkr-shared-gateway-ack-timeout-ms must be a whole number." >&2
    exit 1
    ;;
esac

extra_arg_gateway_enabled_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.enabled)"
extra_arg_gateway_host_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.host)"
extra_arg_gateway_port_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.port)"
extra_arg_gateway_connect_timeout_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.connect-timeout-ms)"
extra_arg_gateway_ack_timeout_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.ack-timeout-ms)"
extra_arg_gateway_skip_direct_override="$(resolve_extra_arg_override trading.ibkr.shared-gateway.skip-direct-connection)"
extra_arg_shared_feed_expected_client_count_override="$(resolve_extra_arg_override trading.databento.shared-feed.expected-client-count)"
extra_arg_shared_feed_client_wait_timeout_override="$(resolve_extra_arg_override trading.databento.shared-feed.client-wait-timeout-ms)"

if [[ -n "$extra_arg_gateway_enabled_override" ]]; then
  if is_truthy "$extra_arg_gateway_enabled_override"; then
    ibkr_shared_gateway_enabled=1
  else
    ibkr_shared_gateway_enabled=0
  fi
fi
if [[ -n "$extra_arg_gateway_host_override" ]]; then
  ibkr_shared_gateway_host="$extra_arg_gateway_host_override"
fi
if [[ -n "$extra_arg_gateway_port_override" ]]; then
  ibkr_shared_gateway_port="$extra_arg_gateway_port_override"
fi
if [[ -n "$extra_arg_gateway_connect_timeout_override" ]]; then
  ibkr_shared_gateway_connect_timeout_ms="$extra_arg_gateway_connect_timeout_override"
fi
if [[ -n "$extra_arg_gateway_ack_timeout_override" ]]; then
  ibkr_shared_gateway_ack_timeout_ms="$extra_arg_gateway_ack_timeout_override"
fi
if [[ -n "$extra_arg_gateway_skip_direct_override" ]]; then
  if is_truthy "$extra_arg_gateway_skip_direct_override"; then
    ibkr_shared_gateway_skip_direct_connection=1
  else
    ibkr_shared_gateway_skip_direct_connection=0
  fi
fi
if [[ -n "$extra_arg_shared_feed_expected_client_count_override" ]]; then
  databento_shared_feed_expected_client_count="$extra_arg_shared_feed_expected_client_count_override"
fi
if [[ -n "$extra_arg_shared_feed_client_wait_timeout_override" ]]; then
  databento_shared_feed_client_wait_timeout_ms="$extra_arg_shared_feed_client_wait_timeout_override"
fi

case "$ibkr_shared_gateway_port" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] Effective trading.ibkr.shared-gateway.port must be a whole number." >&2
    exit 1
    ;;
esac

case "$ibkr_shared_gateway_connect_timeout_ms" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] Effective trading.ibkr.shared-gateway.connect-timeout-ms must be a whole number." >&2
    exit 1
    ;;
esac

case "$ibkr_shared_gateway_ack_timeout_ms" in
  ''|*[!0-9]* )
    echo "[BULK-LAUNCH][ERROR] Effective trading.ibkr.shared-gateway.ack-timeout-ms must be a whole number." >&2
    exit 1
    ;;
esac

mkdir -p \
  "$launch_dir" \
  "$repo_root/runtime/databento/logs" \
  "$repo_root/runtime/databento/output" \
  "$repo_root/runtime/databento/state"

all_symbols=()
while IFS= read -r symbol; do
  if [[ -n "$symbol" ]]; then
    all_symbols+=("$symbol")
  fi
done < <(discover_symbols | LC_ALL=C sort -u)

if [[ ${#all_symbols[@]} -eq 0 ]]; then
  echo "[BULK-LAUNCH][ERROR] No generated bot configs found in $bots_dir" >&2
  exit 1
fi

include_symbols=()
if [[ -n "$symbols_csv" ]]; then
  while IFS= read -r symbol; do
    if [[ -n "$symbol" ]]; then
      include_symbols+=("$symbol")
    fi
  done < <(parse_csv_symbols "$symbols_csv")
fi

exclude_symbols=()
if [[ -n "$exclude_csv" ]]; then
  while IFS= read -r symbol; do
    if [[ -n "$symbol" ]]; then
      exclude_symbols+=("$symbol")
    fi
  done < <(parse_csv_symbols "$exclude_csv")
fi

selected_symbols=()
if [[ ${#include_symbols[@]} -gt 0 ]]; then
  for symbol in "${include_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${all_symbols[@]}"; then
      echo "[BULK-LAUNCH][ERROR] Requested symbol not found in generated bot configs: $symbol" >&2
      exit 1
    fi
    if ! contains_symbol "$symbol" "${selected_symbols[@]-}"; then
      selected_symbols+=("$symbol")
    fi
  done
else
  selected_symbols=("${all_symbols[@]}")
fi

if [[ ${#exclude_symbols[@]} -gt 0 ]]; then
  filtered_symbols=()
  for symbol in "${selected_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${exclude_symbols[@]}"; then
      filtered_symbols+=("$symbol")
    fi
  done
  selected_symbols=("${filtered_symbols[@]}")
fi

if [[ ${#selected_symbols[@]} -eq 0 ]]; then
  echo "[BULK-LAUNCH][ERROR] No symbols remain after applying --symbols/--exclude filters." >&2
  exit 1
fi


direct_ibkr_symbols=()
shared_gateway_only_symbols=()
for symbol in "${selected_symbols[@]}"; do
  if symbol_uses_direct_ibkr_connection "$symbol"; then
    direct_ibkr_symbols+=("$symbol")
  else
    shared_gateway_only_symbols+=("$symbol")
  fi
done

log "discovered ${#all_symbols[@]} bot configs; selected ${#selected_symbols[@]} symbols"
printf '[BULK-LAUNCH] symbols=%s\n' "$(printf '%s,' "${selected_symbols[@]}" | sed 's/,$//')"
printf '[BULK-LAUNCH] mode=%s tee=%s tee_db=%s max_trades=%s trade_amount=%s max_order_notional=%s max_share_cap=%s stagger_seconds=%s\n' \
  "$([[ $start_mode -eq 1 ]] && printf 'START' || printf 'PREVIEW')" \
  "$tee_mode" \
  "$tee_db_mode" \
  "${max_trades_override:-<default>}" \
  "${trade_amount_override:-<properties>}" \
  "${max_notional_override:-<properties>}" \
  "${max_share_cap_override:-<properties/default>}" \
  "$stagger_seconds"
printf '[BULK-LAUNCH] ibkr_client_cap=%s allow_over_cap=%s\n' "$ibkr_client_cap" "$allow_over_ibkr_client_cap"
printf '[BULK-LAUNCH] ibkr_shared_gateway enabled=%s host=%s port=%s skip_direct_connection=%s connect_timeout_ms=%s ack_timeout_ms=%s\n' \
  "$ibkr_shared_gateway_enabled" \
  "$ibkr_shared_gateway_host" \
  "$ibkr_shared_gateway_port" \
  "$ibkr_shared_gateway_skip_direct_connection" \
  "$ibkr_shared_gateway_connect_timeout_ms" \
  "$ibkr_shared_gateway_ack_timeout_ms"
printf '[BULK-LAUNCH] direct_ibkr_symbols=%s shared_gateway_only_symbols=%s\n' "${#direct_ibkr_symbols[@]}" "${#shared_gateway_only_symbols[@]}"

if [[ $list_mode -eq 1 ]]; then
  exit 0
fi

if [[ $local_data_preflight_enabled -eq 1 ]]; then
  if [[ ! -x "$local_startup_data_verifier_script" ]]; then
    echo "[BULK-LAUNCH][ERROR] Missing or non-executable local startup data verifier: $local_startup_data_verifier_script" >&2
    exit 1
  fi
  selected_symbols_csv="$(printf '%s,' "${selected_symbols[@]}" | sed 's/,$//')"
  local_data_verifier_args=("--symbols=$selected_symbols_csv")
  if [[ $tee_mode -eq 1 ]]; then
    local_data_verifier_args+=(--tee)
  fi
  if [[ ${#extra_args[@]} -gt 0 ]]; then
    local_data_verifier_args+=(-- "${extra_args[@]}")
  fi
  log "verifying local startup data for selected symbols"
  "$local_startup_data_verifier_script" "${local_data_verifier_args[@]}"
else
  log "skipping local startup data preflight by request"
fi

ensure_bulk_shared_prereqs

should_run_ensure_ibkr=0
case "$ensure_ibkr_mode" in
  yes)
    should_run_ensure_ibkr=1
    ;;
  auto)
    if [[ $start_mode -eq 1 && ${#direct_ibkr_symbols[@]} -gt 0 ]]; then
      should_run_ensure_ibkr=1
    fi
    ;;
esac

if [[ $start_mode -eq 1 && $ibkr_shared_gateway_enabled -eq 1 && $ibkr_shared_gateway_skip_direct_connection -eq 1 ]]; then
  if ! shared_gateway_protocol_healthy; then
    if [[ ! -x "$start_shared_ibkr_gateway_script" ]]; then
      echo "[BULK-LAUNCH][ERROR] Shared IBKR gateway is required, but the companion start script is missing or not executable: $start_shared_ibkr_gateway_script" >&2
      exit 1
    fi
    log "shared IBKR gateway not protocol-healthy; starting companion gateway server"
    "$start_shared_ibkr_gateway_script" \
      --host="$ibkr_shared_gateway_host" \
      --port="$ibkr_shared_gateway_port"
  fi
  if ! shared_gateway_protocol_healthy; then
    echo "[BULK-LAUNCH][ERROR] Shared IBKR gateway is enabled with skip-direct-connection=true, but the gateway protocol health probe failed at ${ibkr_shared_gateway_host}:${ibkr_shared_gateway_port}." >&2
    echo "[BULK-LAUNCH][ERROR] Check runtime/databento/logs/shared-ibkr-gateway.log, or re-run with --ibkr-shared-gateway-allow-direct-fallback / --no-ibkr-shared-gateway." >&2
    exit 1
  fi
  log "verified shared IBKR gateway protocol health ${ibkr_shared_gateway_host}:${ibkr_shared_gateway_port}"
fi

if [[ $should_run_ensure_ibkr -eq 1 ]]; then
  if [[ ! -x "$ensure_ibkr_script" ]]; then
    echo "[BULK-LAUNCH][ERROR] Missing or non-executable IBKR preflight script: $ensure_ibkr_script" >&2
    exit 1
  fi
  log "running IBKR preflight before bulk launch"
  "$ensure_ibkr_script"
fi

run_preview_for_symbol() {
  local symbol="$1"
  local log_file="$2"
  local cmd_text
  cmd_text="$(build_symbol_cmd "$symbol" preview)"
  log "preview $symbol -> $cmd_text"
  bash -lc "$cmd_text" >"$log_file" 2>&1
}

launch_symbol() {
  local symbol="$1"
  local log_file="$2"
  local cmd_text
  cmd_text="$(build_symbol_cmd "$symbol" start)"
  log "launching $symbol -> $cmd_text" >&2
  nohup bash -lc "$cmd_text" >>"$log_file" 2>&1 &
  printf '%s' "$!"
}

if [[ $start_mode -eq 0 ]]; then
  preview_failures=0
  for symbol in "${selected_symbols[@]}"; do
    preview_log="$launch_dir/$(lower_symbol "$symbol")-preview-$(date '+%Y%m%d_%H%M%S').log"
    printf '\n========== PREVIEW %s ==========' "$symbol"
    printf '\n'
    if run_preview_for_symbol "$symbol" "$preview_log"; then
      cat "$preview_log"
    else
      preview_failures=$((preview_failures + 1))
      cat "$preview_log"
      printf '[BULK-LAUNCH][ERROR] Preview failed for %s (log: %s)\n' "$symbol" "$preview_log" >&2
    fi
  done
  if [[ $preview_failures -gt 0 ]]; then
    printf '[BULK-LAUNCH][ERROR] %s preview(s) failed. Fix those before running with --start.\n' "$preview_failures" >&2
    exit 1
  fi
  printf '[BULK-LAUNCH] Preview complete for %s symbol(s). Re-run with --start to launch.\n' "${#selected_symbols[@]}"
  exit 0
fi

if [[ $allow_over_ibkr_client_cap -eq 0 && ${#direct_ibkr_symbols[@]} -gt $ibkr_client_cap ]]; then
  printf '[BULK-LAUNCH][ERROR] Refusing to start %s direct-IBKR bots: direct connection count exceeds the configured IBKR client cap of %s.\n' "${#direct_ibkr_symbols[@]}" "$ibkr_client_cap" >&2
  printf '[BULK-LAUNCH][ERROR] Too many direct IBKR bots will often appear port_open/listening but remain connected=false and health_ok=false once IBKR client capacity is exhausted.\n' >&2
  printf '[BULK-LAUNCH][ERROR] Shared-gateway-only bots do not count toward this limit; current shared_gateway_only_symbols=%s.\n' "${#shared_gateway_only_symbols[@]}" >&2
  printf '[BULK-LAUNCH][ERROR] Re-run with a smaller --symbols set, switch more bots to shared IBKR gateway mode, set --ibkr-client-cap to your actual TWS/API limit, or explicitly bypass with --allow-over-ibkr-client-cap.\n' >&2
  exit 1
fi

preflight_failures=0
preflight_logs=()
for symbol in "${selected_symbols[@]}"; do
  preflight_log="$launch_dir/$(lower_symbol "$symbol")-preflight-$(date '+%Y%m%d_%H%M%S').log"
  preflight_logs+=("$preflight_log")
  if run_preview_for_symbol "$symbol" "$preflight_log"; then
    log "preflight passed for $symbol"
  else
    preflight_failures=$((preflight_failures + 1))
    log "preflight FAILED for $symbol (log: $preflight_log)"
  fi
done

if [[ $preflight_failures -gt 0 ]]; then
  printf '[BULK-LAUNCH][ERROR] %s symbol preflight(s) failed. No bots were started.\n' "$preflight_failures" >&2
  for preflight_log in "${preflight_logs[@]}"; do
    if [[ -f "$preflight_log" ]]; then
      printf '[BULK-LAUNCH] preflight-log=%s\n' "$preflight_log"
    fi
  done
  exit 1
fi

launch_pids=()
launch_logs=()
for index in "${!selected_symbols[@]}"; do
  symbol="${selected_symbols[$index]}"
  launch_log="$launch_dir/$(lower_symbol "$symbol")-launch-$(date '+%Y%m%d_%H%M%S').log"
  launch_logs+=("$launch_log")
  pid="$(launch_symbol "$symbol" "$launch_log")"
  if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    printf '[BULK-LAUNCH][ERROR] Failed to capture numeric PID for %s; got %q (log: %s)\n' "$symbol" "$pid" "$launch_log" >&2
    exit 1
  fi
  launch_pids+=("$pid")
  printf '[BULK-LAUNCH] started symbol=%s pid=%s log=%s\n' "$symbol" "$pid" "$launch_log"
  if [[ "$stagger_seconds" != "0" && "$index" -lt $((${#selected_symbols[@]} - 1)) ]]; then
    sleep "$stagger_seconds"
  fi
done

printf '[BULK-LAUNCH] Launch requests dispatched for %s symbol(s).\n' "${#selected_symbols[@]}"
printf '[BULK-LAUNCH] Next checks:\n'
printf '  - tail -f %q\n' "${launch_logs[0]}"
printf '  - curl -fsS http://127.0.0.1:<PORT>/actuator/health\n'
printf '  - curl -fsS http://127.0.0.1:<PORT>/api/control/status\n'


