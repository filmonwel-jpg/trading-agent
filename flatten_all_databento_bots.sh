#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
bots_dir="$repo_root/runtime/databento/bots"
legacy_bots_dir="$repo_root/runtime"
mkdir -p "$runtime_dir"

export PATH="/Users/filmonghezehey/miniforge3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

HTTP_CONNECT_TIMEOUT_SECONDS="${HTTP_CONNECT_TIMEOUT_SECONDS:-2}"
HTTP_MAX_TIME_SECONDS="${HTTP_MAX_TIME_SECONDS:-8}"
HTTP_RETRY_COUNT="${HTTP_RETRY_COUNT:-2}"
POLL_INTERVAL_SECONDS=5
WAIT_TIMEOUT_SECONDS=180
MAX_STUCK_POLLS=3
symbols_csv=""
exclude_csv=""
dry_run=0
list_only=0
skip_pause=0
skip_cancel=0
no_wait=0
force_unreachable_success=0
continue_on_failure=0
POST_RESPONSE=""

usage() {
  cat <<'EOF'
Usage:
  ./flatten_all_databento_bots.sh [options]

Options:
  --symbols=CSV              Only flatten these symbols, e.g. --symbols=AAPL,MSFT,NVDA
  --exclude=CSV              Exclude these symbols
  --list                     List discovered/selected targets and exit
  --dry-run                  Print what would be called without sending requests
  --skip-pause               Do not call /api/control/pause before flattening
  --skip-cancel              Do not call /api/control/cancel-open-orders before flattening
  --no-wait                  Trigger requests but do not poll for flat confirmation
  --wait-timeout-seconds=N   Max seconds to wait per symbol for position=0 and openOrders=0 (default: 180)
  --poll-interval-seconds=N  Poll interval while waiting (default: 5)
  --max-stuck-polls=N        Fail if position/openOrders show no broker progress this many polls (default: 3)
  --continue-on-failure      Continue to later symbols after an unsafe/stuck symbol (default: stop on first failure)
  --unreachable-ok           Do not return failure for unreachable selected bot endpoints
  -h, --help                 Show this help

Behavior:
  - Discovers generated Databento bot configs from runtime/databento/bots/trading-*.properties.
  - Falls back to runtime/trading-*.properties if generated configs are absent.
  - For each selected reachable bot endpoint, calls:
      POST /api/control/pause
      POST /api/control/cancel-open-orders
      POST /api/control/flatten
    then polls GET /api/control/status until currentPosition=0 and openOrders=0.
  - This script does NOT stop bot JVMs, harvesters, shared feed relays, IBKR Gateway, or TWS.

Examples:
  ./flatten_all_databento_bots.sh --list
  ./flatten_all_databento_bots.sh --dry-run --symbols=AAPL,MSFT
  ./flatten_all_databento_bots.sh
EOF
}

log() {
  printf '[FLATTEN-ALL][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

trim_spaces() {
  printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
}

normalize_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
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

csv_contains_symbol() {
  local csv="$1"
  local wanted item old_ifs
  wanted="$(normalize_symbol "$2")"
  [[ -n "$csv" && -n "$wanted" ]] || return 1
  old_ifs="$IFS"
  IFS=','
  for item in $csv; do
    item="$(normalize_symbol "$item")"
    if [[ "$item" == "$wanted" ]]; then
      IFS="$old_ifs"
      return 0
    fi
  done
  IFS="$old_ifs"
  return 1
}

symbol_selected() {
  local symbol
  symbol="$(normalize_symbol "$1")"
  if [[ -n "$symbols_csv" ]]; then
    if ! csv_contains_symbol "$symbols_csv" "$symbol"; then
      return 1
    fi
  fi
  if [[ -n "$exclude_csv" ]]; then
    if csv_contains_symbol "$exclude_csv" "$symbol"; then
      return 1
    fi
  fi
  return 0
}

discover_bot_targets() {
  local path filename symbol port found_any=0
  shopt -s nullglob
  for path in "$bots_dir"/trading-*.properties; do
    filename="$(basename "$path")"
    [[ "$filename" == "trading-databento-template.properties" ]] && continue
    found_any=1
    symbol="$(extract_prop "$path" "trading.symbol")"
    if [[ -z "$symbol" ]]; then
      symbol="${filename#trading-}"
      symbol="${symbol%.properties}"
    fi
    port="$(extract_prop "$path" "server.port")"
    symbol="$(normalize_symbol "$symbol")"
    port="$(trim_spaces "$port")"
    [[ -n "$symbol" && -n "$port" ]] && printf '%s|%s|%s\n' "$symbol" "$port" "$path"
  done
  if [[ $found_any -eq 0 ]]; then
    for path in "$legacy_bots_dir"/trading-*.properties; do
      filename="$(basename "$path")"
      [[ "$filename" == "trading-databento-template.properties" ]] && continue
      symbol="$(extract_prop "$path" "trading.symbol")"
      if [[ -z "$symbol" ]]; then
        symbol="${filename#trading-}"
        symbol="${symbol%.properties}"
      fi
      port="$(extract_prop "$path" "server.port")"
      symbol="$(normalize_symbol "$symbol")"
      port="$(trim_spaces "$port")"
      [[ -n "$symbol" && -n "$port" ]] && printf '%s|%s|%s\n' "$symbol" "$port" "$path"
    done
  fi
  shopt -u nullglob
}

control_url() {
  local port="$1"
  local endpoint="$2"
  printf 'http://127.0.0.1:%s%s' "$port" "$endpoint"
}

http_get() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    "$url"
}

http_post() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    -X POST \
    "$url"
}

json_field_from_stdin() {
  local field="$1"
  python3 -c '
import json, sys
field = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("")
    raise SystemExit(0)
cur = data
for part in field.split("."):
    if isinstance(cur, dict) and part in cur:
        cur = cur[part]
    else:
        print("")
        raise SystemExit(0)
print(cur if cur is not None else "")
' "$field"
}

json_field_from_text() {
  local text="$1"
  local field="$2"
  printf '%s' "$text" | json_field_from_stdin "$field"
}

get_status_json() {
  local port="$1"
  http_get "$(control_url "$port" '/api/control/status')"
}

status_field() {
  local port="$1"
  local field="$2"
  get_status_json "$port" 2>/dev/null | json_field_from_stdin "$field"
}

post_control() {
  local symbol="$1"
  local port="$2"
  local endpoint="$3"
  local url attempt response message
  url="$(control_url "$port" "$endpoint")"
  POST_RESPONSE=""
  for attempt in $(seq 1 "$HTTP_RETRY_COUNT"); do
    if response="$(http_post "$url" 2>&1)"; then
      POST_RESPONSE="$response"
      message="$(json_field_from_text "$response" message)"
      if [[ -n "$message" ]]; then
        log "$symbol $endpoint request succeeded attempt=$attempt message=$message"
      else
        log "$symbol $endpoint request succeeded attempt=$attempt"
      fi
      return 0
    fi
    log "$symbol $endpoint request failed attempt=$attempt response=${response:-empty}"
    sleep 1
  done
  return 1
}

is_port_listening() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 0
}

is_flat_status() {
  local position="$1"
  local open_orders="$2"
  [[ "${position:-x}" == "0" && "${open_orders:-x}" == "0" ]]
}

is_integer() {
  case "${1:-}" in
    -[0-9]*|[0-9]*) return 0 ;;
    *) return 1 ;;
  esac
}

abs_int() {
  local value="$1"
  if [[ "$value" == -* ]]; then
    printf '%s' "${value#-}"
  else
    printf '%s' "$value"
  fi
}

execution_transport_available() {
  local status="$1"
  local connected shared_gateway_connected
  connected="$(json_field_from_text "$status" connected)"
  shared_gateway_connected="$(json_field_from_text "$status" ibkrSharedGatewayConnected)"
  [[ "$connected" == "True" || "$connected" == "true" || "$shared_gateway_connected" == "True" || "$shared_gateway_connected" == "true" ]]
}

log_status_summary() {
  local symbol="$1"
  local status="$2"
  local connected shared_gateway_connected position_sync_state order_in_flight last_action last_qty
  connected="$(json_field_from_text "$status" connected)"
  shared_gateway_connected="$(json_field_from_text "$status" ibkrSharedGatewayConnected)"
  position_sync_state="$(json_field_from_text "$status" positionSyncState)"
  order_in_flight="$(json_field_from_text "$status" orderInFlight)"
  last_action="$(json_field_from_text "$status" lastPlacedOrderAction)"
  last_qty="$(json_field_from_text "$status" lastPlacedOrderQuantity)"
  log "$symbol execution status connected=${connected:-unknown} sharedGatewayConnected=${shared_gateway_connected:-unknown} positionSyncState=${position_sync_state:-unknown} orderInFlight=${order_in_flight:-unknown} lastOrder=${last_action:-unknown}/${last_qty:-unknown}"
}

flatten_symbol() {
  local symbol="$1"
  local port="$2"
  local properties_file="$3"
  local status current_position open_orders deadline now confirmed=0 flatten_message
  local initial_position initial_open_orders stuck_polls=0 current_abs initial_abs

  log "$symbol target port=$port properties=$properties_file"

  if [[ $dry_run -eq 1 ]]; then
    if [[ $skip_pause -eq 0 ]]; then log "dry-run: would POST $(control_url "$port" '/api/control/pause')"; fi
    if [[ $skip_cancel -eq 0 ]]; then log "dry-run: would POST $(control_url "$port" '/api/control/cancel-open-orders')"; fi
    log "dry-run: would POST $(control_url "$port" '/api/control/flatten')"
    return 0
  fi

  if ! is_port_listening "$port"; then
    log "$symbol port $port is not listening"
    [[ $force_unreachable_success -eq 1 ]] && return 0 || return 1
  fi

  if ! status="$(get_status_json "$port" 2>/dev/null)"; then
    log "$symbol control endpoint unreachable on port $port"
    [[ $force_unreachable_success -eq 1 ]] && return 0 || return 1
  fi

  current_position="$(printf '%s' "$status" | json_field_from_stdin currentPosition)"
  open_orders="$(printf '%s' "$status" | json_field_from_stdin openOrders)"
  initial_position="$current_position"
  initial_open_orders="$open_orders"
  log "$symbol pre-flatten status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"

  if is_flat_status "$current_position" "$open_orders"; then
    log "$symbol already flat with no open orders; skipping control requests"
    return 0
  fi

  if ! execution_transport_available "$status"; then
    log_status_summary "$symbol" "$status"
    log "$symbol cannot flatten safely: no IBKR execution transport is connected while position/openOrders are non-zero"
    return 1
  fi

  if [[ $skip_pause -eq 0 ]]; then
    post_control "$symbol" "$port" "/api/control/pause" || log "$symbol pause request failed; continuing to cancel/flatten"
  fi

  if [[ $skip_cancel -eq 0 ]]; then
    post_control "$symbol" "$port" "/api/control/cancel-open-orders" || log "$symbol cancel-open-orders request failed; continuing to flatten"
  fi

  if ! post_control "$symbol" "$port" "/api/control/flatten"; then
    log "$symbol flatten request failed"
    return 1
  fi
  flatten_message="$(json_field_from_text "$POST_RESPONSE" message)"
  if [[ "$flatten_message" == flatten-failed-* || "$flatten_message" == flatten-rejected-* || "$flatten_message" == "strategy-not-ready" || "$flatten_message" == "flatten-failed-no-price" ]]; then
    log "$symbol flatten endpoint returned non-success message=$flatten_message"
    return 1
  fi

  if [[ $no_wait -eq 1 ]]; then
    log "$symbol flatten triggered; not waiting because --no-wait was supplied"
    return 0
  fi

  deadline=$(($(date '+%s') + WAIT_TIMEOUT_SECONDS))
  while true; do
    if ! status="$(get_status_json "$port" 2>/dev/null)"; then
      log "$symbol poll status unavailable; control endpoint stopped responding"
      return 1
    fi
    current_position="$(json_field_from_text "$status" currentPosition)"
    open_orders="$(json_field_from_text "$status" openOrders)"
    log "$symbol poll status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"
    if is_flat_status "$current_position" "$open_orders"; then
      confirmed=1
      break
    fi

    if ! execution_transport_available "$status"; then
      log_status_summary "$symbol" "$status"
      log "$symbol lost IBKR execution transport before flat confirmation"
      return 1
    fi

    if is_integer "$current_position" && is_integer "$open_orders" && is_integer "$initial_position" && is_integer "$initial_open_orders"; then
      current_abs="$(abs_int "$current_position")"
      initial_abs="$(abs_int "$initial_position")"
      if [[ "$current_abs" -ge "$initial_abs" && "$open_orders" -ge "$initial_open_orders" ]]; then
        stuck_polls=$((stuck_polls + 1))
      else
        stuck_polls=0
      fi
      if [[ "$stuck_polls" -ge "$MAX_STUCK_POLLS" ]]; then
        log_status_summary "$symbol" "$status"
        log "$symbol no broker progress after $stuck_polls poll(s): initialPosition=$initial_position initialOpenOrders=$initial_open_orders currentPosition=$current_position currentOpenOrders=$open_orders"
        return 1
      fi
    fi

    now="$(date '+%s')"
    if [[ "$now" -ge "$deadline" ]]; then
      break
    fi
    sleep "$POLL_INTERVAL_SECONDS"
  done

  if [[ $confirmed -eq 1 ]]; then
    log "$symbol flat confirmed"
    return 0
  fi

  log "$symbol did not confirm flat within ${WAIT_TIMEOUT_SECONDS}s; check bot/gateway logs and broker state"
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbols=*) symbols_csv="${1#*=}" ;;
    --exclude=*) exclude_csv="${1#*=}" ;;
    --list) list_only=1 ;;
    --dry-run) dry_run=1 ;;
    --skip-pause) skip_pause=1 ;;
    --skip-cancel) skip_cancel=1 ;;
    --no-wait) no_wait=1 ;;
    --wait-timeout-seconds=*) WAIT_TIMEOUT_SECONDS="${1#*=}" ;;
    --poll-interval-seconds=*) POLL_INTERVAL_SECONDS="${1#*=}" ;;
    --max-stuck-polls=*) MAX_STUCK_POLLS="${1#*=}" ;;
    --continue-on-failure) continue_on_failure=1 ;;
    --unreachable-ok) force_unreachable_success=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

case "$WAIT_TIMEOUT_SECONDS" in ''|*[!0-9]*) echo "--wait-timeout-seconds must be a non-negative integer" >&2; exit 2 ;; esac
case "$POLL_INTERVAL_SECONDS" in ''|*[!0-9]*) echo "--poll-interval-seconds must be a positive integer" >&2; exit 2 ;; esac
case "$MAX_STUCK_POLLS" in ''|*[!0-9]*) echo "--max-stuck-polls must be a positive integer" >&2; exit 2 ;; esac
if [[ "$POLL_INTERVAL_SECONDS" -le 0 ]]; then
  echo "--poll-interval-seconds must be greater than zero" >&2
  exit 2
fi
if [[ "$MAX_STUCK_POLLS" -le 0 ]]; then
  echo "--max-stuck-polls must be greater than zero" >&2
  exit 2
fi

run_stamp="$(date '+%Y%m%d_%H%M%S')"
log_file="$runtime_dir/manual-flatten-all-$run_stamp.log"
exec > >(tee -a "$log_file") 2>&1

log "fleet flatten script started repo_root=$repo_root log=$log_file"

selected_count=0
failure_count=0
failed_symbols=""
while IFS='|' read -r symbol port properties_file; do
  [[ -n "$symbol" && -n "$port" ]] || continue
  if ! symbol_selected "$symbol"; then
    continue
  fi
  selected_count=$((selected_count + 1))
  if [[ $list_only -eq 1 ]]; then
    printf '%s|%s|%s\n' "$symbol" "$port" "$properties_file"
    continue
  fi
  if ! flatten_symbol "$symbol" "$port" "$properties_file"; then
    failure_count=$((failure_count + 1))
    failed_symbols="${failed_symbols}${failed_symbols:+,}$symbol"
    if [[ $continue_on_failure -eq 0 ]]; then
      log "$symbol failed; stopping fleet flatten because --continue-on-failure was not supplied"
      break
    fi
  fi
done < <(discover_bot_targets | LC_ALL=C sort -t'|' -k1,1)

if [[ $selected_count -eq 0 ]]; then
  log "no selected bot configs found under $bots_dir or $legacy_bots_dir"
  exit 1
fi

if [[ $list_only -eq 1 ]]; then
  log "listed $selected_count selected target(s)"
  exit 0
fi

if [[ $failure_count -eq 0 ]]; then
  log "fleet flatten completed successfully selected=$selected_count failures=0"
  exit 0
fi

log "fleet flatten completed with failures selected=$selected_count failures=$failure_count failedSymbols=${failed_symbols:-none}"
exit 1


