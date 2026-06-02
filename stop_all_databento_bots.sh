#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./stop_all_databento_bots.sh [--symbols=CSV] [--exclude=CSV] [--dry-run] [--list] [--skip-control] [--flatten-first] [--allow-unsafe-stop] [--copy-live-logs-on-stop] [--force-after-seconds=N]

Behavior:
  - Discovers Databento bot configs from runtime/databento/bots/trading-*.properties.
  - Stops the selected bots by first attempting /api/control pause + kill-switch when reachable.
  - This script is not a flatten workflow unless --flatten-first is passed.
  - Without --flatten-first, reachable bots reporting non-zero position/open orders are refused unless --allow-unsafe-stop is explicit.
  - Then sends SIGTERM to the bot JVM PID(s) discovered from the configured server.port and process command line.
  - If a bot is still alive after the grace period, the script sends SIGKILL.
  - When --copy-live-logs-on-stop is passed, the script snapshots each selected bot's current app/trade/live-log files before stopping it.
  - Default selection is all discovered bot configs.
  - When all discovered symbols are selected, the script also stops the shared Databento relay and the worktree-owned shared IBKR gateway server.

Examples:
  ./stop_all_databento_bots.sh --dry-run
  ./stop_all_databento_bots.sh --list
  ./stop_all_databento_bots.sh --symbols=AAPL,NVDA
  ./stop_all_databento_bots.sh --exclude=SPY,QQQ
  ./stop_all_databento_bots.sh --flatten-first
  ./stop_all_databento_bots.sh --symbols=AAPL --force-after-seconds=3
  ./stop_all_databento_bots.sh --skip-control
  ./stop_all_databento_bots.sh --symbols=TSLA --copy-live-logs-on-stop
EOF
}

repo_root="$(cd "$(dirname "$0")" && pwd)"
bots_dir="$repo_root/runtime/databento/bots"
legacy_bots_dir="$repo_root/runtime"
snapshot_base_dir="$repo_root/runtime/databento/snapshots"
shared_relay_pid_file="$repo_root/runtime/databento/shared-feed-relay.pid"
shared_relay_process_pattern='databento_shared_feed_relay\.py'
stop_shared_ibkr_gateway_script="$repo_root/stop_shared_ibkr_gateway.sh"

HTTP_CONNECT_TIMEOUT_SECONDS=2
HTTP_MAX_TIME_SECONDS=5
HTTP_RETRY_COUNT=2

symbols_csv=""
exclude_csv=""
dry_run=0
list_mode=0
skip_control=0
flatten_first=0
allow_unsafe_stop=0
copy_live_logs_on_stop=0
force_after_seconds=10
snapshot_root_dir=""
snapshot_manifest_file=""
snapshot_date="$(date '+%Y-%m-%d')"
snapshot_stamp="$(date '+%Y%m%dT%H%M%S')"

normalize_symbol() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
}

lower_symbol() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

trim_spaces() {
  printf '%s' "$1" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); print}'
}

log() {
  printf '[BULK-STOP][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
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

resolve_repo_path() {
  local raw_path="$1"
  raw_path="$(trim_spaces "$raw_path")"
  if [[ -z "$raw_path" ]]; then
    return 0
  fi
  if [[ "$raw_path" = /* ]]; then
    printf '%s' "$raw_path"
  else
    printf '%s' "$repo_root/$raw_path"
  fi
}

ensure_snapshot_root() {
  if [[ -n "$snapshot_root_dir" ]]; then
    return 0
  fi
  snapshot_root_dir="$snapshot_base_dir/${snapshot_date}_pre-stop_${snapshot_stamp}"
  snapshot_manifest_file="$snapshot_root_dir/manifest.txt"
  mkdir -p "$snapshot_root_dir/app-logs" "$snapshot_root_dir/trade-output" "$snapshot_root_dir/live-trade-logs"
  : > "$snapshot_manifest_file"
  log "snapshot root created path=$snapshot_root_dir"
}

snapshot_copy_file() {
  local symbol="$1"
  local category="$2"
  local source_path="$3"
  local target_subdir="$4"
  local reason="$5"
  local source_name target_dir target_path
  [[ -n "$source_path" ]] || return 0
  if [[ ! -f "$source_path" ]]; then
    return 1
  fi
  ensure_snapshot_root
  target_dir="$snapshot_root_dir/$target_subdir"
  source_name="$(basename "$source_path")"
  target_path="$target_dir/$source_name"
  cp -p "$source_path" "$target_path"
  printf '%s|%s|%s|%s
' "$symbol" "$category" "$reason" "$target_path" >> "$snapshot_manifest_file"
}

snapshot_symbol_logs() {
  local symbol="$1"
  local properties_file="$2"
  local symbol_lower="$3"
  local app_log_prop trade_log_prop app_log_path trade_log_path live_trade_log_path dated_live_trade_log_path today_app_glob matched rolled_path
  local copied_count=0
  local missing_count=0

  app_log_prop="$(extract_prop "$properties_file" "logging.file.name")"
  trade_log_prop="$(extract_prop "$properties_file" "trading.log.file")"
  app_log_path="$(resolve_repo_path "$app_log_prop")"
  trade_log_path="$(resolve_repo_path "$trade_log_prop")"

  if [[ -n "$app_log_path" ]]; then
    if snapshot_copy_file "$symbol" "app-log" "$app_log_path" "app-logs" "current"; then
      copied_count=$((copied_count + 1))
    else
      missing_count=$((missing_count + 1))
      log "$symbol snapshot missing current app log path=$app_log_path"
    fi
    today_app_glob="${app_log_path}.${snapshot_date}"*.gz
    matched=0
    shopt -s nullglob
    for rolled_path in $today_app_glob; do
      matched=1
      if snapshot_copy_file "$symbol" "app-log" "$rolled_path" "app-logs" "rolled-${snapshot_date}"; then
        copied_count=$((copied_count + 1))
      fi
    done
    shopt -u nullglob
    if [[ $matched -eq 0 ]]; then
      log "$symbol snapshot no rolled app logs found for date=$snapshot_date"
    fi
  fi

  if [[ -n "$trade_log_path" ]]; then
    if snapshot_copy_file "$symbol" "trade-output" "$trade_log_path" "trade-output" "current"; then
      copied_count=$((copied_count + 1))
    else
      missing_count=$((missing_count + 1))
      log "$symbol snapshot missing trade output path=$trade_log_path"
    fi
  fi

  live_trade_log_path="$repo_root/runtime/${symbol_lower}_live_trade_logs.txt"
  if [[ ! -f "$live_trade_log_path" ]]; then
    live_trade_log_path="$repo_root/runtime/${symbol}_live_trade_logs.txt"
  fi
  if snapshot_copy_file "$symbol" "live-trade-log" "$live_trade_log_path" "live-trade-logs" "current"; then
    copied_count=$((copied_count + 1))
    dated_live_trade_log_path="${live_trade_log_path%.*}_${snapshot_date}.${live_trade_log_path##*.}"
    if [[ -f "$dated_live_trade_log_path" ]]; then
      if snapshot_copy_file "$symbol" "live-trade-log" "$dated_live_trade_log_path" "live-trade-logs" "dated-${snapshot_date}"; then
        copied_count=$((copied_count + 1))
      fi
    fi
  else
    missing_count=$((missing_count + 1))
    log "$symbol snapshot missing live trade log path=$live_trade_log_path"
  fi

  log "$symbol snapshot copied=$copied_count missing=$missing_count"
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

discover_property_files() {
  local path
  local found_any=0
  shopt -s nullglob
  for path in "$bots_dir"/trading-*.properties; do
    if [[ "$(basename "$path")" == "trading-databento-template.properties" ]]; then
      continue
    fi
    found_any=1
    printf '%s\n' "$path"
  done
  if [[ $found_any -eq 0 ]]; then
    for path in "$legacy_bots_dir"/trading-*.properties; do
      if [[ "$(basename "$path")" == "trading-databento-template.properties" ]]; then
        continue
      fi
      printf '%s\n' "$path"
    done
  fi
  shopt -u nullglob
}

resolve_properties_file() {
  local symbol_lower="$1"
  local candidates=(
    "$bots_dir/trading-${symbol_lower}.properties"
    "$legacy_bots_dir/trading-${symbol_lower}.properties"
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

discover_symbols() {
  local path symbol
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    symbol="$(extract_prop "$path" "trading.symbol")"
    if [[ -z "$symbol" ]]; then
      symbol="$(basename "$path")"
      symbol="${symbol#trading-}"
      symbol="${symbol%.properties}"
    fi
    symbol="$(normalize_symbol "$symbol")"
    if [[ -n "$symbol" ]]; then
      printf '%s\n' "$symbol"
    fi
  done < <(discover_property_files)
}

control_url() {
  local port="$1"
  local endpoint="$2"
  printf 'http://127.0.0.1:%s%s' "$port" "$endpoint"
}

http_post() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    -X POST \
    "$url"
}

http_get() {
  local url="$1"
  curl --silent --show-error --fail \
    --connect-timeout "$HTTP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    "$url"
}

json_field_from_stdin() {
  local field="$1"
  python3 -c '
import json
import sys

field = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("")
    raise SystemExit(0)

if not isinstance(data, dict):
    print("")
    raise SystemExit(0)

fallback = data.get("position") if field == "currentPosition" else ""
value = data.get(field, fallback)
print("" if value is None else value)
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

json_field() {
  local port="$1"
  local field="$2"
  local status
  status="$(get_status_json "$port" 2>/dev/null || true)"
  json_field_from_text "$status" "$field"
}

post_control() {
  local port="$1"
  local endpoint="$2"
  local symbol="${3:-control}"
  local url attempt
  url="$(control_url "$port" "$endpoint")"
  for attempt in $(seq 1 "$HTTP_RETRY_COUNT"); do
    if http_post "$url" >/dev/null 2>&1; then
      log "$symbol control request succeeded endpoint=$endpoint attempt=$attempt"
      return 0
    fi
    if [[ "$attempt" -lt "$HTTP_RETRY_COUNT" ]]; then
      sleep 1
    fi
  done
  log "$symbol control request failed endpoint=$endpoint attempts=$HTTP_RETRY_COUNT"
  return 1
}

is_port_listening() {
  local port="$1"
  [[ -n "$port" ]] || return 1
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

find_port_pids() {
  local port="$1"
  [[ -n "$port" ]] || return 0
  lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u || true
}

find_property_pids() {
  local properties_file="$1"
  [[ -n "$properties_file" ]] || return 0
  ps -ww -axo pid=,command= | awk -v pattern="--spring.config.additional-location=file:${properties_file}" '
    index($0, pattern) > 0 {print $1}
  ' | sort -u || true
}

find_child_pids_once() {
  local parent_csv="$1"
  [[ -n "$parent_csv" ]] || return 0
  ps -axo pid=,ppid= | awk -v parents=",${parent_csv}," '
    {
      pid=$1
      ppid=$2
      if (index(parents, "," ppid ",") > 0) {
        print pid
      }
    }
  ' | sort -u || true
}

collect_target_pids() {
  local properties_file="$1"
  local port="$2"
  local initial=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    initial+=("$pid")
  done < <(find_port_pids "$port")
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    initial+=("$pid")
  done < <(find_property_pids "$properties_file")

  if [[ ${#initial[@]} -eq 0 ]]; then
    return 0
  fi

  local all_pids
  all_pids="$(printf '%s\n' "${initial[@]}" | sort -u)"
  local frontier
  frontier="$all_pids"
  local children child joined_frontier
  while [[ -n "$frontier" ]]; do
    joined_frontier="$(echo "$frontier" | paste -sd, -)"
    children="$(find_child_pids_once "$joined_frontier")"
    if [[ -z "$children" ]]; then
      break
    fi
    frontier="$(comm -13 <(echo "$all_pids" | sort -u) <(echo "$children" | sort -u) || true)"
    if [[ -z "$frontier" ]]; then
      break
    fi
    all_pids="$(printf '%s\n%s\n' "$all_pids" "$frontier" | awk 'NF' | sort -u)"
  done

  printf '%s\n' "$all_pids" | awk 'NF'
}

pid_is_alive() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  ps -p "$pid" >/dev/null 2>&1
}

stop_pid_list() {
  local signal="$1"
  shift || true
  local pid
  for pid in "$@"; do
    [[ -n "$pid" ]] || continue
    if pid_is_alive "$pid"; then
      kill "-$signal" "$pid" >/dev/null 2>&1 || true
    fi
  done
}

wait_for_pids_to_exit() {
  local timeout_seconds="$1"
  shift || true
  local -a pids=("$@")
  local elapsed=0
  local pid any_alive
  while [[ $elapsed -lt $timeout_seconds ]]; do
    any_alive=0
    for pid in "${pids[@]-}"; do
      [[ -n "$pid" ]] || continue
      if pid_is_alive "$pid"; then
        any_alive=1
        break
      fi
    done
    if [[ $any_alive -eq 0 ]]; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  for pid in "${pids[@]-}"; do
    [[ -n "$pid" ]] || continue
    if pid_is_alive "$pid"; then
      return 1
    fi
  done
  return 0
}

stop_shared_relay() {
  local pids=()
  local pid raw_pids=()
  if [[ -f "$shared_relay_pid_file" ]]; then
    pid="$(tr -d '[:space:]' < "$shared_relay_pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then
      raw_pids+=("$pid")
    fi
  fi
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    raw_pids+=("$pid")
  done < <(pgrep -f "$shared_relay_process_pattern" || true)

  if [[ ${#raw_pids[@]} -eq 0 ]]; then
    log "shared relay already stopped"
    return 0
  fi

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    pids+=("$pid")
  done < <(printf '%s\n' "${raw_pids[@]}" | sort -u)

  log "stopping shared relay pid(s): $(printf '%s ' "${pids[@]}")"
  stop_pid_list TERM "${pids[@]}"
  if wait_for_pids_to_exit "$force_after_seconds" "${pids[@]}"; then
    rm -f "$shared_relay_pid_file"
    log "shared relay exited after SIGTERM"
    return 0
  fi

  remaining=()
  for pid in "${pids[@]}"; do
    if pid_is_alive "$pid"; then
      remaining+=("$pid")
    fi
  done

  if [[ ${#remaining[@]} -gt 0 ]]; then
    log "forcing shared relay pid(s): $(printf '%s ' "${remaining[@]}")"
    stop_pid_list KILL "${remaining[@]}"
    sleep 1
  fi

  for pid in "${pids[@]}"; do
    if pid_is_alive "$pid"; then
      log "shared relay failed to stop pid=$pid"
      return 1
    fi
  done

  rm -f "$shared_relay_pid_file"
  log "shared relay stopped"
  return 0
}

request_control_stop() {
  local symbol="$1"
  local port="$2"
  local endpoint url attempt
  for endpoint in "/api/control/pause" "/api/control/kill-switch/true"; do
    url="$(control_url "$port" "$endpoint")"
    for attempt in $(seq 1 "$HTTP_RETRY_COUNT"); do
      if http_post "$url" >/dev/null 2>&1; then
        log "$symbol control request succeeded endpoint=$endpoint attempt=$attempt"
        break
      fi
      if [[ "$attempt" -eq "$HTTP_RETRY_COUNT" ]]; then
        log "$symbol control request failed endpoint=$endpoint attempts=$HTTP_RETRY_COUNT"
      else
        sleep 1
      fi
    done
  done
}

status_has_exposure() {
  local position="$1"
  local open_orders="$2"
  [[ -n "$position" && "$position" != "0" ]] && return 0
  [[ -n "$open_orders" && "$open_orders" != "0" ]] && return 0
  return 1
}

guard_safe_stop_without_flatten() {
  local symbol="$1"
  local port="$2"
  local current_position open_orders
  current_position="$(json_field "$port" currentPosition || true)"
  open_orders="$(json_field "$port" openOrders || true)"
  log "$symbol pre-stop status position=${current_position:-unknown} openOrders=${open_orders:-unknown} flatten_first=0 allow_unsafe_stop=$allow_unsafe_stop"
  if status_has_exposure "${current_position:-}" "${open_orders:-}"; then
    if [[ $allow_unsafe_stop -eq 1 ]]; then
      log "$symbol UNSAFE STOP allowed explicitly with position=${current_position:-unknown} openOrders=${open_orders:-unknown}"
      return 0
    fi
    log "$symbol refusing unsafe stop while exposure/order risk is reported; rerun with --flatten-first or --allow-unsafe-stop"
    return 1
  fi
  return 0
}

request_flatten_then_confirm() {
  local symbol="$1"
  local port="$2"
  local current_position open_orders flat_confirmed=0

  current_position="$(json_field "$port" currentPosition || true)"
  open_orders="$(json_field "$port" openOrders || true)"
  log "$symbol pre-flatten status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"

  post_control "$port" "/api/control/pause" "$symbol" || true
  current_position="$(json_field "$port" currentPosition || true)"
  open_orders="$(json_field "$port" openOrders || true)"
  log "$symbol post-pause status position=${current_position:-unknown} openOrders=${open_orders:-unknown}"

  if [[ -n "${open_orders:-}" && "${open_orders:-0}" != "0" ]]; then
    post_control "$port" "/api/control/cancel-open-orders" "$symbol" || true
  fi

  # Always call flatten in flatten-first mode. Shared-gateway bots can use this as a broker-state check even when the
  # JVM's last local status reports flat.
  post_control "$port" "/api/control/flatten" "$symbol" || true

  for _ in {1..24}; do
    sleep 5
    current_position="$(json_field "$port" currentPosition || true)"
    open_orders="$(json_field "$port" openOrders || true)"
    log "$symbol flatten poll position=${current_position:-unknown} openOrders=${open_orders:-unknown}"
    if [[ "${current_position:-x}" == "0" && "${open_orders:-x}" == "0" ]]; then
      flat_confirmed=1
      break
    fi
  done

  if [[ $flat_confirmed -eq 1 ]]; then
    log "$symbol flat/order-free confirmed before stop"
    return 0
  fi
  log "$symbol did not confirm flat/order-free before stop; leaving process running"
  return 1
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
    --dry-run)
      dry_run=1
      ;;
    --list)
      list_mode=1
      ;;
    --skip-control)
      skip_control=1
      ;;
    --flatten-first)
      flatten_first=1
      ;;
    --allow-unsafe-stop)
      allow_unsafe_stop=1
      ;;
    --copy-live-logs-on-stop)
      copy_live_logs_on_stop=1
      ;;
    --force-after-seconds=*)
      force_after_seconds="${1#--force-after-seconds=}"
      ;;
    *)
      echo "[BULK-STOP][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ ! -d "$bots_dir" && ! -d "$legacy_bots_dir" ]]; then
  echo "[BULK-STOP][ERROR] Missing runtime directories under $repo_root" >&2
  exit 1
fi

case "$force_after_seconds" in
  ''|*[!0-9]*)
    echo "[BULK-STOP][ERROR] --force-after-seconds must be a whole number." >&2
    exit 1
    ;;
esac

all_symbols=()
while IFS= read -r symbol; do
  [[ -n "$symbol" ]] || continue
  all_symbols+=("$symbol")
done < <(discover_symbols | LC_ALL=C sort -u)

if [[ ${#all_symbols[@]} -eq 0 ]]; then
  echo "[BULK-STOP][ERROR] No generated bot configs found in $bots_dir" >&2
  exit 1
fi

include_symbols=()
if [[ -n "$symbols_csv" ]]; then
  while IFS= read -r symbol; do
    [[ -n "$symbol" ]] || continue
    include_symbols+=("$symbol")
  done < <(parse_csv_symbols "$symbols_csv")
fi

exclude_symbols=()
if [[ -n "$exclude_csv" ]]; then
  while IFS= read -r symbol; do
    [[ -n "$symbol" ]] || continue
    exclude_symbols+=("$symbol")
  done < <(parse_csv_symbols "$exclude_csv")
fi

selected_symbols=()
if [[ ${#include_symbols[@]} -gt 0 ]]; then
  for symbol in "${include_symbols[@]}"; do
    if ! contains_symbol "$symbol" "${all_symbols[@]}"; then
      echo "[BULK-STOP][ERROR] Requested symbol not found in generated bot configs: $symbol" >&2
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
  echo "[BULK-STOP][ERROR] No symbols remain after applying --symbols/--exclude filters." >&2
  exit 1
fi

log "discovered ${#all_symbols[@]} bot configs; selected ${#selected_symbols[@]} symbols"
printf '[BULK-STOP] symbols=%s\n' "$(printf '%s,' "${selected_symbols[@]}" | sed 's/,$//')"
printf '[BULK-STOP] dry_run=%s skip_control=%s flatten_first=%s allow_unsafe_stop=%s copy_live_logs_on_stop=%s force_after_seconds=%s\n' "$dry_run" "$skip_control" "$flatten_first" "$allow_unsafe_stop" "$copy_live_logs_on_stop" "$force_after_seconds"
if [[ $flatten_first -eq 0 ]]; then
  log "safety: bulk stop will not flatten positions; reachable bots with reported exposure/open orders will be refused unless --allow-unsafe-stop is passed"
fi

stop_shared_relay_after=0
if [[ ${#selected_symbols[@]} -eq ${#all_symbols[@]} ]]; then
  stop_shared_relay_after=1
fi

if [[ $list_mode -eq 1 ]]; then
  exit 0
fi

stopped_count=0
already_stopped_count=0
failure_count=0

for symbol in "${selected_symbols[@]}"; do
  symbol_lower="$(lower_symbol "$symbol")"
  properties_file="$(resolve_properties_file "$symbol_lower" || true)"
  if [[ -z "$properties_file" ]]; then
    log "$symbol properties file not found; skipping"
    failure_count=$((failure_count + 1))
    continue
  fi

  port="$(extract_prop "$properties_file" "server.port")"
  if [[ -z "$port" ]]; then
    log "$symbol missing server.port in $properties_file; skipping"
    failure_count=$((failure_count + 1))
    continue
  fi

  pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    pids+=("$pid")
  done < <(collect_target_pids "$properties_file" "$port")

  listening=0
  if is_port_listening "$port"; then
    listening=1
  fi

  if [[ ${#pids[@]} -eq 0 && $listening -eq 0 ]]; then
    log "$symbol already stopped port=$port"
    already_stopped_count=$((already_stopped_count + 1))
    continue
  fi

  printf '[BULK-STOP] target symbol=%s port=%s listening=%s pids=%s properties=%s\n' \
    "$symbol" \
    "$port" \
    "$listening" \
    "$(printf '%s,' "${pids[@]-}" | sed 's/,$//' | sed 's/^$/<none>/')" \
    "$properties_file"

  if [[ $dry_run -eq 1 ]]; then
    if [[ $copy_live_logs_on_stop -eq 1 ]]; then
      log "$symbol dry-run: would snapshot configured app/trade logs before stop"
    else
      log "$symbol dry-run: live log copying disabled (pass --copy-live-logs-on-stop to enable)"
    fi
    continue
  fi

  if [[ $copy_live_logs_on_stop -eq 1 ]]; then
    snapshot_symbol_logs "$symbol" "$properties_file" "$symbol_lower"
  fi

  if [[ $skip_control -eq 0 && $listening -eq 1 ]]; then
    if [[ $flatten_first -eq 1 ]]; then
      if ! request_flatten_then_confirm "$symbol" "$port"; then
        failure_count=$((failure_count + 1))
        continue
      fi
    elif ! guard_safe_stop_without_flatten "$symbol" "$port"; then
      failure_count=$((failure_count + 1))
      continue
    fi
    request_control_stop "$symbol" "$port"
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    pids=()
    while IFS= read -r pid; do
      [[ -n "$pid" ]] || continue
      pids+=("$pid")
    done < <(collect_target_pids "$properties_file" "$port")
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    log "$symbol had no resolvable PIDs after control stop attempts; checking port state only"
    if is_port_listening "$port"; then
      log "$symbol still listening on port=$port but no PID was resolved"
      failure_count=$((failure_count + 1))
    else
      stopped_count=$((stopped_count + 1))
    fi
    continue
  fi

  log "$symbol sending SIGTERM to pid(s): $(printf '%s ' "${pids[@]}")"
  stop_pid_list TERM "${pids[@]}"
  if wait_for_pids_to_exit "$force_after_seconds" "${pids[@]}"; then
    log "$symbol exited after SIGTERM"
    stopped_count=$((stopped_count + 1))
    continue
  fi

  remaining=()
  for pid in "${pids[@]}"; do
    if pid_is_alive "$pid"; then
      remaining+=("$pid")
    fi
  done

  if [[ ${#remaining[@]} -gt 0 ]]; then
    log "$symbol sending SIGKILL to remaining pid(s): $(printf '%s ' "${remaining[@]}")"
    stop_pid_list KILL "${remaining[@]}"
    sleep 1
  fi

  failed_to_stop=0
  port_still_listening=0
  for pid in "${pids[@]}"; do
    if pid_is_alive "$pid"; then
      failed_to_stop=1
      break
    fi
  done
  if is_port_listening "$port"; then
    port_still_listening=1
  fi

  if [[ $failed_to_stop -eq 1 || $port_still_listening -eq 1 ]]; then
    log "$symbol failed to stop cleanly port=$port"
    failure_count=$((failure_count + 1))
  else
    log "$symbol stopped"
    stopped_count=$((stopped_count + 1))
  fi
done

if [[ $stop_shared_relay_after -eq 1 ]]; then
  if [[ $dry_run -eq 1 ]]; then
    log "dry-run: would stop shared Databento relay after stopping all bots"
  elif ! stop_shared_relay; then
    failure_count=$((failure_count + 1))
  fi

  if [[ $dry_run -eq 1 ]]; then
    log "dry-run: would stop shared IBKR gateway after stopping all bots"
  elif [[ -x "$stop_shared_ibkr_gateway_script" ]]; then
    if ! "$stop_shared_ibkr_gateway_script" --force-after-seconds="$force_after_seconds"; then
      failure_count=$((failure_count + 1))
    fi
  else
    log "shared IBKR gateway stop script missing or not executable: $stop_shared_ibkr_gateway_script"
    failure_count=$((failure_count + 1))
  fi
fi

printf '[BULK-STOP] summary stopped=%s already_stopped=%s failures=%s dry_run=%s\n' \
  "$stopped_count" \
  "$already_stopped_count" \
  "$failure_count" \
  "$dry_run"

if [[ -n "$snapshot_root_dir" ]]; then
  printf '[BULK-STOP] snapshots=%s manifest=%s\n' "$snapshot_root_dir" "$snapshot_manifest_file"
fi

if [[ $failure_count -gt 0 ]]; then
  exit 1
fi


