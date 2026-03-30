#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_symbol.sh <SYMBOL> [--start] [--max-trades=N] [--tee[=FILE]] [--tee-db] [-- <extra java args...>]

Examples:
  ./run_symbol.sh TSLA
  ./run_symbol.sh NVDA --start
  ./run_symbol.sh AMD --start --max-trades=0
  ./run_symbol.sh TSLA --start --tee
  ./run_symbol.sh NVDA --start --tee=runtime/nvda_live_trade_logs.txt -- --trading.risk.max-order-notional=90000
  ./run_symbol.sh AMD --start --tee --tee-db

Behavior:
  - Preview mode is the default. The script prints the resolved config and launch command.
  - Use --start to actually launch the packaged jar.
  - Use --max-trades=0 for a safe startup verification that blocks new entries.
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
models_root="$runtime_dir/models"

symbol_input=""
start_mode=0
tee_mode=0
tee_db_mode=0
tee_path=""
max_trades_override=""
extra_args=()

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
properties_file="$runtime_dir/trading-${symbol_lower}.properties"
model_dir="$models_root/$symbol_upper"

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

get_prop() {
  local key="$1"
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
  ' "$properties_file"
}

require_file "$jar_path" "packaged jar"
require_file "$properties_file" "symbol runtime properties"
require_file "$runtime_dir/postgres-local.properties" "runtime postgres properties"
require_dir "$model_dir" "symbol model directory"

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

if [[ $tee_db_mode -eq 1 && ! -f "$repo_root/stream_live_logs_to_db.py" ]]; then
  echo "[RUN][ERROR] Missing DB tee helper: $repo_root/stream_live_logs_to_db.py" >&2
  exit 1
fi

if [[ $tee_db_mode -eq 1 ]] && ! command -v python3 >/dev/null 2>&1; then
  echo "[RUN][ERROR] python3 is required for --tee-db." >&2
  exit 1
fi

server_port="$(get_prop server.port)"
client_id="$(get_prop trading.client-id)"
market_data_request_id="$(get_prop trading.market-data-request-id)"
trade_amount="$(get_prop trading.trade-amount)"
max_notional="$(get_prop trading.risk.max-order-notional)"
ai_long_entry_threshold="$(get_prop trading.ai.long-entry-threshold)"
ai_short_entry_threshold="$(get_prop trading.ai.short-entry-threshold)"
ai_long_exit_threshold="$(get_prop trading.ai.long-exit-threshold)"
ai_short_exit_threshold="$(get_prop trading.ai.short-exit-threshold)"
ai_regime_threshold="$(get_prop trading.ai.regime-threshold)"
shared_capital_enabled="$(get_prop trading.shared-capital.enabled)"
shared_capital_file="$(get_prop trading.shared-capital.file)"
shared_capital_total_notional="$(get_prop trading.shared-capital.total-notional)"
model_dir_prop="$(get_prop trading.model.dir)"
state_file="$(get_prop trading.state.file)"
trade_log_file="$(get_prop trading.log.file)"
app_log_file="$(get_prop logging.file.name)"
onnx_count="$(find "$model_dir" -maxdepth 1 -type f -name '*.onnx' | wc -l | tr -d ' ')"

if [[ -z "$server_port" ]]; then
  echo "[RUN][ERROR] server.port is missing in $properties_file" >&2
  exit 1
fi

if [[ "$onnx_count" -lt 21 ]]; then
  echo "[RUN][ERROR] Expected at least 21 ONNX files in $model_dir but found $onnx_count" >&2
  exit 1
fi

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
if [[ -n "$model_dir_prop" ]]; then
  cmd+=("--trading.model.dir=$model_dir_prop")
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
if [[ -n "$shared_capital_enabled" ]]; then
  cmd+=("--trading.shared-capital.enabled=$shared_capital_enabled")
fi
if [[ -n "$shared_capital_file" ]]; then
  cmd+=("--trading.shared-capital.file=$shared_capital_file")
fi
if [[ -n "$shared_capital_total_notional" ]]; then
  cmd+=("--trading.shared-capital.total-notional=$shared_capital_total_notional")
fi

if [[ -n "$max_trades_override" ]]; then
  cmd+=("--trading.max-trades=$max_trades_override")
fi

if [[ ${#extra_args[@]} -gt 0 ]]; then
  cmd+=("${extra_args[@]}")
fi

printf '[RUN] symbol=%s\n' "$symbol_upper"
printf '[RUN] java_bin=%s java_home=%s\n' "$java_bin" "${JAVA_HOME:-<unset>}"
printf '[RUN] properties=%s\n' "$properties_file"
printf '[RUN] model_dir=%s\n' "$model_dir"
printf '[RUN] configured_model_dir=%s\n' "$model_dir_prop"
printf '[RUN] onnx_count=%s\n' "$onnx_count"
printf '[RUN] server_port=%s client_id=%s market_data_request_id=%s\n' "$server_port" "$client_id" "$market_data_request_id"
printf '[RUN] trade_amount=%s max_order_notional=%s\n' "$trade_amount" "$max_notional"
printf '[RUN] ai_thresholds longEntry=%s shortEntry=%s longExit=%s shortExit=%s regime=%s\n' \
  "${ai_long_entry_threshold:-default}" \
  "${ai_short_entry_threshold:-default}" \
  "${ai_long_exit_threshold:-default}" \
  "${ai_short_exit_threshold:-default}" \
  "${ai_regime_threshold:-default}"
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

if [[ $tee_mode -eq 1 || $tee_db_mode -eq 1 ]]; then
  run_id="${symbol_upper}_$(date '+%Y%m%d_%H%M%S')"
  set -o pipefail
  if [[ $tee_db_mode -eq 1 ]]; then
    helper_cmd=(
      python3
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



