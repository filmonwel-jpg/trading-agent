#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_databento_api_backtest_core5.sh --start YYYY-MM-DD --end YYYY-MM-DD [extra backtest args]

Runs the Databento Historical API -> Java IBKR-sim streaming backtest for the core
five-symbol pilot universe: TSLA,TQQQ,NVDA,SPY,QQQ.

Environment overrides:
  CORE5_SYMBOLS_FILE       Default: config/databento_core_5_symbols.txt
  CORE5_SETUP_MODEL_DIR    Default: model_exports/20260320_192113
  CORE5_LIFECYCLE_MODEL_DIR Default: runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports
  CORE5_OUTPUT_DIR         Default: runtime/local-backtests/databento_api_core5
  CORE5_TIMEOUT_SECONDS    Default: 1800
  CORE5_SKIP_BUILD         Default: false

Examples:
  scripts/run_databento_api_backtest_core5.sh --list-symbols
  scripts/run_databento_api_backtest_core5.sh --start 2026-05-21 --end 2026-05-21 --dry-run --timeout-seconds 60
  scripts/run_databento_api_backtest_core5.sh --start 2026-05-21 --end 2026-05-23 --skip-build
USAGE
}

SYMBOLS_FILE="${CORE5_SYMBOLS_FILE:-config/databento_core_5_symbols.txt}"
SETUP_MODEL_DIR="${CORE5_SETUP_MODEL_DIR:-model_exports/20260320_192113}"
LIFECYCLE_MODEL_DIR="${CORE5_LIFECYCLE_MODEL_DIR:-runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports}"
OUTPUT_DIR="${CORE5_OUTPUT_DIR:-runtime/local-backtests/databento_api_core5}"
TIMEOUT_SECONDS="${CORE5_TIMEOUT_SECONDS:-1800}"
SKIP_BUILD="${CORE5_SKIP_BUILD:-false}"

for arg in "$@"; do
  case "$arg" in
    --help|-h)
      usage
      exit 0
      ;;
  esac
done

args=(
  --source api
  --symbols-file "$SYMBOLS_FILE"
  --model-dir "$SETUP_MODEL_DIR"
  --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR"
  --output-dir "$OUTPUT_DIR"
  --timeout-seconds "$TIMEOUT_SECONDS"
)

if [[ "$SKIP_BUILD" =~ ^(1|true|TRUE|yes|YES|on|ON)$ ]]; then
  args+=(--skip-build)
fi

exec scripts/run_databento_historical_ibkr_sim_backtest.sh "${args[@]}" "$@"
