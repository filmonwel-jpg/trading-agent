#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE="${BASE:-$ROOT/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent}"
LATEST_ENV="${LATEST_ENV:-$BASE/run/latest_run.env}"

read_env_value() {
  local key="$1" file="$2"
  awk -F= -v k="$key" '$1 == k {sub($1"=", ""); print; exit}' "$file" 2>/dev/null || true
}

RUN_TS="${RUN_TS:-$(read_env_value RUN_TS "$LATEST_ENV")}" || true
RUN_LOG="${RUN_LOG:-$(read_env_value RUN_LOG "$LATEST_ENV")}" || true
RECORDED_EVENTS="${RECORDED_EVENTS:-$(read_env_value RECORDED_EVENTS "$LATEST_ENV")}" || true
RUN_TS="${RUN_TS:-latest}"
RUN_LOG="${RUN_LOG:-$BASE/run/databento-core5-4week-recent-20260627_230823.log}"
RECORDED_EVENTS="${RECORDED_EVENTS:-$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz}"
OUTPUT_DIR="${OUTPUT_DIR:-$BASE/setup_micro_counterfactual_$RUN_TS}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

for required_path in "$RUN_LOG" "$RECORDED_EVENTS"; do
  if [[ ! -f "$required_path" ]]; then
    echo "[SETUP_MICRO_CF][ERROR] missing required file: $required_path" >&2
    exit 2
  fi
done

mkdir -p "$OUTPUT_DIR"

echo "[SETUP_MICRO_CF] RUN_LOG=$RUN_LOG"
echo "[SETUP_MICRO_CF] RECORDED_EVENTS=$RECORDED_EVENTS"
echo "[SETUP_MICRO_CF] OUTPUT_DIR=$OUTPUT_DIR"

"$PYTHON_BIN" -u scripts/generate_setup_micro_counterfactual_report.py \
  --log "$RUN_LOG" \
  --recorded-events "$RECORDED_EVENTS" \
  --output-dir "$OUTPUT_DIR" \
  --label-min-micro-prob "${LABEL_MIN_MICRO_PROB:-0.30}" \
  --min-expected-net-r "${MIN_EXPECTED_NET_R:-0.0}" \
  --micro-threshold-grid "${MICRO_THRESHOLD_GRID:-0.20 0.25 0.30 0.35 0.40 0.45 0.50 0.52 0.55 0.57 0.58 0.60 0.62 0.64}" \
  --horizon-seconds "${HORIZON_SECONDS:-600}" \
  --profit-pct "${PROFIT_PCT:-0.0035}" \
  --risk-pct "${RISK_PCT:-0.0025}" \
  --entry-slippage-bps "${ENTRY_SLIPPAGE_BPS:-2.0}" \
  --exit-slippage-bps "${EXIT_SLIPPAGE_BPS:-2.0}" \
  --default-spread-bps "${DEFAULT_SPREAD_BPS:-0.0}" \
  --round-trip-commission-bps "${ROUND_TRIP_COMMISSION_BPS:-0.0}" \
  --fill-probability "${FILL_PROBABILITY:-0.98}" \
  --partial-fill-penalty-r "${PARTIAL_FILL_PENALTY_R:-0.02}" \
  --missed-fill-penalty-r "${MISSED_FILL_PENALTY_R:-0.05}"

echo "[SETUP_MICRO_CF] DONE: $OUTPUT_DIR"

