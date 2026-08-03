#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$repo_root/runtime/databento/logs"
mkdir -p "$repo_root/runtime/databento/state"
mkdir -p "$repo_root/runtime/databento/output"
mkdir -p "$repo_root/runtime/databento/bots"
mkdir -p "$repo_root/runtime/databento/schedule_state"

if [[ ! -f "$repo_root/runtime/databento.env" ]]; then
  cp "$repo_root/databento_ibkr_bridge/.env.example" "$repo_root/runtime/databento.env"
  echo "[BOOTSTRAP] Created runtime/databento.env from template"
else
  echo "[BOOTSTRAP] runtime/databento.env already exists (left unchanged)"
fi

if [[ ! -f "$repo_root/runtime/databento/bots/trading-databento-template.properties" ]]; then
  cat > "$repo_root/runtime/databento/bots/trading-databento-template.properties" <<'EOF'
# Databento upgrade stack template (use one file per symbol process if needed)
trading.symbol=SPY
trading.client-id=210
server.port=9081
trading.market-data.provider=databento
trading.market-data-request-id=2101
trading.trade-amount=10000
trading.risk.max-order-notional=10000
trading.ai.long-entry-threshold=0.68
trading.ai.short-entry-threshold=0.63
trading.ai.long-exit-threshold=0.58
trading.ai.short-exit-threshold=0.60
trading.ai.regime-threshold=0.50
trading.ai.entry-threshold-raise-percent=10.0
trading.ai.open30.long-entry-threshold=0.68
trading.ai.open30.short-entry-threshold=0.63
trading.ai.open30.long-exit-threshold=0.58
trading.ai.open30.short-exit-threshold=0.60
trading.ai.regime.choppy.long-entry-threshold=0.68
trading.ai.regime.choppy.short-entry-threshold=0.63
trading.ai.regime.choppy.long-exit-threshold=0.58
trading.ai.regime.choppy.short-exit-threshold=0.60
trading.ai.regime.trend.long-entry-threshold=0.68
trading.ai.regime.trend.short-entry-threshold=0.63
trading.ai.regime.trend.long-exit-threshold=0.58
trading.ai.regime.trend.short-exit-threshold=0.60
trading.ai.regime.volatile.long-entry-threshold=0.68
trading.ai.regime.volatile.short-entry-threshold=0.63
trading.ai.regime.volatile.long-exit-threshold=0.58
trading.ai.regime.volatile.short-exit-threshold=0.60
trading.shared-capital.enabled=true
trading.shared-capital.file=runtime/databento/shared-capital.properties
trading.shared-capital.total-notional=300000
trading.model.dir=runtime/models/SPY
trading.databento.option-parents=SPY
trading.databento.model-routing-csv=runtime/databento/model-routing.csv
trading.databento.symbol-plan-csv=training_data/databento_30s/symbol_model_plan.csv
trading.state.file=runtime/databento/state/trader-state-SPY.properties
trading.log.file=runtime/databento/output/trades-SPY.csv
logging.file.name=runtime/databento/logs/trading-agent-SPY.log
EOF
  echo "[BOOTSTRAP] Created runtime/databento/bots/trading-databento-template.properties"
else
  echo "[BOOTSTRAP] runtime/databento/bots/trading-databento-template.properties already exists (left unchanged)"
fi

if [[ ! -f "$repo_root/runtime/databento/shared-capital.properties" ]]; then
  cat > "$repo_root/runtime/databento/shared-capital.properties" <<'EOF'
# Dedicated shared-capital file for the Databento stack
trading.shared-capital.available-notional=300000
trading.shared-capital.last-updated-epoch-ms=0
EOF
  echo "[BOOTSTRAP] Created runtime/databento/shared-capital.properties"
else
  echo "[BOOTSTRAP] runtime/databento/shared-capital.properties already exists (left unchanged)"
fi

echo "[BOOTSTRAP] Databento workspace bootstrap complete"

