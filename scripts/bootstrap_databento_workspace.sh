#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

mkdir -p "$repo_root/runtime/databento/logs"
mkdir -p "$repo_root/runtime/databento/state"
mkdir -p "$repo_root/runtime/databento/output"
mkdir -p "$repo_root/runtime/databento/schedule_state"

if [[ ! -f "$repo_root/runtime/databento.env" ]]; then
  cp "$repo_root/databento_ibkr_bridge/.env.example" "$repo_root/runtime/databento.env"
  echo "[BOOTSTRAP] Created runtime/databento.env from template"
else
  echo "[BOOTSTRAP] runtime/databento.env already exists (left unchanged)"
fi

if [[ ! -f "$repo_root/runtime/trading-databento-template.properties" ]]; then
  cat > "$repo_root/runtime/trading-databento-template.properties" <<'EOF'
# Databento upgrade stack template (use one file per symbol process if needed)
trading.symbol=SPY
trading.client-id=210
server.port=9081
trading.market-data-request-id=2101
trading.trade-amount=10000
trading.risk.max-order-notional=10000
trading.shared-capital.enabled=true
trading.shared-capital.file=runtime/databento/shared-capital.properties
trading.shared-capital.total-notional=500000
trading.model.dir=runtime/models/SPY
trading.state.file=runtime/databento/state/trader-state-SPY.properties
trading.log.file=runtime/databento/output/trades-SPY.csv
logging.file.name=runtime/databento/logs/trading-agent-SPY.log
EOF
  echo "[BOOTSTRAP] Created runtime/trading-databento-template.properties"
else
  echo "[BOOTSTRAP] runtime/trading-databento-template.properties already exists (left unchanged)"
fi

if [[ ! -f "$repo_root/runtime/databento/shared-capital.properties" ]]; then
  cat > "$repo_root/runtime/databento/shared-capital.properties" <<'EOF'
# Dedicated shared-capital file for the Databento stack
trading.shared-capital.available-notional=500000
trading.shared-capital.last-updated-epoch-ms=0
EOF
  echo "[BOOTSTRAP] Created runtime/databento/shared-capital.properties"
else
  echo "[BOOTSTRAP] runtime/databento/shared-capital.properties already exists (left unchanged)"
fi

echo "[BOOTSTRAP] Databento workspace bootstrap complete"

