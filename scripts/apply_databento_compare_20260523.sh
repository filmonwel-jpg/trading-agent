#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
PYTHON_BIN="${PYTHON_BIN:-python3}"
COMPARE_ROOT="${COMPARE_ROOT:-$ROOT/training_data/compare_runs_20260523_meta_ab}"
VERSIONED_ROUTING_CSV="${ROUTING_CSV:-$ROOT/runtime/databento/model-routing-20260523.csv}"
CANONICAL_ROUTING_CSV="$ROOT/runtime/databento/model-routing.csv"
PLAN_CSV="$ROOT/training_data/databento_30s_20260523/symbol_model_plan.csv"
FINAL_REPORT_JSON="$COMPARE_ROOT/final_comparison_report.json"
THRESHOLD_CSV="$ROOT/runtime/databento/bot-thresholds.csv"
VERSIONED_THRESHOLD_CSV="$ROOT/runtime/databento/bot-thresholds-20260523.csv"
BOTS_DIR="$ROOT/runtime/databento/bots"
TEMPLATE_PATH="$BOTS_DIR/trading-databento-template.properties"
APPLY_SYMBOLS_FILE="$ROOT/runtime/databento/applied-symbols-20260523.txt"
PROMOTION_ROOT="$ROOT/runtime/databento/promotions"
STAMP="$(date '+%Y%m%d_%H%M%S')"
BACKUP_DIR="$PROMOTION_ROOT/backup_before_20260523_apply_$STAMP"

cd "$ROOT"

for required in "$VERSIONED_ROUTING_CSV" "$PLAN_CSV" "$FINAL_REPORT_JSON"; do
  if [[ ! -f "$required" ]]; then
    echo "Missing required apply input: $required" >&2
    echo "Run scripts/finalize_databento_compare_20260523.sh first." >&2
    exit 1
  fi
done

mkdir -p "$BACKUP_DIR"
[[ -f "$CANONICAL_ROUTING_CSV" ]] && cp -p "$CANONICAL_ROUTING_CSV" "$BACKUP_DIR/model-routing.csv"
[[ -f "$THRESHOLD_CSV" ]] && cp -p "$THRESHOLD_CSV" "$BACKUP_DIR/bot-thresholds.csv"
if [[ -d "$BOTS_DIR" ]]; then
  mkdir -p "$BACKUP_DIR/bots"
  find "$BOTS_DIR" -maxdepth 1 -name 'trading-*.properties' -type f -exec cp -p {} "$BACKUP_DIR/bots/" \;
fi

"$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
import pandas as pd
root = Path(os.environ['ROOT'])
plan = pd.read_csv(root / 'training_data/databento_30s_20260523/symbol_model_plan.csv')
symbols = sorted(plan['Symbol'].astype(str).str.upper().str.strip().dropna().unique())
out = root / 'runtime/databento/applied-symbols-20260523.txt'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text('\n'.join(s for s in symbols if s) + '\n', encoding='utf-8')
print(f'WROTE {out} symbols={len(symbols)}')
PY

cp -p "$VERSIONED_ROUTING_CSV" "$CANONICAL_ROUTING_CSV"

"$PYTHON_BIN" scripts/generate_databento_bot_properties.py \
  --routing-csv "$CANONICAL_ROUTING_CSV" \
  --plan-csv "$PLAN_CSV" \
  --final-report-json "$FINAL_REPORT_JSON" \
  --bots-dir "$BOTS_DIR" \
  --threshold-summary-csv "$THRESHOLD_CSV" \
  --symbols-file "$APPLY_SYMBOLS_FILE" \
  --template-path "$TEMPLATE_PATH" \
  --model-routing-property "runtime/databento/model-routing.csv" \
  --symbol-plan-property "training_data/databento_30s_20260523/symbol_model_plan.csv"

cp -p "$THRESHOLD_CSV" "$VERSIONED_THRESHOLD_CSV"

"$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
import pandas as pd
root = Path(os.environ['ROOT'])
routing = pd.read_csv(root / 'runtime/databento/model-routing.csv')
thresholds = pd.read_csv(root / 'runtime/databento/bot-thresholds.csv')
props = sorted((root / 'runtime/databento/bots').glob('trading-*.properties'))
props = [p for p in props if p.name != 'trading-databento-template.properties']
print(f'APPLIED_ROUTING_ROWS={len(routing)}')
print(f'APPLIED_THRESHOLD_ROWS={len(thresholds)}')
print(f'APPLIED_BOT_PROPERTY_FILES={len(props)}')
print('ROUTING_VARIANTS=')
print(routing['variant'].value_counts(dropna=False).to_string() if 'variant' in routing.columns else '<missing variant column>')
PY

echo "APPLY_BACKUP_DIR=$BACKUP_DIR"
echo "APPLIED_CANONICAL_ROUTING=$CANONICAL_ROUTING_CSV"
echo "APPLIED_VERSIONED_ROUTING=$VERSIONED_ROUTING_CSV"
echo "APPLIED_THRESHOLDS=$THRESHOLD_CSV"
echo "APPLIED_VERSIONED_THRESHOLDS=$VERSIONED_THRESHOLD_CSV"

