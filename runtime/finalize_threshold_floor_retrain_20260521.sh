#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
COMPARE_ROOT="$ROOT/training_data/compare_runs_20260419_meta_ab"
PYTHON_BIN="/Users/filmonghezehey/miniforge3/bin/python3"
LOG="$ROOT/runtime/databento/logs/finalize_threshold_floor_retrain_20260521.log"
ROUTING_20260419="$ROOT/runtime/databento/model-routing-20260419.csv"
ROUTING_LIVE="$ROOT/runtime/databento/model-routing.csv"
THRESHOLDS_20260419="$ROOT/runtime/databento/bot-thresholds-20260419.csv"
THRESHOLDS_LIVE="$ROOT/runtime/databento/bot-thresholds.csv"

mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

cd "$ROOT"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] watcher started"

while pgrep -f "scripts/finalize_databento_compare_20260419.sh|scripts/run_databento_compare_training_20260419.sh|prepare_databento_training.py|train_30s_models.py|generate_timesfm_features.py|train_sequence_meta.py" >/dev/null; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] training/finalize still running; waiting 300s"
  sleep 300
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] training/finalize processes stopped; reconciling artifacts"

"$PYTHON_BIN" "$ROOT/runtime/reconcile_compare_variant_manifest_20260407.py" \
  --variant-dir "$COMPARE_ROOT/baseline_no_generated_meta" \
  --baseline-template-manifest "$COMPARE_ROOT/baseline_no_generated_meta/training_manifest.csv" \
  --write

"$PYTHON_BIN" "$ROOT/runtime/reconcile_compare_variant_manifest_20260407.py" \
  --variant-dir "$COMPARE_ROOT/with_timesfm_and_sequence_proxy" \
  --baseline-template-manifest "$COMPARE_ROOT/baseline_no_generated_meta/training_manifest.csv" \
  --write

"$PYTHON_BIN" "$ROOT/runtime/generate_compare_report_20260407.py" \
  --compare-root "$COMPARE_ROOT"

"$PYTHON_BIN" "$ROOT/runtime/build_mixed_model_routing_20260407.py" \
  --compare-root "$COMPARE_ROOT" \
  --output-csv "$ROUTING_20260419"

cp "$ROUTING_20260419" "$ROUTING_LIVE"

"$PYTHON_BIN" "$ROOT/scripts/generate_databento_bot_properties.py" \
  --routing-csv "$ROUTING_LIVE" \
  --plan-csv "$ROOT/training_data/databento_30s_20260419/symbol_model_plan.csv" \
  --final-report-json "$COMPARE_ROOT/final_comparison_report.json" \
  --threshold-summary-csv "$THRESHOLDS_LIVE" \
  --model-routing-property "runtime/databento/model-routing.csv" \
  --symbol-plan-property "training_data/databento_30s_20260419/symbol_model_plan.csv"

cp "$THRESHOLDS_LIVE" "$THRESHOLDS_20260419"

"$PYTHON_BIN" - <<'PY'
import csv
import json
from pathlib import Path

root = Path('/Users/filmonghehey/trading-agent/worktrees/databento')
if not root.exists():
    root = Path('/Users/filmonghezehey/trading-agent/worktrees/databento')
report_path = root / 'training_data/compare_runs_20260419_meta_ab/final_comparison_report.json'
threshold_csv = root / 'runtime/databento/bot-thresholds.csv'
verify_path = root / 'runtime/databento/threshold_floor_verification_20260521.json'

violations = []
if report_path.exists():
    report = json.loads(report_path.read_text())
    for variant, payload in report.get('variants', {}).items():
        for section in ('primary_score_rows', 'open_score_rows', 'regime_score_rows'):
            for row in payload.get(section, []):
                value = row.get('avg_threshold')
                if value is not None and float(value) < 0.60:
                    violations.append({
                        'source': 'final_report',
                        'variant': variant,
                        'section': section,
                        'job_log': row.get('job_log'),
                        'model': row.get('model'),
                        'regime': row.get('regime', ''),
                        'value': float(value),
                    })

if threshold_csv.exists():
    with threshold_csv.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            for key, raw in row.items():
                if not key.endswith('_threshold') or key == 'regime_threshold':
                    continue
                if raw in (None, ''):
                    continue
                try:
                    value = float(raw)
                except ValueError:
                    continue
                if value < 0.60:
                    violations.append({
                        'source': 'bot_thresholds_csv',
                        'symbol': row.get('symbol'),
                        'job_name': row.get('job_name'),
                        'variant': row.get('variant'),
                        'key': key,
                        'value': value,
                    })

payload = {
    'ok': not violations,
    'min_required_threshold': 0.60,
    'violation_count': len(violations),
    'violations': violations[:200],
}
verify_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
print(json.dumps(payload, indent=2))
if violations:
    raise SystemExit(2)
PY

echo "[$(date '+%Y-%m-%d %H:%M:%S')] finalize watcher completed successfully"

