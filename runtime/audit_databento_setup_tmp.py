import csv
import json
from pathlib import Path

root = Path('/Users/filmonghezehey/trading-agent/worktrees/databento')
routing_rows = list(csv.DictReader((root / 'runtime/databento/model-routing.csv').open()))
threshold_rows = list(csv.DictReader((root / 'runtime/databento/bot-thresholds.csv').open()))
routing = {r['symbol'].upper(): r for r in routing_rows}
thresholds = {r['symbol'].upper(): r for r in threshold_rows}
required_30s = [
    'long_entry.onnx', 'short_entry.onnx', 'regime_classifier.onnx',
    'choppy_long_entry.onnx', 'choppy_short_entry.onnx',
    'trend_long_entry.onnx', 'trend_short_entry.onnx',
    'volatile_long_entry.onnx', 'volatile_short_entry.onnx',
    'open30_long_entry.onnx', 'open30_short_entry.onnx',
]
required_lifecycle = [
    'long_exit_lifecycle.onnx', 'short_exit_lifecycle.onnx',
    'long_micro_entry_5s.onnx', 'short_micro_entry_5s.onnx',
    'long_micro_exit_guard_5s.onnx', 'short_micro_exit_guard_5s.onnx',
    'lifecycle_micro_scorecard.csv', 'lifecycle_micro_route_manifest.json',
]
errors = []
seen = []

for path in sorted((root / 'runtime/databento/bots').glob('trading-*.properties')):
    if path.name == 'trading-databento-template.properties':
        continue
    props = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            props[k.strip()] = v.strip()
    sym = props.get('trading.symbol', '').upper()
    seen.append(sym)
    if sym not in routing:
        errors.append(f'{sym}: missing routing row')
    if sym not in thresholds:
        errors.append(f'{sym}: missing thresholds row')
    if props.get('trading.databento.model-routing-csv') != 'runtime/databento/model-routing.csv':
        errors.append(f'{sym}: noncanonical routing csv')
    if props.get('trading.databento.symbol-plan-csv') != 'training_data/databento_30s_20260523/symbol_model_plan.csv':
        errors.append(f'{sym}: noncanonical symbol plan')
    if props.get('trading.databento.shared-feed.enabled') != 'true':
        errors.append(f'{sym}: shared 1s feed not enabled')
    md = Path(props.get('trading.model.dir', ''))
    if not md.is_dir():
        errors.append(f'{sym}: missing model_dir {md}')
    else:
        miss = [m for m in required_30s if not (md / m).is_file()]
        if miss:
            errors.append(f'{sym}: missing 30s models {miss}')
    if sym in routing and props.get('trading.model.dir') != routing[sym]['model_dir']:
        errors.append(f'{sym}: property model_dir differs from routing')
    if sym in thresholds:
        pairs = [
            ('trading.ai.long-entry-threshold', 'long_entry_threshold'),
            ('trading.ai.short-entry-threshold', 'short_entry_threshold'),
            ('trading.ai.long-exit-threshold', 'long_exit_threshold'),
            ('trading.ai.short-exit-threshold', 'short_exit_threshold'),
            ('trading.ai.regime-threshold', 'regime_threshold'),
        ]
        for pk, ck in pairs:
            if props.get(pk) != thresholds[sym][ck]:
                errors.append(f'{sym}: {pk} {props.get(pk)} != {thresholds[sym][ck]}')

lifecycle_dir = root / 'model_exports/lifecycle_micro_20260523'
miss_life = [m for m in required_lifecycle if not (lifecycle_dir / m).is_file()]
manifest = json.loads((lifecycle_dir / 'lifecycle_micro_route_manifest.json').read_text())
score = list(csv.DictReader((lifecycle_dir / 'lifecycle_micro_scorecard.csv').open()))

print('bot_properties', len(seen), 'unique_symbols', len(set(seen)))
print('routing_rows', len(routing_rows), 'threshold_rows', len(threshold_rows))
print('routing_variants', {v: sum(1 for r in routing_rows if r['variant'] == v) for v in sorted(set(r['variant'] for r in routing_rows))})
print('unique_30s_model_dirs', len(set(r['model_dir'] for r in routing_rows)))
print('lifecycle_missing', miss_life)
print('lifecycle_manifest_models', sorted(manifest.get('models', {})))
print('scorecard_thresholds', {r['model']: r['threshold'] for r in score})
print('symbols_in_bots_not_routing', sorted(set(seen) - set(routing)))
print('symbols_in_routing_not_bots', sorted(set(routing) - set(seen)))
print('error_count', len(errors))
for e in errors[:50]:
    print('ERROR', e)
raise SystemExit(1 if errors or miss_life else 0)

