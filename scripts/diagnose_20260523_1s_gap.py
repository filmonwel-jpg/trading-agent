#!/usr/bin/env python3
from pathlib import Path
import csv
import os
import re

ROOT = Path('/Users/filmonghezehey/trading-agent/worktrees/databento')
TD = ROOT / 'training_data'
CHUNK_ROOT = TD / 'databento_30s_20260523_build_chunks'
DIRS = {
    '30s': TD / 'databento_30s_20260523',
    '5s': TD / 'databento_5s_20260523',
    '1s': TD / 'databento_1s_20260523',
}
EXPECTED = {
    '30s': TD / 'databento_30s_20260523_combined.csv',
    '5s': TD / 'databento_5s_20260523_combined.csv',
    '1s': TD / 'databento_1s_20260523_combined.csv',
    'plan': DIRS['30s'] / 'symbol_model_plan.csv',
}

def symbols(tf: str) -> set[str]:
    d = DIRS[tf]
    suffix = f'_{tf}_training.csv'
    if not d.exists():
        return set()
    return {p.name[:-len(suffix)] for p in d.glob(f'*{suffix}')}

def line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open('rb') as fh:
        return sum(1 for _ in fh)

def human_size(path: Path) -> str:
    if not path.exists():
        return 'MISSING'
    size = path.stat().st_size if path.is_file() else sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    n = float(size)
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f'{n:.1f}{unit}'
        n /= 1024

sets = {tf: symbols(tf) for tf in DIRS}
print('=== SYMBOL COUNTS ===')
for tf in ('30s', '5s', '1s'):
    print(f'{tf}: {len(sets[tf])} files in {DIRS[tf]}')

print('\n=== TOP-LEVEL OUTPUT FILES ===')
for name, path in EXPECTED.items():
    print(f'{name}: exists={path.is_file()} size={human_size(path)} lines={line_count(path)} path={path}')

missing_1s_vs_30s = sorted(sets['30s'] - sets['1s'])
missing_1s_vs_5s = sorted(sets['5s'] - sets['1s'])
print('\n=== 1S MISSING SYMBOLS ===')
print(f'missing_from_1s_vs_30s_count={len(missing_1s_vs_30s)}')
print(','.join(missing_1s_vs_30s))
print(f'missing_from_1s_vs_5s_count={len(missing_1s_vs_5s)}')
print(','.join(missing_1s_vs_5s))

print('\n=== 30S/5S SET DIFFERENCE ===')
print(f'5s_not_30s={len(sets["5s"] - sets["30s"])}: {",".join(sorted(sets["5s"] - sets["30s"]))}')
print(f'30s_not_5s={len(sets["30s"] - sets["5s"])}: {",".join(sorted(sets["30s"] - sets["5s"]))}')

print('\n=== MISSING 1S SYMBOLS: 30S/5S FILE PRESENCE AND SIZES ===')
for sym in missing_1s_vs_30s:
    p30 = DIRS['30s'] / f'{sym}_30s_training.csv'
    p5 = DIRS['5s'] / f'{sym}_5s_training.csv'
    p1 = DIRS['1s'] / f'{sym}_1s_training.csv'
    print(f'{sym}: 30s={p30.is_file()} {human_size(p30)}; 5s={p5.is_file()} {human_size(p5)}; 1s={p1.is_file()} {human_size(p1)}')

print('\n=== CHUNK PLANS CONTAINING MISSING 1S SYMBOLS ===')
missing_set = set(missing_1s_vs_30s)
for plan in sorted(CHUNK_ROOT.glob('chunk_*/symbol_model_plan.csv')):
    chunk = plan.parent.name
    found = []
    try:
        with plan.open(newline='') as fh:
            reader = csv.DictReader(fh)
            headers = reader.fieldnames or []
            sym_col = 'symbol' if 'symbol' in headers else ('Symbol' if 'Symbol' in headers else headers[0] if headers else None)
            for row in reader:
                sym = (row.get(sym_col) or '').strip() if sym_col else ''
                if sym in missing_set:
                    found.append(sym)
    except Exception as exc:
        print(f'{chunk}: ERROR reading plan: {exc}')
        continue
    print(f'{chunk}: missing_1s_symbols={len(found)} {",".join(found)}')

print('\n=== BUILD LOG ERROR/WARNING MARKERS ===')
patterns = re.compile(r'(traceback|error|exception|failed|no space|1s|combined_1s|Wrote 1s|_1s_training)', re.IGNORECASE)
for log in sorted(CHUNK_ROOT.glob('chunk_*/build.log')):
    print(f'--- {log}')
    matched = []
    try:
        with log.open(errors='replace') as fh:
            for line_no, line in enumerate(fh, 1):
                if patterns.search(line):
                    matched.append((line_no, line.rstrip()))
    except Exception as exc:
        print(f'ERROR reading log: {exc}')
        continue
    if not matched:
        print('no matching markers')
    else:
        for line_no, line in matched[-80:]:
            print(f'{line_no}: {line}')

print('\n=== CHUNK COMBINED OUTPUTS ===')
for chunk in sorted(CHUNK_ROOT.glob('chunk_*')):
    if not chunk.is_dir():
        continue
    parts = []
    for name in ('combined_30s.csv', 'combined_5s.csv', 'combined_1s.csv', 'symbol_model_plan.csv'):
        p = chunk / name
        parts.append(f'{name}:exists={p.is_file()} size={human_size(p)} lines={line_count(p)}')
    print(f'{chunk.name}: ' + ' | '.join(parts))

