#!/usr/bin/env python3
import os
from pathlib import Path

ROOT = Path(os.environ.get('TRADING_AGENT_ROOT', Path(__file__).resolve().parents[1]))
SRC = ROOT / 'training_data/databento_30s_20260523_build_chunks'
DST = Path('/Volumes/DatabentoVault/trading-agent-offload/databento/training_data/databento_30s_20260523_build_chunks')

print('=== PATHS ===')
print(f'source_exists={SRC.is_dir()} source={SRC}')
print(f'dest_exists={DST.is_dir()} dest={DST}')

print('\n=== REQUIRED ARTIFACTS CHECK ===')
missing = []
for chunk in ['chunk_01', 'chunk_02', 'chunk_03', 'chunk_04']:
    for name in ['combined_30s.csv', 'combined_5s.csv', 'combined_1s.csv', 'symbol_model_plan.csv', 'build.log']:
        p = DST / chunk / name
        ok = p.is_file() and p.stat().st_size > 0
        size = p.stat().st_size if p.exists() else 0
        print(f'{chunk}/{name}: {"OK" if ok else "MISSING_OR_EMPTY"} size={size}')
        if not ok:
            missing.append(str(p))

print('\n=== MANIFEST SIZE COMPARISON ===')
src_files = {p.relative_to(SRC).as_posix(): p.stat().st_size for p in SRC.rglob('*') if p.is_file()}
dst_files = {p.relative_to(DST).as_posix(): p.stat().st_size for p in DST.rglob('*') if p.is_file()}
missing_in_dest = sorted(set(src_files) - set(dst_files))
extra_in_dest = sorted(set(dst_files) - set(src_files))
size_mismatch = sorted(k for k in set(src_files) & set(dst_files) if src_files[k] != dst_files[k])
print(f'source_files={len(src_files)}')
print(f'dest_files={len(dst_files)}')
print(f'missing_in_dest={len(missing_in_dest)}')
print(f'extra_in_dest={len(extra_in_dest)}')
print(f'size_mismatch={len(size_mismatch)}')
if missing_in_dest:
    print('missing sample:', missing_in_dest[:10])
if extra_in_dest:
    print('extra sample:', extra_in_dest[:10])
if size_mismatch:
    print('mismatch sample:', [(k, src_files[k], dst_files[k]) for k in size_mismatch[:10]])

print('\nVERDICT=' + ('OK' if not missing and not missing_in_dest and not extra_in_dest and not size_mismatch else 'NOT_OK'))

