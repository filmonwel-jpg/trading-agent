#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import csv
import os
import shutil
import sys
import time

ROOT = Path(os.environ.get('TRADING_AGENT_ROOT', Path(__file__).resolve().parents[1]))
TRAINING = ROOT / 'training_data'
CHUNK_ROOT = TRAINING / 'databento_30s_20260523_build_chunks'
FINAL_DIRS = {
    '30s': TRAINING / 'databento_30s_20260523',
    '5s': TRAINING / 'databento_5s_20260523',
    '1s': TRAINING / 'databento_1s_20260523',
}
FINAL_COMBINED = {
    '30s': TRAINING / 'databento_30s_20260523_combined.csv',
    '5s': TRAINING / 'databento_5s_20260523_combined.csv',
    '1s': TRAINING / 'databento_1s_20260523_combined.csv',
}
FINAL_PLAN = FINAL_DIRS['30s'] / 'symbol_model_plan.csv'
CADENCES = ('30s', '5s', '1s')
CHUNKS = ('chunk_01', 'chunk_02', 'chunk_03', 'chunk_04')


def log(message: str) -> None:
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {message}', flush=True)


def ensure_ready() -> None:
    if not CHUNK_ROOT.exists():
        raise SystemExit(f'chunk root is missing: {CHUNK_ROOT}')
    if CHUNK_ROOT.is_symlink():
        target = os.readlink(CHUNK_ROOT)
        log(f'chunk root is symlink -> {target}')
    for chunk in CHUNKS:
        chunk_dir = CHUNK_ROOT / chunk
        if not chunk_dir.is_dir():
            raise SystemExit(f'missing chunk directory: {chunk_dir}')
        for cadence in CADENCES:
            combined = chunk_dir / f'combined_{cadence}.csv'
            data_dir = chunk_dir / f'data_{cadence}'
            if not combined.is_file() or combined.stat().st_size <= 0:
                raise SystemExit(f'missing/empty {combined}')
            if not data_dir.is_dir():
                raise SystemExit(f'missing {data_dir}')
        plan = chunk_dir / 'symbol_model_plan.csv'
        if not plan.is_file() or plan.stat().st_size <= 0:
            raise SystemExit(f'missing/empty {plan}')
    for d in FINAL_DIRS.values():
        d.mkdir(parents=True, exist_ok=True)


def copy_per_symbol_outputs() -> None:
    log('START copy/repair per-symbol outputs')
    copied = 0
    skipped = 0
    mismatched = 0
    for cadence in CADENCES:
        final_dir = FINAL_DIRS[cadence]
        for chunk in CHUNKS:
            data_dir = CHUNK_ROOT / chunk / f'data_{cadence}'
            for src in sorted(data_dir.glob(f'*_{cadence}_training.csv')):
                dst = final_dir / src.name
                if dst.is_file() and dst.stat().st_size == src.stat().st_size:
                    skipped += 1
                    continue
                if dst.exists():
                    mismatched += 1
                    log(f'RECOPY size mismatch/existing: {dst.name}')
                else:
                    log(f'COPY missing: {dst.name}')
                tmp = dst.with_name(dst.name + '.tmp_copy')
                if tmp.exists():
                    tmp.unlink()
                shutil.copy2(src, tmp)
                os.replace(tmp, dst)
                copied += 1
    log(f'DONE copy/repair per-symbol outputs copied={copied} skipped={skipped} mismatched={mismatched}')


def merge_plan() -> None:
    log('START merge symbol_model_plan.csv')
    rows: list[dict[str, str]] = []
    fieldnames: list[str] | None = None
    for chunk in CHUNKS:
        plan = CHUNK_ROOT / chunk / 'symbol_model_plan.csv'
        with plan.open(newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                raise SystemExit(f'plan has no header: {plan}')
            if fieldnames is None:
                fieldnames = list(reader.fieldnames)
            elif list(reader.fieldnames) != fieldnames:
                raise SystemExit(f'plan header mismatch in {plan}: {reader.fieldnames} != {fieldnames}')
            rows.extend(dict(row) for row in reader)
    if not fieldnames:
        raise SystemExit('no plan fieldnames found')

    symbol_col = 'Symbol' if 'Symbol' in fieldnames else ('symbol' if 'symbol' in fieldnames else fieldnames[0])
    dedup: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = (row.get(symbol_col) or '').strip().upper()
        if symbol:
            row[symbol_col] = symbol
            dedup[symbol] = row
    merged = [dedup[sym] for sym in sorted(dedup)]
    tmp = FINAL_PLAN.with_name(FINAL_PLAN.name + '.tmp')
    with tmp.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged)
    os.replace(tmp, FINAL_PLAN)
    log(f'DONE merge plan rows={len(merged)} path={FINAL_PLAN}')


def concatenate_combined(cadence: str) -> None:
    output = FINAL_COMBINED[cadence]
    chunk_paths = [CHUNK_ROOT / chunk / f'combined_{cadence}.csv' for chunk in CHUNKS]
    tmp = output.with_name(output.name + '.tmp')
    if tmp.exists():
        log(f'REMOVE stale tmp {tmp}')
        tmp.unlink()
    log(f'START concatenate {cadence} -> {output}')
    total_bytes = 0
    header_written = False
    with tmp.open('wb') as out_fh:
        for idx, combined in enumerate(chunk_paths, start=1):
            size = combined.stat().st_size
            log(f'  append {cadence} chunk={idx} size={size} path={combined}')
            with combined.open('rb') as in_fh:
                header = in_fh.readline()
                if not header:
                    continue
                if not header_written:
                    out_fh.write(header)
                    total_bytes += len(header)
                    header_written = True
                shutil.copyfileobj(in_fh, out_fh, length=16 * 1024 * 1024)
                total_bytes += max(0, size - len(header))
    os.replace(tmp, output)
    log(f'DONE concatenate {cadence} bytes_written_approx={total_bytes} path={output}')


def verify_counts() -> None:
    log('START verification counts')
    for cadence, final_dir in FINAL_DIRS.items():
        count = len(list(final_dir.glob(f'*_{cadence}_training.csv')))
        log(f'CSV_COUNT_{cadence.upper()}={count} dir={final_dir}')
        if count != 99:
            raise SystemExit(f'expected 99 {cadence} CSVs, found {count}')
    if not FINAL_PLAN.is_file() or FINAL_PLAN.stat().st_size <= 0:
        raise SystemExit(f'missing/empty plan: {FINAL_PLAN}')
    for cadence, output in FINAL_COMBINED.items():
        if not output.is_file() or output.stat().st_size <= 0:
            raise SystemExit(f'missing/empty combined {cadence}: {output}')
        log(f'COMBINED_{cadence.upper()}_SIZE={output.stat().st_size} path={output}')
    log('DONE verification counts')


def main() -> int:
    log('finish_20260523_outputs_from_chunks START')
    ensure_ready()
    copy_per_symbol_outputs()
    merge_plan()
    for cadence in CADENCES:
        concatenate_combined(cadence)
    verify_counts()
    log('finish_20260523_outputs_from_chunks COMPLETE')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

