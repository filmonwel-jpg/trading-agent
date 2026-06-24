#!/usr/bin/env python3
"""Split a combined Databento bar CSV into per-symbol lifecycle training files.

The lifecycle/micro trainer streams directories containing files named
<SYMBOL>_<cadence>_training.csv. This utility creates that layout from a large
combined_30s.csv or combined_5s.csv without loading the full file into memory.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-csv', type=Path, required=True, help='Combined input CSV containing Symbol and Timestamp columns.')
    parser.add_argument('--output-dir', type=Path, required=True, help='Directory to write <SYMBOL>_<cadence>_training.csv files.')
    parser.add_argument('--cadence', required=True, choices=['1s', '5s', '30s'], help='Cadence suffix for output filenames.')
    parser.add_argument('--symbols', default='', help='Optional comma-separated symbol allowlist. Defaults to all symbols in the CSV.')
    parser.add_argument('--chunksize', type=int, default=250_000, help='Rows per streaming chunk.')
    parser.add_argument('--overwrite', action='store_true', help='Delete existing files for this cadence before writing.')
    parser.add_argument('--manifest-json', type=Path, default=None, help='Optional manifest path. Defaults to output-dir/split_manifest_<cadence>.json.')
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def split_symbols(raw: str) -> set[str]:
    return {token.strip().upper() for token in str(raw or '').split(',') if token.strip()}


def update_range(current_min: str | None, current_max: str | None, values: pd.Series) -> tuple[str | None, str | None]:
    if values.empty:
        return current_min, current_max
    as_str = values.astype(str)
    chunk_min = as_str.min()
    chunk_max = as_str.max()
    next_min = chunk_min if current_min is None else min(current_min, chunk_min)
    next_max = chunk_max if current_max is None else max(current_max, chunk_max)
    return str(next_min), str(next_max)


def output_path(output_dir: Path, symbol: str, cadence: str) -> Path:
    safe_symbol = ''.join(ch for ch in symbol.upper() if ch.isalnum() or ch in {'_', '-', '.'})
    if not safe_symbol:
        raise ValueError(f'Invalid empty symbol after sanitizing {symbol!r}')
    return output_dir / f'{safe_symbol}_{cadence}_training.csv'


def main() -> int:
    args = parse_args()
    input_csv = args.input_csv.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    manifest_json = (args.manifest_json.expanduser().resolve() if args.manifest_json else output_dir / f'split_manifest_{args.cadence}.json')
    allowed_symbols = split_symbols(args.symbols)

    if not input_csv.exists():
        raise FileNotFoundError(f'Missing input CSV: {input_csv}')
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for old in output_dir.glob(f'*_{args.cadence}_training.csv'):
            old.unlink()

    stats: dict[str, dict[str, Any]] = {}
    chunks = 0
    rows_in = 0
    rows_written = 0
    for chunk in pd.read_csv(input_csv, chunksize=max(1, args.chunksize)):
        chunks += 1
        rows_in += len(chunk)
        if 'Symbol' not in chunk.columns:
            raise ValueError(f'{input_csv} is missing required Symbol column')
        chunk = chunk.copy()
        chunk['Symbol'] = chunk['Symbol'].astype(str).str.strip().str.upper()
        if allowed_symbols:
            chunk = chunk[chunk['Symbol'].isin(allowed_symbols)].copy()
        if chunk.empty:
            print(f'SPLIT_PROGRESS cadence={args.cadence} chunk={chunks} rows_in={rows_in} rows_written={rows_written}', flush=True)
            continue
        for symbol, group in chunk.groupby('Symbol', sort=True):
            target = output_path(output_dir, symbol, args.cadence)
            header = not target.exists()
            group.to_csv(target, mode='a', header=header, index=False)
            item = stats.setdefault(symbol, {'rows': 0, 'date_min': None, 'date_max': None, 'timestamp_min': None, 'timestamp_max': None})
            item['rows'] += int(len(group))
            if 'Date' in group.columns:
                item['date_min'], item['date_max'] = update_range(item['date_min'], item['date_max'], group['Date'])
            if 'Timestamp' in group.columns:
                item['timestamp_min'], item['timestamp_max'] = update_range(item['timestamp_min'], item['timestamp_max'], group['Timestamp'])
            rows_written += int(len(group))
        print(f'SPLIT_PROGRESS cadence={args.cadence} chunk={chunks} rows_in={rows_in} rows_written={rows_written}', flush=True)

    for symbol, item in stats.items():
        path = output_path(output_dir, symbol, args.cadence)
        item['path'] = str(path)
        item['size_bytes'] = int(path.stat().st_size) if path.exists() else 0

    manifest = {
        'schema_version': 'combined_bars_by_symbol_split_v1',
        'generated_at_utc': utc_now(),
        'input_csv': str(input_csv),
        'input_size_bytes': int(input_csv.stat().st_size),
        'output_dir': str(output_dir),
        'cadence': args.cadence,
        'allowed_symbols': sorted(allowed_symbols),
        'chunksize': int(args.chunksize),
        'chunks': int(chunks),
        'rows_in': int(rows_in),
        'rows_written': int(rows_written),
        'symbols': stats,
    }
    manifest_json.parent.mkdir(parents=True, exist_ok=True)
    manifest_json.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(f'WROTE {manifest_json}')
    print(f'SPLIT_DONE cadence={args.cadence} symbols={len(stats)} rows_written={rows_written} output_dir={output_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
