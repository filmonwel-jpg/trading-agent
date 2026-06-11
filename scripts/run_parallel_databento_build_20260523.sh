#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
PYTHON_BIN="${PYTHON_BIN:-/Users/filmonghezehey/miniforge3/bin/python3}"
EQUS_DIR="/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9"
OPRA_DIR="/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD"
SYMBOLS_FILE="$ROOT/databento_ibkr_bridge/config/symbols_100.txt"
FINAL_30S_DATA_DIR="$ROOT/training_data/databento_30s_20260523"
FINAL_5S_DATA_DIR="$ROOT/training_data/databento_5s_20260523"
FINAL_1S_DATA_DIR="$ROOT/training_data/databento_1s_20260523"
FINAL_COMBINED_30S_CSV="$ROOT/training_data/databento_30s_20260523_combined.csv"
FINAL_COMBINED_5S_CSV="$ROOT/training_data/databento_5s_20260523_combined.csv"
FINAL_COMBINED_1S_CSV="$ROOT/training_data/databento_1s_20260523_combined.csv"
FINAL_PLAN_CSV="$FINAL_30S_DATA_DIR/symbol_model_plan.csv"
CHUNK_ROOT="$ROOT/training_data/databento_30s_20260523_build_chunks"
CHUNK_COUNT="${CHUNK_COUNT:-4}"
MAX_DAYS="${MAX_DAYS:-0}"

if [[ ! -d "$EQUS_DIR" ]]; then
  echo "Missing EQUS_DIR: $EQUS_DIR" >&2
  exit 1
fi
if [[ ! -d "$OPRA_DIR" ]]; then
  echo "Missing OPRA_DIR: $OPRA_DIR" >&2
  exit 1
fi
if [[ ! -f "$SYMBOLS_FILE" ]]; then
  echo "Missing SYMBOLS_FILE: $SYMBOLS_FILE" >&2
  exit 1
fi

rm -rf \
  "$FINAL_30S_DATA_DIR" "$FINAL_5S_DATA_DIR" "$FINAL_1S_DATA_DIR" \
  "$FINAL_COMBINED_30S_CSV" "$FINAL_COMBINED_5S_CSV" "$FINAL_COMBINED_1S_CSV" \
  "$CHUNK_ROOT"
mkdir -p "$CHUNK_ROOT" "$FINAL_30S_DATA_DIR" "$FINAL_5S_DATA_DIR" "$FINAL_1S_DATA_DIR"

"$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
root = Path(os.environ['ROOT'])
symbols_file = root / 'databento_ibkr_bridge' / 'config' / 'symbols_100.txt'
chunk_root = root / 'training_data' / 'databento_30s_20260523_build_chunks'
chunk_count = int(__import__('os').environ.get('CHUNK_COUNT', '4'))
symbols = [line.strip().upper() for line in symbols_file.read_text(encoding='utf-8').splitlines() if line.strip() and not line.lstrip().startswith('#')]
chunks = [[] for _ in range(chunk_count)]
for idx, symbol in enumerate(symbols):
    chunks[idx % chunk_count].append(symbol)
for idx, chunk in enumerate(chunks, start=1):
    chunk_path = chunk_root / f'symbols_chunk_{idx:02d}.txt'
    chunk_path.write_text('\n'.join(chunk) + '\n', encoding='utf-8')
    print(f'WROTE {chunk_path} symbols={len(chunk)}')
PY

pids=()
for idx in $(seq 1 "$CHUNK_COUNT"); do
  chunk_name=$(printf 'chunk_%02d' "$idx")
  chunk_dir="$CHUNK_ROOT/$chunk_name"
  chunk_symbols="$CHUNK_ROOT/$(printf 'symbols_chunk_%02d.txt' "$idx")"
  chunk_data_30s_dir="$chunk_dir/data_30s"
  chunk_data_5s_dir="$chunk_dir/data_5s"
  chunk_data_1s_dir="$chunk_dir/data_1s"
  chunk_combined_30s_csv="$chunk_dir/combined_30s.csv"
  chunk_combined_5s_csv="$chunk_dir/combined_5s.csv"
  chunk_combined_1s_csv="$chunk_dir/combined_1s.csv"
  chunk_plan_csv="$chunk_dir/symbol_model_plan.csv"
  chunk_log="$chunk_dir/build.log"
  mkdir -p "$chunk_dir"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START $chunk_name symbols_file=$chunk_symbols"
  (
    cd "$ROOT"
    "$PYTHON_BIN" build_30s_from_5s_csv.py \
      --dbeq-dir "$EQUS_DIR" \
      --opra-dir "$OPRA_DIR" \
      --output-dir "$chunk_data_30s_dir" \
      --output-dir-5s "$chunk_data_5s_dir" \
      --output-dir-1s "$chunk_data_1s_dir" \
      --combined-output-csv "$chunk_combined_30s_csv" \
      --combined-output-csv-5s "$chunk_combined_5s_csv" \
      --combined-output-csv-1s "$chunk_combined_1s_csv" \
      --assessment-report "$chunk_plan_csv" \
      --symbols-file "$chunk_symbols" \
      --max-days "$MAX_DAYS"
  ) >"$chunk_log" 2>&1 &
  pids+=("$!")
done

rc=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    rc=1
  fi
done
if [[ "$rc" -ne 0 ]]; then
  echo "One or more chunk builds failed. See $CHUNK_ROOT/chunk_*/build.log" >&2
  exit "$rc"
fi

"$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
import pandas as pd
import shutil

root = Path(os.environ['ROOT'])
chunk_root = root / 'training_data' / 'databento_30s_20260523_build_chunks'
final_dirs = {
    '30s': root / 'training_data' / 'databento_30s_20260523',
    '5s': root / 'training_data' / 'databento_5s_20260523',
    '1s': root / 'training_data' / 'databento_1s_20260523',
}
final_combined = {
    '30s': root / 'training_data' / 'databento_30s_20260523_combined.csv',
    '5s': root / 'training_data' / 'databento_5s_20260523_combined.csv',
    '1s': root / 'training_data' / 'databento_1s_20260523_combined.csv',
}
final_plan_csv = final_dirs['30s'] / 'symbol_model_plan.csv'

for cadence, final_dir in final_dirs.items():
    for csv_path in sorted(chunk_root.glob(f'chunk_*/data_{cadence}/*_{cadence}_training.csv')):
        shutil.copy2(csv_path, final_dir / csv_path.name)

plan_frames = []
for plan_path in sorted(chunk_root.glob('chunk_*/symbol_model_plan.csv')):
    if plan_path.exists() and plan_path.stat().st_size > 0:
        plan_frames.append(pd.read_csv(plan_path))
if not plan_frames:
    raise SystemExit('No chunk symbol_model_plan.csv files were produced.')
plan = pd.concat(plan_frames, ignore_index=True)
if 'Symbol' in plan.columns:
    plan['Symbol'] = plan['Symbol'].astype(str).str.upper()
    plan = plan.sort_values('Symbol').drop_duplicates(subset=['Symbol'], keep='last')
plan.to_csv(final_plan_csv, index=False)

for cadence, output_path in final_combined.items():
    combined_paths = sorted(chunk_root.glob(f'chunk_*/combined_{cadence}.csv'))
    header_written = False
    with output_path.open('w', encoding='utf-8', newline='') as out_fh:
        for combined_path in combined_paths:
            if not combined_path.exists():
                continue
            with combined_path.open('r', encoding='utf-8', newline='') as in_fh:
                header = in_fh.readline()
                if not header:
                    continue
                if not header_written:
                    out_fh.write(header)
                    header_written = True
                for line in in_fh:
                    out_fh.write(line)

print(f'WROTE {final_plan_csv}')
for cadence, output_path in final_combined.items():
    print(f'WROTE {output_path}')
    print(f'CSV_COUNT_{cadence.upper()}={len(list(final_dirs[cadence].glob(f"*_{cadence}_training.csv")))}')
PY

echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARALLEL_DATABENTO_20260523_BUILD_COMPLETE"

