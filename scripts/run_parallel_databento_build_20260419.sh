#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
export ROOT
PYTHON_BIN="${PYTHON_BIN:-python3}"
EQUS_DIR="/Users/filmonghezehey/Downloads/EQUS-20260419-LWMAP766M4"
OPRA_DIR="/Users/filmonghezehey/Downloads/OPRA-20260419-PSJKYXSJ7W"
SYMBOLS_FILE="$ROOT/databento_ibkr_bridge/config/symbols_100.txt"
FINAL_DATA_DIR="$ROOT/training_data/databento_30s_20260419"
FINAL_COMBINED_CSV="$ROOT/training_data/databento_30s_20260419_combined.csv"
FINAL_PLAN_CSV="$FINAL_DATA_DIR/symbol_model_plan.csv"
CHUNK_ROOT="$ROOT/training_data/databento_30s_20260419_build_chunks"
CHUNK_COUNT=4

rm -rf "$FINAL_DATA_DIR" "$FINAL_COMBINED_CSV" "$CHUNK_ROOT"
mkdir -p "$CHUNK_ROOT" "$FINAL_DATA_DIR"

"$PYTHON_BIN" - <<'PY'
from pathlib import Path
import os
root = Path(os.environ['ROOT'])
symbols_file = root / 'databento_ibkr_bridge' / 'config' / 'symbols_100.txt'
chunk_root = root / 'training_data' / 'databento_30s_20260419_build_chunks'
chunk_count = 4
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
  chunk_data_dir="$chunk_dir/data"
  chunk_combined_csv="$chunk_dir/combined.csv"
  chunk_plan_csv="$chunk_dir/symbol_model_plan.csv"
  chunk_log="$chunk_dir/build.log"
  mkdir -p "$chunk_dir"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] START $chunk_name symbols_file=$chunk_symbols"
  (
    cd "$ROOT"
    "$PYTHON_BIN" build_30s_from_5s_csv.py \
      --dbeq-dir "$EQUS_DIR" \
      --opra-dir "$OPRA_DIR" \
      --output-dir "$chunk_data_dir" \
      --combined-output-csv "$chunk_combined_csv" \
      --assessment-report "$chunk_plan_csv" \
      --symbols-file "$chunk_symbols"
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
chunk_root = root / 'training_data' / 'databento_30s_20260419_build_chunks'
final_data_dir = root / 'training_data' / 'databento_30s_20260419'
final_combined_csv = root / 'training_data' / 'databento_30s_20260419_combined.csv'
final_plan_csv = final_data_dir / 'symbol_model_plan.csv'

for csv_path in sorted(chunk_root.glob('chunk_*/data/*_30s_training.csv')):
    shutil.copy2(csv_path, final_data_dir / csv_path.name)

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

combined_paths = sorted(chunk_root.glob('chunk_*/combined.csv'))
header_written = False
with final_combined_csv.open('w', encoding='utf-8', newline='') as out_fh:
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
print(f'WROTE {final_combined_csv}')
print(f'CSV_COUNT={len(list(final_data_dir.glob("*_30s_training.csv")))}')
PY

echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARALLEL_DATABENTO_BUILD_COMPLETE"

