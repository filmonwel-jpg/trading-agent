#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

require_dir() {
  local path="$1"
  local label="$2"
  if [[ ! -d "$path" ]]; then
    echo "ERROR: missing required $label directory: $path" >&2
    exit 1
  fi
}

LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
DOWNLOAD_ROOT="${DOWNLOAD_ROOT:-/Users/filmonghezehey/Downloads}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$LAKE_ROOT/model_training_sets}"

# Baseline sources used by the current DBN -> 1s/5s/30s builder.
RAW_EQUS_TBBO="${RAW_EQUS_TBBO:-$DOWNLOAD_ROOT/EQUS-20260523-6J9KE98BJ9}"
RAW_OPRA_OHLCV="${RAW_OPRA_OHLCV:-$DOWNLOAD_ROOT/OPRA-20260523-MSV68VKVKD}"

# Richer 20260612 sources are inventoried here, but not consumed by the current
# baseline builder until dedicated mbp-1/tcbbo/definition normalizers exist.
RAW_EQUS_DEFINITION="${RAW_EQUS_DEFINITION:-${RAW_EQUS_20260612_UNKNOWN:-$DOWNLOAD_ROOT/EQUS-20260612-GFHRSU6F48}}"
RAW_OPRA_DEFINITION="${RAW_OPRA_DEFINITION:-$DOWNLOAD_ROOT/OPRA-20260612-B5D4JV3GV6}"
RAW_EQUS_MBP1="${RAW_EQUS_MBP1:-$DOWNLOAD_ROOT/EQUS-20260612-36BEU4G7M8}"
RAW_OPRA_TCBBO="${RAW_OPRA_TCBBO:-$DOWNLOAD_ROOT/OPRA-20260612-KN5TPHB5EF}"

SYMBOLS="${SYMBOLS:-TSLA,TQQQ,NVDA,SPY,QQQ}"
MAX_DAYS="${MAX_DAYS:-0}"
BUILD_TS="${BUILD_TS:-$(date +%Y%m%d_%H%M%S)}"
BUILD_RUN_ID="${BUILD_RUN_ID:-core_full_window_bars_from_downloads_$BUILD_TS}"
BUILD_ROOT="${BUILD_ROOT:-$OUTPUT_ROOT/$BUILD_RUN_ID}"
RUNNER_PREFLIGHT_ONLY="${RUNNER_PREFLIGHT_ONLY:-0}"
RUN_CHAIN_AFTER_BUILD="${RUN_CHAIN_AFTER_BUILD:-0}"
COPY_RAW_DOWNLOADS="${COPY_RAW_DOWNLOADS:-0}"
USE_COPIED_RAW_FOR_BUILD="${USE_COPIED_RAW_FOR_BUILD:-0}"
WRITE_1S_OUTPUTS="${WRITE_1S_OUTPUTS:-0}"

MIN_UNIQUE_DAYS="${MIN_UNIQUE_DAYS:-100}"
MIN_30S_ROWS="${MIN_30S_ROWS:-100000}"
MIN_5S_ROWS="${MIN_5S_ROWS:-600000}"
MIN_OUTPUT_FREE_GB="${MIN_OUTPUT_FREE_GB:-50}"

echo "repo_root=$repo_root"
echo "LAKE_ROOT=$LAKE_ROOT"
echo "DOWNLOAD_ROOT=$DOWNLOAD_ROOT"
echo "OUTPUT_ROOT=$OUTPUT_ROOT"
echo "BUILD_ROOT=$BUILD_ROOT"
echo "SYMBOLS=$SYMBOLS"
echo "MAX_DAYS=$MAX_DAYS"
echo "RAW_SOURCE_COPY_POLICY=disabled_downloads_in_place"
echo "RUNNER_PREFLIGHT_ONLY=$RUNNER_PREFLIGHT_ONLY"
echo "RUN_CHAIN_AFTER_BUILD=$RUN_CHAIN_AFTER_BUILD"
echo "COPY_RAW_DOWNLOADS=$COPY_RAW_DOWNLOADS (must remain false)"
echo "USE_COPIED_RAW_FOR_BUILD=$USE_COPIED_RAW_FOR_BUILD (must remain false)"
echo "WRITE_1S_OUTPUTS=$WRITE_1S_OUTPUTS"
echo "RAW_EQUS_TBBO=$RAW_EQUS_TBBO"
echo "RAW_OPRA_OHLCV=$RAW_OPRA_OHLCV"
echo "RAW_EQUS_DEFINITION=$RAW_EQUS_DEFINITION"
echo "RAW_OPRA_DEFINITION=$RAW_OPRA_DEFINITION"
echo "RAW_EQUS_MBP1=$RAW_EQUS_MBP1"
echo "RAW_OPRA_TCBBO=$RAW_OPRA_TCBBO"

if truthy "$COPY_RAW_DOWNLOADS" || truthy "$USE_COPIED_RAW_FOR_BUILD"; then
  cat >&2 <<'EOF'
ERROR: source DBN copying is disabled for this runner.
Leave raw DBN source folders in Downloads and write only generated lakev2 outputs under OUTPUT_ROOT.
Unset COPY_RAW_DOWNLOADS and USE_COPIED_RAW_FOR_BUILD, then rerun.
EOF
  exit 1
fi

require_dir "$RAW_EQUS_TBBO" "EQUS tbbo 20260523"
require_dir "$RAW_OPRA_OHLCV" "OPRA ohlcv-1s 20260523"

python3 - "$OUTPUT_ROOT" "$MIN_OUTPUT_FREE_GB" <<'PY'
import shutil
import sys
from pathlib import Path

output_root = Path(sys.argv[1])
min_free_gb = float(sys.argv[2])
probe = output_root
while not probe.exists() and probe.parent != probe:
    probe = probe.parent
usage = shutil.disk_usage(probe)
free_gb = usage.free / (1024 ** 3)
print(f"OUTPUT_DISK_FREE_GB={free_gb:.2f} path={probe}")
if free_gb < min_free_gb:
    raise SystemExit(f"ERROR: insufficient free space at {probe}: {free_gb:.2f}GB < {min_free_gb:.2f}GB")
PY

mkdir -p "$BUILD_ROOT"

write_raw_manifest() {
  python3 - "$BUILD_ROOT/raw_download_manifest.json" \
    "$RAW_EQUS_TBBO" "$RAW_OPRA_OHLCV" "$RAW_EQUS_DEFINITION" \
    "$RAW_OPRA_DEFINITION" "$RAW_EQUS_MBP1" "$RAW_OPRA_TCBBO" <<'PY'
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

manifest_path = Path(sys.argv[1])
source_paths = [Path(arg) for arg in sys.argv[2:]]
known = {
    "EQUS-20260523-6J9KE98BJ9": {
        "dataset": "EQUS.MINI",
        "schema": "tbbo",
        "role": "current baseline equity trade/top-of-book source used by build_30s_from_5s_csv.py",
    },
    "OPRA-20260523-MSV68VKVKD": {
        "dataset": "OPRA.PILLAR",
        "schema": "ohlcv-1s",
        "role": "current baseline compact option-volume source used by build_30s_from_5s_csv.py",
    },
    "EQUS-20260612-36BEU4G7M8": {
        "dataset": "EQUS.MINI",
        "schema": "mbp-1",
        "role": "richer equity quote-state source; inventoried only until normalizer exists",
    },
    "OPRA-20260612-KN5TPHB5EF": {
        "dataset": "OPRA.PILLAR",
        "schema": "tcbbo",
        "role": "richer option trade/quote-state source; inventoried only until normalizer exists",
    },
    "OPRA-20260612-B5D4JV3GV6": {
        "dataset": "OPRA.PILLAR",
        "schema": "definition",
        "role": "option definition source; inventoried only until metadata reader exists",
    },
    "EQUS-20260612-GFHRSU6F48": {
        "dataset": "EQUS.MINI",
        "schema": "definition",
        "role": "equity definition source; inventoried only until definition metadata reader exists",
    },
}

def infer_schemas(files):
    schemas = set()
    dates = set()
    for path in files:
        match = re.search(r"-(\d{8})\.([^.]+)\.dbn(?:\.zst)?$", path.name)
        if match:
            dates.add(match.group(1))
            schemas.add(match.group(2))
    return sorted(schemas), sorted(dates)

entries = []
for source in source_paths:
    files = sorted([p for p in source.rglob("*.dbn*") if p.is_file()]) if source.is_dir() else []
    schemas, dates = infer_schemas(files)
    entries.append({
        "path": str(source),
        "folder_name": source.name,
        "exists": source.is_dir(),
        "documented": source.name in known,
        "known_info": known.get(source.name, {}),
        "file_count": len(files),
        "total_bytes": sum(p.stat().st_size for p in files),
        "total_gib": round(sum(p.stat().st_size for p in files) / (1024 ** 3), 3),
        "inferred_schemas_from_filenames": schemas,
        "dbn_dates": len(dates),
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "sample_files": [p.name for p in files[:10]],
    })

manifest = {
    "schema_version": "raw_databento_download_manifest_v1",
    "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "raw_source_copy_policy": "disabled_downloads_in_place",
    "note": (
        "Only the 20260523 tbbo/ohlcv-1s folders are consumed by the current baseline CSV builder. "
        "The 20260612 mbp-1/tcbbo/equity-definition/option-definition folders are inventory inputs for future normalizers. "
        "EQUS-20260612-GFHRSU6F48 is the EQUS.MINI definition folder. "
        "Source DBN folders stay in Downloads; this runner writes only manifests and generated bar/model outputs under the lakev2 output root."
    ),
    "sources": entries,
}
manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print(f"WROTE {manifest_path}")
for entry in entries:
    print(
        "RAW_SOURCE "
        f"folder={entry['folder_name']} exists={entry['exists']} files={entry['file_count']} "
        f"gib={entry['total_gib']} schemas={','.join(entry['inferred_schemas_from_filenames']) or 'unknown'} "
        f"dates={entry['dbn_dates']}"
    )
PY
}

write_raw_manifest

if truthy "$RUNNER_PREFLIGHT_ONLY"; then
  echo "PREFLIGHT_ONLY: validated required Downloads DBN folders, output free space, and wrote raw manifest; no source DBN copy attempted; skipping DBN build."
  exit 0
fi

printf '%s\n' "$SYMBOLS" | tr ',' '\n' | awk 'NF {gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print toupper($0)}' > "$BUILD_ROOT/symbols_selected.txt"

builder_args=(
  --dbeq-dir "$RAW_EQUS_TBBO"
  --opra-dir "$RAW_OPRA_OHLCV"
  --output-dir "$BUILD_ROOT/data_30s"
  --output-dir-5s "$BUILD_ROOT/data_5s"
  --combined-output-csv "$BUILD_ROOT/combined_30s.csv"
  --combined-output-csv-5s "$BUILD_ROOT/combined_5s.csv"
  --assessment-report "$BUILD_ROOT/symbol_model_plan.csv"
  --symbols-file "$BUILD_ROOT/symbols_selected.txt"
  --max-days "$MAX_DAYS"
)
if truthy "$WRITE_1S_OUTPUTS"; then
  builder_args+=(
    --output-dir-1s "$BUILD_ROOT/data_1s"
    --combined-output-csv-1s "$BUILD_ROOT/combined_1s.csv"
  )
fi

python3 -u build_30s_from_5s_csv.py "${builder_args[@]}" 2>&1 | tee "$BUILD_ROOT/build.log"

python3 - "$BUILD_ROOT" "$MIN_UNIQUE_DAYS" "$MIN_30S_ROWS" "$MIN_5S_ROWS" <<'PY'
import json
import sys
from pathlib import Path

import pandas as pd

build_root = Path(sys.argv[1])
min_days = int(sys.argv[2])
min_30s = int(sys.argv[3])
min_5s = int(sys.argv[4])

def summarize(path: Path, cadence: str) -> dict[str, object]:
    if not path.is_file() or path.stat().st_size <= 0:
        raise SystemExit(f"ERROR: missing/empty combined {cadence} CSV: {path}")
    rows = 0
    symbols = set()
    dates = set()
    for chunk in pd.read_csv(path, usecols=lambda col: col in {"Symbol", "Date", "Timestamp"}, chunksize=200_000):
        rows += len(chunk)
        if "Symbol" in chunk.columns:
            symbols.update(chunk["Symbol"].dropna().astype(str).str.upper().unique().tolist())
        if "Date" in chunk.columns:
            dates.update(chunk["Date"].dropna().astype(str).unique().tolist())
        elif "Timestamp" in chunk.columns:
            raw = chunk["Timestamp"].dropna().astype(str).str.extract(r"(\d{8}|\d{4}-\d{2}-\d{2})", expand=False).dropna()
            dates.update(raw.str.replace(r"^(\d{4})(\d{2})(\d{2})$", r"\1-\2-\3", regex=True).unique().tolist())
    return {"cadence": cadence, "path": str(path), "rows": rows, "symbols": sorted(symbols), "unique_days": len(dates), "first_date": min(dates) if dates else None, "last_date": max(dates) if dates else None}

s30 = summarize(build_root / "combined_30s.csv", "30s")
s5 = summarize(build_root / "combined_5s.csv", "5s")
errors = []
if s30["rows"] < min_30s:
    errors.append(f"30s rows too low: {s30['rows']} < {min_30s}")
if s5["rows"] < min_5s:
    errors.append(f"5s rows too low: {s5['rows']} < {min_5s}")
if s30["unique_days"] < min_days:
    errors.append(f"30s unique days too low: {s30['unique_days']} < {min_days}")
if s5["unique_days"] < min_days:
    errors.append(f"5s unique days too low: {s5['unique_days']} < {min_days}")
manifest = {"schema_version": "core_full_window_bar_build_manifest_v1", "build_root": str(build_root), "cadences": {"30s": s30, "5s": s5}, "errors": errors}
(build_root / "bar_build_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print("BAR_BUILD_SUMMARY", json.dumps(manifest, sort_keys=True))
if errors:
    raise SystemExit("ERROR: bar build validation failed: " + "; ".join(errors))
print("BAR_BUILD_VALIDATION_OK", f"build_root={build_root}")
PY

if truthy "$RUN_CHAIN_AFTER_BUILD"; then
  echo "RUN_CHAIN_AFTER_BUILD=1: launching broader/full-window cost-aware chain from built CSVs."
  SOURCE_30S="$BUILD_ROOT/combined_30s.csv" \
  SOURCE_5S="$BUILD_ROOT/combined_5s.csv" \
  CHAIN_RUN_ID="${CHAIN_RUN_ID:-broader_full_window_cost_aware_from_dbn_$BUILD_TS}" \
  bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh
else
  cat <<EOF
DONE: built broader/core CSVs from DBN Downloads without copying raw source folders.

Next command:

  export SOURCE_30S="$BUILD_ROOT/combined_30s.csv"
  export SOURCE_5S="$BUILD_ROOT/combined_5s.csv"
  bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh

EOF
fi

echo "DONE: $BUILD_ROOT"
