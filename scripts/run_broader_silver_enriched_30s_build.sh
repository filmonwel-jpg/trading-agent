#!/usr/bin/env zsh
# Build a broader Databento six-source silver-enriched 30s dataset.
#
# This script is intended for the write-capable Databento computer. It writes
# only under LAKE_ROOT by default and never trains or exports ONNX models.

set -euo pipefail

log() {
  printf '\n== %s %s ==\n' "$*" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

cd "$(git rev-parse --show-toplevel)"

export LAKE_ROOT="${LAKE_ROOT:-/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2}"
export SOURCE_MANIFEST_ROOT="${SOURCE_MANIFEST_ROOT:-$LAKE_ROOT/source_manifests/source_inventory_hashes_six_source_downloads_20260617_183630}"
export BASELINE_30S="${BASELINE_30S:-$LAKE_ROOT/model_training_sets/broader_full_window_cost_aware_20260616_115143/input_slice/combined_30s.csv}"
export SYMBOLS="${SYMBOLS:-TSLA,TQQQ,NVDA,SPY,QQQ}"
export BROADER_DAYS="${BROADER_DAYS:-213}"
export PLAN_STRATEGY="${PLAN_STRATEGY:-latest}"
export SMOKE_DATE="${SMOKE_DATE:-2026-05-21}"
export RUN_STAMP="${RUN_STAMP:-$(date +%Y%m%d_%H%M%S)}"

export PLAN_ROOT="${PLAN_ROOT:-$LAKE_ROOT/source_manifests/broader_${BROADER_DAYS}d_six_source_plan_$RUN_STAMP}"
export SILVER_ROOT="${SILVER_ROOT:-$LAKE_ROOT/silver/broader_${BROADER_DAYS}d_six_source_phase1_$RUN_STAMP}"
export ENRICHED_30S_ROOT="${ENRICHED_30S_ROOT:-$LAKE_ROOT/model_training_sets/broader_${BROADER_DAYS}d_six_source_enriched_30s_$RUN_STAMP}"

# Optional path rewrite for source manifests generated on another machine.
# Example:
#   RAW_PATH_PREFIX_FROM=/Users/filmonghezehey/Downloads
#   RAW_PATH_PREFIX_TO=/Volumes/DatabentoVault/raw_downloads
export RAW_PATH_PREFIX_FROM="${RAW_PATH_PREFIX_FROM:-/Users/filmonghezehey/Downloads}"
export RAW_PATH_PREFIX_TO="${RAW_PATH_PREFIX_TO:-}"

export RUN_SMOKE="${RUN_SMOKE:-1}"
export RUN_FULL_NORMALIZE="${RUN_FULL_NORMALIZE:-1}"
export RUN_VERIFY="${RUN_VERIFY:-1}"
export RUN_ENRICHED_BUILD="${RUN_ENRICHED_BUILD:-1}"
export PREFLIGHT_ONLY="${PREFLIGHT_ONLY:-0}"
export FORCE_ENRICHED="${FORCE_ENRICHED:-0}"

mkdir -p "$PLAN_ROOT" "$SILVER_ROOT/logs" "$ENRICHED_30S_ROOT"

log "CONFIG"
cat <<EOF | tee "$SILVER_ROOT/logs/build_config.env"
repo=$(pwd)
git_commit=$(git rev-parse HEAD)
LAKE_ROOT=$LAKE_ROOT
SOURCE_MANIFEST_ROOT=$SOURCE_MANIFEST_ROOT
BASELINE_30S=$BASELINE_30S
SYMBOLS=$SYMBOLS
BROADER_DAYS=$BROADER_DAYS
PLAN_STRATEGY=$PLAN_STRATEGY
SMOKE_DATE=$SMOKE_DATE
RUN_STAMP=$RUN_STAMP
PLAN_ROOT=$PLAN_ROOT
SILVER_ROOT=$SILVER_ROOT
ENRICHED_30S_ROOT=$ENRICHED_30S_ROOT
RAW_PATH_PREFIX_FROM=$RAW_PATH_PREFIX_FROM
RAW_PATH_PREFIX_TO=$RAW_PATH_PREFIX_TO
RUN_SMOKE=$RUN_SMOKE
RUN_FULL_NORMALIZE=$RUN_FULL_NORMALIZE
RUN_VERIFY=$RUN_VERIFY
RUN_ENRICHED_BUILD=$RUN_ENRICHED_BUILD
PREFLIGHT_ONLY=$PREFLIGHT_ONLY
FORCE_ENRICHED=$FORCE_ENRICHED
EOF

log "PREFLIGHT PATHS"
for required_path in \
  "$LAKE_ROOT" \
  "$SOURCE_MANIFEST_ROOT/paired_dates.csv" \
  "$SOURCE_MANIFEST_ROOT/source_files.csv" \
  "$BASELINE_30S"; do
  if [[ ! -e "$required_path" ]]; then
    fail "missing required path: $required_path"
  fi
  printf 'OK %s\n' "$required_path"
done | tee "$SILVER_ROOT/logs/preflight_paths.log"

log "PLAN BROADER SIX-SOURCE DATE SET"
python3 scripts/plan_databento_pilot_dates.py \
  --paired-dates "$SOURCE_MANIFEST_ROOT/paired_dates.csv" \
  --source-files "$SOURCE_MANIFEST_ROOT/source_files.csv" \
  --output-dir "$PLAN_ROOT" \
  --days "$BROADER_DAYS" \
  --strategy "$PLAN_STRATEGY" \
  2>&1 | tee "$SILVER_ROOT/logs/plan_broader_six_source.log"

if [[ -n "$RAW_PATH_PREFIX_TO" ]]; then
  log "REWRITE SOURCE PATH PREFIX"
  python3 - "$PLAN_ROOT/pilot_source_files.csv" "$RAW_PATH_PREFIX_FROM" "$RAW_PATH_PREFIX_TO" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
old = sys.argv[2]
new = sys.argv[3]
rows = list(csv.DictReader(path.open(newline='', encoding='utf-8')))
fields = list(rows[0].keys()) if rows else []
rewritten = 0
for row in rows:
    raw = row.get('path', '')
    if raw.startswith(old):
        row['path'] = new + raw[len(old):]
        rewritten += 1
tmp = path.with_suffix(path.suffix + '.tmp')
with tmp.open('w', newline='', encoding='utf-8') as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
tmp.replace(path)
print(f"rewritten_paths={rewritten}")
PY
fi

log "VALIDATE PLANNED RAW SOURCE PATHS"
python3 - "$PLAN_ROOT/pilot_source_files.csv" <<'PY' 2>&1 | tee "$SILVER_ROOT/logs/validate_planned_source_paths.log"
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
rows = list(csv.DictReader(path.open(newline='', encoding='utf-8')))
missing = [row.get('path', '') for row in rows if not Path(row.get('path', '')).exists()]
print(f"planned_files={len(rows)}")
print(f"existing_files={len(rows) - len(missing)}")
print(f"missing_files={len(missing)}")
if missing:
    print("missing_sample=")
    for item in missing[:12]:
        print(item)
    raise SystemExit(2)
PY

log "BUILD EXPECTED DATE ARGUMENTS"
EXPECTED_DATES_FILE="$SILVER_ROOT/logs/expected_dates.txt"
python3 - "$PLAN_ROOT/pilot_dates.csv" "$EXPECTED_DATES_FILE" <<'PY'
import csv
import sys
from pathlib import Path

pilot_dates = Path(sys.argv[1])
out = Path(sys.argv[2])
dates = []
for row in csv.DictReader(pilot_dates.open(newline='', encoding='utf-8')):
    dates.append(row.get('iso_date') or row.get('date'))
if not dates:
    raise SystemExit('no dates found in pilot_dates.csv')
out.write_text('\n'.join(dates) + '\n', encoding='utf-8')
print(f"date_count={len(dates)}")
print(f"first_date={dates[0]}")
print(f"last_date={dates[-1]}")
PY

expected_date_args=()
while IFS= read -r expected_date; do
  [[ -z "$expected_date" ]] && continue
  expected_date_args+=(--expected-date "$expected_date")
done < "$EXPECTED_DATES_FILE"

if ! grep -q "${SMOKE_DATE}" "$EXPECTED_DATES_FILE"; then
  fail "SMOKE_DATE=$SMOKE_DATE is not in planned expected dates file: $EXPECTED_DATES_FILE"
fi

if [[ "$PREFLIGHT_ONLY" == "1" ]]; then
  log "PREFLIGHT_ONLY complete"
  exit 0
fi

if [[ "$RUN_SMOKE" == "1" ]]; then
  log "ONE-DAY SILVER SMOKE $SMOKE_DATE"
  python3 scripts/normalize_databento_definitions.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --output-dir "$SILVER_ROOT/definitions_smoke" \
    --symbols "$SYMBOLS" \
    --date "$SMOKE_DATE" \
    2>&1 | tee "$SILVER_ROOT/logs/definitions_smoke.log"

  python3 scripts/normalize_equs_mbp1.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --output-dir "$SILVER_ROOT/equs_mbp1_1s_smoke" \
    --symbols "$SYMBOLS" \
    --date "$SMOKE_DATE" \
    2>&1 | tee "$SILVER_ROOT/logs/equs_mbp1_smoke.log"

  python3 scripts/normalize_opra_tcbbo.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --definition-root "$SILVER_ROOT/definitions_smoke" \
    --output-dir "$SILVER_ROOT/opra_tcbbo_1s_smoke" \
    --symbols "$SYMBOLS" \
    --date "$SMOKE_DATE" \
    2>&1 | tee "$SILVER_ROOT/logs/opra_tcbbo_smoke.log"
fi

if [[ "$RUN_FULL_NORMALIZE" == "1" ]]; then
  log "FULL BROADER SILVER NORMALIZATION"
  python3 scripts/normalize_databento_definitions.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --output-dir "$SILVER_ROOT/definitions" \
    --symbols "$SYMBOLS" \
    2>&1 | tee "$SILVER_ROOT/logs/definitions_full.log"

  python3 scripts/normalize_equs_mbp1.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --output-dir "$SILVER_ROOT/equs_mbp1_1s" \
    --symbols "$SYMBOLS" \
    2>&1 | tee "$SILVER_ROOT/logs/equs_mbp1_full.log"

  python3 scripts/normalize_opra_tcbbo.py \
    --pilot-source-files "$PLAN_ROOT/pilot_source_files.csv" \
    --definition-root "$SILVER_ROOT/definitions" \
    --output-dir "$SILVER_ROOT/opra_tcbbo_1s" \
    --symbols "$SYMBOLS" \
    2>&1 | tee "$SILVER_ROOT/logs/opra_tcbbo_full.log"
fi

if [[ "$RUN_VERIFY" == "1" ]]; then
  log "VERIFY BROADER SILVER OUTPUTS"
  python3 scripts/verify_databento_silver_outputs.py \
    --silver-root "$SILVER_ROOT" \
    --symbols "$SYMBOLS" \
    "${expected_date_args[@]}" \
    --output-dir "$SILVER_ROOT/quality_check" \
    2>&1 | tee "$SILVER_ROOT/logs/silver_quality_check.log"
fi

if [[ "$RUN_ENRICHED_BUILD" == "1" ]]; then
  log "BUILD BROADER SILVER-ENRICHED 30S CSV"
  force_args=()
  if [[ "$FORCE_ENRICHED" == "1" ]]; then
    force_args+=(--force)
  fi
  python3 scripts/build_databento_enriched_30s.py \
    --silver-root "$SILVER_ROOT" \
    --baseline-30s "$BASELINE_30S" \
    --symbols "$SYMBOLS" \
    "${expected_date_args[@]}" \
    --expected-rows-per-symbol-day 780 \
    --output-dir "$ENRICHED_30S_ROOT" \
    "${force_args[@]}" \
    2>&1 | tee "$SILVER_ROOT/logs/enriched_30s_build.log"
fi

log "FINAL ARTIFACT CHECK"
combined_csv="$ENRICHED_30S_ROOT/combined/combined_30s.csv"
if [[ -f "$combined_csv" ]]; then
  actual_rows=$(( $(wc -l < "$combined_csv") - 1 ))
  printf 'combined_30s_csv=%s\n' "$combined_csv" | tee -a "$SILVER_ROOT/logs/final_artifacts.log"
  printf 'combined_30s_rows=%s\n' "$actual_rows" | tee -a "$SILVER_ROOT/logs/final_artifacts.log"
else
  printf 'combined_30s_csv_missing=%s\n' "$combined_csv" | tee -a "$SILVER_ROOT/logs/final_artifacts.log"
fi

cat <<EOF | tee -a "$SILVER_ROOT/logs/final_artifacts.log"
PLAN_ROOT=$PLAN_ROOT
SILVER_ROOT=$SILVER_ROOT
ENRICHED_30S_ROOT=$ENRICHED_30S_ROOT
BROADER_SILVER_ENRICHED_30S_BUILD_SCRIPT=PASS
EOF
