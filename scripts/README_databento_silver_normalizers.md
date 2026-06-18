# Databento Phase 1 silver normalizers

These scripts unlock the six-source Databento pilot after `PREBUILD_CHECK=PASS`.
They produce compact silver CSVs only; they do **not** train or promote models.

## Inputs

Use the six-source pilot manifest that passed prebuild validation:

```zsh
export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export PILOT_DIR="$LAKE_ROOT/source_manifests/pilot_dates_latest10_six_source_downloads_20260617_183703"
export SYMBOLS="TSLA,TQQQ,NVDA,SPY,QQQ"
export SILVER_ROOT="$LAKE_ROOT/silver/pilot_10d_six_source_phase1_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$SILVER_ROOT/logs"
```

## One-day smoke first

Run this before the full 10-day pilot so schema issues fail small:

```zsh
python3 scripts/normalize_databento_definitions.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --output-dir "$SILVER_ROOT/definitions_smoke" \
  --symbols "$SYMBOLS" \
  --date 2026-05-21 \
  2>&1 | tee "$SILVER_ROOT/logs/definitions_smoke.log"

python3 scripts/normalize_equs_mbp1.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --output-dir "$SILVER_ROOT/equs_mbp1_1s_smoke" \
  --symbols "$SYMBOLS" \
  --date 2026-05-21 \
  2>&1 | tee "$SILVER_ROOT/logs/equs_mbp1_smoke.log"

python3 scripts/normalize_opra_tcbbo.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --definition-root "$SILVER_ROOT/definitions_smoke" \
  --output-dir "$SILVER_ROOT/opra_tcbbo_1s_smoke" \
  --symbols "$SYMBOLS" \
  --date 2026-05-21 \
  2>&1 | tee "$SILVER_ROOT/logs/opra_tcbbo_smoke.log"
```

Expected smoke shape for the five-symbol pilot:

- `normalize_equs_mbp1.py`: up to `5` per-symbol files for one date, each with a full RTH `1s` grid unless `--observed-only` is passed.
- `normalize_opra_tcbbo.py`: up to `5` per-underlying files for one date, each with call/put option flow and quote-context features.
- `normalize_databento_definitions.py`: compact definition metadata for `EQUS definition` and `OPRA definition`, filtered to the requested underlyings/symbols.

## Full 10-day pilot

After smoke outputs and manifests look clean, run the full selected window:

```zsh
python3 scripts/normalize_databento_definitions.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --output-dir "$SILVER_ROOT/definitions" \
  --symbols "$SYMBOLS" \
  2>&1 | tee "$SILVER_ROOT/logs/definitions_full.log"

python3 scripts/normalize_equs_mbp1.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --output-dir "$SILVER_ROOT/equs_mbp1_1s" \
  --symbols "$SYMBOLS" \
  2>&1 | tee "$SILVER_ROOT/logs/equs_mbp1_full.log"

python3 scripts/normalize_opra_tcbbo.py \
  --pilot-source-files "$PILOT_DIR/pilot_source_files.csv" \
  --definition-root "$SILVER_ROOT/definitions" \
  --output-dir "$SILVER_ROOT/opra_tcbbo_1s" \
  --symbols "$SYMBOLS" \
  2>&1 | tee "$SILVER_ROOT/logs/opra_tcbbo_full.log"
```

Each output directory writes a `manifest.json` and summary CSV. Review `errors=[]` before using the silver files in an enriched 30s feature build.

