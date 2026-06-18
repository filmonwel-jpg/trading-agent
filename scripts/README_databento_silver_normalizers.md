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

## QA the full silver outputs

After the three full normalizers finish and their manifests have `errors=[]`, run the structural/coverage QA gate:

```zsh
python3 scripts/verify_databento_silver_outputs.py \
  --silver-root "$SILVER_ROOT" \
  --symbols "$SYMBOLS" \
  --expected-date 2026-05-11 \
  --expected-date 2026-05-12 \
  --expected-date 2026-05-13 \
  --expected-date 2026-05-14 \
  --expected-date 2026-05-15 \
  --expected-date 2026-05-18 \
  --expected-date 2026-05-19 \
  --expected-date 2026-05-20 \
  --expected-date 2026-05-21 \
  --expected-date 2026-05-22 \
  --output-dir "$SILVER_ROOT/quality_check" \
  2>&1 | tee "$SILVER_ROOT/logs/silver_quality_check.log"
```

The verifier writes:

- `$SILVER_ROOT/quality_check/silver_quality_manifest.json`
- `$SILVER_ROOT/quality_check/silver_file_quality.csv`

Proceed to enriched feature-building only if the command prints `SILVER_QUALITY_CHECK=PASS`. Warnings should still be reviewed even when they are not fatal.

Note: OPRA premium-notional consistency uses a small dollar/relative tolerance
(`--opra-notional-abs-tolerance`, default `$1.00`, and
`--opra-notional-rel-tolerance`, default `1e-9`) because large floating-point
notional sums can round-trip through CSV with sub-dollar residuals. Contract
volume, trade-count, and quote-context totals remain strict.

## Build enriched 30s features from QA-passed silver outputs

After `SILVER_QUALITY_CHECK=PASS`, join the silver 1-second features onto the
existing fixed-quality baseline 30s bars. This is still a build/QA step only;
it does **not** train, backtest, promote, or overwrite model artifacts.

Set `BASELINE_30S` to either the baseline build root containing `data_30s/`, a
`data_30s/` directory, or a combined baseline 30s CSV:

```zsh
export BASELINE_30S="$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_YYYYMMDD_HHMMSS"
export ENRICHED_30S_ROOT="$LAKE_ROOT/model_training_sets/pilot_10d_six_source_enriched_30s_$(date +%Y%m%d_%H%M%S)"

python3 scripts/build_databento_enriched_30s.py \
  --silver-root "$SILVER_ROOT" \
  --baseline-30s "$BASELINE_30S" \
  --symbols "$SYMBOLS" \
  --expected-date 2026-05-11 \
  --expected-date 2026-05-12 \
  --expected-date 2026-05-13 \
  --expected-date 2026-05-14 \
  --expected-date 2026-05-15 \
  --expected-date 2026-05-18 \
  --expected-date 2026-05-19 \
  --expected-date 2026-05-20 \
  --expected-date 2026-05-21 \
  --expected-date 2026-05-22 \
  --expected-rows-per-symbol-day 780 \
  --output-dir "$ENRICHED_30S_ROOT" \
  2>&1 | tee "$SILVER_ROOT/logs/enriched_30s_build.log"
```

Expected output shape for the five-symbol, 10-day pilot is `39,000` rows
(`5 symbols * 10 dates * 780 bars`). The builder writes:

- `$ENRICHED_30S_ROOT/combined/combined_30s.csv`
- `$ENRICHED_30S_ROOT/data_30s/<SYMBOL>_30s_training.csv`
- `$ENRICHED_30S_ROOT/reports/enriched_feature_join_summary.csv`
- `$ENRICHED_30S_ROOT/manifest.json`

Proceed to the next QA/comparison step only if the command prints
`DATABENTO_ENRICHED_30S_BUILD=PASS` and the manifest has `errors=[]`.

## Research-only trainer experiment with Databento silver features

By default, `train_30s_models.py` ignores the appended `EqMbp1...` and
`OpraTcbbo...` columns so enriched CSVs remain row/schema neutral for production
compatibility checks. To run a research-only setup comparison that activates the
conservative silver feature subset, set `USE_DATABENTO_SILVER_FEATURES=1` and
keep ONNX export disabled:

```zsh
export AUTO_BUILD_30S_IF_MISSING=0
export UPDATE_CANONICAL_MODEL_ALIASES=0
export TRAIN_LEGACY_30S_EXIT_MODELS=0
export MODEL_FAMILY=random_forest
export REGIME_MODEL_FAMILY=random_forest
export USE_DATABENTO_SILVER_FEATURES=1
export DATABENTO_SILVER_FEATURE_SET=all

python3 train_30s_models.py \
  --input-csv "$ENRICHED_30S_ROOT/combined/combined_30s.csv" \
  --output-dir "$ENRICHED_30S_ROOT/training_runs/setup_silver_features_no_onnx_$(date +%Y%m%d_%H%M%S)" \
  --no-onnx
```

Supported ablation presets are:

- `all` — all 35 active silver research features
- `equs` — equity L1 spread/imbalance/liquidity features only
- `opra` — OPRA options context features only
- `liquidity` — spread, coverage, quote-age, and liquidity-state features
- `equs_liquidity` — EQUS L1 spread/imbalance/coverage/quote-age liquidity features only
- `opra_liquidity` — OPRA active-coverage and option spread-context liquidity features only
- `equs_activity` — EQUS quote update/event count features only
- `options_flow` — OPRA trade count, volume, premium, at-bid/at-ask, and put/call flow features

To run all ablations into separate no-ONNX directories:

```zsh
export RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
export ABLATION_ROOT="$ENRICHED_30S_ROOT/training_runs/setup_silver_ablation_no_onnx_$RUN_STAMP"
mkdir -p "$ABLATION_ROOT"

for preset in all equs opra liquidity options_flow; do
  export DATABENTO_SILVER_FEATURE_SET="$preset"
  export RUN_ROOT="$ABLATION_ROOT/$preset"
  mkdir -p "$RUN_ROOT"

  python3 train_30s_models.py \
    --input-csv "$ENRICHED_30S_ROOT/combined/combined_30s.csv" \
    --output-dir "$RUN_ROOT" \
    --no-onnx \
    2>&1 | tee "$RUN_ROOT/train_30s_no_onnx.log"
done
```

If the first pass points to `liquidity`, run the fine split to isolate whether
the lift comes from EQUS L1 liquidity, OPRA spread/liquidity context, or EQUS
quote activity:

```zsh
export RUN_STAMP="$(date +%Y%m%d_%H%M%S)"
export ABLATION_ROOT="$ENRICHED_30S_ROOT/training_runs/setup_silver_liquidity_split_no_onnx_$RUN_STAMP"
mkdir -p "$ABLATION_ROOT"

for preset in liquidity equs_liquidity opra_liquidity equs_activity equs; do
  export DATABENTO_SILVER_FEATURE_SET="$preset"
  export RUN_ROOT="$ABLATION_ROOT/$preset"
  mkdir -p "$RUN_ROOT"

  python3 train_30s_models.py \
    --input-csv "$ENRICHED_30S_ROOT/combined/combined_30s.csv" \
    --output-dir "$RUN_ROOT" \
    --no-onnx \
    2>&1 | tee "$RUN_ROOT/train_30s_no_onnx.log"
done
```

After the loop completes, compare the preset artifacts against a strict baseline
no-ONNX run. The helper validates required artifacts, checks that no ONNX files
were written, verifies row/schema consistency, summarizes fold-level precision
and predicted-positive-rate stability from `threshold_grid.csv`, prints
short-entry zero/thin fold blockers, and writes comparison CSV/JSON plus
`databento_silver_ablation_short_fold_blockers.csv` under `$ABLATION_ROOT`:

```zsh
python3 scripts/analyze_databento_silver_ablation.py \
  --baseline-dir "$BASELINE_RUN_ROOT" \
  --ablation-root "$ABLATION_ROOT"
```

For a fine-split root, pass the exact preset list:

```zsh
python3 scripts/analyze_databento_silver_ablation.py \
  --baseline-dir "$BASELINE_RUN_ROOT" \
  --ablation-root "$ABLATION_ROOT" \
  --preset liquidity \
  --preset equs_liquidity \
  --preset opra_liquidity \
  --preset equs_activity \
  --preset equs
```

If the fine split passes QA but short-entry folds are still zero/thin, run the
artifact-only threshold floor diagnostic against the saved OOF probabilities.
This estimates the highest per-fold short threshold that would satisfy a strict
minimum predicted-positive floor without retraining or exporting ONNX:

```zsh
python3 scripts/analyze_databento_short_threshold_floor.py \
  --ablation-root "$ABLATION_ROOT" \
  --preset liquidity \
  --preset equs \
  --min-pred-pos-rate 0.005 \
  --min-pred-pos-count 20
```

The diagnostic writes `databento_short_threshold_floor_candidates.csv`. It is a
research triage tool only; threshold-floor candidates still require full
walk-forward policy changes, frozen-holdout checks, backtests, paper/shadow, and
live feature-parity validation before any promotion.

For a no-ONNX research-only trainer experiment, the short-entry OOF threshold
floor can be enabled explicitly. This mode is intentionally leakage-prone: it
uses OOF test-fold probabilities to lower short-entry thresholds enough to meet
the configured prediction floor, and optionally searches for a precision floor.
It refuses to run unless `--no-onnx` is supplied and must never be used for
production promotion:

```zsh
export RESEARCH_SHORT_THRESHOLD_FLOOR_ENABLED=1
export RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_RATE=0.005
export RESEARCH_SHORT_THRESHOLD_MIN_PRED_POS_COUNT=20
export RESEARCH_SHORT_THRESHOLD_MIN_PRECISION=0.20

python3 train_30s_models.py \
  --input-csv "$ENRICHED_30S_ROOT/combined/combined_30s.csv" \
  --output-dir "$RUN_ROOT" \
  --no-onnx
```

The policy settings are recorded under
`setup_manifest.json -> walk_forward -> research_short_threshold_policy`, and
per-fold policy fields are appended to `threshold_grid.csv` for audit.

This is not a production promotion gate. Promotion still requires calibration,
backtest, paper/shadow, and live feature-parity checks for the active silver
feature schema.

