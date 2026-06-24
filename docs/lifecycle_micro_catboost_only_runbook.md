# CatBoost setup OOF + lifecycle/micro RandomForest retrain runbook

Date: 2026-06-22

This run keeps the June decision to avoid MBP-1/`equs_liquidity` infrastructure. The 30-second setup probabilities are generated from the broad 5-symbol CatBoost/no-silver setup cache, then injected into existing 5-second-derived lifecycle/micro staging rows. The exported lifecycle/micro ONNX bundle still uses the existing `train_lifecycle_micro_models.py` RandomForest exporter; this is not an end-to-end CatBoost lifecycle/micro bundle.

## Current output

Research output root:

```text
/Users/FXG06FA/trading-agent-main/runtime/research_runs/lifecycle_micro_catboost_only_20260622
```

Generated CatBoost 30-second OOF setup predictions:

```text
runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.manifest.json
```

Restaged lifecycle/micro training rows with real OOF setup probabilities:

```text
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/long_lifecycle_rows.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/short_lifecycle_rows.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/long_micro_entry_rows.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/short_micro_entry_rows.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/long_micro_exit_rows.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup/short_micro_exit_rows.csv
```

Exported lifecycle/micro model bundle:

```text
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/long_exit_lifecycle.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/short_exit_lifecycle.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/long_micro_entry_5s.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/short_micro_entry_5s.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/long_micro_exit_guard_5s.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/short_micro_exit_guard_5s.onnx
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/lifecycle_micro_scorecard.csv
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/lifecycle_micro_route_manifest.json
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/feature_schema.json
runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports/feature_schema.sha256
```

Validation artifacts:

```text
runtime/research_runs/lifecycle_micro_catboost_only_20260622/bundle_validation.json
runtime/research_runs/lifecycle_micro_catboost_only_20260622/dryrun_load_validation_2/dryrun_load_validation.log
runtime/research_runs/lifecycle_micro_catboost_only_20260622/dryrun_load_validation_2/*-stream-sanity.json
```

## Re-run commands

Generate CatBoost/no-silver OOF setup predictions from the broad local 30-second cache:

```zsh
cd /Users/FXG06FA/trading-agent-main

PYTHONUNBUFFERED=1 caffeinate -dims python3 generate_walk_forward_setup_predictions.py \
  --input-30s-csv runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  --output-csv runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.csv \
  --manifest-json runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.manifest.json \
  --model-family catboost \
  --regime-model-family catboost \
  --min-train-days 35 \
  --test-days-per-fold 35 \
  --day-gap 0 \
  --min-train-positives 5 \
  --min-oof-coverage-frac 0.50 \
  --min-unique-probabilities 3 \
  --no-news-features
```

Restage the existing lifecycle/micro rows and train/export the six RandomForest lifecycle/micro ONNX models:

```zsh
cd /Users/FXG06FA/trading-agent-main

PYTHONUNBUFFERED=1 caffeinate -dims python3 scripts/train_lifecycle_micro_from_staged_rows.py \
  --staged-dir runtime/lifecycle_micro_20260523_staging \
  --setup-predictions-csv runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.csv \
  --output-dir runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports \
  --restaged-dir runtime/research_runs/lifecycle_micro_catboost_only_20260622/restaged_rows_with_oof_setup \
  --max-train-rows-per-model 500000 \
  --min-rows 200
```

Validate the bundle:

```zsh
cd /Users/FXG06FA/trading-agent-main

python3 runtime/research_runs/lifecycle_micro_catboost_only_20260622/validate_lifecycle_micro_bundle.py
```

Run Java load-smoke dry-run:

```zsh
cd /Users/FXG06FA/trading-agent-main

PYTHON_BIN="$PWD/.venv/bin/python" \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_LIFECYCLE_MODEL_DIR="$PWD/runtime/research_runs/lifecycle_micro_catboost_only_20260622/model_exports" \
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbol TSLA \
  --start 2026-06-01 \
  --end 2026-06-01 \
  --model-dir runtime/models/TSLA \
  --output-dir runtime/research_runs/lifecycle_micro_catboost_only_20260622/dryrun_load_validation \
  --dry-run \
  --skip-build \
  --timeout-seconds 60
```

## Important limitations

- `production_promotion=NO-GO` for this run.
- Despite the run-directory naming shorthand, “CatBoost” applies to the 30-second setup OOF source only. The lifecycle/micro exports are RandomForest ONNX models with runtime-aligned real OOF setup/entry probability features.
- The offload-backed raw 30s/5s training-data symlinks were not mounted, and no raw 5s bars were rebuilt in this run.
- The restaged rows are filtered to the five symbols covered by the local broad 30-second cache: `NVDA,QQQ,SPY,TQQQ,TSLA`.
- The existing staged rows previously used bootstrap `1.0` setup/entry probabilities. This run replaces the runtime feature columns `f_setup_*` and `f_entry_*` with real CatBoost OOF setup probabilities and validates they are no longer bootstrap constants.
- Full production-quality lifecycle/micro retraining still requires either mounting the Databento offload volume or regenerating matched 5-second bars from EQUS `tbbo` + OPRA `ohlcv-1s` sources/API for the desired symbol/date universe.
