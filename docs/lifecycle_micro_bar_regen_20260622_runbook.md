# Lifecycle/micro true bar regeneration runbook

Date: 2026-06-22

This run regenerated the lifecycle/micro training rows from true 30-second and 5-second bars instead of using the earlier restaged-row recovery path.

## Data sources

Local 30-second combined bars:

```text
/Users/FXG06FA/trading-agent-main/runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv
```

External-disk 5-second combined bars:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_full_window_cost_aware_catboost_setup_20260616_200413/input_slice/combined_5s.csv
```

CatBoost/no-silver OOF 30-second setup probabilities:

```text
/Users/FXG06FA/trading-agent-main/runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.csv
```

## Output root

```text
/Users/FXG06FA/trading-agent-main/runtime/research_runs/lifecycle_micro_bar_regen_20260622
```

Key outputs:

```text
runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_30s_by_symbol/
runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol/
runtime/research_runs/lifecycle_micro_bar_regen_20260622/staged_lifecycle_micro_rows/
runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports/
runtime/research_runs/lifecycle_micro_bar_regen_20260622/bar_bundle_validation.json
runtime/research_runs/lifecycle_micro_bar_regen_20260622/dryrun_load_validation/
```

## Re-run commands

Split the locally copied 30-second bars by symbol:

```zsh
cd /Users/FXG06FA/trading-agent-main

PYTHONUNBUFFERED=1 python3 scripts/split_combined_bars_by_symbol.py \
  --input-csv runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  --output-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_30s_by_symbol \
  --cadence 30s \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --chunksize 200000 \
  --overwrite \
  --manifest-json runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_30s_by_symbol_manifest.json
```

Split the external-disk 5-second bars by symbol:

```zsh
cd /Users/FXG06FA/trading-agent-main

diskutil mount disk5s1 >/dev/null 2>&1 || true

PYTHONUNBUFFERED=1 python3 scripts/split_combined_bars_by_symbol.py \
  --input-csv /Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_full_window_cost_aware_catboost_setup_20260616_200413/input_slice/combined_5s.csv \
  --output-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol \
  --cadence 5s \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --chunksize 250000 \
  --overwrite \
  --manifest-json runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol_manifest.json
```

Train/export the six lifecycle/micro ONNX models from true per-symbol bars:

```zsh
cd /Users/FXG06FA/trading-agent-main

PYTHONUNBUFFERED=1 caffeinate -dims python3 train_lifecycle_micro_models.py \
  --input-30s-csv runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_30s_by_symbol \
  --input-5s-csv runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol \
  --setup-predictions-csv runtime/research_runs/lifecycle_micro_catboost_only_20260622/setup_predictions/oof_setup_predictions.csv \
  --output-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --staging-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/staged_lifecycle_micro_rows \
  --max-train-rows-per-model 750000 \
  --max-staged-rows-per-symbol-per-model 20000 \
  --max-entry-events-per-symbol-side 500 \
  --max-entry-events 0 \
  --min-rows 200
```

Validate the bar-regenerated bundle:

```zsh
cd /Users/FXG06FA/trading-agent-main

python3 runtime/research_runs/lifecycle_micro_bar_regen_20260622/validate_lifecycle_micro_bar_bundle.py
```

Run Java load-smoke dry-run:

```zsh
cd /Users/FXG06FA/trading-agent-main

./mvnw -q -DskipTests package

PYTHON_BIN="$PWD/.venv/bin/python" \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_LIFECYCLE_MODEL_DIR="$PWD/runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports" \
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbol TSLA \
  --start 2026-06-01 \
  --end 2026-06-01 \
  --model-dir runtime/models/TSLA \
  --output-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/dryrun_load_validation \
  --dry-run \
  --skip-build \
  --timeout-seconds 60
```

## Verified results

Split manifests:

- `30s`: `830700` rows, `166140` rows each for `NVDA`, `QQQ`, `SPY`, `TQQQ`, `TSLA`.
- `5s`: `4984200` rows, `996840` rows each for `NVDA`, `QQQ`, `SPY`, `TQQQ`, `TSLA`.

Staged rows:

- `long_lifecycle_rows.csv`: `50000`
- `short_lifecycle_rows.csv`: `50000`
- `long_micro_entry_rows.csv`: `17242`
- `short_micro_entry_rows.csv`: `17361`
- `long_micro_exit_rows.csv`: `15504`
- `short_micro_exit_rows.csv`: `15120`

Scorecard summary:

| model | rows | positives | threshold | precision | recall | feature_count |
|---|---:|---:|---:|---:|---:|---:|
| `longExitLifecycleAi` | 50000 | 15712 | 0.50 | 99.58% | 68.01% | 34 |
| `shortExitLifecycleAi` | 50000 | 15471 | 0.58 | 99.95% | 63.50% | 34 |
| `longMicroEntryAi` | 17242 | 2683 | 0.62 | 81.87% | 26.97% | 44 |
| `shortMicroEntryAi` | 17361 | 2633 | 0.52 | 87.76% | 24.57% | 44 |
| `longMicroExitGuardAi` | 15504 | 3940 | 0.60 | 100.00% | 45.26% | 50 |
| `shortMicroExitGuardAi` | 15120 | 3933 | 0.60 | 100.00% | 51.46% | 50 |

Validation:

```text
LIFECYCLE_MICRO_BAR_BUNDLE_VALIDATION=PASS
validation=runtime/research_runs/lifecycle_micro_bar_regen_20260622/bar_bundle_validation.json
```

Java dry-run load smoke:

- `long_exit_lifecycle.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=34`
- `short_exit_lifecycle.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=34`
- `long_micro_entry_5s.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=44`
- `short_micro_entry_5s.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=44`
- `long_micro_exit_guard_5s.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=50`
- `short_micro_exit_guard_5s.onnx`: `FEATURE_COUNT_SUPPORTED=PASS`, `expected=50`
- Route manifest validated by `PingPongStrategy`.

## Notes and limitations

- This is a research bundle, not a production promotion.
- The lifecycle/micro models are still the existing RandomForest ONNX architecture; CatBoost is the OOF 30-second setup-probability source.
- The true bar-based trainer was patched on this date to emit Java-runtime-required probability/threshold/margin feature aliases, restoring model feature counts to `34/44/50`.
- The dry-run intentionally used `--dry-run`, so no market bars were processed and no trades were expected.
