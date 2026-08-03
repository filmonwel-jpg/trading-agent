# Pilot Core Symbol Onboarding Runbook

Date: 2026-08-01

This runbook documents the repeatable process for onboarding `AMD`, `MU`, or future symbols into the current `pilot_core_5` workflow. The existing pilot name and filenames still say “Core-5”, but expanding with `AMD` and `MU` makes the practical pilot universe seven symbols:

```text
TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU
```

The process below follows the repository’s current Databento data-prep, setup-model, lifecycle/micro-model, calibration, bot-property, and historical-backtest flow.

> Safety note: use existing models only for first-pass research/backtesting. Do not enable real entries for newly onboarded symbols until data quality, retraining or calibration, historical backtests, and promotion gates pass.

---

## 1. Current state and onboarding decision

### Current pilot source-of-truth files

The current five-symbol pilot universe is defined by:

- `config/databento_core_5_symbols.txt`
- `runtime/pilot_core_5_symbols.txt`
- `config/databento_dynamic_upgrade_pilot_symbols.csv`

Before onboarding `AMD`/`MU`, these files contain only:

```text
TSLA
TQQQ
NVDA
SPY
QQQ
```

### Existing `AMD`/`MU` runtime readiness

Generated Databento bot configs already exist:

- `runtime/databento/bots/trading-amd.properties`
- `runtime/databento/bots/trading-mu.properties`

`AMD` also has a legacy top-level runtime config and legacy runtime model bundle:

- `runtime/trading-amd.properties`
- `runtime/models/AMD/` with 21 ONNX files

`MU` does **not** currently have:

- `runtime/trading-mu.properties`
- `runtime/models/MU/`

The generated Databento bot path does not require those legacy files, because `run_symbol.sh` resolves generated bot properties first:

1. `runtime/databento/bots/trading-<symbol>.properties`
2. `runtime/trading-<symbol>.properties`

### Model decision

For first-pass evaluation:

- Existing shared models can be used to backtest `AMD` and `MU`.
- No retraining is required before this initial transfer test.

For official pilot onboarding or paper/live entries:

- Retraining and/or at least symbol-specific calibration is required.
- The active setup model’s OOF evidence currently covers the original Core-5 only.
- `config/databento_calibrated_micro_entry_thresholds.csv` currently contains calibrated micro thresholds only for `TSLA` and `TQQQ`.

Recommended sequence:

```text
expand symbol contract
→ build data
→ verify quality
→ backtest existing model transfer
→ retrain expanded setup/lifecycle models if justified
→ calibrate thresholds
→ regenerate routing/properties
→ run promotion gates
→ paper/shadow
→ limited live
```

---

## 2. Register the symbols in the pilot contract

### 2.1 Update `config/databento_core_5_symbols.txt`

Add `AMD` and `MU`:

```text
# Core Databento pilot universe; filename retained for compatibility
TSLA
TQQQ
NVDA
SPY
QQQ
AMD
MU
```

### 2.2 Update `runtime/pilot_core_5_symbols.txt`

Keep it synchronized with the config file:

```text
TSLA
TQQQ
NVDA
SPY
QQQ
AMD
MU
```

### 2.3 Update `config/databento_dynamic_upgrade_pilot_symbols.csv`

Add `AMD` and `MU` as trainable primary trade targets:

```csv
symbol,opra_parent,cohort,role,train_target,context_source,priority
TSLA,TSLA.OPT,pilot_core_5,primary_trade_target,true,true,1
TQQQ,TQQQ.OPT,pilot_core_5,primary_trade_target,true,true,2
NVDA,NVDA.OPT,pilot_core_5,primary_trade_target,true,true,3
SPY,SPY.OPT,pilot_core_5,market_context_optional_target,false,true,4
QQQ,QQQ.OPT,pilot_core_5,tech_context_optional_target,false,true,5
AMD,AMD.OPT,pilot_core_5,primary_trade_target,true,true,6
MU,MU.OPT,pilot_core_5,primary_trade_target,true,true,7
```

Keep `cohort=pilot_core_5` unless all dependent scripts/docs are intentionally renamed.

### 2.4 Update hardcoded Core-5 defaults

Some scripts hard-code the current five-symbol list. Either update them to include `AMD,MU` or refactor them to read from `config/databento_core_5_symbols.txt`.

Known files to review:

- `scripts/run_databento_api_backtest_core5.sh`
- `scripts/verify_databento_pilot_build.py`
- `scripts/stage_broader_window_inputs.py`
- `scripts/run_core_full_window_bars_from_downloads_20260616.sh`
- `scripts/run_broader_full_window_cost_aware_chain_20260616.sh`
- `scripts/run_broader_silver_enriched_30s_build.sh`
- `scripts/run_event_carried_whole_range.sh`
- `scripts/analyze_databento_core5_weekly_outputs.py`
- `scripts/calculate_core5_today_pnl.py`
- `scripts/summarize_core5_today_only.py`
- `runtime/analyze_core5_weekly_external.py`
- `runtime/analyze_core5_weekly_6mo_external.py`
- `runtime/analyze_core5_month_external.py`

Validation:

```zsh
grep -R "TSLA,TQQQ,NVDA,SPY,QQQ" scripts runtime docs --include='*.sh' --include='*.py' --include='*.md'
grep -R "TSLA.*TQQQ.*NVDA.*SPY.*QQQ" scripts runtime docs --include='*.sh' --include='*.py' --include='*.md'
```

---

## 3. Runtime property requirements per symbol

Every onboarded symbol needs these runtime properties either in generated Databento bot properties or legacy top-level properties.

| Requirement | Purpose |
|---|---|
| `trading.symbol` | Runtime symbol identity |
| `trading.client-id` | Unique IBKR/client identifier |
| `server.port` | Unique bot HTTP/server port |
| `trading.market-data-request-id` | Unique market-data request ID |
| `trading.market-data.provider=databento` | Databento market-data path |
| `trading.databento.option-parents` | OPRA parent, e.g. `AMD.OPT` |
| `trading.databento.shared-feed.*` | Shared Databento feed relay shard/port/wait config |
| `trading.model.dir` | 30s setup model directory |
| `trading.state.file` | Per-symbol state file |
| `trading.log.file` | Per-symbol trade CSV |
| `logging.file.name` | Per-symbol application log |
| `trading.shared-capital.*` | Shared capital guard |
| `trading.ai.*threshold` | Setup/regime/open-threshold overrides |

### 3.1 Current `AMD` generated bot properties

`runtime/databento/bots/trading-amd.properties` contains:

```ini
trading.symbol=AMD
trading.client-id=217
server.port=9088
trading.market-data.provider=databento
trading.market-data-request-id=2108
trading.databento.option-parents=AMD.OPT
trading.databento.shared-feed.shard-id=2
trading.databento.shared-feed.port=9802
trading.state.file=runtime/databento/state/trader-state-AMD.properties
trading.log.file=runtime/databento/output/trades-AMD.csv
logging.file.name=runtime/databento/logs/trading-agent-AMD.log
```

### 3.2 Current `MU` generated bot properties

`runtime/databento/bots/trading-mu.properties` contains:

```ini
trading.symbol=MU
trading.client-id=263
server.port=9134
trading.market-data.provider=databento
trading.market-data-request-id=2154
trading.databento.option-parents=MU.OPT
trading.databento.shared-feed.shard-id=3
trading.databento.shared-feed.port=9803
trading.state.file=runtime/databento/state/trader-state-MU.properties
trading.log.file=runtime/databento/output/trades-MU.csv
logging.file.name=runtime/databento/logs/trading-agent-MU.log
```

### 3.3 Model-path caution

Generated `AMD`/`MU` property files currently contain `trading.model.dir` paths pointing at another worktree under `/Users/filmonghezehey/...`. The launcher currently overrides those with the local default research bundle when present:

```text
runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
```

and lifecycle/micro:

```text
runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
```

For a clean onboarding, make model paths local and durable by updating `runtime/databento/model-routing.csv` and regenerating properties, or by using explicit launcher/model-dir overrides for experiments.

### 3.4 Non-starting runtime preview

Run before any start:

```zsh
./run_symbol.sh AMD --require-prebuilt-jar --max-trades=0
./run_symbol.sh MU --require-prebuilt-jar --max-trades=0
```

Bulk preview:

```zsh
./start_all_databento_bots.sh --symbols=TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU
```

---

## 4. Databento data preparation

The current documented data-prep path uses:

- equity: `EQUS.MINI / tbbo`
- options: `OPRA.PILLAR / ohlcv-1s`

The raw builder emits:

- 1-second per-symbol CSVs
- 5-second per-symbol CSVs
- 30-second per-symbol CSVs
- combined multi-symbol CSVs
- `symbol_model_plan.csv`

### 4.1 Build expanded pilot bars

Template:

```zsh
python3 build_30s_from_5s_csv.py \
  --dbeq-dir "/path/to/EQUS.MINI_download" \
  --opra-dir "/path/to/OPRA.PILLAR_download" \
  --output-dir training_data/databento_30s_expanded_pilot \
  --output-dir-5s training_data/databento_5s_expanded_pilot \
  --output-dir-1s training_data/databento_1s_expanded_pilot \
  --combined-output-csv training_data/databento_30s_expanded_pilot_combined.csv \
  --combined-output-csv-5s training_data/databento_5s_expanded_pilot_combined.csv \
  --combined-output-csv-1s training_data/databento_1s_expanded_pilot_combined.csv \
  --assessment-report training_data/databento_30s_expanded_pilot/symbol_model_plan.csv \
  --symbols TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU
```

For future symbols, update `--symbols` or use the expanded symbol file where supported.

### 4.2 Expected outputs

Per-symbol outputs should exist for each pilot symbol:

```text
training_data/databento_1s_expanded_pilot/<SYMBOL>_1s_training.csv
training_data/databento_5s_expanded_pilot/<SYMBOL>_5s_training.csv
training_data/databento_30s_expanded_pilot/<SYMBOL>_30s_training.csv
```

Combined outputs:

```text
training_data/databento_1s_expanded_pilot_combined.csv
training_data/databento_5s_expanded_pilot_combined.csv
training_data/databento_30s_expanded_pilot_combined.csv
training_data/databento_30s_expanded_pilot/symbol_model_plan.csv
```

### 4.3 Verify the build

```zsh
python3 scripts/verify_databento_pilot_build.py \
  --output-dir training_data/databento_30s_expanded_pilot \
  --expected-symbol TSLA \
  --expected-symbol TQQQ \
  --expected-symbol NVDA \
  --expected-symbol SPY \
  --expected-symbol QQQ \
  --expected-symbol AMD \
  --expected-symbol MU
```

Inspect the model plan:

```zsh
head -20 training_data/databento_30s_expanded_pilot/symbol_model_plan.csv
```

Check that `AMD` and `MU` have enough days, rows, option activity, and acceptable spread/quality metrics.

---

## 5. Stage training inputs

Use `scripts/stage_broader_window_inputs.py` to make an expanded pilot training slice.

```zsh
python3 scripts/stage_broader_window_inputs.py \
  --input-30s training_data/databento_30s_expanded_pilot_combined.csv \
  --input-5s training_data/databento_5s_expanded_pilot_combined.csv \
  --output-dir training_data/expanded_pilot_training_slice \
  --symbols TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU \
  --start-date 2025-07-21 \
  --end-date-exclusive 2026-05-23 \
  --min-unique-days 100 \
  --min-rows-30s 100000 \
  --min-rows-5s 600000
```

Expected staged inputs:

```text
training_data/expanded_pilot_training_slice/combined_30s.csv
training_data/expanded_pilot_training_slice/combined_5s.csv
```

---

## 6. Train expanded 30-second setup models

Use `train_30s_models.py`.

Template:

```zsh
MODEL_FAMILY=catboost \
REGIME_MODEL_FAMILY=catboost \
python3 train_30s_models.py \
  --input-csv training_data/expanded_pilot_training_slice/combined_30s.csv \
  --output-dir runtime/research_runs/expanded_pilot_setup_$(date +%Y%m%d_%H%M%S)
```

Expected setup artifacts:

```text
setup_scorecard.csv
setup_manifest.json
setup_runtime_thresholds.properties
threshold_grid.csv
calibration_manifest.json
calibration_reliability.csv
oof_setup_predictions.csv
long_entry.onnx
short_entry.onnx
regime_classifier.onnx
choppy_long_entry.onnx
choppy_short_entry.onnx
trend_long_entry.onnx
trend_short_entry.onnx
volatile_long_entry.onnx
volatile_short_entry.onnx
open30_long_entry.onnx
open30_short_entry.onnx
```

The important artifact for lifecycle/micro training is:

```text
oof_setup_predictions.csv
```

Verify `AMD` and `MU` are represented:

```zsh
python3 -c "import pandas as pd; df=pd.read_csv('runtime/research_runs/<SETUP_RUN>/oof_setup_predictions.csv', usecols=['Symbol']); print(sorted(df.Symbol.str.upper().unique()))"
```

---

## 7. Generate standalone OOF setup predictions, if needed

If the setup training run did not produce the needed OOF file, use `generate_walk_forward_setup_predictions.py`.

```zsh
python3 generate_walk_forward_setup_predictions.py \
  --input-30s-csv training_data/expanded_pilot_training_slice/combined_30s.csv \
  --output-csv runtime/research_runs/expanded_pilot_setup_oof/oof_setup_predictions.csv \
  --manifest-json runtime/research_runs/expanded_pilot_setup_oof/manifest.json \
  --model-family catboost \
  --regime-model-family catboost \
  --min-train-days 20 \
  --test-days-per-fold 1
```

Do not train lifecycle/micro models from bootstrap setup proxies for promotion-quality work.

---

## 8. Train lifecycle and 5-second micro models

Use `train_lifecycle_micro_models.py`.

```zsh
python3 train_lifecycle_micro_models.py \
  --input-30s-csv training_data/expanded_pilot_training_slice/combined_30s.csv \
  --input-5s-csv training_data/expanded_pilot_training_slice/combined_5s.csv \
  --setup-predictions-csv runtime/research_runs/<SETUP_RUN>/oof_setup_predictions.csv \
  --output-dir runtime/research_runs/expanded_pilot_lifecycle_micro_$(date +%Y%m%d_%H%M%S)/model_exports \
  --posthoc-calibration both
```

Expected lifecycle/micro artifacts:

```text
lifecycle_micro_scorecard.csv
lifecycle_micro_route_manifest.json
feature_schema.json
feature_schema.sha256
calibration_manifest.json
calibration_reliability.csv
posthoc_calibration_comparison.csv
posthoc_calibration_reliability.csv
posthoc_calibrators.json
long_exit_lifecycle.onnx
short_exit_lifecycle.onnx
long_micro_entry_5s.onnx
short_micro_entry_5s.onnx
long_micro_exit_guard_5s.onnx
short_micro_exit_guard_5s.onnx
```

Never use this for promotion-quality onboarding unless setup predictions are real OOF predictions.

---

## 9. Calibrate thresholds

### 9.1 Setup thresholds

The setup model directory should contain:

```text
setup_runtime_thresholds.properties
```

This file is consumed by:

- `run_symbol.sh`
- `scripts/run_databento_historical_streaming_backtest_20260523.sh`

### 9.2 Lifecycle/micro thresholds

Lifecycle and micro thresholds are read from:

```text
lifecycle_micro_scorecard.csv
```

The wrappers use thresholds for:

```text
longExitLifecycleAi
shortExitLifecycleAi
longMicroEntryAi
shortMicroEntryAi
longMicroExitGuardAi
shortMicroExitGuardAi
```

### 9.3 Per-symbol micro-entry thresholds

Current per-symbol file:

```text
config/databento_calibrated_micro_entry_thresholds.csv
```

Add `AMD`/`MU` rows only after calibration/backtest evidence supports them:

```csv
AMD,<long_threshold>,<short_threshold>,advance,<date>,Expanded pilot calibration
MU,<long_threshold>,<short_threshold>,advance,<date>,Expanded pilot calibration
```

Do not guess these values. Use grid search/backtest evidence.

Relevant scripts to review/use:

- `scripts/run_databento_micro_threshold_grid_search.sh`
- `scripts/run_databento_exit_threshold_grid_search.sh`
- `scripts/run_databento_all_symbol_micro_threshold_grid_parallel.sh`
- `runtime/aggregate_calibration_selected_thresholds.py`
- `runtime/compare_calibration_vs_current.py`

---

## 10. Update model routing and regenerate bot properties

### 10.1 Update `runtime/databento/model-routing.csv`

For newly trained expanded-pilot models, route `AMD` and `MU` to local durable model directories.

Current `AMD`/`MU` rows exist, but their `model_dir` fields may point to old external worktree paths. Replace those with local promoted/research run paths before formal use.

### 10.2 Regenerate Databento bot properties

Use:

```zsh
python3 scripts/generate_databento_bot_properties.py \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --restrict-to-requested-symbols
```

After generation, verify `AMD`/`MU` fields:

```zsh
grep -E '^(trading.symbol|trading.client-id|server.port|trading.market-data-request-id|trading.databento.shared-feed.port|trading.databento.shared-feed.shard-id|trading.model.dir|trading.databento.option-parents)=' runtime/databento/bots/trading-amd.properties
```

```zsh
grep -E '^(trading.symbol|trading.client-id|server.port|trading.market-data-request-id|trading.databento.shared-feed.port|trading.databento.shared-feed.shard-id|trading.model.dir|trading.databento.option-parents)=' runtime/databento/bots/trading-mu.properties
```

Check port conflicts:

```zsh
lsof -nP -iTCP:9088 -sTCP:LISTEN
lsof -nP -iTCP:9134 -sTCP:LISTEN
```

---

## 11. Historical backtesting

### 11.1 Dry-run model/load check

```zsh
DRY_RUN=true \
BACKTEST_MAX_TRADES=0 \
TIMEOUT_SECONDS=30 \
scripts/run_databento_historical_streaming_backtest_20260523.sh AMD
```

```zsh
DRY_RUN=true \
BACKTEST_MAX_TRADES=0 \
TIMEOUT_SECONDS=30 \
scripts/run_databento_historical_streaming_backtest_20260523.sh MU
```

This validates:

- model load
- feature counts
- lifecycle/micro route manifest
- Databento streamer command construction
- IBKR simulation mode

### 11.2 Real historical API backtest

Requires a valid `DATABENTO_API_KEY`.

```zsh
START_DATE=2026-06-25 \
END_DATE=2026-07-31 \
BACKTEST_MAX_TRADES=2000 \
scripts/run_databento_historical_streaming_backtest_20260523.sh AMD
```

```zsh
START_DATE=2026-06-25 \
END_DATE=2026-07-31 \
BACKTEST_MAX_TRADES=2000 \
scripts/run_databento_historical_streaming_backtest_20260523.sh MU
```

### 11.3 Expanded pilot API backtest

After `config/databento_core_5_symbols.txt` includes `AMD` and `MU`:

```zsh
CORE5_SYMBOLS_FILE=config/databento_core_5_symbols.txt \
scripts/run_databento_api_backtest_core5.sh \
  --start 2026-06-25 \
  --end 2026-07-31
```

Dry-run:

```zsh
CORE5_SYMBOLS_FILE=config/databento_core_5_symbols.txt \
scripts/run_databento_api_backtest_core5.sh \
  --start 2026-06-25 \
  --end 2026-07-31 \
  --dry-run \
  --timeout-seconds 60
```

### 11.4 Prepare recorded-events / NDJSON file replay fixtures

Use file replay when onboarding or calibrating symbols so the backtest is reproducible and does not depend on a fresh Databento API download. A valid recorded-events file is a normalized `.ndjson` or `.ndjson.gz` stream containing:

- `status`
- `previous_close`
- `equity_bar`
- optionally `option_bar`

For downstream setup-filter parity, prefer an event-carried enriched fixture: the setup-filter sidecar rows are injected into `equity_bar` events as `enriched_features`, and Java replays the file **without** `--downstream-setup-filter-features-csv`.

> Important: a Core-5 replay file is useful for reproducing Core-5 backtests. For `AMD`/`MU` onboarding, the recorded-events file must actually contain `AMD`/`MU` bars before it can validate those symbols.

Related whole-range workflow: use `docs/event_carried_whole_range_runbook.md` when you need the full continuous-range pipeline instead of a manually prepared fixture. That runbook drives `scripts/run_event_carried_whole_range.sh` to build a continuous sliced replay with daily `previous_close` events, validate slice coverage, generate the matching downstream setup sidecar, inject event-carried feature snapshots, run no-sidecar validation, run the trade-enabled replay, and produce PnL summaries. Treat this section as the onboarding checklist and `docs/event_carried_whole_range_runbook.md` as the detailed whole-range execution runbook.

#### 11.4.1 Start from the repository root

```zsh
cd /Users/FXG06FA/trading-agent-main
```

#### 11.4.2 Option A — use an existing recovered/event-carried file

If a recovered event-carried file already exists for the symbol universe and date range you need, use it directly as `--recorded-events`.

If it does not exist yet, follow `docs/event_carried_whole_range_runbook.md` to produce the continuous whole-range event-carried output first, then return here for symbol onboarding checks and threshold-grid replay.

Example Core-5 recovered file used by the controlled July 2026 replay work:

```zsh
BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
RECORDED_EVENTS="$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz"

test -f "$RECORDED_EVENTS" || { echo "Missing recorded events: $RECORDED_EVENTS"; exit 1; }
```

For expanded onboarding, replace `RECORDED_EVENTS` with the expanded-pilot NDJSON/NDJSON.GZ that includes the candidate symbols.

#### 11.4.3 Option B — build a small diagnostic replay seed from training bars

Use this for smoke tests and ingestion checks. It is not promotion-quality evidence by itself.

```zsh
python3 scripts/build_recorded_events_from_training_bars.py \
  --input-csv runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol/TSLA_5s_training.csv \
  --symbol TSLA \
  --date 2025-07-21 \
  --output runtime/replay/example-recorded-events.ndjson.gz
```

Expected builder shape:

```text
RECORDED_EVENTS_BUILD=PASS
output=runtime/replay/example-recorded-events.ndjson.gz
manifest=runtime/replay/example-recorded-events.manifest.json
```

The default `--epoch-source timestamp` derives emitted `barEpochSec` values from the CSV `Timestamp` column and floors to `--bar-seconds 5`. Use `--epoch-source floor-bar-epoch` or `--epoch-source bar-epoch` only for diagnostics.

#### 11.4.4 Validate the raw recorded stream

Run these before using the file in Java or in a threshold grid:

```zsh
python3 -m py_compile \
  scripts/build_recorded_events_from_training_bars.py \
  scripts/databento_historical_streamer.py \
  scripts/inject_downstream_setup_features_into_ndjson.py

python3 tests/test_build_recorded_events_from_training_bars.py

python3 scripts/databento_historical_streamer.py \
  --source ndjson \
  --symbols TSLA \
  --input-file runtime/replay/example-recorded-events.ndjson.gz
```

Healthy smoke output includes:

```text
RECORDED_EVENTS_VERIFY=PASS
STREAMER_SMOKE=PASS
```

#### 11.4.5 Inject downstream setup-filter features for event-carried parity

Use this when validating the downstream setup-quality filter through the production-like event-carried snapshot path.

For a one-command continuous-range build that also generates labels, joins the enriched 30-second cache, injects snapshots, runs no-trade validation, runs trade-enabled replay, and summarizes lifecycle PnL, prefer `docs/event_carried_whole_range_runbook.md` / `scripts/run_event_carried_whole_range.sh`. Use the manual injection command below when you already have the source `EVENTS`, `SIDECAR`, and `MANIFEST` artifacts.

```zsh
BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz
SIDECAR=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
MANIFEST=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated/downstream_setup_filter_route_manifest.json
ENRICHED_EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.event-snapshots-catboost-core.ndjson.gz

python3 scripts/inject_downstream_setup_features_into_ndjson.py \
  --input-events "$EVENTS" \
  --sidecar-csv "$SIDECAR" \
  --manifest "$MANIFEST" \
  --output-events "$ENRICHED_EVENTS" \
  --snapshot-source setup_downstream_training_rows_v1
```

Expected injection success shape:

```text
INJECT_EVENT_SNAPSHOTS status=PASS ... injected=<N> expected=<N> unmatched=0
```

Inspect the generated summary before replay:

```zsh
python3 -m json.tool "$ENRICHED_EVENTS.summary.json" | head -120

python3 - "$ENRICHED_EVENTS.summary.json" <<'PY'
import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text())
injection = summary.get("injection", {})
carrier = summary.get("carrier_assignment", {})
print("snapshot_rows_expected", injection.get("snapshot_rows_expected"))
print("snapshot_rows_injected", injection.get("snapshot_rows_injected"))
print("snapshot_rows_unmatched", injection.get("snapshot_rows_unmatched"))
print("snapshot_hit_rate_vs_sidecar", injection.get("snapshot_hit_rate_vs_sidecar"))
print("multi_snapshot_carrier_events", carrier.get("multi_snapshot_carrier_events"))
PY
```

Required checks for a clean parity fixture:

```text
snapshot_rows_expected == snapshot_rows_injected
snapshot_rows_unmatched == 0
snapshot_hit_rate_vs_sidecar == 1.0
multi_snapshot_carrier_events == 0
```

#### 11.4.6 Run a no-sidecar / no-trade file replay smoke

Build Java once before `SKIP_BUILD=true` replays:

```zsh
./mvnw -q -DskipTests package
```

Run the enriched file without a sidecar CSV. Keep `--micro-entry-research-no-trade` enabled for the first smoke:

```zsh
OUT=$BASE/controlled_java_replay_event_snapshot_notrade_$(date +%Y%m%d_%H%M%S)
rm -rf "$OUT"
mkdir -p "$OUT"

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols TSLA,TQQQ,NVDA,SPY,QQQ \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-dir "$OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --micro-entry-research-no-trade \
  --timeout-seconds 0 \
  > "$OUT/controlled_java_replay.log" 2>&1
```

For expanded symbols, change `--symbols` to the symbols present in the recorded-events file, for example:

```zsh
--symbols TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU
```

Check completion and event-carried telemetry:

```zsh
grep '\[BACKTEST\] completed' "$OUT/controlled_java_replay.log" | tail -5

printf 'SETUP PASS '; grep -c 'SETUP_FILTER_PASSES=PASS' "$OUT/controlled_java_replay.log"
printf 'SETUP FAIL '; grep -c 'SETUP_FILTER_PASSES=FAIL' "$OUT/controlled_java_replay.log"
printf 'MICRO PASS '; grep -c 'MICRO_ENTRY_CONFIRMS=PASS' "$OUT/controlled_java_replay.log"
printf 'MICRO FAIL '; grep -c 'MICRO_ENTRY_CONFIRMS=FAIL' "$OUT/controlled_java_replay.log"
printf 'SNAPSHOT HIT '; grep -c 'featureSnapshot=hit' "$OUT/controlled_java_replay.log"
printf 'SNAPSHOT MISS '; grep -c 'featureSnapshot=miss' "$OUT/controlled_java_replay.log"
printf 'SIDECAR DISABLED '; grep -c 'featureSidecar=disabled' "$OUT/controlled_java_replay.log"
```

Healthy event-carried parity telemetry should have:

```text
[BACKTEST] completed=<N> failed=0 requested=<N>
featureSnapshot=hit on setup-filter scoring rows
featureSnapshot=miss count 0
featureSidecar=disabled on setup-filter scoring rows
sidecar_hit count 0
```

#### 11.4.7 Run a one-combo file replay threshold-grid smoke

Use `scripts/run_databento_micro_threshold_grid_search.sh` in `--source ndjson` mode after the no-trade smoke passes. First run one threshold pair to verify wiring:

```zsh
SYMBOL=TSLA
GRID_OUT=$BASE/micro_threshold_grid_${SYMBOL}_event_snapshot_notrade_smoke_$(date +%Y%m%d_%H%M%S)

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

scripts/run_databento_micro_threshold_grid_search.sh \
  --symbol "$SYMBOL" \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-base "$GRID_OUT" \
  --thresholds "0.30" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-entry-research-no-trade \
  --timeout-seconds 0 \
  --skip-build
```

Inspect:

```zsh
cat "$GRID_OUT/grid_results.csv"
cat "$GRID_OUT/top_thresholds.tsv"
grep -hE '\[BACKTEST\] completed|featureSnapshot=|featureSidecar=' "$GRID_OUT"/L*-S*/*.log | tail -80
```

#### 11.4.8 Run the trade-enabled file replay grid

After the no-trade replay and telemetry checks pass, remove `--micro-entry-research-no-trade` and run the actual threshold grid:

```zsh
SYMBOL=TSLA
GRID_OUT=$BASE/micro_threshold_grid_${SYMBOL}_event_snapshot_trade_$(date +%Y%m%d_%H%M%S)

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

scripts/run_databento_micro_threshold_grid_search.sh \
  --symbol "$SYMBOL" \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-base "$GRID_OUT" \
  --thresholds "0.25 0.30 0.35" \
  --trade-amount 60000 \
  --max-order-notional 500000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --timeout-seconds 0 \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --skip-build
```

For `AMD`/`MU`, set `SYMBOL=AMD` or `SYMBOL=MU` and ensure `ENRICHED_EVENTS` contains that symbol. Compare candidates using `grid_results.csv`, `top_thresholds.tsv`, per-combo logs, total PnL, realized R, win rate, median R, MAE/MFE, symbol/side stability, and watchdog/error counts.

#### 11.4.9 Optional drift comparison against a sidecar-exact reference

If you have a sidecar-exact no-trade reference log, compare the event-carried replay against it:

```zsh
REFERENCE_LOG=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260718_sidecar_exact_notrade/controlled_java_replay.log

python3 scripts/compare_event_snapshot_replay_drift.py \
  --event-log "$OUT/controlled_java_replay.log" \
  --sidecar-csv "$SIDECAR" \
  --reference-log "$REFERENCE_LOG" \
  --output-dir "$OUT/event_snapshot_replay_drift" \
  --strict-no-trade \
  --require-sidecar-disabled \
  --min-feature-snapshot-hit-rate 1.0 \
  --max-probability-drift 0.0001 \
  --fail-on-no-go
```

Expected:

```text
EVENT_SNAPSHOT_REPLAY_DRIFT status=PASS
```

### 11.5 No-retraining `AMD`/`MU` transfer backtest with existing Core-5 models

Use this path to answer one research question only:

```text
Can the existing Core-5 setup, downstream setup-filter, lifecycle, and micro-entry models transfer to AMD/MU without retraining?
```

This is a robustness/transfer test, not a promotion path. Do **not** add `AMD`/`MU` calibrated thresholds or enable paper/live entries from this result alone. If the transfer test is promising, use it to justify expanded training/calibration.

#### 11.5.1 Minimum changes required

| Area | Minimum for offline file backtest | What not to change for this test |
|---|---|---|
| 30-second setup models | Reuse `runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854` and its `setup_runtime_thresholds.properties`. | Do not retrain setup ONNX models. |
| Lifecycle/micro models | Reuse `runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports`. | Do not retrain lifecycle/micro ONNX models. |
| Downstream setup filter | Reuse the existing downstream setup-filter route manifest that was validated for event-carried replay. | Do not build a new route manifest unless you retrain the downstream filter. |
| Property files | `runtime/databento/bots/trading-amd.properties` and `runtime/databento/bots/trading-mu.properties` already exist for runtime previews. For offline backtests, pass `--model-dir` and `--lifecycle-model-dir` explicitly, so property edits are not required. | Do not promote stale absolute `trading.model.dir` paths to live/paper. |
| Symbol files | Explicit `--symbols AMD,MU` / `SYMBOLS=AMD,MU` is enough for file replay. Add `AMD`/`MU` to `config/databento_core_5_symbols.txt`, `runtime/pilot_core_5_symbols.txt`, and `config/databento_dynamic_upgrade_pilot_symbols.csv` only when you intentionally expand the pilot contract. | Do not permanently expand live/paper symbol lists just to run this research test. |
| Micro thresholds | Run at least two baselines: scorecard thresholds from `lifecycle_micro_scorecard.csv` and the controlled event-carried replay thresholds `0.30/0.30`. Then grid around them. | Do not add `AMD`/`MU` rows to `config/databento_calibrated_micro_entry_thresholds.csv` until after calibration evidence. |
| Recorded events | The NDJSON/NDJSON.GZ must contain `AMD` and `MU` events with `previous_close`, `equity_bar`, and preferably `option_bar`. | Do not use a Core-5-only file to judge AMD/MU. It cannot validate symbols it does not contain. |
| 30s enriched cache | `INPUT_30S_CSV` must contain `AMD`/`MU` rows when generating downstream setup-filter sidecar rows for event-carried snapshots. | Do not inject sidecar features from a CSV that lacks the tested symbols. |

Existing model/threshold artifacts to reuse:

```zsh
SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
SETUP_THRESHOLDS_FILE=$SETUP_MODEL_DIR/setup_runtime_thresholds.properties
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
LIFECYCLE_SCORECARD=$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv
LIFECYCLE_ROUTE_MANIFEST=$LIFECYCLE_MODEL_DIR/lifecycle_micro_route_manifest.json
```

Relevant existing thresholds:

```text
setup_runtime_thresholds.properties:
  trading.ai.long-entry-threshold=0.612
  trading.ai.short-entry-threshold=0.612
  trading.ai.regime-threshold=0.50
  open30 long/short: 0.620 / 0.624

lifecycle_micro_scorecard.csv:
  longExitLifecycleAi=0.52
  shortExitLifecycleAi=0.50
  longMicroEntryAi=0.64
  shortMicroEntryAi=0.62
  longMicroExitGuardAi=0.60
  shortMicroExitGuardAi=0.60

controlled event-carried replay baseline:
  --micro-long-entry-threshold 0.30
  --micro-short-entry-threshold 0.30
```

`config/databento_calibrated_micro_entry_thresholds.csv` currently only contains `TSLA` and `TQQQ`. Leave it unchanged for this transfer test.

#### 11.5.2 Preflight the required files

Set these paths for the workstation where the real Databento files live:

```zsh
cd /Users/FXG06FA/trading-agent-main

SYMBOLS=AMD,MU
SLICE_SYMBOLS=AMD,MU

# Must contain AMD/MU status, previous_close, equity_bar, and preferably option_bar events.
SOURCE_EVENTS=/path/to/databento-expanded-pilot-with-amd-mu.ndjson.gz

# Must contain AMD/MU enriched 30s rows for downstream setup-filter sidecar generation.
INPUT_30S_CSV=/path/to/expanded-pilot-combined-30s.csv

SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

test -f "$SOURCE_EVENTS" || { echo "Missing SOURCE_EVENTS=$SOURCE_EVENTS"; exit 1; }
test -e "$INPUT_30S_CSV" || { echo "Missing INPUT_30S_CSV=$INPUT_30S_CSV"; exit 1; }
test -d "$SETUP_MODEL_DIR" || { echo "Missing SETUP_MODEL_DIR=$SETUP_MODEL_DIR"; exit 1; }
test -d "$LIFECYCLE_MODEL_DIR" || { echo "Missing LIFECYCLE_MODEL_DIR=$LIFECYCLE_MODEL_DIR"; exit 1; }
test -f "$ROUTE_MANIFEST" || { echo "Missing ROUTE_MANIFEST=$ROUTE_MANIFEST"; exit 1; }
test -f "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties" || exit 1
test -f "$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv" || exit 1
```

If using `scripts/run_event_carried_whole_range.sh`, also verify its local slice helpers exist. They are referenced by the whole-range script and may be generated/local-backtest artifacts on the trading Mac:

```zsh
test -f runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/build_4week_slice_with_daily_prevclose.py || \
  echo "Missing slice builder required by scripts/run_event_carried_whole_range.sh"

test -f runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/validate_4week_slice.py || \
  echo "Missing slice validator required by scripts/run_event_carried_whole_range.sh"
```

#### 11.5.3 Verify the recorded-events file contains `AMD`/`MU`

```zsh
python3 - "$SOURCE_EVENTS" <<'PY'
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
open_fn = gzip.open if path.suffix == ".gz" else open
counts = Counter()
events = Counter()
with open_fn(path, "rt", encoding="utf-8", errors="ignore") as handle:
    for line in handle:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        symbol = str(payload.get("symbol") or payload.get("Symbol") or "").upper()
        event = str(payload.get("event") or payload.get("EventType") or "").lower()
        if symbol in {"AMD", "MU"}:
            counts[symbol] += 1
            events[(symbol, event)] += 1

print("symbol_counts", dict(counts))
for key, value in sorted(events.items()):
    print(key, value)
missing = {"AMD", "MU"} - set(counts)
if missing:
    raise SystemExit(f"missing symbols in recorded events: {sorted(missing)}")
PY
```

Validate the streamer path directly:

```zsh
python3 scripts/databento_historical_streamer.py \
  --source ndjson \
  --symbols AMD,MU \
  --input-file "$SOURCE_EVENTS" \
  > /tmp/amd_mu_ndjson_streamer_smoke.ndjson

tail -20 /tmp/amd_mu_ndjson_streamer_smoke.ndjson
grep -c 'recorded-ndjson-replay-complete' /tmp/amd_mu_ndjson_streamer_smoke.ndjson
```

#### 11.5.4 Verify the enriched 30s cache contains `AMD`/`MU`

```zsh
python3 - "$INPUT_30S_CSV" <<'PY'
import csv
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
counts = Counter()
with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
    reader = csv.DictReader(handle)
    symbol_col = "Symbol" if "Symbol" in (reader.fieldnames or []) else "symbol"
    for row in reader:
        symbol = str(row.get(symbol_col, "")).upper()
        if symbol in {"AMD", "MU"}:
            counts[symbol] += 1
print("30s_rows", dict(counts))
missing = {"AMD", "MU"} - set(counts)
if missing:
    raise SystemExit(f"missing symbols in INPUT_30S_CSV: {sorted(missing)}")
PY
```

#### 11.5.5 Preview generated runtime properties, but do not rely on them for offline backtest

Generated bot properties already exist for runtime previews:

```zsh
grep -E '^(trading.symbol|trading.client-id|server.port|trading.market-data-request-id|trading.model.dir|trading.databento.option-parents|trading.ai\.|trading.risk\.|trading.trade-amount)=' \
  runtime/databento/bots/trading-amd.properties

grep -E '^(trading.symbol|trading.client-id|server.port|trading.market-data-request-id|trading.model.dir|trading.databento.option-parents|trading.ai\.|trading.risk\.|trading.trade-amount)=' \
  runtime/databento/bots/trading-mu.properties
```

For this offline transfer test, prefer explicit backtest arguments instead of editing property files:

```text
--model-dir "$SETUP_MODEL_DIR"
--setup-thresholds-file "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties"
--lifecycle-model-dir "$LIFECYCLE_MODEL_DIR"
```

Only fix `trading.model.dir` in `runtime/databento/bots/trading-amd.properties` / `trading-mu.properties` before paper/live or `run_symbol.sh` startup previews.

#### 11.5.6 Build the event-carried whole-range fixture for `AMD`/`MU`

Use `docs/event_carried_whole_range_runbook.md` as the detailed execution reference. For the no-retrain transfer test, override symbols and models explicitly:

```zsh
START=2025-07-22
END=2026-05-22
START_TAG=$(printf '%s' "$START" | tr -d '-')
END_TAG=$(printf '%s' "$END" | tr -d '-')
BASE=runtime/local-backtests/databento-amd-mu-whole-${START_TAG}-${END_TAG}-core5-transfer-event-carried
mkdir -p "$BASE"

nohup env \
  SOURCE_EVENTS="$SOURCE_EVENTS" \
  SYMBOLS=AMD,MU \
  SLICE_SYMBOLS=AMD,MU \
  START="$START" \
  END="$END" \
  BASE="$BASE" \
  INPUT_30S_CSV="$INPUT_30S_CSV" \
  ROUTE_MANIFEST="$ROUTE_MANIFEST" \
  SETUP_MODEL_DIR="$SETUP_MODEL_DIR" \
  LIFECYCLE_MODEL_DIR="$LIFECYCLE_MODEL_DIR" \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=0 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch_notrade.log" 2>&1 &

echo "PID=$!"
tail -f "$BASE/whole_range_batch_notrade.log"
```

Expected completion marker:

```text
[WHOLE_RANGE] DONE
```

Important outputs:

```text
$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.ndjson.gz
$BASE/setup_micro_counterfactual_<timestamp>/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade/controlled_java_replay.log
```

Check no-trade event-carried telemetry:

```zsh
NO_TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade

grep '\[BACKTEST\] completed' "$NO_TRADE_OUT/controlled_java_replay.log" | tail -5
printf 'SETUP PASS '; grep -c 'SETUP_FILTER_PASSES=PASS' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SETUP FAIL '; grep -c 'SETUP_FILTER_PASSES=FAIL' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'MICRO PASS '; grep -c 'MICRO_ENTRY_CONFIRMS=PASS' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'MICRO FAIL '; grep -c 'MICRO_ENTRY_CONFIRMS=FAIL' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT HIT '; grep -c 'featureSnapshot=hit' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT MISS '; grep -c 'featureSnapshot=miss' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR DISABLED '; grep -c 'featureSidecar=disabled' "$NO_TRADE_OUT/controlled_java_replay.log"
```

No-trade replay must complete before running trade-enabled tests. If `featureSnapshot=miss` is non-zero, fix the event-carried fixture before continuing.

#### 11.5.7 Run transfer baselines

Set the enriched file produced by the previous step:

```zsh
ENRICHED_EVENTS=$BASE/databento-${START_TAG}-${END_TAG}-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
test -f "$ENRICHED_EVENTS" || { echo "Missing ENRICHED_EVENTS=$ENRICHED_EVENTS"; exit 1; }
```

Baseline A — strict lifecycle scorecard thresholds from the existing model bundle (`0.64/0.62` micro entry):

```zsh
OUT=$BASE/parallel_file_replay_amd_mu_scorecard_micro_transfer_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

PYTHON_BIN=python3 \
python3 scripts/run_parallel_databento_file_backtest.py \
  --recorded-events "$ENRICHED_EVENTS" \
  --symbols AMD,MU \
  --jobs 2 \
  --output-dir "$OUT" \
  --trade-amount 60000 \
  --max-order-notional 500000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --model-dir "$SETUP_MODEL_DIR" \
  --setup-thresholds-file "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties" \
  --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR" \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --timeout-seconds 0 \
  --title "AMD/MU Core-5 transfer scorecard micro thresholds"
```

Baseline B — controlled event-carried threshold parity (`0.30/0.30` micro entry):

```zsh
OUT=$BASE/parallel_file_replay_amd_mu_micro_0p30_0p30_transfer_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

PYTHON_BIN=python3 \
python3 scripts/run_parallel_databento_file_backtest.py \
  --recorded-events "$ENRICHED_EVENTS" \
  --symbols AMD,MU \
  --jobs 2 \
  --output-dir "$OUT" \
  --trade-amount 60000 \
  --max-order-notional 500000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --model-dir "$SETUP_MODEL_DIR" \
  --setup-thresholds-file "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties" \
  --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR" \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --timeout-seconds 0 \
  --title "AMD/MU Core-5 transfer micro 0.30/0.30" \
  -- \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30
```

Summarize either output:

```zsh
python3 - "$OUT" <<'PY'
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
summary = json.loads((out / "parallel_file_replay_summary.json").read_text())
pnl_path = out / "parallel_pnl_period_summary.json"
pnl = json.loads(pnl_path.read_text()) if pnl_path.exists() else {}

print("OUTPUT", out)
print("completed", summary.get("completed"), "failed", summary.get("failed"), "requested", summary.get("requested"))
print("config", summary.get("config", {}))
print("markers", pnl.get("log", {}).get("markers", {}))
print("errors", pnl.get("log", {}).get("errors"), "watchdogs", pnl.get("log", {}).get("watchdogs"))
for row in pnl.get("all", []):
    print("ALL", row)
for row in pnl.get("by_symbol", []):
    print("BY_SYMBOL", row)
for row in pnl.get("by_symbol_side", []):
    print("BY_SYMBOL_SIDE", row)
PY
```

Check each replay used event-carried snapshots, not sidecar CSV:

```zsh
grep -hE '\[BACKTEST\] lifecycle_micro_enabled=|\[BACKTEST\] micro_entry_thresholds|\[BACKTEST\] downstream_setup_filter|\[BACKTEST\] trade_amount=' "$OUT/logs/"*.log | sort -u

grep -hE 'featureSnapshot=miss|featureSidecar=hit|Exception|ERROR|WATCHDOG|Order hung' "$OUT/controlled_java_replay.parallel_combined.log" | head -160
```

#### 11.5.8 Run per-symbol threshold grids after baselines

Run `AMD` and `MU` separately so one symbol cannot hide the other.

```zsh
for SYMBOL in AMD MU; do
  GRID_OUT=$BASE/micro_threshold_grid_${SYMBOL}_core5_transfer_$(date +%Y%m%d_%H%M%S)

  unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

  scripts/run_databento_micro_threshold_grid_search.sh \
    --symbol "$SYMBOL" \
    --source ndjson \
    --recorded-events "$ENRICHED_EVENTS" \
    --output-base "$GRID_OUT" \
    --thresholds "0.25 0.30 0.35 0.40 0.45 0.50 0.58 0.62 0.64" \
    --trade-amount 60000 \
    --max-order-notional 500000 \
    --max-share-cap 2000 \
    --max-trades 2000 \
    --model-dir "$SETUP_MODEL_DIR" \
    --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR" \
    --timeout-seconds 0 \
    --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
    --skip-build

  echo "GRID_OUT=$GRID_OUT"
  cat "$GRID_OUT/top_thresholds.tsv"
done
```

Promotion-quality criteria are intentionally higher than “positive PnL”. For this transfer-only test, look for:

- both `AMD` and `MU` complete with `failed=0`
- no `featureSnapshot=miss`
- no `featureSidecar=hit` when replaying event-carried mode
- no repeated watchdog/order-hung errors
- enough trade count to be meaningful
- positive total realized R and PnL
- acceptable win rate and median realized R
- no single day/side/symbol dominance
- MAE/MFE profile not materially worse than Core-5 baseline
- thresholds stable across nearby grid points, not a one-cell spike

If one symbol passes and the other fails, keep the conclusion symbol-specific. Do not infer `MU` from `AMD` or vice versa.

#### 11.5.9 Optional diagnostic-only seed from per-symbol training bars

If you only have per-symbol 5-second training CSVs and want to smoke-test Java/NDJSON ingestion, build diagnostic files. This is not a whole-range parity fixture and should not be used as promotion evidence.

```zsh
python3 scripts/build_recorded_events_from_training_bars.py \
  --input-csv /path/to/AMD_5s_training.csv \
  --symbol AMD \
  --date YYYY-MM-DD \
  --output runtime/replay/AMD-diagnostic-recorded-events.ndjson.gz

python3 scripts/build_recorded_events_from_training_bars.py \
  --input-csv /path/to/MU_5s_training.csv \
  --symbol MU \
  --date YYYY-MM-DD \
  --output runtime/replay/MU-diagnostic-recorded-events.ndjson.gz
```

The streamer accepts repeated/comma-separated recorded files:

```zsh
python3 scripts/databento_historical_streamer.py \
  --source ndjson \
  --symbols AMD,MU \
  --input-file runtime/replay/AMD-diagnostic-recorded-events.ndjson.gz \
  --input-file runtime/replay/MU-diagnostic-recorded-events.ndjson.gz
```

Use this only to verify ingestion/model-load plumbing. For actual robustness scoring, use the event-carried whole-range fixture above.

#### 11.5.10 Progress snapshot — expanded raw-DBN event-carried transfer run, 2026-08-03

This snapshot records the concrete progress from the first expanded raw-DBN transfer run on the trading Mac. Treat it as reproducibility evidence and as the known-good artifact map for the next `AMD`/`MU` transfer tests.

Working root on the trading Mac:

```text
/Users/filmonghezehey/trading-agent/worktrees/databento
```

Raw Databento inputs:

```text
/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9
/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD
```

Commands/scripts used to produce the passing artifacts are listed below. Run them from the trading Mac worktree, not from the FXG laptop path.

First, make interactive `zsh` comments safe before pasting commands with `#` lines:

```zsh
setopt interactivecomments
```

Build the expanded normalized recorded-events NDJSON.GZ from the raw DBN directories:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

PYTHON_BIN=/Users/filmonghezehey/miniforge3/bin/python3
EQUS_DIR=/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9
OPRA_DIR=/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD
SYMBOLS_ALL=AMD,MU,TSLA,AMZN,SMCI,MSTR,COIN,NVDA,SPY,TQQQ,QQQ,GOOGL,PLTR,AAPL

RAW_BUILD=runtime/local-backtests/databento-expanded-raw-dbn-build-20260802_101205
SOURCE_EVENTS=runtime/replay/databento-expanded-raw-dbn-recorded-events.ndjson.gz
BUILD_LOG=$RAW_BUILD/build_source_events.log
STATUS_LOG=$RAW_BUILD/source_events_status.log

mkdir -p "$RAW_BUILD" runtime/replay

cat > "$RAW_BUILD/build_source_events.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

: "${PYTHON_BIN:?}"
: "${EQUS_DIR:?}"
: "${OPRA_DIR:?}"
: "${SYMBOLS_ALL:?}"
: "${SOURCE_EVENTS:?}"
: "${STATUS_LOG:?}"

mkdir -p "$(dirname "$SOURCE_EVENTS")"
rm -f "$SOURCE_EVENTS" "$STATUS_LOG"

"$PYTHON_BIN" scripts/databento_historical_streamer.py \
  --source dbn \
  --symbols "$SYMBOLS_ALL" \
  --equity-dir "$EQUS_DIR" \
  --options-dir "$OPRA_DIR" \
  | tee >(grep --line-buffered '"event":"status"' > "$STATUS_LOG" || true) \
  | gzip -c > "$SOURCE_EVENTS"
SH

chmod +x "$RAW_BUILD/build_source_events.sh"

nohup env \
  PYTHON_BIN="$PYTHON_BIN" \
  EQUS_DIR="$EQUS_DIR" \
  OPRA_DIR="$OPRA_DIR" \
  SYMBOLS_ALL="$SYMBOLS_ALL" \
  SOURCE_EVENTS="$SOURCE_EVENTS" \
  STATUS_LOG="$STATUS_LOG" \
  "$RAW_BUILD/build_source_events.sh" \
  > "$BUILD_LOG" 2>&1 &

printf 'PID=%s\n' "$!"
printf 'SOURCE_EVENTS=%s\n' "$SOURCE_EVENTS"
printf 'BUILD_LOG=%s\n' "$BUILD_LOG"
printf 'STATUS_LOG=%s\n' "$STATUS_LOG"
```

Validate the recorded-events file after the build exits:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

SOURCE_EVENTS=runtime/replay/databento-expanded-raw-dbn-recorded-events.ndjson.gz

gzip -t "$SOURCE_EVENTS" && echo "GZIP_OK"
ls -lh "$SOURCE_EVENTS"

python3 - "$SOURCE_EVENTS" <<'PY'
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
targets = {"AMD", "MU", "TSLA", "AMZN", "SMCI", "MSTR", "COIN", "NVDA", "SPY", "TQQQ", "QQQ", "GOOGL", "PLTR", "AAPL"}
counts = Counter()
events = Counter()
days = set()

with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
    for line in f:
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        msg = str(payload.get("message", ""))
        if "historical-day-begin day=" in msg:
            days.add(msg.rsplit("day=", 1)[-1].strip())
        symbol = str(payload.get("symbol") or payload.get("Symbol") or payload.get("underlying") or "").upper()
        event = str(payload.get("event") or payload.get("EventType") or "").lower()
        if symbol in targets:
            counts[symbol] += 1
            events[(symbol, event)] += 1

print("days", len(days), sorted(days)[:3], "...", sorted(days)[-3:] if days else [])
for symbol in sorted(targets):
    print(symbol, counts[symbol])
missing = [symbol for symbol in sorted(targets) if counts[symbol] == 0]
if missing:
    raise SystemExit(f"NO-GO missing symbols: {missing}")
print("PASS recorded-events contains all requested symbols")
PY
```

Build the matching expanded raw-DBN 1s/5s/30s CSVs. The old six-source enriched 30s CSV was rejected because it had zero rows for the expanded symbols, so the whole-range run must use this new `combined_30s.csv`:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

PYTHON_BIN=/Users/filmonghezehey/miniforge3/bin/python3
EQUS_DIR=/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9
OPRA_DIR=/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD
SYMBOLS_ALL=AMD,MU,TSLA,AMZN,SMCI,MSTR,COIN,NVDA,SPY,TQQQ,QQQ,GOOGL,PLTR,AAPL

BARS_BUILD=/Volumes/DatabentoVault/trading-agent-offload/databento/raw-dbn-bars-builds/databento-expanded-raw-dbn-bars-build-20260802_151311
BARS_1S_DIR=$BARS_BUILD/1s_by_symbol
BARS_5S_DIR=$BARS_BUILD/5s_by_symbol
BARS_30S_DIR=$BARS_BUILD/30s_by_symbol
COMBINED_1S=$BARS_BUILD/combined_1s.csv
COMBINED_5S=$BARS_BUILD/combined_5s.csv
INPUT_30S_CSV=$BARS_BUILD/combined_30s.csv
ASSESSMENT=$BARS_BUILD/symbol_model_plan.csv
CSV_LOG=$BARS_BUILD/build_1s_5s_30s_csv.log

mkdir -p "$BARS_BUILD" "$BARS_1S_DIR" "$BARS_5S_DIR" "$BARS_30S_DIR"

nohup "$PYTHON_BIN" -u build_30s_from_5s_csv.py \
  --dbeq-dir "$EQUS_DIR" \
  --opra-dir "$OPRA_DIR" \
  --symbols "$SYMBOLS_ALL" \
  --output-dir "$BARS_30S_DIR" \
  --output-dir-5s "$BARS_5S_DIR" \
  --output-dir-1s "$BARS_1S_DIR" \
  --combined-output-csv "$INPUT_30S_CSV" \
  --combined-output-csv-5s "$COMBINED_5S" \
  --combined-output-csv-1s "$COMBINED_1S" \
  --assessment-report "$ASSESSMENT" \
  > "$CSV_LOG" 2>&1 &

printf 'PID=%s\n' "$!"
printf 'BARS_BUILD=%s\n' "$BARS_BUILD"
printf 'INPUT_30S_CSV=%s\n' "$INPUT_30S_CSV"
printf 'CSV_LOG=%s\n' "$CSV_LOG"
```

Validate the expanded 30s CSV before running event-carried injection:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/raw-dbn-bars-builds/databento-expanded-raw-dbn-bars-build-20260802_151311/combined_30s.csv
SYMBOLS_REPLAY=AMD,MU,AMZN,SMCI,MSTR,COIN,GOOGL,PLTR,AAPL

python3 - "$INPUT_30S_CSV" "$SYMBOLS_REPLAY" <<'PY'
import csv
import sys
from collections import Counter
from pathlib import Path

path = Path(sys.argv[1])
targets = {s.strip().upper() for s in sys.argv[2].split(",") if s.strip()}
counts = Counter()
with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
    reader = csv.DictReader(f)
    symbol_col = next((c for c in (reader.fieldnames or []) if c.lower() == "symbol"), None)
    if not symbol_col:
        raise SystemExit(f"NO-GO no symbol column in {path}")
    for row in reader:
        symbol = str(row.get(symbol_col, "")).upper()
        if symbol in targets:
            counts[symbol] += 1
for symbol in sorted(targets):
    print(symbol, counts[symbol])
missing = [symbol for symbol in sorted(targets) if counts[symbol] == 0]
if missing:
    raise SystemExit(f"NO-GO missing symbols in INPUT_30S_CSV: {missing}")
print("PASS 30s CSV contains all requested symbols")
PY
```

Run the full nine-symbol whole-range event-carried no-trade validation. This command builds the slice, runs the normal replay for labels, builds the downstream setup sidecar, injects event-carried snapshots, and runs no-trade validation:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

PYTHON_BIN=/Users/filmonghezehey/miniforge3/bin/python3

START=2025-07-22
END=2026-05-22
START_TAG=$(printf %s "$START" | tr -d '-')
END_TAG=$(printf %s "$END" | tr -d '-')

SYMBOLS=AMD,MU,AMZN,SMCI,MSTR,COIN,GOOGL,PLTR,AAPL
SLICE_SYMBOLS=$SYMBOLS

SOURCE_EVENTS=runtime/replay/databento-expanded-raw-dbn-recorded-events.ndjson.gz
INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/raw-dbn-bars-builds/databento-expanded-raw-dbn-bars-build-20260802_151311/combined_30s.csv
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports

BASE=runtime/local-backtests/databento-expanded-transfer-${START_TAG}-${END_TAG}-event-carried
mkdir -p "$BASE"

nohup env \
  PYTHON_BIN="$PYTHON_BIN" \
  SOURCE_EVENTS="$SOURCE_EVENTS" \
  SYMBOLS="$SYMBOLS" \
  SLICE_SYMBOLS="$SLICE_SYMBOLS" \
  START="$START" \
  END="$END" \
  BASE="$BASE" \
  INPUT_30S_CSV="$INPUT_30S_CSV" \
  ROUTE_MANIFEST="$ROUTE_MANIFEST" \
  SETUP_MODEL_DIR="$SETUP_MODEL_DIR" \
  LIFECYCLE_MODEL_DIR="$LIFECYCLE_MODEL_DIR" \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=0 \
  REBUILD_EXISTING=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch_notrade.log" 2>&1 &

printf 'PID=%s\n' "$!"
printf 'BASE=%s\n' "$BASE"
printf 'LOG=%s\n' "$BASE/whole_range_batch_notrade.log"
tail -f "$BASE/whole_range_batch_notrade.log"
```

Use a single-pass scanner for the large no-trade log instead of repeatedly grepping a multi-GB file:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried
NO_TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade
LOG=$NO_TRADE_OUT/controlled_java_replay.log

python3 - "$LOG" <<'PY'
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

path = Path(sys.argv[1])
patterns = {
    "backtest_completed": re.compile(r"\[BACKTEST\] completed="),
    "backtest_rc": re.compile(r"BACKTEST_RC="),
    "setup_pass": re.compile(r"SETUP_FILTER_PASSES=PASS"),
    "setup_fail": re.compile(r"SETUP_FILTER_PASSES=FAIL"),
    "micro_pass": re.compile(r"MICRO_ENTRY_CONFIRMS=PASS"),
    "micro_fail": re.compile(r"MICRO_ENTRY_CONFIRMS=FAIL"),
    "snapshot_hit": re.compile(r"featureSnapshot=hit"),
    "snapshot_miss": re.compile(r"featureSnapshot=miss"),
    "sidecar_disabled": re.compile(r"featureSidecar=disabled"),
    "sidecar_hit": re.compile(r"featureSidecar=hit"),
    "exception_error": re.compile(r"Exception|ERROR|WATCHDOG|Order hung|OutOfMemory|No space", re.I),
}
symbol_re = re.compile(r"\bsymbol=([A-Z0-9._-]+)\b")
counts = Counter()
symbols = defaultdict(Counter)
completed_lines = []
with path.open("r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        for name, pat in patterns.items():
            if pat.search(line):
                counts[name] += 1
                match = symbol_re.search(line)
                if match:
                    symbols[match.group(1)][name] += 1
                if name == "backtest_completed":
                    completed_lines.append(line.strip())
print("COUNTS")
for key in sorted(patterns):
    print(key, counts[key])
print("COMPLETED_LINES")
for line in completed_lines[-20:]:
    print(line)
print("SYMBOL_TELEMETRY")
for symbol in sorted(symbols):
    print(symbol, dict(symbols[symbol]))
bad = counts["snapshot_miss"] > 0 or counts["sidecar_hit"] > 0 or counts["exception_error"] > 0
print("VERDICT", "NO-GO_CHECK_DETAILS" if bad else "PASS_BASIC_EVENT_CARRIED_TELEMETRY")
PY
```

Validate injection parity from the event snapshot summary JSON:

```zsh
ENRICHED_EVENTS=runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz

python3 - "$ENRICHED_EVENTS.summary.json" <<'PY'
import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text())
injection = summary.get("injection", {})
carrier = summary.get("carrier_assignment", {})
expected = injection.get("snapshot_rows_expected")
injected = injection.get("snapshot_rows_injected")
unmatched = injection.get("snapshot_rows_unmatched")
hit_rate = injection.get("snapshot_hit_rate_vs_sidecar")
multi = carrier.get("multi_snapshot_carrier_events")
print("snapshot_rows_expected", expected)
print("snapshot_rows_injected", injected)
print("snapshot_rows_unmatched", unmatched)
print("snapshot_hit_rate_vs_sidecar", hit_rate)
print("multi_snapshot_carrier_events", multi)
ok = expected == injected and unmatched == 0 and float(hit_rate or 0) == 1.0 and multi == 0
print("VERDICT", "PASS_INJECTION_PARITY" if ok else "NO-GO_INJECTION_PARITY")
PY
```

Compress the huge no-trade log after extracting the telemetry:

```zsh
LOG=runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade/controlled_java_replay.log

gzip -9 "$LOG"
gzip -t "$LOG.gz" && echo "GZIP_OK"
ls -lh "$LOG.gz"
```

Run the first trade-enabled `AMD`/`MU` baseline on the validated event-carried fixture with controlled micro thresholds `0.30/0.30`:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried
ENRICHED_EVENTS=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports

OUT=$BASE/parallel_file_replay_amd_mu_trade_micro_0p30_0p30_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

unset DOWNSTREAM_SETUP_FILTER_FEATURES_CSV
unset STRATEGY_DOWNSTREAM_SETUP_FILTER_FEATURES_CSV

PYTHON_BIN=/Users/filmonghezehey/miniforge3/bin/python3 \
python3 scripts/run_parallel_databento_file_backtest.py \
  --recorded-events "$ENRICHED_EVENTS" \
  --symbols AMD,MU \
  --jobs 2 \
  --output-dir "$OUT" \
  --trade-amount 60000 \
  --max-order-notional 70000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --model-dir "$SETUP_MODEL_DIR" \
  --setup-thresholds-file "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties" \
  --lifecycle-model-dir "$LIFECYCLE_MODEL_DIR" \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --timeout-seconds 0 \
  --title "AMD/MU expanded event-carried transfer micro 0.30/0.30" \
  -- \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30
```

At-a-glance result summary from the commands above:

| Step | Result |
|---|---|
| Expanded recorded-events raw DBN build | `GZIP_OK`; 213 sessions; all 14 requested symbols present. |
| Expanded raw DBN 30s CSV | Built under `/Volumes/DatabentoVault/trading-agent-offload/databento/raw-dbn-bars-builds/databento-expanded-raw-dbn-bars-build-20260802_151311/`. |
| Old broader 213d six-source 30s CSV | Rejected for this expanded run; zero rows for `AAPL,AMD,AMZN,COIN,GOOGL,MSTR,MU,PLTR,SMCI`. |
| Nine-symbol no-trade whole-range event-carried replay | `[BACKTEST] completed=9 failed=0 requested=9`; `BACKTEST_RC=0`. |
| No-trade event-carried telemetry | `snapshot_hit=71332`, `snapshot_miss=0`, `sidecar_disabled=71332`, `sidecar_hit=0`, `exception_error=0`. |
| Injection parity | `snapshot_rows_expected=71332`, `snapshot_rows_injected=71332`, `snapshot_rows_unmatched=0`, hit rate `1.0`. |
| No-trade stream sanity | `errors=[]` for `AAPL,AMD,AMZN,COIN,GOOGL,MSTR,MU,PLTR,SMCI`. |
| No-trade log compression | `controlled_java_replay.log` was about `13G`; `gzip -9` produced about `766M`. |
| `AMD`/`MU` trade baseline `0.30/0.30` | `PARALLEL_FILE_REPLAY completed=2 failed=0 requested=2`; `MU lifecycle_rows=1`, `AMD lifecycle_rows=16`. |

Expanded recorded-events source built from raw DBN:

```text
runtime/replay/databento-expanded-raw-dbn-recorded-events.ndjson.gz
```

The source file was gzip-valid and contained the requested expanded universe over 213 sessions. The verified symbols were:

```text
AAPL,AMD,AMZN,COIN,GOOGL,MSTR,MU,NVDA,PLTR,QQQ,SMCI,SPY,TQQQ,TSLA
```

The older enriched 30-second cache below was tested and rejected for this expanded replay because it contained zero rows for the requested expanded symbols:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv
```

The matching expanded raw-DBN bar build used for the passing run was:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/raw-dbn-bars-builds/databento-expanded-raw-dbn-bars-build-20260802_151311/combined_30s.csv
```

Nine-symbol transfer validation used this symbol set:

```text
AMD,MU,AMZN,SMCI,MSTR,COIN,GOOGL,PLTR,AAPL
```

Whole-range event-carried base directory:

```text
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried
```

Important outputs:

```text
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/databento-20250722-20260522-core5-whole-daily-prevclose.ndjson.gz
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/setup_micro_counterfactual_20260803_003428/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade/controlled_java_replay.log.gz
```

The whole-range script completed the no-trade event-carried validation:

```text
[WHOLE_RANGE] running normal replay for counterfactual labels ...
BACKTEST_RC=0
[WHOLE_RANGE] injecting event-carried snapshots ...
[WHOLE_RANGE] running no-trade event-carried validation
BACKTEST_RC=0
[WHOLE_RANGE] skipping event snapshot replay drift compare because RUN_DRIFT=0
[WHOLE_RANGE] DONE
```

The no-trade event-carried Java replay completed all nine requested symbols:

```text
[BACKTEST] completed=9 failed=0 requested=9
BACKTEST_RC=0
```

Event-carried telemetry from the compressed no-trade replay log:

```text
backtest_completed 1
backtest_rc 1
exception_error 0
setup_pass 343
setup_fail 70989
micro_pass 240
micro_fail 1199
snapshot_hit 71332
snapshot_miss 0
sidecar_disabled 71332
sidecar_hit 0
VERDICT PASS_BASIC_EVENT_CARRIED_TELEMETRY
```

Injection parity also passed:

```text
snapshot_rows_expected 71332
snapshot_rows_injected 71332
snapshot_rows_unmatched 0
snapshot_hit_rate_vs_sidecar 1.0
multi_snapshot_carrier_events 0
VERDICT PASS_INJECTION_PARITY
```

Per-symbol no-trade event-carried telemetry:

```text
AAPL snapshot_hit=8531  sidecar_disabled=8531  setup_pass=22   setup_fail=8509  micro_pass=8   micro_fail=166
AMD  snapshot_hit=9765  sidecar_disabled=9765  setup_pass=31   setup_fail=9734  micro_pass=25  micro_fail=89
AMZN snapshot_hit=11292 sidecar_disabled=11292 setup_pass=41   setup_fail=11251 micro_pass=22  micro_fail=243
COIN snapshot_hit=2234  sidecar_disabled=2234  setup_pass=0    setup_fail=2234  micro_pass=0   micro_fail=0
GOOGL snapshot_hit=9377 sidecar_disabled=9377  setup_pass=21   setup_fail=9356  micro_pass=9   micro_fail=149
MSTR snapshot_hit=3372  sidecar_disabled=3372  setup_pass=0    setup_fail=3372  micro_pass=0   micro_fail=0
MU   snapshot_hit=8225  sidecar_disabled=8225  setup_pass=3    setup_fail=8222  micro_pass=1   micro_fail=21
PLTR snapshot_hit=10584 sidecar_disabled=10584 setup_pass=38   setup_fail=10546 micro_pass=35  micro_fail=52
SMCI snapshot_hit=7952  sidecar_disabled=7952  setup_pass=187  setup_fail=7765  micro_pass=140 micro_fail=479
```

Stream sanity files reported empty error lists for every requested no-trade replay symbol:

```text
AAPL errors=[]
AMD errors=[]
AMZN errors=[]
COIN errors=[]
GOOGL errors=[]
MSTR errors=[]
MU errors=[]
PLTR errors=[]
SMCI errors=[]
```

Operational notes from this run:

- The no-trade replay log reached about `13G` before compression and compressed to about `766M` with `gzip -9`.
- Avoid repeated full-file `grep` scans on large logs. Prefer one-pass Python counters or bounded scans such as `tail -c 30000000 "$LOG" | grep ...`.
- Interactive `zsh` printed `zsh: command not found: #` for an inline comment. Enable `setopt interactivecomments` or avoid pasting comment lines into the terminal.
- A full nine-symbol, 213-session no-trade replay is expensive but validated the fixture. Trade-enabled robustness work should start with `AMD,MU` only.

The first `AMD`/`MU` trade-enabled transfer baseline then used the validated event-carried fixture with controlled micro thresholds `0.30/0.30`:

```text
runtime/local-backtests/databento-expanded-transfer-20250722-20260522-event-carried/parallel_file_replay_amd_mu_trade_micro_0p30_0p30_20260803_153357
```

Runner-level result:

```text
PARALLEL_FILE_REPLAY completed=2 failed=0 requested=2
MU  rc=0 status=completed lifecycle_rows=1
AMD rc=0 status=completed lifecycle_rows=16
```

The generated PnL summary showed that the controlled `0.30/0.30` no-retraining transfer baseline was not tradable quality:

```text
ALL trades=17 pnl=-1966.44 sum_realized_r=-13.275371 win_rate=29.4%
AMD trades=16 pnl=-1731.44 sum_realized_r=-11.549175 win_rate=31.25%
MU  trades=1  pnl=-235.00  sum_realized_r=-1.726196 win_rate=0.0%
AMD long  trades=12 pnl=-1552.44 sum_realized_r=-10.35232
AMD short trades=4  pnl=-179.00  sum_realized_r=-1.196855
MU long   trades=1  pnl=-235.00  sum_realized_r=-1.726196
```

Event-carried trade-mode telemetry still confirmed correct fixture wiring:

```text
snapshot_hit 17348
snapshot_miss 0
sidecar_disabled 17348
sidecar_hit 0
stream_sanity AMD errors=[]
stream_sanity MU errors=[]
```

Conclusion for this run: the replay infrastructure, raw-DBN fixture, sidecar injection, and event-carried Java path are validated, but the existing Core-5 setup/lifecycle/micro models did **not** transfer with enough quality to justify promotion or more transfer-only investigation. The low trade count, especially `MU` with only one closed trade, plus negative realized R/PnL means the next useful step for `AMD`, `MU`, or future onboarded symbols is retraining and symbol-inclusive calibration, not additional analysis of this no-retraining baseline.

Decision:

- Do not add `AMD`/`MU` calibrated threshold rows from this run.
- Do not enable `AMD`/`MU` paper/live entries from this transfer result.
- Treat the result as proof that the expanded file-replay/event-carried pipeline works.
- Prioritize retraining expanded setup models with the new symbol universe.
- Generate real OOF setup predictions that include the new symbols.
- Retrain lifecycle/micro models on the expanded symbol set.
- Calibrate per-symbol thresholds after retraining, then rerun event-carried no-trade and trade-enabled replay.
- For future symbol onboarding, expect retraining/calibration to be required; no-retraining transfer should be used only as a quick robustness diagnostic and infrastructure smoke test.

### 11.6 Backtest artifacts to inspect

Look for:

```text
*-orders.csv
*-trade-lifecycle-summary.csv
*-sanity.json
```

Key metrics:

- stream sanity `errors=[]`
- equity bar count
- option bar count
- low-quality bar rate
- missing previous-close count
- setup arms
- micro confirmations
- arm conversion rate
- closed trades
- realized R
- total PnL
- max drawdown
- day dominance
- symbol dominance
- exit reason distribution
- false-positive arms
- feature-vector rejects
- model inference skips

Use/adjust existing analysis utilities:

- `docs/databento_core5_weekly_analysis_runbook.md`
- `scripts/analyze_core5_weekly_run.py`
- `scripts/analyze_databento_core5_weekly_outputs.py`
- `scripts/calculate_core5_today_pnl.py`
- `scripts/summarize_core5_today_only.py`

---

## 12. Promotion gates

Before any paper/live entries, run promotion checks documented in:

- `docs/lifecycle_micro_promotion_gates.md`
- `docs/event_carried_whole_range_runbook.md`
- `scripts/validate_lifecycle_micro_promotion.py`
- `scripts/validate_paper_shadow_drift.py`
- `scripts/check_lifecycle_posthoc_gates.py`

Minimum required evidence:

1. Databento source audit passes.
2. Bar build verification passes.
3. Quality sanity reports pass.
4. Setup model feature schema is stable.
5. Lifecycle/micro feature schema is stable.
6. Calibration artifacts exist and are acceptable.
7. Frozen holdout has enough predictions/trades.
8. Day dominance is acceptable.
9. Historical backtest sanity has `errors=[]`.
10. Recorded live-shaped replay parity passes.
11. Paper/shadow drift is acceptable.
12. Runtime model paths are local/durable.
13. Launcher previews pass.
14. Live startup first runs with `--max-trades=0`.

---

## 13. Safe runtime launch sequence

### 13.1 Preview expanded pilot

```zsh
./start_all_databento_bots.sh \
  --symbols=TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU
```

### 13.2 Start with entries disabled

```zsh
./start_all_databento_bots.sh \
  --start \
  --symbols=TSLA,TQQQ,NVDA,SPY,QQQ,AMD,MU \
  --max-trades=0 \
  --tee-db
```

### 13.3 Reduced-size paper/shadow after gates

```zsh
./start_all_databento_bots.sh \
  --start \
  --symbols=AMD,MU \
  --max-trades=1 \
  --per-trade-notional=5000 \
  --max-share-cap=25 \
  --tee-db
```

Do not run full-size entries for newly onboarded symbols until the expanded model/calibration/backtest gates pass.

---

## 14. Future-symbol checklist

For every future symbol:

1. Add raw symbol to `config/databento_core_5_symbols.txt`.
2. Add raw symbol to `runtime/pilot_core_5_symbols.txt`.
3. Add symbol row to `config/databento_dynamic_upgrade_pilot_symbols.csv`.
4. Confirm OPRA parent, usually `<SYMBOL>.OPT`.
5. Build 1s/5s/30s Databento bars.
6. Verify data quality, row count, and day coverage.
7. Include symbol in staged training slice.
8. Train or retrain setup models.
9. Generate OOF setup predictions with symbol included.
10. Train lifecycle/micro models with symbol included.
11. Run calibration and threshold sweeps.
12. Update `runtime/databento/model-routing.csv`.
13. Regenerate `runtime/databento/bots/trading-<symbol>.properties`.
14. Verify unique port/client/request IDs.
15. Verify local durable model paths.
16. Verify state/log/trade paths are symbol-specific.
17. Run dry-run historical backtest.
18. Prepare or source recorded-events NDJSON/NDJSON.GZ that contains the candidate symbol.
19. For continuous whole-range event-carried replay, follow `docs/event_carried_whole_range_runbook.md`.
20. Validate the recorded-events stream with `scripts/databento_historical_streamer.py --source ndjson`.
21. Inject downstream setup-filter sidecar rows into event-carried snapshots when parity testing requires it.
22. Run no-sidecar/no-trade file replay and verify `featureSnapshot=hit`, zero `featureSnapshot=miss`, and `featureSidecar=disabled`.
23. Run trade-enabled file replay threshold grids.
24. Run holdout historical backtest.
25. Run weekly/monthly analysis.
26. Run promotion gates.
27. Start live with `--max-trades=0`.
28. Paper/shadow with tiny size.
29. Only then consider production entries.

---

## 15. Summary for `AMD` and `MU`

For `AMD`/`MU` specifically:

- Use existing models for first-pass backtest transfer checks.
- Do not treat existing models as sufficient for promotion.
- The 2026-08-03 no-retraining event-carried transfer test proved the expanded raw-DBN replay path, but the `0.30/0.30` trade baseline was negative (`17` trades, `-1966.44` PnL, `-13.275371` realized R) and `MU` produced only one closed trade.
- Treat that transfer result as infrastructure validation only, not as evidence that Core-5 models generalize to `AMD`/`MU`.
- Retrain expanded setup and lifecycle/micro models for `AMD`/`MU` and future symbols before promotion-quality evaluation.
- Calibrate `AMD`/`MU` thresholds before paper/live.
- Fix model-routing paths so generated properties do not depend on stale external worktree paths.
- Launch first with `--max-trades=0`.

Recommended path:

```text
register AMD/MU
→ build expanded data
→ validate data quality
→ backtest existing model transfer
→ retrain expanded pilot models
→ calibrate thresholds
→ prepare and validate recorded-events file replay
→ regenerate routing/properties
→ pass promotion gates
→ paper/shadow tiny size
→ limited live only after evidence
```
