# Databento Training Data Prep

This pipeline now supports two input modes:

1. legacy enriched CSV input
2. raw Databento DBN input from:
   - `EQUS.MINI / tbbo` for live-parity equity training
   - `OPRA.PILLAR / ohlcv-1s`

`DBEQ.BASIC / tbbo` can still be processed by the raw builder, but the current live bots subscribe to
`EQUS.MINI`, so retraining should use `EQUS.MINI` unless you intentionally want a richer historical-only source.

## Current synchronized plan

This README is the operational runbook for the data-prep/training side of the upgrade described in
`docs/training_and_execution_upgrade_plan.md`. The current production-candidate rebuild is the versioned
`20260523` path:

- equity source: `/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9`
- option source: `/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD`
- build outputs:
  - `training_data/databento_1s_20260523/`
  - `training_data/databento_5s_20260523/`
  - `training_data/databento_30s_20260523/`
  - `training_data/databento_1s_20260523_combined.csv`
  - `training_data/databento_5s_20260523_combined.csv`
  - `training_data/databento_30s_20260523_combined.csv`
- compare outputs: `training_data/compare_runs_20260523_meta_ab/`
- runtime routing candidate: `runtime/databento/model-routing-20260523.csv`

Do not overwrite the older `20260419` data while validating the `20260523` route. Do not train or promote from
bounded/smoke data unless the run is explicitly marked as a smoke run.

## What the raw Databento path does

For each paired market day:

- decodes equity TBBO trades with best-bid/best-offer context
- builds regular-session 1-second per-symbol bars
- fills sparse seconds with midpoint/last-price synthetic OHLC where needed
- aggregates OPRA option flow into underlying-level call/put deltas
- writes the combined 1-second equity + OPRA state
- rolls the same no-lookahead combined 1-second state into 5-second micro bars
- rolls the same no-lookahead combined 1-second state into 30-second setup/regime bars
- writes one CSV per symbol for each requested cadence
- optionally writes combined multi-symbol CSVs for each cadence
- emits a symbol assessment report with a recommended model-training mode

The same combined 1-second source frame should feed all cadences:

```text
combined_1s = equity tbbo 1s state + OPRA ohlcv-1s option-flow state
training_1s = combined_1s
training_5s = no-lookahead resample/rollup of combined_1s
training_30s = no-lookahead resample/rollup of combined_1s
```

For the first upgraded runtime rollout, 30-second bars remain the setup/regime layer, 5-second bars are the
primary micro-AI cadence, and 1-second bars remain available for spread/slippage/state/guardrails and future
research.

## Smoke run

```bash
python3 build_30s_from_5s_csv.py \
  --dbeq-dir "/Users/filmonghezehey/Downloads/EQUS-20260406-AMU94H9A3Q" \
  --opra-dir "/Users/filmonghezehey/Downloads/OPRA-20260406-JWFB5MVSVF" \
  --output-dir training_data/databento_30s_smoke \
  --assessment-report training_data/databento_30s_smoke/symbol_model_plan.csv \
  --symbols TSLA,NVDA \
  --max-days 1
```

## Backtesting from Databento Historical API streaming

`HistoricalBacktester` now defaults to normalized Databento historical streaming instead of local CSV input. It launches
`scripts/databento_historical_streamer.py --source api`, reads newline-delimited JSON, aggregates the streamed 1-second
Databento events into the configured replay cadence, and feeds the strategy directly.

Example single-day TSLA backtest:

```bash
DATABENTO_API_KEY="${DATABENTO_API_KEY}" ./mvnw -q org.codehaus.mojo:exec-maven-plugin:3.6.3:java \
  -Dexec.mainClass=com.calgary.fili.trader.testers.HistoricalBacktester \
  -Dexec.classpathScope=compile \
  -Dexec.args=TSLA \
  -Dbacktest.source=databento \
  -Dbacktest.databento.start=2026-05-21 \
  -Dbacktest.databento.end=2026-05-21 \
  -Dbacktest.databentoReplayCadence=5s
```

Useful overrides:

- `-Dbacktest.databento.equityDataset=EQUS.MINI`
- `-Dbacktest.databento.equitySchema=tbbo` (or `ohlcv-1s` when you only need bars)
- `-Dbacktest.databento.optionsDataset=OPRA.PILLAR`
- `-Dbacktest.databento.optionsSchema=ohlcv-1s`
- `-Dbacktest.databento.dryRun=true` to verify streamer startup without downloading data

Legacy CSV backtesting is still available only when explicitly requested:

```bash
./mvnw -q org.codehaus.mojo:exec-maven-plugin:3.6.3:java \
  -Dexec.mainClass=com.calgary.fili.trader.testers.HistoricalBacktester \
  -Dexec.classpathScope=compile \
  -Dexec.args=TSLA,TSLA_Historical_Full.csv \
  -Dbacktest.source=csv
```

## Full 100-symbol prep

### Current 20260523 full-history build

Use the versioned wrapper for the current rebuild:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
MAX_DAYS=0 CHUNK_COUNT=4 PYTHONUNBUFFERED=1 scripts/run_parallel_databento_build_20260523.sh
```

Required guardrails:

- `MAX_DAYS=0` means full history. Do not use `MAX_DAYS=10` or another cap for production training.
- The script should process all paired `20260523` source days, currently expected to be 213 EQUS/OPRA days.
- The final `30s` dataset should be audited before any training starts.
- If the build is interrupted or fails, remove/archive the partial `20260523` outputs and rebuild from scratch.

After the build completes, run:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
/Users/filmonghezehey/miniforge3/bin/python3 scripts/audit_databento_retrain_runs.py
```

For an unattended continuation after a bar build is already running, use the guarded supervisor:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
CHECK_INTERVAL_SECONDS=300 \
RUN_COMPARE_FINALIZE=1 \
RUN_LIFECYCLE_MICRO=1 \
RUN_MAVEN_VERIFY=1 \
RUN_BACKTEST=0 \
AUTO_APPLY_COMPARE=1 \
PYTHONUNBUFFERED=1 \
scripts/run_20260523_pipeline_after_bars.sh
```

The supervisor waits for `scripts/run_parallel_databento_build_20260523.sh` / `build_30s_from_5s_csv.py` to exit,
refuses to continue if chunk errors are detected or required bar outputs are missing, validates minimum symbol/day/row
coverage, then runs audit, compare/finalize, lifecycle/micro training, Maven verification, and the versioned runtime
apply step in order.

To keep the unattended run resilient, run the bounded recovery watchdog alongside the supervisor:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
CHECK_INTERVAL_SECONDS=300 \
MAX_RECOVERY_ATTEMPTS=3 \
MAX_BAR_BUILD_RESTARTS=1 \
MIN_FREE_GB_FOR_BAR_RESTART=150 \
RUN_COMPARE_FINALIZE=1 \
RUN_LIFECYCLE_MICRO=1 \
RUN_MAVEN_VERIFY=1 \
RUN_BACKTEST=0 \
AUTO_APPLY_COMPARE=1 \
PYTHONUNBUFFERED=1 \
scripts/recover_20260523_pipeline.sh
```

The recovery watchdog monitors the active after-bars supervisor. If it exits without `PIPELINE_COMPLETE`, the watchdog
captures diagnostics, restarts the full bar build at most once only when required bar outputs are missing and enough disk
is available, then reruns the resumable downstream supervisor up to the configured recovery-attempt limit.

Expected full-history 30-second readiness checks:

- 99 symbol CSVs under `training_data/databento_30s_20260523/`
- `training_data/databento_30s_20260523/symbol_model_plan.csv`
- `training_data/databento_30s_20260523_combined.csv`
- roughly 213 unique market days per symbol
- roughly 166,140 30-second rows per symbol, assuming 780 regular-session 30-second bars/day

Do not run 30-second training until this audit passes.

### Generic raw-builder example

```bash
python3 build_30s_from_5s_csv.py \
  --dbeq-dir "/Users/filmonghezehey/Downloads/EQUS-20260406-AMU94H9A3Q" \
  --opra-dir "/Users/filmonghezehey/Downloads/OPRA-20260406-JWFB5MVSVF" \
  --output-dir training_data/databento_30s \
  --combined-output-csv training_data/databento_30s_combined.csv \
  --assessment-report training_data/databento_30s/symbol_model_plan.csv \
  --symbols-file databento_ibkr_bridge/config/symbols_100.txt
```

## Outputs

- per-symbol 30-second setup bars: `training_data/databento_30s_20260523/<SYMBOL>_30s_training.csv`
- per-symbol 5-second micro bars: `training_data/databento_5s_20260523/<SYMBOL>_5s_training.csv`
- per-symbol 1-second state bars: `training_data/databento_1s_20260523/<SYMBOL>_1s_training.csv`
- combined 30-second dataset: `training_data/databento_30s_20260523_combined.csv`
- combined 5-second dataset: `training_data/databento_5s_20260523_combined.csv`
- combined 1-second dataset: `training_data/databento_1s_20260523_combined.csv`
- training-mode assessment: `training_data/databento_30s_20260523/symbol_model_plan.csv`

## Recommended training topology

Use the generated `symbol_model_plan.csv` as the first pass:

- `mega_liquid` → train a shared core model, then tune symbol-specific thresholds/calibration
- `liquid` → train shared models by liquidity cohort
- `specialized` → train per-symbol or very small niche clusters

This keeps the current single-symbol training scripts usable immediately while still producing a combined dataset for future shared-model training.

## Next step: prepare a manifest-driven training pipeline

Once the build has produced `symbol_model_plan.csv` and the per-symbol 30s files, `prepare_databento_training.py` becomes the single orchestrator for:

1. staging raw per-job datasets
2. optionally generating `TimesFM_*` columns
3. optionally generating `Seq*` columns
4. normalizing those columns back into the canonical lowercase training schema
5. optionally launching `train_30s_models.py`

Generate the manifest only:

```bash
python3 prepare_databento_training.py \
  --data-dir training_data/databento_30s \
  --plan-csv training_data/databento_30s/symbol_model_plan.csv \
  --work-dir training_data/databento_training_runs
```

This creates:

- `training_data/databento_training_runs/training_manifest.csv`
- `training_data/databento_training_runs/training_manifest.sh`
- staged raw datasets under `training_data/databento_training_runs/staged_datasets/raw/`
- reserved stage targets under `training_data/databento_training_runs/staged_datasets/timesfm/`
- reserved stage targets under `training_data/databento_training_runs/staged_datasets/sequence/`

The manifest now tracks, per job:

- source CSV
- raw staged CSV
- TimesFM staged CSV
- sequence staged CSV
- final training CSV
- row counts
- per-stage status (`timesfm_status`, `sequence_status`, `train_status`)
- the effective `USE_META_PRODUCER_FEATURES` mode that will be used for training

## 20260523 base/enhanced compare path

After the full-history `20260523` 30-second dataset passes audit, run the versioned compare/finalize wrapper:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
PYTHONUNBUFFERED=1 scripts/finalize_databento_compare_20260523.sh
```

This performs the production-candidate 30-second A/B comparison without updating canonical runtime model aliases:

1. baseline variant: no generated meta producer block, no TimesFM, no sequence proxy
2. enhanced variant: generated meta producer block plus TimesFM proxy and sequence proxy
3. manifest reconciliation for both variants
4. final comparison report generation
5. mixed routing generation at `runtime/databento/model-routing-20260523.csv`

The `20260523` compare skips the old generic 30-second exit model family by default to reduce runtime:

- skipped by default: `long_exit.onnx`, `short_exit.onnx`, `open30_long_exit.onnx`, `open30_short_exit.onnx`, and regime-specific legacy exit variants
- still trained: 30-second long/short setup entries, opening-entry variants where enough data exists, and the regime classifier
- reason: the final upgraded exit path should be trained separately as position-aware lifecycle exits, not as the old context-only generic exit models

To force a full legacy 30-second package including old exits for a compatibility benchmark, run with:

```bash
SKIP_LEGACY_30S_EXIT_MODELS=0 scripts/finalize_databento_compare_20260523.sh
```

The `20260523` compare launcher contains a coverage preflight and should reject likely capped/incomplete data by
default. Do not bypass that guard for production training.

Review these outputs before applying anything to runtime:

- `training_data/compare_runs_20260523_meta_ab/final_comparison_report.json`
- `training_data/compare_runs_20260523_meta_ab/primary_job_model_comparison.csv`
- `training_data/compare_runs_20260523_meta_ab/open_job_model_comparison.csv`
- `training_data/compare_runs_20260523_meta_ab/regime_job_model_comparison.csv`
- `runtime/databento/model-routing-20260523.csv`

Only after the comparison and routing look valid, apply the versioned candidate route/properties:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
scripts/apply_databento_compare_20260523.sh
```

The apply step should back up current runtime routing/properties, copy the versioned routing to the canonical
runtime routing path, regenerate bot properties, and write versioned threshold summaries. Do not manually copy
partial compare artifacts into runtime.

## Optional: run the full upgrade path from the manifest

Run raw -> TimesFM -> sequence -> training in one command:

```bash
python3 prepare_databento_training.py \
  --data-dir training_data/databento_30s \
  --plan-csv training_data/databento_30s/symbol_model_plan.csv \
  --work-dir training_data/databento_training_runs \
  --run-ready \
  --timesfm-backend proxy \
  --sequence-backend torch \
  --model-family lightgbm \
  --regime-model-family lightgbm
```

Notes:

- `--timesfm-backend off\|proxy\|timesfm` controls whether `generate_timesfm_features.py` is wired in.
- `--sequence-backend off\|proxy\|torch` controls whether `train_sequence_meta.py` is wired in.
- If a staged input already contains `TimesFM_*` or `Seq*` columns, the pipeline reuses them and normalizes them into the lowercase producer columns expected by `train_30s_models.py`.
- The generated `training_manifest.sh` now replays the whole per-job pipeline, not just the final trainer.
- Use `--job-name specialized_tsla` (or another manifest job name) to execute a single job in isolation.

## Multi-symbol trainer safety

`train_30s_models.py` now resets rolling features, ATR/RSI state, and path-dependent labels by `Symbol` and `Date`.
That means the staged shared/cohort datasets are safe inputs for training shared models without cross-symbol lookahead leakage.

## Follow-on lifecycle and micro model plan

The `20260523` 1s/5s/30s rebuild also supports the execution upgrade plan, but these models should be promoted only
after the 30-second setup comparison is complete and backtested:

1. Train/score the 30-second setup/regime route with realistic fill/cost label settings.
2. Build position-aware lifecycle rows and train:
   - `long_exit_lifecycle.onnx`
   - `short_exit_lifecycle.onnx`
3. Use 5-second bars as the first micro-AI cadence and train:
   - `long_micro_entry_5s.onnx`
   - `short_micro_entry_5s.onnx`
   - `long_micro_exit_guard_5s.onnx`
   - `short_micro_exit_guard_5s.onnx`
4. Keep 1-second bars for spread/slippage/state/guardrails; do not require separate 1-second AI models in the first rollout.
5. Validate upgraded behavior with Databento historical streaming backtests, not combined-CSV replay, before runtime promotion.

After the `20260523` bar build and 30-second setup/regime comparison are complete, the versioned runner for this
phase is:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
scripts/run_lifecycle_micro_training_20260523.sh
```

It consumes:

- `training_data/databento_30s_20260523_combined.csv`
- `training_data/databento_5s_20260523_combined.csv`

and writes by default:

- `model_exports/lifecycle_micro_20260523/long_exit_lifecycle.onnx`
- `model_exports/lifecycle_micro_20260523/short_exit_lifecycle.onnx`
- `model_exports/lifecycle_micro_20260523/long_micro_entry_5s.onnx`
- `model_exports/lifecycle_micro_20260523/short_micro_entry_5s.onnx`
- `model_exports/lifecycle_micro_20260523/long_micro_exit_guard_5s.onnx`
- `model_exports/lifecycle_micro_20260523/short_micro_exit_guard_5s.onnx`
- `model_exports/lifecycle_micro_20260523/lifecycle_micro_scorecard.csv`
- `model_exports/lifecycle_micro_20260523/lifecycle_micro_route_manifest.json`

For a smoke run, cap simulated entries:

```bash
MAX_ENTRY_EVENTS=500 scripts/run_lifecycle_micro_training_20260523.sh
```

Required rollout principles from `docs/training_and_execution_upgrade_plan.md`:

- missing lifecycle exit models should force CHOPPY/default routing with hard risk exits instead of falling back to old generic models;
- missing regime-specific entry models should fall back to the CHOPPY/default route before the base/default model;
- missing non-lifecycle upgraded micro models should invalidate an upgraded route instead of silently trading without them;
- thresholds should be calibrated per route/model/side where possible and stored with routing/properties;
- direct 30-second entry, lifecycle exit, and armed 30-second setup + 5-second micro confirmation should be compared separately;
- paper trade first, then promote with reduced size only if trade lifecycle metrics pass.

Java runtime exit routing now follows the same rule in `PingPongStrategy.java`:

- legacy generic 30-second exits are disabled by default with `-Dstrategy.exit.legacy30sEnabled=false`;
- when `-Dstrategy.exit.lifecycleEnabled=true`, the strategy uses only `long_exit_lifecycle.onnx` and `short_exit_lifecycle.onnx` for AI exits;
- lifecycle-enabled routes do not fall back to `long_exit.onnx`, `short_exit.onnx`, open30 exit models, or regime-specific legacy exit models;
- if lifecycle exit mode is enabled and either lifecycle model is missing, the strategy forces CHOPPY/default routing and relies on hard risk controls for exits;
- old 30-second exit models can only be used for an explicit legacy benchmark/rollback route by setting `-Dstrategy.exit.legacy30sEnabled=true` while lifecycle exit mode is off.

