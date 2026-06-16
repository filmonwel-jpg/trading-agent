# Computer Capability Task Organization

Date: 2026-06-13

## New constraint

This computer can read `/Volumes/DatabentoVault`, but it cannot write to that external disk. Treat this machine as a **read-only external-data inspector / code-and-plan controller**, not as the large artifact builder.

Verified local/external state on this computer:

- Workspace: `/Users/FXG06FA/trading-agent-main`
- Local free space: about `106GiB`
- External vault mount: `/Volumes/DatabentoVault`
- External mount flags: `apfs, local, nodev, nosuid, read-only, journaled, noowners`
- External free space shown by `df`: about `2.9TiB`, but not writable from this computer

## Downloaded data visible on the read-only external vault

Raw Databento sources visible at the vault root:

| Folder | Dataset/schema | Symbols | DBN dates | Size | Notes |
|---|---|---:|---:|---:|---|
| `/Volumes/DatabentoVault/EQUS-20260523-6J9KE98BJ9` | `EQUS.MINI` / `tbbo` | existing full symbol set | `213` | `4.29GiB` | Existing aligned equity trade + top-of-book source. |
| `/Volumes/DatabentoVault/OPRA-20260523-MSV68VKVKD` | `OPRA.PILLAR` / `ohlcv-1s` | existing full option parent set | `213` | `8.40GiB` | Existing compact option-volume source. |
| `/Volumes/DatabentoVault/EQUS-20260612-36BEU4G7M8` | `EQUS.MINI` / `mbp-1` | `TSLA,TQQQ,NVDA,SPY,QQQ` | `214` | `58.87GiB` | New pilot equity quote-state feed. Contains `20260403`, which is not present in paired OPRA folders; paired-build logic must skip or explicitly handle that date. |
| `/Volumes/DatabentoVault/OPRA-20260612-KN5TPHB5EF` | `OPRA.PILLAR` / `tcbbo` | `TSLA.OPT,TQQQ.OPT,NVDA.OPT,SPY.OPT,QQQ.OPT` | `213` | `15.08GiB` | New pilot option trade + quote-state feed. |
| `/Volumes/DatabentoVault/OPRA-20260612-B5D4JV3GV6` | `OPRA.PILLAR` / `definition` | existing full option parent set | `213` | `2.15GiB` | New OPRA definition source. |
| `/Volumes/DatabentoVault/OPRA-20260612-B5D4JV3GV6 2` | `OPRA.PILLAR` / `definition` | existing full option parent set | `213` | `2.15GiB` | Appears to be a duplicate of the definition folder; do not process both unless hashes differ. |

External offload data is also visible under `/Volumes/DatabentoVault/trading-agent-offload/databento`:

- `training_data`: about `1.0T`
- `trading-agent-all-symbol-micro-threshold-grid-low-parallel-20260609_114711`: about `250G`
- `runtime`: about `89G`
- Several TSLA/TQQQ micro-threshold grid runs and prior model exports are present.

## Role split

### This computer: read-only external inspector / control plane

Use this computer for tasks that read the external vault but write only small local outputs inside the repository, `/tmp`, or another local writable path.

Good tasks for this computer:

1. Code edits, docs, branch hygiene, and small unit tests.
2. Read-only source inventory: folder sizes, file counts, date coverage, manifest inspection, duplicate detection.
3. Small schema inspections that read one DBN day and write tiny local summaries only.
4. Building helper scripts for manifests, leakage checks, quality aggregation, walk-forward prediction generation, and calibration reports.
5. Running lightweight tests on synthetic or tiny sampled inputs.
6. Reviewing existing offloaded result CSVs and logs in read-only mode.
7. Preparing exact commands for the 48GB machine.

Avoid on this computer:

1. Any command that writes to `/Volumes/DatabentoVault`.
2. Offload scripts that touch, move, delete, or archive external files.
3. Full 1s/5s/30s rebuilds from the new `mbp-1`/`tcbbo` pilot sources.
4. Full walk-forward retraining or threshold grids that create large intermediate outputs.
5. Copying the `1.0T` external `training_data` tree to local storage.

Safe local output policy for this computer:

- Keep inspection outputs small, ideally under `runtime/read_only_inspection_YYYYMMDD/` or `/tmp/trading-agent-*`.
- If a decode or sample run would exceed a few GiB, stop and move that task to the 48GB machine.
- Never assume an external path is writable just because `df` reports free space.

### 48GB computer: write/build/train plane

Use the 48GB computer for all artifact-producing work, especially anything that expands DBN into CSV/parquet/silver/gold datasets or trains models.

Good tasks for the 48GB machine:

1. Mount/copy the raw folders with write-capable output storage available.
2. Create source manifests and paired-date manifests for the new pilot schemas.
3. Decode one day per schema and produce schema/row-count/storage estimates.
4. Build the first 10-day pilot slice for `TSLA`, `TQQQ`, `NVDA`, `SPY`, and `QQQ`.
5. Rebuild fixed 1s/5s/30s datasets after leakage and quality fixes are merged.
6. Generate walk-forward 30s setup predictions and fail if any lifecycle/micro row lacks setup probability.
7. Train lifecycle/micro models only after fixed features, cost-aware labels, manifests, and walk-forward setup predictions are present.
8. Run Maven verification, historical streaming backtests, calibration reports, and promotion-gate reports.
9. Store large outputs on a write-capable local/external path owned by that computer.

## Updated execution order

### Phase A — Do now on this read-only computer

1. Keep the external vault read-only; do not run external write checks or offload scripts.
2. Use the downloaded data inventory above as the current source truth.
3. Fix code blockers before retraining:
   - Remove `bfill()` leakage in `build_30s_from_5s_csv.py`.
   - Replace the parent `DataQualityFlags` union behavior with aggregate quality/coverage fields.
4. Implement and test the walk-forward setup prediction generator using tiny local/synthetic data.
5. Add manifest/join helper code and tests that can later run on the 48GB machine.
6. Prepare the 48GB run commands and expected output paths.

### Phase B — First run on the 48GB machine

1. Verify write-capable output storage and enough free space before starting.
2. Create a source manifest for:
   - existing `EQUS tbbo`
   - existing `OPRA ohlcv-1s`
   - new `EQUS mbp-1`
   - new `OPRA tcbbo`
   - new `OPRA definition`
   Use `scripts/audit_databento_pilot_sources.py` for the first repeatable manifest and paired-date check.
3. Confirm paired dates across sources. Special handling required:
   - `EQUS-20260612-36BEU4G7M8` includes `20260403`.
   - The paired OPRA folders do not include `20260403`.
   - The pilot build should skip unpaired dates or mark them source-unavailable in the manifest; it must not silently inner/outer join without an explicit count.
4. Decode one representative day, preferably `2026-05-21`, for each new schema.
5. Produce row counts, symbol coverage, option contract coverage, and compressed/uncompressed size estimates.
6. Use `scripts/plan_databento_pilot_dates.py` to select a fully paired 10-day pilot manifest from `paired_dates.csv` and `source_files.csv`.
7. Build a 10-day pilot slice before the full-window pilot.
8. Only after the 10-day pilot passes manifest, leakage, row-count, and join checks, expand to the full aligned window.

Use `scripts/audit_databento_dbn_day.py` for step 4. It loads one daily DBN file at a time and writes compact summaries only; it is not a full-window normalizer.

## Action plan / action done ledger

Use this ledger format for every remaining step: write the **Action plan** first, then record **Action done** immediately below it with artifact paths, counts, and stop/go decisions. Do not replace this ledger with prose-only status updates.

### Step 1 — Source inventory and immutable hashes

Action plan:

- Build the source inventory from the five required folders only: existing `EQUS tbbo`, existing `OPRA ohlcv-1s`, new `EQUS mbp-1`, new `OPRA tcbbo`, and primary new `OPRA definition`.
- Exclude the duplicate `OPRA-20260612-B5D4JV3GV6 2` definition folder unless a later hash check proves it is needed.
- Require `hash_error_count == 0` before using the source manifest for pilot planning.

Action done:

- Completed on the 48GB/write-capable computer under `source_inventory_hashes_20260613_133951`.
- `hash_error_count: 0`.
- `hash_ok_count: 1081`.
- `hash_skipped_count: 0`.
- Expected unpaired date remains `20260403`, present only in `equs_mbp1_20260612`.
- `duplicate_candidates: []` after excluding the duplicate definition folder.

### Step 2 — Representative DBN day decode audits

Action plan:

- Decode one recent day and one older day, one DBN file at a time, before any 10-day or full-window build.
- Use the compact audit outputs only: row counts, schema columns, timestamp bounds, symbol/instrument coverage, compressed size, decoded dataframe size, and decode time.
- Stop if any source has `status != ok`.

Action done:

- Recent day completed: `raw_audits/dbn_day_audit_20260521_20260613_142154`.
- Older day completed: `raw_audits/dbn_day_audit_20250721_20260613_150022`.
- Combined summary completed: `raw_audits/dbn_audit_summary_recent_old_20260613_150239`.
- Combined summary results: `error_count=0`, `warning_count=0`, `row_count=10`, `max_row_count=18080585`, `max_dataframe_mib=1836.107`, `max_memory_expansion_ratio=8.488`, `total_dataframe_gib=4.304`, `total_file_gib=0.769`.
- Decision: safe to plan a 10-day pilot manifest, but not safe to start model training or full-window normalization until the Phase 0 builder blockers are fixed.

### Step 3 — Fully paired 10-day pilot manifest

Action plan:

- Select the latest 10 fully paired trading dates from the hashed source manifest.
- The selected set must exclude `20260403` automatically because that date is not paired across sources.
- Require exactly five source files per selected date: `EQUS tbbo`, `OPRA ohlcv-1s`, `EQUS mbp-1`, `OPRA tcbbo`, and primary `OPRA definition`.
- Record compressed-size and decoded-memory estimates before any build.

Action done:

- Completed on the 48GB/write-capable computer under `source_manifests/pilot_dates_latest10_20260613_153639`.
- Selected dates: `2026-05-11`, `2026-05-12`, `2026-05-13`, `2026-05-14`, `2026-05-15`, `2026-05-18`, `2026-05-19`, `2026-05-20`, `2026-05-21`, `2026-05-22`.
- Selected files: `50`.
- Total compressed size estimate: `4.508 GiB`.
- Decision: use this manifest for the first 10-day build after Phase 0 code blockers are fixed; do not expand to full-window work yet.

### Step 4 — Phase 0 builder blocker C1: remove future backfill leakage

Action plan:

- Remove confirmed `ffill().bfill()` leakage from `build_30s_from_5s_csv.py` regularization.
- Price/book state must only forward-fill from already observed values.
- Explicit previous-close fallback is allowed for price anchoring, but it must not create quote/book state.
- Add a regression test proving pre-first-quote seconds do not inherit future bid/ask/close values.

Action done:

- Implemented in `build_30s_from_5s_csv.py` by replacing future backfill with forward-fill-only regularization.
- Added `tests/test_build_30s_from_5s_csv_regularization.py`.
- Verified with `python3 -m py_compile build_30s_from_5s_csv.py tests/test_build_30s_from_5s_csv_regularization.py`.
- Verified with `python3 tests/test_build_30s_from_5s_csv_regularization.py`.
- Verified no `ffill().bfill()` pattern remains in `build_30s_from_5s_csv.py`.

### Step 5 — Phase 0 builder blocker C2: replace parent quality union

Action plan:

- Replace parent `DataQualityFlags` child-union behavior with aggregate coverage/staleness/synthetic quality fields.
- Preserve old child union only as `ChildDataQualityFlagUnion` for audit.
- Add parent thresholds for `no_trade`, `no_quote`, `synthetic_ohlc`, `partial_synthetic_ohlc`, and `stale_quote`.
- Add tests where one child `no_quote` second does not make a valid 5s/30s parent `no_quote`.

Action done:

- Implemented in `build_30s_from_5s_csv.py`.
- Parent `DataQualityFlags` are now derived from aggregate coverage/staleness/synthetic thresholds instead of blindly unioning child flags.
- Child flag union is preserved separately as `ChildDataQualityFlagUnion` for audit/debugging.
- Added aggregate quality fields: `TradeSecondsPresent`, `QuoteUpdateSecondsPresent`, `QuoteStateSecondsValid`, `SyntheticSeconds`, `TradeCoverage`, `QuoteUpdateCoverage`, `QuoteStateCoverage`, `SyntheticCoverage`, `QuoteAgeMsMean`, `QuoteAgeMsMax`, `ValidSpreadCoverage`, `LockedCrossedSeconds`, and `QualityScore`.
- Added tests in `tests/test_build_30s_from_5s_csv_regularization.py` proving a parent with high quote-state coverage is not marked `no_quote` just because one child second has `no_quote`, while the child union is still preserved.
- Verified with `python3 -m py_compile build_30s_from_5s_csv.py tests/test_build_30s_from_5s_csv_regularization.py`.
- Verified with `python3 tests/test_build_30s_from_5s_csv_regularization.py`.
- Verified on the 48GB/write-capable computer after pulling commit `4163c88`; `python3 tests/test_build_30s_from_5s_csv_regularization.py` ran `5` tests successfully and `grep -n "ffill()\\.bfill()" build_30s_from_5s_csv.py` returned no matches.

### Step 6 — 10-day pilot build

Action plan:

- Use `source_manifests/pilot_dates_latest10_20260613_153639` as the date/file contract.
- Before starting any build, verify the external disk mount, write access, selected source-file existence, selected source-file sizes, selected dates, and source labels.
- Use `scripts/verify_databento_pilot_prebuild.py` for repeatable manifest/source-file verification rather than ad-hoc heredocs.
- Process partitioned by date and source; do not materialize all 10 days across all sources at once.
- Write outputs only under external `data_lake_v2`; no large local-disk outputs.
- Start only after pulling the commit that contains both C1 and C2 fixes and rerunning the targeted tests on the 48GB machine.

Action done:

- Pre-build source/code validation is ready to run on the 48GB machine because C1 and C2 are now pulled and tested there.
- External disk write check passed on the 48GB computer after reconnecting the disk: `/Volumes/DatabentoVault` was mounted read-write and a write/read/remove test under `data_lake_v2` succeeded.
- Manifest pre-build checks passed: `hash_error_count=0`, `audit_error_count=0`, `audit_warning_count=0`, selected dates are `20260511`, `20260512`, `20260513`, `20260514`, `20260515`, `20260518`, `20260519`, `20260520`, `20260521`, `20260522`, selected file count is `50`, total compressed input is `4.508 GiB`, and `20260403` is not selected.
- Source-file existence/size/hash checks passed and were written to `raw_audits/prebuild_manifest_check_20260613_172522/prebuild_manifest_check.json` and `raw_audits/prebuild_manifest_check_20260613_172522/prebuild_manifest_check_files.csv`; `errors: []`, `warnings: []`.
- Fixed-quality baseline 10-day build completed on the 48GB/write-capable computer under `model_training_sets/pilot_10d_fixed_quality_20260613_173446`.
- Command used the existing supported sources first: `/Volumes/DatabentoVault/EQUS-20260523-6J9KE98BJ9` (`tbbo`) and `/Volumes/DatabentoVault/OPRA-20260523-MSV68VKVKD` (`ohlcv-1s`) with symbols `TSLA,TQQQ,NVDA,SPY,QQQ` and `--max-days 10`.
- The build processed the expected 10 dates from `20260511` through `20260522` and wrote `reports/symbol_model_plan.csv`.
- This is a fixed-quality baseline build only. It does not yet include the new `EQUS mbp-1`, `OPRA tcbbo`, or OPRA definition feature expansion.
- Decision: run post-build artifact validation next before labels, training, or any full-window build.

### Step 7 — Post-build artifact validation

Action plan:

- Verify the 10-day build root exists and contains `data_1s`, `data_5s`, `data_30s`, `combined`, and `reports` outputs.
- For each expected symbol, require per-symbol `1s`, `5s`, and `30s` CSVs with the expected 10 pilot dates.
- Require combined `combined_1s.csv`, `combined_5s.csv`, and `combined_30s.csv` with all five symbols.
- Require quality-fix output columns, including `DataQualityFlags`, `ChildDataQualityFlagUnion`, coverage/staleness/synthetic quality fields, and `QualityScore`.
- Require exact regular-session row counts for the baseline 10-day build: `234000` rows per symbol at `1s`, `46800` rows per symbol at `5s`, and `7800` rows per symbol at `30s`; combined files should have five times those counts.
- Record output size, row counts, dates, symbols, and parent/child quality-flag summaries under `raw_audits/`.

Action done:

- Added repeatable verifier `scripts/verify_databento_pilot_build.py` with tests in `tests/test_verify_databento_pilot_build.py`.
- Completed on the 48GB/write-capable computer under `raw_audits/pilot_build_check_20260613_220839`.
- `scripts/verify_databento_pilot_build.py` returned `PILOT_BUILD_CHECK=PASS` for `model_training_sets/pilot_10d_fixed_quality_20260613_173446`.
- Verification summary: `errors=[]`, `warnings=[]`, `assessment_report_rows=5`, `file_summary_count=18`, `expected_days=10`, `expected_symbols=[TSLA,TQQQ,NVDA,SPY,QQQ]`, and `total_output_gib=1.354`.
- Verification artifacts:
  - `raw_audits/pilot_build_check_20260613_220839/pilot_build_check.json`
  - `raw_audits/pilot_build_check_20260613_220839/pilot_build_file_summary.csv`
- Decision: the fixed-quality 10-day baseline artifact is structurally valid. Do not start model training yet; inspect quality distributions and then proceed to C3/C4 setup-prediction blockers.

### Step 8 — Fixed-quality baseline quality sanity review

Action plan:

- Inspect `pilot_build_file_summary.csv` from `raw_audits/pilot_build_check_20260613_220839`.
- Confirm `DataQualityFlags` and `ChildDataQualityFlagUnion` are not accidentally identical everywhere for `5s`/`30s` outputs.
- Confirm quality metric columns are populated across combined and per-symbol files.
- Review per-symbol row counts, date coverage, and parent/child quality-flag values before using this build for labels or training.
- If the sanity review is clean, treat `model_training_sets/pilot_10d_fixed_quality_20260613_173446` as the first C1/C2-fixed baseline dataset for downstream label/prediction infrastructure testing only.

Action done:

- Completed manually on the 48GB/write-capable computer by reading `raw_audits/pilot_build_check_20260613_220839/pilot_build_file_summary.csv`.
- The summary contained `18` rows: five per-symbol files plus one combined file for each cadence (`1s`, `5s`, `30s`).
- Row counts matched the expected fixed-quality baseline build contract:
  - `1s`: `234000` rows per symbol and `1170000` rows combined.
  - `5s`: `46800` rows per symbol and `234000` rows combined.
  - `30s`: `7800` rows per symbol and `39000` rows combined.
- Date coverage was `10` for every file and symbol coverage was `1` for per-symbol files / `5` for combined files.
- C2 behavior is confirmed in the built artifacts:
  - `1s` files have `parent_child_flag_different_rows=0`, which is expected because raw-second parent flags and child-union diagnostics are identical at the leaf level.
  - Every `5s` and `30s` per-symbol/combined file has `parent_child_flag_different_rows == row_count`, proving parent `DataQualityFlags` are no longer blind child-flag unions.
  - Child unions still contain heavy `no_trade|no_quote|synthetic_ohlc` counts, while parent `5s`/`30s` quality flags are expressed as aggregate `partial_synthetic_ohlc`, `synthetic_ohlc`, `stale_quote`, `locked_crossed`, and occasional `none` states.
- Important interpretation: the fixed-quality baseline is structurally valid and demonstrates the intended parent/child quality split, but many bars still carry synthetic/stale quality flags. Treat it as a reliability baseline for downstream label/setup-prediction infrastructure testing, not as a model-promotion dataset.
- Added repeatable summarizer `scripts/summarize_databento_pilot_quality.py` with tests in `tests/test_summarize_databento_pilot_quality.py`.
- Durable quality-sanity artifact completed on the 48GB/write-capable computer under `raw_audits/pilot_quality_sanity_20260613_223642`.
- `scripts/summarize_databento_pilot_quality.py` returned `PILOT_QUALITY_SANITY=PASS`.
- Quality sanity summary: `errors=[]`, `warnings=[]`, `row_count=18`, `cadence_counts={1s:6,5s:6,30s:6}`, `min_parent_child_diff_frac_by_cadence={1s:0.0,5s:1.0,30s:1.0}`, `max_parent_child_diff_frac_by_cadence={1s:0.0,5s:1.0,30s:1.0}`, and `parent_child_decoupling_evidence_count=12`.
- Verification artifacts:
  - `raw_audits/pilot_quality_sanity_20260613_223642/pilot_quality_sanity.json`
  - `raw_audits/pilot_quality_sanity_20260613_223642/pilot_quality_sanity_rows.csv`
- Decision: the fixed-quality 10-day baseline has passed source, decode, prebuild, build, post-build, and quality-sanity gates. Next work should move to C3/C4 walk-forward setup prediction infrastructure, not model training.

### Step 9 — C3/C4 walk-forward setup prediction infrastructure

Action plan:

- Inspect `train_30s_models.py` and current lifecycle/micro training code paths to locate existing setup-score/proxy usage.
- Build a dedicated generator that emits one out-of-fold setup prediction row per trainable 30s bar with at least: `Symbol`, `Timestamp`, `fold_id`, raw score/probability, selected threshold, and threshold margin.
- Add a lifecycle/micro training gate that fails by default if setup probabilities are missing, constant, or bootstrap proxy values.
- Use the fixed-quality 10-day baseline only for infrastructure smoke tests; do not treat results from this short slice as model-performance evidence.

Action done:

- Implemented first-pass C3/C4 infrastructure in branch code.
- Added `generate_walk_forward_setup_predictions.py`, which reads a combined/per-symbol 30s dataset, computes the same 30s feature/label preparation path used by `train_30s_models.py`, trains rolling prior-day setup models, and emits one audit row per prepared 30s bar with:
  - `Symbol`, `Timestamp`, `Date`, `Label_Long_Entry`, `Label_Short_Entry`
  - `f_long_setup_prob`, `f_short_setup_prob`
  - `long_setup_fold_id`, `short_setup_fold_id`
  - `f_long_setup_threshold`, `f_short_setup_threshold`
  - `f_long_setup_threshold_margin`, `f_short_setup_threshold_margin`
  - side-specific train/test day counts and `is_oof_setup_prediction`.
- Added manifest output next to the prediction CSV with coverage, fold, feature-column, unique-probability, and error summaries.
- Updated `train_lifecycle_micro_models.py` with `--setup-predictions-csv`, default fail-fast behavior, and research-only `--allow-bootstrap-setup-proxy` override.
- Lifecycle/micro staging now drops unscored early rows explicitly and validates that retained rows have finite, non-constant, non-bootstrap setup probabilities before creating lifecycle/micro rows.
- 48GB/write-capable computer validation completed on 2026-06-14 against `model_training_sets/pilot_10d_fixed_quality_20260613_173446`:
  - `generate_walk_forward_setup_predictions.py` wrote `raw_audits/setup_predictions_10d_20260613.csv` and `raw_audits/setup_predictions_10d_20260613.manifest.json` with `errors=[]`, `warnings=[]`, `row_count=35685`, `trainable_oof_rows=28780`, and `trainable_oof_frac=0.806501331091495`.
  - Long and short setup folds both had `folds_ok=8`, `coverage_frac=0.806501331091495`, and high unique-probability counts (`28647` long, `28736` short in the generator manifest; joined lifecycle summary rounded to `28648`/`28738`).
  - `train_lifecycle_micro_models.py --setup-predictions-csv ... --max-entry-events 2000 --no-onnx` retained `28780` joined 30s rows, dropped `10220` intentionally unscored early rows, trained all six lifecycle/micro smoke models, and wrote `raw_audits/lifecycle_micro_setup_smoke_20260613/lifecycle_micro_scorecard.csv` plus `lifecycle_micro_route_manifest.json`.
  - Audit bundle copied to `raw_audits/c3_c4_10d_smoke_20260614_002956` with the setup manifest, lifecycle/micro route manifest, and scorecard.
  - Durable setup-prediction run copied under `model_training_sets/setup_oof_fixed_quality_20260614_003310`, writing `oof_setup_predictions.csv` and `oof_setup_predictions.manifest.json` with the same clean gates: `errors=[]`, `warnings=[]`, `row_count=35685`, `trainable_oof_rows=28780`, and `trainable_oof_frac=0.806501331091495`.
  - Durable lifecycle/micro smoke using that setup OOF artifact wrote `model_training_sets/lifecycle_micro_fixed_quality_setup_oof_20260614_004734/lifecycle_micro_scorecard.csv` and `lifecycle_micro_route_manifest.json`; it retained the same `28780` joined 30s rows, dropped the same `10220` intentionally unscored early rows, and trained all six smoke routes with ONNX export disabled.
- Added regression tests:
  - `tests/test_generate_walk_forward_setup_predictions.py`
  - expanded `tests/test_lifecycle_micro_models.py`.
- Local validation passed with `python3 -m py_compile generate_walk_forward_setup_predictions.py tests/test_generate_walk_forward_setup_predictions.py train_lifecycle_micro_models.py tests/test_lifecycle_micro_models.py` and the corresponding unit tests.
- Decision: C3/C4 infrastructure smoke is complete for the fixed-quality 10-day baseline. Treat these 10-day lifecycle/micro metrics as infrastructure evidence only, not paper/live promotion evidence.

### Step 10 — Live/backtester sanity parity requirements

Action plan:

- Before any C1/C2-fixed bundle is considered for paper/live, make the live route and replay/backtester route prove the same sanity properties that the offline pilot has now proven.
- Treat the following files as the first implementation surface:
  - Live Python producer: `scripts/databento_live_normalizer.py`.
  - Live Java event contract: `src/main/java/com/calgary/fili/trader/bot/trader/DatabentoEvent.java`.
  - Live Java consumer/health path: `src/main/java/com/calgary/fili/trader/bot/trader/IBKRTrader.java`, `DatabentoLiveGateway.java`, and `DatabentoFeedHealth.java`.
  - Historical/replay producer: `scripts/databento_historical_streamer.py`.
  - Streaming backtester: `src/main/java/com/calgary/fili/trader/testers/DatabentoHistoricalStreamingBacktester.java`.
  - CSV/replay helper: `src/main/java/com/calgary/fili/trader/testers/DatabentoHistoricalReplayProvider.java`.
- Extend the live/replay NDJSON event contract so every emitted bar can carry the same sanity fields used offline: `DataQualityFlags`, `ChildDataQualityFlagUnion`, `TradeSecondsPresent`, `QuoteUpdateSecondsPresent`, `QuoteStateSecondsValid`, `SyntheticSeconds`, `TradeCoverage`, `QuoteUpdateCoverage`, `QuoteStateCoverage`, `SyntheticCoverage`, `QuoteAgeMsMean`, `QuoteAgeMsMax`, `ValidSpreadCoverage`, `LockedCrossedSeconds`, `QualityScore`, timestamp provenance, and schema/version metadata.
- Make live and replay aggregation forward-fill only from already observed state; do not introduce any future `bfill()` or next-row quote/price fallback in Java, Python live normalizer, historical streamer, or replay provider.
- Add live startup preflight that records the feed/model/schema contract: dataset, schema, stype, symbol universe, option parents, model bundle ID, feature schema hash, quality-threshold constants, clock source, as-of lag tolerance, and output/log root.
- Add live session sanity counters by symbol and cadence: expected/seen bar count, missing seconds/bars, no-trade/no-quote/synthetic/stale/locked-crossed counts, parent-child flag diff fraction, quality-score distribution, and feature-vector rejected count.
- Add runtime safety behavior: low-quality or schema-mismatch state should block new entries and write diagnostics; it should not silently use malformed feature vectors. Emergency exits/flattening must remain available.
- Add backtester/replay artifacts equivalent to the offline checks: replay source manifest/hashes, replay date window, event-count summary, feature-vector schema summary, quality-sanity summary, and decision-parity report.
- Add recorded-event replay promotion gate: record a live-shaped NDJSON stream, replay it through `DatabentoHistoricalStreamingBacktester`, and compare bucket boundaries, quality fields, feature vectors, model scores, and decisions against live/backtest expectations.
- Acceptance criteria for this step: live and backtester both emit machine-readable sanity JSON/CSV artifacts with `errors=[]`; `1s` parent/child flags are expected to match, while `5s`/`30s` parent flags must be threshold-derived and not blind child-union copies; replay must prove no lookahead by timestamp/as-of checks.

Action done:

- 2026-06-14 first-pass live/backtester sanity parity implementation is complete in branch code. This is a runtime-contract and replay-sanity gate only; it does not promote any model bundle to paper/live.
- Added a shared Python live/replay NDJSON event contract in `scripts/databento_event_contract.py` and wired it into both `scripts/databento_live_normalizer.py` and `scripts/databento_historical_streamer.py`.
- Live and replay equity bars now carry schema/provenance and offline-style sanity fields including `EventSchemaVersion`, `DataQualityFlags`, `ChildDataQualityFlagUnion`, `TradeSecondsPresent`, `QuoteUpdateSecondsPresent`, `QuoteStateSecondsValid`, `SyntheticSeconds`, coverage fields, quote-age/spread/locked-crossed fields, `QualityScore`, `EventSource`, `Dataset`, `Schema`, `STypeIn`, `tsEventNs`, `BarEpochSec`, and `EventGeneratedAtMs`.
- Fixed historical TBBO aggregation so quote-only rows no longer count as trades: replay `volume`, `tradeCount`, `px_x_sz`, `atBidVol`, and `atAskVol` are now derived from positive trade price/size rows only, while quote state can still form synthetic low-quality OHLC for audit/replay.
- Extended Java live/backtester consumers in `src/main/java/com/calgary/fili/trader/bot/trader/DatabentoEvent.java`, `IBKRTrader.java`, `DatabentoFeedHealth.java`, and `src/main/java/com/calgary/fili/trader/testers/DatabentoHistoricalStreamingBacktester.java` so the live route and streaming backtester parse the same quality/provenance fields and expose per-symbol sanity counters through feed health.
- Added/updated regression coverage in `tests/test_databento_event_contract.py`, `src/test/java/com/calgary/fili/trader/bot/trader/DatabentoEventTest.java`, `src/test/java/com/calgary/fili/trader/bot/trader/DatabentoFeedHealthTest.java`, and `src/test/java/com/calgary/fili/trader/health/DatabentoHealthIndicatorTest.java`.
- Validation run on this computer passed:
  - `python3 -m py_compile scripts/databento_event_contract.py scripts/databento_historical_streamer.py scripts/databento_live_normalizer.py tests/test_databento_event_contract.py`
  - `python3 tests/test_databento_event_contract.py` (`4` tests)
  - `python3 scripts/databento_historical_streamer.py --source api --symbols TSLA --start 2026-05-21 --end 2026-05-21 --dry-run` piped through a JSON schema/version assertion (`EventSchemaVersion=databento_ndjson_v2`)
  - `python3 -m unittest discover -s tests -p 'test*.py' -v` (`35` tests)
  - `./mvnw -q -Dtest=DatabentoEventTest,DatabentoFeedHealthTest,DatabentoHealthIndicatorTest test`
  - `./mvnw -q test` (`71` Surefire tests, `failures=0`, `errors=0`, `skipped=0`)
- Stop/go decision: Step 10 first-pass parity is GO for moving to the next controlled blocker. Remaining before any paper/live promotion: record a real live-shaped NDJSON sample during market hours, replay it through `DatabentoHistoricalStreamingBacktester`, persist machine-readable replay parity artifacts, and complete probability-calibration/manifest hardening. Emergency exits/flattening remain outside the entry block and must stay available.

### Step 11 — Lifecycle/micro probability-calibration manifest hardening

Action plan:

- Add repeatable probability-calibration metrics to the lifecycle/micro trainer before any scorecard can be discussed as paper/live evidence.
- For every trained classifier, compute held-out Brier score, expected calibration error (ECE), calibration row count, and reliability-bin rows using the same chronological held-out split used for threshold optimization.
- Persist machine-readable artifacts next to `lifecycle_micro_scorecard.csv`: a `calibration_manifest.json` and `calibration_reliability.csv` that identify the calibration method, split convention, bins, per-model metrics, warnings, and artifact names.
- Surface Brier/ECE in `lifecycle_micro_scorecard.csv` and include calibration metadata in `lifecycle_micro_route_manifest.json` so downstream promotion-gate checks can reject uncalibrated or poorly calibrated bundles.
- Keep the result research-only until a future controlled step fits and exports an explicit isotonic/Platt/post-hoc calibrator where needed, freezes the internal holdout, and validates threshold stability/trade-count gates on the 48GB machine.

Action done:

- 2026-06-14 implementation is complete in branch code for lifecycle/micro calibration metrics and manifest artifacts. This is calibration measurement/manifest hardening only; it does not make any bundle paper/live promotable.
- Extended `train_lifecycle_micro_models.py` with `calibration_report(...)`, which computes Brier score, ECE, and fixed reliability bins from finite held-out probabilities clipped to `[0,1]`.
- Extended `TrainedModelResult`, `train_binary_model(...)`, and `write_scorecards(...)` so every trained lifecycle/micro classifier now records `brier_score`, `ece`, `calibration_rows`, and reliability-bin rows.
- New/updated emitted artifacts from lifecycle/micro training:
  - `lifecycle_micro_scorecard.csv` now includes `brier_score`, `ece`, and `calibration_rows`.
  - `lifecycle_micro_route_manifest.json` now includes a per-model `calibration` object with raw-probability method, Brier/ECE, row count, and reliability artifact reference.
  - `calibration_manifest.json` records schema version `lifecycle_micro_calibration_v1`, method `raw_random_forest_probability_no_posthoc_calibrator`, chronological held-out split convention, artifact names, per-model metrics, `errors=[]`, and a research-only warning that no isotonic/Platt post-hoc calibrator is exported yet.
  - `calibration_reliability.csv` records per-model reliability-bin rows.
- Added regression coverage in `tests/test_lifecycle_micro_models.py` for direct Brier/ECE/bin calculation, no-ONNX training-time calibration population, and scorecard/route/calibration artifact emission.
- Validation run on this computer passed:
  - `python3 -m py_compile train_lifecycle_micro_models.py tests/test_lifecycle_micro_models.py`
  - `python3 tests/test_lifecycle_micro_models.py` (`11` tests)
  - `python3 -m unittest discover -s tests -p 'test*.py' -v` (`37` tests)
  - `./mvnw -q test` (`71` Surefire tests, `failures=0`, `errors=0`, `skipped=0`)
- 2026-06-14 48GB/write-capable rerun completed after pulling the Step 11 code. Durable output `model_training_sets/lifecycle_micro_fixed_quality_setup_oof_20260614_145406` retained `28780` joined 30s rows, dropped `10220` intentionally unscored early rows, trained all six smoke routes with `--max-entry-events 2000 --no-onnx`, and wrote `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`, `calibration_manifest.json`, `calibration_reliability.csv`, and `train.log`.
- Calibration manifest verification for `model_training_sets/lifecycle_micro_fixed_quality_setup_oof_20260614_145406` recorded schema `lifecycle_micro_calibration_v1`, method `raw_random_forest_probability_no_posthoc_calibrator`, `errors=[]`, `model_count=6`, and `60` reliability rows. Held-out raw-probability metrics: `longExitLifecycleAi` rows `4808`, Brier `0.09692428983675519`, ECE `0.06708904600640704`; `shortExitLifecycleAi` rows `3192`, Brier `0.09102132172669967`, ECE `0.04093852817365681`; `longMicroEntryAi` rows `1670`, Brier `0.12747090562905813`, ECE `0.05871375933179435`; `shortMicroEntryAi` rows `1108`, Brier `0.08259536808446613`, ECE `0.05627806390870092`; `longMicroExitGuardAi` rows `1666`, Brier `0.1214582592554284`, ECE `0.027829839184617847`; `shortMicroExitGuardAi` rows `807`, Brier `0.11557275429308957`, ECE `0.07860105167569663`.
- 2026-06-14 reliability-bin inspection completed on the 48GB artifacts. Selected-threshold bins were very sparse for every route (`14`, `1`, `14`, `4`, `5`, and `10` rows respectively), with threshold-bin absolute calibration error ranging from `0.1328` to `0.4070`. Worst non-empty bins also showed large raw-probability gaps, including `longMicroEntryAi` bin `6` with `3` rows and `0.6104` absolute error, `shortMicroEntryAi` bin `5` with `4` rows and `0.4070` absolute error, and `shortMicroExitGuardAi` bin `5` with `7` rows and `0.4070` absolute error.
- Stop/go decision: Step 11 durable 10-day calibration-artifact rerun and reliability-bin inspection are complete. Decision is NO-GO for using raw RandomForest probabilities as promotion evidence and GO for a controlled post-hoc calibration step before threshold-stability work. The next calibration step should compare isotonic/Platt-style calibration on a larger/frozen held-out design and then rerun calibration, threshold-stability, trade-count, and day-dominance gates. Still NO paper/live promotion until post-hoc calibration/frozen-holdout threshold-stability gates, recorded-event replay parity, and paper/shadow drift checks pass.

### Step 12 — 30s setup model Tier-1 upgrade: scorecard, calibration, OOF, and threshold-grid artifacts

Action plan:

- Bring `train_30s_models.py` to the same artifact standard as `train_lifecycle_micro_models.py` after Step 11.
- Add `--output-dir` and `--no-onnx` flags matching the lifecycle trainer interface.
- Add `calibration_report()` and `feature_schema_hash()` using the same algorithm as the lifecycle trainer and `PingPongStrategy.java`.
- Extend `perform_walk_forward_testing()` with optional `collect_oof=True` for the two primary entry models only, collecting per-fold Brier/ECE, per-row OOF predictions, threshold stability metrics, and fold grid. Keep `collect_oof=False` default for all other call sites.
- Write six artifact files unconditionally when `--output-dir` is given (empty header-only CSV files when no folds ran, never silently absent): `setup_scorecard.csv`, `setup_manifest.json`, `calibration_manifest.json`, `calibration_reliability.csv`, `threshold_grid.csv`, `oof_setup_predictions.csv`.
- `setup_manifest.json` must record `feature_schema_sha256`, `label_info.cost_aware=false` warning, threshold stability fields, and `oof_coverage_frac`.
- The `oof_setup_predictions.csv` output from this run replaces the separate `generate_walk_forward_setup_predictions.py` step for the same input CSV. Pass it directly to `train_lifecycle_micro_models.py --setup-predictions-csv` on the next lifecycle/micro rerun.
- Regime classifier must also respect `--no-onnx`.
- Add 12 unit tests in `tests/test_train_30s_models.py`.
- Verify full test suite passes.

Pre-run gap review for the 48GB machine (reviewed 2026-06-15):

| # | Item | Status | Impact |
|---|---|---|---|
| G1 | `TRAIN_LEGACY_30S_EXIT_MODELS=0` env var prefix syntax | **CONFIRMED SAFE** — Python reads `os.getenv()` at import time, so the prefix env var is effective. Verified: `exit flag: False` in isolation test. | None |
| G2 | News features (`USE_NEWS_BAR_FEATURES=1` default) with no news columns in pilot CSV | **CONFIRMED SAFE** — `calculate_features()` uses `_opt_numeric(col, default=0.0)` for every raw news source column. All 27 `f_news_*` feature columns will be created as zero-valued when the Databento-only pilot CSV lacks news source columns. The model trains on zero-signal news features. | Performance only: news block adds 27 zero-constant features. Acceptable for a smoke run; exclude with `USE_NEWS_BAR_FEATURES=0` for a cleaner baseline. |
| G3 | Exit labels always generated even with `TRAIN_LEGACY_30S_EXIT_MODELS=0` | **CONFIRMED SAFE** — `generate_labels()` always computes and assigns `Label_Long_Exit` and `Label_Short_Exit` regardless of the flag. The flag only controls which models are trained; the models list construction references both exit label columns before filtering, so no KeyError. | None |
| G4 | `ensure_training_csv_available()` with explicit `--input-csv` | **CONFIRMED SAFE** — the function returns `True` immediately if the file exists, no auto-build attempted. | None |
| G5 | OOF predictions will be limited or empty on the 10-day pilot window | **EXPECTED** — the 10-day window has few distinct days for reliable walk-forward folds. With the strict `ENTRY_PROFIT_PCT` / `ENTRY_RISK_PCT` thresholds and `MIN_NET_R_MULTIPLE=1.20` (currently generating a warning that net R is only 1.07), the number of positive labels may be zero or near-zero for entry models on random-ish data. The `oof_setup_predictions.csv` and `threshold_grid.csv` will be written as empty-header files in this case. This is the expected smoke behavior. Useful OOF predictions require the full-window build. | Informational only on this smoke run. |
| G6 | `setup_manifest.json` records `label_info.cost_aware=False` warning | **INFORMATIONAL** — the manifest explicitly flags that labels are binary `tp_before_sl` with basic slippage constants only. No `expected_net_r_after_costs` label or label manifest yet. Any feature-block experiment evaluated against these labels is research-only. | Feature experiments must wait for Phase 4 cost-aware labels. |
| G7 | `$LAKE_ROOT` and `$PILOT_BUILD_ROOT` must be set before running | **USER ACTION REQUIRED** — these are not set by the code. `PILOT_BUILD_ROOT` should point to `model_training_sets/pilot_10d_fixed_quality_20260613_173446` on the 48GB vault. | Command will fail immediately with `:?` error if unset. |
| G8 | `combined_30s.csv` must exist at `$PILOT_BUILD_ROOT/combined/combined_30s.csv` | **CONFIRMED PRESENT** — post-build verification of `pilot_10d_fixed_quality_20260613_173446` confirmed the combined file exists with 39000 rows for 5 symbols × 10 days. | None |

Action done:

- 2026-06-15 Tier-1 upgrade implemented in `train_30s_models.py` on this computer. All verified items above are from code inspection and isolation tests, not a live 48GB run.
- Added `--output-dir`, `--no-onnx`, `calibration_report()`, `feature_schema_hash()`, `collect_oof` option in `perform_walk_forward_testing()`, six unconditional artifact files in `main()`.
- Added `tests/test_train_30s_models.py` with 12 tests; full suite 49 tests OK (up from 37).
- Committed as `e077a2b`.

- 2026-06-15 Step 12 smoke run completed on the 48GB machine after pulling commit `e077a2b`.
- Run ID: `setup_30s_fixed_quality_20260615_124546`
- Output directory: `/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/setup_30s_fixed_quality_20260615_124546`
- Input: `pilot_10d_fixed_quality_20260613_173446/combined/combined_30s.csv` (39000 rows, all kept after regular-session filter).
- After soft separation (>=10:00 ET): 35685 rows.
- Feature count: 61 (30 base + 27 news zero-signal + 4 regime-prob).
- Regime distribution: choppy 26.95%, trend 27.17%, volatile 45.88%.
- WARNING (expected): Net R after costs is 1.07, below `MIN_NET_R_MULTIPLE=1.20`. Labels are cost-naive binary for now.

**Main entry model scorecard** (5 walk-forward folds each):

| Model | Signals/Rows | SignalRate | AvgPrecision | AvgThreshold | ThrStd | Brier | ECE |
|---|---|---|---|---|---|---|---|
| LONG ENTRY | 5202/35685 | 14.58% | 16.21% | 0.68 | 0.0126 | 0.1912 | 0.2274 |
| SHORT ENTRY | 5475/35685 | 15.34% | 7.03% | 0.68 | 0.0427 | 0.1816 | 0.2023 |

All six artifact files verified present:
- `setup_scorecard.csv` ✓
- `setup_manifest.json` ✓ (`schema_version: setup_30s_v1`, `feature_count: 61`)
- `calibration_manifest.json` ✓
- `calibration_reliability.csv` ✓
- `threshold_grid.csv` ✓ (10 fold rows)
- `oof_setup_predictions.csv` ✓ (`oof_long_rows: 18000`, `oof_short_rows: 18000`)

**Bug discovered and fixed (commit `3458d7e`)**: `--no-onnx` was not respected by `train_regime_specific_models()` or `train_open30_models()`. Both functions called `export_to_onnx()` unconditionally, so the 8 regime-specific and open30 ONNX files were exported and `src/main/resources/` canonical models were overwritten on the 48GB machine despite `--no-onnx`. Fixed by adding `no_onnx: bool = False` parameter to both functions and gating `export_to_onnx()`. Added 4 new regression tests (`TestRegimeSpecificNoOnnx`, `TestOpen30NoOnnx`); 53 tests OK. The erroneously written ONNX files on the 48GB machine (`choppy_*.onnx`, `trend_*.onnx`, `volatile_*.onnx`, `open30_*.onnx`) and the overwritten `src/main/resources/` canonical models must be treated as **research-only 10-day smoke artifacts**, not for paper/live promotion.

- Stop/go decision: **GO for the next lifecycle/micro rerun** using `oof_setup_predictions.csv` from this run. Pull commit `3458d7e` on the 48GB machine before the next setup run to ensure `--no-onnx` is fully respected. The `oof_setup_predictions.csv` output is the key artifact; pass it to the lifecycle/micro trainer on the next full-window rerun to replace `generate_walk_forward_setup_predictions.py`.

Exact command for the 48GB machine after pulling commit `e077a2b`:

```zsh
# Set these to match the 48GB machine paths.
export LAKE_ROOT=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2
export PILOT_BUILD_ROOT="$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446"

test -f "$PILOT_BUILD_ROOT/combined/combined_30s.csv" || { echo "ERROR: combined_30s.csv not found"; exit 1; }

export SETUP_RUN_ID="setup_30s_fixed_quality_$(date +%Y%m%d_%H%M%S)"
export SETUP_OUT_DIR="$LAKE_ROOT/model_training_sets/$SETUP_RUN_ID"
mkdir -p "$SETUP_OUT_DIR"

TRAIN_LEGACY_30S_EXIT_MODELS=0 \
python3 train_30s_models.py \
  --input-csv "$PILOT_BUILD_ROOT/combined/combined_30s.csv" \
  --output-dir "$SETUP_OUT_DIR" \
  --no-onnx \
  2>&1 | tee "$SETUP_OUT_DIR/train.log"
```

**NOTE**: For future reruns, pull commit `3458d7e` first. The `e077a2b` version had a bug where `--no-onnx` was not respected for regime-specific (choppy/trend/volatile) and open30 models; fixed in `3458d7e`.

Optional: add `USE_NEWS_BAR_FEATURES=0` as a prefix env var to suppress zero-signal news feature columns from the baseline feature set. This is recommended for the `baseline_current_v1` experiment to keep the feature count clean.

After the run, verify:

```zsh
# All six artifact files must exist (may be empty-header CSVs if no signals).
for f in setup_scorecard.csv setup_manifest.json calibration_manifest.json \
          calibration_reliability.csv threshold_grid.csv oof_setup_predictions.csv; do
  test -f "$SETUP_OUT_DIR/$f" && echo "OK $f" || echo "MISSING $f"
done

# Check manifest schema version.
python3 -c "import json; m=json.load(open('$SETUP_OUT_DIR/setup_manifest.json')); print('schema_version:', m['schema_version']); print('feature_count:', m['feature_count']); print('oof_long_rows:', m['long_entry']['oof_rows']); print('oof_short_rows:', m['short_entry']['oof_rows'])"
```

### Step 13 — Lifecycle/micro rerun using train_30s_models.py OOF setup predictions

Action plan:

- Pull commit `3458d7e` on the 48GB machine to get the `--no-onnx` fix before this or any future setup run.
- Use `oof_setup_predictions.csv` from `setup_30s_fixed_quality_20260615_124546` as the `--setup-predictions-csv` input instead of running `generate_walk_forward_setup_predictions.py`.
- The OOF file has `oof_long_rows: 18000` and `oof_short_rows: 18000` from 5 walk-forward folds over 35685 non-opening rows. Confirm the lifecycle/micro join retains a comparable number of rows to the Step 9/11 runs (`~28780`).
- Use the same `--max-entry-events 2000 --no-onnx` flags as Step 11.
- Verify all lifecycle/micro artifacts are written: `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`, `calibration_manifest.json`, `calibration_reliability.csv`, and `train.log`.
- Record Brier/ECE per route and compare against Step 11 values. Any improvement is informational only (still 10-day slice, still research-only).
- This run retires `generate_walk_forward_setup_predictions.py` as an intermediate step for the pilot pipeline. The `train_30s_models.py --output-dir` run is now the single source for setup OOF predictions.

Action plan command for the 48GB machine after pulling `e69b325`:

```zsh
export LAKE_ROOT=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2
export PILOT_BUILD_ROOT="$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446"
export SETUP_OOF_CSV="$LAKE_ROOT/model_training_sets/setup_30s_fixed_quality_20260615_124546/oof_setup_predictions.csv"

test -f "$SETUP_OOF_CSV" || { echo "ERROR: oof_setup_predictions.csv not found"; exit 1; }

export LIFECYCLE_RUN_ID="lifecycle_micro_setup30_oof_$(date +%Y%m%d_%H%M%S)"
export LIFECYCLE_OUT_DIR="$LAKE_ROOT/model_training_sets/$LIFECYCLE_RUN_ID"
mkdir -p "$LIFECYCLE_OUT_DIR"

python3 train_lifecycle_micro_models.py \
  --input-30s-csv "$PILOT_BUILD_ROOT/combined/combined_30s.csv" \
  --input-5s-csv "$PILOT_BUILD_ROOT/combined/combined_5s.csv" \
  --setup-predictions-csv "$SETUP_OOF_CSV" \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --max-entry-events 2000 \
  --no-onnx \
  2>&1 | tee "$LIFECYCLE_OUT_DIR/train.log"
```

**NOTE on the OOF CSV from `setup_30s_fixed_quality_20260615_124546`**: this was written by commit `e077a2b` (narrow format, wrong schema). It cannot be used directly as `--setup-predictions-csv` because `train_lifecycle_micro_models.py` requires wide-format columns (`long_setup_fold_id`, `short_setup_fold_id`). You must re-run the setup training with commit `e69b325` to produce the corrected wide-format OOF CSV before running Step 13. Use the following corrected setup rerun command first:

```zsh
export LAKE_ROOT=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2
export PILOT_BUILD_ROOT="$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446"

test -f "$PILOT_BUILD_ROOT/combined/combined_30s.csv" || { echo "ERROR: combined_30s.csv not found"; exit 1; }

export SETUP_RUN_ID="setup_30s_fixed_quality_$(date +%Y%m%d_%H%M%S)"
export SETUP_OUT_DIR="$LAKE_ROOT/model_training_sets/$SETUP_RUN_ID"
mkdir -p "$SETUP_OUT_DIR"

TRAIN_LEGACY_30S_EXIT_MODELS=0 \
python3 train_30s_models.py \
  --input-csv "$PILOT_BUILD_ROOT/combined/combined_30s.csv" \
  --output-dir "$SETUP_OUT_DIR" \
  --no-onnx \
  2>&1 | tee "$SETUP_OUT_DIR/train.log"
```

Then pass `$SETUP_OUT_DIR/oof_setup_predictions.csv` as `--setup-predictions-csv` to the lifecycle/micro run.

Action done:

- 2026-06-15 Step 13 completed on the 48GB machine after pulling commit `e69b325`.

**Setup rerun** (`setup_30s_fixed_quality_20260615_144107`):
- Wide-format `oof_setup_predictions.csv` written: `total_rows=35685 oof_rows=18000` ✓
- All ONNX exports: `(skipped --no-onnx)` for all model families including regime-specific and open30 ✓ (`--no-onnx` fix from `3458d7e` confirmed working)
- All 6 artifacts present ✓

**Lifecycle/micro rerun** (`lifecycle_micro_setup30_oof_20260615_131900`):
- OOF join succeeded: `dropped_unscored_30s_rows=21000 retained_rows=18000`, `errors=[]` ✓
- Setup prediction quality: long `unique_values=17901` finite, short `unique_values=17919` finite — fully non-constant ✓
- All 6 routes trained successfully:

| Route | Rows | Positives | Threshold | Precision | Recall |
|---|---|---|---|---|---|
| longExitLifecycleAi | 22999 | 7663 | 0.58 | 100.00% | 73.41% |
| shortExitLifecycleAi | 16567 | 5497 | 0.64 | 100.00% | 46.97% |
| longMicroEntryAi | 8002 | 1276 | 0.50 | 0.00% | 0.00% |
| shortMicroEntryAi | 5910 | 823 | 0.50 | 96.15% | 43.10% |
| longMicroExitGuardAi | 7680 | 1602 | 0.60 | 100.00% | 55.37% |
| shortMicroExitGuardAi | 5448 | 1236 | 0.76 | 99.28% | 39.77% |

- All exports: `disabled` (--no-onnx) ✓
- Artifacts confirmed written: `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`

**Notable observations:**

1. **OOF coverage gap**: `retained_rows=18000` vs `28780` from the Step 9/11 `generate_walk_forward_setup_predictions.py` run. The 18000 rows come from 5 walk-forward test folds × ~3600 rows each. The remaining 21000 dropped rows = 17685 train-only rows (never in a test fold) + 3315 opening-window rows. `generate_walk_forward_setup_predictions.py` used a rolling scheme that produced 80.7% OOF coverage (28780/35685); `train_30s_models.py` with `N_SPLITS=5` produces 50.4% coverage (18000/35685). Fewer OOF rows = fewer lifecycle/micro training samples. This is informational for the 10-day pilot; the full-window build will have more training days and better coverage.

2. **longMicroEntryAi failed to learn**: threshold collapsed to `0.50` with `precision=0%` / `recall=0%`. This means the model predicts no positives above 0.50 on the holdout. With only 10 days of data and `--max-entry-events 2000`, this is expected — insufficient entry events for the long micro-entry model on this slice. Not a blocker for the smoke pipeline.

3. **Calibration artifacts**: output log only showed 2 WROTE lines (`lifecycle_micro_scorecard.csv` and `lifecycle_micro_route_manifest.json`). Verify that `calibration_manifest.json` and `calibration_reliability.csv` are also present in the output directory on the 48GB machine. Run: `ls -la "$LIFECYCLE_OUT_DIR/"` and confirm all 4 expected files exist.

- Stop/go decision: **Step 13 pipeline is GO** — wide-format OOF setup predictions flow correctly through to lifecycle/micro training. Infrastructure smoke confirms the end-to-end pipeline from `train_30s_models.py → oof_setup_predictions.csv → train_lifecycle_micro_models.py` works. Results are still research-only 10-day pilot evidence. Next steps: verify calibration artifacts on 48GB machine, then move to Phase 5 (post-hoc isotonic/Platt calibration) or Phase 1 (new mbp-1/tcbbo pilot normalizers).

Implementation review follow-up on 2026-06-15:

- Commit `e8c206b` hardened the Step 12/13 implementation after review.
- `calibration_reliability.csv` from `train_30s_models.py` now contains actual per-fold reliability-bin rows instead of duplicating threshold-grid fold metrics.
- Setup OOF manifest counts now record side-specific OOF counts and paired rows explicitly.
- `train_30s_models.py --no-onnx` completion logging no longer says models were exported.
- `train_lifecycle_micro_models.py` now prints `calibration_manifest.json`, `calibration_reliability.csv`, and `feature_schema.json` in the run log.
- Validation: `py_compile` OK; `tests/test_train_30s_models.py` now has 17 tests; full Python suite `54` tests OK.

### Step 14 — Verify lifecycle/micro calibration artifacts and decide next priority

Action plan:

- On the 48GB machine, confirm `calibration_manifest.json` and `calibration_reliability.csv` exist in `lifecycle_micro_setup30_oof_20260615_131900`.
- If they exist, read `calibration_manifest.json` and compare Brier/ECE per route against Step 11 values. Any change is informational (different OOF coverage).
- Decide the next priority between:
  - **Phase 5 (calibration)**: Post-hoc isotonic/Platt calibration with a frozen holdout. Reliability-bin ACE was up to 0.407 on raw RF probabilities (Step 11). Required before any paper/live promotion decision.
  - **Phase 1 (new sources)**: Build `normalize_equs_mbp1.py` and `normalize_opra_tcbbo.py` silver normalizers to unlock the 58.87 GiB `EQUS mbp-1` and 15.08 GiB `OPRA tcbbo` pilot feeds. These feeds add true quote-state and option trade coverage that the current tbbo/ohlcv-1s baseline lacks.
  - **Full-window build**: Expand the 10-day pilot to the full aligned window after Phase 1 normalizers are ready.
- Current recommendation: Phase 5 calibration next (completes the existing pipeline), then Phase 1 new sources (expands feature coverage).

Verification command for the 48GB machine:

```zsh
LIFECYCLE_OUT_DIR="$LAKE_ROOT/model_training_sets/lifecycle_micro_setup30_oof_20260615_131900"
ls -la "$LIFECYCLE_OUT_DIR/"
python3 -c "
import json
m = json.load(open('$LIFECYCLE_OUT_DIR/calibration_manifest.json'))
print('schema_version:', m.get('schema_version'))
print('errors:', m.get('errors'))
print('model_count:', len(m.get('models', [])))
for r in m.get('models', []):
    rows = r.get('calibration_rows', r.get('rows'))
    print(f"  {r['model']}: brier={r.get('brier_score')} ece={r.get('ece')} rows={rows}")
"
```

Action done:

- 2026-06-15 Step 14 verification completed on the 48GB machine for `lifecycle_micro_setup30_oof_20260615_131900`.
- Required files present:
  - `calibration_manifest.json` ✓
  - `calibration_reliability.csv` ✓
  - `feature_schema.json` ✓
  - `feature_schema.sha256` ✓
  - `lifecycle_micro_route_manifest.json` ✓
  - `lifecycle_micro_scorecard.csv` ✓
  - `train.log` ✓
- Calibration manifest: `schema_version=lifecycle_micro_calibration_v1`, `errors=[]`, `model_count=6` ✓
- Raw-probability Brier/ECE metrics from the setup-OOF lifecycle/micro smoke:

| Route | Brier | ECE |
|---|---:|---:|
| longExitLifecycleAi | 0.08998221566430302 | 0.04873342874122435 |
| shortExitLifecycleAi | 0.15042817360250915 | 0.08845588817022396 |
| longMicroEntryAi | 0.14619443208228028 | 0.08849331526695946 |
| shortMicroEntryAi | 0.08660043789889454 | 0.052517958287303046 |
| longMicroExitGuardAi | 0.08716045350325542 | 0.06815129182722955 |
| shortMicroExitGuardAi | 0.16015057256614065 | 0.10085371071999 |

- Note: the existing manifest uses key `rows` for calibration sample count, so the original verification print showed `rows=None` when asking for `calibration_rows`. Commit after this verification adds `calibration_rows` as an alias while preserving `rows` for backward compatibility.
- Stop/go decision: **GO** — Step 14 artifact verification passed. The pipeline is valid as infrastructure smoke evidence. Still **NO-GO** for paper/live promotion until post-hoc calibration, cost-aware labels, threshold-stability/day-dominance gates, full-window training, and replay/live parity gates are complete.

### Step 15 — Phase 5 first-pass post-hoc lifecycle/micro calibration

Action plan:

- Add controlled post-hoc probability calibration to `train_lifecycle_micro_models.py` without changing default raw-probability behavior.
- Use a chronological three-way split: base-train rows first, calibrator-fit rows next, frozen-holdout rows last. The frozen holdout must not fit either the classifier or the calibrator.
- Compare raw probabilities against Platt/sigmoid and isotonic calibrated probabilities on the frozen holdout using Brier score, ECE, reliability bins, selected-threshold precision/recall, and predicted-positive count.
- Preserve machine-readable artifacts with model exports/manifests: comparison CSV, reliability CSV, calibrator JSON, route-manifest calibration metadata, and holdout fingerprint hashes.
- Keep calibrated artifacts explicitly research-only until the 48GB machine reruns the pipeline on the frozen dataset and the later promotion gates pass.

Action done:

- Implemented opt-in Phase 5 flags in `train_lifecycle_micro_models.py`:
  - `--posthoc-calibration none|sigmoid|isotonic|both`
  - `--posthoc-calibration-frac`
  - `--frozen-holdout-frac`
  - `--min-frozen-holdout-rows`
  - `--min-holdout-predictions`
  - `--max-day-dominance-frac`
- Added frozen chronological split helper, holdout fingerprinting, Platt/sigmoid calibrator fitting, isotonic calibrator fitting, calibrated-probability application, raw-vs-calibrated comparison metrics, and research-only gate warnings.
- Added/extended artifacts:
  - `posthoc_calibration_comparison.csv`
  - `posthoc_calibration_reliability.csv`
  - `posthoc_calibrators.json`
  - `lifecycle_micro_scorecard.csv` posthoc columns
  - `lifecycle_micro_route_manifest.json` posthoc metadata
  - `calibration_manifest.json` posthoc artifact references and per-model posthoc metadata
- Added regression coverage in `tests/test_lifecycle_micro_models.py` for:
  - final chronological holdout freezing,
  - post-hoc calibrator fitting on a synthetic lifecycle route,
  - comparison CSV / calibrator JSON / calibration-manifest persistence.
- Validation on this computer:
  - `python3 -m py_compile train_lifecycle_micro_models.py` ✓
  - `python3 -m unittest discover -s tests -p 'test_lifecycle_micro_models.py' -v` → `13` tests OK ✓
  - `python3 -m unittest discover -s tests -v` → `56` tests OK ✓
- Stop/go decision: **GO for 48GB calibrated rerun**, **NO-GO for paper/live promotion**. This step adds code and local synthetic validation only. It does not prove calibrated thresholds are stable or promotable.

Recommended 48GB-machine calibrated rerun command:

Use the script below instead of pasting a long heredoc into interactive `zsh`. On the 48GB Mac, interactive `zsh` may not treat `#` as a comment; pasting comment lines with backticked run IDs can accidentally execute text such as `setup_30s_fixed_quality_20260615_124546` as a command. If the prompt is stuck at `>`, press `Ctrl-C`, open a fresh terminal, and use this shorter command sequence.

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only
git --no-pager log --oneline -1
bash scripts/run_lifecycle_micro_posthoc_calibration_20260615.sh
```

Default paths embedded in the runner:

- `LAKE_ROOT=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2`
- `PILOT_BUILD_ROOT=$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446`
- `SETUP_OUT_DIR=$LAKE_ROOT/model_training_sets/setup_30s_fixed_quality_20260615_144107`
- `LIFECYCLE_OUT_DIR=$LAKE_ROOT/model_training_sets/lifecycle_micro_posthoc_calibration_<timestamp>`

The script writes `train.log`, verifies the setup OOF schema before training, and prints the post-hoc comparison table after training.

48GB-machine run result on 2026-06-15:

- Runner: `scripts/run_lifecycle_micro_posthoc_calibration_20260615.sh`
- Output directory: `model_training_sets/lifecycle_micro_posthoc_calibration_20260615_165831`
- Setup OOF source: `model_training_sets/setup_30s_fixed_quality_20260615_144107/oof_setup_predictions.csv`
- OOF setup join succeeded: `dropped_unscored_30s_rows=21000`, `retained_rows=18000`, `errors=[]`, long unique values `17901`, short unique values `17919`.
- All six lifecycle/micro routes trained and all ONNX exports stayed disabled.
- Required Phase 5 artifacts were written:
  - `lifecycle_micro_scorecard.csv`
  - `lifecycle_micro_route_manifest.json`
  - `calibration_manifest.json`
  - `calibration_reliability.csv`
  - `posthoc_calibration_comparison.csv`
  - `posthoc_calibration_reliability.csv`
  - `posthoc_calibrators.json`
  - `feature_schema.json`
  - `feature_schema.sha256`
  - `train.log`
- Manifest summary: `errors=[]`, `model_count=6`, `posthoc_calibrators_exists=True`.

Frozen-holdout comparison from `posthoc_calibration_comparison.csv`:

| Model | Best Brier method in comparison | Best Brier | Best ECE method in comparison | Best ECE | Notes |
|---|---|---:|---|---:|---|
| `longExitLifecycleAi` | raw | 0.085120 | raw | 0.024624 | Raw beat sigmoid/isotonic; selected-method metadata from this run should be corrected by rerunning after the raw-selection fix. |
| `shortExitLifecycleAi` | raw | 0.129457 | raw | 0.047756 | Raw beat sigmoid/isotonic; selected-method metadata from this run should be corrected by rerunning after the raw-selection fix. |
| `longMicroEntryAi` | sigmoid | 0.136396 | sigmoid | 0.066769 | Still predicted zero positives at the selected threshold; not promotion-ready. |
| `shortMicroEntryAi` | sigmoid | 0.135049 | isotonic | 0.036171 | Brier and ECE disagree; keep comparison artifact, do not promote. |
| `longMicroExitGuardAi` | isotonic | 0.082384 | isotonic | 0.013965 | Strongest calibration improvement in the 10-day smoke. |
| `shortMicroExitGuardAi` | isotonic | 0.134292 | isotonic | 0.052617 | Some predicted-positive concentration remains. |

Follow-up hardening after reviewing the run:

- The comparison CSV is valid and useful, but `selected_method` in the first Phase 5 run selected the best fitted calibrator even when raw/no-op probabilities had better frozen-holdout Brier/ECE for the two lifecycle exit routes.
- Code has been hardened so raw/no-op is part of the candidate set and can win selection when it outperforms fitted Platt/isotonic calibrators.
- Added regression coverage to force fitted calibrators to be worse and assert that `selected_method == raw`.
- Validation on this computer after the fix: `py_compile` OK and `tests/test_lifecycle_micro_models.py` now has `14` tests OK.

Corrected 48GB-machine rerun after pulling commit `10e7bf9`:

- Output directory: `model_training_sets/lifecycle_micro_posthoc_calibration_20260615_170924`
- Manifest summary: `errors=[]`, `model_count=6`, `posthoc_calibrators_exists=True`.
- OOF setup join again succeeded: `dropped_unscored_30s_rows=21000`, `retained_rows=18000`, long unique values `17901`, short unique values `17919`.
- Required Phase 5 artifacts again written: scorecard, route manifest, raw calibration manifest/reliability, posthoc comparison/reliability, posthoc calibrators, feature schema/hash, and `train.log`.
- The raw-selection fix worked: `longExitLifecycleAi` and `shortExitLifecycleAi` now report `posthoc=raw`, matching their best frozen-holdout Brier/ECE rows in the comparison CSV.

Selected methods from the corrected rerun:

| Model | Selected method | Selected Brier | Selected threshold | Frozen rows | Key issue |
|---|---|---:|---:|---:|---|
| `longExitLifecycleAi` | raw | 0.085120 | 0.62 | 6597 | `max_predicted_day_fraction=1.00000` |
| `shortExitLifecycleAi` | raw | 0.129457 | 0.68 | 6623 | `max_predicted_day_fraction=1.00000` |
| `longMicroEntryAi` | sigmoid | 0.136396 | 0.50 | 2299 | predicted positives `0`; not useful yet |
| `shortMicroEntryAi` | sigmoid | 0.135049 | 0.70 | 2355 | `max_predicted_day_fraction=1.00000`; Brier/ECE disagree with isotonic |
| `longMicroExitGuardAi` | isotonic | 0.082384 | 0.60 | 2026 | `max_predicted_day_fraction=1.00000` despite calibration gain |
| `shortMicroExitGuardAi` | isotonic | 0.134292 | 0.60 | 2338 | `max_predicted_day_fraction=0.52862` remains above the configured 0.40 gate |

- Stop/go decision: **GO for Phase 5 research artifacts** — post-hoc comparison, selected-method metadata, reliability tables, calibrator parameters, and frozen-holdout fingerprints now exist and are internally consistent for the 10-day smoke. Still **NO-GO for paper/live promotion** because day-dominance, threshold-stability, sufficient trade-count/PnL, cost-aware labels, full-window training, runtime calibration application, replay parity, and paper/shadow drift gates remain open.

### Step 16 — Artifact-only post-hoc gate report

Action plan:

- Add a repeatable checker that reads an existing lifecycle/micro post-hoc output directory without retraining.
- Convert manual inspection of `posthoc_calibration_comparison.csv` into machine-readable gate rows and a gate report JSON.
- Gate at minimum on selected-method consistency, frozen holdout rows, selected predicted-positive count, and max predicted day fraction.
- Keep this as a research/promotion-gate diagnostic only; do not promote any model from a 10-day smoke.

Action done:

- Added `scripts/check_lifecycle_posthoc_gates.py`.
- The checker writes:
  - `posthoc_promotion_gate_rows.csv`
  - `posthoc_promotion_gate_report.json`
- Added `tests/test_check_lifecycle_posthoc_gates.py` covering a PASS case and an expected FAIL case.

Run on the corrected Phase 5 output:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export LIFECYCLE_OUT_DIR="$LAKE_ROOT/model_training_sets/lifecycle_micro_posthoc_calibration_20260615_170924"

python3 scripts/check_lifecycle_posthoc_gates.py \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --min-frozen-holdout-rows 500 \
  --min-predicted-positive-count 20 \
  --max-day-dominance-frac 0.40
```

Expected result for the 10-day corrected Phase 5 smoke: `POSTHOC_PROMOTION_GATE=FAIL`. This is expected because day dominance is still too high and `longMicroEntryAi` has zero selected predicted positives.

48GB-machine Step 16 result on 2026-06-15:

- Ran against `model_training_sets/lifecycle_micro_posthoc_calibration_20260615_170924`.
- Wrote:
  - `posthoc_promotion_gate_rows.csv`
  - `posthoc_promotion_gate_report.json`
- Overall result: `POSTHOC_PROMOTION_GATE=FAIL`.
- Per-route gate rows:

| Model | Selected method | Predicted positives | Max predicted day fraction | Gate status | Main failure |
|---|---|---:|---:|---|---|
| `longExitLifecycleAi` | raw | 1716 | 1.00000 | FAIL | day dominance |
| `shortExitLifecycleAi` | raw | 1290 | 1.00000 | FAIL | day dominance |
| `longMicroEntryAi` | sigmoid | 0 | 0.00000 | FAIL | predicted positives below minimum |
| `shortMicroEntryAi` | sigmoid | 58 | 1.00000 | FAIL | day dominance |
| `longMicroExitGuardAi` | isotonic | 222 | 1.00000 | FAIL | day dominance |
| `shortMicroExitGuardAi` | isotonic | 297 | 0.52862 | FAIL | day dominance above `0.40` cap |

- Stop/go decision: **Correct FAIL**. The checker is doing its job. The 10-day smoke now has explicit machine-readable evidence that Phase 5 calibration artifacts exist but cannot be promoted. Do not relax this gate to pass a short pilot. Next blockers are Step 17 threshold-stability artifacts from a rerun, cost-aware labels, full-window training, runtime calibration application, replay parity, and paper/shadow checks.

### Step 17 — Threshold-stability / stable-island reporting

Action plan:

- During post-hoc lifecycle/micro training, evaluate every configured threshold-grid point on the frozen chronological holdout for each candidate probability stream: raw, sigmoid, and isotonic when fitted.
- Mark each threshold as eligible only when it passes the same frozen-holdout minimum predicted-positive count and max one-day dominance limits used by the promotion-gate checker.
- Summarize the contiguous eligible-threshold island around each candidate's selected threshold.
- Keep this as a research gate: it is a one-frozen-holdout threshold-neighborhood stability check, not cross-fold stability, PnL validation, or paper/live approval.

Action done in code:

- `train_lifecycle_micro_models.py` now writes:
  - `posthoc_threshold_stability.csv`
  - `posthoc_threshold_stability_report.json`
- `calibration_manifest.json` and `lifecycle_micro_route_manifest.json` now reference the threshold-stability artifacts.
- `scripts/check_lifecycle_posthoc_gates.py` now requires a selected-method stable island via `--min-stable-threshold-points` and reports `stable_threshold_island_points` in gate rows.
- Existing Step 16 outputs cannot be upgraded artifact-only because the frozen-holdout probabilities were not persisted; rerun training to produce Step 17 artifacts.

Rerun the corrected Phase 5 smoke with Step 17 artifacts:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only origin ai-training-dynamic-upgrade-20260612
git --no-pager log --oneline -1

bash scripts/run_lifecycle_micro_posthoc_threshold_stability_20260615.sh
```

The runner defaults to the corrected 10-day Phase 5 smoke inputs and avoids accidental ONNX export:

- `LAKE_ROOT=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2`
- `PILOT_BUILD_ROOT=$LAKE_ROOT/model_training_sets/pilot_10d_fixed_quality_20260613_173446`
- `SETUP_PREDICTIONS=$LAKE_ROOT/model_training_sets/setup_30s_fixed_quality_20260615_144107/oof_setup_predictions.csv`
- `RUN_ID=lifecycle_micro_posthoc_threshold_stability_<timestamp>`
- `--no-onnx`

The previously referenced path `model_training_sets/setup_oof_predictions_20260615_153000/setup_oof_predictions.csv` is stale/not available on the 48GB Mac for this smoke. Do not use it unless a matching setup OOF run is generated and verified.

The runner also writes the artifact-only promotion-gate report. To rerun that report manually against the same output directory:

```zsh
python3 scripts/check_lifecycle_posthoc_gates.py \
  --output-dir "$LIFECYCLE_OUT_DIR" \
  --min-frozen-holdout-rows 500 \
  --min-predicted-positive-count 20 \
  --max-day-dominance-frac 0.40 \
  --min-stable-threshold-points 3
```

Expected result for a short corrected smoke remains `POSTHOC_PROMOTION_GATE=FAIL` unless each selected method has enough frozen-holdout predictions, no excessive single-day dominance, and at least three contiguous eligible threshold-grid points around its selected threshold.

### Phase C — Training and promotion gates on the 48GB machine

2. Generate cost-aware expected-net-R labels before evaluating feature lift.
3. Generate out-of-fold 30s setup predictions and join them into lifecycle/micro rows.
4. Add probability calibration metrics before claiming any paper/live promotion:
   - Brier score
   - ECE
   - reliability curves or calibration tables
5. Require stable threshold islands across folds, sufficient holdout trades, and no single-day PnL dominance.
6. Keep all new bundles tagged `research-only` until the critical blockers from `docs/peer_review_ai_training_dynamic_upgrade_plan_20260613.md` are resolved.

## Practical command patterns

Read-only inspection on this computer:

```zsh
cd /Users/FXG06FA/trading-agent-main

# Safe: read-only inventory.
df -h /Users/FXG06FA/trading-agent-main /Volumes/DatabentoVault
mount | grep DatabentoVault | cat
find /Volumes/DatabentoVault -maxdepth 2 -type d | sort | sed -n '1,120p'
```

Do not run this on this computer:

```zsh
# Unsafe here because the external vault is read-only from this machine.
touch /Volumes/DatabentoVault/.databento_write_check
scripts/offload_large_artifacts_to_vault.sh --execute
```

Recommended first 48GB-machine output shape:

```zsh
# Example only; adapt paths to the 48GB machine.
export RAW_EQUS_TBBO=/path/to/EQUS-20260523-6J9KE98BJ9
export RAW_OPRA_OHLCV=/path/to/OPRA-20260523-MSV68VKVKD
export RAW_EQUS_MBP1=/path/to/EQUS-20260612-36BEU4G7M8
export RAW_OPRA_TCBBO=/path/to/OPRA-20260612-KN5TPHB5EF
export RAW_OPRA_DEFINITION=/path/to/OPRA-20260612-B5D4JV3GV6
export OUT_ROOT=/path/to/writeable/training_data/dynamic_pilot_20260613

mkdir -p "$OUT_ROOT"/manifests "$OUT_ROOT"/raw_audits "$OUT_ROOT"/pilot_10d "$OUT_ROOT"/logs

export HASH_RUN_ID="source_inventory_hashes_$(date +%Y%m%d_%H%M%S)"

python3 scripts/audit_databento_pilot_sources.py \
  --output-dir "$OUT_ROOT/manifests/$HASH_RUN_ID" \
  --include-hashes

# If hashing reports an I/O error, pull the version containing resilient hash
# recording and inspect manifest.json/source_files.csv for the exact file path.
# If the problem is only the duplicate OPRA definition folder, rerun with:
#   --exclude-source-label opra_definition_20260612_duplicate

export DBN_AUDIT_RUN_ID="dbn_day_audit_20260521_$(date +%Y%m%d_%H%M%S)"

python3 scripts/audit_databento_dbn_day.py \
  --date 2026-05-21 \
  --source-inventory "$OUT_ROOT/manifests/$HASH_RUN_ID/source_inventory.csv" \
  --output-dir "$OUT_ROOT/raw_audits/$DBN_AUDIT_RUN_ID" \
  --sample-rows 5 \
  --continue-on-error

export PILOT_DATES_RUN_ID="pilot_dates_latest10_$(date +%Y%m%d_%H%M%S)"

python3 scripts/plan_databento_pilot_dates.py \
  --paired-dates "$OUT_ROOT/manifests/$HASH_RUN_ID/paired_dates.csv" \
  --source-files "$OUT_ROOT/manifests/$HASH_RUN_ID/source_files.csv" \
  --output-dir "$OUT_ROOT/source_manifests/$PILOT_DATES_RUN_ID" \
  --days 10 \
  --strategy latest

# If a combined DBN audit summary exists, add:
#   --dbn-audit-summary "$OUT_ROOT/raw_audits/$AUDIT_SUMMARY_RUN_ID/dbn_audit_summary.csv"
```

## Bottom line

The downloaded data is available and readable here, so no more Databento download work is needed for the pilot. The bottleneck is now **safe artifact production**. Use this computer to inspect, code, document, and prepare; use the 48GB computer to write large normalized datasets, train, calibrate, backtest, and produce promotion artifacts.
