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

1. Read raw DBN folders in place with write-capable output storage available for generated artifacts; do not copy `Downloads` source DBNs to the vault unless a later capacity-approved step explicitly reverses this.
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
   - new `EQUS definition` (`EQUS-20260612-GFHRSU6F48`; corrected on 2026-06-16)
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

- Build the source inventory from the required folders: existing `EQUS tbbo`, existing `OPRA ohlcv-1s`, new `EQUS definition`, new `EQUS mbp-1`, new `OPRA tcbbo`, and primary new `OPRA definition`.
- Exclude the duplicate `OPRA-20260612-B5D4JV3GV6 2` definition folder unless a later hash check proves it is needed.
- Require `hash_error_count == 0` before using the source manifest for pilot planning.

Action done:

- Completed on the 48GB/write-capable computer under `source_inventory_hashes_20260613_133951`.
- `hash_error_count: 0`.
- `hash_ok_count: 1081`.
- `hash_skipped_count: 0`.
- Expected unpaired date remains `20260403`, present only in `equs_mbp1_20260612`.
- `duplicate_candidates: []` after excluding the duplicate definition folder.
- 2026-06-16 correction: `EQUS-20260612-GFHRSU6F48` is the `EQUS.MINI` equity definition folder. Earlier five-source manifest language predates that clarification; include `equs_definition_20260612` in future source inventories.

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
- Require exactly six source files per selected date in future enriched-source manifests: `EQUS tbbo`, `OPRA ohlcv-1s`, `EQUS definition`, `EQUS mbp-1`, `OPRA tcbbo`, and primary `OPRA definition`. Historical Step 3 selected-file counts were produced before the `EQUS definition` folder was identified.
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

Operational note from the first Step 17 rerun attempt: an exported shell variable can override the runner defaults. If a terminal still has `SETUP_PREDICTIONS` exported to the stale `setup_oof_predictions_20260615_153000` path, either run `unset SETUP_PREDICTIONS` before the runner or pull the latest branch with the guarded runner. The guarded runner auto-detects that known missing stale path and falls back to `setup_30s_fixed_quality_20260615_144107/oof_setup_predictions.csv`.

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

48GB-machine Step 17 result on 2026-06-15:

- Pulled `26b354a Guard threshold stability runner against stale setup path`.
- Ran `scripts/run_lifecycle_micro_posthoc_threshold_stability_20260615.sh`.
- Output directory: `model_training_sets/lifecycle_micro_posthoc_threshold_stability_20260615_190547`.
- Setup OOF source: `model_training_sets/setup_30s_fixed_quality_20260615_144107/oof_setup_predictions.csv`.
- Setup join retained the corrected OOF population:
  - dropped unscored 30s rows: `21000`
  - retained OOF-scored rows: `18000`
  - manifest/training errors: `[]`
- Wrote Step 17 artifacts:
  - `posthoc_threshold_stability.csv`
  - `posthoc_threshold_stability_report.json`
  - `posthoc_promotion_gate_rows.csv`
  - `posthoc_promotion_gate_report.json`
- Overall result: `POSTHOC_PROMOTION_GATE=FAIL` / `promotion_ready=false`.
- Selected methods matched the corrected raw/no-op selection behavior:

| Model | Selected method | Brier | ECE | Threshold | Frozen holdout rows |
|---|---|---:|---:|---:|---:|
| `longExitLifecycleAi` | raw | 0.085120 | 0.024624 | 0.62 | 6597 |
| `shortExitLifecycleAi` | raw | 0.129457 | 0.047756 | 0.68 | 6623 |
| `longMicroEntryAi` | sigmoid | 0.136396 | 0.066769 | 0.50 | 2299 |
| `shortMicroEntryAi` | sigmoid | 0.135049 | 0.059911 | 0.70 | 2355 |
| `longMicroExitGuardAi` | isotonic | 0.082384 | 0.013965 | 0.60 | 2026 |
| `shortMicroExitGuardAi` | isotonic | 0.134292 | 0.052617 | 0.60 | 2338 |

Promotion-gate rows:

| Model | Selected method | Predicted positives | Max predicted day fraction | Stable island points | Gate status |
|---|---|---:|---:|---:|---|
| `longExitLifecycleAi` | raw | 1716 | 1.00000 | 0 | FAIL |
| `shortExitLifecycleAi` | raw | 1290 | 1.00000 | 0 | FAIL |
| `longMicroEntryAi` | sigmoid | 0 | 0.00000 | 0 | FAIL |
| `shortMicroEntryAi` | sigmoid | 58 | 1.00000 | 0 | FAIL |
| `longMicroExitGuardAi` | isotonic | 222 | 1.00000 | 0 | FAIL |
| `shortMicroExitGuardAi` | isotonic | 297 | 0.52862 | 0 | FAIL |

- Threshold-stability result: every candidate method had `stable_island_points=0` and `pass_stable_threshold_island=false` under `min_stable_threshold_points=3` because the short smoke has either single-day predicted-positive dominance above the `0.40` cap or too few selected predictions.
- Stop/go decision: **Correct FAIL**. Step 17 is now producing the missing stable-threshold-island evidence, and the evidence correctly blocks promotion of this 10-day smoke. Do not relax the gate to pass the pilot. Next blockers remain cost-aware labels, broader/full-window training, Java/runtime calibration application, replay parity, and paper/shadow checks.

### Step 18 — Minimum cost-aware setup labels

Action plan:

- Replace the 30s setup entry training target with a minimum execution-aware binary label derived from `expected_net_r_after_costs > COST_AWARE_MIN_NET_R_LABEL`.
- Preserve the old `tp_before_sl` labels as audit columns, so label deltas are explicit instead of silently changing target semantics.
- Store all required assumptions in a machine-readable label manifest: entry/exit spread model, fixed slippage, fill probability, partial-fill penalty, missed-fill penalty, latency assumption, and ambiguous TP/SL policy.
- Keep all artifacts research-only until the cost-aware setup OOF run is followed by lifecycle/micro reruns, full-window training, promotion gates, runtime parity, and paper/shadow checks.

Action done in code:

- `train_30s_models.py` now defaults `COST_AWARE_LABELS=1` and writes:
  - `cost_aware_setup_labels.csv`
  - `cost_aware_label_manifest.json`
- Entry training targets `Label_Long_Entry` and `Label_Short_Entry` are cost-aware by default.
- Audit columns retained in setup labels and `oof_setup_predictions.csv`:
  - `Label_Long_Entry_CostAware`
  - `Label_Short_Entry_CostAware`
  - `Label_Long_Entry_TpBeforeSl`
  - `Label_Short_Entry_TpBeforeSl`
  - `Label_Long_Entry_ExpectedNetRAfterCosts`
  - `Label_Short_Entry_ExpectedNetRAfterCosts`
- `setup_manifest.json` now records `label_info.cost_aware=true`, the cost-aware label artifacts, and the label summary.
- `calibration_manifest.json` references the cost-aware label artifacts.
- `scripts/run_setup_cost_aware_labels_20260615.sh` is a paste-safe 48GB runner for the setup OOF rerun. It uses `--no-onnx`, disables canonical alias updates, and writes only research artifacts.

Run the 48GB cost-aware setup OOF smoke:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only origin ai-training-dynamic-upgrade-20260612
git --no-pager log --oneline -1

unset SETUP_OUT_DIR
unset RUN_ID

bash scripts/run_setup_cost_aware_labels_20260615.sh
```

Expected output directory shape:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/setup_cost_aware_30s_<timestamp>
```

Expected artifacts:

- `setup_scorecard.csv`
- `threshold_grid.csv`
- `oof_setup_predictions.csv`
- `calibration_manifest.json`
- `calibration_reliability.csv`
- `setup_manifest.json`
- `cost_aware_setup_labels.csv`
- `cost_aware_label_manifest.json`
- `train.log`

Observed 48GB smoke result on 2026-06-15:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/setup_cost_aware_30s_20260615_192705
```

Run summary:

```text
setup_errors: []
label_errors: []
label_cost_aware: True
label_type: binary_expected_net_r_after_costs
oof_rows: 35685
paired_oof_rows: 18000
```

Important economics warning from `train.log`:

```text
WARNING: Net entry reward/risk after configured costs is below MIN_NET_R_MULTIPLE=1.20: 1.07
```

Interpretation:

- This warning does **not** mean the run failed. The setup labels were generated and persisted correctly.
- It does mean that, after configured slippage/cost assumptions, the effective base setup reward/risk multiple is only about `1.07R`, below the configured minimum target of `1.20R`.
- Treat this output as an evaluation artifact, not as a production-ready model bundle. Promotion remains blocked until broader/full-window evidence, lifecycle/micro reruns, threshold-stability/day-dominance gates, runtime parity, replay parity, and paper/shadow checks pass.
- The average and median expected net R are negative on both sides in this 10-day smoke:
  - long: mean `-0.308803`, p50 `-0.383624`
  - short: mean `-0.324221`, p50 `-0.444902`
- The new cost-aware labels identify many more positives than the legacy TP-before-SL method:
  - long: `12,825` cost-aware positives vs `4,943` legacy positives, delta `8,692`
  - short: `11,870` cost-aware positives vs `5,464` legacy positives, delta `7,184`
- This may be intentional if the new label is capturing partial path economics, but it also increases the risk of noisy positives. Do not interpret this 10-day artifact as promotion evidence by itself.

Setup scorecard:

| Model | Avg precision | Avg threshold | Threshold std | Brier score | ECE | Folds used |
|---|---:|---:|---:|---:|---:|---:|
| LONG ENTRY (Dip Buyer) | 0.443288 | 0.60 | 0.000000 | 0.280213 | 0.198917 | 5 |
| SHORT ENTRY (Rip Seller) | 0.112428 | 0.64 | 0.045607 | 0.239966 | 0.148496 | 5 |

Cost-aware label summary:

| Side | Rows | Finite expected-net-R rows | Positive cost-aware labels | Positive cost-aware rate | Positive legacy TP-before-SL labels | Label delta count | Expected-net-R mean | Expected-net-R p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| long | 38385 | 38336 | 12825 | 0.334115 | 4943 | 8692 | -0.308803 | -0.383624 |
| short | 38385 | 38336 | 11870 | 0.309235 | 5464 | 7184 | -0.324221 | -0.444902 |

Stop/go decision: **GO for cost-aware label infrastructure; NO-GO for promotion**. The minimum cost-aware label path is now producing the expected artifacts, manifests, and OOF file, but the tight base economics and noisy-positive risk must be carried into the next lifecycle/micro rerun and promotion-gate analysis.

If the 10-day smoke produces sparse/empty OOF predictions, that is still not a promotion failure by itself. The purpose of this step is to verify cost-aware label generation and manifests. Useful OOF coverage and promotion evidence still require broader/full-window training.

### Step 19 — Lifecycle/micro rerun using cost-aware setup OOF

48GB-machine result on 2026-06-15:

- Pulled branch commit `29724d0`.
- Setup OOF source: `model_training_sets/setup_cost_aware_30s_20260615_192705/oof_setup_predictions.csv`.
- Output directory: `model_training_sets/lifecycle_micro_posthoc_threshold_stability_20260615_201142`.
- ONNX export remained disabled; artifacts are research/evaluation-only.

OOF setup join:

```text
OOF_SETUP_JOIN dropped_unscored_30s_rows=21000 retained_rows=18000
OOF_SETUP_JOIN summary={"errors": [], "min_unique_values": 3, "rows": 18000, "sides": {"long": {"finite_count": 18000, "max": 0.9336708854249688, "mean": 0.5324028887400515, "min": 0.0919267379380229, "missing_count": 0, "unique_values": 18000}, "short": {"finite_count": 18000, "max": 0.7790762200927414, "mean": 0.3642569053623551, "min": 0.0148872498857027, "missing_count": 0, "unique_values": 17998}}}
```

Run summary:

```text
output_dir: /Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/lifecycle_micro_posthoc_threshold_stability_20260615_201142
errors: []
model_count: 6
posthoc_threshold_stability_report_exists: True
posthoc_promotion_gate_report_exists: True
promotion_ready: False
POSTHOC_PROMOTION_GATE=FAIL
```

Selected posthoc methods:

| Model | Selected method | Posthoc Brier | Posthoc ECE | Posthoc threshold | Calibration rows |
|---|---|---:|---:|---:|---:|
| `longExitLifecycleAi` | raw | 0.084169 | 0.022223 | 0.62 | 6597 |
| `shortExitLifecycleAi` | raw | 0.129095 | 0.044535 | 0.70 | 6623 |
| `longMicroEntryAi` | sigmoid | 0.136460 | 0.062930 | 0.50 | 2299 |
| `shortMicroEntryAi` | sigmoid | 0.136171 | 0.053027 | 0.50 | 2355 |
| `longMicroExitGuardAi` | isotonic | 0.082384 | 0.013965 | 0.60 | 2026 |
| `shortMicroExitGuardAi` | isotonic | 0.134292 | 0.052617 | 0.60 | 2338 |

Promotion-gate rows:

| Model | Selected method | Predicted positives | Max predicted day fraction | Stable island points | Gate status |
|---|---|---:|---:|---:|---|
| `longExitLifecycleAi` | raw | 1716 | 1.00000 | 0 | FAIL |
| `shortExitLifecycleAi` | raw | 1276 | 1.00000 | 0 | FAIL |
| `longMicroEntryAi` | sigmoid | 0 | 0.00000 | 0 | FAIL |
| `shortMicroEntryAi` | sigmoid | 118 | 1.00000 | 0 | FAIL |
| `longMicroExitGuardAi` | isotonic | 222 | 1.00000 | 0 | FAIL |
| `shortMicroExitGuardAi` | isotonic | 297 | 0.52862 | 0 | FAIL |

Threshold-stability result:

- Every candidate method still had `stable_island_points=0` and `pass_stable_threshold_island=false` under `min_stable_threshold_points=3`.
- The cost-aware setup OOF changed the setup-probability distribution and some selected thresholds, but it did **not** remove the core 10-day-smoke blockers:
  - `longMicroEntryAi` selected zero frozen-holdout predictions.
  - Most selected routes still have single-day predicted-positive dominance above the `0.40` cap.
  - No selected route has a stable threshold island.

Stop/go decision: **Correct FAIL**. The downstream cost-aware rerun successfully produced lifecycle/micro posthoc calibration, threshold-stability, and promotion-gate artifacts, but it remains **NO-GO for promotion**. Continue treating this as infrastructure/evaluation evidence only. The next material blockers are broader/full-window training, label/economics review, Java/runtime calibration application, replay parity, paper/shadow checks, and full-window promotion gates.

### Step 20 — Broader/full-window cost-aware setup + lifecycle chain

Decision on 2026-06-16: keep the current cost assumptions for now and move to broader/full-window evidence. Do **not** tune the `1.07R < 1.20R` economics warning yet; carry it forward as a documented risk so this run isolates the effect of a broader date window.

Location correction from the 48GB Mac on 2026-06-16: the broader-duration inputs are currently raw DBN download folders under `/Users/filmonghezehey/Downloads`, not prebuilt `combined_30s.csv` / `combined_5s.csv` training files. Earlier docs recorded these folders at `/Volumes/DatabentoVault/...`; for this run, use the `Downloads` paths as the source of truth and build the combined CSVs first.

Storage policy correction from the user on 2026-06-16: do **not** copy the source DBN folders from `Downloads` to the external disk for now. The raw source DBNs should stay in `/Users/filmonghezehey/Downloads`; only generated `data_lake_v2` outputs such as manifests, `combined_30s.csv`, `combined_5s.csv`, setup artifacts, lifecycle artifacts, and logs should write under `/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2`.

Current raw DBN folder mapping:

| Current 48GB source folder | Recorded/known role | Used by current baseline builder? | Notes |
|---|---|---:|---|
| `/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9` | `EQUS.MINI` / `tbbo` | yes | Equity trade + top-of-book source consumed by `build_30s_from_5s_csv.py --dbeq-dir`. |
| `/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD` | `OPRA.PILLAR` / `ohlcv-1s` | yes | Compact option-volume source consumed by `build_30s_from_5s_csv.py --opra-dir`. |
| `/Users/filmonghezehey/Downloads/EQUS-20260612-36BEU4G7M8` | `EQUS.MINI` / `mbp-1` | no | Richer equity quote-state source. Inventory only until an `mbp-1` normalizer is implemented. |
| `/Users/filmonghezehey/Downloads/OPRA-20260612-KN5TPHB5EF` | `OPRA.PILLAR` / `tcbbo` | no | Richer option trade/quote source. Inventory only until a `tcbbo` normalizer is implemented. |
| `/Users/filmonghezehey/Downloads/OPRA-20260612-B5D4JV3GV6` | `OPRA.PILLAR` / `definition` | no | Option metadata source. Inventory only until a definition reader is implemented. |
| `/Users/filmonghezehey/Downloads/EQUS-20260612-GFHRSU6F48` | `EQUS.MINI` / `definition` | no | Equity definition source. Inventory only until an equity definition metadata reader is implemented. |

Correct order for the broader run:

1. Inventory the `Downloads` raw DBN folders and build broader/core 30s/5s CSVs from the two currently supported baseline folders: `EQUS-20260523-6J9KE98BJ9` and `OPRA-20260523-MSV68VKVKD`.
2. Run the cost-aware setup + lifecycle chain against those newly built CSVs.
3. Keep the 20260612 `EQUS definition` / `EQUS mbp-1` / `OPRA tcbbo` / `OPRA definition` folders as audited raw inputs for later richer-feature normalizers; they are not part of this baseline full-window chain yet.

Preflight the raw `Downloads` folders first:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only origin ai-training-dynamic-upgrade-20260612
git --no-pager log --oneline -1

export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export DOWNLOAD_ROOT="/Users/filmonghezehey/Downloads"

RUNNER_PREFLIGHT_ONLY=1 \
bash scripts/run_core_full_window_bars_from_downloads_20260616.sh
```

Then build the broader/core full-window CSVs from DBN. This writes under `$LAKE_ROOT/model_training_sets/core_full_window_bars_from_downloads_<timestamp>/`:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export DOWNLOAD_ROOT="/Users/filmonghezehey/Downloads"

unset BUILD_RUN_ID
unset BUILD_ROOT

bash scripts/run_core_full_window_bars_from_downloads_20260616.sh
```

By default this prerequisite writes only `combined_30s.csv`, `combined_5s.csv`, `data_30s/`, and `data_5s/`, because the cost-aware setup + lifecycle chain does not need full-window 1s outputs. If a later QA pass needs 1s artifacts too, set `WRITE_1S_OUTPUTS=1` before the DBN build.

After the DBN-to-CSV build passes, launch the cost-aware setup + lifecycle chain from its outputs:

```zsh
export BUILD_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/core_full_window_bars_from_downloads_<timestamp>"
export SOURCE_30S="$BUILD_ROOT/combined_30s.csv"
export SOURCE_5S="$BUILD_ROOT/combined_5s.csv"

unset SETUP_OUT_DIR
unset SETUP_PREDICTIONS
unset LIFECYCLE_OUT_DIR
unset CHAIN_RUN_ID
unset CHAIN_ROOT

bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh
```

Optional one-command mode after preflight, if the 48GB Mac can be left running for the full DBN build + setup + lifecycle chain:

```zsh
RUN_CHAIN_AFTER_BUILD=1 \
bash scripts/run_core_full_window_bars_from_downloads_20260616.sh
```

Raw-folder copy policy: do **not** copy the source DBN folders to the external disk for this run. Leave the six raw DBN folders in `Downloads`; `scripts/run_core_full_window_bars_from_downloads_20260616.sh` now rejects `COPY_RAW_DOWNLOADS=1` and `USE_COPIED_RAW_FOR_BUILD=1`. The allowed external writes are the manifest/build/training outputs under the `data_lake_v2` output root.

Main run scripts used for the 2026-06-16 full-window baseline work:

| Script / entrypoint | Role in this run | Main inputs | Main outputs / notes |
|---|---|---|---|
| `scripts/run_core_full_window_bars_from_downloads_20260616.sh` | Top-level DBN-to-core-bars runner. Inventory raw `Downloads` DBN folders in place, enforce no raw DBN source copying, and build baseline full-window 30s/5s CSVs. | `/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9` (`EQUS.MINI/tbbo`) and `/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD` (`OPRA.PILLAR/ohlcv-1s`); the four 20260612 folders are inventory-only. | `raw_download_manifest.json`, `bar_build_manifest.json`, `symbol_model_plan.csv`, `combined_30s.csv`, `combined_5s.csv`, `data_30s/`, `data_5s/` under `core_full_window_bars_from_downloads_<timestamp>/`. Optional `RUN_CHAIN_AFTER_BUILD=1` can launch the chain after the build. |
| `build_30s_from_5s_csv.py` | Core bar builder invoked by the DBN runner after raw-source inventory. Converts the supported baseline DBN-derived source streams into model-training bars. | Supported baseline `EQUS tbbo` and `OPRA ohlcv-1s` source folders for the core symbols/date window. | Combined/per-symbol 30s and 5s training CSVs. `1s` outputs remain optional and were not needed for the current training chain. |
| `scripts/run_broader_full_window_cost_aware_chain_20260616.sh` | Top-level setup + lifecycle/micro chain runner. Stages inputs, optionally trains setup, validates setup OOF predictions, trains lifecycle/micro, and runs posthoc gates. | `SOURCE_30S`, `SOURCE_5S`, core symbols `TSLA,TQQQ,NVDA,SPY,QQQ`, date range `[2025-07-21, 2026-05-23)`. | Chain root under `broader_full_window_cost_aware_<timestamp>/` or an explicit `CHAIN_RUN_ID`; uses `--no-onnx` and does not update canonical aliases. Hardened to reject stale external `SETUP_OUT_DIR` unless explicitly allowed. |
| `scripts/stage_broader_window_inputs.py` | Helper called by the chain runner to stream/filter/split full-window 30s/5s inputs by symbol and date. | `combined_30s.csv` and `combined_5s.csv` from the core bar build. | `input_slice/combined_30s.csv`, `input_slice/combined_5s.csv`, `input_slice/data_30s/`, `input_slice/data_5s/`, `input_slice_manifest.json`. |
| `train_30s_models.py` | 30s setup trainer. Used for the original full-window setup and the no-news RandomForest/LightGBM/CatBoost/min-net-R setup experiments. | Staged `input_slice/combined_30s.csv`; feature flags such as `USE_NEWS_BAR_FEATURES=0`, `MODEL_FAMILY=catboost|lightgbm|random_forest`, and cost-aware label flags. | `setup_scorecard.csv`, `threshold_grid.csv`, `oof_setup_predictions.csv`, `cost_aware_setup_labels.csv`, `cost_aware_label_manifest.json`, `calibration_manifest.json`, `setup_manifest.json`. Latest guard supports `REQUIRE_MODEL_FAMILY=1` to prevent silent fallback from LightGBM/CatBoost to RandomForest. |
| `train_lifecycle_micro_models.py` | Lifecycle/micro trainer. Consumes 30s + 5s staged rows plus setup OOF probabilities, then trains lifecycle exits, micro entries, and micro exit guards. | `input_slice/data_30s/`, `input_slice/data_5s/`, and selected `oof_setup_predictions.csv` such as the CatBoost no-news setup OOF. | `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`, calibration artifacts, `posthoc_threshold_stability.csv`, `posthoc_promotion_gate_rows.csv`, `posthoc_promotion_gate_report.json`, and `feature_schema.json`. |
| `scripts/check_lifecycle_posthoc_gates.py` | Posthoc artifact gate checker called by the chain runner after lifecycle/micro training. | Lifecycle output directory plus gate thresholds for frozen-holdout rows, predicted positives, day dominance, and stable threshold points. | Prints `POSTHOC_PROMOTION_GATE=PASS/FAIL` and writes/validates promotion-gate reports. Passing this gate remains research/artifact evidence, not paper/live approval. |

Action added in code:

- `scripts/stage_broader_window_inputs.py` streams combined or per-symbol full-window 30s/5s inputs and writes:
  - `input_slice/combined_30s.csv` for `train_30s_models.py`
  - `input_slice/combined_5s.csv` for inspection
  - `input_slice/data_30s/<SYMBOL>_30s_training.csv` for streamed lifecycle/micro training
  - `input_slice/data_5s/<SYMBOL>_5s_training.csv` for streamed lifecycle/micro training
  - `input_slice/input_slice_manifest.json`
- `scripts/run_broader_full_window_cost_aware_chain_20260616.sh` is a paste-safe 48GB runner that:
  - intentionally does **not** default to the 10-day pilot dataset,
  - auto-detects the older full-window `20260523` combined training files when present,
  - defaults to the core symbols `TSLA,TQQQ,NVDA,SPY,QQQ`,
  - defaults to the aligned full-window date range `[2025-07-21, 2026-05-23)`,
  - requires at least `100` unique staged days, `100000` staged 30s rows, and `600000` staged 5s rows so the same 10-day smoke cannot be rerun accidentally,
  - runs setup with `COST_AWARE_LABELS=1`, `ENTRY_FILL_MODE=next_open_with_slippage`, `TRAIN_LEGACY_30S_EXIT_MODELS=0`, `UPDATE_CANONICAL_MODEL_ALIASES=0`, and `--no-onnx`,
  - validates `oof_setup_predictions.csv` schema and paired OOF count before lifecycle/micro starts,
  - runs lifecycle/micro with posthoc calibration, threshold-stability, promotion-gate artifacts, streamed per-symbol staging, and `--no-onnx`,
  - writes everything under `$LAKE_ROOT/model_training_sets/broader_full_window_cost_aware_<timestamp>/` by default.
- `scripts/run_core_full_window_bars_from_downloads_20260616.sh` is the prerequisite when only raw DBN folders exist. It inventories the six `Downloads` raw folders in place, does not copy source DBNs to external storage, builds `combined_30s.csv` / `combined_5s.csv` from the two currently supported `20260523` folders, and can optionally launch the cost-aware chain with `RUN_CHAIN_AFTER_BUILD=1`.

If combined full-window CSVs already exist, or after `scripts/run_core_full_window_bars_from_downloads_20260616.sh` creates them, preflight the cost-aware chain with the built CSV paths:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only origin ai-training-dynamic-upgrade-20260612
git --no-pager log --oneline -1

export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export BUILD_ROOT="$LAKE_ROOT/model_training_sets/core_full_window_bars_from_downloads_<timestamp>"
export SOURCE_30S="$BUILD_ROOT/combined_30s.csv"
export SOURCE_5S="$BUILD_ROOT/combined_5s.csv"

RUNNER_PREFLIGHT_ONLY=1 \
bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh
```

Then run the broader/full-window core-symbol chain from those built CSVs:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
export LAKE_ROOT="/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2"
export BUILD_ROOT="$LAKE_ROOT/model_training_sets/core_full_window_bars_from_downloads_<timestamp>"
export SOURCE_30S="$BUILD_ROOT/combined_30s.csv"
export SOURCE_5S="$BUILD_ROOT/combined_5s.csv"

unset SETUP_OUT_DIR
unset SETUP_PREDICTIONS
unset LIFECYCLE_OUT_DIR
unset CHAIN_RUN_ID
unset CHAIN_ROOT

bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh
```

Expected output shape:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_full_window_cost_aware_<timestamp>/
  chain_config.env
  input_slice/
    combined_30s.csv
    combined_5s.csv
    data_30s/
    data_5s/
    input_slice_manifest.json
  setup_cost_aware_full_window/
    cost_aware_label_manifest.json
    cost_aware_setup_labels.csv
    oof_setup_predictions.csv
    setup_manifest.json
    setup_scorecard.csv
    train.log
  lifecycle_micro_full_window_cost_aware/
    lifecycle_micro_scorecard.csv
    lifecycle_micro_route_manifest.json
    posthoc_threshold_stability.csv
    posthoc_threshold_stability_report.json
    posthoc_promotion_gate_rows.csv
    posthoc_promotion_gate_report.json
    train.log
  lifecycle_staging/
```

Operational notes:

- This is still research/evaluation-only. The runner uses `--no-onnx` and does not update canonical aliases.
- The default lifecycle expansion is uncapped per symbol/side (`LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE=0`) and caps model training loads at `LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL=1000000`. If the 48GB Mac hits memory pressure, rerun with a bounded first-pass cap such as:

```zsh
export LIFECYCLE_MAX_ENTRY_EVENTS_PER_SYMBOL_SIDE=5000
export LIFECYCLE_MAX_STAGED_ROWS_PER_SYMBOL_PER_MODEL=100000
export LIFECYCLE_MAX_TRAIN_ROWS_PER_MODEL=750000
bash scripts/run_broader_full_window_cost_aware_chain_20260616.sh
```

- Do not interpret a promotion-gate failure as a script failure. The useful questions are whether stable threshold islands appear, whether day dominance improves, whether predicted-positive counts become adequate, and whether setup label economics remain too noisy under the documented cost assumptions.

Action done on 2026-06-16 — broader/core DBN build:

- Completed on the 48GB/write-capable Mac from raw `Downloads` source folders without copying raw DBNs to the external disk.
- Build root: `model_training_sets/core_full_window_bars_from_downloads_20260616_100914`.
- `bar_build_manifest.json`: `errors=[]`.
- Output coverage:
  - `combined_30s.csv`: `830700` rows, `213` days, symbols `NVDA,QQQ,SPY,TQQQ,TSLA`, date range `2025-07-21` through `2026-05-22`.
  - `combined_5s.csv`: `4984200` rows, `213` days, symbols `NVDA,QQQ,SPY,TQQQ,TSLA`, same date range.
- `raw_download_manifest.json` recorded `raw_source_copy_policy=disabled_downloads_in_place` and correctly inventoried the six `Downloads` folders, including `EQUS-20260612-GFHRSU6F48` as `EQUS.MINI / definition`.
- No `1s` output was produced, which is acceptable for the current setup/lifecycle/micro training chain because it consumes only `30s` and `5s` artifacts. The `1s`/tick path remains a live/backtest hard-stop/reflex path, not a current training input.

Action done on 2026-06-16 — broader/full-window cost-aware setup + lifecycle chain:

- Chain root: `model_training_sets/broader_full_window_cost_aware_20260616_115143`.
- Input staging passed: `30s_rows=830700`, `30s_days=213`, `5s_rows=4984200`, `5s_days=213`, symbols `NVDA,QQQ,SPY,TQQQ,TSLA`.
- Setup training used the full-window staged `combined_30s.csv` and wrote full-window setup artifacts with `code_commit=be51620`, `errors=[]`, `feature_count=61`, and `oof_predictions.paired_rows=630000` / `oof_coverage_frac=0.8219338930311748`.
- Setup metrics remained weak/noisy and research-only:
  - Long setup: `avg_precision=27.86%`, `brier=0.2459`, `ECE=0.1918`.
  - Short setup: `avg_precision=44.13%`, `brier=0.2462`, `ECE=0.1963`.
  - Cost/economics warning persisted: net entry reward/risk after configured costs is `1.07`, below `MIN_NET_R_MULTIPLE=1.20`.
  - Cost-aware labels are much more permissive than legacy TP-before-SL labels: long positive rate `31.14%` vs legacy `10.50%`; short positive rate `30.69%` vs legacy `11.61%`. Carry this as a label-noise/economics review item.
- Important run-hygiene note: `SETUP_OUT_DIR` was still exported from an earlier 10-day run, so full-window setup artifacts were written under `model_training_sets/setup_cost_aware_30s_20260615_192705` instead of under the new chain root. The manifest timestamps/input path prove these are the 2026-06-16 full-window setup artifacts, but the directory name is stale. Future runs should `unset SETUP_OUT_DIR` before launching the chain. The runner has been hardened after this review to fail fast if `RUN_SETUP_STAGE=1` and `SETUP_OUT_DIR` is outside `CHAIN_ROOT`, unless `ALLOW_EXTERNAL_SETUP_OUT_DIR=1` is explicitly set.
- Lifecycle/micro streamed staging completed across all five symbols and produced:
  - `long_lifecycle_rows=1470536` (`1000000` sampled for training cap)
  - `short_lifecycle_rows=1550534` (`1000000` sampled for training cap)
  - `long_micro_entry_rows=511676`
  - `short_micro_entry_rows=540149`
  - `long_micro_exit_rows=489096`
  - `short_micro_exit_rows=505704`
- Lifecycle/micro artifacts were written under `lifecycle_micro_full_window_cost_aware/`, with ONNX export disabled.
- `posthoc_promotion_gate_report.json` result: `promotion_ready=true`, `fail_count=0`, all six selected methods `isotonic`, all six routes passed the artifact gate:
  - `longExitLifecycleAi`: predicted positives `41339`, max day fraction `0.0612`, stable island points `16`.
  - `shortExitLifecycleAi`: predicted positives `37777`, max day fraction `0.0719`, stable island points `16`.
  - `longMicroEntryAi`: predicted positives `1945`, max day fraction `0.0586`, stable island points `16`.
  - `shortMicroEntryAi`: predicted positives `1312`, max day fraction `0.1037`, stable island points `16`.
  - `longMicroExitGuardAi`: predicted positives `8836`, max day fraction `0.0910`, stable island points `17`.
  - `shortMicroExitGuardAi`: predicted positives `8717`, max day fraction `0.0995`, stable island points `17`.
- Interpretation: this is the first full-window baseline chain to pass the post-hoc artifact gate, threshold-stability count gate, and day-dominance gate. It is **GO for deeper review and replay/backtest preparation**, but still **NO-GO for paper/live promotion** until runtime calibration application, recorded-event replay parity, full decision/PnL/day-dominance backtests, paper/shadow drift checks, and cost-aware label economics review are complete.

Action done on 2026-06-16 — setup precision-improvement experiments reviewed from the mounted external disk:

- External disk was mounted at `/Volumes/DatabentoVault`; free space was about `2.9TiB` available (`4.5TiB` size, `1.7TiB` used).
- Two early LightGBM directories were incomplete/aborted because LightGBM initially fell back to RandomForest before `libomp`/latest guard handling was fixed: `setup_cost_aware_30s_lightgbm_nonews_20260616_154515` and `setup_cost_aware_30s_lightgbm_nonews_20260616_155549`. Ignore these directories.
- Completed full-window setup experiment artifacts all had `errors=[]`, `paired_oof_rows=630000`, and `oof_coverage_frac=0.8219338930311748`.
- Base setup scorecard comparison:

| Setup run | Family | Features | Min expected net R label | Long avg precision | Short avg precision | Avg ECE | Notes |
|---|---:|---:|---:|---:|---:|---:|---|
| `setup_cost_aware_30s_20260615_192705` | RandomForest | 61 | `0.00` | `27.86%` | `44.13%` | `19.40%` | Original full-window run; news feature block enabled and setup output directory name was stale. |
| `setup_cost_aware_30s_nonews_20260616_141009` | RandomForest | 34 | `0.00` | `32.25%` | `44.20%` | `19.38%` | Removing zero-signal news features improved long precision without hurting short, but predictions stayed sparse/day-concentrated. |
| `setup_cost_aware_30s_lightgbm_nonews_20260616_163138` | LightGBM | 34 | `0.00` | `37.15%` | `36.70%` | `18.43%` | Best calibration and much more stable OOF coverage across days; `code_commit=7c5368a`. |
| `setup_cost_aware_30s_catboost_nonews_20260616_163956` | CatBoost | 34 | `0.00` | `37.69%` | `36.28%` | `18.70%` | Best balanced/global OOF precision; `code_commit=7c5368a`. |
| `setup_cost_aware_30s_minr_0.25_nonews_20260616_165025` | RandomForest | 34 | `0.25` | `31.16%` | `30.61%` | `26.35%` | Stricter label reduced prevalence/signals but worsened precision/ECE. Do not use as next candidate. |
| `setup_cost_aware_30s_minr_0.50_nonews_20260616_170027` | RandomForest | 34 | `0.50` | `25.79%` | `25.47%` | `29.49%` | Worse on precision/ECE. Do not use as next candidate. |

- OOF-global thresholded comparison gives a more stable view than unweighted fold-average scorecard precision:
  - CatBoost no-news: global OOF precision `37.71%` long / `36.18%` short, predicted positives `68575` long / `74550` short, worst-side max predicted-day fraction `3.04%`.
  - LightGBM no-news: global OOF precision `37.24%` long / `36.15%` short, predicted positives `57164` long / `63135` short, worst-side max predicted-day fraction `3.29%`.
  - RandomForest no-news: global OOF precision `35.38%` long / `37.91%` short, but only `13346` long / `14797` short predicted positives and worst-side max predicted-day fraction `12.00%`.
- Preferred next lifecycle/micro rerun setup input if running only one candidate: `setup_cost_aware_30s_catboost_nonews_20260616_163956/oof_setup_predictions.csv`, because it has the best balanced global OOF precision with low day concentration. LightGBM no-news is a near-tie and is the alternate if prioritizing slightly lower ECE over global OOF precision.
- These setup experiments are still research-only. Setup probabilities remain imperfectly calibrated and mean expected net R among thresholded predicted positives is still negative, so this is not a paper/live promotion signal by itself. Use the selected OOF file to test whether lifecycle/micro downstream gates improve, then evaluate replay/backtest PnL/day dominance before any promotion decision.

Action done on 2026-06-16 — lifecycle/micro rerun using the CatBoost no-news setup OOF:

- Chain root: `model_training_sets/broader_full_window_cost_aware_catboost_setup_20260616_200413`.
- Setup stage was intentionally skipped (`RUN_SETUP_STAGE=0` in the screen output), and lifecycle/micro consumed `setup_cost_aware_30s_catboost_nonews_20260616_163956/oof_setup_predictions.csv`.
- Staging passed with the same full-window coverage as the prior chain: `30s_rows=830700`, `30s_days=213`, `5s_rows=4984200`, `5s_days=213`, symbols `NVDA,QQQ,SPY,TQQQ,TSLA`.
- Selected setup manifest: `code_commit=7c5368a`, `feature_count=34`, `errors=[]`, `paired_oof_rows=630000`, `oof_coverage_frac=0.8219338930311748`.
- Lifecycle/micro output directory: `broader_full_window_cost_aware_catboost_setup_20260616_200413/lifecycle_micro_full_window_cost_aware`.
- Posthoc gate result remained `POSTHOC_PROMOTION_GATE=PASS` / `promotion_ready=true` / `fail_count=0` / `model_count=6`.
- Gate-row comparison versus the prior RandomForest-setup lifecycle run:

| Model | Old predicted positives | New predicted positives | Delta | Old max day frac | New max day frac | Stable island points | Gate |
|---|---:|---:|---:|---:|---:|---:|---|
| `longExitLifecycleAi` | `41339` | `41455` | `+116` | `0.061201` | `0.061271` | `16 → 16` | `PASS` |
| `shortExitLifecycleAi` | `37777` | `38198` | `+421` | `0.071896` | `0.072177` | `16 → 16` | `PASS` |
| `longMicroEntryAi` | `1945` | `1904` | `-41` | `0.058612` | `0.058824` | `16 → 16` | `PASS` |
| `shortMicroEntryAi` | `1312` | `1231` | `-81` | `0.103659` | `0.107230` | `16 → 16` | `PASS` |
| `longMicroExitGuardAi` | `8836` | `8836` | `0` | `0.090991` | `0.090991` | `17 → 17` | `PASS` |
| `shortMicroExitGuardAi` | `8717` | `8717` | `0` | `0.099461` | `0.099461` | `17 → 17` | `PASS` |

- Scorecard deltas were small. The biggest operational change was `shortMicroEntryAi`, where the selected raw threshold moved from `0.72` to `0.68`, precision stayed essentially flat (`90.30% → 90.33%`), and recall improved (`5.65% → 6.46%`). Exit guards were unchanged.
- Interpretation: the CatBoost no-news setup OOF is a healthier upstream setup signal and does not break the downstream lifecycle/micro posthoc artifact gate, but the downstream gate profile is broadly similar to the prior RandomForest-setup run. This is still **research/evaluation-only** and still **NO-GO for paper/live promotion** until runtime calibration, replay parity, full PnL/day-dominance backtests, paper/shadow drift checks, and label-economics review pass.
- Audit hardening after this review: `scripts/run_broader_full_window_cost_aware_chain_20260616.sh` now persists `SETUP_PREDICTIONS`, `RUN_STAGE_INPUTS`, `RUN_SETUP_STAGE`, `RUN_LIFECYCLE_STAGE`, `RUNNER_PREFLIGHT_ONLY`, and lifecycle cap settings into `chain_config.env` for future lifecycle-only reruns. The already-completed CatBoost chain predates this small config-recording hardening, so its setup-skipped proof comes from the screen/log plus the external `SETUP_OUT_DIR` and validated CatBoost OOF path.

Action plan on 2026-06-17 — precision improvement priority before further lifecycle runs:

- Pause additional setup-family/threshold tuning as the primary precision lever. RandomForest, LightGBM, CatBoost, stricter `COST_AWARE_MIN_NET_R_LABEL`, and meta-producer toggles have already shown that model-family changes alone are producing small or mixed setup precision gains.
- Return to the previously skipped richer-source Phase 1: implement silver normalizers/readers for `EQUS mbp-1`, `OPRA tcbbo`, and both definition sources (`EQUS definition`, `OPRA definition`) before making another full-window setup-quality claim.
- Treat the next enriched build as a controlled feature-source experiment, not a promotion run: same dates, symbols, labels, CatBoost/no-news baseline settings, and OOF/global/day-concentration review, changing only the added source-derived feature block.

Action done / decision on 2026-06-17:

- Decision: **GO for Phase 1 richer-source normalizer work before more lifecycle/micro reruns**. The current best setup precision remains around the high-30% range, so the next material precision lever is better market-state data rather than more learner tuning on the same baseline columns.
- Current baseline builder status: `build_30s_from_5s_csv.py` consumes only `EQUS tbbo` via `--dbeq-dir` and `OPRA ohlcv-1s` via `--opra-dir`. The 20260612 `EQUS definition`, `EQUS mbp-1`, `OPRA tcbbo`, and `OPRA definition` folders are still inventory/audit-only until new normalizers/readers are implemented.
- Expected feature lift from richer sources:
  - `EQUS mbp-1`: better quote-state coverage, spread/imbalance stability, quote-update intensity, quote age, locked/crossed flags, and liquidity-regime features than the current trade/top-of-book baseline.
  - `OPRA tcbbo`: option trade + quote context instead of only compact `ohlcv-1s` volume bars; enables option spread/liquidity, call/put pressure quality, quote/trade alignment, and premium-flow features.
  - `EQUS definition`: stable equity metadata/instrument identity checks; useful mainly for data validation/schema integrity on the current five-symbol equity pilot.
  - `OPRA definition`: critical option metadata for expiry, strike, right, multiplier, moneyness/tenor buckets, and filtering illiquid or malformed option rows.
- Guardrail fixed in code: `scripts/verify_databento_pilot_prebuild.py` now defaults to six expected sources and includes `equs_definition_20260612`; `tests/test_verify_databento_pilot_prebuild.py` has a regression test for the six-source default.
- Validation: `python3 -m py_compile scripts/verify_databento_pilot_prebuild.py tests/test_verify_databento_pilot_prebuild.py` and `python3 -m unittest discover -s tests -p 'test_verify_databento_pilot_prebuild.py' -v` passed on this computer.
- Current-machine note: `/Volumes/DatabentoVault` was not mounted during this 2026-06-17 control-plane inspection, so raw external-file existence was not reverified here. Run the six-source prebuild check on the 48GB/write-capable machine before any enriched build.

Action follow-up on 2026-06-17 — first 48GB six-source prebuild check failed correctly:

- The first rerun selected stale artifacts: `source_inventory_hashes_20260613_133951`, `pilot_dates_latest10_20260613_153639`, and `dbn_audit_summary_recent_old_20260613_150239`.
- Failure reason 1: the selected pilot manifest still had only five source files per date (`selected_file_count=50`, each date count `5`) and did not include the corrected `equs_definition_20260612` source. A true six-source 10-day pilot should select `60` files.
- Failure reason 2: the selected source rows pointed to old raw paths under `/Volumes/DatabentoVault/<raw-folder>/...`, but the 2026-06-16 storage-policy correction says the raw DBNs currently live in `/Users/filmonghezehey/Downloads`; generated outputs only should go under `$LAKE_ROOT`.
- Code hardening after this failure: `scripts/verify_databento_pilot_prebuild.py` supports `--path-prefix-map OLD_PREFIX=NEW_PREFIX` and records both `path` and `checked_path` in `prebuild_manifest_check_files.csv`. This is useful for validating legacy manifests against moved raw roots, but the preferred fix for Phase 1 is to regenerate source inventory and pilot-date manifests from the current `Downloads` raw root.
- Validation after this hardening: `python3 -m py_compile scripts/verify_databento_pilot_prebuild.py tests/test_verify_databento_pilot_prebuild.py` and `python3 -m unittest discover -s tests -p 'test_verify_databento_pilot_prebuild.py' -v` passed with `4` focused tests.

Action follow-up on 2026-06-17 — second 48GB prebuild verifier attempt failed because required shell variables were empty:

- Observed traceback ended with `FileNotFoundError: [Errno 2] No such file or directory: '/Users/filmonghezehey/trading-agent/worktrees/databento/manifest.json'`.
- Root cause: one or more of `HASH_DIR`, `AUDIT_SUMMARY_DIR`, or `PILOT_DIR` expanded to an empty string in the verifier command. Python `Path("").resolve()` resolved that empty path to the current worktree, so the verifier tried to read `manifest.json` from the repository root.
- Code hardening after this failure: `scripts/verify_databento_pilot_prebuild.py` now rejects empty path arguments with `path argument must not be empty; check the corresponding shell variable`, and it preflights required manifest/CSV inputs before reading them. Missing inputs now produce a structured `PREBUILD_CHECK=FAIL` report instead of a traceback.
- Validation after this hardening: `python3 -m py_compile scripts/verify_databento_pilot_prebuild.py tests/test_verify_databento_pilot_prebuild.py`, `python3 -m unittest discover -s tests -p 'test_verify_databento_pilot_prebuild.py' -v`, and `git diff --check` passed with `6` focused tests.

Action done on 2026-06-17 — six-source `Downloads`-root manifest/audit/prebuild validation passed on the 48GB Mac:

- Source inventory: `source_manifests/source_inventory_hashes_six_source_downloads_20260617_183630`.
  - Wrote `source_inventory.csv`, `source_files.csv`, `paired_dates.csv`, and `manifest.json`.
  - Warning was expected/informational: `1 dates are not present in every source` (the known unpaired `20260403` from `EQUS mbp-1`).
- Six-source pilot manifest: `source_manifests/pilot_dates_latest10_six_source_downloads_20260617_183703`.
  - Selected dates: `2026-05-11`, `2026-05-12`, `2026-05-13`, `2026-05-14`, `2026-05-15`, `2026-05-18`, `2026-05-19`, `2026-05-20`, `2026-05-21`, `2026-05-22`.
  - Selected files: `60` (`10` dates × `6` sources).
  - Total compressed size for the selected 10-day pilot: `4.508 GiB`.
- Representative DBN audits passed for both recent and older dates:
  - Recent audit dir: `raw_audits/dbn_day_audit_six_source_20260521_20260617_183703`.
  - Older audit dir: `raw_audits/dbn_day_audit_six_source_20250721_20260617_183753`.
  - Audit summary dir: `raw_audits/dbn_audit_summary_six_source_recent_old_20260617_183824`.
  - Summary: `error_count=0`, `warning_count=0`, `row_count=12`, `total_file_gib=0.769`, `total_dataframe_gib=4.304`, `max_dataframe_mib=1836.107`, `max_memory_expansion_ratio=10.613`, `max_row_count=18080585`.
  - Largest decoded source was `equs_mbp1_20260612` on `20260521`: `18,080,585` rows, `305.5 MiB` compressed, `1836.1 MiB` dataframe, memory expansion ratio `6.01`, decode time `15.93s`.
  - Largest definition decode was `opra_definition_20260612` on `20260521`: `244,570` rows, `11.7 MiB` compressed, `95.6 MiB` dataframe, decode time `21.66s`.
- Final prebuild verifier output: `raw_audits/prebuild_six_source_check_20260617_183825`.
  - `expected_file_count=60`.
  - `selected_file_count=60`.
  - Date counts were `6` for every selected date.
  - Source counts were `10` each for `equs_tbbo_20260523`, `opra_ohlcv1s_20260523`, `equs_definition_20260612`, `equs_mbp1_20260612`, `opra_tcbbo_20260612`, and `opra_definition_20260612`.
  - `errors=[]`, `warnings=[]`, `PREBUILD_CHECK=PASS`.
- Stop/go decision: **GO for Phase 1 normalizer implementation**. The six-source pilot inputs are now manifest-clean and decode-clean for representative dates. Next work should implement silver normalizers/readers for `EQUS mbp-1`, `OPRA tcbbo`, and the definition feeds, then build a 10-day enriched pilot before any full-window enriched training.

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
export RAW_EQUS_DEFINITION=/path/to/EQUS-20260612-GFHRSU6F48
export RAW_EQUS_MBP1=/path/to/EQUS-20260612-36BEU4G7M8
export RAW_OPRA_TCBBO=/path/to/OPRA-20260612-KN5TPHB5EF
export RAW_OPRA_DEFINITION=/path/to/OPRA-20260612-B5D4JV3GV6
export OUT_ROOT=/path/to/writeable/training_data/dynamic_pilot_20260613
export DOWNLOAD_ROOT=/Users/filmonghezehey/Downloads

mkdir -p "$OUT_ROOT"/manifests "$OUT_ROOT"/raw_audits "$OUT_ROOT"/pilot_10d "$OUT_ROOT"/logs

export HASH_RUN_ID="source_inventory_hashes_$(date +%Y%m%d_%H%M%S)"

python3 scripts/audit_databento_pilot_sources.py \
  --vault-root "$DOWNLOAD_ROOT" \
  --output-dir "$OUT_ROOT/manifests/$HASH_RUN_ID" \
  --include-hashes \
  --exclude-source-label opra_definition_20260612_duplicate

# If hashing reports an I/O error, pull the version containing resilient hash
# recording and inspect manifest.json/source_files.csv for the exact file path.
# The duplicate OPRA definition folder is excluded by default for Phase 1.

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
