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
- The 10-day pilot build itself has not started yet. Correctly paused after manifest selection and before normalization/training.

### Phase C — Training and promotion gates on the 48GB machine

1. Use the fixed builders only; old `20260523` staged datasets are pre-fix artifacts.
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
