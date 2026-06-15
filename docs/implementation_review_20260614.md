# Implementation Review: AI Training Dynamic Upgrade Branch

Date: 2026-06-14  
Branch: `ai-training-dynamic-upgrade-20260612`  
Reviewed against: `docs/ai_training_dynamic_upgrade_plan_20260612.md` and `docs/computer_capability_task_organization_20260613.md`

---

## Overall verdict

The branch is executing correctly against its stated scope. Every item recorded as "Action done" in the step ledger is verified in code and tests. The infrastructure foundation is complete. No promotable bundle exists yet, which is the correct outcome for this stage.

The upgrade is now at a natural handoff boundary: Phase A (this read-only machine) work is finished through Step 11; Phase B (48GB machine) has delivered a fixed-quality 10-day baseline with quality-sanity pass. The next two blocking items — post-hoc calibration and silver-layer normalization of the new feeds — must both proceed on the 48GB machine before the experiment matrix can begin.

---

## What is verified complete

### Phase 0 critical blockers

| Blocker | Status | Evidence |
|---|---|---|
| C1: `bfill()` leakage removed | **DONE** | `grep -n "bfill" build_30s_from_5s_csv.py` returns zero matches. Six `ffill()` calls remain, all correct forward-only direction. `tests/test_build_30s_from_5s_csv_regularization.py` has a regression test for pre-first-quote non-inheritance. |
| C2: parent quality union replaced | **DONE** | `ChildDataQualityFlagUnion` preserved for audit. 13 aggregate quality fields added (`TradeCoverage`, `QuoteStateCoverage`, `QuoteUpdateCoverage`, `SyntheticCoverage`, `QuoteAgeMsMean`, `QuoteAgeMsMax`, `ValidSpreadCoverage`, `LockedCrossedSeconds`, `QualityScore`, and related). Pilot build confirmed `parent_child_flag_different_rows == row_count` for every 5s/30s file. |
| C3/C4: bootstrap proxy fail-fast and OOF prediction generator | **DONE** | `generate_walk_forward_setup_predictions.py` (347 lines) emits rolling prior-day OOF setup probabilities per 30s bar. `train_lifecycle_micro_models.py` now requires `--setup-predictions-csv` by default and fails if setup probabilities are missing, constant, or bootstrap value `1.0`. `--allow-bootstrap-setup-proxy` override exists for explicit research-only smoke runs. |

### Source audit and pilot planning infrastructure

All seven audit/verify scripts are tested and pass:

- `scripts/audit_databento_pilot_sources.py` — SHA-256 hashes, pairing, duplicate detection
- `scripts/audit_databento_dbn_day.py` — per-day, per-schema schema/row/column summary
- `scripts/summarize_databento_dbn_audits.py` — combined multi-day audit summary
- `scripts/plan_databento_pilot_dates.py` — paired-date manifest with compressed-size estimate
- `scripts/verify_databento_pilot_prebuild.py` — pre-build source manifest/hash gate
- `scripts/verify_databento_pilot_build.py` — post-build row-count/quality gate
- `scripts/summarize_databento_pilot_quality.py` — parent/child flag decoupling verification

The `20260403` unpaired-date hazard (present only in `EQUS mbp-1`, absent from OPRA sources) is explicitly handled in `plan_databento_pilot_dates.py` and will be excluded from every pilot manifest automatically.

### 10-day fixed-quality baseline (48GB machine)

- `model_training_sets/pilot_10d_fixed_quality_20260613_173446` built from `tbbo` + `ohlcv-1s` sources for TSLA, TQQQ, NVDA, SPY, QQQ.
- `PILOT_BUILD_CHECK=PASS`: `errors=[]`, `warnings=[]`, 18 files, expected row counts at 1s/5s/30s, `total_output_gib=1.354`.
- `PILOT_QUALITY_SANITY=PASS`: `errors=[]`, `parent_child_decoupling_evidence_count=12`, `min_parent_child_diff_frac_by_cadence={1s:0.0, 5s:1.0, 30s:1.0}`.

This is a C1/C2-fixed baseline dataset, not a model-promotion dataset. It is the correct first artifact.

### Walk-forward setup prediction smoke (48GB machine)

- `model_training_sets/setup_oof_fixed_quality_20260614_003310/oof_setup_predictions.csv`: `errors=[]`, `row_count=35685`, `trainable_oof_rows=28780`, `trainable_oof_frac=0.8065`, `folds_ok=8` for both long and short.
- Durable lifecycle/micro smoke `model_training_sets/lifecycle_micro_fixed_quality_setup_oof_20260614_004734`: retained 28780 joined rows, dropped 10220 intentionally unscored early rows, trained all six routes.

### Step 10: live/replay sanity parity (runtime contract)

- `scripts/databento_event_contract.py` (220 lines) defines `databento_ndjson_v2` schema. Both live normalizer and historical streamer now emit `EventSchemaVersion`, `DataQualityFlags`, `ChildDataQualityFlagUnion`, all 13 aggregate quality fields, timestamp provenance (`tsEventNs`, `BarEpochSec`, `EventGeneratedAtMs`), and feed metadata (`Dataset`, `Schema`, `STypeIn`).
- `DatabentoEvent.java` extended: `effectiveQualityScore()`, `blocksNewEntries(minQualityScore, expectedVersion)` implemented. Java aliases accept both camelCase and snake_case field names.
- `DatabentoFeedHealth.SymbolSnapshot` tracks `lowQualityBarCount`, `entryRejectedBarCount`, `lastQualityScore`, `lastDataQualityFlags`, `lastEventSchemaVersion`.
- `IBKRTrader.java` calls `event.blocksNewEntries(minQualityScore, databentoExpectedEventSchemaVersion)` and increments `lowQualityBarCount`/`entryRejectedBarCount` through feed health.
- `DatabentoHistoricalStreamingBacktester.java` extended with quality/provenance parsing.
- Historical TBBO quote-only rows (no trade price/size) are now treated as low-quality synthetic-no-trade bars rather than false trade events.
- 71 Java tests, 37 Python tests, all passing.

### Step 11: calibration metrics infrastructure

- `calibration_report(y_true, probas, bins=10)` in `train_lifecycle_micro_models.py` computes Brier score, ECE, and reliability bins.
- Every trained lifecycle/micro classifier emits `brier_score`, `ece`, `calibration_rows` in `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`, `calibration_manifest.json`, and `calibration_reliability.csv`.
- The `calibration_manifest.json` explicitly records method as `raw_random_forest_probability_no_posthoc_calibrator` and includes a research-only warning.
- Reliability-bin inspection on the 10-day baseline found: ECE range 0.028–0.079, Brier range 0.083–0.127, threshold-bin absolute calibration error up to 0.407, worst-bin absolute error up to 0.610. **Raw RandomForest probabilities are confirmed not suitable as promotion evidence.** Decision recorded: post-hoc calibration is required before threshold-stability work.

### Pilot configuration

- `config/databento_dynamic_upgrade_pilot_symbols.csv`: TSLA, TQQQ, NVDA (train targets), SPY, QQQ (context).
- `runtime/pilot_core_5_symbols.txt`: TSLA, TQQQ, NVDA, SPY, QQQ.
- `runtime/symbols_100.txt` exists for broader universe jobs.

### Feature schema hash parity

- `feature_schema_hash()` in `train_lifecycle_micro_models.py` and `featureSchemaHash()` in `PingPongStrategy.java` use the same SHA-256-of-sorted-column-names algorithm. `lifecycle_micro_route_manifest.json` records `feature_schema_sha256`. `PingPongStrategy.java` validates the hash at bundle load time.

---

## What is not yet started

### Phase 1: canonical data lake (next major buildout — 48GB machine)

The `data_lake_v2` bronze/silver/gold structure does not exist. The external vault is not writable from this machine, so this is correctly deferred to the 48GB machine. However, **none of the following normalizer code has been built**:

1. **EQUS `mbp-1` normalizer**: no Python code to decode `EQUS-20260612-36BEU4G7M8` DBN files → silver Parquet. The 58.87 GiB of downloaded `mbp-1` data cannot be used until this exists.
2. **OPRA `tcbbo` normalizer**: no Python code to decode `OPRA-20260612-KN5TPHB5EF` DBN files → silver Parquet. The 15.08 GiB is idle.
3. **OPRA `definition` reader**: no Python code to parse downloaded definition files for reliable option metadata (expiry, strike, call/put, multiplier). The two definition downloads (`OPRA-20260612-B5D4JV3GV6` and its apparent duplicate) are untouched.
4. **EQUS `definition` reader**: not built.
5. **`data_lake_v2` partition builder**: no script creates `bronze_raw_downloads/`, `silver_normalized/`, `gold_state/`, `labels/`, or `model_training_sets/` Parquet outputs under the lake root.
6. **Parquet output throughout**: the current build pipeline produces CSV outputs under `model_training_sets/`. No Parquet/Arrow outputs exist anywhere except inline pandas operations.

The consequence: the first 10-day pilot still runs only on the `tbbo` + `ohlcv-1s` sources. The new feeds (58.87 GiB `mbp-1`, 15.08 GiB `tcbbo`) contribute zero features to any trained model so far.

**Immediate next work required on the 48GB machine:**
- Build `normalize_equs_mbp1.py` or equivalent for at minimum a 10-day pilot slice.
- Build `normalize_opra_tcbbo.py` or equivalent for the same 10-day window.
- Verify schema, symbol mapping, decoded row counts, and join keys before bulk build.

### Phase 3: new feature producers (blocked on Phase 1)

No feature producers exist for any of the new feeds. Specifically absent:

- **EQUS `mbp-1` features**: `l1_imbalance` from continuous quote updates, `microprice`, `spread_bps_continuous`, `quote_update_count_5s/30s`, `spread_widen_count_30s`, `quote_staleness_ms`, `locked_crossed_quote_ratio_30s`. The `EXTENDED_FEATURE_COLS` in `generate_walk_forward_setup_predictions.py` includes `f_l1_imbalance` and `f_signed_flow_30s` as column names only — they are placeholders; no `mbp-1`-based values flow through.
- **OPRA `tcbbo`/`definition` features**: no DTE bucket features, no moneyness bucket features, no near-ATM call/put imbalance, no option spread bps, no trade-at-bid/ask/mid option features.
- **Cross-symbol context block**: `SPY`/`QQQ` currently appear in the training data only because they are in the same combined CSV. There is no dedicated cross-symbol feature extractor for SPY/QQQ context driving TSLA/TQQQ/NVDA model inputs.
- **Event bars / volume bars / dollar bars / trade-count bars**: not built.
- **Sequence model meta features**: TCN/GRU/LSTM/compact Transformer meta producers not built.
- **Feature ablation ledger**: no framework (`feature_ablation_result.json` or equivalent) exists. No experiment can currently be gated by ablation result.

The additive experiment matrix (`baseline_current_v1` → `quality_fixed_v2` → `equs_quote_v2` → `opra_tcbbo_v2` → `event_pressure_v2` → `context_v2` → `sequence_meta_v2` → `full_pilot_v2`) cannot begin until Phase 1 normalizers produce features.

### Phase 4: cost-aware labels (not built)

Current labels remain binary `tp_before_sl` style with `ENTRY_SLIPPAGE_BPS` and `EXIT_SLIPPAGE_BPS` constants (both default `2.0 bps`) baked into `generate_labels()` in `train_30s_models.py`. The following required by the plan do not exist:

- `expected_net_r_after_costs` label with entry spread, exit spread, slippage, partial-fill/missed-fill, and latency assumptions.
- `future_max_r`, `future_min_r`, `max_favorable_excursion_r`, `max_adverse_excursion_r` labels.
- `exit_hazard_next_5s`, `exit_hazard_next_30s`, `hold_value_next_30s` labels.
- `time_to_target_seconds`, `time_to_stop_seconds` labels.
- **Label manifest**: no manifest records the entry/exit cost assumptions, slippage model, partial-fill penalty, or latency assumption used. Any feature-block experiment evaluated against the current labels cannot be claimed as cost-aware.

**This is a phase-ordering dependency.** The plan requires a minimum cost-aware label to exist in parallel with Phase 1. Any experiment block (`equs_quote_v2`, `opra_tcbbo_v2`, `event_pressure_v2`) evaluated against the current binary labels is research-only per the plan's own rules.

### Phase 5: post-hoc calibration (explicitly the next blocker — Step 12)

The plan and step ledger both record this as the next unstarted blocker. The following do not exist:

- `CalibratedClassifierCV` with isotonic or sigmoid method applied to any model.
- Frozen holdout procedure: the current held-out split is the last chronological portion of the 10-day slice, but it is not locked or frozen. No code prevents the holdout from being reused across calibration tuning runs.
- **Threshold stability check**: no code computes or tests whether a threshold is stable across walk-forward folds (required "stable islands" gate). The plan's `threshold_grid.csv` per experiment does not exist.
- **Holdout trade-count gate**: no minimum trade count is enforced before a scorecard can be discussed as promotion evidence.
- **Day-dominance check**: no check prevents a single day from accounting for a disproportionate share of profitability.

These five items are all prerequisites for any bundle to exit research-only status.

### Phase 6: promotion gates (not started)

- **Recorded-event replay parity artifact**: the infrastructure is wired (databento_ndjson_v2 contract, Java consumers parse quality/provenance fields, DatabentoHistoricalStreamingBacktester extended). However, no actual market-hours NDJSON sample has been captured and replayed. No machine-readable parity JSON (`errors=[]`, feature-vector parity, bucket-boundary parity, score parity within tolerance) exists. This requires market hours to capture a real live-shaped stream.
- **Paper/shadow mode**: not implemented.
- **Feature drift monitoring**: not implemented.
- **Automated promotion gate check script**: no `run_promotion_gate_checks.py` or equivalent exists.

### Cross-cutting gaps not addressed anywhere in the codebase

1. **Dataset/join/label manifest infrastructure**: completely absent from code. No `dataset_manifest.json`, `join_manifest.json`, or `label_manifest.json` format or writer exists. No as-of join helper enforces `max_forward_tolerance=0`, records unmatched rates, or asserts zero future-row violations. The plan explicitly acknowledges this: *"Code inspection found no complete dataset/join/label manifest infrastructure yet."*

2. **OPRA/EQUS cross-feed lag-sensitivity tests**: not implemented. The plan requires every timing-sensitive feature block to be tested at OPRA as-of + no extra lag, +1s lag, +2s lag, and EQUS quote state at no extra lag and +1s lag. No code or test harness for this exists.

3. **`rename threshold selection → tune_threshold`**: the plan notes that existing code/comments incorrectly call threshold tuning "calibration." This terminology collision has not been corrected in `train_30s_models.py` or `train_lifecycle_micro_models.py`.

---

## Quality of what has been implemented

### Strengths

- **C1/C2 fixes are clean and well-tested**. The aggregate quality field logic is principled, the `QuoteUpdateCoverage` vs `QuoteStateCoverage` distinction is correctly implemented, and the bfill removal is complete. The 5s/30s `parent_child_flag_different_rows == row_count` result on the pilot build is the expected evidence.
- **Walk-forward OOF prediction generator is architecturally correct**. Rolling prior-day folds with a chronological holdout, bootstrap fail-fast, and manifest output are all right. The 0.8065 OOF coverage fraction is acceptable given the early-session warm-up window.
- **The event contract version stamp (`databento_ndjson_v2`) is a good discipline**. `blocksNewEntries()` in `DatabentoEvent.java` correctly checks both quality score and schema version mismatch. The historical streamer trade/quote distinction fix (quote-only rows → synthetic no-trade bars) closes a real data-integrity gap.
- **Calibration measurement before calibration fitting is the right sequence**. Measuring Brier/ECE on raw RandomForest probabilities first, confirming the raw probabilities are not promotion-quality (threshold-bin ACE up to 0.407), and then explicitly deciding to add a post-hoc calibrator is correct scientific hygiene.
- **Test coverage is proportional to risk**. The most critical correctness properties (no bfill, parent/child flag decoupling, bootstrap fail-fast, event schema version) each have dedicated regression tests.

### Weaknesses and gaps in current implementation quality

1. **`DatabentoFeedHealth.SymbolSnapshot` is missing the parent/child flag diff fraction counter**. The plan's Step 10 requires per-symbol `parent_child_flag_diff_fraction` and explicit `no_trade_count`/`no_quote_count` counters in the live sanity layer. The current `SymbolSnapshot` only has `lowQualityBarCount` and `entryRejectedBarCount`, which are entry-decision counters, not raw quality-state counters. Replaying a session and checking these counters is currently not possible.

2. **`PingPongStrategy.java` does not call `event.blocksNewEntries()` at the strategy level**. The quality gate is enforced in `IBKRTrader.java` before the event is forwarded to the strategy. However, `PingPongStrategy` itself does not check the quality flag when it evaluates whether to arm or enter. If a low-quality bar passes through the IBKR trader route (for example, during a route change, test, or when `databentoMinQualityScore` is `0.0`), the strategy has no secondary defense.

3. **The `calibration_report()` 10-bin design is fixed**. Reliability bins are always fixed at 10 equal-width probability bins regardless of probability distribution. With 807–4808 held-out rows and binary events, several bins will be empty or have 1–3 samples, giving noisy ACE estimates. A variable-binning (equal-frequency) approach or reliability diagram with explicit empty-bin warnings would be more informative. The threshold-bin sparse-count problem (1–14 rows per bin) documented in the step ledger directly results from this.

4. **`generate_walk_forward_setup_predictions.py` uses a simplified 30s feature set**. The OOF predictions currently use the base 30s features from the combined CSV. `EXTENDED_FEATURE_COLS` lists `f_l1_imbalance` and `f_signed_flow_30s` as optional but they fall back silently to 0.0 when absent. When the mbp-1 feature block is added, the OOF generator must be re-run to incorporate those features; there is no version stamp or feature-set hash in the OOF manifest to flag this dependency.

5. **The historical streamer `emit_equity_bar()` and `emit_options_bar()` now emit quality fields**, but the `databento_historical_streamer.py` `--source api` dry-run path (the only path testable without the external vault) does not exercise the actual TBBO aggregation logic where the quote-only row distinction matters. The trade/quote distinction fix is correct in principle but has minimal test coverage against real-shaped input.

6. **No test covers the `blocksNewEntries()` integration path in `IBKRTrader`**. `DatabentoEventTest.java` tests `blocksNewEntries()` in isolation on a `DatabentoEvent` object. But no test constructs a low-quality event and verifies that the full IBKR trader route rejects it, increments `entryRejectedBarCount`, and does not forward it to the strategy. The integration path between quality gate and strategy arming is untested.

---

## Decision table for what to do next

| Priority | Task | Phase | Machine | Blocker for |
|---|---|---|---|---|
| 1 | Post-hoc calibration (isotonic/Platt) on larger/frozen holdout | Phase 5 | 48GB | Threshold stability, paper/live promotion |
| 2 | Build EQUS `mbp-1` silver normalizer for 10-day pilot | Phase 1 | 48GB | Phase 3 feature blocks |
| 3 | Build OPRA `tcbbo` + `definition` silver normalizer for 10-day pilot | Phase 1 | 48GB | Phase 3 feature blocks |
| 4 | Build minimum cost-aware label (`expected_net_r_after_costs` with label manifest) | Phase 4 | 48GB | Feature block experiments |
| 5 | Recorded-event replay parity artifact (market hours capture + replay + parity report) | Phase 6 | 48GB + live | Paper/live promotion |
| 6 | Add `parent_child_flag_diff_fraction` counter to `DatabentoFeedHealth.SymbolSnapshot` | Step 10 gap | This machine | Live quality observability |
| 7 | Add secondary quality check in `PingPongStrategy.java` for low-quality bar arming guard | Step 10 gap | This machine | Defense in depth |
| 8 | Add as-of join helper with future-row assertion and manifest output | Cross-cutting | This machine | Phase 3 feature builds |
| 9 | Fix terminology: rename threshold selection comments from "calibration" to "tune_threshold" | Cross-cutting | This machine | Clarity |
| 10 | OOF prediction manifest: add feature-set hash or version stamp | C3/C4 hardening | This machine | Phase 3 OOF re-run safety |

---

## Summary scorecard by plan section

| Plan section | Status | Notes |
|---|---|---|
| Phase 0: C1 bfill leakage | ✅ Complete | No `bfill()` calls remain; regression tests pass |
| Phase 0: C2 quality union | ✅ Complete | Parent/child decoupled; pilot baseline verified |
| Phase 0: C3/C4 walk-forward setup | ✅ Complete | OOF generator + lifecycle fail-fast + 10-day smoke |
| Phase 0: source audit infrastructure | ✅ Complete | All 7 scripts tested; 48GB manifest passes |
| Phase 0: audit report (old vs new quality) | ✅ Complete | 48GB quality-sanity pass with decoupling evidence |
| Step 10: live/replay parity contract | ✅ Wired | Needs market-hours replay artifact to close |
| Step 11: calibration measurement | ✅ Measured | Confirmed raw RF probabilities inadequate; post-hoc needed |
| Phase 1: data_lake_v2 structure | ❌ Not started | Blocked on 48GB write; no normalizer code exists |
| Phase 1: mbp-1 normalizer | ❌ Not started | 58.87 GiB idle |
| Phase 1: tcbbo + definition normalizer | ❌ Not started | 15.08 GiB idle |
| Phase 2: full-window walk-forward | ⏳ Infrastructure only | 10-day smoke done; full window not run |
| Phase 3: new feature producers | ❌ Not started | Blocked on Phase 1 |
| Phase 3: feature ablation framework | ❌ Not started | No ablation ledger code |
| Phase 4: cost-aware labels + manifest | ❌ Not started | Current labels are cost-naive binary |
| Phase 5: post-hoc calibration | ❌ Not started | Explicitly the next Step 12 blocker |
| Phase 5: threshold stability gates | ❌ Not started | |
| Phase 5: holdout trade count + day dominance | ❌ Not started | |
| Phase 6: replay parity artifact | ⏳ Half wired | Contract exists; no captured + replayed session |
| Phase 6: paper/shadow mode | ❌ Not started | |
| Cross-cutting: dataset/join/label manifests | ❌ Not started | Explicitly acknowledged in plan |
| Cross-cutting: OPRA/EQUS lag-sensitivity tests | ❌ Not started | |
| Cross-cutting: feature ablation gate | ❌ Not started | |

---

## Conclusion

The branch is at the correct place for its stage. All Phase A (read-only machine) code work and Phase B (48GB machine) data foundation work through Step 11 are complete and verified. No shortcuts have been taken on the critical blockers.

The path to the first paper/live-candidate bundle now requires two parallel tracks:

**Track 1 (48GB machine, artifact-producing):** post-hoc calibration step → mbp-1/tcbbo silver normalizers → 10-day feature-enriched build → minimum cost-aware label → additive experiment matrix → threshold stability + trade-count + day-dominance gates.

**Track 2 (this machine, code):** as-of join helper with manifest output → `DatabentoFeedHealth` parent/child counter addition → `PingPongStrategy` secondary quality guard → OOF manifest feature-set versioning → terminology fix.

The recorded-event replay parity artifact (market-hours capture + replay) must be completed before any bundle is discussed as paper/live, but it depends on a live market session and cannot be done before the other blockers are resolved.

