# Peer Review: AI Training Dynamic Upgrade Plan

**Review date:** 2026-06-13
**Document reviewed:** `docs/ai_training_dynamic_upgrade_plan_20260612.md`
**Branch:** `ai-training-dynamic-upgrade-20260612`
**Reviewer role:** senior ML systems / trading infrastructure

---

## Summary verdict

The plan is directionally correct and well-structured. The pilot-first approach, the bronze/silver/gold lake layout, the explicit leakage-discipline requirements, and the decision to keep the existing hierarchical lifecycle/micro runtime architecture are all sound engineering choices.

However, several issues found during code inspection are either more severe than the plan acknowledges or are concrete blockers that must be resolved before any meaningful retraining can be claimed to produce promotable results. These are itemized below as critical, significant, and minor findings.

Operational update: the pilot downloads are now visible on `/Volumes/DatabentoVault`, but this 16GB computer has the external vault mounted read-only. The machine-capability split and verified data inventory are captured in `docs/computer_capability_task_organization_20260613.md`. Treat this computer as a read-only external-data inspector / code-and-plan controller; run large artifact builds, normalization, training, calibration, and backtests on the 48GB write-capable computer.

Verified downloaded pilot sources on the read-only vault:

- `EQUS.MINI` / `mbp-1`: `/Volumes/DatabentoVault/EQUS-20260612-36BEU4G7M8`, about `58.87GiB`, `214` DBN dates, symbols `TSLA,TQQQ,NVDA,SPY,QQQ`.
- `OPRA.PILLAR` / `tcbbo`: `/Volumes/DatabentoVault/OPRA-20260612-KN5TPHB5EF`, about `15.08GiB`, `213` DBN dates, symbols `TSLA.OPT,TQQQ.OPT,NVDA.OPT,SPY.OPT,QQQ.OPT`.
- `OPRA.PILLAR` / `definition`: `/Volumes/DatabentoVault/OPRA-20260612-B5D4JV3GV6`, about `2.15GiB`, `213` DBN dates; `/Volumes/DatabentoVault/OPRA-20260612-B5D4JV3GV6 2` appears to be a duplicate and should not be processed twice without a hash check.
- Existing `EQUS.MINI` / `tbbo` and `OPRA.PILLAR` / `ohlcv-1s` full-window sources remain available from the `20260523` folders.

Important paired-date note: the new `EQUS mbp-1` folder includes `20260403`, while paired OPRA folders do not. The 48GB build must skip or explicitly mark this unpaired date in the source/join manifest rather than silently inner/outer joining it.

---

## Critical findings (must fix before retraining)

### C1. `bfill()` leakage is present in `build_30s_from_5s_csv.py` and is worse than the plan describes

The plan mentions "avoid future `bfill()`" as a leakage guard recommendation. Code inspection shows the issue already exists in the current build pipeline:

```
build_30s_from_5s_csv.py:846  out[book_cols] = out[book_cols].ffill().bfill()
build_30s_from_5s_csv.py:853  base_close = base_close.ffill().bfill()
build_30s_from_5s_csv.py:862  out[col] = out[col].ffill().bfill()
```

These `bfill()` calls operate on a per-session sequence of 1-second rows sorted by time. Within that sequence, `bfill()` propagates values from future seconds backward. This means pre-first-quote seconds and pre-first-trade seconds inherit quote/book/close data from the first real observation that comes later in the session.

The consequence is that training rows generated from those early-session 1-second bars carry feature state (bid, ask, mid, spread, imbalance, base close) that would not have been available to a live decision maker at that instant. Any model that uses those early-session rows as training signal is partially trained on future information.

**Action required:** Replace `ffill().bfill()` on book and price columns with `ffill()` only, and emit explicit low-quality or invalid flags (as proposed in the plan) for any row where the leading null could not be resolved by forward-fill from a previous known state. The `bfill()` fallback must be removed before retraining, not documented as a future recommendation.

### C2. `DataQualityFlags` union bug is confirmed in code and the existing staged datasets are already affected

The `_quality_flag_union()` function at `build_30s_from_5s_csv.py:289` is still the parent aggregation function for `DataQualityFlags` (line 537). The fix described in the plan (aggregate quality scores instead of child-flag union) is correct and necessary. The plan's Phase 0 description is accurate.

However, the plan treats this as a standalone "Phase 0 reliability fix" and lists it first in the phased roadmap. It must also be tracked as a **blocker on all existing staged datasets**, including the `compare_runs_20260523_meta_ab` and `databento_training_runs_20260523` datasets referenced later in the document. Any model scorecard that was produced using those staged datasets has potentially corrupt quality-gating. The plan should explicitly mark those scorecards as pre-fix artifacts, not as baselines to beat.

### C3. Bootstrap proxy scores are not just a "critical caveat" — they are a disqualification from promotion

The plan says the bootstrap proxy "should not be promoted as live-quality signals." The code comment at line 454 says "BOOTSTRAP PLACEHOLDER — replace with walk-forward 30s model score before promotion."

The code inspection confirms:

```python
# train_lifecycle_micro_models.py:52
ENTRY_SCORE_PROXY_BOOTSTRAP = 1.0  # Remove once walk-forward 30s setup scoring is active.

# lines 454, 555 — used in every row of build_lifecycle_rows and build_micro_rows
setup_prob = None

def setup_score_proxy(value):
	return ENTRY_SCORE_PROXY_BOOTSTRAP if value is None else value

example_row = {
	"f_entry_score_proxy": setup_score_proxy(setup_prob),
	"f_setup_score_proxy": setup_score_proxy(setup_prob),
}
```

When `setup_prob is None` (which it is for all rows until walk-forward setup predictions exist), every lifecycle/micro training row has `f_entry_score_proxy = 1.0` and `f_setup_score_proxy = 1.0`. This means the two most important position-context features are degenerate constants. The feature effectively does not exist in training. Any model that lists these features and any threshold or scorecard derived from them must be treated as having been trained without real setup context.

**The current `lifecycle_micro_20260523` bundle is an integration artifact.** The plan's phrasing ("bootstrap artifacts, not live-promotable AI artifacts") is accurate but understated. The scorecard shows `longMicroEntryAi` at `precision=1.0, recall=0.005264, pred_pos_rate=0.000555`. A model that signals on 0.05% of candidate bars with perfect precision but near-zero recall and constant proxy inputs is almost certainly memorizing a very small set of rows rather than learning generalizable setup-quality signals. This is consistent with the bootstrap constant dominating the feature space.

### C4. Walk-forward in `train_30s_models.py` does not save out-of-fold predictions

Phase 2 of the plan says "Generate walk-forward 30-second setup predictions." The `perform_walk_forward_testing()` function in `train_30s_models.py` runs folds and prints average precision and threshold, but it does **not** write out-of-fold probability predictions to a file that could be joined back to the training data and passed to lifecycle/micro training as real setup probabilities.

This means Phase 2 requires a concrete new engineering deliverable: a `generate_walk_forward_setup_predictions.py` script (or equivalent) that:
- Runs time-ordered folds on the 30s training data.
- For each test fold, writes the row index (symbol + timestamp), the predicted probability, and the fold threshold to a predictions CSV.
- Produces one row per training bar, covering the full date range using a rolling out-of-fold scheme.
- Fails loudly if any date range in the 30s training data lacks a prediction.

The lifecycle/micro trainer then replaces the `setup_score_proxy(None)` fallback with a join to this predictions file, and the training must fail if the join produces rows with null setup probability and the research-only override is not set.

The plan describes the desired outcome but does not describe the mechanism. Without this mechanism, Phase 2 cannot be completed, and Phase 3 through 6 all depend on it.

---

## Significant findings (must address in this branch)

### S1. No probability calibration infrastructure exists anywhere in the current codebase

The plan correctly calls for Brier score, ECE, isotonic/Platt calibration per model family, and calibration manifests. Code inspection of both `train_lifecycle_micro_models.py` and `train_30s_models.py` confirms no calibration code exists at all — no `CalibratedClassifierCV`, no Brier score computation, no ECE computation, no calibration curves.

The `perform_walk_forward_testing()` function in `train_30s_models.py` does threshold optimization but treats the output as a ranking score (optimizing precision), not a calibrated probability. The lifecycle trainer's scorecard also lacks any calibration fields.

The plan places calibration in Phase 5, which is reasonable sequentially, but Phase 5 should be considered a hard precondition for any paper/live promotion even in paper mode. The promotion gate checklist at the end of the document lists calibration curves and Brier/ECE. That gate should also close any "promising scorecard" claims made from Phases 1-4 until calibration is added.

### S2. No dataset manifest or join manifest infrastructure exists

The plan's "Leakage enforcement must be code-level, not a guideline" section correctly identifies this as non-negotiable. Code inspection confirms:

- Neither `train_lifecycle_micro_models.py` nor `train_30s_models.py` contains any manifest tracking.
- No `dataset_manifest.json` or `label_manifest.json` is produced during training.
- No join manifest with timestamp columns, lag assumptions, forward-tolerance, or future-row assertion counts exists in any script.
- The only existing artifact that approaches a manifest is `feature_schema.json` and `feature_schema.sha256` in the model export directory.

The immediate engineering task list (item 10) identifies "add an as-of join helper." This helper must be built and integrated into the Phase 0/1 pipeline builds before the Phase 2 retraining produces trustworthy artifacts. Specifically, the EQUS/OPRA join in the silver-to-gold step must emit a manifest entry for every cross-feed join type before that feature block is considered promotable.

### S3. The micro-entry recall crisis indicates a deeper problem than threshold tuning

The `lifecycle_micro_20260523` scorecard:
```
longMicroEntryAi:  threshold=0.74, precision=1.0, recall=0.00526, pred_pos_rate=0.000555
shortMicroEntryAi: threshold=0.74, precision=1.0, recall=0.00514, pred_pos_rate=0.000495
```

This is not simply "recall is low and threshold stability matters." A micro-entry model that signals on approximately 1 in 2000 candidate bars while showing perfect precision on the test set is almost certainly:
1. Memorizing a specific narrow pattern (possibly related to the degenerate bootstrap proxy constant).
2. Being evaluated on an in-distribution test set that includes the same degraded quality days.
3. Producing a threshold that is so tight it would generate near-zero live trades.

The plan acknowledges this implicitly ("threshold calibration and trade-count stability matter more than raw precision") but should state explicitly: **the current micro-entry models are not viable as live signals in any form, including paper mode**. No calibration pass will fix a model with 0.5% recall. The fix is retraining after resolving C2, C3, and C4 above.

### S4. Phase ordering: label upgrade (Phase 4) should be partially concurrent with Phase 1/2, not sequential after feature expansion

The plan places label upgrade in Phase 4, after event/state feature expansion in Phase 3. However, the plan also states in the label upgrade section:

> "Any expected-R, hazard, or hold-value model that ignores realistic entry/exit friction should be considered research-only and not promotable."

If Phase 3 adds a large set of new features and generates experiment results (`equs_quote_v2`, `opra_tcbbo_v2`, etc.) using the existing binary/cost-naive labels, all those experiment results are under the same "research-only" qualifier. Promoting a feature block that was evaluated against a cost-naive label into `full_pilot_v2` would mix the correct gate (cost-aware labels) with the wrong evaluation baseline.

**Recommendation:** Add a minimal label upgrade (at minimum: expected net R with entry/exit spread and fixed slippage) in parallel with Phase 1 rather than after Phase 3. The feature expansion experiments in Phase 3 should be evaluated against the upgraded cost-aware labels, not the existing binary targets.

### S5. The plan does not specify how the holdout window is constructed given the current source window ends at 2026-05-22

The document describes "train/validation/holdout date splits" as required metadata per run, and the experiment matrix mentions "holdout trade count is sufficient" as a promotion gate. However, the current Databento source window ends at `2026-05-22`. Every walk-forward split within this window is in-distribution.

There is no forward holdout without purchasing a new data window (`2026-05-23` onward). The plan should either:
- Explicitly designate the last N months of the current window (e.g., `2026-02-01` through `2026-05-22`) as a held-out evaluation window that is **never used for training or threshold selection**, and
- Acknowledge that a genuine forward holdout requires extending the source window beyond `2026-05-23`.

Without this, "holdout" in the experiment matrix may refer to a fold from the same distribution, which is insufficient for promotion decisions.

### S6. The `pilot_core_5_symbols.txt` file is correct for EQUS but requires clarification for OPRA batch jobs

The file at `runtime/pilot_core_5_symbols.txt` contains raw equity symbols (`TSLA`, `TQQQ`, `NVDA`, `SPY`, `QQQ`). The `submit_batch.py` script's `_normalize_symbol()` function appends `.OPT` automatically when `stype_in=parent`, so OPRA jobs using this file will work correctly. However, the plan's batch commands show both `--symbols-file runtime/pilot_core_5_symbols.txt` for OPRA (using `submit_batch.py`) and EQUS (using `submit_equity_batch.py`) without explaining this normalization behavior.

This should be documented in the file itself (a comment row) to prevent confusion when the same file is used for both script types, and to clarify that the OPRA batch submission script is responsible for the `.OPT` suffix normalization.

### S7. TQQQ option-flow asymmetry is not adequately modeled in the pilot

The five-session data shows TQQQ has:
- Combined liquidity score: 0.825 (lowest in the cohort)
- OPRA RTH option volume: only 1,299,311 (compared to 52M for SPY, 31M for QQQ, 17M for TSLA)
- 0/1DTE option-volume share: 24.7% (lowest in cohort)

This means TQQQ option-flow features will be far sparser and noisier than for other pilot symbols. The plan describes TQQQ as a "primary trade target" but applies the same feature design to it as to TSLA and NVDA. Before committing to TQQQ as a full `tcbbo` feature target, the pilot should explicitly test whether OPRA option-flow features for TQQQ carry any independent signal beyond what QQQ context provides. If they do not, TQQQ should use QQQ option-flow context features rather than its own option-flow features in the micro-entry and lifecycle models.

---

## Minor findings and gaps

### M1. The `symbols_100.txt` name is a misnomer

The file has 99 non-empty, non-comment symbols (verified). The batch commands in the plan reference "99 parent option roots" and "99 raw symbols" in the text, but the commands use `--symbols-file runtime/symbols_100.txt`. This is minor but will cause confusion when someone is auditing job submissions against file contents. Either rename the file to `symbols_99.txt` or add a comment header in the file noting that the count is 99.

### M2. Experiment matrix promotion criteria are not quantified

The table's "Promote only if" column uses qualitative descriptors:
- "Better net R and fewer hard stops"
- "Better entry timing and fewer adverse fills"
- "Better micro-entry confirmation and exits"

Without minimum numerical gates (e.g., "+X% net R over the holdout window, -Y% hard stop rate, stable threshold variance below Z across folds"), these are judgment calls. The plan requires "stable threshold islands across folds, not a single lucky threshold" as a promotion gate, but does not define what constitutes a threshold island (e.g., threshold variance < 0.03 across folds, no fold threshold deviating more than 0.05 from the mean).

**Recommendation:** Add a `min_net_r_improvement_pct`, `max_threshold_variance`, and `min_holdout_trade_count` to the experiment gating table, even if approximate.

### M3. The plan does not address the storage estimate for OPRA `tcbbo`

For 5 pilot symbols over approximately 10 months (`2025-07-21` to `2026-05-22`), OPRA `tcbbo` data could be very large. OPRA generates hundreds of millions of trades per day across all contracts. Even for 5 underlying roots, the tcbbo stream for names like SPY.OPT with 5,565 contracts and 4.6M option trades on a single day could produce file sizes an order of magnitude larger than the existing `ohlcv-1s` files (8.4G for all 99 parents). The 48GB RAM policy section is good, but there is no storage estimate or sanity check before the download completes.

**Recommendation:** After the `tcbbo` job completes, check estimated file size against the source manifest before starting full-window normalization. If the pilot files are unexpectedly large (e.g., >100GB), restrict the first pilot normalization pass to a 10-day validation slice rather than the full window.

**Post-download update:** The completed pilot `tcbbo` folder is about `15.08GiB`, and completed pilot `EQUS mbp-1` is about `58.87GiB`. This resolves the worst-case storage concern, but the 10-day validation slice is still required to validate schema decoding, row counts, join manifests, and memory behavior before a full-window pilot build.

### M4. The `perform_walk_forward_testing()` in `train_30s_models.py` uses trailing train-split rows as its calibration set, not a separate held-out fold

At line 1342-1348 of `train_30s_models.py`, the threshold is calibrated from the last 20% of the training split, not from a separate validation fold. This means:
- The model saw those rows during training.
- The threshold is being tuned on (partially) in-distribution data relative to the model it was just fitted on.
- The resulting threshold tends to be overfit to the training distribution.

The plan calls for "stable threshold islands across folds." Fixing this calibration step (use the test fold for threshold selection, not the tail of the training fold) is a prerequisite for measuring threshold stability correctly.

### M5. The plan is silent on the `cbbo` vs `cbbo-1s` storage/cost trade-off

The document distinguishes `cbbo` (event-based consolidated BBO) from `cbbo-1s` (one-second sampled) but does not give guidance on expected volume difference between them for the pilot_core_5. For active SPY and QQQ option chains, `cbbo` at event frequency could be substantially larger than `cbbo-1s`. Given that `tcbbo` is already being downloaded and will provide option trade-linked quote state, the case for also downloading `cbbo` or `cbbo-1s` should be deferred until the `tcbbo` feature lift is measured, as the plan already recommends but does not enforce with a concrete decision gate.

### M6. The plan references "recorded-event replay" as a promotion gate but no replay harness exists

A historical backtest replay exists (`run_databento_historical_ibkr_sim_backtest.sh`), but this is a Java-based IBKR simulation backtest, not a feature-parity replay harness. The plan's promotion gate requirements for replay (feature vector parity, bucket boundary parity, quote staleness parity, model score parity, and decision parity) are not currently testable.

Phase 6 lists the replay harness as a gate, but it needs to be built before the end of Phase 5 at the latest. Its absence means any paper-mode claim ("runtime decisions match training assumptions") is currently unverifiable. This should be included in the immediate engineering task list as item 13.

### M7. The `train_30s_models.py` walk-forward calibration misuses the term "calibrate"

In `perform_walk_forward_testing()`, the comment at line 1341 says "Calibrate threshold from the tail of train split (time-consistent)." This is threshold selection, not probability calibration. The same conflation appears elsewhere in `train_30s_models.py`. When the plan calls for calibration and the code already uses the word "calibrate" for something different, there is a real risk that a future engineer interprets the existing threshold-selection logic as satisfying the calibration requirement.

Every use of "calibrate" in the existing trainer code should be renamed to "tune_threshold" or "select_threshold," and the new calibration step (isotonic or Platt scaling, Brier score, ECE) should be introduced with unambiguous terminology.

---

## What the plan gets right (affirm for record)

1. **Pilot-first approach** is the correct strategy. Validating schema compatibility, storage, feature joins, and model lift on 5 symbols before committing to the full 99-symbol universe avoids wasted compute and storage.

2. **The distinction between `QuoteUpdateCoverage` and `QuoteStateCoverage`** is precise and technically correct. A quiet second with a stale-but-valid quote has high `QuoteStateCoverage` and low `QuoteUpdateCoverage`. This distinction prevents the main data quality over-marking bug and should be preserved exactly as described.

3. **The OPRA/EQUS lag-sensitivity test specification** is one of the most important controls in the document. Testing each timing-sensitive feature block at `[as-of, +1s lag, +2s lag]` before promotion is the right way to distinguish genuine alpha from exchange-to-SIP latency artifacts.

4. **The `expected_net_r_after_costs` label manifest requirements** are well specified. Requiring entry spread, exit spread, slippage model, partial-fill penalty, and latency assumption to be stored in the label manifest, and failing the build if they are missing, is the correct enforcement mechanism.

5. **"No single day dominates profitability"** as a promotion gate is one of the highest-signal quality checks on a trading ML backtest. It correctly guards against the common failure mode of a model whose apparent edge comes entirely from 2-3 extreme-volatility days.

6. **The batch submission scripts** (`submit_batch.py`, `submit_equity_batch.py`) are well-built: they support `--dry-run`, have explicit start/end validation, handle 502/504 ambiguous submit errors with job-recovery logic, and support both env-variable and file-based API key storage. The auth failure message is clear and actionable.

7. **Keeping the hierarchical runtime architecture** (30s setup → 5s micro-entry → lifecycle exit → micro-exit guard) rather than replacing it with an end-to-end model is the correct call at this stage. The runtime already validates feature schema hashes before allowing the upgraded route, which is a concrete defense against feature-drift silent failures.

---

## Required changes before any retraining is treated as a candidate bundle

The following four items are hard blockers. Until all four are resolved, any retrained bundle should be tagged `research-only`, not considered for paper/shadow mode:

| ID | Blocker | Location |
|---|---|---|
| C1 | Remove `bfill()` from book/price regularization; emit explicit invalid flags | `build_30s_from_5s_csv.py:846,853,862` |
| C2 | Replace `_quality_flag_union()` parent aggregation with aggregate quality score fields | `build_30s_from_5s_csv.py:537` |
| C3 | Build walk-forward setup prediction generator; fail lifecycle/micro training without it | `train_30s_models.py`, new script |
| C4 | Mark `lifecycle_micro_20260523` scorecard as a bootstrap/pre-fix artifact | `model_exports/lifecycle_micro_20260523/` |

---

## Recommended next actions (ordered)

1. On this read-only external computer: do not run any external write/offload commands. Use it for code edits, docs, manifest inspection, tiny local tests, and command preparation only. See `docs/computer_capability_task_organization_20260613.md`.
2. Fix `bfill()` in `build_30s_from_5s_csv.py` (C1) and add aggregate quality fields (C2). These are the same file. Do them together in one PR before any retraining is treated as meaningful.
3. On this computer, write and test the walk-forward setup prediction generator (C4 prerequisite) using tiny local/synthetic data.
4. On this computer, add source/join manifest helpers and tests, including explicit paired-date accounting for the `20260403` EQUS-only `mbp-1` date.
5. On the 48GB write-capable computer: decode one day per schema, produce row-count/schema/memory estimates, and write the source manifest.
6. On the 48GB computer: rebuild a 10-day pilot slice for `TSLA`/`TQQQ`/`NVDA`/`SPY`/`QQQ` using the fixed build pipeline and versioned output paths.
7. Add a minimum cost-aware label (net R with spread + fixed slippage) in parallel with the pilot slice build (S4).
8. Designate a held-out evaluation window (e.g., `2026-01-01` through `2026-05-22`) and confirm it is not used for training, threshold selection, or feature ablation (S5).
9. Begin Phase 2 (walk-forward setup probabilities) only after steps 2, 5, and 6 are complete.
10. Add threshold variance and minimum holdout trade count to experiment promotion gates (M2).
11. Schedule the replay harness as a concrete engineering deliverable for Phase 5 completion (M6).
12. Rename "calibrate" to "tune_threshold" in `train_30s_models.py` to avoid terminology confusion when real probability calibration is added (M7).
