# CatBoost-core downstream replay follow-up — 2026-07-18

Continuation of `docs/catboost_core_downstream_replay_handoff_20260718.md` on the databento worktree.

## Actions completed

1. Reviewed the handoff and verified required artifacts under:
   - `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/`
2. Rebuilt Java classes before replay. An initial `SKIP_BUILD=true` replay was stopped because stale classes ignored the `0.30` micro-entry override and logged `threshold=0.6400`.
3. Ran the full no-trade controlled replay after rebuild:
   - output: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260718_notrade_rebuilt/`
   - manifest: `.../downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json`
   - symbols: `NVDA,QQQ,SPY,TQQQ,TSLA`
   - no-trade mode: `--micro-entry-research-no-trade`
4. Installed Python `onnxruntime` via `python3 -m pip install -r requirements.txt`.
5. Fixed `scripts/export_downstream_setup_filter_onnx.py` ONNX validation for CatBoost native ZipMap outputs.
6. Re-exported/validated CatBoost-core ONNX artifacts into:
   - `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated/`
   - both long/short validations passed `onnxruntime`.
7. Added replay debugging helpers:
   - `scripts/compare_downstream_setup_filter_replay.py`
   - `scripts/score_downstream_feature_debug_vector.py`
8. Added Java debug logging support:
   - `SETUP_FILTER_PASSES` now logs `armEpoch=<epoch>`.
   - opt-in feature-vector logging via `-Dstrategy.downstreamSetupFilter.logFeatureVector=true`.

## Full no-trade replay result

Final counts from `controlled_java_replay_downstream_setup_filter_catboost_core_20260718_notrade_rebuilt/controlled_java_replay.log`:

| marker | count |
|---|---:|
| `SETUP_FILTER_PASSES=PASS` | 200 |
| `SETUP_FILTER_PASSES=FAIL` | 4270 |
| setup candidates total | 4470 |
| `MICRO_ENTRY_CONFIRMS=PASS` | 191 |
| `MICRO_ENTRY_CONFIRMS=FAIL` | 355 |
| trades | 0 per symbol, as expected in no-trade mode |

The candidate count matches the offline `arms_considered=4470`, but pass/confirm counts do **not** match the handoff baseline (`296` filter-pass arms, `285` confirms at micro threshold `0.30`). Therefore the trade-enabled replay was intentionally **not** run.

## Targeted exact-key TQQQ debug

A TQQQ-only replay with explicit `armEpoch` logging reproduced the mismatch:

- output: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260718_notrade_tqqq_armepoch_debug/`
- Java TQQQ setup passes: `58`
- offline selected TQQQ pass arms: `122` (`92` long, `30` short)
- exact key matches: all `122` offline TQQQ pass arms were present as Java candidates
- Java passed only `53` of those `122`; `69` offline-pass arms failed in Java

This proves setup-arm identity is OK for TQQQ; the mismatch is downstream probability/feature parity.

## Feature-vector isolation

A TQQQ feature-debug replay was run with:

```bash
JAVA_TOOL_OPTIONS='-Dstrategy.downstreamSetupFilter.logFeatureVector=true'
```

Output:

- `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260718_notrade_tqqq_featuredebug/`

For high-delta arm `TQQQ.long armEpoch=1778598300`:

- Java log probability: `0.2048`
- Python CatBoost score of Java's exact logged vector: `0.204767329004`
- Python CatBoost score of matching offline row vector: `0.925274442971`

So Java ONNX probability extraction is not the blocker; Java is faithfully scoring its own feature vector. The blocker is feature-value parity.

Largest feature deltas for that arm:

| feature | Java | offline row | abs delta |
|---|---:|---:|---:|
| `ImbalanceStd5s` | `0.70042485` | `177024.8363` | `177024.1358` |
| `AskSizeLast` | `4161` | `1975` | `2186` |
| `CallVolDelta5s` | `1196` | `300` | `896` |
| `PutVolDelta5s` | `305` | `62` | `243` |
| `BidSizeLast` | `105` | `300` | `195` |
| `PutVol` | `55398` | `55490` | `92` |
| `CallVol` | `57758` | `57789` | `31` |

The largest and high-importance mismatch is `ImbalanceStd5s`: the offline training rows have a millions-scale distribution, while Java computes normalized L1 imbalance standard deviation on roughly `[0, 1]` scale.

## Validation commands run

```bash
python3 -m unittest discover -s tests -p 'test_export_downstream_setup_filter_onnx.py'
./mvnw -q -Dtest=DownstreamSetupFilterTest,AiPredictorProbabilityExtractionTest,PingPongStrategyAiEvaluationTest test
./mvnw -q -DskipTests package
python3 scripts/export_downstream_setup_filter_onnx.py \
  --filter-bundle-dir runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_catboost_core_20260628 \
  --output-dir runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated \
  --filter-script scripts/train_downstream_setup_filter.py \
  --training-rows-csv runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv \
  --source-manifest-json runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_catboost_core_20260628/downstream_setup_filter_manifest.json \
  --policy-summary-json runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_filter_replay_policy_eval_catboost_core_20260628/downstream_filter_replay_policy_summary.json \
  --require-onnxruntime-validation
```

## Recommended next steps

1. Do **not** run the trade-enabled replay yet.
2. Decide how to resolve the downstream feature contract:
   - use a sidecar feature CSV for replay parity, if the goal is to reproduce offline research exactly; or
   - retrain/export a CatBoost-core filter on features Java can reproduce without sidecar; likely exclude or redefine `ImbalanceStd5s` and verify option delta / quote-size-last semantics.
3. After feature parity changes, rerun no-trade replay and require:
   - setup candidates: `4470`
   - setup filter passes close/exact to offline selected policy
   - micro confirms close/exact to offline `0.30` selected policy
4. Only after no-trade parity is explained and acceptable, run the trade-enabled controlled replay.

## 2026-07-19 continuation: sidecar exact replay parity

The sidecar exact no-trade replay was run with the revalidated CatBoost-core ONNX manifest plus the offline setup-arm feature sidecar:

- output: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260718_sidecar_exact_notrade/`
- manifest: `.../downstream_setup_filter_onnx_catboost_core_20260718_validated/downstream_setup_filter_route_manifest.json`
- feature sidecar: `.../downstream_setup_training_rows/setup_downstream_training_rows_v1.csv`
- sidecar rows loaded per symbol: `4470`
- run completion: `[BACKTEST] completed=5 failed=0 requested=5`

Final log counts:

| marker | count |
|---|---:|
| `SETUP_FILTER_PASSES=PASS` | `296` |
| `SETUP_FILTER_PASSES=FAIL` | `4174` |
| setup candidates total | `4470` |
| `featureSidecar=hit` | `4470` |
| `featureSidecar=miss` | `0` |
| `MICRO_ENTRY_CONFIRMS=PASS` | `284` |
| `MICRO_ENTRY_CONFIRMS=FAIL` | `506` |

Setup-filter parity against `downstream_filter_replay_policy_eval_catboost_core_20260718_revalidated/downstream_filter_replay_policy_decisions.csv` is exact:

- Java setup rows: `4470`
- offline selected setup-filter pass rows: `296`
- matched pass rows: `296`
- pass/fail disagreements: `0`
- max absolute downstream probability delta on matched selected rows: `0.000050`

A reusable micro-confirmation comparison helper was added:

- `scripts/compare_micro_entry_confirmations.py`

At selected micro threshold `0.30`, Java produced `284` no-trade confirmations vs offline `285`. The one missing Java confirmation is:

| symbol | side | arm epoch | offline confirm epoch | offline micro prob | Java behavior |
|---|---|---:|---:|---:|---|
| `TQQQ` | `short` | `1778767050` | `1778767050` | `0.300000` | Armed and evaluated, but Java logged boundary/under-threshold failures (`0.2906`, rounded `0.3000`, `0.2999`, then lower values) and cleared the arm as expired (`secondsSinceArm=80`). |

Interpretation: using the sidecar CSV proves exact downstream setup-filter feature/probability parity for the offline research policy. The remaining `1` micro-confirm difference is a threshold-boundary precision/timing edge at exactly `0.30`, not a setup-sidecar miss or downstream ONNX scoring issue.

Next action if the goal is to reproduce offline research exactly: run the trade-enabled controlled replay with the same sidecar inputs, but keep the result labeled **research-only / sidecar parity evidence**, not live-feature parity evidence. Do not use this as a live promotion proof unless the production feature contract is separately resolved.

## 2026-07-19 continuation: trade-enabled sidecar replay

The trade-enabled sidecar replay was run by removing `--micro-entry-research-no-trade` from the sidecar exact command while keeping the same manifest, sidecar CSV, symbols, recorded events, and `0.30` micro-entry thresholds.

- output: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260719_sidecar_exact_trade/`
- summary artifacts:
  - `trade_enabled_sidecar_summary_20260719.md`
  - `trade_enabled_sidecar_summary_20260719.json`
- summarizer added: `scripts/summarize_trade_enabled_replay.py`
- run completion: `[BACKTEST] completed=5 failed=0 requested=5`

Marker counts:

| marker | count |
|---|---:|
| `SETUP_FILTER_PASSES=PASS` | `121` |
| `SETUP_FILTER_PASSES=FAIL` | `3228` |
| `featureSidecar=hit` | `3349` |
| `featureSidecar=miss` | `0` |
| `MICRO_ENTRY_CONFIRMS=PASS` | `110` |
| `MICRO_ENTRY_CONFIRMS=FAIL` | `282` |

Trade/lifecycle summary:

| metric | value |
|---|---:|
| closed trades | `110` |
| submitted orders | `220` |
| filled orders | `220` |
| total PnL | `+17662.42` |
| sum realized R | `+97.0384` |
| mean realized R | `+0.8822` |
| median realized R | `+1.3610` |
| win rate | `82.73%` |
| avg MFE R | `+1.5021` |
| avg MAE R | `-0.7927` |

Exit reasons:

| exit reason | count |
|---|---:|
| `guard` | `86` |
| `hard_stop` | `15` |
| `lifecycle` | `9` |
| hard-risk exits | `0` |
| EOD exits | `0` |

Distribution:

| cohort | count / share |
|---|---:|
| long trades | `74` |
| short trades | `36` |
| `TQQQ` trades | `53` / `48.18%` |
| `NVDA` trades | `35` / `31.82%` |
| `TSLA` trades | `22` / `20.00%` |
| max day trade share | `13.64%` (`2026-05-11`, `15` trades) |
| `QQQ` trades | `0` |
| `SPY` trades | `0` |

Interpretation: the trade-enabled sidecar replay produced enough non-zero realized trade evidence to close the original “closed trades = 0” blocker for the **research-only sidecar branch**. However, it is still **not** live-feature parity evidence and remains **NO-GO for promotion/live use** until the production feature contract is resolved without the offline sidecar, or the sidecar/event-carried enriched snapshot contract is intentionally productized and validated in paper/shadow.

## 2026-07-20 continuation: event-carried snapshot no-sidecar replay

The productized event-carried enriched snapshot path was implemented and validated in controlled replay. Java consumed `enriched_features` embedded in the NDJSON stream, and the replay was run **without** `--downstream-setup-filter-features-csv`.

Implementation/tooling added:

- Java event-carried snapshot consumer:
  - `DatabentoEvent` captures nested/top-level finite numeric enriched features plus `FeatureSnapshotEpochSec` metadata.
  - `IBKRTrader` and `DatabentoHistoricalStreamingBacktester` forward snapshots before `onSourceBar(...)`.
  - `PingPongStrategy` caches event-carried snapshots by arm epoch and merges them before downstream setup-filter scoring.
  - CSV sidecar remains the highest-precedence research override.
- Replay fixture generator: `scripts/inject_downstream_setup_features_into_ndjson.py`
- Replay drift validator: `scripts/compare_event_snapshot_replay_drift.py`
- Paper/shadow drift validator: `scripts/validate_paper_shadow_drift.py`
- Contract/runbook: `docs/event_carried_enriched_snapshots.md`
- Summary artifact: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade/event_snapshot_notrade_summary_20260720.md`

Fixture generation result:

| metric | value |
|---|---:|
| enriched NDJSON | `databento-20260427-20260522-core5-4week-daily-prevclose.event-snapshots-catboost-core-20260720.ndjson.gz` |
| sidecar rows expected | `4470` |
| snapshot rows injected | `4470` |
| unmatched snapshot rows | `0` |
| unique carrier events | `4470` |
| multi-snapshot carrier events | `0` |
| carrier policy | `first-at-or-after` (`SetupEpochSec - 1`) |
| carrier lag min / max / mean seconds | `-1 / 57 / 0.6579` |

No-sidecar no-trade replay result:

| marker | count |
|---|---:|
| completion | `[BACKTEST] completed=5 failed=0 requested=5` |
| `SETUP_FILTER_PASSES=PASS` | `296` |
| `SETUP_FILTER_PASSES=FAIL` | `4174` |
| setup candidates total | `4470` |
| `featureSnapshot=hit` | `4470` |
| `featureSnapshot=miss` | `0` |
| `featureSidecar=disabled` | `4470` |
| `MICRO_ENTRY_CONFIRMS=PASS` | `284` |
| `MICRO_ENTRY_CONFIRMS=FAIL` | `506` |

Replay drift validation (`scripts/compare_event_snapshot_replay_drift.py`) passed:

- event rows: `4470`
- sidecar expected keys: `4470`
- missing expected rows: `0`
- extra event rows: `0`
- feature snapshot hit rate: `1.0`
- paired rows vs sidecar-exact reference log: `4470`
- max probability drift vs sidecar-exact reference log: `0.0`
- decision mismatches vs sidecar-exact reference log: `0`

Direct offline selected-policy comparison also matched the prior sidecar-exact result:

- Java rows: `4470`
- offline selected setup-filter pass rows: `296`
- matched pass rows: `296`
- pass/fail disagreements: `0`
- probability delta on matched selected rows: max `0.000050`, mean `0.000026`, median `0.000027`

Micro-confirmation comparison at threshold `0.30` remains the same known one-count boundary delta:

- offline confirmations: `285`
- Java no-trade confirmations: `284`
- missing Java confirmation: `TQQQ short arm_epoch=1778767050`, offline micro prob exactly `0.300000`

Interpretation: controlled replay now proves exact downstream setup-filter parity through the event-carried snapshot path without a Java CSV sidecar. Promotion/live status remains **NO-GO** until paper/shadow validates that the live feature producer emits the same snapshot contract with acceptable hit-rate, probability drift, and decision drift.

## 2026-07-20 continuation: event-carried snapshot trade-enabled replay

The trade-enabled controlled replay was rerun with the enriched NDJSON event-snapshot file and **without** `--downstream-setup-filter-features-csv` or `--micro-entry-research-no-trade`.

- output: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_trade/`
- summary artifacts:
  - `trade_enabled_event_snapshot_summary_20260720.json`
  - `trade_enabled_event_snapshot_summary_20260720.md`
- run completion: `[BACKTEST] completed=5 failed=0 requested=5`

Marker counts:

| marker | count |
|---|---:|
| `SETUP_FILTER_PASSES=PASS` | `121` |
| `SETUP_FILTER_PASSES=FAIL` | `3228` |
| `featureSnapshot=hit` | `3349` |
| `featureSnapshot=miss` | `0` |
| `featureSidecar=disabled` | `3349` |
| `featureSidecar=hit` | `0` |
| `MICRO_ENTRY_CONFIRMS=PASS` | `110` |
| `MICRO_ENTRY_CONFIRMS=FAIL` | `282` |

Realized trade summary:

| metric | value |
|---|---:|
| closed trades | `110` |
| submitted orders | `220` |
| filled orders | `220` |
| total PnL | `+17662.42` |
| sum realized R | `+97.0384` |
| mean realized R | `+0.8822` |
| median realized R | `+1.3610` |
| win rate | `82.73%` |
| avg MFE R | `+1.5021` |
| avg MAE R | `-0.7957` |
| max day trade share | `13.64%` (`2026-05-11`, `15` trades) |
| max symbol trade share | `48.18%` (`TQQQ`, `53` trades) |

Exit reasons:

| exit reason | count |
|---|---:|
| `guard` | `86` |
| `hard_stop` | `15` |
| `lifecycle` | `9` |
| hard-risk exits | `0` |
| EOD exits | `0` |

Distribution:

| cohort | count / share |
|---|---:|
| long trades | `74` |
| short trades | `36` |
| `TQQQ` trades | `53` / `48.18%` |
| `NVDA` trades | `35` / `31.82%` |
| `TSLA` trades | `22` / `20.00%` |
| `QQQ` trades | `0` |
| `SPY` trades | `0` |

Interpretation: the event-carried snapshot path reproduces the sidecar-exact trade-enabled evidence while keeping the Java CSV sidecar disabled. This closes the controlled-replay transport/parity concern for event-carried snapshots, but it is still **research-only replay evidence**. Promotion/live status remains **NO-GO** until real paper/shadow logs show the production event producer emits the same snapshot contract with acceptable hit-rate, probability drift, and decision drift.

## 2026-07-20 continuation: paper/shadow drift validator run

No real local `paper`/`shadow` runtime logs with `SETUP_FILTER_PASSES` telemetry were found at validation time. The only available setup-filter telemetry logs were controlled replay outputs under `runtime/local-backtests/...`.

The paper/shadow validator was run on available controlled artifacts and summary was written to:

- `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade/paper_shadow_drift_validation_20260720.md`

Controlled self-check (`event-snapshot replay log` vs itself, strict zero-drift thresholds):

| metric | value |
|---|---:|
| status | `PASS` |
| paired rows | `4470` |
| max probability drift | `0.0` |
| decision mismatches | `0` |
| paper/shadow snapshot hit rate | `1.0 / 1.0` |
| snapshot misses | `0 / 0` |

Controlled reference proxy (`event-snapshot replay log` vs prior sidecar-exact replay log):

| metric | value |
|---|---:|
| status | `PASS` |
| paired rows | `4470` |
| max probability drift | `0.0` |
| mean probability drift | `0.0` |
| decision mismatches | `0` |
| paper snapshot hit rate | `1.0` |
| shadow snapshot telemetry rows | `0` |
| snapshot-status mismatch rate | `1.0` expected because the sidecar reference log predates `featureSnapshot=...` telemetry |

Interpretation: controlled paper/shadow-style drift validation is **PASS** for the replay artifacts. True live/paper shadow drift validation is still **not complete** because no real paired paper/shadow logs were available locally. Promotion/live status remains **NO-GO** until a real paper/shadow session is captured and passes `scripts/validate_paper_shadow_drift.py` with both sides using event-carried snapshots and CSV sidecar disabled.
