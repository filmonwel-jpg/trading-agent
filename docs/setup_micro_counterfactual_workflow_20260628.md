# Setup → Micro Counterfactual Workflow (June 28, 2026)

This workflow is research-only. It builds evidence for better setup arms by joining replay setup arms, micro-entry evaluations, future equity paths, downstream labels, 30-second features, and an offline policy screen.

Promotion remains **NO-GO** until runtime wiring, calibration, live/replay parity, paper/shadow drift, replay volume, and dominance gates pass.

## 1. Generate counterfactual setup-to-micro labels

```zsh
cd /Users/FXG06FA/trading-agent-main

scripts/run_setup_micro_counterfactual_report_4week_latest.sh
```

Manual equivalent:

```zsh
cd /Users/FXG06FA/trading-agent-main

python3 -u scripts/generate_setup_micro_counterfactual_report.py \
  --log runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/run/databento-core5-4week-recent-20260627_230823.log \
  --recorded-events runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz \
  --output-dir runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823
```

Primary outputs:

- `setup_micro_counterfactual_summary.json`
- `setup_micro_counterfactual_summary.md`
- `setup_micro_counterfactual_arms.csv`
- `setup_micro_counterfactual_micro_evals.csv`
- `setup_micro_counterfactual_decisions.csv`
- `setup_downstream_confirmable_labels_v1.csv`
- `setup_downstream_confirmable_labels_v1_manifest.json`

Current local four-week result:

- Setup arms: `4,470`
- Micro evaluations: `45,919`
- Recorded equity bars: `932,320`
- Downstream-positive labels at `label_min_micro_prob=0.30`, `min_expected_net_r=0.0`: `371` (`263` long, `108` short)

## 2. Join downstream labels to enriched 30-second rows

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/build_downstream_setup_training_rows.py \
  --input-30s-csv runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  --labels-csv "$BASE/setup_downstream_confirmable_labels_v1.csv" \
  --output-dir "$BASE/downstream_setup_training_rows" \
  --join-tolerance-seconds 31
```

Current join result:

- Joined rows: `4,470`
- Matched rows: `4,470`
- Unmatched rows: `0`
- Absolute epoch delta: `1` second min/p50/p90/max

## 3. Train research-only downstream setup-arm filters

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/train_downstream_setup_filter.py \
  --training-rows-csv "$BASE/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv" \
  --output-dir "$BASE/downstream_setup_filter_research" \
  --min-precision 0.40 \
  --min-holdout-predictions 5
```

The trainer excludes future outcome fields, including `Label_*`, `Expected_*`, `Max_Future_Micro_*`, `Best_*`, and `MicroEvalWithinTtlCount`.

Current regenerated no-leakage filter bundle:

| side | holdout AP | holdout AUC | threshold | precision | recall | selected mean expected R | selected p50 expected R |
|---|---:|---:|---:|---:|---:|---:|---:|
| long | `0.4225` | `0.8975` | `0.5000` | `0.4405` | `0.4933` | `+0.1498` | `+1.2199` |
| short | `0.3271` | `0.9032` | `0.5500` | `0.4000` | `0.4118` | `+0.0556` | `-0.0739` |

## 4. Offline controlled replay-policy screen

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/evaluate_downstream_filter_replay_policy.py \
  --training-rows-csv "$BASE/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv" \
  --decisions-csv "$BASE/setup_micro_counterfactual_decisions.csv" \
  --filter-bundle-dir "$BASE/downstream_setup_filter_research" \
  --output-dir "$BASE/downstream_filter_replay_policy_eval" \
  --micro-thresholds "0.20 0.25 0.30 0.35 0.40 0.45 0.50 0.52 0.55 0.57 0.58" \
  --min-confirms-for-best 20 \
  --max-day-dominance-for-best 0.40
```

Current best offline screen result uses selected setup-filter thresholds (`long=0.50`, `short=0.55`) and micro threshold `0.20`:

- Filter-passed arms: `540 / 4,470`
- Confirms: `539`
- Positive outcomes: `312`
- Mean expected net R: `+0.1833`
- P50 expected net R: `+1.0052`
- Expected net R sum: `+98.05`
- Symbol/side cohorts: `6`
- Max day dominance: `11.50%` on `2026-05-12`

This is a useful research screen, but it is **not promotion evidence** because the filter is trained on replay-observed arms, the controlled Java replay does not yet match this offline policy screen, live/replay parity is not proven, and paper/shadow drift evidence is absent.

## 5. Export downstream setup filters as ONNX research artifacts

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/export_downstream_setup_filter_onnx.py \
  --filter-bundle-dir "$BASE/downstream_setup_filter_research" \
  --training-rows-csv "$BASE/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv" \
  --source-manifest-json "$BASE/downstream_setup_filter_research/downstream_setup_filter_manifest.json" \
  --policy-summary-json "$BASE/downstream_filter_replay_policy_eval/downstream_filter_replay_policy_summary.json" \
  --output-dir "$BASE/downstream_setup_filter_onnx_research" \
  --require-onnxruntime-validation
```

Generated research artifacts:

- `downstream_setup_filter_onnx_research/long_downstream_setup_filter.onnx`
- `downstream_setup_filter_onnx_research/short_downstream_setup_filter.onnx`
- `downstream_setup_filter_onnx_research/long_downstream_setup_filter_feature_schema.json`
- `downstream_setup_filter_onnx_research/short_downstream_setup_filter_feature_schema.json`
- `downstream_setup_filter_onnx_research/downstream_setup_filter_route_manifest.json`
- `downstream_setup_filter_onnx_research/downstream_setup_filter_onnx_manifest.json`
- `downstream_setup_filter_onnx_research/downstream_setup_filter_onnx_summary.md`

Current export result:

| side | route | features | threshold | parity validation | sample rows | max abs probability delta |
|---|---|---:|---:|---|---:|---:|
| long | `longDownstreamSetupFilterAi` | `139` | `0.5000` | `passed_onnx_reference` | `512` | `6.74e-07` |
| short | `shortDownstreamSetupFilterAi` | `135` | `0.5500` | `passed_onnx_reference` | `512` | `4.90e-07` |

The local Python environment did not have `onnxruntime` installed at export time, so validation used ONNX's built-in reference evaluator. `requirements.txt` now includes `onnxruntime>=1.17` for future runtime-parity validation environments. The exporter still keeps these artifacts **research-only / NO-GO** and records feature-order hashes, model hashes, thresholds, route names, output names, source pickle hashes, and the embedded offline best-policy context.

## 6. Controlled Java replay wiring

The Java strategy now has an optional controlled replay gate for `downstream_setup_filter_route_manifest.json`:

- Enable with `-Dstrategy.downstreamSetupFilter.enabled=true`.
- Point Java at the route manifest with `-Dstrategy.downstreamSetupFilter.routeManifest=/absolute/path/to/downstream_setup_filter_route_manifest.json`.
- The loader resolves each per-side ONNX model and `*_feature_schema.json` relative to the manifest directory.
- The replay adapter rebuilds the exact side-specific feature vector in schema order before scoring.
- Missing research-only columns are filled with `0.0f`, matching the export transform contract for missing numeric columns and unseen categorical one-hot categories.
- The normal setup arm is evaluated first, then the downstream setup-quality threshold is applied, then the 5-second micro-entry first-cross threshold is applied.
- The filter is fail-closed by default. Set `-Dstrategy.downstreamSetupFilter.failClosed=false` only for diagnostics.

Current controlled Java replay command:

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
COUNTERFACTUAL="$BASE/setup_micro_counterfactual_20260627_230823"
EVENTS="$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz"
MANIFEST="$COUNTERFACTUAL/downstream_setup_filter_onnx_research/downstream_setup_filter_route_manifest.json"
OUT="$BASE/controlled_java_replay_downstream_setup_filter_20260628_agent"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$EVENTS" \
  --output-dir "$OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-long-entry-threshold 0.20 \
  --micro-short-entry-threshold 0.20 \
  --timeout-seconds 0 \
  > "$OUT/controlled_java_replay.log" 2>&1
```

The wrapper now accepts `--downstream-setup-filter-manifest`, converts relative manifest paths to absolute paths, passes `strategy.downstreamSetupFilter.*` Java properties, and preserves the lifecycle/micro route configuration. Compare:

- Java replay `trade_lifecycle_summary` files and `>>> [FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=...` counts.
- Offline policy screen: `downstream_filter_replay_policy_eval/downstream_filter_replay_policy_summary.json`.
- Expected reference policy at export time: setup-filter thresholds `long=0.50`, `short=0.55`; micro first-cross threshold `0.20`; filter-passed arms `540 / 4,470`; confirms `539`; positive outcomes `312`; expected net R sum `+98.05`; max day dominance `11.50%`.

Important interpretation: this Java replay path is still **controlled replay / NO-GO**. Runtime/live parity is not proven because several research-training columns are only available in enriched offline 30-second rows and are zero-filled by the replay adapter unless the live event source is extended to provide them.

Completed local agent replay result (`controlled_java_replay_downstream_setup_filter_20260628_agent`, exit code `0`):

| Symbol | Downstream-passed arms | Long arms | Short arms | Micro confirmations | Arm expirations | Closed trades |
|---|---:|---:|---:|---:|---:|---:|
| NVDA | `25` | `14` | `11` | `0` | `14` | `0` |
| QQQ | `0` | `0` | `0` | `0` | `0` | `0` |
| SPY | `0` | `0` | `0` | `0` | `0` | `0` |
| TQQQ | `39` | `3` | `36` | `0` | `36` | `0` |
| TSLA | `4` | `0` | `4` | `0` | `3` | `0` |
| **Total** | **`68`** | **`17`** | **`51`** | **`0`** | **`53`** | **`0`** |

Log counts were `SETUP_FILTER_PASSES=PASS: 68` and `SETUP_FILTER_PASSES=FAIL: 4402`. This does **not** reproduce the offline screen (`540` filter-pass arms, `539` confirms, `312` positives, expected net R sum `+98.05` at micro threshold `0.20`). Treat the completed Java replay as a **parity-fail / NO-GO** signal, not as promotion evidence. A compact comparison artifact is written at `controlled_java_replay_downstream_setup_filter_20260628_agent/agent_controlled_replay_comparison.md`.

### 2026-06-28 continuation: root cause, sidecar replay parity, and corrected retrain

Root cause for the `68` setup arms / `0` micro confirmations replay was feature parity, not a strategy runtime failure. The downstream setup filter was trained on enriched research rows containing columns such as `tsm_*`, `seq_*`, `regime_*`, and `setup_*`. The default Java replay path can synthesize causal market/quote/option features but zero-fills those unavailable research columns, which materially changes the ONNX score distribution.

Source-side replay support was restored/added so controlled research replay can opt in to the enriched setup-arm sidecar CSV and suppress order placement while still emitting micro-entry telemetry:

- `src/main/java/com/calgary/fili/trader/bot/strategy/DownstreamSetupFeatureStore.java`
- `src/main/java/com/calgary/fili/trader/bot/strategy/PingPongStrategy.java`
- `scripts/run_databento_historical_ibkr_sim_backtest.sh`
- `src/test/java/com/calgary/fili/trader/bot/strategy/DownstreamSetupFeatureStoreTest.java`

The sidecar exact no-trade run (`controlled_java_replay_downstream_setup_filter_20260628_sidecar_exact_notrade`, exit code `0`) matches the selected offline policy counts:

| Source | Setup-filter pass arms | Long arms | Short arms | Micro confirmations | Arm expirations | Closed trades |
|---|---:|---:|---:|---:|---:|---:|
| Offline selected policy | `540` | `397` | `143` | `539` | n/a | n/a |
| Java zero-filled replay | `68` | `17` | `51` | `0` | `53` | `0` |
| Java sidecar exact no-trade replay | `540` | `397` | `143` | `539` | `1` | `0` |

Per-symbol sidecar replay lifecycle summary:

| Symbol | Downstream-passed arms | Long arms | Short arms | Micro confirmations | Arm expirations |
|---|---:|---:|---:|---:|---:|
| NVDA | `197` | `134` | `63` | `196` | `1` |
| QQQ | `0` | `0` | `0` | `0` | `0` |
| SPY | `0` | `0` | `0` | `0` | `0` |
| TQQQ | `272` | `209` | `63` | `272` | `0` |
| TSLA | `71` | `54` | `17` | `71` | `0` |

Corrected setup-arm lifecycle/micro retraining input was generated at:

- `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/corrected_lifecycle_micro_retrain_20260628/corrected_setup_arm_oof_predictions_for_lifecycle_micro.csv`

Generation result: `540` rows, side distribution `{'long': 397, 'short': 143}`. Timestamps are emitted in market-time `%Y%m%d %H:%M:%S America/New_York` format so `train_lifecycle_micro_models.py::parse_timestamp()` can consume them without mixing tz-naive and tz-aware pandas dtypes.

Corrected lifecycle/micro model exports were written to:

- `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/corrected_lifecycle_micro_retrain_20260628/model_exports/`

Training scorecard highlights:

| Model | Rows | Positives | Threshold | Precision | Recall | Brier | ECE |
|---|---:|---:|---:|---:|---:|---:|---:|
| `longExitLifecycleAi` | `5,086` | `2,871` | `0.64` | `96.72%` | `75.64%` | `0.0577` | `0.0446` |
| `shortExitLifecycleAi` | `3,058` | `1,695` | `0.58` | `96.66%` | `88.09%` | `0.0632` | `0.0454` |
| `longMicroEntryAi` | `1,883` | `647` | `0.54` | `100.00%` | `2.52%` | `0.2508` | `0.0988` |
| `shortMicroEntryAi` | `1,134` | `463` | `0.58` | `100.00%` | `4.08%` | `0.2244` | `0.0894` |
| `longMicroExitGuardAi` | `3,696` | `963` | `0.60` | `100.00%` | `45.32%` | `0.1337` | `0.0589` |
| `shortMicroExitGuardAi` | `2,400` | `862` | `0.60` | `100.00%` | `42.64%` | `0.1171` | `0.0910` |

Interpretation remains **research-only / NO-GO**. The sidecar replay proves Java can reproduce the offline policy count distribution when it is given offline-enriched features, but it does not prove live recorded-event parity. The corrected models also have no realized trade/PnL evidence from promoted execution, and micro-entry recall is very low at the selected precision-maximizing thresholds. Do not run or pass promotion gates as GO evidence until the prerequisites in the final section are available.

Follow-up feature feasibility analysis is documented in `docs/downstream_setup_feature_live_parity_analysis_20260628.md`. Summary: Java can causally produce the proxy `tsm_*`, `regime_*`, `seq_*`, and `setup_*` groups, but the current event/strategy path cannot exactly reproduce the high-impact `EqMbp1*30s` and `OpraTcbbo*30s` silver aggregate families without new replay/live aggregators or event-carried enriched snapshots.

CatBoost cost-aware model lineage clarification: the replay setup arms used by this workflow were generated by the June 24 CatBoost cost-aware setup ONNX bundle at `runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854` (`model_family=catboost`, `cost_aware=true`, `feature_count=34`). However, the downstream setup-quality filter trained in `downstream_setup_filter_research/` is a second-stage `RandomForestClassifier` over replay-observed CatBoost setup arms and downstream micro/profit labels. It is not itself a CatBoost cost-aware setup model. The downstream labels are cost-aware in the sense that their expected-net-R simulation applies spread/slippage/fill penalties, but the learner family and feature schema differ from the primary CatBoost setup bundle.

### 2026-06-28 continuation: CatBoost downstream setup-quality filter without enriched meta/silver features

Decision update: the downstream setup-quality filter should no longer expect the high-impact enriched research/silver aggregate columns. The upstream setup arms are already from the CatBoost cost-aware setup bundle, and enriched meta features remain optional on both sides: training may opt into them with `--feature-preset all`, and Java may provide extra feature values, but the promoted/core schema must not require them.

Code update:

- `scripts/train_downstream_setup_filter.py` now defaults to `--model-family catboost` and `--feature-preset catboost_cost_aware_core`.
- `catboost_cost_aware_core` excludes optional `tsm_*`, `regime_*`, `news_*`, `seq_*`, `setup_*`, `News*`, `EqMbp1*`, and `OpraTcbbo*` columns.
- `scripts/export_downstream_setup_filter_onnx.py` can export CatBoost bundles through CatBoost's native ONNX writer and records `feature_preset` / `excluded_feature_groups` in each feature schema.
- Java's downstream scorer still fills missing schema columns with `0.0f`; because the new schema omits enriched/silver groups, Java no longer needs to synthesize or sidecar-load those optional groups for this filter.

Training command used:

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/train_downstream_setup_filter.py \
  --training-rows-csv "$BASE/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv" \
  --output-dir "$BASE/downstream_setup_filter_catboost_core_20260628" \
  --model-family catboost \
  --feature-preset catboost_cost_aware_core \
  --min-precision 0.40 \
  --min-holdout-predictions 5
```

The trainer loaded `4,470` replay-observed CatBoost cost-aware setup arms and excluded `32` enriched meta columns plus `67` Databento silver aggregate columns. The side-specific schemas each use `59` core features.

Training scorecard:

| side | model | features | holdout AP | holdout AUC | threshold | precision | recall | selected mean expected R | selected p50 expected R |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| long | `CatBoostClassifier` | `59` | `0.2816` | `0.8272` | `0.7500` | `0.4211` | `0.1067` | `+0.4560` | `+1.2328` |
| short | `CatBoostClassifier` | `59` | `0.3078` | `0.8723` | `0.7500` | `0.4286` | `0.0882` | `+0.6587` | `+1.2468` |

Offline replay-policy evaluation command:

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823

python3 -u scripts/evaluate_downstream_filter_replay_policy.py \
  --training-rows-csv "$BASE/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv" \
  --decisions-csv "$BASE/setup_micro_counterfactual_decisions.csv" \
  --filter-bundle-dir "$BASE/downstream_setup_filter_catboost_core_20260628" \
  --output-dir "$BASE/downstream_filter_replay_policy_eval_catboost_core_20260628" \
  --micro-thresholds "0.20 0.25 0.30 0.35 0.40 0.45 0.50 0.52 0.55 0.57 0.58" \
  --min-confirms-for-best 20 \
  --max-day-dominance-for-best 0.40
```

Best selected-threshold policy (`long=0.75`, `short=0.75`) was at micro threshold `0.30`:

- Filter-passed arms: `296 / 4,470`
- Confirms: `285`
- Positive outcomes: `228` (`80.0%` of confirms)
- Mean expected net R: `+0.6617`
- P50 expected net R: `+1.2121`
- Expected net R sum: `+187.91`
- Symbol/side cohorts: `6`
- Max day dominance: `12.63%` on `2026-04-28`

ONNX research export was written to:

- `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/`

The export passed ONNX structural validation. Runtime probability parity was marked `skipped_missing_onnx_runtime` in the local environment because `onnxruntime` was not installed; install `requirements.txt` in the replay environment before requiring runtime parity.

## 7. Validation commands

```zsh
cd /Users/FXG06FA/trading-agent-main

bash -n scripts/run_setup_micro_counterfactual_report_4week_latest.sh

python3 -m py_compile \
  scripts/generate_setup_micro_counterfactual_report.py \
  scripts/build_downstream_setup_training_rows.py \
  scripts/train_downstream_setup_filter.py \
  scripts/evaluate_downstream_filter_replay_policy.py \
  scripts/export_downstream_setup_filter_onnx.py \
  tests/test_generate_setup_micro_counterfactual_report.py \
  tests/test_build_downstream_setup_training_rows.py \
  tests/test_train_downstream_setup_filter.py \
  tests/test_evaluate_downstream_filter_replay_policy.py \
  tests/test_export_downstream_setup_filter_onnx.py

python3 tests/test_generate_setup_micro_counterfactual_report.py
python3 tests/test_build_downstream_setup_training_rows.py
python3 tests/test_train_downstream_setup_filter.py
python3 tests/test_evaluate_downstream_filter_replay_policy.py
python3 tests/test_export_downstream_setup_filter_onnx.py

./mvnw -q -Dtest=DownstreamSetupFeatureStoreTest,DownstreamSetupFilterTest,PingPongStrategyFeatureSchemaTest,PingPongStrategyAiEvaluationTest test
```

## Next steps

1. Completed: investigated the `68` / `0` mismatch and identified missing enriched research columns as the driver.
2. Completed for controlled research replay only: Java can now load a setup-arm feature sidecar instead of zero-filling unavailable enriched columns. Live/replay event parity is still not proven because those enriched research producers are not present in normalized live Databento events.
3. Completed: sidecar exact no-trade replay matches the offline selected policy count distribution (`540` setup-filter pass arms, `539` micro confirmations).
4. Completed: corrected setup-arm distribution was used to retrain lifecycle/micro artifacts under `corrected_lifecycle_micro_retrain_20260628/model_exports/`.
5. Current handoff: continue with the CatBoost-core downstream setup-filter replay in `docs/catboost_core_downstream_replay_handoff_20260718.md`. The immediate test is no-sidecar, no-trade Java replay at micro threshold `0.30`, expecting roughly `296` setup-filter pass arms and `285` micro confirmations before any trade-enabled run.
6. Still pending / NO-GO: run promotion gates only after enough closed trades, positive expected and realized net R, acceptable calibration, symbol/day dominance controls, recorded-event/live parity, and paper/shadow drift evidence are available.


