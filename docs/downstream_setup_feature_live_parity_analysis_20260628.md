# Downstream setup-filter enriched feature live/replay parity analysis — 2026-06-28

## Question

Can the missing enriched research features used by the downstream setup-filter training schema be produced on the Java side during backtesting, recorded-event replay, and live trading so Java can match the training feature distribution without the offline setup-arm sidecar?

## Short answer

Partially, but **not enough with the current Java/event pipeline to match the current training schema exactly**.

- The `tsm_*`, `regime_*`, `seq_*`, and `setup_*` meta/proxy features are live-causal and already have Java proxy implementations in `src/main/java/com/calgary/fili/trader/bot/strategy/EnhancedLiveFeatureProducer.java`.
- The setup/bar/basic quote/quality features are mostly live-causal, but Java currently recomputes several quality fields approximately instead of consuming the exact recorded-event quality fields.
- The `EqMbp1*30s` and `OpraTcbbo*30s` silver aggregate groups are the blocking gap. They are produced offline by `scripts/build_databento_enriched_30s.py` from 1-second EQUS MBP/TBBO and OPRA TCBBO silver artifacts. Java currently substitutes last-snapshot or volume-only approximations, which are not distribution-equivalent to the training rows.
- The current `DatabentoEvent` contract ignores unknown enriched fields and only exposes basic equity/option bar fields plus a small quality-field subset. Therefore adding enriched columns to NDJSON alone will not help until Java consumes and merges them.

Current status remains **research-only / NO-GO** for promotion unless either:

1. the current downstream setup filter is replayed/live-scored with causally generated enriched feature aggregates that match the training schema, or
2. the downstream setup filter is retrained on a smaller feature schema that is actually available in Java live/replay without offline sidecars.

Update later on 2026-06-28: option 2 is now implemented as the preferred next research branch. `scripts/train_downstream_setup_filter.py` defaults to a CatBoost second-stage learner with `--feature-preset catboost_cost_aware_core`, which excludes optional enriched meta columns and `EqMbp1*`/`OpraTcbbo*` silver aggregate columns. Java may still provide those extra values, but the CatBoost core downstream schema does not require them unless a future run explicitly opts back into `--feature-preset all`.

## Evidence from current schema and artifacts

The replay setup arms are CatBoost cost-aware setup arms. The controlled replay log loads setup models from `runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854`, and that bundle's `setup_manifest.json` reports `model_family=catboost`, `cost_aware=true`, and `feature_count=34`.

The downstream setup filter analyzed here is a separate second-stage model. Its manifest at `downstream_setup_filter_research/downstream_setup_filter_manifest.json` reports `model_family=RandomForestClassifier` and trains on replay-observed CatBoost setup arms joined back to enriched 30-second rows. The downstream labels are cost-aware in their outcome simulation (`entry_slippage_bps=2.0`, `exit_slippage_bps=2.0`, `fill_probability=0.98`, partial/missed-fill penalties), but the downstream learner itself is not CatBoost.

The union of the long/short exported downstream setup-filter schemas contains `140` unique feature columns:

| Feature group | Count | Current Java/live availability | Exact parity status |
|---|---:|---|---|
| Setup, OHLCV, basic quote, 5s quality | `49` | Mostly available from strategy state / normalized events | Partial; several quality/count fields are approximated |
| Meta/proxy model features: `tsm_*`, `regime_*`, `seq_*`, `setup_*` | `21` | Available via `EnhancedLiveFeatureProducer` | Likely reproducible as proxy features, needs numeric parity test |
| EQUS MBP1 30s silver aggregates: `EqMbp1*30s` | `28` | Not available in current Java event state | Not exact; Java uses latest quote/single-count approximations |
| OPRA TCBBO 30s silver aggregates: `OpraTcbbo*30s` | `31` | Not available in current Java event state | Not exact; Java uses option volume deltas and underlying-price approximations |
| Categorical one-hots | `11` | Available from symbol/arbitration/session | Reproducible |

The actual downstream setup training rows include all non-one-hot source columns for these groups. The one-hot columns are created by `pandas.get_dummies` during training/export.

The trained downstream filters also rely materially on the silver aggregate groups:

| Side | Meta/proxy importance | Basic importance | EQUS MBP1 importance | OPRA TCBBO importance | Categorical importance |
|---|---:|---:|---:|---:|---:|
| Long | `0.4190` | `0.3075` | `0.1511` | `0.1196` | `0.0028` |
| Short | `0.4128` | `0.3480` | `0.1116` | `0.1230` | `0.0045` |

So roughly one quarter of the model importance is in `EqMbp1*` + `OpraTcbbo*` features. Approximating or zero-filling them is enough to change pass/confirm counts.

## Per-group feasibility

### 1. `tsm_*`, `regime_*`, `seq_*`, `setup_*`

**Feasibility:** live-causal and implementable.

`feature_producers_30s.py` generates these with causal rolling proxy formulas by default. The actual training rows inspected did not contain external source-like columns such as `TimesFM_*`, `SeqLSTM_*`, `RegimeRF_*`, `RegimeLGBM_*`, `RegimeCatBoost_*`, `News*`, or `Sentiment*`, so this specific trained bundle appears to use proxy-derived meta features, not external model outputs.

Java already has corresponding proxy code in `EnhancedLiveFeatureProducer` and `PingPongStrategy.constructFeatureValueMap()` merges those values into the feature map.

Remaining work:

- Add a Python-vs-Java parity fixture for `feature_producers_30s.py` vs `EnhancedLiveFeatureProducer` on the same synthetic 30s OHLCV history.
- Confirm the Java history windows, warmup behavior, and current-bar inclusion match the Python formulas exactly.

### 2. Setup/bar/basic quote/quality fields

**Feasibility:** live-causal, but current parity is incomplete.

Examples include `Open`, `High`, `Low`, `Close`, `Volume`, `WAP`, `Ask`, `Bid`, `AskSize`, `BidSize`, `AtAskVol`, `AtBidVol`, `QuoteAgeMsMean`, `QuoteUpdateCount5s`, `TradeSecondsPresent`, and `QualityScore`.

Current Java can populate many of these from strategy state. However, `DatabentoEvent` already exposes normalized event quality fields while `PingPongStrategy.downstreamSetupFilterFeatureValues()` currently recomputes several fields using simplified logic. For exact replay/live parity, Java should consume and carry the event-quality fields from `DatabentoEvent` rather than recomputing approximations at setup-arm time.

Remaining work:

- Add strategy state for latest normalized event quality fields, or pass a quality snapshot into `PingPongStrategy.onSourceBar(...)`.
- Use the recorded/live event values for `TradeSecondsPresent`, `QuoteUpdateSecondsPresent`, `QuoteAgeMsMean`, `QuoteAgeMsMax`, `ValidSpreadCoverage`, `LockedCrossedSeconds`, `QualityScore`, etc.

### 3. `EqMbp1*30s` silver aggregates

**Feasibility:** possible in backtest/recorded/live only if the event pipeline carries enough 1-second quote/microstructure state.

Offline source: `scripts/build_databento_enriched_30s.py::aggregate_equs_1s_to_30s()`.

Training features include 30-second means, lasts, min/max spreads, quote update counts, coverage, quote age, locked/crossed coverage, and L1 imbalance from EQUS MBP/TBBO silver data.

Current Java behavior in `PingPongStrategy.downstreamSetupFilterFeatureValues()` is not equivalent. It fills many `EqMbp1*30s` fields using the latest bid/ask/size and sets counts/coverage to `0/1`. Example mismatch from training rows: `EqMbp1QuoteUpdateCount30s` ranges from `57` to `25,439` with mean about `4,368`; Java currently emits at most a simplified single-count style value.

Backtest/recorded feasibility:

- Feasible if replay emits the same 30-second aggregates from `build_databento_enriched_30s.py`, or if Java/Python streamer accumulates raw EQUS MBP/TBBO events into identical 30-second aggregate fields before scoring.

Live feasibility:

- Feasible if the live normalizer subscribes to the needed EQUS quote stream and maintains the same 30-second aggregation contract.
- Not feasible from only the current `DatabentoEvent` basic equity bar fields.

### 4. `OpraTcbbo*30s` silver aggregates

**Feasibility:** possible but much heavier; not available from current Java/live events.

Offline source: `scripts/build_databento_enriched_30s.py::aggregate_opra_1s_to_30s()`.

Training features include call/put trade counts, contract volumes, premium notional, quote context counts, active contract seconds, quote active coverage, option spread stats, at-bid/at-ask option volumes, call-minus-put volume, imbalance, and put/call ratio.

Current Java receives `option_bar` events with only underlying, right, epoch, and volume. It does not receive option trade price, premium notional, contract quote context, active contract counts, option spread stats, or at-bid/at-ask option trade classification. Java approximates some fields from put/call volume deltas and underlying close, which is not equivalent to the OPRA TCBBO training distribution.

Backtest/recorded feasibility:

- Feasible if replay uses the same OPRA TCBBO silver artifacts or an enriched NDJSON event carrying the computed 30-second `OpraTcbbo*` aggregate fields.

Live feasibility:

- Feasible only with a live OPRA TCBBO/options NBBO/trade-context pipeline and an aggregation layer equivalent to the offline silver builder.
- Not feasible from current OPRA `ohlcv-1s` volume-only events.

## Current replay/live event contract gap

`src/main/java/com/calgary/fili/trader/bot/trader/DatabentoEvent.java` is annotated with `@JsonIgnoreProperties(ignoreUnknown = true)`, and it only declares basic bar, option volume, quote snapshot, previous close, and selected quality fields. It does not retain arbitrary enriched numeric fields.

Therefore, even if recorded NDJSON rows contained `EqMbp1*`, `OpraTcbbo*`, `tsm_*`, `seq_*`, or `setup_*` columns, Java would ignore those fields unless the event contract and strategy handoff are extended.

## Recommended implementation path

### Fastest path to recorded-event parity without the setup-arm sidecar

This proves the event/replay path can carry enriched feature values, but it still does not prove live production.

1. Extend `DatabentoEvent` with an `@JsonAnySetter` numeric map, or explicit enriched-feature fields.
2. Extend the historical/recorded event builder so equity 30s/5s events can include enriched numeric columns from the enriched training CSV.
3. Add a strategy method such as `onEnrichedFeatureSnapshot(epochSec, Map<String, Float>)` or carry `event.extraNumericFields` into `PingPongStrategy` before `onSourceBar(...)`.
4. Merge those event-carried enriched values in `downstreamSetupFilterFeatureValues()` before ONNX scoring.
5. Rerun controlled replay with no setup-arm sidecar and verify `540` setup-pass arms / `539` confirmations.

This is a recorded-event parity test. It is better than an offline sidecar, but still not live parity unless the same fields are generated live.

### Proper live-causal parity path

1. Keep/validate `EnhancedLiveFeatureProducer` for `tsm_*`, `regime_*`, `seq_*`, and `setup_*` proxy features.
2. Implement an EQUS 30-second aggregator matching `aggregate_equs_1s_to_30s()`.
3. Implement an OPRA TCBBO 30-second aggregator matching `aggregate_opra_1s_to_30s()`.
4. Feed those aggregate snapshots into Java at the same 30-second setup-arm boundary used by training.
5. Add fixture-level parity tests:
   - Python `build_databento_enriched_30s.py` output vs Java EQUS/OPRA aggregator output.
   - Python `feature_producers_30s.py` output vs Java `EnhancedLiveFeatureProducer` output.
6. Rerun no-sidecar controlled replay and compare lifecycle summaries to the offline selected policy.

### Safer promotion-oriented alternative — implemented as CatBoost core branch

If live OPRA TCBBO and exact EQUS aggregate parity are too heavy or unavailable, retrain the downstream setup filter on a **live-available schema only**:

- setup/bar/basic OHLCV,
- basic quote snapshot,
- causal Java meta proxies,
- possibly simple option call/put volume deltas if those are truly live/replay available.

Then export a new ONNX bundle and run no-sidecar controlled replay. This is likely the cleaner promotion path because it avoids depending on offline silver-only feature groups.

Implementation status: the CatBoost core downstream filter was trained/evaluated under `downstream_setup_filter_catboost_core_20260628/` and exported under `downstream_setup_filter_onnx_catboost_core_20260628/`. Its schema has `59` features per side and excludes `32` enriched meta columns plus `67` silver aggregate columns from the replay-observed setup-arm training rows.

Because the upstream setup generator is already CatBoost cost-aware, the next downstream experiment should be explicit about whether it is:

1. preserving the current two-stage design but replacing the second-stage RandomForest with CatBoost/regression/ranking, or
2. folding downstream expected-net-R objectives back into a new single-stage CatBoost setup model.

Either path should report both OOF setup precision and replay downstream expected/realized net-R metrics; CatBoost setup precision alone was already investigated separately and remained below the desired threshold under the June 24 cost-aware labels.

## Decision

The missing enriched features can be produced in principle, but the current Java backtest/recorded/live path cannot produce the full current training schema with exact parity. The biggest blockers are the `EqMbp1*30s` and `OpraTcbbo*30s` aggregate families, not the proxy `tsm/regime/seq/setup` features.

Until those aggregate producers are implemented and parity-tested, or the downstream setup filter is retrained on live-available features, the current sidecar-matched result remains **research-only / NO-GO**.

