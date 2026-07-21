# Event-carried enriched snapshots for live/paper parity

This runbook describes the production path for carrying research-grade downstream setup-filter features with Databento events instead of relying on a local CSV sidecar.

## Contract

`DatabentoEvent` now accepts enriched numeric features in either form:

1. Nested snapshot object:

```json
{
  "event": "equity_bar",
  "symbol": "AAPL",
  "barEpochSec": 1778767029,
  "FeatureSnapshotEpochSec": 1778767050,
  "FeatureSnapshotSchemaVersion": "downstream_setup_filter_onnx_research_v1",
  "FeatureSnapshotSource": "silver_30s",
  "enriched_features": {
    "ImbalanceStd5s": 177024.84,
    "AskSizeLast": 1975,
    "BidSizeLast": 300,
    "CallVolDelta5s": 300,
    "PutVolDelta5s": 62
  }
}
```

2. Top-level numeric feature columns:

```json
{
  "event": "equity_bar",
  "symbol": "AAPL",
  "barEpochSec": 1778767029,
  "FeatureSnapshotEpochSec": 1778767050,
  "FeatureSnapshotSchemaVersion": "downstream_setup_filter_onnx_research_v1",
  "FeatureSnapshotSource": "silver_30s",
  "ImbalanceStd5s": 177024.84,
  "AskSizeLast": 1975,
  "BidSizeLast": 300
}
```

Accepted aliases:

- `FeatureSnapshotEpochSec`, `feature_snapshot_epoch_sec`, `SetupEpochSec`, `setup_epoch_sec`
- `FeatureSnapshotSchemaVersion`, `feature_snapshot_schema_version`
- `FeatureSnapshotSource`, `feature_snapshot_source`
- `enriched_features`, `EnrichedFeatures`, `feature_snapshot`, `FeatureSnapshot`, `featureSnapshot`

Only finite numeric values are retained. Non-numeric text, `NaN`, and infinities are ignored.

## Runtime flow

1. `DatabentoEvent` parses the enriched numeric map.
2. `IBKRTrader` and `DatabentoHistoricalStreamingBacktester` forward it through `PingPongStrategy.onEnrichedFeatureSnapshot(...)` before the matching source bar.
3. `PingPongStrategy` stores the snapshot by arm epoch (`FeatureSnapshotEpochSec`, falling back to `barEpochSec`).
4. At downstream setup-filter scoring time, feature precedence is:

   ```text
   Java live-computed features
     < event-carried enriched snapshot
     < CSV sidecar exact research row
     < categorical one-hots added by Java
   ```

The CSV sidecar intentionally remains highest precedence so controlled replays can still force exact research rows and compare against the event-carried path.

The event snapshot cache is bounded by:

```text
-Dstrategy.downstreamSetupFilter.eventSnapshotCacheRows=2048
```

## Telemetry

When a snapshot is accepted:

```text
[FLOW][DATA][AI.DOWNSTREAM_SETUP_FILTER.SNAPSHOT] symbol=AAPL epoch=1778767050 featureCount=59 schemaVersion=... source=silver_30s cacheRows=1 status=research_to_live_parity
```

When the downstream setup filter scores:

```text
[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=PASS | symbol=AAPL side=long ... armEpoch=1778767050 ... featureSnapshot=hit featureSidecar=disabled status=research_only_no_go
```

`featureSnapshot` values:

- `hit`: event-carried enriched snapshot was found for the arm epoch.
- `miss`: event-carried snapshot cache exists, but no row matched this arm epoch.
- `disabled`: no event-carried snapshots have been seen in this strategy process.

## Controlled replay fixture generation

Use `scripts/inject_downstream_setup_features_into_ndjson.py` to create a historical NDJSON fixture that carries the exact downstream setup-filter sidecar row on the source `equity_bar` event that triggers/observes the 30-second bucket finalization. By default the injector assigns each setup row to the first equity bar at or after `SetupEpochSec - 1`, while preserving the original arm epoch in `FeatureSnapshotEpochSec`. This matches `PingPongStrategy` setting `current30sAiDecisionEpoch = finalizedBucketStart + 30` when the prior 30-second bucket finalizes. It validates the production event-carried path without passing `--downstream-setup-filter-features-csv` to Java.

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz
SIDECAR=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
MANIFEST=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated/downstream_setup_filter_route_manifest.json
ENRICHED_EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.event-snapshots-catboost-core-20260720.ndjson.gz

python3 scripts/inject_downstream_setup_features_into_ndjson.py \
  --input-events "$EVENTS" \
  --sidecar-csv "$SIDECAR" \
  --manifest "$MANIFEST" \
  --output-events "$ENRICHED_EVENTS" \
  --snapshot-source setup_downstream_training_rows_v1
```

The injector writes `<output-events>.summary.json` and returns non-zero unless every sidecar row matches an `equity_bar` event, unless `--allow-unmatched-sidecar` is explicitly supplied.

Useful carrier-placement options:

- `--carrier-policy first-at-or-after` — default; carrier event is first `equity_bar` at or after `SetupEpochSec - 1`.
- `--carrier-policy exact` — require `barEpochSec == SetupEpochSec`; useful only when the source stream has exact setup-epoch bars.
- `--max-carrier-lag-seconds 300` — default guard for sparse-event gaps; use `-1` for no maximum.

## No-sidecar event-snapshot replay

After building Java, run no-trade replay with the enriched NDJSON and **without** `--downstream-setup-filter-features-csv`:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
ENRICHED_EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.event-snapshots-catboost-core-20260720.ndjson.gz
MANIFEST=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated/downstream_setup_filter_route_manifest.json
OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade

mkdir -p "$OUT"
PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
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

Validate the no-sidecar replay against sidecar keys and the prior sidecar-exact reference log:

```zsh
BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade
SIDECAR=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
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

Acceptance target for the controlled no-trade fixture:

- `[BACKTEST] completed=5 failed=0 requested=5`
- `featureSnapshot=hit` on every `SETUP_FILTER_PASSES` row
- `featureSidecar=disabled` on every `SETUP_FILTER_PASSES` row
- event replay setup keys match the `4470` sidecar keys
- setup pass/fail decisions and probabilities match the sidecar-exact reference within tolerance

## File/NDJSON historical backtester command cookbook

The file/NDJSON historical backtester supports both downstream setup-filter feature paths:

1. **Event-carried enriched mode** — prepare an enriched `.ndjson.gz` containing `enriched_features`, then replay that file without `--downstream-setup-filter-features-csv`. This is the recommended production-parity path.
2. **CSV sidecar mode** — replay the original recorded NDJSON and pass `--downstream-setup-filter-features-csv` so Java loads the research rows directly.

### 1. Set common paths

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz
SIDECAR=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
MANIFEST=$BASE/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260718_validated/downstream_setup_filter_route_manifest.json
ENRICHED_EVENTS=$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.event-snapshots-catboost-core-20260720.ndjson.gz
```

### 2. Prepare the enriched NDJSON file

This command writes the exact downstream setup-filter sidecar features into carrier `equity_bar` events in the recorded stream:

```zsh
python3 scripts/inject_downstream_setup_features_into_ndjson.py \
  --input-events "$EVENTS" \
  --sidecar-csv "$SIDECAR" \
  --manifest "$MANIFEST" \
  --output-events "$ENRICHED_EVENTS" \
  --snapshot-source setup_downstream_training_rows_v1
```

Expected success shape:

```text
INJECT_EVENT_SNAPSHOTS status=PASS ... injected=4470 expected=4470 unmatched=0
```

Inspect the summary:

```zsh
python3 -m json.tool "$ENRICHED_EVENTS.summary.json" | head -120

python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(json.dumps(r["injection"], indent=2)); print(json.dumps(r["carrier_assignment"], indent=2))' \
  "$ENRICHED_EVENTS.summary.json"
```

Expected important fields:

```text
snapshot_rows_expected: 4470
snapshot_rows_injected: 4470
snapshot_rows_unmatched: 0
multi_snapshot_carrier_events: 0
snapshot_hit_rate_vs_sidecar: 1.0
```

### 3. Build Java before `SKIP_BUILD=true` replays

```zsh
./mvnw -q -DskipTests package
```

### 4. Event-carried enriched replay — no-trade

This is the primary production-parity file replay. It intentionally omits `--downstream-setup-filter-features-csv` so all downstream setup-filter research features come from the event-carried snapshots.

```zsh
OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade_rerun

rm -rf "$OUT"
mkdir -p "$OUT"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
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

Check completion and telemetry counts:

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

Validated target from the controlled run:

```text
[BACKTEST] completed=5 failed=0 requested=5
SETUP_FILTER_PASSES=PASS      296
SETUP_FILTER_PASSES=FAIL      4174
featureSnapshot=hit           4470
featureSnapshot=miss          0
featureSidecar=disabled       4470
MICRO_ENTRY_CONFIRMS=PASS     284
MICRO_ENTRY_CONFIRMS=FAIL     506
```

### 5. Validate event-snapshot replay drift

Compare the event-carried replay against the sidecar-exact reference replay:

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

Print key metrics:

```zsh
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); m=r["gate"]["metrics"]; print("overall", r["overall_status"]); print("event_rows", m["event_rows"]); print("pass_fail", m["pass_count"], m["fail_count"]); print("feature_snapshot_counts", m["feature_snapshot_counts"]); print("feature_sidecar_counts", m["feature_sidecar_counts"]); print("reference", {k: m["reference"][k] for k in ["paired_reference_rows","max_probability_drift","decision_mismatches","event_only_rows_vs_reference","reference_only_rows"]})' \
  "$OUT/event_snapshot_replay_drift/event_snapshot_replay_drift_report.json"
```

### 6. Optional offline-policy comparisons

```zsh
OFFLINE=$BASE/setup_micro_counterfactual_20260627_230823/downstream_filter_replay_policy_eval_catboost_core_20260718_revalidated/downstream_filter_replay_policy_decisions.csv

python3 scripts/compare_downstream_setup_filter_replay.py \
  --replay-log "$OUT/controlled_java_replay.log" \
  --offline-decisions "$OFFLINE" \
  --top 8
```

Expected important lines:

```text
java_rows=4470
matched_rows=296
matched_pass_counts java=296 offline=296 disagreements=0
```

Micro-confirmation comparison:

```zsh
python3 scripts/compare_micro_entry_confirmations.py \
  --replay-log "$OUT/controlled_java_replay.log" \
  --offline-decisions "$OFFLINE" \
  --top 8
```

Known controlled-run result:

```text
offline_confirms=285
java_confirm_rows=284
left_only 1
TQQQ short arm_epoch=1778767050
```

The one missing Java confirmation is the known threshold-boundary/timing case at exactly `0.30`.

### 7. Event-carried enriched replay — trade-enabled

After the no-trade replay and drift gate pass, run the same enriched file with trading enabled by removing `--micro-entry-research-no-trade`:

```zsh
TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_trade

rm -rf "$TRADE_OUT"
mkdir -p "$TRADE_OUT"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-dir "$TRADE_OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --timeout-seconds 0 \
  > "$TRADE_OUT/controlled_java_replay.log" 2>&1
```

Check completion and counts:

```zsh
grep '\[BACKTEST\] completed' "$TRADE_OUT/controlled_java_replay.log" | tail -5

printf 'SETUP PASS '; grep -c 'SETUP_FILTER_PASSES=PASS' "$TRADE_OUT/controlled_java_replay.log"
printf 'SETUP FAIL '; grep -c 'SETUP_FILTER_PASSES=FAIL' "$TRADE_OUT/controlled_java_replay.log"
printf 'MICRO PASS '; grep -c 'MICRO_ENTRY_CONFIRMS=PASS' "$TRADE_OUT/controlled_java_replay.log"
printf 'MICRO FAIL '; grep -c 'MICRO_ENTRY_CONFIRMS=FAIL' "$TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT HIT '; grep -c 'featureSnapshot=hit' "$TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT MISS '; grep -c 'featureSnapshot=miss' "$TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR DISABLED '; grep -c 'featureSidecar=disabled' "$TRADE_OUT/controlled_java_replay.log"
```

Summarize trade-enabled replay:

```zsh
python3 scripts/summarize_trade_enabled_replay.py \
  --output-dir "$TRADE_OUT" \
  --log-file "$TRADE_OUT/controlled_java_replay.log" \
  --summary-json "$TRADE_OUT/trade_enabled_event_snapshot_summary_20260720.json" \
  --summary-md "$TRADE_OUT/trade_enabled_event_snapshot_summary_20260720.md"
```

### 8. CSV sidecar replay mode

Use this mode when you want the Java replay to read the original recorded NDJSON and directly apply the exact research sidecar rows:

```zsh
SIDECAR_OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_sidecar_notrade_rerun

rm -rf "$SIDECAR_OUT"
mkdir -p "$SIDECAR_OUT"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$EVENTS" \
  --output-dir "$SIDECAR_OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --downstream-setup-filter-features-csv "$SIDECAR" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --micro-entry-research-no-trade \
  --timeout-seconds 0 \
  > "$SIDECAR_OUT/controlled_java_replay.log" 2>&1
```

Expected sidecar-mode markers:

```text
featureSidecar=hit
featureSnapshot=disabled
```

### Recommended compact sequence

For the clean productized path, run this sequence after setting the common paths:

```zsh
./mvnw -q -DskipTests package

python3 scripts/inject_downstream_setup_features_into_ndjson.py \
  --input-events "$EVENTS" \
  --sidecar-csv "$SIDECAR" \
  --manifest "$MANIFEST" \
  --output-events "$ENRICHED_EVENTS" \
  --snapshot-source setup_downstream_training_rows_v1

OUT=$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260720_event_snapshot_notrade_rerun
rm -rf "$OUT"
mkdir -p "$OUT"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-dir "$OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --micro-entry-research-no-trade \
  --timeout-seconds 0 \
  > "$OUT/controlled_java_replay.log" 2>&1

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

## Paper/shadow drift validation

Compare paired paper/shadow Java logs:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/validate_paper_shadow_drift.py \
  --paper-log runtime/paper/paper.log \
  --shadow-log runtime/shadow/shadow.log \
  --output-dir runtime/reports/paper_shadow_drift \
  --max-probability-drift 0.02 \
  --max-decision-mismatch-rate 0.01 \
  --min-feature-snapshot-hit-rate 0.95 \
  --fail-on-no-go
```

Or compare decision CSVs with generic columns such as `symbol`, `side`, `armEpoch`/`arm_epoch`, `prob`/`probability`, `passed`/`decision`, and optionally `featureSnapshot`:

```zsh
python3 scripts/validate_paper_shadow_drift.py \
  --paper-decisions runtime/paper/paper-decisions.csv \
  --shadow-decisions runtime/shadow/shadow-decisions.csv \
  --output-dir runtime/reports/paper_shadow_drift \
  --fail-on-no-go
```

Outputs:

- `runtime/reports/paper_shadow_drift/paper_shadow_drift_report.json`
- `runtime/reports/paper_shadow_drift/paper_shadow_drift_report.md`

The validator checks paired row count, probability drift, decision mismatch rate, route mismatch rate, `featureSnapshot` status mismatch rate, and per-side snapshot hit rates.

## Promotion checklist

- [ ] Event producer emits `FeatureSnapshotEpochSec` equal to the downstream setup arm epoch.
- [ ] Paper and shadow logs show `featureSnapshot=hit` for at least the configured hit-rate threshold.
- [ ] `max_probability_drift` is within the configured threshold.
- [ ] `decision_mismatch_rate` is within the configured threshold.
- [ ] Existing lifecycle promotion gate consumes either the generated drift report or paired decision CSVs.
