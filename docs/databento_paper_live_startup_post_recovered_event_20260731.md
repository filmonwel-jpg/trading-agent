# Databento paper/live startup after recovered event-carried replay validation — 2026-07-31

This runbook documents how the validated recovered event-carried backtest maps into Databento paper/live trading.

It follows the July 31, 2026 apples-to-apples parallel replay that used:

```text
Recovered event-carried historical file
No CSV sidecar
Downstream setup-filter manifest
Lifecycle/micro enabled
Micro entry thresholds long=0.30 short=0.30
trade_amount=60000
max_order_notional=300000
parallel symbol execution
```

That run reproduced the recovered sequential lifecycle/micro decision path:

```text
completed=5 failed=0 requested=5
setup_pass=761
setup_fail=38702
micro_pass=417
micro_fail=4233
snapshot_hit=39463
snapshot_miss=0
sidecar_disabled=39463
sidecar_hit=0
trades=417
wins=279
losses=138
win_rate=66.91%
sum_realized_r=143.6226R
```

## Key distinction: backtest inputs vs live/paper inputs

### Historical replay / backtest

The recovered replay uses a recorded file:

```zsh
--recorded-events "$RECORDED_EVENTS"
```

That means Java reads historical Databento NDJSON/NDJSON.GZ events. In the recovered event-carried run, the file already contains enriched downstream setup-filter feature snapshots.

### Live/paper trading

Live/paper does **not** use `--recorded-events`.

Live/paper uses Databento live/shared-feed settings from each symbol bot configuration:

```text
trading.market-data.provider=databento
trading.databento.shared-feed.enabled=true
trading.databento.shared-feed.host=127.0.0.1
trading.databento.shared-feed.port=<shard port>
```

Therefore, for paper/live:

```text
recorded-events: not used
CSV sidecar: not used
Databento source: live/shared feed
feature snapshots: disabled by default, enabled only with the live normalizer flag below
```

The live normalizer now supports an opt-in event-carried snapshot mode:

```zsh
--emit-live-feature-snapshots
```

When enabled, the normalizer computes a live 30-second feature snapshot from the normalized equity stream and attaches it to the carrier `equity_bar` event as:

```text
FeatureSnapshotEpochSec=<30s bucket start + 30>
FeatureSnapshotSource=live_normalizer_30s_v1
FeatureSnapshotSchemaVersion=live_normalizer_30s_v1
enriched_features={...}
```

Expected downstream setup-filter feature-source telemetry when this mode is enabled:

```text
featureSnapshot=hit
featureSidecar=disabled
```

If this mode is not enabled, expected telemetry remains:

```text
featureSnapshot=disabled
featureSidecar=disabled
```

This is different from the recovered historical replay, where the enriched file produced:

```text
featureSnapshot=hit
featureSidecar=disabled
```

Important: `FeatureSnapshotSource=live_normalizer_30s_v1` is a live-computed carrier snapshot, not a recovered historical research row. It exercises the same Java event-carried snapshot path and reduces live/replay feature-source drift, but it should not be labeled as exact recovered research parity.

## Current live/paper readiness status

The five target bot property files exist:

```text
runtime/databento/bots/trading-nvda.properties
runtime/databento/bots/trading-qqq.properties
runtime/databento/bots/trading-spy.properties
runtime/databento/bots/trading-tqqq.properties
runtime/databento/bots/trading-tsla.properties
```

They are configured for Databento live market data and shared capital:

```text
trading.market-data.provider=databento
trading.shared-capital.enabled=true
trading.shared-capital.file=runtime/databento/shared-capital.properties
trading.shared-capital.total-notional=500000
```

The shared capital file is:

```text
runtime/databento/shared-capital.properties
```

with expected total pool:

```text
total.notional=500000.00
```

## Settings needed to match the final 417-trade strategy path

The generated live property files currently default to:

```text
trading.trade-amount=500000
trading.risk.max-order-notional=500000
trading.risk.max-share-cap=2000
```

The final replay used:

```text
trade_amount=60000
max_order_notional=300000
max_share_cap=2000
micro_long_entry_threshold=0.30
micro_short_entry_threshold=0.30
```

So paper/live startup should override:

```zsh
--trade-amount=60000
--max-order-notional=300000
--max-share-cap=2000
TRADING_MICRO_LONG_ENTRY_THRESHOLD=0.30
TRADING_MICRO_SHORT_ENTRY_THRESHOLD=0.30
```

Also set explicit model directories so paper/live does not accidentally use stale property-file model paths:

```zsh
TRADING_SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
TRADING_LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
```

## Model and manifest paths

Use these paths for the recovered setup:

```zsh
SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
```

Required 30-second setup model artifacts:

```text
long_entry.onnx
short_entry.onnx
regime_classifier.onnx
choppy_long_entry.onnx
choppy_short_entry.onnx
trend_long_entry.onnx
trend_short_entry.onnx
volatile_long_entry.onnx
volatile_short_entry.onnx
open30_long_entry.onnx
open30_short_entry.onnx
setup_runtime_thresholds.properties
setup_manifest.json
```

Required lifecycle/micro artifacts:

```text
long_exit_lifecycle.onnx
short_exit_lifecycle.onnx
long_micro_entry_5s.onnx
short_micro_entry_5s.onnx
long_micro_exit_guard_5s.onnx
short_micro_exit_guard_5s.onnx
lifecycle_micro_scorecard.csv
lifecycle_micro_route_manifest.json
```

Required downstream setup-filter artifact:

```text
downstream_setup_filter_route_manifest.json
```

The startup command below validates the manifest before launching. If the file is missing, startup fails closed before any bot starts.

## Launcher enhancement added

The live launch scripts now support first-class downstream setup-filter startup flags:

```text
run_symbol.sh
start_all_databento_bots.sh
scripts/databento_live_normalizer.py
scripts/databento_shared_feed_relay.py
scripts/live_feature_snapshots.py
```

New launcher options:

```zsh
--downstream-setup-filter-manifest=PATH
--downstream-setup-filter-fail-closed=true|false
--disable-downstream-setup-filter   # single-symbol launcher only
--emit-live-feature-snapshots
```

The launcher converts the manifest option into JVM properties:

```text
-Dstrategy.downstreamSetupFilter.enabled=true
-Dstrategy.downstreamSetupFilter.routeManifest=<PATH>
-Dstrategy.downstreamSetupFilter.failClosed=true
```

It intentionally does **not** set `strategy.downstreamSetupFilter.featuresCsv`, so there is no CSV sidecar leakage in paper/live.

`--emit-live-feature-snapshots` converts into:

```text
--trading.databento.feature-snapshots.enabled=true
```

Java forwards that property to either the dedicated normalizer or the shared relay. The relay then starts the normalizer with:

```text
--emit-live-feature-snapshots
--feature-snapshot-source live_normalizer_30s_v1
--feature-snapshot-schema-version live_normalizer_30s_v1
```

Operational caveat: the shared relay only receives these arguments when it is started. If a `databento_shared_feed_relay.py` process is already running from an older launch without `--emit-live-feature-snapshots`, later bot launches will connect to that existing relay and will not retrofit the normalizer arguments. Before the first paper/live start that changes this flag, stop the existing Databento stack/relay after confirming it is safe to stop:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

# Preview first; default selection covers all discovered Databento bot configs,
# which is the mode that also stops the shared Databento relay.
./stop_all_databento_bots.sh --dry-run --copy-live-logs-on-stop

# Run only after positions/open orders are safe according to the dry-run/control checks.
./stop_all_databento_bots.sh --copy-live-logs-on-stop
```

Then launch with `--emit-live-feature-snapshots` and confirm the relay log shows the normalizer command includes that flag.

## Exact paper/live behavior

With the command below, live/paper flow is:

```text
Databento live/shared feed
  -> Java 30s setup models
  -> downstream setup-quality filter
  -> 5s micro-entry confirmation at 0.30/0.30
  -> IBKR paper order through shared IBKR gateway
  -> lifecycle exits / micro exit guards
  -> shared capital reservation across symbols
```

Sizing behavior:

```text
trade_amount=60000
strategy share quantity = floor(60000 / executable_price)
strategy internal absolute cap = 500 shares
broker-side max share cap = 2000 shares
max_order_notional=300000
shared_capital.total_notional=500000 across all selected symbol bots
```

Actual entry notional per trade is:

```text
Quantity × EntryPrice
```

It is not exactly `$60,000` because share quantity is floored and capped.

## Preflight verification

Run this before starting paper/live:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

./mvnw -q -DskipTests package

SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

[[ -d "$SETUP_MODEL_DIR" ]] || { echo "Missing SETUP_MODEL_DIR: $SETUP_MODEL_DIR"; exit 1; }
[[ -d "$LIFECYCLE_MODEL_DIR" ]] || { echo "Missing LIFECYCLE_MODEL_DIR: $LIFECYCLE_MODEL_DIR"; exit 1; }
[[ -f "$ROUTE_MANIFEST" ]] || { echo "Missing ROUTE_MANIFEST: $ROUTE_MANIFEST"; exit 1; }

for f in \
  "$SETUP_MODEL_DIR/long_entry.onnx" \
  "$SETUP_MODEL_DIR/short_entry.onnx" \
  "$SETUP_MODEL_DIR/regime_classifier.onnx" \
  "$SETUP_MODEL_DIR/choppy_long_entry.onnx" \
  "$SETUP_MODEL_DIR/choppy_short_entry.onnx" \
  "$SETUP_MODEL_DIR/trend_long_entry.onnx" \
  "$SETUP_MODEL_DIR/trend_short_entry.onnx" \
  "$SETUP_MODEL_DIR/volatile_long_entry.onnx" \
  "$SETUP_MODEL_DIR/volatile_short_entry.onnx" \
  "$SETUP_MODEL_DIR/open30_long_entry.onnx" \
  "$SETUP_MODEL_DIR/open30_short_entry.onnx" \
  "$SETUP_MODEL_DIR/setup_runtime_thresholds.properties" \
  "$SETUP_MODEL_DIR/setup_manifest.json" \
  "$LIFECYCLE_MODEL_DIR/long_exit_lifecycle.onnx" \
  "$LIFECYCLE_MODEL_DIR/short_exit_lifecycle.onnx" \
  "$LIFECYCLE_MODEL_DIR/long_micro_entry_5s.onnx" \
  "$LIFECYCLE_MODEL_DIR/short_micro_entry_5s.onnx" \
  "$LIFECYCLE_MODEL_DIR/long_micro_exit_guard_5s.onnx" \
  "$LIFECYCLE_MODEL_DIR/short_micro_exit_guard_5s.onnx" \
  "$LIFECYCLE_MODEL_DIR/lifecycle_micro_scorecard.csv" \
  "$LIFECYCLE_MODEL_DIR/lifecycle_micro_route_manifest.json"
do
  [[ -f "$f" ]] || { echo "Missing required artifact: $f"; exit 1; }
done

./reset_shared_capital.sh --check
```

## Preview startup without starting bots

No `--start` means preview-only.

Keep the explicit `--` separator and pass `--trading.model.dir="$SETUP_MODEL_DIR"` after it. The local startup-data verifier resolves the 30-second setup model directory from this extra Spring argument; `TRADING_SETUP_MODEL_DIR` alone does not override stale `trading.model.dir` values in generated bot properties during that preflight.

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

TRADING_SETUP_MODEL_DIR="$SETUP_MODEL_DIR" \
TRADING_LIFECYCLE_MODEL_DIR="$LIFECYCLE_MODEL_DIR" \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_MICRO_LONG_ENTRY_THRESHOLD=0.30 \
TRADING_MICRO_SHORT_ENTRY_THRESHOLD=0.30 \
./start_all_databento_bots.sh \
  --symbols=NVDA,QQQ,SPY,TQQQ,TSLA \
  --trade-amount=60000 \
  --max-order-notional=300000 \
  --max-share-cap=2000 \
  --max-trades=0 \
  --startup-history-seconds=360 \
  --emit-live-feature-snapshots \
  --downstream-setup-filter-manifest="$ROUTE_MANIFEST" \
  -- \
  --trading.model.dir="$SETUP_MODEL_DIR"
```

Confirm each symbol preview prints:

```text
[RUN] model_dir=...catboost_cost_aware_setup_onnx_local_20260624_152854
[RUN] lifecycle_micro_enabled=true lifecycle_model_dir=...lifecycle_micro_external_oof_20260624_120527/model_exports
[RUN] micro_entry_thresholds long=0.30 ... short=0.30 ...
[RUN] downstream_setup_filter enabled=true manifest=...downstream_setup_filter_route_manifest.json features_csv=<none> fail_closed=true
[RUN] live_feature_snapshots enabled=true
[RUN] trade_amount=60000 max_order_notional=300000 max_share_cap=2000
```

## Safe no-trade startup verification

Start the full paper/live stack but block entries with `--max-trades=0`:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

TRADING_SETUP_MODEL_DIR="$SETUP_MODEL_DIR" \
TRADING_LIFECYCLE_MODEL_DIR="$LIFECYCLE_MODEL_DIR" \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_MICRO_LONG_ENTRY_THRESHOLD=0.30 \
TRADING_MICRO_SHORT_ENTRY_THRESHOLD=0.30 \
./start_all_databento_bots.sh \
  --start \
  --symbols=NVDA,QQQ,SPY,TQQQ,TSLA \
  --trade-amount=60000 \
  --max-order-notional=300000 \
  --max-share-cap=2000 \
  --max-trades=0 \
  --startup-history-seconds=360 \
  --emit-live-feature-snapshots \
  --downstream-setup-filter-manifest="$ROUTE_MANIFEST" \
  --tee \
  -- \
  --trading.model.dir="$SETUP_MODEL_DIR"
```

Status checks for the five current ports:

```zsh
for port in 9137 9210 9180 9219 9164; do
  echo "===== port $port ====="
  curl -fsS "http://127.0.0.1:$port/api/control/status" | python3 -m json.tool | head -80
done
```

## Controlled paper start with trades enabled

After the no-trade startup is healthy, stop, reset shared capital, and restart with a small trade cap:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

./stop_all_databento_bots.sh --symbols=NVDA,QQQ,SPY,TQQQ,TSLA --copy-live-logs-on-stop
./reset_shared_capital.sh --reset
```

Then start controlled paper:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

SETUP_MODEL_DIR=runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
LIFECYCLE_MODEL_DIR=runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

TRADING_SETUP_MODEL_DIR="$SETUP_MODEL_DIR" \
TRADING_LIFECYCLE_MODEL_DIR="$LIFECYCLE_MODEL_DIR" \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_MICRO_LONG_ENTRY_THRESHOLD=0.30 \
TRADING_MICRO_SHORT_ENTRY_THRESHOLD=0.30 \
./start_all_databento_bots.sh \
  --start \
  --symbols=NVDA,QQQ,SPY,TQQQ,TSLA \
  --trade-amount=60000 \
  --max-order-notional=300000 \
  --max-share-cap=2000 \
  --max-trades=2 \
  --startup-history-seconds=360 \
  --emit-live-feature-snapshots \
  --downstream-setup-filter-manifest="$ROUTE_MANIFEST" \
  --tee \
  -- \
  --trading.model.dir="$SETUP_MODEL_DIR"
```

Increase `--max-trades` only after the no-trade and low-trade paper run are healthy.

## Monitoring commands

### Effective startup configuration

```zsh
grep -hE 'lifecycle_micro_enabled|micro_entry_thresholds|downstream_setup_filter|trade_amount|max_order_notional|sharedCapital' \
  runtime/databento/logs/trading-agent-{NVDA,QQQ,SPY,TQQQ,TSLA}.log | tail -200
```

### Setup/micro/filter decisions

```zsh
grep -hE 'SETUP_FILTER_PASSES|MICRO_ENTRY_CONFIRMS|featureSnapshot=|featureSidecar=' \
  runtime/databento/logs/trading-agent-{NVDA,QQQ,SPY,TQQQ,TSLA}.log | tail -200
```

### Order lifecycle risk

```zsh
grep -hE 'WATCHDOG|Order hung|ERROR|Exception|Shared capital reserved|shared capital unavailable' \
  runtime/databento/logs/trading-agent-{NVDA,QQQ,SPY,TQQQ,TSLA}.log | tail -200
```

Healthy expected settings:

```text
lifecycle_micro_enabled=true
micro_entry_thresholds long=0.30 short=0.30
downstream_setup_filter enabled=true ... features_csv=<none> fail_closed=true
trade_amount=60000 max_order_notional=300000 max_share_cap=2000
sharedCapital enabled=true total=500000
```

Expected feature source with `--emit-live-feature-snapshots`:

```text
featureSnapshot=hit
featureSidecar=disabled
```

Expected source if the flag is omitted:

```text
featureSnapshot=disabled
featureSidecar=disabled
```

## Stop / emergency controls

Stop selected bots safely:

```zsh
./stop_all_databento_bots.sh --symbols=NVDA,QQQ,SPY,TQQQ,TSLA --copy-live-logs-on-stop
```

If positions are not flat, use flatten workflow first rather than unsafe stopping.

Reset stale shared capital only after confirming all bots are flat:

```zsh
./reset_shared_capital.sh --check
./reset_shared_capital.sh --reset
```

Use `--force` only after manual confirmation that all positions are flat and the file is stale.

## Promotion notes

This setup is suitable for a controlled paper pilot, not immediate full-size unattended trading.

Start with:

```text
--max-trades=0   # no-trade connectivity/config validation
--max-trades=2   # small controlled paper pilot
```

Do not scale until:

- startup logs confirm exact model paths and `0.30/0.30` micro thresholds;
- downstream filter loads the manifest successfully;
- no CSV sidecar is configured;
- shared capital shows correct total and reservations;
- watchdog/order lifecycle behavior is clean;
- Databento shared feed remains healthy through startup history and live stream;
- paper fills/position sync are reliable through the shared IBKR gateway.

## Files changed to support this runbook

```text
run_symbol.sh
start_all_databento_bots.sh
scripts/databento_event_contract.py
scripts/databento_live_normalizer.py
scripts/databento_shared_feed_relay.py
scripts/live_feature_snapshots.py
tests/test_databento_event_contract.py
tests/test_live_feature_snapshots.py
```

The changes add launcher-level support for the downstream setup-filter manifest so paper/live startup can use the same manifest as the recovered backtest without relying on fragile `TRADING_AGENT_JAVA_OPTS`. They also add opt-in live event-carried feature snapshots so the Java downstream setup filter sees `featureSnapshot=hit` in paper/live when the normalizer has produced a live 30-second snapshot for the setup arm epoch.
