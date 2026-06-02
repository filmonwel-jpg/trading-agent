# Databento -> IBKR Upgrade Workspace

This worktree is your isolated upgrade lane for scaling to 100 symbols with Databento market data and IBKR order routing.

Visual architecture diagram: [`docs/databento-ibkr-data-flow.md`](docs/databento-ibkr-data-flow.md)
or browser view [`docs/databento-ibkr-data-flow.html`](docs/databento-ibkr-data-flow.html).

## What is already set up

- Dedicated git branch: `feature/databento-100-ibkr`
- Dedicated worktree path: `worktrees/databento`
- Dedicated upgrade project scaffold: `databento_ibkr_bridge/`
- Dedicated runtime namespace in this worktree:
  - `runtime/databento/logs`
  - `runtime/databento/state`
  - `runtime/databento/output`

This lets your original stack keep running in `/Users/filmonghezehey/trading-agent` while you build here.

## One-time bootstrap (already safe to re-run)

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
./scripts/bootstrap_databento_workspace.sh
```

## Start coding immediately

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 src/databento_ibkr_bridge/bridge_runner.py --dry-run
```

## Java live stack upgrade path

The Spring Boot trader can now run with Databento as the live market-data source while keeping IBKR for positions and execution.

1. Build the mixed baseline/enhanced routing file from the compare-run artifacts:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 runtime/build_mixed_model_routing_20260407.py
```

2. Generate the per-symbol Databento bot property files under `runtime/databento/bots/`:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/generate_databento_bot_properties.py
```

This also writes `runtime/databento/bot-thresholds.csv`, which contains the per-symbol entry/exit thresholds derived from the selected training/test artifacts.

3. Copy/customize one of the generated files, or use `runtime/databento/bots/trading-databento-template.properties` as the template for a new symbol/process.

4. Export your Databento key and start the Java app with the Databento provider enabled in that properties file:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
export DATABENTO_API_KEY="<your-key>"
./mvnw spring-boot:run -Dspring-boot.run.arguments="--spring.config.additional-location=file:runtime/databento/bots/trading-spy.properties"
```

The Java app launches `scripts/databento_live_normalizer.py`, receives normalized live TBBO + OPRA 1s events, builds 30-second bars/features in `PingPongStrategy`, resolves the mixed job bundle from `runtime/databento/model-routing.csv`, and routes orders to IBKR using the latest ask as the limit-price reference.

## Feed resilience and health visibility

- The Databento sidecar is now supervised and will be restarted automatically if the process exits or the feed goes silent past the configured threshold during market hours.
- New stale-quote protection blocks order placement when the Databento quote for the active symbol is too old. Closing `MKT` orders can still be allowed via `trading.databento.allow-stale-closing-market-order=true`.
- New per-symbol feed-health endpoint:

```bash
curl http://127.0.0.1:9081/api/control/feed-health
```

- Feed health is also reflected in:
  - `/api/control/status`
  - `/api/stack/overview`
  - `/actuator/health`
  - symbol-tagged metrics such as:
    - `trading.databento.gateway.running`
    - `trading.databento.quote.fresh`
    - `trading.databento.quote.age.ms`
    - `trading.databento.restarts.total`

- Quick live checks from this worktree:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
curl -fsS http://127.0.0.1:9081/api/control/status
curl -fsS http://127.0.0.1:9082/api/control/status
python3 -u check_morning_stack.py
```

- Operational note: `/api/stack/overview` is an aggregate endpoint. It fans out across the whole fleet, so it is expected to be slower than a direct per-symbol `/api/control/status` check.
- Live verification on the current 99-symbol fleet after the overview discovery cleanup:
  - `/api/stack/overview` now reports `symbolCount=99`
  - legacy root-runtime ports `8081`-`8084` are no longer mixed into the Databento fleet view
  - symbol rows no longer fall back to `positionSyncState="unknown"` for those stale legacy configs
  - representative observed timings were about `8.0s` for `/api/stack/overview`, versus about `10ms` for `http://127.0.0.1:9081/api/control/status` and about `2ms` for `http://127.0.0.1:9082/api/control/status`
  - `summary.overallOk` can still be `false` even when all symbol bots are connected/healthy, because the aggregate status also includes non-symbol components such as the harvester

## Shared IBKR gateway lifecycle

The bulk Databento launcher now manages a shared IBKR gateway companion process for this worktree.

Manual commands:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
./start_shared_ibkr_gateway.sh
./stop_shared_ibkr_gateway.sh
```

Operational notes:

- PID file: `runtime/databento/state/shared-ibkr-gateway.pid`
- Log file: `runtime/databento/logs/shared-ibkr-gateway.log`
- Bulk start auto-starts the gateway in shared-only mode when it is not already reachable.
- Bulk stop shuts down the worktree-owned gateway when the full symbol fleet is selected.

## Offline 100-symbol stress harness baseline

Use `scripts/stress_100_symbols_offline.py` for the shared-feed + shared-IBKR offline load check.

Operational note:

- The harness now defaults `--startup-delay-seconds` to `40` seconds.
- Earlier `25` second startup timing was not reliably stable at full 100-symbol load.
- The `40` second baseline produced repeatable `100/100` symbol coverage for both live-bar delivery and AI dispatch in this worktree.
- The live Databento startup path now also uses a `40` second normalizer startup delay via `trading.databento.startup-delay-seconds=40`.
- Default `trading.databento.max-silence-ms` was widened to `120000` so shared-relay quorum wait plus the delayed live normalizer start do not trigger false feed restarts during boot.

Default run:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/stress_100_symbols_offline.py
```

If you want to probe startup sensitivity explicitly, you can still override the delay:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/stress_100_symbols_offline.py --startup-delay-seconds 25
python3 scripts/stress_100_symbols_offline.py --startup-delay-seconds 40
```

## Parallel run model

- Existing production-ish stack: run from `/Users/filmonghezehey/trading-agent`
- New Databento stack: run from `/Users/filmonghezehey/trading-agent/worktrees/databento`

Keep IBKR `client-id`, log/output folders, and DB table names isolated between stacks.

