# Databento IBKR Bridge (Scaffold)

This folder is the starting point for the next-stage architecture:

- Ingest live market data from Databento for up to 100 symbols
- Generate trading intents/signals
- Route orders to IBKR through a shared execution gateway that can reuse one IBKR client connection across many symbols

Visual architecture diagram: [`../docs/databento-ibkr-data-flow.md`](../docs/databento-ibkr-data-flow.md)
or browser view [`../docs/databento-ibkr-data-flow.html`](../docs/databento-ibkr-data-flow.html).

## Quick start

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 src/databento_ibkr_bridge/bridge_runner.py --dry-run
```

## Files

- `src/databento_ibkr_bridge/bridge_runner.py` - main runner entrypoint
- `src/databento_ibkr_bridge/config.py` - env + symbol config loading and validation
- `src/databento_ibkr_bridge/shared_ibkr_execution_gateway.py` - first shared IBKR execution gateway scaffold
- `src/databento_ibkr_bridge/shared_ibkr_execution_models.py` - shared command/event/state contract
- `SHARED_IBKR_GATEWAY_AUDIT.md` - exact routing audit for moving IBKR off one-client-per-symbol
- `config/symbols_100.txt` - initial 100-symbol universe
- `.env.example` - required runtime settings template

## Notes

- `--dry-run` validates configuration and prints what would run without connecting to Databento or IBKR.
- Real Databento streaming and IBKR order routing hooks are marked in code for implementation next.

## Shared IBKR Gateway Scaffold

Inspect the first shared execution scaffold:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/bridge_runner.py --shared-ibkr-gateway --dry-run
```

Run the smoke test:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/shared_ibkr_execution_gateway_smoke.py
```

Read the routing audit:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
cat SHARED_IBKR_GATEWAY_AUDIT.md
```

Run the JSON socket server for Java bots:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/bridge_runner.py --shared-ibkr-gateway-server --dry-run
```

Worktree companion lifecycle scripts:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
./start_shared_ibkr_gateway.sh --dry-run
./stop_shared_ibkr_gateway.sh
```

These scripts own the worktree PID/log files and are what `start_all_databento_bots.sh` / `stop_all_databento_bots.sh` use during bulk launch and shutdown.

Protocol document:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
cat shared_ibkr_execution_gateway_protocol.md
```

## Backfill Consumer Mode

The bridge can consume `databento_backfill_runs` + `databento_backfill_progress` and build simple order intents from latest `databento_tbbo` snapshots.

Dry-run routing (recommended first):

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/bridge_runner.py --consume-backfill --dry-run
```

Consume a specific run ID:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/bridge_runner.py --consume-backfill --run-id "<run_id>" --dry-run
```

Live order submission (uses one shared `IBKR_HOST/PORT/CLIENT_ID_BASE` connection through the gateway abstraction):

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento/databento_ibkr_bridge
python3 src/databento_ibkr_bridge/bridge_runner.py --consume-backfill
```

