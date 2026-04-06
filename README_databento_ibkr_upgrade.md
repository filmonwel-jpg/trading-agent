# Databento -> IBKR Upgrade Workspace

This worktree is your isolated upgrade lane for scaling to 100 symbols with Databento market data and IBKR order routing.

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

## Parallel run model

- Existing production-ish stack: run from `/Users/filmonghezehey/trading-agent`
- New Databento stack: run from `/Users/filmonghezehey/trading-agent/worktrees/databento`

Keep IBKR `client-id`, log/output folders, and DB table names isolated between stacks.

