# Databento IBKR Bridge (Scaffold)

This folder is the starting point for the next-stage architecture:

- Ingest live market data from Databento for up to 100 symbols
- Generate trading intents/signals
- Route orders to IBKR with isolated client IDs and runtime state

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
- `config/symbols_100.txt` - initial 100-symbol universe
- `.env.example` - required runtime settings template

## Notes

- `--dry-run` validates configuration and prints what would run without connecting to Databento or IBKR.
- Real Databento streaming and IBKR order routing hooks are marked in code for implementation next.

