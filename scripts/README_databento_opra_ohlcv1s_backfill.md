# Databento OPRA OHLCV-1S Backfill

This script downloads historical Databento OPRA `ohlcv-1s` data for a symbol list and upserts rows into PostgreSQL table `databento_opra_ohlcv_1s`.

It follows the same workflow as the TBBO backfill script: retries, resumable run IDs, and pause/continue behavior.

## What It Creates

- Table: `databento_opra_ohlcv_1s`
- Primary key: `record_hash`
- Index: `(symbol, ts_event)`
- Payload: full Databento row in `payload` (JSONB)
- Run table: `databento_backfill_runs`
- Per-chunk progress table: `databento_backfill_progress`

## Required Environment

Set these before running:

- `DATABENTO_API_KEY`
- `DATABENTO_USER_ID` (optional metadata for run logs)
- DB settings from one of:
  - `HARVEST_DB_URL`, `HARVEST_DB_USER`, `HARVEST_DB_PASSWORD`
  - or Spring datasource properties in `runtime/postgres-local.properties`

## Dry Run (No Download / No DB Write)

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_opra_ohlcv1s_to_postgres.py --dry-run
```

## Full Backfill (Defaults: OPRA.PILLAR + ohlcv-1s)

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
export DATABENTO_USER_ID="<your_databento_id>"
export DATABENTO_API_KEY="<your_databento_api_key>"
python3 scripts/backfill_databento_opra_ohlcv1s_to_postgres.py \
  --months 3 \
  --chunk-days 7 \
  --symbols-file runtime/opra_symbols.txt
```

> Note: OPRA expects option contract symbols (not equity underlyings like `TSLA` or `AAPL`).
> Example symbol format: `AAPL  260417C00150000`.

## Resume A Paused/Interrupted Run

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_opra_ohlcv1s_to_postgres.py --run-id "<run_id_from_logs>"
```

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_opra_ohlcv1s_to_postgres.py --resume-latest
```

## Pause Now, Continue Later

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
mkdir -p runtime/databento/state
touch runtime/databento/state/pause_backfill.flag
```

Continue later:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
rm -f runtime/databento/state/pause_backfill.flag
python3 scripts/backfill_databento_opra_ohlcv1s_to_postgres.py --resume-latest
```

## Useful Options

- `--dataset OPRA.PILLAR` (default)
- `--schema ohlcv-1s` (default)
- `--stype-in parent` (default; symbols should be underlyings or `ROOT.OPT`)
- `--max-symbols 5` for trial runs
- `--chunk-days 3` for smaller request windows
- `--run-id <id>` to resume a specific run
- `--resume-latest` to resume newest paused/failed/running run with matching settings
- `--pause-file runtime/databento/state/pause_backfill.flag`
- Retry controls: `--max-retries`, `--retry-base-seconds`, `--retry-max-seconds`, `--pause-on-network-error`
- `--skip-opra-symbol-validation` to bypass the preflight symbol-format check

