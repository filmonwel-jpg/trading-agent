# Databento TBBO 9-Month Backfill (100 Symbols)

This script downloads historical Databento `tbbo` data for the 100-symbol universe in `databento_ibkr_bridge/config/symbols_100.txt` and upserts rows into PostgreSQL table `databento_tbbo`.

It now supports retries, resume checkpoints, and pause/continue workflow for interrupted network sessions.

## What It Creates

- Table: `databento_tbbo`
- Primary key: `record_hash`
- Index: `(symbol, ts_event)`
- Payload: full Databento row stored in `payload` (JSONB)
- Run table: `databento_backfill_runs`
- Per-chunk progress table: `databento_backfill_progress`

## Required Environment

Set these before running:

- `DATABENTO_API_KEY`
- `DATABENTO_USER_ID` (optional metadata for run logs)
- DB settings from one of:
  - `HARVEST_DB_URL`, `HARVEST_DB_USER`, `HARVEST_DB_PASSWORD`
  - or existing Spring datasource properties in `runtime/postgres-local.properties`

## Dry Run (No Download / No DB Write)

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_tbbo_to_postgres.py --dry-run
```

## Full Backfill (9 Months)

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
export DATABENTO_USER_ID="<your_databento_id>"
export DATABENTO_API_KEY="<your_databento_api_key>"
python3 scripts/backfill_databento_tbbo_to_postgres.py --months 9 --chunk-days 14
```

## Resume A Paused/Interrupted Run

Use either the explicit run ID or auto-resume latest matching run.

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_tbbo_to_postgres.py --run-id "<run_id_from_logs>"
```

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python3 scripts/backfill_databento_tbbo_to_postgres.py --resume-latest
```

## Pause Now, Continue Later

Create a pause flag file. The script exits cleanly at next chunk boundary and marks run status as `paused`.

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
mkdir -p runtime/databento/state
touch runtime/databento/state/pause_backfill.flag
```

Then continue later from another network:

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
rm -f runtime/databento/state/pause_backfill.flag
python3 scripts/backfill_databento_tbbo_to_postgres.py --resume-latest
```

## Retry and Rate-Limit Handling

- Exponential backoff with jitter for retryable network/rate-limit errors
- Control via flags:
  - `--max-retries`
  - `--retry-base-seconds`
  - `--retry-max-seconds`
  - `--pause-on-network-error`

## SQL Monitoring Queries

```sql
SELECT run_id, status, symbols_completed, symbols_total, chunks_completed, rows_upserted, updated_at
FROM databento_backfill_runs
ORDER BY updated_at DESC
LIMIT 20;
```

```sql
SELECT symbol, status, COUNT(*) AS chunks, SUM(rows_upserted) AS rows
FROM databento_backfill_progress
WHERE run_id = '<run_id>'
GROUP BY symbol, status
ORDER BY symbol, status;
```

## Useful Options

- `--max-symbols 5` for smaller trial runs
- `--dataset EQUS.MINI` for live-parity backfills (the script itself still accepts any dataset override)
- `--schema tbbo` (default)
- `--stype-in raw_symbol` (default)
- `--chunk-days 7` for smaller request windows
- `--run-id <id>` to resume a specific run
- `--resume-latest` to resume newest paused/failed/running run with matching settings
- `--pause-file runtime/databento/state/pause_backfill.flag`

