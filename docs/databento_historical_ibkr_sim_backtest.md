# Databento historical IBKR-sim backtest

Use `scripts/run_databento_historical_ibkr_sim_backtest.sh` to replay Databento historical data into the Java strategy while simulating IBKR order submission/fills locally.

## Default full-symbol replay

If you do not pass `--symbol` or `--symbols`, the script loads the default universe from `runtime/symbols_100.txt` and runs one backtest per symbol.

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --start 2026-05-21 \
  --end 2026-05-21
```

## Replay selected symbols

```bash
cd /Users/filmonghezehey/trading-agent/worktrees/databento
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols TSLA,NVDA,AAPL \
  --start 2026-05-21 \
  --end 2026-05-21
```

You can also repeat `--symbol`:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbol TSLA \
  --symbol NVDA \
  --start 2026-05-21 \
  --end 2026-05-21
```

## Core five-symbol Databento API replay

The current five-symbol Databento pilot universe is tracked in `config/databento_core_5_symbols.txt`:

```text
TSLA
TQQQ
NVDA
SPY
QQQ
```

Use the convenience wrapper when moving to another computer. It pins the core-five symbols, a portable tracked setup model bundle, the June 22 lifecycle/micro model bundle, and writes outputs to the connected external disk by default:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests/databento_api_core5
```

This Mac currently mounts `DatabentoVault` read-only, so real runs should be launched from the computer that can write to that disk. After the run, bring the disk back here and analyze the generated CSV/JSON outputs from that folder.

```bash
scripts/run_databento_api_backtest_core5.sh \
  --start 2026-05-21 \
  --end 2026-05-21
```

Equivalent explicit command:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source api \
  --symbols-file config/databento_core_5_symbols.txt \
  --start 2026-05-21 \
  --end 2026-05-21 \
  --model-dir model_exports/20260320_192113 \
  --lifecycle-model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir /Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests/databento_api_core5 \
  --timeout-seconds 1800
```

Before a non-dry run, make sure `DATABENTO_API_KEY` is exported or present in `runtime/databento.env` on that machine. For a wiring check that should not download historical data:

```bash
scripts/run_databento_api_backtest_core5.sh \
  --start 2026-05-21 \
  --end 2026-05-21 \
  --dry-run \
  --timeout-seconds 60
```

If the writable computer mounts the disk at a different path, override just the output folder:

```bash
CORE5_OUTPUT_DIR=/path/to/external-disk/trading-agent-offload/databento/runtime/backtests/databento_api_core5 \
  scripts/run_databento_api_backtest_core5.sh \
    --start 2026-05-21 \
    --end 2026-05-21
```

## Use another symbol list

The symbols file may be newline-separated or comma-separated. Blank lines and `#` comments are ignored.

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols-file runtime/databento/applied-symbols-20260523.txt \
  --exclude SPY,QQQ \
  --start 2026-05-21 \
  --end 2026-05-21
```

## Replay recorded normalized NDJSON events

Recorded-event replay uses the same Java backtester and strategy path, but the Python streamer reads an existing normalized NDJSON/NDJSON.GZ file instead of the Databento Historical API:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source ndjson \
  --recorded-events runtime/replay/example-recorded-events.ndjson.gz \
  --symbol TSLA \
  --lifecycle-model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir runtime/backtests/recorded_replay
```

Use `scripts/validate_lifecycle_micro_promotion.py` after the run to combine recorded-event contract checks, replay/live decision parity, PnL/day-dominance, paper/shadow drift, and label-economics evidence.

Preview the resolved list without building or downloading data:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh --list-symbols
```

## Dry-run validation

`--dry-run` validates the Java/Python/strategy wiring without downloading Databento data:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols TSLA,NVDA \
  --start 2026-05-21 \
  --end 2026-05-21 \
  --dry-run \
  --timeout-seconds 30
```

## Outputs

For each symbol, the script writes separate files under `runtime/backtests` by default:

```text
<symbol>-<start>-to-<end>-<timestamp>-trades.csv
<symbol>-<start>-to-<end>-<timestamp>-orders.csv
```

The order history CSV is initialized for every symbol and records simulated IBKR events with:

```text
WallTime,MarketTime,OrderId,EventType,Symbol,Action,Quantity,OrderType,RequestedPrice,FillPrice,Status,PositionBefore,PositionAfter,Note
```

