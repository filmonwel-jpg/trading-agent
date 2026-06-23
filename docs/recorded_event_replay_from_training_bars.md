# Recorded-event NDJSON replay seed from training bars

_Last verified: 2026-06-23_

This documents the portable recorded-event replay seed now committed for `scripts/run_databento_historical_ibkr_sim_backtest.sh --source ndjson` smoke tests.

Committed files:

- `runtime/replay/example-recorded-events.ndjson.gz`
- `runtime/replay/example-recorded-events.manifest.json`
- `scripts/build_recorded_events_from_training_bars.py`
- `tests/test_build_recorded_events_from_training_bars.py`

## Replay contents

The seed was generated from local TSLA 5-second lifecycle/micro training bars for `2025-07-21`.

```text
symbol=TSLA
session_date=2025-07-21
equity_events=4680
option_events=9319
previous_close=334.41
EventSchemaVersion=databento_ndjson_v2
```

The builder emits normalized `status`, `previous_close`, `option_bar`, and `equity_bar` events. Its default `--epoch-source timestamp` derives emitted `barEpochSec` values from the CSV `Timestamp` column and floors to `--bar-seconds 5`, avoiding the source CSV's `+4s` end-of-bar-style `BarEpochSec` offset.

Manifest alignment highlights:

```text
first_bar_epoch_sec=1753104600
last_bar_epoch_sec=1753127995
first_source_bar_epoch_sec=1753104604
last_source_bar_epoch_sec=1753127999
misaligned_source_epoch_rows=4680
```

## Use the committed replay seed

From the repository root:

```bash
PYTHON_BIN="$PWD/.venv/bin/python" \
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source ndjson \
  --recorded-events runtime/replay/example-recorded-events.ndjson.gz \
  --symbol TSLA \
  --lifecycle-model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir runtime/local-backtests/recorded_replay \
  --skip-build \
  --timeout-seconds 180
```

On this workstation `runtime/backtests` was a non-writable external-disk symlink, so `runtime/local-backtests/recorded_replay` was used. If `runtime/backtests` is writable on another computer, change the output directory to `runtime/backtests/recorded_replay`.

The committed `.ndjson.gz` file is enough to run the replay smoke; the original source CSV is only needed to regenerate the seed.

## Regenerate the seed

```bash
python3 scripts/build_recorded_events_from_training_bars.py \
  --input-csv runtime/research_runs/lifecycle_micro_bar_regen_20260622/bars_5s_by_symbol/TSLA_5s_training.csv \
  --symbol TSLA \
  --date 2025-07-21 \
  --output runtime/replay/example-recorded-events.ndjson.gz
```

Epoch modes:

- `--epoch-source timestamp` (default): use `Timestamp`, aligned to `--bar-seconds`; fallback to floored `BarEpochSec`.
- `--epoch-source floor-bar-epoch`: floor source `BarEpochSec` to `--bar-seconds`.
- `--epoch-source bar-epoch`: preserve raw source `BarEpochSec` for diagnostics.

## Validations run locally

```bash
python3 -m py_compile scripts/build_recorded_events_from_training_bars.py
python3 tests/test_build_recorded_events_from_training_bars.py
python3 scripts/databento_historical_streamer.py \
  --source ndjson \
  --symbols TSLA \
  --input-file runtime/replay/example-recorded-events.ndjson.gz
```

Observed replay checks:

```text
RECORDED_EVENTS_VERIFY=PASS
counts={'status': 2, 'previous_close': 1, 'option_bar': 9319, 'equity_bar': 4680}
versions=['databento_ndjson_v2']
equity_misaligned_5s=0
STREAMER_SMOKE=PASS lines=14005 counts={'status': 5, 'previous_close': 1, 'option_bar': 9319, 'equity_bar': 4680} versions=['databento_ndjson_v2'] last_message=recorded-ndjson-replay-complete events=14002 skipped=0 inputLines=14002
```

Latest aligned local backtest result:

```text
[BACKTEST] completed=1 failed=0 requested=1 output_dir=/Users/FXG06FA/trading-agent-main/runtime/local-backtests/recorded_replay
RECORDED_REPLAY_RC=0
Equity bars: 4680
Option bars: 9319
Skipped events: 0
Total trades: 0
Total PnL: 0.0
arms_total: 5
arms_long: 2
arms_short: 3
ai_evaluations=768
flat_entry_evaluations=768
```

This verifies the NDJSON ingestion, streamer bridge, Java backtester, and aligned AI evaluation path. It remains a smoke/diagnostic replay seed rather than evidence of profitable trade/PnL behavior, because this TSLA day produced setup arms but no confirmed fills.
