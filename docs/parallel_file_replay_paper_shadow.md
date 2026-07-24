# Parallel Databento file replay paper-shadow

Use this when you want to replay a recorded Databento NDJSON/NDJSON.GZ file with multiple symbol bots running at the same time, instead of replaying `NVDA`, `QQQ`, `SPY`, `TQQQ`, and `TSLA` one after another.

This is intended as a paper-trading rehearsal tool:

- all selected symbols are launched concurrently;
- every symbol reads the same recorded event file;
- each symbol gets its own Java simulated-broker process, log, trade CSV, order CSV, lifecycle CSV, and stream sanity report;
- the parent process writes structured monitor events and an aggregate summary;
- the existing lifecycle PnL summarizer runs at the end by default.

## Important interpretation

This is closer to live/paper timing than sequential replay because all symbol bots are alive at the same wall-clock time. It is still not a perfect shared-broker simulation: each symbol runs in its own simulated broker process. Use it to observe concurrent bot behavior, log quality, model routing, feature snapshot coverage, order timing, watchdogs, and aggregate PnL/telemetry before paper trading.

## Recovered whole-range event-carried replay example

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

git pull --ff-only origin ai-training-dynamic-upgrade-20260612

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
RECORDED_EVENTS=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
OUT=$BASE/parallel_file_replay_recovered_trade_pnl

python3 scripts/run_parallel_databento_file_backtest.py \
  --recorded-events "$RECORDED_EVENTS" \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --output-dir "$OUT" \
  --trade-amount 500000 \
  --max-order-notional 500000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --monitor-interval-sec 60 \
  --title "Parallel recovered event-carried paper-shadow 2025-07-22 to 2026-05-22"
```

`--max-order-notional` is passed to every child replay process as the simulated/order risk notional cap.

If you need to pass an advanced option that the parallel launcher does not know about yet, put it after `--`; those extra arguments are appended to every child backtest command.

## Main outputs

```text
$OUT/parallel_file_replay.log
$OUT/parallel_file_replay_events.jsonl
$OUT/parallel_file_replay_summary.csv
$OUT/parallel_file_replay_summary.json
$OUT/controlled_java_replay.parallel_combined.log
$OUT/parallel_pnl_period_summary.json
$OUT/parallel_pnl_period_summary.md
$OUT/logs/NVDA.log
$OUT/logs/QQQ.log
$OUT/logs/SPY.log
$OUT/logs/TQQQ.log
$OUT/logs/TSLA.log
```

## Quick status checks

```zsh
python3 -m json.tool "$OUT/parallel_file_replay_summary.json" | head -120

tail -n 80 "$OUT/parallel_file_replay.log"

grep -nE 'ERROR|Exception|WATCHDOG|featureSnapshot=miss|featureSidecar=hit' "$OUT/controlled_java_replay.parallel_combined.log" | head -120
```

## Aggregate PnL summary

```zsh
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(r["log"]["completion"]); print(r["log"]["markers"]); print("errors", r["log"]["errors"]); print("watchdogs", r["log"]["watchdogs"]); print(r.get("all", []))' \
  "$OUT/parallel_pnl_period_summary.json"
```

## Compare with sequential replay

The existing sequential wrapper is still available:

```zsh
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source ndjson \
  --recorded-events "$RECORDED_EVENTS" \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --output-dir "$BASE/sequential_file_replay_recovered_trade_pnl" \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST"
```

Use the parallel output when you want to inspect concurrent launch behavior and monitor events. Use the sequential output when you want the previous deterministic one-symbol-after-another baseline.
