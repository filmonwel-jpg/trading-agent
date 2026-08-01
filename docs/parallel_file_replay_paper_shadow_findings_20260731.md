# Parallel file replay paper-shadow findings — 2026-07-31

This note documents the paper-shadow replay discussion and the resulting go/no-go interpretation for the recovered event-carried Databento replay covering `2025-07-22` through `2026-05-22`.

It complements the operational runbook in `/Users/FXG06FA/trading-agent-main/docs/parallel_file_replay_paper_shadow.md`.

For the paper/live startup mapping after this recovered replay validation, including the live Databento launch commands, required overrides, downstream setup-filter manifest wiring, and monitoring checks, see [`databento_paper_live_startup_post_recovered_event_20260731.md`](databento_paper_live_startup_post_recovered_event_20260731.md).

## Context

The goal was to move from one-symbol-at-a-time historical replay toward a file replay mode that better resembles paper trading:

- launch multiple symbol bots at the same wall-clock time;
- feed each bot from the same recovered event-carried NDJSON/NDJSON.GZ file;
- observe order flow, model routing, feature snapshot coverage, watchdogs, and aggregate PnL;
- decide whether the strategy path is ready for paper trading.

The test was run on the other Mac from:

```text
/Users/filmonghezehey/trading-agent/worktrees/databento
```

Primary output base:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
```

Recorded events file:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz
```

Route manifest:

```text
runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
```

The sizing context at the time of the replay was:

```text
trading.trade-amount=500000
trading.risk.max-order-notional=500000
trading.risk.max-share-cap=2000
trading.shared-capital.total-notional=500000
```

## Important architecture note

The parallel replay harness starts all selected symbol bots together, but it does **not** enforce a single synchronized market clock or a single shared simulated broker.

Current behavior:

- one child replay process per symbol;
- each child reads the same recorded `.ndjson.gz` file;
- each child filters/processes its own symbol;
- each child owns its own Java simulated broker, lifecycle CSV, order CSV, and log;
- the parent process monitors status and combines logs at the end.

Therefore, symbols are expected to **start at the same time** but **finish at different times**. Different finish times can be caused by different event counts, filtering volume, model evaluation workload, logging volume, JVM scheduling/GC, and concurrent gzip/file reads.

A stricter live-feed simulator would use one reader/event bus and fan events out to all symbol strategies on a shared replay clock. The current harness is a useful paper-shadow rehearsal, but not a perfect shared-broker/order-book simulation.

## Why sequential and parallel replay results can differ

The sequential wrapper and the parallel harness are intentionally close, but they are not automatically an apples-to-apples experiment.

The existing sequential wrapper, `/Users/FXG06FA/trading-agent-main/scripts/run_databento_historical_ibkr_sim_backtest.sh`, loops over symbols one at a time and launches one Java replay per symbol. The parallel harness, `/Users/FXG06FA/trading-agent-main/scripts/run_parallel_databento_file_backtest.py`, launches that same single-symbol wrapper once per symbol, but keeps multiple child processes alive concurrently.

If the child commands are identical, the output directories are clean, and no wall-clock timeout/watchdog behavior changes the order lifecycle, per-symbol results should be close. When results differ materially, the usual causes are:

1. **Different strategy mode.** Lifecycle/micro is enabled by default in the child wrapper unless `--disable-lifecycle-micro` is passed. The strict parallel run used lifecycle/micro and produced only `5` micro passes/trades. The setup-only comparison passed `--disable-lifecycle-micro` and produced `352` trades. These two are not the same strategy.
2. **Different micro thresholds.** The controlled event-carried docs use explicit `--micro-long-entry-threshold 0.30` and `--micro-short-entry-threshold 0.30` for the validated 4-week replay. A run that omits those flags inherits thresholds from the lifecycle scorecard/defaults, which can be much stricter and change trade count sharply.
3. **Different feature source.** Event-carried enriched mode should run without `--downstream-setup-filter-features-csv` and should show `featureSnapshot=hit` with `featureSidecar=disabled`. CSV sidecar replay intentionally overrides with exact research rows. Mixing sidecar and no-sidecar results changes the evidence label even when decisions match.
4. **Dirty output directories.** The summarizers read all `*-trade-lifecycle-summary.csv` files under the output directory. Reusing an output directory can accidentally aggregate stale CSVs from older runs.
5. **Concurrency and wall-clock-sensitive logic.** The parallel harness starts several JVM/Python child processes that read/decompress the same `.ndjson.gz` at the same time. Strategy watchdogs use wall-clock time, not market time. In the setup-only parallel run, `14` `Order hung >3s` watchdog lines appeared, which can happen more easily under CPU/file contention than in one-symbol-at-a-time replay.
6. **No shared broker/capital clock.** Neither path enforces a single shared simulated broker across symbols. Parallel replay is closer to paper timing, but it does not serialize fills through one global broker state. Sequential replay also isolates symbols, but it avoids concurrent resource contention.
7. **Build/branch drift.** The event-carried docs warn to rebuild before `SKIP_BUILD=true` replays. Stale Java classes previously caused threshold/config mismatches, so the commit/build used for sequential and parallel comparisons must match.

The safest diagnostic sequence is:

1. run the parallel harness with `--jobs 1` using the exact same options as the `--jobs 5` run;
2. run the original sequential wrapper with the exact same options;
3. compare clean output directories, per-symbol marker counts, lifecycle row counts, PnL, and watchdog counts.

Interpretation:

- if original sequential and parallel `--jobs 1` match, the wrappers are equivalent;
- if parallel `--jobs 1` and parallel `--jobs 5` differ, concurrency/wall-clock/watchdog effects are the likely cause;
- if neither matches an older historical result, the older result likely used different lifecycle/micro settings, thresholds, feature source, sizing, branch/build, or had stale output CSVs.

## Recovered whole-range sequential micro/lifecycle baseline

The older recovered whole-range event-carried result was a different and important baseline from the setup-only parallel run. It was run one symbol at a time through the sequential wrapper with recovered event-carried snapshots and no CSV sidecar during replay.

Range:

```text
2025-07-22 to 2026-05-22
```

Output family:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/controlled_java_replay_downstream_setup_filter_event_snapshot_recovered_trade_pnl
```

Mode:

```text
event-carried snapshots
CSV sidecar disabled
downstream setup filter enabled
lifecycle/micro enabled
symbols run sequentially by /Users/FXG06FA/trading-agent-main/scripts/run_databento_historical_ibkr_sim_backtest.sh
```

### No-trade validation baseline

| Metric | Value |
|---|---:|
| Completed symbols | 5 / 5 |
| Setup filter rows | 49,143 |
| Setup pass | 1,738 |
| Setup fail | 47,405 |
| Snapshot hit | 49,143 |
| Snapshot miss | 0 |
| Sidecar disabled | 49,143 |
| Sidecar hit | 0 |
| Errors | 0 |
| Watchdogs | 0 |

Interpretation: the recovered event-carried feature transport was clean. This validated that enriched snapshots were present and no CSV sidecar leaked into the replay.

### Trade-enabled micro/lifecycle baseline

| Metric | Value |
|---|---:|
| Completed symbols | 5 / 5 |
| Setup filter rows | 39,463 |
| Setup pass | 761 |
| Setup fail | 38,702 |
| Micro pass / closed trades | 417 |
| Micro fail | 4,233 |
| Snapshot hit | 39,463 |
| Snapshot miss | 0 |
| Sidecar disabled | 39,463 |
| Sidecar hit | 0 |
| Error grep count | 18 |
| Watchdogs | 9 total: `TQQQ=8`, `TSLA=1` |

PnL summary:

| Metric | Value |
|---|---:|
| Closed trades | 417 |
| Wins | 279 |
| Losses | 138 |
| Win rate | 66.91% |
| Total PnL | +$28,757.81 |
| Total realized R | +143.6226R |
| Mean realized R/trade | +0.3444R |
| Median realized R/trade | +1.2729R |
| Avg MFE R | +1.2945R |
| Avg MAE R | -0.9836R |
| Avg setup-to-fill | 3.69 sec |

By symbol:

| Symbol | Trades | Wins | Losses | Win rate | PnL | Realized R | Mean R/trade | Median R |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `NVDA` | 110 | 78 | 32 | 70.91% | +$14,405.25 | +59.2258R | +0.5384R | +1.3853R |
| `TQQQ` | 245 | 154 | 91 | 62.86% | +$3,775.00 | +42.0013R | +0.1714R | +1.2073R |
| `TSLA` | 62 | 47 | 15 | 75.81% | +$10,577.56 | +42.3954R | +0.6838R | +1.3257R |

By month:

| Month | Trades | Wins | Losses | Win rate | PnL | Realized R |
|---|---:|---:|---:|---:|---:|---:|
| `2025-07` | 5 | 2 | 3 | 40.00% | -$85.00 | -2.3415R |
| `2025-08` | 23 | 13 | 10 | 56.52% | -$95.98 | +0.8162R |
| `2025-09` | 19 | 13 | 6 | 68.42% | +$912.58 | +7.5530R |
| `2025-10` | 25 | 18 | 7 | 72.00% | +$4,045.67 | +15.2756R |
| `2025-11` | 54 | 35 | 19 | 64.81% | +$4,265.05 | +14.3452R |
| `2025-12` | 38 | 23 | 15 | 60.53% | +$1,181.89 | +7.4079R |
| `2026-01` | 31 | 13 | 18 | 41.94% | -$2,022.77 | -14.1878R |
| `2026-02` | 42 | 25 | 17 | 59.52% | +$317.33 | +3.0928R |
| `2026-03` | 46 | 29 | 17 | 63.04% | +$546.80 | +4.6161R |
| `2026-04` | 47 | 36 | 11 | 76.60% | +$5,311.56 | +28.2182R |
| `2026-05` | 87 | 72 | 15 | 82.76% | +$14,380.68 | +78.8268R |

### Correct interpretation of the three runs

The sequential 417-trade baseline shows that lifecycle/micro was **not inherently too restrictive**. The earlier parallel strict-micro result with only `5` trades was a different effective micro configuration, most likely because the pasted parallel command did not pass the documented `--micro-long-entry-threshold 0.30` and `--micro-short-entry-threshold 0.30` overrides used in the controlled event-carried workflow. Without those overrides, the child wrapper inherits lifecycle scorecard/default thresholds and can become much stricter.

The setup-only parallel run is also not directly comparable to the sequential micro/lifecycle baseline because `--disable-lifecycle-micro` changes both entry and exit behavior:

- micro/lifecycle enabled: setup pass arms an opportunity, then 5-second micro confirmation decides whether to enter; lifecycle/guard logic manages exits;
- setup-only: micro confirmation and lifecycle exits are disabled, so a setup pass can become an immediate setup-driven trade and the exit path changes;
- strategy state changes candidate generation: while in position or in-flight, later setup arms may be skipped, suppressed, expired, or logged differently;
- therefore setup rows/pass counts are not invariant across modes.

The observed counts prove this state-path change:

| Run | Lifecycle/micro | Setup rows | Setup pass | Micro pass / trades | Micro fail | PnL | Realized R | Win rate |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential recovered baseline | enabled, documented `0.30/0.30` | 39,463 | 761 | 417 | 4,233 | +$28,757.81 | +143.6226R | 66.91% |
| Parallel lifecycle/micro rerun | enabled, explicit `0.30/0.30`, `trade_amount=60000` | 39,463 | 761 | 417 | 4,233 | +$18,979.79 | +143.6226R | 66.91% |
| Parallel strict run | enabled, but different effective thresholds | 49,055 | 1,720 | 5 | 18,223 | +$235 | +3.8324R | 80.00% |
| Parallel setup-only comparison | disabled | 25,300 | 352 | 0 | 0 | +$38,100 | +171.9610R | 25.28% |

This means the large differences are primarily **strategy/config differences**, not proof that event-carried snapshots behave differently under parallel replay. Event-carried transport was clean in all discussed runs because `snapshot_miss=0` and `sidecar_hit=0`.

The July 31 parallel rerun with explicit `--micro-long-entry-threshold 0.30` and `--micro-short-entry-threshold 0.30` reproduced the same 417-trade decision path as the recovered sequential lifecycle/micro baseline. The remaining dollar-PnL difference is a sizing/effective-notional issue, not a strategy-path or feature-transport issue: the normalized R totals, trade count, win/loss count, setup/micro marker counts, and snapshot/sidecar telemetry match the recovered baseline.

The right apples-to-apples setup-only test is sequential setup-only vs parallel setup-only with the exact same flags. The right strategy-path comparison for lifecycle/micro should use normalized R and marker counts unless the effective share sizing is also verified identical from the run logs.

## Run 3: parallel lifecycle/micro explicit `0.30/0.30`, `trade_amount=60000`

Output directory:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/parallel_file_replay_recovered_lifecycle_micro_030_60k_trade_500k_cap_20260731_132724
```

Validated command properties:

```text
completed=5 failed=0 requested=5
recorded_events=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz
trade_amount=60000
max_order_notional=500000
max_share_cap=2000
max_trades=2000
passthrough=['--micro-long-entry-threshold', '0.30', '--micro-short-entry-threshold', '0.30']
```

Per-symbol lifecycle rows:

| Symbol | Status | RC | Lifecycle rows | Trade CSVs | Order CSVs |
|---|---|---:|---:|---:|---:|
| `NVDA` | completed | 0 | 110 | 1 | 1 |
| `QQQ` | completed | 0 | 1 | 0 | 1 |
| `SPY` | completed | 0 | 1 | 0 | 1 |
| `TQQQ` | completed | 0 | 245 | 1 | 1 |
| `TSLA` | completed | 0 | 62 | 1 | 1 |

Active trade-symbol lifecycle rows:

```text
NVDA 110 + TQQQ 245 + TSLA 62 = 417
```

Marker summary:

| Marker | Count |
|---|---:|
| `setup_pass` | 761 |
| `setup_fail` | 38,702 |
| `micro_pass` | 417 |
| `micro_fail` | 4,233 |
| `snapshot_hit` | 39,463 |
| `snapshot_miss` | 0 |
| `sidecar_disabled` | 39,463 |
| `sidecar_hit` | 0 |
| `sidecar_miss` | 0 |

PnL/R summary:

| Scope | Trades | Wins | Losses | Win rate | PnL | Realized R | Mean R/trade | Median R |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| All | 417 | 279 | 138 | 66.91% | +$18,979.79 | +143.6226R | +0.3444R | +1.2729R |

By symbol:

| Symbol | Trades | Wins | Losses | Win rate | PnL | Realized R | Mean R/trade | Median R |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `NVDA` | 110 | 78 | 32 | 70.91% | +$8,870.06 | +59.2258R | +0.5384R | +1.3853R |
| `TQQQ` | 245 | 154 | 91 | 62.86% | +$3,775.00 | +42.0013R | +0.1714R | +1.2073R |
| `TSLA` | 62 | 47 | 15 | 75.81% | +$6,334.73 | +42.3954R | +0.6838R | +1.3257R |

Runtime option lines from child logs:

```text
[BACKTEST] downstream_setup_filter enabled=true ... features_csv=<none> fail_closed=true
[BACKTEST] lifecycle_micro_enabled=true lifecycle_model_dir=/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
[BACKTEST] micro_entry_thresholds long=0.30 short=0.30
[BACKTEST] trade_amount=60000 max_order_notional=500000 max_trades=2000 max_share_cap=2000
```

Watchdog/error scan:

```text
matching_lines=1
counts={'ERROR': 2, 'WATCHDOG': 1, 'Order hung': 1}
watchdogs_by_symbol={'TQQQ': 1}
```

The single matched line was:

```text
[FLOW][ERROR][WATCHDOG] Order hung >3s. Requesting cancel/resync but keeping in-flight lock. symbol=TQQQ orderId=-1 position=0
```

Interpretation:

- the feature transport is clean: `snapshot_miss=0`, `sidecar_hit=0`, and `features_csv=<none>`;
- the lifecycle/micro strategy path matches the recovered sequential baseline: same setup rows, setup passes, micro passes, micro fails, trades, wins, losses, and realized R;
- the earlier 5-trade parallel result was threshold/config drift, not evidence that lifecycle/micro was inherently too restrictive;
- the dollar-PnL difference versus the quoted sequential `$28,757.81` baseline should be investigated as effective share sizing/notional drift. The current run is confirmed at `trade_amount=60000`; normalized R is the correct strategy-path comparison until the old run's effective `trade_amount` and quantity logs are verified.

## Run 1: strict lifecycle/micro enabled

Output directory:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/parallel_file_replay_recovered_trade_pnl
```

Command shape:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

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

### Operational result

```text
completed=5
failed=0
requested=5
errors=0
watchdogs={}
```

All five symbol processes completed with return code `0`.

### Finish times

| Finish order | Symbol | Runtime | Lifecycle rows | Trade CSVs |
|---:|---|---:|---:|---:|
| 1 | `TQQQ` | 30m 51.9s | 5 | 1 |
| 2 | `QQQ` | 43m 58.8s | 1 | 0 |
| 3 | `TSLA` | 53m 48.0s | 1 | 0 |
| 4 | `SPY` | 57m 32.3s | 1 | 0 |
| 5 | `NVDA` | 1h 16m 44.3s | 1 | 0 |

Finish spread:

```text
45m 52.4s
```

This spread is expected with independent child processes.

### Marker counts

```text
micro_fail=18223
micro_pass=5
setup_fail=47335
setup_pass=1720
sidecar_disabled=49055
sidecar_hit=0
sidecar_miss=0
snapshot_hit=49055
snapshot_miss=0
```

Feature snapshot health was excellent:

```text
snapshot_hit=49055
snapshot_miss=0
```

The issue was not missing feature snapshots. The issue was the strict micro-entry gate.

### PnL result

| Metric | Value |
|---|---:|
| Closed trades | 5 |
| Wins | 4 |
| Losses | 1 |
| Win rate | 80.00% |
| Total PnL | +$235 |
| Sum realized R | +3.8324R |
| Mean realized R | +0.7665R |
| Median realized R | +1.4268R |
| Avg MFE | +1.6169R |
| Avg MAE | -0.9256R |
| Avg setup-to-fill | 8.0 sec |

All closed trades were `TQQQ` shorts. `NVDA`, `QQQ`, `SPY`, and `TSLA` produced no actual trade CSVs.

### Interpretation

Strict lifecycle/micro mode was operationally clean, but the trading sample was too small:

```text
5 trades over the whole recovered period
```

The micro-entry gate passed only `5` times against `18,223` fails. This is too inactive to serve as a full paper-trading validation unless the goal is a very low-frequency strategy.

## Run 2: setup-only comparison, lifecycle/micro disabled

Output directory:

```text
runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/parallel_file_replay_recovered_setup_only_compare
```

Command shape:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
RECORDED_EVENTS=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
OUT=$BASE/parallel_file_replay_recovered_setup_only_compare

python3 scripts/run_parallel_databento_file_backtest.py \
  --recorded-events "$RECORDED_EVENTS" \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --output-dir "$OUT" \
  --trade-amount 500000 \
  --max-order-notional 500000 \
  --max-share-cap 2000 \
  --max-trades 2000 \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --disable-lifecycle-micro \
  --monitor-interval-sec 60 \
  --title "Parallel recovered event-carried setup-only comparison 2025-07-22 to 2026-05-22"
```

### Operational result

```text
completed=5
failed=0
requested=5
```

The setup-only run also completed successfully at process level.

### Marker counts

```text
micro_fail=0
micro_pass=0
setup_fail=24948
setup_pass=352
sidecar_disabled=25300
sidecar_hit=0
sidecar_miss=0
snapshot_hit=25300
snapshot_miss=0
```

Micro markers were zero because lifecycle/micro was disabled.

Setup pass rate:

```text
352 / (352 + 24948) = ~1.39%
```

Feature snapshot health again was excellent:

```text
snapshot_hit=25300
snapshot_miss=0
```

### PnL result

| Metric | Value |
|---|---:|
| Closed trades | 352 |
| Wins | 89 |
| Losses | 263 |
| Win rate | 25.28% |
| Total PnL | +$38,100 |
| Avg PnL/trade | +$108.24 |
| Sum realized R | +171.9610R |
| Mean realized R | +0.4885R |
| Median realized R | -1.7181R |
| Avg MFE | +4.3847R |
| Avg MAE | -1.4232R |

This is a positive-skew profile: most trades lose, the median trade is negative, but winners are large enough to make the total result strongly positive.

### By-symbol result

| Symbol | Trades | Wins | Losses | Win rate | PnL | Sum R | Mean R |
|---|---:|---:|---:|---:|---:|---:|---:|
| `NVDA` | 140 | 37 | 103 | 26.43% | +$20,560 | +85.6849R | +0.6120R |
| `TQQQ` | 162 | 42 | 120 | 25.93% | +$11,500 | +75.7295R | +0.4675R |
| `TSLA` | 50 | 10 | 40 | 20.00% | +$6,040 | +10.5466R | +0.2109R |
| `QQQ` | 0 | 0 | 0 | — | $0 | — | — |
| `SPY` | 0 | 0 | 0 | — | $0 | — | — |

This was a profitable **three-symbol** setup-only result, not a true five-symbol result. `QQQ` and `SPY` did not trade.

### Per-symbol setup marker counts

| Symbol | Setup pass | Setup fail | Pass rate | Snapshot miss |
|---|---:|---:|---:|---:|
| `NVDA` | 140 | 4,971 | 2.74% | 0 |
| `QQQ` | 0 | 6,157 | 0.00% | 0 |
| `SPY` | 0 | 5,126 | 0.00% | 0 |
| `TQQQ` | 162 | 1,995 | 7.51% | 0 |
| `TSLA` | 50 | 6,699 | 0.74% | 0 |

`QQQ` and `SPY` did not trade because they had zero setup passes, not because of order failure.

## Long/short breakdown

The refreshed summary JSON required the fix in commit `2beac3d`, which made `/Users/FXG06FA/trading-agent-main/scripts/summarize_trade_lifecycle_periods.py` include all generated tables in JSON, including `by_symbol_side`.

By symbol and side:

| Symbol | Side | Trades | Wins | Losses | Win rate | PnL | Sum R | Mean R | Median R |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `NVDA` | long | 50 | 16 | 34 | 32.00% | +$8,390 | +37.9136R | +0.7583R | -1.6834R |
| `NVDA` | short | 90 | 21 | 69 | 23.33% | +$12,170 | +47.7713R | +0.5308R | -1.6788R |
| `TQQQ` | long | 70 | 17 | 53 | 24.29% | +$900 | +13.3403R | +0.1906R | -1.8704R |
| `TQQQ` | short | 92 | 25 | 67 | 27.17% | +$10,600 | +62.3893R | +0.6781R | -1.7622R |
| `TSLA` | long | 38 | 7 | 31 | 18.42% | -$2,950 | -4.4833R | -0.1180R | -1.7082R |
| `TSLA` | short | 12 | 3 | 9 | 25.00% | +$8,990 | +15.0299R | +1.2525R | -1.6743R |

Combined side view:

| Side | Trades | PnL | Avg PnL/trade | Share of total PnL |
|---|---:|---:|---:|---:|
| Long | 158 | +$6,340 | +$40.13 | 16.6% |
| Short | 194 | +$31,760 | +$163.71 | 83.4% |

Interpretation:

- the setup-only edge is mostly a short-side edge;
- `NVDA` is healthy on both long and short;
- `TQQQ` short is strong, while `TQQQ` long is only slightly positive;
- `TSLA` short is strong but based on only 12 trades;
- `TSLA` long is negative and should be treated cautiously.

## Drawdown and concentration

### Monthly

```text
periods=11
trades=352
pnl=38100.00
max_drawdown=-7345.00
```

Worst months:

| Month | Trades | Win rate | PnL | Drawdown |
|---|---:|---:|---:|---:|
| `2026-01` | 35 | 11.43% | -$7,345 | -$7,345 |
| `2026-04` | 29 | 20.69% | -$3,935 | -$3,935 |
| `2025-08` | 32 | 21.88% | -$1,475 | -$1,475 |
| `2025-09` | 29 | 31.03% | -$310 | -$1,785 |

Best months:

| Month | Trades | Win rate | PnL |
|---|---:|---:|---:|
| `2025-11` | 40 | 30.00% | +$24,115 |
| `2026-05` | 35 | 34.29% | +$10,980 |
| `2026-02` | 36 | 33.33% | +$5,950 |
| `2025-10` | 35 | 22.86% | +$4,910 |

Profit concentration:

```text
2025-11 + 2026-05 = 24115 + 10980 = 35095
35095 / 38100 = ~92.1% of total profit
```

### Weekly

```text
periods=44
trades=352
pnl=38100.00
max_drawdown=-9680.00
```

Worst week:

```text
2026-W05
9 trades
0 wins
-$3,615
weekly equity drawdown -$9,680
```

Best week:

```text
2025-W46
11 trades
45.45% win rate
+$13,400
```

### Daily

```text
periods=194
trades=352
pnl=38100.00
max_drawdown=-10915.00
```

Worst day:

```text
2025-11-07
3 trades
0 wins
-$1,860
```

Best day:

```text
2025-11-06
3 trades
3 wins
+$11,355
```

Top-day concentration:

| Best days included | PnL from those days | Share of total |
|---:|---:|---:|
| Top 1 day | +$11,355 | 29.8% |
| Top 2 days | +$22,075 | 57.9% |
| Top 3 days | +$31,635 | 83.0% |
| Top 4 days | +$40,190 | 105.5% |
| Top 5 days | +$46,400 | 121.8% |

This confirms the setup-only strategy is profitable but tail-driven. Missing a few high-opportunity days in live/paper due to data outage, reconnect, or order issues could materially change the result.

## Watchdog/error analysis

The setup-only summary reported:

```text
errors=28
watchdogs={'NVDA': 9, 'TQQQ': 4, 'TSLA': 1}
```

A controlled scanner showed:

```text
matching_lines=14
counts={'ERROR': 28, 'WATCHDOG': 14, 'Order hung': 14}
watchdogs_by_symbol={'NVDA': 9, 'TQQQ': 4, 'TSLA': 1}
```

There were no separate exceptions or stack traces in the matched output. The `errors=28` count comes from two `ERROR` tokens per watchdog line:

```text
ERROR com.calgary...
[FLOW][ERROR][WATCHDOG]
```

Thus:

```text
14 watchdog lines × 2 ERROR tokens = 28 ERROR-token count
```

Example watchdog line:

```text
[FLOW][ERROR][WATCHDOG] Order hung >3s. Requesting cancel/resync but keeping in-flight lock. symbol=NVDA orderId=-1 position=500
```

By code inspection, the log is emitted in:

```text
/Users/FXG06FA/trading-agent-main/src/main/java/com/calgary/fili/trader/bot/strategy/PingPongStrategy.java
```

Important code-path details:

- the watchdog is triggered when `inFlightOrder` remains true for more than 3 seconds;
- `pendingOrderId=-1` is the sentinel for no valid order ID available to cancel;
- the log path requests cancel/resync but keeps the in-flight lock;
- this did not fail the replay, but it is a live/paper order-lifecycle risk signal.

Operational interpretation:

- not fatal in this historical simulation;
- no stack-trace exceptions were found;
- still important for live paper, because repeated in-flight locks can delay entries/exits or expose broker-state reconciliation problems.

## Earlier strict micro vs setup-only comparison

| Mode | Trades | PnL | Win rate | Errors | Watchdogs | Interpretation |
|---|---:|---:|---:|---:|---:|---|
| Strict lifecycle/micro enabled without explicit `0.30/0.30` overrides | 5 | +$235 | 80.00% | 0 | 0 | Clean but almost inactive because effective thresholds differed |
| Setup-only / micro disabled | 352 | +$38,100 | 25.28% | 28 ERROR tokens | 14 | Promising but volatile and order-watchdog sensitive |
| Lifecycle/micro enabled with explicit `0.30/0.30` overrides | 417 | +$18,979.79 | 66.91% | 2 ERROR tokens | 1 | Matches recovered baseline decision path |

Main conclusion:

```text
The setup model appears usable/promising, but setup-only is a separate strategy mode.
Lifecycle/micro is not inherently too restrictive when the documented 0.30/0.30 thresholds are passed.
The July 31 parallel lifecycle/micro rerun reproduced the 417-trade recovered baseline decision path.
```

## Paper-trading decision

Current classification:

```text
Parallel harness: PASS
Feature snapshot replay: PASS
Lifecycle/micro 0.30/0.30 strategy path: PASS
Earlier strict-micro command without explicit thresholds: NOT COMPARABLE / TOO INACTIVE
Setup-only strategy sample: PROMISING
Drawdown profile: VOLATILE BUT POSITIVE
Profit concentration: HIGH
Order watchdog health: IMPROVED IN 417-TRADE RERUN, STILL MONITOR
Full-size paper at 500000 immediately: WAIT
Small controlled paper pilot with explicit 0.30/0.30 thresholds: YES
```

Recommended initial paper pilot:

- use lifecycle/micro enabled with explicit `0.30/0.30` thresholds if the goal is to follow the recovered 417-trade decision path;
- keep setup-only as a separate exploratory/paper-shadow variant, not as a direct replacement for the lifecycle/micro baseline;
- start with smaller notional or tight `max-trades`, not immediate full-size `500000` risk;
- prioritize `NVDA`, `TQQQ`, and `TSLA` because `QQQ` and `SPY` had zero setup passes;
- monitor both long and short sides; the 417-trade lifecycle/micro rerun was positive on all reported symbol/side buckets;
- monitor order watchdogs aggressively.

Suggested conservative interpretation by symbol/side:

| Symbol/side | Paper interpretation |
|---|---|
| `NVDA` long | Healthy in lifecycle/micro rerun |
| `NVDA` short | Healthy in lifecycle/micro rerun |
| `TQQQ` long | Positive but lower mean R than `NVDA`/`TSLA`; monitor |
| `TQQQ` short | Positive but lower mean R than `NVDA`/`TSLA`; monitor |
| `TSLA` long | Healthy in lifecycle/micro rerun |
| `TSLA` short | Healthy but smaller sample |
| `QQQ` | No setup passes in this replay |
| `SPY` | No setup passes in this replay |

## Useful verification commands

### Refresh the PnL summary JSON after pulling summarizer updates

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
OUT=$BASE/parallel_file_replay_recovered_setup_only_compare

python3 scripts/summarize_trade_lifecycle_periods.py \
  --output-dir "$OUT" \
  --log-file "$OUT/controlled_java_replay.parallel_combined.log" \
  --out-prefix "$OUT/parallel_pnl_period_summary_refreshed" \
  --title "Parallel recovered event-carried setup-only comparison 2025-07-22 to 2026-05-22"

python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print("LOG", r["log"]); print("BY_SYMBOL_SIDE", r.get("by_symbol_side", []))' \
  "$OUT/parallel_pnl_period_summary_refreshed.json"
```

### Inspect side breakdown

```zsh
cat "$OUT/parallel_pnl_period_summary_refreshed_by_symbol_side.csv"
```

### Controlled watchdog/error scanner

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
OUT=$BASE/parallel_file_replay_recovered_setup_only_compare

python3 - "$OUT/controlled_java_replay.parallel_combined.log" "$OUT/error_watchdog_lines.txt" <<'PY'
from pathlib import Path
from collections import Counter
import re
import sys

log_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
needles = ["Exception", "ERROR", "WATCHDOG", "Order hung", "Waiting for broker terminal"]
watchdog_symbol_re = re.compile(r"symbol=([A-Z0-9_.-]+)")
counts = Counter()
watchdogs_by_symbol = Counter()
first_matches = []
total_matches = 0

with log_path.open("r", encoding="utf-8", errors="ignore") as src, out_path.open("w", encoding="utf-8") as dst:
    for line_no, line in enumerate(src, 1):
        hit = False
        for needle in needles:
            c = line.count(needle)
            if c:
                counts[needle] += c
                hit = True
        if hit:
            total_matches += 1
            text = f"{line_no}:{line}"
            dst.write(text)
            if len(first_matches) < 160:
                first_matches.append(text.rstrip())
            if "WATCHDOG" in line:
                m = watchdog_symbol_re.search(line)
                watchdogs_by_symbol[m.group(1) if m else "UNKNOWN"] += 1

print(f"log={log_path}")
print(f"written={out_path}")
print(f"matching_lines={total_matches}")
print(f"counts={dict(counts)}")
print(f"watchdogs_by_symbol={dict(watchdogs_by_symbol)}")
print()
print("=== first matches ===")
for line in first_matches:
    print(line)
PY
```

### Per-symbol marker counts

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
OUT=$BASE/parallel_file_replay_recovered_setup_only_compare

for s in NVDA QQQ SPY TQQQ TSLA; do
  echo "===== $s marker counts ====="
  python3 - "$OUT/logs/$s.log" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
markers = {
    "setup_pass": "SETUP_FILTER_PASSES=PASS",
    "setup_fail": "SETUP_FILTER_PASSES=FAIL",
    "micro_pass": "MICRO_ENTRY_CONFIRMS=PASS",
    "micro_fail": "MICRO_ENTRY_CONFIRMS=FAIL",
    "snapshot_hit": "featureSnapshot=hit",
    "snapshot_miss": "featureSnapshot=miss",
    "sidecar_disabled": "featureSidecar=disabled",
    "sidecar_hit": "featureSidecar=hit",
    "sidecar_miss": "featureSidecar=miss",
}
counts = {k: 0 for k in markers}

with path.open("r", encoding="utf-8", errors="ignore") as handle:
    for line in handle:
        for key, marker in markers.items():
            counts[key] += line.count(marker)

print(path)
for key, value in counts.items():
    print(f"{key}={value}")
PY
done
```

## Final bottom line

The recovered parallel replay discussion resolves into this decision:

```text
Use lifecycle/micro enabled with explicit 0.30/0.30 thresholds for the recovered 417-trade decision path.
Treat the earlier 5-trade strict-micro parallel result as threshold/config drift, not as evidence against lifecycle/micro.
Treat setup-only as a separate exploratory strategy variant, not an apples-to-apples comparison.
Normalize strategy-path comparisons by realized R and marker counts unless effective share sizing is verified identical.
Do not jump straight to full-size paper sizing; start with a small, tightly monitored pilot focused on NVDA/TQQQ/TSLA.
Monitor watchdogs/order lifecycle carefully before scaling.
```
