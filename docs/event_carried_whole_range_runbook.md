# Event-carried whole-range Databento backtest runbook

Date: 2026-07-22

This runbook documents the one-command workflow for building a continuous Databento Core-5 replay range, generating the matching downstream setup sidecar for that same range, injecting event-carried feature snapshots, validating no-sidecar replay behavior, running the trade-enabled replay, and producing PnL summaries by day, week, month, symbol, symbol/period, and all symbols.

The workflow is implemented by:

```text
scripts/run_event_carried_whole_range.sh
scripts/summarize_trade_lifecycle_periods.py
```

## What this workflow does

`run_event_carried_whole_range.sh` runs these steps in order:

1. Build a continuous sliced replay with daily `previous_close` events.
2. Validate slice coverage and `previous_close` ordering.
3. Run a normal replay on the slice to generate setup/micro log markers.
4. Generate `setup_downstream_confirmable_labels_v1.csv` from that replay.
5. Join labels to the enriched 30s cache and create `setup_downstream_training_rows_v1.csv`.
6. Inject those rows into the replay as event-carried snapshots.
7. Optionally run no-trade event-carried validation with sidecar disabled.
8. Optionally run trade-enabled event-carried replay.
9. Summarize trade lifecycle PnL by all/symbol/day/week/month and symbol-period combinations.

This is intentionally different from the older split four-week process: it runs one continuous range, so state can carry through the full selected interval.

## Pull the required scripts on the other Mac

Run this on the other computer:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

git fetch origin ai-training-dynamic-upgrade-20260612
git checkout ai-training-dynamic-upgrade-20260612
git pull --ff-only origin ai-training-dynamic-upgrade-20260612
```

Verify the two workflow scripts are present and executable:

```zsh
ls -lh scripts/run_event_carried_whole_range.sh
ls -lh scripts/summarize_trade_lifecycle_periods.py
```

## Required local inputs

The command expects these paths to exist on the other Mac unless overridden:

```text
runtime/replay/databento-20260523-core5.ndjson.gz
runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv
```

Check them first:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

ls -lh runtime/replay/databento-20260523-core5.ndjson.gz
ls -lh runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
ls -ld runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
ls -ld runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports
ls -lh /Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv
```

If the 30s cache path moved, search the mounted external disk:

```zsh
find /Volumes -path '*broader_213d_six_source_enriched_30s_20260619_065347*combined_30s.csv' -print 2>/dev/null
find /Volumes -name combined_30s.csv -path '*model_training_sets*' -print 2>/dev/null
```

## Recommended continuous run: earlier + recent windows together

This covers the two windows already investigated as one continuous run:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried
mkdir -p "$BASE"

nohup env \
  START=2026-03-27 \
  END=2026-05-22 \
  INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  RUN_NOTRADE=1 \
  RUN_TRADE=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch.log" 2>&1 &

echo "PID=$!"
```

Monitor it:

```zsh
tail -f runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried/whole_range_batch.log
```

If the terminal closes, open a new terminal and keep monitoring the same log:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

tail -f runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried/whole_range_batch.log
```

The run is complete when the log prints:

```text
[WHOLE_RANGE] DONE
```

## Full available range mode

Use this if you want the script to discover the source replay range automatically. It chooses the second detected session as `START` so the slice builder can observe a prior close before the first traded session.

For long full-range runs, set `RUN_DRIFT=0`. The strict drift comparison scans the full no-trade Java log and can take much longer than the replay itself on very large logs. You can still validate event-carried coverage afterward with the one-pass telemetry commands below.

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-auto-event-carried
mkdir -p "$BASE"

nohup env \
  START=auto \
  END=auto \
  BASE="$BASE" \
  INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch.log" 2>&1 &

echo "PID=$!"
```

Monitor:

```zsh
tail -f runtime/local-backtests/databento-core5-whole-auto-event-carried/whole_range_batch.log
```

## Exact eight-month mode

If the source replay contains an exact eight-month interval, set explicit dates:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

START=2025-09-22
END=2026-05-22
START_TAG=$(printf '%s' "$START" | tr -d '-')
END_TAG=$(printf '%s' "$END" | tr -d '-')
BASE=runtime/local-backtests/databento-core5-whole-${START_TAG}-${END_TAG}-event-carried
mkdir -p "$BASE"

nohup env \
  START="$START" \
  END="$END" \
  BASE="$BASE" \
  INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch.log" 2>&1 &

echo "PID=$!"
```

If the requested start/end are outside the source replay coverage, the slice build or validation step will fail. In that case use `START=auto END=auto`, or inspect available sessions with:

```zsh
python3 - <<'PY'
import gzip
import re
from datetime import date

path = 'runtime/replay/databento-20260523-core5.ndjson.gz'
day_re = re.compile(r'\bday=(\d{8})\b')
days = set()
with gzip.open(path, 'rt', encoding='utf-8', errors='ignore') as stream:
    for line in stream:
        match = day_re.search(line)
        if match:
            raw = match.group(1)
            days.add(date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])))
ordered = sorted(days)
print('session_count =', len(ordered))
print('first_session =', ordered[0] if ordered else None)
print('second_session =', ordered[1] if len(ordered) > 1 else None)
print('last_session =', ordered[-1] if ordered else None)
PY
```

## Full discovered source range as of 2026-07-23

The source replay inspected on the other Mac contained sessions from `2025-07-21` through `2026-05-22`. Use `2025-07-22` as the first traded session so the builder can use `2025-07-21` as the prior-close source:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

START=2025-07-22
END=2026-05-22
START_TAG=$(printf '%s' "$START" | tr -d '-')
END_TAG=$(printf '%s' "$END" | tr -d '-')
BASE=runtime/local-backtests/databento-core5-whole-${START_TAG}-${END_TAG}-event-carried
mkdir -p "$BASE"

nohup env \
  START="$START" \
  END="$END" \
  BASE="$BASE" \
  INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > "$BASE/whole_range_batch.log" 2>&1 &

echo "PID=$!"
```

Monitor:

```zsh
tail -f runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried/whole_range_batch.log
```

## Output layout

For `START=2026-03-27 END=2026-05-22`, outputs are written under:

```text
runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried/
```

Important files and folders:

```text
build_slice.log
validate_slice.log
inject_event_snapshots.log
run/databento-core5-whole-20260327-20260522-<RUN_TS>.log
setup_micro_counterfactual_<RUN_TS>/setup_downstream_confirmable_labels_v1.csv
setup_micro_counterfactual_<RUN_TS>/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
databento-20260327-20260522-core5-whole-daily-prevclose.ndjson.gz
databento-20260327-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
controlled_java_replay_downstream_setup_filter_event_snapshot_notrade/controlled_java_replay.log
controlled_java_replay_downstream_setup_filter_event_snapshot_notrade/event_snapshot_replay_drift/event_snapshot_replay_drift_report.md
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/controlled_java_replay.log
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522.md
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522.json
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_all.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_symbol.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_day.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_week.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_month.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_symbol_day.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_symbol_week.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_symbol_month.csv
controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl/pnl_period_summary_20260327_20260522_by_symbol_side.csv
```

## Inspect completion, event-carried telemetry, and PnL summaries

After the batch finishes:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried
NO_TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade
TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl
SUMMARY_PREFIX=$TRADE_OUT/pnl_period_summary_20260327_20260522

grep '\[BACKTEST\] completed' "$NO_TRADE_OUT/controlled_java_replay.log" | tail -5
grep '\[BACKTEST\] completed' "$TRADE_OUT/controlled_java_replay.log" | tail -5

echo '--- no-trade event-carried validation ---'
printf 'SETUP PASS '; grep -c 'SETUP_FILTER_PASSES=PASS' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SETUP FAIL '; grep -c 'SETUP_FILTER_PASSES=FAIL' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT HIT '; grep -c 'featureSnapshot=hit' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT MISS '; grep -c 'featureSnapshot=miss' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR DISABLED '; grep -c 'featureSidecar=disabled' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR HIT '; grep -c 'featureSidecar=hit' "$NO_TRADE_OUT/controlled_java_replay.log"
printf 'ERRORS '; grep -Ec 'Exception|ERROR' "$NO_TRADE_OUT/controlled_java_replay.log"

echo '--- trade-enabled event-carried replay ---'
printf 'SETUP PASS '; grep -c 'SETUP_FILTER_PASSES=PASS' "$TRADE_OUT/controlled_java_replay.log"
printf 'SETUP FAIL '; grep -c 'SETUP_FILTER_PASSES=FAIL' "$TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT HIT '; grep -c 'featureSnapshot=hit' "$TRADE_OUT/controlled_java_replay.log"
printf 'SNAPSHOT MISS '; grep -c 'featureSnapshot=miss' "$TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR DISABLED '; grep -c 'featureSidecar=disabled' "$TRADE_OUT/controlled_java_replay.log"
printf 'SIDECAR HIT '; grep -c 'featureSidecar=hit' "$TRADE_OUT/controlled_java_replay.log"
printf 'ERRORS '; grep -Ec 'Exception|ERROR' "$TRADE_OUT/controlled_java_replay.log"

echo '--- PnL summaries ---'
cat "${SUMMARY_PREFIX}_all.csv"
cat "${SUMMARY_PREFIX}_by_symbol.csv"
cat "${SUMMARY_PREFIX}_by_month.csv"
cat "${SUMMARY_PREFIX}_by_week.csv"
cat "${SUMMARY_PREFIX}_by_day.csv"
```

Open the Markdown report for a human-readable summary:

```zsh
cat "$SUMMARY_PREFIX.md"
```

## Re-run only the summarizer

If the trade-enabled replay already completed and you only want to regenerate summaries:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried
TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl

python3 scripts/summarize_trade_lifecycle_periods.py \
  --output-dir "$TRADE_OUT" \
  --log-file "$TRADE_OUT/controlled_java_replay.log" \
  --out-prefix "$TRADE_OUT/pnl_period_summary_20260327_20260522" \
  --title "Whole-range event-carried trade PnL 2026-03-27 to 2026-05-22"
```

## Useful toggles

Set these environment variables before running the workflow when needed:

| Variable | Default | Use |
|---|---:|---|
| `START` | `auto` | First session date, or `auto`. |
| `END` | `auto` | Last session date, or `auto`. |
| `BASE` | derived from date tags | Override the output directory. |
| `SOURCE_EVENTS` | `runtime/replay/databento-20260523-core5.ndjson.gz` | Source recorded NDJSON/NDJSON.GZ replay. |
| `INPUT_30S_CSV` | required | Enriched 30s cache CSV or directory of `*_30s_training.csv` files. |
| `ROUTE_MANIFEST` | recent CatBoost route manifest | Downstream setup filter model/schema manifest. |
| `SETUP_MODEL_DIR` | current setup model dir | Normal replay setup model directory. |
| `LIFECYCLE_MODEL_DIR` | current lifecycle model exports | Normal replay lifecycle model directory. |
| `RUN_NOTRADE` | `1` | Run no-trade event-carried validation. |
| `RUN_DRIFT` | `1` | Run strict no-trade key drift comparison. Use `0` for long full-range runs if the log scan is too slow. |
| `RUN_TRADE` | `1` | Run trade-enabled event-carried replay and summaries. |
| `REBUILD_EXISTING` | `0` | Set to `1` to rebuild slice/sidecar/enriched events even if files exist. |
| `PYTHON_BIN` | `python3` | Python executable. |

Example: rebuild existing artifacts from scratch:

```zsh
nohup env \
  START=2026-03-27 \
  END=2026-05-22 \
  REBUILD_EXISTING=1 \
  INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv \
  RUN_NOTRADE=1 \
  RUN_DRIFT=0 \
  RUN_TRADE=1 \
  bash scripts/run_event_carried_whole_range.sh \
  > runtime/local-backtests/databento-core5-whole-20260327-20260522-event-carried/whole_range_batch_rebuild.log 2>&1 &
```

## Expected validation shape

For a clean event-carried/no-sidecar validation, expect:

```text
featureSnapshot=hit > 0
featureSnapshot=miss 0
featureSidecar=disabled equals featureSnapshot=hit
featureSidecar=hit 0
EVENT_SNAPSHOT_REPLAY_DRIFT status=PASS
```

For long runs with `RUN_DRIFT=0`, replace the strict drift report with a one-pass telemetry scan of the no-trade log and require the same shape except for the `EVENT_SNAPSHOT_REPLAY_DRIFT` line.

If `featureSnapshot=miss` is non-zero, inspect:

```zsh
cat "$BASE/inject_event_snapshots.log"
cat "$BASE/validate_slice.log"
cat "$NO_TRADE_OUT/event_snapshot_replay_drift/event_snapshot_replay_drift_report.md"
```

## If the batch appears stuck after no-trade `BACKTEST_RC=0`

If the parent batch log stops after:

```text
[WHOLE_RANGE] running no-trade event-carried validation
BACKTEST_RC=0
```

then the script is usually scanning the full no-trade log in `compare_event_snapshot_replay_drift.py`. For a 213-session run, it is safe to stop only that drift step and run the trade-enabled replay manually because the no-trade backtest has already completed successfully.

Stop watching the log with `Ctrl-C`, then inspect active processes:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
NO_TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade
TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl

ps -axo pid,ppid,etime,pcpu,pmem,command | egrep 'run_event_carried_whole_range|compare_event_snapshot|DatabentoHistorical|java|python3' | grep -v egrep
ls -lh "$BASE/whole_range_batch.log" "$NO_TRADE_OUT/controlled_java_replay.log" "$NO_TRADE_OUT/event_snapshot_replay_drift.log" 2>/dev/null
```

If `compare_event_snapshot_replay_drift.py` is the only active child, stop it:

```zsh
pkill -f 'scripts/compare_event_snapshot_replay_drift.py.*databento-core5-whole-20250722-20260522'
sleep 3
pgrep -fl 'compare_event_snapshot_replay_drift.py|run_event_carried_whole_range.sh'
```

Then launch the trade-enabled replay using the already-built enriched replay:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
ENRICHED_EVENTS=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core.ndjson.gz
TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_trade_pnl
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json

rm -rf "$TRADE_OUT"
mkdir -p "$TRADE_OUT"

export JAVA_TOOL_OPTIONS='-Dbacktest.strategy.tradeAmount=60000 -Dtrading.trade-amount=60000 -Dtrading.risk.max-order-notional=70000 -Dbacktest.strategy.maxOrderNotional=70000'

./mvnw -q -DskipTests package
./mvnw -q dependency:build-classpath -Dmdep.outputFile=runtime/backtests/databento_ibkr_sim_backtest_cp.txt

nohup env PYTHON_BIN=python3 SKIP_BUILD=true \
  scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$ENRICHED_EVENTS" \
  --output-dir "$TRADE_OUT" \
  --downstream-setup-filter-manifest "$ROUTE_MANIFEST" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --timeout-seconds 0 \
  --max-trades 2000 \
  > "$TRADE_OUT/controlled_java_replay.log" 2>&1 &

echo "PID=$!"
tail -f "$TRADE_OUT/controlled_java_replay.log"
```

## If strict drift is `NO-GO` because of extra `featureSnapshot=miss` rows

For long whole-range runs, the strict drift report may show that every original sidecar row was hit, but the no-trade replay generated extra setup-filter candidates not present in the sidecar, for example:

```text
feature_snapshot_counts = {'hit': 48914, 'miss': 229}
missing_expected_rows = 0
extra_event_rows = 229
```

This means the existing enriched replay covered the generated sidecar, but not the extra setup candidates found only during the event-carried validation replay. Recover by extracting those miss keys, joining them to the 30s cache, appending them to the sidecar, re-injecting, and rerunning validation/trade.

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento

BASE=runtime/local-backtests/databento-core5-whole-20250722-20260522-event-carried
RUN_TS=20260722_100315
NO_TRADE_OUT=$BASE/controlled_java_replay_downstream_setup_filter_event_snapshot_notrade

ORIG_SLICE=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.ndjson.gz
ORIG_SIDECAR=$BASE/setup_micro_counterfactual_$RUN_TS/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv
RECOVERY_DIR=$BASE/setup_micro_counterfactual_$RUN_TS/event_snapshot_miss_recovery
MISS_LABELS=$RECOVERY_DIR/event_snapshot_miss_labels.csv
MISS_ROWS_DIR=$RECOVERY_DIR/downstream_setup_training_rows
MISS_SIDECAR=$MISS_ROWS_DIR/setup_downstream_training_rows_v1.csv
MERGED_SIDECAR=$RECOVERY_DIR/setup_downstream_training_rows_v1.with_event_snapshot_miss_recovery.csv
ENRICHED_RECOVERED=$BASE/databento-20250722-20260522-core5-whole-daily-prevclose.event-snapshots-catboost-core-recovered.ndjson.gz
ROUTE_MANIFEST=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json
INPUT_30S_CSV=/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv

mkdir -p "$RECOVERY_DIR"

python3 scripts/extract_event_snapshot_miss_labels.py \
  --event-log "$NO_TRADE_OUT/controlled_java_replay.log" \
  --existing-sidecar "$ORIG_SIDECAR" \
  --output-labels "$MISS_LABELS"

python3 -u scripts/build_downstream_setup_training_rows.py \
  --input-30s-csv "$INPUT_30S_CSV" \
  --labels-csv "$MISS_LABELS" \
  --output-dir "$MISS_ROWS_DIR" \
  --join-tolerance-seconds 31

python3 scripts/merge_downstream_setup_sidecars.py \
  --input "$ORIG_SIDECAR" \
  --input "$MISS_SIDECAR" \
  --output "$MERGED_SIDECAR"

python3 scripts/inject_downstream_setup_features_into_ndjson.py \
  --input-events "$ORIG_SLICE" \
  --sidecar-csv "$MERGED_SIDECAR" \
  --manifest "$ROUTE_MANIFEST" \
  --output-events "$ENRICHED_RECOVERED" \
  --snapshot-source setup_downstream_training_rows_v1_with_event_snapshot_miss_recovery
```

Then rerun no-trade validation against `ENRICHED_RECOVERED`. If snapshot misses are zero, use `ENRICHED_RECOVERED` for the trade-enabled replay.

## Current split-window reference results

These are prior reference results from the two separate four-week-style runs, useful for comparison only. The whole-range continuous run can differ because state is not reset between the earlier and recent intervals.

| Range | Mode | Closed trades | Total PnL | Realized R | Notes |
|---|---|---:|---:|---:|---|
| `2026-03-27`..`2026-04-24` | event-carried trade-enabled | 19 | `-224.79` | `-3.6055` | 3 watchdog warnings. |
| `2026-04-27`..`2026-05-22` | event-carried trade-enabled | 110 | `+17662.42` | `+97.0384` | Reproduced recent enriched result. |
| split-window combined | arithmetic sum only | 129 | `+17437.63` | `+93.4329` | Not the same as a continuous whole-range run. |

## What not to commit

Generated runtime artifacts should stay local unless explicitly needed for a report:

```text
runtime/local-backtests/databento-core5-whole-*/
runtime/local-backtests/databento-core5-4week-*/run/
*.ndjson.gz
*.summary.json
*-trade-lifecycle-summary.csv
```

Only source scripts and runbook documentation need to be committed for another checkout to run the workflow.

