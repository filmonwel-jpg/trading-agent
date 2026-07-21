# Lifecycle/micro promotion gates

This runbook validates the upgraded lifecycle/micro route before any paper/live promotion. The validator writes both JSON and Markdown reports and marks missing evidence as `NO-GO` instead of silently passing.

## 1. Runtime calibration and label-economics report

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir runtime/reports/lifecycle_micro_promotion_gate
```

For an intentionally raw-probability research bundle, use:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --allow-raw-only-calibration \
  --output-dir runtime/reports/lifecycle_micro_promotion_gate
```

Raw-only calibration should remain research-only unless the replay, backtest, drift, and label-economics gates are explicitly accepted.

## 2. Recorded-event replay parity

Replay a recorded normalized NDJSON stream through the Java strategy/backtester path:

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source ndjson \
  --recorded-events runtime/replay/example-recorded-events.ndjson.gz \
  --symbol TSLA \
  --lifecycle-model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir runtime/backtests/recorded_replay
```

Then compare live/paper decisions against replay decisions when those CSVs are available:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --recorded-events runtime/replay/example-recorded-events.ndjson.gz \
  --live-decisions runtime/replay/live-decisions.csv \
  --replay-decisions runtime/replay/replay-decisions.csv \
  --backtest-dir runtime/backtests/recorded_replay \
  --paper-decisions runtime/paper/paper-decisions.csv \
  --shadow-decisions runtime/shadow/shadow-decisions.csv
```

## 3. Historical API full backtest / PnL / day-dominance

```bash
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols-file runtime/symbols_100.txt \
  --start 2026-05-01 \
  --end 2026-06-19 \
  --lifecycle-model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --output-dir runtime/backtests/lifecycle_micro_full
```

Validate the resulting summaries:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --model-dir runtime/research_runs/lifecycle_micro_bar_regen_20260622/model_exports \
  --backtest-dir runtime/backtests/lifecycle_micro_full \
  --min-backtest-trades 20 \
  --min-backtest-symbols 5
```

## 4. Paper/shadow drift

For event-carried enriched downstream setup snapshots, first run the dedicated paper/shadow validator. It can compare either Java logs with `SETUP_FILTER_PASSES`/`featureSnapshot=hit|miss|disabled` telemetry or paired decision CSVs:

```bash
python3 scripts/validate_paper_shadow_drift.py \
  --paper-log runtime/paper/paper.log \
  --shadow-log runtime/shadow/shadow.log \
  --output-dir runtime/reports/paper_shadow_drift \
  --max-probability-drift 0.02 \
  --max-decision-mismatch-rate 0.01 \
  --min-feature-snapshot-hit-rate 0.95 \
  --fail-on-no-go
```

See `docs/event_carried_enriched_snapshots.md` for the event contract, feature precedence, and validator details.

Pass either a precomputed drift report:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --drift-report runtime/reports/paper_shadow_drift/paper_shadow_drift_report.json
```

or paired decision CSVs:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --paper-decisions runtime/paper/paper-decisions.csv \
  --shadow-decisions runtime/shadow/shadow-decisions.csv \
  --max-probability-drift 0.02 \
  --max-decision-mismatch-rate 0.01
```

## Report locations

Default outputs:

- `runtime/reports/lifecycle_micro_promotion_gate/lifecycle_micro_promotion_gate_report.json`
- `runtime/reports/lifecycle_micro_promotion_gate/lifecycle_micro_promotion_gate_report.md`

## 5. One-week and four-week recorded replay runbook

These commands replay the local core-five recorded NDJSON slices used for the June 26/27 lifecycle-micro checks. They assume the latest June 24 setup and lifecycle/micro bundles are present:

- Setup: `runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854`
- Lifecycle/micro: `runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports`
- Source recording: `runtime/replay/databento-20260523-core5.ndjson.gz`

Start from the Databento worktree:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
```

Optional model/threshold preflight:

```zsh
python3 runtime/local-backtests/databento-core5-week-20260518-20260522-recent/validate_latest_artifacts.py \
  | tee runtime/local-backtests/databento-core5-week-20260518-20260522-recent/latest_artifact_validation.txt
```

### One-week replay: 2026-05-18 through 2026-05-22

Build the one-week recorded slice:

```zsh
python3 runtime/local-backtests/databento-core5-week-20260518-20260522-recent/build_week_slice_fast.py \
  | tee runtime/local-backtests/databento-core5-week-20260518-20260522-recent/build_week_slice_fast.log
```

Validate gzip integrity:

```zsh
gzip -t runtime/local-backtests/databento-core5-week-20260518-20260522-recent/databento-20260518-20260522-core5-week-fast.ndjson.gz
```

Run the one-week replay backtest:

```zsh
bash <<'BASH'
set -euo pipefail

ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
cd "$ROOT"

OUT_BASE="$ROOT/runtime/local-backtests/databento-core5-week-20260518-20260522-recent"
RUN_DIR="$OUT_BASE/run"
SLICE="$OUT_BASE/databento-20260518-20260522-core5-week-fast.ndjson.gz"
SETUP_DIR="$ROOT/runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854"
LIFECYCLE_DIR="$ROOT/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports"
SYMBOLS_FILE="$ROOT/config/databento_core_5_symbols.txt"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_LOG="$RUN_DIR/databento-core5-week-recent-$RUN_TS.log"

mkdir -p "$RUN_DIR"

cat > "$RUN_DIR/latest_run.env" <<ENV
RUN_LOG=$RUN_LOG
OUT_DIR=$RUN_DIR
SETUP_DIR=$SETUP_DIR
LIFECYCLE_DIR=$LIFECYCLE_DIR
SLICE=$SLICE
ENV

scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --source ndjson \
  --recorded-events "$SLICE" \
  --symbols-file "$SYMBOLS_FILE" \
  --start 2026-05-18 \
  --end 2026-05-22 \
  --output-dir "$RUN_DIR" \
  --model-dir "$SETUP_DIR" \
  --lifecycle-model-dir "$LIFECYCLE_DIR" \
  --timeout-seconds 0 \
  --max-trades 2000 \
  2>&1 | tee "$RUN_LOG"

exit "${PIPESTATUS[0]}"
BASH
```

Summarize one-week results:

```zsh
python3 runtime/local-backtests/databento-core5-week-20260518-20260522-recent/summarize_week_run.py \
  | tee runtime/local-backtests/databento-core5-week-20260518-20260522-recent/week_summary.txt
```

### Four-week replay: 2026-04-27 through 2026-05-22

Build the four-week recorded slice with one `previous_close` event per symbol/session:

```zsh
python3 runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/build_4week_slice_with_daily_prevclose.py \
  | tee runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/build_4week_slice_with_daily_prevclose.log
```

Validate gzip integrity:

```zsh
gzip -t runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz
```

Validate daily previous-close coverage and ordering:

```zsh
python3 runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/validate_4week_slice.py \
  | tee runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/validate_4week_slice.log
```

Run the saved four-week latest-model replay wrapper:

```zsh
bash runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/run_4week_latest_models.sh
```

Summarize four-week results:

```zsh
python3 runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/summarize_4week_results.py \
  | tee runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/summarize_4week_results.log
```

Optional four-week promotion-gate report:

```zsh
python3 scripts/validate_lifecycle_micro_promotion.py \
  --model-dir runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports \
  --recorded-events runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz \
  --backtest-dir runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/run \
  --output-dir runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/promotion_gate_report \
  --min-backtest-trades 20 \
  --min-backtest-symbols 5 \
  | tee runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/promotion_gate_report.log
```
