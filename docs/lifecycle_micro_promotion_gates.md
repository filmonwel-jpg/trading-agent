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

Pass either a precomputed drift report:

```bash
python3 scripts/validate_lifecycle_micro_promotion.py \
  --drift-report runtime/reports/paper_shadow_drift.json
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
