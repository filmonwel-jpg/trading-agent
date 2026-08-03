# Databento Core-5 weekly analysis runbook

Date: 2026-07-22

This runbook documents the repository-side utilities used to inspect Databento Core-5 weekly API backtest artifacts after the backtests have already run. These utilities are analysis/triage tools only: they do not place orders, promote models, or update production aliases.

## Scope

The analysis scripts focus on the six-week Core-5 pilot window and the five expected symbols:

```text
TSLA, TQQQ, NVDA, SPY, QQQ
```

Default week windows used by the scripts:

| Week | Window | Default output folder |
|---|---|---|
| `w01` | `2025-12-23`..`2025-12-26` | `databento_api_core5_w01_20251223_to_20251226` |
| `w02` | `2025-12-29`..`2026-01-02` | `databento_api_core5_w02_20251229_to_20260102` |
| `w03` | `2026-01-05`..`2026-01-09` | `databento_api_core5_w03_20260105_to_20260109` |
| `w04` | `2026-01-12`..`2026-01-16` | `databento_api_core5_w04_20260112_to_20260116` |
| `w05` | `2026-01-19`..`2026-01-23` | `databento_api_core5_w05_20260119_to_20260123` |
| `w06` | `2026-01-26`..`2026-01-30` | `databento_api_core5_w06_20260126_to_20260130` |

## Source files vs. generated artifacts

Source-like utilities to keep under version control:

```text
scripts/analyze_core5_weekly_run.py
scripts/analyze_databento_core5_weekly_outputs.py
scripts/calculate_core5_today_pnl.py
scripts/summarize_core5_today_only.py
runtime/run_short_threshold_policy_experiment.sh
```

Generated/runtime artifacts are not source and should normally stay uncommitted unless a specific report is being preserved intentionally:

```text
runtime/core5_*.md
runtime/core5_*.json
runtime/databento_short_threshold_policy_no_onnx_*/
runtime/research_runs/databento_core5_weekly_6mo_analysis_*.md
runtime/research_runs/*/logs/
/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests/databento_api_core5_w*/
/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests/databento_api_core5_weekly_6mo_logs/
```

## Prerequisites

- Run from the repository root: `/Users/FXG06FA/trading-agent-main`.
- Use Python 3. The four Core-5 analysis scripts are intentionally standard-library only.
- Mount the external Databento disk at the default location before running analysis:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests
```

If the vault is mounted somewhere else, pass `--ext` to scripts that support it, or update the hardcoded constants in the run-specific scripts.

## Expected artifact layout

Backtest outputs are expected below the external root:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests/
  databento_api_core5_weekly_6mo_logs/
    w01_*.log
    w02_*.log
    ...
  databento_api_core5_w01_20251223_to_20251226/
    TSLA-2025-12-23-to-2025-12-26-<stamp>-orders.csv
    TSLA-2025-12-23-to-2025-12-26-<stamp>-trades.csv
    TSLA-2025-12-23-to-2025-12-26-<stamp>-trade-lifecycle-summary.csv
    TSLA-2025-12-23-to-2025-12-26-<stamp>-stream-sanity.json
    ...
```

The scripts classify these artifact types:

| Artifact | Purpose |
|---|---|
| `*-orders.csv` | Order history exported by the backtest runner. |
| `*-trades.csv` | Trade log with per-trade and cumulative PnL columns when available. |
| `*-trade-lifecycle-summary.csv` | Lifecycle-level closed-trade summary; used by the PnL aggregator. |
| `*-stream-sanity.json` | Compact stream sanity/coverage diagnostics. |
| `wXX_*.log` | Java runner log parsed for completion counts, symbol starts, warnings, and errors. |

## Recommended triage flow

### 1. Summarize the fixed “today-only” run

Use this first when validating the 2026-06-24 run stamps embedded in `scripts/summarize_core5_today_only.py`.

```zsh
cd /Users/FXG06FA/trading-agent-main

python3 scripts/summarize_core5_today_only.py
```

Default outputs:

```text
runtime/core5_weekly_6week_today_only_20260624.md
runtime/core5_weekly_6week_today_only_20260624.json
```

This script checks each configured week/stamp for orders, trades, lifecycle summaries, and sanity files. It also extracts `w06` failure context from the runner log.

Run-specific notes:

- The week list, run stamps, and expected return codes are hardcoded in `WEEKS`.
- Update `WEEKS` before reusing this script for a different run date.

### 2. Calculate lifecycle PnL for the fixed run

Use this after the today-only summary confirms the expected lifecycle files exist.

```zsh
cd /Users/FXG06FA/trading-agent-main

python3 scripts/calculate_core5_today_pnl.py
```

Default outputs:

```text
runtime/core5_today_pnl_20260624.md
runtime/core5_today_pnl_20260624.json
```

The script sums `TradePnL` from `*-trade-lifecycle-summary.csv`, cross-checks against `*-trades.csv`, and reports totals by week, symbol, and individual trade.

Run-specific notes:

- `WEEKS` includes fixed run stamps from the 2026-06-24 artifact set.
- The current script treats `w06` as partial and calls out missing/failed `NVDA` lifecycle output; revise that note when reusing the script for a clean full run.

### 3. Build the full six-week artifact inventory

Use this for a broad markdown + JSON report across the six configured weekly folders.

```zsh
cd /Users/FXG06FA/trading-agent-main

STAMP="$(date +%Y%m%d_%H%M%S)"
python3 scripts/analyze_core5_weekly_run.py \
  --ext /Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests \
  --report "runtime/core5_weekly_6week_analysis_${STAMP}.md" \
  --json "runtime/core5_weekly_6week_analysis_${STAMP}.json"
```

This report includes:

- per-week completion status,
- file counts and total artifact size,
- present/missing artifact sets by symbol,
- CSV row counts and compact JSON stats,
- log tail and recent error-like lines.

### 4. Build the deeper weekly-output report

Use this for the most detailed artifact-by-symbol view and a concise console status table.

```zsh
cd /Users/FXG06FA/trading-agent-main

STAMP="$(date +%Y%m%d_%H%M%S)"
python3 scripts/analyze_databento_core5_weekly_outputs.py \
  --ext /Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests \
  --report "runtime/research_runs/databento_core5_weekly_6mo_analysis_${STAMP}.md"
```

This script parses runner log artifact paths when present, falls back to globbing by symbol/date range, and classifies each week as `COMPLETE`, `PARTIAL_FAILED`, `PARTIAL`, `FAILED_NO_OUTPUT`, or `MISSING`.

## Script reference

| Script | Main use | CLI arguments | Default output |
|---|---|---|---|
| `scripts/summarize_core5_today_only.py` | Fast run-stamp-specific check for the 2026-06-24 six-week output set. | No CLI arguments. Edit constants for new runs. | `runtime/core5_weekly_6week_today_only_20260624.{md,json}` |
| `scripts/calculate_core5_today_pnl.py` | PnL rollup from lifecycle summaries with trade CSV cross-checks. | No CLI arguments. Edit constants for new runs. | `runtime/core5_today_pnl_20260624.{md,json}` |
| `scripts/analyze_core5_weekly_run.py` | Broad inventory, missing-artifact matrix, and log-tail triage. | `--ext`, `--report`, `--json` | `runtime/core5_weekly_6week_analysis_20260624.{md,json}` |
| `scripts/analyze_databento_core5_weekly_outputs.py` | Deep per-symbol artifact metrics and weekly status classification. | `--ext`, `--report` | `runtime/research_runs/databento_core5_weekly_6mo_analysis_20260624.md` |

## Related research harness: short-threshold policy experiment

`runtime/run_short_threshold_policy_experiment.sh` is a zsh research harness, not a generated report. It trains no-ONNX RandomForest runs for the `liquidity` and `equs` Databento silver feature presets while enabling the research short-threshold floor.

```zsh
cd /Users/FXG06FA/trading-agent-main

zsh runtime/run_short_threshold_policy_experiment.sh
```

Important behavior:

- Requires this input CSV to be readable:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/pilot_10d_six_source_enriched_30s_20260617_220849/combined/combined_30s.csv
```

- Writes a timestamped output root:

```text
runtime/databento_short_threshold_policy_no_onnx_<RUN_STAMP>/
  run_config.log
  liquidity/
    run_marker.log
    train_30s_no_onnx.log
  equs/
    run_marker.log
    train_30s_no_onnx.log
```

- Sets `UPDATE_CANONICAL_MODEL_ALIASES=0`, so it should not promote the trained models into canonical runtime resources.
- Passes `--no-onnx` to `train_30s_models.py`, so this is for threshold/policy research rather than deployment bundle creation.

## Troubleshooting

### External disk missing

Symptoms:

- reports show every output folder as missing,
- logs are reported as missing,
- the policy harness exits with `ERROR: input CSV is not readable`.

Checks:

```zsh
ls -ld /Volumes/DatabentoVault
ls -ld /Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests
```

Mount the DatabentoVault disk or rerun with the correct `--ext` path where supported.

### Multiple logs for the same week

The analysis scripts currently sort matching `wXX_*.log` files and select the first match. If a week has multiple reruns, verify the selected log path in the generated report. If needed, temporarily move old logs aside or patch the script constants/selection logic before generating the final report.

### Missing today-only artifacts

For `scripts/summarize_core5_today_only.py` and `scripts/calculate_core5_today_pnl.py`, missing files often mean the hardcoded run stamp no longer matches the current backtest output. Update the `WEEKS` constants with the desired stamp for each week.

### Lifecycle/trades PnL mismatch

`scripts/calculate_core5_today_pnl.py` emits a cross-check comparing lifecycle `TradePnL` sums to `trades.csv` `TradePnL` sums. If the check fails:

1. inspect the report’s week-level mismatch row,
2. open the affected symbol’s lifecycle and trades CSVs,
3. confirm no partial or duplicate artifact from an old run stamp was selected,
4. review the corresponding `wXX_*.log` for interrupted exports.

### Large generated artifacts in git status

Backtest folders, research run folders, tarballs, and timestamped reports can be large. Treat them as generated evidence, not source. Commit only source scripts and intentionally curated markdown reports; leave bulk data on `/Volumes/DatabentoVault` or under ignored runtime storage.

## Maintenance checklist for a new weekly run

1. Confirm the external output root and week folder names.
2. Record the run stamp for each week and symbol set.
3. Update hardcoded `WEEKS` constants in the run-specific scripts if reusing them.
4. Generate the today-only summary.
5. Generate the PnL report and confirm lifecycle/trades cross-checks.
6. Generate the broader artifact inventory and deep weekly-output report.
7. Preserve only the concise markdown/JSON reports needed for handoff; avoid committing bulk CSV/log output.
