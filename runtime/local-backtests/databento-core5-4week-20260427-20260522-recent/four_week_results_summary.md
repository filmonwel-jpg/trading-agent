# Four-week Databento latest-model replay summary

Run log: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/run/databento-core5-4week-recent-20260627_230823.log`
Recorded events: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz`
Setup dir: `/Users/FXG06FA/trading-agent-main/runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854`
Lifecycle/micro dir: `/Users/FXG06FA/trading-agent-main/runtime/research_runs/lifecycle_micro_external_oof_20260624_120527/model_exports`

## Slice validation

| Metric | Value |
| --- | --- |
| output_events | 4936385 |
| equity_bar | 932320 |
| option_bar | 4003923 |
| previous_close | 100 |
| day_count | 20 |
| missing_previous_close | {} |
| malformed | 0 |

## Per-symbol stream/data and decision output

| Symbol | Equity bars | Option bars | Skipped | Low quality | Low-quality % | Closed trades | Order rows | PnL | Arms | Confirmations | Conversion |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TSLA | 193384 | 909303 | 0 | 3 | 0.0016% | 0 | 0 | 0.00 | 1008 | 0 | 0.000000 |
| TQQQ | 110244 | 355747 | 0 | 2456 | 2.2278% | 0 | 0 | 0.00 | 972 | 0 | 0.000000 |
| NVDA | 287501 | 878435 | 0 | 90 | 0.0313% | 0 | 0 | 0.00 | 1709 | 0 | 0.000000 |
| SPY | 198893 | 934457 | 0 | 154 | 0.0774% | 0 | 0 | 0.00 | 227 | 0 | 0.000000 |
| QQQ | 142298 | 925981 | 0 | 107 | 0.0752% | 0 | 0 | 0.00 | 554 | 0 | 0.000000 |

## Closed trades

| Symbol | Side | Entry | Exit | Qty | EntryPx | ExitPx | PnL | R | Exit | Prob | Threshold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| <none> |  |  |  |  |  |  |  |  |  |  |  |

## Log validation

```json
{
  "backtest_rc_0": 1,
  "feature_count_supported_pass": 85,
  "lifecycle_route_manifest_validated": 5,
  "setup_manifest_loaded": 5,
  "setup_thresholds_file_logged": 5,
  "strategy_threshold_config_logged": 5,
  "wrapper_completed_5_failed_0": 1
}
```

Previous-close event counts from Java log:

| Symbol | Previous-close PASS count |
| --- | --- |
| TSLA | 20 |
| TQQQ | 20 |
| NVDA | 20 |
| SPY | 20 |
| QQQ | 20 |

## Day dominance

```json
{
  "max_abs_day_pnl_fraction": 0.0,
  "max_trade_day_fraction": 0.0,
  "pnl_by_day": {},
  "pnl_by_symbol": {},
  "total_closed_trades": 0,
  "total_pnl": 0,
  "trades_by_day": {}
}
```
