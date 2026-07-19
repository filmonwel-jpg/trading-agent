# Setup → Micro Counterfactual Profitability Report

Generated: `2026-06-28T12:23:06.071457Z`

## Inputs

- Replay log: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/run/databento-core5-4week-recent-20260627_230823.log`
- Recorded events: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz`

## Assumptions

- Label schema: `setup_downstream_confirmable_labels_v1`
- Label minimum micro probability: `0.3`
- Minimum expected net R for a positive label: `0.0`
- Horizon seconds: `600`
- Profit/risk pct: `0.0035` / `0.0025`
- Entry/exit slippage bps: `2.0` / `2.0`

## Parse totals

- Arms: `4470`
- Micro evaluations: `45919`
- Recorded equity bars: `932320`

## Downstream label summary by symbol/side

| symbol.side | arms | candidate arms | positive arms | candidate rate | positive rate | max micro prob | mean best expected R |
|---|---:|---:|---:|---:|---:|---:|---:|
| NVDA.long | 843 | 138 | 82 | 0.1637 | 0.0973 | 0.5175 | 0.2232 |
| NVDA.short | 866 | 87 | 56 | 0.1005 | 0.0647 | 0.4544 | 0.3388 |
| QQQ.long | 448 | 0 | 0 | 0.0000 | 0.0000 | 0.1991 |  |
| QQQ.short | 106 | 0 | 0 | 0.0000 | 0.0000 | 0.2270 |  |
| SPY.long | 130 | 0 | 0 | 0.0000 | 0.0000 | 0.2897 |  |
| SPY.short | 97 | 0 | 0 | 0.0000 | 0.0000 | 0.2734 |  |
| TQQQ.long | 771 | 306 | 142 | 0.3969 | 0.1842 | 0.5961 | -0.0142 |
| TQQQ.short | 201 | 65 | 36 | 0.3234 | 0.1791 | 0.5718 | 0.2005 |
| TSLA.long | 666 | 81 | 39 | 0.1216 | 0.0586 | 0.4977 | -0.1547 |
| TSLA.short | 342 | 35 | 16 | 0.1023 | 0.0468 | 0.4417 | -0.1739 |

## Counterfactual first-cross threshold grid

| threshold | confirms | confirm rate | positive | positive / confirm | mean expected R | p50 expected R | target | stop |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.2000 | 1843 | 0.4123 | 866 | 0.4699 | -0.0684 | -0.1778 | 686 | 821 |
| 0.2500 | 1159 | 0.2593 | 524 | 0.4521 | -0.1163 | -1.0792 | 439 | 573 |
| 0.3000 | 712 | 0.1593 | 307 | 0.4312 | -0.1552 | -1.1047 | 276 | 379 |
| 0.3500 | 411 | 0.0919 | 174 | 0.4234 | -0.1785 | -1.1050 | 164 | 226 |
| 0.4000 | 214 | 0.0479 | 83 | 0.3879 | -0.2522 | -1.1051 | 78 | 127 |
| 0.4500 | 105 | 0.0235 | 39 | 0.3714 | -0.2505 | -1.1051 | 38 | 64 |
| 0.5000 | 38 | 0.0085 | 11 | 0.2895 | -0.4363 | -1.1055 | 11 | 26 |
| 0.5200 | 27 | 0.0060 | 9 | 0.3333 | -0.3184 | -1.1050 | 9 | 17 |
| 0.5500 | 14 | 0.0031 | 2 | 0.1429 | -0.7589 | -1.1056 | 2 | 11 |
| 0.5700 | 5 | 0.0011 | 1 | 0.2000 | -0.6537 | -1.1069 | 1 | 4 |
| 0.5800 | 3 | 0.0007 | 0 | 0.0000 | -1.1344 | -1.1405 | 0 | 3 |
| 0.6000 | 0 | 0.0000 | 0 | 0.0000 |  |  | 0 | 0 |
| 0.6200 | 0 | 0.0000 | 0 | 0.0000 |  |  | 0 | 0 |
| 0.6400 | 0 | 0.0000 | 0 | 0.0000 |  |  | 0 | 0 |

## Output artifacts

- `arms_csv`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_arms.csv`
- `micro_evals_csv`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_micro_evals.csv`
- `decisions_csv`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_decisions.csv`
- `labels_csv`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_downstream_confirmable_labels_v1.csv`
- `summary_json`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_summary.json`
- `summary_md`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_summary.md`
- `labels_manifest_json`: `/Users/FXG06FA/trading-agent-main/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_downstream_confirmable_labels_v1_manifest.json`

Promotion status: **NO-GO** until a retrained bundle passes replay volume, expected net R, calibration, dominance, parity, and paper/shadow drift gates.
