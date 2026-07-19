# Downstream Filter Replay Policy Evaluation

Generated: `2026-06-29T05:19:06.888311Z`

- Schema: `downstream_filter_replay_policy_eval_v1`
- Training rows: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv`
- Counterfactual decisions: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_micro_counterfactual_decisions.csv`
- Filter bundle dir: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_catboost_core_20260628`

## Best policy by expected net R mean

- Filter threshold: `selected`
- Micro threshold: `0.3`
- Confirms: `285`
- Positive trades: `228`
- Expected net R mean: `0.6616619758716764`
- Expected net R sum: `187.9120011475561`
- Symbol/side count: `6`
- Max day dominance: `0.12631578947368421` on `2026-04-28`

## Policy grid

| filter threshold | micro threshold | filter pass arms | confirms | positive | mean R | p50 R | sum R | symbols/sides | max day dominance |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| selected | 0.2000 | 296 | 295 | 226 | 0.5956 | 1.1951 | 174.4991 | 6 | 0.1220 |
| selected | 0.2500 | 296 | 289 | 225 | 0.6204 | 1.2068 | 178.0447 | 6 | 0.1246 |
| selected | 0.3000 | 296 | 285 | 228 | 0.6617 | 1.2121 | 187.9120 | 6 | 0.1263 |
| selected | 0.3500 | 296 | 176 | 133 | 0.5762 | 1.2183 | 100.8264 | 6 | 0.1307 |
| selected | 0.4000 | 296 | 89 | 60 | 0.4141 | 1.2113 | 36.8549 | 6 | 0.1685 |
| selected | 0.4500 | 296 | 46 | 30 | 0.3846 | 1.2144 | 17.6926 | 5 | 0.1522 |
| selected | 0.5000 | 296 | 15 | 8 | 0.1183 | 1.0613 | 1.7741 | 3 | 0.2667 |
| selected | 0.5200 | 296 | 11 | 6 | 0.1701 | 1.2192 | 1.8710 | 2 | 0.2727 |
| selected | 0.5500 | 296 | 5 | 2 | -0.1731 | -1.1054 | -0.8653 | 1 | 0.4000 |
| selected | 0.5700 | 296 | 2 | 1 | 0.0506 | 0.0506 | 0.1012 | 1 | 1.0000 |
| selected | 0.5800 | 296 | 1 | 0 | -1.1405 | -1.1405 | -1.1405 | 1 | 1.0000 |

Promotion status remains **NO-GO**. This is an offline counterfactual replay-policy screen, not a live/replay parity or paper/shadow drift gate.
