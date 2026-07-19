# Downstream Setup Filter Research Bundle

Generated: `2026-06-29T05:18:42.804825Z`

- Schema: `downstream_setup_filter_research_v1`
- Training rows: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv`
- Model family: `catboost`
- Feature preset: `catboost_cost_aware_core`
- Excluded optional feature groups: `databento_silver, enriched_meta`

| side | model | status | rows | positives | features | holdout AP | selected threshold | precision | recall | predicted + | selected mean R | selected p50 R |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| long | CatBoostClassifier | trained | 2858 | 263 | 59 | 0.2816 | 0.7500 | 0.4211 | 0.1067 | 19 | 0.4560 | 1.2328 |
| short | CatBoostClassifier | trained | 1612 | 108 | 59 | 0.3078 | 0.7500 | 0.4286 | 0.0882 | 7 | 0.6587 | 1.2468 |

Warnings:

- Research-only arm-quality filter trained on replay arms, not all candidate setup bars.
- Default CatBoost cost-aware core preset intentionally excludes optional enriched meta and Databento silver aggregate columns so those features are not required by training or Java replay/live scoring.
- Feature importances and holdout metrics are directional; promotion requires integration, replay, calibration, dominance, parity, and paper/shadow gates.
- Future-outcome label fields are explicitly excluded from model features to avoid leakage.

Promotion status remains **NO-GO**. These filters must be wired into a controlled replay experiment and pass promotion gates before use.
