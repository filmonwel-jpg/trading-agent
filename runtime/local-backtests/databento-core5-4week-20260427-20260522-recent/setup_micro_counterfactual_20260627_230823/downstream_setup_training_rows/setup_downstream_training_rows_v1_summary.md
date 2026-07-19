# Downstream Setup Training Rows

Generated: `2026-06-28T12:27:22.862594Z`

- Schema: `setup_downstream_training_rows_v1`
- Input 30s CSV: `runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv`
- Labels CSV: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/setup_downstream_confirmable_labels_v1.csv`
- Join tolerance seconds: `31`

## Join summary

- Labels rows: `4470`
- Input 30s rows: `830700`
- Joined rows: `4470`
- Matched rows: `4470`
- Unmatched rows: `0`
- Match rate: `1.000000`
- Join abs epoch delta seconds: `{'min': 1.0, 'p50': 1.0, 'p90': 1.0, 'max': 1.0}`

## Positives by symbol/side

| symbol.side | rows | matched | long positive | short positive |
|---|---:|---:|---:|---:|
| NVDA.long | 843 | 843 | 82 | 0 |
| NVDA.short | 866 | 866 | 0 | 56 |
| QQQ.long | 448 | 448 | 0 | 0 |
| QQQ.short | 106 | 106 | 0 | 0 |
| SPY.long | 130 | 130 | 0 | 0 |
| SPY.short | 97 | 97 | 0 | 0 |
| TQQQ.long | 771 | 771 | 142 | 0 |
| TQQQ.short | 201 | 201 | 0 | 36 |
| TSLA.long | 666 | 666 | 39 | 0 |
| TSLA.short | 342 | 342 | 0 | 16 |

## Outputs

- `training_rows_csv`: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1.csv`
- `manifest_json`: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1_manifest.json`
- `summary_md`: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_training_rows/setup_downstream_training_rows_v1_summary.md`

Promotion status remains **NO-GO** until retrained models pass the replay/promotion gates.
