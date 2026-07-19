# Downstream Setup Filter ONNX Research Export

Generated: `2026-06-29T05:19:34.378890Z`

- Schema: `downstream_setup_filter_onnx_research_v1`
- Source bundle dir: `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_catboost_core_20260628`
- Target opset: `15`
- Promotion status: **NO-GO**

## Routes

| route | side | ONNX | features | threshold | model sha256 | validation |
|---|---|---|---:|---:|---|---|
| longDownstreamSetupFilterAi | long | `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/long_downstream_setup_filter.onnx` | 59 | 0.7500 | `214f590f598e` | skipped_missing_onnx_runtime |
| shortDownstreamSetupFilterAi | short | `runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/short_downstream_setup_filter.onnx` | 59 | 0.7500 | `a1bed2bc18c0` | skipped_missing_onnx_runtime |

## Embedded offline policy screen

- Filter threshold label: `selected`
- Micro threshold: `0.3`
- Confirms: `285`
- Positive outcomes: `228`
- Expected net R mean: `0.6616619758716764`
- Expected net R sum: `187.9120011475561`

## Warnings

- Research-only export of filters trained on replay-observed setup arms, not all possible runtime setup candidates.
- ONNX conversion preserves model scoring, but Java/replay feature-vector parity is not proven by this artifact alone.
- Runtime use must rebuild the exact feature order from the per-side feature schema and enforce manifest/hash checks.
- Promotion remains NO-GO until controlled Java replay, lifecycle summaries, calibration, dominance, parity, and paper/shadow gates pass.
