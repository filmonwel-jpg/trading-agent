# CatBoost-only candidate path

Decision date: 2026-06-22

## Decision

Continue with CatBoost model-family improvements only. Do **not** add or promote an MBP-1/EQUS liquidity feature path unless we later commit to the full data, backtest, and live-feature parity work.

## Active feature contract

The safe CatBoost-only candidate uses the existing Java-compatible base schema:

- main entry model feature count: `30`
- regime classifier feature count: `24`
- `USE_DATABENTO_SILVER_FEATURES=0`
- `USE_NEWS_BAR_FEATURES=0`
- `USE_META_PRODUCER_FEATURES=0`
- `USE_REGIME_PROB_FEATURES=0`
- `UPDATE_CANONICAL_MODEL_ALIASES=0`

This intentionally avoids MBP-1 / `EqMbp1*` columns and avoids any new live/backtest feature routing.

## Try it

Preflight only:

```bash
scripts/run_catboost_base_candidate_export.sh
```

Train/export an isolated candidate bundle:

```bash
DRY_RUN=0 scripts/run_catboost_base_candidate_export.sh
```

Optional scorecard-only run without ONNX export:

```bash
DRY_RUN=0 NO_ONNX=1 scripts/run_catboost_base_candidate_export.sh
```

## Promotion guardrails

- The runner writes to `runtime/research_runs/catboost_base_30f_onnx_candidate_<UTC>` by default.
- It does not update canonical model aliases.
- Treat every output as `production_promotion=NO-GO` until historical simulation/backtest, paper/shadow checks, and threshold review pass.
- If MBP-1 is revisited later, add it as a separate project with explicit downloader, normalizer, feature parity, backtest parity, and live parity gates.
