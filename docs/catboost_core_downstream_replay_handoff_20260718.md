# CatBoost-core downstream replay handoff — 2026-07-18

This is the handoff note for continuing the downstream setup-filter work on the replay/backtest computer.

## Current status

Promotion status remains **RESEARCH-ONLY / NO-GO**.

Reasons:

- The clean post-fix four-week replay has `0` closed trades.
- Promotion gates still fail.
- There is no paper/shadow drift evidence.
- There is no live/replay decision parity evidence.
- Lifecycle/micro runtime calibration is still raw-probability only.
- The CatBoost-core downstream setup-filter Java replay has not yet been proven.

Do **not** promote this branch to live capital from the current evidence.

## What changed most recently

The earlier downstream setup filter trained on enriched research/silver features looked good offline, but Java replay without the setup-arm sidecar did not match it. The sidecar exact no-trade replay proved the Java gate order can match offline when exact research features are supplied, but that is not live-ready.

The preferred next branch is now the CatBoost-core downstream setup-quality filter:

- `model_family=catboost`
- `feature_preset=catboost_cost_aware_core`
- `59` features per side
- excludes optional enriched meta and Databento silver aggregate groups:
  - `tsm_*`, `regime_*`, `news_*`, `seq_*`, `setup_*`
  - `EqMbp1*`
  - `OpraTcbbo*`

Relevant local artifact directory:

```text
runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_setup_filter_onnx_catboost_core_20260628/
```

The minimal ONNX route artifacts are intentionally small enough to carry with this handoff:

- `downstream_setup_filter_route_manifest.json`
- `downstream_setup_filter_onnx_manifest.json`
- `downstream_setup_filter_onnx_summary.md`
- `long_downstream_setup_filter.onnx`
- `short_downstream_setup_filter.onnx`
- `long_downstream_setup_filter_feature_schema.json`
- `short_downstream_setup_filter_feature_schema.json`

The recorded-events file is not committed here. It must exist on the replay/backtest computer at the path used below, or be copied/regenerated before running the replay.

## Offline CatBoost-core baseline to match

From the offline policy screen at:

```text
runtime/local-backtests/databento-core5-4week-20260427-20260522-recent/setup_micro_counterfactual_20260627_230823/downstream_filter_replay_policy_eval_catboost_core_20260628/downstream_filter_replay_policy_summary.md
```

Expected baseline for selected CatBoost-core setup-filter thresholds and micro threshold `0.30`:

| Metric | Expected value |
|---|---:|
| Filter-pass arms | `296` |
| Micro confirmations | `285` |
| Positive outcomes | `228` |
| Expected net R mean | `+0.6617` |
| Expected net R sum | `+187.91` |
| Symbol/side cohorts | `6` |
| Max day dominance | `12.63%` |

The first Java replay should try to match the **filter-pass and confirmation counts** before allowing orders.

## Priority 1 — no live promotion

Keep this branch disabled for live/paper promotion until all of these are available together:

- enough closed trades in controlled replay,
- positive realized and expected net R after costs,
- acceptable day/symbol/side dominance,
- runtime ONNX probability parity,
- recorded-event/live decision parity,
- post-hoc calibration or explicit raw-probability gate acceptance,
- paper/shadow drift evidence.

## Priority 2 — run CatBoost-core controlled Java replay, no sidecar, no-trade first

Goal: verify whether the new `59`-feature CatBoost-core downstream setup filter works in Java replay without the enriched setup-arm sidecar.

Run on the replay/backtest computer:

```zsh
cd /Users/FXG06FA/trading-agent-main

BASE=runtime/local-backtests/databento-core5-4week-20260427-20260522-recent
COUNTERFACTUAL="$BASE/setup_micro_counterfactual_20260627_230823"
EVENTS="$BASE/databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz"
MANIFEST="$COUNTERFACTUAL/downstream_setup_filter_onnx_catboost_core_20260628/downstream_setup_filter_route_manifest.json"
OUT="$BASE/controlled_java_replay_downstream_setup_filter_catboost_core_20260628_notrade"

mkdir -p "$OUT"

PYTHON_BIN=python3 SKIP_BUILD=true scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbols NVDA,QQQ,SPY,TQQQ,TSLA \
  --source ndjson \
  --recorded-events "$EVENTS" \
  --output-dir "$OUT" \
  --downstream-setup-filter-manifest "$MANIFEST" \
  --micro-long-entry-threshold 0.30 \
  --micro-short-entry-threshold 0.30 \
  --micro-entry-research-no-trade \
  --timeout-seconds 0 \
  > "$OUT/controlled_java_replay.log" 2>&1
```

Count checks:

```zsh
grep -c 'SETUP_FILTER_PASSES=PASS' "$OUT/controlled_java_replay.log"
grep -c 'SETUP_FILTER_PASSES=FAIL' "$OUT/controlled_java_replay.log"
grep -c 'MICRO_ENTRY_CONFIRMS=PASS' "$OUT/controlled_java_replay.log"
grep -c 'MICRO_ENTRY_CONFIRMS=FAIL' "$OUT/controlled_java_replay.log"
```

Interpretation:

- If `SETUP_FILTER_PASSES=PASS` is roughly `296` and `MICRO_ENTRY_CONFIRMS=PASS` is roughly `285`, proceed to the trade-enabled controlled replay.
- If the counts are far away, stop and debug Java feature parity / ONNX scoring before placing any orders.
- In particular, investigate:
  - Java ONNX Runtime probability extraction for CatBoost native ONNX,
  - feature-vector order and count versus `*_feature_schema.json`,
  - missing feature zero-fill behavior,
  - setup arm epoch/side/category one-hot alignment,
  - whether the correct manifest is being loaded.

## Priority 3 — install/use Python `onnxruntime` and rerun CatBoost-core ONNX validation

The CatBoost-core export summary currently says `skipped_missing_onnx_runtime` for Python-side runtime validation. Install dependencies on the replay/backtest computer:

```zsh
cd /Users/FXG06FA/trading-agent-main
python3 -m pip install -r requirements.txt
```

Then rerun export validation with `--require-onnxruntime-validation` using the same filter bundle and training rows if available. This matters because CatBoost native ONNX may have different runtime behavior from the sklearn/reference paths already tested.

## Priority 4 — only if no-trade parity matches, run trade-enabled controlled replay

After the no-trade run matches count parity, rerun the same command without:

```text
--micro-entry-research-no-trade
```

Collect and summarize:

- closed trades,
- realized R,
- PnL,
- MFE/MAE,
- day dominance,
- symbol dominance,
- long/short balance,
- lifecycle exit behavior,
- hard-risk exits,
- EOD exits.

The promotion gate currently fails because closed trades are `0`; the trade-enabled replay must produce enough distributed trade evidence before any paper/live consideration.

## Priority 5 — retune/retrain micro-entry on the corrected setup population

Do **not** simply lower micro thresholds to manufacture fills.

The safer path is:

1. Use the downstream-confirmable labels.
2. Use the corrected setup-arm distribution with real setup probabilities, thresholds, and margins.
3. Retune or retrain `longMicroEntryAi` and `shortMicroEntryAi`.
4. Select thresholds by:
   - expected net R after costs,
   - minimum trade count,
   - calibration,
   - day dominance,
   - symbol/side spread,
   - replay parity.

## Commit/artifact hygiene

This handoff commit should include source/docs/tests and the small CatBoost-core ONNX route artifacts needed for the immediate replay. It should **not** include huge local research caches.

Avoid committing large ignored runtime directories such as:

- broader CatBoost/RF local-cache runs,
- lifecycle micro CatBoost-only full runs,
- large recorded-event files,
- full local backtest output directories unless intentionally promoted to artifact storage.

If the next computer is missing the recorded-events file, copy/regenerate it separately rather than storing it in git.

