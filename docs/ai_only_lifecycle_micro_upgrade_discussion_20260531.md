# AI-Only Lifecycle and Micro-Execution Upgrade Discussion

Date: 2026-05-31

This note captures the current upgrade discussion so it can be reviewed and improved iteratively. It complements `docs/training_and_execution_upgrade_plan.md` and focuses on the current implementation path in `train_lifecycle_micro_models.py`.

## Guiding Decision

Trading decisions should be made by AI models, not by standalone technical-indicator gates. Deterministic logic may still exist for safety, data quality, exchange/session constraints, hard risk controls, and order hygiene, but it should not become a separate directional trading thesis that overrides the model stack.

The upgraded stack should therefore treat indicators and market-state calculations as model features. The model decides whether the combined feature state supports entry, hold, exit, or micro-risk action.

---

## 1. AI-Only Decision Flow

Target flow:

```text
30s context/setup model
  -> arms a long or short opportunity when model score and route threshold pass

5s micro-entry model
  -> decides whether the armed setup has a good execution moment

position-aware lifecycle exit model
  -> decides whether the trade should exit at lifecycle cadence

5s micro-exit guard model
  -> detects fast adverse deterioration between lifecycle checks
```

Important boundaries:

- The 30-second model remains the setup/context layer.
- The 5-second micro-entry model is the execution-timing layer after a setup is armed.
- The lifecycle exit model is the primary AI exit decision once in position.
- The micro-exit guard model is an early-warning AI layer between lifecycle checks.
- Non-model logic should be limited to operational safety such as no stale data, no invalid quotes, position limits, hard stops, EOD flattening, and broker/session constraints.

## 2. Thirty-Second Setup and Arming

The 30-second entry models should not necessarily fire orders immediately in the upgraded route. Instead, a high-quality 30-second setup should arm the side:

```text
30s bar closes
  -> compute 30s features, regime/context, and entry model score
  -> if model score passes calibrated route threshold, create armed setup state
  -> do not submit the order until micro-entry model confirms timing
```

The armed state should preserve enough context for downstream models:

- symbol,
- side,
- setup timestamp,
- setup close/high/low,
- setup score,
- threshold margin,
- regime/context features,
- selected route/cohort metadata,
- expiry/TTL.

The current training script mirrors this idea in `build_micro_rows(...)`: it finds 30-second bars with positive entry labels and expands them into candidate 5-second rows within `MICRO_ARM_TTL_SECONDS`.

## 3. Lifecycle Exit Label Discussion

Lifecycle labels are position-aware. They are not generic “market may go down/up” labels. They are built only after a simulated entry event and should include the active trade state.

Current implementation anchor: `build_lifecycle_rows(...)` in `train_lifecycle_micro_models.py`.

The lifecycle row builder currently:

1. Finds candidate long/short setup entries from `Label_Long_Entry` and `Label_Short_Entry`.
2. Simulates an entry from the setup bar close in the current implementation.
3. Emits one row per later 30-second bar while the simulated trade is within the lifecycle horizon.
4. Adds 30-second context features and regime features.
5. Adds position features such as:
   - `f_pos_side`,
   - `f_bars_since_entry`,
   - `f_unrealized_pnl_r`,
   - `f_mfe_r`,
   - `f_mae_r`,
   - `f_target_remaining_r`,
   - `f_stop_remaining_r`,
   - entry score/side proxy features.
6. Computes the hold outcome over the remaining lifecycle horizon using event-ordered target/stop path logic.
7. Labels exit as positive when exiting now is better than holding by at least `EXIT_DECISION_MARGIN_R`.

Conceptually:

```text
exit_now_r = current unrealized R
hold_r = event-ordered forward outcome over lifecycle lookahead

Label_Long_ExitLifecycle = 1 if exit_now_r >= hold_r + EXIT_DECISION_MARGIN_R
Label_Short_ExitLifecycle = 1 if exit_now_r >= hold_r + EXIT_DECISION_MARGIN_R
```

Discussion items for improvement:

- Use the same realistic fill assumptions as the final entry route, including spread/slippage costs.
- Carry actual 30-second setup model probability instead of only `f_entry_score_proxy` when available.
- Include model route metadata and threshold margin.
- Add realized-R impact reporting, not only classification precision/recall.
- Preserve strict no-lookahead behavior in every label and feature join.

## 4. Micro-Entry Label Discussion

Micro-entry labels train the model that decides whether an already armed setup has a good 5-second execution moment.

Current implementation anchor: the first half of `build_micro_rows(...)` in `train_lifecycle_micro_models.py`.

The micro-entry row builder currently:

1. Starts from a positive 30-second setup event.
2. Finds 5-second candidate bars after the setup timestamp and before the arm TTL expires.
3. Emits one candidate row per 5-second bar while the setup is armed.
4. Combines:
   - current 5-second micro features,
   - 30-second context/setup features,
   - setup metadata such as `f_setup_score_proxy` and `f_seconds_since_arm`.
5. Labels the candidate as positive when entry at that 5-second close reaches the target before the stop over `MICRO_FUTURE_WINDOW_5S`.

Conceptually:

```text
micro_fill = current 5s decision price
future_outcome_r = event-ordered target/stop outcome over micro lookahead

Label_Long_MicroEntry = 1 if future_outcome_r >= 1.0
Label_Short_MicroEntry = 1 if future_outcome_r >= 1.0
```

Discussion items for improvement:

- Replace proxy setup score with actual 30-second model score/threshold margin when persisted.
- Add route/cohort metadata so one global model can learn regime-aware behavior without requiring sparse per-regime models too early.
- Use conservative fill assumptions for long/short side execution.
- Keep 5-second as the first micro-AI cadence; keep 1-second state available for features, quality checks, and future research.
- Report armed-to-filled conversion, expired setups, and skipped opportunities separately from raw precision.

## 5. Micro-Exit Guard Label Discussion

The micro-exit guard model is not a replacement for the lifecycle exit model. It is a faster AI guard for local deterioration between lifecycle checks.

Current implementation anchor: the second half of `build_micro_rows(...)` in `train_lifecycle_micro_models.py`.

The micro-exit row builder currently:

1. Simulates a micro entry at the first candidate 5-second bar after an armed setup.
2. Emits rows for subsequent 5-second bars while the simulated trade remains inside the micro future window.
3. Adds current 5-second features, 30-second context features, and position-state features.
4. Computes whether exiting now is better than holding through the micro-exit lookahead by at least `MICRO_EXIT_MARGIN_R`.

Conceptually:

```text
exit_now_r = current unrealized R
hold_until_next_lifecycle_r = event-ordered outcome over MICRO_EXIT_LOOKAHEAD_5S

Label_Long_MicroExitGuard = 1 if exit_now_r >= hold_until_next_lifecycle_r + MICRO_EXIT_MARGIN_R
Label_Short_MicroExitGuard = 1 if exit_now_r >= hold_until_next_lifecycle_r + MICRO_EXIT_MARGIN_R
```

Discussion items for improvement:

- Align micro-exit rows with the same entry fill and route used by micro-entry labels.
- Add explicit reason tags for post-training diagnostics, for example saved-loss, cut-winner, stale-trade, or severe adverse move.
- Measure whether the guard improves realized R after costs rather than only label metrics.
- Keep hard stops, profit targets, EOD flatten, and circuit breakers outside the model as risk controls, while keeping discretionary exit timing model-driven.

## 6. Streaming Staging and Model Export Discussion

The current script supports streamed staging so lifecycle/micro datasets do not all need to stay in memory.

Current implementation anchors:

- `build_streamed_staging_datasets(...)`,
- `append_frame_csv(...)`,
- `load_staged_training_frame(...)`,
- `train_binary_model(...)`,
- `write_scorecards(...)`.

Staged outputs currently include:

```text
long_lifecycle_rows.csv
short_lifecycle_rows.csv
long_micro_entry_rows.csv
short_micro_entry_rows.csv
long_micro_exit_rows.csv
short_micro_exit_rows.csv
```

Model exports currently target:

```text
long_exit_lifecycle.onnx
short_exit_lifecycle.onnx
long_micro_entry_5s.onnx
short_micro_entry_5s.onnx
long_micro_exit_guard_5s.onnx
short_micro_exit_guard_5s.onnx
```

Scorecard/route artifacts currently include:

```text
lifecycle_micro_scorecard.csv
lifecycle_micro_route_manifest.json
```

Discussion items for improvement:

- Persist label distribution by symbol, side, model type, hour, regime, and date.
- Persist selected threshold and threshold-search curve per model.
- Store feature columns and schema hashes so Java route validation can fail fast on mismatches.
- Keep missing required upgraded models as route invalidation, not silent fallback.
- Keep staged datasets versioned by run tag so labels can be audited later.

## 7. Validation, Backtesting, and Open Decisions

The upgrade should be judged by trade lifecycle quality, not only classifier metrics.

Required validation views:

- label prevalence by model and side,
- positive/negative examples by hour and regime,
- walk-forward precision/recall and predicted-positive rate,
- threshold selected on calibration folds,
- realized R impact in backtest,
- setup-to-fill latency,
- armed setup conversion rate,
- expired/disarmed setup counts,
- lifecycle exit saved-loss versus cut-winner-too-early counts,
- micro-exit guard benefit/harm attribution,
- slippage/chase distance from 30-second setup close to actual fill.

Open decisions to discuss next:

1. Whether lifecycle rows should use setup close, next open, or modeled bid/ask fill as the entry price.
2. How to persist actual 30-second model probabilities into the lifecycle/micro training frame.
3. Which route metadata should become first-class model features.
4. Whether micro-exit labels should include partial-reduce labels or only full-exit labels in the first version.
5. How strict minimum trade-count constraints should be before promoting thresholds.
6. Whether 1-second features improve 5-second micro models enough to justify additional schema complexity.
7. How to compare direct 30-second entry, armed 5-second entry, lifecycle exit, and micro-exit guard in one scorecard.

## Working Command Template

For a smoke training run after the 30-second and 5-second training CSVs exist:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python train_lifecycle_micro_models.py \
  --input-30s-csv training_data/databento_30s_20260523 \
  --input-5s-csv training_data/databento_5s_20260523 \
  --max-entry-events 200 \
  --max-entry-events-per-symbol-side 25 \
  --max-staged-rows-per-symbol-per-model 5000 \
  --max-train-rows-per-model 25000 \
  --no-onnx
```

Use a full training run only after the label/staging scorecards look sane.

