# Training Target, Position-Aware Exit, and Multi-Cadence Execution Upgrade Plan

Date: 2026-05-23

This document turns the model/strategy discussion into an executable implementation plan for the current Databento/IBKR trading stack.

Related follow-up discussion: `docs/ai_only_lifecycle_micro_upgrade_discussion_20260531.md` captures the May 31 AI-only lifecycle/micro training and execution discussion, including the seven current review topics for labels, staging, exports, validation, and open decisions.

## Current State Summary

The current architecture has three strong pieces already in place:

1. `train_30s_models.py` generates 30-second features and exports ONNX models.
2. `PingPongStrategy.java` aggregates incoming source bars into 30-second AI buckets, computes Java-side features, routes to base/open30/regime models, and executes orders.
3. Legacy `HistoricalBacktester.java` can still replay historical CSVs for backward-compatible comparisons, but the upgraded validation path is `DatabentoHistoricalStreamingBacktester.java`, which consumes Databento historical API data through the same live-shaped NDJSON event contract as `DatabentoLiveGateway`.

The main gaps are:

1. Entry labels need to remain positive-expectancy and realistic relative to fills/costs.
2. Exit decisions need position context: entry price, original entry thesis, unrealized PnL, MFE/MAE, and bars since entry.
3. 30-second bars should remain the setup/context layer, but actual order triggering should be faster, using 5-second or 1-second micro-confirmation.
4. The backtester must validate the same two-layer behavior used live.

## Finalized Architecture Decisions

These decisions supersede earlier fallback-oriented rollout notes in this document.

1. `longExitLifecycleAi` and `shortExitLifecycleAi` replace the old standalone exit model concept as the primary exit decision system.
2. Lifecycle exit models are not regime-insensitive. The first implementation trains one long lifecycle model and one short lifecycle model across all regimes, but includes regime, entry reason, entry score, and live trade-state features as model inputs.
3. Runtime may use regime-specific thresholds for lifecycle exits, but should not train separate per-regime lifecycle models until each regime has enough completed trades to avoid overfit.
4. 30-second entry models define setup eligibility and arm the strategy. 5-second or 1-second bars decide execution timing after the 30-second setup is armed.
5. Smaller bars are always built, stored, and used to update state. Micro-entry inference runs only while flat and armed. Micro-exit/risk inference runs only while in a position.
6. First-version micro models should be global but regime-aware: `longMicroEntryAi`, `shortMicroEntryAi`, `longMicroExitGuardAi`, and `shortMicroExitGuardAi`. They should include 30-second regime, setup score, entry reason, spread, micro momentum, pullback depth, and volatility features.
7. Use 5-second bars as the primary micro-AI cadence first. Continue building 1-second bars for spread/slippage/state and OPRA option-flow context, but do not train/run separate 1-second micro AI models in the first implementation unless backtests show 5-second confirmation is too slow. Running both 5-second and 1-second AI inference from day one is likely overkill and can add noise, latency, feature-schema complexity, and harder model attribution.
8. OPRA 1-second option features should be available at both cadences: use native 1-second option features for deterministic guardrails/state, and roll them into no-lookahead 5-second option-flow features for `longMicroEntryAi`, `shortMicroEntryAi`, `longMicroExitGuardAi`, and `shortMicroExitGuardAi`.
9. Old non-regime fallback models are removed from the trading route. If a required regime-routed, lifecycle, or micro model is missing, the symbol/model route is invalid and trading is blocked for that route. The system should log the missing model instead of falling back.
10. New Databento sources for this upgrade are:
   - `EQUS-20260523-6J9KE98BJ9`
   - `OPRA-20260523-MSV68VKVKD`
11. New Databento outputs should be versioned, not written over the old 20260419 datasets.
12. Backtesting for the upgraded route must not depend on combined CSV replay. It should use Databento historical API streaming through `scripts/databento_historical_streamer.py --source api`, emit `equity_bar` and `option_bar` NDJSON payloads, and feed them into `DatabentoHistoricalStreamingBacktester.java` through `DatabentoLiveGateway`.

### Implementation Status as of 2026-05-24

- Multi-cadence Databento dataset building is implemented for 1-second, 5-second, and 30-second outputs from the existing combined TBBO + OPRA 1-second frame.
- Realistic entry fill/cost label controls are implemented in `train_30s_models.py`.
- Lifecycle and 5-second micro model training is implemented in `train_lifecycle_micro_models.py`.
- `PingPongStrategy.java` now maintains independent source-bar and 5-second micro state, loads upgraded lifecycle/micro models, and applies:
  - lifecycle exits when `-Dstrategy.exit.lifecycleEnabled=true`,
  - 30-second setup arming plus 5-second micro-entry confirmation when `-Dstrategy.micro.entryEnabled=true`,
  - 5-second micro-exit guard checks when `-Dstrategy.micro.exitGuardEnabled=true`.
- Direct Databento historical streaming backtesting is implemented through `DatabentoHistoricalStreamingBacktester.java`; this is the upgraded backtest route and does not read combined training CSVs.

---

## Phase 0 — Baseline Audit and Guardrails

Goal: create a repeatable baseline before deeper changes.

### Tasks

- Confirm the current label rates after the stronger target constants in `train_30s_models.py`:
  - `ENTRY_PROFIT_PCT = 0.0035`
  - `ENTRY_RISK_PCT = 0.0025`
  - `EXIT_DROP_PCT = 0.0020`
  - `EXIT_RISK_PCT = 0.0010`
  - `FUTURE_WINDOW_BARS = 20`
- Add or keep a label audit report that prints:
  - label prevalence by model,
  - label prevalence by hour,
  - label prevalence by regime,
  - average time-to-target for positives,
  - average time-to-stop for negatives,
  - ambiguous same-bar TP/SL collisions.
- Save each training run scorecard under `model_exports/<run_tag>/`.
- Do not promote new models to `src/main/resources/` unless scorecard thresholds pass.

### Files

- `train_30s_models.py`
- Optional helper: `analyze_trade_day_20260521.py` or a new `audit_30s_labels.py`

### Acceptance Criteria

- Long/short entry positive rates are no longer near “almost every bar”.
- Entry target reward:risk is at least 1.2R after configured target/stops, preferably around 1.4R before execution costs.
- Walk-forward scorecard is saved and readable for every export.

### Threshold Calibration Policy

Do not treat `0.60` as a universal trading threshold.

Current implementation notes:

- `train_30s_models.py` currently uses `MIN_TUNED_THRESHOLD = 0.60` as a lower bound for 30-second probability threshold optimization.
- `HistoricalBacktester.java` already uses different runtime defaults, for example long entry `0.68`, short entry `0.63`, long exit `0.58`, short exit `0.60`, and regime classifier `0.50`.

The upgraded system should use calibrated thresholds per model route, not one global value.

Recommended first policy:

```text
30s setup entry models:
  threshold selected by walk-forward expected R and trade count constraints
  initial search range: 0.55 to 0.90
  conservative default/floor: around 0.60

5s micro-entry models:
  threshold selected after the 30s setup is already armed
  initial search range: 0.50 to 0.80
  can be lower than 30s setup threshold because the 30s model already filtered the opportunity
  must still pass spread/chase/invalidation guardrails

lifecycle exit models:
  threshold selected by saved-loss vs cut-winner-too-early analysis
  initial search range: 0.50 to 0.80
  optimize realized R impact, not classification precision alone

5s micro-exit guard models:
  threshold should usually be stricter than lifecycle exit threshold
  initial search range: 0.60 to 0.90
  require hard guardrail confirmation to avoid cutting winners on noise

regime classifier:
  use confidence threshold only for route confidence / fallback-to-no-trade decisions
  initial search range: 0.45 to 0.70
```

Threshold optimization objective should include:

1. average realized R after costs,
2. profit factor,
3. minimum trade count / minimum predicted-positive rate,
4. max drawdown impact,
5. precision and recall,
6. false-positive cost for entries,
7. false-exit cost for lifecycle and micro-exit models.

Store selected thresholds in the model route manifest or routing CSV, not only in code defaults. Example columns already planned:

```csv
long_entry_threshold,short_entry_threshold,long_micro_entry_threshold,short_micro_entry_threshold,long_exit_lifecycle_threshold,short_exit_lifecycle_threshold,long_micro_exit_guard_threshold,short_micro_exit_guard_threshold
```

Thresholds may be different by:

- symbol,
- regime,
- cohort,
- model type,
- side,
- time-of-day bucket.

Before promoting any model, the scorecard should include both:

- the threshold selected on calibration folds,
- the realized performance of that threshold on walk-forward validation folds.

Avoid choosing thresholds on the final backtest period. Use walk-forward calibration to prevent threshold overfit.

### Validation Commands

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python train_30s_models.py --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv
```

---

## Phase 1 — Make Entry Labels More Realistic

Goal: train the entry model on trades that can actually be executed, not on idealized bar-close assumptions.

### Problem

The current label generator uses current close as the assumed decision price and future bar highs/lows as target/stop outcomes. That is good enough for rough path-dependent labels but optimistic for live execution. In live trading, the strategy can only act after a bar is closed or after a faster trigger confirms.

### Implementation Tasks

#### 1. Add configurable fill model to `train_30s_models.py`

Add constants:

- `ENTRY_FILL_MODE = os.getenv("ENTRY_FILL_MODE", "next_open")`
- Supported modes:
  - `current_close`: current behavior for compatibility.
  - `next_open`: more realistic bar-close strategy assumption.
  - `next_open_with_slippage`: next open plus side-aware bps penalty.

For long entry labels:

- If mode is `next_open`, label from `Open[i + 1]`, not `Close[i]`.
- Target = fill price × `(1 + ep)`.
- Stop = fill price × `(1 - er)`.

For short entry labels:

- Fill = `Open[i + 1]`.
- Target = fill price × `(1 - ep)`.
- Stop = fill price × `(1 + er)`.

#### 2. Keep label path event-ordered

Current logic already rejects ambiguous same-bar TP/SL collisions by breaking when both hit. Preserve that conservative behavior.

#### 3. Add cost/slippage fields

Add optional configuration:

- `ENTRY_SLIPPAGE_BPS = float(os.getenv("ENTRY_SLIPPAGE_BPS", "2.0"))`
- `EXIT_SLIPPAGE_BPS = float(os.getenv("EXIT_SLIPPAGE_BPS", "2.0"))`
- `MIN_NET_R_MULTIPLE = float(os.getenv("MIN_NET_R_MULTIPLE", "1.2"))`

The trainer should warn if `(ENTRY_PROFIT_PCT - costs) / (ENTRY_RISK_PCT + costs)` falls below `MIN_NET_R_MULTIPLE`.

### Files

- `train_30s_models.py`

### Acceptance Criteria

- Training can run in both compatibility mode and realistic mode.
- Label prevalence and walk-forward precision are printed for both modes during audit.
- No Java feature schema changes are required for this phase.

---

## Phase 1B — Databento 20260523 Data Builder Upgrade

Goal: build all upgraded 30-second and smaller-cadence training data from the new Databento datasets without overwriting earlier outputs.

### Source Datasets

Use:

```zsh
EQUS_DIR="/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9"
OPRA_DIR="/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD"
```

**Requirement / Source Schema Details:**
- **Equity (`EQUS-*`)**: Schema is `tbbo` (Top of Book). This provides the continuous bid/ask quotes required for accurate spread validation and conservative fill simulations.
- **Options (`OPRA-*`)**: Schema is `1s` (1-second aggregates). This provides necessary option flow/context features without the extreme storage overhead of tick-level OPRA data.

OPRA 1-second features should be used in two ways:

1. Native 1-second option features for state, quote-quality, and deterministic guardrails.
2. Rolled-up 5-second option features for 5-second micro-entry and micro-exit guard AI models.

All OPRA-to-equity joins must be timestamp-safe and no-lookahead. A 5-second equity micro bar ending at `T` may only use OPRA 1-second records with timestamps `<= T` and inside the intended lookback window.

### Existing Combined 1s → 30s Builder Flow

This is not a new data-fusion requirement. The current Databento builder already creates combined 1-second equity + option state and then rolls it into 30-second bars.

Verified current implementation in `build_30s_from_5s_csv.py`:

```text
_load_tbbo_second_bars(...)
  -> builds 1-second equity bars/features from equity tbbo

_regularize_second_bars(...)
  -> regularizes the equity 1-second session index and fills quote/price state

_load_opra_second_option_deltas(...)
  -> builds 1-second OPRA option volume deltas from ohlcv-1s

_merge_options_into_second_bars(...)
  -> joins OPRA 1-second option features into equity 1-second bars

_aggregate_intraday_to_30s(...)
  -> resamples the combined 1-second equity + OPRA bars into 30-second training bars
```

Therefore the upgrade should reuse the existing combined 1-second intermediate dataframe as the canonical source for all cadences:

```text
combined_1s = equity_tbbo_1s + opra_ohlcv_1s
training_1s = combined_1s
training_5s = resample combined_1s to 5s
training_30s = resample combined_1s to 30s
```

Do not create separate equity-only 5-second or 1-second micro datasets. Micro datasets should come from the same combined 1-second state that already feeds the 30-second builder.

Do not use the older upgrade sources for new training runs:

```text
EQUS-20260419-LWMAP766M4
OPRA-20260419-PSJKYXSJ7W
```

### Output Paths

Recommended versioned outputs:

```zsh
FINAL_DATA_DIR="$ROOT/training_data/databento_30s_20260523"
FINAL_COMBINED_CSV="$ROOT/training_data/databento_30s_20260523_combined.csv"
CHUNK_ROOT="$ROOT/training_data/databento_30s_20260523_build_chunks"
FINAL_5S_DATA_DIR="$ROOT/training_data/databento_5s_20260523"
FINAL_1S_DATA_DIR="$ROOT/training_data/databento_1s_20260523"
FINAL_MULTI_DATA_DIR="$ROOT/training_data/databento_multi_20260523"
```

The multi-cadence dataset should include aligned equity and option feature outputs:

```text
1s layer:
  equity tbbo-derived spread/slippage state
  OPRA 1s option-flow features

5s layer:
  equity 5s OHLCV/micro features
  OPRA 1s features rolled up to 5s windows

30s layer:
  equity 30s context/regime features
  OPRA 1s features rolled up to 30s context windows
```

### Cadence Feature Parity Rule

The 30-second, 5-second, and 1-second builders should share the existing combined 1-second source frame where practical:

```text
Common source inputs:
  equity tbbo quotes/trades/state
  OPRA 1s option-flow aggregates

30s output:
  combined equity + OPRA context features for setup/regime/lifecycle models

5s output:
  combined equity + OPRA micro features for micro-entry and micro-exit guard models

1s output:
  combined equity + OPRA state features for guardrails, spread/slippage, and future research
```

The goal is not to make every feature numerically identical across cadences. The goal is to preserve the same feature concepts with cadence-specific suffixes and no-lookahead windows. Example:

```text
f_option_put_call_ratio_30s
f_option_put_call_ratio_5s
f_option_put_call_ratio_1s

f_equity_spread_bps_30s
f_equity_spread_bps_5s
f_equity_spread_bps_1s
```

This prevents train/live schema drift and lets the micro models learn from the same option-flow context that the 30-second setup model already sees.

Implementation note: some existing columns are named with `5s` suffixes, such as `PutVolDelta5s`, `CallVolDelta5s`, `QuoteUpdateCount5s`, and `SpreadMaxBps5s`, even though they are currently produced on the 1-second intermediate frame before 30-second aggregation. The upgrade should either:

1. preserve these legacy names for backward compatibility and add new explicit cadence aliases, or
2. introduce normalized names such as `PutVolDelta1s`, `PutVolDelta5s`, and `PutVolDelta30s` while keeping compatibility aliases during transition.

Avoid silently changing feature semantics without a schema migration note and scorecard comparison.

### Scripts

Create versioned scripts instead of changing prior-run scripts in place:

- `scripts/run_parallel_databento_build_20260523.sh`
- `scripts/check_retrain_20260523_status.sh`
- `scripts/run_databento_compare_training_20260523.sh`

The 20260523 builder should extend the existing `build_30s_from_5s_csv.py` Databento path rather than replacing it. Recommended implementation:

1. Factor the existing combined 1-second dataframe creation into a reusable function, for example `build_combined_second_bars_for_symbol_day(...)`.
2. Reuse that function to emit:
   - 1-second combined bars,
   - 5-second combined bars,
   - 30-second combined bars.
3. Keep the current 30-second output path backward-compatible while adding new 5-second and 1-second output directories.

### Acceptance Criteria

- 30-second, 5-second, and, if supported by source granularity, 1-second outputs are written under 20260523 paths.
- Feature names are cadence-stable where possible, with cadence-specific suffixes or prefixes for 30s, 5s, and 1s variants.
- 5-second and 1-second outputs include both equity `tbbo`-derived features and OPRA option-flow features, matching the combined-data approach already used by the 30-second builder.
- The 5-second and 1-second outputs are derived from the same combined 1-second intermediate dataframe currently used to build 30-second bars.
- Feature schema audits compare 30s/5s/1s outputs and report missing common feature concepts.
- The build scripts fail fast if either 20260523 Databento source directory is missing.

---

## Phase 2 — Position-Aware Exit Training Dataset

Goal: replace the blind top/bottom exit model with a model that understands the active trade lifecycle.

### Problem

Current exit labels answer:

- Long exit: “Will price drop by `EXIT_DROP_PCT` before rising by `EXIT_RISK_PCT`?”
- Short exit: “Will price rise by `EXIT_DROP_PCT` before falling by `EXIT_RISK_PCT`?”

That model does not know:

- where the entry was,
- why the position was entered,
- whether the thesis is still valid,
- whether the trade already achieved MFE,
- whether unrealized PnL is positive or negative,
- whether the trade is stale.

### Implementation Tasks

#### 1. Add a simulated trade generator

Create a new function in `train_30s_models.py` or a new module:

- `build_position_lifecycle_rows(df, feature_cols)`

For every historical bar:

1. Use current entry labels and/or entry model-compatible criteria to identify candidate entries.
2. Simulate a long/short position from the realistic fill price.
3. For each subsequent bar until exit horizon or hard stop/target:
   - emit one row with market features plus position-state features,
   - create a label for whether exiting now is better than holding.

#### 2. Add position-state feature columns

Recommended first schema:

| Feature | Long Meaning | Short Meaning |
|---|---|---|
| `f_pos_side` | `1.0` | `-1.0` |
| `f_bars_since_entry` | elapsed 30s bars normalized, e.g. `/ 20` | same |
| `f_unrealized_pnl_pct` | `(close - entry) / entry` | `(entry - close) / entry` |
| `f_unrealized_pnl_r` | pnl pct / entry risk pct | same |
| `f_mfe_r` | max favorable move / risk | same |
| `f_mae_r` | max adverse move / risk | same |
| `f_entry_prob` | entry model probability if available, else label proxy | same |
| `f_entry_rsi` | RSI at entry | same |
| `f_entry_macd_diff` | MACD diff at entry | same |
| `f_entry_dist_vwap` | distance from VWAP at entry | same |
| `f_thesis_macd_delta` | current MACD diff minus entry MACD diff | sign-aware |
| `f_thesis_vwap_delta` | current distance-to-VWAP minus entry distance | sign-aware |
| `f_target_remaining_r` | remaining distance to original target in R | same |
| `f_stop_remaining_r` | remaining distance to stop in R | same |

#### 3. Define exit label as “exit now beats hold”

For each in-position lifecycle row:

Label = 1 if any of these are true:

- hard stop will be hit before target,
- current MFE is high and next N bars materially give back profit,
- thesis invalidates and forward return is negative after costs,
- holding another N bars has lower expected R than exiting now.

Recommended simple first version:

For long lifecycle row at bar `t`:

- `exit_now_value = current_unrealized_r`
- `hold_value = best path outcome over next N bars using stop/target/event ordering`
- label `1` if `exit_now_value >= hold_value + EXIT_DECISION_MARGIN_R`

Use equivalent sign-aware logic for shorts.

#### 4. Train new lifecycle exit ONNX models

Export either:

- `long_exit_lifecycle.onnx` and `short_exit_lifecycle.onnx`, or
- one combined `position_exit_lifecycle.onnx` with `f_pos_side`.

For Java deployment simplicity, start with two models:

- `long_exit_lifecycle.onnx`
- `short_exit_lifecycle.onnx`

Train these models across all regimes, but include regime and entry-reason features. Recommended first-version feature additions:

- one-hot or probability-style current regime features,
- entry regime features,
- entry reason features such as breakout, pullback, reversal, momentum, or mean reversion,
- regime-change-since-entry flags,
- original 30-second entry score and threshold margin.

### Files

- `train_30s_models.py`
- Optional new module: `position_lifecycle_features.py`
- `src/main/resources/*.onnx` after model promotion

### Acceptance Criteria

- The lifecycle dataset has rows only while a simulated position is open.
- Exit model features include current trade state, not only market context.
- Walk-forward evaluation reports exit precision and average realized R impact.
- Existing `long_exit.onnx` and `short_exit.onnx` may remain archived for comparison runs, but they are not runtime fallbacks for the upgraded route.

---

## Phase 2B — 5-Second Micro-Entry and Micro-Exit Guard Training

Goal: train the first-version micro AI models on 5-second bars while keeping 1-second bars for state, spread/slippage, and deterministic emergency guardrails.

### Models to Build

Required first-version micro models:

- `longMicroEntryAi`
- `shortMicroEntryAi`
- `longMicroExitGuardAi`
- `shortMicroExitGuardAi`

Export names:

- `long_micro_entry_5s.onnx`
- `short_micro_entry_5s.onnx`
- `long_micro_exit_guard_5s.onnx`
- `short_micro_exit_guard_5s.onnx`

### Cadence Decision

Use 5-second bars for these AI models first.

1-second bars should still be produced if source data supports them, but they should be used initially for:

- spread and quote-quality checks,
- native OPRA 1-second option-flow context,
- slippage/chase distance checks,
- fast deterministic emergency exits,
- state reconstruction and future research.

Do not train separate 1-second AI models in the first implementation unless comparison backtests show that 5-second micro models consistently react too late after costs.

### Micro-Entry Dataset

For every 30-second bar where the entry setup model would arm a side:

1. Locate the next 5-second bars inside the arm TTL, for example 30 to 90 seconds.
2. Emit one candidate row per 5-second bar while still flat and armed.
3. Include 30-second setup features, current 5-second micro features, and route metadata.
4. Include OPRA 1-second option-flow features rolled into the current 5-second decision window.
5. Label whether entering on that 5-second bar reaches the target before the stop after realistic spread/slippage costs.

Recommended labels:

```text
Label_Long_MicroEntry = 1 if long entry at this 5s bar reaches target before stop
Label_Short_MicroEntry = 1 if short entry at this 5s bar reaches target before stop
```

### Micro-Exit Guard Dataset

For every simulated or actual in-position lifecycle path:

1. Emit one row per 5-second bar while the trade is open.
2. Include current 5-second micro features, 30-second context, active trade-state features, and OPRA 1-second option-flow features rolled into the current 5-second decision window.
3. Label whether the micro guard should exit/reduce before the next lifecycle cadence.

Recommended first label:

```text
Label_Long_MicroExitGuard = 1 if exiting now avoids adverse R before the next 30s lifecycle check
Label_Short_MicroExitGuard = 1 if covering now avoids adverse R before the next 30s lifecycle check
```

More concrete first-version rule:

```text
exit_now_r = current unrealized R
hold_until_next_lifecycle_r = event-ordered outcome over the next 5s bars until the next 30s checkpoint

micro_exit_label = 1 if exit_now_r >= hold_until_next_lifecycle_r + MICRO_EXIT_MARGIN_R
```

Also force positive micro-exit labels for severe local invalidation events, such as:

- hard spread blowout,
- fast adverse move beyond configured R,
- reclaim/loss of setup close against the position,
- high-volume rejection against the position,
- quote/trade state that would have triggered deterministic emergency exit.

### Recommended 5-Second Micro Features

Include:

- `f_micro_return_5s`,
- `f_micro_return_15s`,
- `f_micro_range_atr`,
- `f_micro_body_to_range`,
- `f_micro_volume_burst`,
- `f_micro_vwap_dist`,
- `f_micro_spread_bps`,
- `f_micro_chase_atr_from_setup_close`,
- `f_micro_pullback_depth_atr`,
- `f_micro_break_continuation`,
- `f_micro_rejection_wick`,
- `f_micro_bid_ask_imbalance` if available,
- `f_option_call_volume_5s`,
- `f_option_put_volume_5s`,
- `f_option_put_call_ratio_5s`,
- `f_option_call_trade_count_5s`,
- `f_option_put_trade_count_5s`,
- `f_option_net_call_pressure_5s`,
- `f_option_net_put_pressure_5s`,
- `f_option_volume_burst_5s`,
- `f_option_flow_accel_5s`,
- `f_option_near_atm_call_volume_5s` if strike/underlying mapping is available,
- `f_option_near_atm_put_volume_5s` if strike/underlying mapping is available,
- 30-second regime features,
- 30-second setup score and threshold margin,
- entry reason features,
- active trade-state features for micro-exit guard rows.

### Option Feature Alignment Rules

- OPRA 1-second features may be used directly by the 1-second state/guardrail layer.
- OPRA 1-second features should be aggregated into 5-second windows for 5-second micro AI inference.
- OPRA 1-second features should also be aggregated into 30-second windows for setup/regime/context features.
- Equity `tbbo` features and OPRA option features should be joined into every output cadence: 30s, 5s, and 1s.
- Use only records available at or before the decision timestamp.
- If OPRA data is delayed or missing for a decision window, emit explicit missing/stale flags rather than forward-filling silently.
- Include staleness features such as `f_option_seconds_since_last_update` and `f_option_missing_window_flag`.

### Acceptance Criteria

- Four 5-second micro ONNX models are exported and scorecards are saved under `model_exports/<run_tag>/`.
- Micro-entry datasets contain only rows that occur after a valid 30-second armed setup.
- Micro-exit guard datasets contain only rows while a simulated or actual position is open.
- 1-second AI models are not required for first rollout; 1-second bars remain available for non-AI guardrails and later research.
- Backtests report micro-entry confirmation rate and micro-exit guard benefit/harm separately.

---

## Phase 3 — Runtime Position/Thesis Tracking in `PingPongStrategy.java`

Goal: allow live Java code to build the same position-aware features used by the new lifecycle exit models.

### Current Runtime Hooks

Useful existing state in `PingPongStrategy.java`:

- `currentPosition`
- `avgEntryPrice`
- `barClose`
- `atr12`
- `macdDiff`
- `vwap`
- `currentMarketTime`
- `constructFeatureValueMap(currentRsi)`
- `handleOrderProgress(...)`

### Implementation Tasks

#### 1. Add `EntryThesisState`

Add a small internal state object:

```java
private static final class EntryThesisState {
	int side; // +1 long, -1 short, 0 flat
	double entryPrice;
	double entryRiskPct;
	double entryTargetPct;
	double entryRsi;
	double entryMacdDiff;
	double entryDistVwap;
	double entryAtrNorm;
	double entryProb;
	MarketRegime entryRegime;
	long entryBucketEpoch;
	int barsSinceEntry;
	double mfeR;
	double maeR;
}
```

#### 2. Capture thesis when entry order is submitted/filled

Best source of truth:

- On entry decision: store pending thesis snapshot and probability.
- On fill in `handleOrderProgress(...)`: promote pending thesis to active thesis using `avgFillPrice`.

This avoids recording thesis for an order that never fills.

**Partial Fill Rule:** Partial exits (scale-outs) should update realized PnL and reduce position size, but they should **not** wipe or reset the original entry thesis. The `EntryThesisState` tracks the original entry logic until the strategy goes completely `FLAT`.

#### 3. Update thesis every finalized 30-second bar

Inside `process30SecondBar(...)` after features are updated:

- increment `barsSinceEntry`,
- update `mfeR`,
- update `maeR`,
- update unrealized PnL R,
- clear thesis when flat.

#### 4. Add lifecycle feature construction

Add:

- `constructPositionAwareExitFeatureValueMap(...)`
- `POSITION_AWARE_EXIT_FEATURE_COLUMNS`

This method should merge:

1. all normal market features,
2. regime probability features,
3. position-state features.

#### 5. Load required lifecycle exit models for upgraded routing

Add routed lifecycle models:

- `longExitLifecycleAi`
- `shortExitLifecycleAi`

Runtime routing:

1. If lifecycle mode is enabled and the required lifecycle model plus position thesis state are available, use the lifecycle model.
2. If lifecycle mode is enabled but the required lifecycle model or thesis state is missing, block the exit-AI route, log the invalid route, and rely only on hard risk controls such as stops, targets, time stops, and EOD flatten.
3. Do not fall back from lifecycle exit models to old generic non-regime exit models in the upgraded route.

#### 6. Enforce explicit model routing

The runtime model route should include all required model paths explicitly, for example:

```csv
symbol,regime,cohort,long_entry_model,short_entry_model,long_exit_lifecycle_model,short_exit_lifecycle_model,long_micro_entry_model,short_micro_entry_model,long_micro_exit_guard_model,short_micro_exit_guard_model,long_entry_threshold,short_entry_threshold,long_micro_entry_threshold,short_micro_entry_threshold,long_exit_lifecycle_threshold,short_exit_lifecycle_threshold,long_micro_exit_guard_threshold,short_micro_exit_guard_threshold
```

If any required model for the active mode is missing, mark that symbol/regime/cohort route invalid and do not trade it.

### Files

- `src/main/java/com/calgary/fili/trader/bot/strategy/PingPongStrategy.java`
- `src/test/java/com/calgary/fili/trader/bot/strategy/*`

### Acceptance Criteria

- Long/short exit decisions log active lifecycle features.
- Missing required lifecycle models invalidate the upgraded route instead of triggering a generic fallback.
- Position-state resets on flat sync, EOD flatten, stop exit, and new day.

---

## Phase 4 — 30s Setup Layer + 5s/1s Execution Trigger Layer

Goal: keep 30-second bars for context, but fire entries only after faster micro-confirmation.

This is intended to reduce chasing, not add a second full setup decision. The 30-second model answers “is a valid opportunity present for the next N seconds?” The source-bar/micro layer answers “is this a good execution moment inside that approved opportunity?”

### Target Architecture

```text
30s AI bar close
	-> compute setup score and regime
	-> if entry score passes threshold: ARM_LONG or ARM_SHORT

Incoming 5s/1s source bars
	-> always build/store/update state
	-> if armed: evaluate micro-trigger
	-> if in position: evaluate micro-exit/risk guard if enabled
	-> if trigger passes: place order
	-> if timeout or invalidation: disarm
```

### Small-Bar State Rules

1. Flat and not armed:
   - build/store 1-second or 5-second bars,
   - update indicators and spread/slippage state,
   - do not run micro-entry model inference.
2. Flat and armed:
   - evaluate `longMicroEntryAi` or `shortMicroEntryAi`,
   - require spread/chase/invalidation checks to pass before order submission.
3. In position:
   - update MFE/MAE and trade-state features,
   - evaluate lifecycle exit on its configured cadence,
   - evaluate `longMicroExitGuardAi` or `shortMicroExitGuardAi` on 5-second source bars if enabled.

**Runtime State Requirement:** The Python training builder already has the correct combined 1-second equity `tbbo` + OPRA `ohlcv-1s` intermediate state before rolling into 30-second bars. The Java live strategy must mirror that idea at runtime by maintaining independent source-bar / 5-second rolling state for micro features (e.g., `f_micro_return_15s`, micro-VWAP distance) without corrupting the standard 30-second bucket accumulation.

**Threading/Performance Constraint:** Evaluating ONNX models on every 5s bar for all armed or in-position symbols can spike CPU. Java ONNX evaluation requires an optimized `OrtSession` pool or asynchronous inference task queues if tracking many symbols concurrently.

### Micro Cadence Recommendation

Use 5-second bars as the first production micro-AI cadence.

Reasons:

1. 5-second bars are fast enough to improve execution timing after a 30-second setup without forcing the system to react to every noisy tick-like fluctuation.
2. 5-second features are easier to train robustly because they have more stable OHLCV, spread, and micro-momentum signals than 1-second bars.
3. 1-second AI models would multiply model count and feature alignment work while increasing false positives, especially around wide spreads, quote flicker, and opening volatility.
4. The strategy can still build 1-second bars and use them for deterministic guardrails such as max spread, fast adverse move, stale quote, and slippage checks.

Recommended first version:

```text
30s bars:
  setup/regime/lifecycle context

5s bars:
  longMicroEntryAi
  shortMicroEntryAi
  longMicroExitGuardAi
  shortMicroExitGuardAi

1s bars:
  state building only
  spread/slippage guardrails
  native OPRA 1s option-flow context
  emergency deterministic risk checks
```

Only add separate 1-second micro AI models later if backtests show that 5-second confirmation consistently enters/exits too late and the 1-second features improve realized R after costs. This does not prevent using OPRA 1-second option features as inputs; those features should feed both the 1-second state layer and the 5-second rolled-up micro AI layer.

### Implementation Tasks

#### 1. Add armed setup state

In `PingPongStrategy.java`:

```java
private static final class ArmedSetupState {
	int side; // +1 long, -1 short
	double setupScore;
	double setupClose;
	double setupHigh;
	double setupLow;
	double setupAtr;
	double setupRsi;
	double setupMacdDiff;
	MarketRegime setupRegime;
	long armedAtEpoch;
	int sourceBarsObserved;
}
```

#### 2. Change flat entry behavior

Current behavior:

- If entry model passes on 30s bar, place order immediately.

New behavior:

- If entry model passes on 30s bar, arm setup.
- Do not submit order yet.

#### 3. Evaluate micro-trigger on each source bar

Inside `handle5SecondBar(...)`, after hard stop handling and before/after bucket accumulation:

For `ARM_LONG`, require examples such as:

- source bar close > previous source bar high,
- spread under max threshold,
- ask/bid size not extremely unfavorable,
- price has not already moved too far from setup close,
- setup not expired.

For `ARM_SHORT`, mirror the logic:

- source bar close < previous source bar low,
- spread ok,
- shortable shares ok if available,
- no adverse drift beyond threshold.

First implementation can use deterministic micro rules plus the configured spread/chase checks. When trained micro models are promoted, replace or augment these deterministic rules with:

- `longMicroEntryAi`,
- `shortMicroEntryAi`.

These models should be global but regime-aware in the first version, not separate per-regime models unless training coverage is sufficient.

#### 4. Evaluate micro-exit guard on each 5-second source bar while in position

The lifecycle exit model remains the primary position-aware exit decision, but the micro-exit guard protects against fast local deterioration between 30-second lifecycle evaluations.

For long positions, `longMicroExitGuardAi` should detect conditions such as:

- fast rejection after entry,
- bid/ask spread widening,
- sudden adverse 5-second momentum,
- failed continuation after armed entry,
- micro lower-high / lower-low sequence,
- volume burst against the position,
- price falling back through setup close, VWAP, or short-term support.

For short positions, `shortMicroExitGuardAi` mirrors the logic:

- fast squeeze/reversal after entry,
- spread widening,
- sudden adverse upside momentum,
- failed breakdown continuation,
- micro higher-low / higher-high sequence,
- volume burst against the short,
- price reclaiming setup close, VWAP, or short-term resistance.

First-version micro-exit guard behavior should be conservative:

```text
if in long:
    score = longMicroExitGuardAi(5s_micro_features + 30s_context + trade_state)
    if score >= threshold and hard guardrails agree:
        exit or reduce according to configured policy

if in short:
    score = shortMicroExitGuardAi(5s_micro_features + 30s_context + trade_state)
    if score >= threshold and hard guardrails agree:
        exit or reduce according to configured policy
```

The micro-exit guard should not replace hard stops, profit targets, time stops, or lifecycle exits. It is an early-warning model for fast adverse conditions.

#### 5. Add disarm rules

Disarm if:

- setup age > configured source bars or seconds,
- position opens,
- model/price invalidation occurs,
- source price moves beyond max chase distance,
- hard stop/circuit breaker blocks entries,
- EOD flatten window begins.

#### 6. Add configuration

Suggested properties:

- `strategy.execution.armedEntryEnabled=true`
- `strategy.execution.armTimeoutSeconds=30`
- `strategy.execution.maxChaseAtr=0.35`
- `strategy.execution.maxSpreadBps=8`
- `strategy.execution.requireMicroBreak=true`
- `strategy.micro.primaryCadence=5s`
- `strategy.micro.useOneSecondAi=false`
- `strategy.micro.exitGuardEnabled=true`

### Files

- `PingPongStrategy.java`
- `StrategyEvent.java` if a richer source-bar event is needed
- Backtester tests

### Acceptance Criteria

- 30s model no longer directly fires entries when armed mode is enabled.
- Logs clearly show: setup armed, micro-trigger passed/failed, order submitted.
- Can disable armed mode and return to old direct-entry behavior with one property.
- When flat and not armed, source bars update state but do not run micro-entry inference.
- First implementation uses 5-second micro AI models for entry confirmation and exit guard decisions.
- 1-second bars remain available for state, spread/slippage, and deterministic emergency checks without requiring separate 1-second AI models.

---

## Phase 5 — Backtester Upgrade for Two-Layer Execution

Goal: make historical validation match live behavior.

### Current State

`HistoricalBacktester.java` already feeds 5-second bars into `PingPongStrategy.onSourceBar(...)`, and the strategy aggregates them into 30-second AI buckets.

That means it is a good base for two-layer testing because source bars are already available between 30-second AI decisions.

### Implementation Tasks

#### 0. Add historical Databento replay provider

Create a replay path that can read historical EQUS and OPRA Databento files and emit strategy events in timestamp order:

```text
DatabentoHistoricalReplayProvider
  -> equity quote/trade events
  -> option quote/trade events
  -> 1s bar close events
  -> 5s bar close events
  -> 30s bar close events
```

The replay provider should feed the same strategy interface used by the live Databento provider, so live and backtest behavior share the same arming, micro-confirmation, lifecycle-exit, spread, slippage, and risk-control code paths.

The replay provider must synchronize equity `tbbo` events and OPRA `1s` option-flow events without lookahead. At each 5-second micro decision timestamp, the strategy should see only the option-flow state that would have been available live at that timestamp.

Recommended fill modes:

1. Conservative: long entry at ask, long exit at bid, short entry at bid, short cover at ask, plus slippage.
2. Midpoint: midpoint with configured slippage.
3. Aggressive: marketable next-quote fill assumption.

#### 1. Add backtest metrics

Track and print:

- armed setups count,
- armed-to-filled conversion rate,
- expired setups,
- disarmed due to chase/spread/invalidation,
- micro-entry model evaluated count,
- micro-entry confirmed count,
- micro-exit guard evaluated count,
- micro-exit guard fired count,
- micro-exit saved-loss vs cut-winner-too-early counts,
- average setup-to-fill latency,
- realized R per trade,
- MFE/MAE by trade,
- lifecycle exit vs old exit comparison.

#### 2. Add trade lifecycle CSV

Extend current trade log or add new file with:

- symbol,
- side,
- entry time,
- entry price,
- exit time,
- exit price,
- entry model probability,
- exit model probability,
- regime,
- bars held,
- MFE R,
- MAE R,
- realized R,
- exit reason.

#### 3. Add comparison runs

Run backtests in three modes:

1. Archived baseline: direct 30s entry + archived old exit model, for comparison only.
2. Upgraded direct-entry comparison: direct 30s entry + lifecycle exit model.
3. Target architecture: armed 30s setup + 5s/1s trigger + lifecycle exit model.

### Files

- `HistoricalBacktester.java`
- `PingPongStrategy.java`
- Optional analyzer: `analyze_trade_day_20260521.py`

### Acceptance Criteria

- Same CSV can be replayed in old and new modes.
- Historical Databento EQUS/OPRA data can be replayed through the same strategy event flow as live once `DatabentoHistoricalReplayProvider` is implemented.
- New mode improves at least one of:
  - average realized R,
  - max drawdown,
  - profit factor,
  - adverse selection/slippage proxy,
  - trade quality score.
- If total PnL improves only by taking far fewer trades, report that separately.

### Sanity parity required before live/paper promotion

The June 13 fixed-quality Databento pilot established a repeatable sanity-gate sequence: source hashes, DBN decode audits, paired-date manifest, prebuild source-file checks, fixed-quality build verification, and quality-sanity validation. The live and backtester paths must mirror those controls before any upgraded model bundle can be treated as paper/live promotable.

Implementation requirements:

1. **Shared event contract**
   - Extend `DatabentoEvent.java` and the Python emitters (`scripts/databento_live_normalizer.py`, `scripts/databento_historical_streamer.py`) so live and replay events can carry the same quality/provenance fields used in the offline 1s/5s/30s build.
   - Required fields include `DataQualityFlags`, `ChildDataQualityFlagUnion`, `TradeCoverage`, `QuoteUpdateCoverage`, `QuoteStateCoverage`, `SyntheticCoverage`, `QuoteAgeMsMean`, `QuoteAgeMsMax`, `ValidSpreadCoverage`, `LockedCrossedSeconds`, `QualityScore`, event schema version, source dataset/schema, and timestamp provenance.

2. **No-lookahead replay/live aggregation**
   - Live, historical streamer, and `DatabentoHistoricalReplayProvider` aggregation must be forward-fill-only from already observed state.
   - No future `bfill()`, next-row quote repair, or end-of-bucket lookahead may be used to populate an earlier decision timestamp.
   - Replay must assert that OPRA/equity state used for a decision has `ts_event`/arrival time at or before that decision timestamp within the documented tolerance.

3. **Live startup preflight**
   - Before trading starts, write a machine-readable manifest containing dataset/schema/stype, symbol universe, option parents, quality thresholds, feature schema hash, model bundle ID, Databento clock assumptions, and output/log root.
   - If required quality fields, feature-schema hashes, or model manifest fields are missing, block new entries and keep only flatten/emergency exits available.

4. **Runtime sanity counters**
   - `DatabentoFeedHealth` and status endpoints should expose per-symbol/cadence counts for expected bars, seen bars, missing bars, stale/no-quote/synthetic/locked-crossed states, parent-child quality diff fraction, feature-vector reject counts, and model-inference skip reasons.
   - These counters should be persisted to session artifacts so live sessions can be replayed and audited.

5. **Backtester sanity artifacts**
   - `DatabentoHistoricalStreamingBacktester.java` must write replay source manifests, event-count summaries, quality-sanity summaries, feature-vector schema summaries, and decision-parity summaries under the configured external output root.
   - A replay/backtest run is promotion-eligible only if its sanity artifact reports `errors=[]` and all mismatches above tolerance are explained.

6. **Decision parity gate**
   - A recorded live-shaped NDJSON stream must be replayed through the backtester.
   - The report must compare bucket boundaries, quality fields, feature vectors, setup/entry/exit scores, thresholds, arms, entries, holds, exits, guard exits, and skip reasons.
   - Any mismatch above tolerance blocks promotion until fixed or quarantined.

Acceptance for the upgraded runtime path:

- `1s` leaf quality may have `DataQualityFlags == ChildDataQualityFlagUnion`.
- `5s`/`30s` parent quality must be threshold-derived and must not be a blind child-flag union.
- Live and replay both produce sanity JSON/CSV outputs similar to `pilot_build_check_*` and `pilot_quality_sanity_*`.
- CSV-only historical comparisons remain useful for debugging, but promotion requires the live-shaped event/replay path.

---

## Phase 6 — Model Promotion and Runtime Rollout

Goal: promote safely without breaking live trading.

### Recommended Rollout Order

1. Train with stronger/realistic entry labels, but keep Java behavior unchanged.
2. Run backtests and compare against current ONNX models.
3. Add Java thesis tracking while continuing the current production route until the upgraded model route is complete.
4. Add lifecycle exit model as a required model for the upgraded route, with no generic fallback.
5. Enable lifecycle exit model in paper/backtest only.
6. Add armed execution mode as disabled-by-default property.
7. Backtest armed mode.
8. Paper trade armed mode.
9. Promote to live with reduced size.

### Feature Flags

Add or use system properties:

```zsh
-Dstrategy.exit.lifecycleEnabled=false
-Dstrategy.execution.armedEntryEnabled=false
-Dstrategy.execution.armTimeoutSeconds=30
-Dstrategy.execution.maxChaseAtr=0.35
-Dstrategy.execution.maxSpreadBps=8
```

### Rollback

Rollback should require only properties/model route changes:

1. Switch back to a separately declared legacy route if a legacy route is intentionally kept for comparison or emergency rollback:
   ```zsh
   -Dstrategy.model.route=legacy
   ```
2. Disable armed execution:
   ```zsh
   -Dstrategy.execution.armedEntryEnabled=false
   ```
3. Do not silently fall back from the upgraded route to generic non-regime models.

---

## Proposed Implementation Sequence

### Milestone A — Training Label Realism

- Modify `train_30s_models.py` with entry fill mode and cost-aware warning.
- Add label audit output.
- Run training once with compatibility mode and once with realistic mode.

### Milestone B — Java Thesis State Without Behavior Change

- Add `EntryThesisState` and pending thesis capture.
- Log thesis state while the current production route remains unchanged.
- Add tests for state capture/reset.

### Milestone C — Lifecycle Exit Model

- Generate lifecycle dataset.
- Train/export lifecycle exit ONNX files.
- Add Java lifecycle model routing with missing-model route invalidation.
- Backtest old vs lifecycle exit.

### Milestone D — Armed Micro-Execution

- Generate 5-second micro-entry and micro-exit guard datasets.
- Train/export `long_micro_entry_5s.onnx`, `short_micro_entry_5s.onnx`, `long_micro_exit_guard_5s.onnx`, and `short_micro_exit_guard_5s.onnx`.
- Add `ArmedSetupState`.
- Change entry decision to arm, not fire, behind feature flag.
- Add source-bar micro-trigger and 5-second micro-exit guard.
- Backtest direct vs armed mode.

### Milestone E — Production Rollout

- Paper trade.
- Promote only if trade lifecycle metrics pass.
- Deploy with reduced size and strict circuit breaker.

---

## Test Plan

### Python Tests/Checks

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
python -m py_compile train_30s_models.py
python train_30s_models.py --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv
```

### Java Tests/Checks

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
./mvnw test
```

### Backtest Examples

Current/backward-compatible mode:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
./mvnw -DskipTests exec:java -Dexec.mainClass=com.calgary.fili.trader.testers.HistoricalBacktester -Dexec.args="TSLA TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv"
```

Upgraded Databento historical streaming mode, no combined CSV input:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
START_DATE=2026-05-21 \
END_DATE=2026-05-21 \
scripts/run_databento_historical_streaming_backtest_20260523.sh TSLA
```

The launcher resolves the selected 20260523 30s route from `runtime/databento/bots/trading-<symbol>.properties`
or `runtime/databento/model-routing.csv`, validates required ONNX artifacts, and injects the promoted lifecycle/micro
model directory and scorecard thresholds by default. Orders are handled by the backtester’s IBKR simulation broker; no
external IBKR connection is used.

Dry-run the same path without fetching historical records:

```zsh
cd /Users/filmonghezehey/trading-agent/worktrees/databento
DRY_RUN=true BACKTEST_MAX_TRADES=0 TIMEOUT_SECONDS=30 scripts/run_databento_historical_streaming_backtest_20260523.sh TSLA
```

The upgraded command launches `scripts/databento_historical_streamer.py --source api`, so non-dry-run backtests require a valid `DATABENTO_API_KEY`. The launcher resolves it the same way as the live Databento sidecar: parent environment first, then `trading.databento.env-file` when configured, otherwise `runtime/databento.env`. The key source is logged, but the key value is never printed.

---

## Key Success Metrics

Do not judge the change only by prediction precision. Judge it by trade lifecycle quality:

1. Average realized R per trade.
2. Profit factor.
3. Max intraday drawdown.
4. MFE capture ratio: `realized_profit / MFE` for winners.
5. MAE before exit for losers.
6. Setup-to-fill latency.
7. Slippage/chase distance from 30s setup close to actual fill.
8. Trade frequency by hour/regime.
9. False-positive armed setups that never trigger.
10. Exit model “saved loss” vs “cut winner too early” rates.

---

## Recommended First Code Change

Start with the lowest-risk change:

1. Add training fill-mode realism and label audit.
2. Add Java thesis-state logging without changing behavior.

That gives visibility first. Once the runtime logs and training rows agree, add lifecycle exit models and armed execution.
