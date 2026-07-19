# CatBoost setup precision investigation

Date: 2026-06-27

## Scope

Investigated why the current CatBoost 30-second setup models remain below `40%` OOF precision when the desired minimum is at least `50%` on both long and short setup routes.

Primary references:

- `docs/ai_training_dynamic_upgrade_plan_20260612.md`
- `docs/computer_capability_task_organization_20260613.md`
- `runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854`

## Candidate artifact checked

The active June 24 CatBoost setup bundle is:

```text
runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854
```

Its lineage/settings match the rollback candidate documented in the upgrade plan:

```text
MODEL_FAMILY=catboost
REGIME_MODEL_FAMILY=catboost
REQUIRE_MODEL_FAMILY=1
TRAIN_LEGACY_30S_EXIT_MODELS=0
USE_NEWS_BAR_FEATURES=0
USE_META_PRODUCER_FEATURES=0
USE_DATABENTO_SILVER_FEATURES=0
USE_REGIME_PROB_FEATURES=1
COST_AWARE_LABELS=1
ENTRY_FILL_MODE=next_open_with_slippage
```

The primary scorecard reports:

| Side | Rows | Signal rate | Avg OOF precision | Avg threshold | Brier | ECE |
|---|---:|---:|---:|---:|---:|---:|
| Long | `766,485` | `30.67%` | `37.69%` | `0.612` | `0.2471` | `0.1845` |
| Short | `766,485` | `30.18%` | `36.28%` | `0.612` | `0.2476` | `0.1894` |

## Main finding

This is not primarily a threshold-selection bug.  The OOF CatBoost scores do not contain enough ranking separation to produce `50%` precision on either side at any meaningful trade count.

Counterfactual threshold scan on `oof_setup_predictions.csv`:

| Side | Selected thresholded precision | Selected predictions | Best OOF precision with `>=100` predictions | Best OOF precision with `>=1,000` predictions | Best OOF precision with `>=50,000` predictions |
|---|---:|---:|---:|---:|---:|
| Long | `37.71%` | `68,575` | `39.50%` (`1,362` preds) | `39.50%` (`1,362` preds) | `37.23%` |
| Short | `36.18%` | `74,550` | `37.26%` (`28,739` preds) | `37.26%` (`28,739` preds) | `36.77%` |

Even allowing extremely sparse thresholds does not solve it: long reaches only `45.45%` at `11` predictions, and short never reaches `50%` in the OOF ordering.

## Why precision is below 40%

### 1. The cost-aware binary labels are permissive and noisy for “setup quality”

The current positive label is:

```text
expected_net_r_after_costs > 0.0
```

with configured economics:

```text
entry_profit_pct = 0.0035
entry_risk_pct = 0.0025
entry_slippage_bps = 2.0
exit_slippage_bps = 2.0
net_r_multiple ≈ 1.07 < MIN_NET_R_MULTIPLE=1.20
```

Label prevalence from `cost_aware_setup_labels.csv`:

| Label family | Long positive rate | Short positive rate | Either-side positive rate |
|---|---:|---:|---:|
| Cost-aware expected-net-R `> 0` | `31.14%` | `30.69%` | `61.82%` |
| Legacy TP-before-SL | `10.50%` | `11.61%` | `22.11%` |

The cost-aware labels identify roughly 3x as many positives as the stricter legacy TP-before-SL labels.  That makes the model learn a broad directional/horizon label rather than a rare high-conviction setup label.

### 2. Thresholded predictions still have negative mean expected R

At the actual selected setup thresholds, the predicted-positive rows are still not economically clean:

| Side | Selected precision | Mean expected net R among selected rows | Median expected net R among selected rows | Legacy TP-before-SL rate among selected rows |
|---|---:|---:|---:|---:|
| Long | `37.71%` | about `-0.29R` | about `-0.48R` | about `17.6%` |
| Short | `36.18%` | about `-0.32R` | about `-0.44R` | about `16.3%` |

So the setup model is not merely under-calibrated; the selected cohort still contains too many negative-expectancy rows.

### 3. OOF rank separation is weak

OOF discrimination metrics from the current CatBoost scores:

| Side | Base positive rate | ROC AUC | PR AUC / average precision |
|---|---:|---:|---:|
| Long | `30.78%` | `0.5745` | `0.3523` |
| Short | `30.51%` | `0.5742` | `0.3479` |

PR AUC is only about `4–5` percentage points above base prevalence.  That is not enough separation to support a `50%` precision operating point.

### 4. Raw CatBoost probabilities are overconfident and poorly calibrated

The high-probability reliability bins do not have high observed hit rates.  Examples from `calibration_reliability.csv`:

- Long fold 1, `0.80–0.90` probability bin: mean predicted probability `0.834`, observed positive rate `0.397`.
- Short fold 1, `0.80–0.90` probability bin: mean predicted probability `0.834`, observed positive rate `0.349`.

This explains the high ECE (`18–19%`).  Post-hoc calibration can make probabilities honest, but it cannot create `50%` precision when the OOF ranking itself is weak.

### 5. Model-family and feature-block experiments already showed only marginal setup lift

The referenced docs and local readout artifacts show that changing learners/features alone did not solve the setup blocker:

- CatBoost no-news baseline: about `37.7%` long / `36.2%` short global OOF precision.
- LightGBM no-news: near tie, about `37.2%` long / `36.2%` short.
- RandomForest no-news: mixed, about `35.4%` long / `37.9%` short, but much sparser/day-concentrated.
- CatBoost silver presets improved only marginally.  Best side-specific CatBoost silver readout reached about `38.84%` long and `36.80%` short, still far below `50%`.
- CatBoost + meta + silver degraded the short side in the documented matrix and remained **NO-GO**.

### 6. Runtime probability extraction was a separate blocker, not the OOF precision cause

The June 27 docs identify a Java runtime issue where CatBoost setup ONNX predictions were emitted as binary `0/1` label-derived probabilities during replay.  That would make live/backtest thresholds useless as rank filters, but it does not explain the offline OOF scorecard because the OOF scorecard uses Python CatBoost probabilities directly.

That runtime issue has code/tests in the current branch (`AiPredictor.java`, `PingPongStrategy.java`) and should still be validated in the next replay, but the offline under-40% precision is a real training/label/feature problem.

## Conclusion

The current CatBoost setup model cannot be pushed above `50%/50%` precision with threshold tuning.  The under-40% result is expected from the current combination of:

1. permissive `expected_net_r_after_costs > 0` labels,
2. tight/weak base economics (`~1.07R` after costs),
3. a broad positive label rate near `31%` per side / `62%` either side,
4. weak OOF rank separation (`ROC AUC ≈ 0.574`, PR AUC only modestly above base rate),
5. overconfident raw probabilities, and
6. limited setup feature signal in the Java-compatible 34-feature CatBoost contract.

## Recommended next steps to pursue 50%+ setup precision

Do not promote or threshold-lift the current CatBoost setup bundle as a `50%` setup model.  To realistically target `50%+`:

1. **Tighten the setup label.**  Run CatBoost experiments with a materially positive label such as `COST_AWARE_MIN_NET_R_LABEL >= 0.25` / `0.50`, or train directly on expected-net-R regression/ranking and threshold by expected value plus minimum trade count.
2. **Use side/EV arbitration rather than two independent broad binary labels.**  Since current labels are mutually exclusive long/short/neither, a three-class or EV-margin setup learner may better match the runtime decision problem.
3. **Keep richer-source work as the real feature lever.**  Full-window, QA-passed `EQUS mbp-1` and `OPRA tcbbo` features with live/backtest parity are more likely to improve setup precision than more CatBoost hyperparameter tuning on the same base columns.
4. **Add a hard diagnostic gate to setup artifacts.**  For every setup run, report best attainable OOF precision at minimum prediction counts and explicitly fail the `50%` precision target when no OOF threshold can reach it.
5. **Continue validating the June 27 runtime fixes.**  The next replay must confirm non-binary setup probabilities, side arbitration logs, real `f_setup_prob` propagation into micro features, and decision/PnL trade-count evidence.

Current promotion status remains **NO-GO**.
