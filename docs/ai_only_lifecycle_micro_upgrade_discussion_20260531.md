# AI-Only Lifecycle and Micro-Execution Upgrade Discussion

Date: 2026-05-31
Enhanced: 2026-06-02
Decisions recorded: 2026-06-02
Implementation updated: 2026-06-03

Status: living design/review note after inspecting the current Databento/IBKR training, runtime, launch, and backtest code paths. See the 2026-06-03 implementation ledger below for what has now been implemented from this document, including the completed bootstrap lifecycle/micro model training and export snapshot. The exported bundle is a schema-valid bootstrap artifact, not yet a live-promotable artifact, because the completed run still used bootstrap setup/entry score proxies.

This note captures and extends the current upgrade discussion so it can be reviewed and improved iteratively. It complements `docs/training_and_execution_upgrade_plan.md` and focuses on the current implementation path in `train_lifecycle_micro_models.py`, the Java runtime in `PingPongStrategy.java`, the live launcher in `run_symbol.sh`, and the upgraded historical validation path in `DatabentoHistoricalStreamingBacktester.java`.

## 0. Implementation Ledger — 2026-06-03

The following items from this document have been implemented in the workspace as of 2026-06-03.

### Implemented P0 contract and realism fixes

- **C1 / Decision 9 — canonical 30-second micro arm TTL:** `train_lifecycle_micro_models.py` now defaults `MICRO_ARM_TTL_SECONDS` to `30`, matching Java's `strategy.micro.armTtlSeconds` default.
- **C8 — arm start alignment:** Python micro-entry candidates now start at the 30-second bar close (`ctx["_ts"] + 30s`) and include the TTL endpoint, matching Java's deterministic `current30sAiDecisionEpoch = finalizedBucketStart + 30L`.
- **C2 — side-aware fills:** lifecycle and micro row builders now use side-aware bid/ask fills with `ENTRY_SLIPPAGE_BPS`, falling back to close when quotes are unavailable.
- **C3/C4 — setup/entry probability plumbing:** runtime and training rows now carry the 30-second setup probability instead of constant `1.0` whenever a score column is present. Bootstrap `1.0` remains only as a warned, non-promotable fallback.
- **Setup/entry threshold context:** lifecycle, micro-entry, and micro-exit rows now include first-class model features for setup/entry probability, selected threshold, and threshold margin:
  - `f_setup_score_proxy`, `f_setup_prob`, `f_setup_threshold`, `f_setup_threshold_margin`,
  - `f_entry_score_proxy`, `f_entry_prob`, `f_entry_threshold`, `f_entry_threshold_margin`.
  Java stores the same context through `armedSetupProbability`, `armedSetupThreshold`, `pendingEntryProbability`, `pendingEntryThreshold`, `positionEntryProbability`, and `positionEntryThreshold`, then emits the aligned feature maps for lifecycle and micro-exit models.
- **C7 — micro-exit chain alignment:** micro-exit guard training rows now start from the first positive/confirmed micro-entry candidate and use that candidate's fill price, rather than starting unconditionally at the first arm-window bar.
- **Feature schema contract:** `write_scorecards(...)` writes `lifecycle_micro_route_manifest.json`, `feature_schema.json`, and `feature_schema.sha256`; `PingPongStrategy.java` validates model feature counts, feature-column lists, and SHA-256 schema hashes before allowing the upgraded route.
- **Missing upgraded model policy:** missing required lifecycle/micro artifacts invalidate the upgraded route and disable new entries unless `strategy.lifecycle.diagnosticFallback=true` is explicitly set for diagnostic hard-risk-only paper/backtest runs.
- **C5 — portable runners:** `scripts/run_lifecycle_micro_training_20260523.sh` and related versioned training/pipeline scripts now resolve `ROOT` from `TRADING_AGENT_ROOT` or the Git toplevel and default `PYTHON_BIN` to `python3` while preserving explicit overrides.
- **Durable staged-row identifiers:** lifecycle/micro staged rows now include `arm_id`, `trade_path_id`, `entry_decision_id`, `label_version`, `fill_model_version`, and `feature_schema_version` metadata columns. These are explicitly excluded from model features and are intended for audit joins across setup, micro-entry, lifecycle, and micro-exit rows.

### Tests added or updated

- Python tests in `tests/test_lifecycle_micro_models.py` cover:
  - side-aware bid/ask/slippage fills,
  - arm start at 30-second bar close and TTL endpoint inclusion,
  - micro-exit guard rows chaining from the confirmed micro-entry candidate,
  - setup probability/threshold/margin feature propagation,
  - durable arm/trade/entry IDs and version metadata,
  - route manifest schema hash output.
- Java tests in `PingPongStrategyBucketFinalizationTest.java`, `PingPongStrategyFeatureSchemaTest.java`, and existing AI evaluation tests cover:
  - canonical 30-second decision epoch,
  - stored setup probability/threshold/margin clearing,
  - pending-to-position probability/threshold transfer,
  - lifecycle feature map probability/threshold fields.

### Lifecycle/micro training and export completed — 2026-06-03

- The corrected full lifecycle/micro retrain/export completed on 2026-06-03 and wrote the bundle under `/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523`.
- Active completion log: `/Users/FXG06FA/trading-agent-main/runtime/training_logs/lifecycle_micro_retrain_20260603_085941.log`.
- Corrected matched inputs used for the successful run:
  - 30-second per-symbol files: `/Users/FXG06FA/trading-agent-main/training_data/databento_30s`.
  - 5-second per-symbol files: `/Users/FXG06FA/trading-agent-main/training_data/databento_5s_20260523`.
  - The runner default `/Users/FXG06FA/trading-agent-main/training_data/databento_30s_20260523` existed but contained no `*_30s_training.csv` files for this run, so it could not produce matched symbol pairs.
- Completion checks from the final log/artifact verification:
  - `STREAM_DONE=99`, `STAGED=6`, `TRAINED=6`, `WROTE=4`.
  - `TRACEBACK=0` and no matching `train_lifecycle_micro_models.py` process remained active after completion.
  - The scorecard has six rows, the route manifest has six model routes, and all referenced exported model paths exist.
- Staged rows written under `/Users/FXG06FA/trading-agent-main/runtime/lifecycle_micro_20260523_staging`:
  - `long_lifecycle_rows.csv`: 990,000 rows.
  - `short_lifecycle_rows.csv`: 990,000 rows.
  - `long_micro_entry_rows.csv`: 333,506 rows.
  - `short_micro_entry_rows.csv`: 333,304 rows.
  - `long_micro_exit_rows.csv`: 179,472 rows.
  - `short_micro_exit_rows.csv`: 168,792 rows.
- Exported model/artifact files under `/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523`:
  - `long_exit_lifecycle.onnx`, `short_exit_lifecycle.onnx`,
  - `long_micro_entry_5s.onnx`, `short_micro_entry_5s.onnx`,
  - `long_micro_exit_guard_5s.onnx`, `short_micro_exit_guard_5s.onnx`,
  - `lifecycle_micro_scorecard.csv`, `lifecycle_micro_route_manifest.json`,
  - `feature_schema.json`, `feature_schema.sha256`.
- Bundle-level `feature_schema.sha256`: `e9d15001ee9e7d5f768def2d259576a49ea14573d1d85eb6ed7bd531d678bbd1`.
- Important promotion caveat from the training log: `WARN f_setup_score_proxy and f_entry_score_proxy are using bootstrap value 1.0. Models trained with this placeholder must not be promoted to live trading.`

### Post-export validation and dry-run wiring completed — 2026-06-03

- Python validation after export passed with `python3 -m unittest discover -s tests -p 'test*.py' -v`: 5 lifecycle/micro tests ran successfully.
- Full Maven validation after export and after the 50-feature support fix passed with `./mvnw -q test`; only the known Mockito/Byte Buddy dynamic-agent warnings and expected simulated unit-test log messages were observed.
- Upgraded-route dry-run wiring for `TSLA` completed successfully in Databento dry-run mode using:
  - 30-second model fallback: `/Users/FXG06FA/trading-agent-main/runtime/models/TSLA`, because `runtime/databento/model-routing.csv` still contained a stale TSLA model path from another worktree.
  - Lifecycle/micro bundle: `/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523`.
  - Databento Python venv: `/Users/FXG06FA/trading-agent-main/runtime/databento/python-venv/bin/python3`.
- Dry-run result markers:
  - `FEATURE_COUNT_SUPPORTED=PASS` for all loaded 30-second and lifecycle/micro models, including the 50-feature micro-exit guard models.
  - `Validated lifecycle/micro route manifest schema` emitted once for `TSLA`.
  - `[BACKTEST] completed=1 failed=0 requested=1`.
  - No `FEATURE_COUNT_SUPPORTED=FAIL`, `NoClassDefFoundError`, or Java/Python exception lines remained in the final wrapper dry-run.
- Runtime support fix from the dry-run finding: `AiPredictor.java` now treats `expectedFeatureCount == 50` as supported. Before this fix, the exported `long_micro_exit_guard_5s.onnx` and `short_micro_exit_guard_5s.onnx` loaded and passed manifest validation, but the generic predictor init log still printed a false non-fatal `FEATURE_COUNT_SUPPORTED=FAIL` because the supported-count allowlist had not yet been updated for the new micro-exit schema.
- Backtest wrapper hardening from the dry-run finding: `scripts/run_databento_historical_ibkr_sim_backtest.sh` now supports `--classpath-file` / `BACKTEST_CLASSPATH_FILE` so the Maven runtime classpath cache can be written somewhere other than `runtime/backtests`. It also falls back to `/Users/FXG06FA/trading-agent-main/runtime/models/<SYMBOL>` when a routing CSV model directory is missing or stale.
- This dry-run validates wiring, artifact loading, feature-count recognition, manifest/schema validation, and dry-run process completion only. Because `--dry-run` emits no historical bars, it is not a realized-R or trade-quality validation.

### Backtest lifecycle/micro telemetry completed — 2026-06-03

- Correction C6 is now implemented in code:
  - `PingPongStrategy.java` exposes optional lifecycle/micro telemetry callbacks for arm, confirm, expiry, lifecycle-exit, micro-guard, hard-risk, and EOD exit events.
  - `DatabentoHistoricalStreamingBacktester.java` prints lifecycle/micro counters in `printSummary()` and writes `trade_lifecycle_summary.csv` via `backtest.tradeLifecycleSummaryFile`.
  - The CSV includes backtest-level counters plus per-closed-trade fields for side, entry/exit time, quantity, entry/exit price, trade PnL, realized R, MFE R, MAE R, setup arm time, setup-to-fill seconds, entry route, exit reason, and entry probability/threshold/margin when available.
  - `scripts/databento_historical_streamer.py` now lazy-loads the Databento SDK so API/DBN `--dry-run` validation can complete even when the SDK is not installed; real non-dry API/DBN reads still require `databento`.
- Validation performed in this workspace:
  - `python3 -m py_compile scripts/databento_historical_streamer.py` passed.
  - `./mvnw -q -DskipTests compile` passed.
  - Focused Java tests passed: `PingPongStrategyBucketFinalizationTest`, `PingPongStrategyFeatureSchemaTest`, and `DatabentoHistoricalReplayProviderTest`.
  - Full Java test suite passed with `./mvnw -q test`; only the existing Mockito/Byte Buddy dynamic-agent warnings and expected simulated gateway logs were observed.
  - Direct Java dry-run to writable `/tmp` outputs passed and produced `trade_lifecycle_summary.csv` with all zero counters because dry-run emits no bars/trades.
- Validation refresh after the wrapper hardening:
  - `bash -n scripts/run_databento_historical_ibkr_sim_backtest.sh` passed.
  - `./mvnw -q test` passed.
  - `python3 -m unittest discover -s tests -p 'test*.py' -v` passed: 5 lifecycle/micro tests ran successfully.
  - Upgraded `TSLA` dry-run through `scripts/run_databento_historical_ibkr_sim_backtest.sh` passed using the project Databento venv auto-detected at `/Users/FXG06FA/trading-agent-main/runtime/databento/python-venv/bin/python3`.
  - Dry-run markers: `FEATURE_COUNT_SUPPORTED=PASS` count `8`, `FEATURE_COUNT_SUPPORTED=FAIL` count `0`, lifecycle/micro route manifest validation count `1`, and `[BACKTEST] completed=1 failed=0 requested=1`.
  - The wrapper wrote an explicit lifecycle summary path beside the trade/order CSVs: `/Users/FXG06FA/trading-agent-main/runtime/tmp_backtest_dry_run/TSLA-2026_05_21-to-2026_05_21-20260603_155515-trade-lifecycle-summary.csv`.
  - The dry-run summary CSV had 35 header columns and 1 zero-counter placeholder row, which is expected because `--dry-run` emits no bars/trades.
- `scripts/run_databento_historical_ibkr_sim_backtest.sh` now resolves the Databento API key source without printing the key: current parent environment first, macOS `launchctl getenv DATABENTO_API_KEY` next, the configured `trading.databento.env-file` / `runtime/databento.env` path used by the live sidecar, `databento_ibkr_bridge/.env` when present for parity with the shared Databento/IBKR startup path, and finally `src/main/resources/application.properties` via `trading.databento.api.key` plus `trading.databento.api.userid`.
- The wrapper also builds a verified Python CA bundle at `/Users/FXG06FA/trading-agent-main/runtime/databento/certs/macos-keychain-ca-bundle.pem` from `certifi` plus macOS keychains and exports `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE`, and `CURL_CA_BUNDLE` for the Databento Python subprocess. This fixed the local non-dry TLS failure `CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain` without disabling certificate verification.
- Non-dry historical validation is no longer blocked by credential availability in this shell. A `TSLA` non-dry historical API replay for `2026-05-21` completed successfully using `databento_api_key_source=application.properties:trading.databento.api.key`:
  - `[BACKTEST] completed=1 failed=0 requested=1`.
  - Equity bars: `9303`.
  - Option bars: `44244`.
  - Skipped events: `0`.
  - `FEATURE_COUNT_SUPPORTED=FAIL` count: `0`.
  - Lifecycle/micro route manifest validation count: `1`.
  - SSL verification errors: `0`.
  - Python tracebacks: `0`.
  - Total trades: `0`; total PnL: `0.0`; `arms_total=0`.
  - `trade_lifecycle_summary.csv` was written at `/Users/FXG06FA/trading-agent-main/runtime/tmp_backtest_non_dry_ca/TSLA-2026_05_21-to-2026_05_21-20260603_161359-trade-lifecycle-summary.csv` with 35 columns and one zero-counter placeholder row. The zero trades/arms result is a strategy/model-threshold outcome for this single-day replay, not a data/API wiring failure.

### Next steps after successful non-dry replay — 2026-06-03

The credential, TLS, Databento API, ONNX loading, manifest validation, and `trade_lifecycle_summary.csv` wiring blockers are cleared. The next work should move from “can the upgraded route run?” to “does the upgraded route produce useful lifecycle trades?”

1. **Immediate secret hygiene**
   - Rotate the Databento API key because a working key is currently present in `src/main/resources/application.properties`.
   - Move the rotated key to a non-checked-in source such as `runtime/databento.env`, macOS Keychain, or `launchctl`.
   - Keep `application.properties` with no key or a placeholder only.
   - Confirm local secret files remain ignored by Git.

2. **Diagnose the zero-trade / zero-arm TSLA replay**
   - Parse `/Users/FXG06FA/trading-agent-main/runtime/tmp_backtest_non_dry_ca/non_dry.log` for the exact blockers:
     - `ENTRY_GATE_OPEN=FAIL`
     - `YESTERDAY_CLOSE_AVAILABLE=FAIL`
     - `AI_PREDICTS_ENTRY`
     - `MICRO_ENTRY_CONFIRMS`
     - `Armed long micro-entry`
     - `Armed short micro-entry`
     - `allowNewEntries=false`
     - model probability vs threshold lines
   - Determine whether no trades were caused by missing previous-session context, entry-window gating, thresholds, model scores, or genuine model abstention.

3. **Fix or improve previous-session context**
   - The successful replay still showed `YESTERDAY_CLOSE_AVAILABLE=FAIL` in the log tail.
   - Preferred validation path: replay at least the prior trading session plus the target session so `previousSessionClose` is available naturally.
   - If needed, add an explicit `backtest.previousClose` injection or historical previous-close loader for focused single-day tests.

4. **Run a wider non-dry validation matrix**
   - Run more than one day and more than one symbol before drawing trade-quality conclusions from `TSLA` on a single day.
   - Suggested first matrix: `TSLA,NVDA,AAPL,SPY,QQQ` over `2026-05-20..2026-05-21`.

   ```zsh
   cd /Users/FXG06FA/trading-agent-main

   TRADING_LIFECYCLE_MICRO_ENABLED=true \
   TRADING_LIFECYCLE_MODEL_DIR=/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523 \
   scripts/run_databento_historical_ibkr_sim_backtest.sh \
     --symbols TSLA,NVDA,AAPL,SPY,QQQ \
     --start 2026-05-20 \
     --end 2026-05-21 \
     --timeout-seconds 600 \
     --max-trades 2000 \
     --output-dir runtime/backtest_validation_lifecycle_micro_20260603 \
     --classpath-file runtime/backtest_validation_lifecycle_micro_20260603/databento_ibkr_sim_backtest_cp.txt
   ```

5. **Create an automated backtest scorecard**
   - Aggregate each run’s `trade_lifecycle_summary.csv`, order history, and console markers into a validation scorecard.
   - Minimum columns:
     - symbol
     - date window
     - equity bars
     - option bars
     - skipped events
     - trades
     - total PnL
     - arms total
     - arm confirmations
     - lifecycle exits
     - guard fires
     - hard-risk exits
     - EOD exits
     - avg MFE R
     - avg MAE R
     - avg setup-to-fill seconds
     - exit reason distribution
   - Suggested output: `runtime/backtest_validation_lifecycle_micro_20260603/summary.csv`.

6. **Run baseline vs upgraded A/B replay**
   - Compare the same symbols/date windows with lifecycle/micro disabled and enabled.
   - Baseline route:

     ```zsh
     TRADING_LIFECYCLE_MICRO_ENABLED=false \
     scripts/run_databento_historical_ibkr_sim_backtest.sh ...
     ```

   - Upgraded route:

     ```zsh
     TRADING_LIFECYCLE_MICRO_ENABLED=true \
     TRADING_LIFECYCLE_MODEL_DIR=/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523 \
     scripts/run_databento_historical_ibkr_sim_backtest.sh ...
     ```

   - Compare trade count, realized R, drawdown, PnL, false arm rate, micro confirmation rate, lifecycle exits, and guard exits.

7. **Add targeted no-trade diagnostics if zero trades continue**
   - Add counters for rejected setup opportunities:
     - 30-second bars evaluated
     - max long setup probability
     - max short setup probability
     - bars blocked by missing previous close
     - bars blocked by variance gate
     - bars blocked by entry window
     - bars blocked by position sync
     - bars blocked by hard-stop cooldown
     - threshold-near-miss counts
   - Emit the top closest-to-threshold setup events so “too conservative” can be distinguished from “blocked by state.”

8. **Keep the current lifecycle/micro bundle integration-only**
   - Continue treating `/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523` as schema/integration-valid but not live-promotable.
   - Reason: the training distribution still contains bootstrap setup/entry score proxy behavior.

9. **Start the P1 live-shaped retraining upgrade**
   - Generate lifecycle/micro training rows from actual walk-forward 30-second model scores and threshold decisions.
   - Replace bootstrap setup/entry proxies with actual setup probability, threshold, threshold margin, arm time, micro confirmation time, and realized fill context.
   - Retrain lifecycle exits and micro-entry/exit-guard models.
   - Export a new non-bootstrap lifecycle/micro bundle and rerun manifest, dry-run, and non-dry validations.

10. **Promotion gate before paper/live**
    - Require multi-day non-dry backtests with real trades in `trade_lifecycle_summary.csv`.
    - Require baseline vs upgraded comparison to show acceptable or improved lifecycle quality.
    - Require no `FEATURE_COUNT_SUPPORTED=FAIL`, no manifest failures, no credential/TLS errors, and no unexplained zero-trade behavior.
    - Require a non-bootstrap, live-shaped lifecycle/micro bundle.
    - Require Databento credentials to be removed from checked-in configuration and rotated.

Recommended immediate action: parse the successful `TSLA` non-dry log to explain the zero-trade/zero-arm result, then run a two-day `TSLA` replay including the prior session. If it still produces zero trades, add no-trade gate diagnostics before expanding to the full multi-symbol validation matrix.

### Important follow-up after this implementation and bootstrap export

- The lifecycle/micro feature schema changed and has now been retrained/exported into the bootstrap `lifecycle_micro_20260523` bundle above. Stale lifecycle/micro bundles should fail manifest validation once `strategy.model.upgradedRouteRequired=true` or lifecycle/micro flags are enabled.
- The completed bundle should be treated as schema/integration validation only until a live-shaped walk-forward training distribution replaces the bootstrap `1.0` setup/entry score proxy population.
- The training distribution is still a bootstrap distribution based on label-positive 30-second setup rows. The live-shaped walk-forward model-armed distribution remains the next major P1 item.
- Non-dry historical backtests with real Databento bars and the new `trade_lifecycle_summary.csv` remain required P2 validation before paper/live promotion.

## Project Understanding Snapshot

The project is a multi-cadence AI trading stack that combines Databento market data, Python model training, Java ONNX inference, and IBKR/live or simulated execution.

High-level flow:

```text
Databento EQUS tbbo + OPRA 1s option bars
  -> local 1s/5s/30s training data builders
  -> train_30s_models.py
       exports setup/regime/open30 30-second ONNX models
  -> train_lifecycle_micro_models.py
       exports lifecycle exits + 5-second micro-entry/exit-guard ONNX models
  -> model_exports/<run>/ scorecards + route manifests
  -> run_symbol.sh / runtime properties
  -> PingPongStrategy.java
       30s setup/context, independent 5s micro state, lifecycle exits, hard risk controls
  -> DatabentoHistoricalStreamingBacktester.java for live-shaped replay validation
```

Current code anchors:

- `train_30s_models.py` trains 30-second setup/regime/open30 models. It already has realistic entry label controls such as `ENTRY_FILL_MODE`, `ENTRY_SLIPPAGE_BPS`, `EXIT_SLIPPAGE_BPS`, and calibrated threshold search for the base setup route.
- `train_lifecycle_micro_models.py` trains six upgraded models: long/short lifecycle exits, long/short 5-second micro-entry, and long/short 5-second micro-exit guard. It stages derived datasets to CSV so multi-symbol lifecycle/micro rows do not have to remain in memory.
- `PingPongStrategy.java` maintains independent 5-second micro state alongside the 30-second AI bucket, loads lifecycle/micro ONNX artifacts, and applies them behind `strategy.exit.lifecycleEnabled`, `strategy.micro.entryEnabled`, and `strategy.micro.exitGuardEnabled` flags.
- `run_symbol.sh` treats lifecycle/micro artifacts as required when `TRADING_LIFECYCLE_MICRO_ENABLED=true`, injects upgraded-route flags, and reads model thresholds from `lifecycle_micro_scorecard.csv`.
- `scripts/run_databento_historical_ibkr_sim_backtest.sh` validates the upgraded route through `DatabentoHistoricalStreamingBacktester.java`, which consumes live-shaped Databento NDJSON events and uses a synchronous IBKR simulation broker.
- `scripts/run_lifecycle_micro_training_20260523.sh` is the intended versioned training runner. It now resolves the repository root from `TRADING_AGENT_ROOT` or the Git toplevel and defaults `PYTHON_BIN` to `python3` while preserving overrides.

Important implementation reality as of this review:

- The Java lifecycle/micro feature column lists intentionally mirror the Python row-builder column order, and the upgraded route now validates the manifest feature list plus schema hash before trading.
- `train_lifecycle_micro_models.py` now applies realistic side-aware bid/ask fill handling with configured slippage, falling back to close only when bid/ask data is unavailable.
- The Python micro arm TTL default is now `MICRO_ARM_TTL_SECONDS=30`, matching the Java runtime default `strategy.micro.armTtlSeconds=30`.
- Current lifecycle/micro training uses label-positive 30-second setup events as proxies for armed setups. The more live-shaped target is to generate candidates from walk-forward 30-second model scores and thresholds, including false positives, skipped setups, and expired arms.

## Guiding Decision

Trading decisions should be made by AI models, not by standalone technical-indicator gates. Deterministic logic may still exist for safety, data quality, exchange/session constraints, hard risk controls, and order hygiene, but it should not become a separate directional trading thesis that overrides the model stack.

The upgraded stack should therefore treat indicators and market-state calculations as model features. The model decides whether the combined feature state supports entry, hold, exit, or micro-risk action.

Practical interpretation for this codebase:

- 30-second entry models decide whether a setup is worth arming.
- 5-second micro-entry models decide whether the armed setup has a tradable execution moment.
- Lifecycle exit models decide whether the active trade should be exited on lifecycle cadence.
- 5-second micro-exit guard models detect fast adverse deterioration between lifecycle checks.
- Hard stops, EOD flattening, position limits, stale-data checks, quote validity, max-spread checks, and broker/session constraints remain deterministic because they are safety controls, not alpha theses.
- Optional directional gates such as `strategy.useRsiPreGate=true` should be considered legacy/comparison behavior, not the target AI-only route, unless they are reframed as non-directional safety constraints.

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

Runtime implementation status:

- `PingPongStrategy.java` currently uses the 30-second `askArtificialIntelligence()` pass to choose entry side and either submit directly or call `armMicroEntry(...)` when `strategy.micro.entryEnabled=true`.
- `finalizeIndependent5SecondMicroBucket()` calls `evaluateMicroRoutes(...)`, which evaluates micro-entry only while flat and armed, and evaluates the micro-exit guard only while in position.
- Lifecycle exits are evaluated in the 30-second AI pass through `evaluateLifecycleExitSignal(...)` when `strategy.exit.lifecycleEnabled=true`.
- The runtime now stores the actual 30-second setup probability, selected threshold, and threshold margin for lifecycle/micro features when an entry arms or opens. The proxy feature names remain for schema continuity, but they now carry the stored probability instead of a constant value in live/runtime paths.

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

Enhancement required before promotion:

- The current builder uses label-positive setup bars as the source of arms. That is useful for bootstrapping, but it is not fully live-shaped because runtime arms from model scores and thresholds, not labels known in hindsight.
- The next dataset version should run a walk-forward 30-second setup model over history, persist the score, selected threshold, threshold margin, regime route, and side, and create armed states from those model-accepted setup events.
- Include all live-valid model-accepted arms in audits: profitable arms, unprofitable arms, disarmed setups, and expired setups. Here, “false-positive” means a setup that would have armed live because the 30-second model passed threshold, but whose later outcome did not justify entry; it does not mean injecting setups that would never arm live.
- Do not train the micro-entry model only on hindsight-successful or successfully filled arms. Live only receives armed setups, but the armed population still contains winners, losers, no-fills, disarms, and expirations; the micro-entry model must learn which armed setups deserve execution timing confirmation.
- Align both the TTL value and the TTL start reference across Python and Java. The TTL numeric value must be unified at 30 seconds (see Correction C1 and Decision 9). The TTL start reference is equally important and is currently a separate bug: Python starts the arm window from `ctx["_ts"]` which is the bar **open** time (because the 30s CSV uses `resample(..., label='left')`), while Java starts from approximately `finalizedBucketStart + 30L` which is the bar **close** time. This 30-second offset means training and runtime evaluate entirely different sets of 5-second bars. See Correction C8 in Section 11.2 for the exact fix.

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

Enhancement path:

- Replace the current `entry_price = closes[entry_i]` lifecycle assumption with the same fill model used by the live route. Depending on the rollout mode, this should be one of:
  - direct 30-second entry at next-open or side-aware quote,
  - armed 30-second setup with fill at the selected 5-second micro-entry bar,
  - conservative bid/ask fill plus configurable slippage.
- Build lifecycle rows from actual simulated trade paths, not only from label-positive setup rows. The correct production distribution includes model-accepted entries that fail, entries that never receive micro confirmation, and entries that are skipped by spread/chase/session controls.
- Add route/context features that are known at entry time:
  - `f_entry_prob`,
  - `f_entry_threshold`,
  - `f_entry_threshold_margin`,
  - `f_entry_route_regime_choppy/trend/volatile`,
  - `f_entry_open30_route`,
  - `f_entry_setup_close`,
  - `f_entry_chase_r` or setup-to-fill slippage/chase distance,
  - option-flow missing/stale flags where available.
- Add reason tags for labels and diagnostics, for example `hold_hits_stop`, `hold_gives_back_mfe`, `hold_underperforms_exit_now`, `exit_cuts_winner`, and `ambiguous_tp_sl_collision`.
- Score lifecycle exits by realized R impact: saved loss, preserved winner, cut winner too early, missed adverse move, and net PnL after costs.

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

Enhancement path:

- Candidate generation should be based on walk-forward 30-second model arms, not hindsight-positive labels only.
- Each armed setup should emit all 5-second candidates until one of these terminal states occurs:
  - micro-entry model would confirm and submit,
  - arm expires,
  - chase/spread/stale-data guardrail invalidates the arm,
  - EOD/no-new-entry window begins.
- The label should account for side-aware executable fill:
  - long entry at ask or close plus conservative slippage,
  - short entry at bid or close minus conservative slippage,
  - target/stop evaluated after costs with same event ordering as the 30-second labels.
- The scorecard should separate:
  - candidate-row precision,
  - armed setup conversion rate,
  - average setup-to-fill latency,
  - expired-arm rate,
  - skipped-by-spread/chase rate,
  - realized R of confirmed entries versus direct 30-second entries.
- The model should learn execution timing, not re-decide the entire setup thesis. To support that, keep 30-second setup probability and route metadata as first-class features.

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

Enhancement path:

- The current builder simulates micro-exit guard rows from the first 5-second candidate after an arm. Runtime may enter later, only after `longMicroEntryAi` or `shortMicroEntryAi` confirms. The next builder should chain the same selected micro-entry fill into the micro-exit guard path.
- Current Java micro-exit guard behavior exits when probability crosses threshold. The plan text recommends a conservative guardrail agreement layer; that should be made explicit in code or consciously rejected. If retained, examples include spread blowout, adverse 5-second momentum, reclaim/loss of setup close, or stale quote/option-flow state.
- First rollout should use full-exit labels and actions. Partial reduce can be researched later once trade logs include enough post-entry state to label scale-out value reliably.
- Micro-exit guard thresholds should usually be stricter than lifecycle thresholds because the guard operates on noisier 5-second state and can easily cut winners too early.

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
feature_schema.json
feature_schema.sha256
```

### Completed bootstrap export snapshot — 2026-06-03

The completed full export run wrote the active bootstrap bundle to `/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523` and the streamed staging rows to `/Users/FXG06FA/trading-agent-main/runtime/lifecycle_micro_20260523_staging`.

Run inputs and log:

- 30-second input directory: `/Users/FXG06FA/trading-agent-main/training_data/databento_30s`.
- 5-second input directory: `/Users/FXG06FA/trading-agent-main/training_data/databento_5s_20260523`.
- Completion log: `/Users/FXG06FA/trading-agent-main/runtime/training_logs/lifecycle_micro_retrain_20260603_085941.log`.
- The run completed with 99 matched symbol pairs, six staged datasets, six trained ONNX exports, and scorecard/manifest writes.

Staged dataset sizes:

| Dataset | Rows |
| --- | ---: |
| `long_lifecycle_rows.csv` | 990,000 |
| `short_lifecycle_rows.csv` | 990,000 |
| `long_micro_entry_rows.csv` | 333,506 |
| `short_micro_entry_rows.csv` | 333,304 |
| `long_micro_exit_rows.csv` | 179,472 |
| `short_micro_exit_rows.csv` | 168,792 |

Scorecard summary from `lifecycle_micro_scorecard.csv`:

| Model | Export | Rows | Positives | Threshold | Precision | Recall | Pred-positive rate | Feature count | Feature schema hash |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `longExitLifecycleAi` | `long_exit_lifecycle.onnx` | 750,000 | 239,042 | 0.68 | 97.18% | 57.64% | 19.91% | 34 | `8daf01ed08d106a04a6aefbf4e17b4b64773dcb3d52ad29451e26c3801f78936` |
| `shortExitLifecycleAi` | `short_exit_lifecycle.onnx` | 750,000 | 242,506 | 0.68 | 96.81% | 57.63% | 20.19% | 34 | `8daf01ed08d106a04a6aefbf4e17b4b64773dcb3d52ad29451e26c3801f78936` |
| `longMicroEntryAi` | `long_micro_entry_5s.onnx` | 333,506 | 30,788 | 0.74 | 100.00% | 0.53% | 0.06% | 44 | `9eff9f60ed2e668012b2e5ca89acec5b1a57bb717706ed775b57ab7de2356401` |
| `shortMicroEntryAi` | `short_micro_entry_5s.onnx` | 333,304 | 28,702 | 0.74 | 100.00% | 0.51% | 0.05% | 44 | `9eff9f60ed2e668012b2e5ca89acec5b1a57bb717706ed775b57ab7de2356401` |
| `longMicroExitGuardAi` | `long_micro_exit_guard_5s.onnx` | 179,472 | 61,876 | 0.68 | 99.99% | 59.82% | 21.22% | 50 | `1e93d53a36b0e3a6e73c3342a3f9ec029f063da978fad155386c2d569a459e15` |
| `shortMicroExitGuardAi` | `short_micro_exit_guard_5s.onnx` | 168,792 | 57,001 | 0.74 | 100.00% | 55.68% | 18.47% | 50 | `1e93d53a36b0e3a6e73c3342a3f9ec029f063da978fad155386c2d569a459e15` |

Manifest/schema details:

- `lifecycle_micro_route_manifest.json` contains six route entries with absolute model paths, thresholds, feature counts, feature-column lists, and per-model feature schema hashes.
- `feature_schema.json` contains the same per-model feature column contracts used for Java/Python schema parity checks.
- `feature_schema.sha256` for the full schema file is `e9d15001ee9e7d5f768def2d259576a49ea14573d1d85eb6ed7bd531d678bbd1`.
- The lifecycle models use 34 features, micro-entry models use 44 features, and micro-exit guard models use 50 features.

Promotion caveat:

- This bundle confirms that the updated feature schema can stage, train, export, scorecard, and manifest successfully. It should not be promoted live as-is because the training log emitted the bootstrap proxy warning: `WARN f_setup_score_proxy and f_entry_score_proxy are using bootstrap value 1.0. Models trained with this placeholder must not be promoted to live trading.`

Discussion items for improvement:

- Persist label distribution by symbol, side, model type, hour, regime, and date.
- Persist selected threshold and threshold-search curve per model.
- Store feature columns and schema hashes so Java route validation can fail fast on mismatches.
- Keep missing required upgraded models as route invalidation, not silent fallback.
- Keep staged datasets versioned by run tag so labels can be audited later.

Additional artifacts recommended for every lifecycle/micro run:

```text
training_config.json
feature_schema.json
feature_schema.sha256
label_audit_by_symbol_hour_regime.csv
threshold_search_curves.csv
realized_r_backtest_summary.csv
promotion_manifest.json
```

The current `lifecycle_micro_route_manifest.json` includes model names, paths, thresholds, feature counts, feature columns, and per-model schema hashes. Java now validates the manifest feature columns/counts and schema hashes against the Java-side static lists before enabling the upgraded route.

## 7. Training-to-Runtime Contract

The upgraded route should be treated as a contract between Python training, Java runtime inference, shell launchers, and backtests.

### Required lifecycle/micro artifacts

When the upgraded route is enabled, the runtime and backtest launchers should require:

```text
long_exit_lifecycle.onnx
short_exit_lifecycle.onnx
long_micro_entry_5s.onnx
short_micro_entry_5s.onnx
long_micro_exit_guard_5s.onnx
short_micro_exit_guard_5s.onnx
lifecycle_micro_scorecard.csv
lifecycle_micro_route_manifest.json
```

This is already enforced by `run_symbol.sh` and `scripts/run_databento_historical_ibkr_sim_backtest.sh` when lifecycle/micro mode is enabled.

### Required runtime flags for the target route

```text
-Dstrategy.model.upgradedRouteRequired=true
-Dstrategy.exit.legacy30sEnabled=false
-Dstrategy.exit.lifecycleEnabled=true
-Dstrategy.micro.entryEnabled=true
-Dstrategy.micro.exitGuardEnabled=true
-Dstrategy.lifecycle.modelDir=<lifecycle_micro_model_dir>
-Dstrategy.micro.modelDir=<lifecycle_micro_model_dir>
```

Thresholds should come from `lifecycle_micro_scorecard.csv` or a promoted routing manifest, not from hard-coded defaults.

### Feature schema requirements

Current Java lists:

- `LIFECYCLE_FEATURE_COLUMNS`,
- `MICRO_ENTRY_FEATURE_COLUMNS`,
- `MICRO_EXIT_GUARD_FEATURE_COLUMNS`.

These mirror the current Python row-builder column order:

- sorted `f_30s_` + `f_regime_` features for lifecycle rows,
- sorted `f_5s_` then sorted `f_30s_` + `f_regime_` for micro rows,
- position/setup proxy features appended at the end.

Promotion rule:

1. The route manifest feature list must exactly match Java's expected feature list for each model.
2. The feature count must match the ONNX model input count.
3. Missing required columns must be route-fatal, not silently filled to zero, unless the manifest explicitly declares them optional with a default.

### Missing model policy

Desired policy for live trading:

- Missing 30-second entry/regime route artifacts should block that symbol/route.
- Missing lifecycle exit models should block the upgraded lifecycle route unless an explicit hard-risk-only fallback mode is selected for paper/backtest diagnostics.
- Missing micro-entry or micro-exit guard models should block the full lifecycle/micro route.
- No upgraded route should silently fall back to old generic `long_exit.onnx` or `short_exit.onnx` models.

Current implementation nuance, resolved by Decision 8 in Section 11.1:

- `run_symbol.sh` and the upgraded backtest launcher hard-fail if lifecycle/micro artifacts are absent.
- `PingPongStrategy.java` invalidates the upgraded route for missing non-lifecycle upgraded models when `strategy.model.upgradedRouteRequired=true`, but lifecycle-missing behavior currently logs an error and relies on hard risk exits. Section 11.1 Decision 8 resolves this: live deployment must be route-fatal for new entries unless an explicit diagnostic fallback flag is enabled for paper/backtest only.

## 8. Validation, Backtesting, and Open Decisions

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

Backtest mode matrix:

1. **Legacy benchmark:** direct 30-second entry plus explicitly enabled legacy 30-second exits. This is for comparison only.
2. **Lifecycle-only upgrade:** direct 30-second entry plus lifecycle exit model. This isolates the value of position-aware exits.
3. **Armed-entry upgrade:** 30-second setup arms side, 5-second micro-entry confirms, lifecycle exit manages the trade. This isolates execution-timing value.
4. **Full target route:** armed 30-second setup, 5-second micro-entry, lifecycle exit, and 5-second micro-exit guard.

Backtest observability status:

- `DatabentoHistoricalStreamingBacktester.java` now prints lifecycle/micro-specific counters: arms, confirmations, expirations, conversion rate, guard evaluations, guard fires, lifecycle exits, hard-risk exits, EOD exits, average setup-to-fill latency, average MFE/MAE R, and exit reason distribution.
- It also writes `trade_lifecycle_summary.csv` with realized-R trade lifecycle fields for each closed trade; no-trade dry-runs still produce a header plus summary-only row.
- Remaining gap: run non-dry real Databento backtests across enough symbols/days to populate the lifecycle CSV and audit whether AI exits saved losses or cut winners early.

Original open decisions, now answered in Section 11.1:

1. Whether lifecycle rows should use setup close, next open, or modeled bid/ask fill as the entry price.
2. How to persist actual 30-second model probabilities into the lifecycle/micro training frame.
3. Which route metadata should become first-class model features.
4. Whether micro-exit labels should include partial-reduce labels or only full-exit labels in the first version.
5. How strict minimum trade-count constraints should be before promoting thresholds.
6. Whether 1-second features improve 5-second micro models enough to justify additional schema complexity.
7. How to compare direct 30-second entry, armed 5-second entry, lifecycle exit, and micro-exit guard in one scorecard.
8. Whether lifecycle-missing behavior should be route-fatal for live trading or allowed only in an explicit hard-risk-only diagnostic mode.
9. Whether the canonical arm TTL should be 30 seconds, 60 seconds, or route-specific by symbol/regime/time-of-day.
10. Whether threshold calibration should be global first, symbol-specific first, or global with symbol/regime/time-of-day overlays.

## 9. Prioritized Enhancement Backlog

### P0 — Contract and realism fixes before promotion

- [x] Align `MICRO_ARM_TTL_SECONDS` value with `strategy.micro.armTtlSeconds` (Correction C1).
- [x] Fix micro arm window start reference: Python uses bar open time; Java uses bar close time; 30-second offset makes training and runtime evaluate different 5s bars (Correction C8).
- [x] Apply realistic side-aware fill/slippage assumptions in lifecycle and micro row builders.
- [x] Persist actual 30-second setup probability, threshold, threshold margin, and route/audit metadata into lifecycle/micro rows when those columns are available; retain a warned bootstrap fallback for non-promotable smoke runs.
- [x] Add feature schema hash validation from `lifecycle_micro_route_manifest.json` to Java launch/backtest paths.
- [x] Enforce the missing-lifecycle-model route policy from Section 11.1 Decision 8.
- [x] Parameterize or update `scripts/run_lifecycle_micro_training_20260523.sh` so it does not require an older hard-coded worktree root.

### P1 — Live-shaped training distribution

- [ ] Generate lifecycle/micro rows from walk-forward model-armed events, not only hindsight-positive setup labels.
- [ ] Include live-valid model-armed winners and losers, skipped entries, expired arms, spread/chase invalidations, and no-fill states in audits.
- [x] Chain selected micro-entry fill into lifecycle and micro-exit guard simulations.
- [ ] Add label reason tags and realized-R attribution.

### P2 — Validation and observability

- [x] Extend `DatabentoHistoricalStreamingBacktester.java` summary and CSV outputs with lifecycle/micro counters and realized-R trade lifecycle fields.
- [ ] Produce threshold-search curves and realized-R scorecards by symbol, side, hour, regime, and model type.
- [ ] Add promotion checks that require minimum rows, positives, predicted-positive rate, trade count, and out-of-sample realized-R improvement.

### P3 — Research after first stable rollout

- Test whether 1-second AI models add realized-R after costs beyond 5-second micro models.
- Evaluate separate per-regime lifecycle/micro models only after each regime has enough completed trade samples.
- Research partial-reduce exits once full-exit lifecycle and micro-exit guard behavior is stable.

## 10. Working Command Template

For a smoke training run in this workspace after the 30-second and 5-second training CSVs/directories exist:

```zsh
cd /Users/FXG06FA/trading-agent-main
MICRO_ARM_TTL_SECONDS=30 python3 train_lifecycle_micro_models.py \
  --input-30s-csv training_data/databento_30s \
  --input-5s-csv training_data/databento_5s_20260523 \
  --output-dir model_exports/lifecycle_micro_smoke_$(date +%Y%m%d_%H%M%S) \
  --staging-dir runtime/lifecycle_micro_smoke_staging \
  --max-entry-events 200 \
  --max-entry-events-per-symbol-side 25 \
  --max-staged-rows-per-symbol-per-model 5000 \
  --max-train-rows-per-model 25000 \
  --no-onnx
```

Use a full training run only after the label/staging scorecards look sane and the training TTL/fill assumptions match the intended runtime route.

For a full export run using the versioned wrapper and the corrected matched input directories from the completed 2026-06-03 export:

```zsh
cd /Users/FXG06FA/trading-agent-main
rm -rf runtime/lifecycle_micro_20260523_staging model_exports/lifecycle_micro_20260523
mkdir -p runtime/training_logs
LOG="runtime/training_logs/lifecycle_micro_retrain_$(date +%Y%m%d_%H%M%S).log"
INPUT_30S_CSV="$PWD/training_data/databento_30s" \
INPUT_5S_CSV="$PWD/training_data/databento_5s_20260523" \
PYTHONUNBUFFERED=1 \
scripts/run_lifecycle_micro_training_20260523.sh 2>&1 | tee "$LOG"
```

Do not point the full run at `training_data/databento_30s_20260523` unless that directory has per-symbol `*_30s_training.csv` files. During the completed run, that dated 30-second directory contained only `symbol_model_plan.csv`, so the successful run used `training_data/databento_30s` for the matched 30-second files.

For an upgraded historical streaming dry-run/backtest wiring check:

```zsh
cd /Users/FXG06FA/trading-agent-main
DRY_RUN=true \
TRADING_LIFECYCLE_MICRO_ENABLED=true \
TRADING_LIFECYCLE_MODEL_DIR=/Users/FXG06FA/trading-agent-main/model_exports/lifecycle_micro_20260523 \
scripts/run_databento_historical_ibkr_sim_backtest.sh \
  --symbol TSLA \
  --start 2026-05-21 \
  --end 2026-05-21 \
  --timeout-seconds 30 \
  --max-trades 0 \
  --dry-run \
  --python-bin /Users/FXG06FA/trading-agent-main/runtime/databento/python-venv/bin/python3 \
  --output-dir runtime/tmp_backtest_dry_run \
  --classpath-file runtime/tmp_backtest_dry_run/databento_ibkr_sim_backtest_cp.txt
```

The wrapper now prints the classpath cache path, Databento key source label, and explicit `trade_lifecycle_summary.csv` path. It can fall back to `runtime/models/<SYMBOL>` when `runtime/databento/model-routing.csv` contains a stale model directory. Use `--model-dir <dir>` if a specific 30-second model bundle should be forced instead of relying on routing/fallback behavior.

For a non-dry-run backtest, make the same Databento API key used by the live stream available to this shell/launch environment through parent `DATABENTO_API_KEY`, `launchctl setenv DATABENTO_API_KEY ...`, the configured Databento env file, `databento_ibkr_bridge/.env`, or the checked-in `trading.databento.api.key` / `trading.databento.api.userid` properties, then remove `DRY_RUN=true` / `--dry-run`.

---

## 11. Recommended Decisions and Accepted Corrections

This section records concrete decisions on every open item from Section 8 and lists
specific code-level corrections that should be applied before the upgraded route is
promoted. Decisions were confirmed by reading the current source files.

---

### 11.1 Resolved Open Decisions

**Decision 1 — Entry price for lifecycle rows (Section 8 item 1)**

Use side-aware bid/ask fill plus `ENTRY_SLIPPAGE_BPS`, matching the live route:

- Long entry fill: `ask_price * (1 + ENTRY_SLIPPAGE_BPS / 10_000)`.
- Short entry fill: `bid_price * (1 - ENTRY_SLIPPAGE_BPS / 10_000)`.
- Fallback (when per-bar bid/ask is not available in historical CSV): `close * (1 ± ENTRY_SLIPPAGE_BPS / 10_000)`.

Do not use setup-bar close as the final entry price. Setup-bar close consistently
underestimates long fills and overestimates short fills, which inflates both lifecycle
and micro-exit label quality in training relative to what live trading will see.

---

**Decision 2 — How to persist actual 30-second model probabilities (Section 8 item 2)**

The root cause is confirmed in code: `armMicroEntry(String side, Map<String,Float>
contextFeatures, long armEpoch)` at `PingPongStrategy.java:1722` does not accept or
store the probability that triggered the arm. The `prob` value IS computed in scope at
lines 2416 and 2447 but is never forwarded.

Accepted correction (see Section 11.2 for the exact changes):

1. Add field `private double armedSetupProbability = 0.0` to `PingPongStrategy`.
2. Change `armMicroEntry` signature to accept `double setupProb`, store it in
   `armedSetupProbability`, and clear it in `clearMicroEntryArms(...)`.
3. At micro-entry feature build (line 1622): replace `1.0f` with
   `(float) armedSetupProbability`.
4. Add a parallel field `private double positionEntryProbability = 0.0`. Set it at
   fill time from the micro-entry model's own `prob` when `MICRO_ENTRY_ENABLED=true`,
   or from the 30-second entry model's `prob` when `MICRO_ENTRY_ENABLED=false`.
5. At `positionFeatureValues(...)` (line 2155): replace `1.0f` with
   `(float) positionEntryProbability`.

For Python training: add a walk-forward 30-second model inference pass inside
`train_lifecycle_micro_models.py` that loads `long_entry_30s.onnx` /
`short_entry_30s.onnx` from the promoted model directory, scores all candidate setup
bars, and writes `f_setup_prob`, `f_setup_threshold`, and `f_setup_threshold_margin`
columns into every micro-entry and lifecycle row. Until that pass exists, document
`f_setup_score_proxy = 1.0` explicitly as a bootstrap placeholder — do not promote
models that were trained with the proxy into a live account.

---

**Decision 3 — Which route metadata should become first-class model features (Section 8 item 3)**

Include the following as model features (not metadata-only columns) in the next
row-builder version. Add them before the position proxy block so the Java feature
column lists can be updated in one contiguous block:

```text
f_setup_prob               actual 30s model probability at arm time
f_setup_threshold          threshold in effect at arm time
f_setup_threshold_margin   prob - threshold (can be negative for negative decisions)
f_entry_route_regime_0     one-hot: choppy regime
f_entry_route_regime_1     one-hot: trend regime
f_entry_route_regime_2     one-hot: volatile regime
f_entry_open30_route       1.0 if open-30-minute route was active, else 0.0
f_entry_chase_r            (setup_close - fill_price) / risk_distance; captures urgency and slippage
```

Do not add 1-second features to micro-entry or micro-exit guard columns in this
version (see Decision 6).

---

**Decision 4 — Partial-reduce labels (Section 8 item 4)**

Defer partial-reduce labels to P3. First version uses full-exit labels only. Partial
reduce requires enough completed scale-out examples per symbol per side per regime to
produce meaningful out-of-sample precision. That condition is unlikely to be met
before the full-exit lifecycle route has run for at least 60 live trading days.

---

**Decision 5 — Minimum trade-count constraints for threshold promotion (Section 8 item 5)**

Promotion requires, per model per side:

| Metric | Minimum |
|---|---|
| Training rows | 200 (already enforced by `--min-rows`) |
| Positive label count | 30 |
| Out-of-sample predicted-positive rate | ≥ 5 % |
| Completed backtest trades | 50 |
| Backtest realized-R improvement over legacy benchmark | > 0 R/trade |

Below these minimums: model is paper/backtest-only. In that paper mode, use a
conservative high-precision threshold floor of 0.65 regardless of the calibrated
threshold, and emit a `PAPER_ONLY_THRESHOLD_FLOOR_APPLIED` warning in the scorecard.
Add these thresholds explicitly to `promotion_manifest.json` as a
`promotion_requirements` block.

---

**Decision 6 — Whether 1-second features improve 5-second micro models (Section 8 item 6)**

Defer 1-second feature inputs to P3. Do not expand the micro-entry or micro-exit
guard feature schema until the 5-second micro route has at least 90 live trading days
of trade history for at least two symbols. Adding 1-second features before the
5-second route is stable introduces schema churn that invalidates existing trained
models and complicates the training-to-runtime contract.

1-second state may still be used for safety quality checks (stale data detection,
quote validity) in the current version without becoming model input features.

---

**Decision 7 — How to compare all four routes in one scorecard (Section 8 item 7)**

Require `comparison_scorecard.csv` as a mandatory output from every full training run
once the backtester emits the lifecycle/micro counters from Section 8. The file must
have one row per symbol and the following columns:

```text
symbol, backtest_mode, trades, pnl_per_trade_r, sharpe_proxy,
max_drawdown_r, avg_setup_to_fill_latency_s, expired_arm_rate,
arm_conversion_rate, lifecycle_exit_rate, guard_fire_rate,
avg_mfe_r, avg_mae_r, exit_reason_distribution
```

The four `backtest_mode` values must be: `legacy_benchmark`, `lifecycle_only`,
`armed_entry`, and `full_route`. No promotion is accepted unless `full_route` shows
non-negative `pnl_per_trade_r` improvement over `legacy_benchmark` across all
symbols in the promotion bundle.

---

**Decision 8 — Lifecycle-missing behavior for live trading (Section 8 item 8)**

**For live deployment:**

Missing lifecycle exit models when `strategy.exit.lifecycleEnabled=true` is
route-fatal. The strategy must refuse to open new entries. It may continue to manage
existing positions with hard stops only (no lifecycle model calls), but `allowNewEntries`
must be set to `false` and a FATAL log must be emitted at startup.

**For paper trading and historical backtesting:**

A fallback to hard-risk-only exits is permitted only when the launcher explicitly sets
`-Dstrategy.lifecycle.diagnosticFallback=true`. In that mode, the strategy logs a
WARNING that lifecycle exit is running in hard-risk-only fallback and the results
must not be used to justify live promotion.

This means the current behavior (logs error, continues) must be changed to:
- In live mode: abort entry on lifecycle model missing.
- In diagnostic mode (explicit flag required): proceed with hard-risk exits and log
  prominently.

---

**Decision 9 — Canonical arm TTL (Section 8 item 9)**

**Canonical arm TTL is 30 seconds across all contexts.**

The Python default `MICRO_ARM_TTL_SECONDS = 60` is wrong. It must be changed to `30`
to match `strategy.micro.armTtlSeconds` Java default. Environments may override via
the `MICRO_ARM_TTL_SECONDS` env variable, but the default in source code must be 30.

There is a second, independent alignment issue beyond the TTL numeric value: the arm
window **start reference** is not the same in Python and Java. The 30-second training
CSV is built with `resample(..., label='left', closed='left')`, so the `Timestamp`
column records the bar **open** time (e.g., 09:30:00 for the bar that covers
[09:30:00, 09:30:30)). Python uses `start = ctx["_ts"]` = 09:30:00 as the arm start.
Java uses `current30sAiDecisionEpoch ≈ finalizedBucketStart + 30L` = 09:30:30 as the
arm start. This 30-second offset means:

- Python arm window with TTL=30: 5s bars from [09:30:05 … 09:30:30] — INSIDE the setup bar
- Java arm window with TTL=30: 5s bars from [09:30:30 … 09:31:00] — AFTER bar close
- The two windows share at most one bar (09:30:30), and `f_seconds_since_arm` is
  consistently wrong by 30 seconds.

Both issues must be fixed before any model is trained for promotion. See Corrections
C1 and C8 in Section 11.2.

Additionally, to eliminate a ≤1-second jitter between `latestSourceBarEpoch` (last
arriving source bar) and the canonical bar close time, Java should always use
`finalizedBucketStart + 30L` as the arm epoch rather than `latestSourceBarEpoch`.
This makes the Java arm start deterministic and exactly aligned with the Python
correction.

Symbol- or regime-specific TTL overlays are deferred to P3 and must not be introduced
until a minimum of 200 completed armed-entry trades per symbol per TTL value per
regime are available to compare.

If a longer TTL is ever evaluated, training data, backtesting, and live runtime must
all use the same value simultaneously — no partial promotion of a longer-TTL model
into a runtime with a shorter TTL or vice versa.

---

**Decision 10 — Threshold calibration order (Section 8 item 10)**

Calibrate global thresholds first. Symbol-specific threshold overlays are permitted
only when all of the following are satisfied:

1. The symbol has at least 200 completed trades per side on the candidate threshold.
2. The symbol-specific threshold is no lower (less selective) than the global
   threshold, unless the out-of-sample realized-R improvement versus the global
   threshold is at least +0.05 R per trade.
3. The deviation from global threshold is documented in `promotion_manifest.json`
   with a `symbol_threshold_override_justification` field.

Regime and time-of-day threshold overlays are deferred to P3 and require the same
minimum trade counts per bucket before any override is accepted.

---

### 11.2 Accepted Code-Level Corrections

The following changes are required before the upgraded route is promoted. Each item
includes the exact file, the current code, and the required correction.

---

**Correction C1 — Align Python arm TTL default to 30 seconds**

File: `train_lifecycle_micro_models.py`, line 39.

Current:
```python
MICRO_ARM_TTL_SECONDS = int(os.getenv("MICRO_ARM_TTL_SECONDS", "60"))
```

Required:
```python
# Must match strategy.micro.armTtlSeconds Java default (currently 30).
# Override via MICRO_ARM_TTL_SECONDS env var only when intentionally testing
# a non-standard TTL, and only with a matching Java -Dstrategy.micro.armTtlSeconds= flag.
MICRO_ARM_TTL_SECONDS = int(os.getenv("MICRO_ARM_TTL_SECONDS", "30"))
```

---

**Correction C2 — Lifecycle row builder: replace close-fill with side-aware fill**

File: `train_lifecycle_micro_models.py`, line 344.

Current:
```python
entry_price = float(closes[entry_i])
```

Required:
```python
# Use side-aware fill with slippage. Fall back to close if bid/ask unavailable.
if side == "long":
    _raw = float(asks[entry_i]) if asks is not None and not np.isnan(asks[entry_i]) else float(closes[entry_i])
    entry_price = _raw * (1.0 + ENTRY_SLIPPAGE_BPS / 10_000.0)
else:
    _raw = float(bids[entry_i]) if bids is not None and not np.isnan(bids[entry_i]) else float(closes[entry_i])
    entry_price = _raw * (1.0 - ENTRY_SLIPPAGE_BPS / 10_000.0)
```

This requires that `asks` and `bids` arrays are extracted from the 30-second bar
DataFrame alongside `closes` in `build_lifecycle_rows(...)`. The same applies to
`build_micro_rows(...)`.

---

**Correction C3 — Python row builder: replace proxy 1.0 values with documented placeholders**

File: `train_lifecycle_micro_models.py`, lines 348 and 439.

Current (line 348):
```python
entry_features = {"f_entry_score_proxy": 1.0, ...}
```

Current (line 439):
```python
"f_setup_score_proxy": 1.0,
```

Required (both sites):
```python
# BOOTSTRAP PLACEHOLDER — replace with walk-forward 30s model score before promotion.
# Set via the walk-forward scoring pass that loads the current entry ONNX model.
"f_entry_score_proxy": setup_prob if setup_prob is not None else ENTRY_SCORE_PROXY_BOOTSTRAP,
"f_setup_score_proxy": setup_prob if setup_prob is not None else ENTRY_SCORE_PROXY_BOOTSTRAP,
```

Add near the top of the constants block:
```python
ENTRY_SCORE_PROXY_BOOTSTRAP = 1.0   # Remove once walk-forward scoring pass is active.
```

Add a startup warning:
```python
if setup_prob is None:
    logger.warning("f_setup_score_proxy and f_entry_score_proxy are using bootstrap value 1.0. "
                   "Models trained with this placeholder must not be promoted to live trading.")
```

---

**Correction C4 — Java: capture setup probability at arm time**

File: `PingPongStrategy.java`.

Step 1 — Add field near the other micro-entry arm state fields:

```java
private double armedSetupProbability = 0.0;
private double positionEntryProbability = 0.0;
```

Step 2 — Change `armMicroEntry` signature to accept the probability:

Current (line 1722):
```java
private void armMicroEntry(String side, Map<String, Float> contextFeatures, long armEpoch) {
    armed30sFeatureValues = new HashMap<>(contextFeatures);
    microArmEpoch = armEpoch;
    microLongEntryArmed = "long".equalsIgnoreCase(side);
    microShortEntryArmed = "short".equalsIgnoreCase(side);
    ...
}
```

Required:
```java
private void armMicroEntry(String side, Map<String, Float> contextFeatures, long armEpoch, double setupProb) {
    armed30sFeatureValues = new HashMap<>(contextFeatures);
    microArmEpoch = armEpoch;
    armedSetupProbability = setupProb;
    microLongEntryArmed = "long".equalsIgnoreCase(side);
    microShortEntryArmed = "short".equalsIgnoreCase(side);
    ...
}
```

Step 3 — Clear in `clearMicroEntryArms(...)`:
```java
armedSetupProbability = 0.0;
```

Step 4 — Update the two call sites (lines 2430 and 2463) to pass `prob`:
```java
// Line 2430 (long arm):
armMicroEntry("long", lastTraining30sFeatureValues, currentMicroArmEpoch(), prob);

// Line 2463 (short arm):
armMicroEntry("short", lastTraining30sFeatureValues, currentMicroArmEpoch(), prob);
```

Step 5 — Use stored probability in micro-entry feature build (line 1622):
```java
// Before: features.put("f_setup_score_proxy", 1.0f);
features.put("f_setup_score_proxy", (float) armedSetupProbability);
```

Step 6 — Capture fill probability at position open (in the `placeTrade` path for both
direct 30-second entry and confirmed micro-entry). Store in `positionEntryProbability`.

Step 7 — Use in `positionFeatureValues(...)` (line 2155):
```java
// Before: values.put("f_entry_score_proxy", 1.0f);
values.put("f_entry_score_proxy", (float) positionEntryProbability);
```

---

**Correction C5 — Fix training script root path**

File: `scripts/run_lifecycle_micro_training_20260523.sh`, line 4.

Current:
```bash
ROOT="/Users/filmonghezehey/trading-agent/worktrees/databento"
```

Required:
```bash
ROOT="${TRADING_AGENT_ROOT:-$(git -C "$(dirname "$0")" rev-parse --show-toplevel)}"
```

This resolves automatically from the repository root in any clone location. The same
one-line fix applies to every other script in `scripts/` that still has the old
hardcoded path: `check_retrain_20260523_status.sh`, `check_20260523_build_chunks_offload_status.sh`,
`run_databento_compare_training_20260419.sh`, `finalize_databento_compare_20260523.sh`,
`run_parallel_databento_build_20260419.sh`, `continue_20260523_after_finish.sh`,
`apply_databento_compare_20260523.sh`, and `finalize_databento_compare_20260419.sh`.

---

**Correction C6 — Backtest summary: add lifecycle/micro counters**

File: `DatabentoHistoricalStreamingBacktester.java`, `printSummary()` method.

The current summary prints: bars, skipped events, final position, trade count, total PnL,
output file path.

Required additions to the summary output and the trade-lifecycle CSV:

```text
arms_total              count of micro-entry arm events
arms_long / arms_short  by side
arm_confirmations       count of arms confirmed by micro-entry model
arm_expirations         count of arms that expired before confirmation
arm_conversion_rate     confirmations / arms_total
lifecycle_exits         count of AI lifecycle exit decisions
guard_fires             count of micro-exit guard exit decisions
hard_risk_exits         count of hard stop / target exits (not model-driven)
eod_exits               count of EOD flatten exits
avg_setup_to_fill_s     average seconds from arm to confirmed fill
avg_mfe_r               average max favorable excursion in R across all closed trades
avg_mae_r               average max adverse excursion in R across all closed trades
exit_reason_dist        lifecycle / guard / hard_stop / target / eod distribution
```

Add these fields to the existing order history CSV or produce a separate
`trade_lifecycle_summary.csv`. This data is required by the `comparison_scorecard.csv`
described in Decision 7.

---

**Correction C7 — Micro-exit guard row chain must start from confirmed micro-entry fill**

File: `train_lifecycle_micro_models.py`, `build_micro_rows(...)`.

Currently the micro-exit guard simulation starts from the *first* 5-second candidate
after an armed setup, regardless of whether the micro-entry model would confirm at
that bar.

This means:

- Training simulates the guard as if entry happens at bar 0 of the arm window.
- Runtime evaluates the guard only after a confirmed fill (which may be at bar 3
  or bar 5 of the arm window, at a different price).

Required: the micro-exit guard path must use the same simulated entry price and bar
index as the entry that was accepted by the micro-entry label logic in the same
training pass. In practice: if `build_micro_rows` has already computed
`confirmed_fill_price` and `confirmed_fill_bar_i` for a given arm candidate, the
exit guard row builder must start from `confirmed_fill_bar_i`, not from `entry_i`.

---

**Correction C8 — Fix micro arm window start reference: bar open vs bar close**

File: `train_lifecycle_micro_models.py`, `build_micro_rows(...)`, lines 423–427 and
line 440.

Root cause: The 30-second training CSV is produced by `build_30s_from_5s_csv.py`
using:

```python
out = working.resample(cadence, closed='left', label='left').agg(agg_map)
```

With `label='left'`, the `Timestamp` column — and therefore `_ts` — stores the bar
**open time** (left edge of each 30-second bucket). For example, the bar that covers
[09:30:00, 09:30:30) has `Timestamp = 09:30:00`.

Python arm window (current, wrong):
```python
start = ctx["_ts"]     # = 09:30:00  (bar OPEN time)
end_ns = start + TTL   # = 09:30:30  (for TTL=30s)
start_pos = searchsorted(micro_ts_ns, start_ns, side="right")   # first bar after 09:30:00
end_pos   = searchsorted(micro_ts_ns, end_ns,   side="right")   # first bar after 09:30:30
# candidates: 5s bars at [09:30:05, 09:30:10, 09:30:15, 09:30:20, 09:30:25, 09:30:30]
# These are bars INSIDE the 30s setup bar — the model has not closed yet.
```

Java arm window (current, correct):
```java
// current30sAiDecisionEpoch = latestSourceBarEpoch ≈ 09:30:29
//   OR  finalizedBucketStart + 30L = 09:30:00 + 30 = 09:30:30  (bar CLOSE time)
// secondsSinceArm = microBar.epoch() - microArmEpoch
// expires when secondsSinceArm > MICRO_ARM_TTL_SECONDS
// candidates: 5s bars at [09:30:30, 09:30:35, ..., 09:31:00]  (AFTER bar close)
```

These two windows are offset by exactly one bar duration (30 seconds) and share at
most one bar. The Python training set is entirely wrong: it trains on bars that Java
will never arm, and Java evaluates bars that Python never trained on.

`f_seconds_since_arm` is also wrong because it is measured from `start` (bar open)
instead of from bar close. A Python row with `f_seconds_since_arm = 5` maps to a bar
Java would call `f_seconds_since_arm = -25` (before the arm even exists).

Required fix:

```python
# In build_micro_rows(...), replace lines 423–427 and 440:

# Arm starts at bar CLOSE time (bar_open + 30s), matching Java's arm epoch.
arm_start = ctx["_ts"] + pd.Timedelta(seconds=30)
start_ns = int(arm_start.tz_convert("UTC").value)
end_ns   = int((arm_start + pd.Timedelta(seconds=MICRO_ARM_TTL_SECONDS)).tz_convert("UTC").value)

# Use side="left" so the bar exactly at arm_start is included (Java secondsSinceArm=0).
start_pos = int(np.searchsorted(micro_ts_ns, start_ns, side="left"))
# Use side="right" so the bar at arm_start + TTL is included (Java secondsSinceArm=TTL,
# which is NOT > TTL, so it IS included in Java).
end_pos   = int(np.searchsorted(micro_ts_ns, end_ns, side="right"))
candidate_idx = range(start_pos, end_pos)
# Corrected candidates: [09:30:30, 09:30:35, 09:30:40, 09:30:45, 09:30:50, 09:30:55, 09:31:00]

# Line 440 — measure f_seconds_since_arm from bar close time, not bar open time:
"f_seconds_since_arm": (micro.at[mi, "_ts"] - arm_start).total_seconds(),
```

After this fix:

- `f_seconds_since_arm` range is [0, TTL] = [0, 30], matching Java's range.
- Label outcomes shift forward by one full bar, which will change positive label
  rates. Re-run smoke training to observe the distribution change before doing a
  full training rebuild.

Recommended Java hardening (minor, reduces ≤1s timing jitter from live 1s bars):

In `PingPongStrategy.java`, line 1783, change:

```java
// Current (can use latestSourceBarEpoch which may be up to 1s before bar close):
current30sAiDecisionEpoch = latestSourceBarEpoch > 0L ? latestSourceBarEpoch : finalizedBucketStart + 30L;
```

Required (always use canonical bar close time for deterministic alignment):

```java
// Always use bar start + 30 as the canonical arm epoch.
// This matches Python's arm_start = ctx["_ts"] + pd.Timedelta(seconds=30).
current30sAiDecisionEpoch = finalizedBucketStart + 30L;
```

This makes Java's arm epoch exactly equal to Python's corrected `arm_start` for every
bar, removing the `latestSourceBarEpoch` ambiguity.

---

### 11.3 Disagreements and Alternative Approaches

**A — 30-second TTL may be too tight for slower-moving symbols**

A 30-second TTL means the micro-entry model has at most six 5-second bars to confirm
after a setup arms. For slow, wide-spread symbols, the setup context at bar 6 may
still be valid but the arm has already expired. However, the risk of a longer TTL is
that the 30-second market context at entry diverges from the setup context, which
degrades lifecycle exit model accuracy because position features (especially
`f_setup_prob` and `f_entry_threshold_margin`) will no longer reflect the current
regime.

Alternative: allow per-symbol TTL specified in `lifecycle_micro_route_manifest.json`
as `symbol_arm_ttl_seconds`. Start all symbols at 30 seconds. Increase only for
symbols where the armed-to-confirmed conversion rate falls below 15% across 50+
backtest trades, and only in 15-second increments up to a maximum of 60 seconds.
This is a P3 research item, not a P0 requirement.

**B — Walk-forward score generation adds training pipeline complexity**

Loading and running the 30-second ONNX models inside `train_lifecycle_micro_models.py`
to generate walk-forward setup scores makes the training pipeline dependent on the
current promoted models. This creates a circular dependency: better lifecycle/micro
models depend on better 30-second models, and 30-second model retraining may need to
be followed by a lifecycle/micro retrain.

Alternative: persist the 30-second model probability to the raw training CSV at bar
build time (inside `train_30s_models.py` or the training data builder), so lifecycle
and micro training can consume it as a column without re-running the 30-second model
at lifecycle training time. This is a cleaner separation and is the preferred approach
for the full production pipeline. The walk-forward inference pass inside lifecycle
training is acceptable as an interim bridge until the training data builder emits the
score column directly.

**C — Single global micro-exit guard threshold may fire too early on winning trades**

The current plan reads one probability threshold per model from `lifecycle_micro_scorecard.csv`.
A single threshold for the micro-exit guard can easily cut winners too early when the
guard fires at the first sign of 5-second adverse movement, especially in trend regimes.

Suggestion: the scorecard should include a separate `micro_exit_guard_conservative_threshold`
for each symbol and side that is set at least 0.05 above the calibrated threshold.
Use the conservative threshold in live trading. Use the calibrated threshold only
during backtest comparison. This gives the guard a bias toward holding winners
slightly longer while still protecting against fast adverse deterioration.

**D — RSI pre-gates as legacy behavior**

The current `USE_RSI_PRE_GATES` flag path (lines 2441, 2443) sits inside the same
30-second AI decision block that arms micro-entries. As long as `strategy.useRsiPreGate=true`
(the current default), RSI is acting as a directional gate before the AI model even
runs, which contradicts the guiding decision in Section 2.

The preferred path is `strategy.useRsiPreGate=false` with RSI available as a model
feature. This does not mean removing RSI from the codebase — it means ensuring the
AI model controls the entry decision and RSI informs it as a feature, not blocks it
as a gate. The flag exists precisely to allow this comparison; set it to `false` in
the target backtest modes 3 and 4 (see the Backtest Mode Matrix) so any benchmark
comparison is fair.

---

### 11.4 Recommended Implementation Sequence

The following sequence is designed to produce testable milestones. Each milestone can
be validated in isolation before the next begins.

**Week 1 — Contract fixes (P0, no model retraining required)**

1. Apply Correction C1: change `MICRO_ARM_TTL_SECONDS` default to 30 in Python.
2. Apply Correction C8: fix arm window start from bar open to bar close in
   `build_micro_rows(...)`. Also apply the Java hardening to use
   `finalizedBucketStart + 30L` unconditionally.
3. Apply Correction C5: fix all hardcoded ROOT paths in `scripts/`.
4. Apply Correction C4: add `armedSetupProbability` / `positionEntryProbability`
   fields to Java and thread the arm probability through `armMicroEntry`.
5. Run a smoke training rebuild after C1 and C8. Compare label distributions before
   and after — the positive-label rate and `f_seconds_since_arm` distribution will
   both change. This is expected and required.
6. Confirm that the Java unit tests pass after C4 and the Java C8 hardening.

**Week 2 — Fill realism (P0, requires row rebuild)**

1. Apply Correction C2: side-aware fill in lifecycle and micro row builders.
2. Extract `asks` and `bids` columns from the 30-second training CSV. If they are not
   present, add them to the training data builder before proceeding.
3. Apply Correction C7: chain micro-exit guard start to confirmed micro-entry fill.
4. Run a smoke training rebuild with the corrected fill logic. Compare label
   distributions before and after. Expect slight reduction in positive label rate on
   short-side micro-entry because bid-fill is less favorable than close-fill.

**Week 3 — Walk-forward setup score injection (P0/P1 boundary)**

1. Apply Correction C3: replace proxy 1.0 with documented placeholder + startup warning.
2. Add a walk-forward scoring pass in `train_lifecycle_micro_models.py` that loads
   the current promoted 30-second entry ONNX models and scores all candidate setup
   bars. Write `f_setup_prob`, `f_setup_threshold`, and `f_setup_threshold_margin`
   to the staging CSVs.
3. Update Java `f_setup_score_proxy` and `f_entry_score_proxy` to use stored
   probabilities (Step 5-7 of Correction C4).
4. Run a full training rebuild. Verify that `f_setup_score_proxy` distribution
   in micro-entry staging rows now matches the expected 30-second model score
   distribution rather than being a constant 1.0.

**Week 4 — Observability and validation (P2)**

1. Apply Correction C6: add lifecycle/micro counters and `trade_lifecycle_summary.csv`
   to the historical backtest.
2. Run all four backtest modes for at least two symbols using the corrected models.
3. Produce `comparison_scorecard.csv` for the four modes.
4. Validate that mode 4 (full route) shows non-negative `pnl_per_trade_r` improvement
   over mode 1 (legacy benchmark) before proceeding to paper trading.

**Week 5 — Paper trading and promotion gate**

1. Enable the full target route in paper mode for at least two symbols with
   `strategy.lifecycle.diagnosticFallback=false`.
2. Collect at least 50 completed trades per symbol per side before evaluating
   promotion.
3. Apply promotion requirements from Decision 5. Do not promote any model that
   triggers the `PAPER_ONLY_THRESHOLD_FLOOR_APPLIED` warning.
4. Promote only after the `promotion_manifest.json` `promotion_requirements` block
   is fully satisfied and a second reviewer has signed off on the comparison scorecard.

**P3 backlog (after first stable live rollout)**

- Per-symbol TTL overlays (Decision 9 alternative, see Section 11.3 item A).
- 1-second feature inputs to micro models (Decision 6).
- Partial-reduce exit labels (Decision 4).
- Per-regime threshold overlays (Decision 10).
- Separate per-regime lifecycle/micro models if per-regime trade counts support it.

---

## 12. Training Scope Recommendation with 10 Months of Stock and Option Data

This section answers the training-scope question directly: with roughly ten months
of stock and option data per symbol, which upgraded models should be trained per
symbol, which should be pooled across symbols, and how much history should be used?

Current implementation note: `train_lifecycle_micro_models.py` already stages rows
per symbol, but when a directory of per-symbol 30s/5s files is provided it loads the
staged rows into six pooled side-specific models:

```text
long_exit_lifecycle.onnx
short_exit_lifecycle.onnx
long_micro_entry_5s.onnx
short_micro_entry_5s.onnx
long_micro_exit_guard_5s.onnx
short_micro_exit_guard_5s.onnx
```

So the current lifecycle/micro script is naturally a **pooled multi-symbol model per
side and task**, not a per-symbol model, unless the script is run separately for one
symbol and the runtime is pointed to a symbol-specific export directory.

### 12.1 Recommended model scope by layer

| Layer | Recommended first production scope | Why |
|---|---|---|
| 30s setup entry | Per symbol, side-specific, using the full 10-month window | Entry behavior depends heavily on symbol-specific volatility, spread, news sensitivity, and option-flow behavior. This matches the current belief that setup models are trained per symbol over the full history. |
| Lifecycle exit | Pooled across symbols, side-specific first (`long_exit_lifecycle`, `short_exit_lifecycle`) | Exit labels are derived from completed trade paths, not raw bars. Even though each trade creates many lifecycle rows, the true independent sample count is the number of trades. Per-symbol lifecycle models will overfit unless each symbol has enough completed trade paths. |
| 5s micro-entry | Pooled across symbols, side-specific first (`long_micro_entry_5s`, `short_micro_entry_5s`) | Micro-entry needs many armed setups, including losers, expirations, and no-fills. A per-symbol micro-entry model can easily learn noise unless each symbol has thousands of arms per side. |
| 5s micro-exit guard | Pooled across symbols, side-specific first (`long_micro_exit_guard_5s`, `short_micro_exit_guard_5s`) | Guard-positive labels should be rare and noisy. Pooling is safer. Use conservative thresholds before considering symbol-specific models. |

Recommended first rollout: keep **per-symbol 30s entry models**, but train the
lifecycle and micro models as **global pooled side-specific models** across all
symbols. Then apply symbol-specific threshold overlays only after there is enough
out-of-sample trade evidence.

### 12.2 Recommended history length

Use all ten months initially, but do not evaluate the model by random row split.
Evaluation and threshold selection must be calendar-time based.

Current code note: `train_lifecycle_micro_models.py` uses `train_test_split_time(...)`,
which is a simple sorted 80/20 split. With ten months of data that roughly acts like
eight months train and two months test, but it does not create a separate calibration
month. Before promotion, replace or wrap this helper with explicit calendar folds so
the threshold is selected on a calibration window and judged on a separate holdout.

Recommended split for the first full lifecycle/micro rebuild:

```text
Months 1-7:  training fit window
Month 8:     threshold calibration / model selection window
Months 9-10: untouched out-of-sample promotion holdout
```

After the model class, features, labels, and thresholds are accepted, the final
export for paper trading may be fit using the full ten-month window, but only if the
promotion decision was made from the untouched month-9/month-10 holdout and then
confirmed in paper trading.

Recommended ongoing retraining cadence:

- **Entry setup models:** retrain weekly or monthly using a rolling ten-month window
  per symbol.
- **Lifecycle exit models:** retrain monthly using a rolling ten-month pooled window.
- **Micro-entry models:** retrain monthly using a rolling six-to-ten-month pooled
  window; keep all recent arms if row caps force sampling.
- **Micro-exit guard models:** retrain monthly using a rolling ten-month pooled
  window, because adverse-exit labels are rarer and need maximum sample size.

If storage and training time allow, keep ten months for every upgraded model. If a
row cap is required, do not use uniform random row sampling only. Preserve:

1. all rows from the most recent two months,
2. all positive labels,
3. a balanced sample of negatives by symbol, side, hour, and regime,
4. a cap per trade/arm so one long trade or one high-volume symbol cannot dominate.

### 12.3 Minimum evidence before per-symbol lifecycle or micro models

Do not train symbol-specific lifecycle/micro ONNX models merely because there are ten
months of raw stock/option bars. The relevant sample is not bars; it is live-shaped
arms and completed trades.

Use these promotion gates before moving from pooled models to per-symbol models:

| Model type | Minimum per symbol and side before per-symbol model |
|---|---|
| Lifecycle exit | ≥ 500 completed live-shaped trades, ≥ 150 exit-positive lifecycle events, and positive OOS realized-R improvement versus pooled model |
| Micro-entry | ≥ 3,000 model-armed setups, ≥ 300 positive micro-entry labels, ≥ 300 expired/no-fill arms, and positive OOS conversion/PnL improvement versus pooled model |
| Micro-exit guard | ≥ 500 completed trades, ≥ 150 guard-positive examples, and lower cut-winner rate than pooled model at equal or better saved-loss rate |

If those counts are not met, keep the ONNX model pooled and use only threshold
overlays. Threshold overlays need less data than separate models and are safer for
symbols with different spreads/liquidity.

### 12.4 Threshold policy before per-symbol models

The preferred progression is:

```text
global pooled model
  -> global threshold
  -> symbol-specific threshold overlay when evidence is sufficient
  -> per-symbol model only when trade/arm counts are large enough
```

For lifecycle and micro models, symbol-specific thresholds are acceptable before
symbol-specific ONNX models if the symbol has at least:

- 200 completed trades for lifecycle threshold overlay,
- 500 armed setups for micro-entry threshold overlay,
- 200 completed post-entry trades for micro-exit guard threshold overlay.

The overlay threshold should usually be **more selective** than the global threshold.
Allow a less selective threshold only if the out-of-sample realized-R improvement is
clear and documented in `promotion_manifest.json`.

### 12.5 Lifecycle model-specific recommendation

For the lifecycle exit model, the first promoted route should use:

```text
Scope:       pooled across all symbols
Side split:  separate long and short lifecycle models
History:     full ten months for dataset generation
Validation:  calendar walk-forward, not random rows
Horizon:     keep current 20 x 30s bars = 10 minutes for first rollout
Rows:        sample by trade, symbol, side, hour, and regime if row caps are needed
```

The key warning: lifecycle rows are correlated. One trade can create many 30-second
rows. Therefore a model with 500,000 lifecycle rows may still have only a few
thousand independent trade paths. Promotion should count completed trades, not only
row count.

Suggested lifecycle row weighting once implemented:

```text
weight_per_row = 1.0 / rows_emitted_by_that_trade
```

This prevents long-duration trades from dominating training simply because they
produce more lifecycle rows.

### 12.6 Micro-entry model-specific recommendation

For micro-entry, the first promoted route should use:

```text
Scope:       pooled across all symbols
Side split:  separate long and short micro-entry models
History:     full ten months of walk-forward model-armed events
Arm TTL:     30 seconds, arm starts at 30s bar close
Rows:        all positive candidates + balanced negatives/expired/no-fill arms
```

Micro-entry should not be trained only on successful setup labels. It must train on
the full live-shaped armed population:

- arms that confirm and win,
- arms that confirm and lose,
- arms that never confirm,
- arms that expire,
- arms invalidated by spread/chase/stale-data controls.

If row caps are required, sample by **armed setup**, not just by candidate row. A
single arm can produce seven candidate 5s rows when TTL is 30 seconds and the
inclusive Java behavior is mirrored. Avoid letting one arm contribute too many near-
duplicate negatives.

### 12.7 Micro-exit guard model-specific recommendation

For micro-exit guard, the first promoted route should use:

```text
Scope:       pooled across all symbols
Side split:  separate long and short guard models
History:     full ten months
Entry path:  only from confirmed micro-entry fills, not first candidate bar
Threshold:   conservative; usually higher than lifecycle threshold
```

Do not train or promote per-symbol micro-exit guard models early. The guard is most
likely to hurt production by cutting winners too early. It should only fire when the
evidence of fast deterioration is strong. Therefore the first live/paper threshold
should favor lower false-positive exit rate even if recall is modest.

### 12.8 Practical final recommendation

For the next real training run with ten months of stock/options data:

1. Keep the 30-second setup model per symbol if that is already how the production
   route is organized.
2. Train lifecycle, micro-entry, and micro-exit guard as six pooled side-specific
   models across all symbols.
3. Use all ten months to build candidate rows, but use calendar walk-forward splits
   for evaluation and threshold selection.
4. Do not create per-symbol lifecycle/micro ONNX models until the minimum completed
   trade/arm counts in Section 12.3 are met.
5. Add symbol-specific threshold overlays before adding symbol-specific models.
6. If row limits are hit, preserve recent data, positives, and symbol/hour/regime
   balance rather than uniformly sampling all rows.
7. Promote only if the full route improves realized R on the newest two months and
   then survives paper trading without excessive expired arms or cut winners.

---

## 13. Feature Gap Analysis for Lifecycle and Micro Models

This section evaluates which features are missing from the current lifecycle, micro-entry,
and micro-exit guard models. Analysis was grounded by reading `COMMON_30S_TRAINING_FEATURE_COLUMNS`
and `COMMON_5S_TRAINING_FEATURE_COLUMNS` in `PingPongStrategy.java`,
`constructTraining30sFeatureValueMap()` and `constructTraining5sFeatureValueMap()` in Java,
`add_common_features()` and `select_feature_columns()` in `train_lifecycle_micro_models.py`,
`constructFeatureValueMap()` (the live entry/exit feature builder in Java), and
the 5s CSV columns produced by `build_30s_from_5s_csv.py`.

### 13.1 Key structural finding

`constructFeatureValueMap()` in `PingPongStrategy.java` (lines 2567–2722) computes a
rich feature map used by the 30-second **entry** models. This map includes RSI, MACD,
ATR, Bollinger Band distances, session high/low distances, signed tape flow, L1
imbalance, and spread z-score. **None of these flow into
`constructTraining30sFeatureValueMap()`**, which feeds lifecycle and micro models.

Similarly, the 5s CSV produced by `build_30s_from_5s_csv.py` includes `L1Imbalance`,
`AtBidVol`, `AtAskVol`, and `ImbalanceStd5s` from Databento TBBO data.
**None of these are computed as `f_5s_*` training features** by `add_common_features()`.

This creates two parallel feature worlds: the entry models see a richer contextual
view than the lifecycle and micro models, despite running on the same bar data.

### 13.2 Current feature sets (confirmed from source)

**`COMMON_30S_TRAINING_FEATURE_COLUMNS` (21 features — shared by all lifecycle/micro models)**

```text
f_30s_body_pct         f_30s_lower_wick_pct   f_30s_upper_wick_pct
f_30s_range_pct        f_30s_ret_1            f_30s_ret_3
f_30s_realized_vol_20  f_30s_rel_volume_20    f_30s_vwap_dist
f_30s_spread_bps       f_30s_time_of_day      f_30s_is_open_hour
f_30s_is_close_hour    f_30s_option_put_delta f_30s_option_call_delta
f_30s_option_put_call_ratio                   f_30s_option_delta_put_call_ratio
f_30s_option_volume_burst
f_regime_choppy        f_regime_trend         f_regime_volatile
```

**`COMMON_5S_TRAINING_FEATURE_COLUMNS` (18 features — micro-entry and micro-exit guard)**

```text
f_5s_body_pct          f_5s_lower_wick_pct    f_5s_upper_wick_pct
f_5s_range_pct         f_5s_ret_1             f_5s_ret_3
f_5s_realized_vol_20   f_5s_rel_volume_20     f_5s_vwap_dist
f_5s_spread_bps        f_5s_time_of_day       f_5s_is_open_hour
f_5s_is_close_hour     f_5s_option_put_delta  f_5s_option_call_delta
f_5s_option_put_call_ratio                    f_5s_option_delta_put_call_ratio
f_5s_option_volume_burst
```

**Lifecycle exit — additional position features**

```text
f_entry_score_proxy  (bootstrap placeholder, not actual probability)
f_entry_side_long    f_entry_side_short    f_pos_side
f_bars_since_entry   f_unrealized_pnl_r    f_mfe_r
f_mae_r              f_target_remaining_r  f_stop_remaining_r
```

**Micro-entry — additional arm features**

```text
f_setup_score_proxy  (bootstrap placeholder)
f_seconds_since_arm
```

**Micro-exit guard — additional position features**

```text
f_pos_side            f_bars_since_entry_5s
f_unrealized_pnl_r    f_mfe_r    f_mae_r
```

### 13.3 Missing features: lifecycle exit model

These features are either already computed in Java's entry-side `constructFeatureValueMap()`
or can be computed from raw 30s CSV columns such as OHLC, spread, bid/ask size, and
trade-at-bid/ask volume. They are **not** currently emitted by
`train_lifecycle_micro_models.py`'s `add_common_features()` as `f_30s_*` features, and
they are **not** included in Java's `COMMON_30S_TRAINING_FEATURE_COLUMNS` for
lifecycle/micro inference.

**Priority 1 — High impact, direct Java support exists, compute in Python too**

| Feature | Description | Why important for lifecycle exit |
|---|---|---|
| `f_30s_rsi` | 14-bar Wilder RSI | RSI > 70 while long = momentum may be exhausted; exit signal stronger. Java has `currentRsi` computed but not passed to training feature map. |
| `f_30s_dist_high` | `(sessionHigh - close) / close` | Distance from session high. Long position near session high is a classic exit signal. Java computes `f_dist_high`. |
| `f_30s_dist_low` | `(close - sessionLow) / close` | Distance from session low. Short near session low = exit signal. Java computes `f_dist_low`. |
| `f_30s_signed_flow` | `(atAskVol - atBidVol) / total` | Directional tape flow. Selling pressure rising while long = adverse. Java computes `f_signed_flow_30s`; CSV has `AtAskVol`/`AtBidVol`. |
| `f_30s_l1_imbalance` | `(bidSize - askSize) / total` | L1 order book imbalance. Ask side dominant while long = bearish pressure. Java computes `f_l1_imbalance`; CSV has `L1Imbalance`. |
| `f_30s_atr_norm` | `ATR(12) / close` | Normalized volatility. In high-ATR bar: exit may be timely before next adverse swing. Java computes `f_atr_norm`. |

**Priority 2 — Medium impact, compute in Python from CSV columns**

| Feature | Description | Why useful |
|---|---|---|
| `f_30s_bb_upper_dist` | `(bbUpper - close) / close` | Distance to upper Bollinger Band. Close to upper BB while long = extended, exit signal. |
| `f_30s_bb_lower_dist` | `(close - bbLower) / close` | Distance to lower BB. Close to lower BB while short = extended, exit signal. |
| `f_30s_realized_vol_z` | Realized vol z-score (vol vs 100-bar mean/std) | Is current volatility regime elevated vs historical norm? Java computes `f_realized_vol_z` but not in training map. |
| `f_30s_spread_z` | Spread z-score (spread vs rolling mean/std) | Wide spread relative to norm = harder to exit. Java computes `f_spread_z`. |
| `f_30s_is_new_high` | 1.0 if `close >= sessionHigh` | New session high while long = momentum in direction, hold signal. |
| `f_30s_is_new_low` | 1.0 if `close <= sessionLow` | New session low while short = momentum in direction, hold signal. |
| `f_30s_dist_swing_high` | `(swingHigh - close) / close` | Distance to recent swing high. Java computes `f_dist_swing_high`. |
| `f_30s_dist_swing_low` | `(close - swingLow) / close` | Distance to recent swing low. |

**Priority 3 — Requires new state tracking at entry time**

These features require storing bar state at entry time and comparing to current bar.

| Feature | Description | Why useful |
|---|---|---|
| `f_entry_hour` | `time_of_day` at the bar when the trade was entered | Morning entry vs midday entry have very different exit profiles. |
| `f_entry_regime_choppy/trend/volatile` | Regime one-hots at entry time | Is the current regime the same as entry regime? Regime change is an exit signal. |
| `f_regime_changed_since_entry` | 1.0 if current regime != entry regime | Regime flip (trend to choppy, or choppy to volatile) is a strong exit prompt. |
| `f_vol_change_since_entry` | `f_30s_realized_vol_20` / `realized_vol_at_entry` | Has volatility expanded or contracted since entry? Expansion while losing = exit. |

### 13.4 Missing features: micro-entry model

**Priority 1 — High impact, directly addressable**

| Feature | Description | Why important for micro-entry |
|---|---|---|
| `f_5s_l1_imbalance` | `(bidSizeLast - askSizeLast) / total` | At 5s resolution: bid size > ask size is a favorable execution moment for a long arm. Available in CSV (`L1Imbalance`, `BidSizeLast`, `AskSizeLast`). Must be added to Python `add_common_features()` and Java `constructTraining5sFeatureValueMap()`. |
| `f_5s_signed_flow` | `(AtAskVol - AtBidVol) / total` | Directional tape flow at 5s. Buying pressure at the execution moment favors long micro-entry. Available in CSV (`AtAskVol`, `AtBidVol`). |
| `f_price_drift_from_arm_r` | `(micro_close - setup_close) / (setup_close * ENTRY_RISK_PCT)` | How far has price moved from setup close since the arm was created, in R units. If drift > 0.5R against the trade, the execution window may have passed. Requires storing setup-bar close at arm time in both Python (already have `ctx["Close"]`) and Java (store `lastTraining30sClose` at arm time). |
| `f_30s_rsi` | RSI at arm time | If armed long when RSI is 35, the rebound setup is valid. If RSI already at 65, micro-entry at any 5s bar has lower expected value. Currently missing from `COMMON_30S_TRAINING_FEATURE_COLUMNS`. |
| `f_30s_signed_flow` | Directional tape flow at the 30s setup bar | Was there buying pressure when the arm was created? Stored in `armed30sFeatureValues` but only if added to `COMMON_30S_TRAINING_FEATURE_COLUMNS`. |

**Priority 2 — Useful but requires more effort**

| Feature | Description | Why useful |
|---|---|---|
| `f_30s_dist_high` / `f_30s_dist_low` | Session extremes at arm time | Long arm near session high is more aggressive and may not convert. |
| `f_30s_atr_norm` | ATR at arm time | Wide ATR setup = more time pressure on entry timing. |
| `f_5s_quote_age_ms_norm` | Normalized quote age at 5s bar | Stale quotes (high `QuoteAgeMs`) = uncertain executable price. Normalize by dividing by 5000 (5 seconds). |
| `f_5s_imbalance_std` | L1 imbalance volatility | `ImbalanceStd5s` from CSV. High imbalance volatility = unstable book. |

### 13.5 Missing features: micro-exit guard model

**Priority 1 — Easy to add immediately**

| Feature | Description | Why important for micro-exit guard |
|---|---|---|
| `f_entry_side_long` | 1.0 if position is long, 0 otherwise | **Currently missing from `MICRO_EXIT_GUARD_FEATURE_COLUMNS`**, even though `positionFeatureValues()` already emits the value at runtime. Lifecycle model has this but guard does not. Guard should behave asymmetrically: bearish L1 pressure is more actionable for a long exit vs a short exit. Add this to Java's guard feature column list and to the Python micro-exit row builder. |
| `f_entry_side_short` | 1.0 if position is short, 0 otherwise | Same as above. |
| `f_5s_l1_imbalance` | L1 book imbalance at guard evaluation | Ask side dominant while long = bearish pressure, guard should lean toward exit. |
| `f_5s_signed_flow` | Directional tape at 5s guard bar | Heavy selling while long = adverse. |

**Priority 2 — New computed features**

| Feature | Description | Why useful |
|---|---|---|
| `f_unrealized_r_change_5s` | `current_unrealized_r - previous_5s_unrealized_r` | Speed of deterioration. A fast drop from -0.2R to -0.5R in one 5s bar is more urgent than slow drift. Requires tracking previous-bar unrealized R in both Python exit-guard row builder and Java `positionFeatureValues()`. |
| `f_30s_rsi` | RSI at current lifecycle bar (from `COMMON_30S_TRAINING_FEATURE_COLUMNS`) | Falling RSI while long = momentum deteriorating. Already available in `lastTraining30sFeatureValues` once `f_30s_rsi` is added to common 30s training columns. |
| `f_spread_vs_entry_spread` | `current_spread_bps / entry_spread_bps` | Has spread expanded since entry? Wide spread = harder to exit favorably. Ratio > 2.0 is an adverse quality signal. |
| `f_5s_option_flow_reversal` | `current_5s_put_delta / arm_bar_put_delta` | Is put flow accelerating versus the setup bar? Large increase in put buying while long is an adverse signal. |

### 13.6 Addition feasibility

Each addition requires synchronized changes: Python `add_common_features()` (or row
builder), Java `constructTraining30sFeatureValueMap()` / `constructTraining5sFeatureValueMap()`,
and Java `COMMON_30S_TRAINING_FEATURE_COLUMNS` / `COMMON_5S_TRAINING_FEATURE_COLUMNS`.
Because Python `select_feature_columns(...)` returns sorted feature columns, the Java
static feature lists must be updated in the **same sorted order** or manifest/schema
validation will fail.

| Group | Python source | Java runtime source | Feasibility |
|---|---|---|---|
| `f_30s_rsi` | Compute rolling Wilder RSI in `add_common_features()` | Add RSI to `constructTraining30sFeatureValueMap()` using existing `avgGain`/`avgLoss` state | **Easy** |
| `f_30s_dist_high/low` | Compute from rolling `dayHigh`/`dayLow` in training CSV | Add to `constructTraining30sFeatureValueMap()` using `dayHigh`/`dayLow` state | **Easy** |
| `f_30s_atr_norm` | Compute rolling ATR in `add_common_features()` | Add to training feature map using existing `atr12` field | **Easy** |
| `f_30s_signed_flow` | Compute from `AtBidVol`/`AtAskVol` in 30s CSV | Add using `currentBarVolAsk`/`currentBarVolBid` state in Java | **Easy** |
| `f_30s_l1_imbalance` | Compute from `L1Imbalance` or `BidSizeLast`/`AskSizeLast` in 30s CSV | Add using `latestBidSize`/`latestAskSize` state in Java | **Easy** |
| `f_30s_bb_upper/lower_dist` | Compute Bollinger Bands from rolling close window in Python | Add using `bbWindow` state in Java | **Medium** |
| `f_30s_realized_vol_z` | Compute vol z-score from rolling vol history | Add using `realizedVolWindow100` state in Java | **Easy** |
| `f_30s_spread_z` | Compute from rolling spread history in Python | Add using `spreadWindow100` state in Java | **Easy** |
| `f_5s_l1_imbalance` | Add to `add_common_features()` using CSV `L1Imbalance` / `BidSizeLast`/`AskSizeLast` | Add to `constructTraining5sFeatureValueMap()` using `latestBidSize`/`latestAskSize` | **Easy** |
| `f_5s_signed_flow` | Add to `add_common_features()` using CSV `AtBidVol`/`AtAskVol` | Add to Java 5s feature builder using `microPrevPutVolume` analog for trade flow | **Medium** |
| `f_price_drift_from_arm_r` | Compute in `build_micro_rows()` from `ctx["Close"]` | Store `lastTraining30sClose` at arm time, compare to `microBar.close()` | **Medium** |
| `f_entry_side_long/short` (guard) | Add to `build_micro_rows()` exit-guard row builder | Already emitted by `positionFeatureValues()`; add to `MICRO_EXIT_GUARD_FEATURE_COLUMNS` in the correct sorted/order-contract position | **Easy** |
| `f_unrealized_r_change_5s` | Track previous-bar unrealized R in `build_micro_rows()` exit-guard | Track `prevUnrealizedR` in `positionFeatureValues()` 5s path | **Medium** |
| Entry-time features (`f_entry_hour`, `f_entry_regime_*`, `f_regime_changed`) | Store entry bar state in lifecycle row builder | Store at `positionEntryEpoch` time in Java | **Medium** |

### 13.7 Recommended addition sequence

Add features in waves so each wave is validated before the next. Each wave requires a
full retrain and backtest validation.

**Wave 1 — Easy adds, all three models benefit (P0/P1)**

Apply to `COMMON_30S_TRAINING_FEATURE_COLUMNS` (and Python + Java simultaneously):

```text
f_30s_rsi               — add to Python add_common_features() + Java constructTraining30sFeatureValueMap()
f_30s_dist_high         — same
f_30s_dist_low          — same
f_30s_atr_norm          — same
f_30s_signed_flow       — add using AtBidVol/AtAskVol in Python, currentBarVolAsk/currentBarVolBid in Java
f_30s_l1_imbalance      — add using L1Imbalance in Python, latestBidSize/latestAskSize in Java
f_30s_realized_vol_z    — Python: rolling std from ret history; Java: realizedVolWindow100
f_30s_spread_z          — Python: rolling spread stats; Java: spreadWindow100
```

Apply to `COMMON_5S_TRAINING_FEATURE_COLUMNS`:

```text
f_5s_l1_imbalance       — Python: L1Imbalance col in 5s CSV; Java: latestBidSize/latestAskSize
```

Add to the Python micro-exit guard row builder and Java `MICRO_EXIT_GUARD_FEATURE_COLUMNS` only:

```text
f_entry_side_long       — already in lifecycle, just mirror to guard
f_entry_side_short      — same
```

**Wave 2 — Medium complexity, focused benefit**

```text
f_5s_signed_flow        — Python: AtBidVol/AtAskVol in 5s CSV; Java: needs new per-5s AtBid/AtAsk accumulators, not option-flow counters
f_price_drift_from_arm_r — Python: ctx["Close"] vs micro close; Java: store lastTraining30sClose at arm time
f_30s_bb_upper_dist/lower_dist — Python: rolling Bollinger; Java: bbWindow already exists
f_30s_is_new_high/low   — Python: rolling session high/low; Java: dayHigh/dayLow already tracked
f_30s_dist_swing_high/low — Python: rolling high/low window; Java: highWindow/lowWindow exist
f_unrealized_r_change_5s  — Python: delta between adjacent exit-guard rows for the same simulated trade; Java: new prev-bar R field reset on every new position
```

**Wave 3 — Requires entry-time state capture**

```text
f_entry_hour            — capture time_of_day at entry, not current bar
f_entry_regime_*        — capture regime one-hots at entry time
f_regime_changed_since_entry — compare current regime to entry regime
f_vol_change_since_entry    — compare current realized_vol_20 to entry-bar value
```

### 13.8 What NOT to add (yet)

Do not add the following before the basic lifecycle/micro route is stable:

- **1-second features** in any model (deferred to P3, see Decision 6).
- **News bar features** (`f_news_*`) in micro-entry or micro-exit guard. These are zeroed at
  backtest time and add schema complexity. Add to lifecycle only after the news pipeline
  reliably delivers non-zero values to both training CSVs and live runtime.
- **Meta-producer features** (`tsm_*`, `seq_*`, `setup_*`) in lifecycle/micro. These are
  valuable but add training dependency on the TimesFM and sequence model pipelines. Only
  add when those pipelines are stable and the 30s CSV builder reliably outputs them with
  non-zero values. The schema contract cost (Python + Java sync required) is high.
- **Live promotion with constant `f_entry_score_proxy`/`f_setup_score_proxy` values.**
  Wave 1 feature ablations can be run in research mode, but no lifecycle/micro model
  should be promoted until Correction C3 and C4 from Section 11.2 replace the constant
  proxy values with real setup/entry probabilities. Otherwise the model cannot learn
  how entry quality interacts with lifecycle and micro state.

---

## 14. Final Whole-Document Review and Overall Recommendation

This final section is the review verdict after revisiting the complete document and
cross-checking the highest-risk claims against the current Python and Java code.

### 14.1 Overall verdict

The document is now a strong design and implementation review note. It correctly
identifies the main reason the lifecycle/micro route should **not** be promoted yet:
the current training distribution is not sufficiently live-shaped. The highest-risk
gaps are not model architecture choices; they are contract, timing, fill, and audit
alignment issues between training, backtest, and Java runtime.

The document should be treated as follows:

- Sections 1–8 explain the intended route and the original open questions.
- Section 11 is the authoritative decision/correction record.
- Section 12 is the authoritative training-scope recommendation.
- Section 13 is the authoritative feature-gap recommendation.
- This Section 14 is the final execution ordering and promotion-readiness summary.

### 14.2 Main corrections already captured

The most important corrections are already present in the document:

1. **TTL value mismatch:** Python default 60s vs Java default 30s.
2. **TTL start mismatch:** Python used 30s bar open; Java arms at bar close.
3. **Fill realism mismatch:** lifecycle/micro labels still use close-like fills.
4. **Proxy score mismatch:** `f_setup_score_proxy` and `f_entry_score_proxy` are constant 1.0.
5. **Training distribution mismatch:** lifecycle/micro rows are still label-positive setup proxies, not walk-forward model-armed events.
6. **Micro-exit chain mismatch:** training starts guard path from first candidate bar, runtime starts from confirmed entry fill.
7. **Feature contract risk:** Java static feature lists and Python feature order must be schema-validated.
8. **Backtest observability gap:** current summaries do not expose enough arm/confirm/exit reason counters.

These are all P0/P1 blockers for live promotion.

### 14.3 Remaining missing item: durable arm/trade identifiers

One item should be added to the implementation backlog before coding begins:

```text
arm_id
trade_path_id
entry_decision_id
label_version
fill_model_version
feature_schema_version
```

Why this matters:

- Lifecycle rows are many rows per trade. Without `trade_path_id`, row weighting and
  leakage checks are difficult.
- Micro-entry rows are many rows per arm. Without `arm_id`, sampling by candidate row
  can let one arm dominate training.
- Backtest and training audits need to join the same arm from 30s setup, micro-entry
  candidates, confirmed fill, lifecycle rows, and micro-exit guard rows.
- If fill assumptions change, `fill_model_version` lets older staged datasets be
  invalidated cleanly.

Recommended backlog insertion:

```text
P0/P1: Add arm_id, trade_path_id, label_version, fill_model_version, and
feature_schema_version to all staged lifecycle/micro CSVs and backtest lifecycle logs.
```

### 14.4 Correct implementation order

Do **not** start by adding more features. Feature expansion is valuable, but it should
come after the route contract is correct. Otherwise the model may learn from the wrong
bar window, wrong fill price, or wrong armed population.

Recommended order:

```text
1. Contract/timing fixes
   - C1: Python TTL default 30s
   - C8: Python arm start = 30s bar close; Java arm epoch = finalizedBucketStart + 30L
   - Feature schema hash/manifest validation

2. Fill and path realism
   - C2: side-aware bid/ask + slippage fills
   - C7: micro-exit starts from confirmed micro-entry fill
   - Add fill_model_version to staged rows

3. Live-shaped training distribution
   - C3/C4: real 30s setup probability and entry probability, not constant proxies
   - Walk-forward model-armed setup generation
   - Include winners, losers, expired arms, no-fills, and invalidations

4. Observability
   - C6: backtest lifecycle/micro counters
   - trade_lifecycle_summary.csv
   - comparison_scorecard.csv for all four route modes

5. Feature Wave 1 from Section 13
   - f_30s_rsi, f_30s_atr_norm, f_30s_dist_high/low
   - f_30s_signed_flow, f_30s_l1_imbalance
   - f_30s_realized_vol_z, f_30s_spread_z
   - f_5s_l1_imbalance
   - f_entry_side_long/short for micro-exit guard schema

6. Full retrain and backtest
   - Ten-month pooled lifecycle/micro training
   - Calendar folds: train/calibration/holdout
   - Full route vs legacy benchmark

7. Paper trading gate
   - No live promotion until promotion_manifest.json satisfies all requirements
```

### 14.5 Final training recommendation

With ten months of stock and option data per symbol:

- Keep **30-second setup entry models per symbol**.
- Train lifecycle and micro models as **six pooled side-specific models** first:
  - long lifecycle,
  - short lifecycle,
  - long micro-entry,
  - short micro-entry,
  - long micro-exit guard,
  - short micro-exit guard.
- Use symbol-specific threshold overlays before creating symbol-specific lifecycle/micro models.
- Create per-symbol lifecycle/micro ONNX models only after the completed trade/arm
  count gates in Section 12.3 are met.

### 14.6 Final feature recommendation

The feature additions in Section 13 are good, but they should be staged carefully.
The first production feature expansion should be Wave 1 only. Do not add 1-second,
news, or meta-producer features to lifecycle/micro until the 5s route is stable and
the schema contract is proven.

The most important near-term feature is not another technical indicator. It is the
actual setup/entry probability and threshold margin:

```text
f_setup_prob
f_setup_threshold
f_setup_threshold_margin
f_entry_prob
f_entry_threshold_margin
```

These features let lifecycle and micro models learn whether a trade came from a
strong or marginal setup. Without them, lifecycle/micro models cannot properly
condition exit and execution timing on setup quality.

### 14.7 Final promotion-readiness status

Current status should be considered:

```text
Research/design ready: YES
Paper-trading ready:   NOT YET (P0 code contract fixed; needs retrain + P1/P2 validation)
Live-trading ready:    NO
```

As of the 2026-06-03 implementation pass, items 1, 2, 3, and 5 below are implemented in code but require a fresh lifecycle/micro retrain/export because the model feature schema changed. The route becomes paper-trading ready only after all of the following are true:

1. TTL value and arm-start reference are fixed and verified in the exported bundle.
2. Fill/slippage modeling is side-aware and consistent in the staged rows used for the exported bundle.
3. Setup/entry score proxies are replaced with actual probability/threshold/margin values in the promoted staged rows.
4. Lifecycle/micro rows are generated from live-shaped walk-forward arms.
5. Schema validation is enforced and the deployed bundle manifest matches Java's expected schema hash.
6. Backtest logs include lifecycle/micro counters and trade lifecycle summaries.

Live promotion should wait until the full route beats the legacy benchmark on the
newest holdout months and then survives a paper-trading period without excessive
expired arms, false guard exits, or cut winners.

---

## 15. Mapping This Project to Common Day-Trading Setup Families

This section maps the trading project to the common setup taxonomy: trend/momentum,
breakout, mean reversion, reversal, scalping, VWAP, gap, order-flow, and option-flow
setups. The important conclusion is that this project is **not** a hand-coded
playbook that says “if ORB then buy” or “if RSI oversold then buy.” It is an
AI-scored setup engine: common day-trading patterns are represented as features,
regimes, labels, and route thresholds, and the model decides which combined state is
worth trading.

### 15.1 Where the project sits overall

The project is best described as:

```text
AI-scored intraday multi-setup engine
  + 30-second setup/context model
  + regime/opening-window routing
  + options/order-flow features
  + 5-second micro-entry timing
  + lifecycle and micro-exit AI risk management
```

It overlaps multiple common day-trading setup families, but it should not try to
trade every named setup independently. The strongest interpretation is:

```text
Primary family:     AI-ranked momentum / pullback / reversal / breakout setup model
Secondary family:   VWAP and opening-range context model
Execution layer:    5-second scalping/microstructure confirmation
Exit layer:         AI lifecycle exit + fast micro-exit guard
```

### 15.2 Explicitly represented setup families

The project already contains features that correspond to the major professional
day-trading families.

| Common setup family | Project representation | Notes |
|---|---|---|
| Trend / momentum | `f_macd_diff`, `f_realized_vol_20`, `f_realized_vol_z`, `f_green_streak`, `f_red_streak`, regime trend probabilities, sequence/meta producer features | The model can learn continuation rather than using a fixed “ride trend” rule. |
| Pullback trading | `f_dist_sma`, `f_dist_vwap`, `f_dist_or_high_atr`, `f_dist_or_low_atr`, `setup_pullback_continuation_prob` | Pullbacks are represented as distance-to-reference features. |
| VWAP plays | `f_dist_vwap`, `f_30s_vwap_dist`, `f_5s_vwap_dist` | Can support VWAP bounce, reclaim, breakdown, and mean reversion, but currently as model features, not named heuristic triggers. |
| Breakouts / ORB | `f_dist_or_high_atr`, `f_dist_or_low_atr`, opening-30 models, `setup_breakout_prob`, high/low distance features | Opening range is explicitly represented. ORB is not a hard-coded buy/sell rule; the model learns when OR distance is useful. |
| Range / mean reversion | Bollinger distance features, RSI, VWAP distance, choppy regime classification | Strongly represented in features, especially for “dip buyer / rip seller” style entries. |
| Reversal / exhaustion | RSI, Bollinger distance, wick/body features, `setup_reversal_prob`, `setup_trend_exhaustion_prob`, `setup_failed_breakout_prob` | Represented as features and meta setup probabilities. |
| Scalping / micro timing | 5-second bars, `f_5s_*` features, micro-entry model, micro-exit guard | This is the micro execution layer, not the primary thesis layer. |
| Gap setups | `f_gap_from_prev_close`, opening-window route | Supports gap-and-go, gap fade, and gap fill as learned contexts. Not hard-coded by setup name. |
| Order-flow / tape | `f_vol_ask_ratio`, `f_vol_bid_ratio`, `f_signed_flow_30s`, `f_l1_imbalance`, `f_30s_option_*`, proposed `f_5s_l1_imbalance` / `f_5s_signed_flow` | Strong project differentiator because Databento TBBO + OPRA data are available. |
| News/catalyst | `f_news_*`, `news_event_*`, `news_alpha_*`, sentiment fields where available | Present in builders/producers, but should be enabled only when training/live/backtest all have reliable non-zero coverage. |
| Options-based signals | `PutVol`, `CallVol`, option put/call ratios, option deltas, option volume burst | A major edge candidate for this project versus pure stock-only setups. |

### 15.3 How the “Core 5” setup list maps

| Core setup | Project status | Explanation |
|---|---|---|
| Opening Range Breakout (ORB) | **Supported as context/features** | Opening range distance and open-window routing exist. The model decides whether ORB-like context is tradable. |
| VWAP Reversal / Bounce | **Supported as features** | VWAP distance exists in both 30s and lifecycle/micro feature sets. No fixed VWAP bounce rule should be added; keep VWAP as model input. |
| Gap and Go | **Partially supported** | `f_gap_from_prev_close` exists in the 30s setup model. It should become explicit lifecycle/micro context later if gap behavior matters post-entry. |
| Momentum Continuation | **Supported** | MACD, realized vol, relative volume, streaks, sequence/meta producer probabilities, and trend regime all represent continuation. |
| Reversal from Extremes | **Supported** | RSI, Bollinger distance, session high/low distance, wick/body features, and reversal/exhaustion meta probabilities support this. |

### 15.4 What this project is not

The project should **not** become a giant manual rule engine containing every setup
name in the day-trading universe. Do not add separate deterministic gates such as:

```text
if ORB then buy
if VWAP reclaim then buy
if RSI < 30 then buy
if bullish engulfing then buy
if L1 imbalance > threshold then buy
```

Those should remain **features** or **model-produced setup probabilities**, not
standalone directional trading rules. The project’s guiding decision is AI-only:
indicators and setup descriptors inform the model; they do not override it.

### 15.5 Best taxonomy label for the current project

The most accurate label is:

```text
AI multi-setup intraday stock/options strategy
with 30-second setup scoring and 5-second execution timing.
```

More specifically:

```text
Entry thesis:
  learned blend of pullback, reversal, breakout, VWAP, momentum, gap, and option-flow contexts

Entry cadence:
  30-second setup model arms the side
  5-second micro-entry model confirms execution timing

Exit thesis:
  lifecycle model decides whether hold-vs-exit is favorable
  micro-exit guard catches fast adverse local deterioration

Risk controls:
  deterministic hard stops, position limits, quote/session safety, and EOD flattening
```

### 15.6 Current naming: “dip buyer” and “rip seller”

The Java logs and model names still use terms like:

```text
LONG ENTRY (Dip Buyer)
SHORT ENTRY (Rip Seller)
```

These names make the strategy sound mostly mean-reversion/reversal. That is only
partly true. Because the feature set includes trend, breakout, VWAP, gap, order-flow,
and option-flow context, the AI model can learn more than simple dip-buying or
rip-selling. A clearer conceptual naming would be:

```text
long_setup_ai
short_setup_ai
long_micro_entry_ai
short_micro_entry_ai
long_lifecycle_exit_ai
short_lifecycle_exit_ai
long_micro_exit_guard_ai
short_micro_exit_guard_ai
```

Renaming is not required before promotion, but future documentation should avoid
implying that the entry model is only a mean-reversion strategy.

### 15.7 How each named setup should be represented

The correct implementation pattern is:

```text
Named setup idea
  -> convert to numeric features / setup-probability features
  -> include in training rows with no lookahead
  -> let model decide if the pattern matters in the current regime
  -> validate realized R impact in backtest
```

Examples:

| Named setup | Preferred project representation |
|---|---|
| ORB | `f_dist_or_high_atr`, `f_dist_or_low_atr`, open-window route, opening range high/low reclaim/fail features |
| VWAP bounce/rejection | `f_dist_vwap`, VWAP slope, reclaim/fail state as feature |
| Gap-and-go/fade | `f_gap_from_prev_close`, open-window route, relative volume, news/catalyst features |
| Breakout + retest | distance to prior range high/low, retest count, hold-above/below feature |
| Momentum continuation | return streak, MACD, relative volume, realized vol, sequence model consensus |
| Reversal from extremes | RSI, Bollinger distance, wick/body shape, failed breakout probability |
| Order book imbalance | `f_l1_imbalance`, `f_5s_l1_imbalance`, quote age, spread z-score |
| Options flow | put/call ratio, option delta bursts, option volume burst, call/put acceleration |

### 15.8 Highest-value setup families for this project

Given the existing data and code, the project should focus on only a small number of
families first:

1. **VWAP / pullback / reversal setups** — already aligned with dip/rip style labels.
2. **Opening range / breakout continuation setups** — already supported by open-window
   features and opening specialized models.
3. **Momentum continuation with option-flow confirmation** — strong fit because the
   project has both stock TBBO and OPRA option volume data.
4. **5-second microstructure confirmation** — high value for reducing bad fills and
   filtering weak arms.

Do not attempt to model every chart pattern separately at first. The professional
lesson from the taxonomy is correct: specialize in a few robust setup families and
measure them deeply rather than adding every named pattern.

### 15.9 Final mapping summary

In the taxonomy from the prompt, this project lies primarily here:

```text
Core categories:
  Trend / Momentum       — yes, as learned features/regime
  Breakout               — yes, especially OR/open-window and range expansion context
  Mean Reversion         — yes, strong current bias through dip/rip and VWAP/RSI/BB context
  Reversal               — yes, as exhaustion/failure/reclaim context
  Scalping               — yes, only at the 5-second micro-entry/exit layer

Major named setups:
  ORB                    — supported as features/open-window routing
  VWAP Bounce/Reversal   — supported as features
  Gap and Go/Fade        — partially supported through gap/open/news/volume context
  Momentum Continuation  — supported through trend/volume/sequence/option-flow features
  Reversal from Extremes — supported through RSI/BB/wick/session-extreme features

Not currently primary:
  manual candlestick-only strategies
  pure Level-2 scalping
  explicit Fibonacci/pivot/order-block/FVG heuristics
  low-float runner specialization
```

The implementation should therefore stay AI-first: expand feature coverage for the
best-supported setup families, but do not turn the project into a manual setup checklist.

