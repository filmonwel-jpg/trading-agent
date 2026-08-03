# Quantitative 1-Second Mean-Reversion Trading Engine

A professional-grade algorithmic trading repository for **high-frequency, 1-second interval** mean-reversion research and execution using **Java**, **Spring Boot**, and the **Interactive Brokers (IBKR) TWS API**.

This project targets microscopic bounce opportunities (roughly **0.05%–0.10%**) via a fast scalping framework built around:
- **14-tick RSI**
- **Relative Volume (RVOL)**
- Highly liquid U.S. equities (for example: **TSLA**, **NVDA**, **SPY**, **SHOP**)

---

## Architecture Overview

For the current Databento market-data + IBKR execution upgrade flow, see the visual diagram in
[`docs/databento-ibkr-data-flow.md`](docs/databento-ibkr-data-flow.md), or open
[`docs/databento-ibkr-data-flow.html`](docs/databento-ibkr-data-flow.html) in a browser if Mermaid appears as code.

### 1) Live Trading Engine (`IBKRTrader.java` + `PingPongStrategy.java`)
The live engine executes the strategy in real time and includes a **Synthetic 1-Second Bar Generator** so live data matches backtest assumptions.

- IBKR live feed provides prices and cumulative volume updates in millisecond cadence.
- `tickSize` (Field 8) is interpreted as **cumulative daily volume**.
- The engine computes clean **1-second volume deltas** from cumulative volume and dispatches them with last price into the strategy.
- This aligns live strategy inputs with your historical 1-second bar format and avoids indicator distortion.

Core risk and safety controls include:
- **Hurricane Lockout** (spread/market quality protection during unstable conditions)
- **Circuit Breaker** (max daily drawdown kill-switch behavior)

### 2) Historical Data Downloader (`IBKR1SecSlowScraper.java`)
The scraper builds clean 1-second historical datasets in a pacing-safe, automated way.

- Accepts symbol dynamically (for example: `NVDA`, `TSLA`)
- Generates output files dynamically as: `SYMBOL_1_Sec_Data.csv`
- Auto-generates trading dates from **January 1st (current year)** through **yesterday**
- Uses chunked historical requests with deliberate pacing pauses to stay compliant with IBKR limits (including the practical limit of ~60 requests per 10 minutes)
- Supports resume/append mode and logs whether each run starts fresh or appends

### 3) Historical Backtester (`HistoricalBacktester.java`)
The backtester is designed to mirror live-engine data shape as closely as possible.

- Symbol can be overridden from command line (or read from properties)
- Auto-resolves CSV filename from active symbol (`SYMBOL_1_Sec_Data.csv`)
- Feeds strategy with equivalent 1-second style inputs
- Enables accurate **Implementation Shortfall** style evaluation between simulated and live execution behavior

---

## Prerequisites

Before running the suite, ensure the following are available:

1. **Java 21+** (JDK)
2. **Maven**
3. **IBKR TWS** or **IB Gateway** running locally (`127.0.0.1`)
4. Active **U.S. Equities live market data subscription** in IBKR
5. IBKR account equity requirement for market data/trading permissions (commonly cited at **$500 USD minimum**, but broker policies can change)

For scheduled morning launches, this repo now includes `./ensure_ibkr_workstation.sh`, which can auto-launch TWS / IB Gateway and wait for the API port before starting the bots. Configure optional launch overrides in `runtime/ibkr-launch.env` (see `runtime/ibkr-launch.env.example`).

For local-only credential handling on macOS, this repo also includes:

- `./set_ibkr_keychain_credentials.sh` → stores IBKR username/password in Keychain
- `./ibkr_keychain_launch.sh` → launches TWS / Gateway and optionally types credentials from Keychain

Typical setup:

```bash
cp runtime/ibkr-keychain.env.example runtime/ibkr-keychain.env
./set_ibkr_keychain_credentials.sh
cp runtime/ibkr-launch.env.example runtime/ibkr-launch.env
```

Then set this in `runtime/ibkr-launch.env`:

```bash
IBKR_LAUNCH_CMD='/Users/filmonghezehey/trading-agent/ibkr_keychain_launch.sh'
```

Important: this keeps secrets out of git, but IBKR 2FA / trusted-device prompts may still require manual approval.

For simple phone confirmations after startup and shutdown, the repo also includes `./send_stack_notification.sh`. Configure `runtime/notifications.env` (see `runtime/notifications.env.example`) with either a local webhook URL or a local `ntfy` topic URL.

For a simple mobile-friendly remote status page, any running bot now serves:

- `GET /api/stack/overview` → aggregated JSON stack summary
- `/mobile-status.html` → responsive read-only status page

The status page is now an installable PWA with:

- home-screen install support
- app icons / Apple touch icon
- cached offline snapshot behavior
- remembered dark/light mode
- remembered auto-refresh interval
- cards for the last successful startup and shutdown

Recommended remote-access pattern: expose only one bot port (for example `8081`) through a secure tunnel or private network tool such as Tailscale or Cloudflare Tunnel, rather than opening all bot ports directly to the internet.

---

## Workflow Pipeline (End-to-End)

### Step 1 — Download Historical 1-Second Data
Run the scraper for each symbol you want to trade/research.

```powershell
mvn spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.IBKR1SecSlowScraper' '-Dspring-boot.run.arguments="NVDA"'
mvn spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.IBKR1SecSlowScraper' '-Dspring-boot.run.arguments="TSLA"'
```

This will create/update files like:
- `NVDA_1_Sec_Data.csv`
- `TSLA_1_Sec_Data.csv`

For the 5-second bulk scraper, you can now request a rolling window or an explicit date range:

```powershell
./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.datascrapers.IBKR5SecHistoricalBulkScraper' '-Dspring-boot.run.arguments="NVDA --months=6"'

./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.datascrapers.IBKR5SecHistoricalBulkScraper' '-Dspring-boot.run.arguments="NVDA --start=2025-09-16 --end=2026-03-16"'
```

Supported arguments are `SYMBOL [--months=N] [--start=YYYY-MM-DD] [--end=YYYY-MM-DD]`.

### Step 2 — Run Historical Backtests
Backtest each symbol using the same strategy engine.

```powershell
./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.HistoricalBacktester' '-Dspring-boot.run.arguments="NVDA"'
./mvnw spring-boot:run '-Dspring-boot.run.main-class=com.calgary.fili.trader.bot.HistoricalBacktester' '-Dspring-boot.run.arguments="TSLA"'
```

Use resulting trade logs and PnL outputs to compare strategy behavior versus expected live performance.

---

## Scaling Up: Live Multi-Terminal Matrix

To run multiple live bots simultaneously, open split terminals and launch one process per symbol with unique runtime overrides (application name, client ID, server port, trade log file).

### Filesystem-first ONNX loading

Live runtime now resolves ONNX models in this order:

1. `trading.model.dir/<model-name>.onnx` on the local filesystem
2. bundled classpath resources as a fallback

The default base config uses:

- `trading.model.dir=runtime/models/${trading.symbol}`
- `trading.state.file=runtime/trader-state-${trading.symbol}.properties`
- `trading.log.file=runtime/trades-${trading.symbol}.csv`
- `logging.file.name=runtime/trading-agent-${trading.symbol}.log`

The repo includes dedicated per-symbol runtime override files:

- `runtime/trading-tsla.properties`
- `runtime/trading-nvda.properties`
- `runtime/trading-amd.properties`

Each one sets a unique client ID, server port, request IDs, per-symbol state/log files, and points `trading.model.dir` at `runtime/models/<SYMBOL>`.

Example launches:

```bash
java -jar target/trading-agent-0.0.1-SNAPSHOT.jar \
  --spring.config.additional-location=file:runtime/trading-tsla.properties

java -jar target/trading-agent-0.0.1-SNAPSHOT.jar \
  --spring.config.additional-location=file:runtime/trading-nvda.properties

java -jar target/trading-agent-0.0.1-SNAPSHOT.jar \
  --spring.config.additional-location=file:runtime/trading-amd.properties
```

You can still add one-off CLI overrides on top of the per-symbol file. Example for **TSLA**:

```bash
java -jar target/trading-agent-0.0.1-SNAPSHOT.jar \
  --spring.config.additional-location=file:runtime/trading-tsla.properties \
  --trading.trade-amount=70000 \
  --trading.risk.max-order-notional=100000 2>&1 | tee -a tsla_live_trade_logs1.txt
```

Populate `runtime/models/TSLA`, `runtime/models/NVDA`, and `runtime/models/AMD` with the exported ONNX bundle for each symbol.

To promote an exported bundle into one or more live runtime symbol folders, use `promote_onnx_bundle.py`:

```bash
python3 promote_onnx_bundle.py --latest --symbol TSLA

python3 promote_onnx_bundle.py --run-tag 20260320_181522 --symbol TSLA NVDA AMD --clean

python3 promote_onnx_bundle.py --source-dir model_exports/20260320_181522 --symbol TSLA --dry-run
```

Notes:
- `--latest` picks the newest directory under `model_exports/`.
- `--clean` removes existing `.onnx` files from `runtime/models/<SYMBOL>` before copying the new bundle.
- The helper validates that the bundle contains the expected 21 ONNX files unless you explicitly pass `--allow-partial`.

For day-of-session launch safety, use `run_symbol.sh`. It previews by default and only launches with `--start`:

```bash
./run_symbol.sh TSLA
./run_symbol.sh NVDA --start --tee
./run_symbol.sh AMD --start --max-trades=0
```

The launcher checks:
- `target/trading-agent-0.0.1-SNAPSHOT.jar`
- `runtime/trading-<symbol>.properties`
- `runtime/postgres-local.properties`
- `runtime/models/<SYMBOL>` and ONNX file count
- port conflicts on the configured `server.port`

If symbol-specific `trading.ai.*threshold` values are present in the runtime properties file, the launcher also passes them as explicit CLI args so the live process uses the intended per-symbol gating values.

The live symbol files now also participate in a shared-capital guard through `runtime/shared-capital.properties`. With the current runtime setting of `trading.shared-capital.total-notional=70000`, one symbol taking a ~70k entry will cause the other symbol processes to wait for capital to be released before they can open a new position.

If you need to clear stale shared-capital reservations before the open, use:

```bash
./reset_shared_capital.sh --check
./reset_shared_capital.sh --reset
./reset_shared_capital.sh --reset --force
```

The helper:
- discovers `runtime/trading-*.properties`
- checks reachable bot statuses first
- refuses reset if any reachable bot is non-flat unless `--force` is supplied
- uses `POST /api/control/shared-capital/reset` when a bot is reachable
- falls back to direct file cleanup when no bot is running yet

For bulk shutdowns, use `stop_all_databento_bots.sh`:

```bash
./stop_all_databento_bots.sh --dry-run
./stop_all_databento_bots.sh --symbols=TSLA --copy-live-logs-on-stop
```

Notes:
- log copying during stop is **disabled by default**
- pass `--copy-live-logs-on-stop` when you want a pre-stop snapshot of each selected bot's current app log, trade CSV, and mirrored live trade log
- snapshots are written under `runtime/databento/snapshots/<date>_pre-stop_<timestamp>/`
- each snapshot run also writes a `manifest.txt` describing the copied files

To move a snapshot folder to the external Databento vault and leave a local symlink behind, use:

```bash
./move_snapshot_to_databento_disk.sh --dry-run
./move_snapshot_to_databento_disk.sh
./move_snapshot_to_databento_disk.sh --all
./move_snapshot_to_databento_disk.sh 2026-05-28_pre-stop_20260528T131819
```

When no folder name is supplied, the helper selects the latest local dated folder under `runtime/databento/snapshots`. Use `--all` to offload every remaining local dated snapshot folder. After the external copy is verified and the symlink is created, the original local directory is deleted to free internal disk space. By default it writes to `/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/databento/snapshots`; override with `DATABENTO_VAULT`, `--vault`, or `--dest-base` if needed.

For repo-wide generated-data offload, GitHub push-size checks, and resume steps after accidental external-disk removal, see [`docs/repository_offload_and_github_push.md`](docs/repository_offload_and_github_push.md).

For an after-market position-only flatten that does **not** stop bots, harvesters, or IBKR, use `after_market_flatten_positions_mt.sh`:

```bash
./after_market_flatten_positions_mt.sh --force-run --dry-run
./after_market_flatten_positions_mt.sh --force-run --symbols=AAPL,NVDA
./after_market_flatten_positions_mt.sh --force-run --mode=bot --symbols=AAPL,NVDA
```

By default it runs only once per weekday inside the 14:05-14:30 `America/Denver` window and uses `flatten_all_via_shared_ibkr_gateway.py --execute` for broker-position flattening. Because this is an after-market workflow, gateway mode submits extended-hours limit orders instead of market orders: short covers use current ask + 1%, and long-position sells use current bid - 1%, with `outsideRth` / after-hours allowed. Override the offset or TIF with `AFTER_MARKET_LIMIT_OFFSET_PCT` and `AFTER_MARKET_LIMIT_TIF` if needed. The companion `com.tradingagent.after-market-flatten-positions-mt.plist` can be loaded with `launchctl` if you want launchd to call the gated script every minute.

See `Monday_Runbook.md` for a concrete pre-open and launch checklist.

Recommended isolation per bot instance:
- Unique `--trading.client-id`
- Unique `--server.port`
- Unique `--trading.log.file`

---

## Disclaimer

This repository is provided for **educational and research purposes only**.

Algorithmic trading and high-frequency execution involve substantial financial, technical, and operational risk, including the risk of significant loss of capital. Nothing in this project constitutes financial advice, investment advice, broker/dealer activity, or a recommendation to buy/sell any security. You are solely responsible for validating strategy behavior, risk controls, infrastructure stability, and regulatory compliance before any live deployment.

---

## 30s Enriched Foundation Pipeline Notes

Purpose: branch `feature/enriched-30s-foundation-features` upgrades `build_30s_from_5s_csv.py` so enriched 5s warmup CSVs from `harvester.py` are aggregated into a training-ready 30s dataset while keeping legacy compatibility.

### 5s -> 30s schema behavior
- Core compatibility fields are preserved: `Timestamp`, `Open`, `High`, `Low`, `Close`, `Volume`, `WAP`, `Count`, `YesterdayClose`.
- Enriched columns are discovered dynamically and aggregated with semantic rules:
  - OHLC: `Open=first`, `High=max`, `Low=min`, `Close=last`.
  - `Volume`/update-count/flow deltas: summed per 30s bucket.
  - Snapshot/state columns (for example `PutVol`, `CallVol`, `ShortableShares`, `BidLast`, `AskLast`): last value in bucket.
  - Window extrema fields (for example `SpreadMinBps5s`, `SpreadMaxBps5s`, `ShortableMin5s`, `ShortableMax5s`): min/max.
  - Rolling-news counters (`NewsCount60s`, `NewsCount300s`, `NewsUniqueProviders300s`): max in bucket.
  - `DataQualityFlags`: union of unique flags observed inside bucket.
  - `Mkt_*` context columns: suffix-aware rules (`_Ret*`/`_SpreadBps`/`_L1Imbalance` -> mean, `_Close5s`/`_AsOfLagSec` -> last, `_NewsCount300s` -> max).
- `WAP` is recomputed as volume-weighted from 5s rows; if bucket volume is zero it falls back to the bucket close.

### Post-aggregation producer outputs (baseline proxies)
`build_30s_from_5s_csv.py` now applies modular producers (see `feature_producers_30s.py`) and emits:
- Time-series foundation: `tsm_ret_30s_p50`, `tsm_ret_120s_p50`, `tsm_ret_30s_p10`, `tsm_ret_30s_p90`, `tsm_up_prob_30s`, `tsm_vol_forecast_120s`, `tsm_uncertainty`
- Regime meta: `regime_trend_prob`, `regime_chop_prob`, `regime_volatile_prob`, `regime_transition_prob`
- Sentiment/event: `news_event_earnings`, `news_event_analyst`, `news_event_legal`, `news_novelty_score`, `news_relevance_score`, `news_embedding_cluster`
- Sequence/setup: `setup_breakout_prob`, `setup_pullback_continuation_prob`, `setup_reversal_prob`, `setup_trend_exhaustion_prob`, `setup_failed_breakout_prob`

These are baseline heuristic proxies intended to keep the pipeline runnable now; replace each producer function with true model-backed inference later while preserving the same output column contract.

### Training integration
- `train_30s_models.py` keeps the Java-compatible base feature set unchanged by default.
- Optional flag `USE_META_PRODUCER_FEATURES` enables the 22 new producer columns for experimentation.
- Missing optional columns are auto-filled with safe numeric defaults so ingestion does not fail.

### Swapping in real model producers later
1. Replace the baseline math inside the corresponding function in `feature_producers_30s.py`.
2. Keep output names and numeric dtypes unchanged.
3. Preserve `apply_all_feature_producers()` registration order and fallback behavior.
4. Re-run builder + trainer sanity checks before enabling meta-features in production experiments.

## Foundation/Sequence/Boosted Model Upgrades

The pipeline now supports all three advanced upgrades behind flags/scripts:

1. TimesFM-driven forecast meta-features.
2. Trained sequence heads (LSTM/TCN/Transformer/PatchTST-lite) exported as meta-feature columns.
3. LightGBM/CatBoost training families with ONNX export in `train_30s_models.py`.

### Install Python dependencies

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-ai.txt
```

### 1) Add TimesFM meta-features

Run proxy mode (always available):

```bash
python3 generate_timesfm_features.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --output-csv TSLA_30Sec_with_timesfm.csv \
  --backend proxy
```

Run real TimesFM inference (if `timesfm` is installed):

```bash
python3 generate_timesfm_features.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --output-csv TSLA_30Sec_with_timesfm.csv \
  --backend timesfm \
  --timesfm-repo google/timesfm-2.0-500m-pytorch
```

Then set:

```bash
export TIMESFM_BACKEND=columns
```

### 2) Train sequence models and emit sequence meta-features

```bash
python3 train_sequence_meta.py \
  --input-csv TSLA_30Sec_with_timesfm.csv \
  --output-csv TSLA_30Sec_with_seq.csv \
  --backend torch \
  --window 64 \
  --epochs 4
```

If torch is unavailable, use:

```bash
python3 train_sequence_meta.py \
  --input-csv TSLA_30Sec_with_timesfm.csv \
  --output-csv TSLA_30Sec_with_seq.csv \
  --backend proxy
```

Then set:

```bash
export SEQUENCE_BACKEND=columns
```

### Regime Ensemble Weighting

When `REGIME_ENSEMBLE_BACKEND=columns|blend`, you can weight regime probability sources:

```bash
export REGIME_SOURCE_WEIGHTS="rf=0.45,lgbm=0.35,catboost=0.20,other=0.10"
```

Blend mode also supports proxy-vs-ensemble mix:

```bash
export REGIME_ENSEMBLE_BACKEND=blend
export REGIME_BLEND_PROXY_WEIGHT=0.35
```

Optional class-specific source weights (override the global map per regime target):

```bash
export REGIME_SOURCE_WEIGHTS_TREND="rf=0.55,lgbm=0.30,catboost=0.15,other=0.10"
export REGIME_SOURCE_WEIGHTS_CHOP="rf=0.30,lgbm=0.45,catboost=0.25,other=0.10"
export REGIME_SOURCE_WEIGHTS_VOLATILE="rf=0.25,lgbm=0.35,catboost=0.40,other=0.10"
```

Auto-tune weights from labeled data:

```bash
python3 tune_regime_ensemble_weights.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --target-col RegimeLabel \
  --trials 3000 \
  --seed 42
```

Quick synthetic smoke check for the tuner:

```bash
python3 smoke_tune_regime_weights.py
```

Notes:
- `REGIME_BLEND_PROXY_WEIGHT=1.0` -> proxy-only in blend mode.
- `REGIME_BLEND_PROXY_WEIGHT=0.0` -> ensemble-only in blend mode.
- Missing/invalid source weights fall back to equal defaults.

### 3) Train LightGBM/CatBoost and export ONNX

`train_30s_models.py` now supports:

- `MODEL_FAMILY=random_forest|lightgbm|catboost`
- `REGIME_MODEL_FAMILY=random_forest|lightgbm|catboost`
- ONNX export for all three families (with optional converter dependencies).

Example:

```bash
export MODEL_FAMILY=lightgbm
export REGIME_MODEL_FAMILY=catboost
python3 train_30s_models.py
```

### Optional end-to-end runner with tuning

`run_meta_pipeline.py` can now optionally tune regime ensemble weights before training, or stop after staged feature generation.

```bash
python3 run_meta_pipeline.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --timesfm-backend proxy \
  --sequence-backend torch \
  --tune-regime-weights \
  --auto-export-best-weights \
  --apply-tuned-weights \
  --tune-target-col RegimeLabel \
  --tune-trials 3000
```

When enabled, files are written under `--work-dir`:
- `regime_weights.json`
- `regime_weights.env`

Fast staging-only run:

```bash
python3 run_meta_pipeline.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --timesfm-backend proxy \
  --sequence-backend proxy \
  --skip-train
```

### Complete Script Runbook (all Python scripts)

Use this section as the operational checklist for every script in this repo.

1) **Collect enriched 5s warmup data** (`harvester.py`)

- **Purpose:** Pull IBKR 5s bars/ticks/news and persist harvested data.
- **Default storage:** PostgreSQL tables `harvest_5s_bars`, `harvest_live_ticks`, and `harvest_news_events` using the existing datasource from `src/main/resources/application.properties` + `runtime/postgres-local.properties`.
- **Optional compatibility mode:** set `HARVEST_STORAGE_MODE=both` to keep PostgreSQL as the source of truth while also writing the old warmup/tick/news CSV mirrors for downstream scripts.

```bash
python3 -m pip install -r requirements-harvester.txt

python3 harvester.py

HARVEST_STORAGE_MODE=both python3 harvester.py
```

2) **Aggregate 5s -> 30s + producer features** (`build_30s_from_5s_csv.py`)

- **Purpose:** Build training-ready 30s CSV from enriched 5s source.
- **Main output:** `TSLA_30Sec_Historical_Bulk_fromTrainer.csv` (or custom path).
- **Compatibility note:** if you still want the legacy CSV harvest artifacts for this step, start `harvester.py` with `HARVEST_STORAGE_MODE=both`.

```bash
python3 build_30s_from_5s_csv.py \
  --input-csv TSLA_5s_warmup_20260306.csv \
  --output-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv
```

3) **Train a supervised news-event model from historical outcomes** (`train_news_event_model.py`)

- **Purpose:** Align historical news to future 30s bar outcomes and train event-level scoring heads.
- **Main outputs:**
  - `news_event_model_outputs/<timestamp>/news_event_dataset.csv`
  - `news_event_model_outputs/<timestamp>/scored_news_events.csv`
  - `news_event_model_outputs/<timestamp>/news_event_model_metrics.json`
  - `news_event_model_outputs/<timestamp>/news_event_model_bundle.pkl`
- **Notes:** `scored_news_events.csv` includes `relevance_score`, `impact_score`, `novelty_score`, `directional_impulse`, `alpha_*`, and `volatility_shock_score`, and can be fed directly into `build_30s_from_5s_csv.py --news-csv`.

```bash
python3 train_news_event_model.py \
  --news-csv path/to/TSLA_news.csv \
  --bars-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv
```

**TODO / Best next step**

- Train `train_news_event_model.py` on your real historical news CSVs.
- Or add a small helper script that:
  1. takes your raw harvested news CSV,
  2. runs `train_news_event_model.py`,
  3. rebuilds 30s bars using the scored event output,
  4. then trains the 30s bar models.

Target one-pass workflow:

```text
raw news -> event model training/scoring -> scored news -> 30s bar rebuild -> 30s model training
```

4) **Generate TimesFM columns** (`generate_timesfm_features.py`)

- **Purpose:** Add `TimesFM_*` columns (real model or proxy fallback).
- **Main output:** stage CSV with TimesFM columns.

```bash
python3 generate_timesfm_features.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --output-csv TSLA_30Sec_with_timesfm.csv \
  --backend timesfm
```

```bash
python3 generate_timesfm_features.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --output-csv TSLA_30Sec_with_timesfm.csv \
  --backend proxy
```

5) **Generate sequence-model columns** (`train_sequence_meta.py` -> `sequence_meta_features.py`)

- **Purpose:** Add `Seq*` columns from torch models or proxy fallback.
- **Main output:** stage CSV with sequence columns.

```bash
python3 train_sequence_meta.py \
  --input-csv TSLA_30Sec_with_timesfm.csv \
  --output-csv TSLA_30Sec_with_seq.csv \
  --backend torch \
  --window 64 \
  --epochs 4
```

```bash
python3 train_sequence_meta.py \
  --input-csv TSLA_30Sec_with_timesfm.csv \
  --output-csv TSLA_30Sec_with_seq.csv \
  --backend proxy
```

6) **Tune regime blend/source weights** (`tune_regime_ensemble_weights.py`)

- **Purpose:** Fit blend + source/per-class weights from labeled data.
- **Main outputs (optional):** `regime_weights.json`, `regime_weights.env`.

```bash
python3 tune_regime_ensemble_weights.py \
  --input-csv TSLA_30Sec_with_seq.csv \
  --target-col RegimeLabel \
  --trials 3000 \
  --seed 42 \
  --json-out regime_weights.json \
  --env-out regime_weights.env
```

7) **Train/export ONNX trade + regime models** (`train_30s_models.py`)

- **Purpose:** Train regime + entry/exit models and export ONNX.
- **Main outputs:** ONNX files under `model_exports/<timestamp>/` and canonical updates in `src/main/resources/`.

```bash
export TIMESFM_BACKEND=columns
export SEQUENCE_BACKEND=columns
export REGIME_ENSEMBLE_BACKEND=blend
export MODEL_FAMILY=lightgbm
export REGIME_MODEL_FAMILY=catboost
python3 train_30s_models.py
```

Promote the newest exported bundle into live runtime directories after training:

```bash
python3 promote_onnx_bundle.py --latest --symbol TSLA --clean
```

Train from a custom file without overwriting the default TSLA dataset:

```bash
python3 train_30s_models.py \
  --input-csv /absolute/path/to/custom_30s.csv
```

If the custom 30s file does not exist yet, the trainer can auto-build it from a chosen 5s source:

```bash
python3 train_30s_models.py \
  --input-csv /absolute/path/to/custom_30s.csv \
  --source-5s-csv /absolute/path/to/custom_5s.csv
```

8) **One-command orchestration** (`run_meta_pipeline.py`)

- **Purpose:** Chain stages (TimesFM -> sequence -> optional tuning -> optional training).

```bash
python3 run_meta_pipeline.py \
  --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv \
  --timesfm-backend proxy \
  --sequence-backend torch \
  --tune-regime-weights \
  --auto-export-best-weights \
  --apply-tuned-weights \
  --tune-target-col RegimeLabel \
  --tune-trials 3000
```

9) **Synthetic tuner smoke test** (`smoke_tune_regime_weights.py`)

- **Purpose:** Quick local validation of tuner behavior without production data.

```bash
python3 smoke_tune_regime_weights.py
```

10) **Databento Core-5 weekly analysis utilities** (`scripts/analyze_core5_weekly_run.py`, `scripts/analyze_databento_core5_weekly_outputs.py`, `scripts/summarize_core5_today_only.py`, `scripts/calculate_core5_today_pnl.py`)

- **Purpose:** Inspect completed Core-5 weekly Databento backtest artifacts, summarize missing outputs/failures, and calculate lifecycle PnL.
- **Runbook:** See [`docs/databento_core5_weekly_analysis_runbook.md`](docs/databento_core5_weekly_analysis_runbook.md).

#### Recommended execution order

```bash
python3 harvester.py
python3 build_30s_from_5s_csv.py --input-csv TSLA_5s_warmup_20260306.csv --output-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv
python3 run_meta_pipeline.py --input-csv TSLA_30Sec_Historical_Bulk_fromTrainer.csv --timesfm-backend proxy --sequence-backend torch --tune-regime-weights --auto-export-best-weights --apply-tuned-weights
```
