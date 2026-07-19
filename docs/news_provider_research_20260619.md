# News Provider Research for AI Training / Backtesting / Live Trading

Date: 2026-06-19
Status: research recommendation / planning input

Related docs:

- `docs/ai_training_dynamic_upgrade_plan_20260612.md`
- `docs/computer_capability_task_organization_20260613.md`

## Executive recommendation

For this project, the best first paid provider to evaluate is **Benzinga News API**, either directly from Benzinga or through a market-data aggregator that exposes Benzinga news such as Polygon/Massive if the required historical archive, latency, and storage rights are included in the plan.

Reason: the current strategy is not generic long-horizon sentiment research. It needs ticker-tagged, trading-oriented headlines for `TSLA`, `NVDA`, `SPY`, `QQQ`, and `TQQQ`, plus timestamps that can be replayed without lookahead. Benzinga is one of the closest API-first fits for real-time trading news, analyst actions, price targets, earnings-related headlines, unusual events, and market-moving single-name stories.

Recommended stack:

1. **Primary trading-news candidate:** Benzinga News API / Benzinga via Polygon or Massive.
2. **Budget secondary / schema-validation candidate:** Finnhub or Marketaux.
3. **Structured event/calendar supplement:** Financial Modeling Prep, plus SEC EDGAR where relevant.
4. **Keep existing live IBKR news path:** use it as a fallback / additional provider-breadth signal, not as the only historical training source.
5. **Institutional upgrade only after ablation proves alpha:** RavenPack, LSEG/Refinitiv News Analytics, Dow Jones, Bloomberg, or MT Newswires.

The immediate action should **not** be a full production integration. First run a 5-symbol, 1-3 month historical sample and one live shadow session through a normalized news adapter, then run a strict no-news versus news ablation with the same gates already used for Databento silver features.

## What the current project already expects

The repo already has a news feature path, but recent full-window work intentionally used no-news mode because the Databento-only datasets had zero-signal news columns.

Relevant implementation surface:

- `harvester.py`
  - Collects IBKR headline/news events.
  - Stores normalized news rows in CSV/PostgreSQL with `published_ts`, `received_ts`, `tradable_ts`, provider, article ID, headline, sentiment, event category, and duplicate metadata.
  - Supports broadtape-style provider subscriptions and fallback historical polling.
- `harvest_storage.py`
  - Persists `harvest_news_events` with `symbol`, `published_ts`, `provider`, `article_id`, `headline`, and raw payload.
- `build_30s_from_5s_csv.py`
  - Accepts `--news-csv` and aggregates only events whose `tradable_dt <= bar_dt`.
  - Produces raw bar-level columns such as `NewsCount300s`, `SentimentLatest`, `NewsEventAnalystUpgradeDowngrade300s`, `NewsRelevanceLatest`, `NewsImpactLatest`, `NewsNoveltyLatest`, `NewsAlpha*`, and `NewsVolatilityShock*`.
- `train_30s_models.py`
  - Converts raw bar news columns into 27 setup-model features under `NEWS_BAR_FEATURE_COLS`.
  - Current important features include intensity, freshness, provider breadth, confidence, sentiment level/shift/dispersion, relevance, surprise, directional impulse, event probabilities, novelty, alpha bias, expected-return normalization, volatility shock, and event strength.
- `train_news_event_model.py`
  - Trains an internal supervised event model from historical news aligned to future 30s bar outcomes.
  - Outputs `scored_news_events.csv`, which can be passed directly to `build_30s_from_5s_csv.py --news-csv`.

Implication: the provider does **not** need to supply every final model feature. The provider needs reliable raw/headline/event data. This project can compute or retrain `relevance_score`, `impact_score`, `novelty_score`, `directional_impulse`, `alpha_*`, and `volatility_shock_score` internally.

## Provider comparison and estimated costs

Pricing below is a planning estimate only. API vendors frequently change pricing and license terms, and many serious news feeds require sales quotes. Before buying, verify: historical archive depth, live latency, allowed storage duration, ML-training rights, backtesting rights, redistribution restrictions, and whether full headline/body text can be retained.

| Provider | Fit for this project | Training/backtest | Live trading | Developer/API quality | Indicative monthly cost | Notes |
|---|---|---:|---:|---:|---:|---|
| **Benzinga News API** | **Best first paid candidate** | High if historical archive is included | High; trading-oriented real-time headlines | High | Usually quote-based; expect hundreds to low-thousands/month for API feeds depending on archive/latency/entitlements | Strong match for ticker news, analyst ratings, price targets, earnings, breaking stock headlines. Verify historical archive export and ML/backtest rights. |
| **Polygon/Massive Benzinga news** | Strong API-first path if Benzinga archive/stream is exposed on the chosen plan | Medium-high | Medium-high | High | Commonly self-serve paid market-data tiers; often roughly low hundreds/month and up, but verify news entitlements | Useful if the goal is a Databento-like developer experience. Confirm whether the plan includes real-time news, historical news, ticker tags, and WebSocket delivery. |
| **TradingNews / trading-oriented headline APIs** | Interesting low-latency candidate | Medium if archive is sold | High if streaming/urgency feed is included | Medium-high | Often quote or tiered pricing; verify directly | Potentially good for event-driven bots if urgency/breaking labels are reliable. Need due diligence on historical depth and licensing. |
| **Intrinio** | Institutional-ish developer API | High for historical datasets | Medium-high depending on feed | High | Often starts around `$150/mo` for individual access; richer feeds can be `$1,250+/mo` or custom | Good for normalized financial datasets and some news. More expensive than budget APIs; less clearly the best live headline source than Benzinga. |
| **Finnhub** | Best budget secondary candidate | Medium | Medium; REST plus some real-time APIs depending on plan | High | Free limited; paid often starts around `$50-$100/mo`; enterprise custom | Good ticker/company news and sentiment endpoints. Useful for schema testing and secondary provider breadth. Verify history depth and rate limits. |
| **Marketaux** | Budget pure-news candidate | Medium | Low-medium; REST-first | High | Free limited; paid commonly around `$29-$199/mo` depending on quota | Lightweight, ticker/entity filtering, sentiment. Good for first adapter tests but not ideal as the only live execution feed. |
| **Alpha Vantage News & Sentiment** | Cheap baseline / AI-feature bootstrap | Medium-low to medium | Low; REST polling, rate-limited | High | Free limited; premium commonly around `$50-$250/mo` | Useful for ticker sentiment/relevance and broad historical experiments. Not the best single source for live 5s/30s trading. |
| **Financial Modeling Prep** | Structured calendar/fundamental/news supplement | High for calendars/transcripts/fundamental events | Medium for real-time endpoints depending on plan | High | Often starts around `$20-$50/mo`; higher tiers around `$100-$300+/mo` | Valuable for earnings calendar, transcripts, SEC-like events, financial statements. Use as structured event supplement, not primary breaking-news feed. |
| **NewsAPI.ai / Event Registry** | Broad NLP/event extraction | Medium-high for global web/event research | Medium | High | Trial/free tiers; paid/custom often needed for volume/history | Good event clustering/entities/topics. Less finance-specialized; good macro/regime source if ticker tagging is strong enough. |
| **APITube News API** | Broad web-news source | Medium for macro/news coverage | Low-medium | Medium-high | Typically budget/self-serve; verify current tiers | Can help macro/regime coverage, but it is not a dedicated trading-news feed. Use only if entity/ticker tagging and storage rights pass due diligence. |
| **Tiingo News** | Low-cost historical/news archive candidate | Medium | Low-medium | High | Often low-cost self-serve tiers | Good budget archive option; verify latency, ticker/entity tags, and sentiment availability. |
| **RavenPack** | Best institutional machine-readable news candidate | Very high | Very high | High, enterprise | Custom; often thousands/month+ annual contract | Excellent if news becomes a proven alpha source. Overkill before a cheaper provider ablation proves value. |
| **LSEG/Refinitiv News Analytics / Reuters** | Institutional machine-readable news | Very high | Very high | Enterprise | Custom; often thousands/month+ | Strong but costly and contract-heavy. Better as a later upgrade. |
| **Dow Jones / Factiva / Newswires** | Institutional news/archive | High | Medium-high depending on feed | Enterprise | Custom | High-quality business news. API/storage/ML terms need careful review. |
| **Bloomberg / Bloomberg News / B-PIPE-style feeds** | Institutional terminal/feed ecosystem | High | High | Enterprise | Custom, expensive | Good but not a simple Databento-like developer onboarding path. |
| **MT Newswires / Barchart / Nasdaq data products** | Market-news supplement | Medium-high | Medium-high | Medium-high | Custom or platform-tiered | Good to evaluate if Benzinga is too expensive or insufficient. Verify APIs and historical archive. |
| **GDELT / SEC EDGAR / free public sources** | Free macro/regulatory supplement | Medium for research | Low for trading latency | Medium | Free | Useful as supplemental features, not a replacement for licensed financial headlines. |

## Short answer to “is there a Databento-like stock news company?”

There is no exact Databento equivalent for stock news with the same raw market-data/DBN-style model. News data is more licensing-heavy, text-heavy, and less standardized than trades/quotes.

Closest practical choices:

- **Databento-like developer experience:** Polygon/Massive if Benzinga news entitlements are included.
- **Best trading-news source for this project:** Benzinga direct API.
- **Best institutional machine-readable news:** RavenPack or LSEG News Analytics.
- **Best low-cost adapter test:** Finnhub, Marketaux, Alpha Vantage, or Tiingo.

## Feature mapping to the existing schema

The provider adapter should normalize every vendor into this canonical event table before any model training:

| Canonical field | Current use | Vendor mapping |
|---|---|---|
| `symbol` | Per-symbol feature aggregation | Vendor tickers/entities; route broad market news to `SPY`/`QQQ` context and direct single-name news to `TSLA`/`NVDA`/etc. |
| `published_ts` | Prevent lookahead | Vendor article publication timestamp. |
| `received_ts` | Live replay / latency truth | Local receipt time for live; provider retrieval time for historical backfill if release timestamp is uncertain. |
| `tradable_ts` | As-of feature eligibility | `max(published_ts, received_ts)` unless vendor supplies a more conservative dissemination timestamp. |
| `provider` / `source_site` | Provider breadth and trust | Vendor source, publisher, feed name, or source domain. |
| `article_id` | Deduplication | Vendor ID/story ID/URL hash. |
| `headline` | Internal sentiment/event model | Title/headline. Store body/summary only if license allows. |
| `sentiment_score` / `sentiment_confidence` | Bar sentiment features | Use vendor sentiment if supplied; otherwise run internal FinBERT/event model. |
| `event_label` / `event_prob_*` | Event-family features | Map vendor categories/tags to earnings, analyst, legal/regulatory, product/capex, macro; add new families later. |
| `dup_cluster_id`, `dup_seq_asof`, `dup_provider_count_asof`, `dup_is_repeat` | Novelty/repeat penalty | Use vendor story cluster if available; otherwise compute internally from normalized headline text and timestamp. |
| `relevance_score`, `impact_score`, `novelty_score`, `directional_impulse`, `alpha_*`, `volatility_shock_score` | Higher-level model features | Prefer internal supervised `train_news_event_model.py` outputs so all providers share one scoring standard. Vendor values can be separate raw features. |

## Feature adjustments recommended before provider integration

The current feature set is usable, but provider-realistic adjustments will make it safer and more useful.

1. **Do not treat vendor sentiment as final alpha.**
   - Use vendor sentiment as one raw feature.
   - Keep internally trained `alpha_up_prob_*`, `alpha_down_prob_*`, and `alpha_expected_ret_*` as out-of-fold supervised event scores.

2. **Add provider-latency and source-quality fields.**
   - `news_provider_latency_ms`
   - `news_local_receipt_lag_ms`
   - `news_source_tier`
   - `news_has_vendor_cluster`
   - `news_has_vendor_ticker_relevance`

3. **Add longer memory windows.**
   - Current code focuses on 60s/300s.
   - For day trading, add 15m/60m decayed context and a premarket carry feature, especially for earnings, analyst notes, regulatory headlines, and macro events.

4. **Split direct symbol news from market/context news.**
   - `TSLA` and `NVDA` single-name headlines should be separate from `SPY`/`QQQ` macro/broad market headlines.
   - `TQQQ` should mostly inherit `QQQ`/Nasdaq/mega-cap context; direct `TQQQ` issuer headlines will be sparse and often less useful.

5. **Expand event taxonomy.**
   - Current event families: earnings, analyst, legal/regulatory, product/capex, macro.
   - Add: `m_and_a`, `guidance`, `filing_sec`, `management`, `recall_safety`, `ai_semiconductor_supply_chain`, `fed_cpi_rates`, `geopolitical_tariff`, `index_etf_flow`.

6. **Make dedupe cross-provider, not only provider-local.**
   - Same story can arrive through Benzinga, Reuters, Dow Jones, and web syndication.
   - Provider breadth should increase confidence only when the stories are not duplicate repeats of the same headline cluster.

7. **Keep no-news baselines clean.**
   - Continue using `USE_NEWS_BAR_FEATURES=0` for baseline/silver experiments until real news columns are present.
   - Add the news block only as an ablation-gated feature source.

## Recommended pilot plan

### Phase N0 — Vendor due diligence

Ask each shortlisted provider for:

- Historical archive coverage for `2025-07-21` through the current date.
- Real-time delivery method: WebSocket/SSE/push versus REST polling.
- Timestamp semantics: publication time, feed dissemination time, API availability time, update time.
- Entity/ticker tagging quality for `TSLA`, `NVDA`, `SPY`, `QQQ`, `TQQQ`.
- Whether ETF/index/macro stories can be pulled independently for market context.
- Dedup/story-cluster fields.
- Sentiment/entity/event fields.
- Storage rights for headlines and metadata.
- Explicit permission for ML training, backtesting, and live algorithmic trading.
- Rate limits and bulk download mechanism for historical backfills.

### Phase N1 — Adapter-only sample

For the top 1-2 providers:

1. Pull 1-3 months of historical data for `TSLA`, `NVDA`, `SPY`, `QQQ`, and `TQQQ`.
2. Normalize to a provider-neutral `news_events_v1` CSV/Parquet schema.
3. Preserve raw payload JSON separately for audit.
4. Record `provider`, `provider_plan`, `retrieved_at`, `request_params`, row counts, and symbol coverage in a manifest.
5. Run a single live shadow session and measure local receipt latency.

### Phase N2 — Training/backtest ablation

1. Score events with `train_news_event_model.py` or an upgraded OOF version.
2. Rebuild 30s bars with `build_30s_from_5s_csv.py --news-csv`.
3. Run matched no-news versus news setup experiments.
4. Require the same gates used for Databento silver features:
   - incremental after-cost net-R lift,
   - stable thresholds/folds,
   - sufficient predicted positives,
   - no single-day dominance,
   - calibration not worse,
   - replayable as-of timestamps.

### Phase N3 — Live shadow only

1. Stream/poll live news into `harvest_news_events` and normalized Parquet/CSV.
2. Do not let live news alter entries until replay parity passes.
3. Compare live feature vectors with offline feature rebuild for the same timestamps.
4. Only after parity passes, allow the news feature block into a paper/shadow candidate.

## Provider-specific recommendation

### If budget allows one serious provider

Choose **Benzinga News API**.

Minimum contract requirements:

- Historical archive back to at least `2025-07-21` for the pilot symbols.
- Live streaming or very-low-latency push delivery.
- Ticker tags and categories.
- Storage/backtesting/ML rights.
- Stable article/story IDs.
- Access to analyst ratings, price target, earnings, and breaking headline categories.

### If budget is constrained

Start with **Finnhub** or **Marketaux** for adapter/schema development and cheap historical ablations, then upgrade to Benzinga if the initial ablation shows signal.

### If news becomes proven alpha

Evaluate **RavenPack** or **LSEG News Analytics**. Do not start there unless the cheaper pilot proves that news materially improves this strategy’s after-cost performance.

### What not to do

- Do not use Alpha Vantage or Marketaux as the only live execution feed for 5s/30s decisions unless latency and archive tests prove sufficient.
- Do not train on publication timestamps if the provider cannot prove when the story was actually available to the API.
- Do not store full article bodies unless the license explicitly allows it.
- Do not promote a bundle just because average precision improves; news features must pass threshold-stability, day-dominance, calibration, replay, and paper/shadow gates.

## Bottom line

Use **Benzinga** as the first serious trading-news provider candidate, with **Finnhub/Marketaux** as cheaper adapter-validation backups and **FMP/SEC** as structured-event supplements. Keep the implementation provider-neutral so that if Benzinga is too expensive or insufficient, the normalized event schema can swap to Intrinio, RavenPack, LSEG, or another institutional source without rewriting the 30s/lifecycle feature pipeline.

