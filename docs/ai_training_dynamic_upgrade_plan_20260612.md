# AI Training Dynamic Upgrade Plan

Date: 2026-06-12
Branch: `ai-training-dynamic-upgrade-20260612`
Status: discussion baseline / planning document

## Purpose

This document captures the working plan for upgrading the trading-agent AI training stack from a mostly fixed bar/chart feature approach toward a more dynamic, machine-understandable market-state approach.

The goal is not to remove bars entirely. The goal is to make fixed 30-second and 5-second bars only one view of the market, while adding event-driven state, sequence context, source-data quality controls, live-shaped labels, calibrated probabilities, and stricter model promotion gates.

## High-level verdict

The current project is directionally strong because it already separates:

- 30-second setup/context decisions.
- 5-second micro-entry timing.
- Position-aware lifecycle exits.
- 5-second micro-exit guard behavior.
- Deterministic safety controls such as stops, flattening, and runtime guardrails.

However, parts of the current training approach are still too rigid and human chart-like:

- Fixed 30-second and 5-second bucket boundaries.
- Candle body/wick/indicator-heavy feature sets.
- Binary classification targets that do not fully express expected value, timing, or risk.
- Probability thresholds that often behave like ranking-score cutoffs rather than calibrated probabilities.
- Bootstrap setup/entry score proxies in lifecycle/micro training that should not be promoted as live-quality signals.

The recommended direction is to keep the existing lifecycle/micro architecture, but feed it better machine-native state and labels.

## Source data inventory verified

Primary mounted source folders:

- `/Volumes/DatabentoVault/OPRA-20260523-MSV68VKVKD`
- `/Volumes/DatabentoVault/EQUS-20260523-6J9KE98BJ9`

Existing aligned Databento source window:

- Start: `2025-07-21 00:00`
- End: `2026-05-23 00:00`
- Interpret as a half-open interval for batch requests: `[2025-07-21 00:00, 2026-05-23 00:00)`.
- This corresponds to available daily files from `20250721` through `20260522`.

### OPRA source

- Dataset: `OPRA.PILLAR`
- Schema: `ohlcv-1s`
- Symbol style: parent option roots such as `TSLA.OPT`, `TQQQ.OPT`, `SPY.OPT`
- Files: 213 daily `.dbn.zst` files
- File date range: `20250721` through `20260522`
- Approximate size: `8.4G`
- Listed source conditions:
  - 219 available sessions
  - 1 degraded session: `2025-10-22`
- Missing listed dates correspond to market holidays observed in both source folders:
  - `20250901`
  - `20251127`
  - `20251225`
  - `20260101`
  - `20260119`
  - `20260216`
  - `20260403`

Important limitation: this OPRA source is 1-second option OHLCV, not full option quote book, depth, greeks, or implied volatility. It is still useful for option-volume flow, but current training should not treat it as complete options microstructure.

### EQUS source

- Dataset: `EQUS.MINI`
- Schema: `tbbo`
- Symbol style: raw symbols such as `TSLA`, `TQQQ`, `SPY`
- Files: 213 daily `.dbn.zst` files
- File date range: `20250721` through `20260522`
- Approximate size: `4.3G`
- Listed source conditions:
  - 211 available sessions
  - 9 degraded sessions:
    - `2025-08-08`
    - `2025-08-13`
    - `2025-08-19`
    - `2025-08-25`
    - `2025-09-03`
    - `2025-09-08`
    - `2025-09-12`
    - `2025-10-10`
    - `2025-10-13`

Important limitation: `tbbo` gives trades with top-of-book context. It is not full depth and should be modeled accordingly.

## Raw DBN source deep-dive: what these feeds contain

Temporary reader environment used for this inspection:

- Created outside the repository at `/tmp/trading-agent-databento-reader-venv`.
- Installed packages: `databento`, `pandas`, `pyarrow`, `zstandard`.
- Default project Python did not initially import `databento`, so DBN decoding required this temporary environment.

Representative decoded day:

- EQUS file: `/Volumes/DatabentoVault/EQUS-20260523-6J9KE98BJ9/equs-mini-20260521.tbbo.dbn.zst`
- OPRA file: `/Volumes/DatabentoVault/OPRA-20260523-MSV68VKVKD/opra-pillar-20260521.ohlcv-1s.dbn.zst`

### Decoded EQUS `tbbo` schema

Decoded columns observed:

- `ts_recv`
- `ts_event`
- `rtype`
- `publisher_id`
- `instrument_id`
- `action`
- `side`
- `depth`
- `price`
- `size`
- `flags`
- `ts_in_delta`
- `sequence`
- `bid_px_00`
- `ask_px_00`
- `bid_sz_00`
- `ask_sz_00`
- `bid_ct_00`
- `ask_ct_00`
- `symbol`

Observed interpretation:

- `action` was `T` in the decoded sample, so rows are trade events with contemporaneous top-of-book fields.
- `side` had values `A`, `B`, and `N`.
  - `A` can be treated as trade at/near ask.
  - `B` can be treated as trade at/near bid.
  - `N` is neutral/unknown and should not be forced into buy/sell imbalance.
- `bid_px_00`, `ask_px_00`, `bid_sz_00`, and `ask_sz_00` provide L1 quote state around the trade event.
- This is very useful for trade intensity, spread, top-book liquidity, quote age, and signed-flow features.
- It is not full depth and it is not a quote-update-only stream.

Full-day decoded sample profile for `2026-05-21`:

- Total EQUS rows: `970,304`
- RTH rows: `962,797`
- `action_counts`: `T=970,304`
- `side_counts`:
  - `N=857,861`
  - `A=58,230`
  - `B=54,213`

Target symbol examples from that day:

| Symbol | RTH rows | RTH volume | RTH notional | Quote coverage | Avg spread bps |
|---|---:|---:|---:|---:|---:|
| `TSLA` | 23,644 | 953,847 | 399,622,338 | 1.000 | 4.89 |
| `TQQQ` | 15,992 | 2,237,850 | 170,054,449 | 1.000 | 2.03 |
| `SPY` | 26,269 | 1,556,483 | 1,153,094,689 | 1.000 | 0.59 |
| `QQQ` | 15,365 | 798,569 | 568,185,348 | 1.000 | 1.30 |
| `NVDA` | 75,057 | 7,087,336 | 1,563,657,795 | 1.000 | 4.83 |
| `AAPL` | 22,789 | 1,265,421 | 384,465,830 | 1.000 | 4.16 |

### Decoded OPRA `ohlcv-1s` schema

Decoded columns observed:

- `ts_event`
- `rtype`
- `publisher_id`
- `instrument_id`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `symbol`

Observed symbol format:

- Example: `TSLA  260522C00430000`
- Parsed components:
  - root: `TSLA`
  - expiry: `2026-05-22`
  - right: `C` or `P`
  - strike: `430.000`

Observed interpretation:

- OPRA `ohlcv-1s` provides one-second option trade bars per contract when volume exists.
- It is valuable for option-volume bursts, call/put flow, expiry concentration, strike concentration, and same-time-of-day option activity z-scores.
- It does not provide option NBBO, option spread, resting liquidity, trade-at-bid/ask classification, or greeks/IV by itself.

Full-day decoded sample profile for `2026-05-21`:

- Total OPRA rows: `4,622,733`
- RTH rows: `4,597,860`
- Parse failures in sampled parser: `0`

Target underlying examples from that day:

| Root | RTH option volume | Contracts | Call vol | Put vol | Call/put ratio | Dominant expiries |
|---|---:|---:|---:|---:|---:|---|
| `TSLA` | 2,397,951 | 3,041 | 1,622,247 | 775,705 | 2.09 | 2026-05-22, 2026-05-29, 2026-05-26 |
| `TQQQ` | 237,956 | 1,079 | 128,282 | 109,674 | 1.17 | 2026-05-22, 2026-05-29, 2026-06-18 |
| `SPY` | 11,191,231 | 5,565 | 6,088,257 | 5,304,521 | 1.15 | 2026-05-21, 2026-05-22, 2026-05-26 |
| `QQQ` | 6,136,331 | 5,227 | 3,143,879 | 3,094,313 | 1.02 | 2026-05-21, 2026-05-22, 2026-05-29 |
| `NVDA` | 4,553,406 | 2,832 | 3,179,793 | 1,373,623 | 2.31 | 2026-05-22, 2026-05-29, 2026-06-18 |
| `AAPL` | 865,936 | 1,529 | 556,116 | 309,820 | 1.79 | 2026-05-22, 2026-06-18, 2026-05-29 |

Important OPRA observation:

- For ETF symbols with daily expirations such as `SPY` and `QQQ`, same-day `0DTE` flow was very large on the inspected day.
- For `TSLA` and `TQQQ`, dominant flow on `2026-05-21` was primarily `1DTE` and weekly expiry flow, not same-day `0DTE` in this sample.

## Pilot symbol cohort: recommended top 5

Pilot config file added for this branch:

`config/databento_dynamic_upgrade_pilot_symbols.csv`

Recommended first upgrade cohort:

| Priority | Symbol | OPRA parent | Initial role | Train as target? | Use as context? | Why it belongs in the first cohort |
|---:|---|---|---|---:|---:|---|
| 1 | `TSLA` | `TSLA.OPT` | primary trade target | yes | yes | Existing strategy focus and prior TSLA/TQQQ survivor; high option volume and volatility create rich lifecycle/micro labels. |
| 2 | `TQQQ` | `TQQQ.OPT` | primary trade target | yes | yes | Existing survivor and leveraged QQQ product; useful for testing context-aware models driven by `QQQ`/`SPY`. |
| 3 | `NVDA` | `NVDA.OPT` | primary trade target | yes | yes | Highest combined raw liquidity score in the five-session sample; strong equity and option-flow signal surface. |
| 4 | `SPY` | `SPY.OPT` | market context and optional target | no initially | yes | Broad market/0DTE anchor; should drive regime/context features even if not traded first. |
| 5 | `QQQ` | `QQQ.OPT` | tech context and optional target | no initially | yes | Direct context driver for `TQQQ`, `NVDA`, and mega-cap tech; very high option volume and 0/1DTE share. |

Selection method:

- A five-session raw-source profile was run over `2026-05-18` through `2026-05-22` using the mounted EQUS `tbbo` and OPRA `ohlcv-1s` DBN files.
- The mechanical combined-liquidity ranking from that sample was led by `NVDA`, `SPY`, `IWM`, `INTC`, `MU`, `TSLA`, `AAPL`, `AMZN`, `QQQ`, then other liquid names.
- The recommended production-oriented pilot is intentionally not the exact top-five mechanical ranking. It preserves the current app's known TSLA/TQQQ focus while adding `NVDA` as the strongest new liquid single-name test and `SPY`/`QQQ` as required cross-symbol context anchors.

Five-session sample metrics for the selected cohort:

| Symbol | Combined liquidity score | EQUS RTH rows | EQUS RTH volume | Avg spread bps | OPRA RTH option volume | OPRA contracts | Call/put ratio | 0/1DTE option-volume share |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `NVDA` | 0.979 | 300,050 | 26,786,725 | 3.98 | 17,418,256 | 3,650 | 2.16 | 38.6% |
| `SPY` | 0.960 | 133,652 | 7,885,466 | 0.56 | 52,514,966 | 9,343 | 1.02 | 78.9% |
| `TSLA` | 0.913 | 115,764 | 4,893,924 | 6.91 | 16,098,513 | 4,646 | 1.79 | 56.4% |
| `QQQ` | 0.891 | 79,055 | 4,439,604 | 1.01 | 31,232,162 | 8,657 | 0.95 | 77.1% |
| `TQQQ` | 0.825 | 85,238 | 12,801,611 | 1.65 | 1,299,311 | 1,478 | 1.14 | 24.7% |

Near-next expansion candidates after the core five:

- `IWM`: strong ETF liquidity and useful small-cap/risk-on/risk-off context.
- `AAPL`, `AMZN`, `MSFT`, `META`: mega-cap tech context and optional future single-name targets.
- `INTC`, `MU`, `AMD`, `SOXL`, `SOXS`, `SMH`: semiconductor/AI basket context around `NVDA`.

Pilot modeling rule:

- First live/paper trade targets should remain `TSLA`, `TQQQ`, and possibly `NVDA` only after validation.
- `SPY` and `QQQ` should be included from day one as context features, regime features, and option-flow anchors, even if not initially traded.
- Every pipeline should accept a symbol-cohort file, not hardcoded symbols, so scaling from 5 symbols to 10, 25, and 99 later is just a config change plus capacity validation.

TQQQ-specific peer-review note: `TQQQ` remains a core trade target because it is a current-strategy survivor and a leveraged `QQQ` product, but its own OPRA flow is materially sparser than `TSLA`, `NVDA`, `SPY`, and `QQQ` in the five-session sample. The pilot must explicitly test whether `TQQQ.OPT` flow adds independent lift beyond `QQQ` option-flow context. If not, the promotable `TQQQ` feature set should prefer `QQQ`/`SPY` option-flow context plus `TQQQ` equity microstructure rather than forcing noisy `TQQQ.OPT` buckets into the micro-entry and lifecycle models.

## Information we can extract from the current data

### From existing EQUS `tbbo`

Useful model features to extract:

- Trade intensity per 1s/5s/30s/event window.
- RTH relative volume by symbol and time-of-day baseline.
- Trade count acceleration.
- Notional traded per window.
- At-ask volume, at-bid volume, neutral volume.
- Signed-flow imbalance using only reliable `A`/`B` rows.
- Neutral-trade ratio as an uncertainty/quality feature.
- L1 spread bps and spread percentile.
- Spread widening/compression velocity.
- Bid/ask size imbalance.
- Bid/ask size imbalance trend.
- Quote availability/coverage.
- Quote age/staleness.
- Trade price versus mid/quote location.
- Micro volatility and realized variance.
- Marketable-flow bursts.
- Liquidity drought flags.
- Cross-symbol market context from symbols already present, such as `SPY`, `QQQ`, `IWM`, `VIXY`, `SOXL`, `SOXS`, and mega-cap names.

### From existing OPRA `ohlcv-1s`

Useful model features to extract:

- Total option volume by underlying/root.
- Call volume and put volume.
- Call/put volume ratio.
- Call/put delta from prior 5s/30s/120s windows.
- Volume by expiry bucket:
  - `0DTE`
  - `1DTE`
  - `2-7DTE`
  - `8-30DTE`
  - `31-90DTE`
  - `90DTE+`
- Volume by strike bucket after joining underlying price:
  - ITM
  - ATM
  - near OTM
  - far OTM
- Near-ATM call/put imbalance.
- Short-dated call/put acceleration.
- Option contract concentration:
  - top contract share of volume,
  - top 5 contract share,
  - entropy of option flow across contracts.
- Expiry concentration:
  - share of flow in nearest expiry,
  - share in weekly expiry,
  - share in monthly expiry.
- Option-flow divergence versus underlying price direction.
- Same-symbol same-time-of-day option-volume z-score.
- ETF 0DTE pressure features for `SPY`/`QQQ`, which can become market-context inputs for single-name models.

### Features that require joining EQUS and OPRA

High-value joined features:

- Option strike moneyness using underlying mid/last price.
- Near-ATM option flow around the current underlying price.
- Option flow direction versus equity signed-flow direction.
- Equity spread/quote quality during option-flow bursts.
- Option-volume burst leading or lagging equity movement.
- Cross-asset confirmation:
  - equity price up + call flow acceleration,
  - equity price down + put flow acceleration,
  - equity price up but put flow dominates,
  - option flow bursts while equity liquidity worsens.

These joined features should be built at 1s first, then aggregated to 5s, 30s, and event windows.

## Additional Databento data recommended

Current feeds are useful, but they are incomplete for a high-reliability day-trading AI system. Recommended additions are listed by priority.

### Highest priority additions

1. OPRA `definition`
   - Purpose: reliable option metadata instead of parsing OSI strings only.
   - Benefits:
     - expiry,
     - strike,
     - put/call side,
     - multiplier,
     - instrument lifecycle,
     - robust contract mapping.
   - Why it matters: moneyness and expiry-bucket features become safer and easier to audit.

2. OPRA `tcbbo`, with optional OPRA `cbbo`/`cbbo-1s` if quote-state coverage is needed between trades
   - OPRA `tcbbo` purpose: option trade events with consolidated best-bid/best-offer context at the option trade.
   - OPRA `cbbo` / `cbbo-1s` purpose: consolidated option best-bid/best-offer quote state, useful for continuous spread/liquidity coverage even when no option trade prints.
   - OPRA `cbbo-1m` purpose: one-minute quote-state sampling; useful for coarse liquidity research, but too coarse for 1s/5s/30s micro-entry and lifecycle-exit models.
   - `tcbbo` is the best first pilot schema from the available OPRA choices because it keeps actual option trade events and attaches the quote context needed for option spread, mid, and trade-location features.
   - Benefits:
     - option bid/ask/mid quote state,
     - option spread and spread percentile,
     - option quote size/liquidity quality,
     - option quote availability and staleness,
     - option slippage/spread risk filters,
     - option trade price versus bid/mid/ask from `tcbbo`,
     - better execution-feasibility checks for option-flow signals.
   - Limitation: `cbbo`/`cbbo-1s` by themselves do not contain actual option trade events. Use `tcbbo` first for trade-linked option-flow features; add `cbbo`/`cbbo-1s` if continuous quote-state coverage improves filters.
   - Why it matters: current OPRA `ohlcv-1s` shows volume but not option NBBO/spread/quote quality. `tcbbo` directly fills that gap at the trade event and supports stronger option-flow interpretation than `ohlcv-1s` alone.

3. EQUS `mbp-1` or `bbo-1s` if available for the same universe
   - Purpose: quote-state updates independent of trades.
   - Benefits:
     - continuous spread monitoring,
     - quote changes even when no trades print,
     - better quote age and quote coverage,
     - more accurate L1 imbalance dynamics.
   - Why it matters: current EQUS `tbbo` is trade-centered and can miss quote-only microstructure changes.

### Medium priority additions

4. OPRA `cbbo` / `cbbo-1s` quote snapshots if available and affordable after `tcbbo`
   - Purpose: option NBBO/spread/quote liquidity independent of trades.
   - Benefits:
     - option spread filters,
     - option quote availability,
     - option book imbalance,
     - better option execution feasibility.
   - Why it matters: option-volume bars alone can identify attention, but not execution quality. One-second quote state is useful for the current 1s/5s/30s pipeline; one-minute quote state is mainly a coarse fallback, not a microstructure feed.

5. EQUS `trades` if it is cheaper/lighter than `tbbo` for broad context symbols
   - Purpose: wider symbol universe trade intensity without full TBBO cost.
   - Benefits:
     - market breadth,
     - sector flow,
     - cross-symbol momentum,
     - cheaper broad context.
   - Why it matters: for some context symbols, full TBBO may be unnecessary.

6. `statistics` schemas where available
   - Purpose: exchange/session statistics, and possibly open-interest-like or reference statistics depending on dataset support.
   - Benefits:
     - daily context,
     - volume/open-interest style normalization if available,
     - better abnormal-activity detection.

### Lower priority or conditional additions

7. EQUS `mbp-10` or deeper book data
   - Only worth it if the strategy will explicitly use depth beyond top-of-book.
   - More expensive and heavier; not necessary before fixing current 1s/5s/30s pipeline and labels.

8. OPRA deeper book / high-frequency quote data
   - Potentially valuable but very large.
   - Should be considered after proving that OPRA `tcbbo` and optionally `cbbo`/`cbbo-1s` materially improve model decisions.

9. Broader market/index/futures context
   - If available through Databento datasets used by the project, consider adding liquid index futures or broader ETF context.
   - For now, existing symbols such as `SPY`, `QQQ`, `IWM`, `VIXY`, `SOXL`, and `SOXS` already provide a useful starting context universe.

### Available EQUS schema priority for the pilot

Available EQUS schemas mentioned for the upgrade are `ohlcv-1s`, `ohlcv-1m`, `definition`, `statistics`, `status`, `mbp-1`, `tboo`/`tbbo`, `bbo`, and `trades`.

Recommended first-pilot priority:

1. EQUS `definition`
   - Use for symbol/instrument mapping and source auditability.
   - Downloading it for the existing 99-symbol universe is reasonable if the job size is small.

2. EQUS `mbp-1` for `pilot_core_5`, preferred if storage/cost is acceptable
   - Best new equity microstructure feed because it captures top-of-book quote updates independent of trades.
   - Fills the current gap from existing EQUS `tbbo`, which is trade-centered and can miss quote-only spread/liquidity changes.
   - Key features: spread bps, bid/ask size imbalance, quote update intensity, spread widening/compression, quote staleness, and liquidity drought flags.

3. EQUS `bbo` for `pilot_core_5`, use instead of `mbp-1` if a lighter quote-state feed is needed
   - Useful for top-of-book bid/ask/mid/spread state.
   - Prefer one of `mbp-1` or `bbo` for the first pilot; do not download both until storage/cost and feature lift are understood.

4. Existing EQUS `tbbo` / possible UI label `tboo`
   - Keep using the already downloaded EQUS trade-with-BBO source.
   - Do not redownload unless there is a missing coverage problem or the Databento UI's `tboo` is a distinct feed required for parity.

5. EQUS `statistics`
   - Optional for source QA, session/reference context, and normalization after field inspection.
   - Not a first-order micro-entry signal.

6. EQUS `trades`
   - Skip for the first pilot if `tbbo`/`tboo` is already available, because trade events are already covered with quote context.
   - Consider later only as a cheaper broad-context feed for many non-target symbols.

7. EQUS `ohlcv-1s`
   - Usually skip as a new download because it can be derived from existing trade data.
   - Consider only if a compact all-symbol broad-context dataset is needed without storing event-level feeds.

8. EQUS `ohlcv-1m`
   - Skip for lifecycle/micro-entry training; one minute is too coarse for the current 1s/5s/30s pipeline.

9. EQUS `status`
   - Skip for first-pilot model features.
   - Useful later for market-status, halt/session QA, or excluding abnormal data windows.

## Data acquisition recommendation

Use the existing aligned source window for pilot training data unless running a separate smoke test:

- Start: `2025-07-21 00:00`
- End: `2026-05-23 00:00`
- Daily file coverage target: `20250721` through `20260522`

Best next Databento purchase/download order:

1. OPRA `definition` for the existing 99 parent option roots and the aligned source window.
2. OPRA `tcbbo` for the narrow `pilot_core_5` set first:
   - `TSLA.OPT`
   - `TQQQ.OPT`
   - `NVDA.OPT`
   - `SPY.OPT`
   - `QQQ.OPT`
3. Optional OPRA `cbbo` or `cbbo-1s` for the same `pilot_core_5` if storage/cost allows and if continuous option quote-state features are needed between option trades.
4. Optional OPRA `statistics` for the aligned source window if it includes useful reference/session fields for normalization or data-quality checks.
5. EQUS `definition` for the existing 99 raw symbols if small/affordable.
6. EQUS quote-state data: prefer `mbp-1` for the same `pilot_core_5` symbols, or use `bbo` instead if `mbp-1` is too heavy:
   - `TSLA`
   - `TQQQ`
   - `NVDA`
   - `SPY`
   - `QQQ`
7. After the first validation pass, expand quote-state context to:
   - `AAPL`
   - `IWM`
   - `VIXY`
   - `SOXL`
   - `SOXS`
8. Only after measurable improvement, expand OPRA `tcbbo`/`cbbo`/`cbbo-1s` and EQUS `mbp-1`/`bbo` to the full 99-symbol universe.

The pilot-first approach avoids exploding storage and cost before proving that richer quote/option data improves calibrated walk-forward performance.

Do not download heavy new schemas for the full 99-symbol universe first. First validate schema compatibility, storage footprint, joins, and model lift on the aligned `pilot_core_5` window. Later, if the pilot improves walk-forward stability, extend all required schemas together beyond `2026-05-23` as a forward holdout or production refresh.

For the available OPRA schemas, do not download everything first. Use this priority for the `pilot_core_5` window: `definition` is mandatory; `tcbbo` is the primary new option microstructure feed; `cbbo`/`cbbo-1s` is optional for continuous quote-state coverage; `statistics` is optional if the fields are useful after inspection; `status`, `trades`, and `ohlcv-1m` are not first-pilot priorities. Existing OPRA `ohlcv-1s` should remain because it is already downloaded and is useful for compact option-volume aggregation. Join `tcbbo` and optional `cbbo`/`cbbo-1s` to existing OPRA `ohlcv-1s` by option instrument and timestamp bucket to create features such as option spread bps, trade price versus mid, quote availability, quote size, quote staleness, and spread filters around option-volume bursts.

For the available EQUS schemas, do not download everything first. Use this priority for the `pilot_core_5` window: `definition` is useful for mapping/auditability; `mbp-1` is the preferred new equity quote-state feed; `bbo` is the lighter alternative if `mbp-1` is too heavy; existing `tbbo`/possible UI label `tboo` should continue to provide trade-linked price/quote context; `statistics` is optional for QA/reference context; `status`, `trades`, `ohlcv-1s`, and `ohlcv-1m` are not first-pilot priorities unless a specific gap appears.

## Databento batch job commands for the pilot downloads

These commands submit Databento batch/download jobs. They do not download completed artifacts from the Download Center after the jobs finish.

The aligned full-window request is:

- Start: `2025-07-21`
- End: `2026-05-23`
- Interpretation: `[2025-07-21 00:00, 2026-05-23 00:00)`, covering daily files through `2026-05-22`.

### Pull this branch on the submission computer

```zsh
cd /path/to/trading-agent-main

git fetch origin
git switch ai-training-dynamic-upgrade-20260612 2>/dev/null || git switch --track origin/ai-training-dynamic-upgrade-20260612
git pull --ff-only
```

### Prepare a minimal Python environment

The submit scripts require the Databento Python package. If the submission computer does not already have a working environment, create a small local venv:

```zsh
cd /path/to/trading-agent-main

python3 -m venv .venv-databento-submit
source .venv-databento-submit/bin/activate
python -m pip install --upgrade pip
python -m pip install "databento>=0.79.0"
```

Set the API key without writing it into shell history:

```zsh
read -r -s DATABENTO_API_KEY
export DATABENTO_API_KEY
```

Alternative API-key options supported by `scripts/submit_batch.py` and `scripts/submit_equity_batch.py`:

```zsh
# Preferred if you do not want to export the key into the shell environment.
umask 077
printf '%s' 'PASTE_DATABENTO_API_KEY_HERE' > ~/.databento_api_key
```

Then append this to each submit command:

```zsh
--api-key-file ~/.databento_api_key
```

Direct command-line API key passing is also supported, but it is less safe because it can be visible in shell history and process listings:

```zsh
--api-key 'PASTE_DATABENTO_API_KEY_HERE'
```

If passing `--api-key`, pass the exact Databento key token only. Do not include labels, words, spaces, quotes copied from a password manager, or a `Bearer ` prefix. If the key contains whitespace, the scripts fail before submitting.

If a live submission prints `401 auth_authentication_failed`, the request reached Databento but the key was rejected. Fixes to check:

1. Copy a fresh active key from the Databento portal.
2. Prefer `--api-key-file ~/.databento_api_key` to avoid shell quoting/history issues.
3. Ensure the key file contains only the key token and no trailing explanation text.
4. Ensure the Databento account/key has access to the requested dataset.
5. Re-run with the explicit aligned dates, not empty `START`/`END` variables.

Then define common variables:

```zsh
PY=python
START=2025-07-21
END=2026-05-23

: "${START:?START is not set}"
: "${END:?END is not set}"
```

Do not leave `START` or `END` empty. The submit scripts now fail if empty values are passed with `--start`/`--end`, instead of silently falling back to a relative date window.

To preview any request without submitting it, append `--dry-run` to the command.

### Required OPRA jobs

OPRA `definition` for the existing 99 option parents:

```zsh
"$PY" scripts/submit_batch.py \
  --dataset OPRA.PILLAR \
  --schema definition \
  --stype-in parent \
  --symbols-file runtime/symbols_100.txt \
  --start "$START" \
  --end "$END"
```

OPRA `tcbbo` for `pilot_core_5`:

```zsh
"$PY" scripts/submit_batch.py \
  --dataset OPRA.PILLAR \
  --schema tcbbo \
  --stype-in parent \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

### Optional OPRA jobs

OPRA `cbbo` for `pilot_core_5`, only if continuous option quote-state coverage is needed after `tcbbo`:

```zsh
"$PY" scripts/submit_batch.py \
  --dataset OPRA.PILLAR \
  --schema cbbo \
  --stype-in parent \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

OPRA `statistics` for `pilot_core_5`, only if field inspection confirms useful QA/reference data:

```zsh
"$PY" scripts/submit_batch.py \
  --dataset OPRA.PILLAR \
  --schema statistics \
  --stype-in parent \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

### Required EQUS jobs

EQUS `definition` for the existing 99 raw symbols:

```zsh
"$PY" scripts/submit_equity_batch.py \
  --dataset EQUS.MINI \
  --schema definition \
  --stype-in raw_symbol \
  --symbols-file runtime/symbols_100.txt \
  --start "$START" \
  --end "$END"
```

EQUS `mbp-1` for `pilot_core_5`, preferred quote-state feed:

```zsh
"$PY" scripts/submit_equity_batch.py \
  --dataset EQUS.MINI \
  --schema mbp-1 \
  --stype-in raw_symbol \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

### EQUS alternatives and optional jobs

Use EQUS `bbo` instead of `mbp-1` only if `mbp-1` is too heavy or too expensive. Do not submit both for the first full pilot unless the storage/cost estimate is acceptable.

```zsh
"$PY" scripts/submit_equity_batch.py \
  --dataset EQUS.MINI \
  --schema bbo \
  --stype-in raw_symbol \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

EQUS `statistics` for `pilot_core_5`, optional QA/reference context:

```zsh
"$PY" scripts/submit_equity_batch.py \
  --dataset EQUS.MINI \
  --schema statistics \
  --stype-in raw_symbol \
  --symbols-file runtime/pilot_core_5_symbols.txt \
  --start "$START" \
  --end "$END"
```

### Recommended first submission set

Submit this minimal set first:

1. OPRA `definition` for `runtime/symbols_100.txt`.
2. OPRA `tcbbo` for `runtime/pilot_core_5_symbols.txt`.
3. EQUS `definition` for `runtime/symbols_100.txt`.
4. EQUS `mbp-1` for `runtime/pilot_core_5_symbols.txt`.

Delay OPRA `cbbo`, OPRA `statistics`, EQUS `bbo`, and EQUS `statistics` until after checking estimated job size/cost or after the first pilot feature-build inspection.

Symbol-file clarification from peer review:

- `runtime/pilot_core_5_symbols.txt` intentionally contains raw equity symbols (`TSLA`, `TQQQ`, `NVDA`, `SPY`, `QQQ`). For OPRA requests, `scripts/submit_batch.py` appends `.OPT` automatically when `--stype-in parent`; for EQUS requests, `scripts/submit_equity_batch.py` submits the raw symbols unchanged.
- `runtime/symbols_100.txt` is a historical filename. The current audited command text refers to 99 non-empty symbols/parents, even though the file name still says `100`. Do not infer the request count from the filename; record the loaded symbol count in the source manifest.
- Do not submit optional OPRA `cbbo` or `cbbo-1s` until `tcbbo` storage size, decode speed, feature coverage, and ablation lift have been measured. If continuous option quote-state is still needed after `tcbbo`, prefer a smaller validation slice first; event-frequency `cbbo` may be much larger than sampled `cbbo-1s` for `SPY`/`QQQ` option chains.

## Current derived data observations

Derived/staged data exists under:

`/Volumes/DatabentoVault/trading-agent-offload/databento/training_data`

Important observed folders include:

- `databento_1s_20260523`
- `databento_5s_20260523`
- `databento_30s`
- `compare_runs_20260523_meta_ab`
- `databento_training_runs_20260523`

There are both full-window and short/recent-window staged datasets. This matters because model scorecards, calibration results, and backtests must clearly identify which dataset window they used.

Example full-window staged dataset:

`compare_runs_20260523_meta_ab/baseline_no_generated_meta/staged_datasets/raw/mega_liquid.csv`

Observed properties:

- Rows: about `3,156,660`
- Date range: `20250721` through `20260522`
- Symbols: about 19 mega-liquid symbols

Example recent-window staged dataset:

`databento_training_runs_20260523/staged_datasets/raw/mega_liquid.csv`

Observed properties:

- Rows: about `163,800`
- Date range: `20260511` through `20260522`
- Symbols: about 21 symbols

Recommendation: every training run must produce and preserve a dataset manifest that records exact source paths, date range, symbols, row counts, source hashes, degraded days, build parameters, and schema version.

## Concrete issue found: aggregated quality flags are likely over-marking 5s/30s bars

In `build_30s_from_5s_csv.py`, 1-second rows receive `DataQualityFlags` such as:

- `no_trade`
- `no_quote`
- `synthetic_ohlc`
- `none`

Then `_aggregate_intraday_to_cadence()` aggregates to 5-second and 30-second bars. For `DataQualityFlags`, `_build_agg_fn()` currently uses `_quality_flag_union()`.

That means if any child 1-second row inside a 30-second bar has `no_trade` or `no_quote`, the full 30-second parent bar can be marked `no_trade|no_quote|synthetic_ohlc`, even when the 30-second bar contains many real trades and quote observations.

Observed symptom in a full `mega_liquid.csv` staged dataset:

- Most 30-second rows were marked `no_trade|no_quote|synthetic_ohlc`.
- Sample rows still had meaningful `Volume`, `Count`, `TradePrintCount5s`, bid/ask, and quote-related fields.

Recommended Phase 0 fix:

- Keep 1-second `DataQualityFlags` as raw child-level diagnostic data.
- Preserve the old parent union only as `ChildDataQualityFlagUnion` for audit/debugging.
- Do not use the raw child union as the parent training-exclusion flag.
- For 5-second and 30-second bars, generate aggregate quality features instead of blindly unioning child flags.
- Rebuild parent `DataQualityFlags` from thresholds on aggregate coverage and quote validity.

Important semantic split:

- `QuoteUpdateCoverage`: fraction of child seconds with a fresh quote update event.
- `QuoteStateCoverage`: fraction of child seconds with a valid as-of quote state.

A symbol does not need a quote update every second to have valid quote state. A quote can be stale or invalid, but a quiet second after a fresh quote should not automatically make the parent bar `no_quote`.

Aggregate quality fields to add for 5s/30s bars:

- `TradeSecondsPresent`
- `QuoteUpdateSecondsPresent`
- `QuoteStateSecondsValid`
- `SyntheticSeconds`
- `TradeCoverage`
- `QuoteUpdateCoverage`
- `QuoteStateCoverage`
- `SyntheticCoverage`
- `QuoteAgeMsMean`
- `QuoteAgeMsMax`
- `ValidSpreadCoverage`
- `LockedCrossedSeconds`
- `QualityScore`

Parent flag rules:

- Mark parent `no_trade` only when aggregate trade count or `TradeCoverage` is below a configured threshold.
- Mark parent `no_quote` only when `QuoteStateCoverage` is below a configured threshold, not because one child second lacked a quote update.
- Mark parent `synthetic_ohlc` only when OHLC was materially synthesized rather than supported by real prints.
- Use `partial_synthetic_ohlc` when `SyntheticCoverage` is material but not total.
- Use `stale_quote` when `QuoteAgeMsMax` or quote-age percentile exceeds the model-specific threshold.
- Use explicit invalid flags for missing, zero, crossed, locked, or unreasonable bid/ask state.

Training-row policy:

- Hard reject only truly unusable rows, such as outside-session rows, timestamp-order failures, rows with no trade and no valid quote state, rows with no usable price anchor, or rows whose label path lacks enough real price/mid coverage.
- Keep partial-quality rows and expose their coverage/staleness/synthetic metrics as model features.
- Apply model-specific gates: 5s micro-entry should be strict on quote freshness/spread; 30s setup can tolerate more partial trade coverage if quote/context state is valid; lifecycle and micro-exit models should see quality deterioration as a potential exit-risk feature.

Leakage guard for regularization:

- Avoid future `bfill()` when regularizing quote/book state. Prefer forward-fill from known past state plus explicit previous-close fallback only when allowed by the manifest.
- Pre-first-quote or pre-first-trade seconds should remain low-quality or invalid rather than inheriting a future quote/price.
- Every quality field must be computed using the same half-open as-of window used for model features.

Required audit report for the Phase 0 fix:

- Old versus new `no_quote` counts.
- Old versus new `synthetic_ohlc` counts.
- Bars recovered by symbol/day/cadence.
- Bars hard-rejected by symbol/day/cadence.
- Distributions for `TradeCoverage`, `QuoteUpdateCoverage`, `QuoteStateCoverage`, `SyntheticCoverage`, `QuoteAgeMsMax`, and `FeatureCompleteness`.

Minimum synthetic tests before retraining:

1. A 5s parent with four valid quote-state seconds and one `no_quote` child second must not become parent `no_quote`.
2. A 5s parent with low quote-update coverage but fresh carried quote state should have low `QuoteUpdateCoverage`, high `QuoteStateCoverage`, and no parent `no_quote`.
3. Pre-first-quote seconds must not be backfilled from future quote state.
4. A no-trade parent with valid quote state can remain as a partial-quality row with high `SyntheticCoverage`.
5. A no-trade/no-quote parent should be hard-rejected or assigned a very low `QualityScore`.

This should be treated as a Phase 0 reliability fix before serious retraining.

### June 13 peer-review import: confirmed blockers from code inspection

The peer-review document `docs/peer_review_ai_training_dynamic_upgrade_plan_20260613.md` adds concrete code-inspection findings that should be treated as execution blockers, not optional hardening.

Hard blockers before any retrained bundle is treated as a paper/shadow candidate:

| ID | Blocker | Required action |
|---|---|---|
| `C1` | Future backfill leakage is present in `build_30s_from_5s_csv.py` around the current book/price regularization calls: `out[book_cols] = out[book_cols].ffill().bfill()`, `base_close = base_close.ffill().bfill()`, and per-column `ffill().bfill()`. | Remove `bfill()` from book/price regularization before retraining. Use forward-fill from past-known state only, explicit previous-close fallback only when allowed, and invalid/low-quality flags for leading null state. |
| `C2` | `_quality_flag_union()` is still the parent aggregation path for `DataQualityFlags`. | Replace parent child-flag union with aggregate coverage/staleness/synthetic metrics and threshold-derived parent flags. Preserve the old union only as `ChildDataQualityFlagUnion` for audit. |
| `C3` | `f_setup_score_proxy` / `f_entry_score_proxy` can be constant bootstrap `1.0` when no walk-forward setup probability exists. | Lifecycle/micro training must fail by default if real out-of-fold setup probabilities are missing or constant, unless an explicit research-only override is used. |
| `C4` | `train_30s_models.py` walk-forward reporting does not write out-of-fold setup predictions that can be joined into lifecycle/micro training rows. | Build `generate_walk_forward_setup_predictions.py` or equivalent to emit one out-of-fold prediction row per 30s training bar with symbol, timestamp, fold ID, raw score/probability, selected threshold, and threshold margin. Fail if any trainable bar lacks a prediction. |

Execution status as of 2026-06-13:

- `C1` is fixed in branch code: `build_30s_from_5s_csv.py` no longer uses `ffill().bfill()` in second-bar book/price regularization. It now forward-fills only from already observed state, keeps pre-first-quote bid/ask as missing, allows explicit previous-close price fallback without creating quote state, and has regression coverage in `tests/test_build_30s_from_5s_csv_regularization.py`.
- `C2` is fixed in branch code: parent `DataQualityFlags` are derived from aggregate coverage/staleness/synthetic thresholds, and the old child union is preserved separately as `ChildDataQualityFlagUnion` for audit. Regression coverage in `tests/test_build_30s_from_5s_csv_regularization.py` verifies that one child `no_quote` flag no longer makes a parent `no_quote` when quote-state coverage is valid.
- Source/audit readiness is complete for the first 10-day pilot manifest: `source_inventory_hashes_20260613_133951` has `hash_error_count=0`; `dbn_audit_summary_recent_old_20260613_150239` has `error_count=0` and `warning_count=0`; `pilot_dates_latest10_20260613_153639` selected `2026-05-11` through `2026-05-22` excluding weekend/non-paired dates, with `50` source files and about `4.508 GiB` compressed input.
- The detailed step ledger is maintained in `docs/computer_capability_task_organization_20260613.md` using the required **Action plan** followed by **Action done** format for every step. After pulling and validating the C1/C2 fixes on the 48GB machine, the next controlled execution step is the 10-day pilot build contract, not full-window training.

Pre-fix artifact policy:

- Existing staged datasets built through the current quality path, including `compare_runs_20260523_meta_ab` and `databento_training_runs_20260523`, are pre-fix artifacts. They can be used for debugging and regression comparison only, not as clean baselines to beat.
- `model_exports/lifecycle_micro_20260523` is an integration artifact, not a paper/shadow candidate. Its micro-entry scorecard has extremely low useful coverage (`longMicroEntryAi` around `precision=1.0`, `recall=0.005264`, `pred_pos_rate=0.000555`; `shortMicroEntryAi` around `precision=1.0`, `recall=0.00514`, `pred_pos_rate=0.000495`) and was trained with bootstrap proxy risk. A calibration pass alone cannot promote it.
- Code inspection found no complete probability-calibration infrastructure yet: no persisted Brier score, ECE, calibration curves, or calibration manifest in the current trainers. Until that exists, promising raw scorecards remain research-only.
- Code inspection found no complete dataset/join/label manifest infrastructure yet. The manifest requirements in this plan are new deliverables, not already-satisfied controls.

## Assessment of the current 30-second model approach

Main trainer: `train_30s_models.py`

Strengths:

- Entry labels are event-ordered and path-dependent.
- Labels check target/stop ordering instead of only using future max/min.
- Threshold tuning uses folds/calibration logic.
- Supports stronger model families such as LightGBM/CatBoost.
- Includes regime classification and optional meta-producer features.

Weaknesses:

- Many features are still human chart-derived: candle body/wicks, VWAP distance, Bollinger/MACD/RSI-style indicators, ATR/SMA distances, swing/day-high/day-low features, streaks, and whole-number distances.
- Binary labels do not fully express expected value, drawdown risk, timing, or slippage sensitivity.
- Probabilities should be treated as raw model scores unless calibrated.

Recommended role:

The 30-second model should become a setup/context model that decides whether to arm a trade opportunity, not a complete trade executor by itself.

It should answer:

- Is this symbol in a favorable context?
- Is expected edge positive after spread and slippage?
- Is liquidity good enough?
- Which side, if any, should be armed?
- What is the expected R distribution over the next several minutes?

## Assessment of the current 5-second / lifecycle-micro approach

Legacy 5-second trainer:

`src/main/java/com/calgary/fili/trader/Trainers/train_ai_5sec_4models.py`

Recommended status: legacy/research only.

Reasons:

- Direct 5-second chart predictor.
- Simpler rolling future max/min labels.
- Placeholder order-flow/options features.
- Less aligned with the current lifecycle/micro runtime route.

Current lifecycle/micro trainer:

`train_lifecycle_micro_models.py`

Strengths:

- Separates 30-second setup, 5-second micro-entry timing, lifecycle exits, and fast micro-exit guards.
- Uses position-aware features for exits.
- Better matches how a day-trading system actually operates.

Critical caveat:

- `f_setup_score_proxy` and `f_entry_score_proxy` can fall back to bootstrap value `1.0`.
- Models trained with those placeholders should be treated as integration/bootstrap artifacts, not live-promotable AI artifacts.

Next required upgrade:

- Generate real walk-forward 30-second setup probabilities.
- Generate real entry probabilities and threshold margins.
- Train lifecycle/micro models only on live-shaped arms and simulated position states.
- Fail training by default if proxy scores are constant bootstrap values, unless an explicit research-only override is set.

## Probability and threshold principles

Current thresholds should be interpreted carefully.

A model score of `0.68` is not necessarily a calibrated 68% probability. It may simply be a useful ranking score. RandomForest, LightGBM, and CatBoost probability outputs require explicit calibration if the runtime will treat them as probabilities.

Recommended changes:

- Add probability calibration per model family.
- Track Brier score and expected calibration error.
- Produce calibration curves per symbol/cohort/regime.
- Select thresholds by expected net R, drawdown, trade count, and stability, not precision alone.
- Preserve both raw score and calibrated probability in scorecards.
- Rename existing code/comment terminology that says threshold selection is "calibration" to `tune_threshold` or `select_threshold`. Threshold tuning is not probability calibration; the new calibration step must mean isotonic/Platt/sigmoid-style score-to-probability calibration with Brier/ECE reporting.

Candidate runtime decision logic should become closer to:

```text
enter if:
  calibrated_success_probability is high enough
  expected_net_R is positive
  predicted slippage/spread risk is acceptable
  liquidity quality is acceptable
  regime risk is acceptable
  daily/risk controls allow a new trade
```

not only:

```text
enter if raw_model_score >= fixed_threshold
```

## Target architecture: dynamic machine-state training

The upgraded system should use fixed bars as one representation, but train on richer state.

### Multi-timescale state

Use all of:

- 1-second state snapshots.
- 5-second microstructure windows.
- 30-second setup/context windows.
- Event bars.
- Volume bars.
- Dollar-volume bars.
- Trade-count bars.
- Spread/imbalance event windows.

### Better equity microstructure features

Recommended feature families:

- Trade intensity.
- Signed flow.
- At-bid versus at-ask volume.
- Spread percentile and spread widening speed.
- Quote age and quote coverage.
- L1 imbalance trend.
- Liquidity drought flags.
- Volatility burst.
- Relative volume by symbol/time-of-day baseline.

### Better option-flow features

Current OPRA integration should be expanded beyond total call/put volume.

Recommended feature families:

- Call/put volume by expiry bucket.
- Call/put volume by moneyness bucket.
- Near-ATM call/put imbalance.
- Short-dated option burst.
- Option flow acceleration.
- Option-flow divergence versus underlying price.
- Same-symbol time-of-day option-volume z-scores.

This likely requires preserving parsed option expiry and strike from OPRA symbols, not only underlying and C/P side.

### Cross-symbol context features

For day trading TSLA/TQQQ and similar names, add market context such as:

- SPY/QQQ direction.
- SOXL/SOXS behavior.
- VIXY behavior.
- Mega-tech basket direction.
- Sector/ETF context.
- Market breadth across the available 99-symbol universe.
- Cross-symbol correlation/regime drift.

### Sequence features

Train real sequence models on dynamic windows, then feed their outputs into the tabular models first.

Practical initial sequence windows:

- last 60 to 300 seconds of 1-second state,
- last 10 to 30 minutes of 5-second state,
- last 1 to 2 hours of 30-second context.

Start with practical models:

- TCN,
- GRU/LSTM,
- compact Transformer/PatchTST only after dataset and label quality are stable.

Do not jump directly to a large Transformer as the core production model. Sequence models are feature generators only until they beat calibrated tabular models on walk-forward net R, calibration, threshold stability, and live replay reproducibility.

## Label upgrade plan

Current binary labels are useful but insufficient.

Add target columns such as:

- `label_tp_before_sl`
- `future_max_r`
- `future_min_r`
- `realized_r_if_enter_now`
- `expected_net_r_after_costs`
- `time_to_target_seconds`
- `time_to_stop_seconds`
- `max_favorable_excursion_r`
- `max_adverse_excursion_r`
- `exit_hazard_next_5s`
- `exit_hazard_next_30s`
- `hold_value_next_30s`

For exits, prefer survival/hazard-style labels over generic top/bottom labels:

- probability that holding remains positive EV,
- probability of deterioration soon,
- expected value of exit now versus hold.

`expected_net_r_after_costs` must be execution-aware rather than theoretical. Store the component assumptions in the label manifest and make the label build fail if they are missing:

- entry spread cost,
- exit spread cost,
- slippage model by symbol/time/liquidity regime,
- partial-fill or missed-fill penalty,
- commissions/fees if applicable,
- quote staleness/adverse-selection penalty where available,
- latency assumption used for entry and exit decisions.

Any expected-R, hazard, or hold-value model that ignores realistic entry/exit friction should be considered research-only and not promotable.

Phase-ordering correction from peer review: add a minimum cost-aware label in parallel with the Phase 1 pilot slice, not only after feature expansion. At minimum, feature-block experiments should use a net-R label with entry spread, exit spread, fixed/conservative slippage, and partial-fill/missed-fill assumptions. Any `equs_quote_v2`, `opra_tcbbo_v2`, `event_pressure_v2`, or `full_pilot_v2` experiment evaluated only against cost-naive binary labels is research-only.

## Phased roadmap

### Phase 0: reliability and reproducibility

1. Remove confirmed future `bfill()` leakage and fix aggregate quality flag logic in `build_30s_from_5s_csv.py`.
2. Add a Databento source audit script.
3. Make Databento source paths configurable in build scripts.
4. Make the Python Databento environment reproducible.
5. Rebuild a small TSLA/TQQQ subset from raw DBN as a validation slice.
6. Compare old versus new quality distributions and labels.
7. Add code-level as-of/leakage enforcement for every cross-feed join and label join.
8. Require dataset manifests to record timestamp columns, join rules, lag assumptions, and future-row assertion results.
9. Mark all existing quality-pre-fix staged datasets and scorecards as research/debug artifacts.

### Phase 1: canonical data lake

Create a canonical data root such as:

`/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2`

Recommended structure:

```text
data_lake_v2/
  source_manifests/
  raw_audits/
  equs_tbbo/date=YYYY-MM-DD/symbol=TSLA/
  opra_ohlcv_1s/date=YYYY-MM-DD/root=TSLA/
  state_1s/date=YYYY-MM-DD/symbol=TSLA/
  bars_5s/date=YYYY-MM-DD/symbol=TSLA/
  bars_30s/date=YYYY-MM-DD/symbol=TSLA/
  event_bars/date=YYYY-MM-DD/symbol=TSLA/
  labels/schema_version=.../
  features/schema_version=.../
  model_training_sets/run_id=.../
```

Use partitioned Parquet/Arrow as the main storage format. Keep CSV only for debugging/export.

Before full-window builds, create a 2-5 day pilot slice and verify that EQUS/OPRA joins, feature reproduction, row counts, and quality distributions are stable and explainable. If OPRA `tcbbo` output is unexpectedly large, for example above roughly `100GB` for the pilot download, restrict the first normalization pass to a 10-day validation slice and produce storage/throughput estimates before attempting the full window.

### Phase 2: remove bootstrap proxy scores

1. Generate walk-forward 30-second setup predictions with a concrete out-of-fold prediction artifact, such as `generate_walk_forward_setup_predictions.py`.
2. Store real setup probability, threshold, and margin.
3. Generate live-shaped micro-entry arms from those walk-forward predictions.
4. Store real entry probability, threshold, and margin.
5. Retrain lifecycle/micro models using those real values.
6. Make training fail by default if setup/entry score proxies are constant bootstrap values.

### Phase 3: event/state feature expansion

Do not promote Phase 3 feature-block results unless the minimum cost-aware label from Phase 1/2 is already available. Feature blocks evaluated only against cost-naive binary labels can remain useful research, but cannot enter `full_pilot_v2`.

1. Add event bars and volume/dollar/trade-count bars.
2. Expand OPRA option-flow feature extraction.
3. Add cross-symbol market context.
4. Add rolling time-of-day-normalized liquidity and flow baselines.
5. Add sequence-model meta scores.
6. Add delayed and smoothed variants of timing-sensitive event-pressure features for robustness tests.

### Phase 4: target/label upgrade

1. Add expected-R and excursion labels.
2. Add time-to-target/time-to-stop labels.
3. Add exit hazard and hold-value labels.
4. Train companion models for probability, expected value, and risk.
5. Include spread, slippage, partial-fill, and latency assumptions in every promotable EV label.

### Phase 5: calibration and threshold overhaul

1. Calibrate model scores.
2. Track Brier score and expected calibration error.
3. Select thresholds by net R/PnL, drawdown, trade count, and stability.
4. Require stable threshold islands across folds, not a single lucky threshold.
5. Preserve calibration manifests with model exports.
6. Treat calibration as a hard precondition for paper/shadow promotion, even if Phases 1-4 produce promising raw scorecards.

### Phase 6: runtime/paper-trading gates

1. Validate feature schema hash.
2. Validate model hash.
3. Validate calibration hash.
4. Validate feature freshness and quote/spread quality.
5. Validate no threshold/probability violations in logs.
6. Monitor probability drift.
7. Run paper/shadow mode before symbol expansion.
8. Run recorded-event replay and confirm runtime decisions/features match training assumptions.

## Immediate engineering task list

Recommended first pull requests on this branch:

1. Use `config/databento_dynamic_upgrade_pilot_symbols.csv` as the initial `pilot_core_5` cohort contract.
2. `build_30s_from_5s_csv.py`: remove future `bfill()` leakage and fix aggregate quality flag semantics in the same PR.
3. `scripts/audit_databento_pilot_sources.py`: use the initial source manifest, hash, duplicate-folder, and paired-date audit helper; extend it later if decoded row-count/schema audits need Databento-specific parsing.
4. `scripts/run_parallel_databento_build_20260523.sh`: make `EQUS_DIR`, `OPRA_DIR`, `PYTHON_BIN`, output root, and symbol file configurable for this machine.
5. Add a `pilot_core_5` rebuild script for a small validation slice covering `TSLA`, `TQQQ`, `NVDA`, `SPY`, and `QQQ`.
6. Add a data-quality comparison report script for old/new 1s/5s/30s datasets.
7. Add lifecycle/micro training guardrails that fail on constant bootstrap proxy scores unless explicitly overridden.
8. Add walk-forward setup-score generation for lifecycle/micro training, producing an out-of-fold prediction file with symbol, timestamp, fold ID, raw score/probability, threshold, and threshold margin.
9. Add calibration scorecard fields: raw score, calibrated probability, Brier score, ECE, net R, drawdown, trade count, and threshold stability.
10. Add an as-of join helper that refuses ambiguous timestamp columns and records join assumptions in the dataset manifest.
11. Add a feature ablation ledger so every new feature block must prove incremental net-R lift before it enters `full_pilot_v2`.
12. Add a recorded-event replay harness for feature/decision parity before promotion.
13. Add a minimum cost-aware label builder before feature-block ablations, even if the full hazard/hold-value label upgrade comes later.
14. Define and freeze the internal holdout window before experiments start.
15. Add OPRA `tcbbo` file-size/storage/throughput sanity checks before full-window normalization.
16. Rename threshold-selection terminology in trainer code from "calibrate" to `tune_threshold`/`select_threshold` to avoid confusing it with probability calibration.
17. Add storage preflight checks and external-disk output-root enforcement so large raw/decoded/training/backtest/model artifacts cannot silently land on the 0.7TB local disk.

## Senior peer-review hardening controls

The key peer-review takeaway is that the upgraded system will live or die on data correctness, label correctness, and calibration, not model sophistication. Treat the following as non-negotiable controls for this branch.

### 1. Leakage enforcement must be code-level, not a guideline

Every cross-feed join, label join, and feature/state join must use an explicit as-of operator and must write its assumptions to the dataset manifest.

Required manifest fields per join:

- left table/path and right table/path,
- left timestamp column,
- right timestamp column,
- timestamp type used, such as `ts_event`, `ts_recv`, ingest timestamp, or derived bucket boundary,
- join direction, normally backward/as-of only,
- max lookback tolerance,
- max forward tolerance, which should be `0` for promotable datasets unless explicitly justified,
- intentional lag applied, such as OPRA lagged by `1s`,
- boundary convention, for example `[start, end)`,
- unmatched-row rate,
- future-row violation count,
- code commit and feature schema version.

Builds should fail if timestamp columns are ambiguous, if future-row assertions fail, or if a join lacks a manifest entry.

Cross-feed OPRA/EQUS lag sensitivity is blocking, not optional. A feature block that depends on apparent option-flow lead/lag must be tested with conservative delays. At minimum:

- OPRA as-of with no extra lag,
- OPRA lagged by `1s`,
- OPRA lagged by `2s`,
- EQUS quote state as-of with no extra lag,
- EQUS quote state lagged by `1s`.

If the feature only works at perfect simultaneity and fails with a small realistic lag, reject it or keep it research-only until live replay proves reproducibility.

### 2. Feature expansion must be ablation-gated

The new data can easily create a 1000+ feature system with poor signal density. Add features in blocks and require proof before inclusion in the candidate bundle.

No new feature block should enter `full_pilot_v2` without an ablation result showing:

- incremental net-R lift after costs,
- stable threshold islands across folds,
- stable or explainable feature importance across folds,
- no single day dominating improvement,
- robustness to time shift, lag, or smoothing for timing-sensitive features,
- acceptable calibration impact, including Brier score and ECE.

Feature importance alone is not enough. A block that looks important but destabilizes thresholds, worsens calibration, or only works in one volatility regime should be rejected or kept as research-only.

### 3. Labels must align with executable PnL

Expected-R, hazard, and hold-value labels must be aligned to what the strategy can actually execute. A paper label that ignores spread, slippage, fill probability, or latency will train the model to overtrade fake edge.

Every promotable label build must store:

- raw gross R,
- entry cost assumption,
- exit cost assumption,
- slippage estimate,
- partial-fill/missed-fill penalty,
- latency assumption,
- final net R after costs,
- whether the row is label-quality eligible.

The runtime strategy should use the same cost model family as the label builder, even if live parameters are more conservative.

### 4. Event-pressure features must survive delayed/smoothed tests

Burst, pressure, shock, and aggression features are useful but timing-sensitive. Keep the first production version aggregated into 5s/30s buckets, then test delayed and smoothed variants.

Reject or quarantine event-pressure features that:

- only work at perfect latency,
- reverse under a 1-2 second delay,
- produce unstable fold-to-fold thresholds,
- cannot be reproduced in recorded-event replay.

### 5. Sequence models remain meta-feature producers

Sequence models can be tested, but they are not the decision layer for the next candidate bundle. They can enter as meta-feature producers only after they beat the tabular baseline on:

- walk-forward net R after costs,
- calibration,
- threshold stability,
- replay parity,
- feature-drift checks.

Until then, the hierarchy remains: 30s setup, 5s micro-entry, 30s lifecycle, and 5s micro-exit guard.

### 6. Add live reproducibility / replay as a promotion gate

Backtests are not enough. A promotable bundle needs a recorded-event replay test that reconstructs features from a recorded stream and confirms the runtime would have made the same decisions assumed by training/backtest.

Replay report requirements:

- feature vector parity versus offline feature store,
- bucket boundary parity,
- quote staleness and quality flag parity,
- model score parity within tolerance,
- decision parity for arms, entries, holds, exits, and guard exits,
- explanation of every mismatch above tolerance.

## Next execution plan while new downloads are in progress

This section turns the current-code investigation into the next concrete plan for organizing the incoming data, selecting model families, using new feature families, and evolving the live strategy route.

### Current latest training/runtime stack found in this branch

Latest model/training path to build on:

1. `train_30s_models.py`
   - Role: 30-second setup/context and market-regime layer.
   - Current model families: RandomForest baseline, LightGBM if installed, CatBoost if installed.
   - Current exports: `long_entry.onnx`, `short_entry.onnx`, `regime_classifier.onnx`, optional open30/regime-specific models, and optional legacy 30s exits.
   - Recommended role going forward: setup arm generator only. Do not rely on legacy 30s exit models as the primary exit route.
   - Important live contract: Java dynamically handles feature-count families such as base 30 features, news/regime/meta extensions, and exact runtime vector construction in `PingPongStrategy`.

2. `train_lifecycle_micro_models.py`
   - Role: current latest upgraded route.
   - It trains six ONNX models:
     - `longExitLifecycleAi` / `shortExitLifecycleAi` with 34 features.
     - `longMicroEntryAi` / `shortMicroEntryAi` with 44 features.
     - `longMicroExitGuardAi` / `shortMicroExitGuardAi` with 50 features.
   - The latest local bundle is `model_exports/lifecycle_micro_20260523`.
   - Its manifest schema matches the Java route concept: 30s context, 5s micro features, entry/setup probability fields, and position-state fields.
   - Important caveat: the trainer can still fall back to bootstrap `f_setup_score_proxy = 1.0` / `f_entry_score_proxy = 1.0`. Any bundle trained with constant bootstrap scores is an integration artifact, not a live-promotable model.
   - Latest scorecard observation from `model_exports/lifecycle_micro_20260523`:
     - Lifecycle exits look strong in held-out test rows by precision/recall, but require live-shaped validation.
     - Micro-entry precision appears high but recall is extremely low around the current tuned threshold, so threshold calibration and trade-count stability matter more than raw precision.

3. `feature_producers_30s.py`, `generate_timesfm_features.py`, `sequence_meta_features.py`
   - Current status: useful meta-feature/proxy layer.
   - TimesFM and sequence outputs are currently safest as offline meta features, not core production decision makers.
   - The sequence script can train small torch LSTM/TCN/Transformer-style heads, but it is not yet a full production sequence-model pipeline with walk-forward leakage controls, calibration, export, and live parity.

4. `PingPongStrategy.java`
   - Runtime route already supports the target strategy pattern:
     - 30s setup model arms a side.
     - 5s micro-entry model confirms timing inside a TTL window.
     - lifecycle exit model evaluates position-aware 30s exits.
     - 5s micro-exit guard can exit faster.
   - Runtime validates `lifecycle_micro_route_manifest.json` feature columns and schema hashes before allowing the upgraded route.
   - This means the next training work should preserve exact feature manifests and avoid ad-hoc column-order changes.

### Data organization plan for the incoming downloads

Use a bronze/silver/gold lake layout. Keep raw Databento artifacts immutable; write normalized and feature datasets as versioned Parquet/Arrow; export CSV only for debugging or compatibility scripts.

Storage and memory constraint for the other main computer:

- External disk: about `5TB`; this should be the canonical storage target for raw downloads, decoded Parquet, feature stores, labels, training datasets, experiment outputs, and model export bundles.
- Local available disk: about `0.7TB`; this should be treated as a working/code/cache disk, not the primary data lake.
- Memory: `48GB`; sufficient for pilot work only if processing is partitioned/streaming and never loads the full duration into memory.

The pilot build must be external-disk-first and partitioned/streaming rather than full-window in-memory processing. Treat 48GB RAM and 0.7TB local disk as bounded resources; every heavy step should be bounded by date and symbol/root partitions and should write durable outputs to the external disk.

48GB processing policy:

- Never load full-window multi-symbol OPRA `tcbbo` or EQUS `mbp-1` into one pandas dataframe.
- Process by `date` plus `symbol` or OPRA `root`, then write partitioned Parquet and release memory.
- Start heavy OPRA `tcbbo` normalization with 1-2 workers; raise concurrency only after measuring peak RSS.
- Use modest 2-4 worker concurrency for lighter EQUS partitions only if memory remains stable.
- Prefer Polars lazy, PyArrow dataset scans, or DuckDB-style partition scans where practical; if using pandas, keep chunks small.
- Downcast counts to `int32`/`uint32`, model features to `float32`, and low-cardinality flags/enums to categorical/dictionary encodings where practical.
- Target peak active memory around 24-32GB rather than trying to consume all 48GB.
- Write intermediate silver and gold outputs frequently so failed runs resume from partitions instead of restarting the full window.

External-disk storage policy:

- Use `/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/` as the canonical root for heavy artifacts.
- Do not write full-window decoded DBN, `silver_normalized`, `gold_state`, `labels`, `model_training_sets`, `threshold_grid.csv`, backtest trade logs, calibration curves, SHAP/feature-importance outputs, or ONNX export bundles to the local internal disk by default.
- Keep local repository storage limited to source code, small manifests, tiny debug samples, and logs that are safe to commit or inspect.
- If a script requires a temp directory, point it to an external-disk scratch path such as `/Volumes/DatabentoVault/trading-agent-offload/databento/tmp/` and clean completed partition temp files.
- Training should read partitioned Parquet from external disk and write run artifacts under `model_training_sets/run_id=.../` and `model_exports/run_id=.../` on the external disk. Only copy the final small promoted ONNX bundle into the repo or app runtime path after validation.
- Every long-running decode/build/train script should expose `--data-root`, `--output-root`, `--tmp-dir`, and `--run-id` or equivalent settings so paths do not silently default to local disk.
- Before each full-window normalization or training run, run a disk preflight that records free space on both the external disk and local disk. Abort if the output root is local or if projected free space after the run would be unsafe.
- Keep at least `15-20%` free space on the external disk and at least `100GB` free on local disk during heavy processing to avoid failed writes, OS pressure, and corrupted partial outputs.

Computer-capability handoff as of 2026-06-13:

- Current 16GB machine: can read `/Volumes/DatabentoVault` but cannot write to it. Use it only for code, documentation, read-only inventory, manifest review, and tiny local tests/summaries. Do not run external offload scripts, full-window DBN decoding, large Parquet/CSV builds, training grids, or any command that writes to the external vault from this machine.
- 48GB/write-capable machine: use it for `data_lake_v2` creation, source manifests and hashes, DBN-to-Parquet normalization, 10-day pilot slices, full-window feature builds, walk-forward prediction artifacts, training, calibration, backtests, and model exports.
- Detailed downloaded-data inventory and per-machine task split are recorded in `docs/computer_capability_task_organization_20260613.md`. Pull this branch on the 48GB machine before moving from inspection to artifact-producing work.

Recommended root:

```text
/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/
  bronze_raw_downloads/
    dataset=OPRA.PILLAR/schema=definition/batch_id=.../
    dataset=OPRA.PILLAR/schema=tcbbo/batch_id=.../
    dataset=EQUS.MINI/schema=definition/batch_id=.../
    dataset=EQUS.MINI/schema=mbp-1/batch_id=.../
  source_manifests/
    batch_id=.../manifest.json
    batch_id=.../file_hashes.csv
    batch_id=.../coverage_by_day_symbol.csv
  raw_audits/
    dataset=.../schema=.../audit_run_id=.../
  silver_normalized/
    equs_mbp1/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
    equs_tbbo/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
    opra_definition/date=YYYY-MM-DD/root=TSLA/part-....parquet
    opra_tcbbo/date=YYYY-MM-DD/root=TSLA/part-....parquet
    opra_ohlcv_1s/date=YYYY-MM-DD/root=TSLA/part-....parquet
  gold_state/
    state_1s/schema_version=v2/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
    bars_5s/schema_version=v2/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
    bars_30s/schema_version=v2/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
    event_bars/schema_version=v1/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
  labels/
    label_schema=v2/date=YYYY-MM-DD/symbol=TSLA/part-....parquet
  model_training_sets/
    run_id=YYYYMMDD_HHMMSS__experiment_name/
      dataset_manifest.json
      feature_schema.json
      train.parquet
      validation.parquet
      holdout.parquet
  model_exports/
    run_id=YYYYMMDD_HHMMSS__experiment_name/
      lifecycle_micro_route_manifest.json
      calibration_manifest.json
      scorecards/
      onnx/
```

Required metadata per run:

- Raw Databento batch IDs, dataset, schema, stype, symbol file, start/end, and submit command.
- File path, size, SHA-256, and decoded row count per DBN file.
- Market-day coverage by symbol/root.
- Degraded/missing days and whether they were included or excluded.
- Feature schema version and exact column order.
- Label schema version and target definitions.
- Train/validation/holdout date splits.
- Model family, hyperparameters, calibration method, thresholds, and code commit hash.

### Immediate checks after each download completes

1. Save the Databento job metadata and raw folder path under `source_manifests/`.
2. Hash every downloaded file before decoding.
3. Decode one recent day and one older day per schema before bulk processing.
4. Confirm timestamps, symbols, and stype mapping:
   - OPRA parent/root mapping from `definition`.
   - OPRA `tcbbo` trade rows and attached option bid/ask fields.
   - EQUS `mbp-1` top-book update fields and action/event semantics.
5. Produce a coverage table for `TSLA`, `TQQQ`, `NVDA`, `SPY`, and `QQQ`.
6. Compare new feeds against existing `tbbo`/`ohlcv-1s` for the same days.
7. Use `scripts/audit_databento_dbn_day.py` to decode and summarize one representative day per schema before bulk normalization.
8. Produce a storage/throughput estimate by schema before bulk normalization, especially for OPRA `tcbbo`. If pilot `tcbbo` files are unexpectedly large, start with a 10-day normalization slice instead of the full aligned window.
9. Only then run the full pilot build.

### June 13 decision: bars, buckets, event pressure, and imbalance

Use the new OPRA/EQUS data in the existing 5s/30s lifecycle-micro cadence, but do not reduce it to plain OHLCV bars. The target data product is microstructure-enriched 5s and 30s feature buckets, with a separate event-pressure layer available for attribution and future sequence models.

Canonical flow:

```text
raw Databento events
  -> immutable bronze DBN/download manifests
  -> normalized silver event/state tables
  -> 1s as-of symbol state
  -> 5s micro-entry / micro-exit buckets
  -> 30s setup / lifecycle buckets
  -> optional event_bars for burst/pressure attribution and later sequence research
```

Immediate modeling rule:

- Keep 30s bars for setup, regime, and lifecycle context.
- Keep 5s bars/buckets for micro-entry timing and micro-exit guard decisions.
- Add event-derived and imbalance features into those fixed 5s/30s vectors first.
- Do not make raw tick/event sequence models the primary runtime route until the enriched tabular pipeline is stable, calibrated, and live-reproducible.

Option buckets should summarize OPRA flow by economically meaningful groups instead of exposing individual contracts directly to the first pilot models:

- Root/underlying: `TSLA`, `TQQQ`, `NVDA`, `SPY`, `QQQ`.
- Option side: call versus put.
- DTE bucket: `0DTE`, `1DTE`, `2-7DTE`, `8-30DTE`, `31-90DTE`, `90DTE+`.
- Moneyness bucket: ITM, ATM, near OTM, far OTM.
- Trade location/aggression: at bid, below mid, near mid, above mid, at ask.

Example 5s/30s OPRA event-bucket features:

- `opt_trade_count_5s`, `opt_trade_count_30s`.
- `opt_notional_volume_5s`, `opt_notional_volume_30s`.
- `opt_call_at_ask_volume_5s`, `opt_put_at_bid_volume_5s`.
- `opt_trade_above_mid_ratio_30s`, `opt_trade_below_mid_ratio_30s`.
- `opt_call_put_aggressive_volume_ratio_30s`.
- `opt_0dte_atm_call_trade_count_30s`.
- `opt_near_money_trade_burst_5s`.
- `opt_option_spread_bps_mean_30s`, `opt_option_spread_bps_z_30s`.

EQUS `mbp-1` imbalance should be a first-class feature family. For each as-of top-of-book state:

```text
l1_imbalance = (bid_size - ask_size) / (bid_size + ask_size)
microprice = (ask_price * bid_size + bid_price * ask_size) / (bid_size + ask_size)
```

Guard denominators before division and emit explicit invalid/coverage flags when bid/ask price or size is missing, zero, crossed, or stale.

Example 5s/30s EQUS imbalance and quote-state features:

- `eq_l1_imbalance_last_1s`, `eq_l1_imbalance_mean_5s`, `eq_l1_imbalance_mean_30s`.
- `eq_l1_imbalance_slope_30s`, `eq_l1_imbalance_flip_count_30s`, `eq_l1_imbalance_z_30s`.
- `eq_microprice_dist_bps`, `eq_microprice_return_5s`, `eq_microprice_vs_mid_bps`.
- `eq_quote_update_count_5s`, `eq_quote_update_count_30s`.
- `eq_spread_bps_mean_5s`, `eq_spread_bps_max_30s`, `eq_spread_widen_count_30s`.
- `eq_quote_staleness_ms`, `eq_locked_crossed_quote_ratio_30s`.

Event-pressure features should aggregate raw event behavior inside the 5s/30s decision windows rather than require raw event-sequence inference initially:

- trade burst score,
- quote pressure score,
- spread shock score,
- imbalance flip score,
- option aggression shock score,
- call/put flow acceleration,
- quote staleness risk score.

Route the enriched features by decision layer:

| Feature family | 30s setup | 5s entry | 30s lifecycle | 5s exit guard |
|---|---:|---:|---:|---:|
| Existing OHLCV/technical bars | yes | yes | yes | yes |
| EQUS spread/liquidity | yes | strong | yes | strong |
| EQUS L1 imbalance/microprice | yes | strong | yes | strong |
| EQUS quote update intensity/staleness | maybe | strong | yes | strong |
| OPRA aggressive call/put bucket flow | strong | yes | strong | yes |
| OPRA option spread/liquidity | yes | strong | yes | strong |
| OPRA DTE/moneyness bucket flow | strong | maybe | strong | maybe |
| Cross-symbol `SPY`/`QQQ` context | strong | maybe | strong | maybe |

Strict leakage rule for every bucket: use half-open as-of windows and never include rows after the decision boundary. Record the exact boundary convention in `feature_schema.json` / `dataset_manifest.json`. For example, a 5s decision at `09:45:05` should use the completed feature window `[09:45:00, 09:45:05)` and must not include any event at or after `09:45:05.000` unless the live runtime can prove the event was known before the decision was made.

Cross-feed timestamp discipline is mandatory when joining OPRA and EQUS. Do not assume apparent option-flow lead/lag is alpha until it survives conservative as-of tests. Track `ts_event` versus any receive/ingest timestamp available in the decoded data, document which timestamp drives each join, and avoid letting the model learn structural exchange/SIP latency. Every OPRA/EQUS feature block should be tested under lag sensitivity, for example:

- OPRA features as-of with no extra lag.
- OPRA features lagged by 1s.
- OPRA features lagged by 2s.
- EQUS quote-state features as-of with no extra lag.
- EQUS quote-state features lagged by 1s.

If a feature only works with perfectly simultaneous cross-feed timing and disappears under a small conservative lag, treat it as suspect until live replay proves it is reproducible.

### Feature plan using existing plus new data

Keep the current 30s/5s/lifecycle features as the base contract, then add versioned blocks.

Base feature blocks to preserve:

- `f_30s_*` setup/context features from `train_lifecycle_micro_models.py`.
- `f_5s_*` micro timing features.
- Position lifecycle features: `f_unrealized_pnl_r`, `f_mfe_r`, `f_mae_r`, target/stop remaining, bars since entry.
- 30s setup model scores: real walk-forward `f_setup_prob`, `f_setup_threshold`, and `f_setup_threshold_margin`.
- Real filled-entry metadata: `f_entry_prob`, `f_entry_threshold`, `f_entry_threshold_margin`.

New EQUS `mbp-1` feature block:

- Continuous spread bps, spread percentile, and spread widening/compression speed.
- Bid/ask size imbalance and imbalance slope.
- Quote update intensity.
- Quote age/staleness and quote coverage.
- Liquidity drought and wide-spread flags.
- Mid-price return and mid-price volatility separate from trade-price return.
- Trade-vs-quote disagreement features by joining existing `tbbo` with `mbp-1`.

New OPRA `definition` + `tcbbo` feature block:

- Reliable expiry, DTE, strike, option side, and contract mapping.
- Option trade price versus option bid/mid/ask.
- Option spread bps and option quote-size/liquidity quality.
- Option quote staleness/availability around volume bursts.
- Call/put flow by DTE bucket: 0DTE, 1DTE, 2-7DTE, 8-30DTE, 31-90DTE, 90DTE+.
- Call/put flow by moneyness bucket: ITM, ATM, near OTM, far OTM.
- Near-ATM call/put imbalance.
- Short-dated option-flow acceleration.
- Contract concentration: top contract share, top 5 share, flow entropy.
- Option-flow divergence versus underlying signed flow and underlying return.

Cross-symbol context block:

- `SPY` and `QQQ` returns/volatility/spread/flow as market anchors.
- `QQQ` context for `TQQQ`, `TSLA`, and `NVDA`.
- `SPY`/`QQQ` 0DTE option pressure as risk-on/risk-off context.
- Basket breadth across pilot symbols: percent above short-term VWAP, signed-flow breadth, option-flow breadth.
- Relative strength and beta-adjusted residual return for target symbol versus context symbols.

Quality/reliability feature block:

- Trade coverage and quote coverage by 1s/5s/30s parent bar.
- Synthetic OHLC coverage, not just unioned child flags.
- Feed lag/staleness and source completeness.
- Degraded-day/session flags.
- Symbol-specific valid-trading-window flags.

Sequence/meta feature block:

- Start with small TCN/GRU/LSTM or compact Transformer heads as offline meta-feature producers.
- Feed sequence scores into tabular setup/lifecycle models first.
- Do not make a large sequence model the core runtime decision-maker until it beats calibrated tabular models in walk-forward backtests and can be reproduced live.

### Model strategy we should use next

Primary architecture: keep the current hierarchical route, not a single end-to-end model.

1. 30s setup/context models
   - Primary candidates: LightGBM and CatBoost, with RandomForest as the compatibility/control baseline.
   - Targets:
     - long setup TP-before-SL probability,
     - short setup TP-before-SL probability,
     - expected net R regression or quantile regression,
     - setup risk/slippage/liquidity filter.
   - Output required for downstream models:
     - walk-forward setup score,
     - calibrated setup probability,
     - threshold used,
     - margin over threshold,
     - expected R and risk metrics.

2. 5s micro-entry models
   - Keep separate long/short micro-entry classifiers.
   - Train only on live-shaped arms generated by walk-forward 30s setup scores.
   - Add EQUS quote-state and OPRA option quote/flow features.
   - Optimize for net R and fill quality, not just precision.
   - Required improvement over latest bundle: increase useful confirmation count while keeping loss/drawdown controlled.

3. Lifecycle exit models
   - Keep separate long/short position-aware lifecycle exits.
   - Upgrade labels from simple exit-now-vs-hold classification toward hold-value / exit-hazard targets.
   - Train auxiliary regressors for expected hold value over next 30s/60s/120s.
   - Runtime decision should become `exit if exit_now_value > hold_value + cost_margin` or if hazard exceeds threshold.

4. 5s micro-exit guard models
   - Keep as fast deterioration detectors.
   - Add spread/quote/liquidity degradation and option-flow reversal features.
   - Treat guard exits as protective; measure whether they avoid hard stops without cutting winners too early.

5. Regime models
   - Keep a regime classifier but treat regime probabilities as features and routing modifiers, not hard truth.
   - Add cross-symbol context and realized liquidity/volatility regime features.
   - Use calibrated regime probabilities and entropy rather than only class labels.

6. Calibration and threshold models
   - Add calibration after every classifier: isotonic or Platt/sigmoid by fold, symbol, and model family.
   - Track Brier score and ECE in scorecards.
   - Select thresholds from stable net-R islands across folds, not from a single best PnL row.

### Holdout construction and provisional quantitative gates

The current aligned source window ends at `2026-05-22` because Databento requests use the half-open interval `[2025-07-21, 2026-05-23)`. That means there is no true forward holdout after `2026-05-23` unless a later data window is downloaded. Until then, the plan must explicitly reserve an internal chronological holdout and treat it as untouchable for training, threshold selection, feature ablation, and calibration fitting.

Default internal holdout policy for the pilot:

- Preferred holdout: `2026-02-01` through `2026-05-22`.
- If trade counts are too small, a wider holdout such as `2026-01-01` through `2026-05-22` can be used, but the exact choice must be frozen before experiments and written to `dataset_manifest.json`.
- No feature-block ablation, threshold-grid selection, isotonic/Platt fitting, or model-family choice should use the holdout rows.
- A genuine forward holdout requires extending the source window beyond `2026-05-23`; internal holdout results are necessary but not sufficient for live expansion.

Provisional experiment-gate fields to store in the experiment config/manifest:

- `min_net_r_improvement_pct`: initial default `>= 5%` versus the matched baseline over the frozen holdout, after costs.
- `max_threshold_std`: initial default `<= 0.03` across walk-forward folds.
- `max_threshold_abs_deviation`: initial default `<= 0.05` from the fold median.
- `min_holdout_trade_count`: initial target `>= 30` closed trades per active trade target or `>= 100` aggregate pilot trades; if not met, mark results as insufficient-sample/research-only rather than promotable.
- `max_single_day_profit_share`: initial default `<= 30%` of total holdout profit from any one day.
- `max_hard_stop_rate_delta`: hard-stop rate must not worsen versus the baseline; target improvement should be recorded, for example `>= 10%` reduction when sample size supports it.

These are starting gates, not universal truths. They should be adjusted only by an explicit experiment-governance change, not ad hoc after seeing results.

### Experiment matrix after the pilot data is built

Run experiments as additive feature blocks so lift can be attributed.

| Experiment | Data/features | Purpose | Promote only if |
|---|---|---|---|
| `baseline_current_v1` | existing 30s/5s features | Reproduce current latest stack | Matches current scorecard/backtest within tolerance |
| `quality_fixed_v2` | fixed aggregate quality features | Ensure label/data reliability | Quality distributions and labels are sane |
| `equs_quote_v2` | add EQUS `mbp-1` quote-state features | Test continuous quote/liquidity lift | Better net R and fewer hard stops |
| `equs_imbalance_v2` | add L1 imbalance, microprice, imbalance slope/flip features | Test order-book pressure lift | Better entry timing and fewer adverse fills |
| `opra_tcbbo_v2` | add option NBBO/trade-location features | Test richer option-flow interpretation | Better micro-entry confirmation and exits |
| `event_pressure_v2` | add quote/trade burst, spread shock, option aggression shock features | Test event-derived pressure on top of buckets | Better lifecycle exits and micro-exit guard behavior |
| `context_v2` | add SPY/QQQ/NVDA/TQQQ/TSLA context | Test market/sector context lift | More stable thresholds across days |
| `sequence_meta_v2` | add compact sequence scores | Test temporal-state lift | Improves holdout without overfitting |
| `full_pilot_v2` | all accepted blocks | Candidate paper bundle | Passes all promotion gates |

Each experiment must emit:

- `dataset_manifest.json`
- `feature_schema.json`
- `label_manifest.json`
- `scorecard.csv`
- `calibration_manifest.json`
- `threshold_grid.csv`
- `feature_ablation_result.json`
- backtest trade/lifecycle summaries
- feature importance or SHAP-style attribution where practical

No feature block can move from a standalone experiment into `full_pilot_v2` unless its ablation result shows incremental after-cost net-R lift, stable thresholds, acceptable calibration, and robustness to reasonable lag/smoothing tests.

Every `feature_ablation_result.json` should also record whether the block passed or failed the frozen holdout and quantitative gates above. A feature can have promising feature importance or train-fold lift and still be rejected if it fails holdout trade-count, threshold-stability, or single-day concentration gates.

### Strategy approach for the next candidate bundle

Keep the current runtime strategy pattern, but make decisions expected-value and quality aware:

```text
30s setup layer:
  choose side only if calibrated setup probability, expected net R, liquidity quality,
  option/equity confirmation, and context regime are acceptable

5s micro-entry layer:
  confirm only when current spread, quote freshness, signed flow, option flow,
  and price path improve entry quality inside the arm TTL

position lifecycle layer:
  hold while expected hold value remains better than exit-now value after costs

5s micro-exit guard:
  exit quickly on adverse microstructure, option-flow reversal, or liquidity deterioration

hard safety layer:
  hard stop, daily drawdown, max trades, flatten windows, and shared-capital gates always override AI
```

Initial rollout rule:

- Trade candidates: `TSLA` and `TQQQ` first.
- Shadow/paper candidate: `NVDA` after training validation.
- Context-only at first: `SPY` and `QQQ`.
- Do not expand beyond the pilot until the same bundle passes walk-forward, holdout, and paper-mode drift checks.

Recent calibration/backtest evidence should be treated as encouraging but not sufficient. The selected low-threshold TSLA/TQQQ calibration rows improved materially versus the older current January-May configuration in the stored analysis, but the trade count is still limited and the results rely on current feature/label assumptions. The new data plan should be used to reduce hard-stop reliance, improve micro-entry selectivity, and validate thresholds on more independent windows.

### Concrete next actions when the download finishes

1. Inventory raw completed jobs and write a source manifest.
2. Decode and inspect sample files for every new schema.
3. Implement or extend normalized readers for OPRA `definition`, OPRA `tcbbo`, and EQUS `mbp-1`.
4. Produce file-size, row-count, and decode-throughput estimates; if OPRA `tcbbo` is large, normalize a 10-day validation slice before the full window.
5. Rebuild a 2-10 day pilot slice for `TSLA`, `TQQQ`, `NVDA`, `SPY`, and `QQQ`.
6. Fix `bfill()` leakage and aggregate quality flags before full retraining.
7. Generate a minimum cost-aware net-R label and freeze the internal holdout window.
8. Generate walk-forward out-of-fold 30s setup probabilities.
9. Rebuild full-window pilot 1s state, enriched 5s/30s buckets, and event-pressure Parquet datasets only after the validation slice passes.
10. Retrain lifecycle/micro models with real setup/entry probabilities.
11. Run the additive experiment matrix and preserve all manifests.
12. Backtest TSLA/TQQQ first, then shadow NVDA.
13. Promote only if schema validation, calibration, threshold stability, frozen-holdout gates, replay parity, and paper-mode drift checks pass.

## Promotion gates

A model bundle should not be promoted unless all of these are true:

- Source audit passes.
- Dataset manifest is complete.
- Degraded days are explicitly included/excluded and recorded.
- Pre-fix staged datasets and pre-fix scorecards are not used as promotable baselines.
- Every cross-feed and label join has an as-of join manifest with timestamp columns, boundary convention, lag assumptions, unmatched rates, and zero future-row assertion failures.
- OPRA/EQUS lag-sensitivity tests pass for every timing-sensitive feature block.
- 5s/30s quality aggregation is fixed.
- No constant bootstrap `f_setup_score_proxy`.
- No constant bootstrap `f_entry_score_proxy`.
- Real out-of-fold 30s setup predictions cover every lifecycle/micro training row that requires setup context.
- Every accepted new feature block has an ablation result proving incremental after-cost lift and stable thresholds.
- Quantitative ablation gates are recorded, including minimum after-cost net-R lift, threshold variance/deviation, holdout trade count, hard-stop-rate delta, and single-day profit concentration.
- Event-pressure features survive delayed/smoothed robustness tests or remain research-only.
- Labels include entry spread, exit spread, slippage, partial-fill/missed-fill, and latency assumptions.
- The frozen internal holdout is not used for training, threshold selection, calibration fitting, or feature-block acceptance.
- Java runtime feature schema validation passes exactly.
- Model hashes and feature schema hashes are recorded.
- Calibration curves and metrics are recorded.
- Thresholds are stable across walk-forward folds.
- Backtest uses live-shaped arms and realistic fills/slippage.
- Holdout trade count is sufficient.
- No single day dominates profitability.
- Recorded-event replay shows feature, score, and decision parity within tolerance.
- Runtime probability/threshold validation has zero violations.
- Paper/shadow mode shows no material feature drift.

## Working conclusion

The best next step is not to replace the current system with a giant end-to-end Transformer. The best next step is to make the current lifecycle/micro architecture data-correct, live-shaped, calibrated, and dynamically contextual.

The target is:

- 30-second bars for higher-level context,
- 5-second and 1-second state for execution timing,
- event bars for dynamic market information flow,
- option-flow and cross-symbol context for broader market understanding,
- expected-value and hazard labels instead of only binary direction labels,
- calibrated probabilities and threshold stability checks,
- strict runtime and promotion gates.

This document should be treated as the starting point for the upgrade branch, not the final design.
