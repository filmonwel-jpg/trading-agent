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

Recommended fix:

- Keep 1-second flags as raw child-level diagnostic data.
- For 5-second and 30-second bars, generate aggregate quality features instead of blindly unioning child flags.
- Add fields such as:
  - `TradeSecondsPresent`
  - `QuoteSecondsPresent`
  - `TradeCoverage`
  - `QuoteCoverage`
  - `SyntheticSeconds`
  - `SyntheticCoverage`
  - `QualityScore`
- Mark a parent bar `no_trade` only when aggregate trade count or trade coverage is below a configured threshold.
- Mark a parent bar `no_quote` only when quote coverage is below a configured threshold.
- Mark `synthetic_ohlc` only when OHLC was materially synthesized rather than supported by real prints.

This should be treated as a Phase 0 reliability fix before serious retraining.

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

Do not jump directly to a large Transformer as the core production model. Use sequence scores as meta features first, then compare by walk-forward backtest.

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

## Phased roadmap

### Phase 0: reliability and reproducibility

1. Fix aggregate quality flag logic in `build_30s_from_5s_csv.py`.
2. Add a Databento source audit script.
3. Make Databento source paths configurable in build scripts.
4. Make the Python Databento environment reproducible.
5. Rebuild a small TSLA/TQQQ subset from raw DBN as a validation slice.
6. Compare old versus new quality distributions and labels.

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

### Phase 2: remove bootstrap proxy scores

1. Generate walk-forward 30-second setup predictions.
2. Store real setup probability, threshold, and margin.
3. Generate live-shaped micro-entry arms from those walk-forward predictions.
4. Store real entry probability, threshold, and margin.
5. Retrain lifecycle/micro models using those real values.
6. Make training fail by default if setup/entry score proxies are constant bootstrap values.

### Phase 3: event/state feature expansion

1. Add event bars and volume/dollar/trade-count bars.
2. Expand OPRA option-flow feature extraction.
3. Add cross-symbol market context.
4. Add rolling time-of-day-normalized liquidity and flow baselines.
5. Add sequence-model meta scores.

### Phase 4: target/label upgrade

1. Add expected-R and excursion labels.
2. Add time-to-target/time-to-stop labels.
3. Add exit hazard and hold-value labels.
4. Train companion models for probability, expected value, and risk.

### Phase 5: calibration and threshold overhaul

1. Calibrate model scores.
2. Track Brier score and expected calibration error.
3. Select thresholds by net R/PnL, drawdown, trade count, and stability.
4. Require stable threshold islands across folds, not a single lucky threshold.
5. Preserve calibration manifests with model exports.

### Phase 6: runtime/paper-trading gates

1. Validate feature schema hash.
2. Validate model hash.
3. Validate calibration hash.
4. Validate feature freshness and quote/spread quality.
5. Validate no threshold/probability violations in logs.
6. Monitor probability drift.
7. Run paper/shadow mode before symbol expansion.

## Immediate engineering task list

Recommended first pull requests on this branch:

1. Use `config/databento_dynamic_upgrade_pilot_symbols.csv` as the initial `pilot_core_5` cohort contract.
2. `build_30s_from_5s_csv.py`: fix aggregate quality flag semantics.
3. `scripts/audit_databento_source_data.py`: add source manifest/condition/hash audit.
4. `scripts/run_parallel_databento_build_20260523.sh`: make `EQUS_DIR`, `OPRA_DIR`, `PYTHON_BIN`, output root, and symbol file configurable for this machine.
5. Add a `pilot_core_5` rebuild script for a small validation slice covering `TSLA`, `TQQQ`, `NVDA`, `SPY`, and `QQQ`.
6. Add a data-quality comparison report script for old/new 1s/5s/30s datasets.
7. Add lifecycle/micro training guardrails that fail on constant bootstrap proxy scores unless explicitly overridden.
8. Add walk-forward setup-score generation for lifecycle/micro training.
9. Add calibration scorecard fields: raw score, calibrated probability, Brier score, ECE, net R, drawdown, trade count, and threshold stability.

## Promotion gates

A model bundle should not be promoted unless all of these are true:

- Source audit passes.
- Dataset manifest is complete.
- Degraded days are explicitly included/excluded and recorded.
- 5s/30s quality aggregation is fixed.
- No constant bootstrap `f_setup_score_proxy`.
- No constant bootstrap `f_entry_score_proxy`.
- Java runtime feature schema validation passes exactly.
- Model hashes and feature schema hashes are recorded.
- Calibration curves and metrics are recorded.
- Thresholds are stable across walk-forward folds.
- Backtest uses live-shaped arms and realistic fills/slippage.
- Holdout trade count is sufficient.
- No single day dominates profitability.
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
