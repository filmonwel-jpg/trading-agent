import argparse
from collections import deque
from pathlib import Path
import re

import numpy as np
import pandas as pd

from feature_producers_30s import REQUIRED_PRODUCER_COLUMNS, apply_all_feature_producers

try:
    import databento as db
except Exception:
    db = None


CORE_COLUMN_ORDER = [
    'Symbol',
    'Timestamp',
    'Open',
    'High',
    'Low',
    'Close',
    'Volume',
    'WAP',
    'Count',
    'YesterdayClose',
]

NEWS_BAR_COLUMNS = [
    'NewsCount300s',
    'NewsCount60s',
    'NewsUniqueProviders300s',
    'SentimentMean300s',
    'SentimentMin300s',
    'SentimentMax300s',
    'SentimentStd300s',
    'SentimentLatest',
    'SentimentConfidenceMean300s',
    'SentimentConfidenceLatest',
    'MinutesSinceNews',
    'NewsAsOfLagSec',
    'NewsCoverage300s',
    'NewsEventEarningsBeatMiss300s',
    'NewsEventAnalystUpgradeDowngrade300s',
    'NewsEventLegalRegulatory300s',
    'NewsEventProductCapex300s',
    'NewsEventMacroSpillover300s',
    'NewsRelevanceLatest',
    'NewsRelevanceMean300s',
    'NewsRelevanceMax300s',
    'NewsImpactLatest',
    'NewsImpactMean300s',
    'NewsImpactMax300s',
    'NewsNoveltyLatest',
    'NewsNoveltyMean300s',
    'NewsNoveltyMax300s',
    'NewsDirectionalImpulseLatest',
    'NewsDirectionalImpulseMean300s',
    'NewsDirectionalImpulseDecay300s',
    'NewsAbsDirectionalImpulseMax300s',
    'NewsAlphaUpProb60sLatest',
    'NewsAlphaDownProb60sLatest',
    'NewsAlphaUpProb300sLatest',
    'NewsAlphaDownProb300sLatest',
    'NewsAlphaUpProb60sMean300s',
    'NewsAlphaDownProb60sMean300s',
    'NewsAlphaUpProb300sMean300s',
    'NewsAlphaDownProb300sMean300s',
    'NewsAlphaExpectedRet60sBpsLatest',
    'NewsAlphaExpectedRet300sBpsLatest',
    'NewsAlphaExpectedRet60sBpsMean300s',
    'NewsAlphaExpectedRet300sBpsMean300s',
    'NewsAlphaExpectedRet60sBpsDecay300s',
    'NewsAlphaExpectedRet300sBpsDecay300s',
    'NewsVolatilityShockLatest',
    'NewsVolatilityShockMean300s',
    'NewsVolatilityShockMax300s',
    'SentimentModel',
]

EVENT_MODEL_INPUT_COLUMNS = {
    'relevance_score': 0.0,
    'impact_score': 0.0,
    'novelty_score': 0.0,
    'directional_impulse': 0.0,
    'alpha_up_prob_60s': 0.0,
    'alpha_down_prob_60s': 0.0,
    'alpha_up_prob_300s': 0.0,
    'alpha_down_prob_300s': 0.0,
    'alpha_expected_ret_60s_bps': 0.0,
    'alpha_expected_ret_300s_bps': 0.0,
    'volatility_shock_score': 0.0,
}

NEWS_DEFAULTS = {
    'NewsCount300s': 0,
    'NewsCount60s': 0,
    'NewsUniqueProviders300s': 0,
    'SentimentMean300s': 0.0,
    'SentimentMin300s': 0.0,
    'SentimentMax300s': 0.0,
    'SentimentStd300s': 0.0,
    'SentimentLatest': 0.0,
    'SentimentConfidenceMean300s': 0.0,
    'SentimentConfidenceLatest': 0.0,
    'MinutesSinceNews': 9999.0,
    'NewsAsOfLagSec': 999999.0,
    'NewsCoverage300s': 0.0,
    'NewsEventEarningsBeatMiss300s': 0.0,
    'NewsEventAnalystUpgradeDowngrade300s': 0.0,
    'NewsEventLegalRegulatory300s': 0.0,
    'NewsEventProductCapex300s': 0.0,
    'NewsEventMacroSpillover300s': 0.0,
    'NewsRelevanceLatest': 0.0,
    'NewsRelevanceMean300s': 0.0,
    'NewsRelevanceMax300s': 0.0,
    'NewsImpactLatest': 0.0,
    'NewsImpactMean300s': 0.0,
    'NewsImpactMax300s': 0.0,
    'NewsNoveltyLatest': 0.0,
    'NewsNoveltyMean300s': 0.0,
    'NewsNoveltyMax300s': 0.0,
    'NewsDirectionalImpulseLatest': 0.0,
    'NewsDirectionalImpulseMean300s': 0.0,
    'NewsDirectionalImpulseDecay300s': 0.0,
    'NewsAbsDirectionalImpulseMax300s': 0.0,
    'NewsAlphaUpProb60sLatest': 0.0,
    'NewsAlphaDownProb60sLatest': 0.0,
    'NewsAlphaUpProb300sLatest': 0.0,
    'NewsAlphaDownProb300sLatest': 0.0,
    'NewsAlphaUpProb60sMean300s': 0.0,
    'NewsAlphaDownProb60sMean300s': 0.0,
    'NewsAlphaUpProb300sMean300s': 0.0,
    'NewsAlphaDownProb300sMean300s': 0.0,
    'NewsAlphaExpectedRet60sBpsLatest': 0.0,
    'NewsAlphaExpectedRet300sBpsLatest': 0.0,
    'NewsAlphaExpectedRet60sBpsMean300s': 0.0,
    'NewsAlphaExpectedRet300sBpsMean300s': 0.0,
    'NewsAlphaExpectedRet60sBpsDecay300s': 0.0,
    'NewsAlphaExpectedRet300sBpsDecay300s': 0.0,
    'NewsVolatilityShockLatest': 0.0,
    'NewsVolatilityShockMean300s': 0.0,
    'NewsVolatilityShockMax300s': 0.0,
    'SentimentModel': 'none',
}

CATEGORICAL_COLUMNS = {
    'Timestamp',
    'AsOfTs',
    'SessionBucket',
    'SentimentModel',
    'DataQualityFlags',
}

SUM_COLUMNS = {
    'Volume',
    'Count',
    'QuoteUpdateCount5s',
    'TradePrintCount5s',
    'AtBidVol',
    'AtAskVol',
    'PutVolDelta5s',
    'CallVolDelta5s',
    'ShortableDelta5s',
}

MEAN_COLUMNS = {
    'Bid',
    'Ask',
    'BidSize',
    'AskSize',
    'SpreadBps',
    'L1Imbalance',
    'QuoteCoverage5s',
    'QuoteAgeMs',
    'ImbalanceStd5s',
    'SentimentMean300s',
    'SentimentStd300s',
    'SentimentConfidenceMean300s',
    'MktMeanRet5s',
    'MktDispersion5s',
    'FeatureCompleteness',
    'NewsEventEarningsBeatMiss300s',
    'NewsEventAnalystUpgradeDowngrade300s',
    'NewsEventLegalRegulatory300s',
    'NewsEventProductCapex300s',
    'NewsEventMacroSpillover300s',
    'NewsRelevanceMean300s',
    'NewsImpactMean300s',
    'NewsNoveltyMean300s',
    'NewsDirectionalImpulseMean300s',
    'NewsDirectionalImpulseDecay300s',
    'NewsAlphaUpProb60sMean300s',
    'NewsAlphaDownProb60sMean300s',
    'NewsAlphaUpProb300sMean300s',
    'NewsAlphaDownProb300sMean300s',
    'NewsAlphaExpectedRet60sBpsMean300s',
    'NewsAlphaExpectedRet300sBpsMean300s',
    'NewsAlphaExpectedRet60sBpsDecay300s',
    'NewsAlphaExpectedRet300sBpsDecay300s',
    'NewsVolatilityShockMean300s',
}

MAX_COLUMNS = {
    'High',
    'NewsCount300s',
    'NewsCount60s',
    'NewsUniqueProviders300s',
    'NewsCoverage300s',
    'SentimentMax300s',
    'NewsRelevanceMax300s',
    'NewsImpactMax300s',
    'NewsNoveltyMax300s',
    'NewsAbsDirectionalImpulseMax300s',
    'NewsVolatilityShockMax300s',
    'SpreadMaxBps5s',
    'ShortableMax5s',
    'MktReadyCount',
}

MIN_COLUMNS = {
    'Low',
    'SentimentMin300s',
    'ShortableMin5s',
    'SpreadMinBps5s',
    'MinutesSinceNews',
}

LAST_COLUMNS = {
    'Open',
    'Close',
    'WAP',
    'YesterdayClose',
    'BidLast',
    'AskLast',
    'BidSizeLast',
    'AskSizeLast',
    'PutVol',
    'CallVol',
    'ShortableShares',
    'AsOfTs',
    'BarEpochSec',
    'SessionBucket',
    'MinuteOfDay',
    'SecondsFromOpen',
    'SentimentLatest',
    'SentimentConfidenceLatest',
    'NewsAsOfLagSec',
    'NewsRelevanceLatest',
    'NewsImpactLatest',
    'NewsNoveltyLatest',
    'NewsDirectionalImpulseLatest',
    'NewsAlphaUpProb60sLatest',
    'NewsAlphaDownProb60sLatest',
    'NewsAlphaUpProb300sLatest',
    'NewsAlphaDownProb300sLatest',
    'NewsAlphaExpectedRet60sBpsLatest',
    'NewsAlphaExpectedRet300sBpsLatest',
    'NewsVolatilityShockLatest',
    'SentimentModel',
}

MARKET_TIMEZONE = 'America/New_York'
RTH_OPEN_MINUTE = 9 * 60 + 30
RTH_CLOSE_MINUTE = 16 * 60
SESSION_SECONDS = (RTH_CLOSE_MINUTE - RTH_OPEN_MINUTE) * 60
DBEQ_FILE_RE = re.compile(r'.*?(\d{8})\.tbbo\.dbn\.zst$')
OPRA_FILE_RE = re.compile(r'.*?(\d{8})\.ohlcv-1s\.dbn\.zst$')
OPRA_SYMBOL_RE = re.compile(r'^([A-Z]+)\s+(\d{6,8})([CP])\d+$')


def _parse_timestamp(ts_series):
    ts_str = ts_series.astype(str).str.strip()
    extracted = ts_str.str.extract(r'(\d{8}\s+\d{2}:\d{2}:\d{2})')[0]
    parsed = pd.to_datetime(extracted, format='%Y%m%d %H:%M:%S', errors='coerce')

    if parsed.isna().any():
        fallback = pd.to_datetime(ts_str, errors='coerce')
        parsed = parsed.fillna(fallback)

    if parsed.isna().any():
        bad_examples = ts_str[parsed.isna()].head(3).tolist()
        raise ValueError(f'Unable to parse Timestamp values. Examples: {bad_examples}')

    return parsed


def _quality_flag_union(series):
    tokens = []
    seen = set()
    for raw in series.dropna().astype(str):
        for token in raw.split('|'):
            token = token.strip()
            if not token or token == 'none' or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return 'none' if not tokens else '|'.join(tokens)


def _news_coverage_score(news_count_300s):
    return max(0.0, min(1.0, float(news_count_300s) / 5.0))


def _weighted_mean(values, weights, default=0.0):
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)
    if values.size == 0:
        return float(default)
    weight_sum = float(weights.sum())
    if weight_sum <= 0.0:
        return float(np.mean(values))
    return float(np.dot(values, weights) / weight_sum)


def _coerce_optional_timestamp(series):
    if series is None:
        return pd.Series(dtype='datetime64[ns]')
    parsed = _parse_timestamp(series)
    return parsed


def _load_news_events(news_csv):
    news_path = Path(news_csv)
    if not news_path.exists():
        raise FileNotFoundError(f'News CSV not found: {news_path}')

    news_df = pd.read_csv(news_path)
    if news_df.empty:
        return pd.DataFrame(columns=[
            'published_dt', 'tradable_dt', 'provider', 'sentiment_score', 'sentiment_confidence', 'sentiment_model',
            'event_prob_earnings_beat_miss', 'event_prob_analyst_upgrade_downgrade', 'event_prob_legal_regulatory',
            'event_prob_product_capex', 'event_prob_macro_spillover',
            *EVENT_MODEL_INPUT_COLUMNS.keys(),
        ])

    published_col = 'published_ts' if 'published_ts' in news_df.columns else 'time'
    published_dt = _coerce_optional_timestamp(news_df[published_col])
    received_dt = _coerce_optional_timestamp(news_df['received_ts']) if 'received_ts' in news_df.columns else pd.Series(pd.NaT, index=news_df.index)
    tradable_dt = _coerce_optional_timestamp(news_df['tradable_ts']) if 'tradable_ts' in news_df.columns else pd.Series(pd.NaT, index=news_df.index)

    computed_tradable = pd.concat([published_dt, received_dt], axis=1).max(axis=1)
    tradable_dt = tradable_dt.where(tradable_dt.notna(), computed_tradable)

    prepared = news_df.copy()
    prepared['published_dt'] = published_dt
    prepared['tradable_dt'] = tradable_dt
    prepared = prepared[prepared['published_dt'].notna() & prepared['tradable_dt'].notna()].copy()

    if 'dup_is_repeat' in prepared.columns:
        dup_mask = prepared['dup_is_repeat'].astype(str).str.strip().str.lower().isin({'1', 'true', 'yes', 'y'})
        prepared = prepared[~dup_mask].copy()

    provider = prepared.get('provider', pd.Series('', index=prepared.index)).astype(str).str.strip()
    article_id = prepared.get('article_id', pd.Series('', index=prepared.index)).astype(str).str.strip()
    headline = prepared.get('headline', pd.Series('', index=prepared.index)).astype(str).str.strip().str.lower()
    dedupe_key = np.where(
        article_id.ne(''),
        provider + '|' + article_id + '|' + prepared['published_dt'].dt.strftime('%Y%m%d %H:%M:%S'),
        provider + '|' + headline + '|' + prepared['published_dt'].dt.strftime('%Y%m%d %H:%M:%S'),
    )
    prepared = prepared.loc[~pd.Series(dedupe_key, index=prepared.index).duplicated()].copy()

    numeric_cols = [
        'sentiment_score',
        'sentiment_confidence',
        'event_prob_earnings_beat_miss',
        'event_prob_analyst_upgrade_downgrade',
        'event_prob_legal_regulatory',
        'event_prob_product_capex',
        'event_prob_macro_spillover',
        *EVENT_MODEL_INPUT_COLUMNS.keys(),
    ]
    for col in numeric_cols:
        if col not in prepared.columns:
            prepared[col] = EVENT_MODEL_INPUT_COLUMNS.get(col, 0.0)
        prepared[col] = pd.to_numeric(prepared[col], errors='coerce').fillna(EVENT_MODEL_INPUT_COLUMNS.get(col, 0.0))

    if 'sentiment_model' not in prepared.columns:
        prepared['sentiment_model'] = NEWS_DEFAULTS['SentimentModel']
    prepared['sentiment_model'] = prepared['sentiment_model'].astype(str).str.strip().replace('', NEWS_DEFAULTS['SentimentModel'])
    if 'provider' not in prepared.columns:
        prepared['provider'] = ''
    prepared['provider'] = prepared['provider'].astype(str).str.strip()

    prepared = prepared.sort_values(['tradable_dt', 'published_dt']).reset_index(drop=True)
    return prepared[[
        'published_dt', 'tradable_dt', 'provider', 'sentiment_score', 'sentiment_confidence', 'sentiment_model',
        'event_prob_earnings_beat_miss', 'event_prob_analyst_upgrade_downgrade', 'event_prob_legal_regulatory',
        'event_prob_product_capex', 'event_prob_macro_spillover',
        *EVENT_MODEL_INPUT_COLUMNS.keys(),
    ]]


def enrich_bars_with_news(df, news_csv):
    if not news_csv:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError('News enrichment expects a DatetimeIndex on the bar dataframe.')

    events_df = _load_news_events(news_csv)
    enriched = df.copy()

    if events_df.empty:
        for col, default in NEWS_DEFAULTS.items():
            enriched[col] = default
        return enriched

    event_rows = list(events_df.itertuples(index=False, name='NewsEvent'))
    active_events = deque()
    event_idx = 0
    last_news_dt = None
    result = {col: [] for col in NEWS_BAR_COLUMNS}

    for bar_dt in enriched.index:
        while event_idx < len(event_rows) and event_rows[event_idx].tradable_dt <= bar_dt:
            event = event_rows[event_idx]
            active_events.append(event)
            last_news_dt = event.published_dt
            event_idx += 1

        cutoff_300 = bar_dt - pd.Timedelta(seconds=300)
        cutoff_60 = bar_dt - pd.Timedelta(seconds=60)
        while active_events and active_events[0].tradable_dt < cutoff_300:
            active_events.popleft()

        eligible = list(active_events)
        if not eligible:
            minutes_since_news = NEWS_DEFAULTS['MinutesSinceNews']
            news_lag_sec = NEWS_DEFAULTS['NewsAsOfLagSec']
            if last_news_dt is not None:
                delta = bar_dt - last_news_dt
                minutes_since_news = max(0.0, delta.total_seconds() / 60.0)
                news_lag_sec = max(0.0, delta.total_seconds())

            row = dict(NEWS_DEFAULTS)
            row['MinutesSinceNews'] = minutes_since_news
            row['NewsAsOfLagSec'] = news_lag_sec
        else:
            scores = np.asarray([event.sentiment_score for event in eligible], dtype=float)
            confidences = np.asarray([event.sentiment_confidence for event in eligible], dtype=float)
            relevance = np.asarray([event.relevance_score for event in eligible], dtype=float)
            impact = np.asarray([event.impact_score for event in eligible], dtype=float)
            novelty = np.asarray([event.novelty_score for event in eligible], dtype=float)
            directional_impulse = np.asarray([event.directional_impulse for event in eligible], dtype=float)
            alpha_up_60s = np.asarray([event.alpha_up_prob_60s for event in eligible], dtype=float)
            alpha_down_60s = np.asarray([event.alpha_down_prob_60s for event in eligible], dtype=float)
            alpha_up_300s = np.asarray([event.alpha_up_prob_300s for event in eligible], dtype=float)
            alpha_down_300s = np.asarray([event.alpha_down_prob_300s for event in eligible], dtype=float)
            alpha_ret_60s = np.asarray([event.alpha_expected_ret_60s_bps for event in eligible], dtype=float)
            alpha_ret_300s = np.asarray([event.alpha_expected_ret_300s_bps for event in eligible], dtype=float)
            vol_shock = np.asarray([event.volatility_shock_score for event in eligible], dtype=float)
            latest_event = eligible[-1]
            delta = bar_dt - latest_event.published_dt
            age_seconds = np.asarray([
                max(0.0, (bar_dt - event.tradable_dt).total_seconds())
                for event in eligible
            ], dtype=float)
            decay_weights = np.exp(-np.clip(age_seconds, 0.0, 3600.0) / 300.0)

            row = {
                'NewsCount300s': int(len(eligible)),
                'NewsCount60s': int(sum(1 for event in eligible if event.published_dt >= cutoff_60)),
                'NewsUniqueProviders300s': int(len({event.provider for event in eligible if event.provider})),
                'SentimentMean300s': float(scores.mean()),
                'SentimentMin300s': float(scores.min()),
                'SentimentMax300s': float(scores.max()),
                'SentimentStd300s': float(scores.std(ddof=1)) if len(scores) > 1 else 0.0,
                'SentimentLatest': float(latest_event.sentiment_score),
                'SentimentConfidenceMean300s': float(confidences.mean()),
                'SentimentConfidenceLatest': float(latest_event.sentiment_confidence),
                'MinutesSinceNews': max(0.0, delta.total_seconds() / 60.0),
                'NewsAsOfLagSec': max(0.0, delta.total_seconds()),
                'NewsCoverage300s': _news_coverage_score(len(eligible)),
                'NewsEventEarningsBeatMiss300s': float(np.mean([event.event_prob_earnings_beat_miss for event in eligible])),
                'NewsEventAnalystUpgradeDowngrade300s': float(np.mean([event.event_prob_analyst_upgrade_downgrade for event in eligible])),
                'NewsEventLegalRegulatory300s': float(np.mean([event.event_prob_legal_regulatory for event in eligible])),
                'NewsEventProductCapex300s': float(np.mean([event.event_prob_product_capex for event in eligible])),
                'NewsEventMacroSpillover300s': float(np.mean([event.event_prob_macro_spillover for event in eligible])),
                'NewsRelevanceLatest': float(latest_event.relevance_score),
                'NewsRelevanceMean300s': float(relevance.mean()),
                'NewsRelevanceMax300s': float(relevance.max()),
                'NewsImpactLatest': float(latest_event.impact_score),
                'NewsImpactMean300s': float(impact.mean()),
                'NewsImpactMax300s': float(impact.max()),
                'NewsNoveltyLatest': float(latest_event.novelty_score),
                'NewsNoveltyMean300s': float(novelty.mean()),
                'NewsNoveltyMax300s': float(novelty.max()),
                'NewsDirectionalImpulseLatest': float(latest_event.directional_impulse),
                'NewsDirectionalImpulseMean300s': float(directional_impulse.mean()),
                'NewsDirectionalImpulseDecay300s': _weighted_mean(directional_impulse, decay_weights),
                'NewsAbsDirectionalImpulseMax300s': float(np.abs(directional_impulse).max()),
                'NewsAlphaUpProb60sLatest': float(latest_event.alpha_up_prob_60s),
                'NewsAlphaDownProb60sLatest': float(latest_event.alpha_down_prob_60s),
                'NewsAlphaUpProb300sLatest': float(latest_event.alpha_up_prob_300s),
                'NewsAlphaDownProb300sLatest': float(latest_event.alpha_down_prob_300s),
                'NewsAlphaUpProb60sMean300s': float(alpha_up_60s.mean()),
                'NewsAlphaDownProb60sMean300s': float(alpha_down_60s.mean()),
                'NewsAlphaUpProb300sMean300s': float(alpha_up_300s.mean()),
                'NewsAlphaDownProb300sMean300s': float(alpha_down_300s.mean()),
                'NewsAlphaExpectedRet60sBpsLatest': float(latest_event.alpha_expected_ret_60s_bps),
                'NewsAlphaExpectedRet300sBpsLatest': float(latest_event.alpha_expected_ret_300s_bps),
                'NewsAlphaExpectedRet60sBpsMean300s': float(alpha_ret_60s.mean()),
                'NewsAlphaExpectedRet300sBpsMean300s': float(alpha_ret_300s.mean()),
                'NewsAlphaExpectedRet60sBpsDecay300s': _weighted_mean(alpha_ret_60s, decay_weights),
                'NewsAlphaExpectedRet300sBpsDecay300s': _weighted_mean(alpha_ret_300s, decay_weights),
                'NewsVolatilityShockLatest': float(latest_event.volatility_shock_score),
                'NewsVolatilityShockMean300s': float(vol_shock.mean()),
                'NewsVolatilityShockMax300s': float(vol_shock.max()),
                'SentimentModel': latest_event.sentiment_model or NEWS_DEFAULTS['SentimentModel'],
            }

        for col in NEWS_BAR_COLUMNS:
            result[col].append(row[col])

    for col in NEWS_BAR_COLUMNS:
        enriched[col] = result[col]
    return enriched


def _build_agg_fn(df, col):
    if col == 'WAP':
        def weighted_wap(wap_series):
            idx = wap_series.index
            vols = pd.to_numeric(df.loc[idx, 'Volume'], errors='coerce').fillna(0.0)
            closes = pd.to_numeric(df.loc[idx, 'Close'], errors='coerce').ffill().fillna(0.0)
            wap_values = pd.to_numeric(wap_series, errors='coerce').fillna(closes)
            v_sum = vols.sum()
            if v_sum > 0:
                return float((wap_values * vols).sum() / v_sum)
            return float(closes.iloc[-1]) if len(closes) else np.nan

        return weighted_wap

    if col == 'DataQualityFlags':
        return _quality_flag_union

    if col in SUM_COLUMNS:
        return 'sum'
    if col in MEAN_COLUMNS:
        return 'mean'
    if col in MAX_COLUMNS:
        return 'max'
    if col in MIN_COLUMNS:
        return 'min'
    if col in LAST_COLUMNS:
        return 'last'

    if col.startswith('Mkt_'):
        if col.endswith('_Close5s') or col.endswith('_AsOfLagSec'):
            return 'last'
        if col.endswith('_NewsCount300s'):
            return 'max'
        if col.endswith(('_Ret5s', '_Ret30s', '_SpreadBps', '_L1Imbalance', '_SentimentMean300s')):
            return 'mean'
        return 'last'

    if col.endswith('Delta5s'):
        return 'sum'
    if col.endswith('Count5s'):
        return 'sum'
    if col.endswith('Coverage5s') or col.endswith('Std5s'):
        return 'mean'

    if pd.api.types.is_numeric_dtype(df[col]):
        return 'last'
    return 'last'


def _normalize_input_schema(df):
    required_price_cols = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing_required = [c for c in required_price_cols if c not in df.columns]
    if missing_required:
        raise ValueError(f'Missing required input columns: {missing_required}')

    normalized = df.copy()
    if 'WAP' not in normalized.columns:
        normalized['WAP'] = normalized['Close']
    if 'Count' not in normalized.columns:
        normalized['Count'] = 0
    if 'YesterdayClose' not in normalized.columns:
        normalized['YesterdayClose'] = pd.to_numeric(normalized['Close'], errors='coerce').shift(1)
        normalized['YesterdayClose'] = normalized['YesterdayClose'].ffill().fillna(normalized['Close'])

    numeric_candidates = [c for c in normalized.columns if c not in CATEGORICAL_COLUMNS]
    for col in numeric_candidates:
        if col == 'Timestamp':
            continue
        normalized[col] = pd.to_numeric(normalized[col], errors='coerce')

    if 'DataQualityFlags' not in normalized.columns:
        normalized['DataQualityFlags'] = 'none'

    return normalized


def _require_databento():
    if db is None:
        raise RuntimeError(
            'Databento DBN ingestion requires the databento package. '
            'Install it first, for example: pip install databento'
        )


def _parse_symbol_tokens(raw_text):
    if not raw_text:
        return []
    out = []
    seen = set()
    for token in str(raw_text).split(','):
        symbol = token.strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def _load_symbols_from_file(symbols_file):
    if not symbols_file:
        return []
    path = Path(symbols_file)
    if not path.exists():
        raise FileNotFoundError(f'Symbol file not found: {path}')

    out = []
    seen = set()
    for raw in path.read_text(encoding='utf-8').splitlines():
        symbol = raw.strip().upper()
        if not symbol or symbol.startswith('#') or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _resolve_symbol_universe(symbols='', symbols_file=''):
    explicit = _parse_symbol_tokens(symbols)
    if explicit:
        return explicit
    return _load_symbols_from_file(symbols_file)


def _extract_market_day(path, pattern):
    match = pattern.match(Path(path).name)
    if not match:
        raise ValueError(f'Unable to infer market day from filename: {path}')
    return match.group(1)


def _list_daily_databento_files(root_dir, pattern):
    root = Path(root_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f'Directory not found: {root}')

    out = {}
    for path in sorted(root.glob('*.dbn.zst')):
        try:
            market_day = _extract_market_day(path, pattern)
        except ValueError:
            continue
        out[market_day] = path
    if not out:
        raise FileNotFoundError(f'No Databento DBN files found in {root}')
    return out


def _market_day_to_session_index(market_day):
    start = pd.Timestamp(f'{market_day} 09:30:00', tz=MARKET_TIMEZONE)
    return pd.date_range(start=start, periods=SESSION_SECONDS, freq='1s')


def _filter_regular_session(ts_series):
    local = ts_series.dt.tz_convert(MARKET_TIMEZONE)
    minute_of_day = local.dt.hour * 60 + local.dt.minute
    mask = (minute_of_day >= RTH_OPEN_MINUTE) & (minute_of_day < RTH_CLOSE_MINUTE)
    return mask, local


def _session_bucket_values(index):
    minute_of_day = index.hour * 60 + index.minute
    return np.where(
        minute_of_day < 11 * 60,
        'open',
        np.where(minute_of_day < 15 * 60, 'midday', 'close'),
    )


def _extract_underlying_and_right(symbol_series):
    parsed = symbol_series.astype(str).str.upper().str.extract(OPRA_SYMBOL_RE)
    return parsed[0], parsed[2]


def _append_csv(df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = not output_path.exists()
    df.to_csv(output_path, mode='a', header=header, index=False)


def _format_intraday_output(df):
    working = df.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        raise ValueError('Expected a DatetimeIndex before formatting intraday output.')
    working['Timestamp'] = working.index.strftime('%Y%m%d %H:%M:%S') + f' {MARKET_TIMEZONE}'
    ordered_cols = _stable_column_order(working)
    return working.reset_index(drop=True)[ordered_cols]


def _aggregate_intraday_to_cadence(df, cadence, add_meta_features=False, news_csv=''):
    working = df.copy()
    if not isinstance(working.index, pd.DatetimeIndex):
        raise ValueError(f'Expected a DatetimeIndex before {cadence} aggregation.')

    if news_csv:
        working = enrich_bars_with_news(working, news_csv)

    agg_map = {}
    for col in working.columns:
        if col == 'Timestamp':
            continue
        agg_map[col] = _build_agg_fn(working, col)

    out = working.resample(cadence, closed='left', label='left').agg(agg_map)
    out = out.dropna(subset=['Close']).copy()

    if add_meta_features:
        out = apply_all_feature_producers(out)

    return _format_intraday_output(out)


def _aggregate_intraday_to_30s(df, add_meta_features=True, news_csv=''):
    return _aggregate_intraday_to_cadence(df, '30s', add_meta_features=add_meta_features, news_csv=news_csv)


def _aggregate_intraday_to_5s(df):
    return _aggregate_intraday_to_cadence(df, '5s', add_meta_features=False, news_csv='')


def _load_tbbo_second_bars(tbbo_path, symbol_filter=None):
    _require_databento()

    raw = db.DBNStore.from_file(tbbo_path).to_df().reset_index()
    if raw.empty:
        return {}

    raw['symbol'] = raw['symbol'].astype(str).str.strip().str.upper()
    if symbol_filter:
        raw = raw[raw['symbol'].isin(symbol_filter)].copy()
        if raw.empty:
            return {}

    mask, local_ts = _filter_regular_session(raw['ts_event'])
    raw = raw.loc[mask].copy()
    if raw.empty:
        return {}

    raw['ts_local'] = local_ts.loc[mask]
    raw['Timestamp'] = raw['ts_local'].dt.floor('1s')
    raw['price'] = pd.to_numeric(raw['price'], errors='coerce')
    raw['size'] = pd.to_numeric(raw['size'], errors='coerce').fillna(0.0)
    raw['bid_px_00'] = pd.to_numeric(raw['bid_px_00'], errors='coerce')
    raw['ask_px_00'] = pd.to_numeric(raw['ask_px_00'], errors='coerce')
    raw['bid_sz_00'] = pd.to_numeric(raw['bid_sz_00'], errors='coerce').fillna(0.0)
    raw['ask_sz_00'] = pd.to_numeric(raw['ask_sz_00'], errors='coerce').fillna(0.0)
    raw['has_quote'] = raw[['bid_px_00', 'ask_px_00']].notna().any(axis=1).astype(float)

    mid = (raw['bid_px_00'] + raw['ask_px_00']) / 2.0
    raw['spread_bps_row'] = np.where(
        mid.notna() & mid.ne(0.0),
        ((raw['ask_px_00'] - raw['bid_px_00']) / mid) * 10000.0,
        np.nan,
    )
    raw['l1_imbalance_row'] = (raw['bid_sz_00'] - raw['ask_sz_00']) / (raw['bid_sz_00'] + raw['ask_sz_00'] + 1.0)
    side = raw['side'].astype(str).str.upper()
    raw['at_bid_size'] = np.where(side.eq('B'), raw['size'], 0.0)
    raw['at_ask_size'] = np.where(side.eq('A'), raw['size'], 0.0)
    raw['px_x_sz'] = raw['price'].fillna(0.0) * raw['size']

    grouped = raw.groupby(['symbol', 'Timestamp'], sort=True)
    second_bars = grouped.agg(
        Open=('price', 'first'),
        High=('price', 'max'),
        Low=('price', 'min'),
        Close=('price', 'last'),
        Volume=('size', 'sum'),
        Count=('size', 'size'),
        px_x_sz=('px_x_sz', 'sum'),
        Bid=('bid_px_00', 'last'),
        Ask=('ask_px_00', 'last'),
        BidSize=('bid_sz_00', 'last'),
        AskSize=('ask_sz_00', 'last'),
        BidLast=('bid_px_00', 'last'),
        AskLast=('ask_px_00', 'last'),
        BidSizeLast=('bid_sz_00', 'last'),
        AskSizeLast=('ask_sz_00', 'last'),
        SpreadBps=('spread_bps_row', 'last'),
        SpreadMinBps5s=('spread_bps_row', 'min'),
        SpreadMaxBps5s=('spread_bps_row', 'max'),
        L1Imbalance=('l1_imbalance_row', 'last'),
        ImbalanceStd5s=('l1_imbalance_row', 'std'),
        QuoteUpdateCount5s=('has_quote', 'sum'),
        TradePrintCount5s=('size', 'size'),
        AtBidVol=('at_bid_size', 'sum'),
        AtAskVol=('at_ask_size', 'sum'),
    )
    second_bars['WAP'] = np.where(
        second_bars['Volume'].gt(0.0),
        second_bars['px_x_sz'] / second_bars['Volume'],
        second_bars['Close'],
    )
    second_bars = second_bars.drop(columns=['px_x_sz'])

    out = {}
    for symbol, symbol_df in second_bars.groupby(level=0, sort=True):
        out[symbol] = symbol_df.droplevel(0).sort_index()
    return out


def _regularize_second_bars(symbol_df, market_day, previous_close=None):
    idx = _market_day_to_session_index(market_day)
    out = symbol_df.reindex(idx)

    raw_quote_seen = out['QuoteUpdateCount5s'].fillna(0.0).gt(0.0)

    flow_cols = [
        'Volume', 'Count', 'AtBidVol', 'AtAskVol', 'TradePrintCount5s', 'QuoteUpdateCount5s',
    ]
    for col in flow_cols:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0.0)

    for col in ['BidSize', 'AskSize', 'BidSizeLast', 'AskSizeLast']:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors='coerce')

    for col in ['Bid', 'Ask', 'BidLast', 'AskLast']:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors='coerce')

    book_cols = ['Bid', 'Ask', 'BidSize', 'AskSize', 'BidLast', 'AskLast', 'BidSizeLast', 'AskSizeLast']
    # Forward-fill only: backfilling would leak future top-of-book state into
    # pre-first-quote seconds. Size columns can safely remain zero before the
    # first observed quote; price columns must remain NaN until observed.
    out[book_cols] = out[book_cols].ffill()
    size_cols = ['BidSize', 'AskSize', 'BidSizeLast', 'AskSizeLast']
    out[size_cols] = out[size_cols].fillna(0.0)

    mid = ((out['Bid'] + out['Ask']) / 2.0).where(out[['Bid', 'Ask']].notna().all(axis=1))
    mid = mid.fillna(out['Bid']).fillna(out['Ask'])
    base_close = pd.to_numeric(out['Close'], errors='coerce').combine_first(mid)
    if previous_close is not None:
        base_close = base_close.fillna(float(previous_close))
    base_close = base_close.ffill()

    if base_close.isna().all():
        return None

    for col in ['Open', 'High', 'Low', 'Close', 'WAP']:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors='coerce').combine_first(base_close)
        out[col] = out[col].ffill()

    out['SpreadBps'] = np.where(
        mid.notna() & mid.ne(0.0),
        ((out['Ask'] - out['Bid']) / mid) * 10000.0,
        pd.to_numeric(out.get('SpreadBps', 0.0), errors='coerce'),
    )
    out['SpreadBps'] = pd.to_numeric(out['SpreadBps'], errors='coerce').fillna(0.0)
    out['SpreadMinBps5s'] = pd.to_numeric(out.get('SpreadMinBps5s', out['SpreadBps']), errors='coerce').fillna(out['SpreadBps'])
    out['SpreadMaxBps5s'] = pd.to_numeric(out.get('SpreadMaxBps5s', out['SpreadBps']), errors='coerce').fillna(out['SpreadBps'])
    out['L1Imbalance'] = (
        (out['BidSize'].fillna(0.0) - out['AskSize'].fillna(0.0))
        / (out['BidSize'].fillna(0.0) + out['AskSize'].fillna(0.0) + 1.0)
    )
    out['ImbalanceStd5s'] = pd.to_numeric(out.get('ImbalanceStd5s', 0.0), errors='coerce').fillna(0.0)
    out['QuoteCoverage5s'] = raw_quote_seen.astype(float)

    last_quote_ts = pd.Series(pd.NaT, index=out.index, dtype=f'datetime64[ns, {MARKET_TIMEZONE}]')
    last_quote_ts.loc[raw_quote_seen] = out.index[raw_quote_seen]
    last_quote_ts = last_quote_ts.ffill()
    out['QuoteAgeMs'] = (
        (pd.Series(out.index, index=out.index) - last_quote_ts).dt.total_seconds() * 1000.0
    ).fillna(999999.0)

    minute_of_day = out.index.hour * 60 + out.index.minute
    out['YesterdayClose'] = float(previous_close) if previous_close is not None else float(base_close.iloc[0])
    out['AsOfTs'] = out.index.strftime('%Y%m%d %H:%M:%S') + f' {MARKET_TIMEZONE}'
    out['BarEpochSec'] = pd.Index(out.index).map(lambda ts: int(ts.tz_convert('UTC').timestamp())).astype(np.int64)
    out['SessionBucket'] = _session_bucket_values(out.index)
    out['MinuteOfDay'] = minute_of_day.astype(int)
    out['SecondsFromOpen'] = ((minute_of_day - RTH_OPEN_MINUTE) * 60 + out.index.second).astype(int)
    out['PutVol'] = 0.0
    out['CallVol'] = 0.0
    out['PutVolDelta5s'] = 0.0
    out['CallVolDelta5s'] = 0.0
    out['ShortableShares'] = 0.0
    out['ShortableDelta5s'] = 0.0
    out['ShortableMin5s'] = 0.0
    out['ShortableMax5s'] = 0.0
    out['MktReadyCount'] = 0.0

    synthetic_flags = []
    trade_missing = out['TradePrintCount5s'].eq(0)
    quote_missing = ~raw_quote_seen
    for no_trade, no_quote in zip(trade_missing.tolist(), quote_missing.tolist()):
        tokens = []
        if no_trade:
            tokens.append('no_trade')
        if no_quote:
            tokens.append('no_quote')
        if no_trade:
            tokens.append('synthetic_ohlc')
        synthetic_flags.append('none' if not tokens else '|'.join(tokens))
    out['DataQualityFlags'] = synthetic_flags

    feature_base = (
        0.45 * (~trade_missing).astype(float)
        + 0.35 * (~quote_missing).astype(float)
        + 0.20 * base_close.notna().astype(float)
    )
    out['FeatureCompleteness'] = np.clip(feature_base, 0.0, 1.0)
    return out


def _load_opra_second_option_deltas(opra_path, symbol_filter=None):
    _require_databento()

    raw = db.DBNStore.from_file(opra_path).to_df().reset_index()
    if raw.empty:
        return {}

    mask, local_ts = _filter_regular_session(raw['ts_event'])
    raw = raw.loc[mask].copy()
    if raw.empty:
        return {}

    raw['ts_local'] = local_ts.loc[mask]
    raw['Timestamp'] = raw['ts_local'].dt.floor('1s')
    raw['volume'] = pd.to_numeric(raw['volume'], errors='coerce').fillna(0.0)
    raw['underlying'], raw['option_right'] = _extract_underlying_and_right(raw['symbol'])
    raw = raw[raw['underlying'].notna()].copy()
    raw['underlying'] = raw['underlying'].astype(str).str.upper()
    raw['option_right'] = raw['option_right'].astype(str).str.upper()

    if symbol_filter:
        raw = raw[raw['underlying'].isin(symbol_filter)].copy()
        if raw.empty:
            return {}

    grouped = raw.groupby(['underlying', 'Timestamp', 'option_right'], sort=True)['volume'].sum().unstack().fillna(0.0)
    grouped = grouped.rename(columns={'C': 'CallVolDelta5s', 'P': 'PutVolDelta5s'})
    if 'CallVolDelta5s' not in grouped.columns:
        grouped['CallVolDelta5s'] = 0.0
    if 'PutVolDelta5s' not in grouped.columns:
        grouped['PutVolDelta5s'] = 0.0

    out = {}
    for symbol, symbol_df in grouped.groupby(level=0, sort=True):
        out[symbol] = symbol_df.droplevel(0)[['PutVolDelta5s', 'CallVolDelta5s']].sort_index()
    return out


def _merge_options_into_second_bars(second_df, option_df):
    out = second_df.copy()
    if option_df is not None and not option_df.empty:
        out = out.drop(columns=['PutVolDelta5s', 'CallVolDelta5s'], errors='ignore').join(option_df, how='left')

    for col in ['PutVolDelta5s', 'CallVolDelta5s']:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0.0)

    market_day = out.index.date[0] if len(out.index) else None
    if market_day is not None:
        local_dates = pd.Series(out.index.tz_convert(MARKET_TIMEZONE).date, index=out.index)
        out['PutVol'] = out.groupby(local_dates, sort=False)['PutVolDelta5s'].cumsum()
        out['CallVol'] = out.groupby(local_dates, sort=False)['CallVolDelta5s'].cumsum()
    else:
        out['PutVol'] = 0.0
        out['CallVol'] = 0.0
    return out


def _build_symbol_assessment(symbol_rows):
    if not symbol_rows:
        return pd.DataFrame(columns=[
            'Symbol', 'days', 'rows', 'median_volume_30s', 'median_trade_count_30s', 'median_spread_bps',
            'zero_volume_bar_frac', 'option_active_bar_frac', 'median_close', 'modeling_cohort', 'recommended_training_mode',
        ])

    report = pd.DataFrame(symbol_rows)
    grouped = report.groupby('Symbol', sort=True).agg(
        days=('market_day', 'nunique'),
        rows=('rows', 'sum'),
        median_volume_30s=('median_volume_30s', 'mean'),
        median_trade_count_30s=('median_trade_count_30s', 'mean'),
        median_spread_bps=('median_spread_bps', 'mean'),
        zero_volume_bar_frac=('zero_volume_bar_frac', 'mean'),
        option_active_bar_frac=('option_active_bar_frac', 'mean'),
        median_close=('median_close', 'mean'),
    ).reset_index()
    grouped = grouped.rename(columns={'Symbol': 'Symbol'})

    grouped['liquidity_score'] = (
        0.35 * grouped['median_volume_30s'].rank(pct=True)
        + 0.25 * grouped['median_trade_count_30s'].rank(pct=True)
        + 0.15 * grouped['option_active_bar_frac'].rank(pct=True)
        + 0.15 * (1.0 - grouped['zero_volume_bar_frac']).rank(pct=True)
        + 0.10 * grouped['median_spread_bps'].rank(pct=True, ascending=False)
    )

    specialized = (
        grouped['zero_volume_bar_frac'].ge(0.25)
        | grouped['median_trade_count_30s'].lt(2.0)
        | grouped['median_volume_30s'].lt(50.0)
    )
    mega_liquid = (~specialized) & grouped['liquidity_score'].ge(0.80)
    liquid = (~specialized) & (~mega_liquid) & grouped['liquidity_score'].ge(0.45)

    grouped['modeling_cohort'] = np.where(
        mega_liquid,
        'mega_liquid',
        np.where(liquid, 'liquid', 'specialized'),
    )
    grouped['recommended_training_mode'] = np.where(
        mega_liquid,
        'shared_core_plus_symbol_thresholds',
        np.where(liquid, 'shared_liquidity_cluster', 'per_symbol_or_small_cluster'),
    )
    return grouped.sort_values(['modeling_cohort', 'liquidity_score', 'Symbol'], ascending=[True, False, True]).reset_index(drop=True)


def build_30s_from_databento_dirs(
    dbeq_dir,
    opra_dir,
    output_dir,
    output_dir_5s='',
    output_dir_1s='',
    add_meta_features=True,
    combined_output_csv='',
    combined_output_csv_5s='',
    combined_output_csv_1s='',
    assessment_report='',
    symbols='',
    symbols_file='',
    max_days=0,
):
    symbol_universe = _resolve_symbol_universe(symbols=symbols, symbols_file=symbols_file)
    symbol_filter = set(symbol_universe) if symbol_universe else None

    dbeq_files = _list_daily_databento_files(dbeq_dir, DBEQ_FILE_RE)
    opra_files = _list_daily_databento_files(opra_dir, OPRA_FILE_RE)
    common_days = sorted(set(dbeq_files).intersection(opra_files))
    if not common_days:
        raise ValueError('No overlapping DBEQ/OPRA market days were found.')
    if max_days and int(max_days) > 0:
        common_days = common_days[-int(max_days):]

    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    output_root_5s = Path(output_dir_5s).expanduser().resolve() if output_dir_5s else None
    output_root_1s = Path(output_dir_1s).expanduser().resolve() if output_dir_1s else None
    if output_root_5s is not None:
        output_root_5s.mkdir(parents=True, exist_ok=True)
    if output_root_1s is not None:
        output_root_1s.mkdir(parents=True, exist_ok=True)

    combined_path = Path(combined_output_csv).expanduser().resolve() if combined_output_csv else None
    combined_path_5s = Path(combined_output_csv_5s).expanduser().resolve() if combined_output_csv_5s else None
    combined_path_1s = Path(combined_output_csv_1s).expanduser().resolve() if combined_output_csv_1s else None
    report_rows = []
    previous_close_by_symbol = {}

    for idx, market_day in enumerate(common_days, start=1):
        tbbo_path = dbeq_files[market_day]
        opra_path = opra_files[market_day]
        print(f'[{idx}/{len(common_days)}] Processing {market_day}: {tbbo_path.name} + {opra_path.name}')

        tbbo_by_symbol = _load_tbbo_second_bars(tbbo_path, symbol_filter=symbol_filter)
        opra_by_symbol = _load_opra_second_option_deltas(opra_path, symbol_filter=symbol_filter)

        if not tbbo_by_symbol:
            print(f'  - Skipping {market_day}: no TBBO rows for requested symbols in regular session.')
            continue

        for symbol in sorted(tbbo_by_symbol):
            previous_close = previous_close_by_symbol.get(symbol)
            second_bars = _regularize_second_bars(tbbo_by_symbol[symbol], market_day, previous_close=previous_close)
            if second_bars is None or second_bars.empty:
                continue

            second_bars = _merge_options_into_second_bars(second_bars, opra_by_symbol.get(symbol))

            if output_root_1s is not None or combined_path_1s is not None:
                bars_1s = _format_intraday_output(second_bars)
                bars_1s.insert(0, 'Symbol', symbol)
                if output_root_1s is not None:
                    _append_csv(bars_1s, output_root_1s / f'{symbol}_1s_training.csv')
                if combined_path_1s is not None:
                    _append_csv(bars_1s, combined_path_1s)

            if output_root_5s is not None or combined_path_5s is not None:
                bars_5s = _aggregate_intraday_to_5s(second_bars)
                if not bars_5s.empty:
                    bars_5s.insert(0, 'Symbol', symbol)
                    if output_root_5s is not None:
                        _append_csv(bars_5s, output_root_5s / f'{symbol}_5s_training.csv')
                    if combined_path_5s is not None:
                        _append_csv(bars_5s, combined_path_5s)

            bars_30s = _aggregate_intraday_to_30s(second_bars, add_meta_features=add_meta_features)
            if bars_30s.empty:
                continue

            bars_30s.insert(0, 'Symbol', symbol)
            symbol_output = output_root / f'{symbol}_30s_training.csv'
            _append_csv(bars_30s, symbol_output)
            if combined_path is not None:
                _append_csv(bars_30s, combined_path)

            report_rows.append({
                'Symbol': symbol,
                'market_day': market_day,
                'rows': int(len(bars_30s)),
                'median_volume_30s': float(pd.to_numeric(bars_30s['Volume'], errors='coerce').median()),
                'median_trade_count_30s': float(pd.to_numeric(bars_30s.get('TradePrintCount5s', 0.0), errors='coerce').median()),
                'median_spread_bps': float(pd.to_numeric(bars_30s.get('SpreadBps', 0.0), errors='coerce').median()),
                'zero_volume_bar_frac': float(pd.to_numeric(bars_30s['Volume'], errors='coerce').eq(0.0).mean()),
                'option_active_bar_frac': float(
                    (
                        pd.to_numeric(bars_30s.get('PutVolDelta5s', 0.0), errors='coerce').fillna(0.0)
                        + pd.to_numeric(bars_30s.get('CallVolDelta5s', 0.0), errors='coerce').fillna(0.0)
                    ).gt(0.0).mean()
                ),
                'median_close': float(pd.to_numeric(bars_30s['Close'], errors='coerce').median()),
            })
            previous_close_by_symbol[symbol] = float(pd.to_numeric(bars_30s['Close'], errors='coerce').iloc[-1])

    assessment_df = _build_symbol_assessment(report_rows)
    if assessment_report:
        report_path = Path(assessment_report).expanduser().resolve()
    else:
        report_path = output_root / 'symbol_model_plan.csv'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    assessment_df.to_csv(report_path, index=False)
    print(f'Wrote symbol assessment report to {report_path}')
    return assessment_df


def _stable_column_order(df):
    preferred = [c for c in CORE_COLUMN_ORDER if c in df.columns]
    producer_cols = [c for c in REQUIRED_PRODUCER_COLUMNS if c in df.columns]
    remaining = sorted([c for c in df.columns if c not in set(preferred + producer_cols)])
    return preferred + remaining + [c for c in producer_cols if c not in preferred + remaining]


def build_30s_from_5s_csv(input_csv, output_csv, add_meta_features=True, news_csv=''):
    print(f'Loading {input_csv}...')
    raw = pd.read_csv(input_csv)
    df = _normalize_input_schema(raw)

    parsed_ts = _parse_timestamp(df['Timestamp'])
    df = df.assign(_timestamp=parsed_ts).sort_values('_timestamp').set_index('_timestamp')

    print('Rolling sub-30s bars into 30s buckets with enriched schema support...')
    output_df = _aggregate_intraday_to_30s(df, add_meta_features=add_meta_features, news_csv=news_csv)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)
    print(f'Success! Converted {len(df)} source bars into {len(output_df)} 30s bars.')
    print(f'Output columns: {len(output_df.columns)} (includes meta producer columns={add_meta_features})')


def pick_source_csv(source_dir: Path, allow_warmup_source: bool = False) -> Path:
    # Prefer stable historical clean source first so training stays backward-compatible
    # while live warmup harvesting schema continues to evolve.
    canonical_clean = source_dir / 'TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv'
    if canonical_clean.exists():
        return canonical_clean

    preferred = sorted(source_dir.glob('*_clean.csv'))
    if preferred:
        return preferred[0]

    warmup_candidates = sorted(source_dir.glob('*warmup*.csv'))
    if allow_warmup_source and warmup_candidates:
        return warmup_candidates[0]

    all_csv = sorted(source_dir.glob('*.csv'))
    non_warmup_csv = [path for path in all_csv if 'warmup' not in path.name.lower()]
    if non_warmup_csv:
        return non_warmup_csv[0]
    if allow_warmup_source and warmup_candidates:
        return warmup_candidates[0]
    if warmup_candidates:
        raise FileNotFoundError(
            f'Only warmup CSV files were found in {source_dir}. '
            'Pass --allow-warmup-source to opt into using warmup history for 30s bar creation, '
            'or provide --input-csv explicitly.'
        )

    raise FileNotFoundError(f'No CSV files found in {source_dir}')


def main():
    parser = argparse.ArgumentParser(description='Convert enriched sub-30s source data into training-ready 30s CSV.')
    parser.add_argument('--input-csv', type=str, default='', help='Path to source 5s CSV.')
    parser.add_argument('--output-csv', type=str, default='', help='Path to output 30s CSV.')
    parser.add_argument('--news-csv', type=str, default='', help='Optional standalone live-news CSV to align onto bars before 30s aggregation.')
    parser.add_argument('--dbeq-dir', type=str, default='', help='Path to Databento equity TBBO download directory (for example EQUS.MINI or DBEQ.BASIC).')
    parser.add_argument('--opra-dir', type=str, default='', help='Path to Databento OPRA.PILLAR ohlcv-1s download directory.')
    parser.add_argument('--output-dir', type=str, default='', help='Directory for per-symbol 30s outputs when using Databento raw inputs.')
    parser.add_argument('--output-dir-5s', type=str, default='', help='Optional directory for per-symbol 5s combined equity+OPRA outputs in Databento mode.')
    parser.add_argument('--output-dir-1s', type=str, default='', help='Optional directory for per-symbol 1s combined equity+OPRA outputs in Databento mode.')
    parser.add_argument('--combined-output-csv', type=str, default='', help='Optional combined multi-symbol 30s CSV output path.')
    parser.add_argument('--combined-output-csv-5s', type=str, default='', help='Optional combined multi-symbol 5s CSV output path.')
    parser.add_argument('--combined-output-csv-1s', type=str, default='', help='Optional combined multi-symbol 1s CSV output path.')
    parser.add_argument('--assessment-report', type=str, default='', help='Optional symbol assessment/model-plan CSV path.')
    parser.add_argument('--symbols', type=str, default='', help='Comma-separated symbol subset to process in Databento mode.')
    parser.add_argument('--symbols-file', type=str, default='', help='Optional symbol universe file in Databento mode.')
    parser.add_argument('--max-days', type=int, default=0, help='Optional limit for the most recent N paired market days in Databento mode.')
    parser.add_argument(
        '--allow-warmup-source',
        action='store_true',
        help='Allow auto-discovery to fall back to *warmup*.csv inputs when no clean historical CSV is available.',
    )
    parser.add_argument(
        '--no-meta-features',
        action='store_true',
        help='Disable post-aggregation feature producer columns.',
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    if args.dbeq_dir or args.opra_dir:
        if not args.dbeq_dir or not args.opra_dir:
            raise ValueError('Databento mode requires both --dbeq-dir and --opra-dir.')

        output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (repo_root / 'training_data' / 'databento_30s')
        build_30s_from_databento_dirs(
            dbeq_dir=args.dbeq_dir,
            opra_dir=args.opra_dir,
            output_dir=str(output_dir),
            output_dir_5s=args.output_dir_5s,
            output_dir_1s=args.output_dir_1s,
            add_meta_features=not args.no_meta_features,
            combined_output_csv=args.combined_output_csv,
            combined_output_csv_5s=args.combined_output_csv_5s,
            combined_output_csv_1s=args.combined_output_csv_1s,
            assessment_report=args.assessment_report,
            symbols=args.symbols,
            symbols_file=args.symbols_file,
            max_days=args.max_days,
        )
        return

    input_csv = Path(args.input_csv) if args.input_csv else pick_source_csv(
        repo_root,
        allow_warmup_source=args.allow_warmup_source,
    )
    output_csv = Path(args.output_csv) if args.output_csv else (repo_root / 'TSLA_30Sec_Historical_Bulk_fromTrainer.csv')

    build_30s_from_5s_csv(
        str(input_csv),
        str(output_csv),
        add_meta_features=not args.no_meta_features,
        news_csv=args.news_csv,
    )


if __name__ == '__main__':
    main()
