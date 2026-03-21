import argparse
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

from feature_producers_30s import REQUIRED_PRODUCER_COLUMNS, apply_all_feature_producers


CORE_COLUMN_ORDER = [
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

    if news_csv:
        print(f'Applying standalone news enrichment from {news_csv}...')
        df = enrich_bars_with_news(df, news_csv)

    agg_map = {}
    for col in df.columns:
        if col == 'Timestamp':
            continue
        agg_map[col] = _build_agg_fn(df, col)

    print('Rolling 5s bars into 30s buckets with enriched schema support...')
    df_30s = df.resample('30s', closed='left', label='left').agg(agg_map)
    df_30s = df_30s.dropna(subset=['Close']).copy()

    # Keep timestamp formatting aligned with existing training/parser expectations.
    df_30s['Timestamp'] = df_30s.index.strftime('%Y%m%d %H:%M:%S') + ' America/New_York'

    if add_meta_features:
        df_30s = apply_all_feature_producers(df_30s)

    ordered_cols = _stable_column_order(df_30s)
    output_df = df_30s.reset_index(drop=True)[ordered_cols]

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)
    print(f'Success! Converted {len(df)} 5s bars into {len(output_df)} 30s bars.')
    print(f'Output columns: {len(output_df.columns)} (includes meta producer columns={add_meta_features})')


def pick_source_csv(source_dir: Path) -> Path:
    # Prefer stable historical clean source first so training stays backward-compatible
    # while live warmup harvesting schema continues to evolve.
    canonical_clean = source_dir / 'TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv'
    if canonical_clean.exists():
        return canonical_clean

    preferred = sorted(source_dir.glob('*_clean.csv'))
    if preferred:
        return preferred[0]

    candidates = sorted(source_dir.glob('*warmup*.csv'))
    if candidates:
        return candidates[0]

    all_csv = sorted(source_dir.glob('*.csv'))
    if all_csv:
        return all_csv[0]

    raise FileNotFoundError(f'No CSV files found in {source_dir}')


def main():
    parser = argparse.ArgumentParser(description='Convert enriched 5s warmup CSV into training-ready 30s CSV.')
    parser.add_argument('--input-csv', type=str, default='', help='Path to source 5s CSV.')
    parser.add_argument('--output-csv', type=str, default='', help='Path to output 30s CSV.')
    parser.add_argument('--news-csv', type=str, default='', help='Optional standalone live-news CSV to align onto bars before 30s aggregation.')
    parser.add_argument(
        '--no-meta-features',
        action='store_true',
        help='Disable post-aggregation feature producer columns.',
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    input_csv = Path(args.input_csv) if args.input_csv else pick_source_csv(repo_root)
    output_csv = Path(args.output_csv) if args.output_csv else (repo_root / 'TSLA_30Sec_Historical_Bulk_fromTrainer.csv')

    build_30s_from_5s_csv(
        str(input_csv),
        str(output_csv),
        add_meta_features=not args.no_meta_features,
        news_csv=args.news_csv,
    )


if __name__ == '__main__':
    main()
