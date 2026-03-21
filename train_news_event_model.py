import argparse
import json
import pickle
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import average_precision_score, mean_absolute_error, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit


DEFAULT_BARS_CSV = Path(__file__).resolve().parent / 'TSLA_30Sec_Historical_Bulk_fromTrainer.csv'
DEFAULT_SOURCE_5S_CSV = Path(__file__).resolve().parent / 'TSLA_5Sec_Historical_Bulk_20260228_1558_clean.csv'
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent / 'news_event_model_outputs'
EVENT_SCORE_COLUMNS = [
    'relevance_score',
    'impact_score',
    'novelty_score',
    'directional_impulse',
    'alpha_up_prob_60s',
    'alpha_down_prob_60s',
    'alpha_up_prob_300s',
    'alpha_down_prob_300s',
    'alpha_expected_ret_60s_bps',
    'alpha_expected_ret_300s_bps',
    'volatility_shock_score',
]
EXPECTED_NEWS_COLUMNS = [
    'time', 'provider', 'provider_name', 'article_id', 'headline',
    'sentiment_score', 'sentiment_confidence', 'sentiment_label', 'sentiment_model',
    'event_label', 'event_confidence',
    'event_prob_earnings_beat_miss', 'event_prob_analyst_upgrade_downgrade',
    'event_prob_legal_regulatory', 'event_prob_product_capex', 'event_prob_macro_spillover',
    'published_ts', 'received_ts', 'tradable_ts', 'is_historical_seed',
    'source_raw', 'source_site',
    'dup_cluster_id', 'dup_seq_asof', 'dup_provider_count_asof', 'dup_first_seen_ts', 'dup_is_repeat',
]


def _parse_timestamp(series):
    ts_str = series.astype(str).str.strip()
    parsed = pd.to_datetime(ts_str, format='%Y%m%d %H:%M:%S America/New_York', errors='coerce')
    if parsed.isna().any():
        extracted = ts_str.str.extract(r'(\d{8}\s+\d{2}:\d{2}:\d{2})')[0]
        parsed = parsed.fillna(pd.to_datetime(extracted, format='%Y%m%d %H:%M:%S', errors='coerce'))
    if parsed.isna().any():
        parsed = parsed.fillna(pd.to_datetime(ts_str, errors='coerce'))
    return parsed


def _safe_numeric(series, default=0.0):
    return pd.to_numeric(series, errors='coerce').fillna(default).astype(float)


def _safe_bool(series):
    return series.astype(str).str.strip().str.lower().isin({'1', 'true', 'yes', 'y', 'on'}).astype(int)


def _infer_symbol(explicit_symbol, news_path, bars_path):
    if explicit_symbol:
        return str(explicit_symbol).strip().upper()
    for path in (Path(news_path), Path(bars_path)):
        stem = path.stem.strip()
        token = stem.split('_')[0].strip().upper()
        if token.isalpha() and 1 <= len(token) <= 6:
            return token
    return 'UNKNOWN'


def _ensure_bars_csv_available(bars_csv, source_5s_csv):
    bars_path = Path(bars_csv)
    if bars_path.exists():
        return bars_path
    source_5s_path = Path(source_5s_csv)
    if not source_5s_path.exists():
        raise FileNotFoundError(
            f'Bars CSV not found: {bars_path} and 5s fallback missing: {source_5s_path}'
        )
    from build_30s_from_5s_csv import build_30s_from_5s_csv
    print(f'>>> 30s bars missing, auto-building from {source_5s_path.name}...')
    build_30s_from_5s_csv(str(source_5s_path), str(bars_path), add_meta_features=False)
    if not bars_path.exists():
        raise FileNotFoundError(f'Failed to auto-build bars CSV: {bars_path}')
    return bars_path


def _filter_bars_to_regular_session(df):
    out = df.copy()
    out['Timestamp'] = _parse_timestamp(out['Timestamp'])
    if out['Timestamp'].isna().any():
        samples = out.loc[out['Timestamp'].isna(), 'Timestamp'].head(3).tolist()
        raise ValueError(f'Unable to parse bar timestamps. Examples: {samples}')
    hour = out['Timestamp'].dt.hour
    minute = out['Timestamp'].dt.minute
    mask = ((hour > 9) | ((hour == 9) & (minute >= 30))) & (hour < 16)
    out = out.loc[mask].copy().sort_values('Timestamp').reset_index(drop=True)
    out['Date'] = out['Timestamp'].dt.date
    out['MinuteOfDay'] = out['Timestamp'].dt.hour * 60 + out['Timestamp'].dt.minute
    return out


def load_bars_dataset(bars_csv):
    bars = pd.read_csv(bars_csv)
    required = {'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f'Bars CSV missing required columns: {sorted(missing)}')

    bars = _filter_bars_to_regular_session(bars)
    if bars.empty:
        raise ValueError('No regular-session rows available in bars CSV.')

    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        bars[col] = _safe_numeric(bars[col], 0.0)

    grouped = bars.groupby('Date', sort=False)
    bars['PrevClose'] = grouped['Close'].shift(1)
    bars['PrevVolume'] = grouped['Volume'].shift(1)
    bars['BarRetPrev30sBps'] = ((bars['Close'] / bars['PrevClose']) - 1.0) * 10000.0
    bars['BarRetPrev30sBps'] = bars['BarRetPrev30sBps'].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    bars['BarRangeBps'] = ((bars['High'] - bars['Low']) / bars['Close'].replace(0.0, np.nan)) * 10000.0
    bars['BarRangeBps'] = bars['BarRangeBps'].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    bars['BarVolumeLog'] = np.log1p(bars['Volume'].clip(lower=0.0))
    bars['BarVolumeChange'] = (bars['Volume'] / (bars['PrevVolume'] + 1.0)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    bars['BarVolumeChange'] = np.clip(bars['BarVolumeChange'], 0.0, 10.0)
    bars['SessionProgress'] = ((bars['MinuteOfDay'] - (9 * 60 + 30)) / (6.5 * 60)).clip(0.0, 1.0)
    bars['DateKey'] = bars['Date'].astype(str)

    for horizon_bars, label in [(2, '60s'), (10, '300s')]:
        bars[f'CloseFwd_{label}'] = grouped['Close'].shift(-horizon_bars)
        bars[f'Ret_{label}_bps'] = ((bars[f'CloseFwd_{label}'] / bars['Close']) - 1.0) * 10000.0
        bars[f'Ret_{label}_bps'] = bars[f'Ret_{label}_bps'].replace([np.inf, -np.inf], np.nan)

    future_abs_cols = []
    for step in range(1, 11):
        col = f'CloseFwdStep_{step}'
        bars[col] = grouped['Close'].shift(-step)
        future_abs_cols.append((((bars[col] / bars['Close']) - 1.0) * 10000.0).abs())
    bars['MaxAbsRet_300s_bps'] = pd.concat(future_abs_cols, axis=1).max(axis=1)
    bars['MaxAbsRet_300s_bps'] = bars['MaxAbsRet_300s_bps'].replace([np.inf, -np.inf], np.nan)

    keep_cols = [
        'Timestamp', 'Date', 'DateKey', 'MinuteOfDay', 'SessionProgress',
        'Close', 'BarRetPrev30sBps', 'BarRangeBps', 'BarVolumeLog', 'BarVolumeChange',
        'Ret_60s_bps', 'Ret_300s_bps', 'MaxAbsRet_300s_bps',
    ]
    return bars[keep_cols].copy()


def load_news_dataset(news_csv, symbol):
    path = Path(news_csv)
    if not path.exists():
        raise FileNotFoundError(f'News CSV not found: {path}')

    news = pd.read_csv(path)
    if news.empty:
        raise ValueError(f'News CSV is empty: {path}')

    for col in EXPECTED_NEWS_COLUMNS:
        if col not in news.columns:
            news[col] = ''

    news['published_ts'] = _parse_timestamp(news['published_ts'].where(news['published_ts'].astype(str).str.strip() != '', news['time']))
    news['received_ts'] = _parse_timestamp(news['received_ts'])
    news['tradable_ts'] = _parse_timestamp(news['tradable_ts'])
    computed_tradable = pd.concat([news['published_ts'], news['received_ts']], axis=1).max(axis=1)
    news['tradable_ts'] = news['tradable_ts'].where(news['tradable_ts'].notna(), computed_tradable)
    news = news[news['published_ts'].notna() & news['tradable_ts'].notna()].copy()
    if news.empty:
        raise ValueError('No parseable news timestamps found.')

    news['provider'] = news['provider'].astype(str).str.strip().replace('', 'unknown')
    news['provider_name'] = news['provider_name'].astype(str).str.strip()
    news['article_id'] = news['article_id'].astype(str).str.strip()
    news['headline'] = news['headline'].astype(str).fillna('').str.strip()
    news['source_site'] = news['source_site'].astype(str).str.strip().replace('', 'unknown')
    news['dup_cluster_id'] = news['dup_cluster_id'].astype(str).str.strip()
    news['symbol'] = symbol

    numeric_defaults = {
        'sentiment_score': 0.0,
        'sentiment_confidence': 0.0,
        'event_confidence': 0.0,
        'event_prob_earnings_beat_miss': 0.0,
        'event_prob_analyst_upgrade_downgrade': 0.0,
        'event_prob_legal_regulatory': 0.0,
        'event_prob_product_capex': 0.0,
        'event_prob_macro_spillover': 0.0,
        'dup_seq_asof': 0.0,
        'dup_provider_count_asof': 0.0,
    }
    for col, default in numeric_defaults.items():
        news[col] = _safe_numeric(news[col], default)

    news['is_historical_seed'] = _safe_bool(news['is_historical_seed'])
    news['dup_is_repeat'] = _safe_bool(news['dup_is_repeat'])
    news['latency_sec'] = (news['received_ts'] - news['published_ts']).dt.total_seconds().fillna(0.0).clip(lower=0.0)
    news['tradable_latency_sec'] = (news['tradable_ts'] - news['published_ts']).dt.total_seconds().fillna(0.0).clip(lower=0.0)
    news['event_date'] = news['tradable_ts'].dt.date
    news['DateKey'] = news['event_date'].astype(str)

    dedupe_key = np.where(
        news['article_id'].ne(''),
        news['provider'] + '|' + news['article_id'] + '|' + news['published_ts'].dt.strftime('%Y%m%d %H:%M:%S'),
        news['provider'] + '|' + news['headline'].str.lower() + '|' + news['published_ts'].dt.strftime('%Y%m%d %H:%M:%S'),
    )
    news = news.loc[~pd.Series(dedupe_key, index=news.index).duplicated()].copy()
    news = news.sort_values('tradable_ts').reset_index(drop=True)
    news['event_id'] = 'evt_' + news['provider'].str.lower() + '_' + news.index.astype(str)
    return news


def build_event_dataset(news_df, bars_df, up_60_bps=10.0, up_300_bps=20.0, vol_shock_bps=30.0):
    bars_sorted = bars_df.sort_values(['DateKey', 'Timestamp']).reset_index(drop=True)
    news_sorted = news_df.sort_values(['DateKey', 'tradable_ts']).reset_index(drop=True)

    aligned = pd.merge_asof(
        news_sorted,
        bars_sorted,
        left_on='tradable_ts',
        right_on='Timestamp',
        by='DateKey',
        direction='forward',
        allow_exact_matches=True,
    )
    aligned = aligned.rename(columns={'Timestamp': 'event_bar_ts', 'Close': 'event_bar_close'})
    aligned = aligned.dropna(subset=['event_bar_ts', 'Ret_60s_bps', 'Ret_300s_bps', 'MaxAbsRet_300s_bps']).copy()
    if aligned.empty:
        raise ValueError('No news events could be aligned to bars with future outcomes.')

    aligned['headline_word_count'] = aligned['headline'].str.split().str.len().fillna(0).astype(float)
    aligned['headline_char_count'] = aligned['headline'].str.len().fillna(0).astype(float)
    aligned['headline_upper_ratio'] = aligned['headline'].apply(
        lambda text: (
            sum(1 for ch in str(text) if ch.isupper())
            / max(1, sum(1 for ch in str(text) if ch.isalpha()))
        )
    ).astype(float)
    aligned['headline_digit_ratio'] = aligned['headline'].apply(
        lambda text: (
            sum(1 for ch in str(text) if ch.isdigit())
            / max(1, len(str(text)))
        )
    ).astype(float)
    aligned['headline_punct_count'] = aligned['headline'].str.count(r'[!?%$:]').fillna(0).astype(float)
    aligned['headline_has_quote_terms'] = aligned['headline'].str.contains(r'\b(?:upgrade|downgrade|buy|sell|target)\b', case=False, regex=True).fillna(False).astype(int)
    aligned['headline_has_earnings_terms'] = aligned['headline'].str.contains(r'\b(?:earnings|eps|revenue|guidance|quarter|beat|miss)\b', case=False, regex=True).fillna(False).astype(int)
    aligned['headline_has_legal_terms'] = aligned['headline'].str.contains(r'\b(?:lawsuit|probe|investigation|sec|doj|court|fine)\b', case=False, regex=True).fillna(False).astype(int)

    aligned['label_up_60s'] = (aligned['Ret_60s_bps'] >= float(up_60_bps)).astype(int)
    aligned['label_down_60s'] = (aligned['Ret_60s_bps'] <= -float(up_60_bps)).astype(int)
    aligned['label_up_300s'] = (aligned['Ret_300s_bps'] >= float(up_300_bps)).astype(int)
    aligned['label_down_300s'] = (aligned['Ret_300s_bps'] <= -float(up_300_bps)).astype(int)
    aligned['label_vol_shock_300s'] = (aligned['MaxAbsRet_300s_bps'] >= float(vol_shock_bps)).astype(int)
    return aligned.reset_index(drop=True)


def build_feature_matrix(event_df, top_k_provider=8, top_k_site=6):
    work = event_df.copy()
    numeric_cols = [
        'sentiment_score', 'sentiment_confidence', 'event_confidence',
        'event_prob_earnings_beat_miss', 'event_prob_analyst_upgrade_downgrade',
        'event_prob_legal_regulatory', 'event_prob_product_capex', 'event_prob_macro_spillover',
        'latency_sec', 'tradable_latency_sec', 'is_historical_seed',
        'dup_seq_asof', 'dup_provider_count_asof', 'dup_is_repeat',
        'headline_word_count', 'headline_char_count', 'headline_upper_ratio',
        'headline_digit_ratio', 'headline_punct_count',
        'headline_has_quote_terms', 'headline_has_earnings_terms', 'headline_has_legal_terms',
        'MinuteOfDay', 'SessionProgress', 'BarRetPrev30sBps', 'BarRangeBps', 'BarVolumeLog', 'BarVolumeChange',
    ]
    for col in numeric_cols:
        work[col] = _safe_numeric(work[col], 0.0)

    provider_top = work['provider'].value_counts().head(top_k_provider).index.tolist()
    site_top = work['source_site'].value_counts().head(top_k_site).index.tolist()
    work['provider_bucket'] = np.where(work['provider'].isin(provider_top), work['provider'], 'other')
    work['site_bucket'] = np.where(work['source_site'].isin(site_top), work['source_site'], 'other')
    work['event_bucket'] = work['event_label'].astype(str).str.strip().replace('', 'unknown')
    work['sentiment_bucket'] = work['sentiment_label'].astype(str).str.strip().replace('', 'neutral')

    cat = pd.get_dummies(
        work[['provider_bucket', 'site_bucket', 'event_bucket', 'sentiment_bucket']],
        prefix=['provider', 'site', 'event', 'sentiment'],
        dtype=float,
    )
    X = pd.concat([work[numeric_cols], cat], axis=1)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return X.astype(np.float32), list(X.columns)


def build_event_splits(dates, n_splits=5):
    day_series = pd.Series(pd.to_datetime(dates).astype(str)).reset_index(drop=True)
    unique_days = pd.Index(pd.unique(day_series))
    splits = []

    if len(unique_days) >= 3:
        test_days_per_fold = max(1, len(unique_days) // (n_splits + 1))
        for fold in range(1, n_splits + 1):
            test_start = fold * test_days_per_fold
            test_end = min(test_start + test_days_per_fold, len(unique_days))
            if test_start >= len(unique_days):
                continue
            train_days = unique_days[:test_start]
            test_days = unique_days[test_start:test_end]
            if len(train_days) == 0 or len(test_days) == 0:
                continue
            train_idx = day_series.index[day_series.isin(train_days)].to_numpy(dtype=np.int64)
            test_idx = day_series.index[day_series.isin(test_days)].to_numpy(dtype=np.int64)
            if len(train_idx) == 0 or len(test_idx) == 0:
                continue
            splits.append((train_idx, test_idx, f'days:{len(train_days)}->{len(test_days)}'))
        if splits:
            return splits

    if len(day_series) < 60:
        return []

    ts_splits = min(n_splits, max(2, len(day_series) // 80))
    for fold_idx, (train_idx, test_idx) in enumerate(TimeSeriesSplit(n_splits=ts_splits).split(np.arange(len(day_series))), start=1):
        splits.append((train_idx.astype(np.int64), test_idx.astype(np.int64), f'timeseries:{fold_idx}'))
    return splits


def _build_classifier(random_state):
    return RandomForestClassifier(
        n_estimators=320,
        max_depth=7,
        min_samples_split=16,
        min_samples_leaf=8,
        class_weight='balanced_subsample',
        random_state=random_state,
        n_jobs=-1,
    )


def _build_regressor(random_state):
    return RandomForestRegressor(
        n_estimators=320,
        max_depth=8,
        min_samples_split=14,
        min_samples_leaf=6,
        random_state=random_state,
        n_jobs=-1,
    )


def fit_classifier_oof(X, y, splits, name):
    y = np.asarray(y, dtype=np.int64)
    oof = np.full(len(y), np.nan, dtype=np.float32)
    metrics = []

    if len(np.unique(y)) < 2:
        return {
            'oof_pred': np.full(len(y), float(np.mean(y)), dtype=np.float32),
            'metrics': [{'warning': 'single_class_target'}],
            'model': None,
        }

    for fold_idx, (train_idx, test_idx, split_name) in enumerate(splits, start=1):
        y_train = y[train_idx]
        y_test = y[test_idx]
        if len(np.unique(y_train)) < 2 or len(np.unique(y_test)) < 2:
            continue
        model = _build_classifier(100 + fold_idx)
        model.fit(X.iloc[train_idx], y_train)
        pred = model.predict_proba(X.iloc[test_idx])[:, 1]
        oof[test_idx] = pred
        ap = average_precision_score(y_test, pred)
        try:
            auc = roc_auc_score(y_test, pred)
        except Exception:
            auc = float('nan')
        metrics.append({'split': split_name, 'average_precision': float(ap), 'roc_auc': float(auc)})

    fallback_model = _build_classifier(42)
    fallback_model.fit(X, y)
    missing = np.isnan(oof)
    if missing.any():
        oof[missing] = fallback_model.predict_proba(X.iloc[missing])[:, 1]

    return {
        'oof_pred': oof.astype(np.float32),
        'metrics': metrics,
        'model': fallback_model,
    }


def fit_regressor_oof(X, y, splits, name):
    y = np.asarray(y, dtype=np.float32)
    oof = np.full(len(y), np.nan, dtype=np.float32)
    metrics = []

    for fold_idx, (train_idx, test_idx, split_name) in enumerate(splits, start=1):
        model = _build_regressor(300 + fold_idx)
        model.fit(X.iloc[train_idx], y[train_idx])
        pred = model.predict(X.iloc[test_idx])
        oof[test_idx] = pred
        mae = mean_absolute_error(y[test_idx], pred)
        metrics.append({'split': split_name, 'mae': float(mae)})

    fallback_model = _build_regressor(242)
    fallback_model.fit(X, y)
    missing = np.isnan(oof)
    if missing.any():
        oof[missing] = fallback_model.predict(X.iloc[missing])

    return {
        'oof_pred': oof.astype(np.float32),
        'metrics': metrics,
        'model': fallback_model,
    }


def derive_event_scores(scored_df):
    out = scored_df.copy()
    max_event_family = np.maximum.reduce([
        out['event_prob_earnings_beat_miss'].values,
        out['event_prob_analyst_upgrade_downgrade'].values,
        out['event_prob_legal_regulatory'].values,
        out['event_prob_product_capex'].values,
        out['event_prob_macro_spillover'].values,
    ])
    dup_seq = np.clip(out['dup_seq_asof'].values, 0.0, None)
    dup_provider = np.clip(out['dup_provider_count_asof'].values, 0.0, None)
    repeat_penalty = out['dup_is_repeat'].values.astype(float)
    novelty = np.clip(
        0.55 * (1.0 / (1.0 + dup_seq))
        + 0.25 * (1.0 / (1.0 + dup_provider))
        + 0.20 * (1.0 - repeat_penalty),
        0.0,
        1.0,
    )
    ret60_norm = np.tanh(out['alpha_expected_ret_60s_bps'].values / 25.0)
    ret300_norm = np.tanh(out['alpha_expected_ret_300s_bps'].values / 40.0)
    directional = np.clip(
        0.45 * ret60_norm
        + 0.30 * (out['alpha_up_prob_60s'].values - out['alpha_down_prob_60s'].values)
        + 0.25 * out['sentiment_score'].values,
        -1.0,
        1.0,
    )
    impact = np.clip(
        0.35 * np.abs(ret300_norm)
        + 0.25 * out['volatility_shock_score'].values
        + 0.20 * np.maximum(out['alpha_up_prob_300s'].values, out['alpha_down_prob_300s'].values)
        + 0.10 * out['event_confidence'].values
        + 0.10 * max_event_family,
        0.0,
        1.0,
    )
    relevance = np.clip(
        0.35 * impact
        + 0.20 * novelty
        + 0.20 * out['sentiment_confidence'].values
        + 0.15 * out['event_confidence'].values
        + 0.10 * np.maximum(out['alpha_up_prob_60s'].values, out['alpha_down_prob_60s'].values),
        0.0,
        1.0,
    )

    out['novelty_score'] = novelty.astype(np.float32)
    out['directional_impulse'] = directional.astype(np.float32)
    out['impact_score'] = impact.astype(np.float32)
    out['relevance_score'] = relevance.astype(np.float32)
    return out


def score_events(event_df, X, feature_cols, splits):
    targets = {
        'alpha_up_prob_60s': ('classifier', event_df['label_up_60s'].values),
        'alpha_down_prob_60s': ('classifier', event_df['label_down_60s'].values),
        'alpha_up_prob_300s': ('classifier', event_df['label_up_300s'].values),
        'alpha_down_prob_300s': ('classifier', event_df['label_down_300s'].values),
        'alpha_expected_ret_60s_bps': ('regressor', event_df['Ret_60s_bps'].values),
        'alpha_expected_ret_300s_bps': ('regressor', event_df['Ret_300s_bps'].values),
        'volatility_shock_score': ('classifier', event_df['label_vol_shock_300s'].values),
    }

    scored = event_df.copy()
    model_bundle = {'feature_cols': feature_cols, 'trained_at': datetime.now().isoformat()}
    metrics = {}

    for output_col, (kind, target) in targets.items():
        print(f'>>> Training event head: {output_col} ({kind})')
        if kind == 'classifier':
            result = fit_classifier_oof(X, target, splits, output_col)
        else:
            result = fit_regressor_oof(X, target, splits, output_col)
        scored[output_col] = result['oof_pred']
        metrics[output_col] = result['metrics']
        model_bundle[output_col] = result['model']

    scored = derive_event_scores(scored)
    return scored, metrics, model_bundle


def build_metrics_summary(scored_df, metrics_by_head):
    summary = {
        'rows': int(len(scored_df)),
        'date_min': str(scored_df['event_date'].min()),
        'date_max': str(scored_df['event_date'].max()),
        'positive_rates': {
            'label_up_60s': float(scored_df['label_up_60s'].mean()),
            'label_down_60s': float(scored_df['label_down_60s'].mean()),
            'label_up_300s': float(scored_df['label_up_300s'].mean()),
            'label_down_300s': float(scored_df['label_down_300s'].mean()),
            'label_vol_shock_300s': float(scored_df['label_vol_shock_300s'].mean()),
        },
        'score_columns': EVENT_SCORE_COLUMNS,
        'heads': metrics_by_head,
    }
    return summary


def main():
    parser = argparse.ArgumentParser(
        description='Train a dedicated supervised news_event_model from historical news and labeled 30s bar outcomes.'
    )
    parser.add_argument('--news-csv', type=str, required=True, help='Historical news CSV written by harvester.py or equivalent.')
    parser.add_argument('--bars-csv', type=str, default=str(DEFAULT_BARS_CSV), help='30-second bars CSV used to label event outcomes.')
    parser.add_argument('--source-5s-csv', type=str, default=str(DEFAULT_SOURCE_5S_CSV), help='Optional 5s fallback to auto-build 30s bars if missing.')
    parser.add_argument('--symbol', type=str, default='', help='Optional symbol override for the event dataset.')
    parser.add_argument('--output-dir', type=str, default='', help='Optional custom output directory.')
    parser.add_argument('--up-threshold-60s-bps', type=float, default=10.0, help='Binary up/down label threshold for 60s horizon in bps.')
    parser.add_argument('--up-threshold-300s-bps', type=float, default=20.0, help='Binary up/down label threshold for 300s horizon in bps.')
    parser.add_argument('--vol-shock-threshold-bps', type=float, default=30.0, help='Binary volatility-shock threshold over 300s path in bps.')
    args = parser.parse_args()

    run_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(args.output_dir) if args.output_dir else (DEFAULT_OUTPUT_ROOT / run_tag)
    output_dir.mkdir(parents=True, exist_ok=True)

    bars_path = _ensure_bars_csv_available(args.bars_csv, args.source_5s_csv)
    symbol = _infer_symbol(args.symbol, args.news_csv, bars_path)

    print(f'>>> Building supervised news_event_model dataset for symbol={symbol}')
    print(f'>>> News source: {args.news_csv}')
    print(f'>>> Bars source: {bars_path}')

    bars_df = load_bars_dataset(bars_path)
    news_df = load_news_dataset(args.news_csv, symbol)
    event_df = build_event_dataset(
        news_df,
        bars_df,
        up_60_bps=args.up_threshold_60s_bps,
        up_300_bps=args.up_threshold_300s_bps,
        vol_shock_bps=args.vol_shock_threshold_bps,
    )

    feature_matrix, feature_cols = build_feature_matrix(event_df)
    feature_df = event_df.reset_index(drop=True).copy()
    splits = build_event_splits(feature_df['event_date'].values, n_splits=5)
    if splits:
        print(f'>>> Using {len(splits)} walk-forward splits for out-of-fold event scoring.')
    else:
        print('>>> Not enough temporal breadth for walk-forward folds; scoring via full-fit fallback where needed.')

    scored_df, metrics_by_head, model_bundle = score_events(feature_df, feature_matrix.reset_index(drop=True), feature_cols, splits)
    metrics_summary = build_metrics_summary(scored_df, metrics_by_head)

    dataset_out = output_dir / 'news_event_dataset.csv'
    scored_out = output_dir / 'scored_news_events.csv'
    metrics_out = output_dir / 'news_event_model_metrics.json'
    model_out = output_dir / 'news_event_model_bundle.pkl'

    dataset_cols = [
        'event_id', 'symbol', 'provider', 'provider_name', 'article_id', 'headline',
        'published_ts', 'received_ts', 'tradable_ts', 'event_bar_ts',
        'sentiment_score', 'sentiment_confidence', 'sentiment_label', 'sentiment_model',
        'event_label', 'event_confidence',
        'event_prob_earnings_beat_miss', 'event_prob_analyst_upgrade_downgrade',
        'event_prob_legal_regulatory', 'event_prob_product_capex', 'event_prob_macro_spillover',
        'latency_sec', 'tradable_latency_sec', 'dup_seq_asof', 'dup_provider_count_asof', 'dup_is_repeat',
        'Ret_60s_bps', 'Ret_300s_bps', 'MaxAbsRet_300s_bps',
        'label_up_60s', 'label_down_60s', 'label_up_300s', 'label_down_300s', 'label_vol_shock_300s',
    ]
    dataset_cols = [col for col in dataset_cols if col in scored_df.columns]
    scored_cols = [col for col in EXPECTED_NEWS_COLUMNS if col in scored_df.columns] + EVENT_SCORE_COLUMNS

    scored_df[dataset_cols].to_csv(dataset_out, index=False)
    scored_df[scored_cols].to_csv(scored_out, index=False)
    metrics_out.write_text(json.dumps(metrics_summary, indent=2))
    with open(model_out, 'wb') as fh:
        pickle.dump(model_bundle, fh)

    print('>>> News-event model training complete.')
    print(f'>>> Event dataset: {dataset_out}')
    print(f'>>> Scored events: {scored_out}')
    print(f'>>> Metrics: {metrics_out}')
    print(f'>>> Model bundle: {model_out}')
    print('>>> The scored events CSV can be passed directly to build_30s_from_5s_csv.py --news-csv.')
    print('>>> Zero-shot was intentionally not added in v1; this script trains a supervised event model from historical bar outcomes.')


if __name__ == '__main__':
    main()


