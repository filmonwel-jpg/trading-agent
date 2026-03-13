import os

import numpy as np
import pandas as pd


REQUIRED_PRODUCER_COLUMNS = [
    'tsm_ret_30s_p50',
    'tsm_ret_120s_p50',
    'tsm_ret_30s_p10',
    'tsm_ret_30s_p90',
    'tsm_up_prob_30s',
    'tsm_vol_forecast_120s',
    'tsm_uncertainty',
    'regime_trend_prob',
    'regime_chop_prob',
    'regime_volatile_prob',
    'regime_transition_prob',
    'news_event_earnings',
    'news_event_analyst',
    'news_event_legal',
    'news_event_earnings_beat_miss',
    'news_event_analyst_upgrade_downgrade',
    'news_event_legal_regulatory',
    'news_event_product_capex',
    'news_event_macro_spillover',
    'news_novelty_score',
    'news_relevance_score',
    'news_embedding_cluster',
    'seq_lstm_up_prob_30s',
    'seq_tcn_up_prob_30s',
    'seq_transformer_up_prob_30s',
    'seq_patchtst_up_prob_30s',
    'seq_model_consensus_up_prob_30s',
    'setup_breakout_prob',
    'setup_pullback_continuation_prob',
    'setup_reversal_prob',
    'setup_trend_exhaustion_prob',
    'setup_failed_breakout_prob',
]

TIMESFM_BACKEND = os.getenv('TIMESFM_BACKEND', 'proxy').strip().lower()
SEQUENCE_BACKEND = os.getenv('SEQUENCE_BACKEND', 'proxy').strip().lower()
REGIME_ENSEMBLE_BACKEND = os.getenv('REGIME_ENSEMBLE_BACKEND', 'blend').strip().lower()
REGIME_SOURCE_WEIGHTS = os.getenv(
    'REGIME_SOURCE_WEIGHTS',
    'rf=1.0,lgbm=1.0,catboost=1.0,other=1.0',
).strip()
REGIME_SOURCE_WEIGHTS_TREND = os.getenv('REGIME_SOURCE_WEIGHTS_TREND', '').strip()
REGIME_SOURCE_WEIGHTS_CHOP = os.getenv('REGIME_SOURCE_WEIGHTS_CHOP', '').strip()
REGIME_SOURCE_WEIGHTS_VOLATILE = os.getenv('REGIME_SOURCE_WEIGHTS_VOLATILE', '').strip()
REGIME_BLEND_PROXY_WEIGHT = os.getenv('REGIME_BLEND_PROXY_WEIGHT', '0.35').strip()

REGIME_COLUMN_ALIASES = {
    'trend': [
        'RegimeRF_ProbTrend',
        'RegimeLGBM_ProbTrend',
        'RegimeCatBoost_ProbTrend',
        'RegimeProbTrend',
        'f_regime_prob_trend',
        'regime_trend_prob_model',
    ],
    'chop': [
        'RegimeRF_ProbChoppy',
        'RegimeLGBM_ProbChoppy',
        'RegimeCatBoost_ProbChoppy',
        'RegimeProbChoppy',
        'f_regime_prob_choppy',
        'regime_chop_prob_model',
    ],
    'volatile': [
        'RegimeRF_ProbVolatile',
        'RegimeLGBM_ProbVolatile',
        'RegimeCatBoost_ProbVolatile',
        'RegimeProbVolatile',
        'f_regime_prob_volatile',
        'regime_volatile_prob_model',
    ],
}


def _as_numeric(series, default=0.0, index=None):
    if isinstance(series, pd.Series):
        source = series
    elif index is not None:
        source = pd.Series(default if series is None else series, index=index)
        if len(source) != len(index):
            source = pd.Series([default if series is None else series] * len(index), index=index)
    else:
        source = pd.Series([default if series is None else series])
    return pd.to_numeric(source, errors='coerce').fillna(default).astype(float)


def _sigmoid(x):
    x = np.asarray(x, dtype=float)
    return 1.0 / (1.0 + np.exp(-np.clip(x, -40.0, 40.0)))


def _softmax_three(a, b, c):
    stacked = np.vstack([a, b, c]).T
    stacked = stacked - np.max(stacked, axis=1, keepdims=True)
    exp_v = np.exp(stacked)
    denom = np.sum(exp_v, axis=1, keepdims=True) + 1e-9
    out = exp_v / denom
    return out[:, 0], out[:, 1], out[:, 2]


def _safe_pct_change(series, periods=1):
    return series.pct_change(periods).replace([np.inf, -np.inf], np.nan)


def _to_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _parse_weight_map(raw):
    out = {'rf': 1.0, 'lgbm': 1.0, 'catboost': 1.0, 'other': 1.0}
    text = str(raw or '').strip()
    if not text:
        return out

    for token in text.split(','):
        piece = token.strip()
        if not piece or '=' not in piece:
            continue
        key, value = piece.split('=', 1)
        key = key.strip().lower()
        if key not in out:
            continue
        out[key] = max(0.0, _to_float(value.strip(), out[key]))
    return out


def _regime_source_bucket(col_name):
    name = str(col_name).strip().lower()
    if 'lgbm' in name or 'lightgbm' in name:
        return 'lgbm'
    if 'catboost' in name:
        return 'catboost'
    if 'rf' in name or 'randomforest' in name:
        return 'rf'
    return 'other'


_REGIME_WEIGHT_MAP = _parse_weight_map(REGIME_SOURCE_WEIGHTS)
_REGIME_CLASS_WEIGHT_MAPS = {
    'trend': _parse_weight_map(REGIME_SOURCE_WEIGHTS_TREND) if REGIME_SOURCE_WEIGHTS_TREND else None,
    'chop': _parse_weight_map(REGIME_SOURCE_WEIGHTS_CHOP) if REGIME_SOURCE_WEIGHTS_CHOP else None,
    'volatile': _parse_weight_map(REGIME_SOURCE_WEIGHTS_VOLATILE) if REGIME_SOURCE_WEIGHTS_VOLATILE else None,
}
_REGIME_BLEND_PROXY_WEIGHT = np.clip(_to_float(REGIME_BLEND_PROXY_WEIGHT, 0.5), 0.0, 1.0)


def _compute_regime_transition_from_probs(df):
    transition = (
        np.abs(np.diff(df['regime_trend_prob'], prepend=df['regime_trend_prob'].iloc[0]))
        + np.abs(np.diff(df['regime_chop_prob'], prepend=df['regime_chop_prob'].iloc[0]))
        + np.abs(np.diff(df['regime_volatile_prob'], prepend=df['regime_volatile_prob'].iloc[0]))
    ) / 2.0
    df['regime_transition_prob'] = np.clip(transition, 0.0, 1.0)
    return df


def _compute_regime_column_ensemble(df):
    grouped = {}
    for key, candidates in REGIME_COLUMN_ALIASES.items():
        weight_map = _REGIME_CLASS_WEIGHT_MAPS.get(key) or _REGIME_WEIGHT_MAP
        values = []
        weights = []
        for col in candidates:
            if col in df.columns:
                values.append(_as_numeric(df[col], 0.0, index=df.index).clip(0.0, 1.0).values)
                bucket = _regime_source_bucket(col)
                weights.append(weight_map.get(bucket, weight_map.get('other', 1.0)))
        if values:
            stacked = np.vstack(values)
            w = np.asarray(weights, dtype=float)
            if float(np.sum(w)) <= 0.0:
                grouped[key] = np.mean(stacked, axis=0)
            else:
                grouped[key] = np.average(stacked, axis=0, weights=w)

    if not grouped:
        return None

    trend = grouped.get('trend', np.zeros(len(df), dtype=float))
    chop = grouped.get('chop', np.zeros(len(df), dtype=float))
    volatile = grouped.get('volatile', np.zeros(len(df), dtype=float))

    stacked = np.vstack([trend, chop, volatile]).T
    row_sum = stacked.sum(axis=1, keepdims=True)
    safe = np.divide(stacked, row_sum, out=np.full_like(stacked, 1.0 / 3.0), where=row_sum > 1e-9)
    return safe[:, 0], safe[:, 1], safe[:, 2]


def _compute_regime_proxy_features(df):
    """Baseline proxy for regime classifier probabilities from trend/chop/vol structure."""
    close = _as_numeric(df.get('Close', 0.0), 0.0, index=df.index)
    high = _as_numeric(df.get('High', close), 0.0, index=df.index)
    low = _as_numeric(df.get('Low', close), 0.0, index=df.index)

    tr = np.maximum(high - low, np.maximum((high - close.shift(1)).abs(), (low - close.shift(1)).abs()))
    atr = tr.rolling(window=12, min_periods=4).mean().fillna(0.0)

    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    trend_raw = (ema_fast - ema_slow) / (atr + 1e-9)
    trend_score = _sigmoid(trend_raw)

    ret = close.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    vol_score = _sigmoid((ret.rolling(20, min_periods=6).std(ddof=1).fillna(0.0) - 0.0015) / 0.0008)

    net_move = close.diff(6).abs()
    gross_move = close.diff().abs().rolling(6, min_periods=3).sum()
    chop_raw = 1.0 - (net_move / (gross_move + 1e-9))
    chop_score = np.clip(chop_raw.fillna(0.5), 0.0, 1.0)

    trend_logit = 2.6 * (trend_score - 0.5)
    chop_logit = 2.2 * (chop_score - 0.5)
    vol_logit = 2.4 * (vol_score - 0.5)

    trend_p, chop_p, vol_p = _softmax_three(trend_logit, chop_logit, vol_logit)
    df['regime_trend_prob'] = np.clip(trend_p, 0.0, 1.0)
    df['regime_chop_prob'] = np.clip(chop_p, 0.0, 1.0)
    df['regime_volatile_prob'] = np.clip(vol_p, 0.0, 1.0)
    return _compute_regime_transition_from_probs(df)


def _compute_timesfm_proxy_features(df):
    close = _as_numeric(df.get('Close', 0.0), 0.0, index=df.index)

    ret_30s = _safe_pct_change(close)
    ret_120s = _safe_pct_change(close, periods=4)

    roll = ret_30s.rolling(window=40, min_periods=8)
    df['tsm_ret_30s_p50'] = roll.quantile(0.50).fillna(0.0)
    df['tsm_ret_30s_p10'] = roll.quantile(0.10).fillna(0.0)
    df['tsm_ret_30s_p90'] = roll.quantile(0.90).fillna(0.0)

    roll_120 = ret_120s.rolling(window=30, min_periods=6)
    df['tsm_ret_120s_p50'] = roll_120.quantile(0.50).fillna(0.0)

    up_prob = ret_30s.gt(0.0).astype(float).rolling(window=30, min_periods=6).mean().fillna(0.5)
    df['tsm_up_prob_30s'] = up_prob.clip(0.0, 1.0)

    vol_forecast = ret_120s.rolling(window=30, min_periods=6).std(ddof=1).fillna(0.0)
    df['tsm_vol_forecast_120s'] = np.clip(vol_forecast, 0.0, None)

    quantile_spread = (df['tsm_ret_30s_p90'] - df['tsm_ret_30s_p10']).abs()
    confidence_penalty = (df['tsm_up_prob_30s'] - 0.5).abs() * 2.0
    df['tsm_uncertainty'] = (0.60 * quantile_spread + 0.40 * df['tsm_vol_forecast_120s']) * (1.2 - confidence_penalty)
    df['tsm_uncertainty'] = np.clip(df['tsm_uncertainty'].fillna(0.0), 0.0, None)
    return df


def _compute_timesfm_column_features(df):
    mapping = {
        'TimesFM_Ret30s_p50': 'tsm_ret_30s_p50',
        'TimesFM_Ret120s_p50': 'tsm_ret_120s_p50',
        'TimesFM_Ret30s_p10': 'tsm_ret_30s_p10',
        'TimesFM_Ret30s_p90': 'tsm_ret_30s_p90',
        'TimesFM_UpProb30s': 'tsm_up_prob_30s',
        'TimesFM_VolForecast120s': 'tsm_vol_forecast_120s',
        'TimesFM_Uncertainty': 'tsm_uncertainty',
    }
    available = [src for src in mapping if src in df.columns]
    if len(available) < 4:
        return _compute_timesfm_proxy_features(df)

    for src, dest in mapping.items():
        if src in df.columns:
            df[dest] = _as_numeric(df[src], 0.0, index=df.index)
    if 'tsm_uncertainty' not in df.columns:
        spread = (_as_numeric(df.get('tsm_ret_30s_p90', 0.0), 0.0, index=df.index) - _as_numeric(df.get('tsm_ret_30s_p10', 0.0), 0.0, index=df.index)).abs()
        df['tsm_uncertainty'] = spread
    return df


def compute_time_series_foundation_features(df):
    """Foundation forecast meta-features with TimesFM-compatible fallback behavior."""
    if TIMESFM_BACKEND == 'columns':
        return _compute_timesfm_column_features(df)
    return _compute_timesfm_proxy_features(df)


def compute_regime_meta_features(df):
    proxy_df = _compute_regime_proxy_features(df.copy())

    if REGIME_ENSEMBLE_BACKEND not in ('columns', 'blend'):
        return proxy_df

    ensemble = _compute_regime_column_ensemble(df)
    if ensemble is None:
        return proxy_df

    trend_e, chop_e, vol_e = ensemble
    if REGIME_ENSEMBLE_BACKEND == 'columns':
        proxy_df['regime_trend_prob'] = trend_e
        proxy_df['regime_chop_prob'] = chop_e
        proxy_df['regime_volatile_prob'] = vol_e
        return _compute_regime_transition_from_probs(proxy_df)

    # blend mode: combine proxy structure with model-provided probabilities.
    proxy_w = float(_REGIME_BLEND_PROXY_WEIGHT)
    ensemble_w = float(1.0 - proxy_w)
    proxy_df['regime_trend_prob'] = proxy_w * proxy_df['regime_trend_prob'] + ensemble_w * trend_e
    proxy_df['regime_chop_prob'] = proxy_w * proxy_df['regime_chop_prob'] + ensemble_w * chop_e
    proxy_df['regime_volatile_prob'] = proxy_w * proxy_df['regime_volatile_prob'] + ensemble_w * vol_e

    stacked = np.vstack([
        proxy_df['regime_trend_prob'].values,
        proxy_df['regime_chop_prob'].values,
        proxy_df['regime_volatile_prob'].values,
    ]).T
    row_sum = stacked.sum(axis=1, keepdims=True)
    norm = np.divide(stacked, row_sum, out=np.full_like(stacked, 1.0 / 3.0), where=row_sum > 1e-9)
    proxy_df['regime_trend_prob'] = norm[:, 0]
    proxy_df['regime_chop_prob'] = norm[:, 1]
    proxy_df['regime_volatile_prob'] = norm[:, 2]
    return _compute_regime_transition_from_probs(proxy_df)


def compute_sentiment_event_features(df):
    """Baseline proxy for event and semantic-news outputs from warmup rollup columns."""
    news_60 = _as_numeric(df.get('NewsCount60s', 0.0), 0.0, index=df.index)
    news_300 = _as_numeric(df.get('NewsCount300s', 0.0), 0.0, index=df.index)
    providers = _as_numeric(df.get('NewsUniqueProviders300s', 0.0), 0.0, index=df.index)
    sentiment_latest = _as_numeric(df.get('SentimentLatest', 0.0), 0.0, index=df.index)
    sentiment_mean = _as_numeric(df.get('SentimentMean300s', 0.0), 0.0, index=df.index)
    sentiment_std = _as_numeric(df.get('SentimentStd300s', 0.0), 0.0, index=df.index)
    conf_mean = _as_numeric(df.get('SentimentConfidenceMean300s', 0.0), 0.0, index=df.index)
    conf_latest = _as_numeric(df.get('SentimentConfidenceLatest', 0.0), 0.0, index=df.index)
    lag_sec = _as_numeric(df.get('NewsAsOfLagSec', 999999.0), 999999.0, index=df.index)
    coverage = _as_numeric(df.get('NewsCoverage300s', 0.0), 0.0, index=df.index)

    freshness = np.exp(-np.clip(lag_sec, 0.0, 3600.0) / 300.0)
    novelty = np.clip((news_60 + 1.0) / (news_300 + 2.0), 0.0, 1.0) * freshness
    df['news_novelty_score'] = np.clip(0.6 * novelty + 0.4 * freshness, 0.0, 1.0)

    sentiment_jump = (sentiment_latest - sentiment_mean).abs()
    intensity = np.clip(news_60 / 6.0, 0.0, 1.0)
    conf_mix = np.clip(0.5 * conf_mean + 0.5 * conf_latest, 0.0, 1.0)

    earnings_col = _as_numeric(df.get('NewsEventEarningsBeatMiss300s', np.nan), np.nan, index=df.index)
    analyst_col = _as_numeric(df.get('NewsEventAnalystUpgradeDowngrade300s', np.nan), np.nan, index=df.index)
    legal_col = _as_numeric(df.get('NewsEventLegalRegulatory300s', np.nan), np.nan, index=df.index)
    product_col = _as_numeric(df.get('NewsEventProductCapex300s', np.nan), np.nan, index=df.index)
    macro_col = _as_numeric(df.get('NewsEventMacroSpillover300s', np.nan), np.nan, index=df.index)

    earnings_prob = _sigmoid(3.0 * intensity + 2.0 * sentiment_jump + 1.2 * conf_mix - 2.2)
    analyst_prob = _sigmoid(2.4 * np.clip(providers / 4.0, 0.0, 1.0) + 1.6 * conf_mix + 1.2 * intensity - 2.0)
    legal_prob = _sigmoid(3.0 * np.clip(-sentiment_latest, 0.0, 1.0) + 1.8 * sentiment_std + 1.5 * intensity - 2.0)
    product_prob = _sigmoid(2.4 * intensity + 1.2 * conf_mix + 0.8 * np.clip(sentiment_latest.abs(), 0.0, 1.0) - 1.8)
    macro_prob = _sigmoid(2.0 * intensity + 1.5 * np.clip(news_300 / 10.0, 0.0, 1.0) + 1.3 * sentiment_std - 2.0)

    df['news_event_earnings_beat_miss'] = np.where(np.isnan(earnings_col), earnings_prob, earnings_col).clip(0.0, 1.0)
    df['news_event_analyst_upgrade_downgrade'] = np.where(np.isnan(analyst_col), analyst_prob, analyst_col).clip(0.0, 1.0)
    df['news_event_legal_regulatory'] = np.where(np.isnan(legal_col), legal_prob, legal_col).clip(0.0, 1.0)
    df['news_event_product_capex'] = np.where(np.isnan(product_col), product_prob, product_col).clip(0.0, 1.0)
    df['news_event_macro_spillover'] = np.where(np.isnan(macro_col), macro_prob, macro_col).clip(0.0, 1.0)

    df['news_event_earnings'] = df['news_event_earnings_beat_miss']
    df['news_event_analyst'] = df['news_event_analyst_upgrade_downgrade']
    df['news_event_legal'] = df['news_event_legal_regulatory']

    relevance = (
        0.25 * freshness
        + 0.20 * np.clip(providers / 6.0, 0.0, 1.0)
        + 0.20 * np.clip(coverage, 0.0, 1.0)
        + 0.20 * conf_mix
        + 0.15 * np.maximum.reduce([
            df['news_event_earnings_beat_miss'],
            df['news_event_analyst_upgrade_downgrade'],
            df['news_event_legal_regulatory'],
            df['news_event_product_capex'],
            df['news_event_macro_spillover'],
        ])
    )
    df['news_relevance_score'] = np.clip(relevance, 0.0, 1.0)

    cluster_seed = np.floor((sentiment_latest + 1.0) * 2.0) + np.floor(np.clip(providers, 0.0, 4.0))
    df['news_embedding_cluster'] = np.mod(cluster_seed.astype(int), 8).astype(float)

    return df


def compute_sequence_model_features(df):
    """Proxy outputs shaped like LSTM/TCN/PatchTST/Transformer heads over rolling OHLCV windows."""
    if SEQUENCE_BACKEND == 'columns':
        column_mapping = {
            'SeqLSTM_UpProb30s': 'seq_lstm_up_prob_30s',
            'SeqTCN_UpProb30s': 'seq_tcn_up_prob_30s',
            'SeqTransformer_UpProb30s': 'seq_transformer_up_prob_30s',
            'SeqPatchTST_UpProb30s': 'seq_patchtst_up_prob_30s',
            'SeqModelConsensus_UpProb30s': 'seq_model_consensus_up_prob_30s',
        }
        copied = 0
        for src, dest in column_mapping.items():
            if src in df.columns:
                df[dest] = _as_numeric(df[src], 0.5, index=df.index).clip(0.0, 1.0)
                copied += 1

        if copied >= 4:
            if 'seq_model_consensus_up_prob_30s' not in df.columns:
                df['seq_model_consensus_up_prob_30s'] = (
                    df.get('seq_lstm_up_prob_30s', 0.5)
                    + df.get('seq_tcn_up_prob_30s', 0.5)
                    + df.get('seq_transformer_up_prob_30s', 0.5)
                    + df.get('seq_patchtst_up_prob_30s', 0.5)
                ) / 4.0
            return df

    close = _as_numeric(df.get('Close', 0.0), 0.0, index=df.index)
    volume = _as_numeric(df.get('Volume', 0.0), 0.0, index=df.index)
    ret_30s = _safe_pct_change(close).fillna(0.0)

    trend = ret_30s.rolling(window=12, min_periods=4).mean().fillna(0.0)
    vol = ret_30s.rolling(window=16, min_periods=6).std(ddof=1).fillna(0.0)
    vol_impulse = (volume / (volume.rolling(window=20, min_periods=6).median().fillna(1.0) + 1.0)) - 1.0

    df['seq_lstm_up_prob_30s'] = _sigmoid((trend / (vol + 1e-6)) * 0.8 + 0.3 * vol_impulse)
    df['seq_tcn_up_prob_30s'] = _sigmoid(0.9 * trend.shift(1).fillna(0.0) / (vol + 1e-6) + 0.2 * ret_30s)
    df['seq_transformer_up_prob_30s'] = _sigmoid(0.7 * trend + 0.5 * ret_30s.rolling(4, min_periods=2).mean().fillna(0.0))
    df['seq_patchtst_up_prob_30s'] = _sigmoid(0.5 * ret_30s.rolling(8, min_periods=3).sum().fillna(0.0) / (vol + 1e-6))

    if SEQUENCE_BACKEND not in ('proxy', ''):
        # Reserved for wiring external sequence inference outputs without changing feature names.
        pass

    df['seq_model_consensus_up_prob_30s'] = (
        df['seq_lstm_up_prob_30s']
        + df['seq_tcn_up_prob_30s']
        + df['seq_transformer_up_prob_30s']
        + df['seq_patchtst_up_prob_30s']
    ) / 4.0
    return df


def compute_chart_setup_features(df):
    """Baseline numeric setup probabilities derived from OHLCV sequence behavior."""
    close = _as_numeric(df.get('Close', 0.0), 0.0, index=df.index)
    open_ = _as_numeric(df.get('Open', close), 0.0, index=df.index)
    high = _as_numeric(df.get('High', close), 0.0, index=df.index)
    low = _as_numeric(df.get('Low', close), 0.0, index=df.index)

    prev_high_20 = high.rolling(window=20, min_periods=8).max().shift(1)
    prev_low_20 = low.rolling(window=20, min_periods=8).min().shift(1)

    tr = np.maximum(high - low, np.maximum((high - close.shift(1)).abs(), (low - close.shift(1)).abs()))
    atr = tr.rolling(window=14, min_periods=5).mean().fillna(0.0)
    atr_norm = atr / (close.abs() + 1e-9)

    body = (close - open_).abs() / (close.abs() + 1e-9)
    upper_wick = (high - pd.concat([open_, close], axis=1).max(axis=1)) / (close.abs() + 1e-9)
    lower_wick = (pd.concat([open_, close], axis=1).min(axis=1) - low) / (close.abs() + 1e-9)

    range_now = (high - low).clip(lower=0.0)
    range_mean = range_now.rolling(window=12, min_periods=4).mean().fillna(0.0)
    expansion = np.clip((range_now / (range_mean + 1e-9)) - 1.0, -2.0, 4.0)

    ema_fast = close.ewm(span=10, adjust=False).mean()
    ema_slow = close.ewm(span=24, adjust=False).mean()
    trend_dir = np.tanh((ema_fast - ema_slow) / (atr + 1e-9))

    breakout_raw = (
        ((close - prev_high_20) / (atr + 1e-9)).fillna(0.0)
        + 0.7 * expansion
        + 0.6 * body
        + 0.4 * np.clip(trend_dir, 0.0, 1.0)
    )
    df['setup_breakout_prob'] = np.clip(_sigmoid(2.2 * breakout_raw - 0.8), 0.0, 1.0)

    pullback_dist = ((ema_fast - low) / (atr + 1e-9)).fillna(0.0)
    continuation_raw = (
        1.1 * np.clip(trend_dir, 0.0, 1.0)
        + 0.8 * np.clip(pullback_dist, 0.0, 2.0)
        + 0.6 * np.clip((close - open_) / (atr + 1e-9), -2.0, 2.0)
    )
    df['setup_pullback_continuation_prob'] = np.clip(_sigmoid(continuation_raw - 0.6), 0.0, 1.0)

    reversal_raw = (
        0.9 * (upper_wick + lower_wick)
        + 0.8 * np.clip((close - prev_low_20) / (atr + 1e-9), -2.0, 2.0).abs()
        - 0.5 * body
    )
    df['setup_reversal_prob'] = np.clip(_sigmoid(2.0 * reversal_raw - 1.0), 0.0, 1.0)

    extension = (close - ema_slow).abs() / (atr + 1e-9)
    exhaustion_raw = 0.9 * np.clip(extension - 1.6, 0.0, 4.0) + 0.8 * (upper_wick + lower_wick) + 0.6 * atr_norm
    df['setup_trend_exhaustion_prob'] = np.clip(_sigmoid(1.7 * exhaustion_raw - 0.7), 0.0, 1.0)

    fell_back_inside = ((close < prev_high_20) & (close > prev_low_20)).astype(float)
    failed_raw = (
        1.2 * df['setup_breakout_prob']
        + 0.8 * fell_back_inside
        + 0.6 * df['setup_reversal_prob']
        - 0.7 * np.clip(trend_dir, 0.0, 1.0)
    )
    df['setup_failed_breakout_prob'] = np.clip(_sigmoid(failed_raw - 1.0), 0.0, 1.0)

    return df


def apply_all_feature_producers(df):
    producers = [
        compute_time_series_foundation_features,
        compute_regime_meta_features,
        compute_sentiment_event_features,
        compute_sequence_model_features,
        compute_chart_setup_features,
    ]
    out = df.copy()
    for producer in producers:
        out = producer(out)

    for col in REQUIRED_PRODUCER_COLUMNS:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0.0).astype(float)

    return out


