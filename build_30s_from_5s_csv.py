import argparse
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
}

MAX_COLUMNS = {
    'High',
    'NewsCount300s',
    'NewsCount60s',
    'NewsUniqueProviders300s',
    'NewsCoverage300s',
    'SpreadMaxBps5s',
    'ShortableMax5s',
    'MktReadyCount',
}

MIN_COLUMNS = {
    'Low',
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


def build_30s_from_5s_csv(input_csv, output_csv, add_meta_features=True):
    print(f'Loading {input_csv}...')
    raw = pd.read_csv(input_csv)
    df = _normalize_input_schema(raw)

    parsed_ts = _parse_timestamp(df['Timestamp'])
    df = df.assign(_timestamp=parsed_ts).sort_values('_timestamp').set_index('_timestamp')

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
    candidates = sorted(source_dir.glob('*warmup*.csv'))
    if candidates:
        return candidates[0]

    preferred = sorted(source_dir.glob('*_clean.csv'))
    if preferred:
        return preferred[0]

    all_csv = sorted(source_dir.glob('*.csv'))
    if all_csv:
        return all_csv[0]

    raise FileNotFoundError(f'No CSV files found in {source_dir}')


def main():
    parser = argparse.ArgumentParser(description='Convert enriched 5s warmup CSV into training-ready 30s CSV.')
    parser.add_argument('--input-csv', type=str, default='', help='Path to source 5s CSV.')
    parser.add_argument('--output-csv', type=str, default='', help='Path to output 30s CSV.')
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
    )


if __name__ == '__main__':
    main()
