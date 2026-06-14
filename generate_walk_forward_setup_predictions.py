#!/usr/bin/env python3
"""Generate out-of-fold 30s setup predictions for lifecycle/micro training.

This is the C4 bridge between ``train_30s_models.py`` and
``train_lifecycle_micro_models.py``. It emits one row per prepared 30s training
bar and marks whether that row has true walk-forward setup probabilities.
Rows without enough prior days are kept for audit but have
``is_oof_setup_prediction=0`` and should be excluded from downstream
lifecycle/micro training by the fail-fast gate.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import train_30s_models as train30


BASE_FEATURE_COLS = [
    'f_dist_vwap', 'f_bb_lower_dist', 'f_bb_upper_dist', 'f_macd_diff',
    'f_body_size', 'f_lower_wick', 'f_upper_wick', 'f_atr_norm',
    'f_dist_sma', 'f_dist_high', 'f_dist_low', 'f_rsi', 'f_gap_from_prev_close',
    'f_time_of_day', 'f_dist_swing_high', 'f_dist_swing_low', 'f_is_new_high',
    'f_is_new_low', 'f_dist_whole_num', 'f_is_green', 'f_green_streak',
    'f_red_streak', 'f_put_call_ratio', 'f_vol_ask_ratio', 'f_vol_bid_ratio',
    'f_rel_volume_30s', 'f_realized_vol_20', 'f_realized_vol_z',
    'f_dist_or_high_atr', 'f_dist_or_low_atr',
]

EXTENDED_FEATURE_COLS = ['f_spread_pct', 'f_spread_z', 'f_l1_imbalance', 'f_signed_flow_30s']


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace('+00:00', 'Z')


def read_30s_input(path: Path) -> pd.DataFrame:
    path = path.expanduser()
    if path.is_dir():
        frames = []
        for csv_path in sorted(path.glob('*_30s_training.csv')):
            frame = pd.read_csv(csv_path)
            if 'Symbol' not in frame.columns:
                symbol = csv_path.name.removesuffix('_30s_training.csv').upper()
                frame.insert(0, 'Symbol', symbol)
            frames.append(frame)
        if not frames:
            raise FileNotFoundError(f'No *_30s_training.csv files found under {path}')
        return pd.concat(frames, ignore_index=True)
    return pd.read_csv(path)


def build_rolling_day_splits(dates: np.ndarray, min_train_days: int, test_days_per_fold: int, day_gap: int) -> list[tuple[np.ndarray, np.ndarray, int, int]]:
    day_series = pd.Series(dates).reset_index(drop=True)
    unique_days = pd.Index(pd.unique(day_series))
    splits: list[tuple[np.ndarray, np.ndarray, int, int]] = []
    min_train_days = max(1, int(min_train_days))
    test_days_per_fold = max(1, int(test_days_per_fold))
    day_gap = max(0, int(day_gap))

    test_start = min_train_days + day_gap
    while test_start < len(unique_days):
        train_end = max(0, test_start - day_gap)
        train_days = unique_days[:train_end]
        test_days = unique_days[test_start:test_start + test_days_per_fold]
        if len(train_days) > 0 and len(test_days) > 0:
            train_idx = day_series.index[day_series.isin(train_days)].to_numpy(dtype=np.int64)
            test_idx = day_series.index[day_series.isin(test_days)].to_numpy(dtype=np.int64)
            if len(train_idx) > 0 and len(test_idx) > 0:
                splits.append((train_idx, test_idx, len(train_days), len(test_days)))
        test_start += test_days_per_fold
    return splits


def feature_columns_for_frame(
    *,
    use_extended: bool,
    use_news: bool,
    use_meta: bool,
    use_regime_probs: bool,
) -> list[str]:
    feature_cols = list(BASE_FEATURE_COLS)
    if use_extended:
        feature_cols.extend(EXTENDED_FEATURE_COLS)
    if use_news:
        feature_cols.extend(train30.NEWS_BAR_FEATURE_COLS)
    if use_meta:
        feature_cols.extend(train30.META_PRODUCER_FEATURE_COLS)
    if use_regime_probs:
        feature_cols.extend(train30.REGIME_PROB_FEATURE_COLS)
    return feature_cols


def prepare_setup_frame(args: argparse.Namespace) -> tuple[pd.DataFrame, list[str]]:
    raw = read_30s_input(args.input_30s_csv)
    raw = train30.filter_raw_to_regular_session(raw)
    if raw.empty:
        raise ValueError('No regular-session 30s rows are available after filtering.')

    df = train30.calculate_features(raw)
    df = train30.generate_labels(df)
    df = train30.assign_market_regime(df)
    if args.exclude_before_10:
        df = train30.filter_after_opening_window(df)
    if df.empty:
        raise ValueError('No rows are available after feature/label preparation.')

    use_extended = args.use_extended_features
    use_news = not args.no_news_features
    use_meta = args.use_meta_features
    use_regime_probs = not args.no_regime_prob_features

    if use_news:
        df = train30.ensure_optional_numeric_columns(df, train30.NEWS_BAR_FEATURE_COLS, default_value=0.0)
    if use_meta:
        df = train30.ensure_optional_numeric_columns(df, train30.META_PRODUCER_FEATURE_COLS, default_value=0.0)
    feature_cols = feature_columns_for_frame(
        use_extended=use_extended,
        use_news=use_news,
        use_meta=use_meta,
        use_regime_probs=False,
    )
    if use_regime_probs:
        regime_cols = train30.build_regime_feature_subset(df, feature_cols)
        df = train30.add_regime_probability_features(df, regime_cols, model_family=args.regime_model_family)
        feature_cols = feature_columns_for_frame(
            use_extended=use_extended,
            use_news=use_news,
            use_meta=use_meta,
            use_regime_probs=True,
        )

    missing_features = [col for col in feature_cols if col not in df.columns]
    if missing_features:
        raise ValueError(f'Missing setup feature columns after preparation: {missing_features[:20]}')
    return df.sort_values(['Symbol', 'Timestamp']).reset_index(drop=True), feature_cols


def generate_side_oof_predictions(
    df: pd.DataFrame,
    feature_cols: list[str],
    label_col: str,
    side: str,
    *,
    model_family: str,
    min_train_days: int,
    test_days_per_fold: int,
    day_gap: int,
    min_train_positives: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    X = df[feature_cols].replace([np.inf, -np.inf], 0.0).fillna(0.0).to_numpy(dtype=np.float32)
    y = pd.to_numeric(df[label_col], errors='coerce').fillna(0).astype(np.int8).to_numpy()
    dates = df['Date'].to_numpy()
    splits = build_rolling_day_splits(dates, min_train_days=min_train_days, test_days_per_fold=test_days_per_fold, day_gap=day_gap)

    prob = np.full(len(df), np.nan, dtype=np.float32)
    threshold = np.full(len(df), np.nan, dtype=np.float32)
    fold_id = np.full(len(df), -1, dtype=np.int32)
    train_day_count = np.zeros(len(df), dtype=np.int32)
    test_day_count = np.zeros(len(df), dtype=np.int32)
    fold_rows = []

    for fold, (train_idx, test_idx, train_days, test_days) in enumerate(splits, start=1):
        y_train = y[train_idx]
        if len(np.unique(y_train)) < 2 or int(y_train.sum()) < min_train_positives:
            fold_rows.append({
                'side': side,
                'fold_id': fold,
                'status': 'skipped_insufficient_train_labels',
                'train_rows': int(len(train_idx)),
                'test_rows': int(len(test_idx)),
                'train_day_count': int(train_days),
                'test_day_count': int(test_days),
                'train_positives': int(y_train.sum()),
                'threshold': np.nan,
            })
            continue

        model = train30.build_classifier(model_family=model_family, random_state=1000 + fold, multi_class=False)
        model.fit(X[train_idx], y_train)

        calib_size = max(200, int(len(train_idx) * 0.2))
        calib_size = min(calib_size, len(train_idx) - 1)
        if calib_size > 0:
            cal_idx = train_idx[-calib_size:]
            cal_proba = train30.predict_positive_proba(model, X[cal_idx])
            thr, cal_prec, cal_rec, cal_pos = train30.optimize_threshold(y[cal_idx], cal_proba)
        else:
            thr, cal_prec, cal_rec, cal_pos = (train30.MIN_TUNED_THRESHOLD, 0.0, 0.0, 0.0)

        test_proba = train30.predict_positive_proba(model, X[test_idx]).astype(np.float32)
        prob[test_idx] = test_proba
        threshold[test_idx] = float(thr)
        fold_id[test_idx] = fold
        train_day_count[test_idx] = int(train_days)
        test_day_count[test_idx] = int(test_days)
        fold_rows.append({
            'side': side,
            'fold_id': fold,
            'status': 'ok',
            'train_rows': int(len(train_idx)),
            'test_rows': int(len(test_idx)),
            'train_day_count': int(train_days),
            'test_day_count': int(test_days),
            'train_positives': int(y_train.sum()),
            'test_positives': int(y[test_idx].sum()),
            'threshold': float(thr),
            'cal_precision': float(cal_prec),
            'cal_recall': float(cal_rec),
            'cal_pred_pos_rate': float(cal_pos),
        })

    side_df = pd.DataFrame({
        f'f_{side}_setup_prob': prob,
        f'{side}_setup_fold_id': fold_id,
        f'f_{side}_setup_threshold': threshold,
        f'f_{side}_setup_threshold_margin': prob - threshold,
        f'{side}_setup_train_day_count': train_day_count,
        f'{side}_setup_test_day_count': test_day_count,
        f'{side}_setup_oof_available': np.isfinite(prob).astype(np.int8),
    })
    summary = {
        'side': side,
        'label_col': label_col,
        'rows': int(len(df)),
        'positives': int(y.sum()),
        'fold_count': int(len(splits)),
        'folds_ok': int(sum(1 for row in fold_rows if row['status'] == 'ok')),
        'coverage_rows': int(np.isfinite(prob).sum()),
        'coverage_frac': float(np.isfinite(prob).mean()) if len(prob) else 0.0,
        'unique_probability_values': int(pd.Series(prob[np.isfinite(prob)]).round(8).nunique()) if np.isfinite(prob).any() else 0,
        'folds': fold_rows,
    }
    return side_df, summary


def generate_predictions(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    df, feature_cols = prepare_setup_frame(args)
    long_pred, long_summary = generate_side_oof_predictions(
        df,
        feature_cols,
        'Label_Long_Entry',
        'long',
        model_family=args.model_family,
        min_train_days=args.min_train_days,
        test_days_per_fold=args.test_days_per_fold,
        day_gap=args.day_gap,
        min_train_positives=args.min_train_positives,
    )
    short_pred, short_summary = generate_side_oof_predictions(
        df,
        feature_cols,
        'Label_Short_Entry',
        'short',
        model_family=args.model_family,
        min_train_days=args.min_train_days,
        test_days_per_fold=args.test_days_per_fold,
        day_gap=args.day_gap,
        min_train_positives=args.min_train_positives,
    )

    out = df[['Symbol', 'Timestamp', 'Date', 'Label_Long_Entry', 'Label_Short_Entry']].copy()
    out['Timestamp'] = out['Timestamp'].astype(str)
    out = pd.concat([out.reset_index(drop=True), long_pred, short_pred], axis=1)
    out['is_oof_setup_prediction'] = (
        out['long_setup_oof_available'].astype(int).eq(1)
        & out['short_setup_oof_available'].astype(int).eq(1)
    ).astype(np.int8)

    manifest = {
        'generated_at_utc': utc_now(),
        'input_30s_csv': str(Path(args.input_30s_csv).expanduser()),
        'output_csv': str(Path(args.output_csv).expanduser()),
        'row_count': int(len(out)),
        'trainable_oof_rows': int(out['is_oof_setup_prediction'].sum()),
        'trainable_oof_frac': float(out['is_oof_setup_prediction'].mean()) if len(out) else 0.0,
        'feature_count': int(len(feature_cols)),
        'feature_columns': feature_cols,
        'model_family': train30._normalize_model_family(args.model_family),
        'regime_model_family': train30._normalize_model_family(args.regime_model_family),
        'min_train_days': int(args.min_train_days),
        'test_days_per_fold': int(args.test_days_per_fold),
        'day_gap': int(args.day_gap),
        'exclude_before_10': bool(args.exclude_before_10),
        'long': long_summary,
        'short': short_summary,
        'errors': [],
        'warnings': [],
    }
    if manifest['trainable_oof_frac'] < args.min_oof_coverage_frac:
        manifest['errors'].append(
            f"OOF setup prediction coverage {manifest['trainable_oof_frac']:.3f} < required {args.min_oof_coverage_frac:.3f}"
        )
    for side_summary in [long_summary, short_summary]:
        if side_summary['unique_probability_values'] < args.min_unique_probabilities:
            manifest['errors'].append(
                f"{side_summary['side']} setup probabilities have only {side_summary['unique_probability_values']} unique values"
            )
    return out, manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-30s-csv', required=True, type=Path, help='Combined 30s CSV or directory of *_30s_training.csv files.')
    parser.add_argument('--output-csv', required=True, type=Path, help='Output CSV containing OOF setup predictions.')
    parser.add_argument('--manifest-json', type=Path, default=None, help='Optional manifest JSON path. Defaults next to output CSV.')
    parser.add_argument('--model-family', default=train30.MODEL_FAMILY)
    parser.add_argument('--regime-model-family', default=train30.REGIME_MODEL_FAMILY)
    parser.add_argument('--min-train-days', type=int, default=1)
    parser.add_argument('--test-days-per-fold', type=int, default=1)
    parser.add_argument('--day-gap', type=int, default=train30.DAY_GAP_BETWEEN_TRAIN_TEST)
    parser.add_argument('--min-train-positives', type=int, default=5)
    parser.add_argument('--min-oof-coverage-frac', type=float, default=0.50)
    parser.add_argument('--min-unique-probabilities', type=int, default=3)
    parser.add_argument('--exclude-before-10', action='store_true', help='Match base 30s non-open model training by excluding rows before 10:00 ET.')
    parser.add_argument('--use-extended-features', action='store_true')
    parser.add_argument('--no-news-features', action='store_true')
    parser.add_argument('--use-meta-features', action='store_true')
    parser.add_argument('--no-regime-prob-features', action='store_true')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_csv = args.output_csv.expanduser().resolve()
    manifest_path = args.manifest_json.expanduser().resolve() if args.manifest_json else output_csv.with_suffix('.manifest.json')
    predictions, manifest = generate_predictions(args)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(output_csv, index=False)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(f"WROTE {output_csv}")
    print(f"WROTE {manifest_path}")
    print(json.dumps({k: manifest[k] for k in ['row_count', 'trainable_oof_rows', 'trainable_oof_frac', 'errors', 'warnings']}, indent=2))
    return 2 if manifest['errors'] else 0


if __name__ == '__main__':
    raise SystemExit(main())

