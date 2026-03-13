import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _safe_pct_change(series, periods=1):
    return series.pct_change(periods).replace([np.inf, -np.inf], np.nan)


def _compute_proxy_timesfm_features(df):
    close = pd.to_numeric(df['Close'], errors='coerce').ffill().fillna(0.0)

    ret_30s = _safe_pct_change(close)
    ret_120s = _safe_pct_change(close, periods=4)

    roll = ret_30s.rolling(window=40, min_periods=8)
    df['TimesFM_Ret30s_p50'] = roll.quantile(0.50).fillna(0.0)
    df['TimesFM_Ret30s_p10'] = roll.quantile(0.10).fillna(0.0)
    df['TimesFM_Ret30s_p90'] = roll.quantile(0.90).fillna(0.0)

    roll_120 = ret_120s.rolling(window=30, min_periods=6)
    df['TimesFM_Ret120s_p50'] = roll_120.quantile(0.50).fillna(0.0)

    up_prob = ret_30s.gt(0.0).astype(float).rolling(window=30, min_periods=6).mean().fillna(0.5)
    df['TimesFM_UpProb30s'] = up_prob.clip(0.0, 1.0)

    vol_forecast = ret_120s.rolling(window=30, min_periods=6).std(ddof=1).fillna(0.0)
    df['TimesFM_VolForecast120s'] = np.clip(vol_forecast, 0.0, None)

    spread = (df['TimesFM_Ret30s_p90'] - df['TimesFM_Ret30s_p10']).abs()
    confidence = (df['TimesFM_UpProb30s'] - 0.5).abs() * 2.0
    df['TimesFM_Uncertainty'] = np.clip((0.60 * spread + 0.40 * df['TimesFM_VolForecast120s']) * (1.2 - confidence), 0.0, None)

    return df


def _init_timesfm_model(repo_id, context_len, horizon_len):
    import timesfm  # type: ignore

    if hasattr(timesfm, 'TimesFmHparams') and hasattr(timesfm, 'TimesFmCheckpoint'):
        hparams = timesfm.TimesFmHparams(
            context_len=context_len,
            horizon_len=horizon_len,
            backend='cpu',
            per_core_batch_size=16,
        )
        checkpoint = timesfm.TimesFmCheckpoint(huggingface_repo_id=repo_id)
        return timesfm.TimesFm(hparams=hparams, checkpoint=checkpoint)

    # Older API fallback.
    return timesfm.TimesFm(context_len=context_len, horizon_len=horizon_len, checkpoint=repo_id)


def _extract_forecast_components(result):
    point_fc = None
    quant_fc = None

    if isinstance(result, tuple):
        if len(result) >= 1:
            point_fc = result[0]
        if len(result) >= 2:
            quant_fc = result[1]
    elif isinstance(result, dict):
        point_fc = result.get('point_forecast')
        quant_fc = result.get('quantile_forecast')
    else:
        point_fc = result

    point_fc = np.asarray(point_fc, dtype=float)
    quant_fc = None if quant_fc is None else np.asarray(quant_fc, dtype=float)
    return point_fc, quant_fc


def _timesfm_forecast_row(model, history_values, horizon_len):
    history = np.asarray(history_values, dtype=np.float32)
    if len(history) < 8:
        return 0.0, 0.0, 0.0, 0.5, 0.0, 0.0

    try:
        output = model.forecast([history], freq=[0], horizon_len=horizon_len)
    except TypeError:
        output = model.forecast([history], [0])

    point_fc, quant_fc = _extract_forecast_components(output)
    if point_fc.ndim == 1:
        point = point_fc
    else:
        point = point_fc[0]

    if len(point) == 0:
        return 0.0, 0.0, 0.0, 0.5, 0.0, 0.0

    last_close = float(history[-1]) if float(history[-1]) != 0 else 1.0
    ret_first = (float(point[0]) / last_close) - 1.0
    ret_4 = (float(point[min(3, len(point) - 1)]) / last_close) - 1.0

    if quant_fc is not None and quant_fc.size > 0:
        q = quant_fc[0] if quant_fc.ndim >= 2 else quant_fc
        q = np.asarray(q, dtype=float)
        q10 = float(np.nanpercentile(q, 10)) if q.size else float(point[0])
        q50 = float(np.nanpercentile(q, 50)) if q.size else float(point[0])
        q90 = float(np.nanpercentile(q, 90)) if q.size else float(point[0])
        ret_p10 = (q10 / last_close) - 1.0
        ret_p50 = (q50 / last_close) - 1.0
        ret_p90 = (q90 / last_close) - 1.0
    else:
        sigma = float(np.std(point[: min(len(point), 8)], ddof=1)) if len(point) > 1 else 0.0
        ret_p50 = ret_first
        ret_p10 = ret_first - sigma / last_close
        ret_p90 = ret_first + sigma / last_close

    up_prob = float(np.mean(np.asarray(point) > last_close))
    vol_forecast = float(np.std(np.diff(np.asarray(point) / last_close), ddof=1)) if len(point) > 1 else 0.0
    uncertainty = abs(ret_p90 - ret_p10)
    return ret_p50, ret_4, ret_p10, ret_p90, up_prob, max(vol_forecast, uncertainty)


def compute_timesfm_features(df, repo_id, context_len=128, horizon_len=16):
    model = _init_timesfm_model(repo_id=repo_id, context_len=context_len, horizon_len=horizon_len)

    close = pd.to_numeric(df['Close'], errors='coerce').ffill().fillna(0.0)
    tsm_30s_p50 = np.zeros(len(df), dtype=float)
    tsm_120s_p50 = np.zeros(len(df), dtype=float)
    tsm_30s_p10 = np.zeros(len(df), dtype=float)
    tsm_30s_p90 = np.zeros(len(df), dtype=float)
    tsm_up_prob = np.full(len(df), 0.5, dtype=float)
    tsm_vol = np.zeros(len(df), dtype=float)
    tsm_unc = np.zeros(len(df), dtype=float)

    for i in range(len(df)):
        left = max(0, i - context_len)
        history = close.iloc[left:i].values
        if len(history) < 12:
            continue

        p50, r120, p10, p90, up_p, vol_u = _timesfm_forecast_row(model, history, horizon_len=horizon_len)
        tsm_30s_p50[i] = p50
        tsm_120s_p50[i] = r120
        tsm_30s_p10[i] = p10
        tsm_30s_p90[i] = p90
        tsm_up_prob[i] = np.clip(up_p, 0.0, 1.0)
        tsm_vol[i] = max(0.0, vol_u)
        tsm_unc[i] = max(0.0, abs(p90 - p10))

    df['TimesFM_Ret30s_p50'] = tsm_30s_p50
    df['TimesFM_Ret120s_p50'] = tsm_120s_p50
    df['TimesFM_Ret30s_p10'] = tsm_30s_p10
    df['TimesFM_Ret30s_p90'] = tsm_30s_p90
    df['TimesFM_UpProb30s'] = tsm_up_prob
    df['TimesFM_VolForecast120s'] = tsm_vol
    df['TimesFM_Uncertainty'] = tsm_unc
    return df


def parse_args():
    parser = argparse.ArgumentParser(description='Add TimesFM forecast meta-features to a 30s CSV.')
    parser.add_argument('--input-csv', required=True, type=str)
    parser.add_argument('--output-csv', required=True, type=str)
    parser.add_argument('--backend', choices=['proxy', 'timesfm'], default='proxy')
    parser.add_argument('--timesfm-repo', type=str, default='google/timesfm-2.0-500m-pytorch')
    parser.add_argument('--context-len', type=int, default=128)
    parser.add_argument('--horizon-len', type=int, default=16)
    parser.add_argument('--max-rows', type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()
    in_path = Path(args.input_csv)
    out_path = Path(args.output_csv)

    df = pd.read_csv(in_path)
    if args.max_rows and args.max_rows > 0:
        df = df.head(args.max_rows).copy()

    if 'Close' not in df.columns:
        raise ValueError('Input CSV must include Close column.')

    try:
        if args.backend == 'timesfm':
            df = compute_timesfm_features(
                df,
                repo_id=args.timesfm_repo,
                context_len=max(32, int(args.context_len)),
                horizon_len=max(4, int(args.horizon_len)),
            )
        else:
            df = _compute_proxy_timesfm_features(df)
    except Exception as exc:
        print(f'[TIMESFM] backend={args.backend} failed ({exc}); falling back to proxy.')
        df = _compute_proxy_timesfm_features(df)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f'[TIMESFM] wrote {out_path} rows={len(df)}')


if __name__ == '__main__':
    main()


