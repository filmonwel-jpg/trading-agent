import argparse
import json
from dataclasses import dataclass

import numpy as np
import pandas as pd


SOURCE_KEYS = ['rf', 'lgbm', 'catboost', 'other']
CLASS_KEYS = ['chop', 'trend', 'volatile']
CLASS_TO_ID = {'chop': 0, 'trend': 1, 'volatile': 2}
ID_TO_CLASS = {v: k for k, v in CLASS_TO_ID.items()}

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


def _source_bucket(col_name):
    name = str(col_name).strip().lower()
    if 'lgbm' in name or 'lightgbm' in name:
        return 'lgbm'
    if 'catboost' in name:
        return 'catboost'
    if 'rf' in name or 'randomforest' in name:
        return 'rf'
    return 'other'


def _parse_target(df, target_col):
    if target_col not in df.columns:
        raise ValueError(f'target column not found: {target_col}')

    raw = df[target_col]
    if pd.api.types.is_numeric_dtype(raw):
        y = pd.to_numeric(raw, errors='coerce').fillna(-1).astype(int).values
    else:
        txt = raw.astype(str).str.strip().str.lower()
        map_txt = {
            'choppy': 0,
            'chop': 0,
            'trend': 1,
            'volatile': 2,
            'vol': 2,
        }
        y = txt.map(map_txt).fillna(-1).astype(int).values

    valid = (y >= 0) & (y <= 2)
    return y, valid


def _to_prob(series, n):
    if series is None:
        return np.full(n, np.nan, dtype=float)
    return pd.to_numeric(series, errors='coerce').astype(float).clip(0.0, 1.0).values


def _collect_source_matrices(df):
    n = len(df)
    out = {ck: {sk: np.full(n, np.nan, dtype=float) for sk in SOURCE_KEYS} for ck in CLASS_KEYS}

    for cls in CLASS_KEYS:
        for sk in SOURCE_KEYS:
            vals = []
            for col in REGIME_COLUMN_ALIASES[cls]:
                if col in df.columns and _source_bucket(col) == sk:
                    vals.append(_to_prob(df[col], n))
            if vals:
                stacked = np.vstack(vals)
                out[cls][sk] = np.nanmean(stacked, axis=0)
    return out


def _collect_proxy(df):
    candidates = {
        'trend': ['regime_trend_prob', 'RegimeProxyTrend'],
        'chop': ['regime_chop_prob', 'RegimeProxyChop'],
        'volatile': ['regime_volatile_prob', 'RegimeProxyVolatile'],
    }
    n = len(df)
    out = {}
    for cls in CLASS_KEYS:
        col = next((c for c in candidates[cls] if c in df.columns), None)
        out[cls] = _to_prob(df[col], n) if col else np.full(n, np.nan, dtype=float)
    return out


def _normalize_rows(arr):
    row_sum = np.sum(arr, axis=1, keepdims=True)
    return np.divide(arr, row_sum, out=np.full_like(arr, 1.0 / arr.shape[1]), where=row_sum > 1e-12)


def _combine(source_mat, proxy_mat, class_weights, blend_proxy_weight):
    n = len(next(iter(next(iter(source_mat.values())).values())))
    ens = np.zeros((n, 3), dtype=float)

    for j, cls in enumerate(CLASS_KEYS):
        vals = []
        wts = []
        for sk in SOURCE_KEYS:
            v = source_mat[cls][sk]
            w = float(class_weights[cls][sk])
            if np.any(np.isfinite(v)) and w > 0:
                vals.append(np.nan_to_num(v, nan=0.0))
                wts.append(w)

        if vals:
            stacked = np.vstack(vals)
            w_arr = np.asarray(wts, dtype=float)
            ens[:, j] = np.average(stacked, axis=0, weights=w_arr)
        else:
            ens[:, j] = 1.0 / 3.0

    ens = _normalize_rows(np.clip(ens, 0.0, 1.0))

    proxy = np.vstack([proxy_mat['chop'], proxy_mat['trend'], proxy_mat['volatile']]).T
    if np.any(np.isfinite(proxy)):
        proxy = _normalize_rows(np.clip(np.nan_to_num(proxy, nan=1.0 / 3.0), 0.0, 1.0))
        out = float(blend_proxy_weight) * proxy + (1.0 - float(blend_proxy_weight)) * ens
        out = _normalize_rows(np.clip(out, 0.0, 1.0))
        return out
    return ens


def _multiclass_logloss(y_true, proba):
    p = np.clip(proba[np.arange(len(y_true)), y_true], 1e-12, 1.0)
    return float(-np.mean(np.log(p)))


def _accuracy(y_true, proba):
    pred = np.argmax(proba, axis=1)
    return float(np.mean(pred == y_true))


def _rand_weight_map(rng):
    draw = rng.dirichlet(np.ones(len(SOURCE_KEYS)))
    return {k: float(v) for k, v in zip(SOURCE_KEYS, draw)}


def _weight_map_to_env(weight_map):
    return ','.join([f'{k}={weight_map[k]:.4f}' for k in SOURCE_KEYS])


@dataclass
class Candidate:
    logloss: float
    accuracy: float
    blend_proxy_weight: float
    class_weights: dict


def _search(source_mat, proxy_mat, y, trials, seed, min_class_count):
    rng = np.random.default_rng(seed)

    class_counts = {ck: int(np.sum(y == CLASS_TO_ID[ck])) for ck in CLASS_KEYS}
    per_class_allowed = {ck: class_counts[ck] >= min_class_count for ck in CLASS_KEYS}

    baseline = {ck: {sk: 1.0 for sk in SOURCE_KEYS} for ck in CLASS_KEYS}
    best = None

    def try_candidate(class_weights, blend):
        nonlocal best
        proba = _combine(source_mat, proxy_mat, class_weights, blend)
        ll = _multiclass_logloss(y, proba)
        acc = _accuracy(y, proba)
        cand = Candidate(logloss=ll, accuracy=acc, blend_proxy_weight=float(blend), class_weights=class_weights)
        if best is None or (cand.logloss < best.logloss - 1e-9) or (
            abs(cand.logloss - best.logloss) <= 1e-9 and cand.accuracy > best.accuracy
        ):
            best = cand

    try_candidate(baseline, 0.35)

    for _ in range(max(1, trials)):
        global_w = _rand_weight_map(rng)
        class_w = {}
        for ck in CLASS_KEYS:
            if per_class_allowed[ck]:
                # 60% chance use global, 40% chance class-specific sample.
                class_w[ck] = global_w if rng.uniform() < 0.6 else _rand_weight_map(rng)
            else:
                class_w[ck] = global_w
        blend = float(rng.uniform(0.0, 1.0))
        try_candidate(class_w, blend)

    return best, per_class_allowed


def _build_payload(best, per_class_allowed):
    avg_map = {}
    for sk in SOURCE_KEYS:
        avg_map[sk] = float(np.mean([best.class_weights[ck][sk] for ck in CLASS_KEYS]))

    env_lines = [
        'export REGIME_ENSEMBLE_BACKEND=blend',
        f'export REGIME_BLEND_PROXY_WEIGHT={best.blend_proxy_weight:.4f}',
        f'export REGIME_SOURCE_WEIGHTS="{_weight_map_to_env(avg_map)}"',
    ]

    for ck in CLASS_KEYS:
        if per_class_allowed[ck]:
            suffix = 'CHOP' if ck == 'chop' else ck.upper()
            env_lines.append(f'export REGIME_SOURCE_WEIGHTS_{suffix}="{_weight_map_to_env(best.class_weights[ck])}"')

    payload = {
        'logloss': best.logloss,
        'accuracy': best.accuracy,
        'blend_proxy_weight': best.blend_proxy_weight,
        'weights': best.class_weights,
        'global_weights': avg_map,
        'per_class_allowed': per_class_allowed,
        'env_exports': env_lines,
    }
    return payload


def _print_result(payload):
    best_logloss = payload['logloss']
    best_accuracy = payload['accuracy']
    blend_proxy_weight = payload['blend_proxy_weight']

    print('=== Best Regime Ensemble Weights ===')
    print(f'logloss={best_logloss:.6f} accuracy={best_accuracy:.4%}')
    print(f'blend_proxy_weight={blend_proxy_weight:.4f}')

    print('\nSuggested env:')
    for line in payload['env_exports']:
        print(line)

    print('\nJSON:')
    print(json.dumps(payload, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Tune regime ensemble weights from labeled CSV.')
    parser.add_argument('--input-csv', required=True)
    parser.add_argument('--target-col', default='RegimeLabel')
    parser.add_argument('--trials', type=int, default=2500)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--min-class-count', type=int, default=120)
    parser.add_argument('--json-out', type=str, default='')
    parser.add_argument('--env-out', type=str, default='')
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)
    y_raw, valid = _parse_target(df, args.target_col)
    if int(np.sum(valid)) < 200:
        raise ValueError('Not enough valid labeled rows to tune weights (need >=200).')

    df = df.loc[valid].reset_index(drop=True)
    y = y_raw[valid]

    source_mat = _collect_source_matrices(df)
    proxy_mat = _collect_proxy(df)

    best, per_class_allowed = _search(
        source_mat=source_mat,
        proxy_mat=proxy_mat,
        y=y,
        trials=max(100, int(args.trials)),
        seed=int(args.seed),
        min_class_count=max(20, int(args.min_class_count)),
    )
    payload = _build_payload(best, per_class_allowed)

    if args.json_out:
        with open(args.json_out, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)

    if args.env_out:
        with open(args.env_out, 'w', encoding='utf-8') as f:
            f.write('\n'.join(payload['env_exports']) + '\n')

    _print_result(payload)


if __name__ == '__main__':
    main()
