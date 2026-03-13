from pathlib import Path
import subprocess

import numpy as np
import pandas as pd


def main():
    rng = np.random.default_rng(7)
    n = 1200
    y = rng.choice([0, 1, 2], size=n, p=[0.35, 0.40, 0.25])

    # Synthetic model probabilities with different class strengths.
    rf = np.zeros((n, 3), dtype=float)
    lgbm = np.zeros((n, 3), dtype=float)
    cat = np.zeros((n, 3), dtype=float)

    for i in range(n):
        target = y[i]
        base = np.array([0.15, 0.15, 0.15], dtype=float)
        rf_row = base.copy()
        lgbm_row = base.copy()
        cat_row = base.copy()

        rf_row[target] += 0.55 if target != 2 else 0.20
        lgbm_row[target] += 0.55 if target == 1 else 0.25
        cat_row[target] += 0.55 if target == 2 else 0.25

        rf[i] = rf_row / rf_row.sum()
        lgbm[i] = lgbm_row / lgbm_row.sum()
        cat[i] = cat_row / cat_row.sum()

    df = pd.DataFrame({
        'RegimeLabel': y,
        'RegimeRF_ProbChoppy': rf[:, 0],
        'RegimeRF_ProbTrend': rf[:, 1],
        'RegimeRF_ProbVolatile': rf[:, 2],
        'RegimeLGBM_ProbChoppy': lgbm[:, 0],
        'RegimeLGBM_ProbTrend': lgbm[:, 1],
        'RegimeLGBM_ProbVolatile': lgbm[:, 2],
        'RegimeCatBoost_ProbChoppy': cat[:, 0],
        'RegimeCatBoost_ProbTrend': cat[:, 1],
        'RegimeCatBoost_ProbVolatile': cat[:, 2],
        'regime_chop_prob': rf[:, 0],
        'regime_trend_prob': rf[:, 1],
        'regime_volatile_prob': rf[:, 2],
    })

    out_csv = Path('/tmp/regime_tune_synth.csv')
    df.to_csv(out_csv, index=False)

    cmd = [
        'python3',
        str(Path(__file__).resolve().parent / 'tune_regime_ensemble_weights.py'),
        '--input-csv',
        str(out_csv),
        '--target-col',
        'RegimeLabel',
        '--trials',
        '800',
        '--seed',
        '11',
    ]
    print('[SMOKE] running:', ' '.join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == '__main__':
    main()
