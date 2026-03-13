import argparse
import json
import subprocess
from pathlib import Path


def run(cmd):
    print(f"[PIPELINE] {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(description='Run TimesFM+sequence meta-feature pipeline then train ONNX models.')
    parser.add_argument('--input-csv', required=True, type=str)
    parser.add_argument('--work-dir', default='tmp_meta_pipeline', type=str)
    parser.add_argument('--timesfm-backend', choices=['proxy', 'timesfm'], default='proxy')
    parser.add_argument('--sequence-backend', choices=['proxy', 'torch'], default='torch')
    parser.add_argument('--model-family', choices=['random_forest', 'lightgbm', 'catboost'], default='lightgbm')
    parser.add_argument('--regime-model-family', choices=['random_forest', 'lightgbm', 'catboost'], default='lightgbm')
    parser.add_argument('--skip-train', action='store_true', help='Only generate staged CSV features, skip model training.')
    parser.add_argument('--tune-regime-weights', action='store_true', help='Run regime ensemble tuner on staged sequence CSV.')
    parser.add_argument('--tune-target-col', type=str, default='RegimeLabel')
    parser.add_argument('--tune-trials', type=int, default=3000)
    parser.add_argument('--tune-seed', type=int, default=42)
    parser.add_argument('--tune-min-class-count', type=int, default=120)
    parser.add_argument('--auto-export-best-weights', action='store_true', help='Write tuned regime env/json files into work dir.')
    parser.add_argument('--apply-tuned-weights', action='store_true', help='Apply tuned regime env vars during train_30s_models.py run.')
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    work = (root / args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)

    timesfm_csv = work / 'stage_timesfm.csv'
    seq_csv = work / 'stage_sequence.csv'
    tuned_json = work / 'regime_weights.json'
    tuned_env = work / 'regime_weights.env'

    run([
        'python3', str(root / 'generate_timesfm_features.py'),
        '--input-csv', args.input_csv,
        '--output-csv', str(timesfm_csv),
        '--backend', args.timesfm_backend,
    ])

    run([
        'python3', str(root / 'train_sequence_meta.py'),
        '--input-csv', str(timesfm_csv),
        '--output-csv', str(seq_csv),
        '--backend', args.sequence_backend,
    ])

    if args.tune_regime_weights:
        tune_cmd = [
            'python3', str(root / 'tune_regime_ensemble_weights.py'),
            '--input-csv', str(seq_csv),
            '--target-col', args.tune_target_col,
            '--trials', str(max(100, int(args.tune_trials))),
            '--seed', str(int(args.tune_seed)),
            '--min-class-count', str(max(20, int(args.tune_min_class_count))),
        ]
        if args.auto_export_best_weights:
            tune_cmd += ['--json-out', str(tuned_json), '--env-out', str(tuned_env)]
        run(tune_cmd)

    if args.skip_train:
        print(f'[PIPELINE] skip-train enabled; staged CSV available: {seq_csv}')
        return

    src_target = root / 'TSLA_30Sec_Historical_Bulk_fromTrainer.csv'
    backup = src_target.read_bytes() if src_target.exists() else None
    src_target.write_bytes(seq_csv.read_bytes())
    try:
        train_cmd = [
            'env',
            f'MODEL_FAMILY={args.model_family}',
            f'REGIME_MODEL_FAMILY={args.regime_model_family}',
            'USE_META_PRODUCER_FEATURES=1',
            'TIMESFM_BACKEND=columns',
            'SEQUENCE_BACKEND=columns',
        ]

        if args.apply_tuned_weights and tuned_json.exists():
            payload = json.loads(tuned_json.read_text(encoding='utf-8'))
            global_weights = payload.get('global_weights', {})
            class_weights = payload.get('weights', {})
            blend_w = payload.get('blend_proxy_weight', 0.35)

            def _to_env_map(weight_map):
                order = ['rf', 'lgbm', 'catboost', 'other']
                parts = []
                for k in order:
                    if k in weight_map:
                        parts.append(f'{k}={float(weight_map[k]):.4f}')
                return ','.join(parts)

            train_cmd.append('REGIME_ENSEMBLE_BACKEND=blend')
            train_cmd.append(f'REGIME_BLEND_PROXY_WEIGHT={float(blend_w):.4f}')
            if global_weights:
                train_cmd.append(f'REGIME_SOURCE_WEIGHTS={_to_env_map(global_weights)}')
            if class_weights.get('trend'):
                train_cmd.append(f'REGIME_SOURCE_WEIGHTS_TREND={_to_env_map(class_weights["trend"])}')
            if class_weights.get('chop'):
                train_cmd.append(f'REGIME_SOURCE_WEIGHTS_CHOP={_to_env_map(class_weights["chop"])}')
            if class_weights.get('volatile'):
                train_cmd.append(f'REGIME_SOURCE_WEIGHTS_VOLATILE={_to_env_map(class_weights["volatile"])}')

        train_cmd += ['python3', str(root / 'train_30s_models.py')]
        run(train_cmd)
    finally:
        if backup is not None:
            src_target.write_bytes(backup)


if __name__ == '__main__':
    main()
