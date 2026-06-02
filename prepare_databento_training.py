#!/usr/bin/env python3
import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pandas as pd

from feature_producers_30s import (
    REQUIRED_PRODUCER_COLUMNS,
    SEQUENCE_COLUMN_MAPPING,
    TIMESFM_COLUMN_MAPPING,
    normalize_external_meta_feature_columns,
)


REQUIRED_PLAN_COLUMNS = {
    'Symbol',
    'modeling_cohort',
    'recommended_training_mode',
}
META_PRODUCER_SENTINELS = set(REQUIRED_PRODUCER_COLUMNS)
TIMESFM_EXTERNAL_COLUMNS = set(TIMESFM_COLUMN_MAPPING)
TIMESFM_CANONICAL_COLUMNS = set(TIMESFM_COLUMN_MAPPING.values())
SEQUENCE_EXTERNAL_COLUMNS = set(SEQUENCE_COLUMN_MAPPING)
SEQUENCE_CANONICAL_COLUMNS = set(SEQUENCE_COLUMN_MAPPING.values())


def parse_args():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description='Prepare and optionally run Databento training jobs from per-symbol 30s outputs.'
    )
    parser.add_argument('--data-dir', type=str, required=True, help='Directory containing per-symbol 30s CSV files.')
    parser.add_argument('--plan-csv', type=str, required=True, help='Path to symbol_model_plan.csv.')
    parser.add_argument('--work-dir', type=str, default=str(root / 'training_data' / 'databento_training_runs'))
    parser.add_argument('--manifest-out', type=str, default='')
    parser.add_argument('--include-cohorts', type=str, default='mega_liquid,liquid,specialized')
    parser.add_argument('--job-name', type=str, default='', help='Optional single job name to prepare/run.')
    parser.add_argument('--min-rows', type=int, default=600, help='Minimum CSV row count required for a symbol to be considered ready.')
    parser.add_argument('--max-specialized', type=int, default=0, help='Optional cap on specialized per-symbol jobs.')
    parser.add_argument('--run-ready', action='store_true', help='Run ready jobs through staged meta-feature generation and train_30s_models.py.')
    parser.add_argument('--model-family', type=str, default='lightgbm')
    parser.add_argument('--regime-model-family', type=str, default='lightgbm')
    parser.add_argument('--use-meta-producer-features', choices=['auto', '0', '1'], default='auto')
    parser.add_argument('--timesfm-backend', choices=['off', 'proxy', 'timesfm'], default='off')
    parser.add_argument('--timesfm-repo', type=str, default='google/timesfm-2.0-500m-pytorch')
    parser.add_argument('--timesfm-context-len', type=int, default=128)
    parser.add_argument('--timesfm-horizon-len', type=int, default=16)
    parser.add_argument('--timesfm-max-rows', type=int, default=0)
    parser.add_argument('--sequence-backend', choices=['off', 'proxy', 'torch'], default='off')
    parser.add_argument('--sequence-window', type=int, default=64)
    parser.add_argument('--sequence-epochs', type=int, default=4)
    parser.add_argument('--sequence-lr', type=float, default=1e-3)
    parser.add_argument('--sequence-max-rows', type=int, default=0)
    parser.add_argument('--skip-legacy-30s-exit-models', action='store_true', help='Skip training/export of old generic 30s long_exit/short_exit models and their open/regime variants.')
    parser.add_argument('--python-bin', type=str, default='python3')
    return parser.parse_args()


def normalize_symbol(text):
    return str(text or '').strip().upper()


def parse_cohorts(raw_text):
    out = []
    seen = set()
    for token in str(raw_text or '').split(','):
        cohort = token.strip().lower()
        if cohort and cohort not in seen:
            seen.add(cohort)
            out.append(cohort)
    return out


def count_csv_rows(path):
    with Path(path).open('r', encoding='utf-8', newline='') as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def read_csv_header(csv_path):
    with Path(csv_path).open('r', encoding='utf-8', newline='') as fh:
        reader = csv.reader(fh)
        return next(reader, [])


def inspect_feature_state(csv_path):
    header = set(read_csv_header(csv_path))
    return {
        'canonical_meta': bool(META_PRODUCER_SENTINELS.intersection(header)),
        'canonical_timesfm': bool(TIMESFM_CANONICAL_COLUMNS.intersection(header)),
        'external_timesfm': bool(TIMESFM_EXTERNAL_COLUMNS.intersection(header)),
        'canonical_sequence': bool(SEQUENCE_CANONICAL_COLUMNS.intersection(header)),
        'external_sequence': bool(SEQUENCE_EXTERNAL_COLUMNS.intersection(header)),
    }


def detect_meta_features(csv_path):
    return inspect_feature_state(csv_path)['canonical_meta']


def normalize_meta_csv(csv_path):
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path)
    before_rows = len(df)
    df = normalize_external_meta_feature_columns(df)
    df.to_csv(csv_path, index=False)
    state = inspect_feature_state(csv_path)
    state['rows'] = before_rows
    return state


def copy_csv(source_path, output_path):
    source_path = Path(source_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, output_path)
    return output_path


def concatenate_csv_files(csv_paths, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    header_written = False
    expected_header = None
    total_rows = 0
    for src in csv_paths:
        src = Path(src)
        with src.open('r', encoding='utf-8', newline='') as in_fh:
            first_line = in_fh.readline()
            if not first_line:
                continue
            header = first_line.rstrip('\n')
            if expected_header is None:
                expected_header = header
            elif header != expected_header:
                raise ValueError(f'Header mismatch while combining datasets: {src}')

            mode = 'w' if not header_written else 'a'
            with output_path.open(mode, encoding='utf-8', newline='') as out_fh:
                if not header_written:
                    out_fh.write(first_line)
                    header_written = True
                for line in in_fh:
                    out_fh.write(line)
                    total_rows += 1
    return total_rows


def load_plan(plan_csv):
    plan = pd.read_csv(plan_csv)
    missing = REQUIRED_PLAN_COLUMNS.difference(plan.columns)
    if missing:
        raise ValueError(f'Plan CSV missing required columns: {sorted(missing)}')
    plan['Symbol'] = plan['Symbol'].map(normalize_symbol)
    plan['modeling_cohort'] = plan['modeling_cohort'].astype(str).str.strip().str.lower()
    plan['recommended_training_mode'] = plan['recommended_training_mode'].astype(str).str.strip()
    return plan


def discover_symbol_files(data_dir):
    data_root = Path(data_dir).expanduser().resolve()
    if not data_root.exists():
        raise FileNotFoundError(f'Data directory not found: {data_root}')

    discovered = {}
    for path in sorted(data_root.glob('*_30s_training.csv')):
        symbol = normalize_symbol(path.name.replace('_30s_training.csv', ''))
        if not symbol:
            continue
        state = inspect_feature_state(path)
        discovered[symbol] = {
            'path': path,
            'rows': count_csv_rows(path),
            'has_meta_features': state['canonical_meta'],
            'feature_state': state,
        }
    return discovered


def _build_job_dict(job_name, training_scope, cohort, recommended_training_mode, symbols, missing_symbols, source_dataset_path, row_count, source_has_meta_features, staged_dirs, ready, note):
    return {
        'job_name': job_name,
        'training_scope': training_scope,
        'cohort': cohort,
        'recommended_training_mode': recommended_training_mode,
        'symbols': list(symbols),
        'missing_symbols': list(missing_symbols),
        'symbol_count': len(symbols),
        'source_dataset_path': str(source_dataset_path) if source_dataset_path else '',
        'raw_dataset_path': str(staged_dirs['raw'] / f'{job_name}.csv'),
        'timesfm_dataset_path': str(staged_dirs['timesfm'] / f'{job_name}.csv'),
        'sequence_dataset_path': str(staged_dirs['sequence'] / f'{job_name}.csv'),
        'final_dataset_path': str(staged_dirs['raw'] / f'{job_name}.csv'),
        'source_row_count': int(row_count),
        'raw_row_count': int(row_count),
        'final_row_count': int(row_count),
        'source_has_meta_features': bool(source_has_meta_features),
        'final_has_meta_features': bool(source_has_meta_features),
        'timesfm_status': 'disabled',
        'sequence_status': 'disabled',
        'train_status': 'pending',
        'effective_use_meta_features': '0',
        'ready': bool(ready),
        'note': note,
    }


def build_jobs(plan_df, symbol_files, include_cohorts, min_rows, max_specialized, staged_dirs):
    jobs = []

    for path in staged_dirs.values():
        path.mkdir(parents=True, exist_ok=True)

    plan_df = plan_df[plan_df['modeling_cohort'].isin(include_cohorts)].copy()
    for cohort in ['mega_liquid', 'liquid']:
        cohort_df = plan_df[plan_df['modeling_cohort'] == cohort].copy()
        if cohort_df.empty:
            continue

        ready_symbols = []
        missing_symbols = []
        for symbol in cohort_df['Symbol']:
            meta = symbol_files.get(symbol)
            if meta and meta['rows'] >= min_rows:
                ready_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)

        raw_dataset_path = staged_dirs['raw'] / f'{cohort}.csv'
        row_count = 0
        ready = len(ready_symbols) >= 2
        note = ''
        has_meta = False
        source_path = raw_dataset_path if ready else ''
        if ready:
            row_count = concatenate_csv_files([symbol_files[s]['path'] for s in ready_symbols], raw_dataset_path)
            state = normalize_meta_csv(raw_dataset_path)
            has_meta = state['canonical_meta']
        else:
            note = 'need at least 2 ready symbol files for shared training'

        jobs.append(_build_job_dict(
            job_name=cohort,
            training_scope='shared_cohort',
            cohort=cohort,
            recommended_training_mode=cohort_df['recommended_training_mode'].iloc[0],
            symbols=ready_symbols,
            missing_symbols=missing_symbols,
            source_dataset_path=source_path,
            row_count=row_count,
            source_has_meta_features=has_meta,
            staged_dirs=staged_dirs,
            ready=ready,
            note=note,
        ))

    specialized_df = plan_df[plan_df['modeling_cohort'] == 'specialized'].copy()
    if max_specialized and max_specialized > 0:
        specialized_df = specialized_df.head(int(max_specialized)).copy()

    for row in specialized_df.itertuples(index=False):
        symbol = normalize_symbol(row.Symbol)
        meta = symbol_files.get(symbol)
        ready = bool(meta and meta['rows'] >= min_rows)
        raw_dataset_path = staged_dirs['raw'] / f'specialized_{symbol.lower()}.csv'
        row_count = int(meta['rows']) if meta else 0
        has_meta = bool(meta and meta['has_meta_features'])
        source_path = meta['path'] if meta else ''
        note = '' if ready else 'source file missing or below min-rows threshold'
        if ready:
            copy_csv(meta['path'], raw_dataset_path)
            state = normalize_meta_csv(raw_dataset_path)
            has_meta = state['canonical_meta']

        jobs.append(_build_job_dict(
            job_name=f'specialized_{symbol.lower()}',
            training_scope='per_symbol',
            cohort='specialized',
            recommended_training_mode=row.recommended_training_mode,
            symbols=[symbol] if ready else [],
            missing_symbols=[] if ready else [symbol],
            source_dataset_path=source_path,
            row_count=row_count,
            source_has_meta_features=has_meta,
            staged_dirs=staged_dirs,
            ready=ready,
            note=note,
        ))

    return jobs


def filter_jobs(jobs, job_name=''):
    if not job_name:
        return jobs
    target = str(job_name).strip()
    return [job for job in jobs if job['job_name'] == target]


def resolve_effective_use_meta(job, use_meta_mode):
    if use_meta_mode == 'auto':
        return '1' if job.get('final_has_meta_features') else '0'
    return use_meta_mode


def run_logged_command(cmd, cwd, env, log_path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('w', encoding='utf-8') as log_fh:
        proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=log_fh, stderr=subprocess.STDOUT)
    return proc.returncode


def copy_and_normalize_stage(input_path, output_path):
    copy_csv(input_path, output_path)
    return normalize_meta_csv(output_path)


def expected_row_count(previous_rows, max_rows):
    previous_rows = int(previous_rows)
    max_rows = int(max_rows or 0)
    if max_rows > 0:
        return min(previous_rows, max_rows)
    return previous_rows


def ensure_row_count_unchanged(job, stage_name, previous_rows, new_rows):
    if int(previous_rows) != int(new_rows):
        raise ValueError(f'{job["job_name"]} {stage_name} row count changed: {previous_rows} -> {new_rows}')


def maybe_run_timesfm_stage(job, args, root_dir, logs_dir):
    input_path = Path(job['raw_dataset_path'])
    if args.timesfm_backend == 'off':
        job['timesfm_status'] = 'disabled'
        return input_path

    output_path = Path(job['timesfm_dataset_path'])
    state = inspect_feature_state(input_path)
    expected_rows = expected_row_count(job['raw_row_count'], args.timesfm_max_rows)
    if state['external_timesfm']:
        normalized = copy_and_normalize_stage(input_path, output_path)
        ensure_row_count_unchanged(job, 'timesfm', expected_rows, normalized['rows'])
        job['timesfm_status'] = 'preexisting'
        return output_path

    env = os.environ.copy()
    cmd = [
        args.python_bin,
        str(root_dir / 'generate_timesfm_features.py'),
        '--input-csv', str(input_path),
        '--output-csv', str(output_path),
        '--backend', args.timesfm_backend,
        '--timesfm-repo', args.timesfm_repo,
        '--context-len', str(max(32, int(args.timesfm_context_len))),
        '--horizon-len', str(max(4, int(args.timesfm_horizon_len))),
    ]
    if args.timesfm_max_rows and int(args.timesfm_max_rows) > 0:
        cmd.extend(['--max-rows', str(int(args.timesfm_max_rows))])

    rc = run_logged_command(cmd, cwd=root_dir, env=env, log_path=logs_dir / f'{job["job_name"]}_timesfm.log')
    if rc != 0:
        job['timesfm_status'] = f'failed:{rc}'
        raise RuntimeError(f'{job["job_name"]} TimesFM stage failed with exit code {rc}')

    normalized = normalize_meta_csv(output_path)
    ensure_row_count_unchanged(job, 'timesfm', expected_rows, normalized['rows'])
    job['timesfm_status'] = 'ok'
    return output_path


def maybe_run_sequence_stage(job, args, root_dir, logs_dir, input_path):
    if args.sequence_backend == 'off':
        job['sequence_status'] = 'disabled'
        return input_path

    output_path = Path(job['sequence_dataset_path'])
    state = inspect_feature_state(input_path)
    previous_rows = count_csv_rows(input_path)
    expected_rows = expected_row_count(previous_rows, args.sequence_max_rows)
    if state['external_sequence']:
        normalized = copy_and_normalize_stage(input_path, output_path)
        ensure_row_count_unchanged(job, 'sequence', expected_rows, normalized['rows'])
        job['sequence_status'] = 'preexisting'
        return output_path

    env = os.environ.copy()
    cmd = [
        args.python_bin,
        str(root_dir / 'train_sequence_meta.py'),
        '--input-csv', str(input_path),
        '--output-csv', str(output_path),
        '--backend', args.sequence_backend,
        '--window', str(max(16, int(args.sequence_window))),
        '--epochs', str(max(1, int(args.sequence_epochs))),
        '--lr', str(float(args.sequence_lr)),
    ]
    if args.sequence_max_rows and int(args.sequence_max_rows) > 0:
        cmd.extend(['--max-rows', str(int(args.sequence_max_rows))])

    rc = run_logged_command(cmd, cwd=root_dir, env=env, log_path=logs_dir / f'{job["job_name"]}_sequence.log')
    if rc != 0:
        job['sequence_status'] = f'failed:{rc}'
        raise RuntimeError(f'{job["job_name"]} sequence stage failed with exit code {rc}')

    normalized = normalize_meta_csv(output_path)
    ensure_row_count_unchanged(job, 'sequence', expected_rows, normalized['rows'])
    job['sequence_status'] = 'ok'
    return output_path


def run_training_stage(job, args, root_dir, logs_dir):
    effective_use_meta = resolve_effective_use_meta(job, args.use_meta_producer_features)
    job['effective_use_meta_features'] = effective_use_meta

    cmd = [
        args.python_bin,
        str(root_dir / 'train_30s_models.py'),
        '--input-csv', job['final_dataset_path'],
    ]
    env = os.environ.copy()
    env['MODEL_FAMILY'] = args.model_family
    env['REGIME_MODEL_FAMILY'] = args.regime_model_family
    env['USE_META_PRODUCER_FEATURES'] = effective_use_meta
    env['TRAIN_LEGACY_30S_EXIT_MODELS'] = '0' if args.skip_legacy_30s_exit_models else '1'

    print(f"[TRAIN] {job['job_name']} -> {job['final_dataset_path']}")
    rc = run_logged_command(cmd, cwd=root_dir, env=env, log_path=logs_dir / f'{job["job_name"]}.log')
    job['train_status'] = 'ok' if rc == 0 else f'failed:{rc}'
    return job['train_status']


def materialize_job_pipeline(job, args, root_dir, logs_dir):
    if not job['ready']:
        job['train_status'] = 'skipped'
        return 'skipped'

    try:
        stage_input = maybe_run_timesfm_stage(job, args, root_dir, logs_dir)
        stage_input = maybe_run_sequence_stage(job, args, root_dir, logs_dir, stage_input)
        final_state = inspect_feature_state(stage_input)
        job['final_dataset_path'] = str(stage_input)
        job['final_row_count'] = int(count_csv_rows(stage_input))
        job['final_has_meta_features'] = bool(final_state['canonical_meta'])
        return run_training_stage(job, args, root_dir, logs_dir)
    except Exception as exc:
        job['train_status'] = f'pipeline_failed:{exc}'
        return job['train_status']


def build_summary(jobs):
    frame = pd.DataFrame(jobs)
    if frame.empty:
        by_cohort = {}
    else:
        by_cohort = frame.groupby('cohort')['job_name'].count().to_dict()
    return {
        'jobs_total': len(jobs),
        'jobs_ready': int(sum(1 for job in jobs if job['ready'])),
        'by_cohort': by_cohort,
        'timesfm_backend': frame.get('timesfm_status', pd.Series(dtype=str)).value_counts().to_dict() if not frame.empty else {},
        'sequence_backend': frame.get('sequence_status', pd.Series(dtype=str)).value_counts().to_dict() if not frame.empty else {},
        'train_status': frame.get('train_status', pd.Series(dtype=str)).value_counts().to_dict() if not frame.empty else {},
    }


def write_manifest(jobs, manifest_path, root_dir, args):
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for job in jobs:
        job['effective_use_meta_features'] = resolve_effective_use_meta(job, args.use_meta_producer_features)
        rows.append({
            'job_name': job['job_name'],
            'training_scope': job['training_scope'],
            'cohort': job['cohort'],
            'recommended_training_mode': job['recommended_training_mode'],
            'symbol_count': job['symbol_count'],
            'symbols': ','.join(job['symbols']),
            'missing_symbols': ','.join(job['missing_symbols']),
            'source_dataset_path': job['source_dataset_path'],
            'raw_dataset_path': job['raw_dataset_path'],
            'timesfm_dataset_path': job['timesfm_dataset_path'],
            'sequence_dataset_path': job['sequence_dataset_path'],
            'final_dataset_path': job['final_dataset_path'],
            'source_row_count': job['source_row_count'],
            'raw_row_count': job['raw_row_count'],
            'final_row_count': job['final_row_count'],
            'source_has_meta_features': int(bool(job['source_has_meta_features'])),
            'final_has_meta_features': int(bool(job['final_has_meta_features'])),
            'timesfm_status': job['timesfm_status'],
            'sequence_status': job['sequence_status'],
            'train_status': job['train_status'],
            'effective_use_meta_features': job['effective_use_meta_features'],
            'ready': int(bool(job['ready'])),
            'note': job['note'],
        })
    pd.DataFrame(rows).to_csv(manifest_path, index=False)

    shell_path = manifest_path.with_suffix('.sh')
    commands = ['#!/usr/bin/env bash', 'set -euo pipefail']
    base_cmd = [
        'python3',
        str(root_dir / 'prepare_databento_training.py'),
        '--data-dir', str(Path(args.data_dir).expanduser().resolve()),
        '--plan-csv', str(Path(args.plan_csv).expanduser().resolve()),
        '--work-dir', str(Path(args.work_dir).expanduser().resolve()),
        '--include-cohorts', args.include_cohorts,
        '--min-rows', str(args.min_rows),
        '--max-specialized', str(args.max_specialized),
        '--model-family', args.model_family,
        '--regime-model-family', args.regime_model_family,
        '--use-meta-producer-features', args.use_meta_producer_features,
        '--timesfm-backend', args.timesfm_backend,
        '--timesfm-repo', args.timesfm_repo,
        '--timesfm-context-len', str(args.timesfm_context_len),
        '--timesfm-horizon-len', str(args.timesfm_horizon_len),
        '--sequence-backend', args.sequence_backend,
        '--sequence-window', str(args.sequence_window),
        '--sequence-epochs', str(args.sequence_epochs),
        '--sequence-lr', str(args.sequence_lr),
        '--python-bin', args.python_bin,
        '--run-ready',
    ]
    if args.timesfm_max_rows and int(args.timesfm_max_rows) > 0:
        base_cmd.extend(['--timesfm-max-rows', str(int(args.timesfm_max_rows))])
    if args.sequence_max_rows and int(args.sequence_max_rows) > 0:
        base_cmd.extend(['--sequence-max-rows', str(int(args.sequence_max_rows))])
    if args.skip_legacy_30s_exit_models:
        base_cmd.append('--skip-legacy-30s-exit-models')
    for job in jobs:
        if not job['ready']:
            continue
        cmd = base_cmd + ['--job-name', job['job_name']]
        commands.append(' '.join(shlex.quote(part) for part in cmd))
    shell_path.write_text('\n'.join(commands) + '\n', encoding='utf-8')
    shell_path.chmod(0o755)
    return shell_path


def main():
    args = parse_args()
    root = Path(__file__).resolve().parent
    work_dir = Path(args.work_dir).expanduser().resolve()
    staged_dirs = {
        'raw': work_dir / 'staged_datasets' / 'raw',
        'timesfm': work_dir / 'staged_datasets' / 'timesfm',
        'sequence': work_dir / 'staged_datasets' / 'sequence',
    }
    logs_dir = work_dir / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)

    include_cohorts = parse_cohorts(args.include_cohorts)
    plan_df = load_plan(Path(args.plan_csv).expanduser().resolve())
    symbol_files = discover_symbol_files(args.data_dir)

    jobs = build_jobs(
        plan_df=plan_df,
        symbol_files=symbol_files,
        include_cohorts=include_cohorts,
        min_rows=max(1, int(args.min_rows)),
        max_specialized=max(0, int(args.max_specialized)),
        staged_dirs=staged_dirs,
    )
    jobs = filter_jobs(jobs, args.job_name)

    if not jobs:
        raise ValueError('No jobs matched the current selection.')

    manifest_path = Path(args.manifest_out).expanduser().resolve() if args.manifest_out else (work_dir / 'training_manifest.csv')
    shell_path = write_manifest(jobs, manifest_path, root, args)

    if args.run_ready:
        for job in jobs:
            materialize_job_pipeline(job, args, root, logs_dir)
        shell_path = write_manifest(jobs, manifest_path, root, args)

    summary = build_summary(jobs)
    (work_dir / 'training_manifest_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')

    print(f'Wrote manifest: {manifest_path}')
    print(f'Wrote runnable shell script: {shell_path}')
    print(f'Prepared jobs: total={summary["jobs_total"]} ready={summary["jobs_ready"]}')
    for job in jobs:
        if args.run_ready:
            print(f"[RESULT] {job['job_name']}: {job['train_status']}")


if __name__ == '__main__':
    main()


