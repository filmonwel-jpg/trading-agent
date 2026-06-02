#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re

import pandas as pd

PRIMARY_RE = re.compile(
    r"^(LONG ENTRY \(Dip Buyer\)|SHORT ENTRY \(Rip Seller\)|LONG EXIT \(Top Detector\)|SHORT EXIT \(Bottom Detector\)) \| (\d+)/(\d+) \| ([0-9.]+%) \| ([0-9.]+%) \| ([0-9.]+) \| (\d+) \| (.+)$"
)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Build a mixed baseline/enhanced routing CSV for live model selection.')
    parser.add_argument('--compare-root', type=Path, default=Path('/Users/filmonghezehey/trading-agent/worktrees/databento/training_data/compare_runs_20260407_meta_ab'))
    parser.add_argument('--baseline-variant', type=str, default='baseline_no_generated_meta')
    parser.add_argument('--enhanced-variant', type=str, default='with_timesfm_and_sequence_proxy')
    parser.add_argument('--output-csv', type=Path, default=Path('/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/databento/model-routing.csv'))
    return parser.parse_args()



def parse_pct(text: str) -> float:
    return float(text.rstrip('%')) / 100.0



def parse_job_metrics(log_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []
    for log_path in sorted(log_dir.glob('*.log')):
        section = 'primary'
        for raw_line in log_path.read_text(encoding='utf-8', errors='replace').splitlines():
            line = raw_line.strip()
            if line == '>>> REGIME-SPECIFIC MODEL SCORECARD':
                section = 'regime'
                continue
            if line == '>>> OPENING-30M MODEL SCORECARD':
                section = 'open'
                continue
            if section != 'primary':
                continue
            match = PRIMARY_RE.match(line)
            if not match:
                continue
            export_path = Path(match.group(8).strip()).expanduser()
            rows.append(
                {
                    'job_name': log_path.stem,
                    'model': match.group(1),
                    'avg_precision': parse_pct(match.group(5)),
                    'avg_threshold': float(match.group(6)),
                    'export_path': str(export_path),
                    'model_dir': str(export_path.parent),
                }
            )
    return pd.DataFrame(rows)



def load_manifest_jobs(variant_root: Path) -> pd.DataFrame:
    manifest = pd.read_csv(variant_root / 'training_manifest.csv')
    manifest['symbols'] = manifest['symbols'].fillna('').astype(str)
    return manifest[['job_name', 'symbols', 'training_scope', 'cohort']].copy()



def build_variant_summary(variant_root: Path) -> pd.DataFrame:
    metrics = parse_job_metrics(variant_root / 'logs')
    if metrics.empty:
        return pd.DataFrame(columns=['job_name', 'mean_primary_precision', 'mean_primary_threshold', 'model_dir'])
    grouped = (
        metrics.groupby('job_name', as_index=False)
        .agg(
            mean_primary_precision=('avg_precision', 'mean'),
            mean_primary_threshold=('avg_threshold', 'mean'),
            model_dir=('model_dir', 'first'),
        )
    )
    return grouped



def explode_symbols(manifest_jobs: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for row in manifest_jobs.itertuples(index=False):
        symbols = [token.strip().upper() for token in str(row.symbols).split(',') if token.strip()]
        for symbol in symbols:
            rows.append(
                {
                    'job_name': row.job_name,
                    'symbol': symbol,
                    'training_scope': row.training_scope,
                    'cohort': row.cohort,
                }
            )
    return pd.DataFrame(rows)



def main() -> int:
    args = parse_args()
    compare_root = args.compare_root.expanduser().resolve()
    baseline_root = (compare_root / args.baseline_variant).resolve()
    enhanced_root = (compare_root / args.enhanced_variant).resolve()

    baseline_summary = build_variant_summary(baseline_root).rename(
        columns={
            'mean_primary_precision': 'baseline_mean_primary_precision',
            'mean_primary_threshold': 'baseline_mean_primary_threshold',
            'model_dir': 'baseline_model_dir',
        }
    )
    enhanced_summary = build_variant_summary(enhanced_root).rename(
        columns={
            'mean_primary_precision': 'enhanced_mean_primary_precision',
            'mean_primary_threshold': 'enhanced_mean_primary_threshold',
            'model_dir': 'enhanced_model_dir',
        }
    )

    manifest_jobs = load_manifest_jobs(baseline_root)
    symbol_jobs = explode_symbols(manifest_jobs)
    merged = symbol_jobs.merge(baseline_summary, on='job_name', how='left').merge(enhanced_summary, on='job_name', how='left')

    def choose_variant(row: pd.Series) -> str:
        base = row.get('baseline_mean_primary_precision')
        enh = row.get('enhanced_mean_primary_precision')
        if pd.notna(enh) and (pd.isna(base) or enh >= base):
            return 'enhanced'
        if pd.notna(base):
            return 'baseline'
        return 'default'

    merged['variant'] = merged.apply(choose_variant, axis=1)
    merged['model_dir'] = merged.apply(
        lambda row: row['enhanced_model_dir'] if row['variant'] == 'enhanced' else row['baseline_model_dir'],
        axis=1,
    )
    merged['note'] = merged.apply(
        lambda row: f"baseline={row.get('baseline_mean_primary_precision', float('nan'))} enhanced={row.get('enhanced_mean_primary_precision', float('nan'))}",
        axis=1,
    )

    output = merged[
        [
            'symbol',
            'job_name',
            'variant',
            'model_dir',
            'baseline_mean_primary_precision',
            'enhanced_mean_primary_precision',
            'baseline_model_dir',
            'enhanced_model_dir',
            'training_scope',
            'cohort',
            'note',
        ]
    ].sort_values(['symbol', 'job_name']).reset_index(drop=True)

    args.output_csv.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output_csv.expanduser().resolve(), index=False)
    print(f'Wrote {args.output_csv.expanduser().resolve()} rows={len(output)}')
    print(output[['variant']].value_counts())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

