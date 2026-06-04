#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
import subprocess

ROOT = Path('/Users/filmonghezehey/trading-agent/worktrees/databento')
LOG_ROOT = ROOT / 'runtime' / 'logs'
LOG_ROOT.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_ROOT / f"cleanup_may22_logs_{datetime.now():%Y%m%d_%H%M%S}.log"
START_TS = datetime(2026, 5, 22, 0, 0, 0).timestamp()
END_TS = datetime(2026, 5, 23, 0, 0, 0).timestamp()
LOG_SUFFIXES = {'.log', '.out', '.err', '.trace', '.txt', '.json'}
EXCLUDED_DIR_PARTS = {
    '.git',
    'target',
    '__pycache__',
    'databento_30s_20260523_build_chunks',
    '20260523_pipeline_after_bars',
    '20260523_pipeline_recovery',
}


def disk_line() -> str:
    return subprocess.check_output(['/bin/df', '-h', str(ROOT)], text=True).strip().splitlines()[-1]


def human_size(num_bytes: int) -> str:
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f'{value:.1f}{unit}' if unit != 'B' else f'{int(value)}B'
        value /= 1024.0
    return f'{num_bytes}B'


def is_excluded(path: Path) -> bool:
    rel_parts = path.relative_to(ROOT).parts
    return any(part in EXCLUDED_DIR_PARTS for part in rel_parts)


def is_log_like(path: Path) -> bool:
    name = path.name.lower()
    if name.endswith('.nohup.log'):
        return True
    if path.suffix.lower() in LOG_SUFFIXES:
        return True
    return 'log' in name


def collect_candidates() -> list[Path]:
    candidates: set[Path] = set()
    for path in ROOT.rglob('*'):
        if not path.is_file():
            continue
        try:
            if is_excluded(path) or not is_log_like(path):
                continue
            stat = path.stat()
        except (FileNotFoundError, PermissionError):
            continue
        name_lower = path.name.lower()
        by_name = '20260522' in name_lower or 'may22' in name_lower or 'may_22' in name_lower
        by_mtime = START_TS <= stat.st_mtime < END_TS
        if by_name or by_mtime:
            candidates.add(path)
    return sorted(candidates)


def main() -> int:
    candidates = collect_candidates()
    total_bytes = sum(path.stat().st_size for path in candidates if path.exists())
    with LOG_PATH.open('w', encoding='utf-8') as log:
        def write(line: str) -> None:
            print(line)
            log.write(line + '\n')
            log.flush()

        write(f'CLEANUP_MAY22_LOGS_START {datetime.now():%Y-%m-%d %H:%M:%S}')
        write(f'disk_before={disk_line()}')
        write(f'candidate_count={len(candidates)}')
        write(f'candidate_size_bytes={total_bytes}')
        write(f'candidate_size={human_size(total_bytes)}')
        write('--- deleting May22 log/report candidates ---')
        deleted_count = 0
        deleted_bytes = 0
        for path in candidates:
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                continue
            write(f'{human_size(size)}\t{path.relative_to(ROOT)}')
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            deleted_count += 1
            deleted_bytes += size
        remaining = [path for path in collect_candidates() if path.exists()]
        remaining_bytes = sum(path.stat().st_size for path in remaining if path.exists())
        write(f'deleted_count={deleted_count}')
        write(f'deleted_size_bytes={deleted_bytes}')
        write(f'deleted_size={human_size(deleted_bytes)}')
        write(f'remaining_candidate_count={len(remaining)}')
        write(f'remaining_candidate_size={human_size(remaining_bytes)}')
        write(f'disk_after={disk_line()}')
        write(f'CLEANUP_MAY22_LOGS_DONE {datetime.now():%Y-%m-%d %H:%M:%S}')
        write(f'cleanup_log={LOG_PATH}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

