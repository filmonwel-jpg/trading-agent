#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import subprocess

ROOT = Path(os.environ.get('TRADING_AGENT_ROOT', Path(__file__).resolve().parents[1]))
LOG_DIR = ROOT / 'runtime/logs/finish_20260523_outputs'
PID_FILE = LOG_DIR / 'finish_from_chunks.pid'
CAFF_FILE = LOG_DIR / 'finish_from_chunks_caffeinate.pid'


def alive(pid: str) -> bool:
    return bool(pid) and subprocess.run(['kill', '-0', pid], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def main() -> int:
    pid = PID_FILE.read_text().strip() if PID_FILE.exists() else ''
    caff = CAFF_FILE.read_text().strip() if CAFF_FILE.exists() else ''
    print('=== PROCESS ===')
    if alive(pid):
        print(f'RUNNING pid={pid}')
        subprocess.run(['ps', '-p', pid, '-o', 'pid,ppid,etime,command'])
    else:
        print(f'NOT_RUNNING pid={pid or "none"}')
    print(f'CAFFEINATE_{"RUNNING" if alive(caff) else "NOT_RUNNING"} pid={caff or "none"}')

    print('\n=== DISK ===')
    subprocess.run(['df', '-h', str(ROOT), '/Volumes/DatabentoVault'])

    print('\n=== OUTPUT COUNTS / SIZES ===')
    for cadence in ('30s', '5s', '1s'):
        directory = ROOT / 'training_data' / f'databento_{cadence}_20260523'
        count = len(list(directory.glob(f'*_{cadence}_training.csv'))) if directory.exists() else 0
        final = ROOT / 'training_data' / f'databento_{cadence}_20260523_combined.csv'
        tmp = Path(str(final) + '.tmp')
        print(f'{cadence}_SYMBOL_CSVS={count} dir={directory}')
        if final.exists():
            print(f'  final_size={final.stat().st_size:,} path={final}')
        if tmp.exists():
            print(f'  tmp_size={tmp.stat().st_size:,} path={tmp}')
        if not final.exists() and not tmp.exists():
            print('  combined_missing')
    plan = ROOT / 'training_data/databento_30s_20260523/symbol_model_plan.csv'
    print(f'PLAN exists={plan.exists()} size={plan.stat().st_size if plan.exists() else 0} path={plan}')

    logs = sorted(LOG_DIR.glob('finish_from_chunks_*.log'), key=lambda p: p.stat().st_mtime, reverse=True)
    print('\n=== LOG TAIL ===')
    if logs:
        print(f'log={logs[0]}')
        subprocess.run(['tail', '-80', str(logs[0])])
    else:
        print('no log')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

