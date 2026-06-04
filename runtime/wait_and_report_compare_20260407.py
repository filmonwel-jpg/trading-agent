from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

ROOT_TOKEN = "/Users/filmonghezehey/trading-agent/worktrees/databento/training_data/compare_runs_20260407_meta_ab"
REPORT_SCRIPT = Path("/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/generate_compare_report_20260407.py")
POLL_SECONDS = 300


def active_compare_pids() -> list[tuple[int, str]]:
    out = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    active: list[tuple[int, str]] = []
    self_pid = os.getpid()
    parent_pid = os.getppid()
    for raw_line in out.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            pid_text, command = line.split(None, 1)
            pid = int(pid_text)
        except ValueError:
            continue
        if pid in {self_pid, parent_pid}:
            continue
        if ROOT_TOKEN not in command:
            continue
        if "prepare_databento_training.py" in command or "train_30s_models.py" in command:
            active.append((pid, command))
    return active


def main() -> None:
    while True:
        active = active_compare_pids()
        if not active:
            break
        print(f"[WAIT] active compare processes: {[pid for pid, _cmd in active]}", flush=True)
        time.sleep(POLL_SECONDS)

    subprocess.run(["python3", str(REPORT_SCRIPT)], check=True)


if __name__ == "__main__":
    main()

