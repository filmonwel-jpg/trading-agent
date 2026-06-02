#!/usr/bin/env python3
"""Estimate local disk saved by offloading generated artifacts to the vault."""

from __future__ import annotations

import glob
import os
import subprocess
from pathlib import Path

PATTERNS = [
    "training_data/databento_*",
    "training_data/compare_runs_*",
    "training_data/databento_training_runs_*",
    "runtime/backtests",
    "runtime/logs",
    "runtime/databento/snapshots",
    "runtime/databento/promotions",
    "runtime/lifecycle_micro_*",
    "runtime/tmp_*",
    "runtime/*_live_trade_logs.txt",
    "runtime/*.log",
    "model_exports/20*",
    "model_exports/lifecycle_micro_*",
]


def run(args: list[str]) -> str:
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL)


def du_kib(path: Path) -> int:
    return int(run(["du", "-sk", str(path)]).split()[0])


def tracked_or_has_tracked_children(repo: Path, rel: str) -> bool:
    exact = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--error-unmatch", "--", rel],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if exact.returncode == 0:
        return True
    return bool(run(["git", "-C", str(repo), "ls-files", "--", rel]).strip())


def fmt_gib(kib: int) -> str:
    return f"{kib / 1024 / 1024:.2f} GiB"


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    selected: list[tuple[int, str]] = []
    seen: set[str] = set()

    for pattern in PATTERNS:
        for match in glob.glob(str(repo / pattern)):
            path = Path(match)
            if path.is_symlink() or not path.exists():
                continue
            rel = str(path.relative_to(repo))
            if rel in seen:
                continue
            seen.add(rel)
            if tracked_or_has_tracked_children(repo, rel):
                continue
            selected.append((du_kib(path), rel))

    workspace = du_kib(repo)
    selected_total = sum(size for size, _ in selected)
    projected = max(0, workspace - selected_total)

    print(f"workspace_current={fmt_gib(workspace)}")
    print(f"selected_to_offload={fmt_gib(selected_total)}")
    print(f"projected_workspace_after_symlinks={fmt_gib(projected)}")
    print(f"selected_path_count={len(selected)}")
    print("top_selected:")
    for size, rel in sorted(selected, reverse=True)[:30]:
        print(f"{fmt_gib(size):>12}  {rel}")


if __name__ == "__main__":
    main()

