#!/usr/bin/env python3
"""Audit repository files against GitHub size limits.

Reports:
- reachable Git blobs in HEAD history >= 50 MiB
- staged added/modified/copied/renamed files >= 50 MiB
- modified tracked working-tree files >= 50 MiB
- untracked non-ignored files >= 100 MiB
"""

from __future__ import annotations

import os
import subprocess

MIB = 1024 * 1024
GIB = 1024**3


def run(args: list[str], *, text: bool = True) -> str | bytes:
    return subprocess.check_output(args, text=text, errors="replace" if text else None)


def print_rows(title: str, rows: list[tuple[int, str]], unit: int = MIB, suffix: str = "MiB") -> None:
    print(f"\n== {title} ==")
    for size, path in sorted(rows, reverse=True):
        print(f"{size / unit:9.1f} {suffix}  {path}")
    print(f"count={len(rows)}")


def reachable_blobs() -> None:
    print("== Largest reachable blobs in HEAD history >=50 MiB ==")
    seen: dict[str, str] = {}
    for line in run(["git", "rev-list", "--objects", "HEAD"]).splitlines():
        parts = line.split(" ", 1)
        oid = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        seen.setdefault(oid, path)

    rows: list[tuple[int, str]] = []
    for oid, path in seen.items():
        try:
            if run(["git", "cat-file", "-t", oid]).strip() != "blob":
                continue
            size = int(run(["git", "cat-file", "-s", oid]).strip())
        except subprocess.CalledProcessError:
            continue
        if size >= 50 * MIB:
            rows.append((size, f"{oid}  {path}"))

    for size, descriptor in sorted(rows, reverse=True):
        print(f"{size / MIB:9.1f} MiB  {descriptor}")
    print(f"count={len(rows)}")


def file_size_rows(paths: list[str], threshold: int) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    for path in paths:
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            continue
        if os.path.islink(path):
            continue
        if st.st_size >= threshold:
            rows.append((st.st_size, path))
    return rows


def main() -> None:
    reachable_blobs()

    staged = run(["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"]).splitlines()
    print_rows(
        "Staged added/modified/copied/renamed files >=50 MiB",
        file_size_rows(staged, 50 * MIB),
    )

    modified = run(["git", "diff", "--name-only", "--diff-filter=ACMR"]).splitlines()
    print_rows(
        "Modified tracked working-tree files >=50 MiB",
        file_size_rows(modified, 50 * MIB),
    )

    out = run(["git", "ls-files", "--others", "--exclude-standard", "-z"], text=False)
    untracked_paths = [part.decode("utf-8", "replace") for part in out.split(b"\0") if part]
    print_rows(
        "Untracked non-ignored files >=100 MiB",
        file_size_rows(untracked_paths, 100 * MIB),
        unit=GIB,
        suffix="GiB",
    )


if __name__ == "__main__":
    main()

