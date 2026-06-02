#!/usr/bin/env python3
"""Estimate GitHub push size for the current branch plus staged index.

The script creates a temporary commit object from the current index without
updating any branch, then packs objects reachable from that temporary commit that
are not reachable from origin/* refs. This approximates what a first push of the
current branch plus one commit of staged changes would transfer.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

MIB = 1024 * 1024
GIB = 1024**3


def run(args: list[str], *, input_text: str | None = None, text: bool = True) -> str | bytes:
    return subprocess.check_output(args, input=input_text, text=text)


def fmt_size(size: int) -> str:
    if size >= GIB:
        return f"{size / GIB:.2f} GiB"
    if size >= MIB:
        return f"{size / MIB:.2f} MiB"
    if size >= 1024:
        return f"{size / 1024:.2f} KiB"
    return f"{size} B"


def object_type(oid: str) -> str:
    return run(["git", "cat-file", "-t", oid]).strip()


def object_size(oid: str) -> int:
    return int(run(["git", "cat-file", "-s", oid]).strip())


def staged_worktree_bytes() -> tuple[int, int, int]:
    """Return (regular/symlink bytes, staged path count, staged symlink count)."""
    total = 0
    path_count = 0
    symlink_count = 0
    names = run(["git", "diff", "--cached", "--name-only", "--diff-filter=ACMRT"]).splitlines()
    for name in names:
        try:
            st = os.lstat(name)
        except FileNotFoundError:
            continue
        path_count += 1
        total += st.st_size
        if os.path.islink(name):
            symlink_count += 1
    return total, path_count, symlink_count


def main() -> None:
    repo_root = Path(run(["git", "rev-parse", "--show-toplevel"]).strip())
    os.chdir(repo_root)

    tree = run(["git", "write-tree"]).strip()
    head = run(["git", "rev-parse", "HEAD"]).strip()
    temp_commit = run(
        ["git", "commit-tree", tree, "-p", head, "-m", "temporary push-size estimate"],
    ).strip()

    rev_objects = run(["git", "rev-list", "--objects", temp_commit, "--not", "--remotes=origin"]).splitlines()
    object_ids = [line.split(" ", 1)[0] for line in rev_objects if line]

    with tempfile.NamedTemporaryFile(prefix="github-push-estimate-", suffix=".pack", delete=False) as pack_file:
        pack_path = Path(pack_file.name)
        pack = subprocess.run(
            ["git", "pack-objects", "--quiet", "--stdout"],
            input="\n".join(object_ids) + "\n",
            text=True,
            stdout=pack_file,
            check=True,
        )
        _ = pack

    pack_size = pack_path.stat().st_size
    pack_path.unlink(missing_ok=True)

    blob_rows: list[tuple[int, str, str]] = []
    object_count = 0
    for line in rev_objects:
        if not line:
            continue
        object_count += 1
        parts = line.split(" ", 1)
        oid = parts[0]
        path = parts[1] if len(parts) > 1 else ""
        try:
            if object_type(oid) != "blob":
                continue
            size = object_size(oid)
        except subprocess.CalledProcessError:
            continue
        blob_rows.append((size, oid, path))

    staged_bytes, staged_count, staged_symlinks = staged_worktree_bytes()
    large_blobs = [row for row in blob_rows if row[0] >= 50 * MIB]
    over_100 = [row for row in blob_rows if row[0] >= 100 * MIB]

    print(f"repo_root={repo_root}")
    print(f"temp_commit={temp_commit}")
    print(f"estimated_push_pack_size={fmt_size(pack_size)}")
    print(f"estimated_push_pack_size_bytes={pack_size}")
    print(f"new_object_count_vs_origin={object_count}")
    print(f"new_blob_count_vs_origin={len(blob_rows)}")
    print(f"staged_worktree_payload_uncompressed={fmt_size(staged_bytes)}")
    print(f"staged_path_count={staged_count}")
    print(f"staged_symlink_count={staged_symlinks}")
    print(f"github_100mb_blob_violations={len(over_100)}")
    print("largest_new_blobs_vs_origin:")
    for size, oid, path in sorted(blob_rows, reverse=True)[:25]:
        print(f"  {fmt_size(size):>10}  {oid}  {path}")

    if large_blobs:
        print("new_blobs_over_50MiB:")
        for size, oid, path in sorted(large_blobs, reverse=True):
            print(f"  {fmt_size(size):>10}  {oid}  {path}")


if __name__ == "__main__":
    main()


