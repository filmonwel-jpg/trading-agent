#!/usr/bin/env python3
"""Verify Databento pilot manifests before any 10-day build.

This codifies the pre-build checks that must pass after source hashing, DBN day
audits, and pilot-date selection. It does not decode DBN data and does not run
normalization/training.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_EXPECTED_SOURCES = [
    "equs_tbbo_20260523",
    "opra_ohlcv1s_20260523",
    "equs_definition_20260612",
    "equs_mbp1_20260612",
    "opra_tcbbo_20260612",
    "opra_definition_20260612",
]


@dataclass
class CheckedSourceFile:
    date: str
    source_label: str
    path: str
    checked_path: str
    path_remapped: bool
    expected_size: int
    actual_size: int | None
    exists: bool
    size_ok: bool
    sha256_status: str
    sha256_present: bool
    sha256_error: str


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[CheckedSourceFile]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CheckedSourceFile.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _int(raw: object, default: int = 0) -> int:
    try:
        return int(float(str(raw or "").strip()))
    except ValueError:
        return default


def parse_path_prefix_map(raw: str) -> tuple[Path, Path]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError("--path-prefix-map must be OLD_PREFIX=NEW_PREFIX")
    old, new = raw.split("=", 1)
    if not old.strip() or not new.strip():
        raise argparse.ArgumentTypeError("--path-prefix-map requires non-empty OLD_PREFIX and NEW_PREFIX")
    return Path(old).expanduser().resolve(), Path(new).expanduser().resolve()


def remap_path(path: Path, prefix_maps: list[tuple[Path, Path]]) -> tuple[Path, bool]:
    for old_prefix, new_prefix in prefix_maps:
        try:
            relative = path.resolve().relative_to(old_prefix)
        except ValueError:
            continue
        return new_prefix / relative, True
    return path, False


def latest_child(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No paths matching {pattern!r} under {root}")
    return matches[0]


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    lake_root = args.lake_root.expanduser().resolve()
    hash_dir = args.hash_dir.expanduser().resolve() if args.hash_dir else latest_child(lake_root / "source_manifests", "source_inventory_hashes_*")
    audit_dir = args.audit_summary_dir.expanduser().resolve() if args.audit_summary_dir else latest_child(lake_root / "raw_audits", "dbn_audit_summary_recent_old_*")
    pilot_dir = args.pilot_dates_dir.expanduser().resolve() if args.pilot_dates_dir else latest_child(lake_root / "source_manifests", "pilot_dates_latest10_*")
    return lake_root, hash_dir, audit_dir, pilot_dir


def verify(
    lake_root: Path,
    hash_dir: Path,
    audit_dir: Path,
    pilot_dir: Path,
    output_dir: Path,
    expected_sources: list[str],
    expected_days: int,
    path_prefix_maps: list[tuple[Path, Path]] | None = None,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)

    hash_manifest_path = hash_dir / "manifest.json"
    audit_manifest_path = audit_dir / "manifest.json"
    pilot_manifest_path = pilot_dir / "manifest.json"
    pilot_dates_path = pilot_dir / "pilot_dates.csv"
    pilot_source_files_path = pilot_dir / "pilot_source_files.csv"

    hash_manifest = read_json(hash_manifest_path)
    audit_manifest = read_json(audit_manifest_path)
    pilot_manifest = read_json(pilot_manifest_path)
    dates = read_csv(pilot_dates_path)
    files = read_csv(pilot_source_files_path)

    errors: list[str] = []
    warnings: list[str] = []

    if hash_manifest.get("hash_error_count") != 0:
        errors.append(f"hash manifest has hash_error_count={hash_manifest.get('hash_error_count')}")
    if audit_manifest.get("error_count") != 0:
        errors.append(f"DBN audit manifest has error_count={audit_manifest.get('error_count')}")
    if audit_manifest.get("warning_count") != 0:
        errors.append(f"DBN audit manifest has warning_count={audit_manifest.get('warning_count')}")
    if pilot_manifest.get("selected_day_count") != expected_days:
        errors.append(f"pilot manifest selected_day_count={pilot_manifest.get('selected_day_count')}, expected {expected_days}")
    if len(dates) != expected_days:
        errors.append(f"pilot_dates.csv rows={len(dates)}, expected {expected_days}")

    expected_source_set = set(expected_sources)
    expected_file_count = expected_days * len(expected_sources)
    if len(files) != expected_file_count:
        errors.append(f"pilot_source_files.csv rows={len(files)}, expected {expected_file_count}")

    date_values = [row["date"] for row in dates]
    if "20260403" in date_values:
        errors.append("unpaired date 20260403 is selected")

    date_counts = Counter(row.get("date", "") for row in files)
    source_counts = Counter(row.get("source_label", "") for row in files)

    for date in date_values:
        if date_counts[date] != len(expected_sources):
            errors.append(f"date {date} has {date_counts[date]} source files, expected {len(expected_sources)}")
    for source in expected_sources:
        if source_counts[source] != expected_days:
            errors.append(f"source {source} has {source_counts[source]} files, expected {expected_days}")
    unexpected_sources = set(source_counts) - expected_source_set
    if unexpected_sources:
        errors.append(f"unexpected source labels selected: {sorted(unexpected_sources)}")

    prefix_maps = path_prefix_maps or []
    checked_rows: list[CheckedSourceFile] = []
    total_bytes = 0
    for row in files:
        path = Path(row.get("path", ""))
        checked_path, path_remapped = remap_path(path, prefix_maps)
        expected_size = _int(row.get("bytes"))
        exists = checked_path.exists()
        actual_size = checked_path.stat().st_size if exists else None
        size_ok = exists and actual_size == expected_size
        sha256_status = row.get("sha256_status", "")
        sha256_present = bool(row.get("sha256", ""))
        sha256_error = row.get("sha256_error", "")

        if not exists:
            if path_remapped:
                errors.append(f"missing source file: {path} remapped_to={checked_path}")
            else:
                errors.append(f"missing source file: {path}")
        elif not size_ok:
            errors.append(f"size mismatch: {path} checked_path={checked_path} expected={expected_size} actual={actual_size}")
        if sha256_status != "ok":
            errors.append(f"hash status not ok: {path} status={sha256_status}")
        if not sha256_present:
            errors.append(f"missing sha256 for selected source file: {path}")
        if sha256_error:
            errors.append(f"hash error recorded for selected source file: {path} {sha256_error}")

        total_bytes += expected_size
        checked_rows.append(
            CheckedSourceFile(
                date=row.get("date", ""),
                source_label=row.get("source_label", ""),
                path=str(path),
                checked_path=str(checked_path),
                path_remapped=path_remapped,
                expected_size=expected_size,
                actual_size=actual_size,
                exists=exists,
                size_ok=size_ok,
                sha256_status=sha256_status,
                sha256_present=sha256_present,
                sha256_error=sha256_error,
            )
        )

    manifest = {
        "generated_at_utc": utc_now(),
        "lake_root": str(lake_root),
        "hash_dir": str(hash_dir),
        "audit_dir": str(audit_dir),
        "pilot_dir": str(pilot_dir),
        "path_prefix_maps": [(str(old), str(new)) for old, new in prefix_maps],
        "selected_dates": date_values,
        "selected_file_count": len(files),
        "expected_file_count": expected_file_count,
        "source_counts": dict(source_counts),
        "date_counts": dict(date_counts),
        "total_compressed_gib": round(total_bytes / (1024 ** 3), 3),
        "errors": errors,
        "warnings": warnings,
    }

    (output_dir / "prebuild_manifest_check.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(output_dir / "prebuild_manifest_check_files.csv", checked_rows)

    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"wrote {output_dir / 'prebuild_manifest_check.json'}")
    print(f"wrote {output_dir / 'prebuild_manifest_check_files.csv'}")
    print("PREBUILD_CHECK=FAIL" if errors else "PREBUILD_CHECK=PASS")
    return 2 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", required=True, type=Path, help="External data_lake_v2 root.")
    parser.add_argument("--hash-dir", type=Path, help="source_inventory_hashes_* directory. Defaults to latest.")
    parser.add_argument("--audit-summary-dir", type=Path, help="dbn_audit_summary_recent_old_* directory. Defaults to latest.")
    parser.add_argument("--pilot-dates-dir", type=Path, help="pilot_dates_latest10_* directory. Defaults to latest.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for prebuild check outputs.")
    parser.add_argument("--expected-days", type=int, default=10, help="Expected number of pilot dates.")
    parser.add_argument("--expected-source", action="append", help="Expected source label. Repeatable. Defaults to the six pilot sources.")
    parser.add_argument(
        "--path-prefix-map",
        action="append",
        type=parse_path_prefix_map,
        help="Remap selected manifest source paths from OLD_PREFIX to NEW_PREFIX. Format: OLD_PREFIX=NEW_PREFIX. Repeatable.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    lake_root, hash_dir, audit_dir, pilot_dir = resolve_paths(args)
    expected_sources = args.expected_source if args.expected_source else DEFAULT_EXPECTED_SOURCES
    return verify(
        lake_root=lake_root,
        hash_dir=hash_dir,
        audit_dir=audit_dir,
        pilot_dir=pilot_dir,
        output_dir=args.output_dir.expanduser().resolve(),
        expected_sources=expected_sources,
        expected_days=args.expected_days,
        path_prefix_maps=args.path_prefix_map,
    )


if __name__ == "__main__":
    raise SystemExit(main())
