#!/usr/bin/env python3
"""Audit Databento pilot source folders before normalization/training.

This script is intentionally standard-library only so it can run on either Mac
before the heavier Databento/PyArrow processing environment is validated.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
import re


DATE_RE = re.compile(r"(20\d{6})")
DBN_SUFFIXES = (".dbn", ".dbn.zst")


DEFAULT_SOURCES: tuple[tuple[str, str, str, str], ...] = (
    ("equs_tbbo_20260523", "EQUS.MINI", "tbbo", "EQUS-20260523-6J9KE98BJ9"),
    ("opra_ohlcv1s_20260523", "OPRA.PILLAR", "ohlcv-1s", "OPRA-20260523-MSV68VKVKD"),
    ("equs_mbp1_20260612", "EQUS.MINI", "mbp-1", "EQUS-20260612-36BEU4G7M8"),
    ("opra_tcbbo_20260612", "OPRA.PILLAR", "tcbbo", "OPRA-20260612-KN5TPHB5EF"),
    ("opra_definition_20260612", "OPRA.PILLAR", "definition", "OPRA-20260612-B5D4JV3GV6"),
    ("opra_definition_20260612_duplicate", "OPRA.PILLAR", "definition", "OPRA-20260612-B5D4JV3GV6 2"),
)


@dataclass(frozen=True)
class SourceSpec:
    label: str
    dataset: str
    schema: str
    path: Path


@dataclass
class FileRecord:
    source_label: str
    dataset: str
    schema: str
    path: str
    name: str
    bytes: int
    mtime_utc: str
    date: str
    is_dbn: bool
    sha256: str = ""


@dataclass
class SourceSummary:
    source_label: str
    dataset: str
    schema: str
    path: str
    exists: bool
    total_files: int = 0
    dbn_files: int = 0
    unique_dates: int = 0
    first_date: str = ""
    last_date: str = ""
    total_bytes: int = 0
    dbn_bytes: int = 0
    non_dbn_files: int = 0
    missing_reason: str = ""


def _utc_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _date_from_name(name: str) -> str:
    match = DATE_RE.search(name)
    return match.group(1) if match else ""


def _is_dbn_name(name: str) -> bool:
    return name.endswith(DBN_SUFFIXES)


def _sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def default_sources(vault_root: Path) -> list[SourceSpec]:
    return [
        SourceSpec(label=label, dataset=dataset, schema=schema, path=vault_root / rel_path)
        for label, dataset, schema, rel_path in DEFAULT_SOURCES
    ]


def parse_source(raw: str) -> SourceSpec:
    """Parse label=dataset:schema:/absolute/path."""
    if "=" not in raw:
        raise argparse.ArgumentTypeError("--source must be label=dataset:schema:/absolute/path")
    label, rest = raw.split("=", 1)
    parts = rest.split(":", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("--source must be label=dataset:schema:/absolute/path")
    dataset, schema, path = parts
    if not label or not dataset or not schema or not path:
        raise argparse.ArgumentTypeError("--source must include non-empty label, dataset, schema, and path")
    return SourceSpec(label=label, dataset=dataset, schema=schema, path=Path(path).expanduser())


def audit_source(spec: SourceSpec, include_hashes: bool) -> tuple[SourceSummary, list[FileRecord]]:
    if not spec.path.exists():
        return (
            SourceSummary(
                source_label=spec.label,
                dataset=spec.dataset,
                schema=spec.schema,
                path=str(spec.path),
                exists=False,
                missing_reason="path does not exist",
            ),
            [],
        )
    if not spec.path.is_dir():
        return (
            SourceSummary(
                source_label=spec.label,
                dataset=spec.dataset,
                schema=spec.schema,
                path=str(spec.path),
                exists=False,
                missing_reason="path exists but is not a directory",
            ),
            [],
        )

    records: list[FileRecord] = []
    for path in sorted((p for p in spec.path.iterdir() if p.is_file()), key=lambda p: p.name):
        stat = path.stat()
        is_dbn = _is_dbn_name(path.name)
        records.append(
            FileRecord(
                source_label=spec.label,
                dataset=spec.dataset,
                schema=spec.schema,
                path=str(path),
                name=path.name,
                bytes=stat.st_size,
                mtime_utc=_utc_timestamp(stat.st_mtime),
                date=_date_from_name(path.name) if is_dbn else "",
                is_dbn=is_dbn,
                sha256=_sha256(path) if include_hashes else "",
            )
        )

    dbn_records = [record for record in records if record.is_dbn]
    dates = sorted({record.date for record in dbn_records if record.date})
    summary = SourceSummary(
        source_label=spec.label,
        dataset=spec.dataset,
        schema=spec.schema,
        path=str(spec.path),
        exists=True,
        total_files=len(records),
        dbn_files=len(dbn_records),
        unique_dates=len(dates),
        first_date=dates[0] if dates else "",
        last_date=dates[-1] if dates else "",
        total_bytes=sum(record.bytes for record in records),
        dbn_bytes=sum(record.bytes for record in dbn_records),
        non_dbn_files=len(records) - len(dbn_records),
    )
    return summary, records


def write_csv(path: Path, rows: Iterable[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def paired_date_rows(records: list[FileRecord], source_labels: list[str]) -> list[dict[str, str]]:
    dates_by_source: dict[str, set[str]] = {label: set() for label in source_labels}
    for record in records:
        if record.is_dbn and record.date:
            dates_by_source.setdefault(record.source_label, set()).add(record.date)

    all_dates = sorted(set().union(*dates_by_source.values())) if dates_by_source else []
    rows: list[dict[str, str]] = []
    for date in all_dates:
        row: dict[str, str] = {"date": date}
        present_count = 0
        for label in source_labels:
            present = date in dates_by_source.get(label, set())
            row[label] = "1" if present else "0"
            present_count += int(present)
        row["present_source_count"] = str(present_count)
        row["all_sources_present"] = "1" if present_count == len(source_labels) else "0"
        rows.append(row)
    return rows


def duplicate_candidates(summaries: list[SourceSummary]) -> list[list[str]]:
    groups: dict[tuple[str, str, int, int, int, str, str], list[str]] = {}
    for summary in summaries:
        if not summary.exists:
            continue
        key = (
            summary.dataset,
            summary.schema,
            summary.dbn_files,
            summary.unique_dates,
            summary.dbn_bytes,
            summary.first_date,
            summary.last_date,
        )
        groups.setdefault(key, []).append(summary.source_label)
    return [labels for labels in groups.values() if len(labels) > 1]


def run_audit(sources: list[SourceSpec], output_dir: Path, include_hashes: bool, allow_missing: bool) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[SourceSummary] = []
    records: list[FileRecord] = []
    for source in sources:
        summary, source_records = audit_source(source, include_hashes=include_hashes)
        summaries.append(summary)
        records.extend(source_records)

    missing = [summary for summary in summaries if not summary.exists]
    if missing and not allow_missing:
        for summary in missing:
            print(f"missing source: {summary.source_label}: {summary.path} ({summary.missing_reason})", file=sys.stderr)
        return 2

    summary_rows = [asdict(summary) for summary in summaries]
    file_rows = [asdict(record) for record in records]
    source_labels = [source.label for source in sources if any(summary.source_label == source.label and summary.exists for summary in summaries)]
    pair_rows = paired_date_rows(records, source_labels)

    write_csv(
        output_dir / "source_inventory.csv",
        summary_rows,
        list(SourceSummary.__dataclass_fields__.keys()),
    )
    write_csv(
        output_dir / "source_files.csv",
        file_rows,
        list(FileRecord.__dataclass_fields__.keys()),
    )
    write_csv(
        output_dir / "paired_dates.csv",
        pair_rows,
        ["date", *source_labels, "present_source_count", "all_sources_present"],
    )

    manifest = {
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "include_hashes": include_hashes,
        "output_dir": str(output_dir),
        "sources": summary_rows,
        "duplicate_candidates": duplicate_candidates(summaries),
        "paired_date_count": len(pair_rows),
        "fully_paired_date_count": sum(1 for row in pair_rows if row["all_sources_present"] == "1"),
        "unpaired_dates": [row for row in pair_rows if row["all_sources_present"] != "1"],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'source_inventory.csv'}")
    print(f"wrote {output_dir / 'source_files.csv'}")
    print(f"wrote {output_dir / 'paired_dates.csv'}")
    print(f"wrote {output_dir / 'manifest.json'}")
    if manifest["unpaired_dates"]:
        print(f"warning: {len(manifest['unpaired_dates'])} dates are not present in every source")
    if manifest["duplicate_candidates"]:
        print(f"warning: duplicate-like source groups: {manifest['duplicate_candidates']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--vault-root",
        type=Path,
        default=Path("/Volumes/DatabentoVault"),
        help="Vault root used for the default Databento pilot source folders.",
    )
    parser.add_argument(
        "--source",
        action="append",
        type=parse_source,
        help="Override default sources. Format: label=dataset:schema:/absolute/path. Repeatable.",
    )
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory where audit CSV/JSON outputs will be written.")
    parser.add_argument("--include-hashes", action="store_true", help="Compute SHA-256 for every source file; slower but required for immutable manifests.")
    parser.add_argument("--allow-missing", action="store_true", help="Write a partial manifest instead of failing when a source folder is missing.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    sources = args.source if args.source else default_sources(args.vault_root)
    return run_audit(
        sources=sources,
        output_dir=args.output_dir,
        include_hashes=args.include_hashes,
        allow_missing=args.allow_missing,
    )


if __name__ == "__main__":
    raise SystemExit(main())
