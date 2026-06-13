#!/usr/bin/env python3
"""Plan a fully paired Databento pilot date set before normalization.

Inputs are the `paired_dates.csv` and `source_files.csv` artifacts from
`audit_databento_pilot_sources.py`. Optionally pass the combined
`dbn_audit_summary.csv` from `summarize_databento_dbn_audits.py` to get rough
decoded-memory estimates for the selected date window.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXCLUDED_PAIRED_DATE_COLUMNS = {"date", "present_source_count", "all_sources_present"}
BYTES_PER_MIB = 1024 * 1024
BYTES_PER_GIB = 1024 * 1024 * 1024


@dataclass
class PlannedDate:
    order: int
    date: str
    iso_date: str


@dataclass
class PlannedSourceFile:
    date: str
    source_label: str
    dataset: str
    schema: str
    path: str
    name: str
    bytes: int
    mib: float
    sha256_status: str
    sha256: str
    sha256_error: str


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_date(raw: str) -> str:
    value = str(raw or "").strip().replace("-", "")
    if len(value) != 8 or not value.isdigit() or not value.startswith("20"):
        raise argparse.ArgumentTypeError(f"date must be YYYYMMDD or YYYY-MM-DD, got {raw!r}")
    return value


def iso_date(date: str) -> str:
    return f"{date[:4]}-{date[4:6]}-{date[6:8]}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def truthy(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes"}


def infer_source_labels(paired_rows: list[dict[str, str]], required: list[str] | None) -> list[str]:
    if required:
        return list(required)
    if not paired_rows:
        return []
    return [column for column in paired_rows[0].keys() if column not in EXCLUDED_PAIRED_DATE_COLUMNS]


def select_dates(
    paired_rows: list[dict[str, str]],
    source_labels: list[str],
    days: int,
    start_date: str | None,
    end_date: str | None,
    exclude_dates: set[str],
    strategy: str,
    allow_fewer: bool,
) -> list[PlannedDate]:
    candidates: list[str] = []
    for row in paired_rows:
        date = normalize_date(row.get("date", ""))
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        if date in exclude_dates:
            continue
        if all(truthy(row.get(label, "")) for label in source_labels):
            candidates.append(date)

    candidates = sorted(set(candidates))
    if strategy == "latest":
        selected = candidates[-days:]
    elif strategy == "earliest":
        selected = candidates[:days]
    else:
        raise ValueError(f"unsupported strategy: {strategy}")
    if len(selected) < days and not allow_fewer:
        raise RuntimeError(f"only {len(selected)} fully paired dates available, requested {days}")
    return [PlannedDate(order=index + 1, date=date, iso_date=iso_date(date)) for index, date in enumerate(selected)]


def file_index(source_file_rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    out: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in source_file_rows:
        if not truthy(row.get("is_dbn", "")):
            continue
        date = str(row.get("date", "")).strip()
        source_label = str(row.get("source_label", "")).strip()
        if not date or not source_label:
            continue
        out.setdefault((source_label, date), []).append(row)
    return out


def _int(raw: object, default: int = 0) -> int:
    try:
        return int(float(str(raw or "").strip()))
    except ValueError:
        return default


def _float(raw: object, default: float = 0.0) -> float:
    try:
        return float(str(raw or "").strip())
    except ValueError:
        return default


def plan_source_files(
    selected_dates: list[PlannedDate],
    source_labels: list[str],
    source_file_rows: list[dict[str, str]],
) -> list[PlannedSourceFile]:
    indexed = file_index(source_file_rows)
    planned: list[PlannedSourceFile] = []
    for planned_date in selected_dates:
        for label in source_labels:
            matches = indexed.get((label, planned_date.date), [])
            if len(matches) != 1:
                raise RuntimeError(f"expected exactly one DBN file for source={label} date={planned_date.date}, found {len(matches)}")
            row = matches[0]
            size = _int(row.get("bytes"))
            planned.append(
                PlannedSourceFile(
                    date=planned_date.date,
                    source_label=label,
                    dataset=str(row.get("dataset", "")),
                    schema=str(row.get("schema", "")),
                    path=str(row.get("path", "")),
                    name=str(row.get("name", "")),
                    bytes=size,
                    mib=round(size / BYTES_PER_MIB, 3),
                    sha256_status=str(row.get("sha256_status", "")),
                    sha256=str(row.get("sha256", "")),
                    sha256_error=str(row.get("sha256_error", "")),
                )
            )
    return planned


def load_audit_ratios(path: Path | None) -> dict[str, dict[str, float]]:
    if not path:
        return {}
    ratios: dict[str, dict[str, float]] = {}
    for row in read_csv(path):
        label = str(row.get("source_label", "")).strip()
        if not label or str(row.get("status", "")).strip() != "ok":
            continue
        file_mib = _float(row.get("file_mib"))
        dataframe_mib = _float(row.get("dataframe_mib"))
        ratio = _float(row.get("memory_expansion_ratio"))
        if ratio <= 0 and file_mib > 0:
            ratio = dataframe_mib / file_mib
        existing = ratios.setdefault(label, {"max_ratio": 0.0, "max_file_mib": 0.0, "max_dataframe_mib": 0.0})
        existing["max_ratio"] = max(existing["max_ratio"], ratio)
        existing["max_file_mib"] = max(existing["max_file_mib"], file_mib)
        existing["max_dataframe_mib"] = max(existing["max_dataframe_mib"], dataframe_mib)
    return ratios


def estimate_by_source(planned_files: list[PlannedSourceFile], audit_ratios: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    by_source: dict[str, list[PlannedSourceFile]] = {}
    for planned_file in planned_files:
        by_source.setdefault(planned_file.source_label, []).append(planned_file)

    estimates: list[dict[str, Any]] = []
    for label, files in sorted(by_source.items()):
        compressed_mib = sum(row.mib for row in files)
        max_file_mib = max((row.mib for row in files), default=0.0)
        ratio = audit_ratios.get(label, {}).get("max_ratio", 0.0)
        estimates.append(
            {
                "source_label": label,
                "selected_file_count": len(files),
                "compressed_mib": round(compressed_mib, 3),
                "compressed_gib": round(compressed_mib / 1024, 3),
                "max_single_file_mib": round(max_file_mib, 3),
                "observed_max_memory_expansion_ratio": round(ratio, 3),
                "estimated_peak_dataframe_mib": round(max_file_mib * ratio, 3) if ratio else None,
                "estimated_total_dataframe_mib_if_materialized": round(compressed_mib * ratio, 3) if ratio else None,
            }
        )
    return estimates


def plan(
    paired_dates_path: Path,
    source_files_path: Path,
    output_dir: Path,
    days: int,
    strategy: str,
    start_date: str | None,
    end_date: str | None,
    exclude_dates: set[str],
    required_source_labels: list[str] | None,
    dbn_audit_summary_path: Path | None,
    allow_fewer: bool,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    paired_rows = read_csv(paired_dates_path)
    source_file_rows = read_csv(source_files_path)
    source_labels = infer_source_labels(paired_rows, required_source_labels)
    selected_dates = select_dates(
        paired_rows=paired_rows,
        source_labels=source_labels,
        days=days,
        start_date=start_date,
        end_date=end_date,
        exclude_dates=exclude_dates,
        strategy=strategy,
        allow_fewer=allow_fewer,
    )
    planned_files = plan_source_files(selected_dates, source_labels, source_file_rows)
    audit_ratios = load_audit_ratios(dbn_audit_summary_path)
    estimates = estimate_by_source(planned_files, audit_ratios)

    write_csv(output_dir / "pilot_dates.csv", [asdict(row) for row in selected_dates], list(PlannedDate.__dataclass_fields__.keys()))
    write_csv(output_dir / "pilot_source_files.csv", [asdict(row) for row in planned_files], list(PlannedSourceFile.__dataclass_fields__.keys()))
    write_csv(
        output_dir / "pilot_size_estimates_by_source.csv",
        estimates,
        [
            "source_label",
            "selected_file_count",
            "compressed_mib",
            "compressed_gib",
            "max_single_file_mib",
            "observed_max_memory_expansion_ratio",
            "estimated_peak_dataframe_mib",
            "estimated_total_dataframe_mib_if_materialized",
        ],
    )
    manifest = {
        "generated_at_utc": utc_now(),
        "paired_dates_path": str(paired_dates_path),
        "source_files_path": str(source_files_path),
        "dbn_audit_summary_path": str(dbn_audit_summary_path) if dbn_audit_summary_path else "",
        "output_dir": str(output_dir),
        "strategy": strategy,
        "requested_days": days,
        "selected_day_count": len(selected_dates),
        "selected_dates": [row.date for row in selected_dates],
        "source_labels": source_labels,
        "exclude_dates": sorted(exclude_dates),
        "total_selected_files": len(planned_files),
        "total_compressed_gib": round(sum(row.bytes for row in planned_files) / BYTES_PER_GIB, 3),
        "size_estimates_by_source": estimates,
        "hash_error_count": sum(1 for row in planned_files if row.sha256_status == "error"),
        "hash_missing_count": sum(1 for row in planned_files if not row.sha256),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"selected {len(selected_dates)} dates: {', '.join(row.iso_date for row in selected_dates)}")
    print(f"selected files: {len(planned_files)}")
    print(f"total compressed GiB: {manifest['total_compressed_gib']}")
    print(f"wrote {output_dir / 'pilot_dates.csv'}")
    print(f"wrote {output_dir / 'pilot_source_files.csv'}")
    print(f"wrote {output_dir / 'pilot_size_estimates_by_source.csv'}")
    print(f"wrote {output_dir / 'manifest.json'}")
    if manifest["hash_error_count"]:
        print(f"warning: selected files include {manifest['hash_error_count']} hash errors")
        return 3
    if manifest["hash_missing_count"]:
        print(f"warning: selected files include {manifest['hash_missing_count']} files without SHA-256 hashes")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paired-dates", required=True, type=Path, help="paired_dates.csv from source audit.")
    parser.add_argument("--source-files", required=True, type=Path, help="source_files.csv from source audit.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output directory for pilot date/file manifest.")
    parser.add_argument("--days", type=int, default=10, help="Number of fully paired dates to select.")
    parser.add_argument("--strategy", choices=["latest", "earliest"], default="latest", help="Date selection strategy.")
    parser.add_argument("--start-date", type=normalize_date, help="Optional inclusive lower date bound.")
    parser.add_argument("--end-date", type=normalize_date, help="Optional inclusive upper date bound.")
    parser.add_argument("--exclude-date", action="append", type=normalize_date, help="Date to exclude. Repeatable.")
    parser.add_argument("--required-source-label", action="append", help="Required source label. Repeatable. Defaults to all source columns in paired_dates.csv.")
    parser.add_argument("--dbn-audit-summary", type=Path, help="Optional dbn_audit_summary.csv for decoded memory estimates.")
    parser.add_argument("--allow-fewer", action="store_true", help="Allow fewer selected dates than --days when insufficient fully paired dates exist.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.days <= 0:
        raise SystemExit("--days must be positive")
    return plan(
        paired_dates_path=args.paired_dates,
        source_files_path=args.source_files,
        output_dir=args.output_dir,
        days=args.days,
        strategy=args.strategy,
        start_date=args.start_date,
        end_date=args.end_date,
        exclude_dates=set(args.exclude_date or []),
        required_source_labels=args.required_source_label,
        dbn_audit_summary_path=args.dbn_audit_summary,
        allow_fewer=args.allow_fewer,
    )


if __name__ == "__main__":
    raise SystemExit(main())
