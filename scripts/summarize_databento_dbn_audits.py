#!/usr/bin/env python3
"""Summarize DBN day audit outputs before pilot normalization.

Reads one or more directories produced by `scripts/audit_databento_dbn_day.py`
and emits compact sizing, throughput, and warning summaries.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


BYTES_PER_MIB = 1024 * 1024
BYTES_PER_GIB = 1024 * 1024 * 1024


@dataclass
class AuditSummaryRow:
    audit_dir: str
    source_label: str
    dataset: str
    schema: str
    date: str
    status: str
    row_count: int
    column_count: int
    file_mib: float
    dataframe_mib: float
    decode_seconds: float
    rows_per_second: float
    memory_expansion_ratio: float
    symbol_count: int
    instrument_id_count: int
    ts_event_min_utc: str
    ts_event_max_utc: str
    ts_recv_min_utc: str
    ts_recv_max_utc: str
    warning: str
    error: str


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


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


def _round(value: float, digits: int = 3) -> float:
    return round(float(value), digits)


def read_audit_dir(audit_dir: Path, warn_memory_ratio: float) -> list[AuditSummaryRow]:
    summary_path = audit_dir / "dbn_day_summary.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"missing {summary_path}")

    rows: list[AuditSummaryRow] = []
    with summary_path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            file_bytes = _int(raw.get("file_bytes"))
            dataframe_bytes = _int(raw.get("dataframe_memory_bytes"))
            row_count = _int(raw.get("row_count"))
            decode_seconds = _float(raw.get("decode_seconds"))
            status = str(raw.get("status", "")).strip()
            memory_ratio = dataframe_bytes / file_bytes if file_bytes > 0 else 0.0
            warnings: list[str] = []
            if status != "ok":
                warnings.append("status_not_ok")
            if row_count <= 0 and status == "ok":
                warnings.append("zero_rows")
            if memory_ratio >= warn_memory_ratio:
                warnings.append(f"memory_expansion_ratio>={warn_memory_ratio:g}")
            if not raw.get("ts_event_min_utc") and status == "ok":
                warnings.append("missing_ts_event_bounds")

            rows.append(
                AuditSummaryRow(
                    audit_dir=str(audit_dir),
                    source_label=str(raw.get("source_label", "")),
                    dataset=str(raw.get("dataset", "")),
                    schema=str(raw.get("schema", "")),
                    date=str(raw.get("date", "")),
                    status=status,
                    row_count=row_count,
                    column_count=_int(raw.get("column_count")),
                    file_mib=_round(file_bytes / BYTES_PER_MIB),
                    dataframe_mib=_round(dataframe_bytes / BYTES_PER_MIB),
                    decode_seconds=_round(decode_seconds, 6),
                    rows_per_second=_round(row_count / decode_seconds if decode_seconds > 0 else 0.0),
                    memory_expansion_ratio=_round(memory_ratio),
                    symbol_count=_int(raw.get("symbol_count")),
                    instrument_id_count=_int(raw.get("instrument_id_count")),
                    ts_event_min_utc=str(raw.get("ts_event_min_utc", "")),
                    ts_event_max_utc=str(raw.get("ts_event_max_utc", "")),
                    ts_recv_min_utc=str(raw.get("ts_recv_min_utc", "")),
                    ts_recv_max_utc=str(raw.get("ts_recv_max_utc", "")),
                    warning="|".join(warnings),
                    error=str(raw.get("error", "")),
                )
            )
    return rows


def write_csv(path: Path, rows: Iterable[AuditSummaryRow]) -> None:
    fieldnames = list(AuditSummaryRow.__dataclass_fields__.keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def print_table(rows: list[AuditSummaryRow]) -> None:
    if not rows:
        print("no DBN audit rows found")
        return
    headers = [
        "date",
        "source_label",
        "status",
        "rows",
        "file_mib",
        "df_mib",
        "ratio",
        "decode_s",
        "rows_s",
        "symbols",
        "inst",
        "warning",
    ]
    data = [
        [
            row.date,
            row.source_label,
            row.status,
            str(row.row_count),
            f"{row.file_mib:.1f}",
            f"{row.dataframe_mib:.1f}",
            f"{row.memory_expansion_ratio:.2f}",
            f"{row.decode_seconds:.3f}",
            f"{row.rows_per_second:.0f}",
            str(row.symbol_count),
            str(row.instrument_id_count),
            row.warning,
        ]
        for row in rows
    ]
    widths = [max(len(headers[i]), *(len(record[i]) for record in data)) for i in range(len(headers))]
    print("  ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    print("  ".join("-" * widths[i] for i in range(len(headers))))
    for record in data:
        print("  ".join(record[i].ljust(widths[i]) for i in range(len(headers))))


def build_manifest(rows: list[AuditSummaryRow], output_dir: Path | None, warn_memory_ratio: float) -> dict:
    return {
        "generated_at_utc": _utc_now(),
        "output_dir": str(output_dir) if output_dir else "",
        "warn_memory_ratio": warn_memory_ratio,
        "audit_dir_count": len({row.audit_dir for row in rows}),
        "row_count": len(rows),
        "error_count": sum(1 for row in rows if row.status != "ok" or bool(row.error)),
        "warning_count": sum(1 for row in rows if bool(row.warning)),
        "total_file_gib": _round(sum(row.file_mib for row in rows) / 1024),
        "total_dataframe_gib": _round(sum(row.dataframe_mib for row in rows) / 1024),
        "max_memory_expansion_ratio": _round(max((row.memory_expansion_ratio for row in rows), default=0.0)),
        "max_dataframe_mib": _round(max((row.dataframe_mib for row in rows), default=0.0)),
        "max_row_count": max((row.row_count for row in rows), default=0),
        "warnings": [asdict(row) for row in rows if row.warning],
        "errors": [asdict(row) for row in rows if row.status != "ok" or bool(row.error)],
    }


def summarize(audit_dirs: list[Path], output_dir: Path | None, warn_memory_ratio: float) -> int:
    rows: list[AuditSummaryRow] = []
    for audit_dir in audit_dirs:
        rows.extend(read_audit_dir(audit_dir.expanduser(), warn_memory_ratio=warn_memory_ratio))
    rows.sort(key=lambda row: (row.date, row.source_label))

    print_table(rows)
    manifest = build_manifest(rows, output_dir=output_dir, warn_memory_ratio=warn_memory_ratio)
    print(json.dumps({k: v for k, v in manifest.items() if k not in {"warnings", "errors"}}, indent=2, sort_keys=True))

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        write_csv(output_dir / "dbn_audit_summary.csv", rows)
        (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {output_dir / 'dbn_audit_summary.csv'}")
        print(f"wrote {output_dir / 'manifest.json'}")

    return 0 if manifest["error_count"] == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("audit_dirs", nargs="+", type=Path, help="One or more raw_audits/dbn_day_audit_* directories.")
    parser.add_argument("--output-dir", type=Path, help="Optional directory for combined summary CSV/JSON outputs.")
    parser.add_argument("--warn-memory-ratio", type=float, default=30.0, help="Warn when decoded dataframe memory / compressed file size exceeds this ratio.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return summarize(args.audit_dirs, output_dir=args.output_dir, warn_memory_ratio=args.warn_memory_ratio)


if __name__ == "__main__":
    raise SystemExit(main())
