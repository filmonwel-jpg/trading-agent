#!/usr/bin/env python3
"""Merge downstream setup sidecar CSVs with Symbol/SetupEpochSec de-duplication.

The event snapshot injector loads sidecar rows keyed by (Symbol, SetupEpochSec),
so recovery rows must be appended without introducing duplicate keys. This helper
keeps the first occurrence of each key, making it safe to pass the original
sidecar first and supplemental/recovery sidecars afterward.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SYMBOL_COLUMNS = ("Symbol", "symbol")
EPOCH_COLUMNS = ("SetupEpochSec", "arm_epoch", "armEpoch")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def first_present(row: dict[str, Any], columns: tuple[str, ...]) -> str:
    for column in columns:
        value = row.get(column)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def parse_epoch(value: Any) -> int | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def row_key(row: dict[str, Any]) -> tuple[str, int] | None:
    symbol = first_present(row, SYMBOL_COLUMNS).upper()
    epoch = parse_epoch(first_present(row, EPOCH_COLUMNS))
    if not symbol or epoch is None:
        return None
    return symbol, epoch


def read_rows(paths: list[Path]) -> tuple[list[str], list[dict[str, str]], dict[str, Any]]:
    headers: list[str] = []
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, int]] = set()
    stats = {
        "input_files": [str(path) for path in paths],
        "input_rows": 0,
        "output_rows": 0,
        "duplicate_rows_skipped": 0,
        "missing_key_rows_kept": 0,
        "rows_by_file": {},
    }
    for path in paths:
        file_rows = 0
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            for field in reader.fieldnames:
                if field not in headers:
                    headers.append(field)
            for row in reader:
                stats["input_rows"] += 1
                file_rows += 1
                key = row_key(row)
                if key is None:
                    stats["missing_key_rows_kept"] += 1
                    rows.append({str(k): str(v or "") for k, v in row.items()})
                    continue
                if key in seen:
                    stats["duplicate_rows_skipped"] += 1
                    continue
                seen.add(key)
                rows.append({str(k): str(v or "") for k, v in row.items()})
        stats["rows_by_file"][str(path)] = file_rows
    stats["output_rows"] = len(rows)
    return headers, rows, stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True, type=Path, help="Input sidecar CSV. Pass multiple times; first key wins.")
    parser.add_argument("--output", required=True, type=Path, help="Merged output sidecar CSV.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing = [str(path) for path in args.input if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing input sidecar(s): {missing}")
    headers, rows, stats = read_rows(args.input)
    if not headers:
        raise ValueError("no CSV headers found in inputs")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})
    summary_path = args.output.with_suffix(".summary.json")
    summary = {"generated_at_utc": utc_now(), "output": str(args.output), **stats}
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "MERGE_DOWNSTREAM_SETUP_SIDECARS "
        f"output={args.output} summary={summary_path} "
        f"input_rows={stats['input_rows']} output_rows={stats['output_rows']} "
        f"duplicates_skipped={stats['duplicate_rows_skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

