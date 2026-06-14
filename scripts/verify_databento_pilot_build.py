#!/usr/bin/env python3
"""Verify outputs from the Databento 10-day fixed-quality pilot build.

The verifier is intentionally lightweight: it reads generated CSV headers and
line-level summaries, but it does not train models and does not decode DBN files.
It is meant to be run immediately after ``build_30s_from_5s_csv.py`` completes.
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


DEFAULT_SYMBOLS = ["TSLA", "TQQQ", "NVDA", "SPY", "QQQ"]
CADENCE_SUBDIRS = {"1s": "data_1s", "5s": "data_5s", "30s": "data_30s"}
COMBINED_FILENAMES = {"1s": "combined_1s.csv", "5s": "combined_5s.csv", "30s": "combined_30s.csv"}
ROWS_PER_DAY_BY_CADENCE = {"1s": 23_400, "5s": 4_680, "30s": 780}
REQUIRED_BAR_COLUMNS = [
    "Symbol",
    "Timestamp",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "DataQualityFlags",
    "ChildDataQualityFlagUnion",
    "TradeSecondsPresent",
    "QuoteUpdateSecondsPresent",
    "QuoteStateSecondsValid",
    "SyntheticSeconds",
    "TradeCoverage",
    "QuoteUpdateCoverage",
    "QuoteStateCoverage",
    "SyntheticCoverage",
    "QuoteAgeMsMean",
    "QuoteAgeMsMax",
    "ValidSpreadCoverage",
    "LockedCrossedSeconds",
    "QualityScore",
]
REQUIRED_REPORT_COLUMNS = [
    "Symbol",
    "days",
    "rows",
    "median_volume_30s",
    "median_trade_count_30s",
    "median_spread_bps",
    "zero_volume_bar_frac",
    "option_active_bar_frac",
    "median_close",
    "modeling_cohort",
    "recommended_training_mode",
]


@dataclass
class CsvSummary:
    cadence: str
    scope: str
    symbol: str
    path: str
    exists: bool
    bytes: int
    row_count: int
    column_count: int
    missing_required_columns: str
    date_count: int
    dates: str
    symbol_count: int
    symbols: str
    data_quality_flag_values: str
    child_flag_union_values: str
    parent_child_flag_identical_rows: int
    parent_child_flag_different_rows: int


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[CsvSummary]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CsvSummary.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def latest_child(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No paths matching {pattern!r} under {root}")
    return matches[0]


def _short_counter(counter: Counter[str], limit: int = 12) -> str:
    if not counter:
        return ""
    parts = [f"{key}:{value}" for key, value in counter.most_common(limit)]
    remaining = len(counter) - limit
    if remaining > 0:
        parts.append(f"+{remaining}_more")
    return "|".join(parts)


def _timestamp_date(raw: str) -> str:
    value = str(raw or "").strip()
    if len(value) >= 8 and value[:8].isdigit():
        return value[:8]
    return ""


def summarize_bar_csv(path: Path, cadence: str, scope: str, symbol: str, required_columns: list[str]) -> CsvSummary:
    if not path.exists():
        return CsvSummary(
            cadence=cadence,
            scope=scope,
            symbol=symbol,
            path=str(path),
            exists=False,
            bytes=0,
            row_count=0,
            column_count=0,
            missing_required_columns=",".join(required_columns),
            date_count=0,
            dates="",
            symbol_count=0,
            symbols="",
            data_quality_flag_values="",
            child_flag_union_values="",
            parent_child_flag_identical_rows=0,
            parent_child_flag_different_rows=0,
        )

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing = [col for col in required_columns if col not in fieldnames]
        dates: set[str] = set()
        symbols: set[str] = set()
        parent_flags: Counter[str] = Counter()
        child_flags: Counter[str] = Counter()
        identical_rows = 0
        different_rows = 0
        row_count = 0
        for row in reader:
            row_count += 1
            date = _timestamp_date(row.get("Timestamp", ""))
            if date:
                dates.add(date)
            row_symbol = str(row.get("Symbol", "")).strip()
            if row_symbol:
                symbols.add(row_symbol)
            parent = str(row.get("DataQualityFlags", "")).strip() or "<blank>"
            child = str(row.get("ChildDataQualityFlagUnion", "")).strip() or "<blank>"
            parent_flags[parent] += 1
            child_flags[child] += 1
            if parent == child:
                identical_rows += 1
            else:
                different_rows += 1

    return CsvSummary(
        cadence=cadence,
        scope=scope,
        symbol=symbol,
        path=str(path),
        exists=True,
        bytes=path.stat().st_size,
        row_count=row_count,
        column_count=len(fieldnames),
        missing_required_columns=",".join(missing),
        date_count=len(dates),
        dates=",".join(sorted(dates)),
        symbol_count=len(symbols),
        symbols=",".join(sorted(symbols)),
        data_quality_flag_values=_short_counter(parent_flags),
        child_flag_union_values=_short_counter(child_flags),
        parent_child_flag_identical_rows=identical_rows,
        parent_child_flag_different_rows=different_rows,
    )


def resolve_build_root(args: argparse.Namespace) -> Path:
    if args.build_root:
        return args.build_root.expanduser().resolve()
    if not args.lake_root:
        raise ValueError("Provide --build-root or --lake-root")
    return latest_child(args.lake_root.expanduser().resolve() / "model_training_sets", "pilot_10d_fixed_quality_*")


def resolve_pilot_dates(args: argparse.Namespace) -> tuple[list[str], Path | None]:
    if args.expected_date:
        return [date.replace("-", "") for date in args.expected_date], None
    pilot_dir = None
    if args.pilot_dates_dir:
        pilot_dir = args.pilot_dates_dir.expanduser().resolve()
    elif args.lake_root:
        source_manifest_root = args.lake_root.expanduser().resolve() / "source_manifests"
        if source_manifest_root.exists():
            pilot_dir = latest_child(source_manifest_root, "pilot_dates_latest10_*")
    if pilot_dir is not None:
        rows = read_csv_dicts(pilot_dir / "pilot_dates.csv")
        return [row["date"].replace("-", "") for row in rows], pilot_dir
    return [], None


def _expected_rows(expected_dates: list[str], expected_days: int, cadence: str, symbol_count: int, combined: bool) -> int:
    days = len(expected_dates) if expected_dates else expected_days
    rows = ROWS_PER_DAY_BY_CADENCE[cadence] * days
    return rows * symbol_count if combined else rows


def _parse_float(raw: Any, default: float = 0.0) -> float:
    try:
        return float(str(raw or "").strip())
    except ValueError:
        return default


def verify(
    build_root: Path,
    output_dir: Path,
    expected_symbols: list[str],
    expected_dates: list[str],
    expected_days: int,
    pilot_dates_dir: Path | None = None,
    strict_row_counts: bool = True,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    build_root = build_root.expanduser().resolve()
    expected_symbols = [symbol.upper() for symbol in expected_symbols]
    expected_date_set = set(expected_dates)
    errors: list[str] = []
    warnings: list[str] = []
    summaries: list[CsvSummary] = []

    if not build_root.exists():
        errors.append(f"build root does not exist: {build_root}")

    for subdir in list(CADENCE_SUBDIRS.values()) + ["combined", "reports"]:
        if not (build_root / subdir).is_dir():
            errors.append(f"missing build subdirectory: {build_root / subdir}")

    for cadence, subdir in CADENCE_SUBDIRS.items():
        for symbol in expected_symbols:
            summary = summarize_bar_csv(build_root / subdir / f"{symbol}_{cadence}_training.csv", cadence, "per_symbol", symbol, REQUIRED_BAR_COLUMNS)
            summaries.append(summary)
            expected_rows = _expected_rows(expected_dates, expected_days, cadence, len(expected_symbols), combined=False)
            if not summary.exists:
                errors.append(f"missing {cadence} file for {symbol}: {summary.path}")
                continue
            if summary.row_count <= 0:
                errors.append(f"empty {cadence} file for {symbol}: {summary.path}")
            if summary.missing_required_columns:
                errors.append(f"{cadence} file for {symbol} missing required columns: {summary.missing_required_columns}")
            if summary.symbol_count != 1 or summary.symbols != symbol:
                errors.append(f"{cadence} file for {symbol} has unexpected symbols: {summary.symbols or '<none>'}")
            if expected_date_set and set(filter(None, summary.dates.split(","))) != expected_date_set:
                errors.append(f"{cadence} file for {symbol} dates={summary.dates}, expected={','.join(expected_dates)}")
            if strict_row_counts and summary.row_count != expected_rows:
                errors.append(f"{cadence} file for {symbol} rows={summary.row_count}, expected={expected_rows}")

    for cadence, filename in COMBINED_FILENAMES.items():
        summary = summarize_bar_csv(build_root / "combined" / filename, cadence, "combined", "ALL", REQUIRED_BAR_COLUMNS)
        summaries.append(summary)
        expected_rows = _expected_rows(expected_dates, expected_days, cadence, len(expected_symbols), combined=True)
        if not summary.exists:
            errors.append(f"missing combined {cadence} file: {summary.path}")
            continue
        if summary.missing_required_columns:
            errors.append(f"combined {cadence} file missing required columns: {summary.missing_required_columns}")
        if set(filter(None, summary.symbols.split(","))) != set(expected_symbols):
            errors.append(f"combined {cadence} file symbols={summary.symbols}, expected={','.join(expected_symbols)}")
        if expected_date_set and set(filter(None, summary.dates.split(","))) != expected_date_set:
            errors.append(f"combined {cadence} file dates={summary.dates}, expected={','.join(expected_dates)}")
        if strict_row_counts and summary.row_count != expected_rows:
            errors.append(f"combined {cadence} rows={summary.row_count}, expected={expected_rows}")

    report_path = build_root / "reports" / "symbol_model_plan.csv"
    report_rows: list[dict[str, str]] = []
    if not report_path.exists():
        errors.append(f"missing assessment report: {report_path}")
    else:
        report_rows = read_csv_dicts(report_path)
        report_columns = list(report_rows[0].keys()) if report_rows else []
        missing_report_cols = [col for col in REQUIRED_REPORT_COLUMNS if col not in report_columns]
        if missing_report_cols:
            errors.append(f"assessment report missing columns: {missing_report_cols}")
        report_symbols = sorted(row.get("Symbol", "") for row in report_rows)
        if report_symbols != sorted(expected_symbols):
            errors.append(f"assessment report symbols={report_symbols}, expected={sorted(expected_symbols)}")
        for row in report_rows:
            symbol = row.get("Symbol", "")
            days = int(_parse_float(row.get("days")))
            rows = int(_parse_float(row.get("rows")))
            expected_30s_rows = _expected_rows(expected_dates, expected_days, "30s", len(expected_symbols), combined=False)
            expected_report_days = len(expected_dates) if expected_dates else expected_days
            if strict_row_counts and days != expected_report_days:
                errors.append(f"assessment report {symbol} days={days}, expected={expected_report_days}")
            if strict_row_counts and rows != expected_30s_rows:
                errors.append(f"assessment report {symbol} rows={rows}, expected={expected_30s_rows}")

    for summary in summaries:
        if summary.exists and summary.cadence != "1s" and summary.parent_child_flag_different_rows == 0:
            warnings.append(f"{summary.scope} {summary.cadence} {summary.symbol} has no parent/child quality-flag differences; inspect if unexpected")

    manifest = {
        "generated_at_utc": utc_now(),
        "build_root": str(build_root),
        "output_dir": str(output_dir),
        "pilot_dates_dir": str(pilot_dates_dir) if pilot_dates_dir else "",
        "expected_symbols": expected_symbols,
        "expected_dates": expected_dates,
        "expected_days": len(expected_dates) if expected_dates else expected_days,
        "strict_row_counts": strict_row_counts,
        "file_summary_count": len(summaries),
        "assessment_report": str(report_path),
        "assessment_report_rows": len(report_rows),
        "total_output_bytes": sum(summary.bytes for summary in summaries) + (report_path.stat().st_size if report_path.exists() else 0),
        "total_output_gib": round((sum(summary.bytes for summary in summaries) + (report_path.stat().st_size if report_path.exists() else 0)) / (1024 ** 3), 3),
        "errors": errors,
        "warnings": warnings,
    }

    (output_dir / "pilot_build_check.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(output_dir / "pilot_build_file_summary.csv", summaries)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"wrote {output_dir / 'pilot_build_check.json'}")
    print(f"wrote {output_dir / 'pilot_build_file_summary.csv'}")
    print("PILOT_BUILD_CHECK=FAIL" if errors else "PILOT_BUILD_CHECK=PASS")
    return 2 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, help="pilot_10d_fixed_quality_* build output root. Defaults to latest under --lake-root.")
    parser.add_argument("--lake-root", type=Path, help="External data_lake_v2 root; used for latest build/pilot discovery.")
    parser.add_argument("--pilot-dates-dir", type=Path, help="pilot_dates_latest10_* directory. Defaults to latest under --lake-root/source_manifests.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for post-build check outputs.")
    parser.add_argument("--expected-symbol", action="append", help="Expected symbol. Repeatable. Defaults to the five pilot symbols.")
    parser.add_argument("--expected-date", action="append", help="Expected YYYYMMDD or YYYY-MM-DD date. Repeatable; overrides --pilot-dates-dir.")
    parser.add_argument("--expected-days", type=int, default=10, help="Expected day count if expected dates are not provided/discovered.")
    parser.add_argument("--allow-row-count-mismatch", action="store_true", help="Warn through summaries but do not fail exact session row-count mismatches.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    build_root = resolve_build_root(args)
    expected_dates, pilot_dates_dir = resolve_pilot_dates(args)
    expected_symbols = args.expected_symbol if args.expected_symbol else DEFAULT_SYMBOLS
    return verify(
        build_root=build_root,
        output_dir=args.output_dir.expanduser().resolve(),
        expected_symbols=expected_symbols,
        expected_dates=expected_dates,
        expected_days=args.expected_days,
        pilot_dates_dir=pilot_dates_dir,
        strict_row_counts=not args.allow_row_count_mismatch,
    )


if __name__ == "__main__":
    raise SystemExit(main())

