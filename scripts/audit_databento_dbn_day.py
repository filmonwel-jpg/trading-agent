#!/usr/bin/env python3
"""Decode and summarize one Databento DBN day per pilot source.

This is a deliberately small pre-normalization audit. It loads one daily DBN file
at a time, writes compact CSV/JSON summaries, and should be run before any
full-window silver/gold feature build.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import resource
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_SOURCE_DIRS: tuple[tuple[str, str, str, str], ...] = (
    ("equs_tbbo_20260523", "EQUS.MINI", "tbbo", "/Volumes/DatabentoVault/EQUS-20260523-6J9KE98BJ9"),
    ("opra_ohlcv1s_20260523", "OPRA.PILLAR", "ohlcv-1s", "/Volumes/DatabentoVault/OPRA-20260523-MSV68VKVKD"),
    ("equs_mbp1_20260612", "EQUS.MINI", "mbp-1", "/Volumes/DatabentoVault/EQUS-20260612-36BEU4G7M8"),
    ("opra_tcbbo_20260612", "OPRA.PILLAR", "tcbbo", "/Volumes/DatabentoVault/OPRA-20260612-KN5TPHB5EF"),
    ("opra_definition_20260612", "OPRA.PILLAR", "definition", "/Volumes/DatabentoVault/OPRA-20260612-B5D4JV3GV6"),
)


@dataclass(frozen=True)
class SourceSpec:
    label: str
    dataset: str
    schema: str
    path: Path


@dataclass
class SourceDaySummary:
    source_label: str
    dataset: str
    schema: str
    date: str
    path: str
    file_bytes: int
    status: str
    row_count: int = 0
    column_count: int = 0
    dataframe_memory_bytes: int = 0
    decode_seconds: float = 0.0
    rss_peak_bytes: int = 0
    ts_event_min_utc: str = ""
    ts_event_max_utc: str = ""
    ts_recv_min_utc: str = ""
    ts_recv_max_utc: str = ""
    symbol_count: int = 0
    instrument_id_count: int = 0
    columns_json: str = "[]"
    error: str = ""


@dataclass
class ColumnProfile:
    source_label: str
    column: str
    dtype: str
    non_null_count: int
    null_count: int
    sample_values_json: str


@dataclass
class SymbolCount:
    source_label: str
    count_column: str
    value: str
    row_count: int


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_date(raw: str) -> str:
    cleaned = str(raw or "").strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit() or not cleaned.startswith("20"):
        raise argparse.ArgumentTypeError(f"date must be YYYYMMDD or YYYY-MM-DD, got {raw!r}")
    return cleaned


def rss_peak_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(usage)
    return int(usage) * 1024


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return str(value)


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
        raise argparse.ArgumentTypeError("--source must include label, dataset, schema, and path")
    return SourceSpec(label=label, dataset=dataset, schema=schema, path=Path(path).expanduser())


def default_sources() -> list[SourceSpec]:
    return [
        SourceSpec(label=label, dataset=dataset, schema=schema, path=Path(path))
        for label, dataset, schema, path in DEFAULT_SOURCE_DIRS
    ]


def sources_from_inventory(path: Path, include_duplicate_definition: bool) -> list[SourceSpec]:
    sources: list[SourceSpec] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            label = str(row.get("source_label", "")).strip()
            if not label:
                continue
            if label.endswith("_duplicate") and not include_duplicate_definition:
                continue
            if str(row.get("exists", "")).strip().lower() not in {"true", "1", "yes"}:
                continue
            sources.append(
                SourceSpec(
                    label=label,
                    dataset=str(row.get("dataset", "")).strip(),
                    schema=str(row.get("schema", "")).strip(),
                    path=Path(str(row.get("path", "")).strip()).expanduser(),
                )
            )
    return sources


def find_daily_file(source: SourceSpec, date: str) -> Path:
    if not source.path.is_dir():
        raise FileNotFoundError(f"source directory not found: {source.path}")
    exact_patterns = [
        f"*{date}.{source.schema}.dbn.zst",
        f"*{date}.{source.schema}.dbn",
        f"*{date}*{source.schema}*.dbn.zst",
        f"*{date}*{source.schema}*.dbn",
    ]
    matches: list[Path] = []
    for pattern in exact_patterns:
        matches.extend(sorted(source.path.glob(pattern)))
    unique_matches = sorted(set(matches))
    if not unique_matches:
        raise FileNotFoundError(f"no DBN file for {source.label} date={date} schema={source.schema} in {source.path}")
    if len(unique_matches) > 1:
        raise RuntimeError(f"multiple DBN files matched {source.label} date={date}: {unique_matches}")
    return unique_matches[0]


def load_dbn_frame(path: Path):
    try:
        import databento as db
    except ImportError as exc:
        raise RuntimeError("databento package is required; install requirements.txt first") from exc

    frame = db.DBNStore.from_file(path).to_df().reset_index(drop=False)
    if "ts_event" not in frame.columns and "index" in frame.columns:
        frame = frame.rename(columns={"index": "ts_event"})
    return frame


def timestamp_bounds(frame, column: str) -> tuple[str, str]:
    if column not in frame.columns or frame.empty:
        return "", ""
    import pandas as pd

    parsed = pd.to_datetime(frame[column], utc=True, errors="coerce").dropna()
    if parsed.empty:
        return "", ""
    return parsed.min().isoformat().replace("+00:00", "Z"), parsed.max().isoformat().replace("+00:00", "Z")


def compact_sample_values(series, limit: int = 5) -> str:
    values: list[Any] = []
    for value in series.dropna().head(limit).tolist():
        text = json_safe(value)
        if isinstance(text, str) and len(text) > 160:
            text = text[:157] + "..."
        values.append(text)
    return json.dumps(values, ensure_ascii=False)


def profile_columns(source_label: str, frame) -> list[ColumnProfile]:
    profiles: list[ColumnProfile] = []
    total_rows = len(frame)
    for column in frame.columns:
        series = frame[column]
        non_null = int(series.notna().sum())
        profiles.append(
            ColumnProfile(
                source_label=source_label,
                column=str(column),
                dtype=str(series.dtype),
                non_null_count=non_null,
                null_count=int(total_rows - non_null),
                sample_values_json=compact_sample_values(series),
            )
        )
    return profiles


def top_value_counts(source_label: str, frame, top_n: int) -> list[SymbolCount]:
    count_column = ""
    for candidate in ("symbol", "instrument_id"):
        if candidate in frame.columns:
            count_column = candidate
            break
    if not count_column:
        return []
    counts = frame[count_column].astype(str).value_counts(dropna=False).head(top_n)
    return [
        SymbolCount(source_label=source_label, count_column=count_column, value=str(value), row_count=int(count))
        for value, count in counts.items()
    ]


def summarize_frame(source: SourceSpec, date: str, path: Path, frame, decode_seconds: float) -> tuple[SourceDaySummary, list[ColumnProfile], list[SymbolCount]]:
    ts_event_min, ts_event_max = timestamp_bounds(frame, "ts_event")
    ts_recv_min, ts_recv_max = timestamp_bounds(frame, "ts_recv")
    symbol_count = int(frame["symbol"].nunique(dropna=True)) if "symbol" in frame.columns else 0
    instrument_id_count = int(frame["instrument_id"].nunique(dropna=True)) if "instrument_id" in frame.columns else 0
    summary = SourceDaySummary(
        source_label=source.label,
        dataset=source.dataset,
        schema=source.schema,
        date=date,
        path=str(path),
        file_bytes=path.stat().st_size,
        status="ok",
        row_count=int(len(frame)),
        column_count=int(len(frame.columns)),
        dataframe_memory_bytes=int(frame.memory_usage(deep=True).sum()),
        decode_seconds=round(float(decode_seconds), 6),
        rss_peak_bytes=rss_peak_bytes(),
        ts_event_min_utc=ts_event_min,
        ts_event_max_utc=ts_event_max,
        ts_recv_min_utc=ts_recv_min,
        ts_recv_max_utc=ts_recv_max,
        symbol_count=symbol_count,
        instrument_id_count=instrument_id_count,
        columns_json=json.dumps([str(column) for column in frame.columns]),
    )
    return summary, profile_columns(source.label, frame), top_value_counts(source.label, frame, top_n=20)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_sample(path: Path, frame, max_rows: int) -> None:
    sample = frame.head(max_rows).copy()
    for column in sample.columns:
        sample[column] = sample[column].map(json_safe)
    sample.to_csv(path, index=False)


def audit_sources(sources: list[SourceSpec], date: str, output_dir: Path, sample_rows: int, continue_on_error: bool) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[SourceDaySummary] = []
    profiles: list[ColumnProfile] = []
    counts: list[SymbolCount] = []

    for source in sources:
        print(f"auditing {source.label} date={date} schema={source.schema}")
        try:
            path = find_daily_file(source, date)
            start = time.perf_counter()
            frame = load_dbn_frame(path)
            decode_seconds = time.perf_counter() - start
            summary, source_profiles, source_counts = summarize_frame(source, date, path, frame, decode_seconds)
            summaries.append(summary)
            profiles.extend(source_profiles)
            counts.extend(source_counts)
            write_sample(output_dir / f"sample_{source.label}.csv", frame, max_rows=sample_rows)
            print(
                f"  ok rows={summary.row_count} cols={summary.column_count} "
                f"file_bytes={summary.file_bytes} decode_seconds={summary.decode_seconds}"
            )
        except Exception as exc:
            summary = SourceDaySummary(
                source_label=source.label,
                dataset=source.dataset,
                schema=source.schema,
                date=date,
                path=str(source.path),
                file_bytes=0,
                status="error",
                error=str(exc),
            )
            summaries.append(summary)
            print(f"  error: {exc}", file=sys.stderr)
            if not continue_on_error:
                break

    write_csv(output_dir / "dbn_day_summary.csv", [asdict(row) for row in summaries], list(SourceDaySummary.__dataclass_fields__.keys()))
    write_csv(output_dir / "column_profiles.csv", [asdict(row) for row in profiles], list(ColumnProfile.__dataclass_fields__.keys()))
    write_csv(output_dir / "top_value_counts.csv", [asdict(row) for row in counts], list(SymbolCount.__dataclass_fields__.keys()))
    manifest = {
        "generated_at_utc": utc_now(),
        "date": date,
        "output_dir": str(output_dir),
        "sources": [asdict(row) for row in summaries],
        "error_count": sum(1 for row in summaries if row.status != "ok"),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {output_dir}")
    return 0 if manifest["error_count"] == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=normalize_date, help="DBN day to audit, YYYYMMDD or YYYY-MM-DD.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for compact audit CSV/JSON outputs.")
    parser.add_argument("--source-inventory", type=Path, help="Optional source_inventory.csv from audit_databento_pilot_sources.py.")
    parser.add_argument("--source", action="append", type=parse_source, help="Override sources. Format: label=dataset:schema:/absolute/path. Repeatable.")
    parser.add_argument("--source-label", action="append", help="Only audit matching source label. Repeatable.")
    parser.add_argument("--include-duplicate-definition", action="store_true", help="Include source labels ending in _duplicate from source_inventory.csv.")
    parser.add_argument("--sample-rows", type=int, default=5, help="Number of head rows to write per source sample CSV.")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue auditing remaining sources if one source fails.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.source:
        sources = args.source
    elif args.source_inventory:
        sources = sources_from_inventory(args.source_inventory, include_duplicate_definition=args.include_duplicate_definition)
    else:
        sources = default_sources()
    if args.source_label:
        wanted = set(args.source_label)
        sources = [source for source in sources if source.label in wanted]
    if not sources:
        raise SystemExit("no sources selected")
    return audit_sources(
        sources=sources,
        date=args.date,
        output_dir=args.output_dir,
        sample_rows=max(0, args.sample_rows),
        continue_on_error=args.continue_on_error,
    )


if __name__ == "__main__":
    raise SystemExit(main())
