#!/usr/bin/env python3
"""Normalize Databento definition DBNs into compact instrument metadata CSVs."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from databento_silver_utils import (
    ensure_ts_event_column,
    load_dbn_frame,
    load_pilot_source_files,
    non_empty_path_arg,
    normalize_date_arg,
    normalize_expiration,
    normalize_fixed_price,
    numeric_series,
    option_meta_for_symbols,
    parse_option_symbol,
    split_symbols,
    utc_now,
    write_csv_rows,
    write_json,
)


DEFAULT_SOURCE_LABELS = ["equs_definition_20260612", "opra_definition_20260612"]
SCHEMA_VERSION = "databento_definition_silver_v1"

OUTPUT_COLUMNS = [
    "date",
    "source_label",
    "dataset",
    "schema",
    "instrument_id",
    "symbol",
    "raw_symbol",
    "asset_class",
    "underlying",
    "option_right",
    "expiration",
    "strike",
    "multiplier",
    "ts_event_utc",
]

SUMMARY_COLUMNS = [
    "date",
    "source_label",
    "dataset",
    "schema",
    "source_path",
    "raw_rows",
    "output_rows",
    "instrument_id_count",
    "symbol_count",
    "underlying_count",
    "option_contract_count",
    "output_path",
]


@dataclass
class DefinitionWriteSummary:
    date: str
    source_label: str
    dataset: str
    schema: str
    source_path: str
    raw_rows: int
    output_rows: int
    instrument_id_count: int
    symbol_count: int
    underlying_count: int
    option_contract_count: int
    output_path: str


def _first_text_column(frame: pd.DataFrame, candidates: list[str], default: str = "") -> pd.Series:
    out = pd.Series(default, index=frame.index, dtype="object")
    for column in candidates:
        if column in frame.columns:
            values = frame[column].astype(str).str.strip()
            out = out.mask(out.astype(str).str.len().eq(0), values)
    return out.fillna(default)


def _first_numeric_column(frame: pd.DataFrame, candidates: list[str], default: float = np.nan) -> pd.Series:
    out = pd.Series(default, index=frame.index, dtype="float64")
    for column in candidates:
        if column in frame.columns:
            out = out.combine_first(pd.to_numeric(frame[column], errors="coerce"))
    return out


def _format_ts_event(frame: pd.DataFrame) -> pd.Series:
    if "ts_event" not in frame.columns:
        return pd.Series("", index=frame.index, dtype="object")
    parsed = pd.to_datetime(frame["ts_event"], utc=True, errors="coerce")
    return parsed.map(lambda ts: ts.isoformat().replace("+00:00", "Z") if pd.notna(ts) else "")


def normalize_definition_frame(
    raw: pd.DataFrame,
    market_day: str,
    *,
    source_label: str,
    dataset: str,
    schema: str = "definition",
    symbols: set[str] | None = None,
) -> pd.DataFrame:
    frame = ensure_ts_event_column(raw).copy()
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    symbol = _first_text_column(frame, ["symbol", "raw_symbol", "instrument_symbol", "asset", "ticker"])
    symbol = symbol.astype(str).str.strip().str.upper()
    parsed_meta = option_meta_for_symbols(symbol.dropna().unique())
    parsed_underlying = symbol.map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("underlying", ""))
    parsed_right = symbol.map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("option_right", ""))
    parsed_expiration = symbol.map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("expiration", ""))
    parsed_strike = symbol.map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("strike", np.nan))

    underlying = _first_text_column(frame, ["underlying", "underlying_symbol", "parent", "root_symbol"])
    underlying = underlying.astype(str).str.strip().str.upper()
    underlying = underlying.mask(underlying.str.len().eq(0), parsed_underlying)
    option_right = _first_text_column(frame, ["option_right", "right", "put_call", "instrument_class"])
    option_right = option_right.astype(str).str.strip().str.upper().str[:1]
    option_right = option_right.mask(~option_right.isin(["C", "P"]), parsed_right)

    expiration_text = _first_text_column(frame, ["expiration", "expiration_date", "maturity_date"])
    expiration = expiration_text.map(normalize_expiration)
    expiration = expiration.mask(expiration.astype(str).str.len().eq(0), parsed_expiration)

    strike = _first_numeric_column(frame, ["strike", "strike_price", "strike_px"], default=np.nan)
    strike = strike.map(lambda value: normalize_fixed_price(value, fallback=np.nan))
    strike = strike.combine_first(pd.to_numeric(parsed_strike, errors="coerce"))

    multiplier = _first_numeric_column(frame, ["multiplier", "contract_multiplier", "unit_of_measure_qty", "contract_size"], default=np.nan)
    is_option = option_right.isin(["C", "P"])
    multiplier = multiplier.where(multiplier.gt(0.0), np.nan)
    multiplier = multiplier.fillna(pd.Series(np.where(is_option, 100.0, 1.0), index=frame.index))

    asset_class = pd.Series(np.where(is_option, "option", "equity"), index=frame.index)
    equity_underlying = symbol.where(~is_option, underlying)
    underlying = underlying.mask(underlying.astype(str).str.len().eq(0), equity_underlying)

    out = pd.DataFrame(
        {
            "date": market_day,
            "source_label": source_label,
            "dataset": dataset,
            "schema": schema,
            "instrument_id": numeric_series(frame, "instrument_id", default=0.0).fillna(0).astype("int64"),
            "symbol": symbol,
            "raw_symbol": symbol,
            "asset_class": asset_class,
            "underlying": underlying,
            "option_right": option_right.where(is_option, ""),
            "expiration": expiration.where(is_option, ""),
            "strike": strike.where(is_option, np.nan),
            "multiplier": multiplier,
            "ts_event_utc": _format_ts_event(frame),
        }
    )

    if symbols:
        out = out[out["underlying"].isin(symbols) | out["symbol"].isin(symbols)].copy()
    out = out.drop_duplicates(subset=["instrument_id", "symbol"], keep="last")
    return out.sort_values(["asset_class", "underlying", "option_right", "expiration", "strike", "symbol"]).reset_index(drop=True)[OUTPUT_COLUMNS]


def write_definition_output(
    frame: pd.DataFrame,
    *,
    output_dir: Path,
    source_file: Any,
    raw_rows: int,
) -> DefinitionWriteSummary:
    safe_label = source_file.source_label.replace("/", "_")
    output_path = output_dir / f"source_label={safe_label}" / f"date={source_file.date}" / "definitions.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return DefinitionWriteSummary(
        date=source_file.date,
        source_label=source_file.source_label,
        dataset=source_file.dataset,
        schema=source_file.schema,
        source_path=str(source_file.path),
        raw_rows=raw_rows,
        output_rows=int(len(frame)),
        instrument_id_count=int(frame["instrument_id"].nunique(dropna=True)) if not frame.empty else 0,
        symbol_count=int(frame["symbol"].nunique(dropna=True)) if not frame.empty else 0,
        underlying_count=int(frame["underlying"].replace("", np.nan).nunique(dropna=True)) if not frame.empty else 0,
        option_contract_count=int(frame["asset_class"].eq("option").sum()) if not frame.empty else 0,
        output_path=str(output_path),
    )


def normalize_files(
    *,
    pilot_source_files: Path,
    output_dir: Path,
    source_labels: set[str],
    symbols: set[str] | None,
    dates: set[str] | None,
    max_files: int,
) -> int:
    selected_files = load_pilot_source_files(pilot_source_files, source_labels=source_labels, dates=dates)
    if max_files > 0:
        selected_files = selected_files[:max_files]
    if not selected_files:
        raise SystemExit(f"no definition pilot source files matched labels={sorted(source_labels)}")

    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[DefinitionWriteSummary] = []
    errors: list[str] = []
    for index, source_file in enumerate(selected_files, start=1):
        print(f"[{index}/{len(selected_files)}] normalizing {source_file.source_label} date={source_file.date} path={source_file.path}")
        try:
            raw = load_dbn_frame(source_file.path)
            normalized = normalize_definition_frame(
                raw,
                source_file.date,
                source_label=source_file.source_label,
                dataset=source_file.dataset,
                schema=source_file.schema,
                symbols=symbols,
            )
            summary = write_definition_output(normalized, output_dir=output_dir, source_file=source_file, raw_rows=int(len(raw)))
            summaries.append(summary)
            print(f"  wrote {summary.output_rows} definition rows to {summary.output_path}")
        except Exception as exc:  # pragma: no cover - integration safety path.
            message = f"{source_file.date} {source_file.path}: {exc}"
            errors.append(message)
            print(f"  error: {message}")

    summary_path = output_dir / "definition_silver_summary.csv"
    write_csv_rows(summary_path, [asdict(row) for row in summaries], SUMMARY_COLUMNS)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "pilot_source_files": str(pilot_source_files),
        "output_dir": str(output_dir),
        "source_labels": sorted(source_labels),
        "symbols": sorted(symbols) if symbols else [],
        "selected_dates": sorted({row.date for row in selected_files}),
        "selected_file_count": len(selected_files),
        "summary_csv": str(summary_path),
        "output_file_count": len(summaries),
        "output_rows": sum(row.output_rows for row in summaries),
        "errors": errors,
    }
    write_json(output_dir / "manifest.json", manifest)
    print(f"wrote {summary_path}")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0 if not errors else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-source-files", required=True, type=non_empty_path_arg, help="pilot_source_files.csv from plan_databento_pilot_dates.py")
    parser.add_argument("--output-dir", required=True, type=non_empty_path_arg, help="Directory for normalized definition CSV outputs")
    parser.add_argument("--source-label", action="append", help="Definition source label. Repeatable. Defaults to EQUS and OPRA definition labels")
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying/symbol subset")
    parser.add_argument("--date", action="append", type=normalize_date_arg, help="Optional date filter. Repeatable; accepts YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--max-files", type=int, default=0, help="Optional cap for smoke tests; 0 means all selected definition files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source_labels = set(args.source_label or DEFAULT_SOURCE_LABELS)
    symbols = set(split_symbols(args.symbols)) or None
    return normalize_files(
        pilot_source_files=args.pilot_source_files,
        output_dir=args.output_dir,
        source_labels=source_labels,
        symbols=symbols,
        dates=set(args.date or []) or None,
        max_files=max(0, int(args.max_files or 0)),
    )


if __name__ == "__main__":
    raise SystemExit(main())

