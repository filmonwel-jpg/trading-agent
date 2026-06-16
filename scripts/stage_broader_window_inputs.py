#!/usr/bin/env python3
"""Stage broader/full-window 30s and 5s inputs for setup + lifecycle runs.

The setup trainer currently expects a single 30s CSV, while the lifecycle trainer
can stream per-symbol 30s/5s pairs. This helper takes either combined CSV files
or directories of ``*_30s_training.csv`` / ``*_5s_training.csv`` files, filters by
symbol and an optional half-open date window, and writes both forms:

* ``combined_30s.csv`` / ``combined_5s.csv`` for setup and simple inspection.
* ``data_30s/<SYMBOL>_30s_training.csv`` and ``data_5s/<SYMBOL>_5s_training.csv``
  for streamed lifecycle/micro training.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass
class CadenceSummary:
    cadence: str
    input_path: str
    combined_output_csv: str
    per_symbol_output_dir: str
    rows: int = 0
    symbols: set[str] = field(default_factory=set)
    unique_days: set[str] = field(default_factory=set)
    source_files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        days = sorted(self.unique_days)
        symbols = sorted(self.symbols)
        return {
            "cadence": self.cadence,
            "input_path": self.input_path,
            "combined_output_csv": self.combined_output_csv,
            "per_symbol_output_dir": self.per_symbol_output_dir,
            "rows": self.rows,
            "symbols": symbols,
            "symbol_count": len(symbols),
            "unique_days": len(days),
            "first_date": days[0] if days else None,
            "last_date": days[-1] if days else None,
            "source_files": self.source_files,
        }


class CsvAppender:
    def __init__(self, path: Path):
        self.path = path
        self.columns: list[str] | None = None

    def append(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        out = frame.copy()
        if self.columns is None:
            self.columns = list(out.columns)
        else:
            extra = [col for col in out.columns if col not in self.columns]
            if extra:
                raise ValueError(f"Input schema changed while writing {self.path}; unexpected columns={extra}")
            for col in self.columns:
                if col not in out.columns:
                    out[col] = pd.NA
            out = out[self.columns]
        out.to_csv(self.path, mode="a", header=not self.path.exists(), index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage broader/full-window inputs for setup + lifecycle training.")
    parser.add_argument("--input-30s", required=True, help="Combined 30s CSV or directory of *_30s_training.csv files.")
    parser.add_argument("--input-5s", required=True, help="Combined 5s CSV or directory of *_5s_training.csv files.")
    parser.add_argument("--output-dir", required=True, help="Directory where staged combined/per-symbol CSVs are written.")
    parser.add_argument("--symbols", default="TSLA,TQQQ,NVDA,SPY,QQQ", help="Comma-separated symbols to retain.")
    parser.add_argument("--start-date", default="", help="Inclusive start date, e.g. 2025-07-21.")
    parser.add_argument("--end-date-exclusive", default="", help="Exclusive end date, e.g. 2026-05-23.")
    parser.add_argument("--chunk-size", type=int, default=200_000, help="Rows per chunk for combined CSV inputs.")
    parser.add_argument("--min-unique-days", type=int, default=0, help="Abort when a staged cadence has fewer unique days.")
    parser.add_argument("--min-rows-30s", type=int, default=1, help="Abort when staged 30s rows are below this count.")
    parser.add_argument("--min-rows-5s", type=int, default=1, help="Abort when staged 5s rows are below this count.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing staged output files.")
    return parser.parse_args()


def normalize_symbols(raw: str) -> list[str]:
    symbols = []
    seen = set()
    for item in raw.split(","):
        symbol = item.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    if not symbols:
        raise ValueError("At least one symbol is required.")
    return symbols


def normalize_date(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    parsed = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        raise ValueError(f"Unable to parse date: {value}")
    return parsed.date().isoformat()


def symbol_from_training_path(path: Path, cadence: str) -> str:
    suffix = f"_{cadence}_training.csv"
    name = path.name
    return name[:-len(suffix)].upper() if name.endswith(suffix) else path.stem.upper()


def iter_input_csvs(path: Path, cadence: str, symbols: Iterable[str]) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"Input {cadence} path does not exist: {path}")
    wanted = set(symbols)
    matches = []
    for csv_path in sorted(path.glob(f"*_{cadence}_training.csv")):
        if symbol_from_training_path(csv_path, cadence) in wanted:
            matches.append(csv_path)
    if not matches:
        raise FileNotFoundError(f"No requested {cadence} symbol CSVs found under {path}")
    return matches


def date_strings(frame: pd.DataFrame) -> pd.Series:
    if "Date" in frame.columns:
        raw = frame["Date"].astype(str).str.strip()
    elif "Timestamp" in frame.columns:
        raw = frame["Timestamp"].astype(str).str.strip()
    else:
        raise ValueError("Input CSV must contain either Date or Timestamp for date filtering.")

    hyphen = raw.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    ymd = raw.str.extract(r"(\d{8})", expand=False)
    ymd_iso = ymd.str.slice(0, 4) + "-" + ymd.str.slice(4, 6) + "-" + ymd.str.slice(6, 8)
    out = hyphen.fillna(ymd_iso)
    return out.where(out.str.match(r"\d{4}-\d{2}-\d{2}", na=False))


def read_csv_chunks(path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    if chunk_size <= 0:
        yield pd.read_csv(path)
        return
    yield from pd.read_csv(path, chunksize=chunk_size)


def remove_existing_outputs(output_dir: Path, force: bool) -> None:
    targets = [
        output_dir / "combined_30s.csv",
        output_dir / "combined_5s.csv",
        output_dir / "input_slice_manifest.json",
    ]
    targets.extend((output_dir / "data_30s").glob("*_30s_training.csv") if (output_dir / "data_30s").exists() else [])
    targets.extend((output_dir / "data_5s").glob("*_5s_training.csv") if (output_dir / "data_5s").exists() else [])
    existing = [path for path in targets if path.exists()]
    if existing and not force:
        preview = "; ".join(str(path) for path in existing[:8])
        raise FileExistsError(f"Staged output already exists; pass --force to overwrite. Existing: {preview}")
    for path in existing:
        path.unlink()


def filter_chunk(frame: pd.DataFrame, symbols: set[str], start_date: str | None, end_date: str | None) -> tuple[pd.DataFrame, pd.Series]:
    out = frame.copy()
    if "Symbol" not in out.columns:
        out.insert(0, "Symbol", "SINGLE")
    out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
    dates = date_strings(out)
    mask = out["Symbol"].isin(symbols) & dates.notna()
    if start_date:
        mask &= dates >= start_date
    if end_date:
        mask &= dates < end_date
    filtered = out.loc[mask].copy()
    return filtered, dates.loc[mask]


def process_cadence(
    *,
    cadence: str,
    input_path: Path,
    output_dir: Path,
    symbols: list[str],
    start_date: str | None,
    end_date: str | None,
    chunk_size: int,
) -> CadenceSummary:
    combined_path = output_dir / f"combined_{cadence}.csv"
    per_symbol_dir = output_dir / f"data_{cadence}"
    summary = CadenceSummary(
        cadence=cadence,
        input_path=str(input_path),
        combined_output_csv=str(combined_path),
        per_symbol_output_dir=str(per_symbol_dir),
    )
    combined_writer = CsvAppender(combined_path)
    symbol_writers: dict[str, CsvAppender] = {}
    symbol_set = set(symbols)

    for csv_path in iter_input_csvs(input_path, cadence, symbols):
        summary.source_files.append(str(csv_path))
        for chunk in read_csv_chunks(csv_path, chunk_size):
            filtered, filtered_dates = filter_chunk(chunk, symbol_set, start_date, end_date)
            if filtered.empty:
                continue
            summary.rows += len(filtered)
            summary.symbols.update(filtered["Symbol"].dropna().astype(str).unique().tolist())
            summary.unique_days.update(filtered_dates.dropna().astype(str).unique().tolist())
            combined_writer.append(filtered)
            for symbol, sym_frame in filtered.groupby("Symbol", sort=True):
                writer = symbol_writers.get(symbol)
                if writer is None:
                    writer = CsvAppender(per_symbol_dir / f"{symbol}_{cadence}_training.csv")
                    symbol_writers[symbol] = writer
                writer.append(sym_frame)

    return summary


def validate_summary(summary: CadenceSummary, min_rows: int, min_unique_days: int) -> None:
    if summary.rows < min_rows:
        raise ValueError(f"Staged {summary.cadence} rows too low: {summary.rows} < {min_rows}")
    if len(summary.unique_days) < min_unique_days:
        raise ValueError(
            f"Staged {summary.cadence} unique days too low: {len(summary.unique_days)} < {min_unique_days}"
        )


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    symbols = normalize_symbols(args.symbols)
    start_date = normalize_date(args.start_date)
    end_date = normalize_date(args.end_date_exclusive)
    if start_date and end_date and start_date >= end_date:
        raise ValueError(f"start-date must be before end-date-exclusive: {start_date} >= {end_date}")

    output_dir.mkdir(parents=True, exist_ok=True)
    remove_existing_outputs(output_dir, force=args.force)

    summaries = []
    summaries.append(process_cadence(
        cadence="30s",
        input_path=Path(args.input_30s).expanduser().resolve(),
        output_dir=output_dir,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        chunk_size=args.chunk_size,
    ))
    summaries.append(process_cadence(
        cadence="5s",
        input_path=Path(args.input_5s).expanduser().resolve(),
        output_dir=output_dir,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        chunk_size=args.chunk_size,
    ))

    by_cadence = {summary.cadence: summary for summary in summaries}
    validate_summary(by_cadence["30s"], args.min_rows_30s, args.min_unique_days)
    validate_summary(by_cadence["5s"], args.min_rows_5s, args.min_unique_days)

    manifest = {
        "schema_version": "broader_window_input_slice_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "symbols": symbols,
        "start_date": start_date,
        "end_date_exclusive": end_date,
        "chunk_size": args.chunk_size,
        "outputs": {
            "combined_30s_csv": "combined_30s.csv",
            "combined_5s_csv": "combined_5s.csv",
            "data_30s_dir": "data_30s",
            "data_5s_dir": "data_5s",
        },
        "cadences": {summary.cadence: summary.as_dict() for summary in summaries},
    }
    manifest_path = output_dir / "input_slice_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        "STAGED_INPUTS_OK "
        f"output_dir={output_dir} "
        f"30s_rows={by_cadence['30s'].rows} 30s_days={len(by_cadence['30s'].unique_days)} "
        f"5s_rows={by_cadence['5s'].rows} 5s_days={len(by_cadence['5s'].unique_days)} "
        f"symbols={','.join(sorted(by_cadence['30s'].symbols))}"
    )
    print(f"WROTE {manifest_path}")


if __name__ == "__main__":
    main()

