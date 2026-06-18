#!/usr/bin/env python3
"""Normalize EQUS mbp-1 DBN files into 1-second equity quote-state silver CSVs.

This is the first Phase 1 richer-source normalizer. It intentionally does not
train models. It creates compact, auditable per-symbol/day silver files that can
be joined into the existing 1s/5s/30s training builders after QA passes.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from databento_silver_utils import (
    MARKET_TZ,
    bar_epoch_seconds,
    coalesce_numeric,
    ensure_ts_event_column,
    load_dbn_frame,
    load_pilot_source_files,
    local_timestamp_strings,
    non_empty_path_arg,
    normalize_date_arg,
    numeric_series,
    regular_session_mask_and_local_ts,
    safe_divide,
    seconds_from_open,
    session_bucket_values,
    session_index,
    split_symbols,
    utc_now,
    write_csv_rows,
    write_json,
)


SOURCE_LABEL = "equs_mbp1_20260612"
SCHEMA_VERSION = "equs_mbp1_silver_1s_v1"

OUTPUT_COLUMNS = [
    "date",
    "symbol",
    "Timestamp",
    "BarEpochSec",
    "SessionBucket",
    "MinuteOfDay",
    "SecondsFromOpen",
    "Bid",
    "Ask",
    "BidSize",
    "AskSize",
    "BidCount",
    "AskCount",
    "Mid",
    "Microprice",
    "Spread",
    "SpreadBps",
    "RawSpreadMinBps1s",
    "RawSpreadMaxBps1s",
    "L1Imbalance",
    "QuoteUpdateCount1s",
    "EventCount1s",
    "QuoteCoverage1s",
    "QuoteStateValid1s",
    "ValidSpread1s",
    "LockedCrossed1s",
    "QuoteAgeMs",
]

SUMMARY_COLUMNS = [
    "date",
    "source_label",
    "dataset",
    "schema",
    "source_path",
    "symbol",
    "raw_rows",
    "rth_rows",
    "output_rows",
    "quote_update_seconds",
    "quote_state_valid_seconds",
    "locked_crossed_seconds",
    "median_spread_bps",
    "max_quote_age_ms",
    "output_path",
]


@dataclass
class SymbolWriteSummary:
    date: str
    source_label: str
    dataset: str
    schema: str
    source_path: str
    symbol: str
    raw_rows: int
    rth_rows: int
    output_rows: int
    quote_update_seconds: int
    quote_state_valid_seconds: int
    locked_crossed_seconds: int
    median_spread_bps: float
    max_quote_age_ms: float
    output_path: str


def _prepare_mbp1_events(raw: pd.DataFrame, symbols: set[str] | None = None) -> pd.DataFrame:
    frame = ensure_ts_event_column(raw).copy()
    if frame.empty:
        return pd.DataFrame()
    if "ts_event" not in frame.columns:
        raise ValueError("mbp-1 frame is missing ts_event")
    if "symbol" not in frame.columns:
        raise ValueError("mbp-1 frame is missing symbol; decode with symbol mapping enabled")

    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    if symbols:
        frame = frame[frame["symbol"].isin(symbols)].copy()
        if frame.empty:
            return frame

    mask, local_ts = regular_session_mask_and_local_ts(frame["ts_event"])
    frame = frame.loc[mask].copy()
    if frame.empty:
        return frame
    local_ts = local_ts.loc[frame.index]
    frame["Timestamp"] = local_ts.dt.floor("1s")
    frame["_ts_local"] = local_ts
    frame["Bid"] = coalesce_numeric(frame, ["bid_px_00", "bid_px_0", "bid_px", "bid"], default=np.nan)
    frame["Ask"] = coalesce_numeric(frame, ["ask_px_00", "ask_px_0", "ask_px", "ask"], default=np.nan)
    frame["BidSize"] = coalesce_numeric(frame, ["bid_sz_00", "bid_sz_0", "bid_size", "bidSize"], default=0.0).fillna(0.0)
    frame["AskSize"] = coalesce_numeric(frame, ["ask_sz_00", "ask_sz_0", "ask_size", "askSize"], default=0.0).fillna(0.0)
    frame["BidCount"] = coalesce_numeric(frame, ["bid_ct_00", "bid_ct_0", "bid_count"], default=0.0).fillna(0.0)
    frame["AskCount"] = coalesce_numeric(frame, ["ask_ct_00", "ask_ct_0", "ask_count"], default=0.0).fillna(0.0)
    frame["_quote_seen"] = frame["Bid"].gt(0.0) | frame["Ask"].gt(0.0)
    mid = ((frame["Bid"] + frame["Ask"]) / 2.0).where(frame[["Bid", "Ask"]].gt(0.0).all(axis=1))
    frame["_spread_bps_row"] = np.where(mid.gt(0.0), ((frame["Ask"] - frame["Bid"]) / mid) * 10000.0, np.nan)
    frame["_l1_imbalance_row"] = safe_divide(frame["BidSize"] - frame["AskSize"], frame["BidSize"] + frame["AskSize"] + 1.0, default=0.0)
    return frame.sort_values(["symbol", "Timestamp", "_ts_local"])


def _regularize_symbol_seconds(second_df: pd.DataFrame, market_day: str, *, full_session_grid: bool) -> pd.DataFrame:
    if second_df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if full_session_grid:
        idx = session_index(market_day)
        out = second_df.reindex(idx)
    else:
        out = second_df.sort_index().copy()
        idx = out.index

    for col in ["QuoteUpdateCount1s", "EventCount1s"]:
        out[col] = pd.to_numeric(out.get(col, 0.0), errors="coerce").fillna(0.0)
    raw_quote_seen = out["QuoteUpdateCount1s"].gt(0.0)

    for col in ["Bid", "Ask"]:
        out[col] = pd.to_numeric(out.get(col, np.nan), errors="coerce").ffill()
    for col in ["BidSize", "AskSize", "BidCount", "AskCount"]:
        out[col] = pd.to_numeric(out.get(col, 0.0), errors="coerce").ffill().fillna(0.0)

    out["RawSpreadMinBps1s"] = pd.to_numeric(out.get("RawSpreadMinBps1s", np.nan), errors="coerce")
    out["RawSpreadMaxBps1s"] = pd.to_numeric(out.get("RawSpreadMaxBps1s", np.nan), errors="coerce")

    both_sides = out[["Bid", "Ask"]].gt(0.0).all(axis=1)
    out["Mid"] = ((out["Bid"] + out["Ask"]) / 2.0).where(both_sides)
    denom = out["BidSize"] + out["AskSize"]
    out["Microprice"] = safe_divide(out["Ask"] * out["BidSize"] + out["Bid"] * out["AskSize"], denom, default=np.nan)
    out["Spread"] = (out["Ask"] - out["Bid"]).where(both_sides)
    out["SpreadBps"] = np.where(out["Mid"].gt(0.0), (out["Spread"] / out["Mid"]) * 10000.0, np.nan)
    out["L1Imbalance"] = safe_divide(out["BidSize"] - out["AskSize"], out["BidSize"] + out["AskSize"] + 1.0, default=0.0)
    valid_spread = both_sides & out["Ask"].gt(out["Bid"])
    locked_crossed = both_sides & out["Ask"].le(out["Bid"])
    out["QuoteCoverage1s"] = raw_quote_seen.astype(float)
    out["QuoteStateValid1s"] = valid_spread.astype(float)
    out["ValidSpread1s"] = valid_spread.astype(float)
    out["LockedCrossed1s"] = locked_crossed.astype(float)

    current_ts = pd.Series(idx, index=idx)
    last_quote_ts = pd.Series(pd.NaT, index=idx, dtype=f"datetime64[ns, {MARKET_TZ}]")
    last_quote_ts.loc[raw_quote_seen] = idx[raw_quote_seen]
    out["QuoteAgeMs"] = ((current_ts - last_quote_ts.ffill()).dt.total_seconds() * 1000.0).fillna(999999.0)

    out["date"] = market_day
    out["symbol"] = ""
    out["Timestamp"] = local_timestamp_strings(idx)
    out["BarEpochSec"] = bar_epoch_seconds(idx)
    out["SessionBucket"] = session_bucket_values(idx)
    out["MinuteOfDay"] = (idx.hour * 60 + idx.minute).astype(int)
    out["SecondsFromOpen"] = seconds_from_open(idx)
    return out[OUTPUT_COLUMNS]


def normalize_mbp1_frame(
    raw: pd.DataFrame,
    market_day: str,
    *,
    symbols: set[str] | None = None,
    full_session_grid: bool = True,
) -> tuple[dict[str, pd.DataFrame], int]:
    events = _prepare_mbp1_events(raw, symbols=symbols)
    if events.empty:
        return {}, 0

    grouped = events.groupby(["symbol", "Timestamp"], sort=True)
    seconds = grouped.agg(
        Bid=("Bid", "last"),
        Ask=("Ask", "last"),
        BidSize=("BidSize", "last"),
        AskSize=("AskSize", "last"),
        BidCount=("BidCount", "last"),
        AskCount=("AskCount", "last"),
        RawSpreadMinBps1s=("_spread_bps_row", "min"),
        RawSpreadMaxBps1s=("_spread_bps_row", "max"),
        QuoteUpdateCount1s=("_quote_seen", "sum"),
        EventCount1s=("symbol", "size"),
    )

    out: dict[str, pd.DataFrame] = {}
    for symbol, symbol_seconds in seconds.groupby(level=0, sort=True):
        symbol_df = symbol_seconds.droplevel(0).sort_index()
        regularized = _regularize_symbol_seconds(symbol_df, market_day, full_session_grid=full_session_grid)
        regularized["symbol"] = symbol
        regularized = regularized[OUTPUT_COLUMNS]
        out[str(symbol)] = regularized
    return out, int(len(events))


def write_symbol_outputs(
    frames_by_symbol: dict[str, pd.DataFrame],
    *,
    output_dir: Path,
    source_file: Any,
    raw_rows: int,
    rth_rows: int,
) -> list[SymbolWriteSummary]:
    summaries: list[SymbolWriteSummary] = []
    for symbol, frame in sorted(frames_by_symbol.items()):
        output_path = output_dir / f"date={source_file.date}" / f"{symbol}_equs_mbp1_1s.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(output_path, index=False)
        spread = pd.to_numeric(frame["SpreadBps"], errors="coerce")
        quote_age = pd.to_numeric(frame["QuoteAgeMs"], errors="coerce")
        summaries.append(
            SymbolWriteSummary(
                date=source_file.date,
                source_label=source_file.source_label,
                dataset=source_file.dataset,
                schema=source_file.schema,
                source_path=str(source_file.path),
                symbol=symbol,
                raw_rows=raw_rows,
                rth_rows=rth_rows,
                output_rows=int(len(frame)),
                quote_update_seconds=int(pd.to_numeric(frame["QuoteUpdateCount1s"], errors="coerce").gt(0).sum()),
                quote_state_valid_seconds=int(pd.to_numeric(frame["QuoteStateValid1s"], errors="coerce").gt(0).sum()),
                locked_crossed_seconds=int(pd.to_numeric(frame["LockedCrossed1s"], errors="coerce").gt(0).sum()),
                median_spread_bps=round(float(spread.dropna().median()), 6) if spread.notna().any() else float("nan"),
                max_quote_age_ms=round(float(quote_age.max()), 3) if quote_age.notna().any() else float("nan"),
                output_path=str(output_path),
            )
        )
    return summaries


def normalize_files(
    *,
    pilot_source_files: Path,
    output_dir: Path,
    source_label: str,
    symbols: set[str] | None,
    dates: set[str] | None,
    full_session_grid: bool,
    max_files: int,
) -> int:
    selected_files = load_pilot_source_files(pilot_source_files, source_label=source_label, dates=dates)
    if max_files > 0:
        selected_files = selected_files[:max_files]
    if not selected_files:
        raise SystemExit(f"no pilot source files matched source_label={source_label!r}")

    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[SymbolWriteSummary] = []
    errors: list[str] = []
    for index, source_file in enumerate(selected_files, start=1):
        print(f"[{index}/{len(selected_files)}] normalizing {source_file.source_label} date={source_file.date} path={source_file.path}")
        try:
            raw = load_dbn_frame(source_file.path)
            raw_rows = int(len(raw))
            frames_by_symbol, rth_rows = normalize_mbp1_frame(
                raw,
                source_file.date,
                symbols=symbols,
                full_session_grid=full_session_grid,
            )
            summaries.extend(
                write_symbol_outputs(
                    frames_by_symbol,
                    output_dir=output_dir,
                    source_file=source_file,
                    raw_rows=raw_rows,
                    rth_rows=rth_rows,
                )
            )
            print(f"  wrote {len(frames_by_symbol)} symbol files; raw_rows={raw_rows} rth_rows={rth_rows}")
        except Exception as exc:  # pragma: no cover - integration safety path.
            message = f"{source_file.date} {source_file.path}: {exc}"
            errors.append(message)
            print(f"  error: {message}")

    summary_path = output_dir / "equs_mbp1_silver_summary.csv"
    write_csv_rows(summary_path, [asdict(row) for row in summaries], SUMMARY_COLUMNS)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "source_label": source_label,
        "pilot_source_files": str(pilot_source_files),
        "output_dir": str(output_dir),
        "full_session_grid": full_session_grid,
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
    parser.add_argument("--output-dir", required=True, type=non_empty_path_arg, help="Directory for EQUS mbp-1 silver CSV outputs")
    parser.add_argument("--source-label", default=SOURCE_LABEL, help=f"Source label to normalize. Default: {SOURCE_LABEL}")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol subset, e.g. TSLA,TQQQ,NVDA,SPY,QQQ")
    parser.add_argument("--date", action="append", type=normalize_date_arg, help="Optional date filter. Repeatable; accepts YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--observed-only", action="store_true", help="Write only observed quote seconds instead of a full RTH 1-second grid")
    parser.add_argument("--max-files", type=int, default=0, help="Optional cap for smoke tests; 0 means all selected files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = set(split_symbols(args.symbols)) or None
    return normalize_files(
        pilot_source_files=args.pilot_source_files,
        output_dir=args.output_dir,
        source_label=args.source_label,
        symbols=symbols,
        dates=set(args.date or []) or None,
        full_session_grid=not args.observed_only,
        max_files=max(0, int(args.max_files or 0)),
    )


if __name__ == "__main__":
    raise SystemExit(main())



