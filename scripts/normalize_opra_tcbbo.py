#!/usr/bin/env python3
"""Normalize OPRA tcbbo DBNs into 1-second option flow/liquidity silver CSVs."""

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
    option_meta_for_symbols,
    read_csv_rows,
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


SOURCE_LABEL = "opra_tcbbo_20260612"
SCHEMA_VERSION = "opra_tcbbo_silver_1s_v1"

RIGHT_PREFIX = {"C": "Call", "P": "Put"}
RIGHT_METRICS = [
    "OptionTradeCount1s",
    "OptionContractVolume1s",
    "OptionPremiumNotional1s",
    "OptionQuoteContextCount1s",
    "OptionActiveContracts1s",
    "OptionAvgSpreadBps1s",
    "OptionMedianSpreadBps1s",
    "OptionMinSpreadBps1s",
    "OptionAtBidVolume1s",
    "OptionAtAskVolume1s",
]

OUTPUT_COLUMNS = [
    "date",
    "underlying",
    "Timestamp",
    "BarEpochSec",
    "SessionBucket",
    "MinuteOfDay",
    "SecondsFromOpen",
    "CallOptionTradeCount1s",
    "PutOptionTradeCount1s",
    "CallOptionContractVolume1s",
    "PutOptionContractVolume1s",
    "CallOptionPremiumNotional1s",
    "PutOptionPremiumNotional1s",
    "CallOptionQuoteContextCount1s",
    "PutOptionQuoteContextCount1s",
    "CallOptionActiveContracts1s",
    "PutOptionActiveContracts1s",
    "CallOptionAvgSpreadBps1s",
    "PutOptionAvgSpreadBps1s",
    "CallOptionMedianSpreadBps1s",
    "PutOptionMedianSpreadBps1s",
    "CallOptionMinSpreadBps1s",
    "PutOptionMinSpreadBps1s",
    "CallOptionAtBidVolume1s",
    "PutOptionAtBidVolume1s",
    "CallOptionAtAskVolume1s",
    "PutOptionAtAskVolume1s",
    "TotalOptionContractVolume1s",
    "TotalOptionPremiumNotional1s",
    "TotalOptionTradeCount1s",
    "TotalOptionQuoteContextCount1s",
    "CallMinusPutVolume1s",
    "OptionVolumeImbalance1s",
    "PutCallVolumeRatio1s",
]

FLOW_COLUMNS = [
    column
    for column in OUTPUT_COLUMNS
    if column.endswith("Count1s")
    or column.endswith("Volume1s")
    or column.endswith("Notional1s")
    or column.endswith("Contracts1s")
]

SUMMARY_COLUMNS = [
    "date",
    "source_label",
    "dataset",
    "schema",
    "source_path",
    "underlying",
    "raw_rows",
    "rth_rows",
    "output_rows",
    "option_trade_count",
    "option_contract_volume",
    "option_quote_context_count",
    "active_contract_seconds",
    "output_path",
]


@dataclass
class DefinitionMeta:
    instrument_id: str
    symbol: str
    underlying: str
    option_right: str
    expiration: str
    strike: float
    multiplier: float


@dataclass
class OptionWriteSummary:
    date: str
    source_label: str
    dataset: str
    schema: str
    source_path: str
    underlying: str
    raw_rows: int
    rth_rows: int
    output_rows: int
    option_trade_count: int
    option_contract_volume: float
    option_quote_context_count: int
    active_contract_seconds: int
    output_path: str


def load_definition_lookup(definition_csvs: list[Path], definition_root: Path | None = None) -> tuple[dict[str, DefinitionMeta], dict[str, DefinitionMeta]]:
    paths = list(definition_csvs)
    if definition_root:
        paths.extend(sorted(definition_root.glob("**/*.csv")))
    by_instrument_id: dict[str, DefinitionMeta] = {}
    by_symbol: dict[str, DefinitionMeta] = {}
    for path in paths:
        if not path.exists() or path.name.endswith("summary.csv"):
            continue
        for row in read_csv_rows(path):
            right = str(row.get("option_right", "")).strip().upper()[:1]
            if right not in {"C", "P"}:
                continue
            meta = DefinitionMeta(
                instrument_id=str(row.get("instrument_id", "")).strip(),
                symbol=str(row.get("symbol", row.get("raw_symbol", ""))).strip().upper(),
                underlying=str(row.get("underlying", "")).strip().upper(),
                option_right=right,
                expiration=str(row.get("expiration", "")).strip(),
                strike=float(row.get("strike") or "nan"),
                multiplier=float(row.get("multiplier") or 100.0),
            )
            if meta.instrument_id and meta.instrument_id not in {"0", "0.0"}:
                by_instrument_id[meta.instrument_id] = meta
            if meta.symbol:
                by_symbol[meta.symbol] = meta
    return by_instrument_id, by_symbol


def _map_definition_meta(
    frame: pd.DataFrame,
    by_instrument_id: dict[str, DefinitionMeta],
    by_symbol: dict[str, DefinitionMeta],
) -> pd.Series:
    if not by_instrument_id and not by_symbol:
        return pd.Series([None] * len(frame), index=frame.index, dtype="object")
    inst_keys = pd.to_numeric(frame.get("instrument_id", pd.Series(0, index=frame.index)), errors="coerce").fillna(0).astype("int64").astype(str)
    symbol_keys = frame.get("symbol", pd.Series("", index=frame.index)).astype(str).str.strip().str.upper()
    return pd.Series(
        [by_instrument_id.get(inst_key) or by_symbol.get(symbol_key) for inst_key, symbol_key in zip(inst_keys, symbol_keys)],
        index=frame.index,
        dtype="object",
    )


def _prepare_tcbbo_events(
    raw: pd.DataFrame,
    *,
    symbols: set[str] | None = None,
    definition_lookup: tuple[dict[str, DefinitionMeta], dict[str, DefinitionMeta]] | None = None,
) -> pd.DataFrame:
    frame = ensure_ts_event_column(raw).copy()
    if frame.empty:
        return pd.DataFrame()
    if "ts_event" not in frame.columns:
        raise ValueError("tcbbo frame is missing ts_event")

    mask, local_ts = regular_session_mask_and_local_ts(frame["ts_event"])
    frame = frame.loc[mask].copy()
    if frame.empty:
        return frame
    local_ts = local_ts.loc[frame.index]
    frame["Timestamp"] = local_ts.dt.floor("1s")
    frame["_ts_local"] = local_ts
    if "symbol" not in frame.columns:
        frame["symbol"] = ""
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()

    parsed_meta = option_meta_for_symbols(frame["symbol"].dropna().unique())
    frame["underlying"] = frame["symbol"].map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("underlying", ""))
    frame["option_right"] = frame["symbol"].map(lambda value: parsed_meta.get(str(value).strip().upper(), {}).get("option_right", ""))
    frame["multiplier"] = 100.0

    if definition_lookup:
        meta_series = _map_definition_meta(frame, definition_lookup[0], definition_lookup[1])
        frame["underlying"] = frame["underlying"].mask(
            frame["underlying"].astype(str).str.len().eq(0),
            meta_series.map(lambda meta: meta.underlying if meta else ""),
        )
        frame["option_right"] = frame["option_right"].mask(
            ~frame["option_right"].isin(["C", "P"]),
            meta_series.map(lambda meta: meta.option_right if meta else ""),
        )
        frame["multiplier"] = meta_series.map(lambda meta: meta.multiplier if meta and meta.multiplier > 0 else 100.0).astype(float)

    frame = frame[frame["underlying"].astype(str).str.len().gt(0) & frame["option_right"].isin(["C", "P"])].copy()
    if symbols:
        frame = frame[frame["underlying"].isin(symbols)].copy()
    if frame.empty:
        return frame

    frame["Price"] = coalesce_numeric(frame, ["price", "trade_price", "last_price"], default=np.nan)
    frame["Size"] = coalesce_numeric(frame, ["size", "trade_size", "volume"], default=0.0).fillna(0.0)
    frame["Bid"] = coalesce_numeric(frame, ["bid_px_00", "bid_px_0", "bid_px", "bid"], default=np.nan)
    frame["Ask"] = coalesce_numeric(frame, ["ask_px_00", "ask_px_0", "ask_px", "ask"], default=np.nan)
    frame["BidSize"] = coalesce_numeric(frame, ["bid_sz_00", "bid_sz_0", "bid_size", "bidSize"], default=0.0).fillna(0.0)
    frame["AskSize"] = coalesce_numeric(frame, ["ask_sz_00", "ask_sz_0", "ask_size", "askSize"], default=0.0).fillna(0.0)
    frame["TradeSize"] = frame["Size"].where(frame["Price"].gt(0.0) & frame["Size"].gt(0.0), 0.0)
    frame["TradePresent"] = frame["TradeSize"].gt(0.0).astype(int)
    frame["PremiumNotional"] = frame["Price"].fillna(0.0) * frame["TradeSize"] * frame["multiplier"].fillna(100.0)
    frame["QuoteSeen"] = frame["Bid"].gt(0.0) | frame["Ask"].gt(0.0)
    mid = ((frame["Bid"] + frame["Ask"]) / 2.0).where(frame[["Bid", "Ask"]].gt(0.0).all(axis=1))
    frame["SpreadBps"] = np.where(mid.gt(0.0), ((frame["Ask"] - frame["Bid"]) / mid) * 10000.0, np.nan)
    side = frame.get("side", pd.Series("", index=frame.index)).astype(str).str.upper()
    at_bid_by_px = frame["Bid"].gt(0.0) & frame["Price"].le(frame["Bid"])
    at_ask_by_px = frame["Ask"].gt(0.0) & frame["Price"].ge(frame["Ask"])
    frame["AtBidVolume"] = frame["TradeSize"].where(at_bid_by_px | side.eq("B"), 0.0)
    frame["AtAskVolume"] = frame["TradeSize"].where(at_ask_by_px | side.isin(["A", "S"]), 0.0)
    instrument_key = frame["instrument_id"].astype(str) if "instrument_id" in frame.columns else pd.Series("", index=frame.index)
    frame["ContractKey"] = frame["symbol"].where(frame["symbol"].astype(str).str.len().gt(0), instrument_key)
    return frame.sort_values(["underlying", "option_right", "Timestamp", "_ts_local"])


def _wide_underlying_seconds(agg: pd.DataFrame, market_day: str, underlying: str, *, full_session_grid: bool) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for right, prefix in RIGHT_PREFIX.items():
        try:
            right_df = agg.loc[(underlying, right)].sort_index().copy()
        except (KeyError, TypeError):
            right_df = pd.DataFrame()
        right_df = right_df.rename(columns={metric: f"{prefix}{metric}" for metric in RIGHT_METRICS})
        pieces.append(right_df)

    out = pd.concat(pieces, axis=1) if pieces else pd.DataFrame()
    if full_session_grid:
        idx = session_index(market_day)
        out = out.reindex(idx)
    else:
        out = out.sort_index()
        idx = out.index

    for right_prefix in RIGHT_PREFIX.values():
        for metric in RIGHT_METRICS:
            column = f"{right_prefix}{metric}"
            if column not in out.columns:
                out[column] = np.nan
    for column in FLOW_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)

    out["TotalOptionContractVolume1s"] = out["CallOptionContractVolume1s"] + out["PutOptionContractVolume1s"]
    out["TotalOptionPremiumNotional1s"] = out["CallOptionPremiumNotional1s"] + out["PutOptionPremiumNotional1s"]
    out["TotalOptionTradeCount1s"] = out["CallOptionTradeCount1s"] + out["PutOptionTradeCount1s"]
    out["TotalOptionQuoteContextCount1s"] = out["CallOptionQuoteContextCount1s"] + out["PutOptionQuoteContextCount1s"]
    out["CallMinusPutVolume1s"] = out["CallOptionContractVolume1s"] - out["PutOptionContractVolume1s"]
    out["OptionVolumeImbalance1s"] = safe_divide(out["CallMinusPutVolume1s"], out["TotalOptionContractVolume1s"] + 1.0, default=0.0)
    out["PutCallVolumeRatio1s"] = safe_divide(out["PutOptionContractVolume1s"], out["CallOptionContractVolume1s"] + 1.0, default=0.0)

    out["date"] = market_day
    out["underlying"] = underlying
    out["Timestamp"] = local_timestamp_strings(idx)
    out["BarEpochSec"] = bar_epoch_seconds(idx)
    out["SessionBucket"] = session_bucket_values(idx)
    out["MinuteOfDay"] = (idx.hour * 60 + idx.minute).astype(int)
    out["SecondsFromOpen"] = seconds_from_open(idx)
    return out[OUTPUT_COLUMNS]


def normalize_tcbbo_frame(
    raw: pd.DataFrame,
    market_day: str,
    *,
    symbols: set[str] | None = None,
    full_session_grid: bool = True,
    definition_lookup: tuple[dict[str, DefinitionMeta], dict[str, DefinitionMeta]] | None = None,
) -> tuple[dict[str, pd.DataFrame], int]:
    events = _prepare_tcbbo_events(raw, symbols=symbols, definition_lookup=definition_lookup)
    if events.empty:
        return {}, 0

    grouped = events.groupby(["underlying", "option_right", "Timestamp"], sort=True)
    agg = grouped.agg(
        OptionTradeCount1s=("TradePresent", "sum"),
        OptionContractVolume1s=("TradeSize", "sum"),
        OptionPremiumNotional1s=("PremiumNotional", "sum"),
        OptionQuoteContextCount1s=("QuoteSeen", "sum"),
        OptionActiveContracts1s=("ContractKey", "nunique"),
        OptionAvgSpreadBps1s=("SpreadBps", "mean"),
        OptionMedianSpreadBps1s=("SpreadBps", "median"),
        OptionMinSpreadBps1s=("SpreadBps", "min"),
        OptionAtBidVolume1s=("AtBidVolume", "sum"),
        OptionAtAskVolume1s=("AtAskVolume", "sum"),
    )

    frames: dict[str, pd.DataFrame] = {}
    for underlying in sorted(events["underlying"].dropna().unique()):
        frames[str(underlying)] = _wide_underlying_seconds(agg, market_day, str(underlying), full_session_grid=full_session_grid)
    return frames, int(len(events))


def write_underlying_outputs(
    frames_by_underlying: dict[str, pd.DataFrame],
    *,
    output_dir: Path,
    source_file: Any,
    raw_rows: int,
    rth_rows: int,
) -> list[OptionWriteSummary]:
    summaries: list[OptionWriteSummary] = []
    for underlying, frame in sorted(frames_by_underlying.items()):
        output_path = output_dir / f"date={source_file.date}" / f"{underlying}_opra_tcbbo_1s.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(output_path, index=False)
        summaries.append(
            OptionWriteSummary(
                date=source_file.date,
                source_label=source_file.source_label,
                dataset=source_file.dataset,
                schema=source_file.schema,
                source_path=str(source_file.path),
                underlying=underlying,
                raw_rows=raw_rows,
                rth_rows=rth_rows,
                output_rows=int(len(frame)),
                option_trade_count=int(pd.to_numeric(frame["TotalOptionTradeCount1s"], errors="coerce").sum()),
                option_contract_volume=round(float(pd.to_numeric(frame["TotalOptionContractVolume1s"], errors="coerce").sum()), 3),
                option_quote_context_count=int(pd.to_numeric(frame["TotalOptionQuoteContextCount1s"], errors="coerce").sum()),
                active_contract_seconds=int(
                    (pd.to_numeric(frame["CallOptionActiveContracts1s"], errors="coerce").fillna(0.0)
                     + pd.to_numeric(frame["PutOptionActiveContracts1s"], errors="coerce").fillna(0.0)).gt(0.0).sum()
                ),
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
    definition_csvs: list[Path],
    definition_root: Path | None,
) -> int:
    selected_files = load_pilot_source_files(pilot_source_files, source_label=source_label, dates=dates)
    if max_files > 0:
        selected_files = selected_files[:max_files]
    if not selected_files:
        raise SystemExit(f"no pilot source files matched source_label={source_label!r}")

    definition_lookup = load_definition_lookup(definition_csvs, definition_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[OptionWriteSummary] = []
    errors: list[str] = []
    for index, source_file in enumerate(selected_files, start=1):
        print(f"[{index}/{len(selected_files)}] normalizing {source_file.source_label} date={source_file.date} path={source_file.path}")
        try:
            raw = load_dbn_frame(source_file.path)
            frames_by_underlying, rth_rows = normalize_tcbbo_frame(
                raw,
                source_file.date,
                symbols=symbols,
                full_session_grid=full_session_grid,
                definition_lookup=definition_lookup,
            )
            summaries.extend(
                write_underlying_outputs(
                    frames_by_underlying,
                    output_dir=output_dir,
                    source_file=source_file,
                    raw_rows=int(len(raw)),
                    rth_rows=rth_rows,
                )
            )
            print(f"  wrote {len(frames_by_underlying)} underlying files; raw_rows={len(raw)} rth_rows={rth_rows}")
        except Exception as exc:  # pragma: no cover - integration safety path.
            message = f"{source_file.date} {source_file.path}: {exc}"
            errors.append(message)
            print(f"  error: {message}")

    summary_path = output_dir / "opra_tcbbo_silver_summary.csv"
    write_csv_rows(summary_path, [asdict(row) for row in summaries], SUMMARY_COLUMNS)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "source_label": source_label,
        "pilot_source_files": str(pilot_source_files),
        "output_dir": str(output_dir),
        "full_session_grid": full_session_grid,
        "symbols": sorted(symbols) if symbols else [],
        "definition_csvs": [str(path) for path in definition_csvs],
        "definition_root": str(definition_root) if definition_root else "",
        "definition_lookup_instrument_ids": len(definition_lookup[0]),
        "definition_lookup_symbols": len(definition_lookup[1]),
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
    parser.add_argument("--output-dir", required=True, type=non_empty_path_arg, help="Directory for OPRA tcbbo silver CSV outputs")
    parser.add_argument("--source-label", default=SOURCE_LABEL, help=f"Source label to normalize. Default: {SOURCE_LABEL}")
    parser.add_argument("--symbols", default="", help="Optional comma-separated underlying subset, e.g. TSLA,TQQQ,NVDA,SPY,QQQ")
    parser.add_argument("--date", action="append", type=normalize_date_arg, help="Optional date filter. Repeatable; accepts YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--observed-only", action="store_true", help="Write only observed option seconds instead of a full RTH 1-second grid")
    parser.add_argument("--max-files", type=int, default=0, help="Optional cap for smoke tests; 0 means all selected files")
    parser.add_argument("--definition-csv", action="append", type=non_empty_path_arg, default=[], help="Optional normalized definition CSV. Repeatable")
    parser.add_argument("--definition-root", type=non_empty_path_arg, help="Optional root containing normalized definition CSVs")
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
        definition_csvs=list(args.definition_csv or []),
        definition_root=args.definition_root,
    )


if __name__ == "__main__":
    raise SystemExit(main())





