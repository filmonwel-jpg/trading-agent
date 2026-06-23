#!/usr/bin/env python3
"""Convert per-symbol training bars into replayable normalized NDJSON events."""
from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from databento_event_contract import EVENT_SCHEMA_VERSION, decorate_option_bar  # noqa: E402

MARKET_ZONE = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--input-csv", type=Path, required=True)
	parser.add_argument("--output", type=Path, required=True)
	parser.add_argument("--symbol", required=True)
	parser.add_argument("--date", default="", help="YYYY-MM-DD; defaults to first date for symbol")
	parser.add_argument("--chunksize", type=int, default=100_000)
	parser.add_argument("--max-bars", type=int, default=0)
	parser.add_argument("--no-option-events", action="store_true")
	parser.add_argument("--bar-seconds", type=int, default=5, help="Source bar cadence used for epoch alignment. Default: 5")
	parser.add_argument(
		"--epoch-source",
		choices=["timestamp", "floor-bar-epoch", "bar-epoch"],
		default="timestamp",
		help=(
			"How to derive emitted barEpochSec values. 'timestamp' uses the Timestamp column and floors to "
			"--bar-seconds, falling back to floored BarEpochSec; 'floor-bar-epoch' floors BarEpochSec; "
			"'bar-epoch' preserves raw BarEpochSec. Default: timestamp"
		),
	)
	return parser.parse_args()


def safe_float(value: Any, fallback: float = 0.0) -> float:
	try:
		parsed = float(value)
	except (TypeError, ValueError):
		return fallback
	return parsed if math.isfinite(parsed) else fallback


def safe_int(value: Any, fallback: int = 0) -> int:
	try:
		parsed = float(value)
	except (TypeError, ValueError):
		return fallback
	return int(round(parsed)) if math.isfinite(parsed) else fallback


def normalized_date(raw: object) -> str:
	value = str(raw or "").strip().replace("-", "")
	return value[:8] if len(value) >= 8 and value[:8].isdigit() else ""


def row_date(row: pd.Series) -> str:
	if "Date" in row.index:
		value = normalized_date(row["Date"])
		if value:
			return value
	return normalized_date(row.get("Timestamp", ""))


def display_path(path: Path) -> str:
	try:
		return str(path.relative_to(ROOT))
	except ValueError:
		return str(path)


def aligned_epoch(epoch: int, bar_seconds: int) -> int:
	cadence = max(1, int(bar_seconds))
	return epoch - (epoch % cadence)


def timestamp_epoch_sec(raw: Any) -> int | None:
	value = str(raw or "").strip()
	if not value:
		return None
	if value.endswith(" America/New_York"):
		local_value = value[: -len(" America/New_York")]
		for fmt in ("%Y%m%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
			try:
				return int(datetime.strptime(local_value, fmt).replace(tzinfo=MARKET_ZONE).timestamp())
			except ValueError:
				continue
	try:
		parsed = pd.Timestamp(value)
	except (TypeError, ValueError):
		return None
	if pd.isna(parsed):
		return None
	if parsed.tzinfo is None:
		parsed = parsed.tz_localize(MARKET_ZONE)
	else:
		parsed = parsed.tz_convert(MARKET_ZONE)
	return int(parsed.timestamp())


def event_epoch(row: pd.Series, epoch_source: str, bar_seconds: int) -> int:
	source_epoch = safe_int(row.get("BarEpochSec"))
	if epoch_source == "bar-epoch":
		return source_epoch
	if epoch_source == "floor-bar-epoch":
		return aligned_epoch(source_epoch, bar_seconds) if source_epoch > 0 else 0
	timestamp_epoch = timestamp_epoch_sec(row.get("Timestamp"))
	if timestamp_epoch is not None and timestamp_epoch > 0:
		return aligned_epoch(timestamp_epoch, bar_seconds)
	return aligned_epoch(source_epoch, bar_seconds) if source_epoch > 0 else 0


def discover_first_date(input_csv: Path, symbol: str, chunksize: int) -> str:
	for chunk in pd.read_csv(input_csv, chunksize=max(1, chunksize)):
		chunk = chunk[chunk["Symbol"].astype(str).str.upper().eq(symbol)]
		if not chunk.empty:
			value = row_date(chunk.iloc[0])
			if value:
				return value
	raise ValueError(f"No rows found for symbol={symbol} in {input_csv}")


def open_output(path: Path) -> TextIO:
	path.parent.mkdir(parents=True, exist_ok=True)
	if str(path).endswith(".gz"):
		return gzip.open(path, "wt", encoding="utf-8")
	return path.open("w", encoding="utf-8")


def emit(handle: TextIO, payload: dict[str, Any]) -> None:
	handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")


def quality_fields(row: pd.Series) -> dict[str, Any]:
	out: dict[str, Any] = {}
	for col in [
		"DataQualityFlags", "ChildDataQualityFlagUnion", "TradeSecondsPresent",
		"QuoteUpdateSecondsPresent", "QuoteStateSecondsValid", "SyntheticSeconds",
		"TradeCoverage", "QuoteUpdateCoverage", "QuoteStateCoverage",
		"SyntheticCoverage", "QuoteAgeMsMean", "QuoteAgeMsMax",
		"ValidSpreadCoverage", "LockedCrossedSeconds", "QualityScore",
	]:
		if col in row.index and pd.notna(row[col]):
			out[col] = str(row[col]) if col.endswith("Flags") or col == "ChildDataQualityFlagUnion" else safe_float(row[col])
	out.setdefault("DataQualityFlags", "none")
	out.setdefault("ChildDataQualityFlagUnion", out["DataQualityFlags"])
	out.setdefault("QualityScore", 1.0)
	return out


def contract(source: str, schema: str, ts_ns: int | None = None) -> dict[str, Any]:
	out: dict[str, Any] = {
		"EventSchemaVersion": EVENT_SCHEMA_VERSION,
		"EventSource": source,
		"EventDataset": "local_training_csv",
		"EventSchema": schema,
		"EventGeneratedAtMs": int(time.time() * 1000),
	}
	if ts_ns is not None:
		out["tsEventNs"] = ts_ns
		out["EventTsEventNs"] = ts_ns
	return out


def equity_event(row: pd.Series, symbol: str, epoch: int) -> dict[str, Any]:
	source_epoch = safe_int(row.get("BarEpochSec"))
	close = safe_float(row.get("Close"))
	payload = {
		"event": "equity_bar", "symbol": symbol, "barEpochSec": epoch,
		"open": round(safe_float(row.get("Open"), close), 6),
		"high": round(safe_float(row.get("High"), close), 6),
		"low": round(safe_float(row.get("Low"), close), 6),
		"close": round(close, 6), "wap": round(safe_float(row.get("WAP"), close), 6),
		"volume": max(0, safe_int(row.get("Volume"))),
		"tradeCount": max(0, safe_int(row.get("Count", row.get("TradePrintCount5s", 0)))),
		"quoteCount": max(0, safe_int(row.get("QuoteUpdateCount5s", row.get("Count", 0)))),
		"bid": round(safe_float(row.get("Bid", row.get("BidLast", 0.0))), 6),
		"ask": round(safe_float(row.get("Ask", row.get("AskLast", 0.0))), 6),
		"bidSize": max(0, safe_int(row.get("BidSize", row.get("BidSizeLast", 0)))),
		"askSize": max(0, safe_int(row.get("AskSize", row.get("AskSizeLast", 0)))),
		"atBidVol": max(0, safe_int(row.get("AtBidVol", 0))),
		"atAskVol": max(0, safe_int(row.get("AtAskVol", 0))),
		"historical": True,
	}
	if source_epoch > 0 and source_epoch != epoch:
		payload["sourceBarEpochSec"] = source_epoch
	payload.update(quality_fields(row))
	payload.update(contract("recorded_training_bars", "training_5s", epoch * 1_000_000_000))
	return payload


def option_event(row: pd.Series, symbol: str, right: str, volume: int, epoch: int) -> dict[str, Any]:
	source_epoch = safe_int(row.get("BarEpochSec"))
	payload = decorate_option_bar(
		{"event": "option_bar", "underlying": symbol, "right": right, "barEpochSec": epoch,
		 "tsEventNs": epoch * 1_000_000_000, "volume": max(0, int(volume)), "historical": True},
		event_source="recorded_training_bars",
		dataset="local_training_csv",
		schema="ohlcv-1s_aggregate_from_training_bar",
		stype_in="parent",
		ts_event_ns=epoch * 1_000_000_000,
	)
	if source_epoch > 0 and source_epoch != epoch:
		payload["sourceBarEpochSec"] = source_epoch
	return payload


def option_volume(row: pd.Series, prefix: str) -> int:
	return max(0, safe_int(row.get(f"{prefix}VolDelta5s", row.get(f"{prefix}Vol", 0))))


def main() -> int:
	args = parse_args()
	input_csv = args.input_csv.expanduser().resolve()
	output = args.output.expanduser().resolve()
	symbol = args.symbol.strip().upper()
	target_date = normalized_date(args.date) or discover_first_date(input_csv, symbol, args.chunksize)
	session_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
	bar_seconds = max(1, int(args.bar_seconds))
	equity_count = option_count = rows_seen = 0
	first_epoch = last_epoch = None
	first_source_epoch = last_source_epoch = None
	misaligned_source_epoch_rows = 0
	previous_close = None
	with open_output(output) as handle:
		emit(handle, {"event": "status", "message": "recorded-training-bars-start", "EventSchemaVersion": EVENT_SCHEMA_VERSION,
					  "symbol": symbol, "sessionDate": session_date, "sourceCsv": display_path(input_csv),
					  "epochSource": args.epoch_source, "barSeconds": bar_seconds})
		for chunk in pd.read_csv(input_csv, chunksize=max(1, args.chunksize)):
			chunk = chunk[chunk["Symbol"].astype(str).str.upper().eq(symbol)].copy()
			if chunk.empty:
				continue
			dates = chunk.apply(row_date, axis=1)
			chunk = chunk.loc[dates.eq(target_date)].copy()
			if chunk.empty:
				continue
			chunk["_source_epoch"] = pd.to_numeric(chunk["BarEpochSec"], errors="coerce").fillna(0).astype("int64")
			chunk["_event_epoch"] = chunk.apply(lambda row: event_epoch(row, args.epoch_source, bar_seconds), axis=1)
			for _, row in chunk[chunk["_event_epoch"].gt(0)].sort_values(["_event_epoch", "_source_epoch"]).iterrows():
				if args.max_bars and equity_count >= args.max_bars:
					break
				epoch = safe_int(row["_event_epoch"])
				source_epoch = safe_int(row.get("BarEpochSec"))
				first_epoch = epoch if first_epoch is None else first_epoch
				last_epoch = epoch
				if source_epoch > 0:
					first_source_epoch = source_epoch if first_source_epoch is None else first_source_epoch
					last_source_epoch = source_epoch
					if source_epoch % bar_seconds != 0:
						misaligned_source_epoch_rows += 1
				rows_seen += 1
				if previous_close is None and safe_float(row.get("YesterdayClose")) > 0:
					previous_close = safe_float(row.get("YesterdayClose"))
					emit(handle, {"event": "previous_close", "EventSchemaVersion": EVENT_SCHEMA_VERSION,
								  "EventSource": "recorded_training_bars", "symbol": symbol,
								  "sessionDate": session_date, "previousClose": round(previous_close, 6),
								  "close": round(previous_close, 6), "historical": True})
				if not args.no_option_events:
					for prefix, right in [("Put", "P"), ("Call", "C")]:
						volume = option_volume(row, prefix)
						if volume > 0:
							emit(handle, option_event(row, symbol, right, volume, epoch))
							option_count += 1
				emit(handle, equity_event(row, symbol, epoch))
				equity_count += 1
			if args.max_bars and equity_count >= args.max_bars:
				break
		emit(handle, {"event": "status", "message": f"recorded-training-bars-complete equityEvents={equity_count} optionEvents={option_count}",
					  "EventSchemaVersion": EVENT_SCHEMA_VERSION, "symbol": symbol, "sessionDate": session_date})
	if equity_count <= 0:
		raise ValueError(f"No equity events written for symbol={symbol} date={session_date}")
	manifest = {
		"schema_version": "recorded_training_bars_manifest_v1",
		"event_schema_version": EVENT_SCHEMA_VERSION,
		"source_csv": display_path(input_csv), "output": display_path(output), "symbol": symbol,
		"session_date": session_date, "rows_seen": rows_seen,
		"equity_events": equity_count, "option_events": option_count,
		"previous_close": previous_close, "first_bar_epoch_sec": first_epoch,
		"last_bar_epoch_sec": last_epoch, "first_source_bar_epoch_sec": first_source_epoch,
		"last_source_bar_epoch_sec": last_source_epoch, "misaligned_source_epoch_rows": misaligned_source_epoch_rows,
		"epoch_source": args.epoch_source, "bar_seconds": bar_seconds, "generated_at_ms": int(time.time() * 1000),
	}
	manifest_path = output.with_name(output.name.replace(".ndjson.gz", "").replace(".ndjson", "") + ".manifest.json")
	manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
	print("RECORDED_EVENTS_BUILD=PASS")
	print(f"output={output}")
	print(f"manifest={manifest_path}")
	print(f"symbol={symbol} session_date={session_date} equity_events={equity_count} option_events={option_count}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
