#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

db: Any | None = None

MARKET_TZ = "America/New_York"
DBEQ_FILE_RE = re.compile(r".*?(\d{8})\.tbbo\.dbn\.zst$")
OPRA_FILE_RE = re.compile(r".*?(\d{8})\.ohlcv-1s\.dbn\.zst$")
OPRA_SYMBOL_RE = re.compile(r"^([A-Z]+)\s+(\d{6,8})([CP])\d+$")


def emit(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def require_databento() -> Any:
    global db
    if db is None:
        try:
            import databento as databento_module
        except Exception as exc:  # pragma: no cover
            raise SystemExit(f"Install databento first: {exc}") from exc
        db = databento_module
    return db


def split_csv(raw: str | Iterable[str]) -> list[str]:
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = str(part or "").strip().upper()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def list_daily_files(root: str, pattern: re.Pattern[str]) -> dict[str, Path]:
    base = Path(root).expanduser().resolve()
    if not base.exists():
        raise FileNotFoundError(f"Directory not found: {base}")
    out: dict[str, Path] = {}
    for path in sorted(base.glob("*.dbn.zst")):
        match = pattern.match(path.name)
        if match:
            out[match.group(1)] = path
    if not out:
        raise FileNotFoundError(f"No DBN files matching {pattern.pattern} in {base}")
    return out


def normalize_ts(ts: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(ts, utc=True, errors="coerce")
    return parsed.dt.tz_convert(MARKET_TZ)


def rth_filter(local_ts: pd.Series) -> pd.Series:
    minute = local_ts.dt.hour * 60 + local_ts.dt.minute
    return (minute >= 9 * 60 + 30) & (minute < 16 * 60)


def safe_float(value: object, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
        if pd.isna(parsed):
            return fallback
        return parsed
    except (TypeError, ValueError):
        return fallback


def coerce_utc_datetime(raw: str, is_end: bool = False) -> datetime:
    value = str(raw or "").strip()
    if not value:
        return default_session_window()[1 if is_end else 0]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        local = pd.Timestamp(value, tz=MARKET_TZ)
        if is_end:
            local = local + pd.Timedelta(hours=16)
        else:
            local = local + pd.Timedelta(hours=9, minutes=30)
        return local.tz_convert("UTC").to_pydatetime()
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(MARKET_TZ)
    return parsed.tz_convert("UTC").to_pydatetime()


def default_session_window() -> tuple[datetime, datetime]:
    today = pd.Timestamp.now(tz=MARKET_TZ).normalize()
    day = today - pd.Timedelta(days=1)
    while day.weekday() >= 5:
        day -= pd.Timedelta(days=1)
    start = day + pd.Timedelta(hours=9, minutes=30)
    end = day + pd.Timedelta(hours=16)
    return start.tz_convert("UTC").to_pydatetime(), end.tz_convert("UTC").to_pydatetime()


def frame_from_store(store: Any) -> pd.DataFrame:
    frame = store.to_df().reset_index(drop=False)
    if "ts_event" not in frame.columns and "index" in frame.columns:
        frame = frame.rename(columns={"index": "ts_event"})
    return frame


def equity_events_from_frame(raw: pd.DataFrame, symbols: set[str]) -> list[tuple[int, int, str, dict]]:
    if raw.empty:
        return []
    if "symbol" not in raw.columns:
        raw["symbol"] = next(iter(symbols)) if len(symbols) == 1 else ""
    raw["symbol"] = raw["symbol"].astype(str).str.strip().str.upper()
    raw = raw[raw["symbol"].isin(symbols)].copy()
    if raw.empty:
        return []
    local_ts = normalize_ts(raw["ts_event"])
    raw = raw.loc[rth_filter(local_ts)].copy()
    local_ts = local_ts.loc[raw.index]
    if raw.empty:
        return []
    raw["bar_epoch"] = (local_ts.astype("int64") // 1_000_000_000).astype("int64")
    if "price" not in raw.columns and {"open", "high", "low", "close"}.issubset(raw.columns):
        for col in ["open", "high", "low", "close", "volume", "vwap", "bid", "ask", "bidSize", "askSize"]:
            if col not in raw.columns:
                raw[col] = 0.0
            raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0.0)
        events: list[tuple[int, int, str, dict]] = []
        for row in raw.sort_values(["bar_epoch", "symbol"]).itertuples(index=False):
            close = safe_float(getattr(row, "close", 0.0))
            if close <= 0.0:
                continue
            volume = int(round(safe_float(getattr(row, "volume", 0.0))))
            payload = {
                "event": "equity_bar",
                "symbol": row.symbol,
                "barEpochSec": int(row.bar_epoch),
                "open": round(safe_float(getattr(row, "open", close), close), 6),
                "high": round(safe_float(getattr(row, "high", close), close), 6),
                "low": round(safe_float(getattr(row, "low", close), close), 6),
                "close": round(close, 6),
                "wap": round(safe_float(getattr(row, "vwap", close), close), 6),
                "volume": volume,
                "tradeCount": 0,
                "quoteCount": 0,
                "bid": round(safe_float(getattr(row, "bid", 0.0)), 6),
                "ask": round(safe_float(getattr(row, "ask", 0.0)), 6),
                "bidSize": int(round(safe_float(getattr(row, "bidSize", 0.0)))),
                "askSize": int(round(safe_float(getattr(row, "askSize", 0.0)))),
                "atBidVol": 0,
                "atAskVol": 0,
                "historical": True,
            }
            events.append((int(row.bar_epoch), 1, row.symbol, payload))
        return events
    for col in ["price", "size", "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]:
        if col not in raw.columns:
            raw[col] = 0.0
        raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0.0)
    raw["px_x_sz"] = raw["price"] * raw["size"]
    side = raw.get("side", pd.Series("", index=raw.index)).astype(str).str.upper()
    raw["at_bid_vol"] = raw["size"].where(side.eq("B"), 0.0)
    raw["at_ask_vol"] = raw["size"].where(side.isin(["A", "S"]), 0.0)
    grouped = raw.groupby(["symbol", "bar_epoch"], sort=True)
    bars = grouped.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
        tradeCount=("size", "size"),
        px_x_sz=("px_x_sz", "sum"),
        bid=("bid_px_00", "last"),
        ask=("ask_px_00", "last"),
        bidSize=("bid_sz_00", "last"),
        askSize=("ask_sz_00", "last"),
        quoteCount=("bid_px_00", lambda s: int((pd.to_numeric(s, errors="coerce") > 0).sum())),
        atBidVol=("at_bid_vol", "sum"),
        atAskVol=("at_ask_vol", "sum"),
    ).reset_index()
    events: list[tuple[int, int, str, dict]] = []
    for row in bars.itertuples(index=False):
        close = float(row.close or 0.0)
        if close <= 0.0:
            bid = float(row.bid or 0.0)
            ask = float(row.ask or 0.0)
            close = ask if ask > 0 else bid
        if close <= 0.0:
            continue
        volume = int(round(float(row.volume or 0.0)))
        wap = (float(row.px_x_sz) / volume) if volume > 0 else close
        payload = {
            "event": "equity_bar",
            "symbol": row.symbol,
            "barEpochSec": int(row.bar_epoch),
            "open": round(float(row.open or close), 6),
            "high": round(float(row.high or close), 6),
            "low": round(float(row.low or close), 6),
            "close": round(close, 6),
            "wap": round(wap, 6),
            "volume": volume,
            "tradeCount": int(row.tradeCount or 0),
            "quoteCount": int(row.quoteCount or 0),
            "bid": round(float(row.bid or 0.0), 6),
            "ask": round(float(row.ask or 0.0), 6),
            "bidSize": int(round(float(row.bidSize or 0.0))),
            "askSize": int(round(float(row.askSize or 0.0))),
            "atBidVol": int(round(float(row.atBidVol or 0.0))),
            "atAskVol": int(round(float(row.atAskVol or 0.0))),
            "historical": True,
        }
        events.append((int(row.bar_epoch), 1, row.symbol, payload))
    return events


def load_equity_events(path: Path, symbols: set[str]) -> list[tuple[int, int, str, dict]]:
    databento = require_databento()
    return equity_events_from_frame(frame_from_store(databento.DBNStore.from_file(path)), symbols)


def extract_option_meta(symbol: str) -> tuple[str, str] | None:
    match = OPRA_SYMBOL_RE.match(str(symbol or "").strip().upper())
    if not match:
        return None
    return match.group(1), match.group(3)


def option_events_from_frame(raw: pd.DataFrame, symbols: set[str]) -> list[tuple[int, int, str, dict]]:
    if raw.empty:
        return []
    if "symbol" not in raw.columns:
        return []
    local_ts = normalize_ts(raw["ts_event"])
    raw = raw.loc[rth_filter(local_ts)].copy()
    local_ts = local_ts.loc[raw.index]
    if raw.empty:
        return []
    raw["bar_epoch"] = (local_ts.astype("int64") // 1_000_000_000).astype("int64")
    raw["volume"] = pd.to_numeric(raw.get("volume", 0), errors="coerce").fillna(0.0)
    raw["raw_symbol"] = raw["symbol"].astype(str).str.upper()
    meta = raw["raw_symbol"].map(extract_option_meta)
    raw["underlying"] = meta.map(lambda x: x[0] if x else "")
    raw["right"] = meta.map(lambda x: x[1] if x else "")
    raw = raw[raw["underlying"].isin(symbols) & raw["right"].isin(["C", "P"]) & raw["volume"].gt(0)].copy()
    if raw.empty:
        return []
    grouped = raw.groupby(["underlying", "right", "bar_epoch"], sort=True)["volume"].sum().reset_index()
    events: list[tuple[int, int, str, dict]] = []
    for row in grouped.itertuples(index=False):
        payload = {
            "event": "option_bar",
            "underlying": row.underlying,
            "right": row.right,
            "barEpochSec": int(row.bar_epoch),
            "volume": int(round(float(row.volume))),
            "historical": True,
        }
        events.append((int(row.bar_epoch), 0, row.underlying, payload))
    return events


def load_option_events(path: Path, symbols: set[str]) -> list[tuple[int, int, str, dict]]:
    databento = require_databento()
    return option_events_from_frame(frame_from_store(databento.DBNStore.from_file(path)), symbols)


def api_option_symbol(symbol: str, stype_in: str) -> str:
    token = symbol.strip().upper()
    if stype_in == "parent" and not token.endswith(".OPT"):
        return f"{token}.OPT"
    return token


def previous_close_from_frame(raw: pd.DataFrame, symbols: set[str], before_utc: datetime) -> dict[str, float]:
    if raw.empty:
        return {}
    if "symbol" not in raw.columns:
        raw["symbol"] = next(iter(symbols)) if len(symbols) == 1 else ""
    raw = raw.copy()
    raw["symbol"] = raw["symbol"].astype(str).str.strip().str.upper()
    raw = raw[raw["symbol"].isin(symbols)].copy()
    if raw.empty or "ts_event" not in raw.columns:
        return {}
    local_ts = normalize_ts(raw["ts_event"])
    before_local = pd.Timestamp(before_utc).tz_convert(MARKET_TZ)
    raw = raw.loc[rth_filter(local_ts) & local_ts.lt(before_local)].copy()
    local_ts = local_ts.loc[raw.index]
    if raw.empty:
        return {}
    close_source = "close" if "close" in raw.columns else "price" if "price" in raw.columns else ""
    if not close_source:
        return {}
    raw["local_ts_for_previous_close"] = local_ts
    raw["previous_close_value"] = pd.to_numeric(raw[close_source], errors="coerce").fillna(0.0)
    raw = raw[raw["previous_close_value"].gt(0)].copy()
    if raw.empty:
        return {}
    latest = raw.sort_values(["symbol", "local_ts_for_previous_close"]).groupby("symbol", sort=False).tail(1)
    return {str(row.symbol): round(float(row.previous_close_value), 6) for row in latest.itertuples(index=False)}


def emit_previous_close_context(client: Any, args: argparse.Namespace, symbol: str, start: datetime) -> None:
    lookback_days = max(0, int(args.previous_close_lookback_days or 0))
    if lookback_days <= 0:
        emit({"event": "status", "message": f"historical-api-previous-close-disabled symbol={symbol}"})
        return
    previous_start = (pd.Timestamp(start) - pd.Timedelta(days=lookback_days)).to_pydatetime()
    try:
        store = client.timeseries.get_range(
            dataset=args.equity_dataset,
            schema=args.equity_schema,
            stype_in=args.equity_stype_in,
            symbols=[symbol],
            start=previous_start,
            end=start,
        )
        closes = previous_close_from_frame(frame_from_store(store), {symbol}, start)
    except Exception as exc:
        emit({"event": "status", "message": f"historical-api-previous-close-skip symbol={symbol} error={exc}"})
        return
    previous_close = closes.get(symbol)
    if previous_close and previous_close > 0:
        emit({
            "event": "previous_close",
            "symbol": symbol,
            "sessionDate": pd.Timestamp(start).tz_convert(MARKET_TZ).date().isoformat(),
            "previousClose": previous_close,
            "close": previous_close,
            "historical": True,
        })
    else:
        emit({"event": "status", "message": f"historical-api-previous-close-unavailable symbol={symbol}"})


def stream_api_events(args: argparse.Namespace, symbols: set[str]) -> int:
    api_key = (args.api_key or os.getenv("DATABENTO_API_KEY", "")).strip()
    if not api_key and not args.dry_run:
        raise ValueError("Set DATABENTO_API_KEY or pass --api-key for Databento historical API streaming.")
    start = coerce_utc_datetime(args.start, is_end=False)
    end = coerce_utc_datetime(args.end, is_end=True)
    emit({
        "event": "status",
        "message": "historical-api-stream-start",
        "symbols": sorted(symbols),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "equityDataset": args.equity_dataset,
        "equitySchema": args.equity_schema,
        "optionsDataset": args.options_dataset,
        "optionsSchema": args.options_schema,
    })
    if args.dry_run:
        return 0

    databento = require_databento()
    client = databento.Historical(api_key)
    total = 0
    for symbol in sorted(symbols):
        events: list[tuple[int, int, str, dict]] = []
        emit({"event": "status", "message": f"historical-api-symbol-begin symbol={symbol}"})
        emit_previous_close_context(client, args, symbol, start)
        equity = client.timeseries.get_range(
            dataset=args.equity_dataset,
            schema=args.equity_schema,
            stype_in=args.equity_stype_in,
            symbols=[symbol],
            start=start,
            end=end,
        )
        events.extend(equity_events_from_frame(frame_from_store(equity), {symbol}))

        try:
            option_symbol = api_option_symbol(symbol, args.options_stype_in)
            options = client.timeseries.get_range(
                dataset=args.options_dataset,
                schema=args.options_schema,
                stype_in=args.options_stype_in,
                symbols=[option_symbol],
                start=start,
                end=end,
            )
            events.extend(option_events_from_frame(frame_from_store(options), {symbol}))
        except Exception as exc:
            emit({"event": "status", "message": f"historical-api-options-skip symbol={symbol} error={exc}"})

        events.sort(key=lambda x: (x[0], x[1], x[2]))
        for _, _, _, payload in events:
            emit(payload)
        total += len(events)
        emit({"event": "status", "message": f"historical-api-symbol-complete symbol={symbol} events={len(events)}"})
    emit({"event": "status", "message": f"historical-api-stream-complete events={total}"})
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream historical Databento data as live-normalized NDJSON for backtests.")
    parser.add_argument("--symbols", required=True, help="Comma-separated underlyings, e.g. TSLA,NVDA")
    parser.add_argument("--source", choices=["dbn", "api"], default="dbn", help="Use local DBN files or Databento Historical API streaming.")
    parser.add_argument("--equity-dir", default="/Users/filmonghezehey/Downloads/EQUS-20260523-6J9KE98BJ9")
    parser.add_argument("--options-dir", default="/Users/filmonghezehey/Downloads/OPRA-20260523-MSV68VKVKD")
    parser.add_argument("--start", default="", help="API mode start time/date. Date-only values use 09:30 America/New_York.")
    parser.add_argument("--end", default="", help="API mode end time/date. Date-only values use 16:00 America/New_York.")
    parser.add_argument("--api-key", default="", help="Optional Databento API key; DATABENTO_API_KEY is preferred.")
    parser.add_argument("--equity-dataset", default=os.getenv("DATABENTO_EQUITY_DATASET", "EQUS.MINI"))
    parser.add_argument("--equity-schema", default=os.getenv("DATABENTO_EQUITY_SCHEMA", "tbbo"))
    parser.add_argument("--equity-stype-in", default=os.getenv("DATABENTO_EQUITY_STYPE_IN", "raw_symbol"))
    parser.add_argument("--options-dataset", default=os.getenv("DATABENTO_OPTIONS_DATASET", "OPRA.PILLAR"))
    parser.add_argument("--options-schema", default=os.getenv("DATABENTO_OPTIONS_SCHEMA", "ohlcv-1s"))
    parser.add_argument("--options-stype-in", default=os.getenv("DATABENTO_OPTIONS_STYPE_IN", "parent"))
    parser.add_argument("--previous-close-lookback-days", type=int, default=int(os.getenv("DATABENTO_PREVIOUS_CLOSE_LOOKBACK_DAYS", "10")), help="API mode calendar-day lookback used to emit a previous_close context event. Set 0 to disable.")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = set(split_csv(args.symbols))
    if not symbols:
        raise SystemExit("No symbols supplied")
    if args.source == "api":
        stream_api_events(args, symbols)
        return 0
    emit({"event": "status", "message": "historical-stream-start", "symbols": sorted(symbols), "equityDir": args.equity_dir, "optionsDir": args.options_dir})
    if args.dry_run:
        return 0
    eq_files = list_daily_files(args.equity_dir, DBEQ_FILE_RE)
    op_files = list_daily_files(args.options_dir, OPRA_FILE_RE)
    days = sorted(set(eq_files).intersection(op_files))
    if args.max_days and args.max_days > 0:
        days = days[-args.max_days:]
    if not days:
        raise SystemExit("No overlapping equity/options days found")
    total = 0
    for day in days:
        emit({"event": "status", "message": f"historical-day-begin day={day}"})
        events = load_option_events(op_files[day], symbols) + load_equity_events(eq_files[day], symbols)
        events.sort(key=lambda x: (x[0], x[1], x[2]))
        for _, _, _, payload in events:
            emit(payload)
        total += len(events)
        emit({"event": "status", "message": f"historical-day-complete day={day} events={len(events)}"})
    emit({"event": "status", "message": f"historical-stream-complete events={total}"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



