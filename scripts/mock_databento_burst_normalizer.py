#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


MARKET_ZONE = ZoneInfo("America/New_York")


def split_csv(raw: str) -> list[str]:
    return [item.strip().upper() for item in (raw or "").split(",") if item.strip()]


def emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload), flush=True)


def build_bar(symbol: str, symbol_index: int, step: int, epoch_sec: int) -> dict[str, object]:
    base = 100.0 + (symbol_index % 17) * 3.5
    drift = step * 0.015
    wave = math.sin((step + symbol_index) / 4.0) * 0.35
    close = round(base + drift + wave, 4)
    open_px = round(close - 0.08, 4)
    high = round(close + 0.12, 4)
    low = round(close - 0.12, 4)
    bid = round(close - 0.01, 4)
    ask = round(close + 0.01, 4)
    volume = 1200 + symbol_index * 13 + step * 7
    return {
        "event": "equity_bar",
        "symbol": symbol,
        "barEpochSec": epoch_sec,
        "open": open_px,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "wap": close,
        "bid": bid,
        "ask": ask,
        "bidSize": 100 + (symbol_index % 9) * 10,
        "askSize": 110 + (symbol_index % 11) * 10,
        "atBidVol": int(volume * 0.48),
        "atAskVol": int(volume * 0.52),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit synchronized mock Databento equity_bar events for offline stress testing")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--option-parents", default="")
    parser.add_argument("--equity-dataset", default="DBEQ.BASIC")
    parser.add_argument("--equity-schema", default="tbbo")
    parser.add_argument("--options-dataset", default="OPRA.PILLAR")
    parser.add_argument("--options-schema", default="ohlcv-1s")
    parser.add_argument("--heartbeat-seconds", type=int, default=15)
    parser.add_argument("--startup-delay-seconds", type=float, default=float(os.getenv("MOCK_DATABENTO_BURST_STARTUP_DELAY_SECONDS", "20.0")))
    parser.add_argument("--burst-count", type=int, default=int(os.getenv("MOCK_DATABENTO_BURST_COUNT", "18")), help="Number of closed 30-second buckets to synthesize")
    parser.add_argument("--slice-sleep-seconds", type=float, default=float(os.getenv("MOCK_DATABENTO_SLICE_SLEEP_SECONDS", "0.05")), help="Wall-clock sleep between synchronized 5-second slices")
    parser.add_argument("--source-step-seconds", type=int, default=5)
    parser.add_argument("--base-date", default="2026-04-10", help="Market date used for synthetic timestamps (YYYY-MM-DD)")
    args = parser.parse_args()

    symbols = split_csv(args.symbols)
    if not symbols:
        raise SystemExit("--symbols resolved to an empty list")

    time.sleep(max(0.0, args.startup_delay_seconds))

    emit({
        "event": "status",
        "message": f"mock-normalizer-start symbols={len(symbols)} buckets={args.burst_count} sliceSleep={args.slice_sleep_seconds}",
    })

    base_dt = datetime.fromisoformat(args.base_date).replace(hour=9, minute=30, second=0, microsecond=0, tzinfo=MARKET_ZONE)
    total_steps = max(1, args.burst_count) * max(1, 30 // max(1, args.source_step_seconds))

    for step in range(total_steps):
        epoch_sec = int((base_dt + timedelta(seconds=step * args.source_step_seconds)).timestamp())
        for symbol_index, symbol in enumerate(symbols):
            emit(build_bar(symbol, symbol_index, step, epoch_sec))
        if args.slice_sleep_seconds > 0:
            time.sleep(args.slice_sleep_seconds)

    emit({"event": "status", "message": "mock-normalizer-complete"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

