#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BOTS_DIR = REPO_ROOT / "runtime" / "databento" / "bots"
DEFAULT_OFFSETS_FILE = REPO_ROOT / "runtime" / "live_ai_latency_offsets.json"
DEFAULT_OUTPUT_FILE = REPO_ROOT / "runtime" / "live_ai_latency_report.json"

TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2})")
STRATEGY_BAR_RE = re.compile(r"\[STRATEGY\.BAR\].*symbol=([A-Z]+)")
AI_REQUEST_RE = re.compile(r"\[AI\.REQUEST\].*running prediction")
AI_RESPONSE_RE = re.compile(r"\[AI\.RESPONSE\].*positiveProb=")


@dataclass
class SymbolLog:
    symbol: str
    app_log: Path
    prior_size: int = 0


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def load_symbol_logs(bots_dir: Path) -> list[SymbolLog]:
    symbol_logs: list[SymbolLog] = []
    for path in sorted(bots_dir.glob("trading-*.properties")):
        if path.name == "trading-databento-template.properties":
            continue
        props = read_properties(path)
        symbol = (props.get("trading.symbol") or path.stem.replace("trading-", "")).strip().upper()
        raw_log = (props.get("logging.file.name") or "").strip()
        if not symbol or not raw_log:
            continue
        app_log = Path(raw_log)
        if not app_log.is_absolute():
            app_log = (REPO_ROOT / app_log).resolve()
        symbol_logs.append(SymbolLog(symbol=symbol, app_log=app_log))
    return symbol_logs


def capture_offsets(symbol_logs: list[SymbolLog], offsets_file: Path) -> dict[str, dict[str, object]]:
    payload: dict[str, dict[str, object]] = {}
    for item in symbol_logs:
        prior_size = item.app_log.stat().st_size if item.app_log.exists() else 0
        payload[item.symbol] = {
            "app_log": str(item.app_log),
            "prior_size": prior_size,
        }
    offsets_file.parent.mkdir(parents=True, exist_ok=True)
    offsets_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def parse_timestamp(line: str) -> datetime | None:
    match = TIMESTAMP_RE.match(line)
    if not match:
        return None
    try:
        return datetime.fromisoformat(match.group(1))
    except ValueError:
        return None


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    rank = (len(values) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(values[lower])
    weight = rank - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min_ms": round(ordered[0], 3) if ordered else None,
        "avg_ms": round(mean(ordered), 3) if ordered else None,
        "p50_ms": round(percentile(ordered, 0.50), 3) if ordered else None,
        "p95_ms": round(percentile(ordered, 0.95), 3) if ordered else None,
        "p99_ms": round(percentile(ordered, 0.99), 3) if ordered else None,
        "max_ms": round(ordered[-1], 3) if ordered else None,
    }


def analyze_symbol(symbol: str, app_log: Path, prior_size: int) -> dict[str, object]:
    if not app_log.exists():
        return {
            "symbol": symbol,
            "app_log": str(app_log),
            "prior_size": prior_size,
            "exists": False,
            "strategy_bars": 0,
            "ai_requests": 0,
            "ai_responses": 0,
            "bar_to_request_ms": summarize([]),
            "request_to_response_ms": summarize([]),
            "bar_to_response_ms": summarize([]),
        }

    current_size = app_log.stat().st_size
    with app_log.open("r", encoding="utf-8", errors="ignore") as handle:
        handle.seek(prior_size if current_size >= prior_size else 0)
        content = handle.read()

    last_bar_ts: datetime | None = None
    pending_requests: deque[tuple[datetime, datetime | None]] = deque()
    bar_to_request: list[float] = []
    request_to_response: list[float] = []
    bar_to_response: list[float] = []
    strategy_bars = 0
    ai_requests = 0
    ai_responses = 0

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        ts = parse_timestamp(line)
        if ts is None:
            continue

        if STRATEGY_BAR_RE.search(line):
            strategy_bars += 1
            last_bar_ts = ts
            continue

        if AI_REQUEST_RE.search(line):
            ai_requests += 1
            if last_bar_ts is not None:
                delta_ms = (ts - last_bar_ts).total_seconds() * 1000.0
                if delta_ms >= 0.0:
                    bar_to_request.append(delta_ms)
            pending_requests.append((ts, last_bar_ts))
            continue

        if AI_RESPONSE_RE.search(line):
            ai_responses += 1
            if pending_requests:
                request_ts, request_bar_ts = pending_requests.popleft()
                req_delta_ms = (ts - request_ts).total_seconds() * 1000.0
                if req_delta_ms >= 0.0:
                    request_to_response.append(req_delta_ms)
                if request_bar_ts is not None:
                    bar_delta_ms = (ts - request_bar_ts).total_seconds() * 1000.0
                    if bar_delta_ms >= 0.0:
                        bar_to_response.append(bar_delta_ms)
            continue

    return {
        "symbol": symbol,
        "app_log": str(app_log),
        "prior_size": prior_size,
        "exists": True,
        "strategy_bars": strategy_bars,
        "ai_requests": ai_requests,
        "ai_responses": ai_responses,
        "bar_to_request_ms": summarize(bar_to_request),
        "request_to_response_ms": summarize(request_to_response),
        "bar_to_response_ms": summarize(bar_to_response),
    }


def analyze(offsets_file: Path, output_file: Path) -> dict[str, object]:
    offsets = json.loads(offsets_file.read_text(encoding="utf-8"))
    per_symbol: dict[str, dict[str, object]] = {}
    overall_bar_to_request: list[float] = []
    overall_request_to_response: list[float] = []
    overall_bar_to_response: list[float] = []

    for symbol in sorted(offsets):
        entry = offsets[symbol]
        result = analyze_symbol(
            symbol,
            Path(str(entry.get("app_log") or "")),
            int(entry.get("prior_size") or 0),
        )
        per_symbol[symbol] = result
        for key, bucket in (
            ("bar_to_request_ms", overall_bar_to_request),
            ("request_to_response_ms", overall_request_to_response),
            ("bar_to_response_ms", overall_bar_to_response),
        ):
            count = int(result[key]["count"])
            if count <= 0:
                continue
            # Re-read symbol output summary is enough for report; no need to aggregate percentiles back from summaries.
        symbol_log = Path(str(entry.get("app_log") or ""))
        if symbol_log.exists():
            current_size = symbol_log.stat().st_size
            with symbol_log.open("r", encoding="utf-8", errors="ignore") as handle:
                handle.seek(int(entry.get("prior_size") or 0) if current_size >= int(entry.get("prior_size") or 0) else 0)
                content = handle.read()
            last_bar_ts: datetime | None = None
            pending_requests: deque[tuple[datetime, datetime | None]] = deque()
            for raw_line in content.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                ts = parse_timestamp(line)
                if ts is None:
                    continue
                if STRATEGY_BAR_RE.search(line):
                    last_bar_ts = ts
                    continue
                if AI_REQUEST_RE.search(line):
                    if last_bar_ts is not None:
                        delta_ms = (ts - last_bar_ts).total_seconds() * 1000.0
                        if delta_ms >= 0.0:
                            overall_bar_to_request.append(delta_ms)
                    pending_requests.append((ts, last_bar_ts))
                    continue
                if AI_RESPONSE_RE.search(line) and pending_requests:
                    request_ts, request_bar_ts = pending_requests.popleft()
                    req_delta_ms = (ts - request_ts).total_seconds() * 1000.0
                    if req_delta_ms >= 0.0:
                        overall_request_to_response.append(req_delta_ms)
                    if request_bar_ts is not None:
                        bar_delta_ms = (ts - request_bar_ts).total_seconds() * 1000.0
                        if bar_delta_ms >= 0.0:
                            overall_bar_to_response.append(bar_delta_ms)

    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "offsets_file": str(offsets_file),
        "symbols": per_symbol,
        "overall": {
            "symbols_total": len(per_symbol),
            "symbols_with_ai_requests": sum(1 for item in per_symbol.values() if int(item["ai_requests"]) > 0),
            "symbols_with_ai_responses": sum(1 for item in per_symbol.values() if int(item["ai_responses"]) > 0),
            "bar_to_request_ms": summarize(overall_bar_to_request),
            "request_to_response_ms": summarize(overall_request_to_response),
            "bar_to_response_ms": summarize(overall_bar_to_response),
        },
    }
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture and analyze live Databento->AI latency from per-symbol app logs.")
    parser.add_argument("--bots-dir", type=Path, default=DEFAULT_BOTS_DIR)
    parser.add_argument("--offsets-file", type=Path, default=DEFAULT_OFFSETS_FILE)
    parser.add_argument("--output-file", type=Path, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--capture-offsets", action="store_true")
    parser.add_argument("--analyze", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.capture_offsets:
        payload = capture_offsets(load_symbol_logs(args.bots_dir), args.offsets_file)
        print(json.dumps({"captured_symbols": len(payload), "offsets_file": str(args.offsets_file)}, indent=2))
        return 0
    if args.analyze:
        report = analyze(args.offsets_file, args.output_file)
        print(json.dumps(report, indent=2))
        return 0
    raise SystemExit("Pass either --capture-offsets or --analyze.")


if __name__ == "__main__":
    raise SystemExit(main())

