#!/usr/bin/env python3
"""Summarize only today's Databento core-5 weekly artifacts."""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

EXT = Path("/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests")
LOG_DIR = EXT / "databento_api_core5_weekly_6mo_logs"
WEEKS = [
    ("w01", "2025-12-23", "2025-12-26", "databento_api_core5_w01_20251223_to_20251226", "20260624_145037", 0),
    ("w02", "2025-12-29", "2026-01-02", "databento_api_core5_w02_20251229_to_20260102", "20260624_151156", 0),
    ("w03", "2026-01-05", "2026-01-09", "databento_api_core5_w03_20260105_to_20260109", "20260624_152401", 0),
    ("w04", "2026-01-12", "2026-01-16", "databento_api_core5_w04_20260112_to_20260116", "20260624_153833", 0),
    ("w05", "2026-01-19", "2026-01-23", "databento_api_core5_w05_20260119_to_20260123", "20260624_155306", 0),
    ("w06", "2026-01-26", "2026-01-30", "databento_api_core5_w06_20260126_to_20260130", "20260624_161036", 1),
]
EXPECTED_SYMBOLS = ["TSLA", "TQQQ", "NVDA", "SPY", "QQQ"]
EXPECTED = set(EXPECTED_SYMBOLS)
SUMMARY_RE = re.compile(r"\[BACKTEST\]\s+completed=(\d+)\s+failed=(\d+)\s+requested=(\d+)\s+output_dir=(.*)")
SYMBOL_RE = re.compile(r"\[BACKTEST\]\s+symbol=([A-Z0-9._-]+)\s+start=([^ ]+)\s+end=([^ ]+)")
FAIL_RE = re.compile(r"\[BACKTEST\]\[ERROR\]\s+symbol=([A-Z0-9._-]+)\s+replay failed", re.I)
ERROR_HINT_RE = re.compile(r"(ERROR|Exception|TimeoutError|timed out|timeout|Traceback|failed|failure|Caused by)", re.I)


def human_bytes(size: int | float) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TiB"


def fmt_ts(ts: float | int | None) -> str:
    if not ts:
        return "n/a"
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def kind(path: Path) -> str:
    low = path.name.lower()
    if low.endswith("-orders.csv"):
        return "orders"
    if low.endswith("-trades.csv"):
        return "trades"
    if low.endswith("-trade-lifecycle-summary.csv"):
        return "lifecycle"
    if low.endswith("-stream-sanity.json"):
        return "sanity"
    return "other"


def symbol(path: Path) -> str:
    return path.name.split("-", 1)[0]


def csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        return sum(1 for _ in reader)


def compact_sanity(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:  # diagnostics only
        return {"error": str(exc)}
    if not isinstance(data, dict):
        return {"json_type": type(data).__name__}
    result: dict[str, Any] = {}
    for key in ("symbol", "status", "error", "events", "bars", "barsSeen", "buckets", "start", "end", "firstEventTime", "lastEventTime"):
        if key in data:
            result[key] = data[key]
    result["keys"] = sorted(data.keys())
    return result


def scan_log(log: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "exists": log.exists(),
        "path": str(log),
        "size": 0,
        "mtime": None,
        "symbols_started": [],
        "summary": None,
        "failed_symbols": [],
        "failure_context": [],
        "last_error_hints": [],
    }
    if not log.exists():
        return result
    stat = log.stat()
    result["size"] = stat.st_size
    result["mtime"] = stat.st_mtime
    before: deque[tuple[int, str]] = deque(maxlen=80)
    capture_after = 0
    context: list[tuple[int, str]] = []
    error_hints: deque[tuple[int, str]] = deque(maxlen=40)
    with log.open("r", encoding="utf-8", errors="replace") as fh:
        for line_no, raw in enumerate(fh, 1):
            line = raw.rstrip("\n")
            summary_match = SUMMARY_RE.search(line)
            if summary_match:
                result["summary"] = tuple(summary_match.groups())
            symbol_match = SYMBOL_RE.search(line)
            if symbol_match:
                result["symbols_started"].append(symbol_match.group(1))
            fail_match = FAIL_RE.search(line)
            if fail_match:
                result["failed_symbols"].append(fail_match.group(1))
                context = list(before) + [(line_no, line)]
                capture_after = 25
            if ERROR_HINT_RE.search(line):
                error_hints.append((line_no, line[:500]))
            elif capture_after > 0:
                context.append((line_no, line))
                capture_after -= 1
            before.append((line_no, line[:500]))
    result["failure_context"] = context[-120:]
    result["last_error_hints"] = list(error_hints)[-25:]
    return result


def summarize_week(name: str, start: str, end: str, folder: str, stamp: str, user_rc: int) -> dict[str, Any]:
    output_dir = EXT / folder
    files = sorted(p for p in output_dir.glob(f"*-{stamp}-*") if p.is_file()) if output_dir.is_dir() else []
    counts: Counter[str] = Counter()
    by_kind_symbol: dict[str, set[str]] = defaultdict(set)
    artifacts: list[dict[str, Any]] = []
    for file_path in files:
        item_kind = kind(file_path)
        item_symbol = symbol(file_path)
        counts[item_kind] += 1
        if item_symbol in EXPECTED:
            by_kind_symbol[item_kind].add(item_symbol)
        stat = file_path.stat()
        artifact: dict[str, Any] = {
            "symbol": item_symbol,
            "kind": item_kind,
            "file": file_path.name,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        }
        if item_kind in {"orders", "trades", "lifecycle"}:
            artifact["rows"] = csv_row_count(file_path)
        elif item_kind == "sanity":
            artifact["json"] = compact_sanity(file_path)
        artifacts.append(artifact)
    log_matches = sorted(LOG_DIR.glob(f"{name}_*.log")) if LOG_DIR.is_dir() else []
    log = scan_log(log_matches[0]) if log_matches else {"exists": False}
    missing = {
        item_kind: sorted(EXPECTED - by_kind_symbol.get(item_kind, set()))
        for item_kind in ("orders", "lifecycle", "sanity")
    }
    summary = log.get("summary")
    if summary:
        completed, failed, requested, _ = summary
        status = "complete" if completed == requested == "5" and failed == "0" else "partial"
    else:
        status = "complete" if user_rc == 0 else "partial"
    return {
        "week": name,
        "start": start,
        "end": end,
        "stamp": stamp,
        "folder": str(output_dir),
        "user_rc": user_rc,
        "status": status,
        "counts": dict(counts),
        "missing": missing,
        "artifacts": artifacts,
        "log": log,
    }


def markdown(results: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# Databento Core-5 Today-Only Six-Week Run Summary")
    lines.append("")
    lines.append(f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"External root: `{EXT}`")
    lines.append("")
    lines.append("## Executive summary")
    lines.append("")
    lines.append("| Week | Window | Run stamp | Status | rc | Runner summary | Today artifacts | Missing today artifacts |")
    lines.append("|---|---:|---|---|---:|---|---|---|")
    for result in results:
        counts = result["counts"]
        artifacts = ", ".join(f"{key}={counts.get(key, 0)}" for key in ("orders", "trades", "lifecycle", "sanity"))
        missing_parts = []
        for key, values in result["missing"].items():
            if values:
                missing_parts.append(f"{key}:{'/'.join(values)}")
        summary = result["log"].get("summary")
        summary_text = f"completed={summary[0]} failed={summary[1]} requested={summary[2]}" if summary else "not found"
        lines.append(
            f"| {result['week']} | {result['start']}..{result['end']} | `{result['stamp']}` | **{result['status']}** | {result['user_rc']} | {summary_text} | {artifacts} | {'; '.join(missing_parts) if missing_parts else 'none'} |"
        )
    lines.append("")
    lines.append("## Today artifact row counts")
    lines.append("")
    for result in results:
        lines.append(f"### {result['week']} `{result['start']}..{result['end']}`")
        lines.append("")
        lines.append("| Symbol | Orders rows | Trades rows | Lifecycle rows | Sanity file |")
        lines.append("|---|---:|---:|---:|---|")
        by_symbol_kind: dict[str, dict[str, Any]] = defaultdict(dict)
        for artifact in result["artifacts"]:
            by_symbol_kind[artifact["symbol"]][artifact["kind"]] = artifact
        for item_symbol in EXPECTED_SYMBOLS:
            row = by_symbol_kind.get(item_symbol, {})
            lines.append(
                f"| {item_symbol} | {row.get('orders', {}).get('rows', 'missing')} | {row.get('trades', {}).get('rows', 'none')} | {row.get('lifecycle', {}).get('rows', 'missing')} | {'yes' if 'sanity' in row else 'missing'} |"
            )
        lines.append("")
    lines.append("## w06 failure context")
    lines.append("")
    w06 = next((item for item in results if item["week"] == "w06"), None)
    if w06:
        log = w06["log"]
        lines.append(f"Log: `{log.get('path')}` ({human_bytes(log.get('size', 0))}, mtime {fmt_ts(log.get('mtime'))})")
        lines.append(f"Symbols started: `{', '.join(log.get('symbols_started', []))}`")
        lines.append(f"Failed symbols: `{', '.join(log.get('failed_symbols', [])) or 'not parsed'}`")
        lines.append("")
        lines.append("Last error hints:")
        lines.append("```text")
        for line_no, line in log.get("last_error_hints", [])[-20:]:
            lines.append(f"L{line_no}: {line}")
        lines.append("```")
        lines.append("")
        lines.append("Failure context around replay failure:")
        lines.append("```text")
        for line_no, line in log.get("failure_context", [])[-80:]:
            lines.append(f"L{line_no}: {line}")
        lines.append("```")
    return "\n".join(lines) + "\n"


def main() -> int:
    results = [summarize_week(*week) for week in WEEKS]
    report_path = Path("/Users/FXG06FA/trading-agent-main/runtime/core5_weekly_6week_today_only_20260624.md")
    json_path = Path("/Users/FXG06FA/trading-agent-main/runtime/core5_weekly_6week_today_only_20260624.json")
    report = markdown(results)
    report_path.write_text(report, encoding="utf-8")
    json_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(report)
    print(f"Wrote today-only report: {report_path}")
    print(f"Wrote today-only JSON: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
