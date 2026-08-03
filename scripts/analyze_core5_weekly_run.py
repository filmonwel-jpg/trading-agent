#!/usr/bin/env python3
"""Analyze Databento core-5 weekly backtest logs and artifacts.

This is intentionally dependency-free so it can be run on any machine that can read
/Volumes/DatabentoVault.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

DEFAULT_EXT = Path("/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests")
DEFAULT_WEEKS = [
    ("w01", "2025-12-23", "2025-12-26", "databento_api_core5_w01_20251223_to_20251226", 0),
    ("w02", "2025-12-29", "2026-01-02", "databento_api_core5_w02_20251229_to_20260102", 0),
    ("w03", "2026-01-05", "2026-01-09", "databento_api_core5_w03_20260105_to_20260109", 0),
    ("w04", "2026-01-12", "2026-01-16", "databento_api_core5_w04_20260112_to_20260116", 0),
    ("w05", "2026-01-19", "2026-01-23", "databento_api_core5_w05_20260119_to_20260123", 0),
    ("w06", "2026-01-26", "2026-01-30", "databento_api_core5_w06_20260126_to_20260130", 1),
]
EXPECTED_SYMBOLS = {"TSLA", "TQQQ", "NVDA", "SPY", "QQQ"}
ERROR_RE = re.compile(r"\b(error|exception|caused by|traceback|timeout|timed out|killed|failed|failure)\b", re.I)
SUMMARY_RE = re.compile(r"\[BACKTEST\]\s+completed=(\d+)\s+failed=(\d+)\s+requested=(\d+)\s+output_dir=(.*)")
SYMBOL_RE = re.compile(r"\[BACKTEST\]\s+symbol=([A-Z0-9._-]+)\s+start=([^ ]+)\s+end=([^ ]+)")
SYMBOL_FAIL_RE = re.compile(r"\[BACKTEST\]\[ERROR\]\s+symbol=([A-Z0-9._-]+)\s+replay failed", re.I)


def human_bytes(size: int | float) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TiB"


def fmt_ts(ts: float | int | None) -> str:
    if not ts:
        return "n/a"
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def read_head(path: Path, max_lines: int = 30) -> list[str]:
    lines: list[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for _ in range(max_lines):
                line = fh.readline()
                if not line:
                    break
                lines.append(line.rstrip("\n"))
    except OSError as exc:
        lines.append(f"<read error: {exc}>")
    return lines


def read_tail(path: Path, max_bytes: int = 256 * 1024) -> list[str]:
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            data = fh.read().decode("utf-8", "replace")
    except OSError as exc:
        return [f"<read error: {exc}>"]
    return data.splitlines()


def analyze_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"exists": False}
    stat = path.stat()
    head = read_head(path)
    tail = read_tail(path)
    text = "\n".join(head + tail)
    summary_matches = SUMMARY_RE.findall(text)
    symbols_started = SYMBOL_RE.findall(text)
    failed_symbols = SYMBOL_FAIL_RE.findall(text)
    error_like = [line for line in tail if ERROR_RE.search(line)]
    return {
        "exists": True,
        "path": str(path),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "head": head,
        "tail": tail[-80:],
        "summary": summary_matches[-1] if summary_matches else None,
        "symbols_started": [s[0] for s in symbols_started],
        "failed_symbols": failed_symbols,
        "tail_error_like": error_like[-40:],
    }


def inventory_files(output_dir: Path) -> tuple[list[dict[str, Any]], Counter[str]]:
    rows: list[dict[str, Any]] = []
    ext_counts: Counter[str] = Counter()
    if not output_dir.is_dir():
        return rows, ext_counts
    for root, _dirs, names in os.walk(output_dir):
        for name in names:
            path = Path(root) / name
            try:
                stat = path.stat()
            except OSError:
                continue
            rel = path.relative_to(output_dir).as_posix()
            ext = path.suffix.lower() or "<none>"
            ext_counts[ext] += 1
            rows.append({"path": rel, "size": stat.st_size, "mtime": stat.st_mtime})
    rows.sort(key=lambda item: item["path"])
    return rows, ext_counts


def classify_file(rel: str) -> str:
    low = rel.lower()
    if low.endswith("-orders.csv") or low.endswith("orders.csv"):
        return "orders"
    if low.endswith("-trades.csv") or low.endswith("trades.csv"):
        return "trades"
    if "trade-lifecycle-summary" in low and low.endswith(".csv"):
        return "lifecycle_summary"
    if "sanity" in low and low.endswith(".json"):
        return "stream_sanity"
    if low.endswith(".csv"):
        return "other_csv"
    if low.endswith(".json"):
        return "other_json"
    if low.endswith(".log"):
        return "log"
    return "other"


def csv_stats(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, [])
            count = 0
            first_data: list[str] | None = None
            last_data: list[str] | None = None
            for row in reader:
                count += 1
                if first_data is None:
                    first_data = row
                last_data = row
    except Exception as exc:  # pragma: no cover - diagnostics only
        return {"error": str(exc), "rows": 0, "header": []}
    return {"rows": count, "header": header, "first_data": first_data, "last_data": last_data}


def json_stats(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:  # pragma: no cover - diagnostics only
        return {"error": str(exc)}
    if not isinstance(data, dict):
        return {"type": type(data).__name__}
    keep: dict[str, Any] = {"keys": sorted(data.keys())}
    for key in (
        "symbol",
        "start",
        "end",
        "startDate",
        "endDate",
        "events",
        "bars",
        "barsSeen",
        "buckets",
        "replayEvents",
        "durationSeconds",
        "status",
        "error",
        "firstEventTime",
        "lastEventTime",
    ):
        if key in data:
            keep[key] = data[key]
    return keep


def symbol_from_artifact(filename: str) -> str | None:
    base = Path(filename).name
    token = base.split("-", 1)[0]
    return token if token in EXPECTED_SYMBOLS else None


def analyze_week(ext: Path, log_dir: Path, name: str, start: str, end: str, folder: str, expected_rc: int) -> dict[str, Any]:
    output_dir = ext / folder
    log_matches = sorted(log_dir.glob(f"{name}_*.log")) if log_dir.is_dir() else []
    log = analyze_log(log_matches[0] if log_matches else None)
    files, ext_counts = inventory_files(output_dir)
    kind_counts = Counter(classify_file(item["path"]) for item in files)
    artifact_details: list[dict[str, Any]] = []
    symbols_by_kind: dict[str, set[str]] = defaultdict(set)
    total_size = sum(item["size"] for item in files)
    newest = max(files, key=lambda item: item["mtime"], default=None)

    for item in files:
        kind = classify_file(item["path"])
        symbol = symbol_from_artifact(item["path"])
        if symbol:
            symbols_by_kind[kind].add(symbol)
        detail = {"path": item["path"], "kind": kind, "symbol": symbol, "size": item["size"], "mtime": item["mtime"]}
        full_path = output_dir / item["path"]
        if kind in {"orders", "trades", "lifecycle_summary", "other_csv"}:
            detail["csv"] = csv_stats(full_path)
        elif kind == "stream_sanity":
            detail["json"] = json_stats(full_path)
        artifact_details.append(detail)

    complete_artifact_sets = {
        "orders": sorted(symbols_by_kind.get("orders", set())),
        "trades": sorted(symbols_by_kind.get("trades", set())),
        "lifecycle_summary": sorted(symbols_by_kind.get("lifecycle_summary", set())),
        "stream_sanity": sorted(symbols_by_kind.get("stream_sanity", set())),
    }
    missing_by_kind = {
        kind: sorted(EXPECTED_SYMBOLS - set(symbols))
        for kind, symbols in complete_artifact_sets.items()
    }
    status = "complete" if expected_rc == 0 else "failed_or_partial"
    summary = log.get("summary")
    if summary:
        completed, failed, requested, _out = summary
        if int(failed) == 0 and int(completed) == int(requested) == 5:
            status = "complete"
        elif int(completed) > 0:
            status = "partial"
        else:
            status = "failed"
    elif expected_rc != 0:
        status = "failed_or_partial"

    return {
        "week": name,
        "start": start,
        "end": end,
        "expected_rc_from_user": expected_rc,
        "status": status,
        "output_dir": str(output_dir),
        "output_exists": output_dir.is_dir(),
        "file_count": len(files),
        "total_size": total_size,
        "newest_file": newest,
        "ext_counts": dict(ext_counts),
        "kind_counts": dict(kind_counts),
        "symbols_by_kind": complete_artifact_sets,
        "missing_by_kind": missing_by_kind,
        "artifacts": artifact_details,
        "log": log,
    }


def build_report(results: list[dict[str, Any]], ext: Path, log_dir: Path) -> str:
    lines: list[str] = []
    lines.append("# Databento Core-5 Weekly 6-Week Run Analysis")
    lines.append("")
    lines.append(f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"External output root: `{ext}`")
    lines.append(f"Log dir: `{log_dir}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Week | Date range | Status | User rc | Files | Size | Log mtime | Artifacts present | Missing artifacts |")
    lines.append("|---|---:|---|---:|---:|---:|---|---|---|")
    for result in results:
        log = result["log"]
        kind_counts = result["kind_counts"]
        present = ", ".join(f"{kind}={kind_counts.get(kind, 0)}" for kind in ("orders", "trades", "lifecycle_summary", "stream_sanity"))
        missing_parts = []
        for kind, missing in result["missing_by_kind"].items():
            if missing:
                missing_parts.append(f"{kind}:{'/'.join(missing)}")
        lines.append(
            "| {week} | {start}..{end} | {status} | {rc} | {files} | {size} | {mtime} | {present} | {missing} |".format(
                week=result["week"],
                start=result["start"],
                end=result["end"],
                status=result["status"],
                rc=result["expected_rc_from_user"],
                files=result["file_count"],
                size=human_bytes(result["total_size"]),
                mtime=fmt_ts(log.get("mtime") if log.get("exists") else None),
                present=present,
                missing="; ".join(missing_parts) if missing_parts else "none",
            )
        )
    lines.append("")
    lines.append("## Week Details")
    for result in results:
        lines.append("")
        lines.append(f"### {result['week']} `{result['start']}..{result['end']}`")
        lines.append(f"Status: **{result['status']}**; user rc: `{result['expected_rc_from_user']}`")
        lines.append(f"Output: `{result['output_dir']}`")
        if result["newest_file"]:
            newest = result["newest_file"]
            lines.append(f"Newest output file: `{newest['path']}` at {fmt_ts(newest['mtime'])}")
        log = result["log"]
        if log.get("exists"):
            lines.append(f"Log: `{log['path']}` ({human_bytes(log['size'])}, mtime {fmt_ts(log['mtime'])})")
            lines.append(f"Parsed final summary: `{log.get('summary')}`")
            if log.get("failed_symbols"):
                lines.append(f"Failed symbols from tail: `{', '.join(log['failed_symbols'])}`")
            if log.get("tail_error_like"):
                lines.append("")
                lines.append("Last error-like log lines:")
                lines.append("```text")
                for line in log["tail_error_like"][-12:]:
                    lines.append(line[:500])
                lines.append("```")
            lines.append("")
            lines.append("Log tail:")
            lines.append("```text")
            for line in log["tail"][-25:]:
                lines.append(line[:500])
            lines.append("```")
        else:
            lines.append("Log: missing")
        lines.append("")
        lines.append("Artifact row counts:")
        lines.append("")
        lines.append("| Kind | Symbol | File | Rows | Size |")
        lines.append("|---|---|---|---:|---:|")
        for artifact in result["artifacts"]:
            if artifact["kind"] not in {"orders", "trades", "lifecycle_summary", "stream_sanity"}:
                continue
            rows = ""
            if "csv" in artifact:
                rows = str(artifact["csv"].get("rows", ""))
            elif "json" in artifact:
                rows = "json"
            lines.append(
                f"| {artifact['kind']} | {artifact.get('symbol') or ''} | `{artifact['path']}` | {rows} | {human_bytes(artifact['size'])} |"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ext", type=Path, default=DEFAULT_EXT, help="Backtest output root on external disk")
    parser.add_argument("--report", type=Path, default=Path("runtime/core5_weekly_6week_analysis_20260624.md"))
    parser.add_argument("--json", type=Path, default=Path("runtime/core5_weekly_6week_analysis_20260624.json"))
    args = parser.parse_args()

    log_dir = args.ext / "databento_api_core5_weekly_6mo_logs"
    results = [analyze_week(args.ext, log_dir, *week) for week in DEFAULT_WEEKS]
    report = build_report(results, args.ext, log_dir)

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

    print(report)
    print(f"Wrote report: {args.report}")
    print(f"Wrote JSON: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
