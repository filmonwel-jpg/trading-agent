#!/usr/bin/env python3
"""Analyze Databento core-5 weekly API backtest logs and output folders.

This script is intentionally stdlib-only so it can be run on a workstation that has
only the repository checked out and the external DatabentoVault disk mounted.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_EXT = Path("/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests")
DEFAULT_WEEKS: tuple[tuple[str, str, str, str], ...] = (
    ("w01", "2025-12-23", "2025-12-26", "databento_api_core5_w01_20251223_to_20251226"),
    ("w02", "2025-12-29", "2026-01-02", "databento_api_core5_w02_20251229_to_20260102"),
    ("w03", "2026-01-05", "2026-01-09", "databento_api_core5_w03_20260105_to_20260109"),
    ("w04", "2026-01-12", "2026-01-16", "databento_api_core5_w04_20260112_to_20260116"),
    ("w05", "2026-01-19", "2026-01-23", "databento_api_core5_w05_20260119_to_20260123"),
    ("w06", "2026-01-26", "2026-01-30", "databento_api_core5_w06_20260126_to_20260130"),
)
SYMBOLS = ("TSLA", "TQQQ", "NVDA", "SPY", "QQQ")
INTERESTING_RE = re.compile(
    r"(\[BACKTEST\]|\[BACKTEST\]\[ERROR\]|\[BACKTEST\]\[WARN\]|completed=|failed=|requested=|"
    r"Exception|Traceback|ERROR|Error|error|WARN|timeout|timed out|Killed|OutOfMemory|No space|"
    r"replay failed|stream-sanity|trade_lifecycle_summary|trade_log=|order_history=)",
    re.IGNORECASE,
)
SUMMARY_RE = re.compile(r"\[BACKTEST\]\s+completed=(\d+)\s+failed=(\d+)\s+requested=(\d+)\s+output_dir=(.*)")
SYMBOL_RE = re.compile(r"\[BACKTEST\]\s+symbol=([A-Z0-9._-]+)\s+start=([^\s]+)\s+end=([^\s]+)")
PATH_FIELD_RE = re.compile(r"\[BACKTEST\]\s+(trade_log|order_history|trade_lifecycle_summary|stream_sanity_report)=(.*)")
ERROR_RE = re.compile(r"(\[BACKTEST\]\[ERROR\]|Exception|Traceback|ERROR|Error|error|timeout|timed out|Killed|OutOfMemory|No space|replay failed)", re.IGNORECASE)


@dataclass
class CsvMetrics:
    rows: int = 0
    columns: list[str] = field(default_factory=list)
    numeric_sums: dict[str, float] = field(default_factory=dict)
    first_row: dict[str, str] | None = None
    last_row: dict[str, str] | None = None


@dataclass
class FileInfo:
    path: Path
    exists: bool
    size: int = 0
    mtime: str = ""
    metrics: CsvMetrics | dict[str, Any] | None = None


@dataclass
class WeekReport:
    name: str
    start: str
    end: str
    output_dir: Path
    log_path: Path | None = None
    log_size: int = 0
    log_mtime: str = ""
    output_exists: bool = False
    output_file_count: int = 0
    output_size: int = 0
    output_ext_counts: Counter[str] = field(default_factory=Counter)
    output_newest: str = ""
    output_newest_file: str = ""
    log_summary: tuple[int, int, int, str] | None = None
    log_symbols_seen: list[str] = field(default_factory=list)
    log_artifact_paths: dict[str, dict[str, Path]] = field(default_factory=lambda: defaultdict(dict))
    error_count: int = 0
    last_errors: list[str] = field(default_factory=list)
    last_interesting: list[str] = field(default_factory=list)
    last_tail: list[str] = field(default_factory=list)
    artifacts_by_symbol: dict[str, dict[str, FileInfo]] = field(default_factory=dict)
    status: str = "UNKNOWN"
    notes: list[str] = field(default_factory=list)


def human_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{n} B"


def timestamp(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def safe_rel(path: Path, base: Path) -> str:
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return str(path)


def scan_output_dir(path: Path, report: WeekReport) -> None:
    report.output_exists = path.is_dir()
    if not report.output_exists:
        report.notes.append("output directory missing")
        return
    newest_ts = 0.0
    newest_file: Path | None = None
    for root, _dirs, files in os.walk(path):
        for filename in files:
            file_path = Path(root) / filename
            try:
                st = file_path.stat()
            except OSError:
                continue
            report.output_file_count += 1
            report.output_size += st.st_size
            report.output_ext_counts[file_path.suffix.lower() or "<none>"] += 1
            if st.st_mtime > newest_ts:
                newest_ts = st.st_mtime
                newest_file = file_path
    if newest_file:
        report.output_newest = timestamp(newest_ts)
        report.output_newest_file = safe_rel(newest_file, path)


def parse_log(path: Path, report: WeekReport) -> None:
    if not path.exists():
        report.notes.append("log missing")
        return
    st = path.stat()
    report.log_size = st.st_size
    report.log_mtime = timestamp(st.st_mtime)
    last_errors: deque[str] = deque(maxlen=30)
    last_interesting: deque[str] = deque(maxlen=80)
    last_tail: deque[str] = deque(maxlen=35)
    seen_symbols: list[str] = []
    current_symbol: str | None = None
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.rstrip("\n")
            short = line[:600]
            last_tail.append(short)
            sym_match = SYMBOL_RE.search(line)
            if sym_match:
                current_symbol = sym_match.group(1)
                if current_symbol not in seen_symbols:
                    seen_symbols.append(current_symbol)
            path_match = PATH_FIELD_RE.search(line)
            if path_match and current_symbol:
                key = path_match.group(1)
                report.log_artifact_paths[current_symbol][key] = Path(path_match.group(2).strip())
            summary_match = SUMMARY_RE.search(line)
            if summary_match:
                report.log_summary = (
                    int(summary_match.group(1)),
                    int(summary_match.group(2)),
                    int(summary_match.group(3)),
                    summary_match.group(4).strip(),
                )
            if ERROR_RE.search(line):
                report.error_count += 1
                last_errors.append(f"L{line_no}: {short}")
            if INTERESTING_RE.search(line):
                last_interesting.append(f"L{line_no}: {short}")
    report.log_symbols_seen = seen_symbols
    report.last_errors = list(last_errors)
    report.last_interesting = list(last_interesting)
    report.last_tail = list(last_tail)


def parse_csv_metrics(path: Path) -> CsvMetrics:
    metrics = CsvMetrics()
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            metrics.columns = list(reader.fieldnames or [])
            numeric_sums: dict[str, float] = defaultdict(float)
            for row in reader:
                metrics.rows += 1
                if metrics.first_row is None:
                    metrics.first_row = dict(row)
                metrics.last_row = dict(row)
                for key, value in row.items():
                    if value is None or value == "":
                        continue
                    try:
                        numeric_sums[key] += float(value)
                    except ValueError:
                        pass
            metrics.numeric_sums = dict(numeric_sums)
    except OSError:
        pass
    return metrics


def parse_json_metrics(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            data = json.load(handle)
    except Exception as exc:  # noqa: BLE001 - diagnostic script
        return {"parse_error": str(exc)}
    if isinstance(data, dict):
        compact: dict[str, Any] = {}
        preferred = (
            "symbol", "start", "end", "records", "recordCount", "events", "bars", "equityBars", "optionBars",
            "firstTimestamp", "lastTimestamp", "firstEventTime", "lastEventTime", "eventCount", "sanityPassed",
            "valid", "errors", "warnings", "missing", "status",
        )
        for key in preferred:
            if key in data:
                compact[key] = data[key]
        if not compact:
            for key, value in list(data.items())[:20]:
                if isinstance(value, (str, int, float, bool)) or value is None:
                    compact[key] = value
                elif isinstance(value, (list, dict)):
                    compact[key] = f"{type(value).__name__}[{len(value)}]"
        return compact
    return {"json_type": type(data).__name__}


def resolve_artifacts(report: WeekReport) -> None:
    # Prefer artifact paths printed by the runner. If a failed week lacks the final symbol output,
    # fall back to globbing by symbol and date range.
    by_symbol: dict[str, dict[str, FileInfo]] = defaultdict(dict)
    key_to_suffix = {
        "trade_log": "trades.csv",
        "order_history": "orders.csv",
        "trade_lifecycle_summary": "trade-lifecycle-summary.csv",
        "stream_sanity_report": "stream-sanity.json",
    }
    for symbol in SYMBOLS:
        printed = report.log_artifact_paths.get(symbol, {})
        for key, suffix in key_to_suffix.items():
            candidates: list[Path] = []
            if key in printed:
                candidates.append(printed[key])
            glob = sorted(report.output_dir.glob(f"{symbol}-{report.start}-to-{report.end}-*-{suffix}"))
            candidates.extend(p for p in glob if p not in candidates)
            selected = candidates[0] if candidates else report.output_dir / f"{symbol}-{report.start}-to-{report.end}-MISSING-{suffix}"
            exists = selected.exists()
            info = FileInfo(path=selected, exists=exists)
            if exists:
                st = selected.stat()
                info.size = st.st_size
                info.mtime = timestamp(st.st_mtime)
                if selected.suffix.lower() == ".csv":
                    info.metrics = parse_csv_metrics(selected)
                elif selected.suffix.lower() == ".json":
                    info.metrics = parse_json_metrics(selected)
            by_symbol[symbol][key] = info
    report.artifacts_by_symbol = dict(by_symbol)


def classify(report: WeekReport) -> None:
    completed = failed = requested = None
    if report.log_summary:
        completed, failed, requested, _out = report.log_summary
    complete_symbols = 0
    partial_symbols = 0
    missing_symbols = 0
    for symbol in SYMBOLS:
        infos = report.artifacts_by_symbol.get(symbol, {})
        present_count = sum(1 for info in infos.values() if info.exists)
        if present_count == len(infos) and present_count > 0:
            complete_symbols += 1
        elif present_count > 0:
            partial_symbols += 1
        else:
            missing_symbols += 1
    if completed == requested and failed == 0 and complete_symbols == len(SYMBOLS):
        report.status = "COMPLETE"
    elif completed is not None and failed and completed > 0:
        report.status = "PARTIAL_FAILED"
    elif complete_symbols > 0 or partial_symbols > 0:
        report.status = "PARTIAL"
    elif report.log_path and report.log_path.exists():
        report.status = "FAILED_NO_OUTPUT"
    else:
        report.status = "MISSING"
    if report.log_summary:
        report.notes.append(f"runner_summary completed={completed} failed={failed} requested={requested}")
    report.notes.append(f"artifact_symbols complete={complete_symbols} partial={partial_symbols} missing={missing_symbols}")


def analyze(ext: Path) -> list[WeekReport]:
    log_dir = ext / "databento_api_core5_weekly_6mo_logs"
    reports: list[WeekReport] = []
    for name, start, end, folder in DEFAULT_WEEKS:
        output_dir = ext / folder
        report = WeekReport(name=name, start=start, end=end, output_dir=output_dir)
        log_matches = sorted(log_dir.glob(f"{name}_*.log")) if log_dir.is_dir() else []
        report.log_path = log_matches[0] if log_matches else None
        scan_output_dir(output_dir, report)
        if report.log_path:
            parse_log(report.log_path, report)
        resolve_artifacts(report)
        classify(report)
        reports.append(report)
    return reports


def artifact_row_text(symbol: str, infos: dict[str, FileInfo]) -> str:
    parts: list[str] = []
    for key in ("trade_log", "order_history", "trade_lifecycle_summary", "stream_sanity_report"):
        info = infos[key]
        label = {
            "trade_log": "trades",
            "order_history": "orders",
            "trade_lifecycle_summary": "lifecycle",
            "stream_sanity_report": "sanity",
        }[key]
        if not info.exists:
            parts.append(f"{label}=MISSING")
            continue
        detail = human_bytes(info.size)
        if isinstance(info.metrics, CsvMetrics):
            detail += f",rows={info.metrics.rows}"
            # Include useful sums if the column names are present.
            useful_sums = []
            for col in ("pnl", "realizedPnl", "realized_pnl", "netPnl", "net_pnl", "quantity", "qty", "shares"):
                if col in info.metrics.numeric_sums:
                    useful_sums.append(f"{col}_sum={info.metrics.numeric_sums[col]:.2f}")
            if useful_sums:
                detail += "," + ",".join(useful_sums[:3])
        elif isinstance(info.metrics, dict) and info.metrics:
            compact = ",".join(f"{k}={v}" for k, v in list(info.metrics.items())[:8])
            detail += f",{compact}"
        parts.append(f"{label}=OK({detail})")
    return f"- {symbol}: " + "; ".join(parts)


def render_markdown(reports: list[WeekReport], ext: Path) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("# Databento core5 weekly 6-month run partial analysis")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"External root: `{ext}`")
    lines.append("")
    lines.append("## Executive summary")
    complete = [r for r in reports if r.status == "COMPLETE"]
    partial = [r for r in reports if r.status != "COMPLETE"]
    lines.append(f"- Complete weeks: {len(complete)} / {len(reports)} (`{', '.join(r.name for r in complete)}`)")
    lines.append(f"- Non-complete weeks: {len(partial)} / {len(reports)} (`{', '.join(r.name for r in partial)}`)")
    for r in reports:
        summary = f"completed={r.log_summary[0]} failed={r.log_summary[1]} requested={r.log_summary[2]}" if r.log_summary else "no runner summary"
        lines.append(
            f"- `{r.name}` {r.start}..{r.end}: **{r.status}**, {summary}, "
            f"output_files={r.output_file_count}, output_size={human_bytes(r.output_size)}, errors_seen={r.error_count}"
        )
    lines.append("")
    lines.append("## Weekly details")
    for r in reports:
        lines.append(f"### {r.name} {r.start}..{r.end} — {r.status}")
        lines.append("")
        lines.append(f"- Log: `{r.log_path}`")
        lines.append(f"  - size={human_bytes(r.log_size)} mtime={r.log_mtime or 'n/a'}")
        lines.append(f"- Output dir: `{r.output_dir}`")
        lines.append(f"  - exists={r.output_exists} files={r.output_file_count} size={human_bytes(r.output_size)} newest={r.output_newest or 'n/a'} newest_file=`{r.output_newest_file}`")
        lines.append(f"  - ext_counts={dict(r.output_ext_counts)}")
        lines.append(f"- Notes: {'; '.join(r.notes)}")
        if r.log_symbols_seen:
            lines.append(f"- Symbols seen in log: `{', '.join(r.log_symbols_seen)}`")
        lines.append("- Artifacts by symbol:")
        for symbol in SYMBOLS:
            lines.append(artifact_row_text(symbol, r.artifacts_by_symbol.get(symbol, {})))
        if r.last_errors:
            lines.append("- Last error-like lines:")
            for line in r.last_errors[-12:]:
                lines.append(f"  - `{line}`")
        lines.append("- Last interesting log lines:")
        for line in r.last_interesting[-18:]:
            lines.append(f"  - `{line}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def render_console(reports: list[WeekReport]) -> str:
    lines: list[str] = []
    lines.append("week,start,end,status,runner_completed,runner_failed,runner_requested,output_files,output_size,error_count,log_mtime")
    for r in reports:
        completed = failed = requested = ""
        if r.log_summary:
            completed, failed, requested, _ = r.log_summary
        lines.append(
            f"{r.name},{r.start},{r.end},{r.status},{completed},{failed},{requested},"
            f"{r.output_file_count},{r.output_size},{r.error_count},{r.log_mtime}"
        )
    lines.append("")
    for r in reports:
        lines.append(f"{r.name} {r.status}: {'; '.join(r.notes)}")
        if r.last_errors:
            lines.append("  last error: " + r.last_errors[-1])
        missing = []
        for symbol, infos in r.artifacts_by_symbol.items():
            missing_keys = [key for key, info in infos.items() if not info.exists]
            if missing_keys:
                missing.append(f"{symbol}:{'/'.join(missing_keys)}")
        if missing:
            lines.append("  missing artifacts: " + ", ".join(missing))
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ext", type=Path, default=DEFAULT_EXT, help="External backtests root")
    parser.add_argument("--report", type=Path, default=Path("runtime/research_runs/databento_core5_weekly_6mo_analysis_20260624.md"), help="Markdown report path")
    args = parser.parse_args()

    reports = analyze(args.ext)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_markdown(reports, args.ext), encoding="utf-8")
    print(render_console(reports))
    print(f"\nreport={args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
