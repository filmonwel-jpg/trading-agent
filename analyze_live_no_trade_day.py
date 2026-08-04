#!/usr/bin/env python3
"""Analyze a live Databento/IBKR trading day from bot logs.

The script is intentionally strict about what counts as order activity.  In
particular, shared-gateway registration lines such as request_ids containing
``executions`` are gateway bookkeeping, not fills.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date as date_type
from pathlib import Path
from typing import TextIO


DEFAULT_SYMBOLS = "NVDA,QQQ,SPY,TQQQ,TSLA"

AI_ENTRY_RE = re.compile(
    r"\[AI\.(LONG|SHORT)\.ENTRY\].*AI_PREDICTS_ENTRY=(PASS|FAIL).*?"
    r"symbol=([A-Z0-9._-]+).*?prob=([0-9.]+).*?threshold=([0-9.]+)"
)
SETUP_FILTER_RE = re.compile(r"\[AI\.DOWNSTREAM_SETUP_FILTER\].*SETUP_FILTER_PASSES=(PASS|FAIL).*?symbol=([A-Z0-9._-]+)")
PROB_THRESHOLD_RE = re.compile(r"\bprob=([0-9.]+).*?\bthreshold=([0-9.]+)")
ARBITRATION_FAIL_RE = re.compile(r"\[AI\.ENTRY\.ARBITRATION\].*ENTRY_SIDE_SELECTED=FAIL.*?reason=([^\s|]+)")
ENTRY_GATE_RE = re.compile(r"\[AI\.ENTRY\].*ENTRY_GATE_OPEN=(PASS|FAIL).*?allowNewEntries=([^\s|]+)")
ENTRY_GATE_DATA_QUALITY_RE = re.compile(r"\bdataQualityAllowsNewEntries=([^\s|]+)")
MICRO_ENTRY_RE = re.compile(r"\[AI\.MICRO\.(LONG|SHORT)\.ENTRY\].*MICRO_ENTRY_CONFIRMS=(PASS|FAIL)")
ORDER_SEND_RE = re.compile(r"\[FLOW\]\[DATA\]\[ORDER\.SEND\].*?orderId=([0-9-]+).*?action=([A-Z]+)")
ORDER_SEND_FILLED_RE = re.compile(r"\bfilled=([0-9]+)\b")
TRADE_CSV_NAMES = ("trades-{symbol}.csv", "{symbol}_trades.csv", "{symbol}.csv")
TRADE_TIMESTAMP_FIELDS = (
    "Timestamp",
    "timestamp",
    "Time",
    "time",
    "Date",
    "date",
    "Datetime",
    "datetime",
    "EntryTime",
    "entry_time",
    "ExitTime",
    "exit_time",
    "closedAt",
    "close_time",
)
TRADE_SYMBOL_FIELDS = ("Symbol", "symbol", "tradeSymbol", "trade_symbol", "Ticker", "ticker")
TIMESTAMP_PREFIX_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}(?::\d{2})?)")
COMPACT_TIMESTAMP_PREFIX_RE = re.compile(r"\b(\d{4})(\d{2})(\d{2})[T\s]?(\d{2}:\d{2}(?::\d{2})?)")
TIME_ONLY_RE = re.compile(r"^(\d{2}:\d{2})(?::(\d{2}))?$")

BENIGN_ERROR_SUBSTRINGS = (
    "HikariPool-1 - Shutdown initiated",
    "HikariPool-1 - Shutdown completed",
    "SpringApplicationShutdownHook",
)


@dataclass
class BestMargin:
    margin: float = float("-inf")
    side: str = ""
    probability: float = 0.0
    threshold: float = 0.0
    line: str = ""


@dataclass
class SymbolSummary:
    symbol: str
    counts: Counter[str] = field(default_factory=Counter)
    samples: defaultdict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    reject_reasons: Counter[str] = field(default_factory=Counter)
    entry_gate_reasons: Counter[str] = field(default_factory=Counter)
    best_entry_margin: BestMargin = field(default_factory=BestMargin)
    best_setup_margin: BestMargin = field(default_factory=BestMargin)
    total_today_lines: int = 0
    files: list[str] = field(default_factory=list)


@dataclass
class TradeCsvSummary:
    matching_rows: int = 0
    total_symbol_rows: int = 0
    other_date_rows: int = 0
    outside_window_rows: int = 0
    unscoped_rows: int = 0
    malformed_files: int = 0
    files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AnalysisWindow:
    since: str | None = None
    until: str | None = None


def parse_symbols(raw: str) -> list[str]:
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def add_sample(summary: SymbolSummary, key: str, line: str, limit: int = 5) -> None:
    bucket = summary.samples[key]
    if len(bucket) < limit:
        bucket.append(line.rstrip("\n"))


def open_text(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("rt", encoding="utf-8", errors="replace")


def log_files_for_symbol(log_dir: Path, symbol: str, trading_date: str) -> list[Path]:
    base = f"trading-agent-{symbol}.log"
    patterns = [
        base,
        f"{base}.{trading_date}*",
        f"{symbol.lower()}_live_trade_logs.txt",
        f"{symbol}_live_trade_logs.txt",
    ]
    seen: set[Path] = set()
    files: list[Path] = []
    for pattern in patterns:
        for path in log_dir.glob(pattern):
            if path.is_file() and path not in seen:
                seen.add(path)
                files.append(path)
    return sorted(files, key=lambda p: (p.name == base, p.name))


def has_runtime_issue(line: str) -> bool:
    if any(token in line for token in BENIGN_ERROR_SUBSTRINGS):
        return False
    return "[FLOW][ERROR]" in line or " ERROR " in line or "Exception" in line or "Traceback" in line


def update_best_margin(best: BestMargin, side: str, probability: float, threshold: float, line: str) -> None:
    margin = probability - threshold
    if margin > best.margin:
        best.margin = margin
        best.side = side
        best.probability = probability
        best.threshold = threshold
        best.line = line.rstrip("\n")


def compact_date(trading_date: str) -> str:
    return trading_date.replace("-", "")


def normalize_time_fragment(raw: str) -> str:
    text = raw.strip()
    match = TIME_ONLY_RE.match(text)
    if not match:
        raise ValueError(f"Invalid time value '{raw}'. Use HH:MM or HH:MM:SS.")
    seconds = match.group(2) or "00"
    return f"{match.group(1)}:{seconds}"


def normalize_window_bound(raw: str | None, trading_date: str) -> str | None:
    if raw is None or not raw.strip():
        return None
    text = raw.strip()
    if TIME_ONLY_RE.match(text):
        return f"{trading_date}T{normalize_time_fragment(text)}"
    if match := TIMESTAMP_PREFIX_RE.search(text):
        bound_date, time_fragment = match.groups()
        return f"{bound_date}T{normalize_time_fragment(time_fragment)}"
    if match := COMPACT_TIMESTAMP_PREFIX_RE.search(text):
        year, month, day, time_fragment = match.groups()
        return f"{year}-{month}-{day}T{normalize_time_fragment(time_fragment)}"
    raise ValueError(f"Invalid timestamp value '{raw}'. Use HH:MM, HH:MM:SS, or YYYY-MM-DDTHH:MM:SS.")


def build_analysis_window(since: str | None, until: str | None, trading_date: str) -> AnalysisWindow:
    window = AnalysisWindow(
        since=normalize_window_bound(since, trading_date),
        until=normalize_window_bound(until, trading_date),
    )
    if window.since is not None and window.until is not None and window.since >= window.until:
        raise ValueError(f"--since must be earlier than --until; got since={since!r} until={until!r}")
    return window


def timestamp_sort_key(value: str, trading_date: str) -> str | None:
    if match := TIMESTAMP_PREFIX_RE.search(value):
        value_date, time_fragment = match.groups()
        return f"{value_date}T{normalize_time_fragment(time_fragment)}"
    if match := COMPACT_TIMESTAMP_PREFIX_RE.search(value):
        year, month, day, time_fragment = match.groups()
        return f"{year}-{month}-{day}T{normalize_time_fragment(time_fragment)}"
    if value.strip().startswith(trading_date) or value.strip().startswith(compact_date(trading_date)):
        return f"{trading_date}T00:00:00"
    return None


def timestamp_in_window(value: str, trading_date: str, window: AnalysisWindow) -> bool:
    key = timestamp_sort_key(value, trading_date)
    if key is None:
        return True
    if window.since is not None and key < window.since:
        return False
    if window.until is not None and key >= window.until:
        return False
    return True


def log_line_in_scope(line: str, trading_date: str, window: AnalysisWindow) -> bool:
    return line.startswith(trading_date) and timestamp_in_window(line, trading_date, window)


def timestamp_matches_date(value: str, trading_date: str) -> bool:
    text = value.strip()
    if not text:
        return False
    compact = compact_date(trading_date)
    if text.startswith(trading_date) or text.startswith(compact):
        return True
    if trading_date in text or compact in text:
        return True

    ymd = re.search(r"\b(\d{4})[-/]?(\d{2})[-/]?(\d{2})\b", text)
    if ymd and f"{ymd.group(1)}-{ymd.group(2)}-{ymd.group(3)}" == trading_date:
        return True

    mdy = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if mdy:
        month, day, year = (int(mdy.group(1)), int(mdy.group(2)), int(mdy.group(3)))
        return f"{year:04d}-{month:02d}-{day:02d}" == trading_date
    return False


def first_present_value(row: dict[str, str], fields: tuple[str, ...]) -> str | None:
    for field in fields:
        value = row.get(field)
        if value is not None and value.strip():
            return value

    normalized_targets = {field.lower().replace("_", "") for field in fields}
    for field, value in row.items():
        if value is None:
            continue
        normalized = field.strip().lower().replace("_", "").replace(" ", "")
        if normalized in normalized_targets and value.strip():
            return value
    return None


def row_symbol_matches(row: dict[str, str], symbol: str) -> bool:
    row_symbol = first_present_value(row, TRADE_SYMBOL_FIELDS)
    return row_symbol is None or row_symbol.strip().upper() == symbol.upper()


def row_matches_trading_date(row: dict[str, str], trading_date: str) -> bool | None:
    timestamp = first_present_value(row, TRADE_TIMESTAMP_FIELDS)
    if timestamp is None:
        return None
    return timestamp_matches_date(timestamp, trading_date)


def classify_line(summary: SymbolSummary, line: str) -> None:
    c = summary.counts

    if "DATABENTO" in line:
        c["databento_any"] += 1
        if "heartbeat symbols=" in line:
            c["databento_heartbeat"] += 1
        if "subscription_ack" in line:
            c["databento_subscription_ack"] += 1
        if re.search(r"(?i)auth|unauthorized|permission", line):
            c["databento_auth_or_permission"] += 1
            add_sample(summary, "databento_auth_or_permission", line)
    if "[STRATEGY.BAR]" in line:
        c["strategy_bar"] += 1
    if "30s BUCKET" in line:
        c["bar_30s_bucket"] += 1
    if "startup-delay-complete" in line:
        c["startup_delay_complete"] += 1

    if "[AI.RESPONSE]" in line:
        c["ai_response"] += 1
    if "[AI.DOWNSTREAM_SETUP_FILTER.SNAPSHOT]" in line:
        c["setup_snapshot"] += 1
    if "featureSnapshot=hit" in line:
        c["feature_snapshot_hit"] += 1
    if "featureSnapshot=miss" in line:
        c["feature_snapshot_miss"] += 1
    if "featureSnapshot=disabled" in line:
        c["feature_snapshot_disabled"] += 1
    if "featureSidecar=hit" in line:
        c["feature_sidecar_hit"] += 1
    if "featureSidecar=miss" in line:
        c["feature_sidecar_miss"] += 1
    if "featureSidecar=disabled" in line:
        c["feature_sidecar_disabled"] += 1

    if match := ENTRY_GATE_RE.search(line):
        verdict, allow_new_entries = match.groups()
        c[f"entry_gate_{verdict.lower()}"] += 1
        if verdict == "FAIL":
            summary.entry_gate_reasons[f"allowNewEntries={allow_new_entries}"] += 1
            if data_quality_match := ENTRY_GATE_DATA_QUALITY_RE.search(line):
                data_quality_allows_new_entries = data_quality_match.group(1)
                summary.entry_gate_reasons[f"dataQualityAllowsNewEntries={data_quality_allows_new_entries}"] += 1
                if data_quality_allows_new_entries.lower() == "false":
                    c["entry_gate_data_quality_false"] += 1
            add_sample(summary, "entry_gate_fail", line)

    if match := AI_ENTRY_RE.search(line):
        side, verdict, _symbol, probability_raw, threshold_raw = match.groups()
        probability = float(probability_raw)
        threshold = float(threshold_raw)
        side_key = side.lower()
        c[f"ai_{side_key}_entry_{verdict.lower()}"] += 1
        update_best_margin(summary.best_entry_margin, side_key, probability, threshold, line)
        if verdict == "FAIL":
            add_sample(summary, f"ai_{side_key}_entry_fail", line)
        else:
            add_sample(summary, f"ai_{side_key}_entry_pass", line)

    if match := SETUP_FILTER_RE.search(line):
        verdict = match.group(1)
        c[f"setup_filter_{verdict.lower()}"] += 1
        if prob_threshold := PROB_THRESHOLD_RE.search(line):
            update_best_margin(summary.best_setup_margin, "setup", float(prob_threshold.group(1)), float(prob_threshold.group(2)), line)
        add_sample(summary, f"setup_filter_{verdict.lower()}", line)

    if match := ARBITRATION_FAIL_RE.search(line):
        c["entry_arbitration_fail"] += 1
        summary.reject_reasons[match.group(1)] += 1
        add_sample(summary, "entry_arbitration_fail", line)

    if match := MICRO_ENTRY_RE.search(line):
        side, verdict = match.groups()
        c[f"micro_{side.lower()}_{verdict.lower()}"] += 1
        add_sample(summary, f"micro_{verdict.lower()}", line)
    if "Research no-trade" in line and "[AI.MICRO.ENTRY]" in line:
        c["micro_research_no_trade"] += 1
        add_sample(summary, "micro_research_no_trade", line)

    if "firing order" in line or "Closing position size=" in line:
        c["strategy_fire_order_intent"] += 1
        add_sample(summary, "strategy_fire_order_intent", line)
    if "Shared capital reserved symbol=" in line:
        c["shared_capital_reserved"] += 1
        add_sample(summary, "shared_capital_reserved", line)
    if "shared capital unavailable" in line:
        c["shared_capital_unavailable"] += 1
        add_sample(summary, "shared_capital_unavailable", line)
    if "Shared capital release symbol=" in line:
        c["shared_capital_release"] += 1
    if "Shared capital reconcile symbol=" in line:
        c["shared_capital_reconcile"] += 1

    if ORDER_SEND_RE.search(line):
        c["order_send"] += 1
        if "terminal/noop" in line:
            c["order_send_terminal_noop"] += 1
        if match := ORDER_SEND_FILLED_RE.search(line):
            filled = int(match.group(1))
            if filled > 0:
                c["order_send_terminal_filled_qty"] += filled
        add_sample(summary, "order_send", line)
    if "[FLOW][DATA][STRATEGY.ORDER] submitted orderId=" in line:
        c["strategy_order_submitted"] += 1
        add_sample(summary, "strategy_order_submitted", line)
    if "[FLOW][COND][STRATEGY.ORDER] FILLED_DELTA_POSITIVE=PASS" in line:
        c["strategy_order_fill_positive"] += 1
        add_sample(summary, "strategy_order_fill_positive", line)
    if "[FLOW][ANALYZE][STRATEGY.ORDER] fill orderId=" in line:
        c["strategy_order_fill_analyze"] += 1
        add_sample(summary, "strategy_order_fill_analyze", line)
    if "[FLOW][DATA][STRATEGY.ORDER] closed orderId=" in line:
        c["strategy_order_closed"] += 1
        if "status=Filled" in line:
            c["strategy_order_closed_filled"] += 1
        if "status=Cancelled" in line or "status=ApiCancelled" in line:
            c["strategy_order_closed_cancelled"] += 1

    if "position sync requested" in line or "position sync completed" in line:
        c["ibkr_position_sync"] += 1
    if "EOD flatten workflow completed" in line:
        c["eod_flatten_completed"] += 1
        add_sample(summary, "eod_flatten_completed", line)

    if has_runtime_issue(line):
        c["runtime_issue"] += 1
        add_sample(summary, "runtime_issue", line)


def count_trade_csv_rows(trade_dir: Path, symbol: str, trading_date: str, window: AnalysisWindow | None = None) -> TradeCsvSummary:
    active_window = window or AnalysisWindow()
    summary = TradeCsvSummary()
    if not trade_dir.exists():
        return summary
    for template in TRADE_CSV_NAMES:
        path = trade_dir / template.format(symbol=symbol)
        if not path.exists() or not path.is_file():
            continue
        summary.files.append(str(path))
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            try:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    continue
                for row in reader:
                    if not any((value or "").strip() for value in row.values()):
                        continue
                    if not row_symbol_matches(row, symbol):
                        continue
                    summary.total_symbol_rows += 1
                    timestamp = first_present_value(row, TRADE_TIMESTAMP_FIELDS)
                    if timestamp is None:
                        summary.unscoped_rows += 1
                    elif timestamp_matches_date(timestamp, trading_date):
                        if timestamp_in_window(timestamp, trading_date, active_window):
                            summary.matching_rows += 1
                        else:
                            summary.outside_window_rows += 1
                    else:
                        summary.other_date_rows += 1
            except csv.Error:
                summary.malformed_files += 1
                handle.seek(0)
                for line_number, line in enumerate(handle):
                    if line_number == 0 or not line.strip():
                        continue
                    summary.total_symbol_rows += 1
                    if timestamp_matches_date(line, trading_date):
                        if timestamp_in_window(line, trading_date, active_window):
                            summary.matching_rows += 1
                        else:
                            summary.outside_window_rows += 1
                    else:
                        summary.unscoped_rows += 1
    return summary


def analyze_symbol(log_dir: Path, trade_dir: Path, symbol: str, trading_date: str, window: AnalysisWindow | None = None) -> SymbolSummary:
    active_window = window or AnalysisWindow()
    summary = SymbolSummary(symbol=symbol)
    files = log_files_for_symbol(log_dir, symbol, trading_date)
    summary.files = [str(path) for path in files]
    for path in files:
        with open_text(path) as handle:
            for line in handle:
                if not log_line_in_scope(line, trading_date, active_window):
                    continue
                summary.total_today_lines += 1
                classify_line(summary, line)
    trade_summary = count_trade_csv_rows(trade_dir, symbol, trading_date, active_window)
    summary.counts["trade_csv_rows"] = trade_summary.matching_rows
    if trade_summary.total_symbol_rows:
        summary.counts["trade_csv_rows_total"] = trade_summary.total_symbol_rows
    if trade_summary.other_date_rows:
        summary.counts["trade_csv_rows_other_dates"] = trade_summary.other_date_rows
    if trade_summary.outside_window_rows:
        summary.counts["trade_csv_rows_outside_window"] = trade_summary.outside_window_rows
    if trade_summary.unscoped_rows:
        summary.counts["trade_csv_rows_unscoped"] = trade_summary.unscoped_rows
    if trade_summary.malformed_files:
        summary.counts["trade_csv_malformed_files"] = trade_summary.malformed_files
    if trade_summary.files:
        summary.files.extend(trade_summary.files)
    return summary


def real_order_activity_count(counts: Counter[str]) -> int:
    return sum(
        counts[key]
        for key in (
            "trade_csv_rows",
            "order_send",
            "strategy_order_submitted",
            "strategy_order_fill_positive",
            "strategy_order_fill_analyze",
            "strategy_order_closed_filled",
            "shared_capital_reserved",
        )
    )


def decision_activity_count(counts: Counter[str]) -> int:
    return sum(
        counts[key]
        for key in (
            "ai_long_entry_pass",
            "ai_long_entry_fail",
            "ai_short_entry_pass",
            "ai_short_entry_fail",
            "entry_arbitration_fail",
            "setup_filter_pass",
            "setup_filter_fail",
            "micro_long_pass",
            "micro_long_fail",
            "micro_short_pass",
            "micro_short_fail",
            "entry_gate_fail",
            "entry_gate_pass",
        )
    )


def verdict_for(global_counts: Counter[str]) -> str:
    if real_order_activity_count(global_counts) > 0:
        return "TRADES_OR_REAL_ORDER_ACTIVITY_DETECTED"
    if global_counts["runtime_issue"] > 0:
        return "NO_TRADE_WITH_RUNTIME_ISSUES"
    if decision_activity_count(global_counts) > 0:
        return "NO_TRADE_MODEL_OR_ENTRY_GATES_REJECTED"
    if global_counts["databento_any"] > 0 or global_counts["strategy_bar"] > 0:
        return "NO_TRADE_FEED_SEEN_BUT_NO_ENTRY_DECISIONS"
    return "NO_TRADE_NO_TODAY_ACTIVITY_SEEN"


def counter_to_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def best_margin_to_dict(best: BestMargin) -> dict[str, object] | None:
    if best.margin == float("-inf"):
        return None
    return {
        "side": best.side,
        "probability": best.probability,
        "threshold": best.threshold,
        "margin": best.margin,
        "line": best.line,
    }


def build_result(summaries: list[SymbolSummary], trading_date: str, log_dir: Path, trade_dir: Path, window: AnalysisWindow | None = None) -> dict[str, object]:
    global_counts: Counter[str] = Counter()
    for summary in summaries:
        global_counts.update(summary.counts)
    verdict = verdict_for(global_counts)
    active_window = window or AnalysisWindow()
    return {
        "date": trading_date,
        "since": active_window.since,
        "until": active_window.until,
        "log_dir": str(log_dir),
        "trade_dir": str(trade_dir),
        "verdict": verdict,
        "global_counts": counter_to_dict(global_counts),
        "symbols": {
            summary.symbol: {
                "total_today_lines": summary.total_today_lines,
                "counts": counter_to_dict(summary.counts),
                "reject_reasons": counter_to_dict(summary.reject_reasons),
                "entry_gate_reasons": counter_to_dict(summary.entry_gate_reasons),
                "best_entry_margin": best_margin_to_dict(summary.best_entry_margin),
                "best_setup_margin": best_margin_to_dict(summary.best_setup_margin),
                "files": summary.files,
                "samples": {key: vals for key, vals in sorted(summary.samples.items())},
            }
            for summary in summaries
        },
    }


def fmt_count(counts: Counter[str], key: str) -> int:
    return counts[key]


def fmt_margin(best: BestMargin) -> str:
    if best.margin == float("-inf"):
        return "-"
    return f"{best.side} {best.probability:.4f}/{best.threshold:.4f} ({best.margin:+.4f})"


def build_markdown(result: dict[str, object], summaries: list[SymbolSummary]) -> str:
    global_counts = Counter(result["global_counts"])  # type: ignore[arg-type]
    lines: list[str] = []
    lines.append(f"# Live trading day analysis — {result['date']}")
    lines.append("")
    lines.append(f"Verdict: **{result['verdict']}**")
    if result.get("since") or result.get("until"):
        lines.append("")
        lines.append(
            "Analysis window: "
            + f"`{result.get('since') or str(result['date']) + 'T00:00:00'}`"
            + " ≤ log/trade timestamp < "
            + f"`{result.get('until') or str(result['date']) + 'T23:59:59'}`"
        )
    lines.append("")
    lines.append("## What counts as real order activity")
    lines.append("")
    lines.append("The analyzer counts only strict lifecycle markers: `ORDER.SEND`, `STRATEGY.ORDER submitted`, `FILLED_DELTA_POSITIVE=PASS`, `STRATEGY.ORDER fill`, `STRATEGY.ORDER closed ... status=Filled`, `Shared capital reserved`, or trade CSV rows whose timestamp matches the requested `--date`.")
    lines.append("It does **not** count shared-gateway registration/reconcile lines such as `request_ids={'executions': ...}` as fills.")
    lines.append("Cumulative trade CSV rows from other dates are reported as `trade_csv_rows_other_dates` and do not trigger the strict order verdict for this date.")
    lines.append("")
    lines.append("## Global counts")
    lines.append("")
    for key in sorted(global_counts):
        lines.append(f"- `{key}`: {global_counts[key]}")
    lines.append("")
    lines.append("## Per-symbol funnel")
    lines.append("")
    lines.append("| Symbol | Lines | Feed | AI entry pass/fail | Entry gate fail | Arbitration fail | Setup pass/fail | Micro pass/fail | Strict orders | Trade CSV rows | Best entry margin | Issues |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|")
    for summary in summaries:
        c = summary.counts
        ai_pass = c["ai_long_entry_pass"] + c["ai_short_entry_pass"]
        ai_fail = c["ai_long_entry_fail"] + c["ai_short_entry_fail"]
        setup_pass = c["setup_filter_pass"]
        setup_fail = c["setup_filter_fail"]
        micro_pass = c["micro_long_pass"] + c["micro_short_pass"]
        micro_fail = c["micro_long_fail"] + c["micro_short_fail"]
        lines.append(
            f"| {summary.symbol} | {summary.total_today_lines} | {c['databento_any'] + c['strategy_bar']} | "
            f"{ai_pass}/{ai_fail} | {c['entry_gate_fail']} | {c['entry_arbitration_fail']} | "
            f"{setup_pass}/{setup_fail} | {micro_pass}/{micro_fail} | {real_order_activity_count(c)} | "
            f"{c['trade_csv_rows']} | {fmt_margin(summary.best_entry_margin)} | {c['runtime_issue']} |"
        )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    verdict = str(result["verdict"])
    if verdict == "TRADES_OR_REAL_ORDER_ACTIVITY_DETECTED":
        lines.append("- At least one strict order/fill/trade marker was detected. Inspect the order samples and broker/trade CSVs.")
    elif verdict == "NO_TRADE_MODEL_OR_ENTRY_GATES_REJECTED":
        lines.append("- Feed and/or AI decision markers were present, but no strict order lifecycle markers appeared. This is a clean no-trade day driven by model thresholds, arbitration, setup filters, micro gates, or entry gates.")
    elif verdict == "NO_TRADE_WITH_RUNTIME_ISSUES":
        lines.append("- No strict order activity was detected, and runtime issue markers appeared. Inspect issue samples before treating the result as a normal model no-trade day.")
    elif verdict == "NO_TRADE_FEED_SEEN_BUT_NO_ENTRY_DECISIONS":
        lines.append("- Market/feed markers were present, but no entry decision markers were seen. Check warmup, startup history, and bar accumulation.")
    else:
        lines.append("- No today's lines were found for the requested symbols/date/log directory.")
    if global_counts["entry_arbitration_fail"]:
        lines.append("- Common arbitration reject reasons: " + "; ".join(
            f"{reason}:{count}"
            for reason, count in Counter({reason: sum(s.reject_reasons[reason] for s in summaries) for summary in summaries for reason in summary.reject_reasons}).most_common(5)
        ))
    if global_counts["entry_gate_fail"]:
        lines.append("- Entry gate failures were present; per-symbol details include `allowNewEntries=false` when the strategy was outside its entry window or otherwise gated closed.")
    if global_counts["entry_gate_data_quality_false"]:
        lines.append("- Some entry gate failures were caused by `dataQualityAllowsNewEntries=false`, meaning Databento sanity/data-quality temporarily blocked new entries without changing the clock/manual entry gate.")
    lines.append("")
    lines.append("## Samples")
    lines.append("")
    for summary in summaries:
        if not summary.samples:
            continue
        lines.append(f"### {summary.symbol}")
        lines.append("")
        for key, values in sorted(summary.samples.items()):
            lines.append(f"#### {key}")
            lines.append("")
            for value in values:
                lines.append(f"```text\n{value}\n```")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(result: dict[str, object], summaries: list[SymbolSummary], output_json: Path, output_md: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(build_markdown(result, summaries), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date_type.today().isoformat(), help="Trading date to analyze, YYYY-MM-DD")
    parser.add_argument("--since", default=None, help="Inclusive lower timestamp bound, e.g. 10:00, 10:00:13, or YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--until", default=None, help="Exclusive upper timestamp bound, e.g. 13:50, 13:50:00, or YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--log-dir", type=Path, default=Path("runtime/databento/logs"), help="Directory containing trading-agent-*.log files")
    parser.add_argument("--trade-dir", type=Path, default=Path("runtime/databento/output"), help="Directory containing per-symbol trade CSVs")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS, help="Comma-separated symbol list")
    parser.add_argument("--output-json", type=Path, default=Path("runtime/databento/analysis/no_trade_analysis.json"))
    parser.add_argument("--output-md", type=Path, default=Path("runtime/databento/analysis/no_trade_analysis.md"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    symbols = parse_symbols(args.symbols)
    try:
        window = build_analysis_window(args.since, args.until, args.date)
    except ValueError as exc:
        parser.error(str(exc))
    summaries = [analyze_symbol(args.log_dir, args.trade_dir, symbol, args.date, window) for symbol in symbols]
    result = build_result(summaries, args.date, args.log_dir, args.trade_dir, window)
    write_outputs(result, summaries, args.output_json, args.output_md)
    print(f"ANALYSIS_JSON={args.output_json}")
    print(f"ANALYSIS_MD={args.output_md}")
    print(f"VERDICT={result['verdict']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




