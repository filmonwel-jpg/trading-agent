#!/usr/bin/env python3
"""Summarize Databento historical IBKR simulation backtest logs for calibration.

The parser is intentionally log-based so it can summarize runs stored under /tmp
without depending on workspace symlinks or generated CSV schemas.
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Iterable


SYMBOL_RE = re.compile(r"\[BACKTEST\] symbol=([A-Z][A-Z0-9.]*)|Starting Databento historical streaming backtest symbol=([A-Z][A-Z0-9.]*)")
KV_RE = re.compile(r"([A-Za-z_]+)=([^ |\n]+)")
PROB_RE = re.compile(r"prob=([0-9.]+)")
THRESHOLD_RE = re.compile(r"threshold=([0-9.]+)")
ERROR_RE = re.compile(r"\[ERROR\]|\[BACKTEST\]\[ERROR\]|\bTraceback\b|(?:http|status|code)[^\n]{0,20}\b401\b|CERTIFICATE_VERIFY_FAILED|timed out", re.IGNORECASE)


@dataclass
class SymbolSummary:
    trades: str = ""
    pnl: str = ""
    previous_close: str = ""
    orders: int = 0
    base_passes: int = 0
    arms_total: str = ""
    arms_long: str = ""
    arms_short: str = ""
    confirmations: str = ""
    expirations: str = ""
    micro_passes: int = 0
    micro_fails: int = 0
    long_max_prob: str = ""
    long_max_margin: str = ""
    short_max_prob: str = ""
    short_max_margin: str = ""
    closest: str = ""
    errors: list[str] = field(default_factory=list)


@dataclass
class LogSummary:
    label: str
    path: Path
    completed_line: str = ""
    symbols: dict[str, SymbolSummary] = field(default_factory=dict)
    micro_probs: dict[tuple[str, str], list[tuple[float, float | None]]] = field(default_factory=lambda: defaultdict(list))


def latest(pattern: str) -> str:
    matches = glob.glob(pattern)
    return max(matches, key=os.path.getmtime) if matches else ""


def parse_kv(line: str) -> dict[str, str]:
    return dict(KV_RE.findall(line))


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    idx = min(len(sorted_values) - 1, max(0, round((len(sorted_values) - 1) * q)))
    return sorted_values[int(idx)]


def ensure_symbol(summary: LogSummary, symbol: str) -> SymbolSummary:
    if symbol not in summary.symbols:
        summary.symbols[symbol] = SymbolSummary()
    return summary.symbols[symbol]


def parse_log(label: str, path: str, requested_symbols: Iterable[str] = ()) -> LogSummary:
    summary = LogSummary(label=label, path=Path(path))
    for symbol in requested_symbols:
        ensure_symbol(summary, symbol)

    current: str | None = None
    with open(path, errors="replace") as handle:
        for line in handle:
            match = SYMBOL_RE.search(line)
            if match:
                current = match.group(1) or match.group(2)
                ensure_symbol(summary, current)

            if "[BACKTEST] completed=" in line:
                summary.completed_line = line.strip()

            explicit_symbol = re.search(r"symbol=([A-Z][A-Z0-9.]*)", line)
            symbol = explicit_symbol.group(1) if explicit_symbol else current
            if not symbol:
                continue
            row = ensure_symbol(summary, symbol)

            if "YESTERDAY_CLOSE_AVAILABLE=PASS" in line or "PREVIOUS_CLOSE_AVAILABLE=PASS" in line:
                row.previous_close = "PASS"
            elif ("YESTERDAY_CLOSE_AVAILABLE=FAIL" in line or "PREVIOUS_CLOSE_AVAILABLE=FAIL" in line) and not row.previous_close:
                row.previous_close = "FAIL"
            if "AI_PREDICTS_ENTRY=PASS" in line:
                row.base_passes += 1
            if "simulated orderId" in line or "submitted orderId" in line:
                row.orders += 1
            if "MICRO_ENTRY_CONFIRMS=PASS" in line:
                row.micro_passes += 1
            if "MICRO_ENTRY_CONFIRMS=FAIL" in line:
                row.micro_fails += 1
            if ERROR_RE.search(line):
                row.errors.append(line.strip())

            prob_match = PROB_RE.search(line)
            if "MICRO_ENTRY_CONFIRMS=" in line and prob_match:
                side = "long" if "AI.MICRO.LONG.ENTRY" in line else "short" if "AI.MICRO.SHORT.ENTRY" in line else "?"
                threshold_match = THRESHOLD_RE.search(line)
                threshold = float(threshold_match.group(1)) if threshold_match else None
                summary.micro_probs[(symbol, side)].append((float(prob_match.group(1)), threshold))

            if current and current in summary.symbols:
                cur = summary.symbols[current]
                if "Total trades:" in line:
                    cur.trades = line.rsplit(":", 1)[-1].strip()
                elif "Total PnL:" in line:
                    cur.pnl = line.rsplit(":", 1)[-1].strip()
                elif "arms_total:" in line:
                    cur.arms_total = line.rsplit(":", 1)[-1].strip()
                elif "arms_long:" in line:
                    cur.arms_long = line.rsplit(":", 1)[-1].strip()
                elif "arms_short:" in line:
                    cur.arms_short = line.rsplit(":", 1)[-1].strip()
                elif "arm_confirmations:" in line:
                    cur.confirmations = line.rsplit(":", 1)[-1].strip()
                elif "arm_expirations:" in line:
                    cur.expirations = line.rsplit(":", 1)[-1].strip()

            if "BACKTEST.NO_TRADE_DIAG" in line and "long_entry" in line:
                data = parse_kv(line)
                row.long_max_prob = data.get("max_prob", "")
                row.long_max_margin = data.get("max_margin", "")
            elif "BACKTEST.NO_TRADE_DIAG" in line and "short_entry" in line:
                data = parse_kv(line)
                row.short_max_prob = data.get("max_prob", "")
                row.short_max_margin = data.get("max_margin", "")
            elif "closest_setup rank=1" in line:
                data = parse_kv(line)
                row.closest = f"{data.get('side', '?')} p={data.get('prob', '?')} thr={data.get('threshold', '?')} m={data.get('margin', '?')}"

    return summary


def print_summary(summary: LogSummary) -> None:
    print(f"\n===== {summary.label} =====")
    print(summary.path)
    print(f"{'sym':<6} {'trades':>6} {'pnl':>12} {'prev':>5} {'orders':>6} {'base':>6} {'arms':>5} {'conf':>5} {'exp':>5} {'microP':>7} {'microF':>7} {'longMax':>8} {'longM':>8} {'shortMax':>8} {'shortM':>8} closest")
    for symbol in sorted(summary.symbols):
        row = summary.symbols[symbol]
        print(
            f"{symbol:<6} {row.trades:>6} {row.pnl:>12} {row.previous_close:>5} {row.orders:>6} {row.base_passes:>6} "
            f"{row.arms_total:>5} {row.confirmations:>5} {row.expirations:>5} {row.micro_passes:>7} {row.micro_fails:>7} "
            f"{row.long_max_prob:>8} {row.long_max_margin:>8} {row.short_max_prob:>8} {row.short_max_margin:>8} {row.closest}"
        )
        if row.errors:
            print(f"  errors[{symbol}]={len(row.errors)} latest={row.errors[-1]}")
    print(summary.completed_line)


def print_micro_distribution(summary: LogSummary) -> None:
    if not summary.micro_probs:
        return
    print(f"\n===== {summary.label} MICRO PROBABILITY DISTRIBUTION =====")
    print(f"{'symbol':<6} {'side':<6} {'n':>6} {'maxProb':>9} {'p95':>9} {'p50':>9} {'threshold':>9} {'maxGap':>9}")
    for key in sorted(summary.micro_probs):
        values = sorted(summary.micro_probs[key])
        probs = [value for value, _ in values]
        thresholds = [threshold for _, threshold in values if threshold is not None]
        threshold = thresholds[-1] if thresholds else float("nan")
        max_prob = probs[-1]
        print(
            f"{key[0]:<6} {key[1]:<6} {len(probs):>6} {max_prob:>9.4f} "
            f"{percentile(probs, .95):>9.4f} {median(probs):>9.4f} {threshold:>9.4f} {max_prob - threshold:>9.4f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize Databento calibration backtest logs.")
    parser.add_argument("logs", nargs="*", help="Log files or glob patterns. Use LABEL=PATTERN to set a label.")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols to pre-seed/report even if missing from a partial log.")
    args = parser.parse_args()

    requested = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    specs = args.logs or [
        "ENABLED=/tmp/trading-agent-backtests-local-calibration/enabled-*/enabled-*.log",
        "DISABLED=/tmp/trading-agent-backtests-local-calibration/disabled-*/disabled-*.log",
    ]
    any_log = False
    for spec in specs:
        if "=" in spec:
            label, pattern = spec.split("=", 1)
        else:
            label, pattern = Path(spec).stem.upper(), spec
        path = latest(pattern) if any(ch in pattern for ch in "*?[") else pattern
        if not path:
            print(f"\n===== {label} =====\nMISSING {pattern}")
            continue
        any_log = True
        summary = parse_log(label, path, requested)
        print_summary(summary)
        print_micro_distribution(summary)
    return 0 if any_log else 1


if __name__ == "__main__":
    raise SystemExit(main())
