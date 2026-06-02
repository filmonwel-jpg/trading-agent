#!/usr/bin/env python3
"""Analyze the 2026-05-21 IBKR trade export against Databento bot logs/configs."""
from __future__ import annotations

import csv
import gzip
import glob
import re
import statistics
from collections import Counter, defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TRADE_CSV = Path.home() / "Downloads" / "trades.20260521.csv"
LOG_DIR = ROOT / "runtime" / "databento" / "logs"
BOT_DIR = ROOT / "runtime" / "databento" / "bots"
REPORT = ROOT / "runtime" / "databento" / "analysis_20260521_trade_threshold_report.txt"

ROUTER_RE = re.compile(r"thresholdProfile=([A-Z0-9]+) longEntry=([0-9.]+) shortEntry=([0-9.]+) longExit=([0-9.]+) shortExit=([0-9.]+)")
REGIME_RE = re.compile(r"detectedRegime=([A-Z]+).*confidence=([0-9.]+)")
ENTRY_RE = re.compile(r"\[AI\.(LONG|SHORT)\.ENTRY\].*AI_PREDICTS_ENTRY=PASS.*prob=([0-9.]+) threshold=([0-9.]+)")
EXIT_RE = re.compile(r"\[AI\.(LONG|SHORT)\.EXIT\].*AI_PREDICTS_EXIT=PASS.*prob=([0-9.]+) threshold=([0-9.]+)")
ENTRY_FIRE_RE = re.compile(r"\[AI\.(LONG|SHORT)\.ENTRY\].*firing order")
EXIT_FIRE_RE = re.compile(r"\[AI\.(LONG|SHORT)\.EXIT\].*(Taking LONG profits|Covering SHORT)")


def read_ibkr_trades() -> list[dict]:
    rows: list[dict] = []
    with TRADE_CSV.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if not row.get("Symbol") or row.get("Security Type") != "STK":
                continue
            qty = float(row["Quantity"].replace(",", ""))
            px = float(row["Price"].replace(",", ""))
            row["qty"] = qty
            row["px"] = px
            row["notional"] = qty * px
            rows.append(row)
    return rows


def fifo_pnl(rows: list[dict]) -> tuple[float, float]:
    inv: deque[tuple[float, float]] = deque()
    pos = 0.0
    realized = 0.0
    for row in sorted(rows, key=lambda r: r["Time"]):
        qty = row["qty"]
        px = row["px"]
        if row["Action"] == "BOT":
            rem = qty
            while rem > 1e-9 and inv and inv[0][0] < 0:
                q, p0 = inv[0]
                close = min(rem, -q)
                realized += (p0 - px) * close
                q += close
                rem -= close
                if abs(q) < 1e-9:
                    inv.popleft()
                else:
                    inv[0] = (q, p0)
            if rem > 1e-9:
                inv.append((rem, px))
            pos += qty
        else:
            rem = qty
            while rem > 1e-9 and inv and inv[0][0] > 0:
                q, p0 = inv[0]
                close = min(rem, q)
                realized += (px - p0) * close
                q -= close
                rem -= close
                if q < 1e-9:
                    inv.popleft()
                else:
                    inv[0] = (q, p0)
            if rem > 1e-9:
                inv.append((-rem, px))
            pos -= qty
    return realized, pos


def iter_log_lines(symbol: str):
    files = sorted(glob.glob(str(LOG_DIR / f"trading-agent-{symbol}.log.2026-05-21.*.gz")))
    current = LOG_DIR / f"trading-agent-{symbol}.log"
    if current.exists():
        files.append(str(current))
    for fp in files:
        opener = gzip.open if fp.endswith(".gz") else open
        with opener(fp, "rt", encoding="utf-8", errors="replace") as f:
            yield from f


def parse_bot_thresholds(symbol: str) -> dict[str, str]:
    path = BOT_DIR / f"trading-{symbol.lower()}.properties"
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.startswith("trading.ai."):
            out[k] = v
    return out


def analyze_symbol_logs(symbol: str) -> dict:
    current_profile = "UNKNOWN"
    profile_counts: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    entry_counts: Counter[tuple[str, str, float]] = Counter()
    entry_fire_counts: Counter[tuple[str, str]] = Counter()
    exit_counts: Counter[tuple[str, str, float]] = Counter()
    exit_fire_counts: Counter[tuple[str, str]] = Counter()
    margins: defaultdict[str, list[float]] = defaultdict(list)
    regime_conf: defaultdict[str, list[float]] = defaultdict(list)

    for line in iter_log_lines(symbol):
        if match := ROUTER_RE.search(line):
            current_profile = match.group(1)
            profile_counts[current_profile] += 1
            continue
        if match := REGIME_RE.search(line):
            regime = match.group(1)
            regime_counts[regime] += 1
            regime_conf[regime].append(float(match.group(2)))
            continue
        if match := ENTRY_RE.search(line):
            side = match.group(1)
            prob = float(match.group(2))
            threshold = float(match.group(3))
            entry_counts[(current_profile, side, threshold)] += 1
            margins[current_profile].append(prob - threshold)
            continue
        if match := ENTRY_FIRE_RE.search(line):
            entry_fire_counts[(current_profile, match.group(1))] += 1
            continue
        if match := EXIT_RE.search(line):
            side = match.group(1)
            threshold = float(match.group(3))
            exit_counts[(current_profile, side, threshold)] += 1
            continue
        if match := EXIT_FIRE_RE.search(line):
            exit_fire_counts[(current_profile, match.group(1))] += 1
            continue

    return {
        "profile_counts": profile_counts,
        "regime_counts": regime_counts,
        "entry_counts": entry_counts,
        "entry_fire_counts": entry_fire_counts,
        "exit_counts": exit_counts,
        "exit_fire_counts": exit_fire_counts,
        "margins": margins,
        "regime_conf": regime_conf,
    }


def fmt_counter(counter: Counter, limit: int = 8) -> str:
    return "; ".join(f"{k}:{v}" for k, v in counter.most_common(limit)) or "-"


def fmt_entry_counter(counter: Counter[tuple[str, str, float]], limit: int = 8) -> str:
    return "; ".join(f"{p}/{side}@{thr:.2f}:{cnt}" for (p, side, thr), cnt in counter.most_common(limit)) or "-"


def fmt_side_counter(counter: Counter[tuple[str, str]], limit: int = 8) -> str:
    return "; ".join(f"{p}/{side}:{cnt}" for (p, side), cnt in counter.most_common(limit)) or "-"


def main() -> None:
    rows = read_ibkr_trades()
    by_symbol: defaultdict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_symbol[row["Symbol"]].append(row)

    top_symbols = [sym for sym, _ in sorted(by_symbol.items(), key=lambda kv: -len(kv[1]))[:40]]
    lines: list[str] = []
    lines.append("2026-05-21 trade/threshold investigation")
    lines.append(f"IBKR file: {TRADE_CSV}")
    lines.append(f"fills={len(rows)} symbols={len(by_symbol)} gross_notional=${sum(r['notional'] for r in rows):,.0f} time={min(r['Time'] for r in rows)}..{max(r['Time'] for r in rows)}")
    lines.append("")
    lines.append("TOP SYMBOLS")
    lines.append("symbol fills gross_notional buy_fills sell_fills buy_shares sell_shares fifo_pnl end_pos")
    symbol_stats: dict[str, dict] = {}
    for sym in top_symbols:
        rs = by_symbol[sym]
        buys = [r for r in rs if r["Action"] == "BOT"]
        sells = [r for r in rs if r["Action"] == "SLD"]
        pnl, end_pos = fifo_pnl(rs)
        gross = sum(r["notional"] for r in rs)
        symbol_stats[sym] = {"fills": len(rs), "gross": gross, "pnl": pnl, "end_pos": end_pos}
        lines.append(f"{sym:5} {len(rs):5d} ${gross:11,.0f} {len(buys):9d} {len(sells):10d} {sum(r['qty'] for r in buys):10.0f} {sum(r['qty'] for r in sells):11.0f} ${pnl:8.2f} {end_pos:7.0f}")

    lines.append("")
    lines.append("LOG/REGIME SUMMARY FOR TOP SYMBOLS")
    lines.append("symbol fills entryPass entryFired exitPass exitFired profiles entryPassByProfileSide entryFiredByProfileSide medianEntryMargin")
    log_summaries = {}
    for sym in top_symbols:
        summary = analyze_symbol_logs(sym)
        log_summaries[sym] = summary
        median_margins = "; ".join(f"{profile}:{statistics.median(vals):.3f}" for profile, vals in sorted(summary["margins"].items()) if vals) or "-"
        lines.append(
            f"{sym:5} {symbol_stats[sym]['fills']:5d} "
            f"{sum(summary['entry_counts'].values()):9d} {sum(summary['entry_fire_counts'].values()):10d} "
            f"{sum(summary['exit_counts'].values()):8d} {sum(summary['exit_fire_counts'].values()):9d} "
            f"{fmt_counter(summary['profile_counts'], 5)} | "
            f"{fmt_entry_counter(summary['entry_counts'], 8)} | "
            f"{fmt_side_counter(summary['entry_fire_counts'], 8)} | "
            f"{median_margins}"
        )

    lines.append("")
    lines.append("CURRENT BOT THRESHOLDS FOR TOP 15 BY FILLS")
    for sym in top_symbols[:15]:
        thresholds = parse_bot_thresholds(sym)
        interesting = [
            "trading.ai.long-entry-threshold", "trading.ai.short-entry-threshold",
            "trading.ai.open30.long-entry-threshold", "trading.ai.open30.short-entry-threshold",
            "trading.ai.regime.choppy.long-entry-threshold", "trading.ai.regime.choppy.short-entry-threshold",
            "trading.ai.regime.trend.long-entry-threshold", "trading.ai.regime.trend.short-entry-threshold",
            "trading.ai.regime.volatile.long-entry-threshold", "trading.ai.regime.volatile.short-entry-threshold",
        ]
        pairs = " ".join(f"{k.split('trading.ai.')[-1]}={thresholds.get(k, '-')}" for k in interesting)
        lines.append(f"{sym}: {pairs}")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(lines) + "\n")
    print(REPORT)
    print("\n".join(lines[:80]))


if __name__ == "__main__":
    main()

