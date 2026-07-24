#!/usr/bin/env python3
"""Analyze stock price movement direction/range and compare it with replay PnL.

This script streams Databento NDJSON/NDJSON.GZ equity bars, summarizes each
symbol/day and symbol/month, labels movement as up-trend/down-trend/choppy/etc.,
and joins optional trade lifecycle PnL so profitable and weak periods can be
checked against price hiking, continuous lowering, or high-range choppiness.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

PRICE_FIELD_ALIASES = {
    "open": ("open", "Open", "barOpen", "BarOpen"),
    "high": ("high", "High", "barHigh", "BarHigh"),
    "low": ("low", "Low", "barLow", "BarLow"),
    "close": ("close", "Close", "barClose", "BarClose"),
}
EPOCH_FIELDS = ("barEpochSec", "epochSec", "tsEventSec", "tsEventNs", "EventTsEventNs", "event_ts_event_ns")


@dataclass
class SymbolDayStats:
    symbol: str
    date: str
    first_epoch: int | None = None
    last_epoch: int | None = None
    open: float | None = None
    close: float | None = None
    high: float | None = None
    low: float | None = None
    bar_count: int = 0
    up_bars: int = 0
    down_bars: int = 0
    flat_bars: int = 0
    max_consecutive_up: int = 0
    max_consecutive_down: int = 0
    _current_up: int = field(default=0, repr=False)
    _current_down: int = field(default=0, repr=False)
    _last_close: float | None = field(default=None, repr=False)
    _sum_log_return_sq: float = field(default=0.0, repr=False)

    def update(self, epoch: int, open_px: float, high_px: float, low_px: float, close_px: float) -> None:
        self.bar_count += 1
        if self.first_epoch is None or epoch < self.first_epoch:
            self.first_epoch = epoch
            self.open = open_px
        if self.last_epoch is None or epoch >= self.last_epoch:
            self.last_epoch = epoch
            self.close = close_px
        self.high = high_px if self.high is None else max(self.high, high_px)
        self.low = low_px if self.low is None else min(self.low, low_px)

        if self._last_close is not None and self._last_close > 0 and close_px > 0:
            if close_px > self._last_close:
                self.up_bars += 1
                self._current_up += 1
                self._current_down = 0
            elif close_px < self._last_close:
                self.down_bars += 1
                self._current_down += 1
                self._current_up = 0
            else:
                self.flat_bars += 1
                self._current_up = 0
                self._current_down = 0
            self.max_consecutive_up = max(self.max_consecutive_up, self._current_up)
            self.max_consecutive_down = max(self.max_consecutive_down, self._current_down)
            log_ret = math.log(close_px / self._last_close)
            if math.isfinite(log_ret):
                self._sum_log_return_sq += log_ret * log_ret
        self._last_close = close_px

    def to_row(self, previous_close: float | None, args: argparse.Namespace) -> dict[str, Any]:
        open_px = self.open or 0.0
        close_px = self.close or 0.0
        high_px = self.high or 0.0
        low_px = self.low or 0.0
        oc_return = safe_div(close_px - open_px, open_px)
        range_pct = safe_div(high_px - low_px, open_px)
        close_to_prev_return = safe_div(close_px - previous_close, previous_close) if previous_close else None
        trend_efficiency = safe_div(abs(close_px - open_px), high_px - low_px)
        directional_bars = self.up_bars + self.down_bars
        up_bar_ratio = safe_div(self.up_bars, directional_bars)
        down_bar_ratio = safe_div(self.down_bars, directional_bars)
        realized_vol = math.sqrt(self._sum_log_return_sq) if self._sum_log_return_sq >= 0 else 0.0
        label = classify_movement(
            oc_return,
            range_pct,
            trend_efficiency,
            up_bar_ratio,
            down_bar_ratio,
            args.trend_return_threshold,
            args.large_range_threshold,
            args.quiet_return_threshold,
            args.quiet_range_threshold,
            args.trend_efficiency_threshold,
            args.directional_bar_ratio_threshold,
        )
        return {
            "Symbol": self.symbol,
            "Date": self.date,
            "Open": round(open_px, 8),
            "High": round(high_px, 8),
            "Low": round(low_px, 8),
            "Close": round(close_px, 8),
            "BarCount": self.bar_count,
            "OpenCloseReturnPct": round(100.0 * oc_return, 6),
            "RangePct": round(100.0 * range_pct, 6),
            "CloseToPrevCloseReturnPct": round(100.0 * close_to_prev_return, 6) if close_to_prev_return is not None else "",
            "TrendEfficiency": round(trend_efficiency, 6),
            "UpBarRatio": round(up_bar_ratio, 6),
            "DownBarRatio": round(down_bar_ratio, 6),
            "MaxConsecutiveUpBars": self.max_consecutive_up,
            "MaxConsecutiveDownBars": self.max_consecutive_down,
            "IntradayRealizedVolPct": round(100.0 * realized_vol, 6),
            "MovementLabel": label,
        }


@dataclass
class MonthStats:
    symbol: str
    month: str
    first_date: str | None = None
    last_date: str | None = None
    open: float | None = None
    close: float | None = None
    high: float | None = None
    low: float | None = None
    days: int = 0
    daily_return_sum: float = 0.0
    daily_range_sum: float = 0.0
    daily_eff_sum: float = 0.0
    label_counts: dict[str, int] = field(default_factory=dict)

    def update(self, day_row: dict[str, Any]) -> None:
        date = str(day_row["Date"])
        open_px = float(day_row["Open"])
        close_px = float(day_row["Close"])
        high_px = float(day_row["High"])
        low_px = float(day_row["Low"])
        if self.first_date is None or date < self.first_date:
            self.first_date = date
            self.open = open_px
        if self.last_date is None or date >= self.last_date:
            self.last_date = date
            self.close = close_px
        self.high = high_px if self.high is None else max(self.high, high_px)
        self.low = low_px if self.low is None else min(self.low, low_px)
        self.days += 1
        self.daily_return_sum += float(day_row["OpenCloseReturnPct"]) / 100.0
        self.daily_range_sum += float(day_row["RangePct"]) / 100.0
        self.daily_eff_sum += float(day_row["TrendEfficiency"])
        label = str(day_row["MovementLabel"])
        self.label_counts[label] = self.label_counts.get(label, 0) + 1

    def to_row(self, args: argparse.Namespace) -> dict[str, Any]:
        open_px = self.open or 0.0
        close_px = self.close or 0.0
        high_px = self.high or 0.0
        low_px = self.low or 0.0
        month_return = safe_div(close_px - open_px, open_px)
        range_pct = safe_div(high_px - low_px, open_px)
        avg_daily_return = self.daily_return_sum / self.days if self.days else 0.0
        avg_daily_range = self.daily_range_sum / self.days if self.days else 0.0
        avg_daily_efficiency = self.daily_eff_sum / self.days if self.days else 0.0
        dominant_label = max(self.label_counts.items(), key=lambda kv: kv[1])[0] if self.label_counts else "UNKNOWN"
        up_trend_days = self.label_counts.get("UP_TREND", 0) + self.label_counts.get("VOLATILE_UP", 0) + self.label_counts.get("MILD_UP", 0)
        down_trend_days = self.label_counts.get("DOWN_TREND", 0) + self.label_counts.get("VOLATILE_DOWN", 0) + self.label_counts.get("MILD_DOWN", 0)
        label = classify_month(month_return, range_pct, up_trend_days, down_trend_days, self.days, args)
        return {
            "Symbol": self.symbol,
            "Month": self.month,
            "Days": self.days,
            "Open": round(open_px, 8),
            "High": round(high_px, 8),
            "Low": round(low_px, 8),
            "Close": round(close_px, 8),
            "MonthOpenCloseReturnPct": round(100.0 * month_return, 6),
            "MonthRangePct": round(100.0 * range_pct, 6),
            "AvgDailyOpenCloseReturnPct": round(100.0 * avg_daily_return, 6),
            "AvgDailyRangePct": round(100.0 * avg_daily_range, 6),
            "AvgDailyTrendEfficiency": round(avg_daily_efficiency, 6),
            "UpOrPositiveDays": up_trend_days,
            "DownOrNegativeDays": down_trend_days,
            "DominantDailyMovementLabel": dominant_label,
            "DailyMovementLabelCounts": json.dumps(dict(sorted(self.label_counts.items())), sort_keys=True),
            "MonthMovementLabel": label,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, type=Path, help="Recorded NDJSON/NDJSON.GZ replay containing equity_bar events.")
    parser.add_argument("--trade-output-dir", type=Path, default=None, help="Optional trade replay output directory with lifecycle summary CSVs.")
    parser.add_argument("--out-prefix", required=True, type=Path, help="Output prefix for CSV/JSON/Markdown reports.")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols to include.")
    parser.add_argument("--trend-return-threshold", type=float, default=0.01, help="Daily/monthly open-close return threshold for up/down trend labels.")
    parser.add_argument("--large-range-threshold", type=float, default=0.02, help="High-low range threshold for volatile/choppy labels.")
    parser.add_argument("--quiet-return-threshold", type=float, default=0.0025, help="Abs open-close return threshold for quiet labels.")
    parser.add_argument("--quiet-range-threshold", type=float, default=0.0075, help="High-low range threshold for quiet labels.")
    parser.add_argument("--trend-efficiency-threshold", type=float, default=0.55, help="Abs(open-close)/(high-low) threshold for continuous movement labels.")
    parser.add_argument("--directional-bar-ratio-threshold", type=float, default=0.55, help="Up/down bar ratio threshold for continuous movement labels.")
    parser.add_argument("--top-periods", type=int, default=5, help="Best/worst months/days to highlight.")
    parser.add_argument("--title", default="Price movement vs PnL analysis")
    return parser.parse_args()


def open_text(path: Path, mode: str):
    if path.name.endswith(".gz"):
        return gzip.open(path, mode, encoding="utf-8")
    return path.open(mode, encoding="utf-8")


def safe_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def first_float(payload: dict[str, Any], names: tuple[str, ...]) -> float | None:
    for name in names:
        parsed = safe_float(payload.get(name))
        if parsed is not None:
            return parsed
    return None


def event_epoch(payload: dict[str, Any]) -> int | None:
    for key in EPOCH_FIELDS:
        value = payload.get(key)
        if value is None or value == "":
            continue
        try:
            numeric = int(float(value))
        except (TypeError, ValueError):
            continue
        if key.endswith("Ns") or key.endswith("_ns"):
            return numeric // 1_000_000_000
        return numeric
    return None


def safe_div(numerator: float | int | None, denominator: float | int | None) -> float:
    try:
        n = float(numerator if numerator is not None else 0.0)
        d = float(denominator if denominator is not None else 0.0)
        if d == 0 or not math.isfinite(n) or not math.isfinite(d):
            return 0.0
        return n / d
    except (TypeError, ValueError):
        return 0.0


def classify_movement(
    oc_return: float,
    range_pct: float,
    trend_efficiency: float,
    up_bar_ratio: float,
    down_bar_ratio: float,
    trend_threshold: float,
    large_range_threshold: float,
    quiet_return_threshold: float,
    quiet_range_threshold: float,
    efficiency_threshold: float,
    directional_ratio_threshold: float,
) -> str:
    if (
        oc_return >= trend_threshold
        and trend_efficiency >= efficiency_threshold
        and up_bar_ratio >= directional_ratio_threshold
    ):
        return "UP_TREND"
    if (
        oc_return <= -trend_threshold
        and trend_efficiency >= efficiency_threshold
        and down_bar_ratio >= directional_ratio_threshold
    ):
        return "DOWN_TREND"
    if range_pct >= large_range_threshold and oc_return >= trend_threshold:
        return "VOLATILE_UP"
    if range_pct >= large_range_threshold and oc_return <= -trend_threshold:
        return "VOLATILE_DOWN"
    if range_pct >= large_range_threshold:
        return "CHOPPY_HIGH_RANGE"
    if abs(oc_return) <= quiet_return_threshold and range_pct <= quiet_range_threshold:
        return "QUIET_SIDEWAYS"
    if oc_return > 0:
        return "MILD_UP"
    if oc_return < 0:
        return "MILD_DOWN"
    return "FLAT"


def classify_month(month_return: float, range_pct: float, up_days: int, down_days: int, days: int, args: argparse.Namespace) -> str:
    up_ratio = up_days / days if days else 0.0
    down_ratio = down_days / days if days else 0.0
    if month_return >= args.trend_return_threshold and up_ratio >= 0.55:
        return "MONTH_UP_TREND"
    if month_return <= -args.trend_return_threshold and down_ratio >= 0.55:
        return "MONTH_DOWN_TREND"
    if range_pct >= args.large_range_threshold and month_return > 0:
        return "MONTH_VOLATILE_UP"
    if range_pct >= args.large_range_threshold and month_return < 0:
        return "MONTH_VOLATILE_DOWN"
    if range_pct >= args.large_range_threshold:
        return "MONTH_CHOPPY_HIGH_RANGE"
    if abs(month_return) <= args.quiet_return_threshold and range_pct <= args.quiet_range_threshold:
        return "MONTH_QUIET_SIDEWAYS"
    return "MONTH_MILD_UP" if month_return > 0 else "MONTH_MILD_DOWN"


def parse_equity_bars(events: Path, symbols: set[str], args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    stats: dict[tuple[str, str], SymbolDayStats] = {}
    counters = {"lines": 0, "equity_bar": 0, "kept_equity_bar": 0, "malformed": 0, "missing_fields": 0}
    with open_text(events, "rt") as handle:
        for raw in handle:
            counters["lines"] += 1
            raw = raw.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                counters["malformed"] += 1
                continue
            if not isinstance(payload, dict) or str(payload.get("event", "")).lower() != "equity_bar":
                continue
            counters["equity_bar"] += 1
            symbol = str(payload.get("symbol") or payload.get("underlying") or "").strip().upper()
            if not symbol or (symbols and symbol not in symbols):
                continue
            epoch = event_epoch(payload)
            open_px = first_float(payload, PRICE_FIELD_ALIASES["open"])
            high_px = first_float(payload, PRICE_FIELD_ALIASES["high"])
            low_px = first_float(payload, PRICE_FIELD_ALIASES["low"])
            close_px = first_float(payload, PRICE_FIELD_ALIASES["close"])
            if epoch is None or open_px is None or high_px is None or low_px is None or close_px is None:
                counters["missing_fields"] += 1
                continue
            day = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(NY).date().isoformat()
            key = (symbol, day)
            if key not in stats:
                stats[key] = SymbolDayStats(symbol=symbol, date=day)
            stats[key].update(epoch, open_px, high_px, low_px, close_px)
            counters["kept_equity_bar"] += 1

    rows: list[dict[str, Any]] = []
    previous_close_by_symbol: dict[str, float] = {}
    for (symbol, day), stat in sorted(stats.items()):
        row = stat.to_row(previous_close_by_symbol.get(symbol), args)
        rows.append(row)
        previous_close_by_symbol[symbol] = float(row["Close"])
    counters["daily_symbol_rows"] = len(rows)
    return rows, counters


def build_month_rows(day_rows: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    stats: dict[tuple[str, str], MonthStats] = {}
    for row in day_rows:
        symbol = str(row["Symbol"])
        month = str(row["Date"])[:7]
        key = (symbol, month)
        if key not in stats:
            stats[key] = MonthStats(symbol=symbol, month=month)
        stats[key].update(row)
    return [stat.to_row(args) for _, stat in sorted(stats.items())]


def parse_time(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(NY)
    except ValueError:
        return None


def read_trade_pnl(trade_output_dir: Path | None) -> tuple[dict[tuple[str, str], dict[str, float]], dict[tuple[str, str], dict[str, float]], dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    by_symbol_day: dict[tuple[str, str], dict[str, float]] = {}
    by_symbol_month: dict[tuple[str, str], dict[str, float]] = {}
    by_day: dict[str, dict[str, float]] = {}
    by_month: dict[str, dict[str, float]] = {}
    if trade_output_dir is None or not trade_output_dir.is_dir():
        return by_symbol_day, by_symbol_month, by_day, by_month

    def add(target: dict[Any, dict[str, float]], key: Any, pnl: float, realized_r: float) -> None:
        row = target.setdefault(key, {"Trades": 0.0, "Wins": 0.0, "Losses": 0.0, "PnL": 0.0, "RealizedR": 0.0})
        row["Trades"] += 1.0
        row["Wins"] += 1.0 if pnl > 0 else 0.0
        row["Losses"] += 1.0 if pnl < 0 else 0.0
        row["PnL"] += pnl
        row["RealizedR"] += realized_r

    for path in sorted(trade_output_dir.glob("*-trade-lifecycle-summary.csv")):
        if path.stat().st_size <= 0:
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                pnl = safe_float(row.get("TradePnL"))
                if pnl is None:
                    continue
                realized_r = safe_float(row.get("RealizedR")) or 0.0
                symbol = str(row.get("Symbol") or "").strip().upper()
                entry_time = parse_time(str(row.get("EntryTime") or row.get("SetupArmTime") or ""))
                if not symbol or entry_time is None:
                    continue
                day = entry_time.date().isoformat()
                month = day[:7]
                add(by_symbol_day, (symbol, day), pnl, realized_r)
                add(by_symbol_month, (symbol, month), pnl, realized_r)
                add(by_day, day, pnl, realized_r)
                add(by_month, month, pnl, realized_r)
    return by_symbol_day, by_symbol_month, by_day, by_month


def pnl_fields(stats: dict[Any, dict[str, float]], key: Any) -> dict[str, Any]:
    row = stats.get(key, {})
    trades = int(row.get("Trades", 0))
    wins = int(row.get("Wins", 0))
    losses = int(row.get("Losses", 0))
    pnl = row.get("PnL", 0.0)
    realized_r = row.get("RealizedR", 0.0)
    return {
        "Trades": trades,
        "Wins": wins,
        "Losses": losses,
        "WinRate": round(wins / trades, 6) if trades else "",
        "PnL": round(pnl, 6),
        "RealizedR": round(realized_r, 6),
        "AvgPnLPerTrade": round(pnl / trades, 6) if trades else "",
    }


def attach_symbol_pnl(day_rows: list[dict[str, Any]], month_rows: list[dict[str, Any]], by_symbol_day, by_symbol_month) -> None:
    for row in day_rows:
        row.update(pnl_fields(by_symbol_day, (row["Symbol"], row["Date"])))
    for row in month_rows:
        row.update(pnl_fields(by_symbol_month, (row["Symbol"], row["Month"])))


def build_period_summary(symbol_rows: list[dict[str, Any]], period_col: str, pnl_by_period: dict[str, dict[str, float]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in symbol_rows:
        grouped.setdefault(str(row[period_col]), []).append(row)
    out: list[dict[str, Any]] = []
    for period, rows in sorted(grouped.items()):
        label_counts: dict[str, int] = {}
        for row in rows:
            label_col = "MonthMovementLabel" if period_col == "Month" else "MovementLabel"
            label = str(row[label_col])
            label_counts[label] = label_counts.get(label, 0) + 1
        return_col = "MonthOpenCloseReturnPct" if period_col == "Month" else "OpenCloseReturnPct"
        range_col = "MonthRangePct" if period_col == "Month" else "RangePct"
        eff_col = "AvgDailyTrendEfficiency" if period_col == "Month" else "TrendEfficiency"
        summary = {
            "Period": period,
            "SymbolRows": len(rows),
            "AvgSymbolReturnPct": round(sum(float(row[return_col]) for row in rows) / len(rows), 6),
            "AvgSymbolRangePct": round(sum(float(row[range_col]) for row in rows) / len(rows), 6),
            "AvgTrendEfficiency": round(sum(float(row[eff_col]) for row in rows) / len(rows), 6),
            "MovementLabelCounts": json.dumps(dict(sorted(label_counts.items())), sort_keys=True),
        }
        summary.update(pnl_fields(pnl_by_period, period))
        out.append(summary)
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "No rows."
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(col, "")).replace("|", r"\|") for col in columns) + " |")
    return "\n".join(lines)


def write_markdown(path: Path, title: str, report: dict[str, Any], month_summary: list[dict[str, Any]], day_summary: list[dict[str, Any]]) -> None:
    best_months = sorted(month_summary, key=lambda row: float(row.get("PnL", 0.0)), reverse=True)[:5]
    worst_months = sorted(month_summary, key=lambda row: float(row.get("PnL", 0.0)))[:5]
    best_days = sorted(day_summary, key=lambda row: float(row.get("PnL", 0.0)), reverse=True)[:5]
    worst_days = sorted(day_summary, key=lambda row: float(row.get("PnL", 0.0)))[:5]
    month_cols = ["Period", "Trades", "PnL", "AvgSymbolReturnPct", "AvgSymbolRangePct", "AvgTrendEfficiency", "MovementLabelCounts"]
    lines = [
        f"# {title}",
        "",
        f"- Events: `{report['events']}`",
        f"- Trade output dir: `{report.get('trade_output_dir', '')}`",
        f"- Equity bars kept: `{report['counters']['kept_equity_bar']}`",
        f"- Symbol/day rows: `{report['counters']['daily_symbol_rows']}`",
        "",
        "## Best months by PnL and movement",
        "",
        markdown_table(best_months, month_cols),
        "",
        "## Worst months by PnL and movement",
        "",
        markdown_table(worst_months, month_cols),
        "",
        "## Best days by PnL and movement",
        "",
        markdown_table(best_days, month_cols),
        "",
        "## Worst days by PnL and movement",
        "",
        markdown_table(worst_days, month_cols),
        "",
        "## Label definitions",
        "",
        "- `UP_TREND`: directional open-to-close rise with high trend efficiency and more up bars.",
        "- `DOWN_TREND`: directional open-to-close decline with high trend efficiency and more down bars.",
        "- `VOLATILE_UP` / `VOLATILE_DOWN`: large high-low range with positive/negative open-close direction but less continuous behavior.",
        "- `CHOPPY_HIGH_RANGE`: large high-low range without clean direction.",
        "- `MILD_UP` / `MILD_DOWN`: smaller directional movement.",
        "- `QUIET_SIDEWAYS`: low return and low range.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    symbols = {part.strip().upper() for part in args.symbols.split(",") if part.strip()} if args.symbols else set()
    day_rows, counters = parse_equity_bars(args.events, symbols, args)
    month_rows = build_month_rows(day_rows, args)
    by_symbol_day, by_symbol_month, by_day, by_month = read_trade_pnl(args.trade_output_dir)
    attach_symbol_pnl(day_rows, month_rows, by_symbol_day, by_symbol_month)
    day_summary = build_period_summary(day_rows, "Date", by_day)
    month_summary = build_period_summary(month_rows, "Month", by_month)

    out_prefix = args.out_prefix.expanduser().resolve()
    outputs = {
        "daily_symbol_movement": out_prefix.with_name(f"{out_prefix.name}_daily_symbol_movement.csv"),
        "monthly_symbol_movement": out_prefix.with_name(f"{out_prefix.name}_monthly_symbol_movement.csv"),
        "daily_movement_pnl": out_prefix.with_name(f"{out_prefix.name}_daily_movement_pnl.csv"),
        "monthly_movement_pnl": out_prefix.with_name(f"{out_prefix.name}_monthly_movement_pnl.csv"),
    }
    write_csv(outputs["daily_symbol_movement"], day_rows)
    write_csv(outputs["monthly_symbol_movement"], month_rows)
    write_csv(outputs["daily_movement_pnl"], day_summary)
    write_csv(outputs["monthly_movement_pnl"], month_summary)

    report = {
        "events": str(args.events),
        "trade_output_dir": str(args.trade_output_dir) if args.trade_output_dir else "",
        "out_prefix": str(out_prefix),
        "counters": counters,
        "outputs": {name: str(path) for name, path in outputs.items()},
        "thresholds": {
            "trend_return_threshold": args.trend_return_threshold,
            "large_range_threshold": args.large_range_threshold,
            "quiet_return_threshold": args.quiet_return_threshold,
            "quiet_range_threshold": args.quiet_range_threshold,
            "trend_efficiency_threshold": args.trend_efficiency_threshold,
            "directional_bar_ratio_threshold": args.directional_bar_ratio_threshold,
        },
    }
    json_path = out_prefix.with_suffix(".json")
    md_path = out_prefix.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(md_path, args.title, report, month_summary, day_summary)

    print(f"PRICE_MOVEMENT_VS_PNL json={json_path} markdown={md_path}")
    print(f"PRICE_MOVEMENT_VS_PNL daily_symbol_rows={len(day_rows)} monthly_symbol_rows={len(month_rows)} equity_bars={counters['kept_equity_bar']}")
    if month_summary:
        print("PRICE_MOVEMENT_VS_PNL_BEST_MONTHS")
        for row in sorted(month_summary, key=lambda item: float(item.get("PnL", 0.0)), reverse=True)[: args.top_periods]:
            print(row)
        print("PRICE_MOVEMENT_VS_PNL_WORST_MONTHS")
        for row in sorted(month_summary, key=lambda item: float(item.get("PnL", 0.0)))[: args.top_periods]:
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

