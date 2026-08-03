#!/usr/bin/env python3
"""Calculate PnL from today's Databento core-5 six-week output artifacts."""

from __future__ import annotations

import csv
import datetime as dt
import json
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

EXT = Path("/Volumes/DatabentoVault/trading-agent-offload/databento/runtime/backtests")
WEEKS = [
    ("w01", "2025-12-23", "2025-12-26", "databento_api_core5_w01_20251223_to_20251226", "20260624_145037", True),
    ("w02", "2025-12-29", "2026-01-02", "databento_api_core5_w02_20251229_to_20260102", "20260624_151156", True),
    ("w03", "2026-01-05", "2026-01-09", "databento_api_core5_w03_20260105_to_20260109", "20260624_152401", True),
    ("w04", "2026-01-12", "2026-01-16", "databento_api_core5_w04_20260112_to_20260116", "20260624_153833", True),
    ("w05", "2026-01-19", "2026-01-23", "databento_api_core5_w05_20260119_to_20260123", "20260624_155306", True),
    ("w06", "2026-01-26", "2026-01-30", "databento_api_core5_w06_20260126_to_20260130", "20260624_161036", False),
]
SYMBOLS = ["TSLA", "TQQQ", "NVDA", "SPY", "QQQ"]


def money(value: Decimal) -> str:
    return f"${value:,.2f}"


def dec(raw: str | None) -> Decimal:
    if raw is None or raw.strip() == "":
        return Decimal("0")
    try:
        return Decimal(raw.strip())
    except InvalidOperation:
        return Decimal("0")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh))


def trade_rows_from_lifecycle(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in read_csv_rows(path):
        pnl = dec(row.get("TradePnL"))
        closed_idx = (row.get("ClosedTradeIndex") or "").strip()
        # No-trade lifecycle files have one diagnostic row with empty trade fields and TradePnL blank.
        if not closed_idx and pnl == 0 and not (row.get("EntryTime") or "").strip():
            continue
        rows.append({
            "symbol": row.get("Symbol", ""),
            "side": row.get("TradeSide", ""),
            "entry_time": row.get("EntryTime", ""),
            "exit_time": row.get("ExitTime", ""),
            "quantity": row.get("Quantity", ""),
            "entry_price": row.get("EntryPrice", ""),
            "exit_price": row.get("ExitPrice", ""),
            "exit_reason": row.get("ExitReason", ""),
            "pnl": pnl,
            "source": path.name,
        })
    return rows


def trade_rows_from_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in read_csv_rows(path):
        rows.append({
            "symbol": row.get("Symbol", ""),
            "timestamp": row.get("Timestamp", ""),
            "action": row.get("Action", ""),
            "quantity": row.get("Quantity", ""),
            "entry_price": row.get("EntryPrice", ""),
            "exit_price": row.get("ExitPrice", ""),
            "pnl": dec(row.get("TradePnL")),
            "cumulative_pnl": dec(row.get("CumulativePnL")),
            "source": path.name,
        })
    return rows


def summarize() -> dict[str, Any]:
    week_results: list[dict[str, Any]] = []
    total_completed = Decimal("0")
    total_available = Decimal("0")
    all_trades: list[dict[str, Any]] = []
    cross_checks: list[dict[str, Any]] = []

    for week, start, end, folder, stamp, complete in WEEKS:
        output_dir = EXT / folder
        lifecycle_files = sorted(output_dir.glob(f"*-{stamp}-trade-lifecycle-summary.csv"))
        trade_files = sorted(output_dir.glob(f"*-{stamp}-trades.csv"))
        week_trades: list[dict[str, Any]] = []
        symbol_pnl: dict[str, Decimal] = {symbol: Decimal("0") for symbol in SYMBOLS}
        symbol_counts: dict[str, int] = {symbol: 0 for symbol in SYMBOLS}
        missing_lifecycle = [symbol for symbol in SYMBOLS if not any(path.name.startswith(symbol + "-") for path in lifecycle_files)]

        for lifecycle_path in lifecycle_files:
            rows = trade_rows_from_lifecycle(lifecycle_path)
            for item in rows:
                item.update({"week": week, "start": start, "end": end, "complete_week": complete})
                week_trades.append(item)
                all_trades.append(item)
                symbol = item["symbol"]
                if symbol in symbol_pnl:
                    symbol_pnl[symbol] += item["pnl"]
                    symbol_counts[symbol] += 1

        trade_csv_pnl = Decimal("0")
        trade_csv_count = 0
        trade_csv_final_by_symbol: dict[str, Decimal] = {}
        for trade_path in trade_files:
            rows = trade_rows_from_trades(trade_path)
            for row in rows:
                trade_csv_count += 1
                trade_csv_pnl += row["pnl"]
                trade_csv_final_by_symbol[row["symbol"]] = row["cumulative_pnl"]

        lifecycle_pnl = sum((item["pnl"] for item in week_trades), Decimal("0"))
        if complete:
            total_completed += lifecycle_pnl
        total_available += lifecycle_pnl
        cross_checks.append({
            "week": week,
            "lifecycle_pnl": lifecycle_pnl,
            "lifecycle_trades": len(week_trades),
            "trade_csv_pnl": trade_csv_pnl,
            "trade_csv_trades": trade_csv_count,
            "matches": lifecycle_pnl == trade_csv_pnl and len(week_trades) == trade_csv_count,
        })
        week_results.append({
            "week": week,
            "start": start,
            "end": end,
            "stamp": stamp,
            "complete": complete,
            "missing_lifecycle": missing_lifecycle,
            "trade_count": len(week_trades),
            "pnl": lifecycle_pnl,
            "symbol_pnl": symbol_pnl,
            "symbol_counts": symbol_counts,
            "trade_csv_pnl": trade_csv_pnl,
            "trade_csv_count": trade_csv_count,
            "trades": week_trades,
        })

    by_symbol: dict[str, Decimal] = {symbol: Decimal("0") for symbol in SYMBOLS}
    by_symbol_count: dict[str, int] = {symbol: 0 for symbol in SYMBOLS}
    for item in all_trades:
        symbol = item["symbol"]
        if symbol in by_symbol:
            by_symbol[symbol] += item["pnl"]
            by_symbol_count[symbol] += 1

    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "external_root": str(EXT),
        "total_completed_weeks_pnl": total_completed,
        "total_available_pnl": total_available,
        "by_symbol": by_symbol,
        "by_symbol_count": by_symbol_count,
        "weeks": week_results,
        "cross_checks": cross_checks,
        "all_trades": all_trades,
    }


def render_markdown(result: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Core-5 Today PnL")
    lines.append("")
    lines.append(f"Generated: {result['generated_at']}")
    lines.append(f"External root: `{result['external_root']}`")
    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append(f"- Completed weeks only (`w01`-`w05`): **{money(result['total_completed_weeks_pnl'])}**")
    lines.append(f"- All available today output including partial `w06`: **{money(result['total_available_pnl'])}**")
    lines.append("- `w06` is not a complete week because `NVDA` failed/missing lifecycle output; treat the all-available total as provisional.")
    lines.append("")
    lines.append("## PnL by week")
    lines.append("")
    lines.append("| Week | Window | Complete? | Trades | PnL | Missing lifecycle | Trade CSV cross-check |")
    lines.append("|---|---:|---|---:|---:|---|---|")
    for week in result["weeks"]:
        check = next(item for item in result["cross_checks"] if item["week"] == week["week"])
        lines.append(
            f"| {week['week']} | {week['start']}..{week['end']} | {'yes' if week['complete'] else 'partial'} | {week['trade_count']} | **{money(week['pnl'])}** | {', '.join(week['missing_lifecycle']) or 'none'} | {'match' if check['matches'] else 'MISMATCH'} |"
        )
    lines.append("")
    lines.append("## PnL by symbol")
    lines.append("")
    lines.append("| Symbol | Trades | PnL |")
    lines.append("|---|---:|---:|")
    for symbol in SYMBOLS:
        lines.append(f"| {symbol} | {result['by_symbol_count'][symbol]} | **{money(result['by_symbol'][symbol])}** |")
    lines.append("")
    lines.append("## Individual trades")
    lines.append("")
    lines.append("| Week | Symbol | Side | Entry | Exit | Qty | EntryPx | ExitPx | Exit reason | PnL |")
    lines.append("|---|---|---|---|---|---:|---:|---:|---|---:|")
    for item in result["all_trades"]:
        lines.append(
            f"| {item['week']} | {item['symbol']} | {item['side']} | {item['entry_time']} | {item['exit_time']} | {item['quantity']} | {item['entry_price']} | {item['exit_price']} | {item['exit_reason']} | **{money(item['pnl'])}** |"
        )
    lines.append("")
    lines.append("## Cross-check")
    lines.append("")
    lines.append("Lifecycle `TradePnL` sums were cross-checked against today `trades.csv` `TradePnL` sums:")
    lines.append("")
    lines.append("| Week | Lifecycle trades/PnL | Trades CSV trades/PnL | Match |")
    lines.append("|---|---:|---:|---|")
    for check in result["cross_checks"]:
        lines.append(
            f"| {check['week']} | {check['lifecycle_trades']} / {money(check['lifecycle_pnl'])} | {check['trade_csv_trades']} / {money(check['trade_csv_pnl'])} | {'yes' if check['matches'] else 'no'} |"
        )
    lines.append("")
    return "\n".join(lines)


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def main() -> int:
    result = summarize()
    report_path = Path("/Users/FXG06FA/trading-agent-main/runtime/core5_today_pnl_20260624.md")
    json_path = Path("/Users/FXG06FA/trading-agent-main/runtime/core5_today_pnl_20260624.json")
    report = render_markdown(result)
    report_path.write_text(report, encoding="utf-8")
    json_path.write_text(json.dumps(result, indent=2, default=json_default), encoding="utf-8")
    print(report)
    print(f"Wrote PnL report: {report_path}")
    print(f"Wrote PnL JSON: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
