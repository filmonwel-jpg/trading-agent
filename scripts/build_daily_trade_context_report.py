#!/usr/bin/env python3
"""Build a daily trade context report with PnL, symbols, regimes, and price movement.

Inputs:
- trade replay output directory containing `*-trade-lifecycle-summary.csv`
- optional regime-attributed trades CSV from `summarize_trade_regime_attribution.py`
- optional daily movement/PnL CSV from `analyze_price_movement_vs_pnl.py`

Outputs:
- `<out-prefix>.csv`
- `<out-prefix>.md`
- `<out-prefix>.json`
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

NY_TZ = "America/New_York"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-output-dir", required=True, type=Path, help="Directory containing *-trade-lifecycle-summary.csv files.")
    parser.add_argument("--regime-trades-csv", type=Path, default=None, help="Optional *_trades_with_regime.csv from summarize_trade_regime_attribution.py.")
    parser.add_argument("--price-daily-csv", type=Path, default=None, help="Optional *_daily_movement_pnl.csv from analyze_price_movement_vs_pnl.py.")
    parser.add_argument("--out-prefix", required=True, type=Path, help="Output prefix for CSV/Markdown/JSON files.")
    parser.add_argument("--title", default="Daily trade context report")
    parser.add_argument("--top", type=int, default=10, help="Rows to show in console/summary for best/worst days.")
    return parser.parse_args()


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str)) else False:
        return None
    if hasattr(value, "item"):
        return json_safe(value.item())
    return value


def compact_json(mapping: dict[str, Any]) -> str:
    return json.dumps(mapping, sort_keys=True, separators=(",", ":"))


def read_lifecycle(trade_output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(trade_output_dir.glob("*-trade-lifecycle-summary.csv")):
        if path.stat().st_size <= 0:
            continue
        frame = pd.read_csv(path)
        if frame.empty or "TradePnL" not in frame.columns:
            continue
        frame["SourceFile"] = path.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    trades = pd.concat(frames, ignore_index=True)
    trades["TradePnL"] = pd.to_numeric(trades["TradePnL"], errors="coerce")
    trades = trades[trades["TradePnL"].notna()].copy()
    if trades.empty:
        return trades
    for column in ["RealizedR", "MfeR", "MaeR", "Quantity", "EntryPrice", "ExitPrice", "SetupToFillSeconds"]:
        if column in trades.columns:
            trades[column] = pd.to_numeric(trades[column], errors="coerce")
    entry_ts = pd.to_datetime(trades.get("EntryTime"), errors="coerce", utc=True).dt.tz_convert(NY_TZ)
    trades["Date"] = entry_ts.dt.date.astype(str)
    trades["Win"] = trades["TradePnL"] > 0
    trades["Loss"] = trades["TradePnL"] < 0
    trades["Symbol"] = trades["Symbol"].astype(str).str.upper().str.strip()
    return trades


def summarize_daily_lifecycle(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=[
            "Date", "Trades", "Wins", "Losses", "WinRate", "PnL", "RealizedR",
            "AvgPnLPerTrade", "SymbolsTraded", "SymbolTradeCounts", "SymbolPnL",
        ])
    rows: list[dict[str, Any]] = []
    for date, group in trades.groupby("Date", sort=True):
        trades_count = int(len(group))
        wins = int(group["Win"].sum())
        losses = int(group["Loss"].sum())
        symbol_trade_counts = {symbol: int(len(sg)) for symbol, sg in group.groupby("Symbol")}
        symbol_pnl = {symbol: round(float(sg["TradePnL"].sum()), 2) for symbol, sg in group.groupby("Symbol")}
        pnl = float(group["TradePnL"].sum())
        realized_r = float(group["RealizedR"].sum()) if "RealizedR" in group else 0.0
        rows.append({
            "Date": date,
            "Trades": trades_count,
            "Wins": wins,
            "Losses": losses,
            "WinRate": wins / trades_count if trades_count else 0.0,
            "PnL": pnl,
            "RealizedR": realized_r,
            "AvgPnLPerTrade": pnl / trades_count if trades_count else 0.0,
            "SymbolsTraded": ",".join(sorted(symbol_trade_counts)),
            "SymbolTradeCounts": compact_json(symbol_trade_counts),
            "SymbolPnL": compact_json(symbol_pnl),
        })
    return pd.DataFrame(rows)


def summarize_regime(regime_trades_csv: Path | None) -> pd.DataFrame:
    if regime_trades_csv is None or not regime_trades_csv.is_file():
        return pd.DataFrame(columns=["Date", "RegimeTradeCounts", "RegimePnL"])
    df = pd.read_csv(regime_trades_csv)
    if df.empty or "Regime" not in df.columns or "TradePnL" not in df.columns:
        return pd.DataFrame(columns=["Date", "RegimeTradeCounts", "RegimePnL"])
    df["TradePnL"] = pd.to_numeric(df["TradePnL"], errors="coerce")
    df = df[df["TradePnL"].notna()].copy()
    if "EntryDate" in df.columns:
        df["Date"] = df["EntryDate"].astype(str)
    else:
        entry_ts = pd.to_datetime(df.get("EntryTime"), errors="coerce", utc=True).dt.tz_convert(NY_TZ)
        df["Date"] = entry_ts.dt.date.astype(str)
    rows = []
    for date, group in df.groupby("Date", sort=True):
        regime_counts = {regime: int(len(rg)) for regime, rg in group.groupby("Regime")}
        regime_pnl = {regime: round(float(rg["TradePnL"].sum()), 2) for regime, rg in group.groupby("Regime")}
        rows.append({
            "Date": date,
            "RegimeTradeCounts": compact_json(regime_counts),
            "RegimePnL": compact_json(regime_pnl),
        })
    return pd.DataFrame(rows)


def read_price_daily(price_daily_csv: Path | None) -> pd.DataFrame:
    if price_daily_csv is None or not price_daily_csv.is_file():
        return pd.DataFrame(columns=["Date"])
    df = pd.read_csv(price_daily_csv)
    if df.empty:
        return pd.DataFrame(columns=["Date"])
    rename = {
        "Period": "Date",
        "AvgSymbolReturnPct": "AvgSymbolReturnPct",
        "AvgSymbolRangePct": "AvgSymbolRangePct",
        "AvgTrendEfficiency": "AvgTrendEfficiency",
        "MovementLabelCounts": "MovementLabelCounts",
    }
    keep = [col for col in rename if col in df.columns]
    out = df[keep].rename(columns=rename).copy()
    return out


def build_report(args: argparse.Namespace) -> pd.DataFrame:
    trades = read_lifecycle(args.trade_output_dir)
    daily = summarize_daily_lifecycle(trades)
    regimes = summarize_regime(args.regime_trades_csv)
    movement = read_price_daily(args.price_daily_csv)
    report = daily.merge(regimes, on="Date", how="left").merge(movement, on="Date", how="left")
    for column in ["RegimeTradeCounts", "RegimePnL", "MovementLabelCounts"]:
        if column in report.columns:
            report[column] = report[column].fillna("{}")
    for column in ["AvgSymbolReturnPct", "AvgSymbolRangePct", "AvgTrendEfficiency"]:
        if column in report.columns:
            report[column] = pd.to_numeric(report[column], errors="coerce")
    return report.sort_values("Date").reset_index(drop=True)


def markdown_value(value: Any) -> str:
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str)) else False:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    if hasattr(value, "item"):
        return markdown_value(value.item())
    return str(value).replace("|", r"\|").replace("\n", " ")


def frame_to_markdown(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "No rows."
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(markdown_value(row.get(column, "")) for column in columns) + " |")
    return "\n".join(lines)


def write_markdown(path: Path, title: str, report: pd.DataFrame, args: argparse.Namespace) -> None:
    columns = [
        "Date", "Trades", "PnL", "WinRate", "SymbolsTraded", "SymbolTradeCounts", "SymbolPnL",
        "RegimeTradeCounts", "RegimePnL", "AvgSymbolReturnPct", "AvgSymbolRangePct",
        "AvgTrendEfficiency", "MovementLabelCounts",
    ]
    columns = [column for column in columns if column in report.columns]
    best = report.sort_values("PnL", ascending=False).head(args.top)
    worst = report.sort_values("PnL", ascending=True).head(args.top)
    lines = [
        f"# {title}",
        "",
        f"- Trade output dir: `{args.trade_output_dir}`",
        f"- Regime trades CSV: `{args.regime_trades_csv or ''}`",
        f"- Price daily CSV: `{args.price_daily_csv or ''}`",
        f"- Daily rows: `{len(report)}`",
        f"- Total trades: `{int(report['Trades'].sum()) if 'Trades' in report else 0}`",
        f"- Total PnL: `{float(report['PnL'].sum()):.2f}`" if "PnL" in report else "",
        "",
        "## Best days",
        "",
        frame_to_markdown(best, columns),
        "",
        "## Worst days",
        "",
        frame_to_markdown(worst, columns),
        "",
        "## All days",
        "",
        frame_to_markdown(report, columns),
        "",
    ]
    path.write_text("\n".join(line for line in lines if line is not None) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    out_prefix = args.out_prefix.expanduser().resolve()
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    report = build_report(args)
    csv_path = out_prefix.with_suffix(".csv")
    md_path = out_prefix.with_suffix(".md")
    json_path = out_prefix.with_suffix(".json")
    report.to_csv(csv_path, index=False)
    write_markdown(md_path, args.title, report, args)
    payload = {
        "trade_output_dir": str(args.trade_output_dir),
        "regime_trades_csv": str(args.regime_trades_csv) if args.regime_trades_csv else "",
        "price_daily_csv": str(args.price_daily_csv) if args.price_daily_csv else "",
        "output_csv": str(csv_path),
        "output_markdown": str(md_path),
        "daily_rows": int(len(report)),
        "total_trades": int(report["Trades"].sum()) if "Trades" in report else 0,
        "total_pnl": float(report["PnL"].sum()) if "PnL" in report else 0.0,
    }
    json_path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"DAILY_TRADE_CONTEXT csv={csv_path} markdown={md_path} json={json_path}")
    print(f"DAILY_TRADE_CONTEXT rows={payload['daily_rows']} trades={payload['total_trades']} pnl={payload['total_pnl']:.2f}")
    if not report.empty:
        print("DAILY_TRADE_CONTEXT_BEST")
        print(report.sort_values("PnL", ascending=False).head(args.top).to_string(index=False))
        print("DAILY_TRADE_CONTEXT_WORST")
        print(report.sort_values("PnL", ascending=True).head(args.top).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

