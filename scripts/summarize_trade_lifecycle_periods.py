#!/usr/bin/env python3
"""Summarize trade-enabled replay lifecycle CSVs by all/symbol/day/week/month.

Inputs are the `*-trade-lifecycle-summary.csv` files emitted by
`scripts/run_databento_historical_ibkr_sim_backtest.sh`. Rows with blank
`TradePnL` are diagnostic/no-trade rows and are excluded from PnL rollups.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

MARKERS = {
    "setup_pass": "SETUP_FILTER_PASSES=PASS",
    "setup_fail": "SETUP_FILTER_PASSES=FAIL",
    "micro_pass": "MICRO_ENTRY_CONFIRMS=PASS",
    "micro_fail": "MICRO_ENTRY_CONFIRMS=FAIL",
    "snapshot_hit": "featureSnapshot=hit",
    "snapshot_miss": "featureSnapshot=miss",
    "sidecar_disabled": "featureSidecar=disabled",
    "sidecar_hit": "featureSidecar=hit",
    "sidecar_miss": "featureSidecar=miss",
}
COMPLETION_RE = re.compile(r"\[BACKTEST\] completed=(\d+) failed=(\d+) requested=(\d+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path, help="Replay output directory containing lifecycle CSVs.")
    parser.add_argument("--log-file", type=Path, default=None, help="Replay log. Defaults to <output-dir>/controlled_java_replay.log.")
    parser.add_argument("--out-prefix", type=Path, default=None, help="Output prefix. Defaults to <output-dir>/pnl_period_summary.")
    parser.add_argument("--title", default="Trade lifecycle PnL period summary")
    return parser.parse_args()


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str)) else False:
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def read_lifecycle(output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(output_dir.glob("*-trade-lifecycle-summary.csv")):
        if path.stat().st_size <= 0:
            continue
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        frame["SourceFile"] = path.name
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    lifecycle = pd.concat(frames, ignore_index=True)
    if "TradePnL" not in lifecycle.columns:
        return pd.DataFrame()
    lifecycle["TradePnL"] = pd.to_numeric(lifecycle["TradePnL"], errors="coerce")
    closed = lifecycle[lifecycle["TradePnL"].notna()].copy()
    if closed.empty:
        return closed
    for column in ["ClosedTradeIndex", "RealizedR", "MfeR", "MaeR", "Quantity", "EntryPrice", "ExitPrice", "SetupToFillSeconds"]:
        if column in closed.columns:
            closed[column] = pd.to_numeric(closed[column], errors="coerce")
    closed["EntryTimestamp"] = pd.to_datetime(closed.get("EntryTime"), errors="coerce", utc=True)
    closed["ExitTimestamp"] = pd.to_datetime(closed.get("ExitTime"), errors="coerce", utc=True)
    ny_entry = closed["EntryTimestamp"].dt.tz_convert("America/New_York")
    closed["EntryDate"] = ny_entry.dt.date.astype(str)
    iso = ny_entry.dt.isocalendar()
    closed["EntryWeek"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    closed["EntryMonth"] = ny_entry.dt.strftime("%Y-%m")
    closed["Win"] = closed["TradePnL"] > 0
    closed["Loss"] = closed["TradePnL"] < 0
    return closed


def summarize_group(closed: pd.DataFrame, group_cols: list[str], scope: str) -> pd.DataFrame:
    base_columns = [
        "scope", "period", "symbol", "side", "trades", "wins", "losses", "win_rate",
        "pnl", "sum_realized_r", "mean_realized_r", "median_realized_r",
        "avg_mfe_r", "avg_mae_r", "avg_setup_to_fill_seconds",
    ]
    if closed.empty:
        return pd.DataFrame(columns=base_columns)

    grouped = closed.groupby(group_cols, dropna=False) if group_cols else [((), closed)]
    rows: list[dict[str, Any]] = []
    for key, frame in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        key_map = dict(zip(group_cols, key))
        trades = int(len(frame))
        wins = int(frame["Win"].sum())
        losses = int(frame["Loss"].sum())
        rows.append({
            "scope": scope,
            "period": key_map.get("EntryDate") or key_map.get("EntryWeek") or key_map.get("EntryMonth") or "ALL",
            "symbol": key_map.get("Symbol") or "ALL",
            "side": key_map.get("TradeSide") or "ALL",
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": float(wins / trades) if trades else 0.0,
            "pnl": float(frame["TradePnL"].sum()),
            "sum_realized_r": float(frame["RealizedR"].sum()) if "RealizedR" in frame else 0.0,
            "mean_realized_r": float(frame["RealizedR"].mean()) if "RealizedR" in frame else 0.0,
            "median_realized_r": float(frame["RealizedR"].median()) if "RealizedR" in frame else 0.0,
            "avg_mfe_r": float(frame["MfeR"].mean()) if "MfeR" in frame else 0.0,
            "avg_mae_r": float(frame["MaeR"].mean()) if "MaeR" in frame else 0.0,
            "avg_setup_to_fill_seconds": float(frame["SetupToFillSeconds"].mean()) if "SetupToFillSeconds" in frame else 0.0,
        })
    out = pd.DataFrame(rows, columns=base_columns)
    sort_cols = [c for c in ["period", "symbol", "side"] if c in out.columns]
    return out.sort_values(sort_cols).reset_index(drop=True) if sort_cols else out


def scan_log(log_file: Path) -> dict[str, Any]:
    if not log_file.is_file():
        return {"completion": {"raw": "<missing>", "completed": None, "failed": None, "requested": None}, "markers": {}, "errors": 0, "watchdogs": {}}
    completion = {"raw": "<missing>", "completed": None, "failed": None, "requested": None}
    marker_counts = {name: 0 for name in MARKERS}
    errors = 0
    watchdogs: dict[str, int] = {}
    with log_file.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            match = COMPLETION_RE.search(line)
            if match:
                completion = {
                    "raw": match.group(0),
                    "completed": int(match.group(1)),
                    "failed": int(match.group(2)),
                    "requested": int(match.group(3)),
                }
            for name, marker in MARKERS.items():
                marker_counts[name] += line.count(marker)
            errors += len(re.findall(r"Exception|ERROR", line))
            if "WATCHDOG" not in line:
                continue
            symbol_match = re.search(r"symbol=([A-Z0-9_.-]+)", line)
            symbol = symbol_match.group(1) if symbol_match else "UNKNOWN"
            watchdogs[symbol] = watchdogs.get(symbol, 0) + 1
    return {
        "completion": completion,
        "markers": marker_counts,
        "errors": errors,
        "watchdogs": watchdogs,
    }


def markdown_value(value: Any) -> str:
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str)) else False:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    if hasattr(value, "item"):
        value = value.item()
        if isinstance(value, float):
            return f"{value:.4f}"
    return str(value).replace("|", r"\|").replace("\n", " ")


def frame_to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No rows."
    columns = [str(column) for column in frame.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(markdown_value(row[column]) for column in frame.columns) + " |")
    return "\n".join(lines)


def write_markdown(path: Path, title: str, report: dict[str, Any], tables: dict[str, pd.DataFrame]) -> None:
    lines = [f"# {title}", ""]
    lines.extend([
        "## Status", "",
        f"- Output dir: `{report['output_dir']}`",
        f"- Log file: `{report['log_file']}`",
        f"- Completion: `{report['log']['completion']['raw']}`",
        f"- Error/Exception grep count: `{report['log']['errors']}`",
        f"- Watchdog count: `{sum(report['log']['watchdogs'].values())}`",
        "",
        "## Overall", "",
    ])
    overall = tables["all"]
    if not overall.empty:
        lines.append(frame_to_markdown(overall))
    else:
        lines.append("No closed trades with TradePnL found.")
    for name in ["by_symbol", "by_month", "by_week", "by_day", "by_symbol_month", "by_symbol_week", "by_symbol_day", "by_symbol_side"]:
        frame = tables[name]
        lines.extend(["", f"## {name}", ""])
        if frame.empty:
            lines.append("No rows.")
        else:
            lines.append(frame_to_markdown(frame))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    log_file = (args.log_file.expanduser().resolve() if args.log_file else output_dir / "controlled_java_replay.log")
    out_prefix = (args.out_prefix.expanduser().resolve() if args.out_prefix else output_dir / "pnl_period_summary")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    closed = read_lifecycle(output_dir)
    tables = {
        "all": summarize_group(closed, [], "all"),
        "by_symbol": summarize_group(closed, ["Symbol"], "symbol"),
        "by_symbol_side": summarize_group(closed, ["Symbol", "TradeSide"], "symbol_side"),
        "by_day": summarize_group(closed, ["EntryDate"], "day"),
        "by_week": summarize_group(closed, ["EntryWeek"], "week"),
        "by_month": summarize_group(closed, ["EntryMonth"], "month"),
        "by_symbol_day": summarize_group(closed, ["Symbol", "EntryDate"], "symbol_day"),
        "by_symbol_week": summarize_group(closed, ["Symbol", "EntryWeek"], "symbol_week"),
        "by_symbol_month": summarize_group(closed, ["Symbol", "EntryMonth"], "symbol_month"),
    }

    csv_paths: dict[str, str] = {}
    for name, frame in tables.items():
        csv_path = out_prefix.with_name(f"{out_prefix.name}_{name}.csv")
        frame.to_csv(csv_path, index=False)
        csv_paths[name] = str(csv_path)

    report = {
        "output_dir": str(output_dir),
        "log_file": str(log_file),
        "closed_trade_rows": int(len(closed)),
        "log": scan_log(log_file),
        "csv_outputs": csv_paths,
    }
    for name, frame in tables.items():
        report[name] = frame.to_dict("records")
    json_path = out_prefix.with_suffix(".json")
    md_path = out_prefix.with_suffix(".md")
    json_path.write_text(json.dumps(json_safe(report), indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(md_path, args.title, report, tables)

    print(f"PNL_PERIOD_SUMMARY json={json_path} markdown={md_path}")
    if not tables["all"].empty:
        row = tables["all"].iloc[0].to_dict()
        print(
            "PNL_PERIOD_SUMMARY_ALL "
            f"trades={int(row['trades'])} pnl={float(row['pnl']):.2f} "
            f"sum_realized_r={float(row['sum_realized_r']):.4f} win_rate={float(row['win_rate']):.4f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

