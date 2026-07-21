#!/usr/bin/env python3
"""Summarize a completed controlled Java replay with trade/lifecycle CSV outputs."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

MARKERS = {
    "SETUP_FILTER_PASSES_PASS": "SETUP_FILTER_PASSES=PASS",
    "SETUP_FILTER_PASSES_FAIL": "SETUP_FILTER_PASSES=FAIL",
    "MICRO_ENTRY_CONFIRMS_PASS": "MICRO_ENTRY_CONFIRMS=PASS",
    "MICRO_ENTRY_CONFIRMS_FAIL": "MICRO_ENTRY_CONFIRMS=FAIL",
    "featureSidecar_hit": "featureSidecar=hit",
    "featureSidecar_miss": "featureSidecar=miss",
    "featureSidecar_disabled": "featureSidecar=disabled",
    "featureSnapshot_hit": "featureSnapshot=hit",
    "featureSnapshot_miss": "featureSnapshot=miss",
    "featureSnapshot_disabled": "featureSnapshot=disabled",
}
COMPLETION_RE = re.compile(r"\[BACKTEST\] completed=(\d+) failed=(\d+) requested=(\d+)")


def read_csvs(output_dir: Path, suffix: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(output_dir.glob(f"*{suffix}")):
        if path.stat().st_size == 0:
            continue
        frame = pd.read_csv(path)
        if frame.empty and suffix == "-trades.csv":
            continue
        frame["SourceFile"] = path.name
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def numeric(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def parse_completion(log_text: str) -> dict[str, Any]:
    match = COMPLETION_RE.search(log_text)
    if not match:
        return {"completed": None, "failed": None, "requested": None, "raw": "<missing>"}
    completed, failed, requested = (int(part) for part in match.groups())
    return {"completed": completed, "failed": failed, "requested": requested, "raw": match.group(0)}


def frame_records(frame: pd.DataFrame, float_digits: int = 6) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        normalized: dict[str, Any] = {}
        for key, value in row.items():
            if pd.isna(value):
                normalized[key] = None
            elif isinstance(value, float):
                normalized[key] = round(value, float_digits)
            else:
                normalized[key] = value
        records.append(normalized)
    return records


def summarize(output_dir: Path, log_file: Path | None = None) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    log_path = log_file.resolve() if log_file is not None else output_dir / "controlled_java_replay.log"
    if not log_path.is_file():
        raise FileNotFoundError(f"missing replay log: {log_path}")

    log_text = log_path.read_text(errors="ignore")
    lifecycle = read_csvs(output_dir, "-trade-lifecycle-summary.csv")
    trades = read_csvs(output_dir, "-trades.csv")
    orders = read_csvs(output_dir, "-orders.csv")

    if lifecycle.empty:
        closed = pd.DataFrame()
        symbol_agg = pd.DataFrame()
    else:
        closed = lifecycle[lifecycle["ClosedTradeIndex"].notna()].copy()
        numeric(closed, ["ClosedTradeIndex", "TradePnL", "RealizedR", "MfeR", "MaeR", "Quantity", "SetupToFillSeconds"])
        if not closed.empty:
            closed["EntryDate"] = (
                pd.to_datetime(closed["EntryTime"], errors="coerce", utc=True)
                .dt.tz_convert("America/New_York")
                .dt.date.astype(str)
            )
        agg_columns = [
            "Symbol",
            "ArmsTotal",
            "ArmsLong",
            "ArmsShort",
            "ArmConfirmations",
            "ArmExpirations",
            "ArmConversionRate",
            "GuardEvaluations",
            "GuardFires",
            "LifecycleExits",
            "HardRiskExits",
            "EodExits",
            "AvgSetupToFillSeconds",
            "AvgMfeR",
            "AvgMaeR",
            "ExitReasonDistribution",
        ]
        symbol_agg = lifecycle[[column for column in agg_columns if column in lifecycle.columns]].drop_duplicates("Symbol", keep="first").copy()
        numeric(symbol_agg, [column for column in symbol_agg.columns if column not in {"Symbol", "ExitReasonDistribution"}])

    marker_counts = {name: log_text.count(marker) for name, marker in MARKERS.items()}
    completion = parse_completion(log_text)

    if not orders.empty and "EventType" in orders.columns:
        filled_orders = int(orders["EventType"].eq("FILLED").sum())
        submitted_orders = int(orders["EventType"].eq("SUBMITTED").sum())
    else:
        filled_orders = 0
        submitted_orders = 0

    if closed.empty:
        by_symbol_side = pd.DataFrame()
        by_symbol = pd.DataFrame()
        by_day = pd.DataFrame()
        exit_reason_counts: dict[str, int] = {}
        side_counts: dict[str, int] = {}
        scalar_summary = {
            "closed_trades": 0,
            "total_pnl": 0.0,
            "sum_realized_r": 0.0,
            "mean_realized_r": 0.0,
            "median_realized_r": 0.0,
            "win_rate": 0.0,
            "avg_mfe_r": 0.0,
            "avg_mae_r": 0.0,
            "max_day_trade_share": 0.0,
            "max_symbol_trade_share": 0.0,
        }
    else:
        exit_reason_counts = {str(k): int(v) for k, v in closed["ExitReason"].fillna("<missing>").value_counts().sort_index().items()}
        side_counts = {str(k): int(v) for k, v in closed["TradeSide"].fillna("<missing>").value_counts().sort_index().items()}
        by_symbol_side = closed.groupby(["Symbol", "TradeSide"]).agg(
            trades=("ClosedTradeIndex", "count"),
            pnl=("TradePnL", "sum"),
            sum_r=("RealizedR", "sum"),
            mean_r=("RealizedR", "mean"),
            wins=("TradePnL", lambda series: int((series > 0).sum())),
        ).reset_index()
        by_symbol_side["win_rate"] = by_symbol_side["wins"] / by_symbol_side["trades"]
        by_symbol = closed.groupby("Symbol").agg(
            trades=("ClosedTradeIndex", "count"),
            pnl=("TradePnL", "sum"),
            sum_r=("RealizedR", "sum"),
        ).reset_index().sort_values("trades", ascending=False)
        by_symbol["trade_share"] = by_symbol["trades"] / len(closed)
        by_day = closed.groupby("EntryDate").agg(
            trades=("ClosedTradeIndex", "count"),
            pnl=("TradePnL", "sum"),
            sum_r=("RealizedR", "sum"),
        ).reset_index().sort_values(["trades", "pnl"], ascending=[False, False])
        by_day["trade_share"] = by_day["trades"] / len(closed)
        scalar_summary = {
            "closed_trades": int(len(closed)),
            "total_pnl": float(closed["TradePnL"].sum()),
            "sum_realized_r": float(closed["RealizedR"].sum()),
            "mean_realized_r": float(closed["RealizedR"].mean()),
            "median_realized_r": float(closed["RealizedR"].median()),
            "win_rate": float((closed["TradePnL"] > 0).mean()),
            "avg_mfe_r": float(closed["MfeR"].mean()),
            "avg_mae_r": float(closed["MaeR"].mean()),
            "max_day_trade_share": float(by_day["trade_share"].max()),
            "max_symbol_trade_share": float(by_symbol["trade_share"].max()),
        }

    summary: dict[str, Any] = {
        "output_dir": str(output_dir),
        "completion": completion,
        "marker_counts": marker_counts,
        "submitted_orders": submitted_orders,
        "filled_orders": filled_orders,
        "side_counts": side_counts,
        "exit_reason_counts": exit_reason_counts,
        **scalar_summary,
        "by_symbol_side": frame_records(by_symbol_side),
        "by_symbol": frame_records(by_symbol),
        "by_day_top": frame_records(by_day.head(20)),
        "per_symbol_lifecycle_aggregates": frame_records(symbol_agg),
    }
    return summary


def replay_classification(summary: dict[str, Any]) -> str:
    marker_counts = summary.get("marker_counts", {})
    if marker_counts.get("featureSnapshot_hit", 0) > 0 and marker_counts.get("featureSidecar_disabled", 0) > 0:
        return "research-only event-carried snapshot replay evidence; not live/paper shadow drift evidence"
    if marker_counts.get("featureSidecar_hit", 0) > 0:
        return "research-only sidecar replay evidence; not live-feature parity evidence"
    return "research-only replay evidence; review feature-source telemetry before promotion use"


def markdown_report(summary: dict[str, Any], *, title: str = "Trade-enabled controlled replay summary", classification: str | None = None) -> str:
    classification = classification or replay_classification(summary)

    def markdown_cell(value: Any) -> str:
        if value is None:
            return ""
        return str(value).replace("\n", "<br>").replace("|", "\\|")

    lines = [
        f"# {title}",
        "",
        "## Status",
        "",
        f"- Output: `{summary['output_dir']}`",
        f"- Completion: `{summary['completion']['raw']}`",
        f"- Sidecar misses: `{summary['marker_counts']['featureSidecar_miss']}`",
        f"- Feature snapshot hits: `{summary['marker_counts']['featureSnapshot_hit']}`",
        f"- Feature snapshot misses: `{summary['marker_counts']['featureSnapshot_miss']}`",
        f"- Sidecar disabled rows: `{summary['marker_counts']['featureSidecar_disabled']}`",
        f"- Classification: **{classification}**",
        "",
        "## Marker counts",
        "",
        "| Marker | Count |",
        "|---|---:|",
    ]
    for key, value in summary["marker_counts"].items():
        lines.append(f"| `{key}` | `{value}` |")
    lines.extend([
        "",
        "## Realized trade summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Closed trades | `{summary['closed_trades']}` |",
        f"| Submitted orders | `{summary['submitted_orders']}` |",
        f"| Filled orders | `{summary['filled_orders']}` |",
        f"| Total PnL | `{summary['total_pnl']:.2f}` |",
        f"| Sum realized R | `{summary['sum_realized_r']:.4f}` |",
        f"| Mean realized R | `{summary['mean_realized_r']:.4f}` |",
        f"| Median realized R | `{summary['median_realized_r']:.4f}` |",
        f"| Win rate | `{summary['win_rate']:.2%}` |",
        f"| Avg MFE R | `{summary['avg_mfe_r']:.4f}` |",
        f"| Avg MAE R | `{summary['avg_mae_r']:.4f}` |",
        f"| Max day trade share | `{summary['max_day_trade_share']:.2%}` |",
        f"| Max symbol trade share | `{summary['max_symbol_trade_share']:.2%}` |",
        "",
        "## Exit reasons",
        "",
        "| Exit reason | Count |",
        "|---|---:|",
    ])
    for reason, count in summary["exit_reason_counts"].items():
        lines.append(f"| `{reason}` | `{count}` |")
    lines.extend(["", "## Side balance", "", "| Side | Count |", "|---|---:|"])
    for side, count in summary["side_counts"].items():
        lines.append(f"| `{side}` | `{count}` |")

    def add_table(title: str, records: list[dict[str, Any]], columns: list[str]) -> None:
        lines.extend(["", f"## {title}", ""])
        if not records:
            lines.append("<empty>")
            return
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join(["---"] * len(columns)) + "|")
        for record in records:
            lines.append("| " + " | ".join(markdown_cell(record.get(column, "")) for column in columns) + " |")

    add_table("By symbol", summary["by_symbol"], ["Symbol", "trades", "pnl", "sum_r", "trade_share"])
    add_table("By symbol and side", summary["by_symbol_side"], ["Symbol", "TradeSide", "trades", "pnl", "sum_r", "mean_r", "wins", "win_rate"])
    add_table("Top trading days", summary["by_day_top"], ["EntryDate", "trades", "pnl", "sum_r", "trade_share"])
    add_table("Per-symbol lifecycle aggregates", summary["per_symbol_lifecycle_aggregates"], ["Symbol", "ArmsTotal", "ArmConfirmations", "ArmExpirations", "GuardEvaluations", "GuardFires", "LifecycleExits", "HardRiskExits", "EodExits", "ExitReasonDistribution"])
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--write-artifacts", action="store_true")
    parser.add_argument("--log-file", type=Path, help="Optional replay log path. Defaults to <output-dir>/controlled_java_replay.log.")
    parser.add_argument("--summary-json", type=Path, help="Optional path for a JSON summary artifact.")
    parser.add_argument("--summary-md", type=Path, help="Optional path for a Markdown summary artifact.")
    parser.add_argument("--title", default="Trade-enabled controlled replay summary", help="Markdown report title.")
    parser.add_argument("--classification", help="Optional Markdown classification override.")
    args = parser.parse_args(argv)

    summary = summarize(args.output_dir, args.log_file)
    print(json.dumps(summary, indent=2, sort_keys=True))

    output_dir = args.output_dir.resolve()
    summary_json = args.summary_json
    summary_md = args.summary_md
    if args.write_artifacts:
        summary_json = summary_json or output_dir / "trade_enabled_sidecar_summary_20260719.json"
        summary_md = summary_md or output_dir / "trade_enabled_sidecar_summary_20260719.md"
    if summary_json is not None:
        summary_json.parent.mkdir(parents=True, exist_ok=True)
        summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    if summary_md is not None:
        summary_md.parent.mkdir(parents=True, exist_ok=True)
        summary_md.write_text(markdown_report(summary, title=args.title, classification=args.classification))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
