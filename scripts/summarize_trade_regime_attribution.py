#!/usr/bin/env python3
"""Attribute closed trade PnL to strategy regimes using lifecycle CSVs and replay logs.

The trade lifecycle summary does not currently include a dedicated Regime column.
This script reconstructs it by parsing Java replay log lines such as:

  AI.LONG.ENTRY ... MODEL_AVAILABLE=PASS | symbol=NVDA regime=CHOPPY
  AI.DOWNSTREAM_SETUP_FILTER ... SETUP_FILTER_PASSES=PASS | symbol=NVDA side=long armEpoch=...

It keeps the latest active regime per (symbol, side) and attaches that regime to
subsequent setup-filter rows. Closed trades are matched back to setup-filter rows
by (Symbol, TradeSide, SetupArmTime epoch) with a configurable tolerance.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

MODEL_AVAILABLE_RE = re.compile(
    r"AI\.(?P<side>LONG|SHORT)\.ENTRY.*?MODEL_AVAILABLE=(?:PASS|FAIL).*?symbol=(?P<symbol>[A-Z0-9_.-]+).*?regime=(?P<regime>[A-Z0-9_]+)",
    re.IGNORECASE,
)
SETUP_FILTER_RE = re.compile(r"SETUP_FILTER_PASSES=(?P<status>PASS|FAIL)\s*\|\s*(?P<details>.*)")
KV_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s|]+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path, help="Trade replay output directory containing lifecycle CSVs.")
    parser.add_argument("--log-file", type=Path, default=None, help="Replay log. Defaults to <output-dir>/controlled_java_replay.log.")
    parser.add_argument("--out-prefix", type=Path, default=None, help="Output prefix. Defaults to <output-dir>/trade_regime_attribution.")
    parser.add_argument("--match-tolerance-seconds", type=int, default=60, help="Max absolute seconds between SetupArmTime and setup-filter armEpoch.")
    parser.add_argument("--top-periods", type=int, default=5, help="Number of best/worst months and days to include in markdown/json.")
    parser.add_argument("--title", default="Trade PnL regime attribution")
    return parser.parse_args()


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if hasattr(value, "item"):
        return json_safe(value.item())
    return value


def parse_kv(details: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(details)}


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side in {"buy", "long_entry"}:
        return "long"
    if side in {"sell", "short_entry"}:
        return "short"
    if side.startswith("long"):
        return "long"
    if side.startswith("short"):
        return "short"
    return side


def parse_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def parse_epoch(value: Any) -> int | None:
    parsed = parse_float(value)
    return int(parsed) if parsed is not None else None


def read_lifecycle(output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(output_dir.glob("*-trade-lifecycle-summary.csv")):
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

    trades["TradeSideNorm"] = trades.get("TradeSide", "").map(normalize_side)
    trades["EntryTs"] = pd.to_datetime(trades.get("EntryTime"), errors="coerce", utc=True)
    trades["SetupTs"] = pd.to_datetime(trades.get("SetupArmTime"), errors="coerce", utc=True)
    trades["MatchTs"] = trades["SetupTs"].fillna(trades["EntryTs"])
    trades["MatchEpoch"] = trades["MatchTs"].map(lambda ts: int(ts.timestamp()) if pd.notna(ts) else pd.NA).astype("Int64")
    ny_entry = trades["EntryTs"].dt.tz_convert("America/New_York")
    trades["EntryDate"] = ny_entry.dt.date.astype(str)
    iso = ny_entry.dt.isocalendar()
    trades["EntryWeek"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    trades["EntryMonth"] = ny_entry.dt.strftime("%Y-%m")
    trades["Win"] = trades["TradePnL"] > 0
    return trades


def parse_setup_regimes(log_file: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    latest_regime: dict[tuple[str, str], str] = {}
    if not log_file.is_file():
        return pd.DataFrame(columns=["Symbol", "Side", "ArmEpoch", "Regime", "SetupFilterPassed"])

    with log_file.open("r", encoding="utf-8", errors="ignore") as handle:
        for line_no, line in enumerate(handle, start=1):
            model_match = MODEL_AVAILABLE_RE.search(line)
            if model_match:
                symbol = model_match.group("symbol").upper()
                side = normalize_side(model_match.group("side"))
                regime = model_match.group("regime").upper()
                latest_regime[(symbol, side)] = regime

            setup_match = SETUP_FILTER_RE.search(line)
            if not setup_match:
                continue
            details = parse_kv(setup_match.group("details"))
            symbol = str(details.get("symbol") or "").strip().upper()
            side = normalize_side(details.get("side"))
            epoch = parse_epoch(details.get("armEpoch") or details.get("arm_epoch"))
            if not symbol or not side or epoch is None:
                continue
            rows.append({
                "Symbol": symbol,
                "Side": side,
                "ArmEpoch": epoch,
                "Regime": latest_regime.get((symbol, side), "UNKNOWN"),
                "SetupFilterPassed": setup_match.group("status") == "PASS",
                "SetupFilterRoute": str(details.get("route") or ""),
                "SetupFilterProb": parse_float(details.get("prob")),
                "SetupFilterThreshold": parse_float(details.get("threshold")),
                "FeatureSnapshot": str(details.get("featureSnapshot") or ""),
                "FeatureSidecar": str(details.get("featureSidecar") or ""),
                "LogLine": line_no,
            })
    return pd.DataFrame(rows)


def attach_regimes(trades: pd.DataFrame, setup_rows: pd.DataFrame, tolerance_seconds: int) -> pd.DataFrame:
    if trades.empty:
        return trades
    out_rows: list[dict[str, Any]] = []
    setup_passed = setup_rows[setup_rows.get("SetupFilterPassed", False) == True].copy() if not setup_rows.empty else setup_rows
    for _, trade in trades.iterrows():
        row = trade.to_dict()
        symbol = str(row.get("Symbol") or "").strip().upper()
        side = normalize_side(row.get("TradeSideNorm"))
        epoch = row.get("MatchEpoch")
        regime_match = None
        if not setup_passed.empty and pd.notna(epoch):
            epoch_int = int(epoch)
            candidates = setup_passed[
                (setup_passed["Symbol"] == symbol)
                & (setup_passed["Side"] == side)
                & (setup_passed["ArmEpoch"].between(epoch_int - tolerance_seconds, epoch_int + tolerance_seconds))
            ].copy()
            if not candidates.empty:
                candidates["AbsDelta"] = (candidates["ArmEpoch"] - epoch_int).abs()
                regime_match = candidates.sort_values(["AbsDelta", "LogLine"]).iloc[0]
        if regime_match is None:
            row.update({
                "Regime": "UNKNOWN",
                "RegimeMatchDeltaSeconds": None,
                "SetupFilterRouteMatched": "",
                "SetupFilterProbMatched": None,
                "SetupFilterThresholdMatched": None,
                "FeatureSnapshotMatched": "",
                "FeatureSidecarMatched": "",
            })
        else:
            row.update({
                "Regime": regime_match.get("Regime", "UNKNOWN"),
                "RegimeMatchDeltaSeconds": int(regime_match["ArmEpoch"] - int(epoch)),
                "SetupFilterRouteMatched": regime_match.get("SetupFilterRoute", ""),
                "SetupFilterProbMatched": regime_match.get("SetupFilterProb"),
                "SetupFilterThresholdMatched": regime_match.get("SetupFilterThreshold"),
                "FeatureSnapshotMatched": regime_match.get("FeatureSnapshot", ""),
                "FeatureSidecarMatched": regime_match.get("FeatureSidecar", ""),
            })
        out_rows.append(row)
    return pd.DataFrame(out_rows)


def summarize_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    columns = group_cols + ["trades", "wins", "losses", "win_rate", "pnl", "realized_r", "avg_pnl", "avg_r"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    grouped = df.groupby(group_cols, dropna=False)
    rows = []
    for key, group in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        row = dict(zip(group_cols, key))
        trades = int(len(group))
        wins = int(group["Win"].sum())
        row.update({
            "trades": trades,
            "wins": wins,
            "losses": int((~group["Win"]).sum()),
            "win_rate": wins / trades if trades else 0.0,
            "pnl": float(group["TradePnL"].sum()),
            "realized_r": float(group["RealizedR"].sum()) if "RealizedR" in group else 0.0,
            "avg_pnl": float(group["TradePnL"].mean()),
            "avg_r": float(group["RealizedR"].mean()) if "RealizedR" in group else 0.0,
        })
        rows.append(row)
    return pd.DataFrame(rows, columns=columns).sort_values("pnl", ascending=False).reset_index(drop=True)


def markdown_value(value: Any) -> str:
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, str)) else False:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    if hasattr(value, "item"):
        return markdown_value(value.item())
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
    lines = [
        f"# {title}",
        "",
        f"- Output dir: `{report['output_dir']}`",
        f"- Log file: `{report['log_file']}`",
        f"- Closed trades: `{report['closed_trades']}`",
        f"- Setup-filter rows parsed: `{report['setup_filter_rows']}`",
        f"- Unknown regime trades: `{report['unknown_regime_trades']}`",
        f"- Match tolerance seconds: `{report['match_tolerance_seconds']}`",
        "",
        "## PnL by regime",
        "",
        frame_to_markdown(tables["by_regime"]),
        "",
        "## Best months",
        "",
        frame_to_markdown(tables["best_months"]),
        "",
        "## Regime mix in best months",
        "",
        frame_to_markdown(tables["best_month_regimes"]),
        "",
        "## Worst months",
        "",
        frame_to_markdown(tables["worst_months"]),
        "",
        "## Regime mix in worst months",
        "",
        frame_to_markdown(tables["worst_month_regimes"]),
        "",
        "## Best days",
        "",
        frame_to_markdown(tables["best_days"]),
        "",
        "## Regime mix in best days",
        "",
        frame_to_markdown(tables["best_day_regimes"]),
        "",
        "## Worst days",
        "",
        frame_to_markdown(tables["worst_days"]),
        "",
        "## Regime mix in worst days",
        "",
        frame_to_markdown(tables["worst_day_regimes"]),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    log_file = (args.log_file.expanduser().resolve() if args.log_file else output_dir / "controlled_java_replay.log")
    out_prefix = (args.out_prefix.expanduser().resolve() if args.out_prefix else output_dir / "trade_regime_attribution")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    trades = read_lifecycle(output_dir)
    setup_rows = parse_setup_regimes(log_file)
    enriched = attach_regimes(trades, setup_rows, args.match_tolerance_seconds)
    enriched.to_csv(out_prefix.with_name(f"{out_prefix.name}_trades_with_regime.csv"), index=False)

    by_regime = summarize_group(enriched, ["Regime"])
    by_month = summarize_group(enriched, ["EntryMonth"])
    by_day = summarize_group(enriched, ["EntryDate"])
    by_month_regime = summarize_group(enriched, ["EntryMonth", "Regime"])
    by_day_regime = summarize_group(enriched, ["EntryDate", "Regime"])

    best_months = by_month.head(args.top_periods)
    worst_months = by_month.sort_values("pnl", ascending=True).head(args.top_periods)
    best_days = by_day.head(args.top_periods)
    worst_days = by_day.sort_values("pnl", ascending=True).head(args.top_periods)

    best_month_keys = set(best_months["EntryMonth"].astype(str)) if not best_months.empty else set()
    worst_month_keys = set(worst_months["EntryMonth"].astype(str)) if not worst_months.empty else set()
    best_day_keys = set(best_days["EntryDate"].astype(str)) if not best_days.empty else set()
    worst_day_keys = set(worst_days["EntryDate"].astype(str)) if not worst_days.empty else set()

    tables = {
        "by_regime": by_regime,
        "by_month": by_month,
        "by_day": by_day,
        "by_month_regime": by_month_regime,
        "by_day_regime": by_day_regime,
        "best_months": best_months,
        "worst_months": worst_months,
        "best_days": best_days,
        "worst_days": worst_days,
        "best_month_regimes": by_month_regime[by_month_regime["EntryMonth"].astype(str).isin(best_month_keys)],
        "worst_month_regimes": by_month_regime[by_month_regime["EntryMonth"].astype(str).isin(worst_month_keys)],
        "best_day_regimes": by_day_regime[by_day_regime["EntryDate"].astype(str).isin(best_day_keys)],
        "worst_day_regimes": by_day_regime[by_day_regime["EntryDate"].astype(str).isin(worst_day_keys)],
    }

    csv_outputs: dict[str, str] = {}
    for name, table in tables.items():
        csv_path = out_prefix.with_name(f"{out_prefix.name}_{name}.csv")
        table.to_csv(csv_path, index=False)
        csv_outputs[name] = str(csv_path)

    report = {
        "output_dir": str(output_dir),
        "log_file": str(log_file),
        "closed_trades": int(len(enriched)),
        "setup_filter_rows": int(len(setup_rows)),
        "setup_filter_pass_rows": int(setup_rows["SetupFilterPassed"].sum()) if not setup_rows.empty and "SetupFilterPassed" in setup_rows else 0,
        "unknown_regime_trades": int((enriched.get("Regime", pd.Series(dtype=str)) == "UNKNOWN").sum()) if not enriched.empty else 0,
        "match_tolerance_seconds": int(args.match_tolerance_seconds),
        "csv_outputs": csv_outputs,
        "by_regime": by_regime.to_dict("records"),
        "best_months": best_months.to_dict("records"),
        "worst_months": worst_months.to_dict("records"),
        "best_days": best_days.to_dict("records"),
        "worst_days": worst_days.to_dict("records"),
    }
    json_path = out_prefix.with_suffix(".json")
    md_path = out_prefix.with_suffix(".md")
    json_path.write_text(json.dumps(json_safe(report), indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(md_path, args.title, report, tables)

    print(f"TRADE_REGIME_ATTRIBUTION json={json_path} markdown={md_path}")
    print(f"TRADE_REGIME_ATTRIBUTION closed_trades={len(enriched)} setup_filter_rows={len(setup_rows)} unknown_regime_trades={report['unknown_regime_trades']}")
    if not by_regime.empty:
        print("TRADE_REGIME_ATTRIBUTION_BY_REGIME")
        print(by_regime.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


