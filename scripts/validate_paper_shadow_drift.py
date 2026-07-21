#!/usr/bin/env python3
"""Validate paper/shadow drift for event-carried downstream setup snapshots.

Inputs can be either paired decision CSVs or paired Java logs. Java logs are parsed
from `SETUP_FILTER_PASSES` lines emitted by PingPongStrategy and include the
`featureSnapshot=hit|miss|disabled` telemetry added for live/paper parity.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
STATUS_RANK = {"PASS": 0, "WARN": 1, "NO-GO": 2}
SETUP_FILTER_RE = re.compile(r"SETUP_FILTER_PASSES=(PASS|FAIL)\s*\|\s*(?P<details>.*)")
KV_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s|]+)")

KEY_ALIASES = {
    "symbol": ["symbol", "Symbol"],
    "side": ["side", "SetupSide", "setup_side"],
    "arm_epoch": ["arm_epoch", "armEpoch", "SetupEpochSec", "setup_epoch_sec", "FeatureSnapshotEpochSec", "feature_snapshot_epoch_sec", "epoch", "barEpochSec"],
}
PROBABILITY_COLUMNS = [
    "probability", "prob", "filter_prob", "java_prob", "shadow_prob", "paper_prob",
    "calibrated_probability", "raw_probability", "EntryProb", "entry_prob",
]
DECISION_COLUMNS = [
    "passed", "pass", "filter_pass", "java_pass", "decision", "fired", "confirmed", "action",
]
SNAPSHOT_COLUMNS = ["featureSnapshot", "feature_snapshot", "feature_snapshot_status", "snapshot", "snapshot_status"]
ROUTE_COLUMNS = ["route", "routeName", "model", "event"]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def repo_path(raw: str | Path | None) -> Path | None:
    if raw is None or str(raw).strip() == "":
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return default
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def first_present(row: dict[str, Any], candidates: list[str]) -> Any:
    for key in candidates:
        if key in row and str(row.get(key, "")).strip() != "":
            return row.get(key)
    return None


def boolish(value: Any) -> bool | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "pass", "passed", "buy", "sell", "short", "long"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "fail", "failed", "hold", "none", "skip", "blocked"}:
        return False
    return None


def normalize_side(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"buy", "long_entry"}:
        return "long"
    if raw in {"sell", "short_entry"}:
        return "short"
    return raw


def normalize_snapshot_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"hit", "miss", "disabled"}:
        return raw
    return ""


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * p)))
    return ordered[idx]


def gate(name: str, status: str, summary: str, *, metrics: dict[str, Any] | None = None,
         issues: list[str] | None = None, warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "summary": summary,
        "metrics": metrics or {},
        "issues": issues or [],
        "warnings": warnings or [],
    }


def overall_status(gates: list[dict[str, Any]]) -> str:
    if not gates:
        return "NO-GO"
    return max((str(item["status"]) for item in gates), key=lambda s: STATUS_RANK.get(s, 2))


def row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    symbol = str(first_present(row, KEY_ALIASES["symbol"]) or "").strip().upper()
    side = normalize_side(first_present(row, KEY_ALIASES["side"]))
    epoch_raw = first_present(row, KEY_ALIASES["arm_epoch"])
    epoch_float = as_float(epoch_raw)
    epoch = str(int(epoch_float)) if epoch_float is not None else str(epoch_raw or "").strip()
    return symbol, side, epoch


def normalized_decision_row(raw: dict[str, Any], source: str, index: int) -> dict[str, Any]:
    normalized = dict(raw)
    symbol, side, arm_epoch = row_key(raw)
    probability = as_float(first_present(raw, PROBABILITY_COLUMNS))
    decision = boolish(first_present(raw, DECISION_COLUMNS))
    snapshot_status = normalize_snapshot_status(first_present(raw, SNAPSHOT_COLUMNS))
    route = str(first_present(raw, ROUTE_COLUMNS) or "").strip()
    normalized.update({
        "_source": source,
        "_index": index,
        "symbol": symbol,
        "side": side,
        "arm_epoch": arm_epoch,
        "probability": probability,
        "decision": decision,
        "feature_snapshot_status": snapshot_status,
        "route": route,
    })
    return normalized


def read_csv_rows(path: Path, source: str) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [normalized_decision_row(row, source, idx) for idx, row in enumerate(reader)]


def parse_kv_details(details: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(details)}


def read_log_rows(path: Path, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line_no, line in enumerate(handle, start=1):
            match = SETUP_FILTER_RE.search(line)
            if not match:
                continue
            raw = parse_kv_details(match.group("details"))
            raw["passed"] = match.group(1).lower()
            raw["line"] = str(line_no)
            rows.append(normalized_decision_row(raw, source, len(rows)))
    return rows


def load_rows(csv_path: Path | None, log_path: Path | None, source: str) -> tuple[list[dict[str, Any]], str]:
    if csv_path and log_path:
        raise ValueError(f"Supply either --{source}-decisions or --{source}-log, not both")
    if csv_path:
        if not csv_path.is_file():
            raise FileNotFoundError(csv_path)
        return read_csv_rows(csv_path, source), str(csv_path)
    if log_path:
        if not log_path.is_file():
            raise FileNotFoundError(log_path)
        return read_log_rows(log_path, source), str(log_path)
    raise ValueError(f"Missing {source} input; supply --{source}-decisions or --{source}-log")


def unique_by_key(rows: list[dict[str, Any]], warnings: list[str], label: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_keys = 0
    missing_key_rows = 0
    for row in rows:
        key = (row["symbol"], row["side"], row["arm_epoch"])
        if not all(key):
            missing_key_rows += 1
            key = (f"__index_{row['_index']}", "", "")
        if key in out:
            duplicate_keys += 1
            continue
        out[key] = row
    if missing_key_rows:
        warnings.append(f"{label} rows with incomplete symbol/side/epoch keys were paired by index fallback: {missing_key_rows}")
    if duplicate_keys:
        warnings.append(f"{label} duplicate decision keys dropped: {duplicate_keys}")
    return out


def snapshot_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [str(row.get("feature_snapshot_status") or "") for row in rows]
    present = [status for status in statuses if status]
    hits = sum(1 for status in present if status == "hit")
    misses = sum(1 for status in present if status == "miss")
    disabled = sum(1 for status in present if status == "disabled")
    return {
        "telemetry_rows": len(present),
        "hit_rows": hits,
        "miss_rows": misses,
        "disabled_rows": disabled,
        "hit_rate": (hits / len(present)) if present else 0.0,
    }


def compare_rows(paper_rows: list[dict[str, Any]], shadow_rows: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    paper_by_key = unique_by_key(paper_rows, warnings, "paper")
    shadow_by_key = unique_by_key(shadow_rows, warnings, "shadow")
    shared_keys = sorted(set(paper_by_key) & set(shadow_by_key))
    paper_only = len(set(paper_by_key) - set(shadow_by_key))
    shadow_only = len(set(shadow_by_key) - set(paper_by_key))

    probability_drifts: list[float] = []
    comparable_decisions = 0
    decision_mismatches = 0
    comparable_routes = 0
    route_mismatches = 0
    comparable_snapshot_statuses = 0
    snapshot_status_mismatches = 0
    largest_examples: list[dict[str, Any]] = []

    for key in shared_keys:
        paper = paper_by_key[key]
        shadow = shadow_by_key[key]
        paper_prob = paper.get("probability")
        shadow_prob = shadow.get("probability")
        abs_drift: float | None = None
        if paper_prob is not None and shadow_prob is not None:
            abs_drift = abs(float(paper_prob) - float(shadow_prob))
            probability_drifts.append(abs_drift)
        paper_decision = paper.get("decision")
        shadow_decision = shadow.get("decision")
        if paper_decision is not None and shadow_decision is not None:
            comparable_decisions += 1
            if bool(paper_decision) != bool(shadow_decision):
                decision_mismatches += 1
        paper_route = str(paper.get("route") or "")
        shadow_route = str(shadow.get("route") or "")
        if paper_route or shadow_route:
            comparable_routes += 1
            if paper_route != shadow_route:
                route_mismatches += 1
        paper_snapshot = str(paper.get("feature_snapshot_status") or "")
        shadow_snapshot = str(shadow.get("feature_snapshot_status") or "")
        if paper_snapshot or shadow_snapshot:
            comparable_snapshot_statuses += 1
            if paper_snapshot != shadow_snapshot:
                snapshot_status_mismatches += 1
        if abs_drift is not None:
            largest_examples.append({
                "symbol": key[0],
                "side": key[1],
                "arm_epoch": key[2],
                "paper_probability": paper_prob,
                "shadow_probability": shadow_prob,
                "abs_probability_drift": abs_drift,
                "paper_decision": paper_decision,
                "shadow_decision": shadow_decision,
                "paper_feature_snapshot": paper_snapshot,
                "shadow_feature_snapshot": shadow_snapshot,
            })

    largest_examples = sorted(largest_examples, key=lambda row: row["abs_probability_drift"], reverse=True)[: args.top]
    max_probability_drift = max(probability_drifts) if probability_drifts else 0.0
    mean_probability_drift = statistics.fmean(probability_drifts) if probability_drifts else 0.0
    decision_mismatch_rate = (decision_mismatches / comparable_decisions) if comparable_decisions else 0.0
    route_mismatch_rate = (route_mismatches / comparable_routes) if comparable_routes else 0.0
    snapshot_status_mismatch_rate = (snapshot_status_mismatches / comparable_snapshot_statuses) if comparable_snapshot_statuses else 0.0

    paper_snapshot_metrics = snapshot_metrics(paper_rows)
    shadow_snapshot_metrics = snapshot_metrics(shadow_rows)

    if len(shared_keys) < args.min_paired_rows:
        issues.append(f"paired_rows {len(shared_keys)} < minimum {args.min_paired_rows}")
    if max_probability_drift > args.max_probability_drift:
        issues.append(f"max_probability_drift {max_probability_drift:.6f} > maximum {args.max_probability_drift:.6f}")
    if decision_mismatch_rate > args.max_decision_mismatch_rate:
        issues.append(f"decision_mismatch_rate {decision_mismatch_rate:.6f} > maximum {args.max_decision_mismatch_rate:.6f}")
    if snapshot_status_mismatch_rate > args.max_snapshot_status_mismatch_rate:
        issues.append(f"snapshot_status_mismatch_rate {snapshot_status_mismatch_rate:.6f} > maximum {args.max_snapshot_status_mismatch_rate:.6f}")
    for label, metrics in (("paper", paper_snapshot_metrics), ("shadow", shadow_snapshot_metrics)):
        if metrics["hit_rate"] < args.min_feature_snapshot_hit_rate:
            issues.append(f"{label}_feature_snapshot_hit_rate {metrics['hit_rate']:.6f} < minimum {args.min_feature_snapshot_hit_rate:.6f}")
    if paper_only or shadow_only:
        warnings.append(f"unpaired decision rows paper_only={paper_only} shadow_only={shadow_only}")
    if not probability_drifts:
        warnings.append("no comparable probability columns found; decision drift was still checked where possible")
    if comparable_snapshot_statuses == 0:
        warnings.append("no featureSnapshot telemetry found; run with current Java logs or decision CSV snapshot-status columns to validate snapshot coverage")

    metrics: dict[str, Any] = {
        "paper_rows": len(paper_rows),
        "shadow_rows": len(shadow_rows),
        "paired_rows": len(shared_keys),
        "paper_only_rows": paper_only,
        "shadow_only_rows": shadow_only,
        "probability_pairs": len(probability_drifts),
        "max_probability_drift": max_probability_drift,
        "mean_probability_drift": mean_probability_drift,
        "p95_probability_drift": percentile(probability_drifts, 0.95),
        "decision_pairs": comparable_decisions,
        "decision_mismatches": decision_mismatches,
        "decision_mismatch_rate": decision_mismatch_rate,
        "route_pairs": comparable_routes,
        "route_mismatches": route_mismatches,
        "route_mismatch_rate": route_mismatch_rate,
        "snapshot_status_pairs": comparable_snapshot_statuses,
        "snapshot_status_mismatches": snapshot_status_mismatches,
        "snapshot_status_mismatch_rate": snapshot_status_mismatch_rate,
        "paper_feature_snapshot": paper_snapshot_metrics,
        "shadow_feature_snapshot": shadow_snapshot_metrics,
        "largest_probability_drifts": largest_examples,
    }
    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    return gate("paper_shadow_event_snapshot_drift", status, "Paper/shadow decision, probability, and event-snapshot drift checked.", metrics=metrics, issues=issues, warnings=warnings)


def write_reports(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "paper_shadow_drift_report.json"
    md_path = output_dir / "paper_shadow_drift_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Paper/shadow event snapshot drift report",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        f"Overall status: **{report['overall_status']}**",
        "",
        "| Gate | Status | Summary |",
        "|---|---:|---|",
    ]
    for item in report["gates"]:
        lines.append(f"| `{item['name']}` | **{item['status']}** | {item['summary']} |")
    for item in report["gates"]:
        lines.extend(["", f"## `{item['name']}`", "", f"Status: **{item['status']}**", ""])
        if item.get("issues"):
            lines.append("Issues:")
            for issue in item["issues"]:
                lines.append(f"- {issue}")
            lines.append("")
        if item.get("warnings"):
            lines.append("Warnings:")
            for warning in item["warnings"]:
                lines.append(f"- {warning}")
            lines.append("")
        lines.append("Key metrics:")
        lines.append("```json")
        lines.append(json.dumps(item.get("metrics", {}), indent=2, sort_keys=True)[:12000])
        lines.append("```")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def print_gate_summaries(report: dict[str, Any]) -> None:
    for item in report["gates"]:
        print(f"GATE {item['name']} status={item['status']} issues={len(item['issues'])} warnings={len(item['warnings'])}")
        for issue in item.get("issues", []):
            print(f"ISSUE {item['name']} {issue}")
        for warning in item.get("warnings", []):
            print(f"WARNING {item['name']} {warning}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-decisions", default="", help="Paper decision CSV. Mutually exclusive with --paper-log.")
    parser.add_argument("--shadow-decisions", default="", help="Shadow decision CSV. Mutually exclusive with --shadow-log.")
    parser.add_argument("--paper-log", default="", help="Paper Java log containing SETUP_FILTER_PASSES lines.")
    parser.add_argument("--shadow-log", default="", help="Shadow Java log containing SETUP_FILTER_PASSES lines.")
    parser.add_argument("--output-dir", default="runtime/reports/paper_shadow_drift")
    parser.add_argument("--min-paired-rows", type=int, default=1)
    parser.add_argument("--max-probability-drift", type=float, default=0.02)
    parser.add_argument("--max-decision-mismatch-rate", type=float, default=0.01)
    parser.add_argument("--max-snapshot-status-mismatch-rate", type=float, default=0.01)
    parser.add_argument("--min-feature-snapshot-hit-rate", type=float, default=0.95)
    parser.add_argument("--top", type=int, default=20, help="Number of largest-drift examples to include.")
    parser.add_argument("--fail-on-no-go", action="store_true", help="Exit non-zero if the drift gate is NO-GO.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        paper_rows, paper_source = load_rows(repo_path(args.paper_decisions), repo_path(args.paper_log), "paper")
        shadow_rows, shadow_source = load_rows(repo_path(args.shadow_decisions), repo_path(args.shadow_log), "shadow")
    except Exception as exc:
        failed_gate = gate(
            "paper_shadow_event_snapshot_drift",
            "NO-GO",
            "Paper/shadow drift inputs could not be loaded.",
            metrics={
                "paper_decisions": args.paper_decisions,
                "shadow_decisions": args.shadow_decisions,
                "paper_log": args.paper_log,
                "shadow_log": args.shadow_log,
            },
            issues=[str(exc)],
        )
        report = {
            "schema_version": "paper_shadow_event_snapshot_drift_v1",
            "generated_at_utc": utc_now(),
            "overall_status": "NO-GO",
            "inputs": {},
            "gates": [failed_gate],
        }
        output_dir = repo_path(args.output_dir) or (REPO_ROOT / "runtime" / "reports" / "paper_shadow_drift")
        json_path, md_path = write_reports(report, output_dir)
        print(f"PAPER_SHADOW_DRIFT status=NO-GO json={json_path} markdown={md_path}")
        print_gate_summaries(report)
        return 1 if args.fail_on_no_go else 0

    drift_gate = compare_rows(paper_rows, shadow_rows, args)
    report = {
        "schema_version": "paper_shadow_event_snapshot_drift_v1",
        "generated_at_utc": utc_now(),
        "overall_status": overall_status([drift_gate]),
        "inputs": {"paper": paper_source, "shadow": shadow_source},
        "gates": [drift_gate],
    }
    output_dir = repo_path(args.output_dir) or (REPO_ROOT / "runtime" / "reports" / "paper_shadow_drift")
    json_path, md_path = write_reports(report, output_dir)
    print(f"PAPER_SHADOW_DRIFT status={report['overall_status']} json={json_path} markdown={md_path}")
    print_gate_summaries(report)
    if args.fail_on_no_go and report["overall_status"] == "NO-GO":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
