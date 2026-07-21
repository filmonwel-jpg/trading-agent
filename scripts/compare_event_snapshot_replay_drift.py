#!/usr/bin/env python3
"""Validate an event-carried downstream setup-filter replay log.

The primary no-trade replay gate is:

* Java log has `featureSnapshot=hit` for every downstream setup candidate.
* Java log has `featureSidecar=disabled` when validating the productized path.
* Candidate keys match the sidecar CSV keys when `--strict-no-trade` is used.
* If a reference sidecar replay log is supplied, probabilities and decisions match.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any


SETUP_FILTER_RE = re.compile(r"SETUP_FILTER_PASSES=(PASS|FAIL)\s*\|\s*(?P<details>.*)")
KV_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s|]+)")
KEY_COLUMNS = ("symbol", "side", "arm_epoch")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def parse_epoch(value: Any) -> str:
    parsed = parse_float(value)
    return str(int(parsed)) if parsed is not None else str(value or "").strip()


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side in {"buy", "long_entry"}:
        return "long"
    if side in {"sell", "short_entry"}:
        return "short"
    return side


def parse_kv(details: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(details)}


def row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return str(row.get("symbol") or "").upper(), normalize_side(row.get("side")), parse_epoch(row.get("arm_epoch"))


def parse_setup_log(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line_no, line in enumerate(handle, start=1):
            match = SETUP_FILTER_RE.search(line)
            if not match:
                continue
            details = parse_kv(match.group("details"))
            rows.append(
                {
                    "symbol": str(details.get("symbol") or "").upper(),
                    "side": normalize_side(details.get("side")),
                    "arm_epoch": parse_epoch(details.get("armEpoch") or details.get("arm_epoch")),
                    "passed": match.group(1) == "PASS",
                    "probability": parse_float(details.get("prob")),
                    "threshold": parse_float(details.get("threshold")),
                    "route": str(details.get("route") or ""),
                    "feature_snapshot": str(details.get("featureSnapshot") or ""),
                    "feature_sidecar": str(details.get("featureSidecar") or ""),
                    "line": line_no,
                }
            )
    return rows


def load_sidecar_keys(path: Path) -> set[tuple[str, str, str]]:
    keys: set[tuple[str, str, str]] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("Symbol") or row.get("symbol") or "").strip().upper()
            side = normalize_side(row.get("SetupSide") or row.get("side"))
            epoch = parse_epoch(row.get("SetupEpochSec") or row.get("arm_epoch") or row.get("armEpoch"))
            if symbol and side and epoch:
                keys.add((symbol, side, epoch))
    return keys


def unique_rows(rows: list[dict[str, Any]], label: str, warnings: list[str]) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicate_rows = 0
    missing_key_rows = 0
    for row in rows:
        key = row_key(row)
        if not all(key):
            missing_key_rows += 1
            continue
        if key in out:
            duplicate_rows += 1
            continue
        out[key] = row
    if duplicate_rows:
        warnings.append(f"{label} duplicate setup-filter rows dropped: {duplicate_rows}")
    if missing_key_rows:
        warnings.append(f"{label} setup-filter rows with incomplete keys dropped: {missing_key_rows}")
    return out


def status_counts(rows: list[dict[str, Any]], column: str) -> dict[str, int]:
    return dict(Counter(str(row.get(column) or "") for row in rows))


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * p)))
    return ordered[index]


def compare_replay(args: argparse.Namespace) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    event_rows = parse_setup_log(Path(args.event_log))
    event_by_key = unique_rows(event_rows, "event", warnings)
    sidecar_keys = load_sidecar_keys(Path(args.sidecar_csv)) if args.sidecar_csv else set()
    expected_keys = sidecar_keys if sidecar_keys else set(event_by_key)

    event_keys = set(event_by_key)
    missing_expected = sorted(expected_keys - event_keys)
    extra_event = sorted(event_keys - expected_keys) if sidecar_keys else []
    snapshot_counts = status_counts(event_rows, "feature_snapshot")
    sidecar_counts = status_counts(event_rows, "feature_sidecar")
    snapshot_hits = snapshot_counts.get("hit", 0)
    snapshot_misses = snapshot_counts.get("miss", 0)
    snapshot_disabled = snapshot_counts.get("disabled", 0)
    snapshot_hit_rate = (snapshot_hits / len(event_rows)) if event_rows else 0.0
    sidecar_disabled_rows = sidecar_counts.get("disabled", 0)
    pass_count = sum(1 for row in event_rows if row.get("passed"))
    fail_count = len(event_rows) - pass_count

    reference_metrics: dict[str, Any] = {}
    if args.reference_log:
        reference_rows = parse_setup_log(Path(args.reference_log))
        reference_by_key = unique_rows(reference_rows, "reference", warnings)
        shared_keys = sorted(event_keys & set(reference_by_key))
        probability_drifts: list[float] = []
        decision_mismatches = 0
        largest_drifts: list[dict[str, Any]] = []
        for key in shared_keys:
            event = event_by_key[key]
            reference = reference_by_key[key]
            event_prob = event.get("probability")
            reference_prob = reference.get("probability")
            drift = None
            if event_prob is not None and reference_prob is not None:
                drift = abs(float(event_prob) - float(reference_prob))
                probability_drifts.append(drift)
                largest_drifts.append(
                    {
                        "symbol": key[0],
                        "side": key[1],
                        "arm_epoch": key[2],
                        "event_probability": event_prob,
                        "reference_probability": reference_prob,
                        "abs_probability_drift": drift,
                        "event_passed": event.get("passed"),
                        "reference_passed": reference.get("passed"),
                    }
                )
            if bool(event.get("passed")) != bool(reference.get("passed")):
                decision_mismatches += 1
        max_probability_drift = max(probability_drifts) if probability_drifts else 0.0
        reference_metrics = {
            "reference_rows": len(reference_rows),
            "paired_reference_rows": len(shared_keys),
            "event_only_rows_vs_reference": len(event_keys - set(reference_by_key)),
            "reference_only_rows": len(set(reference_by_key) - event_keys),
            "probability_pairs": len(probability_drifts),
            "max_probability_drift": max_probability_drift,
            "mean_probability_drift": statistics.fmean(probability_drifts) if probability_drifts else 0.0,
            "p95_probability_drift": percentile(probability_drifts, 0.95),
            "decision_mismatches": decision_mismatches,
            "largest_probability_drifts": sorted(largest_drifts, key=lambda row: row["abs_probability_drift"], reverse=True)[: args.top],
        }
        if max_probability_drift > args.max_probability_drift:
            issues.append(f"max_probability_drift {max_probability_drift:.6f} > maximum {args.max_probability_drift:.6f}")
        if decision_mismatches > args.max_decision_mismatches:
            issues.append(f"decision_mismatches {decision_mismatches} > maximum {args.max_decision_mismatches}")
        if reference_metrics["event_only_rows_vs_reference"] or reference_metrics["reference_only_rows"]:
            warnings.append(
                "reference row key mismatch "
                f"event_only={reference_metrics['event_only_rows_vs_reference']} "
                f"reference_only={reference_metrics['reference_only_rows']}"
            )

    if not event_rows:
        issues.append("event replay log contains no SETUP_FILTER_PASSES rows")
    if snapshot_hit_rate < args.min_feature_snapshot_hit_rate:
        issues.append(f"feature_snapshot_hit_rate {snapshot_hit_rate:.6f} < minimum {args.min_feature_snapshot_hit_rate:.6f}")
    if snapshot_misses > args.max_snapshot_miss_rows:
        issues.append(f"feature_snapshot_miss_rows {snapshot_misses} > maximum {args.max_snapshot_miss_rows}")
    if snapshot_disabled > args.max_snapshot_disabled_rows:
        issues.append(f"feature_snapshot_disabled_rows {snapshot_disabled} > maximum {args.max_snapshot_disabled_rows}")
    if args.require_sidecar_disabled and sidecar_disabled_rows != len(event_rows):
        issues.append(f"feature_sidecar_disabled_rows {sidecar_disabled_rows} != event_rows {len(event_rows)}")
    if args.strict_no_trade:
        if missing_expected:
            issues.append(f"missing expected sidecar setup rows in event log: {len(missing_expected)}")
        if extra_event:
            issues.append(f"event log has setup rows not present in sidecar CSV: {len(extra_event)}")

    metrics = {
        "event_rows": len(event_rows),
        "event_unique_keys": len(event_by_key),
        "sidecar_expected_keys": len(sidecar_keys),
        "missing_expected_rows": len(missing_expected),
        "extra_event_rows": len(extra_event),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "feature_snapshot_counts": snapshot_counts,
        "feature_snapshot_hit_rate": snapshot_hit_rate,
        "feature_sidecar_counts": sidecar_counts,
        "sidecar_disabled_rows": sidecar_disabled_rows,
        "missing_expected_examples": [dict(zip(KEY_COLUMNS, key)) for key in missing_expected[: args.top]],
        "extra_event_examples": [dict(zip(KEY_COLUMNS, key)) for key in extra_event[: args.top]],
        "reference": reference_metrics,
    }
    return {
        "name": "event_carried_snapshot_replay_drift",
        "status": "NO-GO" if issues else ("WARN" if warnings else "PASS"),
        "summary": "Event-carried snapshot replay coverage and optional reference parity checked.",
        "metrics": metrics,
        "issues": issues,
        "warnings": warnings,
    }


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "event_snapshot_replay_drift_report.json"
    md_path = output_dir / "event_snapshot_replay_drift_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    gate = report["gate"]
    lines = [
        "# Event-carried snapshot replay drift report",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        f"Overall status: **{report['overall_status']}**",
        "",
        f"Event log: `{report['inputs']['event_log']}`",
        f"Sidecar CSV: `{report['inputs'].get('sidecar_csv', '')}`",
        f"Reference log: `{report['inputs'].get('reference_log', '')}`",
        "",
        f"## `{gate['name']}`",
        "",
        f"Status: **{gate['status']}**",
        "",
    ]
    if gate["issues"]:
        lines.append("Issues:")
        lines.extend(f"- {issue}" for issue in gate["issues"])
        lines.append("")
    if gate["warnings"]:
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in gate["warnings"])
        lines.append("")
    lines.extend(["Key metrics:", "```json", json.dumps(gate["metrics"], indent=2, sort_keys=True)[:16000], "```"])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", required=True, help="Java replay log from event-carried snapshot run.")
    parser.add_argument("--sidecar-csv", default="", help="Expected sidecar CSV keys for strict no-trade coverage checks.")
    parser.add_argument("--reference-log", default="", help="Optional sidecar-exact Java replay log for probability/decision parity.")
    parser.add_argument("--output-dir", default="runtime/reports/event_snapshot_replay_drift")
    parser.add_argument("--strict-no-trade", action="store_true", help="Require event setup row keys to exactly match sidecar CSV keys.")
    parser.add_argument("--require-sidecar-disabled", action="store_true", help="Require every event replay setup row to show featureSidecar=disabled.")
    parser.add_argument("--min-feature-snapshot-hit-rate", type=float, default=0.995)
    parser.add_argument("--max-snapshot-miss-rows", type=int, default=0)
    parser.add_argument("--max-snapshot-disabled-rows", type=int, default=0)
    parser.add_argument("--max-probability-drift", type=float, default=1.0e-4)
    parser.add_argument("--max-decision-mismatches", type=int, default=0)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--fail-on-no-go", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    gate = compare_replay(args)
    report = {
        "schema_version": "event_snapshot_replay_drift_v1",
        "generated_at_utc": utc_now(),
        "overall_status": gate["status"],
        "inputs": {
            "event_log": str(Path(args.event_log)),
            "sidecar_csv": str(Path(args.sidecar_csv)) if args.sidecar_csv else "",
            "reference_log": str(Path(args.reference_log)) if args.reference_log else "",
        },
        "gate": gate,
    }
    json_path, md_path = write_report(report, Path(args.output_dir))
    print(f"EVENT_SNAPSHOT_REPLAY_DRIFT status={report['overall_status']} json={json_path} markdown={md_path}")
    if args.fail_on_no_go and report["overall_status"] == "NO-GO":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
