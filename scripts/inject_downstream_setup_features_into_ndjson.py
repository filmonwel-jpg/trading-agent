#!/usr/bin/env python3
"""Inject downstream setup-filter sidecar features into Databento NDJSON events.

This creates a replay fixture for the production event-carried snapshot path:
sidecar CSV rows keyed by (Symbol, SetupEpochSec) are embedded into matching
`equity_bar` events as `enriched_features`, with `FeatureSnapshotEpochSec`
metadata. The Java replay should then be run without
`--downstream-setup-filter-features-csv` so the only exact feature source is the
event itself.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, TextIO


SNAPSHOT_ALIASES = ("enriched_features", "EnrichedFeatures", "feature_snapshot", "FeatureSnapshot", "featureSnapshot")
SYMBOL_COLUMNS = ("Symbol", "symbol")
EPOCH_COLUMNS = ("SetupEpochSec", "setup_epoch_sec", "FeatureSnapshotEpochSec", "feature_snapshot_epoch_sec", "arm_epoch", "armEpoch")
SIDE_COLUMNS = ("SetupSide", "setup_side", "side")
EXCLUDED_PREFIXES = (
    "Label_",
    "Expected_",
    "Max_Future_Micro_",
    "Best_",
)
EXCLUDED_COLUMNS = {
    "training_rows_schema_version",
    "Label_Version",
    "arm_id",
    "Symbol",
    "symbol",
    "SetupEpochSec",
    "setup_epoch_sec",
    "SetupTimeUtc",
    "setup_time_utc",
    "SetupSide",
    "setup_side",
    "MicroEvalWithinTtlCount",
    "join_epoch_delta_seconds",
    "join_abs_epoch_delta_seconds",
}


@dataclass(frozen=True)
class SnapshotRow:
    symbol: str
    epoch: int
    side: str
    features: dict[str, float]
    arm_id: str = ""

def open_text(path: Path, mode: str) -> TextIO:
    if "b" in mode:
        raise ValueError("open_text expects a text mode")
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8", newline="")
    return path.open(mode, encoding="utf-8", newline="")


def first_present(row: dict[str, Any], candidates: Iterable[str]) -> Any:
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def parse_epoch(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).strip())
        if not math.isfinite(parsed) or parsed <= 0:
            return None
        return int(parsed)
    except (TypeError, ValueError):
        return None


def parse_finite_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def resolve_manifest_relative(manifest_dir: Path, file_name: str, fallback_path: str) -> Path:
    if file_name and file_name.strip():
        return (manifest_dir / file_name.strip()).resolve()
    fallback = Path((fallback_path or "").strip())
    if fallback.is_absolute():
        return fallback.resolve()
    relative_to_manifest = (manifest_dir / fallback).resolve()
    if relative_to_manifest.is_file():
        return relative_to_manifest
    return fallback.resolve()


def load_feature_columns_from_schema(schema_path: Path) -> tuple[list[str], str]:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    input_contract = schema.get("input")
    if not isinstance(input_contract, dict):
        raise ValueError(f"feature schema missing input object: {schema_path}")
    raw_columns = input_contract.get("feature_columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise ValueError(f"feature schema missing input.feature_columns: {schema_path}")
    columns = [str(column) for column in raw_columns]
    schema_version = str(schema.get("schema_version") or "").strip()
    return columns, schema_version


def load_feature_columns(manifest_path: Path | None, schema_paths: list[Path]) -> tuple[list[str], str, list[str]]:
    columns: list[str] = []
    schema_versions: list[str] = []
    sources: list[str] = []

    def add_schema(path: Path) -> None:
        schema_columns, schema_version = load_feature_columns_from_schema(path)
        sources.append(str(path))
        if schema_version and schema_version not in schema_versions:
            schema_versions.append(schema_version)
        for column in schema_columns:
            if column not in columns:
                columns.append(column)

    if manifest_path is not None:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        routes = manifest.get("routes")
        if not isinstance(routes, list) or not routes:
            raise ValueError(f"downstream route manifest missing routes list: {manifest_path}")
        manifest_dir = manifest_path.parent
        for route in routes:
            if not isinstance(route, dict):
                continue
            schema_path = resolve_manifest_relative(
                manifest_dir,
                str(route.get("feature_schema_filename") or ""),
                str(route.get("feature_schema") or ""),
            )
            add_schema(schema_path)

    for schema_path in schema_paths:
        add_schema(schema_path)

    return columns, (schema_versions[0] if schema_versions else ""), sources


def should_infer_feature(column: str) -> bool:
    if column in EXCLUDED_COLUMNS:
        return False
    return not any(column.startswith(prefix) for prefix in EXCLUDED_PREFIXES)


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side in {"buy", "long_entry"}:
        return "long"
    if side in {"sell", "short_entry"}:
        return "short"
    return side


def load_sidecar_snapshots(sidecar_csv: Path, feature_columns: list[str], duplicate_policy: str) -> tuple[dict[tuple[str, int], SnapshotRow], dict[str, Any]]:
    snapshots: dict[tuple[str, int], SnapshotRow] = {}
    duplicate_keys: Counter[tuple[str, int]] = Counter()
    missing_key_rows = 0
    rows_read = 0
    rows_without_features = 0
    missing_feature_values = 0
    inferred_mode = not feature_columns

    with sidecar_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"empty sidecar CSV: {sidecar_csv}")
        inferred_columns = [column for column in reader.fieldnames if should_infer_feature(column)]
        selected_columns = feature_columns or inferred_columns
        for row in reader:
            rows_read += 1
            symbol = str(first_present(row, SYMBOL_COLUMNS) or "").strip().upper()
            epoch = parse_epoch(first_present(row, EPOCH_COLUMNS))
            if not symbol or epoch is None:
                missing_key_rows += 1
                continue
            features: dict[str, float] = {}
            for column in selected_columns:
                parsed = parse_finite_float(row.get(column))
                if parsed is None:
                    missing_feature_values += 1
                    continue
                features[column] = parsed
            if not features:
                rows_without_features += 1
                continue
            key = (symbol, epoch)
            if key in snapshots:
                duplicate_keys[key] += 1
                if duplicate_policy == "error":
                    continue
                if duplicate_policy == "first":
                    continue
            snapshots[key] = SnapshotRow(
                symbol=symbol,
                epoch=epoch,
                side=normalize_side(first_present(row, SIDE_COLUMNS)),
                features=features,
                arm_id=str(row.get("arm_id") or "").strip(),
            )

    duplicate_count = sum(duplicate_keys.values())
    if duplicate_count and duplicate_policy == "error":
        examples = ", ".join(f"{symbol}:{epoch}" for (symbol, epoch), _ in duplicate_keys.most_common(5))
        raise ValueError(f"duplicate sidecar Symbol/SetupEpochSec keys are not supported by epoch-keyed event snapshots: {duplicate_count} duplicates; examples={examples}")

    summary = {
        "sidecar_rows_read": rows_read,
        "snapshot_rows_loaded": len(snapshots),
        "sidecar_rows_missing_key": missing_key_rows,
        "sidecar_rows_without_features": rows_without_features,
        "duplicate_sidecar_keys": duplicate_count,
        "feature_columns": selected_columns,
        "feature_column_count": len(selected_columns),
        "missing_feature_values": missing_feature_values,
        "inferred_feature_columns": inferred_mode,
    }
    return snapshots, summary


def existing_snapshot_features(event: dict[str, Any]) -> dict[str, float]:
    features: dict[str, float] = {}
    for alias in SNAPSHOT_ALIASES:
        raw = event.get(alias)
        if isinstance(raw, dict):
            for key, value in raw.items():
                parsed = parse_finite_float(value)
                if parsed is not None:
                    features[str(key)] = parsed
    return features


def normalize_snapshot_container(event: dict[str, Any]) -> None:
    for alias in SNAPSHOT_ALIASES:
        if alias != "enriched_features":
            event.pop(alias, None)


def event_key(event: dict[str, Any]) -> tuple[str, int] | None:
    if str(event.get("event") or "").strip().lower() != "equity_bar":
        return None
    symbol = str(event.get("symbol") or "").strip().upper()
    epoch = parse_epoch(event.get("barEpochSec"))
    if not symbol or epoch is None:
        return None
    return symbol, epoch


def collect_equity_event_epochs(input_events: Path, allow_invalid_json: bool) -> dict[str, list[int]]:
    epochs_by_symbol: dict[str, list[int]] = defaultdict(list)
    with open_text(input_events, "rt") as source:
        for line in source:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                if allow_invalid_json:
                    continue
                raise
            if not isinstance(event, dict):
                continue
            key = event_key(event)
            if key is not None:
                epochs_by_symbol[key[0]].append(key[1])
    return {symbol: sorted(set(epochs)) for symbol, epochs in epochs_by_symbol.items()}


def assign_carrier_events(args: argparse.Namespace, snapshots: dict[tuple[str, int], SnapshotRow]) -> tuple[dict[tuple[str, int], list[SnapshotRow]], dict[str, Any]]:
    input_events = Path(args.input_events)
    carrier_rows: dict[tuple[str, int], list[SnapshotRow]] = defaultdict(list)
    missing: list[SnapshotRow] = []
    lag_values: list[int] = []
    exact_carriers = 0
    prior_finalize_carriers = 0

    epochs_by_symbol = collect_equity_event_epochs(input_events, args.allow_invalid_json)
    for snapshot in snapshots.values():
        epochs = epochs_by_symbol.get(snapshot.symbol, [])
        if not epochs:
            missing.append(snapshot)
            continue
        if args.carrier_policy == "exact":
            index = bisect_left(epochs, snapshot.epoch)
            if index >= len(epochs) or epochs[index] != snapshot.epoch:
                missing.append(snapshot)
                continue
            carrier_epoch = snapshot.epoch
        else:
            start_epoch = snapshot.epoch - args.carrier_lookback_seconds
            index = bisect_left(epochs, start_epoch)
            if index >= len(epochs):
                missing.append(snapshot)
                continue
            carrier_epoch = epochs[index]
            lag = carrier_epoch - snapshot.epoch
            if args.max_carrier_lag_seconds >= 0 and lag > args.max_carrier_lag_seconds:
                missing.append(snapshot)
                continue
        lag = carrier_epoch - snapshot.epoch
        lag_values.append(lag)
        if lag == 0:
            exact_carriers += 1
        elif lag == -args.carrier_lookback_seconds:
            prior_finalize_carriers += 1
        carrier_rows[(snapshot.symbol, carrier_epoch)].append(snapshot)

    multi_snapshot_carriers = sum(1 for rows in carrier_rows.values() if len(rows) > 1)
    summary = {
        "carrier_policy": args.carrier_policy,
        "carrier_lookback_seconds": args.carrier_lookback_seconds,
        "max_carrier_lag_seconds": args.max_carrier_lag_seconds,
        "carrier_rows_assigned": sum(len(rows) for rows in carrier_rows.values()),
        "unique_carrier_events": len(carrier_rows),
        "multi_snapshot_carrier_events": multi_snapshot_carriers,
        "missing_carrier_rows": len(missing),
        "carrier_lag_seconds_min": min(lag_values) if lag_values else 0,
        "carrier_lag_seconds_max": max(lag_values) if lag_values else 0,
        "carrier_lag_seconds_mean": (sum(lag_values) / len(lag_values)) if lag_values else 0.0,
        "exact_carriers": exact_carriers,
        "prior_finalize_carriers": prior_finalize_carriers,
        "missing_carrier_examples": [
            {"symbol": row.symbol, "epoch": row.epoch, "side": row.side} for row in missing[:20]
        ],
    }
    return dict(carrier_rows), summary


def inject_snapshots(
    args: argparse.Namespace,
    snapshots: dict[tuple[str, int], SnapshotRow],
    carrier_rows: dict[tuple[str, int], list[SnapshotRow]],
    schema_version: str,
) -> dict[str, Any]:
    input_events = Path(args.input_events)
    output_events = Path(args.output_events)
    output_events.parent.mkdir(parents=True, exist_ok=True)

    seen_snapshot_keys: set[tuple[str, int]] = set()
    injected_carrier_keys: set[tuple[str, int]] = set()
    counters: Counter[str] = Counter()
    invalid_json_examples: list[str] = []

    with open_text(input_events, "rt") as source, open_text(output_events, "wt") as target:
        for line_no, line in enumerate(source, start=1):
            counters["lines_read"] += 1
            stripped = line.strip()
            if not stripped:
                target.write(line)
                continue
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError as exc:
                counters["invalid_json_lines"] += 1
                if len(invalid_json_examples) < 5:
                    invalid_json_examples.append(f"line={line_no} error={exc}")
                if not args.allow_invalid_json:
                    raise
                target.write(line)
                continue
            if not isinstance(event, dict):
                counters["non_object_json_lines"] += 1
                target.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")
                continue

            key = event_key(event)
            if key is not None:
                counters["equity_bar_events"] += 1
                carrier_snapshots = carrier_rows.get(key, [])
                if carrier_snapshots:
                    counters["snapshot_events_injected"] += 1
                    counters["snapshot_rows_injected"] += len(carrier_snapshots)
                    injected_carrier_keys.add(key)
                    for snapshot in carrier_snapshots:
                        seen_snapshot_keys.add((snapshot.symbol, snapshot.epoch))
                    if any(alias in event for alias in SNAPSHOT_ALIASES):
                        counters["events_with_existing_snapshot"] += 1
                    existing = existing_snapshot_features(event)
                    normalize_snapshot_container(event)
                    if len(carrier_snapshots) > 1:
                        counters["multi_snapshot_carrier_events_injected"] += 1
                    snapshot = carrier_snapshots[-1]
                    if args.no_overwrite_existing:
                        merged = dict(snapshot.features)
                        merged.update(existing)
                    else:
                        merged = existing
                        merged.update(snapshot.features)
                    event["FeatureSnapshotEpochSec"] = snapshot.epoch
                    event["FeatureSnapshotSchemaVersion"] = args.schema_version or schema_version
                    event["FeatureSnapshotSource"] = args.snapshot_source
                    if snapshot.side:
                        event["FeatureSnapshotSide"] = snapshot.side
                    if snapshot.arm_id:
                        event["FeatureSnapshotArmId"] = snapshot.arm_id
                    event["enriched_features"] = merged
            counters["events_written"] += 1
            target.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")

    missing_keys = sorted(set(snapshots) - seen_snapshot_keys)
    assigned_but_not_seen = sorted(set(carrier_rows) - injected_carrier_keys)
    summary = {
        "input_events": str(input_events),
        "output_events": str(output_events),
        "snapshot_source": args.snapshot_source,
        "schema_version": args.schema_version or schema_version,
        "lines_read": counters["lines_read"],
        "events_written": counters["events_written"],
        "equity_bar_events": counters["equity_bar_events"],
        "snapshot_rows_expected": len(snapshots),
        "snapshot_events_injected": counters["snapshot_events_injected"],
        "snapshot_rows_injected": counters["snapshot_rows_injected"],
        "snapshot_rows_unmatched": len(missing_keys),
        "snapshot_hit_rate_vs_sidecar": (counters["snapshot_rows_injected"] / len(snapshots)) if snapshots else 0.0,
        "multi_snapshot_carrier_events_injected": counters["multi_snapshot_carrier_events_injected"],
        "events_with_existing_snapshot": counters["events_with_existing_snapshot"],
        "invalid_json_lines": counters["invalid_json_lines"],
        "non_object_json_lines": counters["non_object_json_lines"],
        "unmatched_examples": [{"symbol": symbol, "epoch": epoch} for symbol, epoch in missing_keys[:20]],
        "assigned_carrier_events_not_seen": len(assigned_but_not_seen),
        "assigned_carrier_not_seen_examples": [{"symbol": symbol, "epoch": epoch} for symbol, epoch in assigned_but_not_seen[:20]],
        "invalid_json_examples": invalid_json_examples,
    }
    return summary


def default_summary_path(output_events: Path) -> Path:
    return output_events.with_name(output_events.name + ".summary.json")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-events", required=True, help="Source Databento NDJSON/NDJSON.GZ file.")
    parser.add_argument("--sidecar-csv", required=True, help="Setup downstream training/sidecar CSV keyed by Symbol + SetupEpochSec.")
    parser.add_argument("--manifest", default="", help="Downstream setup-filter route manifest containing feature schema paths.")
    parser.add_argument("--feature-schema", action="append", default=[], help="Additional feature schema JSON path. Can be supplied multiple times.")
    parser.add_argument("--output-events", required=True, help="Destination NDJSON/NDJSON.GZ file.")
    parser.add_argument("--summary-json", default="", help="Optional summary JSON path. Defaults to <output-events>.summary.json.")
    parser.add_argument("--schema-version", default="", help="Override FeatureSnapshotSchemaVersion. Defaults to schema_version from feature schema.")
    parser.add_argument("--snapshot-source", default="", help="FeatureSnapshotSource value. Defaults to sidecar CSV basename.")
    parser.add_argument("--duplicate-policy", choices=("error", "first", "last"), default="error", help="How to handle duplicate Symbol/SetupEpochSec sidecar rows.")
    parser.add_argument("--carrier-policy", choices=("first-at-or-after", "exact"), default="first-at-or-after", help="Event that carries each snapshot. first-at-or-after uses SetupEpochSec - carrier-lookback-seconds to match 30s bucket finalization.")
    parser.add_argument("--carrier-lookback-seconds", type=int, default=1, help="For first-at-or-after, begin searching at SetupEpochSec minus this many seconds.")
    parser.add_argument("--max-carrier-lag-seconds", type=int, default=300, help="Maximum carrierEpoch - SetupEpochSec for first-at-or-after. Use -1 for no maximum.")
    parser.add_argument("--allow-multi-snapshot-carrier", action="store_true", help="Allow multiple sidecar rows to map to the same carrier event; the last row wins under the current single-snapshot event contract.")
    parser.add_argument("--no-overwrite-existing", action="store_true", help="Preserve existing event snapshot values when keys overlap sidecar values.")
    parser.add_argument("--allow-unmatched-sidecar", action="store_true", help="Return success even if sidecar rows did not match any equity_bar event.")
    parser.add_argument("--allow-invalid-json", action="store_true", help="Copy invalid JSON lines instead of failing.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sidecar_csv = Path(args.sidecar_csv)
    manifest_path = Path(args.manifest) if args.manifest else None
    schema_paths = [Path(path) for path in args.feature_schema]
    if not sidecar_csv.is_file():
        raise FileNotFoundError(sidecar_csv)
    if manifest_path is not None and not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    for schema_path in schema_paths:
        if not schema_path.is_file():
            raise FileNotFoundError(schema_path)
    if not args.snapshot_source:
        args.snapshot_source = sidecar_csv.name

    feature_columns, schema_version, schema_sources = load_feature_columns(manifest_path, schema_paths)
    snapshots, sidecar_summary = load_sidecar_snapshots(sidecar_csv, feature_columns, args.duplicate_policy)
    carrier_rows, carrier_summary = assign_carrier_events(args, snapshots)
    if carrier_summary["multi_snapshot_carrier_events"] and not args.allow_multi_snapshot_carrier:
        raise ValueError(
            "multiple sidecar snapshots mapped to the same carrier event; "
            f"count={carrier_summary['multi_snapshot_carrier_events']}. "
            "Current Java event contract carries one snapshot per event."
        )
    injection_summary = inject_snapshots(args, snapshots, carrier_rows, schema_version)
    report = {
        "schema_version": "event_snapshot_ndjson_injection_report_v1",
        "sidecar_csv": str(sidecar_csv),
        "manifest": str(manifest_path) if manifest_path else "",
        "feature_schema_sources": schema_sources,
        "sidecar": sidecar_summary,
        "carrier_assignment": carrier_summary,
        "injection": injection_summary,
    }
    summary_path = Path(args.summary_json) if args.summary_json else default_summary_path(Path(args.output_events))
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    unmatched = injection_summary["snapshot_rows_unmatched"]
    status = "PASS" if unmatched == 0 or args.allow_unmatched_sidecar else "NO-GO"
    print(
        "INJECT_EVENT_SNAPSHOTS "
        f"status={status} output={args.output_events} summary={summary_path} "
        f"injected={injection_summary['snapshot_rows_injected']} expected={injection_summary['snapshot_rows_expected']} "
        f"unmatched={unmatched}"
    )
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
