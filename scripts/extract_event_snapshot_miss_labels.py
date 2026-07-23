#!/usr/bin/env python3
"""Extract event-carried featureSnapshot=miss setup rows into minimal labels CSV.

This is a recovery helper for long event-carried replay runs where the strict
no-trade drift gate reports extra setup-filter rows that were not present in the
sidecar generated from the original counterfactual replay. The output is a
minimal `setup_downstream_confirmable_labels_v1`-compatible CSV that can be fed
to `scripts/build_downstream_setup_training_rows.py` to recover 30s feature rows
for those missing setup candidates.

The generated label target columns are intentionally neutral zeros; this file is
for feature-snapshot transport recovery, not for supervised model training.
"""
from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SETUP_FILTER_RE = re.compile(r"SETUP_FILTER_PASSES=(PASS|FAIL)\s*\|\s*(?P<details>.*)")
KV_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s|]+)")
LABEL_SCHEMA_VERSION = "setup_downstream_confirmable_labels_v1"

OUTPUT_COLUMNS = [
    "Label_Version",
    "arm_id",
    "Symbol",
    "SetupEpochSec",
    "SetupTimeUtc",
    "SetupSide",
    "Label_Long_Setup_DownstreamPositive",
    "Label_Short_Setup_DownstreamPositive",
    "Expected_Long_Setup_DownstreamNetR",
    "Expected_Short_Setup_DownstreamNetR",
    "Max_Future_Micro_Long_Prob",
    "Max_Future_Micro_Short_Prob",
    "Best_Entry_Delay_Seconds",
    "Recovery_Source",
    "Recovery_Reason",
    "Recovery_Setup_Filter_Status",
    "Recovery_Setup_Filter_Prob",
    "Recovery_Setup_Filter_Threshold",
    "Recovery_Setup_Filter_Route",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_kv(details: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in KV_RE.finditer(details)}


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side in {"buy", "long_entry"}:
        return "long"
    if side in {"sell", "short_entry"}:
        return "short"
    return side


def parse_epoch(value: Any) -> int | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def load_existing_keys(sidecar_csv: Path | None) -> set[tuple[str, int]]:
    if sidecar_csv is None or not sidecar_csv.is_file():
        return set()
    keys: set[tuple[str, int]] = set()
    with sidecar_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("Symbol") or row.get("symbol") or "").strip().upper()
            epoch = parse_epoch(row.get("SetupEpochSec") or row.get("arm_epoch") or row.get("armEpoch"))
            if symbol and epoch is not None:
                keys.add((symbol, epoch))
    return keys


def extract_miss_rows(log_path: Path, existing_keys: set[tuple[str, int]]) -> tuple[list[dict[str, str]], dict[str, int]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, int, str]] = set()
    counters = {
        "setup_filter_rows": 0,
        "snapshot_miss_rows": 0,
        "missing_key_rows": 0,
        "already_in_sidecar_rows": 0,
        "duplicate_rows": 0,
        "output_rows": 0,
    }
    with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            match = SETUP_FILTER_RE.search(line)
            if not match:
                continue
            counters["setup_filter_rows"] += 1
            details = parse_kv(match.group("details"))
            if str(details.get("featureSnapshot") or "").strip().lower() != "miss":
                continue
            counters["snapshot_miss_rows"] += 1
            symbol = str(details.get("symbol") or "").strip().upper()
            side = normalize_side(details.get("side"))
            epoch = parse_epoch(details.get("armEpoch") or details.get("arm_epoch"))
            if not symbol or not side or epoch is None:
                counters["missing_key_rows"] += 1
                continue
            if (symbol, epoch) in existing_keys:
                counters["already_in_sidecar_rows"] += 1
                continue
            key = (symbol, epoch, side)
            if key in seen:
                counters["duplicate_rows"] += 1
                continue
            seen.add(key)
            rows.append({
                "Label_Version": LABEL_SCHEMA_VERSION,
                "arm_id": f"recovery-{symbol}-{side}-{epoch}",
                "Symbol": symbol,
                "SetupEpochSec": str(epoch),
                "SetupTimeUtc": datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "SetupSide": side,
                "Label_Long_Setup_DownstreamPositive": "0",
                "Label_Short_Setup_DownstreamPositive": "0",
                "Expected_Long_Setup_DownstreamNetR": "0.0",
                "Expected_Short_Setup_DownstreamNetR": "0.0",
                "Max_Future_Micro_Long_Prob": "0.0",
                "Max_Future_Micro_Short_Prob": "0.0",
                "Best_Entry_Delay_Seconds": "0",
                "Recovery_Source": str(log_path),
                "Recovery_Reason": "event_snapshot_miss_extra_setup_filter_row",
                "Recovery_Setup_Filter_Status": match.group(1),
                "Recovery_Setup_Filter_Prob": str(details.get("prob") or ""),
                "Recovery_Setup_Filter_Threshold": str(details.get("threshold") or ""),
                "Recovery_Setup_Filter_Route": str(details.get("route") or ""),
            })
    counters["output_rows"] = len(rows)
    return rows, counters


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", required=True, type=Path, help="No-trade event-carried Java log containing SETUP_FILTER_PASSES rows.")
    parser.add_argument("--existing-sidecar", type=Path, default=None, help="Existing setup_downstream_training_rows_v1.csv; used to skip already-covered Symbol/SetupEpochSec keys.")
    parser.add_argument("--output-labels", required=True, type=Path, help="Destination minimal labels CSV for build_downstream_setup_training_rows.py.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    existing_keys = load_existing_keys(args.existing_sidecar)
    rows, counters = extract_miss_rows(args.event_log, existing_keys)
    args.output_labels.parent.mkdir(parents=True, exist_ok=True)
    with args.output_labels.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    summary_path = Path(str(args.output_labels) + ".summary.json")
    summary = {
        "generated_at_utc": utc_now(),
        "event_log": str(args.event_log),
        "existing_sidecar": str(args.existing_sidecar) if args.existing_sidecar else "",
        "output_labels": str(args.output_labels),
        "existing_sidecar_keys": len(existing_keys),
        **counters,
    }
    import json
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "EXTRACT_EVENT_SNAPSHOT_MISS_LABELS "
        f"output={args.output_labels} summary={summary_path} "
        f"snapshot_miss_rows={counters['snapshot_miss_rows']} output_rows={counters['output_rows']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

