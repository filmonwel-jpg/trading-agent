#!/usr/bin/env python3
"""Score a downstream setup-filter feature-vector debug line.

Use with logs emitted by `strategy.downstreamSetupFilter.logFeatureVector=true`.
The script scores Java's exact logged feature vector with the side-specific pickle
and optionally compares it with the matching offline training row.
"""
from __future__ import annotations

import argparse
import math
import pickle
import re
from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_LINE_RE = re.compile(
    r"AI\.DOWNSTREAM_SETUP_FILTER\.FEATURES\].*?"
    r"symbol=(?P<symbol>\w+) side=(?P<side>long|short) armEpoch=(?P<arm_epoch>\d+) .*?features=(?P<features>.*)$"
)


def parse_feature_pairs(raw: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for token in raw.strip().split(","):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        key = key.strip()
        try:
            parsed = float(value)
        except ValueError:
            parsed = 0.0
        values[key] = parsed if math.isfinite(parsed) else 0.0
    return values


def find_debug_features(log_path: Path, symbol: str, side: str, arm_epoch: int) -> dict[str, float]:
    with log_path.open(errors="ignore") as handle:
        for line in handle:
            match = FEATURE_LINE_RE.search(line)
            if not match:
                continue
            if (
                match.group("symbol") == symbol
                and match.group("side") == side
                and int(match.group("arm_epoch")) == arm_epoch
            ):
                return parse_feature_pairs(match.group("features"))
    raise ValueError(f"no feature-debug line found for {symbol}.{side} arm_epoch={arm_epoch}")


def load_bundle(filter_bundle_dir: Path, side: str) -> dict:
    with (filter_bundle_dir / f"{side}_downstream_setup_filter.pkl").open("rb") as handle:
        return pickle.load(handle)


def vector_from_mapping(feature_columns: list[str], values: dict[str, float]) -> np.ndarray:
    return np.asarray([[values.get(column, 0.0) for column in feature_columns]], dtype=np.float32)


def positive_class_index(model) -> int:
    classes = list(getattr(model, "classes_", []))
    for index, value in enumerate(classes):
        try:
            if int(value) == 1:
                return index
        except (TypeError, ValueError):
            if str(value) == "1":
                return index
    return max(0, len(classes) - 1) if classes else 1


def score(bundle: dict, values: dict[str, float]) -> float:
    matrix = vector_from_mapping(list(bundle["feature_columns"]), values)
    return float(bundle["model"].predict_proba(matrix)[:, positive_class_index(bundle["model"])][0])


def offline_row_values(training_rows_csv: Path, symbol: str, side: str, arm_epoch: int) -> dict[str, float]:
    rows = pd.read_csv(training_rows_csv)
    row = rows[
        rows["Symbol"].astype(str).eq(symbol)
        & rows["SetupSide"].astype(str).str.lower().eq(side)
        & pd.to_numeric(rows["SetupEpochSec"], errors="coerce").eq(arm_epoch)
    ]
    if row.empty:
        raise ValueError(f"no offline training row found for {symbol}.{side} arm_epoch={arm_epoch}")
    record = row.iloc[0].to_dict()
    values: dict[str, float] = {}
    for key, value in record.items():
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        values[key] = parsed if math.isfinite(parsed) else 0.0
    return values


def print_top_deltas(feature_columns: list[str], left: dict[str, float], right: dict[str, float], top: int) -> None:
    rows = []
    for column in feature_columns:
        java_value = float(left.get(column, 0.0))
        offline_value = float(right.get(column, 0.0))
        rows.append((column, java_value, offline_value, abs(java_value - offline_value)))
    rows.sort(key=lambda item: item[3], reverse=True)
    print("largest_feature_deltas:")
    for column, java_value, offline_value, delta in rows[:top]:
        print(f"{column:36s} java={java_value:.10g} offline={offline_value:.10g} abs_delta={delta:.10g}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-log", required=True, type=Path)
    parser.add_argument("--filter-bundle-dir", required=True, type=Path)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["long", "short"])
    parser.add_argument("--arm-epoch", required=True, type=int)
    parser.add_argument("--training-rows-csv", type=Path)
    parser.add_argument("--top", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = load_bundle(args.filter_bundle_dir, args.side)
    java_values = find_debug_features(args.replay_log, args.symbol, args.side, args.arm_epoch)
    java_prob = score(bundle, java_values)
    print(f"java_vector_pickle_prob={java_prob:.12f}")

    if args.training_rows_csv:
        offline_values = offline_row_values(args.training_rows_csv, args.symbol, args.side, args.arm_epoch)
        offline_prob = score(bundle, offline_values)
        print(f"offline_row_pickle_prob={offline_prob:.12f}")
        print_top_deltas(list(bundle["feature_columns"]), java_values, offline_values, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
