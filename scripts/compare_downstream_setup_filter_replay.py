#!/usr/bin/env python3
"""Compare downstream setup-filter Java replay logs against offline policy decisions.

The controlled Java replay emits `SETUP_FILTER_PASSES` lines. Newer logs may
include `armEpoch=<epoch>` on that line; older logs can be approximately matched
by carrying forward the latest `STRATEGY.BAR` epoch for each symbol.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import pandas as pd

BAR_RE = re.compile(r"STRATEGY\.BAR\] symbol=(\w+) epoch=(\d+)")
SETUP_RE = re.compile(
    r"SETUP_FILTER_PASSES=(PASS|FAIL) \| symbol=(\w+) side=(long|short).*?"
    r" prob=([0-9.]+)"
)
ARM_EPOCH_RE = re.compile(r" armEpoch=(\d+)")


def parse_replay_log(path: Path) -> pd.DataFrame:
    last_epoch_by_symbol: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    with path.open(errors="ignore") as handle:
        for line in handle:
            bar_match = BAR_RE.search(line)
            if bar_match:
                last_epoch_by_symbol[bar_match.group(1)] = int(bar_match.group(2))

            setup_match = SETUP_RE.search(line)
            if not setup_match:
                continue
            status, symbol, side, probability = setup_match.groups()
            explicit_epoch_match = ARM_EPOCH_RE.search(line)
            explicit_epoch = explicit_epoch_match.group(1) if explicit_epoch_match else None
            rows.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "arm_epoch": int(explicit_epoch) if explicit_epoch else last_epoch_by_symbol.get(symbol),
                    "java_prob": float(probability),
                    "java_pass": status == "PASS",
                    "epoch_source": "explicit" if explicit_epoch else "inferred_bar_epoch",
                }
            )
    return pd.DataFrame(rows)


def load_offline_decisions(path: Path) -> pd.DataFrame:
    decisions = pd.read_csv(path)
    required = ["symbol", "side", "arm_epoch", "filter_prob", "filter_pass"]
    missing = [column for column in required if column not in decisions.columns]
    if missing:
        raise ValueError(f"offline decisions missing columns: {missing}")
    selected = decisions[required].copy()
    duplicate_count = int(selected.duplicated(["symbol", "side", "arm_epoch"]).sum())
    selected = selected.drop_duplicates(["symbol", "side", "arm_epoch"], keep="first").reset_index(drop=True)
    selected.attrs["duplicate_rows_dropped"] = duplicate_count
    return selected


def print_counts(java: pd.DataFrame, offline: pd.DataFrame, merged: pd.DataFrame) -> None:
    print(f"java_rows={len(java)} offline_rows={len(offline)} merged_rows={len(merged)}")
    duplicate_rows_dropped = offline.attrs.get("duplicate_rows_dropped", 0)
    if duplicate_rows_dropped:
        print(f"offline_duplicate_rows_dropped={duplicate_rows_dropped}")
    print("merge_counts:")
    print(merged["_merge"].value_counts().to_string())
    print()
    print("java setup totals:")
    print(java["java_pass"].value_counts().rename(index={True: "PASS", False: "FAIL"}).to_string())
    print()
    print("offline setup totals:")
    print(offline["filter_pass"].value_counts().rename(index={True: "PASS", False: "FAIL"}).to_string())
    print()
    print("java by symbol/side:")
    print(java.groupby(["symbol", "side", "java_pass"]).size().unstack(fill_value=0).rename(columns={True: "PASS", False: "FAIL"}).to_string())
    print()
    print("offline by symbol/side:")
    print(offline.groupby(["symbol", "side", "filter_pass"]).size().unstack(fill_value=0).rename(columns={True: "PASS", False: "FAIL"}).to_string())


def print_matched_deltas(merged: pd.DataFrame, top: int) -> None:
    matched = merged[merged["_merge"].eq("both")].copy()
    print()
    print(f"matched_rows={len(matched)}")
    if matched.empty:
        return
    matched["abs_delta"] = (matched["java_prob"] - matched["filter_prob"]).abs()
    disagreements = matched[matched["java_pass"] != matched["filter_pass"]]
    print(
        "matched_pass_counts "
        f"java={int(matched['java_pass'].sum())} offline={int(matched['filter_pass'].sum())} "
        f"disagreements={len(disagreements)}"
    )
    print(
        "prob_delta "
        f"max={matched['abs_delta'].max():.6f} "
        f"mean={matched['abs_delta'].mean():.6f} "
        f"median={matched['abs_delta'].median():.6f}"
    )
    if not disagreements.empty:
        print()
        print("largest probability deltas:")
        columns = ["symbol", "side", "arm_epoch", "java_prob", "filter_prob", "java_pass", "filter_pass", "abs_delta"]
        print(matched.sort_values("abs_delta", ascending=False).head(top)[columns].to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-log", required=True, type=Path)
    parser.add_argument("--offline-decisions", required=True, type=Path)
    parser.add_argument("--top", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    java = parse_replay_log(args.replay_log)
    offline = load_offline_decisions(args.offline_decisions)
    merged = java.merge(offline, on=["symbol", "side", "arm_epoch"], how="outer", indicator=True)
    print_counts(java, offline, merged)
    print_matched_deltas(merged, top=args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
