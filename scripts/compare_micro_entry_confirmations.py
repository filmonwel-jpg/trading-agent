#!/usr/bin/env python3
"""Compare Java no-trade micro-entry confirmations against offline policy decisions.

The sidecar downstream setup-filter replay emits successful no-trade confirmations as
lines like:

    Research no-trade long confirmation symbol=TQQQ armEpoch=... confirmEpoch=... prob=... threshold=...

Offline policy decisions are read from ``downstream_filter_replay_policy_decisions.csv``.
The selected micro threshold row is matched by ``policy_micro_threshold`` when present,
falling back to ``counterfactual_micro_threshold`` for older artifacts.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

NO_TRADE_CONFIRM_RE = re.compile(
    r"Research no-trade (long|short) confirmation symbol=([A-Z0-9_.-]+)\s+"
    r"armEpoch=(\d+)\s+confirmEpoch=(\d+)\s+prob=([-+0-9.Ee]+)\s+threshold=([-+0-9.Ee]+)"
)

KEY_COLUMNS = ["symbol", "side", "arm_epoch"]


def parse_bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    normalized = series.astype(str).str.strip().str.lower()
    return normalized.isin({"1", "1.0", "true", "t", "yes", "y", "pass"})


def parse_java_confirmations(path: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    with path.open(errors="ignore") as handle:
        for line_no, line in enumerate(handle, start=1):
            match = NO_TRADE_CONFIRM_RE.search(line)
            if not match:
                continue
            side, symbol, arm_epoch, confirm_epoch, probability, threshold = match.groups()
            rows.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "arm_epoch": int(arm_epoch),
                    "java_confirm_epoch": int(confirm_epoch),
                    "java_confirm_prob": float(probability),
                    "java_confirm_threshold": float(threshold),
                    "java_line_no": line_no,
                }
            )
    if not rows:
        return pd.DataFrame(columns=KEY_COLUMNS + [
            "java_confirm_epoch",
            "java_confirm_prob",
            "java_confirm_threshold",
            "java_line_no",
        ])
    frame = pd.DataFrame(rows)
    duplicate_count = int(frame.duplicated(KEY_COLUMNS).sum())
    frame = frame.drop_duplicates(KEY_COLUMNS, keep="first").reset_index(drop=True)
    frame.attrs["duplicate_confirm_lines_dropped"] = duplicate_count
    return frame


def load_offline_confirmations(path: Path, micro_threshold: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    decisions = pd.read_csv(path)
    missing = [column for column in KEY_COLUMNS + ["counterfactual_confirms", "filter_pass"] if column not in decisions.columns]
    if missing:
        raise ValueError(f"offline decisions missing required columns: {missing}")

    threshold_column = "policy_micro_threshold" if "policy_micro_threshold" in decisions.columns else "counterfactual_micro_threshold"
    selected_threshold = decisions[threshold_column].astype(float).sub(micro_threshold).abs().le(1e-9)
    filter_pass = parse_bool_series(decisions["filter_pass"])
    selected = decisions[selected_threshold & filter_pass].copy()
    selected["counterfactual_confirms_bool"] = parse_bool_series(selected["counterfactual_confirms"])

    selected_duplicate_count = int(selected.duplicated(KEY_COLUMNS).sum())
    selected = selected.drop_duplicates(KEY_COLUMNS, keep="first").reset_index(drop=True)
    selected.attrs["selected_duplicate_rows_dropped"] = selected_duplicate_count
    selected.attrs["threshold_column"] = threshold_column

    confirmed = selected[selected["counterfactual_confirms_bool"]].copy()
    confirmed_duplicate_count = int(confirmed.duplicated(KEY_COLUMNS).sum())
    confirmed = confirmed.drop_duplicates(KEY_COLUMNS, keep="first").reset_index(drop=True)
    confirmed.attrs["confirmed_duplicate_rows_dropped"] = confirmed_duplicate_count
    confirmed.attrs["threshold_column"] = threshold_column
    return selected, confirmed


def existing_columns(frame: pd.DataFrame, candidates: Iterable[str]) -> list[str]:
    return [column for column in candidates if column in frame.columns]


def print_group_counts(title: str, frame: pd.DataFrame) -> None:
    print(title)
    if frame.empty:
        print("<empty>")
        return
    print(frame.groupby(["symbol", "side"]).size().to_string())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay-log", required=True, type=Path)
    parser.add_argument("--offline-decisions", required=True, type=Path)
    parser.add_argument("--micro-threshold", type=float, default=0.30)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    java = parse_java_confirmations(args.replay_log)
    offline_selected, offline_confirmed = load_offline_confirmations(args.offline_decisions, args.micro_threshold)

    merge_columns = KEY_COLUMNS + existing_columns(
        offline_confirmed,
        [
            "confirm_eval_epoch",
            "confirm_micro_prob",
            "confirm_delay_seconds",
            "expected_net_r",
            "outcome_status",
            "policy_micro_threshold",
            "counterfactual_micro_threshold",
        ],
    )
    merged = offline_confirmed[merge_columns].merge(java, on=KEY_COLUMNS, how="outer", indicator=True)

    print(f"micro_threshold={args.micro_threshold:.4f}")
    print(f"threshold_column={offline_confirmed.attrs.get('threshold_column', '<unknown>')}")
    print(
        f"offline_filter_pass_rows={len(offline_selected)} "
        f"offline_confirms={len(offline_confirmed)} "
        f"java_confirm_rows={len(java)} "
        f"merged_rows={len(merged)}"
    )
    if offline_selected.attrs.get("selected_duplicate_rows_dropped", 0):
        print(f"offline_selected_duplicate_rows_dropped={offline_selected.attrs['selected_duplicate_rows_dropped']}")
    if offline_confirmed.attrs.get("confirmed_duplicate_rows_dropped", 0):
        print(f"offline_confirmed_duplicate_rows_dropped={offline_confirmed.attrs['confirmed_duplicate_rows_dropped']}")
    if java.attrs.get("duplicate_confirm_lines_dropped", 0):
        print(f"java_duplicate_confirm_lines_dropped={java.attrs['duplicate_confirm_lines_dropped']}")

    print("merge_counts:")
    print(merged["_merge"].value_counts().to_string())
    print()
    print_group_counts("offline confirms by symbol/side:", offline_confirmed)
    print()
    print_group_counts("java confirms by symbol/side:", java)

    left_only = merged[merged["_merge"].eq("left_only")].copy()
    right_only = merged[merged["_merge"].eq("right_only")].copy()
    if not left_only.empty:
        print()
        print("offline_confirmed_missing_in_java:")
        missing_columns = KEY_COLUMNS + existing_columns(
            left_only,
            ["confirm_eval_epoch", "confirm_micro_prob", "confirm_delay_seconds", "expected_net_r", "outcome_status"],
        )
        print(left_only.sort_values(KEY_COLUMNS).head(args.top)[missing_columns].to_string(index=False))
    if not right_only.empty:
        print()
        print("java_confirmed_not_in_offline:")
        extra_columns = KEY_COLUMNS + existing_columns(
            right_only,
            ["java_confirm_epoch", "java_confirm_prob", "java_confirm_threshold", "java_line_no"],
        )
        print(right_only.sort_values(KEY_COLUMNS).head(args.top)[extra_columns].to_string(index=False))

    both = merged[merged["_merge"].eq("both")].copy()
    if not both.empty and "confirm_eval_epoch" in both.columns:
        both["confirm_epoch_delta"] = both["java_confirm_epoch"] - both["confirm_eval_epoch"].astype(float)
        print()
        print(
            "matched_confirm_epoch_delta "
            f"max_abs={both['confirm_epoch_delta'].abs().max():.3f} "
            f"mean={both['confirm_epoch_delta'].mean():.3f} "
            f"median={both['confirm_epoch_delta'].median():.3f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
