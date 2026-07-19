#!/usr/bin/env python3
"""Join downstream setup labels back to enriched 30-second setup rows.

The counterfactual report emits one label row per replay setup arm. This script
joins those labels to the full 30s raw/enriched bar cache so downstream setup
filter/retraining jobs can use the same contextual columns as the normal setup
trainer. It intentionally keeps the joined artifact separate from canonical
training data because these labels are replay-derived research evidence.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

LABEL_SCHEMA_VERSION = "setup_downstream_confirmable_labels_v1"
TRAINING_ROWS_SCHEMA_VERSION = "setup_downstream_training_rows_v1"

REQUIRED_LABEL_COLUMNS = {
    "Symbol",
    "SetupEpochSec",
    "SetupSide",
    "Label_Long_Setup_DownstreamPositive",
    "Label_Short_Setup_DownstreamPositive",
    "Expected_Long_Setup_DownstreamNetR",
    "Expected_Short_Setup_DownstreamNetR",
    "Max_Future_Micro_Long_Prob",
    "Max_Future_Micro_Short_Prob",
    "Best_Entry_Delay_Seconds",
}
REQUIRED_30S_COLUMNS = {"Symbol", "Timestamp", "BarEpochSec"}


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def read_30s_input(path: Path) -> pd.DataFrame:
    path = path.expanduser()
    if path.is_dir():
        frames = []
        for csv_path in sorted(path.glob("*_30s_training.csv")):
            frame = pd.read_csv(csv_path)
            if "Symbol" not in frame.columns:
                frame.insert(0, "Symbol", csv_path.name.removesuffix("_30s_training.csv").upper())
            frames.append(frame)
        if not frames:
            raise FileNotFoundError(f"No *_30s_training.csv files found under {path}")
        return pd.concat(frames, ignore_index=True)
    return pd.read_csv(path)


def require_columns(df: pd.DataFrame, columns: set[str], source: str) -> None:
    missing = sorted(columns - set(df.columns))
    if missing:
        raise ValueError(f"{source} missing required columns: {missing}")


def normalize_labels(labels: pd.DataFrame) -> pd.DataFrame:
    require_columns(labels, REQUIRED_LABEL_COLUMNS, "labels CSV")
    out = labels.copy()
    out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
    out["SetupSide"] = out["SetupSide"].astype(str).str.strip().str.lower()
    out["SetupEpochSec"] = pd.to_numeric(out["SetupEpochSec"], errors="coerce")
    out = out[out["Symbol"].ne("") & out["SetupEpochSec"].notna()].copy()
    out["SetupEpochSec"] = out["SetupEpochSec"].astype("int64")
    for col in [
        "Label_Long_Setup_DownstreamPositive",
        "Label_Short_Setup_DownstreamPositive",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int8")
    return out


def normalize_30s_rows(rows_30s: pd.DataFrame) -> pd.DataFrame:
    require_columns(rows_30s, REQUIRED_30S_COLUMNS, "30s CSV")
    out = rows_30s.copy()
    out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
    out["BarEpochSec"] = pd.to_numeric(out["BarEpochSec"], errors="coerce")
    out = out[out["Symbol"].ne("") & out["BarEpochSec"].notna()].copy()
    out["BarEpochSec"] = out["BarEpochSec"].astype("int64")
    return out


def join_labels_to_30s(labels: pd.DataFrame, rows_30s: pd.DataFrame, tolerance_seconds: int) -> pd.DataFrame:
    symbols = sorted(labels["Symbol"].dropna().unique())
    min_epoch = int(labels["SetupEpochSec"].min()) - tolerance_seconds
    max_epoch = int(labels["SetupEpochSec"].max()) + tolerance_seconds
    features = rows_30s[
        rows_30s["Symbol"].isin(symbols)
        & rows_30s["BarEpochSec"].between(min_epoch, max_epoch)
    ].copy()

    merged_frames: list[pd.DataFrame] = []
    for symbol in symbols:
        label_group = labels[labels["Symbol"] == symbol].sort_values("SetupEpochSec").copy()
        feature_group = features[features["Symbol"] == symbol].sort_values("BarEpochSec").copy()
        if feature_group.empty:
            label_group["BarEpochSec"] = pd.NA
            label_group["join_epoch_delta_seconds"] = pd.NA
            label_group["join_abs_epoch_delta_seconds"] = pd.NA
            merged_frames.append(label_group)
            continue
        merged = pd.merge_asof(
            label_group,
            feature_group,
            left_on="SetupEpochSec",
            right_on="BarEpochSec",
            direction="nearest",
            tolerance=max(0, int(tolerance_seconds)),
            suffixes=("", "_30s"),
        )
        merged["join_epoch_delta_seconds"] = merged["SetupEpochSec"] - merged["BarEpochSec"]
        merged["join_abs_epoch_delta_seconds"] = merged["join_epoch_delta_seconds"].abs()
        merged_frames.append(merged)

    if not merged_frames:
        return pd.DataFrame()
    joined = pd.concat(merged_frames, ignore_index=True)
    joined.insert(0, "training_rows_schema_version", TRAINING_ROWS_SCHEMA_VERSION)
    joined["Label_Long_Entry_Downstream"] = joined["Label_Long_Setup_DownstreamPositive"]
    joined["Label_Short_Entry_Downstream"] = joined["Label_Short_Setup_DownstreamPositive"]
    return joined


def summarize(joined: pd.DataFrame, labels: pd.DataFrame, rows_30s: pd.DataFrame, args: argparse.Namespace, outputs: dict[str, Path]) -> dict[str, Any]:
    matched = joined["BarEpochSec"].notna() if "BarEpochSec" in joined.columns else pd.Series(dtype=bool)
    matched_rows = int(matched.sum()) if len(joined) else 0
    unmatched_rows = int((~matched).sum()) if len(joined) else 0
    deltas = pd.to_numeric(joined.get("join_abs_epoch_delta_seconds", pd.Series(dtype=float)), errors="coerce").dropna()
    by_symbol_side = {}
    if len(joined):
        for (symbol, side), group in joined.groupby(["Symbol", "SetupSide"], dropna=False):
            by_symbol_side[f"{symbol}.{side}"] = {
                "rows": int(len(group)),
                "matched_rows": int(group["BarEpochSec"].notna().sum()) if "BarEpochSec" in group else 0,
                "long_positive": int(pd.to_numeric(group.get("Label_Long_Setup_DownstreamPositive", 0), errors="coerce").fillna(0).sum()),
                "short_positive": int(pd.to_numeric(group.get("Label_Short_Setup_DownstreamPositive", 0), errors="coerce").fillna(0).sum()),
            }
    return {
        "generated_at_utc": utc_now(),
        "schema_version": TRAINING_ROWS_SCHEMA_VERSION,
        "input_30s_csv": str(args.input_30s_csv),
        "labels_csv": str(args.labels_csv),
        "outputs": {name: str(path) for name, path in outputs.items()},
        "join_tolerance_seconds": args.join_tolerance_seconds,
        "labels_rows": int(len(labels)),
        "input_30s_rows": int(len(rows_30s)),
        "joined_rows": int(len(joined)),
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "match_rate": matched_rows / len(joined) if len(joined) else 0.0,
        "join_abs_epoch_delta_seconds": {
            "min": float(deltas.min()) if len(deltas) else None,
            "p50": float(deltas.quantile(0.50)) if len(deltas) else None,
            "p90": float(deltas.quantile(0.90)) if len(deltas) else None,
            "max": float(deltas.max()) if len(deltas) else None,
        },
        "positive_counts": {
            "long": int(labels["Label_Long_Setup_DownstreamPositive"].sum()),
            "short": int(labels["Label_Short_Setup_DownstreamPositive"].sum()),
        },
        "by_symbol_side": by_symbol_side,
        "warnings": [
            "Rows are replay setup arms, not all possible 30s bars; use as arm-quality/filter training evidence unless a broader all-candidate replay is generated.",
            "Nearest-epoch join is required because replay arm epochs and 30s BarEpochSec can differ by approximately one second.",
        ],
    }


def write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Downstream Setup Training Rows",
        "",
        f"Generated: `{manifest['generated_at_utc']}`",
        "",
        f"- Schema: `{manifest['schema_version']}`",
        f"- Input 30s CSV: `{manifest['input_30s_csv']}`",
        f"- Labels CSV: `{manifest['labels_csv']}`",
        f"- Join tolerance seconds: `{manifest['join_tolerance_seconds']}`",
        "",
        "## Join summary",
        "",
        f"- Labels rows: `{manifest['labels_rows']}`",
        f"- Input 30s rows: `{manifest['input_30s_rows']}`",
        f"- Joined rows: `{manifest['joined_rows']}`",
        f"- Matched rows: `{manifest['matched_rows']}`",
        f"- Unmatched rows: `{manifest['unmatched_rows']}`",
        f"- Match rate: `{manifest['match_rate']:.6f}`",
        f"- Join abs epoch delta seconds: `{manifest['join_abs_epoch_delta_seconds']}`",
        "",
        "## Positives by symbol/side",
        "",
        "| symbol.side | rows | matched | long positive | short positive |",
        "|---|---:|---:|---:|---:|",
    ]
    for key, row in manifest["by_symbol_side"].items():
        lines.append(f"| {key} | {row['rows']} | {row['matched_rows']} | {row['long_positive']} | {row['short_positive']} |")
    lines.extend([
        "",
        "## Outputs",
        "",
    ])
    for name, output_path in manifest["outputs"].items():
        lines.append(f"- `{name}`: `{output_path}`")
    lines.extend([
        "",
        "Promotion status remains **NO-GO** until retrained models pass the replay/promotion gates.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join setup_downstream_confirmable_labels_v1 rows to enriched 30s setup rows.")
    parser.add_argument("--input-30s-csv", required=True, type=Path, help="Full enriched 30s CSV or directory of *_30s_training.csv files.")
    parser.add_argument("--labels-csv", required=True, type=Path, help="setup_downstream_confirmable_labels_v1.csv from the counterfactual report.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for joined training rows and manifest.")
    parser.add_argument("--join-tolerance-seconds", type=int, default=31, help="Nearest-epoch join tolerance between SetupEpochSec and BarEpochSec.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.labels_csv.is_file():
        raise FileNotFoundError(f"missing labels CSV: {args.labels_csv}")
    if not args.input_30s_csv.exists():
        raise FileNotFoundError(f"missing 30s input: {args.input_30s_csv}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[DOWNSTREAM_ROWS] reading labels: {args.labels_csv}", flush=True)
    labels = normalize_labels(pd.read_csv(args.labels_csv))
    print(f"[DOWNSTREAM_ROWS] reading 30s rows: {args.input_30s_csv}", flush=True)
    rows_30s = normalize_30s_rows(read_30s_input(args.input_30s_csv))
    print("[DOWNSTREAM_ROWS] joining labels to 30s rows", flush=True)
    joined = join_labels_to_30s(labels, rows_30s, args.join_tolerance_seconds)

    outputs = {
        "training_rows_csv": args.output_dir / f"{TRAINING_ROWS_SCHEMA_VERSION}.csv",
        "manifest_json": args.output_dir / f"{TRAINING_ROWS_SCHEMA_VERSION}_manifest.json",
        "summary_md": args.output_dir / f"{TRAINING_ROWS_SCHEMA_VERSION}_summary.md",
    }
    joined.to_csv(outputs["training_rows_csv"], index=False)
    manifest = summarize(joined, labels, rows_30s, args, outputs)
    outputs["manifest_json"].write_text(json.dumps(json_safe(manifest), indent=2), encoding="utf-8")
    write_markdown(outputs["summary_md"], manifest)
    print(
        "[DOWNSTREAM_ROWS] done "
        f"joined_rows={manifest['joined_rows']} matched_rows={manifest['matched_rows']} "
        f"match_rate={manifest['match_rate']:.6f} output={outputs['training_rows_csv']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

