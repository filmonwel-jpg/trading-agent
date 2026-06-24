#!/usr/bin/env python3
"""Inject real setup probabilities into staged lifecycle/micro rows and retrain the bundle.

This is a recovery/research path for cases where the raw 5s bar directories are not
mounted locally but the streamed lifecycle/micro staging CSVs are available. It does
not rebuild labels from raw 5s bars; it filters/restages existing rows to the subset
covered by a real OOF 30s setup-prediction CSV, then trains the six existing
RandomForest lifecycle/micro ONNX models with the runtime-aligned f_setup_* /
f_entry_* probability context populated.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import train_lifecycle_micro_models as lm

DATASETS = [
    {
        "key": "long_lifecycle",
        "filename": "long_lifecycle_rows.csv",
        "side": "long",
        "context": "entry",
        "label_col": "Label_Long_ExitLifecycle",
        "model_name": "longExitLifecycleAi",
        "onnx_filename": "long_exit_lifecycle.onnx",
        "kind": "lifecycle",
    },
    {
        "key": "short_lifecycle",
        "filename": "short_lifecycle_rows.csv",
        "side": "short",
        "context": "entry",
        "label_col": "Label_Short_ExitLifecycle",
        "model_name": "shortExitLifecycleAi",
        "onnx_filename": "short_exit_lifecycle.onnx",
        "kind": "lifecycle",
    },
    {
        "key": "long_micro_entry",
        "filename": "long_micro_entry_rows.csv",
        "side": "long",
        "context": "setup",
        "label_col": "Label_Long_MicroEntry",
        "model_name": "longMicroEntryAi",
        "onnx_filename": "long_micro_entry_5s.onnx",
        "kind": "micro_entry",
    },
    {
        "key": "short_micro_entry",
        "filename": "short_micro_entry_rows.csv",
        "side": "short",
        "context": "setup",
        "label_col": "Label_Short_MicroEntry",
        "model_name": "shortMicroEntryAi",
        "onnx_filename": "short_micro_entry_5s.onnx",
        "kind": "micro_entry",
    },
    {
        "key": "long_micro_exit",
        "filename": "long_micro_exit_rows.csv",
        "side": "long",
        "context": "entry",
        "label_col": "Label_Long_MicroExitGuard",
        "model_name": "longMicroExitGuardAi",
        "onnx_filename": "long_micro_exit_guard_5s.onnx",
        "kind": "micro_exit_guard",
    },
    {
        "key": "short_micro_exit",
        "filename": "short_micro_exit_rows.csv",
        "side": "short",
        "context": "entry",
        "label_col": "Label_Short_MicroExitGuard",
        "model_name": "shortMicroExitGuardAi",
        "onnx_filename": "short_micro_exit_guard_5s.onnx",
        "kind": "micro_exit_guard",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged-dir", type=Path, default=Path("runtime/lifecycle_micro_20260523_staging"))
    parser.add_argument("--setup-predictions-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--restaged-dir", type=Path, default=None)
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols. Defaults to OOF prediction symbols.")
    parser.add_argument("--chunksize", type=int, default=100_000)
    parser.add_argument("--max-train-rows-per-model", type=int, default=500_000)
    parser.add_argument("--min-rows", type=int, default=200)
    parser.add_argument("--min-setup-prob-unique", type=int, default=3)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--no-onnx", action="store_true")
    parser.add_argument("--posthoc-calibration", choices=sorted(lm.POSTHOC_CALIBRATION_METHODS), default="none")
    parser.add_argument("--posthoc-calibration-frac", type=float, default=0.20)
    parser.add_argument("--frozen-holdout-frac", type=float, default=0.20)
    parser.add_argument("--min-frozen-holdout-rows", type=int, default=200)
    parser.add_argument("--min-holdout-predictions", type=int, default=20)
    parser.add_argument("--max-day-dominance-frac", type=float, default=0.40)
    parser.add_argument("--min-stable-threshold-points", type=int, default=lm.MIN_STABLE_THRESHOLD_POINTS)
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def split_symbols(raw: str) -> set[str]:
    return {token.strip().upper() for token in str(raw or "").split(",") if token.strip()}


def setup_key(symbol: pd.Series, timestamp: pd.Series) -> pd.Series:
    parsed = lm.parse_timestamp(timestamp)
    return symbol.astype(str).str.strip().str.upper() + "|" + parsed.dt.strftime("%Y-%m-%d %H:%M:%S")


def load_oof_prediction_maps(path: Path, symbols: set[str]) -> tuple[dict[str, dict[str, dict[str, float]]], dict[str, Any]]:
    pred = lm.load_setup_predictions(str(path))
    pred = pred[pred["is_oof_setup_prediction"].astype(int).eq(1)].copy()
    if symbols:
        pred = pred[pred["Symbol"].isin(symbols)].copy()
    if pred.empty:
        raise ValueError("No OOF setup predictions remain after filtering.")
    pred["_key"] = pred["Symbol"].astype(str).str.upper() + "|" + pred["_setup_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    maps: dict[str, dict[str, dict[str, float]]] = {}
    side_summaries: dict[str, Any] = {}
    for side in ["long", "short"]:
        prob_col = f"f_{side}_setup_prob"
        thr_col = f"f_{side}_setup_threshold"
        margin_col = f"f_{side}_setup_threshold_margin"
        side_frame = pred[["_key", prob_col, thr_col, margin_col]].copy()
        side_frame[prob_col] = pd.to_numeric(side_frame[prob_col], errors="coerce")
        side_frame[thr_col] = pd.to_numeric(side_frame[thr_col], errors="coerce")
        side_frame[margin_col] = pd.to_numeric(side_frame[margin_col], errors="coerce")
        missing_margin = side_frame[margin_col].isna() & side_frame[prob_col].notna() & side_frame[thr_col].notna()
        side_frame.loc[missing_margin, margin_col] = side_frame.loc[missing_margin, prob_col] - side_frame.loc[missing_margin, thr_col]
        side_frame = side_frame[side_frame[prob_col].notna() & side_frame[thr_col].notna()].drop_duplicates("_key", keep="last")
        maps[side] = {
            "prob": dict(zip(side_frame["_key"], side_frame[prob_col])),
            "threshold": dict(zip(side_frame["_key"], side_frame[thr_col])),
            "margin": dict(zip(side_frame["_key"], side_frame[margin_col])),
        }
        side_summaries[side] = {
            "rows": int(len(side_frame)),
            "unique_values": int(side_frame[prob_col].round(8).nunique()),
            "min": float(side_frame[prob_col].min()),
            "max": float(side_frame[prob_col].max()),
            "mean": float(side_frame[prob_col].mean()),
        }
    manifest = {
        "path": str(path),
        "oof_rows": int(len(pred)),
        "symbols": sorted(pred["Symbol"].astype(str).str.upper().unique().tolist()),
        "date_min": str(pred["_setup_ts"].dt.strftime("%Y-%m-%d").min()),
        "date_max": str(pred["_setup_ts"].dt.strftime("%Y-%m-%d").max()),
        "sides": side_summaries,
    }
    return maps, manifest


def assign_context_columns(chunk: pd.DataFrame, context: str, prob: pd.Series, threshold: pd.Series, margin: pd.Series) -> None:
    if "setup_probability" in chunk.columns:
        chunk["setup_probability"] = prob.to_numpy(dtype=float)
    if "setup_threshold" in chunk.columns:
        chunk["setup_threshold"] = threshold.to_numpy(dtype=float)
    if "setup_threshold_margin" in chunk.columns:
        chunk["setup_threshold_margin"] = margin.to_numpy(dtype=float)
    prefix = "f_setup" if context == "setup" else "f_entry"
    replacements = {
        f"{prefix}_score_proxy": prob,
        f"{prefix}_prob": prob,
        f"{prefix}_threshold": threshold,
        f"{prefix}_threshold_margin": margin,
    }
    for col, values in replacements.items():
        if col in chunk.columns:
            chunk[col] = values.to_numpy(dtype=float)


def restage_one_dataset(
    source: Path,
    target: Path,
    side: str,
    context: str,
    label_col: str,
    maps: dict[str, dict[str, float]],
    allowed_symbols: set[str],
    chunksize: int,
    min_unique_values: int,
) -> dict[str, Any]:
    if not source.exists():
        raise FileNotFoundError(f"Missing staged source: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    input_rows = matched_rows = positives = 0
    symbols_seen: set[str] = set()
    dates_seen: set[str] = set()
    prob_values: list[pd.Series] = []
    header_written = False
    for chunk in pd.read_csv(source, chunksize=max(1, chunksize)):
        input_rows += len(chunk)
        if "Symbol" not in chunk.columns or "SetupTime" not in chunk.columns:
            raise ValueError(f"{source} is missing Symbol or SetupTime")
        chunk["Symbol"] = chunk["Symbol"].astype(str).str.strip().str.upper()
        if allowed_symbols:
            chunk = chunk[chunk["Symbol"].isin(allowed_symbols)].copy()
            if chunk.empty:
                continue
        keys = setup_key(chunk["Symbol"], chunk["SetupTime"])
        prob = keys.map(maps["prob"])
        threshold = keys.map(maps["threshold"])
        margin = keys.map(maps["margin"])
        missing_margin = margin.isna() & prob.notna() & threshold.notna()
        if missing_margin.any():
            margin.loc[missing_margin] = prob.loc[missing_margin] - threshold.loc[missing_margin]
        keep = prob.notna() & threshold.notna() & margin.notna()
        if not keep.any():
            continue
        chunk = chunk.loc[keep].copy()
        prob = prob.loc[keep].astype(float).reset_index(drop=True)
        threshold = threshold.loc[keep].astype(float).reset_index(drop=True)
        margin = margin.loc[keep].astype(float).reset_index(drop=True)
        chunk.reset_index(drop=True, inplace=True)
        assign_context_columns(chunk, context, prob, threshold, margin)
        matched_rows += len(chunk)
        prob_values.append(prob)
        if label_col in chunk.columns:
            positives += int(pd.to_numeric(chunk[label_col], errors="coerce").fillna(0).sum())
        if "Date" in chunk.columns:
            dates_seen.update(chunk["Date"].astype(str).unique().tolist())
        symbols_seen.update(chunk["Symbol"].astype(str).unique().tolist())
        chunk.to_csv(target, mode="a", header=not header_written, index=False)
        header_written = True
    if matched_rows == 0:
        raise ValueError(f"No rows from {source.name} matched OOF setup predictions.")
    all_prob = pd.concat(prob_values, ignore_index=True) if prob_values else pd.Series(dtype=float)
    unique_values = int(all_prob.round(8).nunique())
    if unique_values < min_unique_values:
        raise ValueError(f"{source.name} setup probabilities have only {unique_values} unique values; need >= {min_unique_values}")
    return {
        "source": str(source),
        "target": str(target),
        "side": side,
        "context": context,
        "input_rows": int(input_rows),
        "matched_rows": int(matched_rows),
        "dropped_rows": int(input_rows - matched_rows),
        "positives": int(positives),
        "symbols": sorted(symbols_seen),
        "date_min": min(dates_seen) if dates_seen else None,
        "date_max": max(dates_seen) if dates_seen else None,
        "unique_dates": len(dates_seen),
        "setup_probability_unique_values": unique_values,
        "setup_probability_min": float(all_prob.min()),
        "setup_probability_max": float(all_prob.max()),
        "setup_probability_mean": float(all_prob.mean()),
        "size_bytes": int(target.stat().st_size),
    }


def train_from_restaged(args: argparse.Namespace, restaged_paths: dict[str, Path]) -> list[lm.TrainedModelResult]:
    results: list[lm.TrainedModelResult] = []
    for index, spec in enumerate(DATASETS):
        frame = lm.load_staged_training_frame(restaged_paths[spec["key"]], args.max_train_rows_per_model, args.random_state + index)
        result = lm.train_binary_model(
            frame,
            spec["label_col"],
            spec["model_name"],
            spec["onnx_filename"],
            spec["kind"],
            args.output_dir,
            args.min_rows,
            args.random_state + index,
            args.no_onnx,
            posthoc_calibration=args.posthoc_calibration,
            posthoc_calibration_frac=args.posthoc_calibration_frac,
            frozen_holdout_frac=args.frozen_holdout_frac,
            min_frozen_holdout_rows=args.min_frozen_holdout_rows,
            min_holdout_predictions=args.min_holdout_predictions,
            max_day_dominance_frac=args.max_day_dominance_frac,
            min_stable_threshold_points=args.min_stable_threshold_points,
        )
        if result is not None:
            results.append(result)
    return results


def main() -> int:
    args = parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    restaged_dir = (args.restaged_dir.expanduser().resolve() if args.restaged_dir else args.output_dir / "restaged_rows_with_oof_setup")
    staged_dir = args.staged_dir.expanduser().resolve()
    setup_predictions_csv = args.setup_predictions_csv.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    restaged_dir.mkdir(parents=True, exist_ok=True)

    requested_symbols = split_symbols(args.symbols)
    maps_by_side, setup_manifest = load_oof_prediction_maps(setup_predictions_csv, requested_symbols)
    allowed_symbols = requested_symbols or set(setup_manifest["symbols"])

    restaged_paths: dict[str, Path] = {}
    restage_manifest: dict[str, Any] = {
        "schema_version": "lifecycle_micro_restaged_oof_setup_v1",
        "generated_at_utc": utc_now(),
        "source_staged_dir": str(staged_dir),
        "setup_predictions": setup_manifest,
        "allowed_symbols": sorted(allowed_symbols),
        "output_dir": str(args.output_dir),
        "restaged_dir": str(restaged_dir),
        "production_promotion": "NO-GO",
        "notes": [
            "Research-only recovery path: labels/features come from existing 5s-derived staged rows; raw 5s bars were not rebuilt in this run.",
            "Runtime-aligned f_setup_* and f_entry_* columns are populated from CatBoost OOF 30s setup predictions.",
        ],
        "datasets": {},
    }
    for spec in DATASETS:
        source = staged_dir / spec["filename"]
        target = restaged_dir / spec["filename"]
        summary = restage_one_dataset(
            source=source,
            target=target,
            side=spec["side"],
            context=spec["context"],
            label_col=spec["label_col"],
            maps=maps_by_side[spec["side"]],
            allowed_symbols=allowed_symbols,
            chunksize=args.chunksize,
            min_unique_values=args.min_setup_prob_unique,
        )
        restaged_paths[spec["key"]] = target
        restage_manifest["datasets"][spec["key"]] = summary
        print(
            "RESTAGED "
            f"{spec['key']} matched_rows={summary['matched_rows']} positives={summary['positives']} "
            f"unique_prob={summary['setup_probability_unique_values']} target={target}",
            flush=True,
        )

    manifest_path = args.output_dir / "restaged_oof_setup_manifest.json"
    manifest_path.write_text(json.dumps(restage_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"WROTE {manifest_path}")

    results = train_from_restaged(args, restaged_paths)
    lm.write_scorecards(args.output_dir, results)
    if len(results) != len(DATASETS):
        print(f"LIFECYCLE_MICRO_FROM_STAGED=FAIL trained={len(results)} expected={len(DATASETS)}")
        return 2
    print(f"WROTE {args.output_dir / 'lifecycle_micro_scorecard.csv'}")
    print(f"WROTE {args.output_dir / 'lifecycle_micro_route_manifest.json'}")
    print(f"LIFECYCLE_MICRO_FROM_STAGED=PASS output_dir={args.output_dir}")
    print("production_promotion=NO-GO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
