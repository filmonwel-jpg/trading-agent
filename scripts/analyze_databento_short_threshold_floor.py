#!/usr/bin/env python3
"""Diagnose short-entry threshold floors from saved Databento ablation OOF artifacts.

This script is artifact-only: it reads existing no-ONNX trainer output directories
and evaluates per-fold short-entry thresholds from ``oof_setup_predictions.csv``.
It does not train models, export ONNX, or modify model bundles.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_PRESETS = ("liquidity", "equs")
OUTPUT_FILENAME = "databento_short_threshold_floor_candidates.csv"
REQUIRED_COLUMNS = (
    "Label_Short_Entry",
    "f_short_setup_prob",
    "short_setup_fold_id",
    "f_short_setup_threshold",
)
DISPLAY_COLUMNS = (
    "preset",
    "fold_id",
    "original_blocker",
    "fold_rows",
    "positive_rows",
    "required_pred_pos_count",
    "original_threshold",
    "original_pred_pos_count",
    "original_pred_pos_rate",
    "original_precision",
    "floor_threshold",
    "threshold_drop",
    "floor_pred_pos_count",
    "floor_pred_pos_rate",
    "floor_precision",
    "precision_delta_vs_original",
)


def _finite_float(raw: Any, default: float = math.nan) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _required_pred_pos_count(fold_rows: int, min_pred_pos_rate: float, min_pred_pos_count: int) -> int:
    if fold_rows <= 0:
        return 0
    rate_count = math.floor(max(0.0, min_pred_pos_rate) * fold_rows) + 1
    required = max(1, int(min_pred_pos_count), rate_count)
    return min(required, fold_rows)


def _metrics_at_threshold(labels: pd.Series, probs: pd.Series, threshold: float) -> dict[str, Any]:
    pred = probs >= threshold
    pred_count = int(pred.sum())
    positive_count = int((labels == 1).sum())
    true_positive_count = int(((labels == 1) & pred).sum())
    rows = int(len(labels))
    return {
        "pred_pos_count": pred_count,
        "pred_pos_rate": pred_count / rows if rows else math.nan,
        "precision": true_positive_count / pred_count if pred_count else 0.0,
        "recall": true_positive_count / positive_count if positive_count else 0.0,
        "true_positive_count": true_positive_count,
        "positive_rows": positive_count,
    }


def _floor_threshold_for_count(probs: pd.Series, required_count: int) -> float:
    values = sorted((_finite_float(value) for value in probs), reverse=True)
    values = [value for value in values if math.isfinite(value)]
    if not values:
        return math.nan
    required_count = max(1, min(required_count, len(values)))
    return float(values[required_count - 1])


def _read_oof(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    for column in REQUIRED_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def analyze_preset(
    preset: str,
    run_dir: Path,
    *,
    min_pred_pos_rate: float,
    min_pred_pos_count: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    oof_path = run_dir / "oof_setup_predictions.csv"
    if not oof_path.exists():
        raise FileNotFoundError(f"missing OOF artifact for {preset}: {oof_path}")

    frame = _read_oof(oof_path)
    frame = frame.dropna(subset=["Label_Short_Entry", "f_short_setup_prob", "short_setup_fold_id"])
    frame = frame[frame["short_setup_fold_id"] > 0].copy()
    if frame.empty:
        warnings.append(f"{preset}: no short-entry OOF rows found in {oof_path}")
        return [], warnings

    rows: list[dict[str, Any]] = []
    for fold_id, fold in frame.groupby("short_setup_fold_id", sort=True):
        labels = fold["Label_Short_Entry"].astype(int)
        probs = fold["f_short_setup_prob"].astype(float)
        thresholds = fold["f_short_setup_threshold"].dropna().astype(float).unique()
        if len(thresholds) == 0:
            warnings.append(f"{preset} fold {fold_id}: missing original threshold")
            continue
        if len(thresholds) > 1:
            warnings.append(f"{preset} fold {fold_id}: multiple original thresholds found; using first")
        original_threshold = float(thresholds[0])
        fold_rows = int(len(fold))
        required_count = _required_pred_pos_count(fold_rows, min_pred_pos_rate, min_pred_pos_count)

        original = _metrics_at_threshold(labels, probs, original_threshold)
        floor_threshold = _floor_threshold_for_count(probs, required_count)
        floor = _metrics_at_threshold(labels, probs, floor_threshold)
        original_count = int(original["pred_pos_count"])
        if original_count <= 0:
            original_blocker = "zero"
        elif original_count < required_count:
            original_blocker = "thin"
        else:
            original_blocker = "ok"

        rows.append(
            {
                "preset": preset,
                "fold_id": int(fold_id),
                "original_blocker": original_blocker,
                "fold_rows": fold_rows,
                "positive_rows": int(original["positive_rows"]),
                "required_pred_pos_count": required_count,
                "original_threshold": original_threshold,
                "original_pred_pos_count": original_count,
                "original_pred_pos_rate": float(original["pred_pos_rate"]),
                "original_precision": float(original["precision"]),
                "original_recall": float(original["recall"]),
                "floor_threshold": floor_threshold,
                "threshold_drop": original_threshold - floor_threshold if math.isfinite(floor_threshold) else math.nan,
                "floor_pred_pos_count": int(floor["pred_pos_count"]),
                "floor_pred_pos_rate": float(floor["pred_pos_rate"]),
                "floor_precision": float(floor["precision"]),
                "floor_recall": float(floor["recall"]),
                "floor_true_positive_count": int(floor["true_positive_count"]),
                "precision_delta_vs_original": float(floor["precision"] - original["precision"]),
            }
        )
    return rows, warnings


def analyze_threshold_floor(
    ablation_root: Path,
    *,
    presets: tuple[str, ...] = DEFAULT_PRESETS,
    min_pred_pos_rate: float = 0.005,
    min_pred_pos_count: int = 20,
    output_dir: Path | None = None,
    write_outputs: bool = True,
) -> dict[str, Any]:
    all_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    errors: list[str] = []
    for preset in presets:
        try:
            rows, preset_warnings = analyze_preset(
                preset,
                Path(ablation_root) / preset,
                min_pred_pos_rate=min_pred_pos_rate,
                min_pred_pos_count=min_pred_pos_count,
            )
            all_rows.extend(rows)
            warnings.extend(preset_warnings)
        except Exception as exc:  # pragma: no cover - defensive CLI path
            errors.append(str(exc))

    result = {
        "qa_pass": not errors,
        "ablation_root": str(ablation_root),
        "presets": list(presets),
        "min_pred_pos_rate": min_pred_pos_rate,
        "min_pred_pos_count": min_pred_pos_count,
        "rows": all_rows,
        "warnings": warnings,
        "errors": errors,
    }
    if write_outputs:
        out_dir = Path(output_dir) if output_dir else Path(ablation_root)
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(all_rows, columns=list(DISPLAY_COLUMNS) + [
            "original_recall",
            "floor_recall",
            "floor_true_positive_count",
        ]).to_csv(out_dir / OUTPUT_FILENAME, index=False)
    return result


def print_report(result: dict[str, Any], *, all_folds: bool = False) -> None:
    print("== SHORT_THRESHOLD_FLOOR_QA")
    print(f"ablation_root={result.get('ablation_root')}")
    print(f"presets={result.get('presets')}")
    print(f"min_pred_pos_rate={result.get('min_pred_pos_rate')}")
    print(f"min_pred_pos_count={result.get('min_pred_pos_count')}")
    for warning in result.get("warnings", []):
        print(f"  WARNING: {warning}")
    for error in result.get("errors", []):
        print(f"  ERROR: {error}")

    frame = pd.DataFrame(result.get("rows") or [], columns=DISPLAY_COLUMNS)
    if not all_folds and not frame.empty:
        frame = frame[frame["original_blocker"] != "ok"]

    print("\n== SHORT_THRESHOLD_FLOOR_CANDIDATES")
    if frame.empty:
        print("none")
    else:
        print(frame[list(DISPLAY_COLUMNS)].to_string(index=False))

    print("\n== SHORT_THRESHOLD_FLOOR_READOUT")
    rows = result.get("rows") or []
    blockers = [row for row in rows if row.get("original_blocker") != "ok"]
    if blockers:
        for row in blockers:
            print(
                f"{row['preset']} fold={row['fold_id']} blocker={row['original_blocker']} "
                f"orig_count={row['original_pred_pos_count']} required={row['required_pred_pos_count']} "
                f"orig_thr={row['original_threshold']:.4f} floor_thr={row['floor_threshold']:.4f} "
                f"floor_precision={row['floor_precision']:.6f} "
                f"precision_delta={row['precision_delta_vs_original']:+.6f}"
            )
    else:
        print("no short-entry folds violate the configured prediction floor")
    print("production_promotion=NO-GO")
    print(f"DATABENTO_SHORT_THRESHOLD_FLOOR_QA={'PASS' if result.get('qa_pass') else 'FAIL'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ablation-root", required=True, type=Path, help="Directory containing preset subdirectories")
    parser.add_argument(
        "--preset",
        action="append",
        help="Preset to include; may be repeated. Defaults to liquidity and equs.",
    )
    parser.add_argument(
        "--min-pred-pos-rate",
        type=float,
        default=0.005,
        help="Minimum short predicted-positive rate floor. The required count is strict: floor(rate * rows) + 1.",
    )
    parser.add_argument(
        "--min-pred-pos-count",
        type=int,
        default=20,
        help="Minimum short predicted-positive count per fold.",
    )
    parser.add_argument("--output-dir", type=Path, help="Where to write CSV output; defaults to ablation root")
    parser.add_argument("--no-write", action="store_true", help="Print only; do not write CSV output")
    parser.add_argument("--all-folds", action="store_true", help="Print all folds instead of only zero/thin folds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    presets = tuple(args.preset) if args.preset else DEFAULT_PRESETS
    result = analyze_threshold_floor(
        args.ablation_root,
        presets=presets,
        min_pred_pos_rate=args.min_pred_pos_rate,
        min_pred_pos_count=args.min_pred_pos_count,
        output_dir=args.output_dir,
        write_outputs=not args.no_write,
    )
    print_report(result, all_folds=args.all_folds)
    return 0 if result.get("qa_pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
