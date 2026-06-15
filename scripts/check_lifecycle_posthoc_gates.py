#!/usr/bin/env python3
"""Check lifecycle/micro post-hoc calibration promotion gates.

This script is intentionally artifact-only: it reads an existing
train_lifecycle_micro_models.py output directory and emits a compact JSON/CSV
gate report. It does not retrain models and it does not promote artifacts.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


REQUIRED_ARTIFACTS = [
    "calibration_manifest.json",
    "lifecycle_micro_scorecard.csv",
    "posthoc_calibration_comparison.csv",
    "posthoc_calibrators.json",
]


def finite_float(value, default: float = float("nan")) -> float:
    try:
        result = float(value)
    except Exception:
        return default
    return result


def finite_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def selected_manifest_methods(manifest: dict) -> dict[str, str]:
    methods: dict[str, str] = {}
    for model in manifest.get("models", []) or []:
        name = str(model.get("model", ""))
        posthoc = model.get("posthoc", {}) or {}
        if name:
            methods[name] = str(posthoc.get("selected_method", ""))
    return methods


def evaluate_gates(
    output_dir: Path,
    *,
    min_frozen_holdout_rows: int = 500,
    min_predicted_positive_count: int = 20,
    max_day_dominance_frac: float = 0.40,
    brier_tolerance: float = 1e-12,
) -> tuple[dict, pd.DataFrame]:
    missing = [name for name in REQUIRED_ARTIFACTS if not (output_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing required lifecycle posthoc artifacts in {output_dir}: {missing}")

    manifest = json.loads((output_dir / "calibration_manifest.json").read_text(encoding="utf-8"))
    scorecard = pd.read_csv(output_dir / "lifecycle_micro_scorecard.csv")
    comparison = pd.read_csv(output_dir / "posthoc_calibration_comparison.csv")
    manifest_methods = selected_manifest_methods(manifest)

    required_scorecard_cols = {"model", "posthoc_selected_method"}
    required_comparison_cols = {
        "model",
        "calibration_method",
        "brier_score",
        "ece",
        "threshold",
        "calibration_rows",
        "predicted_positive_count",
        "max_predicted_day_fraction",
    }
    missing_scorecard = sorted(required_scorecard_cols - set(scorecard.columns))
    missing_comparison = sorted(required_comparison_cols - set(comparison.columns))
    if missing_scorecard or missing_comparison:
        raise ValueError(
            f"Artifact schema mismatch: missing_scorecard={missing_scorecard} "
            f"missing_comparison={missing_comparison}"
        )

    rows: list[dict] = []
    manifest_errors = list(manifest.get("errors", []) or [])
    for _, score_row in scorecard.iterrows():
        model = str(score_row["model"])
        selected_method = str(score_row.get("posthoc_selected_method", ""))
        model_comparison = comparison[comparison["model"].astype(str).eq(model)].copy()
        selected_rows = model_comparison[model_comparison["calibration_method"].astype(str).eq(selected_method)]

        warnings: list[str] = []
        if selected_rows.empty:
            warnings.append(f"selected_method {selected_method!r} is missing from posthoc_calibration_comparison.csv")
            selected = pd.Series(dtype=object)
        else:
            selected = selected_rows.iloc[0]

        manifest_selected_method = manifest_methods.get(model, "")
        if manifest_selected_method and manifest_selected_method != selected_method:
            warnings.append(
                f"manifest selected_method {manifest_selected_method!r} does not match scorecard {selected_method!r}"
            )

        selected_brier = finite_float(selected.get("brier_score")) if not selected.empty else float("nan")
        model_briers = pd.to_numeric(model_comparison.get("brier_score", pd.Series(dtype=float)), errors="coerce")
        best_brier = float(model_briers.min()) if len(model_briers.dropna()) else float("nan")
        pass_selected_brier = bool(selected_brier <= best_brier + float(brier_tolerance)) if selected_brier == selected_brier and best_brier == best_brier else False
        if not pass_selected_brier:
            warnings.append(f"selected Brier {selected_brier} is not the best comparison Brier {best_brier}")

        holdout_rows = finite_int(selected.get("calibration_rows")) if not selected.empty else 0
        predicted_positive_count = finite_int(selected.get("predicted_positive_count")) if not selected.empty else 0
        max_day_fraction = finite_float(selected.get("max_predicted_day_fraction"), default=1.0) if not selected.empty else 1.0

        pass_min_rows = holdout_rows >= int(min_frozen_holdout_rows)
        pass_min_predictions = predicted_positive_count >= int(min_predicted_positive_count)
        pass_day_dominance = max_day_fraction <= float(max_day_dominance_frac)
        if not pass_min_rows:
            warnings.append(f"frozen_holdout_rows {holdout_rows} < minimum {int(min_frozen_holdout_rows)}")
        if not pass_min_predictions:
            warnings.append(f"predicted_positive_count {predicted_positive_count} < minimum {int(min_predicted_positive_count)}")
        if not pass_day_dominance:
            warnings.append(f"max_predicted_day_fraction {max_day_fraction:.5f} > maximum {float(max_day_dominance_frac):.5f}")

        gate_status = "PASS" if (
            not manifest_errors
            and not warnings
            and pass_min_rows
            and pass_min_predictions
            and pass_day_dominance
            and pass_selected_brier
        ) else "FAIL"

        rows.append({
            "model": model,
            "selected_method": selected_method,
            "selected_brier_score": selected_brier,
            "selected_ece": finite_float(selected.get("ece")) if not selected.empty else float("nan"),
            "selected_threshold": finite_float(selected.get("threshold")) if not selected.empty else float("nan"),
            "frozen_holdout_rows": holdout_rows,
            "predicted_positive_count": predicted_positive_count,
            "max_predicted_day_fraction": max_day_fraction,
            "best_brier_score": best_brier,
            "pass_min_frozen_holdout_rows": pass_min_rows,
            "pass_min_predicted_positive_count": pass_min_predictions,
            "pass_max_day_dominance": pass_day_dominance,
            "pass_selected_best_brier": pass_selected_brier,
            "gate_status": gate_status,
            "warnings": "; ".join(warnings),
        })

    rows_df = pd.DataFrame(rows)
    fail_count = int((rows_df["gate_status"] != "PASS").sum()) if not rows_df.empty else 0
    summary = {
        "schema_version": "lifecycle_micro_posthoc_promotion_gate_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "output_dir": str(output_dir),
        "manifest_errors": manifest_errors,
        "model_count": int(len(rows_df)),
        "fail_count": fail_count,
        "promotion_ready": bool(not manifest_errors and fail_count == 0 and len(rows_df) > 0),
        "thresholds": {
            "min_frozen_holdout_rows": int(min_frozen_holdout_rows),
            "min_predicted_positive_count": int(min_predicted_positive_count),
            "max_day_dominance_frac": float(max_day_dominance_frac),
            "brier_tolerance": float(brier_tolerance),
        },
        "artifacts": {
            "gate_rows_csv": "posthoc_promotion_gate_rows.csv",
            "gate_report_json": "posthoc_promotion_gate_report.json",
        },
        "models": rows,
    }
    return summary, rows_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check lifecycle/micro post-hoc calibration promotion gates from artifacts.")
    parser.add_argument("--output-dir", required=True, help="Lifecycle/micro training output directory containing posthoc artifacts.")
    parser.add_argument("--min-frozen-holdout-rows", type=int, default=500)
    parser.add_argument("--min-predicted-positive-count", type=int, default=20)
    parser.add_argument("--max-day-dominance-frac", type=float, default=0.40)
    parser.add_argument("--brier-tolerance", type=float, default=1e-12)
    parser.add_argument("--no-write", action="store_true", help="Print only; do not write gate report artifacts.")
    parser.add_argument("--fail-on-gate", action="store_true", help="Exit 1 when promotion_ready is false.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser()
    summary, rows_df = evaluate_gates(
        output_dir,
        min_frozen_holdout_rows=args.min_frozen_holdout_rows,
        min_predicted_positive_count=args.min_predicted_positive_count,
        max_day_dominance_frac=args.max_day_dominance_frac,
        brier_tolerance=args.brier_tolerance,
    )
    if not args.no_write:
        rows_df.to_csv(output_dir / "posthoc_promotion_gate_rows.csv", index=False)
        (output_dir / "posthoc_promotion_gate_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"WROTE {output_dir / 'posthoc_promotion_gate_rows.csv'}")
        print(f"WROTE {output_dir / 'posthoc_promotion_gate_report.json'}")

    print(rows_df[[
        "model",
        "selected_method",
        "predicted_positive_count",
        "max_predicted_day_fraction",
        "gate_status",
    ]].to_string(index=False))
    print(f"POSTHOC_PROMOTION_GATE={'PASS' if summary['promotion_ready'] else 'FAIL'}")
    if args.fail_on_gate and not summary["promotion_ready"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
