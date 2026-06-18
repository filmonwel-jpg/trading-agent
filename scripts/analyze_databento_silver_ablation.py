#!/usr/bin/env python3
"""Compare no-ONNX Databento silver ablation runs against a baseline trainer run.

The script is intentionally artifact-only: it reads setup manifests, scorecards,
OOF files, and logs produced by train_30s_models.py. It does not train models
or write ONNX artifacts.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_PRESETS = ("all", "equs", "opra", "liquidity", "options_flow")
KNOWN_PRESETS = (
    "all",
    "equs",
    "opra",
    "liquidity",
    "equs_liquidity",
    "opra_liquidity",
    "equs_activity",
    "options_flow",
)
ENTRY_FILENAMES = ("long_entry.onnx", "short_entry.onnx")
SILVER_SCHEMA_VERSION = "databento_silver_30s_opt_in_v1"
EXPECTED_SILVER_FEATURE_COUNTS = {
    "all": 35,
    "equs": 14,
    "opra": 21,
    "liquidity": 17,
    "equs_liquidity": 12,
    "opra_liquidity": 5,
    "equs_activity": 2,
    "options_flow": 16,
}
REQUIRED_ARTIFACTS = (
    "setup_manifest.json",
    "setup_scorecard.csv",
    "threshold_grid.csv",
    "oof_setup_predictions.csv",
    "calibration_manifest.json",
    "calibration_reliability.csv",
    "cost_aware_label_manifest.json",
    "cost_aware_setup_labels.csv",
    "train_30s_no_onnx.log",
)
SCORECARD_METRICS = (
    "signals",
    "rows",
    "signal_rate",
    "avg_precision",
    "avg_threshold",
    "threshold_std",
    "brier_score",
    "ece",
    "folds_used",
)
FOLD_STABILITY_METRICS = (
    "fold_count",
    "fold_precision_mean",
    "fold_precision_min",
    "fold_precision_max",
    "fold_precision_std",
    "pred_pos_rate_mean",
    "pred_pos_rate_min",
    "pred_pos_rate_max",
    "pred_pos_rate_std",
    "threshold_min",
    "threshold_max",
    "zero_pred_folds",
    "thin_pred_folds",
)
THIN_PRED_POS_RATE = 0.005
SHORT_FOLD_BLOCKER_COLUMNS = (
    "preset",
    "fold_id",
    "threshold",
    "test_precision",
    "pred_pos_rate",
    "pred_pos_count_est",
    "test_rows",
    "blocker",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _finite_float(raw: Any, default: float = math.nan) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def _finite_int(raw: Any, default: int = 0) -> int:
    value = _finite_float(raw, math.nan)
    return int(value) if math.isfinite(value) else default


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _append_issue(target: list[str], message: str) -> None:
    if message and message not in target:
        target.append(message)


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        # Header-only CSVs should count as zero artifact rows.
        return max(0, sum(1 for _ in handle) - 1)


def _read_scorecard(path: Path, errors: list[str]) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive path
        _append_issue(errors, f"failed to read scorecard {path}: {exc}")
        return {}
    if "filename" not in frame.columns:
        _append_issue(errors, f"scorecard missing filename column: {path}")
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        filename = str(row.get("filename", "")).strip()
        if not filename:
            continue
        item: dict[str, Any] = {"filename": filename}
        for metric in SCORECARD_METRICS:
            item[metric] = _finite_float(row.get(metric), math.nan)
        rows[filename] = item
    for filename in ENTRY_FILENAMES:
        if filename not in rows:
            _append_issue(errors, f"scorecard missing entry model row {filename}: {path}")
    return rows


def _read_oof_counts(path: Path, errors: list[str]) -> dict[str, int]:
    if not path.exists():
        return {"total_rows": 0, "paired_oof_rows": 0}
    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive path
        _append_issue(errors, f"failed to read OOF predictions {path}: {exc}")
        return {"total_rows": 0, "paired_oof_rows": 0}
    paired = 0
    if "is_oof_setup_prediction" in frame.columns:
        paired = int(pd.to_numeric(frame["is_oof_setup_prediction"], errors="coerce").fillna(0).astype(int).sum())
    else:
        _append_issue(errors, f"OOF predictions missing is_oof_setup_prediction column: {path}")
    return {"total_rows": int(len(frame)), "paired_oof_rows": paired}


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").dropna()


def _series_stat(series: pd.Series, stat: str) -> float:
    if len(series) == 0:
        return math.nan
    if stat == "mean":
        return float(series.mean())
    if stat == "min":
        return float(series.min())
    if stat == "max":
        return float(series.max())
    if stat == "std":
        return float(series.std(ddof=0)) if len(series) > 1 else 0.0
    raise ValueError(f"Unsupported stat={stat!r}")


def _threshold_fold_details(subset: pd.DataFrame) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    if "fold_id" in subset.columns:
        subset = subset.sort_values("fold_id")
    for _, row in subset.iterrows():
        pred_pos_rate = _finite_float(row.get("pred_pos_rate"), math.nan)
        test_rows = _finite_int(row.get("test_rows"), 0)
        pred_pos_count_est = (
            int(round(pred_pos_rate * test_rows))
            if math.isfinite(pred_pos_rate) and test_rows > 0
            else None
        )
        details.append(
            {
                "fold_id": _finite_int(row.get("fold_id"), 0),
                "threshold": _finite_float(row.get("threshold"), math.nan),
                "test_precision": _finite_float(row.get("test_precision"), math.nan),
                "test_recall": _finite_float(row.get("test_recall"), math.nan),
                "pred_pos_rate": pred_pos_rate,
                "pred_pos_count_est": pred_pos_count_est,
                "test_rows": test_rows,
                "brier_score": _finite_float(row.get("brier_score"), math.nan),
                "ece": _finite_float(row.get("ece"), math.nan),
            }
        )
    return details


def _read_threshold_stability(path: Path, errors: list[str]) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive path
        _append_issue(errors, f"failed to read threshold grid {path}: {exc}")
        return {}
    required = {"model", "fold_id", "threshold", "test_precision", "pred_pos_rate"}
    missing = sorted(required - set(frame.columns))
    if missing:
        _append_issue(errors, f"threshold grid missing columns {missing}: {path}")
        return {}

    stability: dict[str, dict[str, Any]] = {}
    model_values = frame["model"].astype(str)
    for filename in ENTRY_FILENAMES:
        subset = frame[model_values == filename].copy()
        if subset.empty:
            _append_issue(errors, f"threshold grid missing entry model rows {filename}: {path}")
            continue
        precision = _numeric_column(subset, "test_precision")
        pred_pos = _numeric_column(subset, "pred_pos_rate")
        threshold = _numeric_column(subset, "threshold")
        stability[filename] = {
            "fold_count": int(len(subset)),
            "fold_precision_mean": _series_stat(precision, "mean"),
            "fold_precision_min": _series_stat(precision, "min"),
            "fold_precision_max": _series_stat(precision, "max"),
            "fold_precision_std": _series_stat(precision, "std"),
            "pred_pos_rate_mean": _series_stat(pred_pos, "mean"),
            "pred_pos_rate_min": _series_stat(pred_pos, "min"),
            "pred_pos_rate_max": _series_stat(pred_pos, "max"),
            "pred_pos_rate_std": _series_stat(pred_pos, "std"),
            "threshold_min": _series_stat(threshold, "min"),
            "threshold_max": _series_stat(threshold, "max"),
            "zero_pred_folds": int(pred_pos.le(0.0).sum()),
            "thin_pred_folds": int(pred_pos.le(THIN_PRED_POS_RATE).sum()),
            "folds": _threshold_fold_details(subset),
        }
    return stability


def _validate_manifest_errors(run_name: str, artifact_name: str, manifest: dict[str, Any], errors: list[str]) -> None:
    manifest_errors = manifest.get("errors")
    if manifest_errors:
        _append_issue(errors, f"{run_name} {artifact_name} manifest errors: {manifest_errors}")


def summarize_run(
    run_name: str,
    run_dir: Path,
    *,
    expected_preset: str | None,
    baseline_feature_count: int | None = None,
    baseline_training_rows: int | None = None,
    baseline_oof_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Return artifact QA and key metrics for one trainer output directory."""

    errors: list[str] = []
    warnings: list[str] = []
    run_dir = Path(run_dir)
    if not run_dir.exists():
        _append_issue(errors, f"missing run directory: {run_dir}")

    missing = [name for name in REQUIRED_ARTIFACTS if not (run_dir / name).exists()]
    for name in missing:
        _append_issue(errors, f"missing required artifact: {run_dir / name}")

    onnx_files = sorted(path.name for path in run_dir.glob("*.onnx")) if run_dir.exists() else []
    if onnx_files:
        _append_issue(errors, f"unexpected ONNX files in no-ONNX run: {onnx_files}")

    setup_manifest: dict[str, Any] = {}
    feature_blocks: dict[str, Any] = {}
    manifest_path = run_dir / "setup_manifest.json"
    if manifest_path.exists():
        try:
            setup_manifest = _read_json(manifest_path)
            _validate_manifest_errors(run_name, "setup", setup_manifest, errors)
            feature_blocks = setup_manifest.get("feature_blocks") or {}
        except Exception as exc:
            _append_issue(errors, f"failed to read setup manifest {manifest_path}: {exc}")

    for artifact in ("calibration_manifest.json", "cost_aware_label_manifest.json"):
        path = run_dir / artifact
        if path.exists():
            try:
                _validate_manifest_errors(run_name, artifact, _read_json(path), errors)
            except Exception as exc:
                _append_issue(errors, f"failed to read {path}: {exc}")

    log_path = run_dir / "train_30s_no_onnx.log"
    if log_path.exists():
        text = log_path.read_text(encoding="utf-8", errors="replace")
        if "ONNX export disabled" not in text:
            _append_issue(warnings, f"log does not contain ONNX export disabled marker: {log_path}")
        if "Exported Production Model" in text or "Updated Canonical Model" in text:
            _append_issue(errors, f"log suggests ONNX/canonical export occurred: {log_path}")

    feature_count = _finite_int(setup_manifest.get("feature_count"), 0)
    training_rows = _finite_int(setup_manifest.get("training_rows"), 0)
    selected_count = _finite_int(feature_blocks.get("databento_silver_feature_count"), 0)
    selected_set = feature_blocks.get("databento_silver_feature_set")
    enabled = bool(feature_blocks.get("databento_silver_features_enabled"))
    schema_version = feature_blocks.get("databento_silver_feature_schema_version")

    if expected_preset is None:
        if enabled:
            _append_issue(errors, f"baseline has Databento silver features enabled: {run_dir}")
        if selected_count != 0:
            _append_issue(errors, f"baseline databento_silver_feature_count={selected_count}, expected 0")
    else:
        if not enabled:
            _append_issue(errors, f"{run_name} does not have Databento silver features enabled")
        if selected_set != expected_preset:
            _append_issue(errors, f"{run_name} feature_set={selected_set!r}, expected {expected_preset!r}")
        expected_count = EXPECTED_SILVER_FEATURE_COUNTS.get(expected_preset)
        if expected_count is not None and selected_count != expected_count:
            _append_issue(errors, f"{run_name} silver feature count={selected_count}, expected {expected_count}")
        if schema_version != SILVER_SCHEMA_VERSION:
            _append_issue(errors, f"{run_name} schema_version={schema_version!r}, expected {SILVER_SCHEMA_VERSION!r}")
        if baseline_feature_count is not None and feature_count - baseline_feature_count != selected_count:
            _append_issue(
                errors,
                f"{run_name} feature_count delta={feature_count - baseline_feature_count}, expected selected_count={selected_count}",
            )

    if baseline_training_rows is not None and training_rows != baseline_training_rows:
        _append_issue(errors, f"{run_name} training_rows={training_rows}, baseline={baseline_training_rows}")

    scorecard = _read_scorecard(run_dir / "setup_scorecard.csv", errors)
    oof_counts = _read_oof_counts(run_dir / "oof_setup_predictions.csv", errors)
    threshold_stability = _read_threshold_stability(run_dir / "threshold_grid.csv", errors)
    if baseline_oof_counts is not None and oof_counts != baseline_oof_counts:
        _append_issue(errors, f"{run_name} OOF counts={oof_counts}, baseline={baseline_oof_counts}")

    return {
        "run_name": run_name,
        "run_dir": str(run_dir),
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "onnx_files": onnx_files,
        "feature_count": feature_count,
        "training_rows": training_rows,
        "databento_silver_feature_set": selected_set,
        "databento_silver_feature_count": selected_count,
        "threshold_grid_rows": _csv_row_count(run_dir / "threshold_grid.csv"),
        "oof_counts": oof_counts,
        "scorecard": scorecard,
        "threshold_stability": threshold_stability,
    }


def build_comparison_rows(baseline: dict[str, Any], preset_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    baseline_scorecard = baseline.get("scorecard") or {}
    baseline_stability = baseline.get("threshold_stability") or {}
    for summary in preset_summaries:
        preset = summary["run_name"]
        scorecard = summary.get("scorecard") or {}
        threshold_stability = summary.get("threshold_stability") or {}
        for filename in ENTRY_FILENAMES:
            base = baseline_scorecard.get(filename, {})
            current = scorecard.get(filename, {})
            base_stability = baseline_stability.get(filename, {})
            current_stability = threshold_stability.get(filename, {})
            row: dict[str, Any] = {
                "preset": preset,
                "filename": filename,
                "feature_count": summary.get("feature_count"),
                "databento_silver_feature_count": summary.get("databento_silver_feature_count"),
                "training_rows": summary.get("training_rows"),
                "qa_ok": bool(summary.get("ok")),
            }
            for metric in SCORECARD_METRICS:
                baseline_value = _finite_float(base.get(metric), math.nan)
                current_value = _finite_float(current.get(metric), math.nan)
                row[f"{metric}_baseline"] = baseline_value
                row[f"{metric}_current"] = current_value
                row[f"{metric}_delta"] = current_value - baseline_value if math.isfinite(baseline_value) and math.isfinite(current_value) else math.nan
            for metric in FOLD_STABILITY_METRICS:
                baseline_value = _finite_float(base_stability.get(metric), math.nan)
                current_value = _finite_float(current_stability.get(metric), math.nan)
                row[f"{metric}_baseline"] = baseline_value
                row[f"{metric}_current"] = current_value
                row[f"{metric}_delta"] = current_value - baseline_value if math.isfinite(baseline_value) and math.isfinite(current_value) else math.nan
            rows.append(row)
    return rows


def _best_by_metric(rows: list[dict[str, Any]], filename: str, metric: str) -> dict[str, Any] | None:
    eligible = [row for row in rows if row.get("filename") == filename and math.isfinite(_finite_float(row.get(metric), math.nan))]
    if not eligible:
        return None
    return max(eligible, key=lambda row: _finite_float(row.get(metric), -math.inf))


def build_recommendations(compare_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_preset: dict[str, dict[str, float]] = {}
    for row in compare_rows:
        preset = str(row.get("preset"))
        filename = str(row.get("filename"))
        by_preset.setdefault(preset, {})[filename] = _finite_float(row.get("avg_precision_delta"), math.nan)

    balanced_candidates: list[dict[str, Any]] = []
    for preset, deltas in by_preset.items():
        long_delta = deltas.get("long_entry.onnx", math.nan)
        short_delta = deltas.get("short_entry.onnx", math.nan)
        if math.isfinite(long_delta) and math.isfinite(short_delta) and long_delta > 0.0 and short_delta > 0.0:
            balanced_candidates.append(
                {
                    "preset": preset,
                    "long_precision_delta": long_delta,
                    "short_precision_delta": short_delta,
                    "balanced_min_precision_delta": min(long_delta, short_delta),
                }
            )
    balanced_candidates.sort(key=lambda row: row["balanced_min_precision_delta"], reverse=True)

    best_long = _best_by_metric(compare_rows, "long_entry.onnx", "avg_precision_delta")
    best_short = _best_by_metric(compare_rows, "short_entry.onnx", "avg_precision_delta")
    return {
        "best_long_precision_delta_preset": best_long.get("preset") if best_long else None,
        "best_long_precision_delta": best_long.get("avg_precision_delta") if best_long else None,
        "best_short_precision_delta_preset": best_short.get("preset") if best_short else None,
        "best_short_precision_delta": best_short.get("avg_precision_delta") if best_short else None,
        "balanced_positive_precision_candidates": balanced_candidates,
        "production_promotion": "NO-GO",
        "reason": "Ablation output is research-only no-ONNX; promotion still requires longer-window validation, calibration, backtests, paper/shadow, and live feature parity.",
    }


def build_short_fold_blockers(preset_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in preset_summaries:
        preset = str(summary.get("run_name"))
        short_stability = (summary.get("threshold_stability") or {}).get("short_entry.onnx") or {}
        for fold in short_stability.get("folds") or []:
            pred_pos_rate = _finite_float(fold.get("pred_pos_rate"), math.nan)
            if not math.isfinite(pred_pos_rate) or pred_pos_rate > THIN_PRED_POS_RATE:
                continue
            row = {
                "preset": preset,
                "fold_id": _finite_int(fold.get("fold_id"), 0),
                "threshold": _finite_float(fold.get("threshold"), math.nan),
                "test_precision": _finite_float(fold.get("test_precision"), math.nan),
                "pred_pos_rate": pred_pos_rate,
                "pred_pos_count_est": fold.get("pred_pos_count_est"),
                "test_rows": _finite_int(fold.get("test_rows"), 0),
                "blocker": "zero" if pred_pos_rate <= 0.0 else "thin",
            }
            rows.append(row)
    rows.sort(key=lambda row: (row["preset"], row["fold_id"]))
    return rows


def analyze_ablation(
    baseline_dir: Path,
    ablation_root: Path,
    *,
    presets: tuple[str, ...] = DEFAULT_PRESETS,
    output_dir: Path | None = None,
    write_outputs: bool = True,
) -> dict[str, Any]:
    baseline = summarize_run("baseline", Path(baseline_dir), expected_preset=None)
    baseline_feature_count = baseline.get("feature_count") or None
    baseline_training_rows = baseline.get("training_rows") or None
    baseline_oof_counts = baseline.get("oof_counts") or None

    preset_summaries = [
        summarize_run(
            preset,
            Path(ablation_root) / preset,
            expected_preset=preset,
            baseline_feature_count=baseline_feature_count,
            baseline_training_rows=baseline_training_rows,
            baseline_oof_counts=baseline_oof_counts,
        )
        for preset in presets
    ]
    compare_rows = build_comparison_rows(baseline, preset_summaries)
    qa_pass = bool(baseline.get("ok")) and all(bool(summary.get("ok")) for summary in preset_summaries)
    recommendations = build_recommendations(compare_rows)
    short_fold_blockers = build_short_fold_blockers(preset_summaries)
    result = {
        "qa_pass": qa_pass,
        "baseline": baseline,
        "presets": preset_summaries,
        "compare_rows": compare_rows,
        "short_fold_blockers": short_fold_blockers,
        "recommendations": recommendations,
    }

    if write_outputs:
        out_dir = Path(output_dir) if output_dir else Path(ablation_root)
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(compare_rows).to_csv(out_dir / "databento_silver_ablation_scorecard_compare.csv", index=False)
        pd.DataFrame(short_fold_blockers, columns=SHORT_FOLD_BLOCKER_COLUMNS).to_csv(
            out_dir / "databento_silver_ablation_short_fold_blockers.csv",
            index=False,
        )
        (out_dir / "databento_silver_ablation_summary.json").write_text(
            json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return result


def print_report(result: dict[str, Any]) -> None:
    print("== RUN_QA")
    for summary in [result["baseline"], *result["presets"]]:
        status = "OK" if summary.get("ok") else "FAIL"
        print(
            f"{summary['run_name']}: {status} "
            f"feature_count={summary.get('feature_count')} "
            f"silver_count={summary.get('databento_silver_feature_count')} "
            f"training_rows={summary.get('training_rows')} "
            f"oof={summary.get('oof_counts')}"
        )
        for warning in summary.get("warnings", []):
            print(f"  WARNING: {warning}")
        for error in summary.get("errors", []):
            print(f"  ERROR: {error}")

    print("\n== ENTRY_SCORECARD_COMPARE")
    frame = pd.DataFrame(result["compare_rows"])
    display_cols = [
        "preset",
        "filename",
        "feature_count",
        "databento_silver_feature_count",
        "avg_precision_current",
        "avg_precision_delta",
        "avg_threshold_current",
        "avg_threshold_delta",
        "brier_score_delta",
        "ece_delta",
    ]
    if not frame.empty:
        print(frame[display_cols].to_string(index=False))

    print("\n== FOLD_STABILITY_COMPARE")
    stability_cols = [
        "preset",
        "filename",
        "fold_precision_min_current",
        "fold_precision_std_current",
        "pred_pos_rate_min_current",
        "pred_pos_rate_std_current",
        "zero_pred_folds_current",
        "thin_pred_folds_current",
    ]
    if not frame.empty:
        print(frame[stability_cols].to_string(index=False))

    print("\n== SHORT_FOLD_BLOCKERS")
    blockers = pd.DataFrame(result.get("short_fold_blockers") or [], columns=SHORT_FOLD_BLOCKER_COLUMNS)
    if blockers.empty:
        print("none")
    else:
        print(blockers.to_string(index=False))

    rec = result["recommendations"]
    print("\n== RESEARCH_READOUT")
    print(f"best_long_precision_delta_preset={rec.get('best_long_precision_delta_preset')} delta={rec.get('best_long_precision_delta')}")
    print(f"best_short_precision_delta_preset={rec.get('best_short_precision_delta_preset')} delta={rec.get('best_short_precision_delta')}")
    print("balanced_positive_precision_candidates=")
    for candidate in rec.get("balanced_positive_precision_candidates", []):
        print(
            f"  {candidate['preset']}: "
            f"long_delta={candidate['long_precision_delta']:+.6f} "
            f"short_delta={candidate['short_precision_delta']:+.6f} "
            f"balanced_min={candidate['balanced_min_precision_delta']:+.6f}"
        )
    print(f"production_promotion={rec.get('production_promotion')}")
    print(f"DATABENTO_SILVER_ABLATION_QA={'PASS' if result.get('qa_pass') else 'FAIL'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True, type=Path, help="Strict baseline no-ONNX trainer output directory")
    parser.add_argument("--ablation-root", required=True, type=Path, help="Directory containing preset subdirectories")
    parser.add_argument(
        "--preset",
        action="append",
        choices=KNOWN_PRESETS,
        help="Preset to include; may be repeated. Defaults to the original five-preset ablation set.",
    )
    parser.add_argument("--output-dir", type=Path, help="Where to write comparison CSV/JSON; defaults to ablation root")
    parser.add_argument("--no-write", action="store_true", help="Print only; do not write comparison artifacts")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    presets = tuple(args.preset) if args.preset else DEFAULT_PRESETS
    result = analyze_ablation(
        args.baseline_dir,
        args.ablation_root,
        presets=presets,
        output_dir=args.output_dir,
        write_outputs=not args.no_write,
    )
    print_report(result)
    return 0 if result.get("qa_pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
