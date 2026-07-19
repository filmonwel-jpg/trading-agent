#!/usr/bin/env python3
"""Train research-only filters for downstream-profitable setup arms.

This trains side-specific classifiers on ``setup_downstream_training_rows_v1.csv``.
The models are intended to answer: "given that the current setup policy armed a
long/short entry, which arms look downstream-confirmable and profitable?"

It is deliberately not a production promotion script. The output is an arm-quality
research bundle used to guide retraining and replay experiments.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, precision_score, recall_score, roc_auc_score

try:
    from catboost import CatBoostClassifier
except Exception:  # pragma: no cover - exercised when optional dependency is unavailable
    CatBoostClassifier = None

BUNDLE_SCHEMA_VERSION = "downstream_setup_filter_research_v1"

TARGET_BY_SIDE = {
    "long": "Label_Long_Setup_DownstreamPositive",
    "short": "Label_Short_Setup_DownstreamPositive",
}
EXPECTED_R_BY_SIDE = {
    "long": "Expected_Long_Setup_DownstreamNetR",
    "short": "Expected_Short_Setup_DownstreamNetR",
}

CATEGORICAL_FEATURES = [
    "Symbol",
    "SetupArbitrationReason",
    "SessionBucket",
]

ENRICHED_META_PREFIXES = (
    "tsm_",
    "regime_",
    "news_",
    "seq_",
    "setup_",
    "TimesFM_",
    "SeqLSTM_",
    "RegimeRF_",
    "RegimeLGBM_",
    "RegimeCatBoost_",
    "Sentiment",
    "News",
    "f_news_",
)
SILVER_AGGREGATE_PREFIXES = (
    "EqMbp1",
    "OpraTcbbo",
)
FEATURE_PRESETS = {
    "all": set(),
    "legacy_all": set(),
    "catboost_cost_aware_core": {"enriched_meta", "databento_silver"},
    "core": {"enriched_meta", "databento_silver"},
    "live_core": {"enriched_meta", "databento_silver"},
}

EXCLUDE_EXACT = {
    "training_rows_schema_version",
    "Label_Version",
    "arm_id",
    "Timestamp",
    "AsOfTs",
    "SetupTimeUtc",
    "Best_Entry_TimeUtc",
    "Best_Exit_Reason",
    "Best_Outcome_Status",
    "SetupSide",
    "SetupEpochSec",
    "Best_Entry_EpochSec",
    "BarEpochSec",
    "join_epoch_delta_seconds",
    "join_abs_epoch_delta_seconds",
    "Label_Min_Micro_Prob",
    "Label_Min_Expected_Net_R",
    "MicroEvalWithinTtlCount",
}
EXCLUDE_PREFIXES = (
    "Label_",
    "Expected_",
    "Max_Future_Micro_",
    "Best_",
)


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return json_safe(value.item())
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def is_excluded_feature(column: str) -> bool:
    if column in EXCLUDE_EXACT:
        return True
    return any(column.startswith(prefix) for prefix in EXCLUDE_PREFIXES)


def optional_feature_group(column: str) -> str | None:
    if column.startswith(SILVER_AGGREGATE_PREFIXES):
        return "databento_silver"
    if column.startswith(ENRICHED_META_PREFIXES):
        return "enriched_meta"
    return None


def normalize_exclude_feature_groups(groups: set[str] | list[str] | tuple[str, ...] | None) -> set[str]:
    normalized = set()
    for group in groups or []:
        text = str(group or "").strip().lower().replace("-", "_")
        if text in {"meta", "enriched", "enriched_meta_features", "meta_features"}:
            text = "enriched_meta"
        elif text in {"silver", "silver_aggregate", "silver_aggregates", "databento_silver_features"}:
            text = "databento_silver"
        if text:
            normalized.add(text)
    return normalized


def feature_exclusion_summary(columns: list[str], exclude_feature_groups: set[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "excluded_groups": sorted(exclude_feature_groups),
        "excluded_counts": {},
        "excluded_columns": {},
    }
    for column in columns:
        group = optional_feature_group(column)
        if group is None or group not in exclude_feature_groups:
            continue
        summary["excluded_counts"][group] = int(summary["excluded_counts"].get(group, 0)) + 1
        summary["excluded_columns"].setdefault(group, []).append(column)
    return summary


def build_inferred_feature_matrix(df: pd.DataFrame, exclude_feature_groups: set[str] | None = None) -> pd.DataFrame:
    exclude_feature_groups = normalize_exclude_feature_groups(exclude_feature_groups)
    candidates: dict[str, pd.Series] = {}
    for column in df.columns:
        if is_excluded_feature(column):
            continue
        group = optional_feature_group(column)
        if group is not None and group in exclude_feature_groups:
            continue
        if column in CATEGORICAL_FEATURES:
            continue
        series = pd.to_numeric(df[column], errors="coerce")
        if series.notna().sum() == 0:
            continue
        if series.nunique(dropna=True) <= 1:
            continue
        candidates[column] = series
    numeric = pd.DataFrame(candidates, index=df.index)
    categorical_frames = []
    for column in CATEGORICAL_FEATURES:
        if column in df.columns and not is_excluded_feature(column):
            values = df[column].astype(str).str.strip().replace("", "UNKNOWN")
            categorical_frames.append(pd.get_dummies(values, prefix=column, dtype=np.float32))
    if categorical_frames:
        matrix = pd.concat([numeric] + categorical_frames, axis=1)
    else:
        matrix = numeric
    return matrix.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(np.float32)


def build_feature_matrix(
    df: pd.DataFrame,
    feature_columns: list[str] | None = None,
    exclude_feature_groups: set[str] | list[str] | tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    if feature_columns is None:
        matrix = build_inferred_feature_matrix(df, normalize_exclude_feature_groups(exclude_feature_groups))
        return matrix, list(matrix.columns)

    inferred = build_inferred_feature_matrix(df)
    matrix = inferred.reindex(columns=feature_columns, fill_value=0.0)
    matrix = matrix.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(np.float32)
    return matrix, feature_columns


def positive_class_index(model: Any) -> int:
    classes = list(getattr(model, "classes_", []))
    if not classes:
        return 1
    for idx, value in enumerate(classes):
        try:
            if int(value) == 1:
                return idx
        except (TypeError, ValueError):
            if str(value) == "1":
                return idx
    return max(0, len(classes) - 1)


def build_model(side: str, args: argparse.Namespace) -> Any:
    model_family = str(args.model_family).strip().lower().replace("-", "_")
    random_state = args.random_state + (0 if side == "long" else 10000)
    if model_family in {"catboost", "cb"}:
        if CatBoostClassifier is None:
            raise RuntimeError("--model-family catboost requested, but the catboost package is not installed")
        return CatBoostClassifier(
            iterations=args.catboost_iterations,
            depth=args.catboost_depth,
            learning_rate=args.catboost_learning_rate,
            l2_leaf_reg=args.catboost_l2_leaf_reg,
            loss_function="Logloss",
            eval_metric="AUC",
            auto_class_weights="Balanced",
            random_seed=random_state,
            thread_count=args.n_jobs,
            verbose=False,
            allow_writing_files=False,
        )
    if model_family in {"random_forest", "randomforest", "rf"}:
        return RandomForestClassifier(
            n_estimators=args.n_estimators,
            min_samples_leaf=args.min_samples_leaf,
            max_depth=args.max_depth if args.max_depth > 0 else None,
            max_features=args.max_features,
            class_weight="balanced_subsample",
            random_state=random_state,
            n_jobs=args.n_jobs,
        )
    raise ValueError(f"unsupported --model-family={args.model_family!r}; expected catboost or random_forest")


def chronological_split(df: pd.DataFrame, train_frac: float) -> tuple[np.ndarray, np.ndarray]:
    ordered = df.sort_values(["SetupEpochSec", "Symbol", "arm_id"], kind="mergesort") if "arm_id" in df.columns else df.sort_values(["SetupEpochSec", "Symbol"], kind="mergesort")
    ordered_idx = ordered.index.to_numpy()
    cut = int(len(ordered_idx) * min(max(train_frac, 0.1), 0.9))
    cut = min(max(cut, 1), len(ordered_idx) - 1)
    return ordered_idx[:cut], ordered_idx[cut:]


def safe_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    return float(roc_auc_score(y_true, y_prob))


def safe_average_precision(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    if len(np.unique(y_true)) < 2:
        return None
    return float(average_precision_score(y_true, y_prob))


def safe_mean(values: np.ndarray) -> float | None:
    finite_values = values[np.isfinite(values)]
    if len(finite_values) == 0:
        return None
    return float(np.mean(finite_values))


def safe_quantile(values: np.ndarray, q: float) -> float | None:
    finite_values = values[np.isfinite(values)]
    if len(finite_values) == 0:
        return None
    return float(np.quantile(finite_values, q))


def threshold_metrics(y_true: np.ndarray, y_prob: np.ndarray, thresholds: list[float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for threshold in thresholds:
        pred = (y_prob >= threshold).astype(np.int8)
        pred_count = int(pred.sum())
        rows.append(
            {
                "threshold": threshold,
                "predicted_positive": pred_count,
                "pred_pos_rate": pred_count / len(pred) if len(pred) else 0.0,
                "precision": float(precision_score(y_true, pred, zero_division=0)) if len(pred) else 0.0,
                "recall": float(recall_score(y_true, pred, zero_division=0)) if len(pred) else 0.0,
            }
        )
    for row in rows:
        precision = float(row["precision"])
        recall = float(row["recall"])
        row["f1"] = 2.0 * precision * recall / (precision + recall) if precision + recall > 0.0 else 0.0
    return rows


def select_threshold(rows: list[dict[str, Any]], min_precision: float, min_predictions: int) -> tuple[float, str]:
    eligible = [
        row for row in rows
        if row["predicted_positive"] >= min_predictions and row["precision"] >= min_precision
    ]
    if eligible:
        chosen = max(eligible, key=lambda row: (row["f1"], row["precision"], row["recall"], row["threshold"]))
        return float(chosen["threshold"]), "eligible_min_precision"
    with_predictions = [row for row in rows if row["predicted_positive"] >= max(1, min_predictions)]
    pool = with_predictions or rows
    chosen = max(pool, key=lambda row: (row["f1"], row["precision"], row["recall"], -row["threshold"]))
    return float(chosen["threshold"]), "fallback_best_f1"


def train_side(df: pd.DataFrame, side: str, args: argparse.Namespace) -> dict[str, Any]:
    target_col = TARGET_BY_SIDE[side]
    expected_col = EXPECTED_R_BY_SIDE[side]
    side_df = df[df["SetupSide"].astype(str).str.lower() == side].copy()
    if len(side_df) < args.min_rows:
        return {"side": side, "status": "skipped_insufficient_rows", "rows": int(len(side_df))}
    y = pd.to_numeric(side_df[target_col], errors="coerce").fillna(0).astype(np.int8).to_numpy()
    positives = int(y.sum())
    if positives < args.min_positives or len(np.unique(y)) < 2:
        return {"side": side, "status": "skipped_insufficient_positives", "rows": int(len(side_df)), "positives": positives}

    X, feature_columns = build_feature_matrix(side_df, exclude_feature_groups=args.exclude_feature_groups)
    train_idx, holdout_idx = chronological_split(side_df, args.train_frac)
    train_pos = int(pd.Series(y, index=side_df.index).loc[train_idx].sum())
    holdout_y = pd.Series(y, index=side_df.index).loc[holdout_idx].to_numpy(dtype=np.int8)
    if train_pos < 1 or len(np.unique(pd.Series(y, index=side_df.index).loc[train_idx])) < 2:
        return {"side": side, "status": "skipped_train_split_single_class", "rows": int(len(side_df)), "positives": positives}

    model = build_model(side, args)
    model.fit(X.loc[train_idx].to_numpy(dtype=np.float32), pd.Series(y, index=side_df.index).loc[train_idx].to_numpy(dtype=np.int8))
    holdout_prob = model.predict_proba(X.loc[holdout_idx].to_numpy(dtype=np.float32))[:, positive_class_index(model)]
    grid = [round(v, 4) for v in np.arange(args.threshold_min, args.threshold_max + 1e-9, args.threshold_step)]
    threshold_rows = threshold_metrics(holdout_y, holdout_prob, grid)
    selected_threshold, selected_reason = select_threshold(threshold_rows, args.min_precision, args.min_holdout_predictions)
    selected_pred = (holdout_prob >= selected_threshold).astype(np.int8)
    holdout_expected_r = pd.to_numeric(side_df.loc[holdout_idx, expected_col], errors="coerce").to_numpy(dtype=float)
    selected_expected_r = holdout_expected_r[selected_pred.astype(bool)]

    holdout_rows = side_df.loc[
        holdout_idx,
        ["arm_id", "Symbol", "SetupEpochSec", "SetupTimeUtc", "SetupProb", "SetupThresholdMargin", target_col, expected_col],
    ].copy()
    holdout_rows["side"] = side
    holdout_rows["downstream_filter_prob"] = holdout_prob
    holdout_rows["downstream_filter_threshold"] = selected_threshold
    holdout_rows["downstream_filter_predicted_positive"] = selected_pred

    importance_rows = []
    if hasattr(model, "feature_importances_"):
        for column, importance in sorted(zip(feature_columns, model.feature_importances_), key=lambda item: item[1], reverse=True):
            importance_rows.append({"side": side, "feature": column, "importance": float(importance)})

    model_path = args.output_dir / f"{side}_downstream_setup_filter.pkl"
    with model_path.open("wb") as handle:
        pickle.dump(
            {
                "model": model,
                "model_family": type(model).__name__,
                "feature_columns": feature_columns,
                "feature_preset": args.feature_preset,
                "excluded_feature_groups": sorted(args.exclude_feature_groups),
                "selected_threshold": selected_threshold,
                "schema_version": BUNDLE_SCHEMA_VERSION,
            },
            handle,
        )

    return {
        "side": side,
        "status": "trained",
        "model_family": type(model).__name__,
        "feature_preset": args.feature_preset,
        "excluded_feature_groups": ";".join(sorted(args.exclude_feature_groups)),
        "rows": int(len(side_df)),
        "positives": positives,
        "positive_rate": positives / len(side_df),
        "feature_count": len(feature_columns),
        "train_rows": int(len(train_idx)),
        "train_positives": train_pos,
        "holdout_rows": int(len(holdout_idx)),
        "holdout_positives": int(holdout_y.sum()),
        "holdout_positive_rate": float(holdout_y.mean()) if len(holdout_y) else 0.0,
        "holdout_auc": safe_auc(holdout_y, holdout_prob),
        "holdout_average_precision": safe_average_precision(holdout_y, holdout_prob),
        "holdout_brier_score": float(brier_score_loss(holdout_y, holdout_prob)) if len(np.unique(holdout_y)) >= 2 else None,
        "selected_threshold": selected_threshold,
        "selected_threshold_reason": selected_reason,
        "selected_precision": float(precision_score(holdout_y, selected_pred, zero_division=0)),
        "selected_recall": float(recall_score(holdout_y, selected_pred, zero_division=0)),
        "selected_predicted_positive": int(selected_pred.sum()),
        "selected_pred_pos_rate": float(selected_pred.mean()) if len(selected_pred) else 0.0,
        "selected_expected_net_r_mean": safe_mean(selected_expected_r),
        "selected_expected_net_r_p50": safe_quantile(selected_expected_r, 0.50),
        "selected_expected_net_r_positive_count": int(np.sum(selected_expected_r > 0.0)) if len(selected_expected_r) else 0,
        "holdout_expected_net_r_mean": safe_mean(holdout_expected_r),
        "model_path": str(model_path),
        "threshold_grid": threshold_rows,
        "holdout_predictions": holdout_rows,
        "feature_importance": importance_rows,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        fieldnames = list(rows[0].keys())
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    else:
        fieldnames = []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)


def write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Downstream Setup Filter Research Bundle",
        "",
        f"Generated: `{manifest['generated_at_utc']}`",
        "",
        f"- Schema: `{manifest['schema_version']}`",
        f"- Training rows: `{manifest['training_rows_csv']}`",
        f"- Model family: `{manifest['model_family']}`",
        f"- Feature preset: `{manifest['feature_preset']}`",
        f"- Excluded optional feature groups: `{', '.join(manifest['feature_selection']['excluded_groups']) or 'none'}`",
        "",
        "| side | model | status | rows | positives | features | holdout AP | selected threshold | precision | recall | predicted + | selected mean R | selected p50 R |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in manifest["scorecard"]:
        lines.append(
            "| {side} | {model} | {status} | {rows} | {positives} | {features} | {ap} | {thr} | {precision} | {recall} | {pred} | {mean_r} | {p50_r} |".format(
                side=row.get("side", ""),
                model=row.get("model_family", ""),
                status=row.get("status", ""),
                rows=row.get("rows", ""),
                positives=row.get("positives", ""),
                features=row.get("feature_count", ""),
                ap="" if row.get("holdout_average_precision") is None else f"{row['holdout_average_precision']:.4f}",
                thr="" if row.get("selected_threshold") is None else f"{row['selected_threshold']:.4f}",
                precision="" if row.get("selected_precision") is None else f"{row['selected_precision']:.4f}",
                recall="" if row.get("selected_recall") is None else f"{row['selected_recall']:.4f}",
                pred=row.get("selected_predicted_positive", ""),
                mean_r="" if row.get("selected_expected_net_r_mean") is None else f"{row['selected_expected_net_r_mean']:.4f}",
                p50_r="" if row.get("selected_expected_net_r_p50") is None else f"{row['selected_expected_net_r_p50']:.4f}",
            )
        )
    lines.extend([
        "",
        "Warnings:",
        "",
    ])
    for warning in manifest.get("warnings", []):
        lines.append(f"- {warning}")
    lines.extend([
        "",
        "Promotion status remains **NO-GO**. These filters must be wired into a controlled replay experiment and pass promotion gates before use.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train research-only downstream setup arm-quality filters.")
    parser.add_argument("--training-rows-csv", required=True, type=Path, help="setup_downstream_training_rows_v1.csv")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output research bundle directory.")
    parser.add_argument("--model-family", default="catboost", choices=["catboost", "random_forest", "rf"], help="Second-stage setup-quality learner family.")
    parser.add_argument(
        "--feature-preset",
        default="catboost_cost_aware_core",
        choices=sorted(FEATURE_PRESETS),
        help="Feature schema preset. catboost_cost_aware_core excludes optional enriched meta and Databento silver aggregate features.",
    )
    parser.add_argument("--exclude-enriched-meta-features", action="store_true", help="Drop optional tsm/regime/news/seq/setup enriched meta feature columns.")
    parser.add_argument("--exclude-databento-silver-features", action="store_true", help="Drop optional EqMbp1*/OpraTcbbo* Databento silver aggregate columns.")
    parser.add_argument("--train-frac", type=float, default=0.70, help="Chronological train fraction; remainder is holdout.")
    parser.add_argument("--min-rows", type=int, default=200)
    parser.add_argument("--min-positives", type=int, default=20)
    parser.add_argument("--n-estimators", type=int, default=400)
    parser.add_argument("--min-samples-leaf", type=int, default=10)
    parser.add_argument("--max-depth", type=int, default=0, help="0 means unlimited depth.")
    parser.add_argument("--max-features", default="sqrt", help="RandomForest max_features value; default matches sklearn classifier default for classification.")
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-jobs", type=int, default=-1)
    parser.add_argument("--threshold-min", type=float, default=0.05)
    parser.add_argument("--threshold-max", type=float, default=0.95)
    parser.add_argument("--threshold-step", type=float, default=0.05)
    parser.add_argument("--min-precision", type=float, default=0.40)
    parser.add_argument("--min-holdout-predictions", type=int, default=5)
    parser.add_argument("--catboost-iterations", type=int, default=500)
    parser.add_argument("--catboost-depth", type=int, default=5)
    parser.add_argument("--catboost-learning-rate", type=float, default=0.04)
    parser.add_argument("--catboost-l2-leaf-reg", type=float, default=6.0)
    args = parser.parse_args(argv)
    excluded = set(FEATURE_PRESETS[args.feature_preset])
    if args.exclude_enriched_meta_features:
        excluded.add("enriched_meta")
    if args.exclude_databento_silver_features:
        excluded.add("databento_silver")
    args.exclude_feature_groups = normalize_exclude_feature_groups(excluded)
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.training_rows_csv.is_file():
        raise FileNotFoundError(f"missing training rows CSV: {args.training_rows_csv}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.training_rows_csv)
    if "SetupSide" not in df.columns:
        raise ValueError("training rows missing SetupSide")
    df["SetupSide"] = df["SetupSide"].astype(str).str.lower()
    df["SetupEpochSec"] = pd.to_numeric(df["SetupEpochSec"], errors="coerce")
    df = df[df["SetupEpochSec"].notna()].copy()

    feature_selection = feature_exclusion_summary(list(df.columns), args.exclude_feature_groups)
    print(
        f"[DOWNSTREAM_FILTER] loaded rows={len(df)} from {args.training_rows_csv} "
        f"model_family={args.model_family} feature_preset={args.feature_preset} "
        f"excluded_groups={sorted(args.exclude_feature_groups)} excluded_counts={feature_selection['excluded_counts']}",
        flush=True,
    )
    results = [train_side(df, side, args) for side in ("long", "short")]

    scorecard_rows = []
    threshold_rows = []
    importance_rows = []
    holdout_frames = []
    manifest_results = []
    for result in results:
        score_row = {k: v for k, v in result.items() if k not in {"threshold_grid", "holdout_predictions", "feature_importance"}}
        scorecard_rows.append(score_row)
        if result.get("status") == "trained":
            threshold_rows.extend({"side": result["side"], **row} for row in result["threshold_grid"])
            importance_rows.extend(result["feature_importance"])
            holdout_frames.append(result["holdout_predictions"])
        manifest_results.append(score_row)

    outputs = {
        "scorecard_csv": args.output_dir / "downstream_setup_filter_scorecard.csv",
        "threshold_grid_csv": args.output_dir / "downstream_setup_filter_threshold_grid.csv",
        "feature_importance_csv": args.output_dir / "downstream_setup_filter_feature_importance.csv",
        "holdout_predictions_csv": args.output_dir / "downstream_setup_filter_holdout_predictions.csv",
        "manifest_json": args.output_dir / "downstream_setup_filter_manifest.json",
        "summary_md": args.output_dir / "downstream_setup_filter_summary.md",
    }
    write_csv(outputs["scorecard_csv"], scorecard_rows)
    write_csv(outputs["threshold_grid_csv"], threshold_rows)
    write_csv(outputs["feature_importance_csv"], importance_rows)
    if holdout_frames:
        pd.concat(holdout_frames, ignore_index=True).to_csv(outputs["holdout_predictions_csv"], index=False)
    else:
        pd.DataFrame().to_csv(outputs["holdout_predictions_csv"], index=False)

    manifest = {
        "generated_at_utc": utc_now(),
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "training_rows_csv": str(args.training_rows_csv),
        "outputs": {name: str(path) for name, path in outputs.items()},
        "model_family": args.model_family,
        "feature_preset": args.feature_preset,
        "feature_selection": feature_selection,
        "scorecard": manifest_results,
        "warnings": [
            "Research-only arm-quality filter trained on replay arms, not all candidate setup bars.",
            "Default CatBoost cost-aware core preset intentionally excludes optional enriched meta and Databento silver aggregate columns so those features are not required by training or Java replay/live scoring.",
            "Feature importances and holdout metrics are directional; promotion requires integration, replay, calibration, dominance, parity, and paper/shadow gates.",
            "Future-outcome label fields are explicitly excluded from model features to avoid leakage.",
        ],
    }
    outputs["manifest_json"].write_text(json.dumps(json_safe(manifest), indent=2), encoding="utf-8")
    write_markdown(outputs["summary_md"], manifest)
    print(f"[DOWNSTREAM_FILTER] done output={args.output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())





