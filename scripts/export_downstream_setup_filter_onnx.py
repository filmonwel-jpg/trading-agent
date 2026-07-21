#!/usr/bin/env python3
"""Export downstream setup-filter research bundles as ONNX artifacts.

The downstream setup filters are trained by ``train_downstream_setup_filter.py``
and saved as side-specific pickle files. This exporter converts those sklearn
models to ONNX, writes explicit feature-order schemas, and emits a route manifest
that a controlled replay adapter can consume later.

This remains research-only. The exported artifacts do not by themselves prove
Java/live feature parity, calibration, replay PnL, or promotion readiness.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import pickle
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "downstream_setup_filter_onnx_research_v1"
INPUT_NAME = "features"
LABEL_OUTPUT_NAME = "label"
PROBABILITY_OUTPUT_NAME = "probabilities"
SIDE_TO_ROUTE = {
    "long": "longDownstreamSetupFilterAi",
    "short": "shortDownstreamSetupFilterAi",
}


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return json_safe(value.tolist())
    if isinstance(value, np.generic):
        return json_safe(value.item())
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    payload = json.dumps(json_safe(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_filter_module(script_path: Path):
    spec = importlib.util.spec_from_file_location("train_downstream_setup_filter_for_onnx_export", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load filter module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_filter_bundle(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        bundle = pickle.load(handle)
    required = ["model", "feature_columns", "selected_threshold"]
    missing = [key for key in required if key not in bundle]
    if missing:
        raise ValueError(f"filter bundle {path} missing required keys: {', '.join(missing)}")
    if not hasattr(bundle["model"], "predict_proba"):
        raise TypeError(f"filter bundle {path} model does not expose predict_proba")
    feature_columns = list(bundle["feature_columns"])
    if not feature_columns or any(not str(column).strip() for column in feature_columns):
        raise ValueError(f"filter bundle {path} has an empty/invalid feature column list")
    bundle["feature_columns"] = [str(column) for column in feature_columns]
    bundle["selected_threshold"] = float(bundle["selected_threshold"])
    return bundle


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


def convert_model_to_onnx(model: Any, feature_count: int, target_opset: int) -> bytes:
    if type(model).__name__ == "CatBoostClassifier" and hasattr(model, "save_model"):
        with tempfile.NamedTemporaryFile(suffix=".onnx") as tmp:
            model.save_model(
                tmp.name,
                format="onnx",
                export_parameters={
                    "onnx_graph_name": "downstream_setup_filter",
                    "onnx_domain": "com.tradingagent.research",
                    "onnx_model_version": 1,
                    "onnx_doc_string": "Research-only downstream setup-quality CatBoost filter",
                },
            )
            return Path(tmp.name).read_bytes()

    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
    except ImportError as exc:
        raise RuntimeError(
            "ONNX export requires skl2onnx. Install project requirements before exporting."
        ) from exc

    initial_types = [(INPUT_NAME, FloatTensorType([None, feature_count]))]
    onnx_model = convert_sklearn(
        model,
        initial_types=initial_types,
        target_opset=target_opset,
        options={id(model): {"zipmap": False}},
    )
    return onnx_model.SerializeToString()


def check_onnx_model(path: Path) -> dict[str, Any]:
    try:
        import onnx
    except ImportError as exc:
        raise RuntimeError("ONNX export validation requires the onnx package.") from exc
    model = onnx.load(str(path))
    onnx.checker.check_model(model)
    return {
        "ir_version": int(model.ir_version),
        "producer_name": model.producer_name,
        "opset_imports": {entry.domain or "ai.onnx": int(entry.version) for entry in model.opset_import},
        "inputs": [value.name for value in model.graph.input],
        "outputs": [value.name for value in model.graph.output],
    }


def class_labels(model: Any) -> list[Any]:
    labels = list(getattr(model, "classes_", []))
    return labels if labels else [0, 1]


def label_lookup_candidates(label: Any) -> list[Any]:
    candidates: list[Any] = [label, str(label)]
    for coercer in (int, float):
        try:
            candidates.append(coercer(label))
        except (TypeError, ValueError):
            pass
    deduped: list[Any] = []
    for candidate in candidates:
        if not any(candidate == existing and type(candidate) is type(existing) for existing in deduped):
            deduped.append(candidate)
    return deduped


def probability_matrix_from_zipmap(output: Any, expected_rows: int, labels: list[Any]) -> np.ndarray | None:
    rows = output.tolist() if isinstance(output, np.ndarray) and output.dtype == object else output
    if not isinstance(rows, (list, tuple)) or len(rows) != expected_rows:
        return None
    matrix: list[list[float]] = []
    for row in rows:
        if not hasattr(row, "items"):
            return None
        values: list[float] = []
        for label in labels:
            value = None
            for candidate in label_lookup_candidates(label):
                if candidate in row:
                    value = row[candidate]
                    break
            if value is None:
                return None
            values.append(float(value))
        matrix.append(values)
    return np.asarray(matrix, dtype=np.float64)


def probability_matrix_from_output(output: Any, expected_rows: int, labels: list[Any]) -> np.ndarray | None:
    zipmap_matrix = probability_matrix_from_zipmap(output, expected_rows=expected_rows, labels=labels)
    if zipmap_matrix is not None:
        return zipmap_matrix
    candidate = np.asarray(output)
    if candidate.ndim == 2 and candidate.shape[0] == expected_rows and candidate.shape[1] >= len(labels):
        return candidate.astype(np.float64)
    return None


def find_probability_output(output_names: list[str], outputs: list[Any], expected_rows: int, labels: list[Any]) -> np.ndarray:
    preferred_outputs = [output for name, output in zip(output_names, outputs) if name == PROBABILITY_OUTPUT_NAME]
    for output in [*preferred_outputs, *outputs]:
        matrix = probability_matrix_from_output(output, expected_rows=expected_rows, labels=labels)
        if matrix is not None:
            return matrix
    raise ValueError("Unable to locate an ONNX probability output")


def run_onnx_for_validation(onnx_path: Path, x: np.ndarray) -> tuple[str, list[str], list[Any]]:
    try:
        import onnxruntime as ort

        session = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
        output_names = [output.name for output in session.get_outputs()]
        return "onnxruntime", output_names, session.run(None, {INPUT_NAME: x})
    except ImportError:
        try:
            from onnx.reference import ReferenceEvaluator
        except ImportError as exc:
            raise RuntimeError("neither onnxruntime nor onnx.reference is available") from exc
        evaluator = ReferenceEvaluator(str(onnx_path))
        output_names = [str(name) for name in evaluator.output_names]
        return "onnx_reference", output_names, evaluator.run(None, {INPUT_NAME: x})


def validate_onnx_runtime(
    *,
    side: str,
    bundle: dict[str, Any],
    onnx_path: Path,
    training_rows_csv: Path | None,
    filter_module: Any | None,
    sample_rows: int,
    atol: float,
    rtol: float,
) -> dict[str, Any]:
    if training_rows_csv is None:
        return {"status": "skipped_no_training_rows_csv"}
    if filter_module is None:
        return {"status": "skipped_no_filter_module"}

    df = pd.read_csv(training_rows_csv)
    side_df = df[df["SetupSide"].astype(str).str.lower() == side].copy() if "SetupSide" in df.columns else df.copy()
    if side_df.empty:
        return {"status": "skipped_no_rows_for_side"}
    side_df = side_df.head(max(1, sample_rows)).copy()
    matrix, _ = filter_module.build_feature_matrix(side_df, list(bundle["feature_columns"]))
    x = matrix.to_numpy(dtype=np.float32)
    labels = class_labels(bundle["model"])
    sklearn_prob = bundle["model"].predict_proba(x)[:, positive_class_index(bundle["model"])].astype(np.float64)

    try:
        runtime_name, output_names, outputs = run_onnx_for_validation(onnx_path, x)
    except RuntimeError as exc:
        return {"status": "skipped_missing_onnx_runtime", "reason": str(exc)}
    onnx_prob_matrix = find_probability_output(output_names, outputs, expected_rows=x.shape[0], labels=labels)
    onnx_prob = onnx_prob_matrix[:, positive_class_index(bundle["model"])]
    max_abs_delta = float(np.max(np.abs(sklearn_prob - onnx_prob))) if len(sklearn_prob) else 0.0
    passed = bool(np.allclose(sklearn_prob, onnx_prob, atol=atol, rtol=rtol))
    return {
        "status": f"passed_{runtime_name}" if passed else f"failed_{runtime_name}",
        "runtime": runtime_name,
        "rows": int(x.shape[0]),
        "max_abs_delta": max_abs_delta,
        "atol": float(atol),
        "rtol": float(rtol),
        "onnx_outputs": output_names,
    }


def feature_schema(side: str, bundle: dict[str, Any], *, filter_script: Path, source_bundle_path: Path) -> dict[str, Any]:
    feature_columns = list(bundle["feature_columns"])
    model = bundle["model"]
    classes = list(getattr(model, "classes_", []))
    return {
        "schema_version": SCHEMA_VERSION,
        "side": side,
        "source_pickle": str(source_bundle_path),
        "model_family": type(model).__name__,
        "input": {
            "name": INPUT_NAME,
            "dtype": "float32",
            "shape": [None, len(feature_columns)],
            "feature_count": len(feature_columns),
            "feature_columns": feature_columns,
            "feature_columns_sha256": sha256_json(feature_columns),
            "feature_preset": bundle.get("feature_preset"),
            "excluded_feature_groups": bundle.get("excluded_feature_groups", []),
        },
        "outputs": {
            "label": LABEL_OUTPUT_NAME,
            "probabilities": PROBABILITY_OUTPUT_NAME,
            "classes": classes,
            "positive_class_label": 1,
            "positive_class_index": positive_class_index(model),
        },
        "threshold": {
            "selected_threshold": float(bundle["selected_threshold"]),
            "source": "train_downstream_setup_filter.py:selected_threshold",
        },
        "transform_contract": {
            "script": str(filter_script),
            "function": "build_feature_matrix",
            "input_expectation": "Runtime/replay adapter must provide the same raw setup-row columns used by the trainer, then rebuild this exact float32 feature vector order.",
            "optional_enriched_features": "Feature schemas trained with catboost_cost_aware_core intentionally omit enriched meta and Databento silver aggregate columns; Java may provide them, but they are not required unless present in feature_columns.",
            "categorical_handling": "Symbol, SetupArbitrationReason, and SessionBucket are one-hot encoded by pandas.get_dummies; unseen categories become all-zero for that category family after reindexing.",
            "null_handling": "Numeric conversion uses pandas.to_numeric(errors='coerce'), replaces infinities with NaN, fills NaN with 0.0, and casts to float32.",
            "excluded_future_fields": [
                "Label_*",
                "Expected_*",
                "Max_Future_Micro_*",
                "Best_*",
                "MicroEvalWithinTtlCount",
            ],
        },
    }


def write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Downstream Setup Filter ONNX Research Export",
        "",
        f"Generated: `{manifest['generated_at_utc']}`",
        "",
        f"- Schema: `{manifest['schema_version']}`",
        f"- Source bundle dir: `{manifest['source']['filter_bundle_dir']}`",
        f"- Target opset: `{manifest['target_opset']}`",
        f"- Promotion status: **{manifest['promotion_status']}**",
        "",
        "## Routes",
        "",
        "| route | side | ONNX | features | threshold | model sha256 | validation |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for route in manifest.get("routes", []):
        validation = route.get("validation", {})
        validation_label = validation.get("status", "")
        if validation.get("max_abs_delta") is not None:
            validation_label += f" max_abs_delta={validation['max_abs_delta']:.8g}"
        lines.append(
            "| {route_name} | {side} | `{onnx_model}` | {feature_count} | {threshold:.4f} | `{sha}` | {validation} |".format(
                route_name=route["route_name"],
                side=route["side"],
                onnx_model=route["onnx_model"],
                feature_count=route["feature_count"],
                threshold=float(route["selected_threshold"]),
                sha=route["onnx_sha256"][:12],
                validation=validation_label,
            )
        )
    best_policy = manifest.get("offline_policy", {}).get("best_policy")
    if best_policy:
        lines.extend(
            [
                "",
                "## Embedded offline policy screen",
                "",
                f"- Filter threshold label: `{best_policy.get('filter_threshold_label') or best_policy.get('filter_threshold')}`",
                f"- Micro threshold: `{best_policy.get('micro_threshold')}`",
                f"- Confirms: `{best_policy.get('confirms')}`",
                f"- Positive outcomes: `{best_policy.get('positive')}`",
                f"- Expected net R mean: `{best_policy.get('expected_net_r_mean')}`",
                f"- Expected net R sum: `{best_policy.get('expected_net_r_sum')}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Warnings",
            "",
        ]
    )
    for warning in manifest.get("warnings", []):
        lines.append(f"- {warning}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export downstream setup-filter research pickle bundles to ONNX plus manifests.")
    parser.add_argument("--filter-bundle-dir", required=True, type=Path, help="Directory containing long/short downstream setup filter pickle files.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output directory for ONNX artifacts and manifests.")
    parser.add_argument("--filter-script", default=Path("scripts/train_downstream_setup_filter.py"), type=Path, help="Trainer script that owns build_feature_matrix semantics.")
    parser.add_argument("--training-rows-csv", type=Path, help="Optional setup_downstream_training_rows_v1.csv for ONNX-vs-sklearn parity validation.")
    parser.add_argument("--source-manifest-json", type=Path, help="Optional downstream_setup_filter_manifest.json from training.")
    parser.add_argument("--policy-summary-json", type=Path, help="Optional downstream_filter_replay_policy_summary.json to embed current best offline policy context.")
    parser.add_argument("--target-opset", type=int, default=15)
    parser.add_argument("--sample-rows", type=int, default=512, help="Per-side sample rows for optional onnxruntime validation.")
    parser.add_argument("--validation-atol", type=float, default=1e-6)
    parser.add_argument("--validation-rtol", type=float, default=1e-5)
    parser.add_argument("--require-onnxruntime-validation", action="store_true", help="Fail if ONNX probability validation via onnxruntime or onnx.reference is unavailable or fails.")
    return parser.parse_args(argv)


def validate_inputs(args: argparse.Namespace) -> None:
    if not args.filter_bundle_dir.is_dir():
        raise FileNotFoundError(f"missing filter bundle dir: {args.filter_bundle_dir}")
    if not args.filter_script.is_file():
        raise FileNotFoundError(f"missing filter script: {args.filter_script}")
    for optional_path in [args.training_rows_csv, args.source_manifest_json, args.policy_summary_json]:
        if optional_path is not None and not optional_path.is_file():
            raise FileNotFoundError(optional_path)


def load_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def export_side(side: str, args: argparse.Namespace, filter_module: Any | None) -> tuple[dict[str, Any], str]:
    source_pickle = args.filter_bundle_dir / f"{side}_downstream_setup_filter.pkl"
    if not source_pickle.is_file():
        raise FileNotFoundError(source_pickle)
    bundle = load_filter_bundle(source_pickle)
    feature_count = len(bundle["feature_columns"])
    onnx_path = args.output_dir / f"{side}_downstream_setup_filter.onnx"
    schema_path = args.output_dir / f"{side}_downstream_setup_filter_feature_schema.json"

    onnx_path.write_bytes(convert_model_to_onnx(bundle["model"], feature_count, args.target_opset))
    onnx_metadata = check_onnx_model(onnx_path)
    schema = feature_schema(side, bundle, filter_script=args.filter_script, source_bundle_path=source_pickle)
    schema_path.write_text(json.dumps(json_safe(schema), indent=2), encoding="utf-8")
    validation = validate_onnx_runtime(
        side=side,
        bundle=bundle,
        onnx_path=onnx_path,
        training_rows_csv=args.training_rows_csv,
        filter_module=filter_module,
        sample_rows=args.sample_rows,
        atol=args.validation_atol,
        rtol=args.validation_rtol,
    )
    if args.require_onnxruntime_validation and not str(validation.get("status", "")).startswith("passed_"):
        raise RuntimeError(f"ONNX runtime validation did not pass for {side}: {validation}")

    route = {
        "route_name": SIDE_TO_ROUTE[side],
        "side": side,
        "status": "research_only_no_go",
        "onnx_model": str(onnx_path),
        "onnx_model_filename": onnx_path.name,
        "onnx_sha256": sha256_file(onnx_path),
        "feature_schema": str(schema_path),
        "feature_schema_filename": schema_path.name,
        "feature_schema_sha256": sha256_file(schema_path),
        "feature_columns_sha256": schema["input"]["feature_columns_sha256"],
        "feature_count": feature_count,
        "feature_preset": bundle.get("feature_preset"),
        "excluded_feature_groups": bundle.get("excluded_feature_groups", []),
        "input_name": INPUT_NAME,
        "label_output_name": LABEL_OUTPUT_NAME,
        "probability_output_name": PROBABILITY_OUTPUT_NAME,
        "positive_class_index": schema["outputs"]["positive_class_index"],
        "positive_class_label": 1,
        "selected_threshold": float(bundle["selected_threshold"]),
        "model_family": type(bundle["model"]).__name__,
        "source_pickle": str(source_pickle),
        "source_pickle_sha256": sha256_file(source_pickle),
        "onnx_metadata": onnx_metadata,
        "validation": validation,
    }
    print(
        f"[DOWNSTREAM_FILTER_ONNX] side={side} features={feature_count} threshold={bundle['selected_threshold']:.4f} "
        f"onnx={onnx_path.name} validation={validation.get('status')}",
        flush=True,
    )
    return route, str(schema_path)


def build_outputs(output_dir: Path, feature_schema_paths: dict[str, str]) -> dict[str, Any]:
    return {
        "route_manifest_json": output_dir / "downstream_setup_filter_route_manifest.json",
        "artifact_manifest_json": output_dir / "downstream_setup_filter_onnx_manifest.json",
        "summary_md": output_dir / "downstream_setup_filter_onnx_summary.md",
        "feature_schemas": feature_schema_paths,
    }


def build_manifest(
    args: argparse.Namespace,
    routes: list[dict[str, Any]],
    source_manifest: dict[str, Any] | None,
    policy_summary: dict[str, Any] | None,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    return {
        "generated_at_utc": utc_now(),
        "schema_version": SCHEMA_VERSION,
        "promotion_status": "NO-GO",
        "target_opset": args.target_opset,
        "source": {
            "filter_bundle_dir": str(args.filter_bundle_dir),
            "filter_script": str(args.filter_script),
            "training_rows_csv": str(args.training_rows_csv) if args.training_rows_csv else None,
            "source_manifest_json": str(args.source_manifest_json) if args.source_manifest_json else None,
            "policy_summary_json": str(args.policy_summary_json) if args.policy_summary_json else None,
        },
        "routes": routes,
        "source_training_manifest": source_manifest,
        "offline_policy": {
            "summary_json": str(args.policy_summary_json) if args.policy_summary_json else None,
            "side_filter_threshold_selected": (policy_summary or {}).get("side_filter_threshold_selected"),
            "best_policy": (policy_summary or {}).get("best_policy"),
        },
        "outputs": {name: str(path) if isinstance(path, Path) else path for name, path in outputs.items()},
        "warnings": [
            "Research-only export of filters trained on replay-observed setup arms, not all possible runtime setup candidates.",
            "ONNX conversion preserves model scoring, but Java/replay feature-vector parity is not proven by this artifact alone.",
            "Runtime use must rebuild the exact feature order from the per-side feature schema and enforce manifest/hash checks.",
            "Promotion remains NO-GO until controlled Java replay, lifecycle summaries, calibration, dominance, parity, and paper/shadow gates pass.",
        ],
    }


def write_manifests(outputs: dict[str, Any], manifest: dict[str, Any]) -> None:
    route_manifest = {
        "generated_at_utc": manifest["generated_at_utc"],
        "schema_version": SCHEMA_VERSION,
        "promotion_status": manifest["promotion_status"],
        "routes": manifest["routes"],
        "offline_policy": manifest["offline_policy"],
        "warnings": manifest["warnings"],
    }
    outputs["route_manifest_json"].write_text(json.dumps(json_safe(route_manifest), indent=2), encoding="utf-8")
    outputs["artifact_manifest_json"].write_text(json.dumps(json_safe(manifest), indent=2), encoding="utf-8")
    write_markdown(outputs["summary_md"], manifest)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_inputs(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    filter_module = load_filter_module(args.filter_script) if args.training_rows_csv is not None else None
    source_manifest = load_optional_json(args.source_manifest_json)
    policy_summary = load_optional_json(args.policy_summary_json)

    routes: list[dict[str, Any]] = []
    feature_schema_paths: dict[str, str] = {}
    print(f"[DOWNSTREAM_FILTER_ONNX] exporting bundle={args.filter_bundle_dir} output={args.output_dir}", flush=True)
    for side in ["long", "short"]:
        route, schema_path = export_side(side, args, filter_module)
        routes.append(route)
        feature_schema_paths[side] = schema_path

    outputs = build_outputs(args.output_dir, feature_schema_paths)
    manifest = build_manifest(args, routes, source_manifest, policy_summary, outputs)
    write_manifests(outputs, manifest)
    print(f"[DOWNSTREAM_FILTER_ONNX] done manifest={outputs['artifact_manifest_json']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




