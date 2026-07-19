#!/usr/bin/env python3
"""Evaluate a downstream setup-filter policy against counterfactual replay decisions.

This is an offline, research-only evaluator. It does not place trades and does not
change Java runtime behavior. It answers: if a setup arm first had to pass the
research downstream setup filter, and then micro-entry had to first-cross a chosen
threshold during the TTL, what would the counterfactual trade volume and net-R
shape look like on the completed replay?
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "downstream_filter_replay_policy_eval_v1"


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


def parse_float_list(raw: str) -> list[float]:
    values = []
    for part in raw.replace(",", " ").split():
        try:
            values.append(round(float(part), 6))
        except ValueError:
            continue
    if not values:
        raise ValueError("empty threshold list")
    return sorted(set(values))


def load_filter_module(script_path: Path):
    spec = importlib.util.spec_from_file_location("train_downstream_setup_filter_for_eval", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load filter module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_filter_bundle(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        bundle = pickle.load(handle)
    for key in ["model", "feature_columns", "selected_threshold"]:
        if key not in bundle:
            raise ValueError(f"filter bundle {path} missing {key}")
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


def score_side(df: pd.DataFrame, side: str, bundle: dict[str, Any], filter_module: Any) -> pd.Series:
    side_mask = df["SetupSide"].astype(str).str.lower() == side
    out = pd.Series(np.nan, index=df.index, dtype=float)
    if not side_mask.any():
        return out
    matrix, _ = filter_module.build_feature_matrix(df.loc[side_mask].copy(), list(bundle["feature_columns"]))
    prob = bundle["model"].predict_proba(matrix.to_numpy(dtype=np.float32))[:, positive_class_index(bundle["model"])]
    out.loc[side_mask] = prob
    return out


def safe_mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return None
    return float(values.mean())


def safe_quantile(series: pd.Series, q: float) -> float | None:
    values = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if values.empty:
        return None
    return float(values.quantile(q))


def max_day_dominance(decisions: pd.DataFrame) -> tuple[float, str]:
    if decisions.empty or "confirm_eval_epoch_utc" not in decisions.columns:
        return 0.0, ""
    days = decisions["confirm_eval_epoch_utc"].astype(str).str.slice(0, 10)
    counts = days.value_counts()
    if counts.empty:
        return 0.0, ""
    top_day = str(counts.index[0])
    return float(counts.iloc[0] / len(decisions)), top_day


def summarize_policy(decisions: pd.DataFrame, arms_count: int, *, filter_threshold: float, micro_threshold: float) -> dict[str, Any]:
    confirmed = decisions[decisions["counterfactual_confirms"].astype(int) == 1].copy()
    expected = pd.to_numeric(confirmed.get("expected_net_r", pd.Series(dtype=float)), errors="coerce")
    positive = expected > 0.0
    target = confirmed.get("exit_reason", pd.Series(dtype=str)).astype(str).eq("target")
    stop = confirmed.get("exit_reason", pd.Series(dtype=str)).astype(str).isin(["stop", "ambiguous_stop_first"])
    dominance, top_day = max_day_dominance(confirmed)
    by_symbol_side = {}
    if not confirmed.empty:
        for (symbol, side), group in confirmed.groupby(["symbol", "side"], dropna=False):
            group_expected = pd.to_numeric(group["expected_net_r"], errors="coerce")
            by_symbol_side[f"{symbol}.{side}"] = {
                "confirms": int(len(group)),
                "positive": int((group_expected > 0.0).sum()),
                "expected_net_r_mean": safe_mean(group_expected),
                "expected_net_r_p50": safe_quantile(group_expected, 0.50),
            }
    return {
        "filter_threshold": filter_threshold,
        "micro_threshold": micro_threshold,
        "arms_considered": int(arms_count),
        "filter_pass_arms": int(decisions["filter_pass"].sum()) if "filter_pass" in decisions.columns else 0,
        "confirms": int(len(confirmed)),
        "confirm_rate_per_arm": float(len(confirmed) / arms_count) if arms_count else 0.0,
        "positive": int(positive.sum()),
        "positive_rate_per_confirm": float(positive.mean()) if len(positive) else 0.0,
        "expected_net_r_sum": safe_mean(expected) * int(len(expected.dropna())) if len(expected.dropna()) else None,
        "expected_net_r_mean": safe_mean(expected),
        "expected_net_r_p10": safe_quantile(expected, 0.10),
        "expected_net_r_p50": safe_quantile(expected, 0.50),
        "expected_net_r_p90": safe_quantile(expected, 0.90),
        "target": int(target.sum()) if len(target) else 0,
        "stop": int(stop.sum()) if len(stop) else 0,
        "max_day_dominance_frac": dominance,
        "max_day_dominance_day": top_day,
        "symbol_side_count": int(len(by_symbol_side)),
        "by_symbol_side": by_symbol_side,
    }


def write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Downstream Filter Replay Policy Evaluation",
        "",
        f"Generated: `{manifest['generated_at_utc']}`",
        "",
        f"- Schema: `{manifest['schema_version']}`",
        f"- Training rows: `{manifest['inputs']['training_rows_csv']}`",
        f"- Counterfactual decisions: `{manifest['inputs']['decisions_csv']}`",
        f"- Filter bundle dir: `{manifest['inputs']['filter_bundle_dir']}`",
        "",
        "## Best policy by expected net R mean",
        "",
    ]
    best = manifest.get("best_policy") or {}
    if best:
        best_filter_label = best.get("filter_threshold_label") or best.get("filter_threshold")
        lines.extend([
            f"- Filter threshold: `{best_filter_label}`",
            f"- Micro threshold: `{best.get('micro_threshold')}`",
            f"- Confirms: `{best.get('confirms')}`",
            f"- Positive trades: `{best.get('positive')}`",
            f"- Expected net R mean: `{best.get('expected_net_r_mean')}`",
            f"- Expected net R sum: `{best.get('expected_net_r_sum')}`",
            f"- Symbol/side count: `{best.get('symbol_side_count')}`",
            f"- Max day dominance: `{best.get('max_day_dominance_frac')}` on `{best.get('max_day_dominance_day')}`",
            "",
        ])
    lines.extend([
        "## Policy grid",
        "",
        "| filter threshold | micro threshold | filter pass arms | confirms | positive | mean R | p50 R | sum R | symbols/sides | max day dominance |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in manifest.get("policy_grid", []):
        filter_label = row.get("filter_threshold_label") or f"{float(row['filter_threshold']):.4f}"
        lines.append(
            "| {filter_threshold} | {micro_threshold:.4f} | {filter_pass_arms} | {confirms} | {positive} | {mean_r} | {p50_r} | {sum_r} | {symbol_side_count} | {dom:.4f} |".format(
                filter_threshold=filter_label,
                micro_threshold=float(row["micro_threshold"]),
                filter_pass_arms=row["filter_pass_arms"],
                confirms=row["confirms"],
                positive=row["positive"],
                mean_r="" if row.get("expected_net_r_mean") is None else f"{row['expected_net_r_mean']:.4f}",
                p50_r="" if row.get("expected_net_r_p50") is None else f"{row['expected_net_r_p50']:.4f}",
                sum_r="" if row.get("expected_net_r_sum") is None else f"{row['expected_net_r_sum']:.4f}",
                symbol_side_count=row["symbol_side_count"],
                dom=float(row.get("max_day_dominance_frac") or 0.0),
            )
        )
    lines.extend([
        "",
        "Promotion status remains **NO-GO**. This is an offline counterfactual replay-policy screen, not a live/replay parity or paper/shadow drift gate.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate downstream setup filter + micro-threshold policies on counterfactual replay decisions.")
    parser.add_argument("--training-rows-csv", required=True, type=Path)
    parser.add_argument("--decisions-csv", required=True, type=Path)
    parser.add_argument("--filter-bundle-dir", required=True, type=Path)
    parser.add_argument("--filter-script", default="scripts/train_downstream_setup_filter.py", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--filter-thresholds", default="selected", help="selected, or comma/space list. selected uses side-specific trained thresholds.")
    parser.add_argument("--micro-thresholds", default="0.20 0.25 0.30 0.35 0.40 0.45 0.50 0.52 0.55 0.57 0.58")
    parser.add_argument("--min-confirms-for-best", type=int, default=20)
    parser.add_argument("--max-day-dominance-for-best", type=float, default=0.40)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    for path in [args.training_rows_csv, args.decisions_csv, args.filter_script]:
        if not path.is_file():
            raise FileNotFoundError(path)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    filter_module = load_filter_module(args.filter_script)
    long_bundle = load_filter_bundle(args.filter_bundle_dir / "long_downstream_setup_filter.pkl")
    short_bundle = load_filter_bundle(args.filter_bundle_dir / "short_downstream_setup_filter.pkl")

    print(f"[POLICY_EVAL] loading training rows {args.training_rows_csv}", flush=True)
    arms = pd.read_csv(args.training_rows_csv).copy()
    arms["SetupSide"] = arms["SetupSide"].astype(str).str.lower()
    arms["filter_prob"] = np.nan
    arms.loc[:, "filter_prob"] = score_side(arms, "long", long_bundle, filter_module).combine_first(score_side(arms, "short", short_bundle, filter_module))
    side_selected_threshold = {
        "long": float(long_bundle["selected_threshold"]),
        "short": float(short_bundle["selected_threshold"]),
    }

    print(f"[POLICY_EVAL] loading decisions {args.decisions_csv}", flush=True)
    decisions = pd.read_csv(args.decisions_csv)
    decisions["side"] = decisions["side"].astype(str).str.lower()
    decisions = decisions.merge(
        arms[["arm_id", "SetupSide", "filter_prob"]],
        on="arm_id",
        how="left",
        validate="many_to_one",
    )
    decisions["side_filter_threshold_selected"] = decisions["side"].map(side_selected_threshold)

    micro_thresholds = parse_float_list(args.micro_thresholds)
    if args.filter_thresholds.strip().lower() == "selected":
        filter_thresholds = [math.nan]
    else:
        filter_thresholds = parse_float_list(args.filter_thresholds)

    policy_rows: list[dict[str, Any]] = []
    decision_frames = []
    for filter_threshold in filter_thresholds:
        if math.isnan(filter_threshold):
            decisions["filter_threshold"] = decisions["side_filter_threshold_selected"]
            filter_label = "selected"
        else:
            decisions["filter_threshold"] = filter_threshold
            filter_label = f"{filter_threshold:.4f}"
        decisions["filter_pass"] = pd.to_numeric(decisions["filter_prob"], errors="coerce") >= pd.to_numeric(decisions["filter_threshold"], errors="coerce")
        for micro_threshold in micro_thresholds:
            subset = decisions[np.isclose(pd.to_numeric(decisions["counterfactual_micro_threshold"], errors="coerce"), micro_threshold)].copy()
            if subset.empty:
                continue
            subset = subset[subset["filter_pass"]].copy()
            subset["policy_filter_threshold_label"] = filter_label
            subset["policy_micro_threshold"] = micro_threshold
            policy_rows.append(summarize_policy(subset, len(arms), filter_threshold=(float("nan") if filter_label == "selected" else float(filter_threshold)), micro_threshold=micro_threshold))
            policy_rows[-1]["filter_threshold_label"] = filter_label
            decision_frames.append(subset)

    best_candidates = [
        row for row in policy_rows
        if row["confirms"] >= args.min_confirms_for_best
        and row["max_day_dominance_frac"] <= args.max_day_dominance_for_best
        and row.get("expected_net_r_mean") is not None
    ]
    if best_candidates:
        best_policy = max(best_candidates, key=lambda row: (row["expected_net_r_mean"], row["expected_net_r_sum"] or -999.0, row["confirms"]))
    elif policy_rows:
        best_policy = max(policy_rows, key=lambda row: (row.get("expected_net_r_mean") if row.get("expected_net_r_mean") is not None else -999.0, row["confirms"]))
    else:
        best_policy = {}

    outputs = {
        "policy_grid_csv": args.output_dir / "downstream_filter_replay_policy_grid.csv",
        "policy_decisions_csv": args.output_dir / "downstream_filter_replay_policy_decisions.csv",
        "summary_json": args.output_dir / "downstream_filter_replay_policy_summary.json",
        "summary_md": args.output_dir / "downstream_filter_replay_policy_summary.md",
    }
    pd.DataFrame(policy_rows).to_csv(outputs["policy_grid_csv"], index=False)
    if decision_frames:
        pd.concat(decision_frames, ignore_index=True).to_csv(outputs["policy_decisions_csv"], index=False)
    else:
        pd.DataFrame().to_csv(outputs["policy_decisions_csv"], index=False)

    manifest = {
        "generated_at_utc": utc_now(),
        "schema_version": SCHEMA_VERSION,
        "inputs": {
            "training_rows_csv": str(args.training_rows_csv),
            "decisions_csv": str(args.decisions_csv),
            "filter_bundle_dir": str(args.filter_bundle_dir),
        },
        "outputs": {name: str(path) for name, path in outputs.items()},
        "side_filter_threshold_selected": side_selected_threshold,
        "micro_thresholds": micro_thresholds,
        "policy_grid": policy_rows,
        "best_policy": best_policy,
        "warnings": [
            "Offline counterfactual screen only; Java runtime is not wired and no live/replay parity is established.",
            "Filter models are trained on replay-observed arms, not all setup candidates; use for research prioritization only.",
        ],
    }
    outputs["summary_json"].write_text(json.dumps(json_safe(manifest), indent=2), encoding="utf-8")
    write_markdown(outputs["summary_md"], manifest)
    print(
        f"[POLICY_EVAL] done policies={len(policy_rows)} best_micro={best_policy.get('micro_threshold')} "
        f"best_confirms={best_policy.get('confirms')} summary={outputs['summary_json']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



