#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import math
import os
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
STATUS_RANK = {"PASS": 0, "WARN": 1, "NO-GO": 2}
EXPECTED_ROUTE_MODELS = {
    "longExitLifecycleAi",
    "shortExitLifecycleAi",
    "longMicroEntryAi",
    "shortMicroEntryAi",
    "longMicroExitGuardAi",
    "shortMicroExitGuardAi",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def repo_path(raw: str | Path | None) -> Path | None:
    if raw is None or str(raw).strip() == "":
        return None
    path = Path(str(raw)).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def as_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    parsed = as_float(value)
    return default if parsed is None else int(parsed)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def gate(name: str, status: str, summary: str, *, metrics: dict[str, Any] | None = None,
         issues: list[str] | None = None, warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "summary": summary,
        "metrics": metrics or {},
        "issues": issues or [],
        "warnings": warnings or [],
    }


def overall_status(gates: list[dict[str, Any]]) -> str:
    if not gates:
        return "NO-GO"
    return max((str(g["status"]) for g in gates), key=lambda s: STATUS_RANK.get(s, 2))


def discover_latest_model_dir() -> Path | None:
    env_dir = repo_path(os.getenv("TRADING_LIFECYCLE_MODEL_DIR", ""))
    candidates: list[Path] = []
    if env_dir:
        candidates.append(env_dir)
    candidates.extend(sorted((REPO_ROOT / "runtime" / "research_runs").glob("*/model_exports"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True))
    candidates.extend(sorted((REPO_ROOT / "model_exports").glob("lifecycle_micro*"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True))
    for candidate in candidates:
        if (candidate / "lifecycle_micro_route_manifest.json").is_file():
            return candidate
    return None


def resolve_model_dir(raw: str | None) -> Path | None:
    explicit = repo_path(raw) if raw else None
    if explicit:
        return explicit
    return discover_latest_model_dir()


def object_map(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_from_json(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalize_method(method: Any) -> str:
    value = str(method or "").strip().lower()
    if value == "" or value == "identity" or "no_posthoc_calibrator" in value or "identity_raw_probability" in value:
        return "raw"
    if "platt" in value:
        return "sigmoid"
    return value


def selected_calibrators(calibrators_json: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in list_from_json(calibrators_json.get("models")):
        if isinstance(row, dict) and bool(row.get("selected")):
            model = str(row.get("model", "")).strip()
            if model:
                out[model] = row
    return out


def scorecard_by_model(scorecard_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {str(row.get("model", "")).strip(): row for row in scorecard_rows if str(row.get("model", "")).strip()}


def runtime_calibration_gate(model_dir: Path | None, allow_raw_only: bool) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {"model_dir": str(model_dir) if model_dir else ""}
    if model_dir is None:
        return gate("runtime_calibration_application", "NO-GO", "No lifecycle/micro model export directory found.", metrics=metrics, issues=["Missing --model-dir and no discoverable lifecycle_micro route manifest."])

    required = [
        "lifecycle_micro_route_manifest.json",
        "lifecycle_micro_scorecard.csv",
        "calibration_manifest.json",
        "feature_schema.json",
    ]
    missing = [name for name in required if not (model_dir / name).is_file()]
    if missing:
        issues.append("Missing required lifecycle/micro artifacts: " + ", ".join(missing))

    route_path = model_dir / "lifecycle_micro_route_manifest.json"
    scorecard_path = model_dir / "lifecycle_micro_scorecard.csv"
    calibration_manifest_path = model_dir / "calibration_manifest.json"
    calibrators_path = model_dir / "posthoc_calibrators.json"
    try:
        route_rows = read_json(route_path) if route_path.is_file() else []
    except Exception as exc:
        route_rows = []
        issues.append(f"Could not read route manifest: {exc}")
    try:
        score_rows = read_csv_rows(scorecard_path) if scorecard_path.is_file() else []
    except Exception as exc:
        score_rows = []
        issues.append(f"Could not read scorecard: {exc}")
    try:
        calibration_manifest = read_json(calibration_manifest_path) if calibration_manifest_path.is_file() else {}
    except Exception as exc:
        calibration_manifest = {}
        issues.append(f"Could not read calibration manifest: {exc}")
    try:
        calibrators_json = read_json(calibrators_path) if calibrators_path.is_file() else {"models": []}
    except Exception as exc:
        calibrators_json = {"models": []}
        issues.append(f"Could not read posthoc_calibrators.json: {exc}")

    score_by_model = scorecard_by_model(score_rows)
    selected_by_model = selected_calibrators(calibrators_json)
    route_models = {str(row.get("model", "")).strip() for row in route_rows if isinstance(row, dict)}
    metrics.update({
        "route_models": sorted(route_models),
        "scorecard_models": sorted(score_by_model),
        "selected_posthoc_calibrators": sorted(selected_by_model),
        "calibration_manifest_method": calibration_manifest.get("method", ""),
        "calibration_manifest_warnings": calibration_manifest.get("warnings", []),
    })

    missing_models = sorted(EXPECTED_ROUTE_MODELS - route_models)
    if missing_models:
        issues.append("Route manifest missing expected models: " + ", ".join(missing_models))

    posthoc_enabled_count = 0
    raw_only_models: list[str] = []
    runtime_thresholds: dict[str, float] = {}
    for row in route_rows:
        if not isinstance(row, dict):
            continue
        model = str(row.get("model", "")).strip()
        if not model:
            continue
        model_file = model_dir / str(row.get("model_path", row.get("filename", ""))).split("/")[-1]
        if not model_file.is_file():
            issues.append(f"Missing ONNX artifact for {model}: {model_file.name}")
        score = score_by_model.get(model, {})
        if not score:
            issues.append(f"Scorecard missing row for {model}")
        calibration = object_map(row.get("calibration"))
        posthoc = object_map(calibration.get("posthoc"))
        route_threshold = as_float(row.get("threshold"))
        score_posthoc_threshold = as_float(score.get("posthoc_threshold"))
        selected_metrics_threshold = as_float(object_map(posthoc.get("selected_metrics")).get("threshold"))
        runtime_threshold = selected_metrics_threshold if selected_metrics_threshold is not None else (score_posthoc_threshold if score_posthoc_threshold is not None else route_threshold)
        if runtime_threshold is not None:
            runtime_thresholds[model] = runtime_threshold
        if bool(posthoc.get("enabled")):
            posthoc_enabled_count += 1
            selected_method = normalize_method(posthoc.get("selected_method"))
            calibrator = selected_by_model.get(model)
            if calibrator is None:
                issues.append(f"{model}: route posthoc enabled but no selected calibrator artifact row exists")
            else:
                artifact_method = normalize_method(calibrator.get("method"))
                if artifact_method != selected_method:
                    issues.append(f"{model}: selected_method mismatch route={selected_method} artifact={artifact_method}")
                route_hash = str(row.get("feature_schema_sha256", "")).strip().lower()
                artifact_hash = str(calibrator.get("feature_schema_sha256", "")).strip().lower()
                if route_hash and artifact_hash and route_hash != artifact_hash:
                    issues.append(f"{model}: calibrator feature_schema_sha256 mismatch")
            if runtime_threshold is None:
                issues.append(f"{model}: posthoc enabled but no runtime threshold found")
        else:
            raw_only_models.append(model)

    metrics["posthoc_enabled_models"] = posthoc_enabled_count
    metrics["raw_only_models"] = raw_only_models
    metrics["runtime_thresholds"] = runtime_thresholds
    if posthoc_enabled_count == 0:
        msg = "No route models have post-hoc calibration enabled; runtime will apply raw probabilities only."
        if allow_raw_only:
            warnings.append(msg)
        else:
            issues.append(msg + " Pass --allow-raw-only-calibration only for an intentional raw-probability research gate.")
    elif raw_only_models:
        warnings.append("Some route models remain raw-only: " + ", ".join(raw_only_models))

    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    summary = "Runtime calibrator artifacts and manifest thresholds are promotion-ready." if status == "PASS" else "Runtime calibration evidence is incomplete."
    return gate("runtime_calibration_application", status, summary, metrics=metrics, issues=issues, warnings=warnings)


def label_economics_gate(model_dir: Path | None, args: argparse.Namespace) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {"model_dir": str(model_dir) if model_dir else ""}
    if model_dir is None:
        return gate("label_economics_review", "NO-GO", "No model directory available for label economics.", metrics=metrics, issues=["Missing lifecycle_micro_scorecard.csv."])
    scorecard = model_dir / "lifecycle_micro_scorecard.csv"
    if not scorecard.is_file():
        return gate("label_economics_review", "NO-GO", "Scorecard is missing.", metrics=metrics, issues=[f"Missing {scorecard}"])
    rows = read_csv_rows(scorecard)
    per_model: dict[str, Any] = {}
    for row in rows:
        model = str(row.get("model", "")).strip()
        if not model:
            continue
        model_metrics = {
            "rows": as_int(row.get("rows")),
            "positives": as_int(row.get("positives")),
            "label_pos_rate": as_float(row.get("label_pos_rate"), 0.0),
            "threshold": as_float(row.get("threshold"), 0.0),
            "precision": as_float(row.get("precision"), 0.0),
            "recall": as_float(row.get("recall"), 0.0),
            "pred_pos_rate": as_float(row.get("pred_pos_rate"), 0.0),
            "ece": as_float(row.get("ece"), 1.0),
            "calibration_rows": as_int(row.get("calibration_rows")),
            "posthoc_selected_method": row.get("posthoc_selected_method", ""),
            "posthoc_threshold": as_float(row.get("posthoc_threshold")),
        }
        per_model[model] = model_metrics
        if model_metrics["rows"] < args.min_label_rows:
            issues.append(f"{model}: rows {model_metrics['rows']} < minimum {args.min_label_rows}")
        if model_metrics["positives"] < args.min_label_positives:
            issues.append(f"{model}: positives {model_metrics['positives']} < minimum {args.min_label_positives}")
        if model_metrics["calibration_rows"] < args.min_calibration_rows:
            issues.append(f"{model}: calibration_rows {model_metrics['calibration_rows']} < minimum {args.min_calibration_rows}")
        if (model_metrics["precision"] or 0.0) < args.min_precision:
            issues.append(f"{model}: precision {model_metrics['precision']} < minimum {args.min_precision}")
        if (model_metrics["ece"] or 1.0) > args.max_ece:
            issues.append(f"{model}: ece {model_metrics['ece']} > maximum {args.max_ece}")
        if (model_metrics["pred_pos_rate"] or 0.0) < args.min_pred_pos_rate:
            issues.append(f"{model}: pred_pos_rate {model_metrics['pred_pos_rate']} < minimum {args.min_pred_pos_rate}")
        if not model_metrics["posthoc_selected_method"]:
            warnings.append(f"{model}: no posthoc selected method in scorecard")
    metrics["models"] = per_model
    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    summary = f"Reviewed {len(per_model)} model label/economics rows."
    return gate("label_economics_review", status, summary, metrics=metrics, issues=issues, warnings=warnings)


def expand_paths(values: list[str] | None, pattern: str | None = None) -> list[Path]:
    paths: list[Path] = []
    for raw in values or []:
        for token in str(raw).split(","):
            token = token.strip()
            if token:
                path = repo_path(token)
                if path:
                    paths.append(path)
    if pattern:
        glob_pattern = str(repo_path(pattern) if not Path(pattern).is_absolute() else Path(pattern))
        paths.extend(Path(p) for p in glob.glob(glob_pattern))
    seen: set[Path] = set()
    out: list[Path] = []
    for path in paths:
        resolved = path.resolve() if path.exists() else path
        if resolved not in seen:
            seen.add(resolved)
            out.append(path)
    return out


def event_symbol(payload: dict[str, Any]) -> str:
    event = str(payload.get("event", "")).lower()
    key = "underlying" if event == "option_bar" else "symbol"
    return str(payload.get(key, payload.get("symbol", payload.get("underlying", "")))).strip().upper()


def inspect_recorded_events(paths: list[Path]) -> tuple[dict[str, Any], list[str], list[str]]:
    issues: list[str] = []
    warnings: list[str] = []
    event_counts: Counter[str] = Counter()
    symbols: Counter[str] = Counter()
    schema_versions: Counter[str] = Counter()
    non_monotonic = 0
    malformed = 0
    total_lines = 0
    last_epoch_by_symbol_event: dict[tuple[str, str], int] = {}
    for path in paths:
        if not path.is_file():
            issues.append(f"Recorded event file not found: {path}")
            continue
        opener = __import__("gzip").open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                total_lines += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(payload, dict):
                    malformed += 1
                    continue
                event = str(payload.get("event", "unknown")).strip().lower() or "unknown"
                event_counts[event] += 1
                symbol = event_symbol(payload)
                if symbol:
                    symbols[symbol] += 1
                schema = str(payload.get("EventSchemaVersion", payload.get("event_schema_version", ""))).strip()
                schema_versions[schema or "<missing>"] += 1
                epoch = as_int(payload.get("barEpochSec"), -1)
                key = (symbol, event)
                if epoch >= 0:
                    previous = last_epoch_by_symbol_event.get(key)
                    if previous is not None and epoch < previous:
                        non_monotonic += 1
                    last_epoch_by_symbol_event[key] = epoch
    metrics = {
        "files": [str(path) for path in paths],
        "input_lines": total_lines,
        "event_counts": dict(event_counts),
        "symbols": dict(symbols),
        "schema_versions": dict(schema_versions),
        "malformed_lines": malformed,
        "non_monotonic_events": non_monotonic,
    }
    if malformed:
        issues.append(f"Recorded events contain {malformed} malformed JSON lines")
    if schema_versions.get("<missing>", 0) > 0:
        issues.append(f"Recorded events missing EventSchemaVersion on {schema_versions['<missing>']} lines")
    if non_monotonic:
        warnings.append(f"Recorded events contain {non_monotonic} non-monotonic per-symbol/event timestamps")
    return metrics, issues, warnings


def boolish(value: Any) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "pass", "fired", "confirmed", "buy", "sell"}:
        return True
    if text in {"0", "false", "no", "n", "fail", "none", "", "hold"}:
        return False
    return None


def first_present(row: dict[str, Any], candidates: list[str]) -> Any:
    lower = {k.lower(): k for k in row.keys()}
    for candidate in candidates:
        actual = lower.get(candidate.lower())
        if actual is not None and row.get(actual) not in (None, ""):
            return row.get(actual)
    return None


def compare_decision_csv(left_path: Path, right_path: Path, args: argparse.Namespace) -> tuple[dict[str, Any], list[str], list[str]]:
    issues: list[str] = []
    warnings: list[str] = []
    left_rows = read_csv_rows(left_path)
    right_rows = read_csv_rows(right_path)
    if not left_rows or not right_rows:
        return {"left_rows": len(left_rows), "right_rows": len(right_rows), "paired_rows": 0}, ["Decision comparison requires non-empty CSVs"], warnings
    left_cols = set(left_rows[0].keys())
    right_cols = set(right_rows[0].keys())
    candidate_keys = ["symbol", "model", "side", "epoch", "barEpochSec", "timestamp", "marketTime", "action", "event"]
    key_cols = [col for col in candidate_keys if col in left_cols and col in right_cols]
    use_index_pairing = not key_cols
    if use_index_pairing:
        warnings.append("Decision CSVs have no shared key columns; comparing by row index")
        pairs = list(zip(left_rows, right_rows))
    else:
        def key(row: dict[str, str]) -> tuple[str, ...]:
            return tuple(str(row.get(col, "")).strip() for col in key_cols)
        right_by_key = {key(row): row for row in right_rows}
        pairs = [(row, right_by_key[key(row)]) for row in left_rows if key(row) in right_by_key]
    prob_candidates = ["calibrated_probability", "probability", "prob", "raw_probability", "EntryProb", "entry_prob"]
    decision_candidates = ["decision", "fired", "pass", "shouldExit", "confirmed", "action"]
    prob_drifts: list[float] = []
    mismatches = 0
    comparable_decisions = 0
    for left, right in pairs:
        left_prob = as_float(first_present(left, prob_candidates))
        right_prob = as_float(first_present(right, prob_candidates))
        if left_prob is not None and right_prob is not None:
            prob_drifts.append(abs(left_prob - right_prob))
        left_decision = boolish(first_present(left, decision_candidates))
        right_decision = boolish(first_present(right, decision_candidates))
        if left_decision is not None and right_decision is not None:
            comparable_decisions += 1
            if left_decision != right_decision:
                mismatches += 1
    paired = len(pairs)
    max_drift = max(prob_drifts) if prob_drifts else 0.0
    mean_drift = statistics.fmean(prob_drifts) if prob_drifts else 0.0
    mismatch_rate = (mismatches / comparable_decisions) if comparable_decisions else 0.0
    if paired == 0:
        issues.append("No paired decision rows for comparison")
    if max_drift > args.max_probability_drift:
        issues.append(f"max_probability_drift {max_drift:.6f} > maximum {args.max_probability_drift:.6f}")
    if mismatch_rate > args.max_decision_mismatch_rate:
        issues.append(f"decision_mismatch_rate {mismatch_rate:.6f} > maximum {args.max_decision_mismatch_rate:.6f}")
    metrics = {
        "left_file": str(left_path),
        "right_file": str(right_path),
        "left_rows": len(left_rows),
        "right_rows": len(right_rows),
        "paired_rows": paired,
        "key_columns": key_cols if key_cols else ["<row_index>"],
        "probability_pairs": len(prob_drifts),
        "max_probability_drift": max_drift,
        "mean_probability_drift": mean_drift,
        "decision_pairs": comparable_decisions,
        "decision_mismatches": mismatches,
        "decision_mismatch_rate": mismatch_rate,
    }
    return metrics, issues, warnings


def replay_parity_gate(args: argparse.Namespace) -> dict[str, Any]:
    event_paths = expand_paths(args.recorded_events, args.recorded_events_glob)
    issues: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {}
    if not event_paths:
        issues.append("No recorded event files supplied. Use --recorded-events or --recorded-events-glob.")
    else:
        event_metrics, event_issues, event_warnings = inspect_recorded_events(event_paths)
        metrics["recorded_events"] = event_metrics
        issues.extend(event_issues)
        warnings.extend(event_warnings)
        if event_metrics.get("input_lines", 0) < args.min_recorded_events:
            issues.append(f"recorded input lines {event_metrics.get('input_lines', 0)} < minimum {args.min_recorded_events}")
    if args.live_decisions and args.replay_decisions:
        compare_metrics, compare_issues, compare_warnings = compare_decision_csv(repo_path(args.live_decisions), repo_path(args.replay_decisions), args)  # type: ignore[arg-type]
        metrics["decision_parity"] = compare_metrics
        issues.extend(compare_issues)
        warnings.extend(compare_warnings)
    else:
        issues.append("No live/replay decision CSV pair supplied for parity comparison (--live-decisions and --replay-decisions).")
    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    return gate("recorded_event_replay_parity", status, "Recorded event contract and decision parity evidence checked.", metrics=metrics, issues=issues, warnings=warnings)


def parse_trade_day(row: dict[str, str]) -> str:
    for key in ("EntryTime", "ExitTime", "RunCompletedAt"):
        value = str(row.get(key, "")).strip()
        match = re.search(r"\d{4}-\d{2}-\d{2}", value)
        if match:
            return match.group(0)
    return "unknown"


def discover_backtest_summaries(args: argparse.Namespace) -> list[Path]:
    explicit = expand_paths(args.backtest_summaries)
    directory = repo_path(args.backtest_dir)
    if directory and directory.is_dir():
        explicit.extend(sorted(directory.glob(args.backtest_summary_glob)))
    seen: set[Path] = set()
    out: list[Path] = []
    for path in explicit:
        resolved = path.resolve() if path.exists() else path
        if resolved not in seen:
            seen.add(resolved)
            out.append(path)
    return out


def backtest_gate(args: argparse.Namespace) -> dict[str, Any]:
    files = discover_backtest_summaries(args)
    issues: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {"files": [str(path) for path in files]}
    if not files:
        return gate("full_backtest_pnl_day_dominance", "NO-GO", "No backtest lifecycle summaries found.", metrics=metrics, issues=["Run the Databento IBKR-sim backtest and pass --backtest-dir or --backtest-summary."])
    trades: list[dict[str, str]] = []
    for path in files:
        if not path.is_file():
            issues.append(f"Backtest summary file not found: {path}")
            continue
        for row in read_csv_rows(path):
            if str(row.get("ClosedTradeIndex", "")).strip():
                trades.append(row)
    total_trades = len(trades)
    symbols = {str(row.get("Symbol", "")).strip().upper() for row in trades if str(row.get("Symbol", "")).strip()}
    total_pnl = sum(as_float(row.get("TradePnL"), 0.0) or 0.0 for row in trades)
    pnl_by_day: dict[str, float] = defaultdict(float)
    trades_by_day: Counter[str] = Counter()
    for row in trades:
        day = parse_trade_day(row)
        pnl_by_day[day] += as_float(row.get("TradePnL"), 0.0) or 0.0
        trades_by_day[day] += 1
    total_abs_day_pnl = sum(abs(v) for v in pnl_by_day.values())
    max_abs_day_pnl_fraction = (max((abs(v) for v in pnl_by_day.values()), default=0.0) / total_abs_day_pnl) if total_abs_day_pnl > 0 else 0.0
    max_trade_day_fraction = (max(trades_by_day.values(), default=0) / total_trades) if total_trades else 0.0
    metrics.update({
        "closed_trades": total_trades,
        "symbols": sorted(symbols),
        "symbol_count": len(symbols),
        "total_pnl": total_pnl,
        "pnl_by_day": dict(sorted(pnl_by_day.items())),
        "trades_by_day": dict(sorted(trades_by_day.items())),
        "max_abs_day_pnl_fraction": max_abs_day_pnl_fraction,
        "max_trade_day_fraction": max_trade_day_fraction,
    })
    if total_trades < args.min_backtest_trades:
        issues.append(f"closed_trades {total_trades} < minimum {args.min_backtest_trades}")
    if len(symbols) < args.min_backtest_symbols:
        issues.append(f"symbol_count {len(symbols)} < minimum {args.min_backtest_symbols}")
    if total_pnl < args.min_total_pnl:
        issues.append(f"total_pnl {total_pnl:.2f} < minimum {args.min_total_pnl:.2f}")
    if max_abs_day_pnl_fraction > args.max_backtest_day_pnl_dominance:
        issues.append(f"max_abs_day_pnl_fraction {max_abs_day_pnl_fraction:.4f} > maximum {args.max_backtest_day_pnl_dominance:.4f}")
    if max_trade_day_fraction > args.max_backtest_day_trade_dominance:
        issues.append(f"max_trade_day_fraction {max_trade_day_fraction:.4f} > maximum {args.max_backtest_day_trade_dominance:.4f}")
    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    return gate("full_backtest_pnl_day_dominance", status, "Full backtest PnL and day-dominance evidence checked.", metrics=metrics, issues=issues, warnings=warnings)


def drift_report_gate(args: argparse.Namespace) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []
    metrics: dict[str, Any] = {}
    if args.drift_report:
        path = repo_path(args.drift_report)
        if path is None or not path.is_file():
            issues.append(f"Drift report not found: {path}")
        elif path.suffix.lower() == ".json":
            data = read_json(path)
            metrics = data if isinstance(data, dict) else {"report": data}
        else:
            rows = read_csv_rows(path)
            max_probability_drift = max((as_float(first_present(row, ["max_probability_drift", "probability_drift", "abs_probability_drift"]), 0.0) or 0.0 for row in rows), default=0.0)
            decision_mismatch_rate = max((as_float(first_present(row, ["decision_mismatch_rate", "mismatch_rate"]), 0.0) or 0.0 for row in rows), default=0.0)
            metrics = {"report_file": str(path), "rows": len(rows), "max_probability_drift": max_probability_drift, "decision_mismatch_rate": decision_mismatch_rate}
    elif args.paper_decisions and args.shadow_decisions:
        compare_metrics, compare_issues, compare_warnings = compare_decision_csv(repo_path(args.paper_decisions), repo_path(args.shadow_decisions), args)  # type: ignore[arg-type]
        metrics = compare_metrics
        issues.extend(compare_issues)
        warnings.extend(compare_warnings)
    else:
        issues.append("No paper/shadow drift evidence supplied. Use --drift-report or --paper-decisions plus --shadow-decisions.")
    max_probability_drift = as_float(metrics.get("max_probability_drift"), 0.0) or 0.0
    decision_mismatch_rate = as_float(metrics.get("decision_mismatch_rate"), 0.0) or 0.0
    if max_probability_drift > args.max_probability_drift:
        issues.append(f"max_probability_drift {max_probability_drift:.6f} > maximum {args.max_probability_drift:.6f}")
    if decision_mismatch_rate > args.max_decision_mismatch_rate:
        issues.append(f"decision_mismatch_rate {decision_mismatch_rate:.6f} > maximum {args.max_decision_mismatch_rate:.6f}")
    status = "NO-GO" if issues else ("WARN" if warnings else "PASS")
    return gate("paper_shadow_drift", status, "Paper/shadow probability and decision drift evidence checked.", metrics=metrics, issues=issues, warnings=warnings)


def write_reports(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "lifecycle_micro_promotion_gate_report.json"
    md_path = output_dir / "lifecycle_micro_promotion_gate_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Lifecycle/Micro Promotion Gate Report",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        f"Overall status: **{report['overall_status']}**",
        f"Model dir: `{report.get('model_dir', '')}`",
        "",
        "| Gate | Status | Summary |",
        "|---|---:|---|",
    ]
    for item in report["gates"]:
        lines.append(f"| `{item['name']}` | **{item['status']}** | {item['summary']} |")
    for item in report["gates"]:
        lines.extend(["", f"## `{item['name']}`", "", f"Status: **{item['status']}**", ""])
        if item.get("issues"):
            lines.append("Issues:")
            for issue in item["issues"]:
                lines.append(f"- {issue}")
            lines.append("")
        if item.get("warnings"):
            lines.append("Warnings:")
            for warning in item["warnings"]:
                lines.append(f"- {warning}")
            lines.append("")
        lines.append("Key metrics:")
        lines.append("```json")
        lines.append(json.dumps(item.get("metrics", {}), indent=2, sort_keys=True)[:8000])
        lines.append("```")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate lifecycle/micro runtime calibration, replay parity, backtest, drift, and label-economics promotion gates.")
    parser.add_argument("--model-dir", default="", help="Lifecycle/micro model_exports directory. Defaults to TRADING_LIFECYCLE_MODEL_DIR or newest discoverable lifecycle_micro export.")
    parser.add_argument("--allow-raw-only-calibration", action="store_true", help="Allow raw/no-posthoc model bundles to warn instead of fail the runtime calibration gate.")
    parser.add_argument("--recorded-events", action="append", default=[], help="Recorded normalized NDJSON/NDJSON.GZ file(s), comma-separated or repeated.")
    parser.add_argument("--recorded-events-glob", default="", help="Glob for recorded event files.")
    parser.add_argument("--live-decisions", default="", help="CSV of live/paper decisions for replay parity comparison.")
    parser.add_argument("--replay-decisions", default="", help="CSV of recorded-event replay decisions for parity comparison.")
    parser.add_argument("--backtest-dir", default="runtime/backtests", help="Directory containing *trade-lifecycle-summary.csv files.")
    parser.add_argument("--backtest-summary", dest="backtest_summaries", action="append", default=[], help="Explicit trade lifecycle summary CSV. Can be repeated/comma-separated.")
    parser.add_argument("--backtest-summary-glob", default="*trade-lifecycle-summary.csv")
    parser.add_argument("--paper-decisions", default="", help="Paper decision CSV for drift comparison.")
    parser.add_argument("--shadow-decisions", default="", help="Shadow decision CSV for drift comparison.")
    parser.add_argument("--drift-report", default="", help="Precomputed paper/shadow drift report JSON or CSV.")
    parser.add_argument("--output-dir", default="runtime/reports/lifecycle_micro_promotion_gate", help="Report output directory.")
    parser.add_argument("--min-recorded-events", type=int, default=100)
    parser.add_argument("--min-backtest-trades", type=int, default=20)
    parser.add_argument("--min-backtest-symbols", type=int, default=5)
    parser.add_argument("--min-total-pnl", type=float, default=0.0)
    parser.add_argument("--max-backtest-day-pnl-dominance", type=float, default=0.50)
    parser.add_argument("--max-backtest-day-trade-dominance", type=float, default=0.35)
    parser.add_argument("--max-probability-drift", type=float, default=0.02)
    parser.add_argument("--max-decision-mismatch-rate", type=float, default=0.01)
    parser.add_argument("--min-label-rows", type=int, default=1000)
    parser.add_argument("--min-label-positives", type=int, default=100)
    parser.add_argument("--min-calibration-rows", type=int, default=1000)
    parser.add_argument("--min-precision", type=float, default=0.50)
    parser.add_argument("--max-ece", type=float, default=0.08)
    parser.add_argument("--min-pred-pos-rate", type=float, default=0.001)
    parser.add_argument("--fail-on-no-go", action="store_true", help="Exit non-zero if overall status is NO-GO.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    model_dir = resolve_model_dir(args.model_dir)
    gates = [
        runtime_calibration_gate(model_dir, args.allow_raw_only_calibration),
        label_economics_gate(model_dir, args),
        replay_parity_gate(args),
        backtest_gate(args),
        drift_report_gate(args),
    ]
    report = {
        "schema_version": "lifecycle_micro_promotion_gate_v1",
        "generated_at_utc": utc_now(),
        "model_dir": str(model_dir) if model_dir else "",
        "overall_status": overall_status(gates),
        "gates": gates,
    }
    output_dir = repo_path(args.output_dir) or (REPO_ROOT / "runtime" / "reports" / "lifecycle_micro_promotion_gate")
    json_path, md_path = write_reports(report, output_dir)
    print(f"PROMOTION_GATE status={report['overall_status']} json={json_path} markdown={md_path}")
    for item in gates:
        print(f"GATE {item['name']} status={item['status']} issues={len(item['issues'])} warnings={len(item['warnings'])}")
    if args.fail_on_no_go and report["overall_status"] == "NO-GO":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
