from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path("/Users/filmonghezehey/trading-agent/worktrees/databento/training_data/compare_runs_20260407_meta_ab")
VARIANTS = [
    "baseline_no_generated_meta",
    "with_timesfm_and_sequence_proxy",
]

PRIMARY_RE = re.compile(
    r"^(LONG ENTRY \(Dip Buyer\)|SHORT ENTRY \(Rip Seller\)|LONG EXIT \(Top Detector\)|SHORT EXIT \(Bottom Detector\)) \| (\d+)/(\d+) \| ([0-9.]+%) \| ([0-9.]+%) \| ([0-9.]+) \| (\d+) \| (.+)$"
)
REGIME_RE = re.compile(
    r"^(choppy|trend|volatile) \| (LONG ENTRY \(Dip Buyer\)|SHORT ENTRY \(Rip Seller\)|LONG EXIT \(Top Detector\)|SHORT EXIT \(Bottom Detector\)) \| (\d+)/(\d+) \| ([0-9.]+%) \| ([0-9.]+) \| (.+)$"
)
OPEN_RE = re.compile(
    r"^(LONG ENTRY \(Dip Buyer\)|SHORT ENTRY \(Rip Seller\)|LONG EXIT \(Top Detector\)|SHORT EXIT \(Bottom Detector\)) \| (\d+)/(\d+) \| ([0-9.]+%) \| ([0-9.]+) \| (.+)$"
)
REGIME_CLS_RE = re.compile(r"^MarketRegimeClassifier \| AvgAccuracy=([0-9.]+%) \| Export=(.+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate final comparison report from compare-run logs and manifests.")
    parser.add_argument("--compare-root", type=Path, default=ROOT)
    return parser.parse_args()


def parse_pct(text: str) -> float:
    return float(text.rstrip("%")) / 100.0


def summarize_manifest(manifest: pd.DataFrame) -> dict:
    out = {}
    for col in [
        "train_status",
        "timesfm_status",
        "sequence_status",
        "effective_use_meta_features",
        "ready",
    ]:
        if col in manifest.columns:
            out[col] = Counter(manifest[col].astype(str).fillna("<NA>")).copy()
    return {k: dict(v) for k, v in out.items()}


def parse_logs(log_dir: Path) -> dict:
    primary_rows = []
    regime_rows = []
    open_rows = []
    regime_classifier_rows = []

    for log_path in sorted(log_dir.glob("*.log")):
        section = "primary"
        for raw_line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if line == ">>> REGIME-SPECIFIC MODEL SCORECARD":
                section = "regime"
                continue
            if line == ">>> OPENING-30M MODEL SCORECARD":
                section = "open"
                continue
            if line == ">>> REGIME CLASSIFIER":
                section = "regime_classifier"
                continue

            m = PRIMARY_RE.match(line)
            if m and section == "primary":
                primary_rows.append(
                    {
                        "job_log": log_path.name,
                        "model": m.group(1),
                        "signals": int(m.group(2)),
                        "rows": int(m.group(3)),
                        "signal_rate": parse_pct(m.group(4)),
                        "avg_precision": parse_pct(m.group(5)),
                        "avg_threshold": float(m.group(6)),
                        "folds_used": int(m.group(7)),
                        "export": m.group(8),
                    }
                )
                continue

            m = REGIME_RE.match(line)
            if m:
                regime_rows.append(
                    {
                        "job_log": log_path.name,
                        "regime": m.group(1),
                        "model": m.group(2),
                        "signals": int(m.group(3)),
                        "rows": int(m.group(4)),
                        "avg_precision": parse_pct(m.group(5)),
                        "avg_threshold": float(m.group(6)),
                        "export": m.group(7),
                    }
                )
                continue

            m = OPEN_RE.match(line)
            if m and section == "open":
                open_rows.append(
                    {
                        "job_log": log_path.name,
                        "model": m.group(1),
                        "signals": int(m.group(2)),
                        "rows": int(m.group(3)),
                        "avg_precision": parse_pct(m.group(4)),
                        "avg_threshold": float(m.group(5)),
                        "export": m.group(6),
                    }
                )
                continue

            m = REGIME_CLS_RE.match(line)
            if m:
                regime_classifier_rows.append(
                    {
                        "job_log": log_path.name,
                        "avg_accuracy": parse_pct(m.group(1)),
                        "export": m.group(2),
                    }
                )

    return {
        "primary_score_rows": primary_rows,
        "regime_score_rows": regime_rows,
        "open_score_rows": open_rows,
        "regime_classifier_rows": regime_classifier_rows,
    }


def best_primary_metrics(rows: list[dict]) -> dict:
    by_model: dict[str, dict] = {}
    for row in rows:
        key = row["model"]
        current = by_model.get(key)
        if current is None or row["avg_precision"] > current["avg_precision"]:
            by_model[key] = row
    return by_model


def _job_name(row: dict) -> str:
    return Path(str(row.get("job_log", ""))).stem


def compare_score_rows(
    baseline_rows: list[dict],
    enhanced_rows: list[dict],
    extra_keys: tuple[str, ...] = (),
) -> list[dict]:
    def key_for(row: dict) -> tuple:
        return (_job_name(row), row.get("model", ""), *(row.get(k, "") for k in extra_keys))

    baseline_by_key = {key_for(row): row for row in baseline_rows}
    enhanced_by_key = {key_for(row): row for row in enhanced_rows}
    rows = []
    for key in sorted(set(baseline_by_key) | set(enhanced_by_key)):
        base = baseline_by_key.get(key, {})
        enh = enhanced_by_key.get(key, {})
        base_precision = base.get("avg_precision")
        enh_precision = enh.get("avg_precision")
        if enh_precision is not None and (base_precision is None or enh_precision >= base_precision):
            winner = "enhanced"
        elif base_precision is not None:
            winner = "baseline"
        else:
            winner = "missing"

        row = {
            "job_name": key[0],
            "model": key[1],
            "baseline_avg_precision": base_precision,
            "enhanced_avg_precision": enh_precision,
            "precision_delta": None if base_precision is None or enh_precision is None else enh_precision - base_precision,
            "baseline_avg_threshold": base.get("avg_threshold"),
            "enhanced_avg_threshold": enh.get("avg_threshold"),
            "baseline_signals": base.get("signals"),
            "enhanced_signals": enh.get("signals"),
            "baseline_rows": base.get("rows"),
            "enhanced_rows": enh.get("rows"),
            "baseline_export": base.get("export"),
            "enhanced_export": enh.get("export"),
            "winner": winner,
        }
        for idx, name in enumerate(extra_keys, start=2):
            row[name] = key[idx]
        rows.append(row)
    return rows


def main() -> None:
    args = parse_args()
    root = args.compare_root.expanduser().resolve()
    report: dict[str, dict] = {}
    for variant in VARIANTS:
        vdir = root / variant
        manifest = pd.read_csv(vdir / "training_manifest.csv")
        summary_path = vdir / "training_manifest_summary.json"
        summary_json = json.loads(summary_path.read_text()) if summary_path.exists() else None
        logs_dir = vdir / "logs"
        parsed_logs = parse_logs(logs_dir) if logs_dir.exists() else {
            "primary_score_rows": [],
            "regime_score_rows": [],
            "open_score_rows": [],
            "regime_classifier_rows": [],
        }
        export_files = [str(p.relative_to(vdir)) for p in sorted((vdir / "model_exports").glob("**/*.onnx"))]

        report[variant] = {
            "manifest_rows": int(len(manifest)),
            "summary_json": summary_json,
            "manifest_status_counts": summarize_manifest(manifest),
            "model_export_file_count": len(export_files),
            "model_export_files": export_files,
            **parsed_logs,
            "best_primary_metrics": best_primary_metrics(parsed_logs["primary_score_rows"]),
        }

    baseline = report[VARIANTS[0]]
    enhanced = report[VARIANTS[1]]
    comparison = {}
    for model_name, base_row in baseline["best_primary_metrics"].items():
        enhanced_row = enhanced["best_primary_metrics"].get(model_name)
        if not enhanced_row:
            continue
        comparison[model_name] = {
            "baseline_avg_precision": base_row["avg_precision"],
            "enhanced_avg_precision": enhanced_row["avg_precision"],
            "precision_delta": enhanced_row["avg_precision"] - base_row["avg_precision"],
            "baseline_avg_threshold": base_row["avg_threshold"],
            "enhanced_avg_threshold": enhanced_row["avg_threshold"],
            "baseline_signal_rate": base_row["signal_rate"],
            "enhanced_signal_rate": enhanced_row["signal_rate"],
        }

    primary_job_model_comparison = compare_score_rows(
        baseline["primary_score_rows"],
        enhanced["primary_score_rows"],
    )
    open_job_model_comparison = compare_score_rows(
        baseline["open_score_rows"],
        enhanced["open_score_rows"],
    )
    regime_job_model_comparison = compare_score_rows(
        baseline["regime_score_rows"],
        enhanced["regime_score_rows"],
        extra_keys=("regime",),
    )

    final_payload = {
        "root": str(root),
        "variants": report,
        "primary_model_comparison": comparison,
        "primary_job_model_comparison": primary_job_model_comparison,
        "open_job_model_comparison": open_job_model_comparison,
        "regime_job_model_comparison": regime_job_model_comparison,
    }
    out_path = root / "final_comparison_report.json"
    out_path.write_text(json.dumps(final_payload, indent=2), encoding="utf-8")
    for name, rows in [
        ("primary_job_model_comparison.csv", primary_job_model_comparison),
        ("open_job_model_comparison.csv", open_job_model_comparison),
        ("regime_job_model_comparison.csv", regime_job_model_comparison),
    ]:
        pd.DataFrame(rows).to_csv(root / name, index=False)
    print(f"WROTE {out_path}")
    print(f"WROTE {root / 'primary_job_model_comparison.csv'}")
    print(f"WROTE {root / 'open_job_model_comparison.csv'}")
    print(f"WROTE {root / 'regime_job_model_comparison.csv'}")
    for variant, payload in report.items():
        print(f"VARIANT {variant}")
        print(json.dumps(payload["manifest_status_counts"], indent=2))
        print(f"exports={payload['model_export_file_count']} primary_rows={len(payload['primary_score_rows'])} regime_rows={len(payload['regime_score_rows'])} open_rows={len(payload['open_score_rows'])}")
    print("PRIMARY_COMPARISON")
    print(json.dumps(comparison, indent=2))


if __name__ == "__main__":
    main()

