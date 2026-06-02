from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


DEFAULT_VARIANT_DIR = Path(
    "/Users/filmonghezehey/trading-agent/worktrees/databento/training_data/compare_runs_20260407_meta_ab/with_timesfm_and_sequence_proxy"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile a compare-run variant manifest using completed stage/training logs."
    )
    parser.add_argument(
        "--variant-dir",
        default=str(DEFAULT_VARIANT_DIR),
        help="Variant work directory containing training_manifest.csv, logs/, and model_exports/.",
    )
    parser.add_argument(
        "--baseline-template-manifest",
        default="",
        help="Compatibility argument accepted by older finalize scripts; not needed for reconciliation.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Overwrite training_manifest.csv and training_manifest_summary.json with reconciled values.",
    )
    return parser.parse_args()


def _log_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _stage_status(log_path: Path, stage_name: str) -> str:
    if not log_path.exists():
        return "pending"
    text = _log_text(log_path)
    if not text.strip():
        return "pending"
    lowered = text.lower()
    if "traceback" in lowered or "exception" in lowered or "failed" in lowered:
        return "failed"
    output_markers = {
        "timesfm": [">>> wrote output csv:", "[timesfm] wrote "],
        "sequence": [">>> wrote sequence meta csv:", "[seq] wrote "],
    }
    if any(marker in lowered for marker in output_markers[stage_name]):
        return "ok"
    return "running"


def _train_status(log_path: Path) -> str:
    if not log_path.exists():
        return "pending"
    text = _log_text(log_path)
    if not text.strip():
        return "pending"
    lowered = text.lower()
    if ">>> pipeline complete." in lowered:
        return "ok"
    if "traceback" in lowered or "exception" in lowered or "failed" in lowered:
        return "failed"
    return "running"


def reconcile_manifest(variant_dir: Path) -> tuple[pd.DataFrame, dict]:
    manifest_path = variant_dir / "training_manifest.csv"
    logs_dir = variant_dir / "logs"
    frame = pd.read_csv(manifest_path)

    for idx, row in frame.iterrows():
        job_name = str(row["job_name"])
        timesfm_log = logs_dir / f"{job_name}_timesfm.log"
        sequence_log = logs_dir / f"{job_name}_sequence.log"
        train_log = logs_dir / f"{job_name}.log"

        current_timesfm = str(row.get("timesfm_status", "pending"))
        current_sequence = str(row.get("sequence_status", "pending"))

        if current_timesfm not in {"disabled", "preexisting"}:
            frame.at[idx, "timesfm_status"] = _stage_status(timesfm_log, "timesfm")

        if current_sequence not in {"disabled", "preexisting"}:
            frame.at[idx, "sequence_status"] = _stage_status(sequence_log, "sequence")

        frame.at[idx, "train_status"] = _train_status(train_log)

    summary = {
        "jobs_total": int(len(frame)),
        "jobs_ready": int(pd.to_numeric(frame.get("ready", pd.Series(dtype=int)), errors="coerce").fillna(0).astype(int).sum()),
        "by_cohort": frame.groupby("cohort")["job_name"].count().to_dict() if not frame.empty else {},
        "timesfm_backend": frame["timesfm_status"].astype(str).value_counts().to_dict() if "timesfm_status" in frame.columns else {},
        "sequence_backend": frame["sequence_status"].astype(str).value_counts().to_dict() if "sequence_status" in frame.columns else {},
        "train_status": frame["train_status"].astype(str).value_counts().to_dict() if "train_status" in frame.columns else {},
    }
    return frame, summary


def main() -> None:
    args = parse_args()
    variant_dir = Path(args.variant_dir).expanduser().resolve()
    frame, summary = reconcile_manifest(variant_dir)

    if args.write:
        manifest_path = variant_dir / "training_manifest.csv"
        summary_path = variant_dir / "training_manifest_summary.json"
        frame.to_csv(manifest_path, index=False)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"WROTE {manifest_path}")
        print(f"WROTE {summary_path}")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

