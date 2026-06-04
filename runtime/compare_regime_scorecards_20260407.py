from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path("/Users/filmonghezehey/trading-agent/worktrees/databento/training_data/compare_runs_20260407_meta_ab")
BASE = ROOT / "baseline_no_generated_meta" / "logs"
ENH = ROOT / "with_timesfm_and_sequence_proxy" / "logs"

REGIME_RE = re.compile(
    r"^(choppy|trend|volatile) \| (LONG ENTRY \(Dip Buyer\)|SHORT ENTRY \(Rip Seller\)|LONG EXIT \(Top Detector\)|SHORT EXIT \(Bottom Detector\)) \| (\d+)/(\d+) \| ([0-9.]+)% \| ([0-9.]+) \| (.+)$"
)


def parse_variant(log_dir: Path, variant: str) -> pd.DataFrame:
    rows: list[dict] = []
    for log_path in sorted(log_dir.glob("*.log")):
        in_regime = False
        for raw in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if line == ">>> REGIME-SPECIFIC MODEL SCORECARD":
                in_regime = True
                continue
            if not in_regime:
                continue
            m = REGIME_RE.match(line)
            if not m:
                continue
            rows.append(
                {
                    "variant": variant,
                    "job_log": log_path.name,
                    "job_name": log_path.stem,
                    "regime": m.group(1),
                    "model": m.group(2),
                    "signals": int(m.group(3)),
                    "rows": int(m.group(4)),
                    "avg_precision": float(m.group(5)),
                    "avg_threshold": float(m.group(6)),
                }
            )
    return pd.DataFrame(rows)


def weighted_average(group: pd.DataFrame, value_col: str) -> float:
    weights = group["rows"].astype(float)
    values = group[value_col].astype(float)
    return float((values * weights).sum() / weights.sum())


def main() -> None:
    base = parse_variant(BASE, "baseline")
    enh = parse_variant(ENH, "enhanced")
    merged = base.merge(
        enh,
        on=["job_name", "job_log", "regime", "model"],
        suffixes=("_base", "_enh"),
    )
    merged["precision_delta"] = merged["avg_precision_enh"] - merged["avg_precision_base"]
    merged["threshold_delta"] = merged["avg_threshold_enh"] - merged["avg_threshold_base"]

    shared = merged[merged["job_name"].isin(["liquid", "mega_liquid"])].copy()
    shared_table = (
        shared[["job_name", "regime", "model", "avg_precision_base", "avg_precision_enh", "precision_delta"]]
        .sort_values(["job_name", "regime", "model"])
        .to_dict(orient="records")
    )

    aggregate_rows = []
    for (regime, model), group in merged.groupby(["regime", "model"]):
        aggregate_rows.append(
            {
                "regime": regime,
                "model": model,
                "baseline_weighted_precision": round(weighted_average(group, "avg_precision_base"), 3),
                "enhanced_weighted_precision": round(weighted_average(group, "avg_precision_enh"), 3),
                "weighted_precision_delta": round(weighted_average(group.assign(v=group["precision_delta"]), "precision_delta"), 3),
                "mean_job_delta": round(float(group["precision_delta"].mean()), 3),
                "enhanced_job_wins": int((group["precision_delta"] > 0).sum()),
                "enhanced_job_losses": int((group["precision_delta"] < 0).sum()),
            }
        )

    regime_rollup = []
    for regime, group in merged.groupby("regime"):
        regime_rollup.append(
            {
                "regime": regime,
                "baseline_weighted_precision": round(weighted_average(group, "avg_precision_base"), 3),
                "enhanced_weighted_precision": round(weighted_average(group, "avg_precision_enh"), 3),
                "weighted_precision_delta": round(weighted_average(group.assign(v=group["precision_delta"]), "precision_delta"), 3),
                "mean_job_delta": round(float(group["precision_delta"].mean()), 3),
            }
        )

    payload = {
        "matched_rows": int(len(merged)),
        "shared_cohort_regime_comparison": shared_table,
        "aggregate_regime_model_comparison": sorted(aggregate_rows, key=lambda x: (x["regime"], x["model"])),
        "aggregate_regime_rollup": sorted(regime_rollup, key=lambda x: x["regime"]),
    }
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

