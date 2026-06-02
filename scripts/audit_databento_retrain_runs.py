#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return sum(1 for _ in handle)


def summarize_30s(tag: str) -> None:
    data_dir = ROOT / f"training_data/databento_30s_{tag}"
    combined = ROOT / f"training_data/databento_30s_{tag}_combined.csv"
    plan = data_dir / "symbol_model_plan.csv"
    print(f"\n=== DATASET {tag} 30s ===")
    print(f"csv_count={len(list(data_dir.glob('*_30s_training.csv')))}")
    print(f"combined_exists={combined.exists()} combined_lines={line_count(combined)}")
    if plan.exists():
        frame = pd.read_csv(plan)
        cohorts = frame.get("modeling_cohort", pd.Series(dtype=str)).value_counts(dropna=False).to_dict()
        print(f"plan_rows={len(frame)} cohorts={cohorts}")
    else:
        print("plan_rows=0 cohorts={}")

    rows: list[tuple[str, int, int, str, str]] = []
    all_dates: set[str] = set()
    for csv_path in sorted(data_dir.glob("*_30s_training.csv")):
        df = pd.read_csv(csv_path, usecols=lambda col: col in {"Date", "Timestamp", "Symbol"})
        if "Date" in df.columns:
            dates = sorted(set(df["Date"].astype(str)))
        elif "Timestamp" in df.columns:
            raw_dates = df["Timestamp"].astype(str).str.split().str[0]
            parsed_dates = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce")
            dates = sorted(set(parsed_dates.dt.date.dropna().astype(str)))
        else:
            dates = []
        all_dates.update(dates)
        rows.append((
            csv_path.name.replace("_30s_training.csv", ""),
            len(df),
            len(dates),
            dates[0] if dates else "",
            dates[-1] if dates else "",
        ))
    if rows:
        print(
            "symbols_with_rows={} min_rows={} max_rows={} unique_dates={} first_date={} last_date={}".format(
                len(rows),
                min(row[1] for row in rows),
                max(row[1] for row in rows),
                len(all_dates),
                min(all_dates) if all_dates else "",
                max(all_dates) if all_dates else "",
            )
        )
        print(f"first_10_symbol_rows={rows[:10]}")


def summarize_cadence(tag: str, cadence: str) -> None:
    data_dir = ROOT / f"training_data/databento_{cadence}_{tag}"
    combined = ROOT / f"training_data/databento_{cadence}_{tag}_combined.csv"
    print(f"\n=== DATASET {tag} {cadence} ===")
    print(f"csv_count={len(list(data_dir.glob(f'*_{cadence}_training.csv')))}")
    print(f"combined_exists={combined.exists()} combined_lines={line_count(combined)}")


def summarize_compare(tag: str) -> None:
    root = ROOT / f"training_data/compare_runs_{tag}_meta_ab"
    print(f"\n=== COMPARE {tag} ===")
    print(f"exists={root.exists()}")
    for variant in ["baseline_no_generated_meta", "with_timesfm_and_sequence_proxy"]:
        vdir = root / variant
        logs_dir = vdir / "logs"
        logs = sorted(logs_dir.glob("*.log")) if logs_dir.exists() else []
        complete = 0
        errors = 0
        for log in logs:
            text = log.read_text(encoding="utf-8", errors="replace").lower()
            complete += int(">>> pipeline complete." in text)
            errors += int(any(token in text for token in ["traceback", "exception", "pipeline_failed", "failed:1", "failed:2"]))
        print(f"variant={variant} logs={len(logs)} complete={complete} errors={errors}")
        manifest = vdir / "training_manifest.csv"
        summary = vdir / "training_manifest_summary.json"
        if manifest.exists():
            m = pd.read_csv(manifest)
            print(f"  manifest_rows={len(m)} train_status={m.get('train_status', pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict()}")
            for col in ["timesfm_status", "sequence_status", "effective_use_meta_features"]:
                if col in m.columns:
                    print(f"  {col}={m[col].astype(str).value_counts(dropna=False).to_dict()}")
            if "final_row_count" in m.columns:
                print(f"  final_rows_sum={int(pd.to_numeric(m['final_row_count'], errors='coerce').fillna(0).sum())}")
        print(f"  summary_exists={summary.exists()}")
    final_report = root / "final_comparison_report.json"
    print(f"final_report_exists={final_report.exists()}")


def main() -> int:
    for tag in ["20260419", "20260523"]:
        summarize_30s(tag)
    for cadence in ["5s", "1s"]:
        summarize_cadence("20260523", cadence)
    for tag in ["20260419", "20260523"]:
        summarize_compare(tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



