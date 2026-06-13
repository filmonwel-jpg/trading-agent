import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "plan_databento_pilot_dates.py"
SPEC = importlib.util.spec_from_file_location("plan_databento_pilot_dates", SCRIPT_PATH)
planner = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = planner
SPEC.loader.exec_module(planner)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class PlanDatabentoPilotDatesTest(unittest.TestCase):
    def test_selects_latest_fully_paired_dates_and_writes_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paired = root / "paired_dates.csv"
            source_files = root / "source_files.csv"
            audit_summary = root / "dbn_audit_summary.csv"
            out = root / "out"

            write_csv(
                paired,
                [
                    {"date": "20260101", "a": "1", "b": "1", "present_source_count": "2", "all_sources_present": "1"},
                    {"date": "20260102", "a": "1", "b": "0", "present_source_count": "1", "all_sources_present": "0"},
                    {"date": "20260103", "a": "1", "b": "1", "present_source_count": "2", "all_sources_present": "1"},
                    {"date": "20260104", "a": "1", "b": "1", "present_source_count": "2", "all_sources_present": "1"},
                ],
                ["date", "a", "b", "present_source_count", "all_sources_present"],
            )

            file_rows = []
            for date in ["20260101", "20260103", "20260104"]:
                for label, size in [("a", 2 * 1024 * 1024), ("b", 3 * 1024 * 1024)]:
                    file_rows.append(
                        {
                            "source_label": label,
                            "dataset": "DATASET",
                            "schema": "schema",
                            "path": f"/tmp/{label}-{date}.dbn.zst",
                            "name": f"{label}-{date}.dbn.zst",
                            "bytes": str(size),
                            "mtime_utc": "2026-01-01T00:00:00Z",
                            "date": date,
                            "is_dbn": "True",
                            "sha256": f"hash-{label}-{date}",
                            "sha256_status": "ok",
                            "sha256_error": "",
                        }
                    )
            write_csv(
                source_files,
                file_rows,
                [
                    "source_label",
                    "dataset",
                    "schema",
                    "path",
                    "name",
                    "bytes",
                    "mtime_utc",
                    "date",
                    "is_dbn",
                    "sha256",
                    "sha256_status",
                    "sha256_error",
                ],
            )
            write_csv(
                audit_summary,
                [
                    {"source_label": "a", "status": "ok", "file_mib": "2.0", "dataframe_mib": "10.0", "memory_expansion_ratio": "5.0"},
                    {"source_label": "b", "status": "ok", "file_mib": "3.0", "dataframe_mib": "6.0", "memory_expansion_ratio": "2.0"},
                ],
                ["source_label", "status", "file_mib", "dataframe_mib", "memory_expansion_ratio"],
            )

            rc = planner.plan(
                paired_dates_path=paired,
                source_files_path=source_files,
                output_dir=out,
                days=2,
                strategy="latest",
                start_date=None,
                end_date=None,
                exclude_dates=set(),
                required_source_labels=None,
                dbn_audit_summary_path=audit_summary,
                allow_fewer=False,
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["selected_dates"], ["20260103", "20260104"])
            self.assertEqual(manifest["total_selected_files"], 4)
            self.assertEqual(manifest["hash_error_count"], 0)
            self.assertEqual(manifest["hash_missing_count"], 0)
            estimates = {row["source_label"]: row for row in manifest["size_estimates_by_source"]}
            self.assertEqual(estimates["a"]["estimated_peak_dataframe_mib"], 10.0)
            self.assertEqual(estimates["b"]["estimated_peak_dataframe_mib"], 6.0)

            with (out / "pilot_dates.csv").open(newline="", encoding="utf-8") as handle:
                dates = [row["date"] for row in csv.DictReader(handle)]
            self.assertEqual(dates, ["20260103", "20260104"])

    def test_fails_when_selected_source_file_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paired = root / "paired_dates.csv"
            source_files = root / "source_files.csv"
            out = root / "out"
            write_csv(
                paired,
                [{"date": "20260101", "a": "1", "present_source_count": "1", "all_sources_present": "1"}],
                ["date", "a", "present_source_count", "all_sources_present"],
            )
            write_csv(
                source_files,
                [],
                ["source_label", "dataset", "schema", "path", "name", "bytes", "date", "is_dbn", "sha256", "sha256_status", "sha256_error"],
            )

            with self.assertRaises(RuntimeError):
                planner.plan(
                    paired_dates_path=paired,
                    source_files_path=source_files,
                    output_dir=out,
                    days=1,
                    strategy="latest",
                    start_date=None,
                    end_date=None,
                    exclude_dates=set(),
                    required_source_labels=None,
                    dbn_audit_summary_path=None,
                    allow_fewer=False,
                )


if __name__ == "__main__":
    unittest.main()
