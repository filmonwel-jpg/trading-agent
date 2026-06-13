import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_databento_pilot_prebuild.py"
SPEC = importlib.util.spec_from_file_location("verify_databento_pilot_prebuild", SCRIPT_PATH)
verifier = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class VerifyDatabentoPilotPrebuildTest(unittest.TestCase):
    def make_lake(self, root: Path) -> tuple[Path, Path, Path, Path]:
        lake = root / "data_lake_v2"
        hash_dir = lake / "source_manifests" / "source_inventory_hashes_test"
        audit_dir = lake / "raw_audits" / "dbn_audit_summary_recent_old_test"
        pilot_dir = lake / "source_manifests" / "pilot_dates_latest10_test"
        files_dir = root / "raw"
        output_dir = lake / "raw_audits" / "prebuild_test"

        expected_sources = ["a", "b"]
        dates = ["20260101", "20260102"]
        selected_files = []
        for date in dates:
            for source in expected_sources:
                raw_path = files_dir / f"{source}-{date}.dbn.zst"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(f"{source}-{date}".encode("utf-8"))
                selected_files.append(
                    {
                        "date": date,
                        "source_label": source,
                        "dataset": "DATASET",
                        "schema": "schema",
                        "path": str(raw_path),
                        "name": raw_path.name,
                        "bytes": str(raw_path.stat().st_size),
                        "mib": "0.001",
                        "sha256_status": "ok",
                        "sha256": f"sha-{source}-{date}",
                        "sha256_error": "",
                    }
                )

        hash_dir.mkdir(parents=True)
        audit_dir.mkdir(parents=True)
        pilot_dir.mkdir(parents=True)
        (hash_dir / "manifest.json").write_text(json.dumps({"hash_error_count": 0}) + "\n", encoding="utf-8")
        (audit_dir / "manifest.json").write_text(json.dumps({"error_count": 0, "warning_count": 0}) + "\n", encoding="utf-8")
        (pilot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "selected_day_count": 2,
                    "selected_dates": dates,
                    "total_selected_files": 4,
                    "hash_error_count": 0,
                    "hash_missing_count": 0,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        write_csv(pilot_dir / "pilot_dates.csv", [{"order": i + 1, "date": date, "iso_date": f"2026-01-0{i + 1}"} for i, date in enumerate(dates)], ["order", "date", "iso_date"])
        write_csv(
            pilot_dir / "pilot_source_files.csv",
            selected_files,
            ["date", "source_label", "dataset", "schema", "path", "name", "bytes", "mib", "sha256_status", "sha256", "sha256_error"],
        )
        return lake, hash_dir, audit_dir, pilot_dir, output_dir

    def test_verify_passes_clean_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lake, hash_dir, audit_dir, pilot_dir, output_dir = self.make_lake(Path(temp_dir))

            rc = verifier.verify(
                lake_root=lake,
                hash_dir=hash_dir,
                audit_dir=audit_dir,
                pilot_dir=pilot_dir,
                output_dir=output_dir,
                expected_sources=["a", "b"],
                expected_days=2,
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((output_dir / "prebuild_manifest_check.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["selected_file_count"], 4)
            self.assertEqual(manifest["source_counts"], {"a": 2, "b": 2})

    def test_verify_fails_missing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lake, hash_dir, audit_dir, pilot_dir, output_dir = self.make_lake(Path(temp_dir))
            with (pilot_dir / "pilot_source_files.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            Path(rows[0]["path"]).unlink()

            rc = verifier.verify(
                lake_root=lake,
                hash_dir=hash_dir,
                audit_dir=audit_dir,
                pilot_dir=pilot_dir,
                output_dir=output_dir,
                expected_sources=["a", "b"],
                expected_days=2,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "prebuild_manifest_check.json").read_text(encoding="utf-8"))
            self.assertTrue(any("missing source file" in error for error in manifest["errors"]))


if __name__ == "__main__":
    unittest.main()

