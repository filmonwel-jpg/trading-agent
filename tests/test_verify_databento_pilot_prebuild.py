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
    def test_default_expected_sources_include_definition_feeds(self):
        self.assertEqual(
            verifier.DEFAULT_EXPECTED_SOURCES,
            [
                "equs_tbbo_20260523",
                "opra_ohlcv1s_20260523",
                "equs_definition_20260612",
                "equs_mbp1_20260612",
                "opra_tcbbo_20260612",
                "opra_definition_20260612",
            ],
        )

    def test_empty_hash_dir_argument_fails_before_repo_manifest_lookup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "data_lake_v2"
            out = lake / "raw_audits" / "prebuild_test"
            with self.assertRaises(SystemExit) as raised:
                verifier.main([
                    "--lake-root",
                    str(lake),
                    "--hash-dir",
                    "",
                    "--output-dir",
                    str(out),
                ])
            self.assertEqual(raised.exception.code, 2)

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

    def test_verify_reports_missing_manifest_inputs_without_traceback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lake = root / "data_lake_v2"
            output_dir = lake / "raw_audits" / "prebuild_test"

            rc = verifier.verify(
                lake_root=lake,
                hash_dir=root / "missing_hash_dir",
                audit_dir=root / "missing_audit_dir",
                pilot_dir=root / "missing_pilot_dir",
                output_dir=output_dir,
                expected_sources=["a", "b"],
                expected_days=2,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "prebuild_manifest_check.json").read_text(encoding="utf-8"))
            self.assertTrue(any("missing required input hash manifest" in error for error in manifest["errors"]))
            self.assertTrue(any("HASH_DIR" in warning for warning in manifest["warnings"]))

    def test_verify_accepts_path_prefix_map(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            lake, hash_dir, audit_dir, pilot_dir, output_dir = self.make_lake(root)
            old_root = root / "old_raw_root"
            new_root = root / "new_raw_root"

            with (pilot_dir / "pilot_source_files.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            for row in rows:
                actual_path = Path(row["path"])
                remapped_path = new_root / actual_path.name
                remapped_path.parent.mkdir(parents=True, exist_ok=True)
                actual_path.rename(remapped_path)
                row["path"] = str(old_root / actual_path.name)

            write_csv(
                pilot_dir / "pilot_source_files.csv",
                rows,
                ["date", "source_label", "dataset", "schema", "path", "name", "bytes", "mib", "sha256_status", "sha256", "sha256_error"],
            )

            rc = verifier.verify(
                lake_root=lake,
                hash_dir=hash_dir,
                audit_dir=audit_dir,
                pilot_dir=pilot_dir,
                output_dir=output_dir,
                expected_sources=["a", "b"],
                expected_days=2,
                path_prefix_maps=[(old_root.resolve(), new_root.resolve())],
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((output_dir / "prebuild_manifest_check.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["path_prefix_maps"], [[str(old_root.resolve()), str(new_root.resolve())]])
            with (output_dir / "prebuild_manifest_check_files.csv").open(newline="", encoding="utf-8") as handle:
                checked_rows = list(csv.DictReader(handle))
            self.assertTrue(all(row["path_remapped"] == "True" for row in checked_rows))
            self.assertTrue(all(row["checked_path"].startswith(str(new_root.resolve())) for row in checked_rows))


if __name__ == "__main__":
    unittest.main()
