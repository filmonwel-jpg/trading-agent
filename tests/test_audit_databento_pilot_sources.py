import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "audit_databento_pilot_sources.py"
SPEC = importlib.util.spec_from_file_location("audit_databento_pilot_sources", SCRIPT_PATH)
audit_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = audit_module
SPEC.loader.exec_module(audit_module)


class AuditDatabentoPilotSourcesTest(unittest.TestCase):
    def test_writes_inventory_and_unpaired_dates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            equs = root / "EQUS"
            opra = root / "OPRA"
            out = root / "out"
            equs.mkdir()
            opra.mkdir()

            (equs / "equs-mini-20260102.mbp-1.dbn.zst").write_bytes(b"eq-1")
            (equs / "equs-mini-20260103.mbp-1.dbn.zst").write_bytes(b"eq-2")
            (equs / "manifest.json").write_text("{}\n", encoding="utf-8")
            (opra / "opra-pillar-20260102.tcbbo.dbn.zst").write_bytes(b"op-1")

            rc = audit_module.run_audit(
                sources=[
                    audit_module.SourceSpec("equs", "EQUS.MINI", "mbp-1", equs),
                    audit_module.SourceSpec("opra", "OPRA.PILLAR", "tcbbo", opra),
                ],
                output_dir=out,
                include_hashes=False,
                allow_missing=False,
            )

            self.assertEqual(rc, 0)
            self.assertTrue((out / "source_inventory.csv").exists())
            self.assertTrue((out / "source_files.csv").exists())
            self.assertTrue((out / "paired_dates.csv").exists())
            self.assertTrue((out / "manifest.json").exists())

            with (out / "source_inventory.csv").open(newline="", encoding="utf-8") as handle:
                inventory = {row["source_label"]: row for row in csv.DictReader(handle)}
            self.assertEqual(inventory["equs"]["dbn_files"], "2")
            self.assertEqual(inventory["equs"]["unique_dates"], "2")
            self.assertEqual(inventory["opra"]["dbn_files"], "1")

            with (out / "paired_dates.csv").open(newline="", encoding="utf-8") as handle:
                paired_rows = {row["date"]: row for row in csv.DictReader(handle)}
            self.assertEqual(paired_rows["20260102"]["all_sources_present"], "1")
            self.assertEqual(paired_rows["20260103"]["all_sources_present"], "0")
            self.assertEqual(paired_rows["20260103"]["equs"], "1")
            self.assertEqual(paired_rows["20260103"]["opra"], "0")

            manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["paired_date_count"], 2)
            self.assertEqual(manifest["fully_paired_date_count"], 1)
            self.assertEqual(len(manifest["unpaired_dates"]), 1)


if __name__ == "__main__":
    unittest.main()
