import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "audit_databento_dbn_day.py"
SPEC = importlib.util.spec_from_file_location("audit_databento_dbn_day", SCRIPT_PATH)
audit_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = audit_module
SPEC.loader.exec_module(audit_module)


class AuditDatabentoDbnDayTest(unittest.TestCase):
    def test_find_daily_file_and_normalize_date(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            expected = root / "equs-mini-20260521.mbp-1.dbn.zst"
            expected.write_bytes(b"stub")
            source = audit_module.SourceSpec("equs", "EQUS.MINI", "mbp-1", root)

            self.assertEqual(audit_module.normalize_date("2026-05-21"), "20260521")
            self.assertEqual(audit_module.find_daily_file(source, "20260521"), expected)

    def test_sources_from_inventory_skips_duplicate_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            inventory = root / "source_inventory.csv"
            with inventory.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["source_label", "dataset", "schema", "path", "exists"])
                writer.writeheader()
                writer.writerow({"source_label": "primary", "dataset": "OPRA.PILLAR", "schema": "definition", "path": str(root / "a"), "exists": "True"})
                writer.writerow({"source_label": "primary_duplicate", "dataset": "OPRA.PILLAR", "schema": "definition", "path": str(root / "b"), "exists": "True"})

            labels = [source.label for source in audit_module.sources_from_inventory(inventory, include_duplicate_definition=False)]
            self.assertEqual(labels, ["primary"])
            labels_with_duplicate = [source.label for source in audit_module.sources_from_inventory(inventory, include_duplicate_definition=True)]
            self.assertEqual(labels_with_duplicate, ["primary", "primary_duplicate"])

    def test_summarize_frame_reports_timestamps_symbols_and_profiles(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            dbn_path = Path(temp_dir) / "opra-pillar-20260521.tcbbo.dbn.zst"
            dbn_path.write_bytes(b"stub")
            frame = pd.DataFrame(
                {
                    "ts_event": pd.to_datetime(["2026-05-21T13:30:00Z", "2026-05-21T13:30:01Z"]),
                    "ts_recv": pd.to_datetime(["2026-05-21T13:30:00.100Z", "2026-05-21T13:30:01.200Z"]),
                    "symbol": ["TSLA250621C00100000", "TSLA250621P00100000"],
                    "instrument_id": [1, 2],
                    "price": [1.25, 1.40],
                }
            )
            source = audit_module.SourceSpec("opra", "OPRA.PILLAR", "tcbbo", Path(temp_dir))

            summary, profiles, counts = audit_module.summarize_frame(source, "20260521", dbn_path, frame, decode_seconds=0.25)

            self.assertEqual(summary.status, "ok")
            self.assertEqual(summary.row_count, 2)
            self.assertEqual(summary.column_count, 5)
            self.assertEqual(summary.symbol_count, 2)
            self.assertEqual(summary.instrument_id_count, 2)
            self.assertEqual(summary.ts_event_min_utc, "2026-05-21T13:30:00Z")
            self.assertEqual(summary.ts_recv_max_utc, "2026-05-21T13:30:01.200000Z")
            self.assertEqual({profile.column for profile in profiles}, set(frame.columns))
            self.assertEqual(len(counts), 2)
            self.assertEqual(counts[0].count_column, "symbol")


if __name__ == "__main__":
    unittest.main()
