import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "summarize_databento_dbn_audits.py"
SPEC = importlib.util.spec_from_file_location("summarize_databento_dbn_audits", SCRIPT_PATH)
summary_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = summary_module
SPEC.loader.exec_module(summary_module)


FIELDNAMES = [
    "source_label",
    "dataset",
    "schema",
    "date",
    "path",
    "file_bytes",
    "status",
    "row_count",
    "column_count",
    "dataframe_memory_bytes",
    "decode_seconds",
    "rss_peak_bytes",
    "ts_event_min_utc",
    "ts_event_max_utc",
    "ts_recv_min_utc",
    "ts_recv_max_utc",
    "symbol_count",
    "instrument_id_count",
    "columns_json",
    "error",
]


class SummarizeDatabentoDbnAuditsTest(unittest.TestCase):
    def write_audit(self, audit_dir: Path, rows: list[dict]) -> None:
        audit_dir.mkdir(parents=True)
        with (audit_dir / "dbn_day_summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def test_summarize_writes_combined_outputs_and_warnings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            audit_dir = root / "raw_audits" / "dbn_day_audit_20260521"
            out = root / "summary"
            self.write_audit(
                audit_dir,
                [
                    {
                        "source_label": "equs_mbp1_20260612",
                        "dataset": "EQUS.MINI",
                        "schema": "mbp-1",
                        "date": "20260521",
                        "path": "/tmp/equs.dbn.zst",
                        "file_bytes": str(1024 * 1024),
                        "status": "ok",
                        "row_count": "1000",
                        "column_count": "20",
                        "dataframe_memory_bytes": str(40 * 1024 * 1024),
                        "decode_seconds": "2.0",
                        "rss_peak_bytes": "100",
                        "ts_event_min_utc": "2026-05-21T13:30:00Z",
                        "ts_event_max_utc": "2026-05-21T20:00:00Z",
                        "ts_recv_min_utc": "",
                        "ts_recv_max_utc": "",
                        "symbol_count": "5",
                        "instrument_id_count": "5",
                        "columns_json": "[]",
                        "error": "",
                    }
                ],
            )

            rc = summary_module.summarize([audit_dir], output_dir=out, warn_memory_ratio=30.0)

            self.assertEqual(rc, 0)
            with (out / "dbn_audit_summary.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["rows_per_second"], "500.0")
            self.assertEqual(rows[0]["memory_expansion_ratio"], "40.0")
            self.assertIn("memory_expansion_ratio", rows[0]["warning"])

            manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["row_count"], 1)
            self.assertEqual(manifest["warning_count"], 1)
            self.assertEqual(manifest["error_count"], 0)


if __name__ == "__main__":
    unittest.main()
