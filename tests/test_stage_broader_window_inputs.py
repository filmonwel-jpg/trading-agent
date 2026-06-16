import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
STAGER = ROOT / "scripts" / "stage_broader_window_inputs.py"


class StageBroaderWindowInputsTest(unittest.TestCase):
    def test_filters_combined_inputs_and_writes_lifecycle_symbol_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src30 = tmp_path / "source_30s.csv"
            src5 = tmp_path / "source_5s.csv"
            out = tmp_path / "staged"

            pd.DataFrame([
                {"Symbol": "TSLA", "Timestamp": "20250721 09:30:00", "Open": 1, "High": 2, "Low": 1, "Close": 1.5, "Volume": 10},
                {"Symbol": "TSLA", "Timestamp": "20250722 09:30:00", "Open": 2, "High": 3, "Low": 2, "Close": 2.5, "Volume": 11},
                {"Symbol": "TQQQ", "Timestamp": "20250723 09:30:00", "Open": 3, "High": 4, "Low": 3, "Close": 3.5, "Volume": 12},
                {"Symbol": "NVDA", "Timestamp": "20250722 09:30:00", "Open": 4, "High": 5, "Low": 4, "Close": 4.5, "Volume": 13},
            ]).to_csv(src30, index=False)
            pd.DataFrame([
                {"Symbol": "TSLA", "Timestamp": "20250722 09:30:00", "Open": 2, "High": 3, "Low": 2, "Close": 2.5, "Volume": 11},
                {"Symbol": "TSLA", "Timestamp": "20250722 09:30:05", "Open": 2, "High": 3, "Low": 2, "Close": 2.6, "Volume": 12},
                {"Symbol": "TQQQ", "Timestamp": "20250723 09:30:00", "Open": 3, "High": 4, "Low": 3, "Close": 3.5, "Volume": 12},
                {"Symbol": "NVDA", "Timestamp": "20250722 09:30:00", "Open": 4, "High": 5, "Low": 4, "Close": 4.5, "Volume": 13},
            ]).to_csv(src5, index=False)

            subprocess.run([
                sys.executable,
                str(STAGER),
                "--input-30s", str(src30),
                "--input-5s", str(src5),
                "--output-dir", str(out),
                "--symbols", "TSLA,TQQQ",
                "--start-date", "2025-07-22",
                "--end-date-exclusive", "2025-07-24",
                "--min-unique-days", "2",
                "--min-rows-30s", "2",
                "--min-rows-5s", "3",
            ], check=True)

            combined_30s = pd.read_csv(out / "combined_30s.csv")
            combined_5s = pd.read_csv(out / "combined_5s.csv")
            self.assertEqual(["TSLA", "TQQQ"], combined_30s["Symbol"].tolist())
            self.assertEqual(["TSLA", "TSLA", "TQQQ"], combined_5s["Symbol"].tolist())
            self.assertTrue((out / "data_30s" / "TSLA_30s_training.csv").exists())
            self.assertTrue((out / "data_30s" / "TQQQ_30s_training.csv").exists())
            self.assertTrue((out / "data_5s" / "TSLA_5s_training.csv").exists())
            self.assertFalse((out / "data_30s" / "NVDA_30s_training.csv").exists())

            manifest = json.loads((out / "input_slice_manifest.json").read_text())
            self.assertEqual("broader_window_input_slice_v1", manifest["schema_version"])
            self.assertEqual(["TSLA", "TQQQ"], manifest["symbols"])
            self.assertEqual(2, manifest["cadences"]["30s"]["rows"])
            self.assertEqual(3, manifest["cadences"]["5s"]["rows"])
            self.assertEqual(2, manifest["cadences"]["30s"]["unique_days"])

    def test_min_unique_days_guard_fails_fast(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src30 = tmp_path / "source_30s.csv"
            src5 = tmp_path / "source_5s.csv"
            rows = [
                {"Symbol": "TSLA", "Timestamp": "20250722 09:30:00", "Open": 1, "High": 2, "Low": 1, "Close": 1.5, "Volume": 10},
            ]
            pd.DataFrame(rows).to_csv(src30, index=False)
            pd.DataFrame(rows).to_csv(src5, index=False)

            proc = subprocess.run([
                sys.executable,
                str(STAGER),
                "--input-30s", str(src30),
                "--input-5s", str(src5),
                "--output-dir", str(tmp_path / "staged"),
                "--symbols", "TSLA",
                "--min-unique-days", "2",
            ], text=True, capture_output=True)

            self.assertNotEqual(0, proc.returncode)
            self.assertIn("unique days too low", proc.stderr + proc.stdout)

    def test_accepts_per_symbol_input_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            dir30 = tmp_path / "data_30s"
            dir5 = tmp_path / "data_5s"
            out = tmp_path / "staged"
            dir30.mkdir()
            dir5.mkdir()
            rows30 = [
                {"Symbol": "TSLA", "Timestamp": "20250722 09:30:00", "Open": 1, "High": 2, "Low": 1, "Close": 1.5, "Volume": 10},
                {"Symbol": "TSLA", "Timestamp": "20250723 09:30:00", "Open": 2, "High": 3, "Low": 2, "Close": 2.5, "Volume": 11},
            ]
            rows5 = rows30 + [
                {"Symbol": "TSLA", "Timestamp": "20250723 09:30:05", "Open": 2, "High": 3, "Low": 2, "Close": 2.6, "Volume": 12},
            ]
            pd.DataFrame(rows30).to_csv(dir30 / "TSLA_30s_training.csv", index=False)
            pd.DataFrame(rows5).to_csv(dir5 / "TSLA_5s_training.csv", index=False)
            pd.DataFrame(rows30).assign(Symbol="NVDA").to_csv(dir30 / "NVDA_30s_training.csv", index=False)
            pd.DataFrame(rows5).assign(Symbol="NVDA").to_csv(dir5 / "NVDA_5s_training.csv", index=False)

            subprocess.run([
                sys.executable,
                str(STAGER),
                "--input-30s", str(dir30),
                "--input-5s", str(dir5),
                "--output-dir", str(out),
                "--symbols", "TSLA",
                "--min-unique-days", "2",
                "--min-rows-30s", "2",
                "--min-rows-5s", "3",
            ], check=True)

            combined_30s = pd.read_csv(out / "combined_30s.csv")
            combined_5s = pd.read_csv(out / "combined_5s.csv")
            self.assertEqual({"TSLA"}, set(combined_30s["Symbol"]))
            self.assertEqual({"TSLA"}, set(combined_5s["Symbol"]))
            self.assertEqual(2, len(combined_30s))
            self.assertEqual(3, len(combined_5s))
            self.assertTrue((out / "data_30s" / "TSLA_30s_training.csv").exists())
            self.assertFalse((out / "data_30s" / "NVDA_30s_training.csv").exists())


if __name__ == "__main__":
    unittest.main()

