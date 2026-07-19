import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_downstream_setup_training_rows.py"

spec = importlib.util.spec_from_file_location("build_downstream_setup_training_rows", SCRIPT_PATH)
builder = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = builder
spec.loader.exec_module(builder)


class TestBuildDownstreamSetupTrainingRows(unittest.TestCase):
    def test_nearest_epoch_join_with_one_second_bar_end_offset(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            labels_csv = tmp_path / "labels.csv"
            rows_30s_csv = tmp_path / "rows_30s.csv"
            out_dir = tmp_path / "out"

            pd.DataFrame(
                [
                    {
                        "Symbol": "TSLA",
                        "SetupEpochSec": 1777299180,
                        "SetupSide": "long",
                        "Label_Long_Setup_DownstreamPositive": 1,
                        "Label_Short_Setup_DownstreamPositive": 0,
                        "Expected_Long_Setup_DownstreamNetR": 1.2,
                        "Expected_Short_Setup_DownstreamNetR": "",
                        "Max_Future_Micro_Long_Prob": 0.4,
                        "Max_Future_Micro_Short_Prob": "",
                        "Best_Entry_Delay_Seconds": 5,
                    }
                ]
            ).to_csv(labels_csv, index=False)
            pd.DataFrame(
                [
                    {
                        "Symbol": "TSLA",
                        "Timestamp": "20260427 10:12:30 America/New_York",
                        "BarEpochSec": 1777299179,
                        "Open": 365.5,
                        "Close": 365.7,
                    }
                ]
            ).to_csv(rows_30s_csv, index=False)

            rc = builder.main(
                [
                    "--input-30s-csv",
                    str(rows_30s_csv),
                    "--labels-csv",
                    str(labels_csv),
                    "--output-dir",
                    str(out_dir),
                    "--join-tolerance-seconds",
                    "31",
                ]
            )

            self.assertEqual(rc, 0)
            joined = pd.read_csv(out_dir / "setup_downstream_training_rows_v1.csv")
            self.assertEqual(len(joined), 1)
            self.assertEqual(int(joined.loc[0, "BarEpochSec"]), 1777299179)
            self.assertEqual(int(joined.loc[0, "join_abs_epoch_delta_seconds"]), 1)
            self.assertEqual(int(joined.loc[0, "Label_Long_Entry_Downstream"]), 1)
            manifest = json.loads((out_dir / "setup_downstream_training_rows_v1_manifest.json").read_text())
            self.assertEqual(manifest["matched_rows"], 1)
            self.assertEqual(manifest["unmatched_rows"], 0)


if __name__ == "__main__":
    unittest.main()

