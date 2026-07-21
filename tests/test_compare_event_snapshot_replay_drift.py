import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "compare_event_snapshot_replay_drift.py"

spec = importlib.util.spec_from_file_location("compare_event_snapshot_replay_drift", SCRIPT_PATH)
validator = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = validator
spec.loader.exec_module(validator)


class TestCompareEventSnapshotReplayDrift(unittest.TestCase):
    def test_strict_event_snapshot_replay_passes_against_reference_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            sidecar = tmp_path / "sidecar.csv"
            event_log = tmp_path / "event.log"
            reference_log = tmp_path / "reference.log"
            output_dir = tmp_path / "report"

            with sidecar.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["Symbol", "SetupSide", "SetupEpochSec"])
                writer.writeheader()
                writer.writerow({"Symbol": "TEST", "SetupSide": "long", "SetupEpochSec": "1000"})
                writer.writerow({"Symbol": "TEST", "SetupSide": "short", "SetupEpochSec": "1030"})

            event_log.write_text(
                "\n".join(
                    [
                        "[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=PASS | symbol=TEST side=long route=r prob=0.7500 threshold=0.7000 armEpoch=1000 featureSnapshot=hit featureSidecar=disabled",
                        "[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=FAIL | symbol=TEST side=short route=r prob=0.2500 threshold=0.7000 armEpoch=1030 featureSnapshot=hit featureSidecar=disabled",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            reference_log.write_text(
                "\n".join(
                    [
                        "[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=PASS | symbol=TEST side=long route=r prob=0.7500 threshold=0.7000 armEpoch=1000 featureSnapshot=disabled featureSidecar=hit",
                        "[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=FAIL | symbol=TEST side=short route=r prob=0.2500 threshold=0.7000 armEpoch=1030 featureSnapshot=disabled featureSidecar=hit",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            rc = validator.main(
                [
                    "--event-log",
                    str(event_log),
                    "--sidecar-csv",
                    str(sidecar),
                    "--reference-log",
                    str(reference_log),
                    "--output-dir",
                    str(output_dir),
                    "--strict-no-trade",
                    "--require-sidecar-disabled",
                    "--fail-on-no-go",
                ]
            )

            self.assertEqual(rc, 0)
            report = json.loads((output_dir / "event_snapshot_replay_drift_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["overall_status"], "PASS")
            metrics = report["gate"]["metrics"]
            self.assertEqual(metrics["event_rows"], 2)
            self.assertEqual(metrics["missing_expected_rows"], 0)
            self.assertEqual(metrics["feature_snapshot_counts"], {"hit": 2})
            self.assertEqual(metrics["feature_sidecar_counts"], {"disabled": 2})
            self.assertEqual(metrics["reference"]["decision_mismatches"], 0)
            self.assertAlmostEqual(metrics["reference"]["max_probability_drift"], 0.0)


if __name__ == "__main__":
    unittest.main()
