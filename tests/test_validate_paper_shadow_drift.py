import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "validate_paper_shadow_drift.py"

spec = importlib.util.spec_from_file_location("validate_paper_shadow_drift", SCRIPT_PATH)
validator = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = validator
spec.loader.exec_module(validator)


class TestValidatePaperShadowDrift(unittest.TestCase):
    def test_missing_feature_snapshot_telemetry_is_no_go_when_hit_rate_required(self):
        args = Namespace(
            min_paired_rows=1,
            max_probability_drift=0.02,
            max_decision_mismatch_rate=0.01,
            max_snapshot_status_mismatch_rate=0.01,
            min_feature_snapshot_hit_rate=0.95,
            top=20,
        )
        paper_rows = [
            {
                "symbol": "TEST",
                "side": "long",
                "arm_epoch": "1000",
                "probability": 0.80,
                "decision": True,
                "feature_snapshot_status": "",
                "route": "routeA",
                "_index": 0,
            }
        ]
        shadow_rows = [
            {
                "symbol": "TEST",
                "side": "long",
                "arm_epoch": "1000",
                "probability": 0.80,
                "decision": True,
                "feature_snapshot_status": "",
                "route": "routeA",
                "_index": 0,
            }
        ]

        gate = validator.compare_rows(paper_rows, shadow_rows, args)

        self.assertEqual(gate["status"], "NO-GO")
        self.assertIn("paper_feature_snapshot_hit_rate 0.000000 < minimum 0.950000", gate["issues"])
        self.assertIn("shadow_feature_snapshot_hit_rate 0.000000 < minimum 0.950000", gate["issues"])

    def test_load_failure_prints_gate_issue_to_stdout(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "report"
            missing_paper = Path(tmp) / "missing-paper.log"
            missing_shadow = Path(tmp) / "missing-shadow.log"

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = validator.main(
                    [
                        "--paper-log",
                        str(missing_paper),
                        "--shadow-log",
                        str(missing_shadow),
                        "--output-dir",
                        str(output_dir),
                        "--fail-on-no-go",
                    ]
                )

            self.assertEqual(rc, 1)
            text = stdout.getvalue()
            self.assertIn("PAPER_SHADOW_DRIFT status=NO-GO", text)
            self.assertIn("GATE paper_shadow_event_snapshot_drift status=NO-GO issues=1", text)
            self.assertIn("ISSUE paper_shadow_event_snapshot_drift", text)
            report = json.loads((output_dir / "paper_shadow_drift_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["overall_status"], "NO-GO")
            self.assertEqual(report["gates"][0]["metrics"]["paper_log"], str(missing_paper))


if __name__ == "__main__":
    unittest.main()
