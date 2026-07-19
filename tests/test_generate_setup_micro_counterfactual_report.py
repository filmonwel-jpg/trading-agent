import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "generate_setup_micro_counterfactual_report.py"

spec = importlib.util.spec_from_file_location("generate_setup_micro_counterfactual_report", SCRIPT_PATH)
cf = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = cf
spec.loader.exec_module(cf)


class TestSetupMicroCounterfactualReport(unittest.TestCase):
    def test_end_to_end_long_positive_label_and_threshold_decisions(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            log_path = tmp_path / "replay.log"
            events_path = tmp_path / "events.ndjson"
            out_dir = tmp_path / "out"

            log_path.write_text(
                "\n".join(
                    [
                        "00:00:00.000 [Strategy-Actor-Thread-TEST] INFO x -- >>> [FLOW][COND][AI.LONG.ENTRY] AI_PREDICTS_ENTRY=PASS | symbol=TEST rsi=40 askOrFallback=100.00 qty=10 prob=0.6200 threshold=0.6000",
                        "00:00:00.001 [Strategy-Actor-Thread-TEST] INFO x -- >>> [FLOW][COND][AI.ENTRY.ARBITRATION] ENTRY_SIDE_SELECTED=PASS | symbol=TEST selected=long reason=only_long_passed longMargin=0.0200",
                        "00:00:00.002 [Strategy-Actor-Thread-TEST] INFO x -- >>> [FLOW][INFO][AI.MICRO.ENTRY] Armed long micro-entry symbol=TEST epoch=1000 ttlSeconds=30 setupProb=0.6200",
                        "00:00:00.003 [Strategy-Actor-Thread-TEST] INFO x -- >>> [FLOW][COND][AI.MICRO.LONG.ENTRY] MICRO_ENTRY_CONFIRMS=FAIL | symbol=TEST prob=0.2500 threshold=0.6400 secondsSinceArm=0",
                        "00:00:00.004 [Strategy-Actor-Thread-TEST] INFO x -- >>> [FLOW][COND][AI.MICRO.LONG.ENTRY] MICRO_ENTRY_CONFIRMS=FAIL | symbol=TEST prob=0.4000 threshold=0.6400 secondsSinceArm=5",
                    ]
                ),
                encoding="utf-8",
            )

            bars = [
                {"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1000, "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "bid": 100.0, "ask": 100.0},
                {"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1005, "open": 100.0, "high": 100.1, "low": 99.95, "close": 100.0, "bid": 100.0, "ask": 100.0},
                {"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1010, "open": 100.1, "high": 100.5, "low": 100.0, "close": 100.4, "bid": 100.4, "ask": 100.4},
                {"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1015, "open": 100.4, "high": 100.6, "low": 100.3, "close": 100.5, "bid": 100.5, "ask": 100.5},
            ]
            events_path.write_text("\n".join(json.dumps(row) for row in bars), encoding="utf-8")

            rc = cf.main(
                [
                    "--log",
                    str(log_path),
                    "--recorded-events",
                    str(events_path),
                    "--output-dir",
                    str(out_dir),
                    "--label-min-micro-prob",
                    "0.30",
                    "--micro-threshold-grid",
                    "0.30 0.50",
                    "--entry-slippage-bps",
                    "0",
                    "--exit-slippage-bps",
                    "0",
                    "--fill-probability",
                    "1",
                    "--partial-fill-penalty-r",
                    "0",
                    "--missed-fill-penalty-r",
                    "0",
                    "--horizon-seconds",
                    "30",
                ]
            )

            self.assertEqual(rc, 0)
            with (out_dir / "setup_downstream_confirmable_labels_v1.csv").open() as handle:
                labels = list(csv.DictReader(handle))
            self.assertEqual(len(labels), 1)
            self.assertEqual(labels[0]["Label_Long_Setup_DownstreamPositive"], "1")
            self.assertEqual(labels[0]["Label_Short_Setup_DownstreamPositive"], "0")
            self.assertEqual(labels[0]["Best_Entry_Delay_Seconds"], "5")
            self.assertGreater(float(labels[0]["Expected_Long_Setup_DownstreamNetR"]), 0.0)

            with (out_dir / "setup_micro_counterfactual_decisions.csv").open() as handle:
                decisions = list(csv.DictReader(handle))
            by_threshold = {row["counterfactual_micro_threshold"]: row for row in decisions}
            self.assertEqual(by_threshold["0.3"]["counterfactual_confirms"], "1")
            self.assertEqual(by_threshold["0.5"]["counterfactual_confirms"], "0")

            summary = json.loads((out_dir / "setup_micro_counterfactual_summary.json").read_text())
            self.assertEqual(summary["arms_total"], 1)
            self.assertEqual(summary["micro_evals_total"], 2)
            self.assertEqual(summary["positive_labels_by_symbol_side"], {"TEST.long": 1})


if __name__ == "__main__":
    unittest.main()



