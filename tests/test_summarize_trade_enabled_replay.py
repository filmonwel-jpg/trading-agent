import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "summarize_trade_enabled_replay.py"

spec = importlib.util.spec_from_file_location("summarize_trade_enabled_replay", SCRIPT_PATH)
summarizer = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = summarizer
spec.loader.exec_module(summarizer)


class TestSummarizeTradeEnabledReplay(unittest.TestCase):
    def test_cli_writes_custom_event_snapshot_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "event_snapshot_trade"
            output_dir.mkdir()
            log_file = output_dir / "controlled_java_replay.log"
            log_file.write_text(
                "\n".join(
                    [
                        "[BACKTEST] completed=1 failed=0 requested=1",
                        "[FLOW][COND][AI.DOWNSTREAM_SETUP_FILTER] SETUP_FILTER_PASSES=PASS | symbol=TEST side=long prob=0.8000 threshold=0.7500 armEpoch=1000 featureSnapshot=hit featureSidecar=disabled",
                        "[FLOW][COND][AI.MICRO_ENTRY] MICRO_ENTRY_CONFIRMS=PASS | symbol=TEST side=long armEpoch=1000",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (output_dir / "TEST-orders.csv").write_text(
                "EventType\nSUBMITTED\nFILLED\n",
                encoding="utf-8",
            )
            (output_dir / "TEST-trade-lifecycle-summary.csv").write_text(
                "ClosedTradeIndex,TradePnL,RealizedR,MfeR,MaeR,Quantity,SetupToFillSeconds,EntryTime,ExitReason,TradeSide,Symbol,ArmsTotal,ArmConfirmations,ArmExpirations,GuardEvaluations,GuardFires,LifecycleExits,HardRiskExits,EodExits,ExitReasonDistribution\n"
                "1,12.50,1.25,1.80,-0.40,1,5,2026-05-01T14:30:00Z,guard,long,TEST,1,1,0,2,1,0,0,0,guard:1\n",
                encoding="utf-8",
            )
            summary_json = output_dir / "trade_enabled_event_snapshot_summary_20260720.json"
            summary_md = output_dir / "trade_enabled_event_snapshot_summary_20260720.md"

            with contextlib.redirect_stdout(io.StringIO()):
                rc = summarizer.main(
                    [
                        "--output-dir",
                        str(output_dir),
                        "--log-file",
                        str(log_file),
                        "--summary-json",
                        str(summary_json),
                        "--summary-md",
                        str(summary_md),
                        "--title",
                        "Trade-enabled event-carried snapshot replay summary — 2026-07-20",
                    ]
                )

            self.assertEqual(rc, 0)
            summary = json.loads(summary_json.read_text(encoding="utf-8"))
            self.assertEqual(summary["completion"], {"completed": 1, "failed": 0, "requested": 1, "raw": "[BACKTEST] completed=1 failed=0 requested=1"})
            self.assertEqual(summary["marker_counts"]["SETUP_FILTER_PASSES_PASS"], 1)
            self.assertEqual(summary["marker_counts"]["MICRO_ENTRY_CONFIRMS_PASS"], 1)
            self.assertEqual(summary["marker_counts"]["featureSnapshot_hit"], 1)
            self.assertEqual(summary["marker_counts"]["featureSidecar_disabled"], 1)
            self.assertEqual(summary["closed_trades"], 1)
            self.assertEqual(summary["submitted_orders"], 1)
            self.assertEqual(summary["filled_orders"], 1)
            report = summary_md.read_text(encoding="utf-8")
            self.assertIn("Trade-enabled event-carried snapshot replay summary", report)
            self.assertIn("research-only event-carried snapshot replay evidence", report)


if __name__ == "__main__":
    unittest.main()
