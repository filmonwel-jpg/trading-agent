import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_lifecycle_posthoc_gates.py"
SPEC = importlib.util.spec_from_file_location("check_lifecycle_posthoc_gates", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
gates = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gates)


class CheckLifecyclePosthocGatesTest(unittest.TestCase):
    def _write_artifacts(
        self,
        out: Path,
        score_rows: list[dict],
        comparison_rows: list[dict],
        stability_rows: list[dict] | None = None,
    ) -> None:
        manifest = {
            "schema_version": "lifecycle_micro_calibration_v1",
            "errors": [],
            "models": [
                {
                    "model": row["model"],
                    "posthoc": {"enabled": True, "selected_method": row["posthoc_selected_method"]},
                }
                for row in score_rows
            ],
        }
        (out / "calibration_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (out / "posthoc_calibrators.json").write_text(json.dumps({"models": []}), encoding="utf-8")
        pd.DataFrame(score_rows).to_csv(out / "lifecycle_micro_scorecard.csv", index=False)
        pd.DataFrame(comparison_rows).to_csv(out / "posthoc_calibration_comparison.csv", index=False)
        if stability_rows is None:
            stability_rows = [
                {
                    "model": row["model"],
                    "filename": f"{row['model']}.onnx",
                    "selected": True,
                    "calibration_method": row["posthoc_selected_method"],
                    "selected_threshold": 0.62,
                    "nearest_threshold": 0.62,
                    "selected_threshold_index": 6,
                    "selected_predicted_positive_count": 40,
                    "selected_max_predicted_day_fraction": 0.25,
                    "stable_island_points": 4,
                    "stable_island_threshold_min": 0.58,
                    "stable_island_threshold_max": 0.64,
                    "min_stable_threshold_points": 3,
                    "pass_stable_threshold_island": True,
                }
                for row in score_rows
            ]
        (out / "posthoc_threshold_stability_report.json").write_text(
            json.dumps({"schema_version": "lifecycle_micro_posthoc_threshold_stability_v1", "models": stability_rows}),
            encoding="utf-8",
        )

    def test_evaluate_gates_passes_when_selected_method_is_best_and_limits_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self._write_artifacts(
                out,
                [{"model": "longExitLifecycleAi", "posthoc_selected_method": "raw"}],
                [
                    {
                        "model": "longExitLifecycleAi",
                        "calibration_method": "raw",
                        "brier_score": 0.10,
                        "ece": 0.02,
                        "threshold": 0.62,
                        "calibration_rows": 600,
                        "predicted_positive_count": 40,
                        "max_predicted_day_fraction": 0.25,
                    },
                    {
                        "model": "longExitLifecycleAi",
                        "calibration_method": "sigmoid",
                        "brier_score": 0.12,
                        "ece": 0.03,
                        "threshold": 0.70,
                        "calibration_rows": 600,
                        "predicted_positive_count": 42,
                        "max_predicted_day_fraction": 0.25,
                    },
                ],
            )

            summary, rows = gates.evaluate_gates(out)

        self.assertTrue(summary["promotion_ready"])
        self.assertEqual(0, summary["fail_count"])
        self.assertEqual("PASS", rows.iloc[0]["gate_status"])

    def test_evaluate_gates_fails_day_dominance_and_zero_predictions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self._write_artifacts(
                out,
                [{"model": "longMicroEntryAi", "posthoc_selected_method": "sigmoid"}],
                [
                    {
                        "model": "longMicroEntryAi",
                        "calibration_method": "raw",
                        "brier_score": 0.20,
                        "ece": 0.08,
                        "threshold": 0.50,
                        "calibration_rows": 700,
                        "predicted_positive_count": 0,
                        "max_predicted_day_fraction": 0.0,
                    },
                    {
                        "model": "longMicroEntryAi",
                        "calibration_method": "sigmoid",
                        "brier_score": 0.18,
                        "ece": 0.06,
                        "threshold": 0.50,
                        "calibration_rows": 700,
                        "predicted_positive_count": 0,
                        "max_predicted_day_fraction": 1.0,
                    },
                ],
            )

            summary, rows = gates.evaluate_gates(out)

        self.assertFalse(summary["promotion_ready"])
        self.assertEqual(1, summary["fail_count"])
        self.assertEqual("FAIL", rows.iloc[0]["gate_status"])
        self.assertIn("predicted_positive_count", rows.iloc[0]["warnings"])
        self.assertIn("max_predicted_day_fraction", rows.iloc[0]["warnings"])

    def test_evaluate_gates_fails_unstable_selected_threshold_island(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            self._write_artifacts(
                out,
                [{"model": "shortExitLifecycleAi", "posthoc_selected_method": "raw"}],
                [
                    {
                        "model": "shortExitLifecycleAi",
                        "calibration_method": "raw",
                        "brier_score": 0.10,
                        "ece": 0.02,
                        "threshold": 0.62,
                        "calibration_rows": 600,
                        "predicted_positive_count": 45,
                        "max_predicted_day_fraction": 0.25,
                    },
                ],
                stability_rows=[
                    {
                        "model": "shortExitLifecycleAi",
                        "filename": "short_exit_lifecycle.onnx",
                        "selected": True,
                        "calibration_method": "raw",
                        "selected_threshold": 0.62,
                        "nearest_threshold": 0.62,
                        "selected_threshold_index": 6,
                        "selected_predicted_positive_count": 45,
                        "selected_max_predicted_day_fraction": 0.25,
                        "stable_island_points": 1,
                        "stable_island_threshold_min": 0.62,
                        "stable_island_threshold_max": 0.62,
                        "min_stable_threshold_points": 3,
                        "pass_stable_threshold_island": False,
                    }
                ],
            )

            summary, rows = gates.evaluate_gates(out)

        self.assertFalse(summary["promotion_ready"])
        self.assertEqual("FAIL", rows.iloc[0]["gate_status"])
        self.assertEqual(1, rows.iloc[0]["stable_threshold_island_points"])
        self.assertIn("stable_threshold_island_points", rows.iloc[0]["warnings"])


if __name__ == "__main__":
    unittest.main()
