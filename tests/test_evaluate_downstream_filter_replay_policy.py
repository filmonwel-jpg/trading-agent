import importlib.util
import json
import pickle
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "evaluate_downstream_filter_replay_policy.py"
FILTER_SCRIPT = REPO_ROOT / "scripts" / "train_downstream_setup_filter.py"

spec = importlib.util.spec_from_file_location("evaluate_downstream_filter_replay_policy", SCRIPT_PATH)
evaluator = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = evaluator
spec.loader.exec_module(evaluator)


class DummyModel:
    def predict_proba(self, x):
        p = np.clip(x[:, 0], 0.0, 1.0)
        return np.column_stack([1.0 - p, p])


class TestEvaluateDownstreamFilterReplayPolicy(unittest.TestCase):
    def test_policy_eval_filters_arms_then_uses_micro_first_cross_decisions(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            rows_csv = tmp_path / "rows.csv"
            decisions_csv = tmp_path / "decisions.csv"
            bundle_dir = tmp_path / "bundle"
            out_dir = tmp_path / "out"
            bundle_dir.mkdir()

            pd.DataFrame(
                [
                    {"arm_id": "A1", "Symbol": "TEST", "SetupSide": "long", "SetupEpochSec": 1000, "SetupTimeUtc": "1970-01-01T00:16:40Z", "feature_edge": 0.8},
                    {"arm_id": "A2", "Symbol": "TEST", "SetupSide": "long", "SetupEpochSec": 1030, "SetupTimeUtc": "1970-01-01T00:17:10Z", "feature_edge": 0.2},
                ]
            ).to_csv(rows_csv, index=False)
            pd.DataFrame(
                [
                    {"arm_id": "A1", "symbol": "TEST", "side": "long", "counterfactual_micro_threshold": 0.3, "counterfactual_confirms": 1, "confirm_eval_epoch_utc": "1970-01-01T00:16:45Z", "expected_net_r": 1.0, "exit_reason": "target"},
                    {"arm_id": "A2", "symbol": "TEST", "side": "long", "counterfactual_micro_threshold": 0.3, "counterfactual_confirms": 1, "confirm_eval_epoch_utc": "1970-01-01T00:17:15Z", "expected_net_r": -1.0, "exit_reason": "stop"},
                ]
            ).to_csv(decisions_csv, index=False)
            bundle = {"model": DummyModel(), "feature_columns": ["feature_edge"], "selected_threshold": 0.5}
            with (bundle_dir / "long_downstream_setup_filter.pkl").open("wb") as handle:
                pickle.dump(bundle, handle)
            with (bundle_dir / "short_downstream_setup_filter.pkl").open("wb") as handle:
                pickle.dump(bundle, handle)

            rc = evaluator.main(
                [
                    "--training-rows-csv",
                    str(rows_csv),
                    "--decisions-csv",
                    str(decisions_csv),
                    "--filter-bundle-dir",
                    str(bundle_dir),
                    "--filter-script",
                    str(FILTER_SCRIPT),
                    "--output-dir",
                    str(out_dir),
                    "--micro-thresholds",
                    "0.30",
                    "--min-confirms-for-best",
                    "1",
                    "--max-day-dominance-for-best",
                    "1.0",
                ]
            )
            self.assertEqual(rc, 0)
            summary = json.loads((out_dir / "downstream_filter_replay_policy_summary.json").read_text())
            self.assertEqual(summary["best_policy"]["confirms"], 1)
            self.assertEqual(summary["best_policy"]["positive"], 1)
            self.assertAlmostEqual(summary["best_policy"]["expected_net_r_mean"], 1.0)
            decisions = pd.read_csv(out_dir / "downstream_filter_replay_policy_decisions.csv")
            self.assertEqual(decisions["arm_id"].tolist(), ["A1"])


if __name__ == "__main__":
    unittest.main()

