import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "train_downstream_setup_filter.py"

spec = importlib.util.spec_from_file_location("train_downstream_setup_filter", SCRIPT_PATH)
trainer = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = trainer
spec.loader.exec_module(trainer)


class TestTrainDownstreamSetupFilter(unittest.TestCase):
    def test_catboost_cost_aware_core_excludes_optional_enriched_feature_groups(self):
        df = pd.DataFrame(
            [
                {
                    "Symbol": "NVDA",
                    "SetupSide": "long",
                    "SetupEpochSec": 1000,
                    "SetupProb": 0.72,
                    "SetupThresholdMargin": 0.04,
                    "Open": 100.0,
                    "Close": 101.0,
                    "tsm_up_prob_30s": 0.61,
                    "regime_trend_prob": 0.55,
                    "seq_lstm_up_prob_30s": 0.52,
                    "setup_breakout_prob": 0.40,
                    "NewsImpactLatest": 0.70,
                    "EqMbp1SpreadBpsMean30s": 1.2,
                    "OpraTcbboTotalContractVolume30s": 42.0,
                    "Label_Long_Setup_DownstreamPositive": 1,
                    "Expected_Long_Setup_DownstreamNetR": 1.1,
                },
                {
                    "Symbol": "NVDA",
                    "SetupSide": "long",
                    "SetupEpochSec": 1030,
                    "SetupProb": 0.58,
                    "SetupThresholdMargin": -0.01,
                    "Open": 101.0,
                    "Close": 100.5,
                    "tsm_up_prob_30s": 0.39,
                    "regime_trend_prob": 0.20,
                    "seq_lstm_up_prob_30s": 0.48,
                    "setup_breakout_prob": 0.15,
                    "NewsImpactLatest": 0.10,
                    "EqMbp1SpreadBpsMean30s": 2.4,
                    "OpraTcbboTotalContractVolume30s": 0.0,
                    "Label_Long_Setup_DownstreamPositive": 0,
                    "Expected_Long_Setup_DownstreamNetR": -0.8,
                },
            ]
        )

        matrix, feature_columns = trainer.build_feature_matrix(
            df,
            exclude_feature_groups={"enriched_meta", "databento_silver"},
        )

        self.assertIn("SetupProb", feature_columns)
        self.assertIn("SetupThresholdMargin", feature_columns)
        self.assertIn("Open", feature_columns)
        self.assertIn("Close", feature_columns)
        self.assertIn("Symbol_NVDA", feature_columns)
        self.assertNotIn("tsm_up_prob_30s", feature_columns)
        self.assertNotIn("regime_trend_prob", feature_columns)
        self.assertNotIn("seq_lstm_up_prob_30s", feature_columns)
        self.assertNotIn("setup_breakout_prob", feature_columns)
        self.assertNotIn("NewsImpactLatest", feature_columns)
        self.assertNotIn("EqMbp1SpreadBpsMean30s", feature_columns)
        self.assertNotIn("OpraTcbboTotalContractVolume30s", feature_columns)
        self.assertEqual(matrix.shape[0], 2)


if __name__ == "__main__":
    unittest.main()

