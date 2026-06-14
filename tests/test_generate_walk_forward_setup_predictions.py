import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_walk_forward_setup_predictions as gen


class GenerateWalkForwardSetupPredictionsTest(unittest.TestCase):
    def test_rolling_day_splits_use_only_prior_days(self):
        dates = np.asarray(["2026-01-01"] * 2 + ["2026-01-02"] * 2 + ["2026-01-03"] * 2 + ["2026-01-04"] * 2)

        splits = gen.build_rolling_day_splits(dates, min_train_days=1, test_days_per_fold=1, day_gap=0)

        self.assertEqual(3, len(splits))
        first_train, first_test, train_days, test_days = splits[0]
        self.assertEqual([0, 1], first_train.tolist())
        self.assertEqual([2, 3], first_test.tolist())
        self.assertEqual(1, train_days)
        self.assertEqual(1, test_days)

    def test_generate_side_oof_predictions_marks_scored_rows(self):
        rows = []
        for day in range(1, 6):
            for i in range(8):
                rows.append({
                    "Symbol": "TSLA",
                    "Timestamp": pd.Timestamp(f"2026-01-0{day} 10:{i:02d}:00"),
                    "Date": f"2026-01-0{day}",
                    "f_a": float(i),
                    "f_b": float(day),
                    "Label_Long_Entry": int(i % 3 == 0),
                })
        df = pd.DataFrame(rows)

        pred, summary = gen.generate_side_oof_predictions(
            df,
            ["f_a", "f_b"],
            "Label_Long_Entry",
            "long",
            model_family="random_forest",
            min_train_days=2,
            test_days_per_fold=1,
            day_gap=0,
            min_train_positives=1,
        )

        self.assertEqual(len(df), len(pred))
        self.assertGreater(summary["coverage_rows"], 0)
        self.assertIn("f_long_setup_prob", pred.columns)
        self.assertTrue(pred["long_setup_oof_available"].isin([0, 1]).all())


if __name__ == "__main__":
    unittest.main()
