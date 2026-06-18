#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import analyze_databento_short_threshold_floor as floor_analyzer


def _write_oof(run_dir: Path, *, preset: str) -> None:
    del preset
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    # Fold 1: original threshold 0.60 produces zero predictions; floor count 3
    # should choose 0.53 and capture 2/3 positives.
    fold1_probs = [0.55, 0.54, 0.53, 0.30, 0.20, 0.10]
    fold1_labels = [1, 0, 1, 0, 0, 1]
    for prob, label in zip(fold1_probs, fold1_labels):
        rows.append(
            {
                "Label_Short_Entry": label,
                "f_short_setup_prob": prob,
                "short_setup_fold_id": 1,
                "f_short_setup_threshold": 0.60,
            }
        )

    # Fold 2: original threshold produces 2 predictions; count floor 3 should
    # lower threshold to 0.69 with precision 2/3.
    fold2_probs = [0.80, 0.70, 0.69, 0.40, 0.30, 0.20]
    fold2_labels = [1, 0, 1, 0, 1, 0]
    for prob, label in zip(fold2_probs, fold2_labels):
        rows.append(
            {
                "Label_Short_Entry": label,
                "f_short_setup_prob": prob,
                "short_setup_fold_id": 2,
                "f_short_setup_threshold": 0.70,
            }
        )

    pd.DataFrame(rows).to_csv(run_dir / "oof_setup_predictions.csv", index=False)


class TestDatabentoShortThresholdFloor(unittest.TestCase):
    def test_analyze_threshold_floor_finds_zero_and_thin_folds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_oof(root / "liquidity", preset="liquidity")

            result = floor_analyzer.analyze_threshold_floor(
                root,
                presets=("liquidity",),
                min_pred_pos_rate=0.0,
                min_pred_pos_count=3,
            )

            self.assertTrue(result["qa_pass"])
            self.assertTrue((root / floor_analyzer.OUTPUT_FILENAME).is_file())
            rows = result["rows"]
            self.assertEqual([row["original_blocker"] for row in rows], ["zero", "thin"])
            self.assertEqual(rows[0]["floor_threshold"], 0.53)
            self.assertEqual(rows[0]["floor_pred_pos_count"], 3)
            self.assertAlmostEqual(rows[0]["floor_precision"], 2 / 3)
            self.assertEqual(rows[1]["floor_threshold"], 0.69)
            self.assertEqual(rows[1]["floor_pred_pos_count"], 3)
            self.assertAlmostEqual(rows[1]["floor_precision"], 2 / 3)

    def test_required_count_uses_strict_rate_floor(self):
        self.assertEqual(floor_analyzer._required_pred_pos_count(3600, 0.005, 1), 19)
        self.assertEqual(floor_analyzer._required_pred_pos_count(3600, 0.005, 20), 20)
        self.assertEqual(floor_analyzer._required_pred_pos_count(10, 0.0, 3), 3)


if __name__ == "__main__":
    unittest.main()
