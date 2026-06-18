#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import analyze_databento_silver_ablation as analyzer


BASELINE_LONG = 0.443288
BASELINE_SHORT = 0.112428


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_minimal_run(
    run_dir: Path,
    *,
    feature_count: int,
    long_precision: float,
    short_precision: float,
    preset: str | None = None,
    silver_count: int = 0,
    training_rows: int = 100,
    paired_rows: int = 60,
    short_pred_pos_rates: list[float] | None = None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        run_dir / "setup_manifest.json",
        {
            "feature_count": feature_count,
            "training_rows": training_rows,
            "errors": [],
            "feature_blocks": {
                "databento_silver_features_enabled": preset is not None,
                "databento_silver_feature_schema_version": analyzer.SILVER_SCHEMA_VERSION,
                "databento_silver_feature_set": preset,
                "databento_silver_feature_count": silver_count,
            },
        },
    )
    _write_json(run_dir / "calibration_manifest.json", {"errors": []})
    _write_json(run_dir / "cost_aware_label_manifest.json", {"errors": []})
    pd.DataFrame(
        [
            {
                "filename": "long_entry.onnx",
                "signals": 10,
                "rows": training_rows,
                "signal_rate": 0.1,
                "avg_precision": long_precision,
                "avg_threshold": 0.60,
                "threshold_std": 0.0,
                "brier_score": 0.28,
                "ece": 0.19,
                "folds_used": 5,
            },
            {
                "filename": "short_entry.onnx",
                "signals": 9,
                "rows": training_rows,
                "signal_rate": 0.09,
                "avg_precision": short_precision,
                "avg_threshold": 0.62,
                "threshold_std": 0.01,
                "brier_score": 0.24,
                "ece": 0.15,
                "folds_used": 5,
            },
        ]
    ).to_csv(run_dir / "setup_scorecard.csv", index=False)
    threshold_rows = []
    for model, precision, pred_pos_base in [
        ("long_entry.onnx", long_precision, 0.10),
        ("short_entry.onnx", short_precision, 0.03),
    ]:
        side = "long" if model == "long_entry.onnx" else "short"
        for fold_id in range(1, 6):
            pred_pos_rate = pred_pos_base
            if model == "short_entry.onnx":
                if short_pred_pos_rates is not None:
                    pred_pos_rate = short_pred_pos_rates[fold_id - 1]
                elif fold_id == 1:
                    pred_pos_rate = 0.0
            threshold_rows.append(
                {
                    "model": model,
                    "side": side,
                    "fold_id": fold_id,
                    "train_days": fold_id,
                    "test_days": 1,
                    "train_rows": 20,
                    "test_rows": 20,
                    "threshold": 0.60,
                    "test_precision": max(0.0, precision + (fold_id - 3) * 0.01),
                    "test_recall": 0.2,
                    "pred_pos_rate": pred_pos_rate,
                    "brier_score": 0.25,
                    "ece": 0.1,
                    "calibration_rows": 20,
                }
            )
    pd.DataFrame(threshold_rows).to_csv(run_dir / "threshold_grid.csv", index=False)
    pd.DataFrame({"is_oof_setup_prediction": [1] * paired_rows + [0] * (training_rows - paired_rows)}).to_csv(
        run_dir / "oof_setup_predictions.csv",
        index=False,
    )
    pd.DataFrame([{"model": "long_entry.onnx", "bin": 0}]).to_csv(run_dir / "calibration_reliability.csv", index=False)
    pd.DataFrame([{"Label_Long_Entry": 1}]).to_csv(run_dir / "cost_aware_setup_labels.csv", index=False)
    (run_dir / "train_30s_no_onnx.log").write_text(
        ">>> ONNX export disabled (--no-onnx); scorecard/calibration/OOF artifacts were written only.\n",
        encoding="utf-8",
    )


class TestDatabentoSilverAblationAnalysis(unittest.TestCase):
    def test_analyze_ablation_identifies_research_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = root / "baseline"
            ablation_root = root / "ablation"
            _write_minimal_run(
                baseline,
                feature_count=61,
                long_precision=BASELINE_LONG,
                short_precision=BASELINE_SHORT,
            )
            metrics = {
                "all": (96, 35, 0.423488, 0.246309),
                "equs": (75, 14, 0.453100, 0.172900),
                "opra": (82, 21, 0.387700, 0.156400),
                "liquidity": (78, 17, 0.476100, 0.168200),
                "options_flow": (77, 16, 0.422800, 0.153500),
            }
            for preset, (feature_count, silver_count, long_precision, short_precision) in metrics.items():
                _write_minimal_run(
                    ablation_root / preset,
                    feature_count=feature_count,
                    silver_count=silver_count,
                    preset=preset,
                    long_precision=long_precision,
                    short_precision=short_precision,
                )

            result = analyzer.analyze_ablation(baseline, ablation_root)

            self.assertTrue(result["qa_pass"])
            self.assertEqual(result["recommendations"]["best_short_precision_delta_preset"], "all")
            self.assertEqual(result["recommendations"]["best_long_precision_delta_preset"], "liquidity")
            balanced = result["recommendations"]["balanced_positive_precision_candidates"]
            self.assertEqual([row["preset"] for row in balanced], ["liquidity", "equs"])
            liquidity_long = next(
                row for row in result["compare_rows"]
                if row["preset"] == "liquidity" and row["filename"] == "long_entry.onnx"
            )
            self.assertAlmostEqual(liquidity_long["fold_precision_min_current"], 0.456100)
            all_short = next(
                row for row in result["compare_rows"]
                if row["preset"] == "all" and row["filename"] == "short_entry.onnx"
            )
            self.assertEqual(all_short["zero_pred_folds_current"], 1)
            blockers = result["short_fold_blockers"]
            self.assertEqual(len(blockers), 5)
            self.assertEqual(blockers[0]["preset"], "all")
            self.assertEqual(blockers[0]["fold_id"], 1)
            self.assertEqual(blockers[0]["blocker"], "zero")
            self.assertTrue((ablation_root / "databento_silver_ablation_scorecard_compare.csv").is_file())
            self.assertTrue((ablation_root / "databento_silver_ablation_short_fold_blockers.csv").is_file())
            self.assertTrue((ablation_root / "databento_silver_ablation_summary.json").is_file())

    def test_short_fold_blockers_identify_thin_nonzero_folds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = root / "baseline"
            ablation_root = root / "ablation"
            _write_minimal_run(
                baseline,
                feature_count=61,
                long_precision=BASELINE_LONG,
                short_precision=BASELINE_SHORT,
            )
            _write_minimal_run(
                ablation_root / "liquidity",
                feature_count=78,
                silver_count=17,
                preset="liquidity",
                long_precision=0.476100,
                short_precision=0.168200,
                short_pred_pos_rates=[0.0, 0.004, 0.006, 0.03, 0.03],
            )

            result = analyzer.analyze_ablation(baseline, ablation_root, presets=("liquidity",))

            blockers = result["short_fold_blockers"]
            self.assertEqual([(row["fold_id"], row["blocker"]) for row in blockers], [(1, "zero"), (2, "thin")])
            self.assertEqual(blockers[1]["pred_pos_count_est"], 0)

    def test_analyze_ablation_flags_unexpected_onnx_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = root / "baseline"
            ablation_root = root / "ablation"
            _write_minimal_run(
                baseline,
                feature_count=61,
                long_precision=BASELINE_LONG,
                short_precision=BASELINE_SHORT,
            )
            _write_minimal_run(
                ablation_root / "all",
                feature_count=96,
                silver_count=35,
                preset="all",
                long_precision=0.42,
                short_precision=0.24,
            )
            (ablation_root / "all" / "long_entry.onnx").write_text("not really onnx", encoding="utf-8")

            result = analyzer.analyze_ablation(baseline, ablation_root, presets=("all",), write_outputs=False)

            self.assertFalse(result["qa_pass"])
            self.assertIn("unexpected ONNX files", "\n".join(result["presets"][0]["errors"]))


if __name__ == "__main__":
    unittest.main()
