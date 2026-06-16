import unittest
from unittest import mock
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import train_lifecycle_micro_models as lm


class LifecycleMicroRowBuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        lm._ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED = False
        lm.ALLOW_BOOTSTRAP_SETUP_PROXY = False

    def test_side_aware_entry_fill_uses_quotes_and_slippage(self) -> None:
        closes = np.asarray([100.0], dtype=float)
        bids = np.asarray([99.50], dtype=float)
        asks = np.asarray([100.50], dtype=float)

        self.assertAlmostEqual(
            100.50 * 1.001,
            lm.side_aware_entry_fill("long", closes, bids, asks, 0, slippage_bps=10.0),
        )
        self.assertAlmostEqual(
            99.50 * 0.999,
            lm.side_aware_entry_fill("short", closes, bids, asks, 0, slippage_bps=10.0),
        )
        self.assertAlmostEqual(
            100.0 * 1.001,
            lm.side_aware_entry_fill("long", closes, bids, np.asarray([np.nan]), 0, slippage_bps=10.0),
        )

    def test_micro_arm_window_starts_at_30s_bar_close_and_includes_ttl_endpoint(self) -> None:
        df30 = self._bars_30s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.1, "Low": 99.9, "Close": 100.0, "Label_Long_Entry": 1, "Label_Short_Entry": 0},
        ])
        df5 = self._bars_5s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:05"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:10"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:15"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:20"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:25"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:35"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:40"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:45"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:50"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:55"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:31:00"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:31:05"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
        ])

        long_entry, _, long_exit, _ = lm.build_micro_rows(df30, df5)

        self.assertEqual(
            [self._ts(t) for t in ["09:30:30", "09:30:35", "09:30:40", "09:30:45", "09:30:50", "09:30:55", "09:31:00"]],
            long_entry["Timestamp"].tolist(),
        )
        self.assertEqual([0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0], long_entry["f_seconds_since_arm"].tolist())
        self.assertTrue(long_exit.empty)

    def test_micro_exit_guard_rows_start_from_first_positive_micro_entry_candidate(self) -> None:
        df30 = self._bars_30s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0, "Label_Long_Entry": 1, "Label_Short_Entry": 0},
        ])
        df5 = self._bars_5s([
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0},
            {"Timestamp": self._ts("09:30:35"), "Open": 99.7, "High": 99.8, "Low": 99.7, "Close": 99.7},
            {"Timestamp": self._ts("09:30:40"), "Open": 99.8, "High": 100.1, "Low": 99.8, "Close": 100.0},
            {"Timestamp": self._ts("09:30:45"), "Open": 100.0, "High": 100.2, "Low": 99.9, "Close": 100.1},
        ])

        long_entry, _, long_exit, _ = lm.build_micro_rows(df30, df5)

        self.assertEqual([0, 1, 0, 0], long_entry["Label_Long_MicroEntry"].tolist())
        self.assertFalse(long_exit.empty)
        self.assertEqual(self._ts("09:30:35"), long_exit.iloc[0]["EntryTime"])
        self.assertEqual(self._ts("09:30:40"), long_exit.iloc[0]["Timestamp"])

    def test_setup_probability_column_replaces_bootstrap_proxy(self) -> None:
        df30 = self._bars_30s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.2, "Low": 99.9, "Close": 100.0, "Label_Long_Entry": 1, "Label_Short_Entry": 0, "f_entry_prob": 0.42},
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.4, "Low": 99.8, "Close": 100.2, "Label_Long_Entry": 0, "Label_Short_Entry": 0, "f_entry_prob": 0.10},
            {"Timestamp": self._ts("09:31:00"), "Open": 100.2, "High": 100.6, "Low": 100.0, "Close": 100.4, "Label_Long_Entry": 0, "Label_Short_Entry": 0, "f_entry_prob": 0.11},
        ])
        df5 = self._bars_5s([
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.1, "Low": 99.9, "Close": 100.0},
            {"Timestamp": self._ts("09:30:35"), "Open": 100.0, "High": 100.2, "Low": 99.9, "Close": 100.1},
        ])

        long_lifecycle, _ = lm.build_lifecycle_rows(df30)
        long_entry, _, _, _ = lm.build_micro_rows(df30, df5)

        self.assertFalse(long_lifecycle.empty)
        self.assertFalse(long_entry.empty)
        self.assertAlmostEqual(0.42, long_lifecycle.iloc[0]["f_entry_score_proxy"])
        np.testing.assert_allclose(long_entry["f_setup_score_proxy"].to_numpy(dtype=float), 0.42)
        self.assertFalse(lm._ENTRY_SCORE_BOOTSTRAP_WARNING_EMITTED)

    def test_missing_setup_probability_fails_by_default(self) -> None:
        with self.assertRaisesRegex(ValueError, "Missing real out-of-fold setup probability"):
            lm.setup_score_proxy(None)

    def test_apply_setup_predictions_retains_only_oof_rows_and_validates_variation(self) -> None:
        df30 = self._bars_30s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.2, "Low": 99.9, "Close": 100.0, "Label_Long_Entry": 1, "Label_Short_Entry": 0},
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.4, "Low": 99.8, "Close": 100.2, "Label_Long_Entry": 0, "Label_Short_Entry": 0},
            {"Timestamp": self._ts("09:31:00"), "Open": 100.2, "High": 100.6, "Low": 100.0, "Close": 100.4, "Label_Long_Entry": 0, "Label_Short_Entry": 0},
        ])
        preds = pd.DataFrame({
            "Symbol": ["AAPL", "AAPL", "AAPL"],
            "Timestamp": [self._ts("09:30:00"), self._ts("09:30:30"), self._ts("09:31:00")],
            "f_long_setup_prob": [0.21, 0.37, 0.63],
            "f_short_setup_prob": [0.71, 0.53, 0.41],
            "long_setup_fold_id": [1, 1, 2],
            "short_setup_fold_id": [1, 1, 2],
            "is_oof_setup_prediction": [1, 1, 1],
        })
        preds = self._prediction_frame(preds)

        out = lm.apply_setup_predictions(df30, preds, min_unique_values=3)

        self.assertEqual(3, len(out))
        self.assertEqual([0.21, 0.37, 0.63], out["f_long_setup_prob"].round(2).tolist())
        self.assertEqual([0.71, 0.53, 0.41], out["f_short_setup_prob"].round(2).tolist())

    def test_apply_setup_predictions_rejects_constant_bootstrap_scores(self) -> None:
        df30 = self._bars_30s([
            {"Timestamp": self._ts("09:30:00"), "Open": 100.0, "High": 100.2, "Low": 99.9, "Close": 100.0, "Label_Long_Entry": 1, "Label_Short_Entry": 0},
            {"Timestamp": self._ts("09:30:30"), "Open": 100.0, "High": 100.4, "Low": 99.8, "Close": 100.2, "Label_Long_Entry": 0, "Label_Short_Entry": 0},
            {"Timestamp": self._ts("09:31:00"), "Open": 100.2, "High": 100.6, "Low": 100.0, "Close": 100.4, "Label_Long_Entry": 0, "Label_Short_Entry": 0},
        ])
        preds = self._prediction_frame(pd.DataFrame({
            "Symbol": ["AAPL", "AAPL", "AAPL"],
            "Timestamp": [self._ts("09:30:00"), self._ts("09:30:30"), self._ts("09:31:00")],
            "f_long_setup_prob": [1.0, 1.0, 1.0],
            "f_short_setup_prob": [1.0, 1.0, 1.0],
            "long_setup_fold_id": [1, 1, 2],
            "short_setup_fold_id": [1, 1, 2],
            "is_oof_setup_prediction": [1, 1, 1],
        }))

        with self.assertRaisesRegex(ValueError, "bootstrap constant"):
            lm.apply_setup_predictions(df30, preds, min_unique_values=3)

    def test_load_bar_csv_close_fill_is_causal_per_symbol(self) -> None:
        frame = pd.DataFrame([
            {"Symbol": "AAPL", "Timestamp": self._ts("09:30:00"), "Open": np.nan, "High": np.nan, "Low": np.nan, "Close": np.nan, "Volume": 10, "WAP": np.nan, "Count": 1},
            {"Symbol": "MSFT", "Timestamp": self._ts("09:30:00"), "Open": 200.0, "High": 200.2, "Low": 199.8, "Close": 200.0, "Volume": 10, "WAP": 200.0, "Count": 1},
            {"Symbol": "AAPL", "Timestamp": self._ts("09:30:30"), "Open": 101.0, "High": 101.2, "Low": 100.8, "Close": 101.0, "Volume": 10, "WAP": 101.0, "Count": 1},
            {"Symbol": "MSFT", "Timestamp": self._ts("09:30:30"), "Open": np.nan, "High": np.nan, "Low": np.nan, "Close": np.nan, "Volume": 10, "WAP": np.nan, "Count": 1},
        ])
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "bars.csv"
            frame.to_csv(csv_path, index=False)
            out = lm.load_bar_csv(str(csv_path), "30s")

        aapl = out[out["Symbol"] == "AAPL"].sort_values("_ts").reset_index(drop=True)
        msft = out[out["Symbol"] == "MSFT"].sort_values("_ts").reset_index(drop=True)
        self.assertTrue(pd.isna(aapl.loc[0, "Close"]))
        self.assertEqual(101.0, aapl.loc[1, "Close"])
        self.assertEqual(200.0, msft.loc[0, "Close"])
        self.assertEqual(200.0, msft.loc[1, "Close"])

    def test_calibration_report_computes_brier_ece_and_bins(self) -> None:
        report = lm.calibration_report(
            np.asarray([0, 0, 1, 1], dtype=int),
            np.asarray([0.10, 0.20, 0.80, 0.90], dtype=float),
            bins=2,
        )

        self.assertEqual(4, report["rows"])
        self.assertAlmostEqual(0.025, report["brier_score"])
        self.assertAlmostEqual(0.15, report["ece"])
        self.assertEqual(2, len(report["bins"]))
        self.assertEqual(2, report["bins"][0]["rows"])
        self.assertAlmostEqual(0.15, report["bins"][0]["mean_predicted_probability"])
        self.assertAlmostEqual(0.0, report["bins"][0]["observed_positive_rate"])
        self.assertEqual(2, report["bins"][1]["rows"])
        self.assertAlmostEqual(0.85, report["bins"][1]["mean_predicted_probability"])
        self.assertAlmostEqual(1.0, report["bins"][1]["observed_positive_rate"])

    def test_train_binary_model_populates_calibration_metrics(self) -> None:
        rows = []
        base_ts = pd.Timestamp("2026-05-21 09:30:00", tz="America/New_York")
        for i in range(80):
            timestamp = (base_ts + pd.Timedelta(seconds=30 * i)).strftime("%Y%m%d %H:%M:%S America/New_York")
            rows.append({
                "Symbol": "AAPL",
                "Date": "2026-05-21",
                "Timestamp": timestamp,
                "f_30s_ret_1": i / 100.0,
                "f_30s_spread_bps": (i % 5) / 10.0,
                "f_entry_score_proxy": 0.20 + (i % 9) / 20.0,
                "Label_Long_ExitLifecycle": 1 if i % 4 == 0 or i % 9 == 0 else 0,
            })
        frame = pd.DataFrame(rows)

        with tempfile.TemporaryDirectory() as tmp:
            result = lm.train_binary_model(
                frame,
                "Label_Long_ExitLifecycle",
                "longExitLifecycleAi",
                "long_exit_lifecycle.onnx",
                "lifecycle",
                Path(tmp),
                min_rows=20,
                random_state=7,
                no_onnx=True,
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(16, result.calibration_rows)
        self.assertEqual(10, len(result.calibration_bins))
        self.assertTrue(np.isfinite(result.brier_score))
        self.assertTrue(np.isfinite(result.ece))
        self.assertGreaterEqual(result.brier_score, 0.0)
        self.assertGreaterEqual(result.ece, 0.0)

    def test_chronological_three_way_split_freezes_final_holdout(self) -> None:
        train_idx, calibration_idx, holdout_idx = lm.chronological_three_way_split(
            100,
            calibration_frac=0.20,
            holdout_frac=0.20,
        )

        self.assertEqual(60, len(train_idx))
        self.assertEqual(20, len(calibration_idx))
        self.assertEqual(20, len(holdout_idx))
        self.assertLess(train_idx.max(), calibration_idx.min())
        self.assertLess(calibration_idx.max(), holdout_idx.min())
        self.assertEqual(80, int(holdout_idx.min()))

    def test_train_binary_model_posthoc_calibration_artifacts(self) -> None:
        rows = []
        base_ts = pd.Timestamp("2026-05-21 09:30:00", tz="America/New_York")
        for i in range(180):
            timestamp = (base_ts + pd.Timedelta(seconds=30 * i)).strftime("%Y%m%d %H:%M:%S America/New_York")
            rows.append({
                "Symbol": "AAPL",
                "Date": (base_ts + pd.Timedelta(seconds=30 * i)).strftime("%Y-%m-%d"),
                "Timestamp": timestamp,
                "f_30s_ret_1": (i % 17) / 17.0,
                "f_30s_spread_bps": (i % 7) / 10.0,
                "f_entry_score_proxy": 0.10 + (i % 11) / 12.0,
                "Label_Long_ExitLifecycle": 1 if (i % 4 == 0 or i % 9 == 0 or i > 150) else 0,
            })
        frame = pd.DataFrame(rows)

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            result = lm.train_binary_model(
                frame,
                "Label_Long_ExitLifecycle",
                "longExitLifecycleAi",
                "long_exit_lifecycle.onnx",
                "lifecycle",
                out,
                min_rows=50,
                random_state=7,
                no_onnx=True,
                posthoc_calibration="both",
                posthoc_calibration_frac=0.20,
                frozen_holdout_frac=0.20,
                min_frozen_holdout_rows=1,
                min_holdout_predictions=0,
                max_day_dominance_frac=1.0,
            )
            self.assertIsNotNone(result)
            assert result is not None
            self.assertIsNotNone(result.posthoc_calibration)
            posthoc = result.posthoc_calibration or {}
            self.assertEqual(True, posthoc["enabled"])
            self.assertIn(posthoc["selected_method"], {"sigmoid", "isotonic", "raw"})
            self.assertEqual(36, posthoc["calibration_fit_rows"])
            self.assertEqual(36, posthoc["frozen_holdout_rows"])
            self.assertEqual(64, len(posthoc["holdout_fingerprint_sha256"]))
            self.assertGreaterEqual(len(posthoc["threshold_stability_rows"]), 16)
            self.assertIn("stable_island_points", posthoc["threshold_stability"])

            lm.write_scorecards(out, [result])
            comparison = pd.read_csv(out / "posthoc_calibration_comparison.csv")
            stability = pd.read_csv(out / "posthoc_threshold_stability.csv")
            stability_report = json.loads((out / "posthoc_threshold_stability_report.json").read_text(encoding="utf-8"))
            calibrators = json.loads((out / "posthoc_calibrators.json").read_text(encoding="utf-8"))
            manifest = json.loads((out / "calibration_manifest.json").read_text(encoding="utf-8"))

        self.assertIn("raw", set(comparison["calibration_method"]))
        self.assertGreaterEqual(len(comparison), 2)
        self.assertGreaterEqual(len(stability), 16)
        self.assertIn("eligible_threshold", stability.columns)
        self.assertEqual("lifecycle_micro_posthoc_threshold_stability_v1", stability_report["schema_version"])
        self.assertGreaterEqual(len(stability_report["models"]), 1)
        self.assertGreaterEqual(len(calibrators["models"]), 1)
        self.assertEqual("raw_random_forest_probability_with_posthoc_sigmoid_isotonic_comparison", manifest["method"])
        self.assertEqual("chronological_base_train_then_calibration_then_frozen_holdout", manifest["holdout_split"])
        self.assertEqual(True, manifest["models"][0]["posthoc"]["enabled"])
        self.assertIn("posthoc_calibrators_json", manifest["artifacts"])
        self.assertIn("posthoc_threshold_stability_report_json", manifest["artifacts"])

    def test_posthoc_selection_keeps_raw_when_calibrators_are_worse(self) -> None:
        class DummyModel:
            classes_ = np.asarray([0, 1])

            def predict_proba(self, X):
                p = np.asarray(X[:, 0], dtype=float)
                return np.column_stack([1.0 - p, p])

        x_cal = np.asarray([[0.02], [0.98], [0.04], [0.96], [0.03], [0.97]], dtype=np.float32)
        y_cal = np.asarray([0, 1, 0, 1, 0, 1], dtype=int)
        x_hold = np.asarray([[0.01], [0.99], [0.05], [0.95], [0.08], [0.92]], dtype=np.float32)
        y_hold = np.asarray([0, 1, 0, 1, 0, 1], dtype=int)
        data = pd.DataFrame({
            "Symbol": ["AAPL"] * len(y_hold),
            "Date": ["2026-05-21"] * len(y_hold),
            "Timestamp": [f"2026-05-21 09:3{i}:00" for i in range(len(y_hold))],
            "Label_Long_ExitLifecycle": y_hold,
        })

        with mock.patch.object(
            lm,
            "fit_sigmoid_calibrator",
            return_value={"method": "sigmoid", "type": "forced_bad", "coef": 0.0, "intercept": 0.0},
        ), mock.patch.object(
            lm,
            "fit_isotonic_calibrator",
            return_value={"method": "isotonic", "type": "forced_bad", "x_thresholds": [0.0, 1.0], "y_thresholds": [0.5, 0.5]},
        ):
            posthoc = lm.fit_posthoc_calibration(
                model=DummyModel(),
                x_calibration=x_cal,
                y_calibration=y_cal,
                x_holdout=x_hold,
                y_holdout=y_hold,
                holdout_dates=data["Date"],
                data=data,
                holdout_idx=np.arange(len(y_hold)),
                label_col="Label_Long_ExitLifecycle",
                model_kind="lifecycle",
                mode="both",
                random_state=7,
                min_frozen_holdout_rows=1,
                min_holdout_predictions=0,
                max_day_dominance_frac=1.0,
            )

        self.assertEqual("raw", posthoc["selected_method"])
        self.assertEqual(posthoc["raw_metrics"], posthoc["selected_metrics"])
        self.assertIn("raw", {c["method"] for c in posthoc["calibrator_candidates"]})
        self.assertTrue(
            any("Raw/no-op probabilities outperformed" in warning for warning in posthoc["promotion_gate"]["warnings"])
        )

    def test_write_scorecards_writes_route_manifest_schema_hash(self) -> None:
        feature_columns = ["f_30s_ret_1", "f_entry_score_proxy"]
        result = lm.TrainedModelResult(
            name="longExitLifecycleAi",
            filename="long_exit_lifecycle.onnx",
            threshold=0.62,
            precision=0.71,
            recall=0.44,
            brier_score=0.18,
            ece=0.07,
            calibration_rows=120,
            calibration_bins=[{
                "bin_index": 0,
                "prob_min": 0.0,
                "prob_max": 0.1,
                "rows": 12,
                "mean_predicted_probability": 0.04,
                "observed_positive_rate": 0.08,
                "abs_calibration_error": 0.04,
            }],
            pred_pos_rate=0.12,
            label_pos_rate=0.10,
            rows=500,
            positives=50,
            feature_count=len(feature_columns),
            feature_columns=feature_columns,
            exported_to="/tmp/long_exit_lifecycle.onnx",
        )

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            lm.write_scorecards(out, [result])
            route = json.loads((out / "lifecycle_micro_route_manifest.json").read_text(encoding="utf-8"))
            feature_schema = json.loads((out / "feature_schema.json").read_text(encoding="utf-8"))
            calibration_manifest = json.loads((out / "calibration_manifest.json").read_text(encoding="utf-8"))
            reliability = pd.read_csv(out / "calibration_reliability.csv")

        expected_hash = lm.feature_schema_hash(feature_columns)
        self.assertEqual(expected_hash, route[0]["feature_schema_sha256"])
        self.assertEqual(feature_columns, route[0]["feature_columns"])
        self.assertAlmostEqual(0.18, route[0]["calibration"]["brier_score"])
        self.assertAlmostEqual(0.07, route[0]["calibration"]["ece"])
        self.assertEqual(expected_hash, feature_schema["longExitLifecycleAi"]["feature_schema_sha256"])
        self.assertEqual("lifecycle_micro_calibration_v1", calibration_manifest["schema_version"])
        self.assertEqual([], calibration_manifest["errors"])
        self.assertEqual("longExitLifecycleAi", calibration_manifest["models"][0]["model"])
        self.assertEqual(120, calibration_manifest["models"][0]["rows"])
        self.assertEqual(120, calibration_manifest["models"][0]["calibration_rows"])
        self.assertIn("reliability_csv", calibration_manifest["artifacts"])
        self.assertEqual("longExitLifecycleAi", reliability.iloc[0]["model"])
        self.assertAlmostEqual(0.04, reliability.iloc[0]["abs_calibration_error"])

    @staticmethod
    def _ts(clock: str) -> str:
        return f"20260521 {clock} America/New_York"

    @staticmethod
    def _bars_30s(rows: list[dict]) -> pd.DataFrame:
        return LifecycleMicroRowBuilderTest._bars(rows, "30s", with_regime=True)

    @staticmethod
    def _bars_5s(rows: list[dict]) -> pd.DataFrame:
        return LifecycleMicroRowBuilderTest._bars(rows, "5s", with_regime=False)

    @staticmethod
    def _bars(rows: list[dict], cadence: str, with_regime: bool) -> pd.DataFrame:
        frame = pd.DataFrame(rows)
        frame.insert(0, "Symbol", "AAPL")
        if cadence == "30s" and "f_entry_prob" not in frame.columns:
            frame["f_entry_prob"] = 0.42
        frame["Volume"] = frame.get("Volume", 1000.0)
        frame["WAP"] = frame.get("WAP", frame["Close"])
        frame["Count"] = frame.get("Count", 1.0)
        frame["_ts"] = lm.parse_timestamp(frame["Timestamp"])
        frame["Date"] = frame["_ts"].dt.strftime("%Y-%m-%d")
        out = lm.add_common_features(frame, cadence)
        return lm.assign_simple_regime(out) if with_regime else out

    @staticmethod
    def _prediction_frame(frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        out["Symbol"] = out["Symbol"].astype(str).str.upper()
        out["_setup_ts"] = lm.parse_timestamp(out["Timestamp"])
        return out


if __name__ == "__main__":
    unittest.main()


