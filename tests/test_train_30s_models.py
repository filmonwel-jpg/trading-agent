"""Tests for the Tier-1 upgrades to train_30s_models.py.

Covers:
- calibration_report() correctness (Brier, ECE, bins)
- feature_schema_hash() matches lifecycle trainer algorithm
- perform_walk_forward_testing() new return keys when collect_oof=True
- setup_scorecard.csv, setup_manifest.json, calibration_manifest.json,
  threshold_grid.csv, and oof_setup_predictions.csv are all written by main()
  when --output-dir is given
- Backward compat: perform_walk_forward_testing() without collect_oof still
  returns only the original keys
- --no-onnx suppresses ONNX export for ALL model families:
  base entry/exit, regime classifier, regime-specific (choppy/trend/volatile),
  and opening-30m models
"""
import json
import math
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import train_30s_models as t30


class TestCalibrationReport(unittest.TestCase):
    def test_perfect_predictions(self):
        y = np.array([0, 0, 1, 1], dtype=float)
        p = np.array([0.0, 0.0, 1.0, 1.0], dtype=float)
        r = t30.calibration_report(y, p)
        self.assertEqual(r["rows"], 4)
        self.assertAlmostEqual(r["brier_score"], 0.0, places=6)
        self.assertAlmostEqual(r["ece"], 0.0, places=6)
        self.assertEqual(len(r["bins"]), 10)

    def test_worst_predictions(self):
        y = np.array([0, 0, 1, 1], dtype=float)
        p = np.array([1.0, 1.0, 0.0, 0.0], dtype=float)
        r = t30.calibration_report(y, p)
        self.assertEqual(r["rows"], 4)
        self.assertAlmostEqual(r["brier_score"], 1.0, places=6)

    def test_empty_returns_nan(self):
        r = t30.calibration_report(np.array([]), np.array([]))
        self.assertEqual(r["rows"], 0)
        self.assertTrue(math.isnan(r["brier_score"]))
        self.assertTrue(math.isnan(r["ece"]))

    def test_bins_count(self):
        rng = np.random.default_rng(0)
        y = rng.integers(0, 2, 100).astype(float)
        p = rng.random(100)
        r = t30.calibration_report(y, p, bins=5)
        self.assertEqual(len(r["bins"]), 5)


class TestFeatureSchemaHash(unittest.TestCase):
    def test_matches_lifecycle_algorithm(self):
        """Hash must equal SHA-256("\n".join(cols).encode()) to match lifecycle and Java."""
        import hashlib
        cols = ["f_dist_vwap", "f_atr_norm", "f_rsi"]
        expected = hashlib.sha256("\n".join(cols).encode("utf-8")).hexdigest()
        self.assertEqual(t30.feature_schema_hash(cols), expected)

    def test_order_sensitive(self):
        cols_a = ["a", "b", "c"]
        cols_b = ["c", "b", "a"]
        self.assertNotEqual(t30.feature_schema_hash(cols_a), t30.feature_schema_hash(cols_b))


class TestOptionalModelFamilyFallback(unittest.TestCase):
    def test_require_model_family_raises_instead_of_fallback(self):
        with unittest.mock.patch.object(t30, "LGBMClassifier", None), \
             unittest.mock.patch.object(t30, "LIGHTGBM_IMPORT_ERROR", "ImportError: libomp missing"), \
             unittest.mock.patch.object(t30, "REQUIRE_MODEL_FAMILY", True):
            with self.assertRaisesRegex(RuntimeError, "REQUIRE_MODEL_FAMILY=1"):
                t30.build_classifier("lightgbm")


class TestPerformWalkForwardOof(unittest.TestCase):
    def _make_dataset(self, n_days=12, rows_per_day=30, seed=42):
        rng = np.random.default_rng(seed)
        n = n_days * rows_per_day
        X = rng.random((n, 5)).astype(np.float32)
        y = (rng.random(n) > 0.75).astype(np.int8)
        dates = np.repeat([f"2026-01-{d+1:02d}" for d in range(n_days)], rows_per_day)
        return X, y, dates, n

    def test_backward_compat_no_collect_oof(self):
        X, y, dates, _ = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "test_no_oof")
        # Original keys must be present
        for key in ("model", "total_signals", "total_rows", "avg_precision", "avg_threshold", "folds_used"):
            self.assertIn(key, result)
        # New keys must NOT be present when collect_oof=False
        for key in ("oof_rows", "fold_grid", "brier_score", "ece", "calibration_rows"):
            self.assertNotIn(key, result)

    def test_collect_oof_adds_new_keys(self):
        X, y, dates, n = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "test_with_oof", collect_oof=True)
        for key in ("oof_rows", "fold_grid", "brier_score", "ece", "calibration_rows",
                    "calibration_reliability_rows", "threshold_std", "threshold_max_dev"):
            self.assertIn(key, result)

    def test_oof_rows_cover_test_folds(self):
        X, y, dates, n = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "test_oof_coverage", collect_oof=True)
        oof = result["oof_rows"]
        self.assertGreater(len(oof), 0)
        # Each OOF row must have required fields
        for row in oof[:3]:
            self.assertIn("input_row_idx", row)
            self.assertIn("prob", row)
            self.assertIn("fold_id", row)
            self.assertIn("threshold", row)
            self.assertIn("margin_over_threshold", row)
            self.assertIn("y_true", row)

    def test_threshold_std_nonneg(self):
        X, y, dates, _ = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "thr_std", collect_oof=True)
        self.assertGreaterEqual(result["threshold_std"], 0.0)
        self.assertGreaterEqual(result["threshold_max_dev"], 0.0)

    def test_fold_grid_has_expected_fields(self):
        X, y, dates, _ = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "grid", collect_oof=True)
        for fg in result["fold_grid"]:
            for f in ("fold_id", "threshold", "brier_score", "ece", "calibration_rows"):
                self.assertIn(f, fg)

    def test_calibration_reliability_rows_have_bins(self):
        X, y, dates, _ = self._make_dataset()
        result = t30.perform_walk_forward_testing(X, y, dates, "reliability", collect_oof=True)
        rows = result["calibration_reliability_rows"]
        self.assertGreater(len(rows), 0)
        for row in rows[:5]:
            for f in ("fold_id", "bin_index", "prob_min", "prob_max", "rows",
                      "mean_predicted_probability", "observed_positive_rate",
                      "abs_calibration_error"):
                self.assertIn(f, row)


class TestMainArtifacts(unittest.TestCase):
    """Smoke-test that main() writes expected artifacts when --output-dir is given."""

    def _make_csv(self, path: Path, n_days=10, rows_per_day=80):
        """Create a minimal 30s CSV covering 09:30–13:30 (80 bars × 30 s = 2400 s = 40 min
        into session from 09:30, reaching 11:10), so >=10:00 rows are available after
        the soft-separation filter."""
        rng = np.random.default_rng(1)
        n = n_days * rows_per_day
        dates = np.repeat([f"2026-01-{d+1:02d}" for d in range(n_days)], rows_per_day)
        closes = 100.0 + rng.standard_normal(n).cumsum() * 0.5
        opens = closes * (1 + rng.standard_normal(n) * 0.001)
        highs = np.maximum(opens, closes) * (1 + abs(rng.standard_normal(n)) * 0.002)
        lows = np.minimum(opens, closes) * (1 - abs(rng.standard_normal(n)) * 0.002)
        volumes = rng.integers(100_000, 500_000, n).astype(float)
        # 80 bars × 30 s = 2400 s = 40 min → spans 09:30:00 – 11:09:30 ET.
        # Bars 61+ (09:30 + 61×30 = 09:30 + 1830s = 09:30 + 30m30s = 10:00:30) are in hour 10.
        session_start = 9 * 3600 + 30 * 60
        bar_seconds = np.arange(rows_per_day) * 30
        times = np.tile(
            [
                f"{(session_start + s) // 3600:02d}:{((session_start + s) % 3600) // 60:02d}:{(session_start + s) % 60:02d}"
                for s in bar_seconds
            ],
            n_days,
        )
        df = pd.DataFrame({
            "Timestamp": times,
            "Date": dates,
            "Symbol": "TEST",
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": volumes,
            "WAP": closes,
            "BidPrice": closes - 0.01,
            "AskPrice": closes + 0.01,
            "BidSize": rng.integers(100, 1000, n).astype(float),
            "AskSize": rng.integers(100, 1000, n).astype(float),
            "AtBidVol": rng.integers(1000, 5000, n).astype(float),
            "AtAskVol": rng.integers(1000, 5000, n).astype(float),
            "OptionCallVolume": rng.integers(100, 1000, n).astype(float),
            "OptionPutVolume": rng.integers(100, 1000, n).astype(float),
            "YesterdayClose": closes * 0.999,
            "PreviousSessionHigh": highs.max() * 0.99,
            "PreviousSessionLow": lows.min() * 1.01,
            "TradePrintCount5s": rng.integers(1, 20, n).astype(float),
        })
        df.to_csv(path, index=False)
        return path

    def test_artifacts_written(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            csv_path = tmp / "test_30s.csv"
            self._make_csv(csv_path)
            out_dir = tmp / "setup_out"
            sys.argv = [
                "train_30s_models.py",
                "--input-csv", str(csv_path),
                "--output-dir", str(out_dir),
                "--no-onnx",
            ]
            import os
            # Force minimal config to keep test fast
            os.environ["TRAIN_LEGACY_30S_EXIT_MODELS"] = "0"
            os.environ["USE_NEWS_BAR_FEATURES"] = "0"
            os.environ["USE_REGIME_PROB_FEATURES"] = "0"
            os.environ["USE_META_PRODUCER_FEATURES"] = "0"
            os.environ["UPDATE_CANONICAL_MODEL_ALIASES"] = "0"
            try:
                t30.main()
            finally:
                for k in ("TRAIN_LEGACY_30S_EXIT_MODELS", "USE_NEWS_BAR_FEATURES",
                          "USE_REGIME_PROB_FEATURES", "USE_META_PRODUCER_FEATURES",
                          "UPDATE_CANONICAL_MODEL_ALIASES"):
                    os.environ.pop(k, None)

            # Required artifacts — all must be written even when no signals exist
            for fname in ("setup_scorecard.csv", "setup_manifest.json",
                          "calibration_manifest.json", "threshold_grid.csv",
                          "calibration_reliability.csv", "oof_setup_predictions.csv",
                          "cost_aware_setup_labels.csv", "cost_aware_label_manifest.json"):
                self.assertTrue((out_dir / fname).exists(), f"Missing {fname}")

            # Scorecard columns
            sc = pd.read_csv(out_dir / "setup_scorecard.csv")
            for col in ("model", "avg_threshold", "threshold_std", "threshold_max_dev",
                        "brier_score", "ece", "calibration_rows", "folds_used"):
                self.assertIn(col, sc.columns, f"scorecard missing column {col}")

            # Manifest schema version
            manifest = json.loads((out_dir / "setup_manifest.json").read_text())
            self.assertEqual(manifest["schema_version"], t30.SETUP_MANIFEST_SCHEMA_VERSION)
            self.assertIn("feature_schema_sha256", manifest)
            self.assertIn("feature_columns", manifest)
            self.assertIn("label_info", manifest)
            self.assertTrue(manifest["label_info"]["cost_aware"])
            self.assertEqual("binary_expected_net_r_after_costs", manifest["label_info"]["type"])
            self.assertIn("cost_aware_label_manifest", manifest["artifacts"])

            # Cost-aware label manifest records execution assumptions and summaries.
            label_manifest = json.loads((out_dir / "cost_aware_label_manifest.json").read_text())
            self.assertEqual("setup_cost_aware_labels_v1", label_manifest["schema_version"])
            self.assertTrue(label_manifest["used_for_entry_training"])
            for key in ("entry_spread_model", "exit_spread_model", "entry_slippage_bps",
                        "exit_slippage_bps", "fill_probability", "partial_fill_penalty_r",
                        "missed_fill_penalty_r", "latency_assumption"):
                self.assertIn(key, label_manifest["assumptions"])
            self.assertIn("long", label_manifest["summary"])
            self.assertIn("short", label_manifest["summary"])

            cost_labels = pd.read_csv(out_dir / "cost_aware_setup_labels.csv")
            for col in ("Label_Long_Entry_CostAware", "Label_Short_Entry_CostAware",
                        "Label_Long_Entry_TpBeforeSl", "Label_Short_Entry_TpBeforeSl",
                        "Label_Long_Entry_ExpectedNetRAfterCosts",
                        "Label_Short_Entry_ExpectedNetRAfterCosts"):
                self.assertIn(col, cost_labels.columns, f"cost labels missing column {col}")

            # Calibration manifest
            cal = json.loads((out_dir / "calibration_manifest.json").read_text())
            self.assertEqual(cal["errors"], [])
            self.assertIn("models", cal)

            # Reliability CSV uses lifecycle-compatible reliability-bin schema
            rel = pd.read_csv(out_dir / "calibration_reliability.csv")
            for col in ("model", "side", "fold_id", "bin_index", "prob_min", "prob_max",
                        "rows", "mean_predicted_probability", "observed_positive_rate",
                        "abs_calibration_error"):
                self.assertIn(col, rel.columns, f"calibration_reliability missing column {col}")

            # OOF predictions file: wide format — one row per bar, both long/short columns present
            oof = pd.read_csv(out_dir / "oof_setup_predictions.csv")
            for col in ("Symbol", "Timestamp", "f_long_setup_prob", "long_setup_fold_id",
                        "f_long_setup_threshold", "f_long_setup_threshold_margin",
                        "f_short_setup_prob", "short_setup_fold_id",
                        "is_oof_setup_prediction", "Label_Long_Entry_ExpectedNetRAfterCosts",
                        "Label_Short_Entry_ExpectedNetRAfterCosts"):
                self.assertIn(col, oof.columns, f"oof missing column {col}")
            # side and fold_id (narrow format) must NOT be present
            self.assertNotIn("side", oof.columns, "oof must be wide format — 'side' column not expected")
            self.assertNotIn("fold_id", oof.columns, "oof must be wide format — bare 'fold_id' not expected")

            # No ONNX files (--no-onnx)
            onnx_files = list(out_dir.glob("*.onnx"))
            self.assertEqual(onnx_files, [], "Expected no ONNX files with --no-onnx")

    def test_unused_nan_enrichment_columns_do_not_change_feature_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            csv_path = tmp / "test_30s.csv"
            self._make_csv(csv_path, n_days=3, rows_per_day=90)
            raw = pd.read_csv(csv_path)
            filtered = t30.filter_raw_to_regular_session(raw)

            baseline = t30.calculate_features(filtered.copy())
            enriched_like = filtered.copy()
            enriched_like["EqMbp1BidMean30s"] = np.nan
            enriched_like["OpraTcbboCallAvgSpreadBpsMean30s"] = np.nan
            enriched = t30.calculate_features(enriched_like)

            self.assertEqual(len(enriched), len(baseline))
            pd.testing.assert_series_equal(
                enriched["Timestamp"].reset_index(drop=True),
                baseline["Timestamp"].reset_index(drop=True),
                check_names=False,
            )

    def test_databento_silver_feature_block_is_distinct_and_opt_in(self):
        self.assertGreater(len(t30.DATABENTO_SILVER_FEATURE_COLS), 0)
        self.assertEqual(len(t30.DATABENTO_SILVER_FEATURE_COLS), len(set(t30.DATABENTO_SILVER_FEATURE_COLS)))
        self.assertFalse(set(t30.DATABENTO_SILVER_FEATURE_COLS) & set(t30.META_PRODUCER_FEATURE_COLS))
        self.assertFalse(set(t30.DATABENTO_SILVER_FEATURE_COLS) & set(t30.NEWS_BAR_FEATURE_COLS))
        self.assertFalse(t30.USE_DATABENTO_SILVER_FEATURES)

    def test_databento_silver_feature_presets_resolve_expected_subsets(self):
        expected_presets = {"all", "equs", "opra", "liquidity", "options_flow"}
        self.assertEqual(set(t30.DATABENTO_SILVER_FEATURE_PRESETS), expected_presets)

        all_cols, preset = t30.resolve_databento_silver_feature_columns("all")
        self.assertEqual(preset, "all")
        self.assertEqual(all_cols, t30.DATABENTO_SILVER_FEATURE_COLS)

        equs_cols, preset = t30.resolve_databento_silver_feature_columns("equs")
        self.assertEqual(preset, "equs")
        self.assertEqual(equs_cols, t30.DATABENTO_SILVER_EQUS_FEATURE_COLS)
        self.assertTrue(all(col.startswith("EqMbp1") for col in equs_cols))

        opra_cols, preset = t30.resolve_databento_silver_feature_columns("opra")
        self.assertEqual(preset, "opra")
        self.assertEqual(opra_cols, t30.DATABENTO_SILVER_OPRA_FEATURE_COLS)
        self.assertTrue(all(col.startswith("OpraTcbbo") for col in opra_cols))
        self.assertFalse(set(equs_cols) & set(opra_cols))

        for preset_name in ["liquidity", "options_flow"]:
            cols, resolved = t30.resolve_databento_silver_feature_columns(preset_name)
            self.assertEqual(resolved, preset_name)
            self.assertGreater(len(cols), 0)
            self.assertEqual(len(cols), len(set(cols)))
            self.assertTrue(set(cols).issubset(set(t30.DATABENTO_SILVER_FEATURE_COLS)))

    def test_databento_silver_feature_preset_aliases_and_invalid_values(self):
        self.assertEqual(t30.resolve_databento_silver_feature_columns("options")[1], "options_flow")
        self.assertEqual(t30.resolve_databento_silver_feature_columns("spread-liquidity")[1], "liquidity")
        self.assertEqual(t30.resolve_databento_silver_feature_columns("equity")[1], "equs")
        with self.assertRaisesRegex(ValueError, "Unsupported DATABENTO_SILVER_FEATURE_SET"):
            t30.resolve_databento_silver_feature_columns("not_a_real_preset")

    def test_optional_numeric_columns_fill_missing_nan_and_infinite_values(self):
        frame = pd.DataFrame({
            "EqMbp1SpreadBpsMean30s": ["1.25", None, "bad"],
            "OpraTcbboPutCallVolumeRatio30s": [np.inf, "2.5", -np.inf],
        })

        out = t30.ensure_optional_numeric_columns(frame, t30.DATABENTO_SILVER_FEATURE_COLS, default_value=0.0)

        for col in t30.DATABENTO_SILVER_FEATURE_COLS:
            self.assertIn(col, out.columns)
        self.assertAlmostEqual(out.loc[0, "EqMbp1SpreadBpsMean30s"], 1.25)
        self.assertAlmostEqual(out.loc[1, "OpraTcbboPutCallVolumeRatio30s"], 2.5)
        self.assertEqual(out.loc[2, "EqMbp1SpreadBpsMean30s"], 0.0)
        self.assertEqual(out.loc[0, "OpraTcbboPutCallVolumeRatio30s"], 0.0)
        self.assertEqual(out.loc[2, "OpraTcbboPutCallVolumeRatio30s"], 0.0)
        values = out[t30.DATABENTO_SILVER_FEATURE_COLS].to_numpy(dtype=float)
        self.assertTrue(np.isfinite(values).all())

    def test_append_unique_feature_columns_preserves_order_without_duplicates(self):
        base = ["f_dist_vwap", "EqMbp1SpreadBpsMean30s"]
        appended = t30.append_unique_feature_columns(base, ["EqMbp1SpreadBpsMean30s", "OpraTcbboPutCallVolumeRatio30s"])

        self.assertEqual(appended, ["f_dist_vwap", "EqMbp1SpreadBpsMean30s", "OpraTcbboPutCallVolumeRatio30s"])


class TestRegimeSpecificNoOnnx(unittest.TestCase):
    """Regression: --no-onnx must suppress export in train_regime_specific_models()."""

    def _make_regime_df(self, n=200, seed=7):
        """Minimal DataFrame with required columns for regime-specific training."""
        rng = np.random.default_rng(seed)
        dates = np.repeat([f"2026-01-{d+1:02d}" for d in range(10)], n // 10)
        closes = 100.0 + rng.standard_normal(n).cumsum() * 0.3
        df = pd.DataFrame({
            "Timestamp": pd.to_datetime("2026-01-01 10:00:00"),
            "Date": dates,
            "Symbol": "TEST",
            "MarketRegime": rng.choice(["choppy", "trend", "volatile"], n),
            "Label_Long_Entry": (rng.random(n) > 0.75).astype(int),
            "Label_Short_Entry": (rng.random(n) > 0.75).astype(int),
            "Label_Long_Exit": (rng.random(n) > 0.85).astype(int),
            "Label_Short_Exit": (rng.random(n) > 0.85).astype(int),
        })
        return df

    def test_no_onnx_regime_specific_never_calls_export(self):
        df = self._make_regime_df()
        feature_cols = [f"f_{i}" for i in range(5)]
        for col in feature_cols:
            df[col] = np.random.default_rng(42).random(len(df)).astype(np.float32)

        sentinel = unittest.mock.MagicMock(
            side_effect=AssertionError("export_to_onnx must not be called with --no-onnx")
        )
        # Lower thresholds so training proceeds; assert export never fires.
        with unittest.mock.patch.object(t30, 'MIN_REGIME_ROWS', 1), \
             unittest.mock.patch.object(t30, 'MIN_REGIME_SIGNALS', 1), \
             unittest.mock.patch.object(t30, 'export_to_onnx', sentinel):
            with tempfile.TemporaryDirectory() as tmpdir:
                rows = t30.train_regime_specific_models(
                    df, feature_cols, Path(tmpdir), no_onnx=True
                )
        sentinel.assert_not_called()
        # All exported_to values must reflect skipped status
        for r in rows:
            self.assertIn("skipped", r["exported_to"])

    def test_onnx_enabled_regime_specific_calls_export(self):
        """When no_onnx=False the export function IS called (verify wiring in both directions)."""
        df = self._make_regime_df()
        feature_cols = [f"f_{i}" for i in range(5)]
        for col in feature_cols:
            df[col] = np.random.default_rng(42).random(len(df)).astype(np.float32)

        call_log = []
        def fake_export(model, fc, filename, alias_filename=None):
            call_log.append(filename)

        with unittest.mock.patch.object(t30, 'MIN_REGIME_ROWS', 1), \
             unittest.mock.patch.object(t30, 'MIN_REGIME_SIGNALS', 1), \
             unittest.mock.patch.object(t30, 'export_to_onnx', fake_export):
            with tempfile.TemporaryDirectory() as tmpdir:
                rows = t30.train_regime_specific_models(
                    df, feature_cols, Path(tmpdir), no_onnx=False
                )
        self.assertGreater(len(call_log), 0, "export_to_onnx should be called when no_onnx=False")


class TestOpen30NoOnnx(unittest.TestCase):
    """Regression: --no-onnx must suppress export in train_open30_models()."""

    def _make_open30_df(self, n=200, seed=13):
        rng = np.random.default_rng(seed)
        # All rows in the 09:30–09:59 window so open30 filter passes.
        ts = pd.date_range("2026-01-01 09:30:00", periods=n, freq="30s")
        dates = [t.strftime("%Y-%m-%d") for t in ts]
        df = pd.DataFrame({
            "Timestamp": ts,
            "Date": dates,
            "Symbol": "TEST",
            "Hour": ts.hour,
            "Minute": ts.minute,
            "Label_Long_Entry": (rng.random(n) > 0.75).astype(int),
            "Label_Short_Entry": (rng.random(n) > 0.75).astype(int),
            "Label_Long_Exit": (rng.random(n) > 0.85).astype(int),
            "Label_Short_Exit": (rng.random(n) > 0.85).astype(int),
        })
        return df

    def test_no_onnx_open30_never_calls_export(self):
        df = self._make_open30_df()
        feature_cols = [f"f_{i}" for i in range(5)]
        for col in feature_cols:
            df[col] = np.random.default_rng(42).random(len(df)).astype(np.float32)

        sentinel = unittest.mock.MagicMock(
            side_effect=AssertionError("export_to_onnx must not be called with --no-onnx")
        )
        with unittest.mock.patch.object(t30, 'MIN_OPEN30_ROWS', 1), \
             unittest.mock.patch.object(t30, 'MIN_OPEN30_SIGNALS', 1), \
             unittest.mock.patch.object(t30, 'export_to_onnx', sentinel):
            with tempfile.TemporaryDirectory() as tmpdir:
                rows = t30.train_open30_models(
                    df, feature_cols, Path(tmpdir), no_onnx=True
                )
        sentinel.assert_not_called()
        for r in rows:
            self.assertIn("skipped", r["exported_to"])

    def test_onnx_enabled_open30_calls_export(self):
        df = self._make_open30_df()
        feature_cols = [f"f_{i}" for i in range(5)]
        for col in feature_cols:
            df[col] = np.random.default_rng(42).random(len(df)).astype(np.float32)

        call_log = []
        def fake_export(model, fc, filename, alias_filename=None):
            call_log.append(filename)

        with unittest.mock.patch.object(t30, 'MIN_OPEN30_ROWS', 1), \
             unittest.mock.patch.object(t30, 'MIN_OPEN30_SIGNALS', 1), \
             unittest.mock.patch.object(t30, 'export_to_onnx', fake_export):
            with tempfile.TemporaryDirectory() as tmpdir:
                rows = t30.train_open30_models(
                    df, feature_cols, Path(tmpdir), no_onnx=False
                )
        self.assertGreater(len(call_log), 0, "export_to_onnx should be called when no_onnx=False")


if __name__ == "__main__":
    unittest.main()





