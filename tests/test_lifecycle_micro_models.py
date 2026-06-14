import unittest
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

    def test_write_scorecards_writes_route_manifest_schema_hash(self) -> None:
        feature_columns = ["f_30s_ret_1", "f_entry_score_proxy"]
        result = lm.TrainedModelResult(
            name="longExitLifecycleAi",
            filename="long_exit_lifecycle.onnx",
            threshold=0.62,
            precision=0.71,
            recall=0.44,
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

        expected_hash = lm.feature_schema_hash(feature_columns)
        self.assertEqual(expected_hash, route[0]["feature_schema_sha256"])
        self.assertEqual(feature_columns, route[0]["feature_columns"])
        self.assertEqual(expected_hash, feature_schema["longExitLifecycleAi"]["feature_schema_sha256"])

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



