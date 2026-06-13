import unittest
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from build_30s_from_5s_csv import MARKET_TIMEZONE, _aggregate_intraday_to_cadence, _regularize_second_bars


class RegularizeSecondBarsTest(unittest.TestCase):
    def make_late_first_observation(self) -> pd.DataFrame:
        first_observation = pd.Timestamp("2026-05-21 09:30:02", tz=MARKET_TIMEZONE)
        return pd.DataFrame(
            {
                "Open": [101.0],
                "High": [101.0],
                "Low": [101.0],
                "Close": [101.0],
                "WAP": [101.0],
                "Volume": [100.0],
                "Count": [1.0],
                "AtBidVol": [0.0],
                "AtAskVol": [100.0],
                "TradePrintCount5s": [1.0],
                "QuoteUpdateCount5s": [1.0],
                "Bid": [100.9],
                "Ask": [101.1],
                "BidSize": [10.0],
                "AskSize": [20.0],
                "BidLast": [100.9],
                "AskLast": [101.1],
                "BidSizeLast": [10.0],
                "AskSizeLast": [20.0],
                "SpreadBps": [2.0],
                "SpreadMinBps5s": [2.0],
                "SpreadMaxBps5s": [2.0],
                "L1Imbalance": [-0.25],
                "ImbalanceStd5s": [0.0],
            },
            index=[first_observation],
        )

    def test_does_not_backfill_future_quote_or_price_without_previous_close(self):
        out = _regularize_second_bars(self.make_late_first_observation(), "20260521", previous_close=None)

        first_second = pd.Timestamp("2026-05-21 09:30:00", tz=MARKET_TIMEZONE)
        second_before_observation = pd.Timestamp("2026-05-21 09:30:01", tz=MARKET_TIMEZONE)
        observation_second = pd.Timestamp("2026-05-21 09:30:02", tz=MARKET_TIMEZONE)

        self.assertIsNotNone(out)
        self.assertTrue(pd.isna(out.loc[first_second, "Bid"]))
        self.assertTrue(pd.isna(out.loc[first_second, "Ask"]))
        self.assertTrue(pd.isna(out.loc[first_second, "Close"]))
        self.assertTrue(pd.isna(out.loc[second_before_observation, "Close"]))
        self.assertEqual(out.loc[first_second, "BidSize"], 0.0)
        self.assertEqual(out.loc[observation_second, "Bid"], 100.9)
        self.assertEqual(out.loc[observation_second, "Close"], 101.0)

    def test_previous_close_is_explicit_price_fallback_but_not_quote_fallback(self):
        out = _regularize_second_bars(self.make_late_first_observation(), "20260521", previous_close=99.0)

        first_second = pd.Timestamp("2026-05-21 09:30:00", tz=MARKET_TIMEZONE)
        observation_second = pd.Timestamp("2026-05-21 09:30:02", tz=MARKET_TIMEZONE)

        self.assertIsNotNone(out)
        self.assertEqual(out.loc[first_second, "Close"], 99.0)
        self.assertEqual(out.loc[first_second, "Open"], 99.0)
        self.assertTrue(pd.isna(out.loc[first_second, "Bid"]))
        self.assertTrue(pd.isna(out.loc[first_second, "Ask"]))
        self.assertEqual(out.loc[observation_second, "Close"], 101.0)
        self.assertEqual(out.loc[observation_second, "Bid"], 100.9)


class ParentQualityAggregationTest(unittest.TestCase):
    def make_child_seconds(self) -> pd.DataFrame:
        index = pd.date_range("2026-05-21 09:30:00", periods=5, freq="1s", tz=MARKET_TIMEZONE)
        return pd.DataFrame(
            {
                "Open": [100.0, 100.1, 100.2, 100.3, 100.4],
                "High": [100.1, 100.2, 100.3, 100.4, 100.5],
                "Low": [99.9, 100.0, 100.1, 100.2, 100.3],
                "Close": [100.0, 100.1, 100.2, 100.3, 100.4],
                "WAP": [100.0, 100.1, 100.2, 100.3, 100.4],
                "Volume": [100.0] * 5,
                "Count": [1.0] * 5,
                "TradePrintCount5s": [1.0] * 5,
                "QuoteUpdateCount5s": [0.0, 1.0, 1.0, 1.0, 1.0],
                "Bid": [99.9, 100.0, 100.1, 100.2, 100.3],
                "Ask": [100.1, 100.2, 100.3, 100.4, 100.5],
                "BidSize": [10.0] * 5,
                "AskSize": [12.0] * 5,
                "BidLast": [99.9, 100.0, 100.1, 100.2, 100.3],
                "AskLast": [100.1, 100.2, 100.3, 100.4, 100.5],
                "BidSizeLast": [10.0] * 5,
                "AskSizeLast": [12.0] * 5,
                "QuoteAgeMs": [1000.0, 0.0, 0.0, 0.0, 0.0],
                "DataQualityFlags": ["no_quote", "none", "none", "none", "none"],
            },
            index=index,
        )

    def test_parent_no_quote_uses_quote_state_not_child_union(self):
        out = _aggregate_intraday_to_cadence(self.make_child_seconds(), "5s", add_meta_features=False)

        self.assertEqual(len(out), 1)
        row = out.iloc[0]
        self.assertEqual(row["ChildDataQualityFlagUnion"], "no_quote")
        self.assertEqual(row["DataQualityFlags"], "none")
        self.assertEqual(row["QuoteStateSecondsValid"], 5.0)
        self.assertEqual(row["QuoteUpdateSecondsPresent"], 4.0)
        self.assertAlmostEqual(float(row["QuoteStateCoverage"]), 1.0)
        self.assertAlmostEqual(float(row["QuoteUpdateCoverage"]), 0.8)

    def test_parent_no_quote_when_quote_state_coverage_is_low(self):
        children = self.make_child_seconds()
        children[["Bid", "Ask", "BidLast", "AskLast"]] = pd.NA
        children[["QuoteUpdateCount5s", "QuoteAgeMs"]] = 0.0
        children["DataQualityFlags"] = "no_quote"

        out = _aggregate_intraday_to_cadence(children, "5s", add_meta_features=False)

        row = out.iloc[0]
        self.assertIn("no_quote", row["DataQualityFlags"].split("|"))
        self.assertEqual(row["ChildDataQualityFlagUnion"], "no_quote")
        self.assertEqual(row["QuoteStateSecondsValid"], 0.0)
        self.assertAlmostEqual(float(row["QuoteStateCoverage"]), 0.0)

    def test_parent_partial_synthetic_when_some_child_seconds_lack_trades(self):
        children = self.make_child_seconds()
        children.loc[children.index[:2], ["Volume", "Count", "TradePrintCount5s"]] = 0.0
        children.loc[children.index[:2], "DataQualityFlags"] = "no_trade|synthetic_ohlc"

        out = _aggregate_intraday_to_cadence(children, "5s", add_meta_features=False)

        row = out.iloc[0]
        tokens = row["DataQualityFlags"].split("|")
        self.assertIn("partial_synthetic_ohlc", tokens)
        self.assertNotIn("synthetic_ohlc", tokens)
        self.assertNotIn("no_trade", tokens)
        self.assertEqual(row["SyntheticSeconds"], 2.0)
        self.assertAlmostEqual(float(row["SyntheticCoverage"]), 0.4)


if __name__ == "__main__":
    unittest.main()
