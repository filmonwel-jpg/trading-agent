import unittest
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from build_30s_from_5s_csv import MARKET_TIMEZONE, _regularize_second_bars


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


if __name__ == "__main__":
    unittest.main()
