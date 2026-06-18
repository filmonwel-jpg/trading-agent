#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import normalize_databento_definitions as definition_normalizer
import normalize_equs_mbp1 as mbp1_normalizer
import normalize_opra_tcbbo as tcbbo_normalizer
from databento_silver_utils import SESSION_SECONDS, parse_option_symbol


class DatabentoSilverNormalizerTest(unittest.TestCase):
    def test_parse_option_symbol_handles_spaced_and_compact_opra_symbols(self) -> None:
        spaced = parse_option_symbol("TSLA 260621C00100000")
        compact = parse_option_symbol("TQQQ260621P00075000")

        self.assertEqual(spaced["underlying"], "TSLA")
        self.assertEqual(spaced["expiration"], "2026-06-21")
        self.assertEqual(spaced["option_right"], "C")
        self.assertEqual(spaced["strike"], 100.0)
        self.assertEqual(compact["underlying"], "TQQQ")
        self.assertEqual(compact["option_right"], "P")
        self.assertEqual(compact["strike"], 75.0)

    def test_mbp1_regularizes_full_session_without_backfilling_quotes(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(
                    [
                        "2026-05-21T13:30:02.100Z",
                        "2026-05-21T13:30:04.000Z",
                    ]
                ),
                "symbol": ["TSLA", "TSLA"],
                "bid_px_00": [100.00, 100.05],
                "ask_px_00": [100.10, 100.00],
                "bid_sz_00": [100, 120],
                "ask_sz_00": [80, 90],
                "bid_ct_00": [2, 3],
                "ask_ct_00": [2, 4],
            }
        )

        frames, rth_rows = mbp1_normalizer.normalize_mbp1_frame(
            raw,
            "20260521",
            symbols={"TSLA"},
            full_session_grid=True,
        )

        self.assertEqual(rth_rows, 2)
        self.assertEqual(set(frames), {"TSLA"})
        frame = frames["TSLA"]
        self.assertEqual(len(frame), SESSION_SECONDS)
        self.assertTrue(pd.isna(frame.iloc[0]["Bid"]))
        self.assertEqual(frame.iloc[0]["QuoteAgeMs"], 999999.0)
        self.assertEqual(frame.iloc[2]["QuoteUpdateCount1s"], 1)
        self.assertEqual(frame.iloc[2]["QuoteAgeMs"], 0.0)
        self.assertEqual(frame.iloc[2]["BarEpochSec"], 1779370202)
        self.assertEqual(frame.iloc[3]["QuoteUpdateCount1s"], 0)
        self.assertEqual(frame.iloc[3]["QuoteAgeMs"], 1000.0)
        self.assertEqual(frame.iloc[2]["QuoteStateValid1s"], 1.0)
        self.assertEqual(frame.iloc[4]["LockedCrossed1s"], 1.0)
        self.assertEqual(frame.iloc[4]["QuoteStateValid1s"], 0.0)

    def test_definition_normalizer_extracts_option_and_equity_metadata(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(
                    [
                        "2026-05-21T00:00:00Z",
                        "2026-05-21T00:00:00Z",
                        "2026-05-21T00:00:00Z",
                    ]
                ),
                "instrument_id": [101, 102, 103],
                "symbol": ["TSLA 260621C00100000", "TSLA", "SPY 260621P00400000"],
                "unit_of_measure_qty": [100, 1, 100],
            }
        )

        frame = definition_normalizer.normalize_definition_frame(
            raw,
            "20260521",
            source_label="opra_definition_20260612",
            dataset="OPRA.PILLAR",
            symbols={"TSLA"},
        )

        self.assertEqual(set(frame["symbol"]), {"TSLA 260621C00100000", "TSLA"})
        option = frame[frame["asset_class"].eq("option")].iloc[0]
        equity = frame[frame["asset_class"].eq("equity")].iloc[0]
        self.assertEqual(option["underlying"], "TSLA")
        self.assertEqual(option["option_right"], "C")
        self.assertEqual(option["expiration"], "2026-06-21")
        self.assertEqual(option["strike"], 100.0)
        self.assertEqual(option["multiplier"], 100.0)
        self.assertEqual(equity["underlying"], "TSLA")
        self.assertEqual(equity["multiplier"], 1.0)

    def test_tcbbo_aggregates_call_put_flow_and_liquidity(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(
                    [
                        "2026-05-21T13:30:00.100Z",
                        "2026-05-21T13:30:00.200Z",
                        "2026-05-21T13:30:01.000Z",
                    ]
                ),
                "instrument_id": [1, 1, 2],
                "symbol": ["TSLA 260621C00100000", "TSLA 260621C00100000", "TSLA 260621P00100000"],
                "price": [1.00, 0.96, 2.00],
                "size": [2, 3, 4],
                "side": ["A", "B", "A"],
                "bid_px_00": [0.95, 0.95, 1.95],
                "ask_px_00": [1.05, 1.05, 2.05],
                "bid_sz_00": [10, 10, 12],
                "ask_sz_00": [11, 11, 13],
            }
        )

        frames, rth_rows = tcbbo_normalizer.normalize_tcbbo_frame(
            raw,
            "20260521",
            symbols={"TSLA"},
            full_session_grid=False,
        )

        self.assertEqual(rth_rows, 3)
        self.assertEqual(set(frames), {"TSLA"})
        frame = frames["TSLA"].reset_index(drop=True)
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.loc[0, "CallOptionContractVolume1s"], 5)
        self.assertEqual(frame.loc[0, "BarEpochSec"], 1779370200)
        self.assertEqual(frame.loc[0, "CallOptionTradeCount1s"], 2)
        self.assertEqual(frame.loc[0, "CallOptionQuoteContextCount1s"], 2)
        self.assertAlmostEqual(frame.loc[0, "CallOptionPremiumNotional1s"], 488.0)
        self.assertEqual(frame.loc[1, "PutOptionContractVolume1s"], 4)
        self.assertEqual(frame.loc[1, "PutOptionTradeCount1s"], 1)
        self.assertAlmostEqual(frame.loc[0, "OptionVolumeImbalance1s"], 5.0 / 6.0)
        self.assertAlmostEqual(frame.loc[1, "OptionVolumeImbalance1s"], -4.0 / 5.0)

    def test_tcbbo_can_use_definition_lookup_when_symbol_is_missing(self) -> None:
        raw = pd.DataFrame(
            {
                "ts_event": pd.to_datetime(["2026-05-21T13:30:00.100Z"]),
                "instrument_id": [99],
                "symbol": [""],
                "price": [1.25],
                "size": [2],
                "bid_px_00": [1.20],
                "ask_px_00": [1.30],
            }
        )
        lookup = (
            {
                "99": tcbbo_normalizer.DefinitionMeta(
                    instrument_id="99",
                    symbol="TSLA 260621C00100000",
                    underlying="TSLA",
                    option_right="C",
                    expiration="2026-06-21",
                    strike=100.0,
                    multiplier=50.0,
                )
            },
            {},
        )

        frames, rth_rows = tcbbo_normalizer.normalize_tcbbo_frame(
            raw,
            "20260521",
            symbols={"TSLA"},
            full_session_grid=False,
            definition_lookup=lookup,
        )

        self.assertEqual(rth_rows, 1)
        frame = frames["TSLA"].reset_index(drop=True)
        self.assertEqual(frame.loc[0, "CallOptionContractVolume1s"], 2)
        self.assertEqual(frame.loc[0, "CallOptionPremiumNotional1s"], 125.0)


if __name__ == "__main__":
    unittest.main()




