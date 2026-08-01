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

from databento_event_contract import EVENT_SCHEMA_VERSION, attach_feature_snapshot, decorate_equity_bar, decorate_option_bar
from databento_historical_streamer import equity_events_from_frame


class DatabentoEventContractTest(unittest.TestCase):
    def test_decorates_valid_equity_bar_as_high_quality(self) -> None:
        payload = decorate_equity_bar(
            {
                "event": "equity_bar",
                "symbol": "TSLA",
                "barEpochSec": 1_800_000_000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
                "tradeCount": 10,
                "quoteCount": 8,
                "bid": 100.49,
                "ask": 100.51,
            },
            event_source="unit",
            dataset="EQUS.MINI",
            schema="tbbo",
            stype_in="raw_symbol",
            ts_event_ns=1_800_000_000_123_456_789,
            generated_at_ms=42,
        )

        self.assertEqual(EVENT_SCHEMA_VERSION, payload["EventSchemaVersion"])
        self.assertEqual("none", payload["DataQualityFlags"])
        self.assertEqual(1.0, payload["TradeCoverage"])
        self.assertEqual(1.0, payload["QuoteStateCoverage"])
        self.assertGreaterEqual(payload["QualityScore"], 0.99)
        self.assertEqual(1_800_000_000_123_456_789, payload["tsEventNs"])
        self.assertEqual(42, payload["EventGeneratedAtMs"])

    def test_marks_quote_only_equity_bar_as_synthetic_low_quality(self) -> None:
        payload = decorate_equity_bar(
            {
                "event": "equity_bar",
                "symbol": "TSLA",
                "barEpochSec": 1_800_000_000,
                "close": 100.5,
                "volume": 0,
                "tradeCount": 0,
                "quoteCount": 1,
                "bid": 100.49,
                "ask": 100.51,
            },
            event_source="unit",
        )

        self.assertIn("no_trade", payload["DataQualityFlags"])
        self.assertIn("synthetic_ohlc", payload["DataQualityFlags"])
        self.assertEqual(1.0, payload["SyntheticCoverage"])
        self.assertLess(payload["QualityScore"], 0.70)

    def test_decorates_option_volume_bar_without_equity_quote_penalty(self) -> None:
        payload = decorate_option_bar(
            {"event": "option_bar", "underlying": "TSLA", "right": "C", "barEpochSec": 1, "volume": 12},
            event_source="unit",
            generated_at_ms=42,
        )

        self.assertEqual("none", payload["DataQualityFlags"])
        self.assertEqual(1.0, payload["QualityScore"])
        self.assertEqual(EVENT_SCHEMA_VERSION, payload["EventSchemaVersion"])

    def test_attach_feature_snapshot_keeps_only_numeric_features(self) -> None:
        payload = attach_feature_snapshot(
            {"event": "equity_bar", "symbol": "TSLA", "barEpochSec": 100},
            epoch_sec=120,
            source="unit_source",
            schema_version="unit_schema_v1",
            features={"Good": 1.25, "StringNumber": "2.5", "Bad": "nan", "": 4.0, "Text": "nope"},
        )

        self.assertEqual(120, payload["FeatureSnapshotEpochSec"])
        self.assertEqual("unit_source", payload["FeatureSnapshotSource"])
        self.assertEqual("unit_schema_v1", payload["FeatureSnapshotSchemaVersion"])
        self.assertEqual({"Good": 1.25, "StringNumber": 2.5}, payload["enriched_features"])

    def test_historical_quote_only_tbbo_bar_remains_trade_absent(self) -> None:
        frame = pd.DataFrame({
            "ts_event": [
                pd.Timestamp("2026-06-10T13:30:00Z"),
                pd.Timestamp("2026-06-10T13:30:00.500Z"),
            ],
            "symbol": ["TSLA", "TSLA"],
            "price": [0.0, 0.0],
            "size": [0.0, 0.0],
            "bid_px_00": [100.49, 100.50],
            "ask_px_00": [100.51, 100.52],
            "bid_sz_00": [100, 110],
            "ask_sz_00": [120, 130],
        })

        events = equity_events_from_frame(frame, {"TSLA"}, event_source="unit", dataset="EQUS.MINI", schema="tbbo")

        self.assertEqual(1, len(events))
        payload = events[0][3]
        self.assertEqual(0, payload["tradeCount"])
        self.assertEqual(0, payload["volume"])
        self.assertIn("no_trade", payload["DataQualityFlags"])
        self.assertIn("synthetic_ohlc", payload["DataQualityFlags"])
        self.assertEqual(1.0, payload["QuoteStateCoverage"])
        self.assertLess(payload["QualityScore"], 0.70)


if __name__ == "__main__":
    unittest.main()
