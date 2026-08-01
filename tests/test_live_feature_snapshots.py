#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from live_feature_snapshots import LiveEquityFeatureSnapshotEmitter


class LiveFeatureSnapshotEmitterTest(unittest.TestCase):
    def _payload(self, epoch: int, *, symbol: str = "TSLA", price: float = 100.0) -> dict:
        return {
            "event": "equity_bar",
            "symbol": symbol,
            "barEpochSec": epoch,
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price,
            "wap": price,
            "volume": 100,
            "tradeCount": 2,
            "quoteCount": 3,
            "bid": price - 0.01,
            "ask": price + 0.01,
            "bidSize": 10,
            "askSize": 12,
            "atBidVol": 40,
            "atAskVol": 60,
            "TradeSecondsPresent": 1.0,
            "QuoteUpdateSecondsPresent": 1.0,
            "QuoteStateSecondsValid": 1.0,
            "SyntheticSeconds": 0.0,
            "ValidSpreadCoverage": 1.0,
            "LockedCrossedSeconds": 0.0,
            "QuoteAgeMsMean": 0.0,
            "QualityScore": 1.0,
        }

    def test_disabled_emitter_leaves_payload_unchanged(self) -> None:
        emitter = LiveEquityFeatureSnapshotEmitter(enabled=False)
        payload = self._payload(1_800_000_000)
        out = emitter.decorate_payload(payload)
        self.assertIs(out, payload)
        self.assertNotIn("enriched_features", out)

    def test_emits_snapshot_on_last_second_of_bucket(self) -> None:
        emitter = LiveEquityFeatureSnapshotEmitter(enabled=True, source="unit", schema_version="unit_schema")
        base = 1_800_000_000
        for offset in range(29):
            out = emitter.decorate_payload(self._payload(base + offset, price=100.0 + offset * 0.01))
            self.assertNotIn("enriched_features", out)

        out = emitter.decorate_payload(self._payload(base + 29, price=101.0))
        self.assertEqual(base + 30, out["FeatureSnapshotEpochSec"])
        self.assertEqual("unit", out["FeatureSnapshotSource"])
        self.assertEqual("unit_schema", out["FeatureSnapshotSchemaVersion"])
        features = out["enriched_features"]
        self.assertEqual(1.0, features["LiveFeatureSnapshotAvailable"])
        self.assertEqual(30.0, features["LiveObservedSeconds30s"])
        self.assertEqual(3000.0, features["Volume30s"])
        self.assertGreater(features["EqMbp1BidMean30s"], 99.0)
        self.assertGreater(features["EqMbp1AskMean30s"], 99.0)
        self.assertEqual(1.0, features["TradeCoverage"])
        self.assertEqual(1.0, features["QuoteStateCoverage"])

    def test_emits_previous_snapshot_on_next_bucket_if_last_second_missing(self) -> None:
        emitter = LiveEquityFeatureSnapshotEmitter(enabled=True)
        base = 1_800_000_000
        emitter.decorate_payload(self._payload(base))
        out = emitter.decorate_payload(self._payload(base + 30))
        self.assertEqual(base + 30, out["FeatureSnapshotEpochSec"])
        self.assertIn("enriched_features", out)
        self.assertEqual(1.0, out["enriched_features"]["LiveObservedSeconds30s"])

    def test_raw_payload_fallback_coverage_uses_current_second_only(self) -> None:
        emitter = LiveEquityFeatureSnapshotEmitter(enabled=True)
        base = 1_800_000_000
        for offset in range(30):
            has_trade = offset == 0
            payload = {
                "event": "equity_bar",
                "symbol": "TSLA",
                "barEpochSec": base + offset,
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "wap": 100.0,
                "volume": 100 if has_trade else 0,
                "tradeCount": 1 if has_trade else 0,
                "quoteCount": 1 if has_trade else 0,
                "bid": 99.99 if has_trade else 0.0,
                "ask": 100.01 if has_trade else 0.0,
                "bidSize": 10 if has_trade else 0,
                "askSize": 12 if has_trade else 0,
            }
            out = emitter.decorate_payload(payload)

        features = out["enriched_features"]
        self.assertEqual(1.0, features["TradeSecondsPresent"])
        self.assertEqual(1.0 / 30.0, features["TradeCoverage"])
        self.assertEqual(1.0, features["QuoteUpdateSecondsPresent"])
        self.assertEqual(1.0 / 30.0, features["QuoteUpdateCoverage"])


if __name__ == "__main__":
    unittest.main()
