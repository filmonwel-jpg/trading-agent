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

import build_databento_enriched_30s as builder


DATE = "20260102"
SYMBOL = "TSLA"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _market_seconds(start_epoch: int, count: int) -> list[int]:
    return list(range(start_epoch, start_epoch + count))


def _timestamp_strings(count: int, start_second: int = 0) -> list[str]:
    return [f"{DATE} 09:30:{second:02d} America/New_York" for second in range(start_second, start_second + count)]


def _baseline_rows(symbol: str = SYMBOL) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Symbol": [symbol, symbol],
            "Timestamp": [f"{DATE} 09:30:00 America/New_York", f"{DATE} 09:30:30 America/New_York"],
            "Open": [100.0, 101.0],
            "High": [101.0, 102.0],
            "Low": [99.0, 100.0],
            "Close": [100.5, 101.5],
            "Volume": [1000, 2000],
            "WAP": [100.25, 101.25],
            "Count": [10, 20],
            "DataQualityFlags": ["none", "none"],
            "ChildDataQualityFlagUnion": ["none", "none"],
        }
    )


def _equs_frame(start_epoch: int, *, first_bucket_only: bool = False) -> pd.DataFrame:
    seconds = _market_seconds(start_epoch, 30 if first_bucket_only else 60)
    timestamps = _timestamp_strings(30 if first_bucket_only else 60)
    second_offsets = list(range(30 if first_bucket_only else 60))
    return pd.DataFrame(
        {
            "date": [DATE] * len(seconds),
            "symbol": [SYMBOL] * len(seconds),
            "Timestamp": timestamps,
            "BarEpochSec": seconds,
            "SessionBucket": ["open"] * len(seconds),
            "MinuteOfDay": [570] * len(seconds),
            "SecondsFromOpen": second_offsets,
            "Bid": [100.00 + i * 0.01 for i in second_offsets],
            "Ask": [100.10 + i * 0.01 for i in second_offsets],
            "BidSize": [100 + i for i in second_offsets],
            "AskSize": [90 + i for i in second_offsets],
            "BidCount": [1] * len(seconds),
            "AskCount": [2] * len(seconds),
            "Mid": [100.05 + i * 0.01 for i in second_offsets],
            "Microprice": [100.04 + i * 0.01 for i in second_offsets],
            "Spread": [0.10] * len(seconds),
            "SpreadBps": [10.0] * len(seconds),
            "RawSpreadMinBps1s": [9.0] * len(seconds),
            "RawSpreadMaxBps1s": [11.0] * len(seconds),
            "L1Imbalance": [0.1] * len(seconds),
            "QuoteUpdateCount1s": [1] * len(seconds),
            "EventCount1s": [2] * len(seconds),
            "QuoteCoverage1s": [1.0] * len(seconds),
            "QuoteStateValid1s": [1.0] * len(seconds),
            "ValidSpread1s": [1.0] * len(seconds),
            "LockedCrossed1s": [0.0] * len(seconds),
            "QuoteAgeMs": [0.0] * len(seconds),
        }
    )


def _opra_frame(start_epoch: int, *, first_bucket_only: bool = False, second_bucket_only: bool = False) -> pd.DataFrame:
    if second_bucket_only:
        seconds = _market_seconds(start_epoch + 30, 30)
        timestamps = _timestamp_strings(30, start_second=30)
        second_offsets = list(range(30, 60))
    else:
        seconds = _market_seconds(start_epoch, 30 if first_bucket_only else 60)
        timestamps = _timestamp_strings(30 if first_bucket_only else 60)
        second_offsets = list(range(30 if first_bucket_only else 60))
    call_volume = [1.0 if offset < 30 else 0.0 for offset in second_offsets]
    put_volume = [0.0 if offset < 30 else 2.0 for offset in second_offsets]
    return pd.DataFrame(
        {
            "date": [DATE] * len(seconds),
            "underlying": [SYMBOL] * len(seconds),
            "Timestamp": timestamps,
            "BarEpochSec": seconds,
            "SessionBucket": ["open"] * len(seconds),
            "MinuteOfDay": [570] * len(seconds),
            "SecondsFromOpen": second_offsets,
            "CallOptionTradeCount1s": [1 if value > 0 else 0 for value in call_volume],
            "PutOptionTradeCount1s": [1 if value > 0 else 0 for value in put_volume],
            "CallOptionContractVolume1s": call_volume,
            "PutOptionContractVolume1s": put_volume,
            "CallOptionPremiumNotional1s": [100.0 * value for value in call_volume],
            "PutOptionPremiumNotional1s": [200.0 * value for value in put_volume],
            "CallOptionQuoteContextCount1s": [1 if value > 0 else 0 for value in call_volume],
            "PutOptionQuoteContextCount1s": [1 if value > 0 else 0 for value in put_volume],
            "CallOptionActiveContracts1s": [1 if value > 0 else 0 for value in call_volume],
            "PutOptionActiveContracts1s": [1 if value > 0 else 0 for value in put_volume],
            "CallOptionAvgSpreadBps1s": [10.0 if value > 0 else None for value in call_volume],
            "PutOptionAvgSpreadBps1s": [12.0 if value > 0 else None for value in put_volume],
            "CallOptionMedianSpreadBps1s": [10.0 if value > 0 else None for value in call_volume],
            "PutOptionMedianSpreadBps1s": [12.0 if value > 0 else None for value in put_volume],
            "CallOptionMinSpreadBps1s": [9.0 if value > 0 else None for value in call_volume],
            "PutOptionMinSpreadBps1s": [11.0 if value > 0 else None for value in put_volume],
            "CallOptionAtBidVolume1s": [0.25 * value for value in call_volume],
            "PutOptionAtBidVolume1s": [0.25 * value for value in put_volume],
            "CallOptionAtAskVolume1s": [0.75 * value for value in call_volume],
            "PutOptionAtAskVolume1s": [0.75 * value for value in put_volume],
            "TotalOptionContractVolume1s": [c + p for c, p in zip(call_volume, put_volume)],
            "TotalOptionPremiumNotional1s": [100.0 * c + 200.0 * p for c, p in zip(call_volume, put_volume)],
            "TotalOptionTradeCount1s": [(1 if c > 0 else 0) + (1 if p > 0 else 0) for c, p in zip(call_volume, put_volume)],
            "TotalOptionQuoteContextCount1s": [(1 if c > 0 else 0) + (1 if p > 0 else 0) for c, p in zip(call_volume, put_volume)],
            "CallMinusPutVolume1s": [c - p for c, p in zip(call_volume, put_volume)],
            "OptionVolumeImbalance1s": [0.0] * len(seconds),
            "PutCallVolumeRatio1s": [0.0] * len(seconds),
        }
    )


def build_silver_tree(root: Path, start_epoch: int, *, omit_opra: bool = False, future_only: bool = False) -> None:
    equs_path = root / "equs_mbp1_1s" / f"date={DATE}" / f"{SYMBOL}_equs_mbp1_1s.csv"
    opra_path = root / "opra_tcbbo_1s" / f"date={DATE}" / f"{SYMBOL}_opra_tcbbo_1s.csv"
    equs_path.parent.mkdir(parents=True, exist_ok=True)
    opra_path.parent.mkdir(parents=True, exist_ok=True)
    _equs_frame(start_epoch, first_bucket_only=future_only).to_csv(equs_path, index=False)
    if future_only:
        _opra_frame(start_epoch, second_bucket_only=True).to_csv(opra_path, index=False)
    elif not omit_opra:
        _opra_frame(start_epoch).to_csv(opra_path, index=False)

    pd.DataFrame(
        [
            {
                "date": DATE,
                "source_label": "equs_mbp1_20260612",
                "dataset": "EQUS.MINI",
                "schema": "mbp-1",
                "source_path": "/raw/equs.dbn.zst",
                "symbol": SYMBOL,
                "raw_rows": 60,
                "rth_rows": 60,
                "output_rows": 30 if future_only else 60,
                "quote_update_seconds": 30 if future_only else 60,
                "quote_state_valid_seconds": 30 if future_only else 60,
                "locked_crossed_seconds": 0,
                "median_spread_bps": 10.0,
                "max_quote_age_ms": 0.0,
                "output_path": str(equs_path),
            }
        ]
    ).to_csv(root / "equs_mbp1_1s" / "equs_mbp1_silver_summary.csv", index=False)
    opra_rows = []
    if not omit_opra:
        opra_rows.append(
            {
                "date": DATE,
                "source_label": "opra_tcbbo_20260612",
                "dataset": "OPRA.PILLAR",
                "schema": "tcbbo",
                "source_path": "/raw/opra.dbn.zst",
                "underlying": SYMBOL,
                "raw_rows": 60,
                "rth_rows": 60,
                "output_rows": 30 if future_only else 60,
                "option_trade_count": 60,
                "option_contract_volume": 90.0,
                "option_quote_context_count": 60,
                "active_contract_seconds": 60,
                "output_path": str(opra_path),
            }
        )
    pd.DataFrame(opra_rows).to_csv(root / "opra_tcbbo_1s" / "opra_tcbbo_silver_summary.csv", index=False)
    _write_json(root / "quality_check" / "silver_quality_manifest.json", {"errors": [], "warnings": [], "strict_quality": True})
    _write_json(root / "equs_mbp1_1s" / "manifest.json", {"schema_version": "equs_mbp1_silver_1s_v1", "errors": []})
    _write_json(root / "opra_tcbbo_1s" / "manifest.json", {"schema_version": "opra_tcbbo_silver_1s_v1", "errors": []})


class BuildDatabentoEnriched30sTest(unittest.TestCase):
    def test_build_preserves_rows_and_aggregates_current_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_epoch = int(pd.Timestamp(f"{DATE} 09:30:00", tz="America/New_York").tz_convert("UTC").timestamp())
            silver_root = root / "silver"
            baseline_path = root / "baseline" / "combined_30s.csv"
            output_dir = root / "enriched"
            build_silver_tree(silver_root, start_epoch)
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            _baseline_rows().to_csv(baseline_path, index=False)

            rc = builder.build_enriched_30s(
                silver_root=silver_root,
                baseline_30s=baseline_path,
                output_dir=output_dir,
                symbols=[SYMBOL],
                expected_dates=[DATE],
                expected_rows_per_symbol_day=2,
                force=True,
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["baseline_rows"], 2)
            self.assertEqual(manifest["output_rows"], 2)
            out = pd.read_csv(output_dir / "combined" / "combined_30s.csv")
            self.assertEqual(len(out), 2)
            self.assertEqual(out.loc[0, "EqMbp1QuoteUpdateCount30s"], 30)
            self.assertEqual(out.loc[0, "OpraTcbboCallContractVolume30s"], 30.0)
            self.assertEqual(out.loc[0, "OpraTcbboPutContractVolume30s"], 0.0)
            self.assertEqual(out.loc[1, "OpraTcbboCallContractVolume30s"], 0.0)
            self.assertEqual(out.loc[1, "OpraTcbboPutContractVolume30s"], 60.0)
            self.assertAlmostEqual(out.loc[0, "OpraTcbboOptionVolumeImbalance30s"], 30.0 / 31.0)
            self.assertTrue((output_dir / "data_30s" / f"{SYMBOL}_30s_training.csv").exists())
            self.assertTrue((output_dir / "reports" / "enriched_feature_join_summary.csv").exists())

    def test_build_preserves_baseline_row_order_and_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_epoch = int(pd.Timestamp(f"{DATE} 09:30:00", tz="America/New_York").tz_convert("UTC").timestamp())
            silver_root = root / "silver"
            baseline_path = root / "baseline" / "combined_30s.csv"
            output_dir = root / "enriched"
            build_silver_tree(silver_root, start_epoch)
            baseline = _baseline_rows().iloc[[1, 0]].reset_index(drop=True)
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline.to_csv(baseline_path, index=False)

            rc = builder.build_enriched_30s(
                silver_root=silver_root,
                baseline_30s=baseline_path,
                output_dir=output_dir,
                symbols=[SYMBOL],
                expected_dates=[DATE],
                expected_rows_per_symbol_day=2,
                force=True,
            )

            self.assertEqual(rc, 0)
            out = pd.read_csv(output_dir / "combined" / "combined_30s.csv")
            pd.testing.assert_frame_equal(out[list(baseline.columns)], baseline, check_dtype=False)

    def test_build_fails_duplicate_symbol_timestamp_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_epoch = int(pd.Timestamp(f"{DATE} 09:30:00", tz="America/New_York").tz_convert("UTC").timestamp())
            silver_root = root / "silver"
            baseline_path = root / "baseline" / "combined_30s.csv"
            output_dir = root / "enriched"
            build_silver_tree(silver_root, start_epoch)
            duplicate_baseline = pd.concat([_baseline_rows().iloc[[0]], _baseline_rows().iloc[[0]]], ignore_index=True)
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            duplicate_baseline.to_csv(baseline_path, index=False)

            rc = builder.build_enriched_30s(
                silver_root=silver_root,
                baseline_30s=baseline_path,
                output_dir=output_dir,
                symbols=[SYMBOL],
                expected_dates=[DATE],
                expected_rows_per_symbol_day=2,
                force=True,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["baseline_duplicate_symbol_timestamp_rows"], 2)
            self.assertTrue(any("duplicate Symbol/Timestamp" in error for error in manifest["errors"]))

    def test_build_fails_missing_silver_when_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_epoch = int(pd.Timestamp(f"{DATE} 09:30:00", tz="America/New_York").tz_convert("UTC").timestamp())
            silver_root = root / "silver"
            baseline_path = root / "baseline" / "combined_30s.csv"
            output_dir = root / "enriched"
            build_silver_tree(silver_root, start_epoch, omit_opra=True)
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            _baseline_rows().to_csv(baseline_path, index=False)

            rc = builder.build_enriched_30s(
                silver_root=silver_root,
                baseline_30s=baseline_path,
                output_dir=output_dir,
                symbols=[SYMBOL],
                expected_dates=[DATE],
                expected_rows_per_symbol_day=2,
                force=True,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(any("missing OPRA silver features" in error for error in manifest["errors"]))

    def test_exact_join_does_not_asof_future_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_epoch = int(pd.Timestamp(f"{DATE} 09:30:00", tz="America/New_York").tz_convert("UTC").timestamp())
            silver_root = root / "silver"
            baseline_path = root / "baseline" / "combined_30s.csv"
            output_dir = root / "enriched"
            build_silver_tree(silver_root, start_epoch, future_only=True)
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            _baseline_rows().iloc[[0]].to_csv(baseline_path, index=False)

            rc = builder.build_enriched_30s(
                silver_root=silver_root,
                baseline_30s=baseline_path,
                output_dir=output_dir,
                symbols=[SYMBOL],
                expected_dates=[DATE],
                expected_rows_per_symbol_day=1,
                fail_on_missing_silver=False,
                force=True,
            )

            self.assertEqual(rc, 0)
            out = pd.read_csv(output_dir / "combined" / "combined_30s.csv")
            self.assertEqual(len(out), 1)
            self.assertEqual(out.loc[0, "OpraTcbboJoinCoverage30s"], 0.0)
            self.assertEqual(out.loc[0, "OpraTcbboPutContractVolume30s"], 0.0)
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertTrue(any("without OPRA silver features" in warning for warning in manifest["warnings"]))


if __name__ == "__main__":
    unittest.main()
