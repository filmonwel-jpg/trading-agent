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

import verify_databento_silver_outputs as verifier


DATES = ["20260102", "20260103"]
SYMBOLS = ["TSLA", "QQQ"]


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _definition_frame(date: str, source_label: str) -> pd.DataFrame:
    if source_label.startswith("opra"):
        return pd.DataFrame(
            {
                "date": [date, date],
                "source_label": [source_label, source_label],
                "dataset": ["OPRA.PILLAR", "OPRA.PILLAR"],
                "schema": ["definition", "definition"],
                "instrument_id": [101, 102],
                "symbol": ["TSLA 260621C00100000", "QQQ 260621P00400000"],
                "raw_symbol": ["TSLA 260621C00100000", "QQQ 260621P00400000"],
                "asset_class": ["option", "option"],
                "underlying": ["TSLA", "QQQ"],
                "option_right": ["C", "P"],
                "expiration": ["2026-06-21", "2026-06-21"],
                "strike": [100.0, 400.0],
                "multiplier": [100.0, 100.0],
                "ts_event_utc": ["2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"],
            }
        )
    return pd.DataFrame(
        {
            "date": [date, date],
            "source_label": [source_label, source_label],
            "dataset": ["EQUS.MINI", "EQUS.MINI"],
            "schema": ["definition", "definition"],
            "instrument_id": [1, 2],
            "symbol": ["TSLA", "QQQ"],
            "raw_symbol": ["TSLA", "QQQ"],
            "asset_class": ["equity", "equity"],
            "underlying": ["TSLA", "QQQ"],
            "option_right": ["", ""],
            "expiration": ["", ""],
            "strike": [None, None],
            "multiplier": [1.0, 1.0],
            "ts_event_utc": ["2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"],
        }
    )


def _equs_frame(date: str, symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date, date, date],
            "symbol": [symbol, symbol, symbol],
            "Timestamp": ["20260102 09:30:00 America/New_York", "20260102 09:30:01 America/New_York", "20260102 09:30:02 America/New_York"],
            "BarEpochSec": [1, 2, 3],
            "SessionBucket": ["open", "open", "open"],
            "MinuteOfDay": [570, 570, 570],
            "SecondsFromOpen": [0, 1, 2],
            "Bid": [100.0, 100.0, 100.0],
            "Ask": [100.1, 100.1, 100.1],
            "BidSize": [10, 11, 12],
            "AskSize": [12, 13, 14],
            "BidCount": [1, 1, 1],
            "AskCount": [1, 1, 1],
            "Mid": [100.05, 100.05, 100.05],
            "Microprice": [100.05, 100.05, 100.05],
            "Spread": [0.1, 0.1, 0.1],
            "SpreadBps": [10.0, 10.0, 10.0],
            "RawSpreadMinBps1s": [10.0, 10.0, 10.0],
            "RawSpreadMaxBps1s": [10.0, 10.0, 10.0],
            "L1Imbalance": [0.0, 0.0, 0.0],
            "QuoteUpdateCount1s": [1, 1, 1],
            "EventCount1s": [1, 1, 1],
            "QuoteCoverage1s": [1.0, 1.0, 1.0],
            "QuoteStateValid1s": [1.0, 1.0, 1.0],
            "ValidSpread1s": [1.0, 1.0, 1.0],
            "LockedCrossed1s": [0.0, 0.0, 0.0],
            "QuoteAgeMs": [0.0, 0.0, 0.0],
        }
    )


def _opra_frame(date: str, underlying: str, bad_total: bool = False, bad_notional: bool = False, notional_residual: bool = False) -> pd.DataFrame:
    total_volume = [5.0, 1.0, 0.0]
    if bad_total:
        total_volume = [999.0, 1.0, 0.0]
    total_notional = [500.0, 100.0, 0.0]
    if notional_residual:
        total_notional = [500.25, 100.0, 0.0]
    if bad_notional:
        total_notional = [700.0, 100.0, 0.0]
    return pd.DataFrame(
        {
            "date": [date, date, date],
            "underlying": [underlying, underlying, underlying],
            "Timestamp": ["20260102 09:30:00 America/New_York", "20260102 09:30:01 America/New_York", "20260102 09:30:02 America/New_York"],
            "BarEpochSec": [1, 2, 3],
            "SessionBucket": ["open", "open", "open"],
            "MinuteOfDay": [570, 570, 570],
            "SecondsFromOpen": [0, 1, 2],
            "CallOptionTradeCount1s": [2, 0, 0],
            "PutOptionTradeCount1s": [1, 1, 0],
            "CallOptionContractVolume1s": [4.0, 0.0, 0.0],
            "PutOptionContractVolume1s": [1.0, 1.0, 0.0],
            "CallOptionPremiumNotional1s": [400.0, 0.0, 0.0],
            "PutOptionPremiumNotional1s": [100.0, 100.0, 0.0],
            "CallOptionQuoteContextCount1s": [2, 0, 0],
            "PutOptionQuoteContextCount1s": [1, 1, 0],
            "CallOptionActiveContracts1s": [1, 0, 0],
            "PutOptionActiveContracts1s": [1, 1, 0],
            "CallOptionAvgSpreadBps1s": [10.0, None, None],
            "PutOptionAvgSpreadBps1s": [12.0, 12.0, None],
            "CallOptionMedianSpreadBps1s": [10.0, None, None],
            "PutOptionMedianSpreadBps1s": [12.0, 12.0, None],
            "CallOptionMinSpreadBps1s": [10.0, None, None],
            "PutOptionMinSpreadBps1s": [12.0, 12.0, None],
            "CallOptionAtBidVolume1s": [1.0, 0.0, 0.0],
            "PutOptionAtBidVolume1s": [0.0, 1.0, 0.0],
            "CallOptionAtAskVolume1s": [3.0, 0.0, 0.0],
            "PutOptionAtAskVolume1s": [1.0, 0.0, 0.0],
            "TotalOptionContractVolume1s": total_volume,
            "TotalOptionPremiumNotional1s": total_notional,
            "TotalOptionTradeCount1s": [3, 1, 0],
            "TotalOptionQuoteContextCount1s": [3, 1, 0],
            "CallMinusPutVolume1s": [3.0, -1.0, 0.0],
            "OptionVolumeImbalance1s": [0.5, -0.5, 0.0],
            "PutCallVolumeRatio1s": [0.2, 1.0, 0.0],
        }
    )


def build_silver_tree(root: Path, *, bad_opra_total: bool = False, bad_opra_notional: bool = False, notional_residual: bool = False) -> None:
    summaries = {"definitions": [], "equs_mbp1_1s": [], "opra_tcbbo_1s": []}
    for date in DATES:
        for source_label in ["equs_definition_20260612", "opra_definition_20260612"]:
            frame = _definition_frame(date, source_label)
            path = root / "definitions" / f"source_label={source_label}" / f"date={date}" / "definitions.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False)
            summaries["definitions"].append(
                {
                    "date": date,
                    "source_label": source_label,
                    "dataset": "EQUS.MINI" if source_label.startswith("equs") else "OPRA.PILLAR",
                    "schema": "definition",
                    "source_path": f"/raw/{date}/{source_label}.dbn.zst",
                    "raw_rows": len(frame),
                    "output_rows": len(frame),
                    "instrument_id_count": len(frame),
                    "symbol_count": len(frame),
                    "underlying_count": len(SYMBOLS),
                    "option_contract_count": int(frame["asset_class"].eq("option").sum()),
                    "output_path": str(path),
                }
            )
        for symbol in SYMBOLS:
            equs = _equs_frame(date, symbol)
            equs_path = root / "equs_mbp1_1s" / f"date={date}" / f"{symbol}_equs_mbp1_1s.csv"
            equs_path.parent.mkdir(parents=True, exist_ok=True)
            equs.to_csv(equs_path, index=False)
            summaries["equs_mbp1_1s"].append(
                {
                    "date": date,
                    "source_label": "equs_mbp1_20260612",
                    "dataset": "EQUS.MINI",
                    "schema": "mbp-1",
                    "source_path": f"/raw/{date}/equs.dbn.zst",
                    "symbol": symbol,
                    "raw_rows": 10,
                    "rth_rows": 10,
                    "output_rows": len(equs),
                    "quote_update_seconds": 3,
                    "quote_state_valid_seconds": 3,
                    "locked_crossed_seconds": 0,
                    "median_spread_bps": 10.0,
                    "max_quote_age_ms": 0.0,
                    "output_path": str(equs_path),
                }
            )
            opra = _opra_frame(
                date,
                symbol,
                bad_total=bad_opra_total and date == DATES[0] and symbol == SYMBOLS[0],
                bad_notional=bad_opra_notional and date == DATES[0] and symbol == SYMBOLS[0],
                notional_residual=notional_residual and date == DATES[0] and symbol == SYMBOLS[0],
            )
            opra_path = root / "opra_tcbbo_1s" / f"date={date}" / f"{symbol}_opra_tcbbo_1s.csv"
            opra_path.parent.mkdir(parents=True, exist_ok=True)
            opra.to_csv(opra_path, index=False)
            summaries["opra_tcbbo_1s"].append(
                {
                    "date": date,
                    "source_label": "opra_tcbbo_20260612",
                    "dataset": "OPRA.PILLAR",
                    "schema": "tcbbo",
                    "source_path": f"/raw/{date}/opra.dbn.zst",
                    "underlying": symbol,
                    "raw_rows": 10,
                    "rth_rows": 10,
                    "output_rows": len(opra),
                    "option_trade_count": 4,
                    "option_contract_volume": 6.0,
                    "option_quote_context_count": 4,
                    "active_contract_seconds": 2,
                    "output_path": str(opra_path),
                }
            )

    _write_json(root / "definitions" / "manifest.json", {"schema_version": "databento_definition_silver_v1", "selected_dates": DATES, "selected_file_count": 4, "output_file_count": 4, "output_rows": 8, "errors": []})
    _write_json(root / "equs_mbp1_1s" / "manifest.json", {"schema_version": "equs_mbp1_silver_1s_v1", "selected_dates": DATES, "selected_file_count": 2, "output_file_count": 4, "output_rows": 12, "errors": []})
    _write_json(root / "opra_tcbbo_1s" / "manifest.json", {"schema_version": "opra_tcbbo_silver_1s_v1", "selected_dates": DATES, "selected_file_count": 2, "output_file_count": 4, "output_rows": 12, "errors": []})
    pd.DataFrame(summaries["definitions"]).to_csv(root / "definitions" / "definition_silver_summary.csv", index=False)
    pd.DataFrame(summaries["equs_mbp1_1s"]).to_csv(root / "equs_mbp1_1s" / "equs_mbp1_silver_summary.csv", index=False)
    pd.DataFrame(summaries["opra_tcbbo_1s"]).to_csv(root / "opra_tcbbo_1s" / "opra_tcbbo_silver_summary.csv", index=False)


class VerifyDatabentoSilverOutputsTest(unittest.TestCase):
    def test_verify_passes_clean_silver_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "silver"
            build_silver_tree(root, notional_residual=True)
            rc = verifier.verify(
                silver_root=root,
                output_dir=root / "quality_check",
                symbols=SYMBOLS,
                expected_dates=DATES,
                expected_rows_per_grid=3,
                min_equs_state_coverage=0.95,
                min_equs_valid_spread_coverage=0.95,
                max_equs_locked_crossed_frac=0.02,
                min_opra_active_seconds_frac=0.1,
                opra_notional_abs_tolerance=1.0,
                opra_notional_rel_tolerance=1e-9,
                strict_quality=True,
                fail_on_warning=True,
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((root / "quality_check" / "silver_quality_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["warnings"], [])
            self.assertEqual(manifest["quality_file_count"], 12)

    def test_verify_fails_opra_total_consistency_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "silver"
            build_silver_tree(root, bad_opra_total=True)
            rc = verifier.verify(
                silver_root=root,
                output_dir=root / "quality_check",
                symbols=SYMBOLS,
                expected_dates=DATES,
                expected_rows_per_grid=3,
                min_equs_state_coverage=0.95,
                min_equs_valid_spread_coverage=0.95,
                max_equs_locked_crossed_frac=0.02,
                min_opra_active_seconds_frac=0.1,
                opra_notional_abs_tolerance=1.0,
                opra_notional_rel_tolerance=1e-9,
                strict_quality=True,
                fail_on_warning=True,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((root / "quality_check" / "silver_quality_manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(any("total consistency failed" in error for error in manifest["errors"]))

    def test_verify_fails_large_opra_notional_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "silver"
            build_silver_tree(root, bad_opra_notional=True)
            rc = verifier.verify(
                silver_root=root,
                output_dir=root / "quality_check",
                symbols=SYMBOLS,
                expected_dates=DATES,
                expected_rows_per_grid=3,
                min_equs_state_coverage=0.95,
                min_equs_valid_spread_coverage=0.95,
                max_equs_locked_crossed_frac=0.02,
                min_opra_active_seconds_frac=0.1,
                opra_notional_abs_tolerance=1.0,
                opra_notional_rel_tolerance=1e-9,
                strict_quality=True,
                fail_on_warning=True,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((root / "quality_check" / "silver_quality_manifest.json").read_text(encoding="utf-8"))
            self.assertTrue(any("total consistency failed" in error for error in manifest["errors"]))


if __name__ == "__main__":
    unittest.main()

