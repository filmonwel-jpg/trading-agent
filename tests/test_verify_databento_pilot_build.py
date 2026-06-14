import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_databento_pilot_build.py"
SPEC = importlib.util.spec_from_file_location("verify_databento_pilot_build", SCRIPT_PATH)
verifier = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


REQUIRED_COLUMNS = verifier.REQUIRED_BAR_COLUMNS


def write_rows(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_bar_row(symbol: str, date: str, idx: int, child_flag: str = "none") -> dict[str, object]:
    row = {col: 1 for col in REQUIRED_COLUMNS}
    row.update(
        {
            "Symbol": symbol,
            "Timestamp": f"{date} 09:30:{idx:02d} America/New_York",
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.5,
            "Volume": 10,
            "DataQualityFlags": "none",
            "ChildDataQualityFlagUnion": child_flag,
            "TradeCoverage": 1.0,
            "QuoteUpdateCoverage": 1.0,
            "QuoteStateCoverage": 1.0,
            "SyntheticCoverage": 0.0,
            "QuoteAgeMsMean": 100.0,
            "QuoteAgeMsMax": 200.0,
            "ValidSpreadCoverage": 1.0,
            "QualityScore": 1.0,
        }
    )
    return row


class VerifyDatabentoPilotBuildTest(unittest.TestCase):
    def setUp(self):
        self._original_rows_per_day = dict(verifier.ROWS_PER_DAY_BY_CADENCE)
        verifier.ROWS_PER_DAY_BY_CADENCE.update({"1s": 2, "5s": 1, "30s": 1})

    def tearDown(self):
        verifier.ROWS_PER_DAY_BY_CADENCE.clear()
        verifier.ROWS_PER_DAY_BY_CADENCE.update(self._original_rows_per_day)

    def make_build(self, root: Path) -> tuple[Path, Path, list[str], list[str]]:
        build_root = root / "data_lake_v2" / "model_training_sets" / "pilot_10d_fixed_quality_test"
        output_dir = root / "data_lake_v2" / "raw_audits" / "pilot_build_check_test"
        symbols = ["TSLA", "QQQ"]
        dates = ["20260103", "20260104"]
        rows_per_cadence = {"1s": 2, "5s": 1, "30s": 1}

        for cadence, subdir in verifier.CADENCE_SUBDIRS.items():
            combined_rows = []
            for symbol in symbols:
                rows = []
                for date in dates:
                    for idx in range(rows_per_cadence[cadence]):
                        child_flag = "no_quote" if cadence != "1s" and idx == 0 else "none"
                        rows.append(make_bar_row(symbol, date, idx, child_flag=child_flag))
                write_rows(build_root / subdir / f"{symbol}_{cadence}_training.csv", rows, REQUIRED_COLUMNS)
                combined_rows.extend(rows)
            write_rows(build_root / "combined" / verifier.COMBINED_FILENAMES[cadence], combined_rows, REQUIRED_COLUMNS)

        report_rows = [
            {
                "Symbol": symbol,
                "days": "2",
                "rows": "2",
                "median_volume_30s": "10",
                "median_trade_count_30s": "1",
                "median_spread_bps": "1",
                "zero_volume_bar_frac": "0",
                "option_active_bar_frac": "1",
                "median_close": "100",
                "modeling_cohort": "liquid",
                "recommended_training_mode": "shared_liquidity_cluster",
            }
            for symbol in symbols
        ]
        write_rows(build_root / "reports" / "symbol_model_plan.csv", report_rows, verifier.REQUIRED_REPORT_COLUMNS)
        (build_root / "logs").mkdir(parents=True, exist_ok=True)
        return build_root, output_dir, symbols, dates

    def test_verify_passes_clean_build(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            build_root, output_dir, symbols, dates = self.make_build(Path(temp_dir))

            rc = verifier.verify(
                build_root=build_root,
                output_dir=output_dir,
                expected_symbols=symbols,
                expected_dates=dates,
                expected_days=2,
            )

            self.assertEqual(rc, 0)
            manifest = json.loads((output_dir / "pilot_build_check.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["assessment_report_rows"], 2)

    def test_verify_fails_missing_combined_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            build_root, output_dir, symbols, dates = self.make_build(Path(temp_dir))
            (build_root / "combined" / "combined_30s.csv").unlink()

            rc = verifier.verify(
                build_root=build_root,
                output_dir=output_dir,
                expected_symbols=symbols,
                expected_dates=dates,
                expected_days=2,
            )

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "pilot_build_check.json").read_text(encoding="utf-8"))
            self.assertTrue(any("missing combined 30s file" in error for error in manifest["errors"]))


if __name__ == "__main__":
    unittest.main()
