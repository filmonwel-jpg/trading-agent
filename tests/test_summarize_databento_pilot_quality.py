import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "summarize_databento_pilot_quality.py"
SPEC = importlib.util.spec_from_file_location("summarize_databento_pilot_quality", SCRIPT_PATH)
summarizer = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = summarizer
SPEC.loader.exec_module(summarizer)


FIELDNAMES = [
    "cadence",
    "scope",
    "symbol",
    "row_count",
    "date_count",
    "symbol_count",
    "parent_child_flag_different_rows",
    "data_quality_flag_values",
    "child_flag_union_values",
]


def write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def row(cadence: str, scope: str, symbol: str, diff_rows: int, parent: str, child: str) -> dict[str, object]:
    return {
        "cadence": cadence,
        "scope": scope,
        "symbol": symbol,
        "row_count": 100,
        "date_count": 2,
        "symbol_count": 1 if scope == "per_symbol" else 2,
        "parent_child_flag_different_rows": diff_rows,
        "data_quality_flag_values": parent,
        "child_flag_union_values": child,
    }


class SummarizeDatabentoPilotQualityTest(unittest.TestCase):
    def test_passes_when_parent_child_quality_is_decoupled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_csv = root / "pilot_build_file_summary.csv"
            output_dir = root / "out"
            rows = [
                row("1s", "per_symbol", "TSLA", 0, "none:50|no_trade|no_quote|synthetic_ohlc:50", "none:50|no_trade|no_quote|synthetic_ohlc:50"),
                row("5s", "per_symbol", "TSLA", 100, "partial_synthetic_ohlc:90|none:10", "no_trade|no_quote|synthetic_ohlc:100"),
                row("30s", "per_symbol", "TSLA", 100, "partial_synthetic_ohlc:95|none:5", "no_trade|no_quote|synthetic_ohlc:100"),
            ]
            write_summary(input_csv, rows)

            rc = summarizer.summarize(input_csv, output_dir, expected_rows=3)

            self.assertEqual(rc, 0)
            manifest = json.loads((output_dir / "pilot_quality_sanity.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["errors"], [])
            self.assertEqual(manifest["parent_child_decoupling_evidence_count"], 2)

    def test_fails_when_parent_child_quality_still_matches_on_5s(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_csv = root / "pilot_build_file_summary.csv"
            output_dir = root / "out"
            rows = [
                row("5s", "per_symbol", "TSLA", 0, "no_trade|no_quote|synthetic_ohlc:100", "no_trade|no_quote|synthetic_ohlc:100"),
            ]
            write_summary(input_csv, rows)

            rc = summarizer.summarize(input_csv, output_dir, min_parent_child_diff_frac_5s_30s=0.5, expected_rows=1)

            self.assertEqual(rc, 2)
            manifest = json.loads((output_dir / "pilot_quality_sanity.json").read_text(encoding="utf-8"))
            self.assertTrue(any("parent/child quality diff frac" in error for error in manifest["errors"]))


if __name__ == "__main__":
    unittest.main()
