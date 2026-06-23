#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import gzip
import io
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

import build_recorded_events_from_training_bars as builder


class BuildRecordedEventsFromTrainingBarsTest(unittest.TestCase):
    def test_epoch_sources_support_aligned_and_raw_replay_modes(self) -> None:
        row = pd.Series({"Timestamp": "20250721 09:30:00 America/New_York", "BarEpochSec": 1_753_104_604})

        self.assertEqual(1_753_104_600, builder.timestamp_epoch_sec(row["Timestamp"]))
        self.assertEqual(1_753_104_600, builder.event_epoch(row, "timestamp", 5))
        self.assertEqual(1_753_104_600, builder.event_epoch(row, "floor-bar-epoch", 5))
        self.assertEqual(1_753_104_604, builder.event_epoch(row, "bar-epoch", 5))

    def test_writes_aligned_ndjson_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            csv_path = root / "TSLA_5s_training.csv"
            output_path = root / "example-recorded-events.ndjson.gz"
            pd.DataFrame(
                [
                    {
                        "Symbol": "TSLA",
                        "Timestamp": "20250721 09:30:00 America/New_York",
                        "Open": 335.00,
                        "High": 335.24,
                        "Low": 334.32,
                        "Close": 335.24,
                        "Volume": 7806,
                        "WAP": 334.85499,
                        "Bid": 334.588,
                        "Ask": 335.278,
                        "BidSize": 88,
                        "AskSize": 242,
                        "PutVol": 4123,
                        "CallVol": 3805,
                        "BarEpochSec": 1_753_104_604,
                        "YesterdayClose": 334.41,
                    }
                ]
            ).to_csv(csv_path, index=False)

            old_argv = sys.argv
            try:
                sys.argv = [
                    "build_recorded_events_from_training_bars.py",
                    "--input-csv",
                    str(csv_path),
                    "--symbol",
                    "TSLA",
                    "--date",
                    "2025-07-21",
                    "--output",
                    str(output_path),
                ]
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(0, builder.main())
            finally:
                sys.argv = old_argv

            with gzip.open(output_path, "rt", encoding="utf-8") as handle:
                events = [json.loads(line) for line in handle if line.strip()]
            equity_events = [event for event in events if event.get("event") == "equity_bar"]
            option_events = [event for event in events if event.get("event") == "option_bar"]

            self.assertEqual(1, len(equity_events))
            self.assertEqual(2, len(option_events))
            self.assertEqual(1_753_104_600, equity_events[0]["barEpochSec"])
            self.assertEqual(1_753_104_604, equity_events[0]["sourceBarEpochSec"])
            self.assertEqual(1_753_104_600_000_000_000, equity_events[0]["tsEventNs"])
            self.assertTrue(all(event["barEpochSec"] == 1_753_104_600 for event in option_events))

            manifest = json.loads((root / "example-recorded-events.manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("timestamp", manifest["epoch_source"])
            self.assertEqual(5, manifest["bar_seconds"])
            self.assertEqual(1, manifest["misaligned_source_epoch_rows"])
            self.assertEqual(1_753_104_600, manifest["first_bar_epoch_sec"])
            self.assertEqual(1_753_104_604, manifest["first_source_bar_epoch_sec"])


if __name__ == "__main__":
    unittest.main()
