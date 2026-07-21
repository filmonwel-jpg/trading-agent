import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "inject_downstream_setup_features_into_ndjson.py"

spec = importlib.util.spec_from_file_location("inject_downstream_setup_features_into_ndjson", SCRIPT_PATH)
injector = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = injector
spec.loader.exec_module(injector)


class TestInjectDownstreamSetupFeaturesIntoNdjson(unittest.TestCase):
    def test_injects_manifest_feature_columns_into_matching_equity_bar(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            events = tmp_path / "events.ndjson"
            sidecar = tmp_path / "sidecar.csv"
            output = tmp_path / "events.enriched.ndjson"
            summary = tmp_path / "summary.json"
            schema = tmp_path / "long_downstream_setup_filter_feature_schema.json"
            manifest = tmp_path / "downstream_setup_filter_route_manifest.json"

            events.write_text(
                "\n".join(
                    [
                        json.dumps({"event": "status", "message": "start"}),
                        json.dumps({"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1000, "close": 10.5}),
                        json.dumps({"event": "equity_bar", "symbol": "TEST", "barEpochSec": 1030, "close": 10.7}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with sidecar.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "Symbol",
                        "SetupEpochSec",
                        "SetupSide",
                        "arm_id",
                        "SetupProb",
                        "ImbalanceStd5s",
                        "Symbol_TEST",
                        "Label_FutureLeak",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "Symbol": "TEST",
                        "SetupEpochSec": "1000",
                        "SetupSide": "long",
                        "arm_id": "TEST-long-1000",
                        "SetupProb": "0.77",
                        "ImbalanceStd5s": "123.5",
                        "Symbol_TEST": "1",
                        "Label_FutureLeak": "1",
                    }
                )
            schema.write_text(
                json.dumps(
                    {
                        "schema_version": "downstream_setup_filter_onnx_research_v1",
                        "input": {"feature_columns": ["SetupProb", "ImbalanceStd5s", "Symbol_TEST"]},
                    }
                ),
                encoding="utf-8",
            )
            manifest.write_text(
                json.dumps(
                    {
                        "routes": [
                            {
                                "side": "long",
                                "feature_schema_filename": schema.name,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            rc = injector.main(
                [
                    "--input-events",
                    str(events),
                    "--sidecar-csv",
                    str(sidecar),
                    "--manifest",
                    str(manifest),
                    "--output-events",
                    str(output),
                    "--summary-json",
                    str(summary),
                    "--snapshot-source",
                    "unit_test_sidecar",
                ]
            )

            self.assertEqual(rc, 0)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            enriched = rows[1]
            self.assertEqual(enriched["FeatureSnapshotEpochSec"], 1000)
            self.assertEqual(enriched["FeatureSnapshotSchemaVersion"], "downstream_setup_filter_onnx_research_v1")
            self.assertEqual(enriched["FeatureSnapshotSource"], "unit_test_sidecar")
            self.assertEqual(enriched["FeatureSnapshotSide"], "long")
            self.assertEqual(enriched["FeatureSnapshotArmId"], "TEST-long-1000")
            self.assertEqual(enriched["enriched_features"], {"SetupProb": 0.77, "ImbalanceStd5s": 123.5, "Symbol_TEST": 1.0})
            self.assertNotIn("enriched_features", rows[2])
            report = json.loads(summary.read_text(encoding="utf-8"))
            self.assertEqual(report["injection"]["snapshot_rows_expected"], 1)
            self.assertEqual(report["injection"]["snapshot_events_injected"], 1)
            self.assertEqual(report["injection"]["snapshot_rows_unmatched"], 0)


if __name__ == "__main__":
    unittest.main()
