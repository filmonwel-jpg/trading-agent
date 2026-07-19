import importlib.util
import json
import pickle
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "export_downstream_setup_filter_onnx.py"
FILTER_SCRIPT = REPO_ROOT / "scripts" / "train_downstream_setup_filter.py"

spec = importlib.util.spec_from_file_location("export_downstream_setup_filter_onnx", SCRIPT_PATH)
exporter = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = exporter
spec.loader.exec_module(exporter)


class TestExportDownstreamSetupFilterOnnx(unittest.TestCase):
    def test_load_filter_bundle_rejects_missing_required_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "broken.pkl"
            with path.open("wb") as handle:
                pickle.dump({"feature_columns": ["feature_edge"], "selected_threshold": 0.5}, handle)
            with self.assertRaisesRegex(ValueError, "missing required keys: model"):
                exporter.load_filter_bundle(path)

    def test_positive_class_index_prefers_label_one(self):
        class Model:
            classes_ = np.array([0, 1])

        self.assertEqual(exporter.positive_class_index(Model()), 1)

    @unittest.skipIf(importlib.util.find_spec("skl2onnx") is None, "skl2onnx not installed")
    def test_export_writes_onnx_feature_schemas_and_route_manifest(self):
        from sklearn.ensemble import RandomForestClassifier

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bundle_dir = tmp_path / "bundle"
            out_dir = tmp_path / "onnx"
            bundle_dir.mkdir()
            rows_csv = tmp_path / "rows.csv"
            source_manifest = tmp_path / "source_manifest.json"
            policy_summary = tmp_path / "policy_summary.json"

            rows = pd.DataFrame(
                [
                    {"arm_id": "L1", "Symbol": "TEST", "SetupSide": "long", "SetupEpochSec": 1000, "feature_edge": 0.90},
                    {"arm_id": "L2", "Symbol": "TEST", "SetupSide": "long", "SetupEpochSec": 1030, "feature_edge": 0.10},
                    {"arm_id": "S1", "Symbol": "TEST", "SetupSide": "short", "SetupEpochSec": 1060, "feature_edge": 0.85},
                    {"arm_id": "S2", "Symbol": "TEST", "SetupSide": "short", "SetupEpochSec": 1090, "feature_edge": 0.15},
                ]
            )
            rows.to_csv(rows_csv, index=False)
            x = rows[["feature_edge"]].to_numpy(dtype=np.float32)
            y = np.array([1, 0, 1, 0], dtype=np.int8)
            model = RandomForestClassifier(n_estimators=8, max_depth=2, random_state=7)
            model.fit(x, y)
            for side, threshold in [("long", 0.50), ("short", 0.55)]:
                with (bundle_dir / f"{side}_downstream_setup_filter.pkl").open("wb") as handle:
                    pickle.dump(
                        {
                            "model": model,
                            "feature_columns": ["feature_edge"],
                            "selected_threshold": threshold,
                            "schema_version": "downstream_setup_filter_research_v1",
                        },
                        handle,
                    )
            source_manifest.write_text(json.dumps({"schema_version": "downstream_setup_filter_research_v1"}), encoding="utf-8")
            policy_summary.write_text(
                json.dumps(
                    {
                        "side_filter_threshold_selected": {"long": 0.50, "short": 0.55},
                        "best_policy": {"filter_threshold_label": "selected", "micro_threshold": 0.20, "confirms": 2},
                    }
                ),
                encoding="utf-8",
            )

            rc = exporter.main(
                [
                    "--filter-bundle-dir",
                    str(bundle_dir),
                    "--output-dir",
                    str(out_dir),
                    "--filter-script",
                    str(FILTER_SCRIPT),
                    "--training-rows-csv",
                    str(rows_csv),
                    "--source-manifest-json",
                    str(source_manifest),
                    "--policy-summary-json",
                    str(policy_summary),
                ]
            )
            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "long_downstream_setup_filter.onnx").is_file())
            self.assertTrue((out_dir / "short_downstream_setup_filter.onnx").is_file())
            manifest = json.loads((out_dir / "downstream_setup_filter_route_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["promotion_status"], "NO-GO")
            self.assertEqual([route["route_name"] for route in manifest["routes"]], ["longDownstreamSetupFilterAi", "shortDownstreamSetupFilterAi"])
            self.assertEqual(manifest["routes"][0]["onnx_model_filename"], "long_downstream_setup_filter.onnx")
            self.assertEqual(manifest["routes"][0]["feature_schema_filename"], "long_downstream_setup_filter_feature_schema.json")
            self.assertEqual(manifest["routes"][0]["feature_count"], 1)
            self.assertAlmostEqual(manifest["routes"][1]["selected_threshold"], 0.55)
            self.assertIn(
                manifest["routes"][0]["validation"]["status"],
                {"passed_onnxruntime", "passed_onnx_reference", "skipped_missing_onnx_runtime"},
            )
            schema = json.loads((out_dir / "long_downstream_setup_filter_feature_schema.json").read_text(encoding="utf-8"))
            self.assertEqual(schema["input"]["feature_columns"], ["feature_edge"])
            self.assertEqual(schema["outputs"]["positive_class_index"], 1)
            artifact_manifest = json.loads((out_dir / "downstream_setup_filter_onnx_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact_manifest["offline_policy"]["best_policy"]["micro_threshold"], 0.20)


if __name__ == "__main__":
    unittest.main()


