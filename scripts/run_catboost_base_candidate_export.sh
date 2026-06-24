#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INPUT_CSV="${INPUT_CSV:-$ROOT/runtime/research_runs/input_cache/broader_213d_six_source_enriched_30s_20260619_065347/combined/combined_30s.csv}"
RUN_TS="$(date -u +%Y%m%d_%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT/runtime/research_runs/catboost_base_30f_onnx_candidate_$RUN_TS}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DRY_RUN="${DRY_RUN:-1}"
NO_ONNX="${NO_ONNX:-0}"

truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

export INPUT_CSV OUTPUT_DIR NO_ONNX

cat <<SUMMARY
[CATBOOST_BASE] root=$ROOT
[CATBOOST_BASE] input_csv=$INPUT_CSV
[CATBOOST_BASE] output_dir=$OUTPUT_DIR
[CATBOOST_BASE] dry_run=$DRY_RUN
[CATBOOST_BASE] no_onnx=$NO_ONNX
[CATBOOST_BASE] feature_contract=base_30f_live_compatible
[CATBOOST_BASE] mbp1_silver_features=disabled
[CATBOOST_BASE] production_promotion=NO-GO
SUMMARY

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "[CATBOOST_BASE][ERROR] input CSV does not exist: $INPUT_CSV" >&2
  exit 1
fi

if pgrep -fl 'train_30s_models.py' >/dev/null 2>&1; then
  echo "[CATBOOST_BASE][ERROR] another trainer appears to be running; refusing to start." >&2
  pgrep -fl 'train_30s_models.py' >&2 || true
  exit 1
fi

export MODEL_FAMILY=catboost
export REGIME_MODEL_FAMILY=catboost
export REQUIRE_MODEL_FAMILY=1
export TRAIN_LEGACY_30S_EXIT_MODELS=0
export USE_NEWS_BAR_FEATURES=0
export USE_META_PRODUCER_FEATURES=0
export USE_DATABENTO_SILVER_FEATURES=0
export USE_REGIME_PROB_FEATURES=0
export UPDATE_CANONICAL_MODEL_ALIASES=0
export RESEARCH_SHORT_THRESHOLD_FLOOR_ENABLED=0
export AUTO_BUILD_30S_IF_MISSING=0
export PYTHONUNBUFFERED=1
unset DATABENTO_SILVER_FEATURE_SET

"$PYTHON_BIN" - <<'PY'
import os
import sys
from pathlib import Path

input_csv = Path(os.environ["INPUT_CSV"])
print(f"[CATBOOST_BASE][PREFLIGHT] input_size_bytes={input_csv.stat().st_size}")

import train_30s_models as t

checks = {
    "catboost_available": t.CatBoostClassifier is not None,
    "model_family_catboost": t._normalize_model_family(t.MODEL_FAMILY) == "catboost",
    "regime_model_family_catboost": t._normalize_model_family(t.REGIME_MODEL_FAMILY) == "catboost",
    "news_disabled": not t.USE_NEWS_BAR_FEATURES,
    "meta_disabled": not t.USE_META_PRODUCER_FEATURES,
    "silver_disabled": not t.USE_DATABENTO_SILVER_FEATURES,
    "regime_prob_disabled": not t.USE_REGIME_PROB_FEATURES,
}
for name, ok in checks.items():
    print(f"[CATBOOST_BASE][PREFLIGHT] {name}={ok}")

if not all(checks.values()):
    missing = ", ".join(name for name, ok in checks.items() if not ok)
    print(f"[CATBOOST_BASE][ERROR] preflight failed: {missing}", file=sys.stderr)
    if not checks["catboost_available"]:
        print(f"[CATBOOST_BASE][ERROR] catboost import_error={t.CATBOOST_IMPORT_ERROR}", file=sys.stderr)
    sys.exit(2)

print("[CATBOOST_BASE][PREFLIGHT] expected_main_feature_count=30")
print("[CATBOOST_BASE][PREFLIGHT] expected_regime_feature_count=24")
PY

if truthy "$DRY_RUN"; then
  echo "[CATBOOST_BASE] dry run complete; set DRY_RUN=0 to train/export the isolated CatBoost base candidate."
  exit 0
fi

mkdir -p "$OUTPUT_DIR"

cmd=("$PYTHON_BIN" train_30s_models.py --input-csv "$INPUT_CSV" --output-dir "$OUTPUT_DIR")
if truthy "$NO_ONNX"; then
  cmd+=(--no-onnx)
fi

printf '[CATBOOST_BASE] running:'
printf ' %q' "${cmd[@]}"
printf '\n'
"${cmd[@]}" 2>&1 | tee "$OUTPUT_DIR/train_30s_catboost_base.console.log"
cp "$OUTPUT_DIR/train_30s_catboost_base.console.log" "$OUTPUT_DIR/train_30s_catboost_base.log"

"$PYTHON_BIN" - <<'PY'
import json
import os
import sys
from pathlib import Path

out = Path(os.environ["OUTPUT_DIR"])
manifest_path = out / "setup_manifest.json"
if not manifest_path.exists():
    print(f"[CATBOOST_BASE][ERROR] missing manifest: {manifest_path}", file=sys.stderr)
    sys.exit(3)
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
blocks = manifest.get("feature_blocks") or {}
errors = []
if manifest.get("model_family") != "catboost":
    errors.append(f"model_family={manifest.get('model_family')!r}")
if manifest.get("feature_count") != 30:
    errors.append(f"feature_count={manifest.get('feature_count')!r}; expected 30")
if blocks.get("news_bar_features_enabled"):
    errors.append("news_bar_features_enabled=True")
if blocks.get("meta_producer_features_enabled"):
    errors.append("meta_producer_features_enabled=True")
if blocks.get("databento_silver_features_enabled"):
    errors.append("databento_silver_features_enabled=True")
if blocks.get("regime_probability_features_enabled"):
    errors.append("regime_probability_features_enabled=True")
if int(blocks.get("databento_silver_feature_count") or 0) != 0:
    errors.append(f"databento_silver_feature_count={blocks.get('databento_silver_feature_count')!r}")

if os.environ.get("NO_ONNX", "0").strip().lower() not in {"1", "true", "yes", "on"}:
    expected_models = {
        "long_entry.onnx": 30,
        "short_entry.onnx": 30,
        "regime_classifier.onnx": 24,
    }
    try:
        import onnx  # type: ignore
    except Exception as exc:
        onnx = None
        print(f"[CATBOOST_BASE][POSTCHECK] onnx import unavailable; shape check skipped: {type(exc).__name__}: {exc}")
    for filename, expected_count in expected_models.items():
        path = out / filename
        if not path.exists():
            errors.append(f"missing {filename}")
            continue
        if onnx is None:
            continue
        model = onnx.load(str(path))
        dims = model.graph.input[0].type.tensor_type.shape.dim if model.graph.input else []
        actual = int(getattr(dims[1], "dim_value", 0)) if len(dims) > 1 else 0
        print(f"[CATBOOST_BASE][POSTCHECK] {filename}_features={actual}")
        if actual and actual != expected_count:
            errors.append(f"{filename} features={actual}; expected {expected_count}")

if errors:
    for error in errors:
        print(f"[CATBOOST_BASE][ERROR] {error}", file=sys.stderr)
    sys.exit(4)

print(f"[CATBOOST_BASE][POSTCHECK] manifest={manifest_path}")
print("CATBOOST_BASE_CANDIDATE=PASS")
print("production_promotion=NO-GO")
PY
