#!/usr/bin/env python3
import argparse
import shutil
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent
MODEL_EXPORTS_DIR = ROOT / "model_exports"
RUNTIME_MODELS_DIR = ROOT / "runtime" / "models"

EXPECTED_MODEL_FILES = [
    "long_entry.onnx",
    "short_entry.onnx",
    "long_exit.onnx",
    "short_exit.onnx",
    "regime_classifier.onnx",
    "choppy_long_entry.onnx",
    "choppy_short_entry.onnx",
    "choppy_long_exit.onnx",
    "choppy_short_exit.onnx",
    "trend_long_entry.onnx",
    "trend_short_entry.onnx",
    "trend_long_exit.onnx",
    "trend_short_exit.onnx",
    "volatile_long_entry.onnx",
    "volatile_short_entry.onnx",
    "volatile_long_exit.onnx",
    "volatile_short_exit.onnx",
    "open30_long_entry.onnx",
    "open30_short_entry.onnx",
    "open30_long_exit.onnx",
    "open30_short_exit.onnx",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Promote an exported ONNX bundle into runtime/models/<SYMBOL> for live trading."
    )
    parser.add_argument(
        "--symbol",
        nargs="+",
        required=True,
        help="One or more symbols to promote to, for example: --symbol TSLA NVDA AMD",
    )

    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--source-dir",
        type=str,
        help="Absolute or repo-relative path to the ONNX export bundle directory.",
    )
    source_group.add_argument(
        "--run-tag",
        type=str,
        help="Export run tag under model_exports/, for example: 20260320_181522",
    )
    source_group.add_argument(
        "--latest",
        action="store_true",
        help="Use the newest directory under model_exports/.",
    )

    parser.add_argument(
        "--target-root",
        type=str,
        default=str(RUNTIME_MODELS_DIR),
        help="Root directory that contains one subdirectory per symbol (default: runtime/models).",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow promotion when some expected ONNX files are missing from the source bundle.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing ONNX files in the destination symbol directory before copying the new bundle.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the actions that would be taken without copying files.",
    )
    return parser.parse_args()



def _resolve_source_dir(args: argparse.Namespace) -> Path:
    if args.source_dir:
        source = Path(args.source_dir).expanduser()
        if not source.is_absolute():
            source = (ROOT / source).resolve()
        return source

    if args.run_tag:
        return (MODEL_EXPORTS_DIR / args.run_tag).resolve()

    export_dirs = [p for p in MODEL_EXPORTS_DIR.iterdir() if p.is_dir()] if MODEL_EXPORTS_DIR.exists() else []
    if not export_dirs:
        raise FileNotFoundError(f"No export directories found under {MODEL_EXPORTS_DIR}")

    latest = max(export_dirs, key=lambda p: p.stat().st_mtime)
    return latest.resolve()



def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
    normalized = []
    seen = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    if not normalized:
        raise ValueError("At least one non-empty symbol is required.")
    return normalized



def _collect_source_models(source_dir: Path) -> tuple[list[Path], set[str], set[str]]:
    model_paths = sorted(p for p in source_dir.iterdir() if p.is_file() and p.suffix.lower() == ".onnx")
    found_names = {p.name for p in model_paths}
    expected_names = set(EXPECTED_MODEL_FILES)
    missing = expected_names - found_names
    extras = found_names - expected_names
    return model_paths, missing, extras



def _print_bundle_summary(source_dir: Path, model_paths: list[Path], missing: set[str], extras: set[str]) -> None:
    print(f"[PROMOTE] source bundle: {source_dir}")
    print(f"[PROMOTE] ONNX files found: {len(model_paths)}")
    if missing:
        print(f"[PROMOTE] missing expected models ({len(missing)}): {', '.join(sorted(missing))}")
    if extras:
        print(f"[PROMOTE] extra ONNX files ({len(extras)}): {', '.join(sorted(extras))}")



def _copy_bundle_to_symbol(
    symbol: str,
    source_dir: Path,
    model_paths: list[Path],
    target_root: Path,
    clean: bool,
    dry_run: bool,
) -> None:
    destination = target_root / symbol
    print(f"[PROMOTE] target symbol={symbol} destination={destination}")

    if dry_run:
        if clean and destination.exists():
            stale = sorted(destination.glob("*.onnx"))
            for path in stale:
                print(f"[DRY-RUN] remove {path}")
        for source_path in model_paths:
            print(f"[DRY-RUN] copy {source_path} -> {destination / source_path.name}")
        return

    destination.mkdir(parents=True, exist_ok=True)

    if clean:
        for stale_model in destination.glob("*.onnx"):
            stale_model.unlink()
            print(f"[PROMOTE] removed stale {stale_model}")

    for source_path in model_paths:
        target_path = destination / source_path.name
        shutil.copy2(source_path, target_path)
        print(f"[PROMOTE] copied {source_path.name} -> {target_path}")



def main() -> int:
    args = parse_args()

    try:
        symbols = _normalize_symbols(args.symbol)
        source_dir = _resolve_source_dir(args)
        target_root = Path(args.target_root).expanduser()
        if not target_root.is_absolute():
            target_root = (ROOT / target_root).resolve()

        if not source_dir.exists() or not source_dir.is_dir():
            raise FileNotFoundError(f"Source bundle directory does not exist: {source_dir}")

        model_paths, missing, extras = _collect_source_models(source_dir)
        if not model_paths:
            raise FileNotFoundError(f"No .onnx files found in source bundle: {source_dir}")

        _print_bundle_summary(source_dir, model_paths, missing, extras)

        if missing and not args.allow_partial:
            raise RuntimeError(
                "Source bundle is missing expected ONNX files. "
                "Re-run with --allow-partial only if you intentionally want a partial promotion."
            )

        print(f"[PROMOTE] symbols: {', '.join(symbols)}")
        print(f"[PROMOTE] target root: {target_root}")
        print(f"[PROMOTE] clean destination: {'yes' if args.clean else 'no'}")
        print(f"[PROMOTE] dry run: {'yes' if args.dry_run else 'no'}")

        for symbol in symbols:
            _copy_bundle_to_symbol(symbol, source_dir, model_paths, target_root, args.clean, args.dry_run)

        print("[PROMOTE] completed successfully")
        return 0
    except Exception as exc:
        print(f"[PROMOTE][ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

