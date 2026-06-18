#!/usr/bin/env python3
"""QA-check Databento Phase 1 silver normalizer outputs.

This verifier is intentionally separate from model training. It validates that
the six-source silver artifacts are structurally complete and internally
consistent before any enriched 30s feature build consumes them.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from databento_silver_utils import non_empty_path_arg, normalize_date_arg, split_symbols, utc_now, write_csv_rows, write_json


EXPECTED_DIRS = {
    "definitions": {
        "manifest": "manifest.json",
        "summary": "definition_silver_summary.csv",
        "schema_version": "databento_definition_silver_v1",
        "source_files_per_date": 2,
    },
    "equs_mbp1_1s": {
        "manifest": "manifest.json",
        "summary": "equs_mbp1_silver_summary.csv",
        "schema_version": "equs_mbp1_silver_1s_v1",
        "source_files_per_date": 1,
    },
    "opra_tcbbo_1s": {
        "manifest": "manifest.json",
        "summary": "opra_tcbbo_silver_summary.csv",
        "schema_version": "opra_tcbbo_silver_1s_v1",
        "source_files_per_date": 1,
    },
}

DEFINITION_COLUMNS = {
    "date",
    "source_label",
    "dataset",
    "schema",
    "instrument_id",
    "symbol",
    "raw_symbol",
    "asset_class",
    "underlying",
    "option_right",
    "expiration",
    "strike",
    "multiplier",
    "ts_event_utc",
}

EQUS_COLUMNS = {
    "date",
    "symbol",
    "Timestamp",
    "BarEpochSec",
    "Bid",
    "Ask",
    "BidSize",
    "AskSize",
    "Mid",
    "Microprice",
    "SpreadBps",
    "L1Imbalance",
    "QuoteUpdateCount1s",
    "QuoteCoverage1s",
    "QuoteStateValid1s",
    "ValidSpread1s",
    "LockedCrossed1s",
    "QuoteAgeMs",
}

OPRA_COLUMNS = {
    "date",
    "underlying",
    "Timestamp",
    "BarEpochSec",
    "CallOptionTradeCount1s",
    "PutOptionTradeCount1s",
    "CallOptionContractVolume1s",
    "PutOptionContractVolume1s",
    "CallOptionPremiumNotional1s",
    "PutOptionPremiumNotional1s",
    "CallOptionQuoteContextCount1s",
    "PutOptionQuoteContextCount1s",
    "CallOptionActiveContracts1s",
    "PutOptionActiveContracts1s",
    "TotalOptionContractVolume1s",
    "TotalOptionPremiumNotional1s",
    "TotalOptionTradeCount1s",
    "TotalOptionQuoteContextCount1s",
    "CallMinusPutVolume1s",
    "OptionVolumeImbalance1s",
    "PutCallVolumeRatio1s",
}


@dataclass
class FileQualityRow:
    artifact: str
    date: str
    key: str
    path: str
    rows: int
    expected_rows: int
    required_columns_ok: bool
    row_count_ok: bool
    key_ok: bool
    monotonic_epoch_ok: bool
    duplicate_epoch_count: int
    metric_1_name: str = ""
    metric_1_value: float | str = ""
    metric_2_name: str = ""
    metric_2_value: float | str = ""
    metric_3_name: str = ""
    metric_3_value: float | str = ""
    warning: str = ""
    error: str = ""


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _float(raw: object, default: float = 0.0) -> float:
    try:
        value = float(str(raw or "").strip())
        return value if math.isfinite(value) else default
    except ValueError:
        return default


def _int(raw: object, default: int = 0) -> int:
    try:
        return int(float(str(raw or "").strip()))
    except ValueError:
        return default


def _frac(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return round(float(pd.to_numeric(series, errors="coerce").fillna(0.0).mean()), 6)


def _all_nonnegative(frame: pd.DataFrame, columns: list[str]) -> bool:
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.dropna().lt(-1e-9).any():
            return False
    return True


def _max_abs_diff(left: pd.Series, right: pd.Series) -> float:
    diff = (pd.to_numeric(left, errors="coerce").fillna(0.0) - pd.to_numeric(right, errors="coerce").fillna(0.0)).abs()
    return round(float(diff.max()), 9) if len(diff) else 0.0


def _append_issue(target: list[str], message: str) -> None:
    if message and message not in target:
        target.append(message)


def validate_manifests(
    silver_root: Path,
    symbols: list[str],
    expected_dates: list[str],
    errors: list[str],
    warnings: list[str],
) -> dict[str, dict[str, Any]]:
    manifests: dict[str, dict[str, Any]] = {}
    symbol_count = len(symbols)
    for artifact, spec in EXPECTED_DIRS.items():
        path = silver_root / artifact / str(spec["manifest"])
        if not path.exists():
            _append_issue(errors, f"missing manifest for {artifact}: {path}")
            continue
        manifest = read_json(path)
        manifests[artifact] = manifest
        if manifest.get("schema_version") != spec["schema_version"]:
            _append_issue(errors, f"{artifact} schema_version={manifest.get('schema_version')} expected={spec['schema_version']}")
        if manifest.get("errors"):
            _append_issue(errors, f"{artifact} manifest errors: {manifest.get('errors')}")
        dates = [str(date) for date in manifest.get("selected_dates", [])]
        if expected_dates and dates != expected_dates:
            _append_issue(errors, f"{artifact} selected_dates mismatch: {dates} expected={expected_dates}")
        expected_selected = len(expected_dates or dates) * int(spec["source_files_per_date"])
        if _int(manifest.get("selected_file_count")) != expected_selected:
            _append_issue(errors, f"{artifact} selected_file_count={manifest.get('selected_file_count')} expected={expected_selected}")
        expected_outputs = expected_selected if artifact == "definitions" else len(expected_dates or dates) * symbol_count
        if _int(manifest.get("output_file_count")) != expected_outputs:
            _append_issue(errors, f"{artifact} output_file_count={manifest.get('output_file_count')} expected={expected_outputs}")
        if _int(manifest.get("output_rows")) <= 0:
            _append_issue(errors, f"{artifact} manifest output_rows is not positive")
        if not expected_dates:
            _append_issue(warnings, f"{artifact} was used to infer expected dates; pass --expected-date for a stricter check")
    return manifests


def validate_summary_files(silver_root: Path, errors: list[str]) -> dict[str, list[dict[str, str]]]:
    summaries: dict[str, list[dict[str, str]]] = {}
    for artifact, spec in EXPECTED_DIRS.items():
        path = silver_root / artifact / str(spec["summary"])
        if not path.exists():
            _append_issue(errors, f"missing summary for {artifact}: {path}")
            continue
        rows = read_csv_rows(path)
        summaries[artifact] = rows
        for row in rows:
            output_path = Path(str(row.get("output_path", "")).strip())
            if not output_path.exists():
                _append_issue(errors, f"missing silver output listed by {artifact}: {output_path}")
    return summaries


def validate_definition_file(
    row: dict[str, str],
    symbols: set[str],
    warnings: list[str],
    errors: list[str],
) -> FileQualityRow:
    path = Path(str(row.get("output_path", "")).strip())
    date = str(row.get("date", "")).strip()
    source_label = str(row.get("source_label", "")).strip()
    if not path.exists():
        return FileQualityRow("definitions", date, source_label, str(path), 0, 0, False, False, False, False, 0, error="missing file")
    frame = pd.read_csv(path)
    missing = sorted(DEFINITION_COLUMNS.difference(frame.columns))
    if missing:
        message = f"definitions {path} missing columns: {missing}"
        _append_issue(errors, message)
        return FileQualityRow("definitions", date, source_label, str(path), len(frame), 0, False, True, False, True, 0, error=message)

    underlying = set(frame["underlying"].astype(str).str.upper().replace("", np.nan).dropna().unique())
    missing_symbols = sorted(symbols.difference(underlying))
    if missing_symbols:
        _append_issue(errors, f"definitions {source_label} date={date} missing underlyings: {missing_symbols}")
    option_rows = frame[frame["asset_class"].astype(str).str.lower().eq("option")].copy()
    option_bad = 0
    if not option_rows.empty:
        option_bad = int(
            (~option_rows["option_right"].astype(str).str.upper().isin(["C", "P"])
             | option_rows["expiration"].astype(str).str.len().eq(0)
             | pd.to_numeric(option_rows["strike"], errors="coerce").le(0.0)
             | pd.to_numeric(option_rows["multiplier"], errors="coerce").le(0.0)).sum()
        )
        if option_bad:
            _append_issue(errors, f"definitions {source_label} date={date} has {option_bad} malformed option rows")
    elif "opra_definition" in source_label:
        _append_issue(errors, f"OPRA definitions date={date} has zero option rows after symbol filtering")

    return FileQualityRow(
        artifact="definitions",
        date=date,
        key=source_label,
        path=str(path),
        rows=int(len(frame)),
        expected_rows=_int(row.get("output_rows")),
        required_columns_ok=True,
        row_count_ok=int(len(frame)) == _int(row.get("output_rows")),
        key_ok=not missing_symbols,
        monotonic_epoch_ok=True,
        duplicate_epoch_count=0,
        metric_1_name="underlying_count",
        metric_1_value=len(underlying),
        metric_2_name="option_rows",
        metric_2_value=int(len(option_rows)),
        metric_3_name="bad_option_rows",
        metric_3_value=option_bad,
    )


def _grid_common_checks(
    frame: pd.DataFrame,
    *,
    artifact: str,
    path: Path,
    date: str,
    key_column: str,
    key: str,
    required_columns: set[str],
    expected_rows: int,
    errors: list[str],
) -> tuple[bool, bool, bool, bool, int, str]:
    missing = sorted(required_columns.difference(frame.columns))
    required_ok = not missing
    row_count_ok = len(frame) == expected_rows
    key_ok = required_ok and frame[key_column].astype(str).str.upper().nunique(dropna=True) == 1 and frame[key_column].astype(str).str.upper().iloc[0] == key
    date_ok = required_ok and frame["date"].astype(str).eq(date).all()
    epoch = pd.to_numeric(frame.get("BarEpochSec", pd.Series(dtype=float)), errors="coerce")
    monotonic_ok = bool(epoch.is_monotonic_increasing and epoch.notna().all())
    duplicate_epoch_count = int(epoch.duplicated().sum()) if len(epoch) else 0
    messages: list[str] = []
    if missing:
        messages.append(f"missing columns={missing}")
    if not row_count_ok:
        messages.append(f"rows={len(frame)} expected={expected_rows}")
    if not key_ok:
        messages.append(f"{key_column} mismatch expected={key}")
    if not date_ok:
        messages.append(f"date column mismatch expected={date}")
    if not monotonic_ok:
        messages.append("BarEpochSec not monotonic/non-null")
    if duplicate_epoch_count:
        messages.append(f"duplicate BarEpochSec count={duplicate_epoch_count}")
    error = "; ".join(messages)
    if error:
        _append_issue(errors, f"{artifact} {path}: {error}")
    return required_ok, row_count_ok, key_ok and date_ok, monotonic_ok, duplicate_epoch_count, error


def validate_equs_file(
    row: dict[str, str],
    *,
    expected_rows: int,
    min_state_coverage: float,
    min_valid_spread_coverage: float,
    max_locked_crossed_frac: float,
    strict_quality: bool,
    warnings: list[str],
    errors: list[str],
) -> FileQualityRow:
    path = Path(str(row.get("output_path", "")).strip())
    date = str(row.get("date", "")).strip()
    symbol = str(row.get("symbol", "")).strip().upper()
    if not path.exists():
        _append_issue(errors, f"missing EQUS silver file: {path}")
        return FileQualityRow("equs_mbp1_1s", date, symbol, str(path), 0, expected_rows, False, False, False, False, 0, error="missing file")
    frame = pd.read_csv(path)
    required_ok, row_count_ok, key_ok, monotonic_ok, dup_count, error = _grid_common_checks(
        frame,
        artifact="equs_mbp1_1s",
        path=path,
        date=date,
        key_column="symbol",
        key=symbol,
        required_columns=EQUS_COLUMNS,
        expected_rows=expected_rows,
        errors=errors,
    )
    if not required_ok:
        return FileQualityRow("equs_mbp1_1s", date, symbol, str(path), len(frame), expected_rows, False, row_count_ok, key_ok, monotonic_ok, dup_count, error=error)

    if not _all_nonnegative(frame, ["BidSize", "AskSize", "BidCount", "AskCount", "QuoteUpdateCount1s", "QuoteAgeMs"]):
        _append_issue(errors, f"EQUS {symbol} {date} has negative size/count/age values")
    state_frac = _frac(frame["QuoteStateValid1s"])
    valid_spread_frac = _frac(frame["ValidSpread1s"])
    locked_frac = _frac(frame["LockedCrossed1s"])
    quote_update_frac = round(float(pd.to_numeric(frame["QuoteUpdateCount1s"], errors="coerce").fillna(0.0).gt(0.0).mean()), 6)
    quality_messages: list[str] = []
    if state_frac < min_state_coverage:
        quality_messages.append(f"QuoteStateValid1s mean {state_frac} < {min_state_coverage}")
    if valid_spread_frac < min_valid_spread_coverage:
        quality_messages.append(f"ValidSpread1s mean {valid_spread_frac} < {min_valid_spread_coverage}")
    if locked_frac > max_locked_crossed_frac:
        quality_messages.append(f"LockedCrossed1s mean {locked_frac} > {max_locked_crossed_frac}")
    if quality_messages:
        message = f"EQUS {symbol} {date}: " + "; ".join(quality_messages)
        if strict_quality:
            _append_issue(errors, message)
        else:
            _append_issue(warnings, message)
    return FileQualityRow(
        artifact="equs_mbp1_1s",
        date=date,
        key=symbol,
        path=str(path),
        rows=int(len(frame)),
        expected_rows=expected_rows,
        required_columns_ok=required_ok,
        row_count_ok=row_count_ok,
        key_ok=key_ok,
        monotonic_epoch_ok=monotonic_ok,
        duplicate_epoch_count=dup_count,
        metric_1_name="quote_state_valid_frac",
        metric_1_value=state_frac,
        metric_2_name="locked_crossed_frac",
        metric_2_value=locked_frac,
        metric_3_name="quote_update_frac",
        metric_3_value=quote_update_frac,
        warning="; ".join(quality_messages),
        error=error,
    )


def validate_opra_file(
    row: dict[str, str],
    *,
    expected_rows: int,
    min_active_seconds_frac: float,
    strict_quality: bool,
    warnings: list[str],
    errors: list[str],
) -> FileQualityRow:
    path = Path(str(row.get("output_path", "")).strip())
    date = str(row.get("date", "")).strip()
    underlying = str(row.get("underlying", "")).strip().upper()
    if not path.exists():
        _append_issue(errors, f"missing OPRA silver file: {path}")
        return FileQualityRow("opra_tcbbo_1s", date, underlying, str(path), 0, expected_rows, False, False, False, False, 0, error="missing file")
    frame = pd.read_csv(path)
    required_ok, row_count_ok, key_ok, monotonic_ok, dup_count, error = _grid_common_checks(
        frame,
        artifact="opra_tcbbo_1s",
        path=path,
        date=date,
        key_column="underlying",
        key=underlying,
        required_columns=OPRA_COLUMNS,
        expected_rows=expected_rows,
        errors=errors,
    )
    if not required_ok:
        return FileQualityRow("opra_tcbbo_1s", date, underlying, str(path), len(frame), expected_rows, False, row_count_ok, key_ok, monotonic_ok, dup_count, error=error)

    flow_columns = [
        column
        for column in frame.columns
        if (column.endswith("Count1s") or column.endswith("Volume1s") or column.endswith("Notional1s") or column.endswith("Contracts1s"))
        and "Minus" not in column
        and "Imbalance" not in column
    ]
    if not _all_nonnegative(frame, flow_columns):
        _append_issue(errors, f"OPRA {underlying} {date} has negative flow/count/notional values")
    checks = {
        "volume": _max_abs_diff(frame["TotalOptionContractVolume1s"], frame["CallOptionContractVolume1s"] + frame["PutOptionContractVolume1s"]),
        "notional": _max_abs_diff(frame["TotalOptionPremiumNotional1s"], frame["CallOptionPremiumNotional1s"] + frame["PutOptionPremiumNotional1s"]),
        "trade_count": _max_abs_diff(frame["TotalOptionTradeCount1s"], frame["CallOptionTradeCount1s"] + frame["PutOptionTradeCount1s"]),
        "quote_context": _max_abs_diff(frame["TotalOptionQuoteContextCount1s"], frame["CallOptionQuoteContextCount1s"] + frame["PutOptionQuoteContextCount1s"]),
    }
    bad_checks = {name: value for name, value in checks.items() if value > 1e-6}
    if bad_checks:
        _append_issue(errors, f"OPRA {underlying} {date} total consistency failed: {bad_checks}")
    imbalance = pd.to_numeric(frame["OptionVolumeImbalance1s"], errors="coerce").dropna()
    if (imbalance.abs() > 1.000001).any():
        _append_issue(errors, f"OPRA {underlying} {date} OptionVolumeImbalance1s outside [-1, 1]")
    total_volume = float(pd.to_numeric(frame["TotalOptionContractVolume1s"], errors="coerce").fillna(0.0).sum())
    total_trades = float(pd.to_numeric(frame["TotalOptionTradeCount1s"], errors="coerce").fillna(0.0).sum())
    active_seconds_frac = round(
        float(
            (pd.to_numeric(frame["CallOptionActiveContracts1s"], errors="coerce").fillna(0.0)
             + pd.to_numeric(frame["PutOptionActiveContracts1s"], errors="coerce").fillna(0.0)).gt(0.0).mean()
        ),
        6,
    )
    quality_messages: list[str] = []
    if total_volume <= 0.0 or total_trades <= 0.0:
        quality_messages.append("zero OPRA trade volume/count")
    if active_seconds_frac < min_active_seconds_frac:
        quality_messages.append(f"active_seconds_frac {active_seconds_frac} < {min_active_seconds_frac}")
    if quality_messages:
        message = f"OPRA {underlying} {date}: " + "; ".join(quality_messages)
        if strict_quality:
            _append_issue(errors, message)
        else:
            _append_issue(warnings, message)
    return FileQualityRow(
        artifact="opra_tcbbo_1s",
        date=date,
        key=underlying,
        path=str(path),
        rows=int(len(frame)),
        expected_rows=expected_rows,
        required_columns_ok=required_ok,
        row_count_ok=row_count_ok,
        key_ok=key_ok,
        monotonic_epoch_ok=monotonic_ok,
        duplicate_epoch_count=dup_count,
        metric_1_name="total_contract_volume",
        metric_1_value=round(total_volume, 3),
        metric_2_name="total_trade_count",
        metric_2_value=round(total_trades, 3),
        metric_3_name="active_seconds_frac",
        metric_3_value=active_seconds_frac,
        warning="; ".join(quality_messages),
        error=error,
    )


def verify(
    *,
    silver_root: Path,
    output_dir: Path,
    symbols: list[str],
    expected_dates: list[str],
    expected_rows_per_grid: int,
    min_equs_state_coverage: float,
    min_equs_valid_spread_coverage: float,
    max_equs_locked_crossed_frac: float,
    min_opra_active_seconds_frac: float,
    strict_quality: bool,
    fail_on_warning: bool,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    warnings: list[str] = []
    if not silver_root.exists():
        _append_issue(errors, f"silver root not found: {silver_root}")
    manifests = validate_manifests(silver_root, symbols, expected_dates, errors, warnings) if silver_root.exists() else {}
    if not expected_dates and manifests:
        first_manifest = next(iter(manifests.values()))
        expected_dates = [str(date) for date in first_manifest.get("selected_dates", [])]
    summaries = validate_summary_files(silver_root, errors) if silver_root.exists() else {}

    quality_rows: list[FileQualityRow] = []
    symbol_set = set(symbols)
    for row in summaries.get("definitions", []):
        quality_rows.append(validate_definition_file(row, symbol_set, warnings, errors))
    for row in summaries.get("equs_mbp1_1s", []):
        quality_rows.append(
            validate_equs_file(
                row,
                expected_rows=expected_rows_per_grid,
                min_state_coverage=min_equs_state_coverage,
                min_valid_spread_coverage=min_equs_valid_spread_coverage,
                max_locked_crossed_frac=max_equs_locked_crossed_frac,
                strict_quality=strict_quality,
                warnings=warnings,
                errors=errors,
            )
        )
    for row in summaries.get("opra_tcbbo_1s", []):
        quality_rows.append(
            validate_opra_file(
                row,
                expected_rows=expected_rows_per_grid,
                min_active_seconds_frac=min_opra_active_seconds_frac,
                strict_quality=strict_quality,
                warnings=warnings,
                errors=errors,
            )
        )

    manifest = {
        "generated_at_utc": utc_now(),
        "silver_root": str(silver_root),
        "output_dir": str(output_dir),
        "symbols": symbols,
        "expected_dates": expected_dates,
        "expected_rows_per_grid": expected_rows_per_grid,
        "strict_quality": strict_quality,
        "fail_on_warning": fail_on_warning,
        "manifest_artifacts": {artifact: manifests.get(artifact, {}) for artifact in EXPECTED_DIRS},
        "summary_row_counts": {artifact: len(rows) for artifact, rows in summaries.items()},
        "quality_file_count": len(quality_rows),
        "errors": errors,
        "warnings": warnings,
    }
    write_json(output_dir / "silver_quality_manifest.json", manifest)
    write_csv_rows(output_dir / "silver_file_quality.csv", [asdict(row) for row in quality_rows], list(FileQualityRow.__dataclass_fields__.keys()))

    status = "FAIL" if errors or (fail_on_warning and warnings) else "PASS"
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"wrote {output_dir / 'silver_quality_manifest.json'}")
    print(f"wrote {output_dir / 'silver_file_quality.csv'}")
    print(f"SILVER_QUALITY_CHECK={status}")
    return 2 if status == "FAIL" else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", required=True, type=non_empty_path_arg, help="Root containing definitions/equs_mbp1_1s/opra_tcbbo_1s silver outputs.")
    parser.add_argument("--output-dir", type=non_empty_path_arg, help="QA output directory. Defaults to <silver-root>/quality_check.")
    parser.add_argument("--symbols", default="TSLA,TQQQ,NVDA,SPY,QQQ", help="Comma-separated expected symbol/underlying set.")
    parser.add_argument("--expected-date", action="append", type=normalize_date_arg, help="Expected selected date. Repeatable. Defaults to manifest dates.")
    parser.add_argument("--expected-rows-per-grid", type=int, default=23400, help="Expected rows per full regular-session 1-second file.")
    parser.add_argument("--min-equs-state-coverage", type=float, default=0.95, help="Soft EQUS QuoteStateValid1s coverage threshold.")
    parser.add_argument("--min-equs-valid-spread-coverage", type=float, default=0.95, help="Soft EQUS ValidSpread1s coverage threshold.")
    parser.add_argument("--max-equs-locked-crossed-frac", type=float, default=0.02, help="Soft EQUS locked/crossed fraction threshold.")
    parser.add_argument("--min-opra-active-seconds-frac", type=float, default=0.001, help="Soft OPRA active-contract seconds threshold.")
    parser.add_argument("--strict-quality", action="store_true", help="Treat soft quality-threshold breaches as errors.")
    parser.add_argument("--fail-on-warning", action="store_true", help="Return non-zero if any warnings are emitted.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    silver_root = args.silver_root.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve() if args.output_dir else silver_root / "quality_check"
    symbols = split_symbols(args.symbols)
    if not symbols:
        raise SystemExit("--symbols must not be empty")
    if args.expected_rows_per_grid <= 0:
        raise SystemExit("--expected-rows-per-grid must be positive")
    return verify(
        silver_root=silver_root,
        output_dir=output_dir,
        symbols=symbols,
        expected_dates=list(args.expected_date or []),
        expected_rows_per_grid=int(args.expected_rows_per_grid),
        min_equs_state_coverage=float(args.min_equs_state_coverage),
        min_equs_valid_spread_coverage=float(args.min_equs_valid_spread_coverage),
        max_equs_locked_crossed_frac=float(args.max_equs_locked_crossed_frac),
        min_opra_active_seconds_frac=float(args.min_opra_active_seconds_frac),
        strict_quality=bool(args.strict_quality),
        fail_on_warning=bool(args.fail_on_warning),
    )


if __name__ == "__main__":
    raise SystemExit(main())


