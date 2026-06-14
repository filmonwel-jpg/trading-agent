#!/usr/bin/env python3
"""Summarize fixed-quality pilot build quality flags.

Input is the small ``pilot_build_file_summary.csv`` produced by
``scripts/verify_databento_pilot_build.py``. This script does not read the large
training CSVs; it only checks that the post-build verifier's quality summaries
show the expected C2 behavior: parent 5s/30s flags should be derived quality
flags, not blind copies of child-second flag unions.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class QualitySummaryRow:
    cadence: str
    scope: str
    symbol: str
    row_count: int
    date_count: int
    symbol_count: int
    parent_child_flag_different_rows: int
    parent_child_diff_frac: float
    parent_has_no_quote: bool
    child_has_no_quote: bool
    parent_has_stale_quote: bool
    parent_has_synthetic: bool
    parent_has_partial_synthetic: bool
    parent_has_locked_crossed: bool
    parent_quality_flags: str
    child_flag_union: str


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _int(raw: object, default: int = 0) -> int:
    try:
        return int(float(str(raw or "").strip()))
    except ValueError:
        return default


def _contains_flag(raw: str, flag: str) -> bool:
    return flag in str(raw or "")


def read_summary(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def build_quality_rows(rows: list[dict[str, str]]) -> list[QualitySummaryRow]:
    out: list[QualitySummaryRow] = []
    for row in rows:
        row_count = _int(row.get("row_count"))
        different = _int(row.get("parent_child_flag_different_rows"))
        parent_flags = row.get("data_quality_flag_values", "")
        child_flags = row.get("child_flag_union_values", "")
        diff_frac = round(different / row_count, 6) if row_count > 0 else 0.0
        out.append(
            QualitySummaryRow(
                cadence=row.get("cadence", ""),
                scope=row.get("scope", ""),
                symbol=row.get("symbol", ""),
                row_count=row_count,
                date_count=_int(row.get("date_count")),
                symbol_count=_int(row.get("symbol_count")),
                parent_child_flag_different_rows=different,
                parent_child_diff_frac=diff_frac,
                parent_has_no_quote=_contains_flag(parent_flags, "no_quote"),
                child_has_no_quote=_contains_flag(child_flags, "no_quote"),
                parent_has_stale_quote=_contains_flag(parent_flags, "stale_quote"),
                parent_has_synthetic=_contains_flag(parent_flags, "synthetic_ohlc"),
                parent_has_partial_synthetic=_contains_flag(parent_flags, "partial_synthetic_ohlc"),
                parent_has_locked_crossed=_contains_flag(parent_flags, "locked_crossed"),
                parent_quality_flags=parent_flags,
                child_flag_union=child_flags,
            )
        )
    return out


def write_csv(path: Path, rows: list[QualitySummaryRow]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(QualitySummaryRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def summarize(
    input_csv: Path,
    output_dir: Path,
    min_parent_child_diff_frac_5s_30s: float = 0.50,
    expected_rows: int = 18,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_rows = read_summary(input_csv)
    quality_rows = build_quality_rows(raw_rows)

    errors: list[str] = []
    warnings: list[str] = []

    if expected_rows and len(quality_rows) != expected_rows:
        errors.append(f"summary row count is {len(quality_rows)}, expected {expected_rows}")

    parent_child_evidence_rows = []
    for row in quality_rows:
        if row.row_count <= 0:
            errors.append(f"{row.scope} {row.cadence} {row.symbol} has no rows")
        if row.cadence in {"5s", "30s"}:
            if row.parent_child_diff_frac < min_parent_child_diff_frac_5s_30s:
                errors.append(
                    f"{row.scope} {row.cadence} {row.symbol} parent/child quality diff frac "
                    f"{row.parent_child_diff_frac:.3f} < {min_parent_child_diff_frac_5s_30s:.3f}"
                )
            if row.child_has_no_quote and not row.parent_has_no_quote:
                parent_child_evidence_rows.append(f"{row.scope}:{row.cadence}:{row.symbol}")
            if row.parent_has_no_quote:
                warnings.append(f"{row.scope} {row.cadence} {row.symbol} still has parent no_quote; inspect quote-state coverage")
        if row.cadence == "1s" and row.parent_child_flag_different_rows != 0:
            warnings.append(f"{row.scope} 1s {row.symbol} has parent/child differences; inspect raw-second quality handling")

    expected_evidence_count = sum(1 for row in quality_rows if row.cadence in {"5s", "30s"})
    if len(parent_child_evidence_rows) < expected_evidence_count:
        warnings.append(
            f"parent/child no_quote decoupling evidence rows={len(parent_child_evidence_rows)}, "
            f"expected up to {expected_evidence_count}"
        )

    manifest = {
        "generated_at_utc": utc_now(),
        "input_csv": str(input_csv),
        "output_dir": str(output_dir),
        "row_count": len(quality_rows),
        "errors": errors,
        "warnings": warnings,
        "parent_child_decoupling_evidence_count": len(parent_child_evidence_rows),
        "parent_child_decoupling_evidence_rows": parent_child_evidence_rows,
        "min_parent_child_diff_frac_5s_30s": min_parent_child_diff_frac_5s_30s,
        "cadence_counts": {
            cadence: sum(1 for row in quality_rows if row.cadence == cadence)
            for cadence in sorted({row.cadence for row in quality_rows})
        },
        "max_parent_child_diff_frac_by_cadence": {
            cadence: max((row.parent_child_diff_frac for row in quality_rows if row.cadence == cadence), default=0.0)
            for cadence in sorted({row.cadence for row in quality_rows})
        },
        "min_parent_child_diff_frac_by_cadence": {
            cadence: min((row.parent_child_diff_frac for row in quality_rows if row.cadence == cadence), default=0.0)
            for cadence in sorted({row.cadence for row in quality_rows})
        },
    }

    (output_dir / "pilot_quality_sanity.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(output_dir / "pilot_quality_sanity_rows.csv", quality_rows)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"wrote {output_dir / 'pilot_quality_sanity.json'}")
    print(f"wrote {output_dir / 'pilot_quality_sanity_rows.csv'}")
    print("PILOT_QUALITY_SANITY=FAIL" if errors else "PILOT_QUALITY_SANITY=PASS")
    return 2 if errors else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", required=True, type=Path, help="pilot_build_file_summary.csv from verify_databento_pilot_build.py")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for quality sanity outputs")
    parser.add_argument("--min-parent-child-diff-frac-5s-30s", type=float, default=0.50)
    parser.add_argument("--expected-rows", type=int, default=18)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return summarize(
        input_csv=args.input_csv.expanduser().resolve(),
        output_dir=args.output_dir.expanduser().resolve(),
        min_parent_child_diff_frac_5s_30s=args.min_parent_child_diff_frac_5s_30s,
        expected_rows=args.expected_rows,
    )


if __name__ == "__main__":
    raise SystemExit(main())
