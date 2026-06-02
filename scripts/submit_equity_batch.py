#!/usr/bin/env python3
import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import databento as db

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYMBOLS_FILE = REPO_ROOT / "runtime" / "symbols_100.txt"
DEFAULT_DATASET = os.getenv("DATABENTO_EQUITY_DATASET", os.getenv("DATABENTO_DATASET", "EQUS.MINI"))
DEFAULT_SCHEMA = os.getenv("DATABENTO_EQUITY_SCHEMA", "tbbo")
DEFAULT_STYPE_IN = "raw_symbol"
DEFAULT_MONTHS = 9
DEFAULT_DAYS_BACK = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Databento historical equity batch job for retraining/live-parity downloads."
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Databento equity dataset to request.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Databento schema to request.")
    parser.add_argument("--stype-in", default=DEFAULT_STYPE_IN, help="Databento symbol type.")
    parser.add_argument("--symbols-file", default=str(DEFAULT_SYMBOLS_FILE), help="Path to newline-delimited symbol file.")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="Approximate history window in 30-day months.")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK, help="How many full UTC days back to stop the request window.")
    parser.add_argument("--encoding", default="dbn", help="Databento batch encoding.")
    parser.add_argument("--dry-run", action="store_true", help="Print the request that would be submitted without calling Databento.")
    return parser.parse_args()


def load_symbols(symbols_file_path: Path) -> list[str]:
    with symbols_file_path.open("r", encoding="utf-8") as handle:
        symbols = [line.strip().upper() for line in handle if line.strip() and not line.lstrip().startswith("#")]
    if not symbols:
        raise ValueError(f"No symbols loaded from {symbols_file_path}")
    return symbols


def resolve_window(months: int, days_back: int) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    end_date = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    start_date = end_date - timedelta(days=max(1, months) * 30)
    return start_date, end_date


def main() -> int:
    args = parse_args()
    symbols_file_path = Path(args.symbols_file).expanduser()
    if not symbols_file_path.is_absolute():
        symbols_file_path = (REPO_ROOT / symbols_file_path).resolve()

    api_key = os.getenv("DATABENTO_API_KEY", "").strip()
    symbols = load_symbols(symbols_file_path)
    start_date, end_date = resolve_window(args.months, args.days_back)

    print(f"Loaded {len(symbols)} symbols from {symbols_file_path}. Example symbol: {symbols[0]}")
    print(
        "Preparing US equity batch job "
        f"dataset={args.dataset} schema={args.schema} stype_in={args.stype_in} "
        f"window={start_date.date()}->{end_date.date()} encoding={args.encoding}"
    )

    if args.dry_run:
        print("\nDry run only. No Databento job was submitted.")
        return 0

    if not api_key:
        raise SystemExit("Set DATABENTO_API_KEY before submitting a live Databento batch job.")

    client = db.Historical(api_key)
    job = client.batch.submit_job(
        dataset=args.dataset,
        schema=args.schema,
        stype_in=args.stype_in,
        symbols=symbols,
        start=start_date,
        end=end_date,
        encoding=args.encoding,
    )

    print("\n✅ SUCCESS! US equity batch job submitted.")
    print(f"Job ID: {job.get('id', 'Unknown')}")
    print(f"Status: {job.get('state', 'Unknown')}")
    print("\nYou can close the terminal after noting the job ID.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
