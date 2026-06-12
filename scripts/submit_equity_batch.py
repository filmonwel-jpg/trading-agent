#!/usr/bin/env python3
import argparse
import os
import re
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
    parser.add_argument("--start", help="Exact request start, e.g. 2025-07-21 or 2025-07-21T00:00:00Z.")
    parser.add_argument("--end", help="Exact request end as an exclusive boundary, e.g. 2026-05-23 or 2026-05-23T00:00:00Z.")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="Approximate history window in 30-day months.")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK, help="How many full UTC days back to stop the request window.")
    parser.add_argument("--encoding", default="dbn", help="Databento batch encoding.")
    api_key_group = parser.add_mutually_exclusive_group()
    api_key_group.add_argument("--api-key", help="Databento API key. Prefer DATABENTO_API_KEY or --api-key-file to avoid exposing secrets in shell history/process lists.")
    api_key_group.add_argument("--api-key-file", help="Path to a file containing the Databento API key.")
    parser.add_argument("--dry-run", action="store_true", help="Print the request that would be submitted without calling Databento.")
    return parser.parse_args()


def load_symbols(symbols_file_path: Path) -> list[str]:
    with symbols_file_path.open("r", encoding="utf-8") as handle:
        symbols = [line.strip().upper() for line in handle if line.strip() and not line.lstrip().startswith("#")]
    if not symbols:
        raise ValueError(f"No symbols loaded from {symbols_file_path}")
    return symbols


def _parse_datetime_utc(raw: str, field_name: str) -> datetime:
    value = raw.strip()
    if not value:
        raise ValueError(f"--{field_name} cannot be empty")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        parsed = datetime.strptime(value, "%Y-%m-%d")
    else:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"Invalid --{field_name} value {raw!r}. Use YYYY-MM-DD or an ISO timestamp."
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def resolve_window(months: int, days_back: int, start: str | None = None, end: str | None = None) -> tuple[datetime, datetime]:
    if (start is None) != (end is None):
        raise ValueError("Provide both --start and --end, or neither.")
    if start is not None and end is not None:
        start_date = _parse_datetime_utc(start, "start")
        end_date = _parse_datetime_utc(end, "end")
        if start_date >= end_date:
            raise ValueError(f"--start must be before --end, got {start_date.isoformat()} >= {end_date.isoformat()}")
        return start_date, end_date

    now = datetime.now(timezone.utc)
    end_date = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    start_date = end_date - timedelta(days=max(1, months) * 30)
    return start_date, end_date


def resolve_api_key(api_key_arg: str | None, api_key_file: str | None) -> str:
    if api_key_arg is not None:
        value = api_key_arg.strip()
        if not value:
            raise ValueError("--api-key cannot be empty")
        if re.search(r"\s", value):
            raise ValueError("--api-key contains whitespace. Paste the exact Databento key token only, or use --api-key-file.")
        return value

    if api_key_file is not None:
        key_path = Path(api_key_file).expanduser()
        if not key_path.is_absolute():
            key_path = (REPO_ROOT / key_path).resolve()
        try:
            value = key_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(f"Unable to read --api-key-file {key_path}: {exc}") from exc
        if not value:
            raise ValueError(f"--api-key-file {key_path} is empty")
        if re.search(r"\s", value):
            raise ValueError(f"--api-key-file {key_path} contains whitespace. Store only the exact Databento key token.")
        return value

    return os.getenv("DATABENTO_API_KEY", "").strip()


def _is_authentication_error(exc: Exception) -> bool:
    message = str(exc).lower()
    status = getattr(exc, "http_status", None) or getattr(exc, "status", None)
    return status == 401 or "auth_authentication_failed" in message or ("401" in message and "auth" in message)


def _authentication_failure_message() -> str:
    return (
        "Databento authentication failed with 401. The API key was not accepted.\n"
        "Check that you copied an active key from the Databento portal, pasted only the key token "
        "without labels/spaces/quotes, and are using the correct account. Prefer storing it in a file "
        "and passing --api-key-file ~/.databento_api_key."
    )


def main() -> int:
    args = parse_args()
    symbols_file_path = Path(args.symbols_file).expanduser()
    if not symbols_file_path.is_absolute():
        symbols_file_path = (REPO_ROOT / symbols_file_path).resolve()

    symbols = load_symbols(symbols_file_path)
    try:
        start_date, end_date = resolve_window(args.months, args.days_back, args.start, args.end)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Loaded {len(symbols)} symbols from {symbols_file_path}. Example symbol: {symbols[0]}")
    print(
        "Preparing US equity batch job "
        f"dataset={args.dataset} schema={args.schema} stype_in={args.stype_in} "
        f"window={start_date.date()}->{end_date.date()} encoding={args.encoding}"
    )

    if args.dry_run:
        print("\nDry run only. No Databento job was submitted.")
        return 0

    try:
        api_key = resolve_api_key(args.api_key, args.api_key_file)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if not api_key:
        raise SystemExit("Set DATABENTO_API_KEY, pass --api-key-file, or pass --api-key before submitting a live Databento batch job.")

    client = db.Historical(api_key)
    try:
        job = client.batch.submit_job(
            dataset=args.dataset,
            schema=args.schema,
            stype_in=args.stype_in,
            symbols=symbols,
            start=start_date,
            end=end_date,
            encoding=args.encoding,
        )
    except Exception as exc:
        if _is_authentication_error(exc):
            raise SystemExit(_authentication_failure_message()) from exc
        raise

    print("\n✅ SUCCESS! US equity batch job submitted.")
    print(f"Job ID: {job.get('id', 'Unknown')}")
    print(f"Status: {job.get('state', 'Unknown')}")
    print("\nYou can close the terminal after noting the job ID.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
