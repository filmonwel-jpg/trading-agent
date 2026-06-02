#!/usr/bin/env python3
import argparse
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import databento as db
from databento.common.error import BentoHttpError

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYMBOLS_FILE = REPO_ROOT / "runtime" / "symbols_100.txt"
DEFAULT_DATASET = os.getenv("DATABENTO_OPTIONS_DATASET", os.getenv("DATABENTO_DATASET", "OPRA.PILLAR"))
DEFAULT_SCHEMA = os.getenv("DATABENTO_OPTIONS_SCHEMA", os.getenv("DATABENTO_SCHEMA", "ohlcv-1s"))
DEFAULT_STYPE_IN = os.getenv("DATABENTO_OPTIONS_STYPE_IN", os.getenv("DATABENTO_STYPE_IN", "parent"))
DEFAULT_MONTHS = 9
DEFAULT_DAYS_BACK = 2
OCC_SYMBOL_RE = re.compile(r"\d{6,8}[CP]\d{1,8}$")
RECOVERY_JOB_STATES = "received,queued,processing,finalizing,done,expired,purged"
RECOVERY_LOOKBACK_MINUTES = 30
RECOVERY_ATTEMPTS = 4
RECOVERY_SLEEP_SECONDS = 5.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Databento OPRA historical batch job for retraining/live-parity downloads."
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Databento options dataset to request.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Databento options schema to request.")
    parser.add_argument("--stype-in", default=DEFAULT_STYPE_IN, help="Databento symbol type, e.g. parent or raw_symbol.")
    parser.add_argument("--symbols-file", default=str(DEFAULT_SYMBOLS_FILE), help="Path to newline-delimited symbol file.")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="Approximate history window in 30-day months.")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK, help="How many full UTC days back to stop the request window.")
    parser.add_argument("--encoding", default="dbn", help="Databento batch encoding.")
    parser.add_argument("--dry-run", action="store_true", help="Print the request that would be submitted without calling Databento.")
    return parser.parse_args()


def _looks_like_occ_contract(symbol: str) -> bool:
    return bool(OCC_SYMBOL_RE.search(symbol.replace(" ", "")))


def _normalize_symbol(raw: str, stype_in: str) -> str:
    symbol = raw.strip().upper()
    if not symbol:
        return ""
    if stype_in == "parent":
        if _looks_like_occ_contract(symbol):
            raise ValueError(
                "When using --stype-in parent for OPRA, provide underlyings or ROOT.OPT symbols, "
                f"not OCC contracts like {symbol}."
            )
        return symbol if symbol.endswith(".OPT") else f"{symbol}.OPT"
    return symbol


def load_symbols(symbols_file_path: Path, stype_in: str) -> list[str]:
    with symbols_file_path.open("r", encoding="utf-8") as handle:
        symbols = [
            normalized
            for line in handle
            if line.strip() and not line.lstrip().startswith("#")
            for normalized in [_normalize_symbol(line, stype_in)]
            if normalized
        ]
    if not symbols:
        raise ValueError(f"No symbols loaded from {symbols_file_path}")
    return symbols


def resolve_window(months: int, days_back: int) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    end_date = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    start_date = end_date - timedelta(days=max(1, months) * 30)
    return start_date, end_date


def _parse_job_timestamp(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _job_matches_request(job: dict, request: dict) -> bool:
    if str(job.get("dataset", "")).upper() != str(request["dataset"]).upper():
        return False
    if str(job.get("schema", "")).lower() != str(request["schema"]).lower():
        return False
    if str(job.get("stype_in", "")).lower() != str(request["stype_in"]).lower():
        return False
    if str(job.get("encoding", "")).lower() != str(request["encoding"]).lower():
        return False

    job_start = _parse_job_timestamp(job.get("start"))
    job_end = _parse_job_timestamp(job.get("end"))
    if job_start != request["start"] or job_end != request["end"]:
        return False

    job_symbols = [token.strip().upper() for token in str(job.get("symbols", "")).split(",") if token.strip()]
    return job_symbols == request["symbols"]


def _recover_recent_job(client: db.Historical, request: dict, submit_started_at: datetime) -> dict | None:
    since = submit_started_at - timedelta(minutes=RECOVERY_LOOKBACK_MINUTES)
    best_job: dict | None = None
    best_ts: datetime | None = None
    for _ in range(RECOVERY_ATTEMPTS):
        jobs = client.batch.list_jobs(states=RECOVERY_JOB_STATES, since=since)
        for job in jobs:
            if not _job_matches_request(job, request):
                continue
            ts_received = _parse_job_timestamp(job.get("ts_received")) or _parse_job_timestamp(job.get("ts_queued"))
            if best_job is None or (ts_received is not None and (best_ts is None or ts_received > best_ts)):
                best_job = job
                best_ts = ts_received
        if best_job is not None:
            return best_job
        time.sleep(RECOVERY_SLEEP_SECONDS)
    return None


def _is_ambiguous_submit_error(exc: Exception) -> bool:
    if isinstance(exc, BentoHttpError) and exc.http_status in {502, 503, 504}:
        return True
    message = str(exc).lower()
    return any(marker in message for marker in ("timeout", "timed out", "connection reset", "bad gateway", "gateway timed out"))


def _submit_job_with_recovery(client: db.Historical, request: dict) -> tuple[dict, bool]:
    submit_started_at = datetime.now(timezone.utc)
    try:
        return client.batch.submit_job(**request), False
    except Exception as exc:
        if not _is_ambiguous_submit_error(exc):
            raise
        recovered = _recover_recent_job(client, request, submit_started_at)
        if recovered is not None:
            return recovered, True
        request_id = exc.request_id if isinstance(exc, BentoHttpError) else None
        details = f" request_id={request_id}" if request_id else ""
        raise SystemExit(
            "Databento submit returned an ambiguous gateway/network error and no matching recent job could be recovered. "
            f"Please check the Databento Download Center before retrying to avoid duplicates.{details}\nOriginal error: {exc}"
        ) from exc


def main() -> int:
    args = parse_args()
    symbols_file_path = Path(args.symbols_file).expanduser()
    if not symbols_file_path.is_absolute():
        symbols_file_path = (REPO_ROOT / symbols_file_path).resolve()

    api_key = os.getenv("DATABENTO_API_KEY", "").strip()
    try:
        symbols = load_symbols(symbols_file_path, args.stype_in)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    start_date, end_date = resolve_window(args.months, args.days_back)

    print(f"Loaded {len(symbols)} symbols from {symbols_file_path}. Example formatted symbol: {symbols[0]}")
    print(
        "Preparing OPRA batch job "
        f"dataset={args.dataset} schema={args.schema} stype_in={args.stype_in} "
        f"window={start_date.date()}->{end_date.date()} encoding={args.encoding}"
    )

    if args.dry_run:
        print("\nDry run only. No Databento job was submitted.")
        return 0

    if not api_key:
        raise SystemExit("Set DATABENTO_API_KEY before submitting a live Databento batch job.")

    client = db.Historical(api_key)
    request = {
        "dataset": args.dataset,
        "schema": args.schema,
        "stype_in": args.stype_in,
        "symbols": symbols,
        "start": start_date,
        "end": end_date,
        "encoding": args.encoding,
    }
    job, recovered = _submit_job_with_recovery(client, request)

    if recovered:
        print("\n⚠️ Databento submit timed out, but a matching recent batch job was recovered from the API.")

    print("\n✅ SUCCESS! OPRA batch job submitted.")
    print(f"Job ID: {job.get('id', 'Unknown')}")
    print(f"Status: {job.get('state', 'Unknown')}")
    print("\nYou can close the terminal after noting the job ID.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
