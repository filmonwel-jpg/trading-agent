#!/usr/bin/env python3
"""Backfill missed live trade log files into databento_live_process_logs.

This script scans the mirrored `runtime/*_live_trade_logs.txt` files, extracts only the
requested day from each file, reconstructs a reasonable `run_id` per process start, and
inserts the rows into PostgreSQL with the original log timestamps.

By default it targets yesterday and skips a file when matching rows already exist for the
same `source_file` within the extracted timestamp window.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import re
from typing import Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harvest_storage import load_postgres_settings
from stream_live_logs_to_db import LIVE_PROCESS_LOG_TABLE, live_process_log_index_names, live_process_log_table_name

TIMESTAMP_PREFIX_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))"
)
STARTUP_MARKER = "Starting TradingAgentApplication"
DEFAULT_SOURCE = "run_symbol.sh"
DEFAULT_FILE_GLOB = "runtime/*_live_trade_logs.txt"
WINDOW_SLACK = timedelta(minutes=5)
BANNER_PREFIXES = (
    "  .   ____",
    " /\\ / ___",
    "( ( )\\___",
    " \\/  ___)",
    "  '  ____",
    " =========",
    " :: Spring Boot ::",
)


@dataclass(frozen=True)
class BackfillRow:
    run_id: str
    symbol: str
    source: str
    source_file: str
    log_line: str
    log_ts: datetime


@dataclass(frozen=True)
class FileExtraction:
    path: Path
    symbol: str
    source_file: str
    rows: list[BackfillRow]

    @property
    def first_ts(self) -> datetime | None:
        return self.rows[0].log_ts if self.rows else None

    @property
    def last_ts(self) -> datetime | None:
        return self.rows[-1].log_ts if self.rows else None


class LiveProcessLogBackfillStore:
    def __init__(self, repo_root: Path) -> None:
        settings = load_postgres_settings(repo_root)
        self._url = str(settings.get("url", "") or "").strip()
        self._username = str(settings.get("username", "") or "").strip()
        self._password = settings.get("password", "")
        if not self._url:
            raise RuntimeError("Missing PostgreSQL datasource URL.")
        try:
            import psycopg  # type: ignore
        except ImportError as exc:  # pragma: no cover - runtime dependent
            raise RuntimeError("psycopg is required. Install it with: pip install 'psycopg[binary]>=3.2'") from exc
        self._psycopg = psycopg
        self._conn = self._psycopg.connect(
            self._url,
            user=self._username or None,
            password=self._password or None,
            autocommit=True,
            connect_timeout=5,
            application_name="live_process_log_backfill",
        )
        self._schema_ready: set[str] = set()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _ensure_schema(self, table_name: str) -> None:
        if table_name in self._schema_ready:
            return
        symbol_ts_index, run_id_index = live_process_log_index_names(table_name)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_file TEXT,
                    log_line TEXT NOT NULL,
                    log_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS {symbol_ts_index} ON {table_name} (symbol, log_ts DESC)"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS {run_id_index} ON {table_name} (run_id)"
            )
        self._schema_ready.add(table_name)

    def existing_window_status(self, source_file: str, symbol: str, start_ts: datetime, end_ts: datetime) -> tuple[int, datetime | None]:
        table_name = live_process_log_table_name(symbol)
        self._ensure_schema(table_name)
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*), MAX(log_ts)
                FROM {table_name}
                WHERE source_file = %s
                  AND symbol = %s
                  AND log_ts >= %s
                  AND log_ts < %s
                """,
                (source_file, symbol, start_ts, end_ts),
            )
            row = cursor.fetchone()
        return int(row[0] if row else 0), (row[1] if row else None)

    def insert_rows(self, rows: Sequence[BackfillRow], batch_size: int = 1000) -> int:
        if not rows:
            return 0
        table_name = live_process_log_table_name(rows[0].symbol)
        self._ensure_schema(table_name)
        inserted = 0
        with self._conn.cursor() as cursor:
            for offset in range(0, len(rows), batch_size):
                chunk = rows[offset : offset + batch_size]
                cursor.executemany(
                    f"""
                    INSERT INTO {table_name} (run_id, symbol, source, source_file, log_line, log_ts)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            row.run_id,
                            row.symbol,
                            row.source,
                            row.source_file,
                            row.log_line,
                            row.log_ts,
                        )
                        for row in chunk
                    ],
                )
                inserted += len(chunk)
        return inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missed live trade log files into databento_live_process_logs.")
    parser.add_argument("--day", default="", help="Target log day in the log file's own timestamp date, e.g. 2026-04-14. Defaults to yesterday.")
    parser.add_argument("--glob", default=DEFAULT_FILE_GLOB, help=f"Glob to scan for mirrored live log files. Default: {DEFAULT_FILE_GLOB}")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol filter.")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help=f"Source label stored in the table. Default: {DEFAULT_SOURCE}")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be imported without inserting.")
    parser.add_argument("--force", action="store_true", help="Import even if rows already exist for the file's extracted timestamp window.")
    parser.add_argument("--max-files", type=int, default=0, help="Optional limit for testing.")
    return parser.parse_args()


def default_target_day(now: datetime | None = None) -> date:
    reference = now or datetime.now().astimezone()
    return (reference - timedelta(days=1)).date()


def parse_target_day(raw_day: str) -> date:
    text = str(raw_day or "").strip()
    return date.fromisoformat(text) if text else default_target_day()


def normalize_symbols(raw: str) -> set[str]:
    out: set[str] = set()
    for token in str(raw or "").split(","):
        cleaned = token.strip().upper()
        if cleaned:
            out.add(cleaned)
    return out


def resolve_source_file(repo_root: Path, file_path: Path) -> str:
    try:
        relative = file_path.resolve().relative_to(repo_root.resolve())
    except ValueError:
        relative = file_path.resolve()
    return str(relative).replace("\\", "/")


def symbol_from_log_path(path: Path) -> str:
    stem = path.name
    suffix = "_live_trade_logs.txt"
    if not stem.lower().endswith(suffix):
        raise ValueError(f"Unsupported live log filename: {path.name}")
    return stem[: -len(suffix)].upper()


def parse_timestamp(raw_value: str) -> datetime:
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))


def is_banner_line(line: str) -> bool:
    if line.startswith(BANNER_PREFIXES):
        return True
    stripped = line.strip()
    return stripped in {"", "/", "\\"}


def build_run_id(symbol: str, dt: datetime, seen: set[str]) -> str:
    base = f"{symbol}_{dt.strftime('%Y%m%d_%H%M%S')}"
    if base not in seen:
        seen.add(base)
        return base
    suffix = 2
    while f"{base}_r{suffix}" in seen:
        suffix += 1
    run_id = f"{base}_r{suffix}"
    seen.add(run_id)
    return run_id


def extract_rows_for_day(
    repo_root: Path,
    file_path: Path,
    target_day: date,
    source: str,
) -> FileExtraction:
    symbol = symbol_from_log_path(file_path)
    source_file = resolve_source_file(repo_root, file_path)
    rows: list[BackfillRow] = []
    pending_for_next_timestamp: list[str] = []
    last_timestamp: datetime | None = None
    current_run_id: str | None = None
    seen_run_ids: set[str] = set()

    with file_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            match = TIMESTAMP_PREFIX_RE.match(line)
            if match:
                timestamp = parse_timestamp(match.group("ts"))
                starts_new_run = current_run_id is None or STARTUP_MARKER in line
                if starts_new_run:
                    current_run_id = build_run_id(symbol, timestamp, seen_run_ids)
                if pending_for_next_timestamp and timestamp.date() == target_day and current_run_id is not None:
                    for pending_line in pending_for_next_timestamp:
                        rows.append(
                            BackfillRow(
                                run_id=current_run_id,
                                symbol=symbol,
                                source=source,
                                source_file=source_file,
                                log_line=pending_line,
                                log_ts=timestamp.astimezone(timezone.utc),
                            )
                        )
                pending_for_next_timestamp.clear()
                last_timestamp = timestamp
                if timestamp.date() == target_day and current_run_id is not None:
                    rows.append(
                        BackfillRow(
                            run_id=current_run_id,
                            symbol=symbol,
                            source=source,
                            source_file=source_file,
                            log_line=line,
                            log_ts=timestamp.astimezone(timezone.utc),
                        )
                    )
                continue

            if is_banner_line(line) or last_timestamp is None:
                pending_for_next_timestamp.append(line)
                continue

            if last_timestamp.date() != target_day or current_run_id is None:
                continue

            rows.append(
                BackfillRow(
                    run_id=current_run_id,
                    symbol=symbol,
                    source=source,
                    source_file=source_file,
                    log_line=line,
                    log_ts=last_timestamp.astimezone(timezone.utc),
                )
            )

    return FileExtraction(path=file_path, symbol=symbol, source_file=source_file, rows=rows)


def discover_files(repo_root: Path, pattern: str, symbol_filter: set[str]) -> list[Path]:
    matches = sorted(repo_root.glob(pattern))
    filtered: list[Path] = []
    for path in matches:
        if not path.is_file():
            continue
        try:
            symbol = symbol_from_log_path(path)
        except ValueError:
            continue
        if symbol_filter and symbol not in symbol_filter:
            continue
        filtered.append(path)
    return filtered


def summarize_extraction(extraction: FileExtraction) -> str:
    if not extraction.rows:
        return f"{extraction.symbol}: no target-day rows"
    return (
        f"{extraction.symbol}: rows={len(extraction.rows)} "
        f"first={extraction.first_ts.isoformat() if extraction.first_ts else 'n/a'} "
        f"last={extraction.last_ts.isoformat() if extraction.last_ts else 'n/a'} file={extraction.source_file}"
    )


def main() -> int:
    args = parse_args()
    repo_root = REPO_ROOT
    target_day = parse_target_day(args.day)
    symbol_filter = normalize_symbols(args.symbols)
    files = discover_files(repo_root, args.glob, symbol_filter)
    if args.max_files > 0:
        files = files[: args.max_files]

    print(f"[BACKFILL] repo_root={repo_root}")
    print(f"[BACKFILL] day={target_day.isoformat()} dry_run={args.dry_run} force={args.force} files={len(files)}")
    if not files:
        print("[BACKFILL] no candidate files matched")
        return 0

    store = None if args.dry_run else LiveProcessLogBackfillStore(repo_root)
    scanned_files = 0
    skipped_existing_files = 0
    skipped_empty_files = 0
    imported_files = 0
    imported_rows = 0
    candidate_rows = 0

    try:
        for file_path in files:
            scanned_files += 1
            extraction = extract_rows_for_day(repo_root, file_path, target_day, args.source)
            if not extraction.rows:
                skipped_empty_files += 1
                print(f"[BACKFILL][SKIP] {summarize_extraction(extraction)}")
                continue

            candidate_rows += len(extraction.rows)
            window_start = extraction.first_ts - WINDOW_SLACK if extraction.first_ts else datetime.combine(target_day, time.min, tzinfo=timezone.utc)
            window_end = extraction.last_ts + WINDOW_SLACK if extraction.last_ts else datetime.combine(target_day + timedelta(days=1), time.min, tzinfo=timezone.utc)
            existing_rows = 0
            existing_max_ts = None
            if store is not None:
                existing_rows, existing_max_ts = store.existing_window_status(extraction.source_file, extraction.symbol, window_start, window_end)
            rows_to_insert = extraction.rows
            if existing_rows >= len(extraction.rows) and not args.force:
                skipped_existing_files += 1
                print(f"[BACKFILL][SKIP-EXISTING] {summarize_extraction(extraction)} existingRows={existing_rows}")
                continue
            if 0 < existing_rows < len(extraction.rows) and not args.force:
                rows_to_insert = extraction.rows[existing_rows:]
                print(
                    f"[BACKFILL][RESUME] {summarize_extraction(extraction)} "
                    f"existingRows={existing_rows} remainingRows={len(rows_to_insert)} existingMaxTs={existing_max_ts.isoformat() if existing_max_ts else 'n/a'}"
                )

            if args.dry_run:
                print(f"[BACKFILL][DRY-RUN] {summarize_extraction(extraction)}")
                continue

            inserted = store.insert_rows(rows_to_insert)
            imported_files += 1
            imported_rows += inserted
            print(f"[BACKFILL][IMPORTED] {summarize_extraction(extraction)} inserted={inserted} existingRows={existing_rows}")
    finally:
        if store is not None:
            store.close()

    print(
        "[BACKFILL][SUMMARY] "
        f"scannedFiles={scanned_files} candidateRows={candidate_rows} importedFiles={imported_files} importedRows={imported_rows} "
        f"skippedExistingFiles={skipped_existing_files} skippedEmptyFiles={skipped_empty_files}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



