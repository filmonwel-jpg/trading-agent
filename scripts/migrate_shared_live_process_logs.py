#!/usr/bin/env python3
"""One-time migration from databento_live_process_logs into per-symbol tables."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harvest_storage import load_postgres_settings
from stream_live_logs_to_db import (
    LIVE_PROCESS_LOG_TABLE,
    live_process_log_index_names,
    live_process_log_table_name,
)

@dataclass(frozen=True)
class SymbolMigrationResult:
    symbol: str
    source_table: str
    destination_table: str
    source_rows: int
    missing_rows: int
    inserted_rows: int


class SharedLiveProcessLogMigrationStore:
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
            application_name="live_process_log_migration",
        )
        self._schema_ready: set[str] = set()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def table_exists(self, table_name: str) -> bool:
        with self._conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            row = cursor.fetchone()
        return bool(row and row[0] is not None)

    def resolve_source_table(self) -> str:
        if self.table_exists(LIVE_PROCESS_LOG_TABLE):
            return LIVE_PROCESS_LOG_TABLE
        raise RuntimeError(f"Could not find required source table: {LIVE_PROCESS_LOG_TABLE}")

    def _ensure_destination_schema(self, symbol: str) -> str:
        table_name = live_process_log_table_name(symbol)
        if table_name in self._schema_ready:
            return table_name
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
        return table_name

    def discover_symbols(self, source_table: str, symbol_filter: set[str]) -> list[str]:
        with self._conn.cursor() as cursor:
            if symbol_filter:
                cursor.execute(
                    f"SELECT DISTINCT symbol FROM {source_table} WHERE symbol = ANY(%s) ORDER BY symbol",
                    (sorted(symbol_filter),),
                )
            else:
                cursor.execute(f"SELECT DISTINCT symbol FROM {source_table} ORDER BY symbol")
            rows = cursor.fetchall()
        return [str(row[0]).upper() for row in rows if row and row[0]]

    def count_source_rows(self, source_table: str, symbol: str) -> int:
        with self._conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {source_table} WHERE symbol = %s", (symbol,))
            row = cursor.fetchone()
        return int(row[0] if row else 0)

    def count_missing_rows(self, source_table: str, destination_table: str, symbol: str) -> int:
        with self._conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {source_table} src
                WHERE src.symbol = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {destination_table} dst
                      WHERE dst.run_id = src.run_id
                        AND dst.symbol = src.symbol
                        AND dst.source = src.source
                        AND COALESCE(dst.source_file, '') = COALESCE(src.source_file, '')
                        AND dst.log_ts = src.log_ts
                        AND dst.log_line = src.log_line
                  )
                """,
                (symbol,),
            )
            row = cursor.fetchone()
        return int(row[0] if row else 0)

    def migrate_symbol(self, source_table: str, symbol: str, dry_run: bool = False) -> SymbolMigrationResult:
        destination_table = self._ensure_destination_schema(symbol)
        source_rows = self.count_source_rows(source_table, symbol)
        missing_rows = self.count_missing_rows(source_table, destination_table, symbol)
        inserted_rows = 0
        if not dry_run and missing_rows > 0:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {destination_table} (run_id, symbol, source, source_file, log_line, log_ts)
                    SELECT src.run_id, src.symbol, src.source, src.source_file, src.log_line, src.log_ts
                    FROM {source_table} src
                    WHERE src.symbol = %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {destination_table} dst
                          WHERE dst.run_id = src.run_id
                            AND dst.symbol = src.symbol
                            AND dst.source = src.source
                            AND COALESCE(dst.source_file, '') = COALESCE(src.source_file, '')
                            AND dst.log_ts = src.log_ts
                            AND dst.log_line = src.log_line
                      )
                    ORDER BY src.log_ts, src.id
                    """,
                    (symbol,),
                )
            inserted_rows = missing_rows
        return SymbolMigrationResult(
            symbol=symbol,
            source_table=source_table,
            destination_table=destination_table,
            source_rows=source_rows,
            missing_rows=missing_rows,
            inserted_rows=inserted_rows,
        )


def normalize_symbols(raw: str) -> set[str]:
    out: set[str] = set()
    for token in str(raw or "").split(","):
        cleaned = token.strip().upper()
        if cleaned:
            out.add(cleaned)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate databento_live_process_logs rows into per-symbol tables.")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol filter.")
    parser.add_argument("--dry-run", action="store_true", help="Report rows that would migrate without inserting.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbol_filter = normalize_symbols(args.symbols)
    store = SharedLiveProcessLogMigrationStore(REPO_ROOT)
    try:
        source_table = store.resolve_source_table()
        symbols = store.discover_symbols(source_table, symbol_filter)
        print(f"[MIGRATE] repo_root={REPO_ROOT}")
        print(f"[MIGRATE] source_table={source_table} dry_run={args.dry_run} symbols={len(symbols)}")
        if not symbols:
            print("[MIGRATE] no matching symbols found in source table")
            return 0
        migrated_symbols = 0
        migrated_rows = 0
        missing_rows = 0
        for symbol in symbols:
            result = store.migrate_symbol(source_table, symbol, dry_run=args.dry_run)
            missing_rows += result.missing_rows
            if args.dry_run:
                print(
                    f"[MIGRATE][DRY-RUN] symbol={result.symbol} sourceRows={result.source_rows} "
                    f"missingRows={result.missing_rows} destination={result.destination_table}"
                )
                continue
            migrated_symbols += 1
            migrated_rows += result.inserted_rows
            print(
                f"[MIGRATE][DONE] symbol={result.symbol} sourceRows={result.source_rows} "
                f"insertedRows={result.inserted_rows} destination={result.destination_table}"
            )
        print(
            f"[MIGRATE][SUMMARY] sourceTable={source_table} symbols={len(symbols)} "
            f"missingRows={missing_rows} migratedSymbols={migrated_symbols} migratedRows={migrated_rows} dryRun={args.dry_run}"
        )
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())


