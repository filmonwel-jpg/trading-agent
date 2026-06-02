#!/usr/bin/env python3
"""Mirror combined live process logs to stdout/file and persist them to PostgreSQL."""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


JDBC_POSTGRES_RE = re.compile(r"^jdbc:postgresql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)")
LIVE_PROCESS_LOG_TABLE = "databento_live_process_logs"


def normalize_symbol_identifier(symbol: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", str(symbol or "").strip().lower())
    cleaned = cleaned.strip("_")
    if not cleaned:
        raise ValueError("Symbol is required for per-symbol live process log tables.")
    if cleaned[0].isdigit():
        cleaned = f"s_{cleaned}"
    return cleaned


def live_process_log_table_name(symbol: str | None = None) -> str:
    if symbol is None:
        return LIVE_PROCESS_LOG_TABLE
    return f"{normalize_symbol_identifier(symbol)}_{LIVE_PROCESS_LOG_TABLE}"


def live_process_log_index_names(table_name: str) -> tuple[str, str]:
    return (
        f"idx_{table_name}_symbol_ts",
        f"idx_{table_name}_run_id",
    )


@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist live run_symbol output to PostgreSQL while mirroring to stdout.")
    parser.add_argument("--symbol", required=True, help="Trading symbol for this process log stream.")
    parser.add_argument("--run-id", required=True, help="Logical run identifier for grouping rows.")
    parser.add_argument("--tee-file", default="", help="Optional file path to append the mirrored stream to.")
    parser.add_argument("--db-url", default="", help="Override JDBC/PostgreSQL URL.")
    parser.add_argument("--db-user", default="", help="Override PostgreSQL username.")
    parser.add_argument("--db-password", default="", help="Override PostgreSQL password.")
    parser.add_argument("--source", default="run_symbol.sh", help="Logical source label stored with each row.")
    return parser.parse_args()


def load_properties(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    props: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def resolve_db_config(repo_root: Path, args: argparse.Namespace) -> DbConfig:
    app_props = load_properties(repo_root / "src/main/resources/application.properties")
    runtime_props = load_properties(repo_root / "runtime/postgres-local.properties")

    jdbc_url = (
        args.db_url
        or os.getenv("HARVEST_DB_URL", "")
        or os.getenv("SPRING_DATASOURCE_URL", "")
        or runtime_props.get("spring.datasource.url", "")
        or app_props.get("spring.datasource.url", "")
    )
    user = (
        args.db_user
        or os.getenv("HARVEST_DB_USER", "")
        or os.getenv("SPRING_DATASOURCE_USERNAME", "")
        or runtime_props.get("spring.datasource.username", "")
        or app_props.get("spring.datasource.username", "")
    )
    password = (
        args.db_password
        or os.getenv("HARVEST_DB_PASSWORD", "")
        or os.getenv("SPRING_DATASOURCE_PASSWORD", "")
        or runtime_props.get("spring.datasource.password", "")
    )

    match = JDBC_POSTGRES_RE.match(jdbc_url)
    if not match:
        raise ValueError(f"Unsupported or missing PostgreSQL JDBC URL: {jdbc_url!r}")
    if not user:
        raise ValueError("Missing PostgreSQL username.")

    return DbConfig(
        host=match.group("host"),
        port=int(match.group("port") or "5432"),
        database=match.group("database"),
        user=user,
        password=password,
    )


class LiveProcessLogStore:
    def __init__(self, config: DbConfig, symbol: str) -> None:
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError(
                "psycopg is required for --tee-db support. Install it with: pip install 'psycopg[binary]>=3.2'"
            ) from exc

        self._config = config
        self._psycopg = psycopg
        self._table_name = live_process_log_table_name(symbol)
        self._schema_ready = False

    def _connect(self):
        return self._psycopg.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.database,
            user=self._config.user,
            password=self._config.password,
            autocommit=True,
            connect_timeout=5,
            application_name="live_process_log_sink",
        )

    def _ensure_schema(self, conn) -> None:
        symbol_ts_index, run_id_index = live_process_log_index_names(self._table_name)
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table_name} (
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
                f"CREATE INDEX IF NOT EXISTS {symbol_ts_index} ON {self._table_name} (symbol, log_ts DESC)"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS {run_id_index} ON {self._table_name} (run_id)"
            )
        self._schema_ready = True

    def write_batch(self, rows: Iterable[tuple[str, str, str, str, str]]) -> None:
        rows = list(rows)
        if not rows:
            return
        with self._connect() as conn:
            if not self._schema_ready:
                self._ensure_schema(conn)
            with conn.cursor() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO {self._table_name} (run_id, symbol, source, source_file, log_line)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    def close(self) -> None:
        return None


class MirrorSink:
    def __init__(self, tee_file: str) -> None:
        self._tee_handle = None
        if tee_file:
            path = Path(tee_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._tee_handle = path.open("a", encoding="utf-8")

    def write(self, line: str) -> None:
        sys.stdout.write(line)
        sys.stdout.flush()
        if self._tee_handle is not None:
            self._tee_handle.write(line)
            self._tee_handle.flush()

    def close(self) -> None:
        if self._tee_handle is not None:
            self._tee_handle.close()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent

    mirror = MirrorSink(args.tee_file)
    db_store = None
    db_enabled = True
    buffer: list[tuple[str, str, str, str, str]] = []

    try:
        try:
            db_store = LiveProcessLogStore(resolve_db_config(repo_root, args), args.symbol)
        except Exception as exc:  # pragma: no cover - environment dependent
            db_enabled = False
            print(f"[RUN][WARN] live process log DB sink disabled: {exc}", file=sys.stderr, flush=True)

        for line in sys.stdin:
            mirror.write(line)
            if not db_enabled:
                continue
            clean_line = line.rstrip("\n")
            buffer.append((args.run_id, args.symbol.upper(), args.source, args.tee_file, clean_line))
            if len(buffer) >= 50:
                try:
                    db_store.write_batch(buffer)
                    buffer.clear()
                except Exception as exc:  # pragma: no cover - environment dependent
                    db_enabled = False
                    print(f"[RUN][WARN] live process log DB sink failed mid-stream: {exc}", file=sys.stderr, flush=True)

        if db_enabled and buffer:
            db_store.write_batch(buffer)
        return 0
    finally:
        mirror.close()
        if db_store is not None:
            db_store.close()


if __name__ == "__main__":
    raise SystemExit(main())

