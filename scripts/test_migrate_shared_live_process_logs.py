#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "migrate_shared_live_process_logs.py"


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self._fetchone = None
        self._fetchall = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=()) -> None:
        normalized = " ".join(sql.split())
        self.conn.executed.append((normalized, tuple(params)))
        if "SELECT to_regclass" in normalized:
            self._fetchone = ("databento_live_process_logs",)
        elif "SELECT DISTINCT symbol" in normalized:
            self._fetchall = [("TSLA",)]
        elif "SELECT COUNT(*) FROM databento_live_process_logs WHERE symbol = %s" in normalized:
            self._fetchone = (5,)
        elif "SELECT COUNT(*) FROM databento_live_process_logs src" in normalized:
            self._fetchone = (3,)
        else:
            self._fetchone = None
            self._fetchall = []

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class FakePsycopg(ModuleType):
    def __init__(self) -> None:
        super().__init__("psycopg")
        self.connection = FakeConnection()

    def connect(self, *args, **kwargs):
        return self.connection


def load_module(fake_psycopg: FakePsycopg):
    module_name = "migrate_shared_live_process_logs_test"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.modules["psycopg"] = fake_psycopg
    spec.loader.exec_module(module)
    return module


def main() -> int:
    fake_psycopg = FakePsycopg()
    module = load_module(fake_psycopg)
    module.load_postgres_settings = lambda repo_root: {
        "url": "postgresql://127.0.0.1:5432/trading_agent",
        "username": "trading_agent_databento",
        "password": "secret",
    }

    store = module.SharedLiveProcessLogMigrationStore(REPO_ROOT)
    source_table = store.resolve_source_table()
    symbols = store.discover_symbols(source_table, set())
    result = store.migrate_symbol(source_table, "TSLA", dry_run=False)
    store.close()

    assert source_table == module.LIVE_PROCESS_LOG_TABLE
    assert symbols == ["TSLA"]
    assert result.destination_table == module.live_process_log_table_name("TSLA")
    assert result.inserted_rows == 3
    executed_sql = "\n".join(sql for sql, _ in fake_psycopg.connection.executed)
    assert f"CREATE TABLE IF NOT EXISTS {result.destination_table}" in executed_sql
    assert f"INSERT INTO {result.destination_table}" in executed_sql
    assert f"FROM {source_table} src" in executed_sql
    assert fake_psycopg.connection.closed

    print("migrate-shared-live-process-logs-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

