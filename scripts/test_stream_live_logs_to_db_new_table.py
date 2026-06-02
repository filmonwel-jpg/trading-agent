#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "stream_live_logs_to_db.py"


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=()) -> None:
        self.conn.executed.append(("execute", sql, tuple(params)))

    def executemany(self, sql: str, rows) -> None:
        normalized_rows = [tuple(row) for row in rows]
        self.conn.executed.append(("executemany", sql, normalized_rows))


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, str, object]] = []
        self.closed = False

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class FakePsycopg(ModuleType):
    def __init__(self) -> None:
        super().__init__("psycopg")
        self.connections: list[FakeConnection] = []

    def connect(self, *args, **kwargs) -> FakeConnection:
        conn = FakeConnection()
        self.connections.append(conn)
        return conn


def load_module(fake_psycopg: FakePsycopg):
    module_name = f"stream_live_logs_to_db_test_{len(fake_psycopg.connections)}"
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

    config = module.DbConfig(
        host="127.0.0.1",
        port=5432,
        database="trading_agent",
        user="trading_agent_databento",
        password="secret",
    )
    symbol = "AAPL"
    table_name = module.live_process_log_table_name(symbol)
    symbol_ts_index, run_id_index = module.live_process_log_index_names(table_name)
    store = module.LiveProcessLogStore(config, symbol)
    store.write_batch([
        ("RUN_1", "AAPL", "run_symbol.sh", "runtime/aapl_live_trade_logs.txt", "hello world"),
    ])
    store.close()

    if len(fake_psycopg.connections) != 1:
        raise AssertionError(f"expected one fake connection, got {len(fake_psycopg.connections)}")

    executed = fake_psycopg.connections[0].executed
    sql_text = "\n".join(sql for _, sql, _ in executed)

    if "ALTER TABLE IF EXISTS live_process_logs RENAME TO databento_live_process_logs" in sql_text:
        raise AssertionError("unexpected legacy rename statement executed")
    if f"CREATE TABLE IF NOT EXISTS {table_name}" not in sql_text:
        raise AssertionError(f"missing create-table statement for {table_name}")
    if f"CREATE INDEX IF NOT EXISTS {symbol_ts_index}" not in sql_text:
        raise AssertionError(f"missing symbol-ts index statement for {symbol_ts_index}")
    if f"CREATE INDEX IF NOT EXISTS {run_id_index}" not in sql_text:
        raise AssertionError(f"missing run-id index statement for {run_id_index}")
    if f"INSERT INTO {table_name}" not in sql_text:
        raise AssertionError(f"missing insert statement for {table_name}")
    if any("live_process_logs" in sql and table_name not in sql for _, sql, _ in executed):
        raise AssertionError(f"unexpected legacy-only SQL found: {executed!r}")
    if not fake_psycopg.connections[0].closed:
        raise AssertionError("store.close() did not close the connection")

    print("stream-live-logs-new-table-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

