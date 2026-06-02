#!/usr/bin/env python3
from __future__ import annotations

import re
import socket
import os
from pathlib import Path

import psycopg


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PROPS = REPO_ROOT / "src" / "main" / "resources" / "application.properties"
RUNTIME_PROPS = REPO_ROOT / "runtime" / "postgres-local.properties"


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def parse_jdbc(url: str) -> tuple[str, int, str, str]:
    normalized = url[5:] if url.startswith("jdbc:") else url
    match = re.match(r"postgresql://([^:/]+):(\d+)/(\S+)", normalized)
    if not match:
        raise ValueError(f"Unsupported PostgreSQL URL format: {url}")
    host, port, database = match.group(1), int(match.group(2)), match.group(3)
    return normalized, host, port, database


def tcp_reachable(host: str, port: int, timeout: float = 3.0) -> bool:
    sock = socket.socket()
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True
    finally:
        sock.close()


def scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return None if row is None else row[0]


def main() -> int:
    app = read_properties(APP_PROPS)
    runtime = read_properties(RUNTIME_PROPS)
    jdbc_url = (
        os.getenv("HARVEST_DB_URL")
        or os.getenv("SPRING_DATASOURCE_URL")
        or runtime.get("spring.datasource.url", "")
        or app.get("spring.datasource.url", "")
    )
    user = (
        os.getenv("HARVEST_DB_USER")
        or os.getenv("SPRING_DATASOURCE_USERNAME")
        or runtime.get("spring.datasource.username", "")
        or app.get("spring.datasource.username", "")
    )
    password = (
        os.getenv("HARVEST_DB_PASSWORD")
        or os.getenv("SPRING_DATASOURCE_PASSWORD")
        or runtime.get("spring.datasource.password", "")
    )

    normalized_url, host, port, database = parse_jdbc(jdbc_url)
    print({
        "config": {
            "host": host,
            "port": port,
            "database": database,
            "username": user,
        }
    })

    reachable = tcp_reachable(host, port)
    print({"tcp_reachable": reachable})
    if not reachable:
        print({"diagnosis": "DB down or port blocked"})
        return 1

    try:
        with psycopg.connect(normalized_url, user=user, password=password, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                current_user = scalar(cur, "select current_user")
                current_db = scalar(cur, "select current_database()")
                table_name = scalar(cur, "select to_regclass('public.databento_live_trade_logs')")
                owner = scalar(
                    cur,
                    """
                    SELECT pg_get_userbyid(c.relowner)
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relname = 'databento_live_trade_logs'
                      AND c.relkind = 'r'
                    """,
                )
                max_connections = scalar(cur, "show max_connections")
                activity_count = scalar(cur, "select count(*) from pg_stat_activity")
                reserved_connections = scalar(cur, "show superuser_reserved_connections")
                can_create = scalar(cur, "select has_schema_privilege(current_user, 'public', 'CREATE')")
                can_usage = scalar(cur, "select has_schema_privilege(current_user, 'public', 'USAGE')")
                has_insert = None
                has_select = None
                row_count = None
                write_probe = None
                if table_name:
                    has_insert = scalar(cur, "select has_table_privilege(current_user, 'public.databento_live_trade_logs', 'INSERT')")
                    has_select = scalar(cur, "select has_table_privilege(current_user, 'public.databento_live_trade_logs', 'SELECT')")
                    try:
                        row_count = scalar(cur, "select count(*) from databento_live_trade_logs")
                    except Exception as exc:  # noqa: BLE001
                        row_count = f"error: {exc}"
                try:
                    cur.execute("select 1")
                    write_probe = "jdbc-acquisition-ok"
                except Exception as exc:  # noqa: BLE001
                    write_probe = f"error: {exc}"

                print({
                    "login_ok": True,
                    "current_user": current_user,
                    "current_db": current_db,
                    "table_exists": bool(table_name),
                    "table_name": table_name,
                    "table_owner": owner,
                    "schema_usage": can_usage,
                    "schema_create": can_create,
                    "table_select": has_select,
                    "table_insert": has_insert,
                    "row_count": row_count,
                    "pg_stat_activity_count": activity_count,
                    "max_connections": max_connections,
                    "superuser_reserved_connections": reserved_connections,
                    "probe": write_probe,
                })
    except Exception as exc:  # noqa: BLE001
        print({"login_ok": False, "error": str(exc)})
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

