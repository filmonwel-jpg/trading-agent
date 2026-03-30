#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import URLError, HTTPError
from urllib.request import urlopen

from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = REPO_ROOT / "runtime"
LAUNCH_LABELS = [
    "com.tradingagent.weekday-morning-mt",
    "com.tradingagent.afternoon-flatten-mt",
]
EXPECTED_SYMBOLS = ["TSLA", "NVDA", "AMD", "AMZN"]
HARVESTER_EXPECTED_SYMBOLS = ["TSLA", "QQQ", "NVDA", "AMD", "AMZN"]
HARVESTER_PROCESS_PATTERN = r"harvester\.py"
FULLY_ARMED_MARKER = "[+] Harvester fully armed. Streaming all symbols concurrently..."
MARKET_ZONE = ZoneInfo("America/New_York")


@dataclass
class SymbolStatus:
    symbol: str
    port: int
    client_id: str
    model_dir: str
    app_log_file: str
    port_open: bool
    health_ok: bool
    control_ok: bool
    connected: Optional[bool]
    control_symbol_ok: Optional[bool]
    strategy_enabled: Optional[bool]
    kill_switch: Optional[bool]
    status_payload: dict


@dataclass
class DbTableStatus:
    table_name: str
    exists: bool
    ok: bool
    row_count: Optional[int]
    latest_ts: Optional[str]
    per_symbol_counts: dict[str, int]
    missing_symbols: list[str]
    note: str = ""


@dataclass
class DbValidationStatus:
    checked: bool
    ok: bool
    url: str
    username: str
    market_day: str
    detail: str
    tables: list[DbTableStatus]


def run_command(command: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(command, capture_output=True, text=True)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def http_json(url: str) -> Optional[dict]:
    try:
        with urlopen(url, timeout=4) as response:
            return json.loads(response.read().decode("utf-8"))
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError):
        return None


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def normalize_postgres_url(raw_url: str) -> str:
    value = (raw_url or "").strip()
    if value.startswith("jdbc:"):
        value = value[len("jdbc:") :]
    return value


def resolve_db_settings() -> dict[str, str]:
    app_props = read_properties(REPO_ROOT / "src" / "main" / "resources" / "application.properties")
    runtime_props = read_properties(RUNTIME_DIR / "postgres-local.properties")
    url = (
        os.getenv("HARVEST_DB_URL")
        or os.getenv("SPRING_DATASOURCE_URL")
        or app_props.get("spring.datasource.url", "")
    )
    username = (
        os.getenv("HARVEST_DB_USER")
        or os.getenv("SPRING_DATASOURCE_USERNAME")
        or app_props.get("spring.datasource.username", "")
    )
    password = (
        os.getenv("HARVEST_DB_PASSWORD")
        or os.getenv("SPRING_DATASOURCE_PASSWORD")
        or runtime_props.get("spring.datasource.password", "")
    )
    return {
        "url": normalize_postgres_url(url),
        "username": username.strip(),
        "password": password,
    }


def format_ts(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        try:
            return value.astimezone(MARKET_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
        except ValueError:
            return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def query_table_status(cursor, table_name: str, time_column: str, date_predicate: str, expected_symbols: list[str] | None = None) -> DbTableStatus:
    cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
    exists = cursor.fetchone()[0] is not None
    if not exists:
        return DbTableStatus(
            table_name=table_name,
            exists=False,
            ok=False,
            row_count=None,
            latest_ts=None,
            per_symbol_counts={},
            missing_symbols=list(expected_symbols or []),
            note="table is missing",
        )

    cursor.execute(f"SELECT COUNT(*), MAX({time_column}) FROM {table_name} WHERE {date_predicate}")
    row_count, latest_ts = cursor.fetchone()
    per_symbol_counts: dict[str, int] = {}
    if expected_symbols is not None:
        cursor.execute(f"SELECT symbol, COUNT(*) FROM {table_name} WHERE {date_predicate} GROUP BY symbol")
        per_symbol_counts = {str(symbol).upper(): int(count) for symbol, count in cursor.fetchall()}
    missing_symbols = [symbol for symbol in (expected_symbols or []) if per_symbol_counts.get(symbol, 0) <= 0]
    ok = row_count > 0 and not missing_symbols
    note = ""
    if row_count <= 0:
        note = "no rows saved for today"
    elif missing_symbols:
        note = f"missing today for: {','.join(missing_symbols)}"

    return DbTableStatus(
        table_name=table_name,
        exists=True,
        ok=ok,
        row_count=int(row_count),
        latest_ts=format_ts(latest_ts),
        per_symbol_counts=per_symbol_counts,
        missing_symbols=missing_symbols,
        note=note,
    )


def validate_database() -> DbValidationStatus:
    settings = resolve_db_settings()
    market_day = datetime.now(MARKET_ZONE).date().isoformat()
    url = settings.get("url", "")
    username = settings.get("username", "")
    if not url or not username:
        return DbValidationStatus(
            checked=False,
            ok=False,
            url=url,
            username=username,
            market_day=market_day,
            detail="skipped: datasource URL or username is missing",
            tables=[],
        )

    try:
        import psycopg
    except ImportError:
        return DbValidationStatus(
            checked=False,
            ok=False,
            url=url,
            username=username,
            market_day=market_day,
            detail="skipped: psycopg is not installed",
            tables=[],
        )

    try:
        with psycopg.connect(url, user=username, password=settings.get("password", ""), connect_timeout=5) as conn:
            with conn.cursor() as cursor:
                tables = [
                    query_table_status(
                        cursor,
                        table_name="live_process_logs",
                        time_column="log_ts",
                        date_predicate="log_ts::date = CURRENT_DATE",
                        expected_symbols=EXPECTED_SYMBOLS,
                    ),
                    query_table_status(
                        cursor,
                        table_name="harvest_5s_bars",
                        time_column="bar_time",
                        date_predicate="market_day = CURRENT_DATE",
                        expected_symbols=HARVESTER_EXPECTED_SYMBOLS,
                    ),
                    query_table_status(
                        cursor,
                        table_name="harvest_news_events",
                        time_column="published_ts",
                        date_predicate="market_day = CURRENT_DATE",
                    ),
                ]
                live_trade_status = query_table_status(
                    cursor,
                    table_name="live_trade_logs",
                    time_column="trade_ts",
                    date_predicate="market_day = CURRENT_DATE",
                )
                if not live_trade_status.exists:
                    live_trade_status.ok = True
                    live_trade_status.note = "optional: trade log table not created yet"
                tables.append(live_trade_status)
    except Exception as exc:
        return DbValidationStatus(
            checked=False,
            ok=False,
            url=url,
            username=username,
            market_day=market_day,
            detail=f"connection/query failed: {exc}",
            tables=[],
        )

    required_tables_ok = all(table.ok for table in tables if table.table_name in {"live_process_logs", "harvest_5s_bars"})
    return DbValidationStatus(
        checked=True,
        ok=required_tables_ok,
        url=url,
        username=username,
        market_day=market_day,
        detail="validated current-day PostgreSQL rows",
        tables=tables,
    )


def launchd_job_loaded(label: str) -> tuple[bool, str]:
    uid = str(os.getuid())
    code, out, err = run_command(["launchctl", "print", f"gui/{uid}/{label}"])
    if code == 0 and out:
        state_line = next((line.strip() for line in out.splitlines() if "state =" in line), "state = unknown")
        return True, state_line
    return False, err or out or "not loaded"


def is_process_running(pattern: str) -> tuple[bool, str]:
    code, out, _ = run_command(["pgrep", "-f", pattern])
    return code == 0 and bool(out), out


def latest_matching_file(pattern: str) -> Optional[Path]:
    matches = list(RUNTIME_DIR.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def harvester_status() -> dict:
    running, pids = is_process_running(HARVESTER_PROCESS_PATTERN)
    latest_log = latest_matching_file("harvester-scheduled-*.log")
    armed = False
    last_lines: list[str] = []
    if latest_log and latest_log.exists():
        text = latest_log.read_text(errors="ignore")
        armed = FULLY_ARMED_MARKER in text
        last_lines = text.splitlines()[-5:]
    return {
        "running": running,
        "pids": pids,
        "latest_log": str(latest_log) if latest_log else "missing",
        "armed_marker_found": armed,
        "tail": last_lines,
    }


def port_open(port: int) -> bool:
    code, out, _ = run_command(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])
    return code == 0 and bool(out)


def collect_symbol_status(symbol: str) -> SymbolStatus:
    props_path = RUNTIME_DIR / f"trading-{symbol.lower()}.properties"
    props = read_properties(props_path)
    port = int(props.get("server.port", "0") or 0)
    client_id = props.get("trading.client-id", "")
    model_dir = props.get("trading.model.dir", "")
    app_log_file = props.get("logging.file.name", "")
    listening = port_open(port) if port else False
    health = http_json(f"http://127.0.0.1:{port}/actuator/health") if listening else None
    control = http_json(f"http://127.0.0.1:{port}/api/control/status") if listening else None
    health_ok = bool(health and health.get("status") == "UP")
    control_ok = bool(control and str(control.get("symbol", "")).upper() == symbol and control.get("connected") is True)
    return SymbolStatus(
        symbol=symbol,
        port=port,
        client_id=client_id,
        model_dir=model_dir,
        app_log_file=app_log_file,
        port_open=listening,
        health_ok=health_ok,
        control_ok=control_ok,
        connected=None if control is None else bool(control.get("connected")),
        control_symbol_ok=None if control is None else str(control.get("symbol", "")).upper() == symbol,
        strategy_enabled=None if control is None else bool(control.get("strategyEnabled")),
        kill_switch=None if control is None else bool(control.get("killSwitch")),
        status_payload=control or {},
    )


def latest_morning_log() -> Optional[Path]:
    return latest_matching_file("morning-launch-*.log")


def print_db_validation(status: DbValidationStatus) -> None:
    print("\n=== PostgreSQL Validation ===")
    print(f"market_day={status.market_day} checked={status.checked} ok={status.ok} user={status.username or '<missing>'}")
    print(f"url={status.url or '<missing>'}")
    print(f"detail={status.detail}")
    for table in status.tables:
        print(
            f"{table.table_name}: exists={table.exists} ok={table.ok} "
            f"rows_today={table.row_count if table.row_count is not None else 'n/a'} latest={table.latest_ts or 'n/a'}"
        )
        if table.per_symbol_counts:
            counts = " ".join(f"{symbol}={table.per_symbol_counts.get(symbol, 0)}" for symbol in sorted(table.per_symbol_counts))
            print(f"  per_symbol: {counts}")
        if table.note:
            print(f"  note: {table.note}")


def main() -> int:
    print("=== Launchd Jobs ===")
    for label in LAUNCH_LABELS:
        loaded, detail = launchd_job_loaded(label)
        print(f"{label}: loaded={loaded} detail={detail}")

    print("\n=== Harvester ===")
    hs = harvester_status()
    print(f"running={hs['running']} pids={hs['pids'] or 'none'}")
    print(f"latest_log={hs['latest_log']}")
    print(f"fully_armed={hs['armed_marker_found']}")
    if hs["tail"]:
        print("tail:")
        for line in hs["tail"]:
            print(f"  {line}")

    ml = latest_morning_log()
    print("\n=== Morning Launcher Log ===")
    print(str(ml) if ml else "missing")

    print("\n=== Symbol Bots ===")
    all_ok = hs["running"] and hs["armed_marker_found"]
    for symbol in EXPECTED_SYMBOLS:
        status = collect_symbol_status(symbol)
        ok = status.port_open and status.health_ok and status.control_ok and not bool(status.kill_switch)
        all_ok = all_ok and ok
        print(
            f"{status.symbol}: ok={ok} port={status.port} port_open={status.port_open} "
            f"health_ok={status.health_ok} connected={status.connected} "
            f"symbol_match={status.control_symbol_ok} strategy_enabled={status.strategy_enabled} "
            f"kill_switch={status.kill_switch} client_id={status.client_id} model_dir={status.model_dir}"
        )
        if status.status_payload:
            print(
                f"  status: openOrders={status.status_payload.get('openOrders')} "
                f"position={status.status_payload.get('currentPosition', status.status_payload.get('position'))} "
                f"app_log={status.app_log_file}"
            )
        else:
            print(f"  status: unavailable app_log={status.app_log_file}")

    db_status = validate_database()
    print_db_validation(db_status)
    if db_status.checked:
        all_ok = all_ok and db_status.ok

    print("\n=== Summary ===")
    print(f"ALL_STARTED_OK={all_ok}")
    if all_ok:
        print("All expected morning services are up: harvester fully armed, every bot is listening/healthy/connected, and DB validation passed.")
        return 0
    if db_status.checked and not db_status.ok:
        print("One or more services are up, but DB validation did not pass. Check the PostgreSQL section above for missing rows or tables.")
        return 1
    print("One or more services are not fully ready. Check the logs above and the launchd stdout/stderr files in runtime/.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


