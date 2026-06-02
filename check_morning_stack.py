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

from stream_live_logs_to_db import LIVE_PROCESS_LOG_TABLE, live_process_log_table_name

REPO_ROOT = Path(__file__).resolve().parent
BRIDGE_SRC_ROOT = REPO_ROOT / "databento_ibkr_bridge" / "src"
if BRIDGE_SRC_ROOT.exists() and str(BRIDGE_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(BRIDGE_SRC_ROOT))

try:
    from databento_ibkr_bridge.shared_ibkr_gateway_health import probe_gateway
except ImportError:
    probe_gateway = None

RUNTIME_DIR = REPO_ROOT / "runtime"
DATABENTO_BOTS_DIR = RUNTIME_DIR / "databento" / "bots"
SHARED_GATEWAY_PID_FILE = RUNTIME_DIR / "databento" / "state" / "shared-ibkr-gateway.pid"
RUNTIME_ENV_FILE = RUNTIME_DIR / "databento.env"
BRIDGE_ENV_FILE = REPO_ROOT / "databento_ibkr_bridge" / ".env"
DEFAULT_SHARED_GATEWAY_HOST = "127.0.0.1"
DEFAULT_SHARED_GATEWAY_PORT = 9910
LAUNCH_LABELS = [
    "com.tradingagent.weekday-morning-mt",
    "com.tradingagent.afternoon-flatten-mt",
]
DEFAULT_EXPECTED_SYMBOLS = ["TSLA", "NVDA", "AMD", "AMZN"]
MARKET_ZONE = ZoneInfo("America/New_York")
LEGACY_LIVE_PROCESS_LOG_TABLE = "live_process_logs"
SHARED_GATEWAY_ARG_PATTERNS = {
    "enabled": re.compile(r"--trading\.ibkr\.shared-gateway\.enabled=(\S+)"),
    "host": re.compile(r"--trading\.ibkr\.shared-gateway\.host=(\S+)"),
    "port": re.compile(r"--trading\.ibkr\.shared-gateway\.port=(\d+)"),
    "connect_timeout_ms": re.compile(r"--trading\.ibkr\.shared-gateway\.connect-timeout-ms=(\d+)"),
    "ack_timeout_ms": re.compile(r"--trading\.ibkr\.shared-gateway\.ack-timeout-ms=(\d+)"),
    "skip_direct_connection": re.compile(r"--trading\.ibkr\.shared-gateway\.skip-direct-connection=(\S+)"),
}


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
    recent_issue: Optional[str]
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


@dataclass
class SharedGatewayExpectation:
    enabled: bool
    required: bool
    host: str
    port: int
    connect_timeout_seconds: float
    ack_timeout_seconds: float
    source: str


@dataclass
class SharedGatewayStatus:
    ok: bool
    enabled: bool
    required: bool
    host: str
    port: int
    connect_timeout_seconds: float
    ack_timeout_seconds: float
    source: str
    detail: str
    pid: str
    pid_alive: bool
    port_open: bool
    protocol_ok: bool
    connected: Optional[bool]
    dry_run: Optional[bool]
    connect_latency_ms: Optional[float]
    ping_latency_ms: Optional[float]
    snapshot_latency_ms: Optional[float]


def run_command(command: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(command, capture_output=True, text=True)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def parse_int(value: object, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return default


def timeout_ms_to_seconds(value: object, default_ms: int) -> float:
    millis = parse_int(value, default_ms)
    if millis <= 0:
        millis = default_ms
    return round(millis / 1000.0, 3)


def http_json(url: str, attempts: int = 2) -> Optional[dict]:
    for attempt in range(max(1, attempts)):
        try:
            with urlopen(url, timeout=4) as response:
                return json.loads(response.read().decode("utf-8"))
        except (URLError, HTTPError, TimeoutError, ConnectionResetError, OSError, json.JSONDecodeError, ValueError):
            if attempt + 1 >= max(1, attempts):
                return None
    return None


def databento_feed_health_ok(control: Optional[dict], feed_health: Optional[dict]) -> bool:
    if not isinstance(control, dict) or not isinstance(feed_health, dict):
        return False
    provider = str(control.get("marketDataProvider", "") or "").strip().lower()
    if provider != "databento":
        return False
    if feed_health.get("healthy") is True:
        return True
    if feed_health.get("withinStartupGrace") is True:
        return True
    return feed_health.get("marketDataExpectedNow") is False


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


def resolve_symbol_properties_path(symbol: str) -> Path:
    symbol_lower = symbol.lower()
    candidates = [
        DATABENTO_BOTS_DIR / f"trading-{symbol_lower}.properties",
        RUNTIME_DIR / f"trading-{symbol_lower}.properties",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def list_symbol_property_files() -> list[Path]:
    databento_files = sorted(
        path
        for path in DATABENTO_BOTS_DIR.glob("trading-*.properties")
        if path.name != "trading-databento-template.properties"
    )
    if databento_files:
        return databento_files

    legacy_files = sorted(
        path
        for path in RUNTIME_DIR.glob("trading-*.properties")
        if path.name != "trading-databento-template.properties"
    )
    return legacy_files


def discover_expected_symbols() -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for path in list_symbol_property_files():
        props = read_properties(path)
        raw_symbol = props.get("trading.symbol", path.stem.replace("trading-", ""))
        symbol = raw_symbol.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols or list(DEFAULT_EXPECTED_SYMBOLS)


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
        or runtime_props.get("spring.datasource.url", "")
        or app_props.get("spring.datasource.url", "")
    )
    username = (
        os.getenv("HARVEST_DB_USER")
        or os.getenv("SPRING_DATASOURCE_USERNAME")
        or runtime_props.get("spring.datasource.username", "")
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


def market_day_predicate(column_name: str) -> str:
    return f"({column_name} AT TIME ZONE 'America/New_York')::date = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date"


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


def query_live_process_status_for_symbol(cursor, symbol: str) -> DbTableStatus:
    per_symbol_table = live_process_log_table_name(symbol)
    primary_status = query_table_status(
        cursor,
        table_name=per_symbol_table,
        time_column="log_ts",
        date_predicate=market_day_predicate("log_ts"),
        expected_symbols=[symbol],
    )
    if primary_status.exists:
        return primary_status

    shared_status = query_table_status(
        cursor,
        table_name=LIVE_PROCESS_LOG_TABLE,
        time_column="log_ts",
        date_predicate=market_day_predicate("log_ts"),
        expected_symbols=[symbol],
    )
    if shared_status.exists:
        shared_status.note = f"using shared table {LIVE_PROCESS_LOG_TABLE}"
        return shared_status

    legacy_status = query_table_status(
        cursor,
        table_name=LEGACY_LIVE_PROCESS_LOG_TABLE,
        time_column="log_ts",
        date_predicate=market_day_predicate("log_ts"),
        expected_symbols=[symbol],
    )
    if legacy_status.exists:
        legacy_status.table_name = per_symbol_table
        legacy_status.note = f"using legacy table {LEGACY_LIVE_PROCESS_LOG_TABLE}"
        return legacy_status

    primary_status.note = "table is missing"
    return primary_status


def validate_database(expected_symbols: list[str]) -> DbValidationStatus:
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
                tables = [query_live_process_status_for_symbol(cursor, symbol) for symbol in expected_symbols]
    except Exception as exc:
        detail = f"connection/query failed: {exc}"
        if "remaining connection slots are reserved for roles with the SUPERUSER attribute" in detail:
            detail = (
                "connection/query failed: PostgreSQL connection slots are exhausted. "
                "Too many live log DB sink sessions may still be open; restart the bot launchers so they use the updated short-lived sink connections."
            )
        return DbValidationStatus(
            checked=False,
            ok=False,
            url=url,
            username=username,
            market_day=market_day,
            detail=detail,
            tables=[],
        )

    required_tables_ok = all(table.ok for table in tables)
    return DbValidationStatus(
        checked=True,
        ok=required_tables_ok,
        url=url,
        username=username,
        market_day=market_day,
        detail="validated required current-day live process log rows",
        tables=tables,
    )


def launchd_job_loaded(label: str) -> tuple[bool, str]:
    uid = str(os.getuid())
    code, out, err = run_command(["launchctl", "print", f"gui/{uid}/{label}"])
    if code == 0 and out:
        state_line = next((line.strip() for line in out.splitlines() if "state =" in line), "state = unknown")
        return True, state_line
    return False, err or out or "not loaded"


def latest_matching_file(pattern: str) -> Optional[Path]:
    matches = list(RUNTIME_DIR.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def file_is_for_current_market_day(path: Optional[Path]) -> bool:
    if path is None or not path.exists():
        return False
    try:
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=MARKET_ZONE)
    except OSError:
        return False
    return modified.date() == datetime.now(MARKET_ZONE).date()


def port_open(port: int) -> bool:
    code, out, _ = run_command(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])
    return code == 0 and bool(out)


def pid_is_alive(pid: str) -> bool:
    pid_text = str(pid or "").strip()
    if not pid_text.isdigit():
        return False
    code, _, _ = run_command(["ps", "-p", pid_text])
    return code == 0


def read_recent_text(path: Optional[Path], max_lines: int = 400) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    try:
        return "\n".join(path.read_text(errors="ignore").splitlines()[-max(1, max_lines) :])
    except OSError:
        return ""


def latest_shared_gateway_config_texts() -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for source, path in [
        ("bulk-launch-log", latest_matching_file("start_all_databento_bots*.out*")),
        ("morning-launch-log", latest_morning_log()),
    ]:
        if not file_is_for_current_market_day(path):
            continue
        text = read_recent_text(path)
        if text:
            texts.append((source, text))
    return texts


def extract_shared_gateway_settings_from_text(text: str) -> dict[str, str]:
    settings: dict[str, str] = {}
    if not text:
        return settings
    for key, pattern in SHARED_GATEWAY_ARG_PATTERNS.items():
        matches = pattern.findall(text)
        if matches:
            settings[key] = str(matches[-1]).strip()
    return settings


def running_process_commands() -> str:
    code, out, _ = run_command(["ps", "axww", "-o", "command="])
    return out if code == 0 else ""


def read_shared_gateway_pid() -> str:
    if not SHARED_GATEWAY_PID_FILE.exists():
        return ""
    try:
        return SHARED_GATEWAY_PID_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def resolve_shared_gateway_expectation() -> SharedGatewayExpectation:
    app_props = read_properties(REPO_ROOT / "src" / "main" / "resources" / "application.properties")
    runtime_env = read_properties(RUNTIME_ENV_FILE)
    bridge_env = read_properties(BRIDGE_ENV_FILE)

    enabled = parse_bool(app_props.get("trading.ibkr.shared-gateway.enabled"), False)
    required = parse_bool(app_props.get("trading.ibkr.shared-gateway.skip-direct-connection"), False)
    host = (
        os.getenv("IBKR_SHARED_GATEWAY_HOST")
        or runtime_env.get("IBKR_SHARED_GATEWAY_HOST", "")
        or bridge_env.get("IBKR_SHARED_GATEWAY_HOST", "")
        or app_props.get("trading.ibkr.shared-gateway.host", "")
        or DEFAULT_SHARED_GATEWAY_HOST
    )
    port = parse_int(
        os.getenv("IBKR_SHARED_GATEWAY_PORT")
        or runtime_env.get("IBKR_SHARED_GATEWAY_PORT", "")
        or bridge_env.get("IBKR_SHARED_GATEWAY_PORT", "")
        or app_props.get("trading.ibkr.shared-gateway.port", "")
        or DEFAULT_SHARED_GATEWAY_PORT,
        DEFAULT_SHARED_GATEWAY_PORT,
    )
    connect_timeout_seconds = timeout_ms_to_seconds(
        os.getenv("IBKR_SHARED_GATEWAY_CONNECT_TIMEOUT_MS")
        or runtime_env.get("IBKR_SHARED_GATEWAY_CONNECT_TIMEOUT_MS", "")
        or bridge_env.get("IBKR_SHARED_GATEWAY_CONNECT_TIMEOUT_MS", "")
        or app_props.get("trading.ibkr.shared-gateway.connect-timeout-ms", "")
        or 3000,
        3000,
    )
    ack_timeout_seconds = timeout_ms_to_seconds(
        os.getenv("IBKR_SHARED_GATEWAY_ACK_TIMEOUT_MS")
        or runtime_env.get("IBKR_SHARED_GATEWAY_ACK_TIMEOUT_MS", "")
        or bridge_env.get("IBKR_SHARED_GATEWAY_ACK_TIMEOUT_MS", "")
        or app_props.get("trading.ibkr.shared-gateway.ack-timeout-ms", "")
        or 5000,
        5000,
    )
    source_parts: list[str] = []
    if enabled or required:
        source_parts.append("application.properties")
    if os.getenv("IBKR_SHARED_GATEWAY_HOST") or os.getenv("IBKR_SHARED_GATEWAY_PORT"):
        source_parts.append("environment")
    elif runtime_env.get("IBKR_SHARED_GATEWAY_HOST") or runtime_env.get("IBKR_SHARED_GATEWAY_PORT"):
        source_parts.append("runtime/databento.env")
    elif bridge_env.get("IBKR_SHARED_GATEWAY_HOST") or bridge_env.get("IBKR_SHARED_GATEWAY_PORT"):
        source_parts.append("databento_ibkr_bridge/.env")

    process_settings = extract_shared_gateway_settings_from_text(running_process_commands())
    if process_settings:
        enabled = parse_bool(process_settings.get("enabled"), enabled)
        required = parse_bool(process_settings.get("skip_direct_connection"), required)
        host = process_settings.get("host", host) or host
        port = parse_int(process_settings.get("port"), port)
        connect_timeout_seconds = timeout_ms_to_seconds(process_settings.get("connect_timeout_ms"), int(connect_timeout_seconds * 1000))
        ack_timeout_seconds = timeout_ms_to_seconds(process_settings.get("ack_timeout_ms"), int(ack_timeout_seconds * 1000))
        source_parts.append("active-processes")

    for source_name, text in latest_shared_gateway_config_texts():
        settings = extract_shared_gateway_settings_from_text(text)
        if not settings:
            continue
        enabled = parse_bool(settings.get("enabled"), enabled)
        required = parse_bool(settings.get("skip_direct_connection"), required)
        host = settings.get("host", host) or host
        port = parse_int(settings.get("port"), port)
        connect_timeout_seconds = timeout_ms_to_seconds(settings.get("connect_timeout_ms"), int(connect_timeout_seconds * 1000))
        ack_timeout_seconds = timeout_ms_to_seconds(settings.get("ack_timeout_ms"), int(ack_timeout_seconds * 1000))
        source_parts.append(source_name)

    enabled = enabled or required
    source = ",".join(dict.fromkeys(source_parts)) if source_parts else "defaults"
    return SharedGatewayExpectation(
        enabled=enabled,
        required=required,
        host=host,
        port=port,
        connect_timeout_seconds=connect_timeout_seconds,
        ack_timeout_seconds=ack_timeout_seconds,
        source=source,
    )


def collect_shared_gateway_status() -> SharedGatewayStatus:
    expectation = resolve_shared_gateway_expectation()
    pid = read_shared_gateway_pid()
    pid_alive = pid_is_alive(pid)
    listening = port_open(expectation.port) if expectation.port > 0 else False
    observed = pid_alive or listening

    if probe_gateway is None:
        detail = "probe-unavailable: databento_ibkr_bridge.shared_ibkr_gateway_health import failed"
        return SharedGatewayStatus(
            ok=not (expectation.enabled or expectation.required or observed),
            enabled=expectation.enabled,
            required=expectation.required,
            host=expectation.host,
            port=expectation.port,
            connect_timeout_seconds=expectation.connect_timeout_seconds,
            ack_timeout_seconds=expectation.ack_timeout_seconds,
            source=expectation.source,
            detail=detail,
            pid=pid,
            pid_alive=pid_alive,
            port_open=listening,
            protocol_ok=False,
            connected=None,
            dry_run=None,
            connect_latency_ms=None,
            ping_latency_ms=None,
            snapshot_latency_ms=None,
        )

    if expectation.enabled or expectation.required or observed:
        result = probe_gateway(
            expectation.host,
            expectation.port,
            connect_timeout_seconds=expectation.connect_timeout_seconds,
            ack_timeout_seconds=expectation.ack_timeout_seconds,
            require_connected=True,
        )
        return SharedGatewayStatus(
            ok=bool(result.ok),
            enabled=expectation.enabled,
            required=expectation.required,
            host=expectation.host,
            port=expectation.port,
            connect_timeout_seconds=expectation.connect_timeout_seconds,
            ack_timeout_seconds=expectation.ack_timeout_seconds,
            source=expectation.source,
            detail=result.detail,
            pid=pid,
            pid_alive=pid_alive,
            port_open=listening,
            protocol_ok=bool(result.ok),
            connected=result.connected,
            dry_run=result.dry_run,
            connect_latency_ms=result.connect_latency_ms,
            ping_latency_ms=result.ping_latency_ms,
            snapshot_latency_ms=result.snapshot_latency_ms,
        )

    return SharedGatewayStatus(
        ok=True,
        enabled=False,
        required=False,
        host=expectation.host,
        port=expectation.port,
        connect_timeout_seconds=expectation.connect_timeout_seconds,
        ack_timeout_seconds=expectation.ack_timeout_seconds,
        source=expectation.source,
        detail="not-configured",
        pid=pid,
        pid_alive=pid_alive,
        port_open=listening,
        protocol_ok=False,
        connected=None,
        dry_run=None,
        connect_latency_ms=None,
        ping_latency_ms=None,
        snapshot_latency_ms=None,
    )


def recent_log_issue(app_log_file: str) -> Optional[str]:
    if not app_log_file:
        return None
    path = REPO_ROOT / app_log_file
    if not path.exists() or not path.is_file():
        return None
    try:
        lines = path.read_text(errors="ignore").splitlines()[-160:]
    except OSError:
        return None

    patterns = [
        re.compile(r"maximum API connection threshold \(default 32\) is not exceeded", re.IGNORECASE),
        re.compile(r"Couldn't connect to TWS", re.IGNORECASE),
        re.compile(r"Initial connection failed", re.IGNORECASE),
        re.compile(r"Max reconnection attempts reached", re.IGNORECASE),
        re.compile(r"Address already in use", re.IGNORECASE),
    ]
    for line in reversed(lines):
        stripped = line.strip()
        if any(pattern.search(stripped) for pattern in patterns):
            return stripped
    return None


def collect_symbol_status(symbol: str) -> SymbolStatus:
    props_path = resolve_symbol_properties_path(symbol)
    props = read_properties(props_path)
    port = int(props.get("server.port", "0") or 0)
    client_id = props.get("trading.client-id", "")
    model_dir = props.get("trading.model.dir", "")
    app_log_file = props.get("logging.file.name", "")
    listening = port_open(port) if port else False
    health = http_json(f"http://127.0.0.1:{port}/actuator/health") if listening else None
    control = http_json(f"http://127.0.0.1:{port}/api/control/status") if listening else None
    feed_health = http_json(f"http://127.0.0.1:{port}/api/control/feed-health") if listening else None
    health_ok = bool(health and health.get("status") == "UP")
    if not health_ok:
        health_ok = databento_feed_health_ok(control, feed_health)
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
        recent_issue=recent_log_issue(app_log_file),
        status_payload=control or {},
    )


def latest_morning_log() -> Optional[Path]:
    return latest_matching_file("morning-launch-*.log")


def print_db_validation(status: DbValidationStatus, expected_symbols: list[str]) -> None:
    print("\n=== PostgreSQL Validation ===")
    print(f"market_day={status.market_day} checked={status.checked} ok={status.ok} user={status.username or '<missing>'}")
    print(f"url={status.url or '<missing>'}")
    print(f"detail={status.detail}")
    print("required_tables=" + ",".join(live_process_log_table_name(symbol) for symbol in expected_symbols))
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


def print_shared_gateway_status(status: SharedGatewayStatus) -> None:
    print("\n=== Shared IBKR Gateway ===")
    print(
        f"ok={status.ok} enabled={status.enabled} required={status.required} "
        f"host={status.host} port={status.port} source={status.source} "
        f"connect_timeout_s={status.connect_timeout_seconds} ack_timeout_s={status.ack_timeout_seconds}"
    )
    print(
        f"pid={status.pid or '<missing>'} pid_alive={status.pid_alive} port_open={status.port_open} "
        f"protocol_ok={status.protocol_ok} connected={status.connected} dry_run={status.dry_run}"
    )
    print(f"detail={status.detail}")
    if status.connect_latency_ms is not None:
        print(
            f"latency_ms: connect={status.connect_latency_ms} "
            f"ping={status.ping_latency_ms if status.ping_latency_ms is not None else 'n/a'} "
            f"snapshot={status.snapshot_latency_ms if status.snapshot_latency_ms is not None else 'n/a'}"
        )


def main() -> int:
    expected_symbols = discover_expected_symbols()
    shared_gateway_status = collect_shared_gateway_status()

    print("=== Launchd Jobs ===")
    for label in LAUNCH_LABELS:
        loaded, detail = launchd_job_loaded(label)
        print(f"{label}: loaded={loaded} detail={detail}")

    ml = latest_morning_log()
    print("\n=== Morning Launcher Log ===")
    print(str(ml) if ml else "missing")

    print("\n=== Symbol Bots ===")
    print(f"expected_symbols={','.join(expected_symbols)}")
    all_ok = True
    for symbol in expected_symbols:
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
        if not ok and status.recent_issue:
            print(f"  recent_issue: {status.recent_issue}")

    db_status = validate_database(expected_symbols)
    print_shared_gateway_status(shared_gateway_status)
    print_db_validation(db_status, expected_symbols)
    if shared_gateway_status.required:
        all_ok = all_ok and shared_gateway_status.ok
    if db_status.checked:
        all_ok = all_ok and db_status.ok

    print("\n=== Summary ===")
    print(f"ALL_STARTED_OK={all_ok}")
    if all_ok:
        print("All expected morning services are up: every bot is listening/healthy/connected, shared gateway health passed when required, and DB validation passed.")
        return 0
    if shared_gateway_status.required and not shared_gateway_status.ok:
        print("Shared IBKR gateway is required for this morning stack, but the protocol health probe did not pass. Check the Shared IBKR Gateway section above and the gateway log in runtime/databento/logs/.")
        return 1
    if db_status.checked and not db_status.ok:
        print("One or more services are up, but required live process log DB validation did not pass. Check the PostgreSQL section above for missing required rows or tables.")
        return 1
    print("One or more services are not fully ready. Check the logs above and the launchd stdout/stderr files in runtime/.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


