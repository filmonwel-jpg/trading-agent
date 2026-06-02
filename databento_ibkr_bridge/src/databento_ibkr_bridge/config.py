from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from typing import List


@dataclass(frozen=True)
class BridgeConfig:
    databento_api_key: str
    databento_dataset: str
    databento_schema: str
    ibkr_host: str
    ibkr_port: int
    ibkr_client_id_base: int
    symbols_file: Path
    symbols: List[str]
    dry_run: bool
    max_symbols: int
    order_notional_usd: float
    output_root: Path
    log_root: Path
    state_root: Path
    repo_root: Path
    bridge_mode: str
    harvest_db_url: str
    harvest_db_user: str
    harvest_db_password: str
    backfill_run_id: str
    max_orders_per_cycle: int
    min_imbalance_ratio: float
    max_quote_age_minutes: int
    shared_ibkr_gateway_host: str
    shared_ibkr_gateway_port: int
    shared_ibkr_gateway_ib_call_timeout_seconds: float
    shared_ibkr_gateway_qualified_contract_ttl_seconds: float
    shared_ibkr_gateway_require_qualified_contracts: bool
    shared_ibkr_gateway_recovery_enabled: bool
    shared_ibkr_gateway_recovery_cooldown_seconds: float
    shared_ibkr_gateway_recovery_max_cooldown_seconds: float


def _parse_bool(raw: str, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _read_simple_env_file(env_file: Path) -> None:
    """Load KEY=VALUE lines into process env only when key is not already set."""
    if not env_file.exists():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and os.getenv(key) is None:
            os.environ[key] = value


def _load_properties_file(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists() or not path.is_file():
        return props
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def _normalize_postgres_url(url: str) -> str:
    normalized = url.strip()
    if normalized.startswith("jdbc:"):
        normalized = normalized[len("jdbc:") :]
    return normalized


def _load_symbols(symbols_file: Path, max_symbols: int) -> List[str]:
    if not symbols_file.exists():
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")

    symbols: List[str] = []
    seen = set()
    for line in symbols_file.read_text(encoding="utf-8").splitlines():
        symbol = line.strip().upper()
        if not symbol or symbol.startswith("#"):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        raise ValueError(f"No symbols loaded from: {symbols_file}")

    return symbols[:max_symbols]


def load_config(project_root: Path) -> BridgeConfig:
    env_file = project_root / ".env"
    _read_simple_env_file(env_file)

    symbols_file = project_root / os.getenv("SYMBOLS_FILE", "config/symbols_100.txt")
    max_symbols = int(os.getenv("MAX_SYMBOLS", "100"))
    symbols = _load_symbols(symbols_file, max_symbols=max_symbols)

    databento_api_key = os.getenv("DATABENTO_API_KEY", "")
    repo_root = project_root.parent.resolve()

    props: dict[str, str] = {}
    props.update(_load_properties_file(repo_root / "src" / "main" / "resources" / "application.properties"))
    props.update(_load_properties_file(repo_root / "runtime" / "postgres-local.properties"))

    db_url = (
        os.getenv("HARVEST_DB_URL")
        or os.getenv("SPRING_DATASOURCE_URL")
        or props.get("spring.datasource.url", "")
    ).strip()
    db_user = (
        os.getenv("HARVEST_DB_USER")
        or os.getenv("SPRING_DATASOURCE_USERNAME")
        or props.get("spring.datasource.username", "")
    ).strip()
    db_password = (
        os.getenv("HARVEST_DB_PASSWORD")
        or os.getenv("SPRING_DATASOURCE_PASSWORD")
        or props.get("spring.datasource.password", "")
    )

    cfg = BridgeConfig(
        databento_api_key=databento_api_key,
        databento_dataset=os.getenv("DATABENTO_DATASET", "DBEQ.BASIC"),
        databento_schema=os.getenv("DATABENTO_SCHEMA", "tbbo"),
        ibkr_host=os.getenv("IBKR_HOST", "127.0.0.1"),
        ibkr_port=int(os.getenv("IBKR_PORT", "7497")),
        ibkr_client_id_base=int(os.getenv("IBKR_CLIENT_ID_BASE", "210")),
        symbols_file=symbols_file,
        symbols=symbols,
        dry_run=_parse_bool(os.getenv("DRY_RUN"), default=True),
        max_symbols=max_symbols,
        order_notional_usd=float(os.getenv("ORDER_NOTIONAL_USD", "10000")),
        output_root=(repo_root / os.getenv("OUTPUT_ROOT", "runtime/databento/output")).resolve(),
        log_root=(repo_root / os.getenv("LOG_ROOT", "runtime/databento/logs")).resolve(),
        state_root=(repo_root / os.getenv("STATE_ROOT", "runtime/databento/state")).resolve(),
        repo_root=repo_root,
        bridge_mode=os.getenv("BRIDGE_MODE", "scaffold").strip().lower(),
        harvest_db_url=_normalize_postgres_url(db_url),
        harvest_db_user=db_user,
        harvest_db_password=db_password,
        backfill_run_id=os.getenv("BACKFILL_RUN_ID", "").strip(),
        max_orders_per_cycle=max(1, int(os.getenv("MAX_ORDERS_PER_CYCLE", "20"))),
        min_imbalance_ratio=float(os.getenv("MIN_IMBALANCE_RATIO", "0.35")),
        max_quote_age_minutes=max(1, int(os.getenv("MAX_QUOTE_AGE_MINUTES", "30"))),
        shared_ibkr_gateway_host=os.getenv("SHARED_IBKR_GATEWAY_HOST", "127.0.0.1").strip() or "127.0.0.1",
        shared_ibkr_gateway_port=max(1, int(os.getenv("SHARED_IBKR_GATEWAY_PORT", "9910"))),
        shared_ibkr_gateway_ib_call_timeout_seconds=max(0.5, float(os.getenv("SHARED_IBKR_GATEWAY_IB_CALL_TIMEOUT_SECONDS", "4.0"))),
        shared_ibkr_gateway_qualified_contract_ttl_seconds=max(1.0, float(os.getenv("SHARED_IBKR_GATEWAY_QUALIFIED_CONTRACT_TTL_SECONDS", "21600"))),
        shared_ibkr_gateway_require_qualified_contracts=_parse_bool(os.getenv("SHARED_IBKR_GATEWAY_REQUIRE_QUALIFIED_CONTRACTS"), default=False),
        shared_ibkr_gateway_recovery_enabled=_parse_bool(os.getenv("SHARED_IBKR_GATEWAY_RECOVERY_ENABLED"), default=True),
        shared_ibkr_gateway_recovery_cooldown_seconds=max(0.05, float(os.getenv("SHARED_IBKR_GATEWAY_RECOVERY_COOLDOWN_SECONDS", "5.0"))),
        shared_ibkr_gateway_recovery_max_cooldown_seconds=max(0.05, float(os.getenv("SHARED_IBKR_GATEWAY_RECOVERY_MAX_COOLDOWN_SECONDS", "60.0"))),
    )

    if not cfg.dry_run and not cfg.databento_api_key:
        raise ValueError("DATABENTO_API_KEY is required when DRY_RUN=false")

    if cfg.bridge_mode == "consume_backfill" and not cfg.harvest_db_url:
        raise ValueError("HARVEST_DB_URL or spring.datasource.url is required for BRIDGE_MODE=consume_backfill")

    return cfg

