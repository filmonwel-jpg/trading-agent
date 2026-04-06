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

    cfg = BridgeConfig(
        databento_api_key=databento_api_key,
        databento_dataset=os.getenv("DATABENTO_DATASET", "GLBX.MDP3"),
        databento_schema=os.getenv("DATABENTO_SCHEMA", "mbp-1"),
        ibkr_host=os.getenv("IBKR_HOST", "127.0.0.1"),
        ibkr_port=int(os.getenv("IBKR_PORT", "7497")),
        ibkr_client_id_base=int(os.getenv("IBKR_CLIENT_ID_BASE", "210")),
        symbols_file=symbols_file,
        symbols=symbols,
        dry_run=_parse_bool(os.getenv("DRY_RUN"), default=True),
        max_symbols=max_symbols,
        order_notional_usd=float(os.getenv("ORDER_NOTIONAL_USD", "10000")),
        output_root=(project_root.parent / os.getenv("OUTPUT_ROOT", "runtime/databento/output")).resolve(),
        log_root=(project_root.parent / os.getenv("LOG_ROOT", "runtime/databento/logs")).resolve(),
        state_root=(project_root.parent / os.getenv("STATE_ROOT", "runtime/databento/state")).resolve(),
    )

    if not cfg.dry_run and not cfg.databento_api_key:
        raise ValueError("DATABENTO_API_KEY is required when DRY_RUN=false")

    return cfg

