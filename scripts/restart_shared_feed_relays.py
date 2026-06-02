#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BOTS_DIR = REPO_ROOT / "runtime" / "databento" / "bots"
DEFAULT_ENV_FILE = REPO_ROOT / "runtime" / "databento.env"
DEFAULT_PYTHON_BIN = "/Users/filmonghezehey/miniforge3/bin/python3"


@dataclass
class RelayConfig:
    shard_id: int
    port: int
    expected_clients: int
    startup_history_seconds: float
    startup_history_schema: str
    pid_file: Path
    log_file: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restart Databento shared-feed relay shards using generated bot configs.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--bots-dir", default=str(BOTS_DIR))
    parser.add_argument("--python-bin", default=DEFAULT_PYTHON_BIN)
    parser.add_argument("--normalizer-script", default=str(REPO_ROOT / "scripts" / "databento_live_normalizer.py"))
    parser.add_argument("--relay-script", default=str(REPO_ROOT / "scripts" / "databento_shared_feed_relay.py"))
    parser.add_argument("--working-dir", default=str(REPO_ROOT))
    parser.add_argument("--port-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def parse_float(raw_value: str | None, default: float) -> float:
    try:
        return float(str(raw_value).strip()) if raw_value is not None and str(raw_value).strip() else default
    except (TypeError, ValueError):
        return default


def discover_relay_configs(bots_dir: Path, repo_root: Path) -> list[RelayConfig]:
    configs: dict[int, RelayConfig] = {}
    for path in sorted(bots_dir.glob("trading-*.properties")):
        if path.name == "trading-databento-template.properties":
            continue
        props = read_properties(path)
        shard_id = int(props.get("trading.databento.shared-feed.shard-id", "0") or 0)
        if shard_id in configs:
            continue
        configs[shard_id] = RelayConfig(
            shard_id=shard_id,
            port=int(props.get("trading.databento.shared-feed.port", "9800") or 9800),
            expected_clients=int(props.get("trading.databento.shared-feed.expected-client-count", "1") or 1),
            startup_history_seconds=parse_float(
                props.get("trading.databento.shared-feed.startup-history-seconds")
                or props.get("trading.databento.startup-history-seconds"),
                0.0,
            ),
            startup_history_schema=props.get("trading.databento.startup-history-schema", "ohlcv-1s") or "ohlcv-1s",
            pid_file=(repo_root / props.get("trading.databento.shared-feed.pid-file", "runtime/databento/shared-feed-relay.pid")).resolve(),
            log_file=(repo_root / props.get("trading.databento.shared-feed.log-file", "runtime/databento/logs/databento-shared-feed-relay.log")).resolve(),
        )
    return [configs[shard_id] for shard_id in sorted(configs)]


def port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def wait_for_port_state(port: int, *, want_open: bool, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if port_open(port) == want_open:
            return
        time.sleep(0.25)
    state = "open" if want_open else "closed"
    raise TimeoutError(f"port {port} did not become {state} within {timeout_seconds:.1f}s")


def stop_relay(config: RelayConfig, timeout_seconds: float) -> None:
    if config.pid_file.exists():
        try:
            pid = int(config.pid_file.read_text(encoding="utf-8").strip())
            os.kill(pid, signal.SIGTERM)
        except (ValueError, ProcessLookupError, PermissionError, OSError):
            pass
    try:
        wait_for_port_state(config.port, want_open=False, timeout_seconds=timeout_seconds)
    except TimeoutError:
        pass


def start_relay(config: RelayConfig, args: argparse.Namespace, bots_dir: Path) -> None:
    config.log_file.parent.mkdir(parents=True, exist_ok=True)
    config.pid_file.parent.mkdir(parents=True, exist_ok=True)
    command = [
        args.python_bin,
        args.relay_script,
        "--python-bin",
        args.python_bin,
        "--normalizer-script",
        args.normalizer_script,
        "--bots-dir",
        str(bots_dir),
        "--working-dir",
        args.working_dir,
        "--listen-host",
        "127.0.0.1",
        "--listen-port",
        str(config.port),
        "--pid-file",
        str(config.pid_file),
        "--equity-dataset",
        "EQUS.MINI",
        "--equity-schema",
        "tbbo",
        "--startup-history-seconds",
        str(max(0.0, config.startup_history_seconds)),
        "--startup-history-schema",
        config.startup_history_schema,
        "--options-dataset",
        "OPRA.PILLAR",
        "--options-schema",
        "ohlcv-1s",
        "--heartbeat-seconds",
        "15",
        "--startup-delay-seconds",
        "40",
        "--expected-client-count",
        str(config.expected_clients),
        "--wait-for-clients-timeout-seconds",
        "60",
    ]
    with config.log_file.open("a", encoding="utf-8") as handle:
        subprocess.Popen(
            command,
            cwd=args.working_dir,
            env=os.environ.copy(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    wait_for_port_state(config.port, want_open=True, timeout_seconds=args.port_timeout_seconds)


def main() -> int:
    args = parse_args()
    bots_dir = Path(args.bots_dir).resolve()
    load_env_file(Path(args.env_file).resolve())
    configs = discover_relay_configs(bots_dir, REPO_ROOT)
    if not configs:
        raise SystemExit(f"No relay configs discovered from {bots_dir}")
    for config in configs:
        print(f"[RELAY-RESTART] stopping shard={config.shard_id} port={config.port} expectedClients={config.expected_clients} startupHistorySeconds={config.startup_history_seconds:g}")
        stop_relay(config, timeout_seconds=args.port_timeout_seconds)
    if args.dry_run:
        return 0
    for config in configs:
        print(f"[RELAY-RESTART] starting shard={config.shard_id} port={config.port} expectedClients={config.expected_clients} startupHistorySeconds={config.startup_history_seconds:g}")
        start_relay(config, args, bots_dir)
    print(f"[RELAY-RESTART] restarted {len(configs)} shard relays")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

