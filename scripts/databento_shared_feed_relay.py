#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def split_csv(raw_value: str) -> list[str]:
    values: list[str] = []
    for part in (raw_value or "").split(","):
        item = part.strip().upper()
        if item and item not in values:
            values.append(item)
    return values


def normalize_option_parent(raw_value: str) -> str:
    token = str(raw_value or "").strip().upper()
    if not token:
        return ""
    if "." not in token:
        return f"{token}.OPT"
    root, suffix = token.rsplit(".", 1)
    root = root.strip().upper()
    suffix = suffix.strip().upper()
    if root and suffix in {"FUT", "OPT", "SPOT"}:
        return f"{root}.{suffix}"
    return token


def normalize_option_parents(raw_value: str) -> list[str]:
    values: list[str] = []
    for part in split_csv(raw_value):
        token = normalize_option_parent(part)
        if token and token not in values:
            values.append(token)
    return values


def parse_int(raw_value: str | None, default: int | None = None) -> int | None:
    if raw_value is None:
        return default
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default


def is_truthy(raw_value: str | None, default: bool = False) -> bool:
    normalized = str(raw_value or "").strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "on"}


def discover_symbols_and_option_parents(bots_dir: Path, listen_port: int) -> tuple[list[str], list[str]]:
    property_files = sorted(
        path for path in bots_dir.glob("trading-*.properties") if path.name != "trading-databento-template.properties"
    )
    symbols: list[str] = []
    option_parents: list[str] = []
    for path in property_files:
        props = read_properties(path)
        if not is_truthy(props.get("trading.databento.shared-feed.enabled"), default=True):
            continue
        configured_port = parse_int(props.get("trading.databento.shared-feed.port"), default=9800)
        if configured_port != listen_port:
            continue
        symbol = (props.get("trading.symbol") or path.stem.replace("trading-", "")).strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        for parent in normalize_option_parents(props.get("trading.databento.option-parents") or symbol):
            if parent and parent not in option_parents:
                option_parents.append(parent)
    return symbols, option_parents


@dataclass
class RelayClient:
    socket: socket.socket
    address: tuple[str, int]
    connected_at: float
    symbol: str = ""
    client_key: str = ""
    registered: bool = False


class SharedFeedRelay:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.server_socket: socket.socket | None = None
        self.clients_by_socket: dict[socket.socket, RelayClient] = {}
        self.registered_clients: dict[str, RelayClient] = {}
        self.clients_lock = threading.Lock()
        self.normalizer_process: subprocess.Popen[str] | None = None
        self.accept_thread: threading.Thread | None = None
        self.stdout_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None
        self.started_at = time.time()
        self.normalizer_started_at = 0.0
        self.last_normalizer_payload_at = 0.0
        self.last_market_data_at = 0.0
        self.last_equity_payload_at = 0.0
        self.last_option_payload_at = 0.0
        self.last_status_payload_at = 0.0
        self.last_status_message = ""
        self.equity_payload_count = 0
        self.option_payload_count = 0
        self.status_payload_count = 0
        self.last_no_data_diagnostic_at = 0.0

    def log(self, message: str) -> None:
        sys.stderr.write(f"[DATABENTO-RELAY][{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")
        sys.stderr.flush()

    def shard_label(self) -> str:
        shard_index = self.args.listen_port - 9800
        if shard_index >= 0:
            return f"shard-{shard_index}"
        return f"port-{self.args.listen_port}"

    def write_pid_file(self) -> None:
        pid_file = Path(self.args.pid_file) if self.args.pid_file else None
        if not pid_file:
            return
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")

    def remove_pid_file(self) -> None:
        pid_file = Path(self.args.pid_file) if self.args.pid_file else None
        if not pid_file:
            return
        try:
            if pid_file.exists():
                pid_file.unlink()
        except OSError:
            pass

    def start_server(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.args.listen_host, self.args.listen_port))
        server.listen()
        server.settimeout(1.0)
        self.server_socket = server
        self.log(f"relay-listening shard={self.shard_label()} host={self.args.listen_host} port={self.args.listen_port}")
        self.accept_thread = threading.Thread(target=self.accept_loop, name="databento-relay-accept", daemon=True)
        self.accept_thread.start()

    def accept_loop(self) -> None:
        assert self.server_socket is not None
        while not self.stop_event.is_set():
            try:
                client, address = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stop_event.is_set():
                    self.log("relay-accept-loop terminated unexpectedly")
                break
            client.setblocking(True)
            relay_client = RelayClient(socket=client, address=address, connected_at=time.time())
            with self.clients_lock:
                self.clients_by_socket[client] = relay_client
            self.log(
                f"client-connected address={address[0]}:{address[1]} raw_clients={self.raw_client_count()} registered_clients={self.client_count()}"
            )
            threading.Thread(
                target=self.client_reader_loop,
                args=(relay_client,),
                name=f"databento-relay-client-{address[0]}:{address[1]}",
                daemon=True,
            ).start()
            threading.Thread(
                target=self.expire_unregistered_client,
                args=(relay_client,),
                name=f"databento-relay-expire-{address[0]}:{address[1]}",
                daemon=True,
            ).start()

    def expire_unregistered_client(self, relay_client: RelayClient) -> None:
        time.sleep(max(0.5, self.args.client_registration_timeout_seconds))
        with self.clients_lock:
            current = self.clients_by_socket.get(relay_client.socket)
            if current is None or current.registered:
                return
        self.log(
            f"client-registration-timeout address={relay_client.address[0]}:{relay_client.address[1]} timeoutSeconds={self.args.client_registration_timeout_seconds}"
        )
        self.remove_client(relay_client, "registration-timeout")

    def client_reader_loop(self, relay_client: RelayClient) -> None:
        try:
            reader = relay_client.socket.makefile("r", encoding="utf-8")
            for raw_line in reader:
                payload = raw_line.strip()
                if not payload:
                    continue
                try:
                    message = json.loads(payload)
                except json.JSONDecodeError:
                    self.log(
                        f"client-invalid-json address={relay_client.address[0]}:{relay_client.address[1]} payload={payload[:160]}"
                    )
                    continue
                self.handle_client_message(relay_client, message)
        except OSError as exc:
            if not self.stop_event.is_set():
                self.log(
                    f"client-reader-error address={relay_client.address[0]}:{relay_client.address[1]} reason={exc}"
                )
        finally:
            self.remove_client(relay_client, "reader-exit")

    def handle_client_message(self, relay_client: RelayClient, message: dict[str, object]) -> None:
        msg_type = str(message.get("type") or "").strip().lower()
        if msg_type in {"probe", "ping", "health"}:
            self.send_status(
                relay_client.socket,
                f"relay_probe_ack shard={self.shard_label()} host={self.args.listen_host} port={self.args.listen_port} registeredClients={self.client_count()} expectedClients={self.expected_client_count()}",
            )
            return
        if msg_type != "register":
            self.log(
                f"client-unhandled-message address={relay_client.address[0]}:{relay_client.address[1]} type={msg_type or 'unknown'}"
            )
            return

        symbol = str(message.get("symbol") or "").strip().upper()
        client_key = str(message.get("clientKey") or "").strip()
        if not client_key:
            fallback_symbol = symbol or relay_client.address[0]
            client_key = f"{fallback_symbol}:{relay_client.address[1]}"

        duplicate: RelayClient | None = None
        with self.clients_lock:
            current = self.clients_by_socket.get(relay_client.socket)
            if current is None:
                return
            duplicate = self.registered_clients.get(client_key)
            if duplicate is not None and duplicate.socket is relay_client.socket:
                relay_client.symbol = symbol or relay_client.symbol
                relay_client.client_key = client_key
                relay_client.registered = True
            else:
                if duplicate is not None:
                    self.registered_clients.pop(client_key, None)
                relay_client.symbol = symbol
                relay_client.client_key = client_key
                relay_client.registered = True
                self.registered_clients[client_key] = relay_client
            count = len(self.registered_clients)
            if count >= self.expected_client_count():
                self.ready_event.set()

        if duplicate is not None and duplicate.socket is not relay_client.socket:
            self.log(
                f"client-duplicate-replaced clientKey={client_key} oldAddress={duplicate.address[0]}:{duplicate.address[1]} newAddress={relay_client.address[0]}:{relay_client.address[1]}"
            )
            self.remove_client(duplicate, "duplicate-replaced")

        self.send_status(
            relay_client.socket,
            f"relay_client_registered shard={self.shard_label()} host={self.args.listen_host} port={self.args.listen_port} symbol={relay_client.symbol or 'UNKNOWN'} clientKey={client_key} registeredClients={count} expectedClients={self.expected_client_count()}",
        )
        self.log(
            f"client-registered symbol={relay_client.symbol or 'UNKNOWN'} clientKey={client_key} registeredClients={count} expectedClients={self.expected_client_count()}"
        )

    def send_status(self, client: socket.socket, message: str) -> None:
        try:
            client.sendall((json.dumps({"event": "status", "message": message}) + "\n").encode("utf-8"))
        except OSError:
            pass

    def broadcast_status(self, message: str) -> None:
        self.broadcast(json.dumps({"event": "status", "message": message}) + "\n")

    def remove_client(self, relay_client: RelayClient, reason: str) -> None:
        with self.clients_lock:
            current = self.clients_by_socket.pop(relay_client.socket, None)
            if current is None:
                return
            if current.registered and current.client_key:
                existing = self.registered_clients.get(current.client_key)
                if existing is not None and existing.socket is current.socket:
                    self.registered_clients.pop(current.client_key, None)
            if self.client_count_unlocked() < self.expected_client_count():
                self.ready_event.clear()
        try:
            relay_client.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            relay_client.socket.close()
        except OSError:
            pass
        self.log(
            f"client-disconnected reason={reason} address={relay_client.address[0]}:{relay_client.address[1]} raw_clients={self.raw_client_count()} registered_clients={self.client_count()}"
        )

    def client_count_unlocked(self) -> int:
        return len(self.registered_clients)

    def client_count(self) -> int:
        with self.clients_lock:
            return self.client_count_unlocked()

    def raw_client_count(self) -> int:
        with self.clients_lock:
            return len(self.clients_by_socket)

    def expected_client_count(self) -> int:
        return max(1, int(self.args.expected_client_count))

    def wait_for_expected_clients(self) -> None:
        expected = self.expected_client_count()
        if self.client_count() >= expected:
            self.ready_event.set()
            return
        timeout_seconds = max(0.5, float(self.args.wait_for_clients_timeout_seconds))
        self.log(
            f"waiting-for-clients expectedClients={expected} timeoutSeconds={timeout_seconds} registeredClients={self.client_count()}"
        )
        ready = self.ready_event.wait(timeout_seconds)
        if ready:
            self.log(
                f"client-quorum-reached registeredClients={self.client_count()} expectedClients={expected}"
            )
            return
        self.log(
            f"client-quorum-timeout registeredClients={self.client_count()} expectedClients={expected} timeoutSeconds={timeout_seconds} proceeding=true"
        )

    def build_normalizer_command(self, symbols: list[str], option_parents: list[str]) -> list[str]:
        command = [
            self.args.python_bin,
            self.args.normalizer_script,
            "--symbols",
            ",".join(symbols),
            "--option-parents",
            ",".join(option_parents),
            "--equity-dataset",
            self.args.equity_dataset,
            "--equity-schema",
            self.args.equity_schema,
            "--startup-history-seconds",
            str(max(0.0, float(self.args.startup_history_seconds))),
            "--startup-history-schema",
            self.args.startup_history_schema,
            "--options-dataset",
            self.args.options_dataset,
            "--options-schema",
            self.args.options_schema,
            "--equity-flush-lag-ms",
            str(max(0.0, float(self.args.equity_flush_lag_ms))),
            "--heartbeat-seconds",
            str(max(1, self.args.heartbeat_seconds)),
            "--startup-delay-seconds",
            str(max(0.0, float(self.args.startup_delay_seconds))),
        ]
        live_gateway = str(getattr(self.args, "live_gateway", "") or "").strip()
        if live_gateway:
            command.extend(["--live-gateway", live_gateway])
        if bool(getattr(self.args, "emit_live_feature_snapshots", False)):
            command.append("--emit-live-feature-snapshots")
        feature_snapshot_source = str(getattr(self.args, "feature_snapshot_source", "") or "").strip()
        if feature_snapshot_source:
            command.extend(["--feature-snapshot-source", feature_snapshot_source])
        feature_snapshot_schema_version = str(getattr(self.args, "feature_snapshot_schema_version", "") or "").strip()
        if feature_snapshot_schema_version:
            command.extend(["--feature-snapshot-schema-version", feature_snapshot_schema_version])
        return command

    def start_normalizer(self) -> None:
        bots_dir = Path(self.args.bots_dir)
        symbols, option_parents = discover_symbols_and_option_parents(bots_dir, int(self.args.listen_port))
        if not symbols:
            raise SystemExit(f"No bot property files found under {bots_dir}")
        command = self.build_normalizer_command(symbols, option_parents or symbols)
        self.log(
            f"starting-normalizer shard={self.shard_label()} symbols={len(symbols)} option_parents={len(option_parents or symbols)} command={' '.join(command)}"
        )
        self.normalizer_started_at = time.time()
        self.last_normalizer_payload_at = 0.0
        self.last_market_data_at = 0.0
        self.last_equity_payload_at = 0.0
        self.last_option_payload_at = 0.0
        self.last_status_payload_at = 0.0
        self.last_status_message = ""
        self.equity_payload_count = 0
        self.option_payload_count = 0
        self.status_payload_count = 0
        self.last_no_data_diagnostic_at = 0.0
        self.normalizer_process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=str(Path(self.args.working_dir).resolve()),
            env=os.environ.copy(),
        )
        self.stdout_thread = threading.Thread(target=self.read_normalizer_stdout, name="databento-relay-stdout", daemon=True)
        self.stderr_thread = threading.Thread(target=self.read_normalizer_stderr, name="databento-relay-stderr", daemon=True)
        self.stdout_thread.start()
        self.stderr_thread.start()

    def read_normalizer_stdout(self) -> None:
        assert self.normalizer_process is not None
        assert self.normalizer_process.stdout is not None
        for line in self.normalizer_process.stdout:
            payload = line.strip()
            if not payload:
                continue
            self.track_normalizer_payload(payload)
            self.broadcast(payload + "\n")
        if not self.stop_event.is_set():
            self.log("normalizer-stdout-closed")
            self.stop_event.set()

    def read_normalizer_stderr(self) -> None:
        assert self.normalizer_process is not None
        assert self.normalizer_process.stderr is not None
        for line in self.normalizer_process.stderr:
            message = line.rstrip()
            if message:
                self.log(f"normalizer-stderr {message}")

    def track_normalizer_payload(self, payload: str) -> None:
        now = time.time()
        self.last_normalizer_payload_at = now
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            return
        if not isinstance(message, dict):
            return
        event_type = str(message.get("event") or "").strip().lower()
        if event_type == "status":
            self.status_payload_count += 1
            self.last_status_payload_at = now
            self.last_status_message = str(message.get("message") or "").strip()
            if self._should_log_normalizer_status(self.last_status_message):
                self.log(f"normalizer-status {self.last_status_message}")
            return
        if event_type == "equity_bar":
            self.equity_payload_count += 1
            self.last_equity_payload_at = now
            self.last_market_data_at = now
            return
        if event_type == "option_bar":
            self.option_payload_count += 1
            self.last_option_payload_at = now
            self.last_market_data_at = now

    def maybe_emit_no_data_diagnostic(self) -> None:
        if self.normalizer_process is None or self.normalizer_process.poll() is not None:
            return
        if self.client_count() <= 0:
            return
        now = time.time()
        startup_grace_seconds = max(
            20.0,
            float(self.args.startup_delay_seconds) + max(15.0, float(self.args.heartbeat_seconds) * 2.0),
        )
        if self.normalizer_started_at <= 0.0 or (now - self.normalizer_started_at) < startup_grace_seconds:
            return
        if self.equity_payload_count > 0:
            return
        diagnostic_interval = max(15.0, float(self.args.heartbeat_seconds))
        if (now - self.last_no_data_diagnostic_at) < diagnostic_interval:
            return
        self.last_no_data_diagnostic_at = now
        message = (
            f"relay-no-equity-data shard={self.shard_label()} host={self.args.listen_host} port={self.args.listen_port} "
            f"registeredClients={self.client_count()} expectedClients={self.expected_client_count()} "
            f"equityEvents={self.equity_payload_count} optionEvents={self.option_payload_count} statusEvents={self.status_payload_count} "
            f"normalizerPayloadAgeSec={self._age_seconds(now, self.last_normalizer_payload_at):.1f} "
            f"statusAgeSec={self._age_seconds(now, self.last_status_payload_at):.1f} "
            f"uptimeSec={now - self.started_at:.1f} normalizerUptimeSec={now - self.normalizer_started_at:.1f} "
            f"lastStatus={self._quote_status_message(self.last_status_message)}"
        )
        self.log(message)
        self.broadcast_status(message)

    def _age_seconds(self, now: float, last_ts: float) -> float:
        if last_ts <= 0.0:
            return -1.0
        return max(0.0, now - last_ts)

    def _quote_status_message(self, message: str) -> str:
        compact = " ".join(str(message or "").split())
        if not compact:
            return '""'
        return json.dumps(compact[:220])

    def _should_log_normalizer_status(self, message: str) -> bool:
        normalized = str(message or "").strip().lower()
        if not normalized:
            return False
        return normalized.startswith(
            (
                "equity-subscribe",
                "options-subscribe",
                "equity-stream-fatal",
                "options-stream-fatal",
                "equity-stream-error",
                "options-stream-error",
                "equity-error",
                "options-error",
                "normalizer-fatal",
                "missing databento_api_key",
            )
        )

    def select_clients_for_payload(self, payload: str) -> list[RelayClient]:
        with self.clients_lock:
            registered_clients = list(self.registered_clients.values())
        if not registered_clients:
            return []

        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            return registered_clients

        if not isinstance(message, dict):
            return registered_clients

        event_type = str(message.get("event") or "").strip().lower()
        if event_type == "equity_bar":
            symbol = str(message.get("symbol") or "").strip().upper()
            if symbol:
                return [client for client in registered_clients if client.symbol.upper() == symbol]
            return []
        if event_type == "option_bar":
            underlying = str(message.get("underlying") or "").strip().upper()
            if underlying:
                return [client for client in registered_clients if client.symbol.upper() == underlying]
            return []
        return registered_clients

    def broadcast(self, payload: str) -> None:
        data = payload.encode("utf-8")
        clients = self.select_clients_for_payload(payload)
        stale_clients: list[RelayClient] = []
        for relay_client in clients:
            try:
                relay_client.socket.sendall(data)
            except OSError:
                stale_clients.append(relay_client)
        for relay_client in stale_clients:
            self.remove_client(relay_client, "broadcast-failed")
        if stale_clients:
            self.log(f"dropped-clients count={len(stale_clients)} active_clients={self.client_count()}")

    def shutdown_clients(self) -> None:
        with self.clients_lock:
            clients = list(self.clients_by_socket.values())
            self.clients_by_socket.clear()
            self.registered_clients.clear()
            self.ready_event.clear()
        for relay_client in clients:
            try:
                relay_client.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                relay_client.socket.close()
            except OSError:
                pass

    def shutdown_server(self) -> None:
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None

    def shutdown_normalizer(self) -> int:
        if self.normalizer_process is None:
            return 0
        process = self.normalizer_process
        if process.poll() is None:
            process.terminate()
            try:
                return process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                return process.wait(timeout=5)
        return process.wait(timeout=1)

    def run(self) -> int:
        self.write_pid_file()
        self.start_server()
        self.wait_for_expected_clients()
        self.start_normalizer()
        exit_code = 0
        try:
            while not self.stop_event.is_set():
                process = self.normalizer_process
                if process is not None:
                    polled = process.poll()
                    if polled is not None:
                        exit_code = polled
                        self.log(f"normalizer-exited code={polled}")
                        self.stop_event.set()
                        break
                self.maybe_emit_no_data_diagnostic()
                time.sleep(0.5)
        finally:
            self.stop_event.set()
            normalizer_exit = self.shutdown_normalizer()
            if exit_code == 0 and normalizer_exit != 0:
                exit_code = normalizer_exit
            self.shutdown_clients()
            self.shutdown_server()
            self.remove_pid_file()
        return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Relay a single shared Databento sidecar stream to multiple local bot clients.")
    parser.add_argument("--python-bin", required=True)
    parser.add_argument("--normalizer-script", required=True)
    parser.add_argument("--bots-dir", required=True)
    parser.add_argument("--working-dir", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=9800)
    parser.add_argument("--pid-file", default="")
    parser.add_argument("--live-gateway", default=os.environ.get("DATABENTO_LIVE_GATEWAY", ""))
    parser.add_argument("--equity-dataset", default="EQUS.MINI")
    parser.add_argument("--equity-schema", default="tbbo")
    parser.add_argument("--startup-history-seconds", type=float, default=360.0)
    parser.add_argument("--startup-history-schema", default="ohlcv-1s")
    parser.add_argument("--options-dataset", default="OPRA.PILLAR")
    parser.add_argument("--options-schema", default="ohlcv-1s")
    parser.add_argument("--equity-flush-lag-ms", type=float, default=250.0)
    parser.add_argument("--heartbeat-seconds", type=int, default=15)
    parser.add_argument("--startup-delay-seconds", type=float, default=0.0)
    parser.add_argument("--emit-live-feature-snapshots", action="store_true", default=os.environ.get("DATABENTO_EMIT_LIVE_FEATURE_SNAPSHOTS", "").strip().lower() in {"1", "true", "yes", "on"})
    parser.add_argument("--feature-snapshot-source", default=os.environ.get("DATABENTO_FEATURE_SNAPSHOT_SOURCE", "live_normalizer_30s_v1"))
    parser.add_argument("--feature-snapshot-schema-version", default=os.environ.get("DATABENTO_FEATURE_SNAPSHOT_SCHEMA_VERSION", "live_normalizer_30s_v1"))
    parser.add_argument("--expected-client-count", type=int, default=1)
    parser.add_argument("--wait-for-clients-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--client-registration-timeout-seconds", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    relay = SharedFeedRelay(args)

    def handle_signal(signum, _frame) -> None:
        relay.log(f"received-signal signum={signum}")
        relay.stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    return relay.run()


if __name__ == "__main__":
    raise SystemExit(main())

