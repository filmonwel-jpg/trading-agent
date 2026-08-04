from __future__ import annotations

import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from databento_ibkr_bridge.config import BridgeConfig
from databento_ibkr_bridge.shared_ibkr_gateway_health import probe_gateway
from databento_ibkr_bridge.shared_ibkr_execution_gateway_server import SharedIbkrGatewayProtocolServer


class SharedIbkrGatewayHealthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cfg = self._build_config(Path(self.temp_dir.name))

    def test_probe_gateway_succeeds_against_real_dry_run_server(self) -> None:
        server = SharedIbkrGatewayProtocolServer(self.cfg, dry_run=True)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(lambda: self._shutdown_server(server, server_thread))

        port = server._server.server_address[1]
        result = probe_gateway("127.0.0.1", port, connect_timeout_seconds=1.0, ack_timeout_seconds=1.0)

        self.assertTrue(result.ok)
        self.assertTrue(result.ping_ok)
        self.assertTrue(result.snapshot_ok)
        self.assertTrue(result.connected)
        self.assertTrue(result.dry_run)

    def test_probe_gateway_fails_for_port_open_non_protocol_listener(self) -> None:
        ready = threading.Event()
        stop = threading.Event()
        port_holder: dict[str, int] = {}
        thread = threading.Thread(target=self._run_non_protocol_listener, args=(ready, stop, port_holder), daemon=True)
        thread.start()
        self.addCleanup(lambda: thread.join(timeout=5.0))
        self.addCleanup(stop.set)

        self.assertTrue(ready.wait(5.0))
        result = probe_gateway("127.0.0.1", port_holder["port"], connect_timeout_seconds=1.0, ack_timeout_seconds=1.0)

        self.assertFalse(result.ok)
        self.assertIn("probe-failed", result.detail)

    def test_probe_gateway_honors_longer_ack_timeout(self) -> None:
        ready = threading.Event()
        stop = threading.Event()
        port_holder: dict[str, int] = {}
        thread = threading.Thread(target=self._run_delayed_protocol_listener, args=(ready, stop, port_holder), daemon=True)
        thread.start()
        self.addCleanup(lambda: thread.join(timeout=5.0))
        self.addCleanup(stop.set)

        self.assertTrue(ready.wait(5.0))
        result = probe_gateway("127.0.0.1", port_holder["port"], connect_timeout_seconds=1.0, ack_timeout_seconds=3.0)

        self.assertTrue(result.ok)
        self.assertGreaterEqual(result.ping_latency_ms or 0.0, 2000.0)
        self.assertGreaterEqual(result.snapshot_latency_ms or 0.0, 2000.0)

    def test_health_payload_reports_stale_ib_client_as_disconnected(self) -> None:
        server = SharedIbkrGatewayProtocolServer(self.cfg, dry_run=True)
        self.addCleanup(server._server.server_close)
        server.gateway.dry_run = False
        server.gateway._connected = True
        server.gateway._ib = _DisconnectedIBClient()

        payload = server._health_payload()

        self.assertFalse(payload["connected"])
        self.assertTrue(payload["gatewayConnectedFlag"])
        self.assertFalse(payload["ibClientConnected"])

    def _shutdown_server(self, server: SharedIbkrGatewayProtocolServer, server_thread: threading.Thread) -> None:
        try:
            server.shutdown()
        finally:
            server_thread.join(timeout=5.0)

    def _run_non_protocol_listener(self, ready: threading.Event, stop: threading.Event, port_holder: dict[str, int]) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(0.25)
            port_holder["port"] = listener.getsockname()[1]
            ready.set()
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except TimeoutError:
                    continue
                with conn:
                    conn.sendall(json.dumps({"type": "event", "eventType": "noise"}).encode("utf-8") + b"\n")
                    time.sleep(0.5)

    def _run_delayed_protocol_listener(self, ready: threading.Event, stop: threading.Event, port_holder: dict[str, int]) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(0.25)
            port_holder["port"] = listener.getsockname()[1]
            ready.set()
            while not stop.is_set():
                try:
                    conn, _ = listener.accept()
                except TimeoutError:
                    continue
                with conn, conn.makefile("r", encoding="utf-8", newline="\n") as reader, conn.makefile("w", encoding="utf-8", newline="\n") as writer:
                    raw_line = reader.readline()
                    if not raw_line:
                        break
                    request = json.loads(raw_line)
                    writer.write(
                        json.dumps(
                            {
                                "type": "ack",
                                "requestId": request.get("requestId"),
                                "ok": False,
                                "detail": "unknown-command=health",
                                "payload": {},
                            }
                        )
                        + "\n"
                    )
                    writer.flush()
                    for command_name in ("ping", "snapshot"):
                        raw_line = reader.readline()
                        if not raw_line:
                            break
                        request = json.loads(raw_line)
                        actual_command = request.get("command")
                        if actual_command != command_name:
                            break
                        time.sleep(2.2)
                        payload = {"connected": True, "dry_run": False} if command_name == "snapshot" else {"connected": True}
                        writer.write(
                            json.dumps(
                                {
                                    "type": "ack",
                                    "requestId": request.get("requestId"),
                                    "ok": True,
                                    "detail": command_name,
                                    "payload": payload,
                                }
                            )
                            + "\n"
                        )
                        writer.flush()
                break

    def _build_config(self, temp_root: Path) -> BridgeConfig:
        return BridgeConfig(
            databento_api_key="unit-test-key",
            databento_dataset="DBEQ.BASIC",
            databento_schema="tbbo",
            ibkr_host="127.0.0.1",
            ibkr_port=7497,
            ibkr_client_id_base=210,
            symbols_file=temp_root / "symbols.txt",
            symbols=["AAPL"],
            dry_run=True,
            max_symbols=1,
            order_notional_usd=10000.0,
            output_root=temp_root / "output",
            log_root=temp_root / "logs",
            state_root=temp_root / "state",
            repo_root=temp_root,
            bridge_mode="shared_ibkr_gateway_server",
            harvest_db_url="",
            harvest_db_user="",
            harvest_db_password="",
            backfill_run_id="",
            max_orders_per_cycle=20,
            min_imbalance_ratio=0.35,
            max_quote_age_minutes=30,
            shared_ibkr_gateway_host="127.0.0.1",
            shared_ibkr_gateway_port=0,
            shared_ibkr_gateway_ib_call_timeout_seconds=4.0,
            shared_ibkr_gateway_qualified_contract_ttl_seconds=21600.0,
            shared_ibkr_gateway_require_qualified_contracts=False,
            shared_ibkr_gateway_recovery_enabled=True,
            shared_ibkr_gateway_recovery_cooldown_seconds=5.0,
            shared_ibkr_gateway_recovery_max_cooldown_seconds=60.0,
        )


class _DisconnectedIBClient:
    def isConnected(self) -> bool:
        return False


if __name__ == "__main__":
    unittest.main()


