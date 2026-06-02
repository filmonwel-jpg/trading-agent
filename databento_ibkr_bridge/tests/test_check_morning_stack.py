from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRIDGE_SRC_ROOT = PROJECT_ROOT / "databento_ibkr_bridge" / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(BRIDGE_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(BRIDGE_SRC_ROOT))

import check_morning_stack


class CheckMorningStackSharedGatewayTest(unittest.TestCase):
    def test_extract_shared_gateway_settings_from_text_reads_protocol_flags(self) -> None:
        settings = check_morning_stack.extract_shared_gateway_settings_from_text(
            "run_symbol.sh TSLA -- --trading.ibkr.shared-gateway.enabled=true "
            "--trading.ibkr.shared-gateway.host=10.0.0.9 "
            "--trading.ibkr.shared-gateway.port=19910 "
            "--trading.ibkr.shared-gateway.skip-direct-connection=true"
        )

        self.assertEqual("true", settings["enabled"])
        self.assertEqual("10.0.0.9", settings["host"])
        self.assertEqual("19910", settings["port"])
        self.assertEqual("true", settings["skip_direct_connection"])

    def test_resolve_shared_gateway_expectation_prefers_runtime_process_flags(self) -> None:
        with (
            patch.object(check_morning_stack, "read_properties", return_value={}),
            patch.object(
                check_morning_stack,
                "running_process_commands",
                return_value=(
                    "java -jar trading-agent.jar --trading.ibkr.shared-gateway.enabled=true "
                    "--trading.ibkr.shared-gateway.host=127.0.0.8 "
                    "--trading.ibkr.shared-gateway.port=19910 "
                    "--trading.ibkr.shared-gateway.skip-direct-connection=true"
                ),
            ),
            patch.object(check_morning_stack, "latest_shared_gateway_config_texts", return_value=[]),
        ):
            expectation = check_morning_stack.resolve_shared_gateway_expectation()

        self.assertTrue(expectation.enabled)
        self.assertTrue(expectation.required)
        self.assertEqual("127.0.0.8", expectation.host)
        self.assertEqual(19910, expectation.port)
        self.assertIn("active-processes", expectation.source)

    def test_collect_shared_gateway_status_uses_protocol_probe_when_required(self) -> None:
        expectation = check_morning_stack.SharedGatewayExpectation(
            enabled=True,
            required=True,
            host="127.0.0.1",
            port=9910,
            connect_timeout_seconds=3.0,
            ack_timeout_seconds=5.0,
            source="active-processes",
        )
        probe_result = SimpleNamespace(
            ok=True,
            detail="ok",
            connected=True,
            dry_run=False,
            connect_latency_ms=1.5,
            ping_latency_ms=2.5,
            snapshot_latency_ms=3.5,
        )

        with (
            patch.object(check_morning_stack, "resolve_shared_gateway_expectation", return_value=expectation),
            patch.object(check_morning_stack, "read_shared_gateway_pid", return_value="12345"),
            patch.object(check_morning_stack, "pid_is_alive", return_value=True),
            patch.object(check_morning_stack, "port_open", return_value=True),
            patch.object(check_morning_stack, "probe_gateway", return_value=probe_result),
        ):
            status = check_morning_stack.collect_shared_gateway_status()

        self.assertTrue(status.ok)
        self.assertTrue(status.protocol_ok)
        self.assertTrue(status.connected)
        self.assertEqual("ok", status.detail)
        self.assertEqual(1.5, status.connect_latency_ms)
        self.assertEqual(2.5, status.ping_latency_ms)
        self.assertEqual(3.5, status.snapshot_latency_ms)

    def test_collect_shared_gateway_status_is_ok_when_not_configured(self) -> None:
        expectation = check_morning_stack.SharedGatewayExpectation(
            enabled=False,
            required=False,
            host="127.0.0.1",
            port=9910,
            connect_timeout_seconds=3.0,
            ack_timeout_seconds=5.0,
            source="defaults",
        )

        with (
            patch.object(check_morning_stack, "resolve_shared_gateway_expectation", return_value=expectation),
            patch.object(check_morning_stack, "read_shared_gateway_pid", return_value=""),
            patch.object(check_morning_stack, "pid_is_alive", return_value=False),
            patch.object(check_morning_stack, "port_open", return_value=False),
            patch.object(check_morning_stack, "probe_gateway") as mock_probe,
        ):
            status = check_morning_stack.collect_shared_gateway_status()

        mock_probe.assert_not_called()
        self.assertTrue(status.ok)
        self.assertFalse(status.protocol_ok)
        self.assertEqual("not-configured", status.detail)


if __name__ == "__main__":
    unittest.main()

