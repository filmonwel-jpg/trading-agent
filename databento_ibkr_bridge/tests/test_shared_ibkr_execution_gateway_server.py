from __future__ import annotations

import json
import socket
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from databento_ibkr_bridge.config import BridgeConfig
from databento_ibkr_bridge.shared_ibkr_execution_gateway import SharedIBKRExecutionGateway
from databento_ibkr_bridge.shared_ibkr_execution_gateway_server import SharedIbkrGatewayProtocolServer
from databento_ibkr_bridge.shared_ibkr_execution_models import GatewayEvent, GatewayEventType, GatewaySnapshot, OrderSubmitCommand


class SharedIbkrExecutionGatewayServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cfg = self._build_config(Path(self.temp_dir.name))

    def test_request_position_sync_emits_symbol_state_and_completion(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self.cfg, dry_run=True)
        gateway.connect()
        gateway.register_symbol("AAPL")
        gateway.drain_events()

        req_id = gateway.request_position_sync()
        events = gateway.drain_events()
        event_types = [event.event_type.value for event in events]
        position_events = [event for event in events if event.event_type == GatewayEventType.POSITION_UPDATED]

        self.assertIn(GatewayEventType.POSITION_SYNC_COMPLETED.value, event_types)
        self.assertEqual(1, len(position_events))
        self.assertEqual("AAPL", position_events[0].symbol)
        self.assertEqual(0, position_events[0].position)
        self.assertEqual(req_id, position_events[0].req_id)

    def test_snapshot_retains_position_sync_completion_after_event_drain(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self.cfg, dry_run=True)
        gateway.connect()
        gateway.register_symbol("AAPL")
        gateway.drain_events()

        req_id = gateway.request_position_sync()
        gateway.drain_events()
        snapshot = gateway.snapshot()

        matching_completed = [
            event
            for event in snapshot.recent_events
            if event["event_type"] == GatewayEventType.POSITION_SYNC_COMPLETED.value and event["req_id"] == req_id
        ]
        matching_positions = [
            event
            for event in snapshot.recent_events
            if event["event_type"] == GatewayEventType.POSITION_UPDATED.value and event["req_id"] == req_id and event["symbol"] == "AAPL"
        ]
        self.assertEqual(1, len(matching_completed))
        self.assertEqual(1, len(matching_positions))

    def test_live_request_position_sync_uses_req_positions_snapshot(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        fake_ib = _FakeIBClient(positions=[_FakePosition("AMD", 17, 123.45)])
        gateway._connected = True
        gateway._ib = fake_ib
        gateway.register_symbol("AMD")
        gateway.drain_events()

        req_id = gateway.request_position_sync()
        snapshot = gateway.snapshot()

        self.assertEqual(1, fake_ib.req_positions_calls)
        self.assertEqual(17, snapshot.symbols["AMD"]["position"])
        self.assertEqual(123.45, snapshot.symbols["AMD"]["avg_cost"])
        completed = [event for event in gateway.drain_events() if event.event_type == GatewayEventType.POSITION_SYNC_COMPLETED]
        self.assertEqual(req_id, completed[-1].req_id)

    def test_live_request_position_sync_provides_event_loop_in_ib_worker(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient(
            positions=[_FakePosition("AMD", 17, 123.45)],
            require_event_loop_for_positions=True,
        )
        gateway._connected = True
        gateway._ib = fake_ib
        gateway.register_symbol("AMD")
        gateway.drain_events()

        gateway.request_position_sync()

        self.assertIsNotNone(fake_ib.positions_event_loop)
        self.assertTrue(fake_ib.positions_thread_name.startswith("shared-ibkr-gateway-ib"))

    def test_live_connect_and_position_sync_share_ib_worker_event_loop(self) -> None:
        fake_ib = _FakeIBClient(
            positions=[_FakePosition("AMD", 17, 123.45)],
            require_event_loop_for_positions=True,
        )
        self._install_fake_ib_async(ib_factory=lambda: fake_ib)
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)

        gateway.connect()
        gateway.register_symbol("AMD")
        gateway.request_position_sync()

        self.assertIsNotNone(fake_ib.connect_event_loop)
        self.assertIs(fake_ib.connect_event_loop, fake_ib.positions_event_loop)
        self.assertTrue(fake_ib.connect_thread_name.startswith("shared-ibkr-gateway-ib"))
        self.assertEqual(fake_ib.connect_thread_name, fake_ib.positions_thread_name)

    def test_live_request_position_sync_failure_does_not_emit_completion(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        fake_ib = _FakeIBClient(positions_error=RuntimeError("positions unavailable"))
        gateway._connected = True
        gateway._ib = fake_ib
        gateway.register_symbol("AMD")
        gateway.drain_events()

        with self.assertRaises(RuntimeError):
            gateway.request_position_sync()
        events = gateway.drain_events()

        self.assertTrue(any(event.event_type == GatewayEventType.ERROR and "position-sync-failed" in event.detail for event in events))
        self.assertFalse(any(event.event_type == GatewayEventType.POSITION_SYNC_COMPLETED for event in events))

    def test_live_request_position_sync_connection_error_degrades_gateway(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                recovery_cooldown_seconds=0.05,
                recovery_max_cooldown_seconds=0.05,
            )
        )
        fake_ib = _FakeIBClient(positions_error=ConnectionError("Not connected"))
        gateway._connected = True
        gateway._ib = fake_ib
        gateway.register_symbol("AMD")
        gateway.drain_events()

        with self.assertRaises(ConnectionError):
            gateway.request_position_sync()
        snapshot = gateway.snapshot()
        events = gateway.drain_events()

        self.assertFalse(snapshot.connected)
        self.assertTrue(snapshot.degraded)
        self.assertIn("request_position_sync.req_positions", snapshot.degraded_reason)
        self.assertTrue(fake_ib.disconnected)
        self.assertIsNotNone(snapshot.next_recovery_attempt_in_seconds)
        self.assertFalse(any(event.event_type == GatewayEventType.POSITION_SYNC_COMPLETED for event in events))

    def test_submit_order_coerces_entries_to_fast_limit_and_default_exits_to_market(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self.cfg, dry_run=True)
        gateway.connect()

        long_entry = gateway.submit_order(
            OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25)
        )
        short_entry = gateway.submit_order(
            OrderSubmitCommand(symbol="AAPL", action="SELL", quantity=10, order_type="FAST_LMT", reference_price=101.0)
        )
        long_exit = gateway.submit_order(
            OrderSubmitCommand(symbol="AAPL", action="SELL", quantity=10, order_type="MKT", reference_price=100.5, metadata={"closing_trade": True})
        )
        cover = gateway.submit_order(
            OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.75, metadata={"closing_trade": True})
        )

        self.assertEqual("FAST_LMT", long_entry.order_type)
        self.assertEqual("FAST_LMT", short_entry.order_type)
        self.assertEqual("MKT", long_exit.order_type)
        self.assertEqual("MKT", cover.order_type)

    def test_explicit_closing_limit_order_sets_outside_rth_on_live_ib_order(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        routed = gateway.submit_order(
            OrderSubmitCommand(
                symbol="AAPL",
                action="BUY",
                quantity=10,
                order_type="LMT",
                reference_price=101.0,
                limit_price=102.01,
                tif="DAY",
                outside_rth=True,
                metadata={"closing_trade": True},
            )
        )

        self.assertEqual("LMT", routed.order_type)
        self.assertEqual(102.01, routed.limit_price)
        self.assertTrue(routed.outside_rth)
        self.assertIsNotNone(fake_ib.last_order)
        self.assertEqual(102.01, fake_ib.last_order.lmtPrice)
        self.assertEqual("DAY", fake_ib.last_order.tif)
        self.assertTrue(fake_ib.last_order.outsideRth)

    def test_quote_snapshot_returns_bid_and_ask(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        quote = gateway.quote_snapshot("AAPL")

        self.assertEqual("AAPL", quote["symbol"])
        self.assertEqual(99.5, quote["bid"])
        self.assertEqual(100.0, quote["ask"])
        self.assertEqual(1, fake_ib.qualify_calls)

    def test_duplicate_active_closing_order_is_suppressed(self) -> None:
        gateway = SharedIBKRExecutionGateway.from_config(self.cfg, dry_run=True)
        gateway.connect()

        first = gateway.submit_order(
            OrderSubmitCommand(
                symbol="PLTR",
                action="SELL",
                quantity=66,
                order_type="MKT",
                reference_price=133.36,
                metadata={"closing_trade": True},
            )
        )
        second = gateway.submit_order(
            OrderSubmitCommand(
                symbol="PLTR",
                action="SELL",
                quantity=66,
                order_type="MKT",
                reference_price=133.20,
                metadata={"closing_trade": True},
            )
        )

        self.assertEqual(first.gateway_order_id, second.gateway_order_id)
        snapshot = gateway.snapshot()
        self.assertEqual([first.gateway_order_id], snapshot.symbols["PLTR"]["open_gateway_order_ids"])
        self.assertEqual(1, len(snapshot.orders))
        duplicate_events = [event for event in gateway.drain_events() if event.detail == "duplicate-closing-order-suppressed"]
        self.assertEqual(1, len(duplicate_events))
        self.assertEqual(first.gateway_order_id, duplicate_events[0].gateway_order_id)

    def test_server_pushes_async_events_without_waiting_for_next_command(self) -> None:
        server = SharedIbkrGatewayProtocolServer(self.cfg, dry_run=True)
        server.gateway = _FakeGateway()
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(lambda: self._shutdown_server(server, server_thread))

        port = server._server.server_address[1]
        deadline = time.time() + 5.0
        while time.time() < deadline and not server.gateway.connected:
            time.sleep(0.05)

        with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
            sock_file = sock.makefile("r", encoding="utf-8")
            raw_line = sock_file.readline().strip()
            payload = json.loads(raw_line)

        self.assertEqual("event", payload["type"])
        self.assertEqual("position_updated", payload["eventType"])
        self.assertEqual("AAPL", payload["symbol"])
        self.assertEqual(7, payload["position"])

    def test_live_submit_reuses_cached_qualified_contract(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                ib_call_timeout_seconds=0.2,
                qualified_contract_ttl_seconds=60.0,
                require_qualified_contracts=True,
            )
        )
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))
        gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=5, order_type="MKT", reference_price=101.50))

        snapshot = gateway.snapshot()
        self.assertEqual(1, fake_ib.qualify_calls)
        self.assertEqual(2, fake_ib.place_calls)
        self.assertEqual(["AAPL"], snapshot.qualified_contract_symbols)
        self.assertFalse(snapshot.degraded)

    def test_live_submit_defaults_to_unqualified_stock_contract_without_degrading_on_slow_qualification(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(Path(self.temp_dir.name), dry_run=False, ib_call_timeout_seconds=0.05)
        )
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient(qualify_delay_seconds=0.8)
        gateway._connected = True
        gateway._ib = fake_ib

        routed = gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))

        snapshot = gateway.snapshot()
        self.assertEqual("PendingSubmit", routed.status)
        self.assertEqual(0, fake_ib.qualify_calls)
        self.assertEqual(1, fake_ib.place_calls)
        self.assertEqual([], snapshot.qualified_contract_symbols)
        self.assertTrue(snapshot.connected)
        self.assertFalse(snapshot.degraded)
        self.assertFalse(fake_ib.disconnected)

    def test_live_trade_status_callback_emits_fill_and_terminal_events(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        routed = gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))
        gateway.drain_events()
        self.assertIsNotNone(fake_ib.last_trade)

        fake_ib.last_trade.orderStatus.status = "Submitted"
        fake_ib.last_trade.orderStatus.filled = 4
        fake_ib.last_trade.orderStatus.remaining = 6
        fake_ib.last_trade.orderStatus.avgFillPrice = 101.3
        fake_ib.last_trade.statusEvent.emit(fake_ib.last_trade)
        partial_events = [event for event in gateway.drain_events() if event.event_type == GatewayEventType.ORDER_STATUS]

        self.assertEqual(1, len(partial_events))
        self.assertEqual(routed.gateway_order_id, partial_events[0].gateway_order_id)
        self.assertEqual(4, partial_events[0].payload["filled"])
        self.assertEqual(6, partial_events[0].payload["remaining"])

        fake_ib.last_trade.orderStatus.status = "Filled"
        fake_ib.last_trade.orderStatus.filled = 10
        fake_ib.last_trade.orderStatus.remaining = 0
        fake_ib.last_trade.orderStatus.avgFillPrice = 101.4
        fake_ib.last_trade.filledEvent.emit(fake_ib.last_trade)
        terminal_events = [event for event in gateway.drain_events() if event.event_type == GatewayEventType.ORDER_STATUS]

        self.assertEqual(1, len(terminal_events))
        self.assertEqual("Filled", terminal_events[0].payload["status"])
        self.assertEqual(10, terminal_events[0].payload["filled"])
        snapshot = gateway.snapshot()
        self.assertEqual([], snapshot.symbols["AAPL"]["open_gateway_order_ids"])

    def test_open_order_sync_clears_gateway_ids_absent_from_broker_open_trades(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        routed = gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))
        self.assertEqual([routed.gateway_order_id], gateway.snapshot().symbols["AAPL"]["open_gateway_order_ids"])

        fake_ib.open_trades.clear()
        gateway.request_open_orders_sync()

        snapshot = gateway.snapshot()
        self.assertEqual([], snapshot.symbols["AAPL"]["open_gateway_order_ids"])
        cleared_order = next(order for order in snapshot.orders if order["gateway_order_id"] == routed.gateway_order_id)
        self.assertEqual("Inactive", cleared_order["status"])
        self.assertEqual("broker-open-order-absent", cleared_order["last_detail"])
        self.assertEqual(0, cleared_order["remaining_quantity"])

    def test_zero_remaining_pending_cancel_does_not_suppress_new_closing_order(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        first = gateway.submit_order(
            OrderSubmitCommand(
                symbol="AAPL",
                action="SELL",
                quantity=10,
                order_type="MKT",
                reference_price=101.25,
                metadata={"closing_trade": True},
            )
        )
        fake_ib.last_trade.orderStatus.status = "PendingCancel"
        fake_ib.last_trade.orderStatus.remaining = 0
        gateway.request_open_orders_sync()

        snapshot = gateway.snapshot()
        self.assertEqual([], snapshot.symbols["AAPL"]["open_gateway_order_ids"])
        pending_cancel_order = next(order for order in snapshot.orders if order["gateway_order_id"] == first.gateway_order_id)
        self.assertEqual("PendingCancel", pending_cancel_order["status"])
        self.assertEqual(0, pending_cancel_order["remaining_quantity"])

        second = gateway.submit_order(
            OrderSubmitCommand(
                symbol="AAPL",
                action="SELL",
                quantity=10,
                order_type="MKT",
                reference_price=101.25,
                metadata={"closing_trade": True},
            )
        )

        self.assertNotEqual(first.gateway_order_id, second.gateway_order_id)
        self.assertEqual([second.gateway_order_id], gateway.snapshot().symbols["AAPL"]["open_gateway_order_ids"])

    def test_zero_remaining_pending_submit_ioc_is_not_effectively_terminal(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(self._build_config(Path(self.temp_dir.name), dry_run=False))
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient()
        gateway._connected = True
        gateway._ib = fake_ib

        routed = gateway.submit_order(
            OrderSubmitCommand(
                symbol="SOXL",
                action="BUY",
                quantity=53,
                order_type="FAST_LMT",
                reference_price=185.50,
                metadata={"closing_trade": True},
            )
        )
        fake_ib.last_trade.orderStatus.status = "PendingSubmit"
        fake_ib.last_trade.orderStatus.filled = 0
        fake_ib.last_trade.orderStatus.remaining = 0
        gateway.request_open_orders_sync()

        snapshot = gateway.snapshot()
        self.assertEqual([routed.gateway_order_id], snapshot.symbols["SOXL"]["open_gateway_order_ids"])
        pending_submit_order = next(order for order in snapshot.orders if order["gateway_order_id"] == routed.gateway_order_id)
        self.assertEqual("PendingSubmit", pending_submit_order["status"])
        self.assertEqual(0, pending_submit_order["filled_quantity"])
        self.assertEqual(0, pending_submit_order["remaining_quantity"])
        terminal_events = [event for event in gateway.drain_events() if event.gateway_order_id == routed.gateway_order_id and event.event_type == GatewayEventType.ORDER_STATUS]
        self.assertTrue(any(event.payload.get("effective_terminal") is False for event in terminal_events))

        second = gateway.submit_order(
            OrderSubmitCommand(
                symbol="SOXL",
                action="BUY",
                quantity=53,
                order_type="FAST_LMT",
                reference_price=185.75,
                metadata={"closing_trade": True},
            )
        )
        self.assertEqual(routed.gateway_order_id, second.gateway_order_id)

    def test_live_submit_qualification_timeout_degrades_gateway_and_disconnects(self) -> None:
        self._install_fake_ib_async()
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                ib_call_timeout_seconds=0.05,
                qualified_contract_ttl_seconds=60.0,
                require_qualified_contracts=True,
                recovery_cooldown_seconds=0.05,
                recovery_max_cooldown_seconds=0.05,
            )
        )
        self.addCleanup(gateway.disconnect)
        fake_ib = _FakeIBClient(qualify_delay_seconds=0.8)
        gateway._connected = True
        gateway._ib = fake_ib

        with self.assertRaises(TimeoutError):
            gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))

        snapshot = gateway.snapshot()
        self.assertFalse(snapshot.connected)
        self.assertTrue(snapshot.degraded)
        self.assertIn("submit_order.qualify_contracts[AAPL]", snapshot.degraded_reason)
        self.assertTrue(fake_ib.disconnected)
        self.assertEqual([], snapshot.qualified_contract_symbols)
        self.assertFalse(snapshot.recovering)
        self.assertIsNotNone(snapshot.next_recovery_attempt_in_seconds)

    def test_live_gateway_auto_recovers_after_degraded_timeout(self) -> None:
        reconnect_ib = _FakeIBClient()
        self._install_fake_ib_async(ib_factory=_FakeIBFactory([reconnect_ib]))
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                ib_call_timeout_seconds=0.05,
                qualified_contract_ttl_seconds=60.0,
                require_qualified_contracts=True,
                recovery_cooldown_seconds=0.05,
                recovery_max_cooldown_seconds=0.05,
            )
        )
        self.addCleanup(gateway.disconnect)
        stuck_ib = _FakeIBClient(qualify_delay_seconds=0.8)
        gateway._connected = True
        gateway._ib = stuck_ib

        with self.assertRaises(TimeoutError):
            gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))

        recovered_immediately = gateway.maybe_auto_recover()
        self.assertFalse(recovered_immediately)
        time.sleep(0.08)
        self.assertTrue(gateway.maybe_auto_recover())

        snapshot = gateway.snapshot()
        self.assertTrue(snapshot.connected)
        self.assertFalse(snapshot.degraded)
        self.assertFalse(snapshot.recovering)
        self.assertEqual(0, snapshot.recovery_attempts)
        self.assertIsNone(snapshot.next_recovery_attempt_in_seconds)
        self.assertEqual("", snapshot.recovery_last_error)
        self.assertEqual(1, reconnect_ib.connect_calls)

        routed = gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=5, order_type="MKT", reference_price=101.5))
        self.assertEqual("PendingSubmit", routed.status)
        self.assertEqual(1, reconnect_ib.qualify_calls)
        self.assertEqual(1, reconnect_ib.place_calls)

    def test_live_gateway_failed_recovery_uses_backoff_and_surfaces_last_error(self) -> None:
        failing_reconnect_ib = _FakeIBClient(connect_error=RuntimeError("connect boom"))
        self._install_fake_ib_async(ib_factory=_FakeIBFactory([failing_reconnect_ib]))
        gateway = SharedIBKRExecutionGateway.from_config(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                ib_call_timeout_seconds=0.05,
                qualified_contract_ttl_seconds=60.0,
                require_qualified_contracts=True,
                recovery_cooldown_seconds=0.05,
                recovery_max_cooldown_seconds=0.2,
            )
        )
        self.addCleanup(gateway.disconnect)
        stuck_ib = _FakeIBClient(qualify_delay_seconds=0.8)
        gateway._connected = True
        gateway._ib = stuck_ib

        with self.assertRaises(TimeoutError):
            gateway.submit_order(OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=101.25))

        time.sleep(0.08)
        self.assertFalse(gateway.maybe_auto_recover())

        snapshot = gateway.snapshot()
        self.assertFalse(snapshot.connected)
        self.assertTrue(snapshot.degraded)
        self.assertFalse(snapshot.recovering)
        self.assertEqual(1, snapshot.recovery_attempts)
        self.assertIn("connect boom", snapshot.recovery_last_error)
        self.assertGreaterEqual(snapshot.next_recovery_attempt_in_seconds or 0.0, 0.09)
        self.assertTrue(failing_reconnect_ib.disconnected)

    def test_server_background_loop_auto_recovers_gateway_after_timeout(self) -> None:
        startup_ib = _FakeIBClient(qualify_delay_seconds=0.8)
        recovery_ib = _FakeIBClient()
        self._install_fake_ib_async(ib_factory=_FakeIBFactory([startup_ib, recovery_ib]))
        server = SharedIbkrGatewayProtocolServer(
            self._build_config(
                Path(self.temp_dir.name),
                dry_run=False,
                ib_call_timeout_seconds=0.05,
                qualified_contract_ttl_seconds=60.0,
                require_qualified_contracts=True,
                recovery_cooldown_seconds=0.05,
                recovery_max_cooldown_seconds=0.05,
            ),
            dry_run=False,
        )
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(lambda: self._shutdown_server(server, server_thread))

        deadline = time.time() + 5.0
        while time.time() < deadline and not server.gateway.snapshot().connected:
            time.sleep(0.05)

        port = server._server.server_address[1]
        with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
            sock.settimeout(5.0)
            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            writer = sock.makefile("w", encoding="utf-8", newline="\n")
            submit_ack = self._send_protocol_command(
                reader,
                writer,
                command="submit_order",
                symbol="AAPL",
                payload={"action": "BUY", "quantity": 10, "orderType": "MKT", "referencePrice": 101.25},
            )
            self.assertFalse(submit_ack["ok"])
            self.assertIn("TimeoutError", submit_ack["detail"])

            health_payload = {}
            deadline = time.time() + 4.0
            while time.time() < deadline:
                time.sleep(0.2)
                health_ack = self._send_protocol_command(reader, writer, command="health")
                health_payload = dict(health_ack.get("payload") or {})
                if bool(health_payload.get("connected")) and not bool(health_payload.get("degraded")):
                    break

        self.assertTrue(bool(health_payload.get("connected")))
        self.assertFalse(bool(health_payload.get("degraded")))
        self.assertFalse(bool(health_payload.get("recovering")))
        self.assertEqual(1, recovery_ib.connect_calls)
        self.assertGreaterEqual(startup_ib.qualify_calls, 1)

    def _shutdown_server(self, server: SharedIbkrGatewayProtocolServer, server_thread: threading.Thread) -> None:
        try:
            server.shutdown()
        finally:
            server_thread.join(timeout=5.0)

    def _send_protocol_command(self, reader, writer, *, command: str, symbol: str = "", payload: dict | None = None) -> dict:
        request_id = str(uuid.uuid4())
        envelope = {"type": "command", "requestId": request_id, "command": command, "payload": dict(payload or {})}
        if symbol:
            envelope["symbol"] = symbol
        writer.write(json.dumps(envelope) + "\n")
        writer.flush()
        while True:
            raw_line = reader.readline()
            if not raw_line:
                raise RuntimeError(f"unexpected EOF waiting for ack command={command}")
            message = json.loads(raw_line)
            if message.get("type") == "event":
                continue
            if message.get("type") != "ack":
                raise RuntimeError(f"unexpected message type {message.get('type')}")
            if message.get("requestId") != request_id:
                continue
            return message

    def _install_fake_ib_async(self, ib_factory=None) -> None:
        previous = sys.modules.get("ib_async")
        fake_module = types.ModuleType("ib_async")
        fake_module.IB = ib_factory if ib_factory is not None else (lambda: _FakeIBClient())
        fake_module.Stock = _FakeStock
        fake_module.LimitOrder = _FakeLimitOrder
        fake_module.MarketOrder = _FakeMarketOrder
        sys.modules["ib_async"] = fake_module

        def _restore() -> None:
            if previous is None:
                sys.modules.pop("ib_async", None)
            else:
                sys.modules["ib_async"] = previous

        self.addCleanup(_restore)

    def _build_config(
        self,
        temp_root: Path,
        *,
        dry_run: bool = True,
        ib_call_timeout_seconds: float = 4.0,
        qualified_contract_ttl_seconds: float = 21600.0,
        require_qualified_contracts: bool = False,
        recovery_enabled: bool = True,
        recovery_cooldown_seconds: float = 5.0,
        recovery_max_cooldown_seconds: float = 60.0,
    ) -> BridgeConfig:
        return BridgeConfig(
            databento_api_key="unit-test-key",
            databento_dataset="DBEQ.BASIC",
            databento_schema="tbbo",
            ibkr_host="127.0.0.1",
            ibkr_port=7497,
            ibkr_client_id_base=210,
            symbols_file=temp_root / "symbols.txt",
            symbols=["AAPL"],
            dry_run=dry_run,
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
            shared_ibkr_gateway_ib_call_timeout_seconds=ib_call_timeout_seconds,
            shared_ibkr_gateway_qualified_contract_ttl_seconds=qualified_contract_ttl_seconds,
            shared_ibkr_gateway_require_qualified_contracts=require_qualified_contracts,
            shared_ibkr_gateway_recovery_enabled=recovery_enabled,
            shared_ibkr_gateway_recovery_cooldown_seconds=recovery_cooldown_seconds,
            shared_ibkr_gateway_recovery_max_cooldown_seconds=recovery_max_cooldown_seconds,
        )


class _FakeGateway:
    def __init__(self) -> None:
        self.connected = False
        self._events: list[GatewayEvent] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def poll_live_state(self) -> None:
        if self.connected:
            self._events.append(
                GatewayEvent.now(
                    GatewayEventType.POSITION_UPDATED,
                    symbol="AAPL",
                    position=7,
                    avg_cost=101.5,
                    detail="fake-poll",
                )
            )

    def drain_events(self) -> list[GatewayEvent]:
        events = list(self._events)
        self._events.clear()
        return events

    def snapshot(self) -> GatewaySnapshot:
        return GatewaySnapshot(
            connected=self.connected,
            dry_run=True,
            degraded=False,
            degraded_reason="",
            recovering=False,
            recovery_attempts=0,
            next_recovery_attempt_in_seconds=None,
            recovery_last_error="",
            host="127.0.0.1",
            port=9910,
            client_id=210,
            registered_symbols=["AAPL"],
            qualified_contract_symbols=[],
            orders=[],
            symbols={"AAPL": {}},
            recent_events=[],
        )


class _FakeStock:
    def __init__(self, symbol: str, exchange: str, currency: str) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self.currency = currency


class _FakeLimitOrder:
    def __init__(self, action: str, quantity: int, limit_price: float, tif: str = "IOC") -> None:
        self.action = action
        self.totalQuantity = quantity
        self.lmtPrice = limit_price
        self.tif = tif
        self.outsideRth = False


class _FakeMarketOrder:
    def __init__(self, action: str, quantity: int) -> None:
        self.action = action
        self.totalQuantity = quantity
        self.tif = "IOC"
        self.outsideRth = False


class _FakeTicker:
    bid = 99.5
    ask = 100.0
    last = 99.75

    def marketPrice(self) -> float:
        return 99.75


class _FakeEvent:
    def __init__(self) -> None:
        self._handlers = []

    def __iadd__(self, handler):
        self._handlers.append(handler)
        return self

    def emit(self, *args, **kwargs) -> None:
        for handler in list(self._handlers):
            handler(*args, **kwargs)


class _FakeTrade:
    def __init__(self, order_id: int, perm_id: int, contract=None, quantity: int = 0) -> None:
        self.contract = contract
        self.order = types.SimpleNamespace(orderId=order_id, permId=perm_id)
        self.orderStatus = types.SimpleNamespace(status="PendingSubmit", filled=0, remaining=quantity, avgFillPrice=0.0)
        self.statusEvent = _FakeEvent()
        self.modifyEvent = _FakeEvent()
        self.fillEvent = _FakeEvent()
        self.filledEvent = _FakeEvent()
        self.cancelEvent = _FakeEvent()
        self.cancelledEvent = _FakeEvent()
        self.fills = []


class _FakePosition:
    def __init__(self, symbol: str, position: int, avg_cost: float) -> None:
        self.contract = types.SimpleNamespace(symbol=symbol)
        self.position = position
        self.avgCost = avg_cost


class _FakeIBClient:
    def __init__(
        self,
        qualify_delay_seconds: float = 0.0,
        connect_error: Exception | None = None,
        positions: list[_FakePosition] | None = None,
        positions_error: Exception | None = None,
        require_event_loop_for_positions: bool = False,
        connected: bool = True,
    ) -> None:
        self.qualify_delay_seconds = qualify_delay_seconds
        self.connect_error = connect_error
        self.positions_error = positions_error
        self.require_event_loop_for_positions = require_event_loop_for_positions
        self.connected = connected
        self.connect_calls = 0
        self.qualify_calls = 0
        self.place_calls = 0
        self.req_positions_calls = 0
        self.disconnected = False
        self.last_trade = None
        self.last_order = None
        self.open_trades = []
        self.positions_entries = list(positions or [])
        self.connect_event_loop = None
        self.connect_thread_name = ""
        self.positions_event_loop = None
        self.positions_thread_name = ""

    def connect(self, host: str, port: int, clientId: int) -> None:
        self.connect_calls += 1
        import asyncio

        self.connect_event_loop = asyncio.get_event_loop()
        self.connect_thread_name = threading.current_thread().name
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    def isConnected(self) -> bool:
        return self.connected and not self.disconnected

    def qualifyContracts(self, contract):
        self.qualify_calls += 1
        if self.qualify_delay_seconds > 0.0:
            time.sleep(self.qualify_delay_seconds)
        return [contract]

    def placeOrder(self, contract, order):
        self.place_calls += 1
        self.last_order = order
        quantity = int(getattr(order, "totalQuantity", 0) or 0)
        self.last_trade = _FakeTrade(order_id=500 + self.place_calls, perm_id=700000 + self.place_calls, contract=contract, quantity=quantity)
        self.open_trades.append(self.last_trade)
        return self.last_trade

    def reqTickers(self, contract):
        return [_FakeTicker()]

    def reqPositions(self):
        self.req_positions_calls += 1
        if self.require_event_loop_for_positions:
            import asyncio

            self.positions_event_loop = asyncio.get_event_loop()
            self.positions_thread_name = threading.current_thread().name
        if self.positions_error is not None:
            raise self.positions_error
        return list(self.positions_entries)

    def positions(self):
        return list(self.positions_entries)

    def openTrades(self):
        return list(self.open_trades)

    def disconnect(self) -> None:
        self.disconnected = True
        self.connected = False


class _FakeIBFactory:
    def __init__(self, clients: list[_FakeIBClient]) -> None:
        self._clients = list(clients)

    def __call__(self):
        if not self._clients:
            raise RuntimeError("no fake IB clients configured")
        return self._clients.pop(0)


if __name__ == "__main__":
    unittest.main()





