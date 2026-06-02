from __future__ import annotations

import argparse
import json
import socket
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import flatten_all_via_shared_ibkr_gateway as flatten


class GatewayClientTest(unittest.TestCase):
    def test_command_ignores_stale_ack_and_returns_matching_ack(self) -> None:
        server = _OneShotJsonServer(self._send_stale_then_matching_ack)
        server.start()

        with flatten.GatewayClient("127.0.0.1", server.port, connect_timeout_seconds=1.0, command_timeout_seconds=1.0) as client:
            ack = client.command("snapshot")

        self.assertTrue(ack["ok"])
        self.assertEqual("snapshot", ack["detail"])
        server.join()

    def test_command_socket_timeout_has_actionable_context(self) -> None:
        server = _OneShotJsonServer(lambda _request, _writer: time.sleep(0.4))
        server.start()

        with flatten.GatewayClient("127.0.0.1", server.port, connect_timeout_seconds=1.0, command_timeout_seconds=0.1) as client:
            with self.assertRaises(TimeoutError) as raised:
                client.command("snapshot")

        message = str(raised.exception)
        self.assertIn("command=snapshot", message)
        self.assertIn("--command-timeout-seconds", message)
        server.join()

    def test_default_command_timeout_uses_flatten_timeout_capped_at_thirty_seconds(self) -> None:
        args = argparse.Namespace(command_timeout_seconds=None, connect_timeout_seconds=5.0, timeout_seconds=90)

        self.assertEqual(30.0, flatten.resolve_command_timeout_seconds(args))

    def test_explicit_command_timeout_wins(self) -> None:
        args = argparse.Namespace(command_timeout_seconds=12.5, connect_timeout_seconds=5.0, timeout_seconds=90)

        self.assertEqual(12.5, flatten.resolve_command_timeout_seconds(args))

    def test_zero_remaining_pending_cancel_order_is_not_unresolved(self) -> None:
        snapshot = {
            "symbols": {"AMD": {"open_gateway_order_ids": [21000000]}},
            "orders": [
                {
                    "gateway_order_id": 21000000,
                    "symbol": "AMD",
                    "status": "PendingCancel",
                    "remaining_quantity": 0,
                    "filled_quantity": 0,
                }
            ],
        }

        self.assertEqual([], flatten.unresolved_symbol_orders(snapshot, "AMD"))

    def test_positive_remaining_pending_cancel_order_still_blocks(self) -> None:
        snapshot = {
            "symbols": {"AMD": {"open_gateway_order_ids": [21000000]}},
            "orders": [
                {
                    "gateway_order_id": 21000000,
                    "symbol": "AMD",
                    "status": "PendingCancel",
                    "remaining_quantity": 312,
                    "filled_quantity": 0,
                }
            ],
        }

        self.assertEqual(1, len(flatten.unresolved_symbol_orders(snapshot, "AMD")))

    def test_max_stuck_polls_zero_disables_early_stuck_failure(self) -> None:
        states = [
            {"AMD": {"position": 5}},
            {"AMD": {"position": 0}},
        ]

        with patch.object(flatten, "sync_positions", side_effect=states), patch.object(flatten.time, "sleep", return_value=None):
            confirmed, failed = flatten.wait_for_flat(
                object(),
                ["AMD"],
                {"AMD": 5},
                timeout_seconds=60,
                poll_interval_seconds=1,
                max_stuck_polls=0,
            )

        self.assertEqual({"AMD"}, confirmed)
        self.assertEqual(set(), failed)

    def test_sync_positions_rejects_hidden_gateway_position_sync_error(self) -> None:
        client = _ScriptedGatewayClient(
            {
                "request_position_sync": {"ok": True, "payload": {"reqId": 42}},
                "snapshot": {
                    "ok": True,
                    "payload": {
                        "connected": True,
                        "symbols": {"AMD": {"position": 0}},
                        "recent_events": [
                            {"event_type": "error", "detail": "position-sync-failed reason=positions unavailable"},
                            {"event_type": "position_sync_completed", "req_id": 42},
                        ],
                    },
                },
            }
        )

        with self.assertRaises(RuntimeError) as raised:
            flatten.sync_positions(client)

        self.assertIn("position sync error", str(raised.exception))
        self.assertIn("refusing to treat missing broker positions as flat", str(raised.exception))

    def test_sync_positions_accepts_completed_position_sync_snapshot(self) -> None:
        client = _ScriptedGatewayClient(
            {
                "request_position_sync": {"ok": True, "payload": {"reqId": 43}},
                "snapshot": {
                    "ok": True,
                    "payload": {
                        "connected": True,
                        "symbols": {"AMD": {"position": 11}},
                        "recent_events": [
                            {"event_type": "position_updated", "symbol": "AMD", "position": 11, "req_id": 43},
                            {"event_type": "position_sync_completed", "req_id": 43},
                        ],
                    },
                },
            }
        )

        self.assertEqual({"AMD": {"position": 11}}, flatten.sync_positions(client))

    def test_after_hours_limit_price_uses_ask_plus_one_percent_for_cover(self) -> None:
        price, basis = flatten.after_hours_limit_price("BUY", {"ask": 100.0, "bid": 99.5}, 1.0)

        self.assertEqual("ask", basis)
        self.assertEqual(101.0, price)

    def test_after_hours_limit_price_uses_bid_minus_one_percent_for_sell(self) -> None:
        price, basis = flatten.after_hours_limit_price("SELL", {"bid": 100.0, "ask": 100.5}, 1.0)

        self.assertEqual("bid", basis)
        self.assertEqual(99.0, price)

    def test_after_hours_flatten_submits_lmt_outside_rth_payload_for_cover(self) -> None:
        client = _RecordingGatewayClient(quote={"symbol": "AMD", "bid": 99.5, "ask": 100.0, "last": 99.75})

        result = flatten.flatten_symbol(
            client,
            "AMD",
            -7,
            False,
            force_new_order=False,
            cancel_only=False,
            after_hours_limit=True,
            limit_offset_pct=1.0,
            after_hours_limit_tif="DAY",
            allow_after_hours=True,
        )

        self.assertTrue(result)
        submit = client.submitted_payload
        self.assertEqual("BUY", submit["action"])
        self.assertEqual(7, submit["quantity"])
        self.assertEqual("LMT", submit["orderType"])
        self.assertEqual(101.0, submit["limitPrice"])
        self.assertTrue(submit["outsideRth"])
        self.assertTrue(submit["allowAfterHours"])
        self.assertEqual("DAY", submit["tif"])

    def test_main_logs_completion_when_all_selected_symbols_are_flat(self) -> None:
        logs: list[str] = []
        args = argparse.Namespace(
            host="127.0.0.1",
            port=9910,
            symbols="",
            exclude="",
            list=False,
            dry_run=False,
            execute=True,
            timeout_seconds=90,
            poll_interval_seconds=5,
            max_stuck_polls=0,
            connect_timeout_seconds=5.0,
            command_timeout_seconds=None,
            continue_on_failure=True,
            cancel_only=False,
            force_new_order=False,
            skip_blocked=False,
            after_hours_limit=False,
            allow_after_hours=False,
            limit_offset_pct=1.0,
            after_hours_limit_tif="DAY",
        )

        class _DummyClient:
            def __enter__(self):
                return object()

            def __exit__(self, *_args):
                return None

        with (
            patch.object(flatten, "parse_args", return_value=args),
            patch.object(flatten, "discover_targets", return_value=[flatten.Target("AMD", "9084", Path("trading-amd.properties"))]),
            patch.object(flatten, "GatewayClient", return_value=_DummyClient()),
            patch.object(flatten, "register_symbols", return_value=None),
            patch.object(flatten, "sync_positions", return_value={"AMD": {"position": 0}}),
            patch.object(flatten, "sync_open_orders", return_value={"orders": [], "symbols": {"AMD": {}}}),
            patch.object(flatten, "log", side_effect=logs.append),
        ):
            result = flatten.main()

        self.assertEqual(0, result)
        self.assertIn("direct gateway flatten complete confirmed=all-selected-flat failed=none skipped=none", logs)

    def _send_stale_then_matching_ack(self, request: dict, writer) -> None:
        writer.write(json.dumps({"type": "ack", "requestId": "stale-request", "ok": True, "detail": "old"}) + "\n")
        writer.flush()
        writer.write(json.dumps({"type": "ack", "requestId": request["requestId"], "ok": True, "detail": "snapshot", "payload": {}}) + "\n")
        writer.flush()


class _OneShotJsonServer:
    def __init__(self, handler) -> None:
        self._handler = handler
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = int(self._sock.getsockname()[1])
        self._thread = threading.Thread(target=self._serve_once, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def join(self) -> None:
        self._thread.join(timeout=2.0)
        self._sock.close()

    def _serve_once(self) -> None:
        try:
            conn, _addr = self._sock.accept()
        except OSError:
            return
        with conn:
            reader = conn.makefile("r", encoding="utf-8", newline="\n")
            writer = conn.makefile("w", encoding="utf-8", newline="\n")
            raw = reader.readline()
            if not raw:
                return
            request = json.loads(raw)
            try:
                self._handler(request, writer)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass


class _RecordingGatewayClient:
    def __init__(self, quote: dict) -> None:
        self.quote = quote
        self.submitted_payload: dict = {}

    def command(self, command: str, *, symbol: str = "", payload: dict | None = None) -> dict:
        if command == "cancel_symbol_orders":
            return {"ok": True, "detail": "cancel-requested", "payload": {"cancelledCount": 0}}
        if command == "request_open_orders_sync":
            return {"ok": True, "detail": "open-orders-sync-requested", "payload": {}}
        if command == "snapshot":
            return {"ok": True, "detail": "snapshot", "payload": {"connected": True, "orders": [], "symbols": {symbol.upper(): {}}}}
        if command == "quote_snapshot":
            return {"ok": True, "detail": "quote-snapshot", "payload": dict(self.quote)}
        if command == "submit_order":
            self.submitted_payload = dict(payload or {})
            return {
                "ok": True,
                "detail": "order-submitted",
                "payload": {
                    "action": self.submitted_payload.get("action"),
                    "quantity": self.submitted_payload.get("quantity"),
                    "permId": 123,
                    "status": "PendingSubmit",
                },
            }
        raise AssertionError(f"unexpected command={command}")


class _ScriptedGatewayClient:
    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses

    def command(self, command: str, *, symbol: str = "", payload: dict | None = None) -> dict:
        if command not in self.responses:
            raise AssertionError(f"unexpected command={command}")
        return dict(self.responses[command])


if __name__ == "__main__":
    unittest.main()
