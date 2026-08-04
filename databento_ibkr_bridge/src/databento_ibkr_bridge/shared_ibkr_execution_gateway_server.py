from __future__ import annotations

import contextlib
import json
import logging
import socketserver
import threading
import time
from dataclasses import asdict
from typing import Any

from databento_ibkr_bridge.config import BridgeConfig
from databento_ibkr_bridge.shared_ibkr_execution_gateway import SharedIBKRExecutionGateway
from databento_ibkr_bridge.shared_ibkr_execution_models import (
    CancelSymbolOrdersCommand,
    FlattenSymbolCommand,
    GatewayEvent,
    OrderSubmitCommand,
    SymbolRegistration,
    SyncOpenOrdersCommand,
    SyncPositionsCommand,
)


logger = logging.getLogger(__name__)


class SharedIbkrGatewayProtocolServer:
    def __init__(self, cfg: BridgeConfig, dry_run: bool | None = None) -> None:
        self.cfg = cfg
        self.gateway = SharedIBKRExecutionGateway.from_config(cfg, dry_run=dry_run)
        self.host = cfg.shared_ibkr_gateway_host
        self.port = cfg.shared_ibkr_gateway_port
        self._lock = threading.Lock()
        self._gateway_lock = threading.Lock()
        self._handlers: set["_GatewayRequestHandler"] = set()
        self._broadcast_stop = threading.Event()
        self._broadcast_thread: threading.Thread | None = None
        self._call_watchdog_stop = threading.Event()
        self._call_watchdog_thread: threading.Thread | None = None
        self._active_call_lock = threading.Lock()
        self._active_call: dict[str, Any] | None = None
        owner = self

        class _ThreadingServer(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
            request_queue_size = 256

        class _GatewayRequestHandler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                logger.info("shared IBKR gateway client connected client=%s", self.client_address)
                owner._register_handler(self)
                try:
                    while True:
                        raw = self.rfile.readline()
                        if not raw:
                            break
                        payload = raw.decode("utf-8").strip()
                        if not payload:
                            continue
                        response = owner.handle_message(payload)
                        self._send_json(response)
                        owner._broadcast_gateway_events()
                except Exception:
                    logger.exception("shared IBKR gateway client loop failed client=%s", self.client_address)
                finally:
                    owner._unregister_handler(self)
                    logger.info("shared IBKR gateway client disconnected client=%s", self.client_address)

            def _send_json(self, payload: dict[str, Any]) -> None:
                encoded = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
                self.wfile.write(encoded)
                self.wfile.flush()

        self._server = _ThreadingServer((self.host, self.port), _GatewayRequestHandler)

    def serve_forever(self) -> None:
        logger.info(
            "shared IBKR gateway server starting listen=%s:%s dry_run=%s",
            self.host,
            self.port,
            getattr(self.gateway, "dry_run", False),
        )
        self._start_call_watchdog()
        with self._track_gateway_call("startup.connect", request_id="startup", command="startup", symbol=""):
            with self._gateway_lock:
                self.gateway.connect()
        self._broadcast_stop.clear()
        self._broadcast_thread = threading.Thread(target=self._background_event_loop, name="shared-ibkr-gateway-broadcast", daemon=True)
        self._broadcast_thread.start()
        try:
            logger.info("shared IBKR gateway server accepting clients listen=%s:%s", self.host, self.port)
            self._server.serve_forever()
        finally:
            self._broadcast_stop.set()
            if self._broadcast_thread is not None:
                self._broadcast_thread.join(timeout=2.0)
                self._broadcast_thread = None
            with self._track_gateway_call("shutdown.disconnect", request_id="shutdown", command="shutdown", symbol=""):
                with self._gateway_lock:
                    self.gateway.disconnect()
            self._stop_call_watchdog()
            self._server.server_close()
            logger.info("shared IBKR gateway server stopped listen=%s:%s", self.host, self.port)

    def shutdown(self) -> None:
        self._broadcast_stop.set()
        self._server.shutdown()
        self._server.server_close()
        if self._broadcast_thread is not None:
            self._broadcast_thread.join(timeout=2.0)
            self._broadcast_thread = None
        with self._track_gateway_call("shutdown.disconnect", request_id="shutdown", command="shutdown", symbol=""):
            with self._gateway_lock:
                self.gateway.disconnect()
        self._stop_call_watchdog()

    def _register_handler(self, handler) -> None:
        with self._lock:
            self._handlers.add(handler)

    def _unregister_handler(self, handler) -> None:
        with self._lock:
            self._handlers.discard(handler)

    def _broadcast_gateway_events(self) -> None:
        with self._gateway_lock:
            events = self.gateway.drain_events()
        if not events:
            return
        payloads = [self._event_payload(event) for event in events]
        with self._lock:
            handlers = list(self._handlers)
        dead_handlers = []
        for handler in handlers:
            try:
                for payload in payloads:
                    handler._send_json(payload)
            except Exception:
                dead_handlers.append(handler)
        if dead_handlers:
            with self._lock:
                for handler in dead_handlers:
                    self._handlers.discard(handler)

    def _background_event_loop(self) -> None:
        while not self._broadcast_stop.is_set():
            try:
                with self._track_gateway_call(
                    "background.poll_live_state",
                    request_id="background-poll",
                    command="poll_live_state",
                    symbol="",
                    log_start=False,
                    log_success=False,
                ):
                    with self._gateway_lock:
                        maybe_auto_recover = getattr(self.gateway, "maybe_auto_recover", None)
                        if callable(maybe_auto_recover):
                            maybe_auto_recover()
                        self.gateway.poll_live_state()
                self._broadcast_gateway_events()
            except Exception:
                logger.exception("shared IBKR gateway background poll failed")
            self._broadcast_stop.wait(0.25)

    def handle_message(self, raw_message: str) -> dict[str, Any]:
        request_id = ""
        command = ""
        symbol = ""
        stage = "request-parse"
        try:
            request = json.loads(raw_message)
            request_id = str(request.get("requestId", "")).strip()
            command = str(request.get("command", "")).strip().lower()
            symbol = str(request.get("symbol", "")).strip().upper()
            payload = request.get("payload") or {}
            if not request_id:
                return self._ack(ok=False, request_id="", detail="missing-request-id")
            if command == "health":
                return self._ack(ok=True, request_id=request_id, detail="health", payload=self._health_payload())
            if command == "ping":
                stage = "command.ping.snapshot"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        connected = self.gateway.snapshot().connected
                return self._ack(ok=True, request_id=request_id, detail="pong", payload={"connected": connected})
            if command == "register_symbol":
                stage = "command.register_symbol"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        state = self.gateway.register_symbol(
                            SymbolRegistration(symbol=symbol, strategy_id=str(payload.get("strategyId", "")).strip())
                        )
                return self._ack(
                    ok=True,
                    request_id=request_id,
                    detail="symbol-registered",
                    payload={"symbol": state.symbol, "requestIds": dict(state.request_ids)},
                )
            if command == "request_position_sync":
                stage = "command.request_position_sync"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        req_id = self.gateway.request_position_sync(SyncPositionsCommand(reason=str(payload.get("reason", "")).strip()))
                return self._ack(ok=True, request_id=request_id, detail="position-sync-requested", payload={"reqId": req_id})
            if command == "request_open_orders_sync":
                stage = "command.request_open_orders_sync"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        req_id = self.gateway.request_open_orders_sync(SyncOpenOrdersCommand(reason=str(payload.get("reason", "")).strip()))
                return self._ack(ok=True, request_id=request_id, detail="open-orders-sync-requested", payload={"reqId": req_id})
            if command == "quote_snapshot":
                stage = "command.quote_snapshot"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        quote = self.gateway.quote_snapshot(symbol)
                return self._ack(ok=True, request_id=request_id, detail="quote-snapshot", payload=quote)
            if command == "submit_order":
                stage = "command.submit_order"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        outside_rth = payload.get("outsideRth", payload.get("outside_rth", payload.get("allowAfterHours", False)))
                        routed = self.gateway.submit_order(
                            OrderSubmitCommand(
                                symbol=symbol,
                                action=str(payload.get("action", "")).strip().upper(),
                                quantity=int(payload.get("quantity", 0) or 0),
                                order_type=str(payload.get("orderType", "MKT")).strip().upper() or "MKT",
                                reference_price=float(payload.get("referencePrice", 0.0) or 0.0),
                                limit_price=float(payload.get("limitPrice")) if payload.get("limitPrice") is not None else None,
                                tif=str(payload.get("tif", "IOC")).strip().upper() or "IOC",
                                outside_rth=self._payload_bool(outside_rth),
                                reason=str(payload.get("reason", "")).strip(),
                                metadata=dict(payload.get("metadata") or {}),
                            )
                        )
                return self._ack(
                    ok=True,
                    request_id=request_id,
                    detail="order-submitted",
                    payload={
                        "gatewayOrderId": routed.gateway_order_id,
                        "ibOrderId": routed.ib_order_id,
                        "permId": routed.perm_id,
                        "status": routed.status,
                        "remaining": routed.remaining_quantity,
                        "filled": routed.filled_quantity,
                        "avg_fill_price": routed.avg_fill_price,
                        "effective_terminal": self.gateway._is_effectively_terminal_order_status(
                            routed.status,
                            routed.remaining_quantity,
                            routed.filled_quantity,
                            routed.tif,
                        ),
                        "symbol": routed.symbol,
                        "action": routed.action,
                        "quantity": routed.quantity,
                        "orderType": routed.order_type,
                        "referencePrice": routed.reference_price,
                        "limitPrice": routed.limit_price,
                        "outsideRth": routed.outside_rth,
                        "tif": routed.tif,
                    },
                )
            if command == "cancel_symbol_orders":
                stage = "command.cancel_symbol_orders"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        cancelled = self.gateway.cancel_symbol_orders(
                            CancelSymbolOrdersCommand(symbol=symbol, reason=str(payload.get("reason", "")).strip())
                        )
                return self._ack(
                    ok=True,
                    request_id=request_id,
                    detail="cancel-requested",
                    payload={"gatewayOrderIds": cancelled, "cancelledCount": len(cancelled)},
                )
            if command == "flatten_symbol":
                stage = "command.flatten_symbol"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        routed = self.gateway.flatten_symbol(
                            FlattenSymbolCommand(
                                symbol=symbol,
                                reference_price=float(payload.get("referencePrice", 0.0) or 0.0),
                                quantity_override=int(payload.get("quantityOverride")) if payload.get("quantityOverride") is not None else None,
                                reason=str(payload.get("reason", "")).strip(),
                            )
                        )
                return self._ack(
                    ok=True,
                    request_id=request_id,
                    detail="flatten-requested",
                    payload=None
                    if routed is None
                    else {
                        "gatewayOrderId": routed.gateway_order_id,
                        "ibOrderId": routed.ib_order_id,
                        "permId": routed.perm_id,
                        "status": routed.status,
                        "symbol": routed.symbol,
                        "action": routed.action,
                        "quantity": routed.quantity,
                        "orderType": routed.order_type,
                    },
                )
            if command == "snapshot":
                stage = "command.snapshot"
                with self._track_gateway_call(stage, request_id=request_id, command=command, symbol=symbol):
                    with self._gateway_lock:
                        snapshot = asdict(self.gateway.snapshot())
                return self._ack(ok=True, request_id=request_id, detail="snapshot", payload=snapshot)
        except Exception as exc:
            logger.exception(
                "shared IBKR gateway command failed stage=%s requestId=%s command=%s symbol=%s",
                stage,
                request_id,
                command,
                symbol,
            )
            return self._ack(
                ok=False,
                request_id=request_id,
                detail=f"command-failed stage={stage} reason={self._format_exception(exc)}",
            )
        return self._ack(ok=False, request_id=request_id, detail=f"unknown-command={command}")

    def _payload_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(value, (int, float)):
            return bool(value)
        return False

    def _start_call_watchdog(self) -> None:
        if self._call_watchdog_thread is not None and self._call_watchdog_thread.is_alive():
            return
        self._call_watchdog_stop.clear()
        self._call_watchdog_thread = threading.Thread(target=self._call_watchdog_loop, name="shared-ibkr-gateway-watchdog", daemon=True)
        self._call_watchdog_thread.start()

    def _stop_call_watchdog(self) -> None:
        self._call_watchdog_stop.set()
        if self._call_watchdog_thread is not None:
            self._call_watchdog_thread.join(timeout=2.0)
            self._call_watchdog_thread = None
        with self._active_call_lock:
            self._active_call = None

    @contextlib.contextmanager
    def _track_gateway_call(
        self,
        stage: str,
        *,
        request_id: str,
        command: str,
        symbol: str,
        log_start: bool = True,
        log_success: bool = True,
    ):
        started_at = time.monotonic()
        state = {
            "stage": stage,
            "request_id": request_id,
            "command": command,
            "symbol": symbol,
            "thread_name": threading.current_thread().name,
            "started_at": started_at,
            "last_warning_at": None,
        }
        with self._active_call_lock:
            self._active_call = state
        if log_start:
            logger.info(
                "shared IBKR gateway call start stage=%s requestId=%s command=%s symbol=%s thread=%s",
                stage,
                request_id,
                command,
                symbol,
                state["thread_name"],
            )
        try:
            yield
            if log_success:
                logger.info(
                    "shared IBKR gateway call done stage=%s requestId=%s command=%s symbol=%s elapsedMs=%.1f",
                    stage,
                    request_id,
                    command,
                    symbol,
                    (time.monotonic() - started_at) * 1000.0,
                )
        finally:
            with self._active_call_lock:
                if self._active_call is state:
                    self._active_call = None

    def _call_watchdog_loop(self) -> None:
        while not self._call_watchdog_stop.wait(0.5):
            snapshot: dict[str, Any] | None = None
            with self._active_call_lock:
                if self._active_call is not None:
                    elapsed_seconds = time.monotonic() - float(self._active_call["started_at"])
                    last_warning_at = self._active_call.get("last_warning_at")
                    now = time.monotonic()
                    if elapsed_seconds >= 2.0 and (last_warning_at is None or now - float(last_warning_at) >= 5.0):
                        self._active_call["last_warning_at"] = now
                        snapshot = dict(self._active_call)
            if snapshot is None:
                continue
            gateway_operation_started_at = getattr(self.gateway, "current_operation_started_at", None)
            gateway_operation_elapsed_ms = 0.0 if gateway_operation_started_at is None else (time.monotonic() - float(gateway_operation_started_at)) * 1000.0
            logger.warning(
                "shared IBKR gateway call still running stage=%s requestId=%s command=%s symbol=%s elapsedMs=%.1f thread=%s gatewayOperation=%s gatewayOperationElapsedMs=%.1f",
                snapshot.get("stage", ""),
                snapshot.get("request_id", ""),
                snapshot.get("command", ""),
                snapshot.get("symbol", ""),
                (time.monotonic() - float(snapshot.get("started_at", time.monotonic()))) * 1000.0,
                snapshot.get("thread_name", ""),
                getattr(self.gateway, "current_operation", "unknown"),
                gateway_operation_elapsed_ms,
            )

    def _format_exception(self, exc: Exception) -> str:
        detail = str(exc).strip()
        return exc.__class__.__name__ if not detail else f"{exc.__class__.__name__}: {detail}"

    def _health_payload(self) -> dict[str, Any]:
        active_call: dict[str, Any] | None = None
        with self._active_call_lock:
            if self._active_call is not None:
                active_call = dict(self._active_call)
        gateway_operation_started_at = getattr(self.gateway, "current_operation_started_at", None)
        gateway_operation_elapsed_ms = 0.0 if gateway_operation_started_at is None else round((time.monotonic() - float(gateway_operation_started_at)) * 1000.0, 1)
        active_call_elapsed_ms = 0.0
        if active_call is not None:
            active_call_elapsed_ms = round((time.monotonic() - float(active_call.get("started_at", time.monotonic()))) * 1000.0, 1)
        gateway_connected_flag = bool(getattr(self.gateway, "_connected", False))
        dry_run = bool(getattr(self.gateway, "dry_run", False))
        live_connected = getattr(self.gateway, "is_live_connected", None)
        connected = bool(live_connected()) if callable(live_connected) else gateway_connected_flag
        ib_client_connected = None
        ib_client_check = getattr(self.gateway, "_ib_client_is_connected", None)
        if not dry_run and callable(ib_client_check):
            ib_client_connected = bool(ib_client_check())
        return {
            "connected": connected,
            "gatewayConnectedFlag": gateway_connected_flag,
            "ibClientConnected": ib_client_connected,
            "dry_run": dry_run,
            "degraded": bool(getattr(self.gateway, "_degraded_reason", "")),
            "degradedReason": str(getattr(self.gateway, "_degraded_reason", "") or ""),
            "recovering": bool(getattr(self.gateway, "_recovering", False)),
            "recoveryEnabled": bool(getattr(self.gateway, "_recovery_enabled", False)),
            "recoveryAttempts": int(getattr(self.gateway, "_recovery_attempts", 0) or 0),
            "nextRecoveryAttemptInSeconds": getattr(self.gateway, "_next_recovery_delay_seconds", lambda: None)(),
            "recoveryLastError": str(getattr(self.gateway, "_last_recovery_error", "") or ""),
            "gatewayOperation": getattr(self.gateway, "current_operation", "idle"),
            "gatewayOperationElapsedMs": gateway_operation_elapsed_ms,
            "activeCallStage": "" if active_call is None else str(active_call.get("stage", "")),
            "activeCallCommand": "" if active_call is None else str(active_call.get("command", "")),
            "activeCallSymbol": "" if active_call is None else str(active_call.get("symbol", "")),
            "activeCallElapsedMs": active_call_elapsed_ms,
            "qualifiedContractSymbols": sorted(getattr(self.gateway, "_qualified_contracts", {}).keys()),
        }

    def _ack(self, *, ok: bool, request_id: str, detail: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "type": "ack",
            "requestId": request_id,
            "ok": ok,
            "detail": detail,
            "payload": payload or {},
        }

    def _event_payload(self, event: GatewayEvent) -> dict[str, Any]:
        return {
            "type": "event",
            "eventType": event.event_type.value,
            "symbol": event.symbol,
            "detail": event.detail,
            "gatewayOrderId": event.gateway_order_id,
            "ibOrderId": event.ib_order_id,
            "permId": event.perm_id,
            "reqId": event.req_id,
            "position": event.position,
            "avgCost": event.avg_cost,
            "payload": dict(event.payload),
        }

