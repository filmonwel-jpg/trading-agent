#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field


def log(message: str) -> None:
    print(f"[MOCK-IBKR-GATEWAY][{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


@dataclass
class ConnectionState:
    socket: socket.socket
    address: tuple[str, int]
    registered_symbol: str = ""
    strategy_id: str = ""
    next_request_id: int = 100
    lock: threading.Lock = field(default_factory=threading.Lock)

    def next_req_id(self) -> int:
        with self.lock:
            self.next_request_id += 1
            return self.next_request_id

    def send(self, payload: dict[str, object]) -> None:
        encoded = (json.dumps(payload) + "\n").encode("utf-8")
        with self.lock:
            self.socket.sendall(encoded)

    def ack(self, request_id: str, detail: str, payload: dict[str, object] | None = None) -> None:
        self.send(
            {
                "type": "ack",
                "requestId": request_id,
                "ok": True,
                "detail": detail,
                "payload": payload or {},
            }
        )

    def event(self, event_type: str, **payload: object) -> None:
        body = {
            "type": "event",
            "eventType": event_type,
            "detail": event_type,
        }
        body.update(payload)
        self.send(body)


def emit_flat_position_sync(state: ConnectionState, source: str) -> None:
    symbol = state.registered_symbol or ""
    state.event(
        "position_updated",
        symbol=symbol,
        position=0,
        avgCost=0.0,
        payload={"source": source},
    )
    state.event(
        "position_sync_completed",
        symbol=symbol,
        payload={"source": source},
    )


def handle_client(client_socket: socket.socket, address: tuple[str, int]) -> None:
    state = ConnectionState(socket=client_socket, address=address)
    log(f"client-connected address={address[0]}:{address[1]}")
    try:
        reader = client_socket.makefile("r", encoding="utf-8")
        for raw_line in reader:
            payload = raw_line.strip()
            if not payload:
                continue
            try:
                message = json.loads(payload)
            except json.JSONDecodeError as exc:
                log(f"invalid-json address={address[0]}:{address[1]} reason={exc}")
                continue

            request_id = str(message.get("requestId") or uuid.uuid4())
            command = str(message.get("command") or "").strip().lower()
            symbol = str(message.get("symbol") or "").strip().upper()
            data = message.get("payload") or {}
            if not isinstance(data, dict):
                data = {}

            if command == "ping":
                state.ack(request_id, "pong", {"connected": True, "reqId": state.next_req_id()})
            elif command == "register_symbol":
                state.registered_symbol = symbol
                state.strategy_id = str(data.get("strategyId") or "")
                req_id = state.next_req_id()
                state.ack(request_id, "symbol-registered", {"reqId": req_id})
                emit_flat_position_sync(state, "register_symbol")
            elif command == "request_position_sync":
                req_id = state.next_req_id()
                state.ack(request_id, "position-sync-requested", {"reqId": req_id})
                emit_flat_position_sync(state, "request_position_sync")
            elif command == "request_open_orders_sync":
                req_id = state.next_req_id()
                state.ack(request_id, "open-orders-sync-requested", {"reqId": req_id})
                state.event(
                    "open_orders_sync_completed",
                    symbol=state.registered_symbol,
                    payload={"source": "request_open_orders_sync"},
                )
            elif command in {"submit_order", "cancel_symbol_orders", "flatten_symbol"}:
                req_id = state.next_req_id()
                state.ack(request_id, f"{command}-accepted", {"reqId": req_id, "gatewayOrderId": req_id})
                state.event(
                    "order_status",
                    symbol=state.registered_symbol,
                    gatewayOrderId=req_id,
                    detail="Cancelled",
                    payload={
                        "status": "Cancelled",
                        "remaining": 0,
                        "filled": 0,
                        "avg_fill_price": 0.0,
                    },
                )
            else:
                req_id = state.next_req_id()
                state.ack(request_id, "noop", {"reqId": req_id, "command": command})
    except OSError as exc:
        log(f"client-disconnected address={address[0]}:{address[1]} reason={exc}")
    finally:
        try:
            client_socket.close()
        except OSError:
            pass


def serve(host: str, port: int) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()
    log(f"listening host={host} port={port}")
    try:
        while True:
            client_socket, address = server.accept()
            thread = threading.Thread(target=handle_client, args=(client_socket, address), daemon=True)
            thread.start()
    finally:
        server.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Mock shared IBKR gateway for offline multi-symbol stress tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19910)
    args = parser.parse_args()
    serve(args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

