from __future__ import annotations

import argparse
import json
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class GatewayProbeResult:
    ok: bool
    host: str
    port: int
    detail: str
    ping_ok: bool = False
    snapshot_ok: bool = False
    connected: bool | None = None
    dry_run: bool | None = None
    connect_latency_ms: float | None = None
    ping_latency_ms: float | None = None
    snapshot_latency_ms: float | None = None
    snapshot_payload: dict[str, Any] = field(default_factory=dict)


def probe_gateway(
    host: str,
    port: int,
    *,
    connect_timeout_seconds: float = 3.0,
    ack_timeout_seconds: float = 5.0,
    require_connected: bool = True,
) -> GatewayProbeResult:
    connect_started = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=max(0.1, float(connect_timeout_seconds))) as sock:
            connect_latency_ms = round((time.monotonic() - connect_started) * 1000.0, 1)
            sock.settimeout(max(0.1, float(ack_timeout_seconds)))
            with sock.makefile("r", encoding="utf-8", newline="\n") as reader, sock.makefile("w", encoding="utf-8", newline="\n") as writer:
                health_ack, health_latency_ms = _send_command(
                    sock,
                    reader,
                    writer,
                    command="health",
                    symbol="",
                    payload={},
                    ack_timeout_seconds=ack_timeout_seconds,
                )
                if bool(health_ack.get("ok")):
                    health_payload = dict(health_ack.get("payload") or {})
                    connected = _coerce_optional_bool(health_payload.get("connected"))
                    dry_run = _coerce_optional_bool(health_payload.get("dry_run"))
                    recovering = _coerce_optional_bool(health_payload.get("recovering"))
                    if require_connected and connected is not True:
                        return GatewayProbeResult(
                            ok=False,
                            host=host,
                            port=port,
                            detail=(
                                "protocol-health-ok-but-gateway-not-connected "
                                f"connected={connected} dry_run={dry_run} recovering={recovering}"
                            ),
                            ping_ok=True,
                            snapshot_ok=True,
                            connected=connected,
                            dry_run=dry_run,
                            connect_latency_ms=connect_latency_ms,
                            ping_latency_ms=health_latency_ms,
                            snapshot_payload=health_payload,
                        )
                    return GatewayProbeResult(
                        ok=True,
                        host=host,
                        port=port,
                        detail="ok",
                        ping_ok=True,
                        snapshot_ok=True,
                        connected=connected,
                        dry_run=dry_run,
                        connect_latency_ms=connect_latency_ms,
                        ping_latency_ms=health_latency_ms,
                        snapshot_payload=health_payload,
                    )

                if str(health_ack.get("detail", "")).strip().lower() != "unknown-command=health":
                    return GatewayProbeResult(
                        ok=False,
                        host=host,
                        port=port,
                        detail=f"health-ack-not-ok detail={health_ack.get('detail', '')}",
                        ping_ok=False,
                        snapshot_ok=False,
                        connect_latency_ms=connect_latency_ms,
                        ping_latency_ms=health_latency_ms,
                    )

                ping_ack, ping_latency_ms = _send_command(
                    sock,
                    reader,
                    writer,
                    command="ping",
                    symbol="",
                    payload={},
                    ack_timeout_seconds=ack_timeout_seconds,
                )
                if not bool(ping_ack.get("ok")):
                    return GatewayProbeResult(
                        ok=False,
                        host=host,
                        port=port,
                        detail=f"ping-ack-not-ok detail={ping_ack.get('detail', '')}",
                        ping_ok=False,
                        snapshot_ok=False,
                        connect_latency_ms=connect_latency_ms,
                        ping_latency_ms=ping_latency_ms,
                    )

                snapshot_ack, snapshot_latency_ms = _send_command(
                    sock,
                    reader,
                    writer,
                    command="snapshot",
                    symbol="",
                    payload={},
                    ack_timeout_seconds=ack_timeout_seconds,
                )
                if not bool(snapshot_ack.get("ok")):
                    return GatewayProbeResult(
                        ok=False,
                        host=host,
                        port=port,
                        detail=f"snapshot-ack-not-ok detail={snapshot_ack.get('detail', '')}",
                        ping_ok=True,
                        snapshot_ok=False,
                        connect_latency_ms=connect_latency_ms,
                        ping_latency_ms=ping_latency_ms,
                        snapshot_latency_ms=snapshot_latency_ms,
                    )

                snapshot_payload = dict(snapshot_ack.get("payload") or {})
                connected = _coerce_optional_bool(snapshot_payload.get("connected"))
                dry_run = _coerce_optional_bool(snapshot_payload.get("dry_run"))
                if require_connected and connected is not True:
                    return GatewayProbeResult(
                        ok=False,
                        host=host,
                        port=port,
                        detail=(
                            "protocol-handshake-ok-but-gateway-not-connected "
                            f"connected={connected} dry_run={dry_run}"
                        ),
                        ping_ok=True,
                        snapshot_ok=True,
                        connected=connected,
                        dry_run=dry_run,
                        connect_latency_ms=connect_latency_ms,
                        ping_latency_ms=ping_latency_ms,
                        snapshot_latency_ms=snapshot_latency_ms,
                        snapshot_payload=snapshot_payload,
                    )

                return GatewayProbeResult(
                    ok=True,
                    host=host,
                    port=port,
                    detail="ok",
                    ping_ok=True,
                    snapshot_ok=True,
                    connected=connected,
                    dry_run=dry_run,
                    connect_latency_ms=connect_latency_ms,
                    ping_latency_ms=ping_latency_ms,
                    snapshot_latency_ms=snapshot_latency_ms,
                    snapshot_payload=snapshot_payload,
                )
    except Exception as exc:
        return GatewayProbeResult(
            ok=False,
            host=host,
            port=port,
            detail=f"probe-failed reason={_format_exception(exc)}",
            connect_latency_ms=round((time.monotonic() - connect_started) * 1000.0, 1),
        )


def _send_command(
    sock: socket.socket,
    reader,
    writer,
    *,
    command: str,
    symbol: str,
    payload: dict[str, Any],
    ack_timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    request_id = str(uuid.uuid4())
    envelope = {
        "type": "command",
        "requestId": request_id,
        "command": command,
        "payload": dict(payload),
    }
    if symbol:
        envelope["symbol"] = symbol

    started = time.monotonic()
    writer.write(json.dumps(envelope, separators=(",", ":")))
    writer.write("\n")
    writer.flush()

    deadline = started + max(0.1, float(ack_timeout_seconds))
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"timed out waiting for ack command={command} requestId={request_id}")
        sock.settimeout(remaining)
        raw_line = reader.readline()
        if not raw_line:
            raise RuntimeError(f"unexpected EOF waiting for ack command={command} requestId={request_id}")
        message = json.loads(raw_line)
        if message.get("type") == "event":
            continue
        if message.get("type") != "ack":
            raise RuntimeError(f"unexpected-message-type type={message.get('type')} command={command}")
        if str(message.get("requestId", "")).strip() != request_id:
            raise RuntimeError(
                "unexpected-ack-request-id "
                f"expected={request_id} actual={message.get('requestId')} command={command}"
            )
        return message, round((time.monotonic() - started) * 1000.0, 1)


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _format_exception(exc: Exception) -> str:
    detail = str(exc).strip()
    return exc.__class__.__name__ if not detail else f"{exc.__class__.__name__}: {detail}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe shared IBKR gateway protocol health")
    parser.add_argument("--host", default="127.0.0.1", help="Gateway host")
    parser.add_argument("--port", type=int, default=9910, help="Gateway port")
    parser.add_argument("--connect-timeout-seconds", type=float, default=3.0, help="TCP connect timeout")
    parser.add_argument("--ack-timeout-seconds", type=float, default=5.0, help="Protocol ack timeout")
    parser.add_argument(
        "--allow-disconnected",
        action="store_true",
        help="Treat a healthy protocol handshake as success even if snapshot.connected=false",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress JSON output and return exit status only")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = probe_gateway(
        args.host,
        args.port,
        connect_timeout_seconds=args.connect_timeout_seconds,
        ack_timeout_seconds=args.ack_timeout_seconds,
        require_connected=not args.allow_disconnected,
    )
    if not args.quiet:
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

