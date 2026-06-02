#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Target:
    symbol: str
    port: str
    properties_file: Path


class GatewayClient:
    def __init__(self, host: str, port: int, connect_timeout_seconds: float, command_timeout_seconds: float | None = None) -> None:
        self.host = host
        self.port = port
        self.connect_timeout_seconds = max(0.1, float(connect_timeout_seconds))
        self.command_timeout_seconds = max(0.1, float(command_timeout_seconds if command_timeout_seconds is not None else connect_timeout_seconds))
        self.sock: socket.socket | None = None
        self.reader = None
        self.writer = None

    def __enter__(self) -> "GatewayClient":
        self.sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout_seconds)
        self.sock.settimeout(self.command_timeout_seconds)
        self.reader = self.sock.makefile("r", encoding="utf-8", newline="\n")
        self.writer = self.sock.makefile("w", encoding="utf-8", newline="\n")
        return self

    def __exit__(self, *_args: Any) -> None:
        for handle in (self.reader, self.writer):
            try:
                if handle is not None:
                    handle.close()
            except Exception:
                pass
        try:
            if self.sock is not None:
                self.sock.close()
        except Exception:
            pass

    def command(self, command: str, *, symbol: str = "", payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.sock is None or self.reader is None or self.writer is None:
            raise RuntimeError("gateway client is not connected")
        request_id = str(uuid.uuid4())
        envelope: dict[str, Any] = {
            "type": "command",
            "requestId": request_id,
            "command": command,
            "payload": dict(payload or {}),
        }
        if symbol:
            envelope["symbol"] = symbol
        self.writer.write(json.dumps(envelope, separators=(",", ":")))
        self.writer.write("\n")
        self.writer.flush()
        deadline = time.monotonic() + self.command_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out waiting for gateway ack after {self.command_timeout_seconds:.1f}s "
                    f"command={command} symbol={symbol or '-'} requestId={request_id}"
                )
            self.sock.settimeout(remaining)
            try:
                raw = self.reader.readline()
            except socket.timeout as exc:
                raise TimeoutError(
                    f"timed out waiting for gateway ack after {self.command_timeout_seconds:.1f}s "
                    f"command={command} symbol={symbol or '-'} requestId={request_id}; "
                    "the shared gateway may be busy in an IBKR sync/poll call; retry with a larger --command-timeout-seconds if needed"
                ) from exc
            if not raw:
                raise RuntimeError(f"gateway closed connection while waiting for ack command={command}")
            message = json.loads(raw)
            if message.get("type") == "event":
                continue
            if message.get("type") != "ack":
                raise RuntimeError(f"unexpected gateway message type={message.get('type')} command={command}")
            if str(message.get("requestId", "")) != request_id:
                continue
            return message


def log(message: str) -> None:
    print(f"[GATEWAY-FLATTEN][{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def read_prop(path: Path, key: str) -> str:
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            current, value = line.split("=", 1)
            if current.strip() == key:
                return value.strip()
    except FileNotFoundError:
        return ""
    return ""


def discover_targets(repo_root: Path) -> list[Target]:
    bots_dir = repo_root / "runtime" / "databento" / "bots"
    legacy_dir = repo_root / "runtime"
    paths = sorted(p for p in bots_dir.glob("trading-*.properties") if p.name != "trading-databento-template.properties")
    if not paths:
        paths = sorted(p for p in legacy_dir.glob("trading-*.properties") if p.name != "trading-databento-template.properties")
    targets: list[Target] = []
    for path in paths:
        symbol = read_prop(path, "trading.symbol") or path.stem.removeprefix("trading-")
        port = read_prop(path, "server.port")
        symbol = symbol.strip().upper()
        if symbol and port:
            targets.append(Target(symbol=symbol, port=port.strip(), properties_file=path))
    return targets


def csv_set(value: str) -> set[str]:
    return {item.strip().upper() for item in value.split(",") if item.strip()}


def selected_targets(targets: list[Target], symbols_csv: str, exclude_csv: str) -> list[Target]:
    include = csv_set(symbols_csv)
    exclude = csv_set(exclude_csv)
    selected: list[Target] = []
    for target in targets:
        if include and target.symbol not in include:
            continue
        if target.symbol in exclude:
            continue
        selected.append(target)
    return selected


def require_ack_ok(ack: dict[str, Any], command: str) -> None:
    if not bool(ack.get("ok")):
        raise RuntimeError(f"gateway command failed command={command} detail={ack.get('detail', '')}")


def format_exception(exc: Exception) -> str:
    detail = str(exc).strip()
    return exc.__class__.__name__ if not detail else f"{exc.__class__.__name__}: {detail}"


def resolve_command_timeout_seconds(args: argparse.Namespace) -> float:
    explicit = getattr(args, "command_timeout_seconds", None)
    if explicit is not None:
        return max(0.1, float(explicit))
    connect_timeout = max(0.1, float(args.connect_timeout_seconds))
    flatten_timeout = max(connect_timeout, float(max(1, args.timeout_seconds)))
    return max(connect_timeout, min(flatten_timeout, 30.0))


def register_symbols(client: GatewayClient, symbols: list[str]) -> None:
    for symbol in symbols:
        ack = client.command("register_symbol", symbol=symbol, payload={"strategyId": "manual-direct-gateway-flatten"})
        require_ack_ok(ack, "register_symbol")


def sync_positions(client: GatewayClient) -> dict[str, dict[str, Any]]:
    ack = client.command("request_position_sync", payload={"reason": "manual-direct-gateway-flatten"})
    require_ack_ok(ack, "request_position_sync")
    req_id = optional_int((ack.get("payload") or {}).get("reqId"))
    snapshot_ack = client.command("snapshot")
    require_ack_ok(snapshot_ack, "snapshot")
    payload = dict(snapshot_ack.get("payload") or {})
    if payload.get("connected") is not True:
        raise RuntimeError(f"gateway snapshot is not connected payload.connected={payload.get('connected')}")
    validate_position_sync_snapshot(payload, req_id)
    symbols = dict(payload.get("symbols") or {})
    return {str(symbol).upper(): dict(state or {}) for symbol, state in symbols.items()}


def snapshot(client: GatewayClient) -> dict[str, Any]:
    snapshot_ack = client.command("snapshot")
    require_ack_ok(snapshot_ack, "snapshot")
    payload = dict(snapshot_ack.get("payload") or {})
    if payload.get("connected") is not True:
        raise RuntimeError(f"gateway snapshot is not connected payload.connected={payload.get('connected')}")
    return payload


def sync_open_orders(client: GatewayClient) -> dict[str, Any]:
    ack = client.command("request_open_orders_sync", payload={"reason": "manual-direct-gateway-flatten"})
    require_ack_ok(ack, "request_open_orders_sync")
    return snapshot(client)


def quote_snapshot(client: GatewayClient, symbol: str) -> dict[str, Any]:
    ack = client.command("quote_snapshot", symbol=symbol, payload={"reason": "manual-direct-gateway-flatten"})
    require_ack_ok(ack, "quote_snapshot")
    return dict(ack.get("payload") or {})


def missing_symbols(states: dict[str, dict[str, Any]], selected_symbols: list[str]) -> list[str]:
    return sorted(symbol for symbol in selected_symbols if symbol.upper() not in states)


def position_for(symbols: dict[str, dict[str, Any]], symbol: str) -> int:
    state = symbols.get(symbol.upper()) or {}
    try:
        return int(state.get("position") or 0)
    except Exception:
        return 0


def closing_action_for_position(position: int) -> tuple[str, str, int]:
    if position > 0:
        return "LONG", "SELL", position
    if position < 0:
        return "SHORT", "BUY", abs(position)
    return "FLAT", "", 0


def positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0.0 else None


def after_hours_limit_price(action: str, quote: dict[str, Any], offset_pct: float) -> tuple[float, str]:
    normalized_action = action.strip().upper()
    pct = max(0.0, float(offset_pct)) / 100.0
    if normalized_action == "BUY":
        ask = positive_float(quote.get("ask"))
        if ask is None:
            raise RuntimeError(f"cannot price BUY cover: quote ask is missing/non-positive quote={quote}")
        return round(ask * (1.0 + pct), 2), "ask"
    if normalized_action == "SELL":
        bid = positive_float(quote.get("bid"))
        if bid is None:
            raise RuntimeError(f"cannot price SELL flatten: quote bid is missing/non-positive quote={quote}")
        return round(bid * max(0.0, 1.0 - pct), 2), "bid"
    raise ValueError(f"unsupported flatten action={action}")


def is_terminal_order_status(status: str) -> bool:
    return status.strip().lower() in {"filled", "cancelled", "inactive", "apicancelled"}


def optional_int(value: Any) -> int | None:
    text = str(value).strip()
    if not text or not text.lstrip("-").isdigit():
        return None
    return int(text)


def validate_position_sync_snapshot(snapshot_payload: dict[str, Any], req_id: int | None) -> None:
    if req_id is None:
        return
    recent_events = [dict(event or {}) for event in list(snapshot_payload.get("recent_events") or [])]
    matching_completed = any(
        optional_int(event.get("req_id")) == req_id and str(event.get("event_type", "")) == "position_sync_completed"
        for event in recent_events
    )
    if not matching_completed:
        raise RuntimeError(
            f"gateway snapshot did not include completion for requested position sync reqId={req_id}; "
            "refusing to treat missing broker positions as flat"
        )
    matching_updates = [
        event
        for event in recent_events
        if optional_int(event.get("req_id")) == req_id and str(event.get("event_type", "")) == "position_updated"
    ]
    matching_errors = [
        event
        for event in recent_events
        if str(event.get("event_type", "")) == "error"
        and (
            optional_int(event.get("req_id")) == req_id
            or (not matching_updates and str(event.get("detail", "")).startswith("position-sync-failed"))
        )
    ]
    if matching_errors:
        details = "; ".join(str(event.get("detail", "")) for event in matching_errors if str(event.get("detail", "")))
        raise RuntimeError(
            f"gateway reported position sync error for reqId={req_id}: {details or 'unknown error'}; "
            "refusing to treat missing broker positions as flat"
        )


def is_zero_remaining_cancel_pending_order(status: str, remaining: int | None) -> bool:
    return remaining == 0 and status.strip().lower() in {"pendingcancel", "cancelsubmitted"}


def is_zero_remaining_non_actionable_order(status: str, remaining: int | None, filled: int | None = None) -> bool:
    return remaining == 0 and (filled or 0) == 0 and status.strip().lower() in {"pendingcancel", "cancelsubmitted", "pendingsubmit", "apipending"}


def symbol_orders(snapshot_payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    normalized = symbol.upper()
    return [dict(order or {}) for order in list(snapshot_payload.get("orders") or []) if str((order or {}).get("symbol", "")).upper() == normalized]


def unresolved_symbol_orders(snapshot_payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    unresolved: list[dict[str, Any]] = []
    symbol_state = dict((snapshot_payload.get("symbols") or {}).get(symbol.upper()) or {})
    open_gateway_order_ids = {int(order_id) for order_id in list(symbol_state.get("open_gateway_order_ids") or []) if str(order_id).strip().lstrip("-").isdigit()}
    for order in symbol_orders(snapshot_payload, symbol):
        status = str(order.get("status", "") or "")
        gateway_order_id = int(order.get("gateway_order_id") or 0)
        remaining_int = optional_int(order.get("remaining_quantity"))
        filled_int = optional_int(order.get("filled_quantity"))
        if is_zero_remaining_non_actionable_order(status, remaining_int, filled_int):
            continue
        if gateway_order_id in open_gateway_order_ids or not is_terminal_order_status(status) or (remaining_int is not None and remaining_int > 0):
            unresolved.append(order)
    return unresolved


def describe_orders(orders: list[dict[str, Any]]) -> str:
    if not orders:
        return "none"
    parts = []
    for order in orders:
        parts.append(
            "gatewayOrderId={gateway_order_id} ibOrderId={ib_order_id} permId={perm_id} action={action} qty={quantity} status={status} remaining={remaining_quantity} filled={filled_quantity} detail={last_detail}".format(
                **{k: order.get(k) for k in ["gateway_order_id", "ib_order_id", "perm_id", "action", "quantity", "status", "remaining_quantity", "filled_quantity", "last_detail"]}
            )
        )
    return "; ".join(parts)


def flatten_symbol(
    client: GatewayClient,
    symbol: str,
    position: int,
    dry_run: bool,
    *,
    force_new_order: bool,
    cancel_only: bool,
    after_hours_limit: bool,
    limit_offset_pct: float,
    after_hours_limit_tif: str,
    allow_after_hours: bool,
) -> bool:
    if position == 0:
        log(f"{symbol} broker position already flat; skipping")
        return True
    side, action, quantity = closing_action_for_position(position)
    log(f"{symbol} broker position={position} side={side}; flatten action={action} quantity={quantity}")
    if dry_run:
        if after_hours_limit:
            log(
                f"dry-run: would cancel gateway-tracked orders, request quote, and submit closing {action} LMT outsideRth={allow_after_hours} "
                f"for {symbol} quantity={quantity} priceRule={'ask+' if action == 'BUY' else 'bid-'}{limit_offset_pct:.4g}% tif={after_hours_limit_tif}"
            )
        else:
            log(f"dry-run: would cancel gateway-tracked orders and submit closing {action} MKT for {symbol} quantity={quantity}")
        return True
    cancel_ack = client.command("cancel_symbol_orders", symbol=symbol, payload={"reason": "manual-direct-gateway-flatten"})
    require_ack_ok(cancel_ack, "cancel_symbol_orders")
    log(f"{symbol} cancel ack detail={cancel_ack.get('detail')} payload={cancel_ack.get('payload')}")
    order_snapshot = sync_open_orders(client)
    unresolved = unresolved_symbol_orders(order_snapshot, symbol)
    if unresolved:
        log(f"{symbol} unresolved gateway order(s) after cancel request: {describe_orders(unresolved)}")
        if cancel_only:
            log(f"{symbol} cancel-only mode: not submitting a new closing order")
            return False
        if not force_new_order:
            log(f"{symbol} refusing to submit another closing order while unresolved order(s) remain; use --force-new-order only after verifying TWS/IBKR order state")
            return False
        log(f"{symbol} --force-new-order supplied; submitting another closing order despite unresolved order(s)")
    elif cancel_only:
        log(f"{symbol} cancel-only mode: no unresolved gateway orders remain; not submitting a new closing order")
        return True
    order_type = "MKT"
    reference_price = 0.0
    limit_price = None
    tif = "IOC"
    outside_rth = False
    pricing_basis = "market"
    quote = None
    if after_hours_limit:
        quote = quote_snapshot(client, symbol)
        limit_price, pricing_basis = after_hours_limit_price(action, quote, limit_offset_pct)
        reference_price = limit_price
        order_type = "LMT"
        tif = after_hours_limit_tif.strip().upper() or "DAY"
        outside_rth = allow_after_hours
        log(f"{symbol} after-hours limit pricing action={action} basis={pricing_basis} quote={quote} offsetPct={limit_offset_pct} limitPrice={limit_price} tif={tif} outsideRth={outside_rth}")
    submit_ack = client.command(
        "submit_order",
        symbol=symbol,
        payload={
            "action": action,
            "quantity": quantity,
            "orderType": order_type,
            "referencePrice": reference_price,
            "limitPrice": limit_price,
            "tif": tif,
            "outsideRth": outside_rth,
            "allowAfterHours": outside_rth,
            "reason": "manual-direct-gateway-flatten",
            "metadata": {
                "closing_trade": True,
                "source": "flatten_all_via_shared_ibkr_gateway.py",
                "broker_position": position,
                "broker_side": side,
                "after_hours_limit": after_hours_limit,
                "pricing_basis": pricing_basis,
                "limit_offset_pct": limit_offset_pct,
                "quote": quote,
            },
        },
    )
    require_ack_ok(submit_ack, "submit_order")
    log(f"{symbol} submit closing order ack detail={submit_ack.get('detail')} payload={submit_ack.get('payload')}")
    routed = dict(submit_ack.get("payload") or {})
    routed_action = str(routed.get("action", "")).upper()
    routed_quantity = int(routed.get("quantity") or 0)
    if routed_action != action or routed_quantity != quantity:
        raise RuntimeError(
            f"{symbol} routed closing order mismatch requested={action}/{quantity} routed={routed_action}/{routed_quantity}"
        )
    if routed and int(routed.get("permId") or 0) <= 0:
        log(f"{symbol} warning: closing order route has permId={routed.get('permId')} status={routed.get('status')}; will require position confirmation")
    return True


def wait_for_flat(client: GatewayClient, symbols: list[str], initial_positions: dict[str, int], timeout_seconds: int, poll_interval_seconds: int, max_stuck_polls: int) -> tuple[set[str], set[str]]:
    pending = {symbol for symbol in symbols if initial_positions.get(symbol, 0) != 0}
    confirmed: set[str] = set()
    failed: set[str] = set()
    stuck_counts = {symbol: 0 for symbol in pending}
    deadline = time.monotonic() + max(0, timeout_seconds)
    while pending:
        states = sync_positions(client)
        for symbol in sorted(list(pending)):
            current_position = position_for(states, symbol)
            log(f"{symbol} broker poll position={current_position}")
            if current_position == 0:
                pending.remove(symbol)
                confirmed.add(symbol)
                log(f"{symbol} broker flat confirmed")
                continue
            if max_stuck_polls > 0 and abs(current_position) >= abs(initial_positions.get(symbol, current_position)):
                stuck_counts[symbol] += 1
            else:
                stuck_counts[symbol] = 0
            if max_stuck_polls > 0 and stuck_counts[symbol] >= max_stuck_polls:
                pending.remove(symbol)
                failed.add(symbol)
                log(f"{symbol} no broker position progress after {stuck_counts[symbol]} poll(s); current={current_position} initial={initial_positions.get(symbol)}")
        if not pending:
            break
        if time.monotonic() >= deadline:
            failed.update(pending)
            for symbol in sorted(pending):
                log(f"{symbol} did not confirm flat before timeout")
            break
        time.sleep(max(1, poll_interval_seconds))
    return confirmed, failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten selected Databento symbols directly via the shared IBKR gateway broker position sync.")
    parser.add_argument("--host", default="127.0.0.1", help="Shared IBKR gateway host")
    parser.add_argument("--port", type=int, default=9910, help="Shared IBKR gateway port")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to include")
    parser.add_argument("--exclude", default="", help="Comma-separated symbols to exclude")
    parser.add_argument("--list", action="store_true", help="List selected bot-config symbols and exit")
    parser.add_argument("--dry-run", action="store_true", help="Show intended direct gateway flatten actions without sending cancel/flatten commands")
    parser.add_argument("--execute", action="store_true", help="Actually submit live closing orders. Without this flag, the script is inspect/dry-run only.")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Max seconds to wait for broker-flat confirmation")
    parser.add_argument("--poll-interval-seconds", type=int, default=5, help="Broker position sync poll interval")
    parser.add_argument("--max-stuck-polls", type=int, default=3, help="Fail a symbol after this many no-progress broker position polls; 0 disables stuck-poll early failure")
    parser.add_argument("--connect-timeout-seconds", type=float, default=5.0, help="Gateway socket connect timeout")
    parser.add_argument(
        "--command-timeout-seconds",
        type=float,
        default=None,
        help="Gateway per-command ack timeout; defaults to min(--timeout-seconds, 30) with --connect-timeout-seconds as a lower bound",
    )
    parser.add_argument("--continue-on-failure", action="store_true", help="Continue to later non-flat symbols after one symbol fails confirmation")
    parser.add_argument("--cancel-only", action="store_true", help="Request cancellation of gateway-tracked symbol orders but do not submit new closing orders")
    parser.add_argument("--force-new-order", action="store_true", help="Submit a new closing order even if unresolved gateway orders remain after cancel; use only after manual TWS/IBKR verification")
    parser.add_argument("--skip-blocked", action="store_true", help="In --execute mode, skip symbols with unresolved gateway orders instead of aborting before submitting any new orders")
    parser.add_argument("--after-hours-limit", action="store_true", help="Submit closing orders as outside-RTH LMT orders priced from current quote instead of IOC MKT")
    parser.add_argument("--allow-after-hours", action="store_true", help="Set outsideRth/allowAfterHours=true on submit_order payloads; intended for --after-hours-limit")
    parser.add_argument("--limit-offset-pct", type=float, default=1.0, help="After-hours limit offset percent: BUY at ask+N%%, SELL at bid-N%% (default: 1.0)")
    parser.add_argument("--after-hours-limit-tif", default="DAY", help="TIF for --after-hours-limit orders (default: DAY)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit_offset_pct < 0.0:
        log("--limit-offset-pct must be non-negative")
        return 2
    args.after_hours_limit_tif = (args.after_hours_limit_tif or "DAY").strip().upper() or "DAY"
    repo_root = Path(__file__).resolve().parent
    targets = selected_targets(discover_targets(repo_root), args.symbols, args.exclude)
    if not targets:
        log("no selected bot config targets found")
        return 1
    if args.list:
        for target in targets:
            print(f"{target.symbol}|{target.port}|{target.properties_file}")
        return 0

    selected_symbols = [target.symbol for target in targets]
    inspect_only = args.dry_run or (not args.execute and not args.cancel_only)
    command_timeout_seconds = resolve_command_timeout_seconds(args)
    log(
        f"selectedTargets={len(selected_symbols)} gateway={args.host}:{args.port} dryRun={args.dry_run} "
        f"execute={args.execute} cancelOnly={args.cancel_only} afterHoursLimit={args.after_hours_limit} "
        f"allowAfterHours={args.allow_after_hours} limitOffsetPct={args.limit_offset_pct} commandTimeoutSeconds={command_timeout_seconds:.1f}"
    )
    if inspect_only:
        log("inspect-only mode: no live orders will be submitted; add --execute to submit closing orders or --cancel-only to only request cancels")
    try:
        with GatewayClient(args.host, args.port, args.connect_timeout_seconds, command_timeout_seconds) as client:
            log(f"registering selected symbols before broker sync count={len(selected_symbols)}")
            register_symbols(client, selected_symbols)
            states = sync_positions(client)
            missing = missing_symbols(states, selected_symbols)
            if missing:
                raise RuntimeError(f"gateway snapshot missing registered selected symbols={','.join(missing)}")
            initial_positions = {symbol: position_for(states, symbol) for symbol in selected_symbols}
            nonflat_symbols = [symbol for symbol in selected_symbols if initial_positions.get(symbol, 0) != 0]
            log(f"broker nonflat selected symbols={len(nonflat_symbols)} symbols={','.join(nonflat_symbols) if nonflat_symbols else 'none'}")
            try:
                order_snapshot = sync_open_orders(client)
            except Exception as exc:
                if not inspect_only and not args.cancel_only and not args.force_new_order:
                    log(
                        "open-order preflight failed before live order submission; no new closing orders were submitted. "
                        f"detail={format_exception(exc)}. Retry with --command-timeout-seconds={max(command_timeout_seconds * 2.0, 30.0):.0f} "
                        "or use --force-new-order only after manually verifying TWS/IBKR has no conflicting open orders."
                    )
                    return 1
                log(f"open-order preflight unavailable; continuing because mode does not require new-order preflight detail={format_exception(exc)}")
                order_snapshot = {"orders": [], "symbols": {}}
            blocked_symbols: set[str] = set()
            for symbol in nonflat_symbols:
                unresolved = unresolved_symbol_orders(order_snapshot, symbol)
                if unresolved:
                    blocked_symbols.add(symbol)
                    log(f"{symbol} preflight unresolved gateway order(s): {describe_orders(unresolved)}")
            if blocked_symbols:
                log(f"preflight blocked symbols with unresolved gateway orders={','.join(sorted(blocked_symbols))}")
            for symbol in selected_symbols:
                if initial_positions.get(symbol, 0) == 0:
                    log(f"{symbol} broker position already flat; skipping")

            if inspect_only:
                for symbol in nonflat_symbols:
                    flatten_symbol(
                        client,
                        symbol,
                        initial_positions.get(symbol, 0),
                        True,
                        force_new_order=args.force_new_order,
                        cancel_only=args.cancel_only,
                        after_hours_limit=args.after_hours_limit,
                        limit_offset_pct=args.limit_offset_pct,
                        after_hours_limit_tif=args.after_hours_limit_tif,
                        allow_after_hours=args.allow_after_hours,
                    )
                log("direct gateway flatten inspect complete confirmed=none failed=none skipped=none")
                return 0
            if not nonflat_symbols:
                log("direct gateway flatten complete confirmed=all-selected-flat failed=none skipped=none")
                return 0

            if blocked_symbols and not args.cancel_only and not args.force_new_order and not args.skip_blocked:
                log(
                    "execute aborted before submitting new orders because unresolved gateway orders exist; "
                    "use --cancel-only to request cancels, --skip-blocked to flatten only unblocked symbols, or manually clear orders in TWS/IBKR"
                )
                return 1

            confirmed: set[str] = set()
            failed: set[str] = set()
            skipped: set[str] = set()
            for symbol in nonflat_symbols:
                if symbol in blocked_symbols and args.skip_blocked and not args.force_new_order:
                    skipped.add(symbol)
                    log(f"{symbol} skipped because unresolved gateway orders remain and --skip-blocked was supplied")
                    continue
                try:
                    submitted_or_cancelled = flatten_symbol(
                        client,
                        symbol,
                        initial_positions.get(symbol, 0),
                        False,
                        force_new_order=args.force_new_order,
                        cancel_only=args.cancel_only,
                        after_hours_limit=args.after_hours_limit,
                        limit_offset_pct=args.limit_offset_pct,
                        after_hours_limit_tif=args.after_hours_limit_tif,
                        allow_after_hours=args.allow_after_hours,
                    )
                    if not submitted_or_cancelled:
                        failed.add(symbol)
                        if not args.continue_on_failure:
                            log(f"{symbol} failed before new order submission; stopping direct gateway flatten because --continue-on-failure was not supplied")
                            break
                        continue
                    if args.cancel_only:
                        continue
                    symbol_confirmed, symbol_failed = wait_for_flat(
                        client,
                        [symbol],
                        initial_positions,
                        args.timeout_seconds,
                        args.poll_interval_seconds,
                        args.max_stuck_polls,
                    )
                    confirmed.update(symbol_confirmed)
                    failed.update(symbol_failed)
                    if symbol_failed and not args.continue_on_failure:
                        log(f"{symbol} failed; stopping direct gateway flatten because --continue-on-failure was not supplied")
                        break
                except Exception as exc:
                    failed.add(symbol)
                    log(f"{symbol} gateway flatten step failed detail={format_exception(exc)}")
                    if not args.continue_on_failure:
                        log(f"{symbol} failed; stopping direct gateway flatten because --continue-on-failure was not supplied")
                        break
            if args.cancel_only and failed:
                log(
                    "cancel-only result: cancelNotTerminallyConfirmed="
                    f"{','.join(sorted(failed))}; these symbols still have unresolved gateway/TWS order state and need manual IBKR/TWS verification"
                )
            log(f"direct gateway flatten complete confirmed={','.join(sorted(confirmed)) if confirmed else 'none'} failed={','.join(sorted(failed)) if failed else 'none'} skipped={','.join(sorted(skipped)) if skipped else 'none'}")
            return 0 if not failed else 1
    except Exception as exc:
        log(f"direct gateway flatten failed before completion detail={format_exception(exc)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

