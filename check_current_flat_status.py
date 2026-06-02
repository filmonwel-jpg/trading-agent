#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import flatten_all_via_shared_ibkr_gateway as f


def as_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only shared IBKR gateway flat/open-order status check.")
    parser.add_argument("--host", default="127.0.0.1", help="Shared IBKR gateway host")
    parser.add_argument("--port", type=int, default=9910, help="Shared IBKR gateway port")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to check; defaults to all discovered bot symbols")
    parser.add_argument("--exclude", default="", help="Comma-separated symbols to exclude when discovering bot symbols")
    parser.add_argument("--connect-timeout-seconds", type=float, default=5.0, help="Gateway socket connect timeout")
    parser.add_argument("--command-timeout-seconds", type=float, default=30.0, help="Gateway per-command ack timeout")
    return parser.parse_args()


def row_for_order(order: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    symbol = str(order.get("symbol", "")).upper()
    symbol_state = dict((snapshot.get("symbols") or {}).get(symbol) or {})
    open_ids = {as_int(order_id) for order_id in symbol_state.get("open_gateway_order_ids", [])}
    return {
        "symbol": symbol,
        "gateway_order_id": as_int(order.get("gateway_order_id")),
        "ib_order_id": order.get("ib_order_id"),
        "perm_id": order.get("perm_id"),
        "action": order.get("action"),
        "qty": order.get("quantity"),
        "status": str(order.get("status", "") or ""),
        "remaining": as_int(order.get("remaining_quantity")),
        "filled": order.get("filled_quantity"),
        "detail": order.get("last_detail"),
        "open_ids_for_symbol": sorted(order_id for order_id in open_ids if order_id is not None),
    }


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    targets = f.selected_targets(f.discover_targets(root), args.symbols, args.exclude)
    symbols = [target.symbol for target in targets]
    if args.symbols:
        discovered = set(symbols)
        requested = f.csv_set(args.symbols)
        symbols.extend(sorted(requested - discovered))
    symbols = sorted(dict.fromkeys(symbol.upper() for symbol in symbols if symbol.strip()))

    with f.GatewayClient(args.host, args.port, args.connect_timeout_seconds, args.command_timeout_seconds) as client:
        f.register_symbols(client, symbols)
        states = f.sync_positions(client)
        snapshot = f.sync_open_orders(client)

    positions = {symbol: f.position_for(states, symbol) for symbol in symbols}
    nonflat = {symbol: position for symbol, position in positions.items() if position != 0}
    open_or_pending = []
    zero_cancel = []
    all_symbol_orders = []
    symbol_set = set(symbols)
    for raw in list(snapshot.get("orders") or []):
        order = dict(raw or {})
        symbol = str(order.get("symbol", "")).upper()
        if symbol not in symbol_set:
            continue
        row = row_for_order(order, snapshot)
        all_symbol_orders.append(row)
        status = row["status"]
        remaining = row["remaining"]
        filled = as_int(row["filled"])
        gateway_order_id = row["gateway_order_id"]
        open_ids = set(row["open_ids_for_symbol"])
        zero_remaining_cancel_pending = f.is_zero_remaining_non_actionable_order(status, remaining, filled)
        if ((gateway_order_id in open_ids) and (not zero_remaining_cancel_pending)) or ((not f.is_terminal_order_status(status)) and (not zero_remaining_cancel_pending)) or (remaining is not None and remaining > 0):
            open_or_pending.append(row)
        if zero_remaining_cancel_pending:
            zero_cancel.append(row)

    print(
        json.dumps(
            {
                "gateway_connected": snapshot.get("connected"),
                "selected_symbol_count": len(symbols),
                "selected_symbols": symbols,
                "positions": positions,
                "nonflat_count": len(nonflat),
                "nonflat": nonflat,
                "open_or_pending_order_count": len(open_or_pending),
                "open_or_pending_orders": open_or_pending,
                "zero_remaining_cancel_pending_count": len(zero_cancel),
                "zero_remaining_cancel_pending_orders": zero_cancel,
                "all_selected_symbol_orders": all_symbol_orders,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

