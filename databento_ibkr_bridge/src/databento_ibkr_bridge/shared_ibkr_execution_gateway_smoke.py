from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from databento_ibkr_bridge.config import load_config
from databento_ibkr_bridge.shared_ibkr_execution_gateway import SharedIBKRExecutionGateway
from databento_ibkr_bridge.shared_ibkr_execution_models import (
    CancelSymbolOrdersCommand,
    FlattenSymbolCommand,
    GatewayEventType,
    OrderSubmitCommand,
)


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    cfg = replace(load_config(project_root=project_root), dry_run=True)
    gateway = SharedIBKRExecutionGateway.from_config(cfg, dry_run=True)

    gateway.connect()
    gateway.register_symbols(["AAPL", "NVDA"])
    position_req_id = gateway.request_position_sync()
    open_orders_req_id = gateway.request_open_orders_sync()
    assert position_req_id != open_orders_req_id

    first = gateway.submit_order(
        OrderSubmitCommand(symbol="AAPL", action="BUY", quantity=10, order_type="MKT", reference_price=100.0, reason="smoke-aapl")
    )
    second = gateway.submit_order(
        OrderSubmitCommand(symbol="NVDA", action="SELL", quantity=5, order_type="LMT", reference_price=200.0, limit_price=201.5, reason="smoke-nvda")
    )
    assert first.gateway_order_id != second.gateway_order_id

    gateway.record_position("AAPL", position=10, avg_cost=100.25)
    flattened = gateway.flatten_symbol(FlattenSymbolCommand(symbol="AAPL", reference_price=101.0, reason="smoke-flatten"))
    assert flattened is not None
    cancelled = gateway.cancel_symbol_orders(CancelSymbolOrdersCommand(symbol="NVDA", reason="smoke-cancel"))
    assert cancelled

    snapshot = gateway.snapshot()
    assert snapshot.connected is True
    assert snapshot.dry_run is True
    assert snapshot.registered_symbols == ["AAPL", "NVDA"]
    assert snapshot.symbols["AAPL"]["position"] == 10
    assert snapshot.symbols["AAPL"]["request_ids"]["positions"] != snapshot.symbols["NVDA"]["request_ids"]["positions"]
    assert len(snapshot.orders) >= 3
    assert any(event["event_type"] == GatewayEventType.ORDER_SUBMITTED.value for event in snapshot.recent_events)
    assert any(event["event_type"] == GatewayEventType.ORDER_CANCELLED.value for event in snapshot.recent_events)

    gateway.disconnect()
    print(
        "shared-ibkr-gateway-smoke-ok",
        f"symbols={len(snapshot.registered_symbols)}",
        f"orders={len(snapshot.orders)}",
        f"events={len(snapshot.recent_events)}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

