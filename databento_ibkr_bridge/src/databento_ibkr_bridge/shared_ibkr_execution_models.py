from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class GatewayEventType(StrEnum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECOVERING = "recovering"
    RECOVERED = "recovered"
    DISCONNECTED = "disconnected"
    SYMBOL_REGISTERED = "symbol_registered"
    POSITION_SYNC_REQUESTED = "position_sync_requested"
    POSITION_SYNC_COMPLETED = "position_sync_completed"
    OPEN_ORDERS_SYNC_REQUESTED = "open_orders_sync_requested"
    ORDER_SUBMIT_REQUESTED = "order_submit_requested"
    ORDER_SUBMITTED = "order_submitted"
    ORDER_STATUS = "order_status"
    ORDER_CANCEL_REQUESTED = "order_cancel_requested"
    ORDER_CANCELLED = "order_cancelled"
    FLATTEN_REQUESTED = "flatten_requested"
    POSITION_UPDATED = "position_updated"
    ERROR = "error"
    INFO = "info"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MKT = "MKT"
    LMT = "LMT"
    FAST_LMT = "FAST_LMT"


@dataclass(frozen=True)
class SymbolRegistration:
    symbol: str
    strategy_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderSubmitCommand:
    symbol: str
    action: str
    quantity: int
    order_type: str = OrderType.MKT.value
    reference_price: float = 0.0
    limit_price: float | None = None
    tif: str = "IOC"
    outside_rth: bool = False
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CancelSymbolOrdersCommand:
    symbol: str
    reason: str = ""


@dataclass(frozen=True)
class FlattenSymbolCommand:
    symbol: str
    reference_price: float = 0.0
    quantity_override: int | None = None
    reason: str = ""


@dataclass(frozen=True)
class SyncPositionsCommand:
    reason: str = ""


@dataclass(frozen=True)
class SyncOpenOrdersCommand:
    reason: str = ""


@dataclass(frozen=True)
class GatewayEvent:
    event_type: GatewayEventType
    ts_event: datetime
    symbol: str = ""
    detail: str = ""
    gateway_order_id: int | None = None
    ib_order_id: int | None = None
    perm_id: int | None = None
    req_id: int | None = None
    position: int | None = None
    avg_cost: float | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def now(event_type: GatewayEventType, **kwargs: Any) -> "GatewayEvent":
        return GatewayEvent(event_type=event_type, ts_event=datetime.now(UTC), **kwargs)


@dataclass
class RoutedOrder:
    gateway_order_id: int
    symbol: str
    action: str
    quantity: int
    order_type: str
    reference_price: float
    limit_price: float | None
    tif: str
    reason: str
    outside_rth: bool = False
    ib_order_id: int | None = None
    perm_id: int | None = None
    status: str = "created"
    last_detail: str = ""
    filled_quantity: int = 0
    remaining_quantity: int | None = None
    avg_fill_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SymbolExecutionState:
    symbol: str
    strategy_id: str = ""
    position: int = 0
    avg_cost: float = 0.0
    last_error: str = ""
    last_event_type: str = ""
    last_event_detail: str = ""
    last_event_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    request_ids: dict[str, int] = field(default_factory=dict)
    open_gateway_order_ids: set[int] = field(default_factory=set)
    gateway_order_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class GatewaySnapshot:
    connected: bool
    dry_run: bool
    degraded: bool
    degraded_reason: str
    recovering: bool
    recovery_attempts: int
    next_recovery_attempt_in_seconds: float | None
    recovery_last_error: str
    host: str
    port: int
    client_id: int
    registered_symbols: list[str]
    qualified_contract_symbols: list[str]
    orders: list[dict[str, Any]]
    symbols: dict[str, dict[str, Any]]
    recent_events: list[dict[str, Any]]

