from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import logging
import threading
import time
from dataclasses import asdict
from itertools import count
from typing import Any

from databento_ibkr_bridge.config import BridgeConfig
from databento_ibkr_bridge.shared_ibkr_execution_models import (
    CancelSymbolOrdersCommand,
    FlattenSymbolCommand,
    GatewayEvent,
    GatewayEventType,
    GatewaySnapshot,
    OrderSubmitCommand,
    RoutedOrder,
    SymbolExecutionState,
    SymbolRegistration,
    SyncOpenOrdersCommand,
    SyncPositionsCommand,
)


logger = logging.getLogger(__name__)


class SharedIBKRExecutionGateway:
    """First scaffold for a single-client IBKR execution router.

    The goal of this class is to centralize all IBKR socket ownership into one gateway,
    maintain symbol/order/request routing maps, and present a command/event contract that
    many symbol strategies or callers can share.

    This first patch intentionally focuses on:
      - one shared connection lifecycle
      - symbol registration
      - request-id and gateway-order-id allocation
      - dry-run safe order routing
      - live batch order submission through one IBKR client when explicitly enabled
      - explicit placeholders for the callback routing that still must be wired
    """

    def __init__(self, cfg: BridgeConfig, dry_run: bool | None = None) -> None:
        self.cfg = cfg
        self.dry_run = cfg.dry_run if dry_run is None else dry_run
        self.host = cfg.ibkr_host
        self.port = cfg.ibkr_port
        self.client_id = cfg.ibkr_client_id_base
        self._connected = False
        self._ib = None
        self._symbols: dict[str, SymbolExecutionState] = {}
        self._events: list[GatewayEvent] = []
        self._recent_events: list[GatewayEvent] = []
        self._orders_by_gateway_id: dict[int, RoutedOrder] = {}
        self._gateway_order_id_to_trade: dict[int, Any] = {}
        self._ib_order_id_to_gateway_id: dict[int, int] = {}
        self._perm_id_to_gateway_id: dict[int, int] = {}
        self._gateway_order_counter = count(max(1, self.client_id * 100_000))
        self._request_id_counter = count(max(1, self.client_id * 1_000_000))
        self._ib_call_timeout_seconds = max(0.5, float(getattr(cfg, "shared_ibkr_gateway_ib_call_timeout_seconds", 4.0)))
        self._qualified_contract_ttl_seconds = max(1.0, float(getattr(cfg, "shared_ibkr_gateway_qualified_contract_ttl_seconds", 21600.0)))
        self._require_qualified_contracts = bool(getattr(cfg, "shared_ibkr_gateway_require_qualified_contracts", False))
        self._recovery_enabled = bool(getattr(cfg, "shared_ibkr_gateway_recovery_enabled", True))
        self._recovery_cooldown_seconds = max(0.05, float(getattr(cfg, "shared_ibkr_gateway_recovery_cooldown_seconds", 5.0)))
        self._recovery_max_cooldown_seconds = max(self._recovery_cooldown_seconds, float(getattr(cfg, "shared_ibkr_gateway_recovery_max_cooldown_seconds", 60.0)))
        self._live_ib_executor: ThreadPoolExecutor | None = None
        self._thread_local = threading.local()
        self._qualified_contracts: dict[str, tuple[Any, float]] = {}
        self._degraded_reason = ""
        self._degraded_at: float | None = None
        self._recovering = False
        self._recovery_attempts = 0
        self._next_recovery_at: float | None = None
        self._last_recovery_error = ""
        self.current_operation = "idle"
        self.current_operation_started_at: float | None = None

    def connect(self) -> None:
        if self._connected:
            self._emit(GatewayEventType.INFO, detail="already-connected")
            return
        self._emit(GatewayEventType.CONNECTING, detail=f"host={self.host} port={self.port} client_id={self.client_id}")
        if self.dry_run:
            self._connected = True
            self._clear_recovery_state()
            self._emit(GatewayEventType.CONNECTED, detail="dry-run")
            return

        self._reset_live_ib_executor()
        try:
            ib = self._run_live_ib_call(
                "connect.ib.connect",
                self._connect_live_ib,
                timeout_seconds=self._ib_call_timeout_seconds,
            )
        except Exception:
            self._reset_live_ib_executor()
            raise
        self._ib = ib
        self._connected = True
        self._clear_recovery_state()
        self._emit(GatewayEventType.CONNECTED, detail="live")

    def disconnect(self) -> None:
        if not self._connected:
            return
        if self._ib is not None:
            ib = self._ib
            try:
                if self._live_ib_executor is not None:
                    self._run_live_ib_call(
                        "disconnect.ib.disconnect",
                        ib.disconnect,
                        timeout_seconds=self._ib_call_timeout_seconds,
                    )
                else:
                    ib.disconnect()
            except Exception as exc:  # pragma: no cover
                self._emit(GatewayEventType.ERROR, detail=f"disconnect-failed reason={exc}")
        self._ib = None
        self._connected = False
        self._qualified_contracts.clear()
        self._reset_live_ib_executor()
        self._emit(GatewayEventType.DISCONNECTED, detail="gateway-disconnected")

    def is_live_connected(self) -> bool:
        if self.dry_run:
            return self._connected
        return self._connected and self._ib_client_is_connected()

    def register_symbol(self, registration: SymbolRegistration | str) -> SymbolExecutionState:
        if isinstance(registration, str):
            registration = SymbolRegistration(symbol=registration)
        symbol = registration.symbol.strip().upper()
        if not symbol:
            raise ValueError("symbol must be non-empty")
        state = self._symbols.get(symbol)
        if state is None:
            state = SymbolExecutionState(symbol=symbol, strategy_id=registration.strategy_id)
            self._symbols[symbol] = state
        if registration.strategy_id:
            state.strategy_id = registration.strategy_id
        self._ensure_request_bundle(state)
        self._touch_symbol_state(state, GatewayEventType.SYMBOL_REGISTERED, "symbol-registered")
        self._emit(
            GatewayEventType.SYMBOL_REGISTERED,
            symbol=symbol,
            detail=f"strategy_id={state.strategy_id or 'n/a'} request_ids={state.request_ids}",
            payload={"strategy_id": state.strategy_id, "request_ids": dict(state.request_ids), "metadata": registration.metadata},
        )
        return state

    def register_symbols(self, symbols: list[str]) -> None:
        for symbol in symbols:
            self.register_symbol(symbol)

    def request_position_sync(self, command: SyncPositionsCommand | None = None) -> int:
        req_id = next(self._request_id_counter)
        detail = (command.reason if command else "") or "position-sync"
        self._emit(GatewayEventType.POSITION_SYNC_REQUESTED, req_id=req_id, detail=detail)
        if self.dry_run:
            for symbol, state in sorted(self._symbols.items()):
                self.record_position(symbol, position=state.position, avg_cost=state.avg_cost, req_id=req_id, detail="position-sync-dry-run", force_event=True)
        else:
            self._require_connected()
            with self._track_operation("request_position_sync.refresh_positions", log_start=True, log_success=True):
                self._refresh_positions(req_id=req_id, force_registered=True)
        self._emit(
            GatewayEventType.POSITION_SYNC_COMPLETED,
            req_id=req_id,
            detail=detail,
            payload={"registered_symbols": len(self._symbols)},
        )
        return req_id

    def request_open_orders_sync(self, command: SyncOpenOrdersCommand | None = None) -> int:
        req_id = next(self._request_id_counter)
        detail = (command.reason if command else "") or "open-orders-sync"
        self._emit(GatewayEventType.OPEN_ORDERS_SYNC_REQUESTED, req_id=req_id, detail=detail)
        if not self.dry_run and self._ib is not None:
            with self._track_operation("request_open_orders_sync.refresh_open_orders", log_start=True, log_success=True):
                self._refresh_open_orders(force_event=True)
        return req_id

    def submit_order(self, command: OrderSubmitCommand) -> RoutedOrder:
        self._require_connected()
        state = self.register_symbol(command.symbol)
        normalized_action = command.action.strip().upper()
        is_closing_trade = self._is_closing_trade_command(command)
        duplicate_closing_order = self._find_active_closing_order(state, normalized_action) if is_closing_trade else None
        if duplicate_closing_order is not None:
            self._emit(
                GatewayEventType.INFO,
                symbol=state.symbol,
                gateway_order_id=duplicate_closing_order.gateway_order_id,
                ib_order_id=duplicate_closing_order.ib_order_id,
                perm_id=duplicate_closing_order.perm_id,
                detail="duplicate-closing-order-suppressed",
                payload={
                    "action": duplicate_closing_order.action,
                    "quantity": duplicate_closing_order.quantity,
                    "status": duplicate_closing_order.status,
                    "requested_quantity": max(0, int(command.quantity)),
                    "reason": command.reason,
                },
            )
            return duplicate_closing_order
        gateway_order_id = next(self._gateway_order_counter)
        normalized_order_type = self._normalized_order_type_for_action(normalized_action, is_closing_trade, command.order_type)
        normalized_limit_price = self._normalized_limit_price_for_order_type(normalized_order_type, command.limit_price, float(command.reference_price or 0.0))
        routed = RoutedOrder(
            gateway_order_id=gateway_order_id,
            symbol=state.symbol,
            action=normalized_action,
            quantity=max(0, int(command.quantity)),
            order_type=normalized_order_type,
            reference_price=float(command.reference_price or 0.0),
            limit_price=normalized_limit_price,
            tif=(command.tif or "IOC").strip().upper(),
            reason=command.reason.strip(),
            outside_rth=bool(command.outside_rth),
            metadata=dict(command.metadata),
        )
        self._orders_by_gateway_id[gateway_order_id] = routed
        state.gateway_order_ids.append(gateway_order_id)
        state.open_gateway_order_ids.add(gateway_order_id)
        self._emit(
            GatewayEventType.ORDER_SUBMIT_REQUESTED,
            symbol=state.symbol,
            gateway_order_id=gateway_order_id,
            detail=(command.reason or "submit-order"),
            payload={
                "action": routed.action,
                "quantity": routed.quantity,
                "order_type": routed.order_type,
                "reference_price": routed.reference_price,
                "limit_price": routed.limit_price,
                "tif": routed.tif,
                "outside_rth": routed.outside_rth,
                "metadata": routed.metadata,
            },
        )

        if self.dry_run:
            routed.status = "submitted"
            routed.last_detail = "dry-run"
            self._touch_symbol_state(state, GatewayEventType.ORDER_SUBMITTED, "dry-run-submit")
            self._emit(
                GatewayEventType.ORDER_SUBMITTED,
                symbol=state.symbol,
                gateway_order_id=gateway_order_id,
                detail="dry-run",
                payload={"action": routed.action, "quantity": routed.quantity, "order_type": routed.order_type},
            )
            return routed

        if self._ib is None:  # pragma: no cover
            raise RuntimeError("shared IBKR gateway is not connected")

        try:
            from ib_async import LimitOrder, MarketOrder, Stock  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("ib_async is required for live shared IBKR gateway mode.") from exc

        contract = self._resolve_live_contract(state.symbol, Stock)
        order = self._build_live_order(command, routed, MarketOrder, LimitOrder)
        trade = self._run_live_ib_call(
            f"submit_order.place_order[{state.symbol}]",
            lambda: self._ib.placeOrder(contract, order),
            symbol=state.symbol,
            timeout_seconds=self._ib_call_timeout_seconds,
        )
        self._gateway_order_id_to_trade[gateway_order_id] = trade
        ib_order_id = getattr(getattr(trade, "order", None), "orderId", None)
        perm_id = getattr(getattr(trade, "order", None), "permId", None)
        routed.ib_order_id = ib_order_id
        routed.perm_id = perm_id
        if ib_order_id is not None:
            self._ib_order_id_to_gateway_id[int(ib_order_id)] = gateway_order_id
        if perm_id is not None:
            self._perm_id_to_gateway_id[int(perm_id)] = gateway_order_id
        self._register_trade_callbacks(gateway_order_id, trade)
        self._touch_symbol_state(state, GatewayEventType.ORDER_SUBMITTED, "live-submit")
        self._emit(
            GatewayEventType.ORDER_SUBMITTED,
            symbol=state.symbol,
            gateway_order_id=gateway_order_id,
            ib_order_id=ib_order_id,
            perm_id=perm_id,
            detail="live",
            payload={"action": routed.action, "quantity": routed.quantity, "order_type": routed.order_type},
        )
        self._sync_trade_status_from_trade(gateway_order_id, trade, detail="live-submit-status", force_event=True)
        return routed

    def quote_snapshot(self, symbol: str) -> dict[str, float | str]:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must be non-empty")
        self._require_connected()
        if self.dry_run or self._ib is None:
            self._emit(GatewayEventType.INFO, symbol=normalized_symbol, detail="quote-snapshot-dry-run")
            return {"symbol": normalized_symbol, "bid": 0.0, "ask": 0.0, "last": 0.0, "market_price": 0.0}
        try:
            from ib_async import Stock  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("ib_async is required for live shared IBKR gateway mode.") from exc

        # reqTickers/open ticker bookkeeping in ib_async requires a hashable contract with conId populated.
        # The hot submit path may deliberately use unqualified Stock contracts, but quote snapshots cannot.
        contract = self._resolve_live_contract(normalized_symbol, Stock, force_qualification=True)
        tickers = self._run_live_ib_call(
            f"quote_snapshot.req_tickers[{normalized_symbol}]",
            lambda: self._ib.reqTickers(contract),
            symbol=normalized_symbol,
            timeout_seconds=self._ib_call_timeout_seconds,
        )
        ticker = tickers[0] if isinstance(tickers, (list, tuple)) and tickers else tickers
        bid = self._coerce_optional_float(getattr(ticker, "bid", None)) or 0.0
        ask = self._coerce_optional_float(getattr(ticker, "ask", None)) or 0.0
        last = self._coerce_optional_float(getattr(ticker, "last", None)) or 0.0
        market_price_value = getattr(ticker, "marketPrice", None)
        try:
            market_price = self._coerce_optional_float(market_price_value() if callable(market_price_value) else market_price_value) or 0.0
        except Exception:
            market_price = 0.0
        payload = {"symbol": normalized_symbol, "bid": bid, "ask": ask, "last": last, "market_price": market_price}
        self._emit(GatewayEventType.INFO, symbol=normalized_symbol, detail="quote-snapshot", payload=payload)
        return payload

    def cancel_symbol_orders(self, command: CancelSymbolOrdersCommand) -> list[int]:
        state = self.register_symbol(command.symbol)
        open_ids = sorted(state.open_gateway_order_ids)
        self._emit(
            GatewayEventType.ORDER_CANCEL_REQUESTED,
            symbol=state.symbol,
            detail=(command.reason or "cancel-symbol-orders"),
            payload={"gateway_order_ids": list(open_ids)},
        )
        cancelled: list[int] = []
        for gateway_order_id in open_ids:
            trade = self._gateway_order_id_to_trade.get(gateway_order_id)
            if self.dry_run:
                self.record_order_status(gateway_order_id=gateway_order_id, status="Cancelled", detail="dry-run-cancel")
                cancelled.append(gateway_order_id)
                continue
            if self._ib is None or trade is None:
                self.record_error(state.symbol, f"cancel-not-routable gateway_order_id={gateway_order_id}")
                continue
            try:  # pragma: no cover
                self._ib.cancelOrder(trade.order)
                self.record_order_status(gateway_order_id=gateway_order_id, status="CancelSubmitted", detail="live-cancel-requested")
                cancelled.append(gateway_order_id)
            except Exception as exc:  # pragma: no cover
                self.record_error(state.symbol, f"cancel-failed gateway_order_id={gateway_order_id} reason={exc}")
        return cancelled

    def flatten_symbol(self, command: FlattenSymbolCommand) -> RoutedOrder | None:
        state = self.register_symbol(command.symbol)
        quantity = command.quantity_override if command.quantity_override is not None else abs(state.position)
        if quantity <= 0:
            self._emit(GatewayEventType.FLATTEN_REQUESTED, symbol=state.symbol, detail="already-flat")
            return None
        action = "SELL" if state.position > 0 else "BUY"
        self._emit(
            GatewayEventType.FLATTEN_REQUESTED,
            symbol=state.symbol,
            detail=(command.reason or "flatten-symbol"),
            payload={"quantity": quantity, "action": action, "position": state.position},
        )
        return self.submit_order(
            OrderSubmitCommand(
                symbol=state.symbol,
                action=action,
                quantity=quantity,
                order_type=self._normalized_order_type_for_action(action, True),
                reference_price=command.reference_price,
                reason=command.reason or "flatten-symbol",
                metadata={"closing_trade": True},
            )
        )

    def record_position(
        self,
        symbol: str,
        position: int,
        avg_cost: float,
        *,
        req_id: int | None = None,
        detail: str = "position-callback",
        force_event: bool = False,
    ) -> None:
        state = self.register_symbol(symbol)
        next_position = int(position)
        next_avg_cost = float(avg_cost)
        changed = force_event or state.position != next_position or abs(state.avg_cost - next_avg_cost) > 1e-9
        state.position = next_position
        state.avg_cost = next_avg_cost
        if not changed:
            return
        self._touch_symbol_state(state, GatewayEventType.POSITION_UPDATED, f"position={state.position}")
        self._emit(
            GatewayEventType.POSITION_UPDATED,
            symbol=state.symbol,
            position=state.position,
            avg_cost=state.avg_cost,
            req_id=req_id,
            detail=detail,
        )

    def record_order_status(
        self,
        *,
        gateway_order_id: int | None = None,
        ib_order_id: int | None = None,
        perm_id: int | None = None,
        status: str,
        detail: str = "",
        remaining: int | None = None,
        filled: int | None = None,
        avg_fill_price: float | None = None,
        force_event: bool = False,
    ) -> None:
        routed = self._resolve_routed_order(gateway_order_id=gateway_order_id, ib_order_id=ib_order_id, perm_id=perm_id)
        if routed is None:
            self._emit(
                GatewayEventType.ERROR,
                detail=f"unmapped-order-status status={status} gateway_order_id={gateway_order_id} ib_order_id={ib_order_id} perm_id={perm_id}",
            )
            return
        normalized_status = status.strip() or routed.status or "Unknown"
        normalized_remaining = None if remaining is None else int(remaining)
        normalized_filled = routed.filled_quantity if filled is None else int(filled)
        normalized_avg_fill_price = None if avg_fill_price is None else float(avg_fill_price)
        changed = force_event or any(
            (
                routed.status != normalized_status,
                routed.last_detail != detail,
                routed.remaining_quantity != normalized_remaining,
                routed.filled_quantity != normalized_filled,
                routed.avg_fill_price != normalized_avg_fill_price,
            )
        )
        routed.status = normalized_status
        routed.last_detail = detail
        routed.remaining_quantity = normalized_remaining
        routed.filled_quantity = normalized_filled
        routed.avg_fill_price = normalized_avg_fill_price
        if not changed:
            return
        state = self.register_symbol(routed.symbol)
        terminal = self._is_effectively_terminal_order_status(normalized_status, normalized_remaining, normalized_filled, routed.tif)
        if terminal:
            state.open_gateway_order_ids.discard(routed.gateway_order_id)
        self._touch_symbol_state(state, GatewayEventType.ORDER_STATUS, f"status={status}")
        self._emit(
            GatewayEventType.ORDER_STATUS,
            symbol=routed.symbol,
            gateway_order_id=routed.gateway_order_id,
            ib_order_id=routed.ib_order_id,
            perm_id=routed.perm_id,
            detail=detail or normalized_status,
            payload={
                "status": normalized_status,
                "remaining": normalized_remaining,
                "filled": normalized_filled,
                "avg_fill_price": normalized_avg_fill_price,
                "effective_terminal": terminal,
            },
        )
        if terminal and self._is_cancel_or_noop_terminal_order_status(normalized_status, normalized_remaining, normalized_filled, routed.tif):
            self._emit(
                GatewayEventType.ORDER_CANCELLED,
                symbol=routed.symbol,
                gateway_order_id=routed.gateway_order_id,
                ib_order_id=routed.ib_order_id,
                perm_id=routed.perm_id,
                detail=detail or status,
            )

    def record_error(self, symbol: str, detail: str) -> None:
        state = self.register_symbol(symbol)
        state.last_error = detail
        self._touch_symbol_state(state, GatewayEventType.ERROR, detail)
        self._emit(GatewayEventType.ERROR, symbol=state.symbol, detail=detail)

    def drain_events(self) -> list[GatewayEvent]:
        events = list(self._events)
        self._events.clear()
        return events

    def poll_live_state(self) -> None:
        if self.dry_run or self._ib is None or not self._connected:
            return
        with self._track_operation("poll_live_state.refresh_positions", log_start=False, log_success=False):
            self._refresh_positions(force_registered=False)
        with self._track_operation("poll_live_state.refresh_open_orders", log_start=False, log_success=False):
            self._refresh_open_orders(force_event=False)

    def _register_trade_callbacks(self, gateway_order_id: int, trade: Any) -> None:
        callbacks: list[tuple[str, Any]] = []
        for event_name, detail in (
            ("statusEvent", "live-status-callback"),
            ("modifyEvent", "live-modify-callback"),
            ("fillEvent", "live-fill-callback"),
            ("filledEvent", "live-filled-callback"),
            ("cancelEvent", "live-cancel-callback"),
            ("cancelledEvent", "live-cancelled-callback"),
        ):
            event = getattr(trade, event_name, None)
            if event is None:
                continue

            def _handler(*_args: Any, _gateway_order_id: int = gateway_order_id, _trade: Any = trade, _detail: str = detail) -> None:
                self._sync_trade_status_from_trade(_gateway_order_id, _trade, detail=_detail, force_event=True)

            try:
                event += _handler
                callbacks.append((event_name, _handler))
            except Exception as exc:  # pragma: no cover - depends on ib_async event implementation
                self._emit(GatewayEventType.ERROR, detail=f"trade-callback-register-failed event={event_name} gateway_order_id={gateway_order_id} reason={exc}")
        if callbacks:
            try:
                setattr(trade, "_shared_gateway_callbacks", callbacks)
            except Exception:
                pass

    def _sync_trade_status_from_trade(self, gateway_order_id: int, trade: Any, *, detail: str, force_event: bool) -> None:
        order = getattr(trade, "order", None)
        order_status = getattr(trade, "orderStatus", None)
        ib_order_id = self._coerce_optional_int(getattr(order, "orderId", None))
        perm_id = self._coerce_optional_int(getattr(order, "permId", None))
        status = str(getattr(order_status, "status", "") or getattr(trade, "status", "") or "Submitted")
        remaining = self._coerce_optional_int(getattr(order_status, "remaining", None))
        filled = self._coerce_optional_int(getattr(order_status, "filled", None))
        avg_fill_price = self._coerce_optional_float(getattr(order_status, "avgFillPrice", None))
        inferred_filled, inferred_avg_price = self._infer_fill_summary_from_trade(trade)
        if filled is None and inferred_filled is not None:
            filled = inferred_filled
        if avg_fill_price is None and inferred_avg_price is not None:
            avg_fill_price = inferred_avg_price
        self.record_order_status(
            gateway_order_id=gateway_order_id,
            ib_order_id=ib_order_id,
            perm_id=perm_id,
            status=status,
            detail=detail,
            remaining=remaining,
            filled=filled,
            avg_fill_price=avg_fill_price,
            force_event=force_event,
        )

    def _infer_fill_summary_from_trade(self, trade: Any) -> tuple[int | None, float | None]:
        fills = getattr(trade, "fills", None)
        try:
            fill_items = fills() if callable(fills) else list(fills or [])
        except Exception:
            return None, None
        total_shares = 0.0
        total_notional = 0.0
        for fill in fill_items:
            execution = getattr(fill, "execution", fill)
            shares = self._coerce_optional_float(getattr(execution, "shares", None))
            price = self._coerce_optional_float(getattr(execution, "price", None))
            if shares is None or shares <= 0.0:
                continue
            total_shares += shares
            if price is not None and price > 0.0:
                total_notional += shares * price
        if total_shares <= 0.0:
            return None, None
        avg_price = total_notional / total_shares if total_notional > 0.0 else None
        return int(total_shares), avg_price

    def maybe_auto_recover(self) -> bool:
        if self.dry_run or not self._recovery_enabled or not self._degraded_reason or self._connected or self._recovering:
            return False
        now = time.monotonic()
        if self._next_recovery_at is not None and now < self._next_recovery_at:
            return False

        attempt_number = self._recovery_attempts + 1
        self._recovering = True
        self._emit(
            GatewayEventType.RECOVERING,
            detail=f"attempt={attempt_number} degraded_reason={self._degraded_reason or 'unknown'}",
            payload={"attempt": attempt_number},
        )
        try:
            self.connect()
        except Exception as exc:
            detail = (
                f"attempt={attempt_number} reason={exc.__class__.__name__}: {exc}"
                if str(exc).strip()
                else f"attempt={attempt_number} reason={exc.__class__.__name__}"
            )
            failed_ib = self._ib
            self._ib = None
            self._connected = False
            self._reset_live_ib_executor()
            if failed_ib is not None:
                try:
                    failed_ib.disconnect()
                except Exception:  # pragma: no cover
                    logger.exception("shared IBKR gateway recovery disconnect failed attempt=%s", attempt_number)
            self._recovery_attempts = attempt_number
            self._last_recovery_error = detail
            self._next_recovery_at = now + self._recovery_delay_seconds(attempt_number + 1)
            self._emit(GatewayEventType.ERROR, detail=f"gateway-recovery-failed {detail}")
            return False
        finally:
            self._recovering = False

        self._emit(GatewayEventType.RECOVERED, detail=f"attempt={attempt_number}", payload={"attempt": attempt_number})
        return True

    def snapshot(self) -> GatewaySnapshot:
        symbols = {
            symbol: {
                "strategy_id": state.strategy_id,
                "position": state.position,
                "avg_cost": state.avg_cost,
                "last_error": state.last_error,
                "last_event_type": state.last_event_type,
                "last_event_detail": state.last_event_detail,
                "last_event_at": state.last_event_at.isoformat(),
                "request_ids": dict(state.request_ids),
                "open_gateway_order_ids": sorted(state.open_gateway_order_ids),
                "gateway_order_ids": list(state.gateway_order_ids),
            }
            for symbol, state in sorted(self._symbols.items())
        }
        orders = [asdict(order) for _, order in sorted(self._orders_by_gateway_id.items())]
        recent_events = [
            {
                "event_type": event.event_type.value,
                "ts_event": event.ts_event.isoformat(),
                "symbol": event.symbol,
                "detail": event.detail,
                "gateway_order_id": event.gateway_order_id,
                "ib_order_id": event.ib_order_id,
                "perm_id": event.perm_id,
                "req_id": event.req_id,
                "position": event.position,
                "avg_cost": event.avg_cost,
                "payload": dict(event.payload),
            }
            for event in self._recent_events[-200:]
        ]
        return GatewaySnapshot(
            connected=self.is_live_connected(),
            dry_run=self.dry_run,
            degraded=bool(self._degraded_reason),
            degraded_reason=self._degraded_reason,
            recovering=self._recovering,
            recovery_attempts=self._recovery_attempts,
            next_recovery_attempt_in_seconds=self._next_recovery_delay_seconds(),
            recovery_last_error=self._last_recovery_error,
            host=self.host,
            port=self.port,
            client_id=self.client_id,
            registered_symbols=sorted(self._symbols),
            qualified_contract_symbols=sorted(self._qualified_contracts),
            orders=orders,
            symbols=symbols,
            recent_events=recent_events,
        )

    @classmethod
    def from_config(cls, cfg: BridgeConfig, dry_run: bool | None = None) -> "SharedIBKRExecutionGateway":
        return cls(cfg=cfg, dry_run=dry_run)

    def _emit(self, event_type: GatewayEventType, **kwargs: Any) -> GatewayEvent:
        event = GatewayEvent.now(event_type=event_type, **kwargs)
        self._events.append(event)
        self._recent_events.append(event)
        if len(self._recent_events) > 500:
            del self._recent_events[:-500]
        return event

    @contextlib.contextmanager
    def _track_operation(self, stage: str, *, symbol: str = "", log_start: bool, log_success: bool):
        previous_operation = self.current_operation
        previous_started_at = self.current_operation_started_at
        started_at = time.monotonic()
        self.current_operation = stage
        self.current_operation_started_at = started_at
        if log_start:
            logger.info("shared IBKR gateway operation start stage=%s symbol=%s", stage, symbol)
        try:
            yield
            if log_success:
                logger.info(
                    "shared IBKR gateway operation done stage=%s symbol=%s elapsedMs=%.1f",
                    stage,
                    symbol,
                    (time.monotonic() - started_at) * 1000.0,
                )
        except Exception:
            logger.exception(
                "shared IBKR gateway operation failed stage=%s symbol=%s elapsedMs=%.1f",
                stage,
                symbol,
                (time.monotonic() - started_at) * 1000.0,
            )
            raise
        finally:
            self.current_operation = previous_operation
            self.current_operation_started_at = previous_started_at

    def _touch_symbol_state(self, state: SymbolExecutionState, event_type: GatewayEventType, detail: str) -> None:
        state.last_event_type = event_type.value
        state.last_event_detail = detail
        state.last_event_at = GatewayEvent.now(GatewayEventType.INFO).ts_event

    def _reset_live_ib_executor(self) -> None:
        executor = self._live_ib_executor
        self._live_ib_executor = None
        if executor is not None:
            submitted_loop_close = False
            with contextlib.suppress(Exception):
                executor.submit(self._close_current_thread_event_loop)
                submitted_loop_close = True
            executor.shutdown(wait=False, cancel_futures=not submitted_loop_close)

    def _ensure_live_ib_executor(self) -> ThreadPoolExecutor:
        if self._live_ib_executor is None:
            self._live_ib_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="shared-ibkr-gateway-ib",
                initializer=self._ensure_current_thread_event_loop,
            )
        return self._live_ib_executor

    def _ensure_current_thread_event_loop(self) -> asyncio.AbstractEventLoop:
        loop = getattr(self._thread_local, "event_loop", None)
        if loop is not None and not loop.is_closed():
            asyncio.set_event_loop(loop)
            return loop
        try:
            loop = asyncio.get_running_loop()
            owned = False
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            owned = True
        self._thread_local.event_loop = loop
        self._thread_local.event_loop_owned = owned
        return loop

    def _close_current_thread_event_loop(self) -> None:
        loop = getattr(self._thread_local, "event_loop", None)
        owned = bool(getattr(self._thread_local, "event_loop_owned", False))
        self._thread_local.event_loop = None
        self._thread_local.event_loop_owned = False
        if owned and loop is not None and not loop.is_closed():
            asyncio.set_event_loop(None)
            loop.close()

    def _run_live_ib_call(
        self,
        stage: str,
        callback,
        *,
        symbol: str = "",
        timeout_seconds: float | None = None,
    ):
        timeout = max(0.5, self._ib_call_timeout_seconds if timeout_seconds is None else float(timeout_seconds))
        executor = self._ensure_live_ib_executor()
        future = executor.submit(self._execute_tracked_ib_call, stage, symbol, callback)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError as exc:
            detail = f"timeout-after-{timeout:.3f}s"
            self._mark_gateway_degraded(stage=stage, detail=detail, symbol=symbol)
            raise TimeoutError(f"shared IBKR live call timed out stage={stage} symbol={symbol or '<global>'} timeoutSeconds={timeout:.3f}") from exc
        except Exception as exc:
            if self._is_ib_disconnect_exception(exc) or (self._connected and not self._ib_client_is_connected()):
                self._mark_gateway_degraded(stage=stage, detail=self._format_exception_detail(exc), symbol=symbol)
            raise

    def _execute_tracked_ib_call(self, stage: str, symbol: str, callback):
        self._ensure_current_thread_event_loop()
        with self._track_operation(stage, symbol=symbol, log_start=True, log_success=True):
            return callback()

    def _connect_live_ib(self):
        try:
            from ib_async import IB  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("ib_async is required for live shared IBKR gateway mode.") from exc

        ib = IB()
        try:
            ib.connect(self.host, self.port, clientId=self.client_id)
        except Exception:
            with contextlib.suppress(Exception):
                ib.disconnect()
            raise
        if not self._ib_client_is_connected(ib):
            with contextlib.suppress(Exception):
                ib.disconnect()
            raise ConnectionError("IB client connect returned but isConnected=false")
        return ib

    def _ib_client_is_connected(self, ib: Any | None = None) -> bool:
        live_ib = self._ib if ib is None else ib
        if live_ib is None:
            return False
        for source in (live_ib, getattr(live_ib, "client", None)):
            if source is None:
                continue
            for attr_name in ("isConnected", "connected"):
                attr = getattr(source, attr_name, None)
                if attr is None:
                    continue
                try:
                    value = attr() if callable(attr) else attr
                except Exception:
                    return False
                if value is not None:
                    return bool(value)
        # Test doubles and some wrappers may not expose an explicit connectivity accessor. If a live IB object exists
        # and no accessor is available, preserve the previous optimistic behavior until a broker call fails.
        return True

    def _is_ib_disconnect_exception(self, exc: Exception) -> bool:
        text = f"{exc.__class__.__name__}: {exc}".strip().lower()
        return any(marker in text for marker in ("connectionerror", "not connected", "disconnected", "connection closed", "connection reset"))

    def _format_exception_detail(self, exc: Exception) -> str:
        detail = str(exc).strip()
        return exc.__class__.__name__ if not detail else f"{exc.__class__.__name__}: {detail}"

    def _mark_gateway_degraded(self, *, stage: str, detail: str, symbol: str = "") -> None:
        summary = f"stage={stage} detail={detail}"
        self._degraded_reason = summary
        self._degraded_at = time.monotonic()
        self._recovering = False
        self._last_recovery_error = ""
        self._next_recovery_at = None if not self._recovery_enabled else self._degraded_at + self._recovery_delay_seconds(self._recovery_attempts + 1)
        self._qualified_contracts.clear()
        if symbol:
            state = self._symbols.get(symbol)
            if state is not None:
                state.last_error = summary
        self._emit(GatewayEventType.ERROR, symbol=symbol, detail=f"gateway-degraded {summary}")
        stuck_ib = self._ib
        self._ib = None
        self._connected = False
        self._reset_live_ib_executor()
        if stuck_ib is not None:
            try:
                stuck_ib.disconnect()
            except Exception as exc:  # pragma: no cover
                self._emit(GatewayEventType.ERROR, symbol=symbol, detail=f"disconnect-after-stall-failed reason={exc}")
        self._emit(GatewayEventType.DISCONNECTED, symbol=symbol, detail=f"gateway-degraded {summary}")

    def _resolve_live_contract(self, symbol: str, Stock, *, force_qualification: bool = False):
        now = time.monotonic()
        cached = self._qualified_contracts.get(symbol)
        if cached is not None and cached[1] > now:
            return cached[0]
        contract = Stock(symbol, "SMART", "USD")
        if not force_qualification and not self._require_qualified_contracts:
            self._emit(
                GatewayEventType.INFO,
                symbol=symbol,
                detail="using-unqualified-stock-contract",
                payload={"reason": "qualification-disabled-for-submit-hot-path"},
            )
            return contract
        qualified = self._run_live_ib_call(
            f"{'quote_snapshot' if force_qualification else 'submit_order'}.qualify_contracts[{symbol}]",
            lambda: self._ib.qualifyContracts(contract),
            symbol=symbol,
            timeout_seconds=self._ib_call_timeout_seconds,
        )
        if isinstance(qualified, (list, tuple)) and qualified:
            contract = qualified[0]
        self._qualified_contracts[symbol] = (contract, now + self._qualified_contract_ttl_seconds)
        return contract

    def _ensure_request_bundle(self, state: SymbolExecutionState) -> None:
        if state.request_ids:
            return
        state.request_ids["positions"] = next(self._request_id_counter)
        state.request_ids["open_orders"] = next(self._request_id_counter)
        state.request_ids["executions"] = next(self._request_id_counter)
        state.request_ids["account_updates"] = next(self._request_id_counter)

    def _require_connected(self) -> None:
        if not self.is_live_connected():
            if self._connected and not self.dry_run and not self._degraded_reason:
                self._mark_gateway_degraded(stage="connection-check", detail="ib-client-not-connected")
            if self._degraded_reason:
                next_attempt = self._next_recovery_delay_seconds()
                if next_attempt is None:
                    raise RuntimeError(f"shared IBKR execution gateway circuit open reason={self._degraded_reason}")
                raise RuntimeError(
                    "shared IBKR execution gateway circuit open "
                    f"reason={self._degraded_reason} nextRecoveryAttemptInSeconds={next_attempt:.3f}"
                )
            raise RuntimeError("shared IBKR execution gateway is not connected")

    def _clear_recovery_state(self) -> None:
        self._degraded_reason = ""
        self._degraded_at = None
        self._recovering = False
        self._recovery_attempts = 0
        self._next_recovery_at = None
        self._last_recovery_error = ""

    def _recovery_delay_seconds(self, attempt_number: int) -> float:
        exponent = max(0, int(attempt_number) - 1)
        return min(self._recovery_max_cooldown_seconds, self._recovery_cooldown_seconds * (2**exponent))

    def _next_recovery_delay_seconds(self) -> float | None:
        if self._next_recovery_at is None:
            return None
        return max(0.0, round(self._next_recovery_at - time.monotonic(), 3))

    def _resolve_routed_order(
        self,
        *,
        gateway_order_id: int | None,
        ib_order_id: int | None,
        perm_id: int | None,
    ) -> RoutedOrder | None:
        if gateway_order_id is not None:
            return self._orders_by_gateway_id.get(gateway_order_id)
        if ib_order_id is not None:
            mapped = self._ib_order_id_to_gateway_id.get(int(ib_order_id))
            if mapped is not None:
                return self._orders_by_gateway_id.get(mapped)
        if perm_id is not None:
            mapped = self._perm_id_to_gateway_id.get(int(perm_id))
            if mapped is not None:
                return self._orders_by_gateway_id.get(mapped)
        return None

    def _refresh_positions(self, *, req_id: int | None = None, force_registered: bool) -> None:
        try:
            live_positions = self._read_live_positions()
        except Exception as exc:  # pragma: no cover
            self._emit(GatewayEventType.ERROR, req_id=req_id, detail=f"position-sync-failed reason={exc}")
            raise
        seen_symbols: set[str] = set()
        for entry in live_positions or []:
            contract = getattr(entry, "contract", None)
            symbol = str(getattr(contract, "symbol", "") or "").strip().upper()
            if not symbol or (self._symbols and symbol not in self._symbols):
                continue
            seen_symbols.add(symbol)
            self.record_position(
                symbol,
                position=self._coerce_int(getattr(entry, "position", None)),
                avg_cost=self._coerce_float(getattr(entry, "avgCost", None)),
                req_id=req_id,
                detail="position-sync" if req_id is not None else "position-poll",
                force_event=force_registered,
            )
        for symbol, state in sorted(self._symbols.items()):
            if symbol in seen_symbols:
                continue
            if force_registered or state.position != 0 or abs(state.avg_cost) > 1e-9:
                self.record_position(
                    symbol,
                    position=0,
                    avg_cost=0.0,
                    req_id=req_id,
                    detail="position-sync-flat" if req_id is not None else "position-poll-flat",
                    force_event=True,
                )

    def _read_live_positions(self) -> list[Any]:
        req_positions_source = getattr(self._ib, "reqPositions", None)
        if callable(req_positions_source):
            live_positions = self._run_live_ib_call(
                "request_position_sync.req_positions",
                req_positions_source,
                timeout_seconds=self._ib_call_timeout_seconds,
            )
            if live_positions is not None:
                return list(live_positions or [])
        positions_source = getattr(self._ib, "positions", None)
        if callable(positions_source):
            return list(
                self._run_live_ib_call(
                    "request_position_sync.positions_snapshot",
                    positions_source,
                    timeout_seconds=self._ib_call_timeout_seconds,
                )
                or []
            )
        return list(positions_source or [])

    def _refresh_open_orders(self, *, force_event: bool) -> None:
        trades_source = getattr(self._ib, "openTrades", None)
        if trades_source is None:
            trades_source = getattr(self._ib, "trades", None)
        try:
            live_trades = (
                self._run_live_ib_call(
                    "request_open_orders_sync.open_trades_snapshot",
                    trades_source,
                    timeout_seconds=self._ib_call_timeout_seconds,
                )
                if callable(trades_source)
                else list(trades_source or [])
            )
        except Exception as exc:  # pragma: no cover
            self._emit(GatewayEventType.ERROR, detail=f"open-orders-sync-failed reason={exc}")
            return
        seen_gateway_order_ids: set[int] = set()
        for trade in live_trades or []:
            contract = getattr(trade, "contract", None)
            symbol = str(getattr(contract, "symbol", "") or "").strip().upper()
            if not symbol or (self._symbols and symbol not in self._symbols):
                continue
            order = getattr(trade, "order", None)
            ib_order_id = self._coerce_optional_int(getattr(order, "orderId", None))
            perm_id = self._coerce_optional_int(getattr(order, "permId", None))
            gateway_order_id = self._resolve_gateway_order_id_for_trade(trade, ib_order_id=ib_order_id, perm_id=perm_id)
            if gateway_order_id is None:
                continue
            seen_gateway_order_ids.add(gateway_order_id)
            self._sync_trade_status_from_trade(gateway_order_id, trade, detail="live-poll", force_event=force_event)
        if force_event:
            self._clear_gateway_orders_absent_from_live_open_trades(seen_gateway_order_ids)

    def _clear_gateway_orders_absent_from_live_open_trades(self, seen_gateway_order_ids: set[int]) -> None:
        for symbol, state in sorted(self._symbols.items()):
            stale_gateway_order_ids = sorted(state.open_gateway_order_ids.difference(seen_gateway_order_ids))
            for gateway_order_id in stale_gateway_order_ids:
                routed = self._orders_by_gateway_id.get(gateway_order_id)
                if routed is None:
                    state.open_gateway_order_ids.discard(gateway_order_id)
                    self._emit(
                        GatewayEventType.INFO,
                        symbol=symbol,
                        gateway_order_id=gateway_order_id,
                        detail="cleared-unmapped-gateway-open-order-id",
                    )
                    continue
                self.record_order_status(
                    gateway_order_id=gateway_order_id,
                    ib_order_id=routed.ib_order_id,
                    perm_id=routed.perm_id,
                    status="Inactive",
                    detail="broker-open-order-absent",
                    remaining=0,
                    filled=routed.filled_quantity,
                    avg_fill_price=routed.avg_fill_price,
                    force_event=True,
                )

    def _resolve_gateway_order_id_for_trade(self, trade: Any, *, ib_order_id: int | None, perm_id: int | None) -> int | None:
        if ib_order_id is not None:
            mapped = self._ib_order_id_to_gateway_id.get(int(ib_order_id))
            if mapped is not None:
                return mapped
        if perm_id is not None:
            mapped = self._perm_id_to_gateway_id.get(int(perm_id))
            if mapped is not None:
                return mapped
        for gateway_order_id, known_trade in self._gateway_order_id_to_trade.items():
            if known_trade is trade:
                return gateway_order_id
        return None

    def _is_terminal_order_status(self, status: str) -> bool:
        normalized = status.strip().lower()
        return normalized in {"filled", "cancelled", "inactive", "apicancelled"}

    def _is_effectively_terminal_order_status(self, status: str, remaining: int | None = None, filled: int | None = None, tif: str = "") -> bool:
        if self._is_terminal_order_status(status):
            return True
        normalized = status.strip().lower()
        if remaining != 0:
            return False
        if normalized in {"pendingcancel", "cancelsubmitted"}:
            return True
        # Submission lifecycle statuses are not terminal. IBKR can still fill IOC/limit orders after an initial
        # PendingSubmit/ApiPending response where ib_async reports remaining=0, so only explicit terminal or
        # cancellation lifecycle statuses should close gateway/local order state.
        return False

    def _is_cancel_or_noop_terminal_order_status(self, status: str, remaining: int | None = None, filled: int | None = None, tif: str = "") -> bool:
        normalized = status.strip().lower()
        if normalized in {"cancelled", "apicancelled", "inactive", "pendingcancel", "cancelsubmitted"}:
            return True
        return self._is_effectively_terminal_order_status(status, remaining, filled, tif) and (filled or 0) == 0

    def _find_active_closing_order(self, state: SymbolExecutionState, action: str) -> RoutedOrder | None:
        normalized_action = action.strip().upper()
        for gateway_order_id in sorted(state.open_gateway_order_ids):
            routed = self._orders_by_gateway_id.get(gateway_order_id)
            if routed is None:
                continue
            if routed.action != normalized_action:
                continue
            if self._is_effectively_terminal_order_status(routed.status, routed.remaining_quantity, routed.filled_quantity, routed.tif):
                continue
            if self._is_closing_trade_metadata(routed.metadata):
                return routed
        return None

    def _normalized_order_type_for_action(self, action: str, is_closing_trade: bool, requested_order_type: str = "") -> str:
        requested = (requested_order_type or "").strip().upper()
        if is_closing_trade and requested in {"LMT", "FAST_LMT"}:
            return requested
        return "MKT" if is_closing_trade else "FAST_LMT"

    def _normalized_limit_price_for_action(self, action: str, is_closing_trade: bool, limit_price: float | None, reference_price: float) -> float | None:
        return self._normalized_limit_price_for_order_type(self._normalized_order_type_for_action(action, is_closing_trade), limit_price, reference_price)

    def _normalized_limit_price_for_order_type(self, order_type: str, limit_price: float | None, reference_price: float) -> float | None:
        if order_type.strip().upper() == "MKT":
            return None
        if limit_price is not None and float(limit_price) > 0.0:
            return float(limit_price)
        return reference_price if reference_price > 0.0 else None

    def _is_closing_trade_command(self, command: OrderSubmitCommand) -> bool:
        return self._is_closing_trade_metadata(command.metadata or {})

    def _is_closing_trade_metadata(self, metadata: dict[str, Any]) -> bool:
        raw_flag = metadata.get("closing_trade")
        if isinstance(raw_flag, bool):
            return raw_flag
        if isinstance(raw_flag, str):
            return raw_flag.strip().lower() in {"1", "true", "yes", "on"}
        if isinstance(raw_flag, (int, float)):
            return bool(raw_flag)
        return False

    def _coerce_int(self, value: Any) -> int:
        coerced = self._coerce_optional_int(value)
        return 0 if coerced is None else coerced

    def _coerce_float(self, value: Any) -> float:
        coerced = self._coerce_optional_float(value)
        return 0.0 if coerced is None else coerced

    def _coerce_optional_int(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if hasattr(value, "value") and callable(getattr(value, "value")):
            try:
                nested = value.value()
                return None if nested is None else int(nested)
            except Exception:
                return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _coerce_optional_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if hasattr(value, "value") and callable(getattr(value, "value")):
            try:
                nested = value.value()
                return None if nested is None else float(nested)
            except Exception:
                return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _build_live_order(self, command: OrderSubmitCommand, routed: RoutedOrder, MarketOrder: Any, LimitOrder: Any) -> Any:  # pragma: no cover
        order_type = routed.order_type.upper()
        if order_type == "MKT":
            order = MarketOrder(routed.action, routed.quantity)
            setattr(order, "outsideRth", bool(command.outside_rth))
            return order
        price = routed.limit_price
        if price is None:
            price = routed.reference_price
        if price <= 0.0:
            raise ValueError(f"limit-style order requires positive price symbol={routed.symbol} order_type={order_type}")
        if order_type == "FAST_LMT":
            offset = 0.05
            if routed.action == "BUY":
                price += offset
            else:
                price -= offset
        order = LimitOrder(routed.action, routed.quantity, round(price, 2), tif=routed.tif)
        setattr(order, "outsideRth", bool(command.outside_rth))
        return order

