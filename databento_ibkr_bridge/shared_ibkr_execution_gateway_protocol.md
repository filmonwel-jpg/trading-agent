# Shared IBKR Gateway JSON Protocol

Transport: **newline-delimited JSON over a persistent TCP socket**.

Default listen address:

- host: `127.0.0.1`
- port: `9910`

Each Java bot keeps one local socket connection to the gateway and sends command messages. The gateway replies with one synchronous `ack` per command and may also stream later `event` messages for asynchronous execution updates.

---

## Envelope types

### Command

```json
{
  "type": "command",
  "requestId": "req-123",
  "command": "submit_order",
  "symbol": "AAPL",
  "payload": {
    "action": "BUY",
    "quantity": 10,
    "orderType": "MKT",
    "referencePrice": 187.21,
    "limitPrice": null,
    "tif": "IOC",
    "outsideRth": false,
    "reason": "strategy-entry"
  }
}
```

### Ack

```json
{
  "type": "ack",
  "requestId": "req-123",
  "ok": true,
  "detail": "order-submitted",
  "payload": {
    "gatewayOrderId": 21000001,
    "ibOrderId": null,
    "permId": null,
    "status": "submitted",
    "remaining": null,
    "filled": 0,
    "avg_fill_price": null,
    "effective_terminal": false
  }
}
```

### Event

```json
{
  "type": "event",
  "eventType": "order_status",
  "symbol": "AAPL",
  "gatewayOrderId": 21000001,
  "ibOrderId": 501,
  "permId": 70000001,
  "detail": "Filled",
  "position": null,
  "avgCost": null,
  "payload": {
    "status": "Filled",
    "remaining": 0,
    "avg_fill_price": 187.24
  }
}
```

---

## Supported commands in the first patch

### `ping`
Health/handshake.

### `register_symbol`
Registers a symbol session with the shared gateway.

Payload:

```json
{
  "strategyId": "AAPL:9082"
}
```

### `request_position_sync`
Requests a shared account position refresh.

Payload:

```json
{
  "reason": "startup"
}
```

### `request_open_orders_sync`
Requests a shared open-orders refresh.

Payload:

```json
{
  "reason": "reconnect"
}
```

### `submit_order`
Routes an order through the shared gateway.

Payload:

```json
{
  "action": "BUY",
  "quantity": 10,
  "orderType": "MKT",
  "referencePrice": 187.21,
  "limitPrice": null,
  "tif": "IOC",
  "outsideRth": false,
  "allowAfterHours": false,
  "reason": "strategy-entry",
  "metadata": {
    "source": "IBKRTrader"
  }
}
```

For after-hours flattening, submit `orderType=LMT`, `outsideRth=true` / `allowAfterHours=true`, and an explicit `limitPrice`.

### `quote_snapshot`
Requests a current quote snapshot from the shared IBKR connection for limit-order pricing.

Ack payload:

```json
{
  "symbol": "AAPL",
  "bid": 187.12,
  "ask": 187.18,
  "last": 187.15,
  "market_price": 187.15
}
```

### `cancel_symbol_orders`
Cancels only the orders currently owned by the symbol session.

Payload:

```json
{
  "reason": "kill-switch"
}
```

### `flatten_symbol`
Requests a symbol flatten order.

Payload:

```json
{
  "referencePrice": 187.21,
  "quantityOverride": null,
  "reason": "eod-flatten"
}
```

### `snapshot`
Returns the gateway state snapshot.

---

## Event routing contract

The gateway must eventually route these async events back to bots:

- `position_updated`
- `order_submitted`
- `order_status`
- `order_cancelled`
- `error`
- `connected`
- `disconnected`

Routing keys:

- `symbol`
- `gatewayOrderId`
- `ibOrderId`
- `permId`
- `reqId`

---

## Current migration status

This protocol has moved beyond the original first-patch scaffold. The current implementation supports the shared
gateway as a live execution path for Java symbol bots, including command acknowledgements and asynchronous event
delivery back into Java strategy state.

Implemented now:

- Java bots can send gateway commands and receive matching `ack` responses plus asynchronous `event` messages.
- The Python bridge hosts the shared gateway socket server and broadcasts gateway events to connected Java bots.
- The Java-side client is wired into outbound order/cancel/flatten/position/open-order replacement paths when
  `trading.ibkr.shared-gateway.enabled=true`.
- The Python gateway submits live orders through one shared `ib_async.IB()` connection and tracks
  `gatewayOrderId -> ibOrderId/permId -> symbol` for orders it routed.
- Live order-status fanout exists for gateway-routed orders via `ib_async` trade events and open-trade polling.
- Position fanout exists through explicit position sync and background position polling.
- Java `IBKRTrader` reconciles shared-gateway `position_updated`, `position_sync_completed`, `order_status`, and
  `order_cancelled` events into `PingPongStrategy`, local order state, and shared-capital state.
- Bulk Databento launch defaults to shared-gateway-only mode by passing
  `trading.ibkr.shared-gateway.skip-direct-connection=true`, so the symbol bots do not each consume a direct IBKR
  API client slot.

Still partial / not fully cut over:

1. Callback demultiplexing is complete for the main gateway-routed order-status and position paths, but not for every
   IBKR callback surface. `openOrder`, `execDetails`, `commissionReport`, account summary, portfolio/account-value,
   and finely-routed broker error fanout are still future hardening items.
2. Open-order reconciliation currently works best for orders created through the shared gateway. Broker-existing or
   manual open orders that cannot be mapped to a known `gatewayOrderId` are not fully imported into Java strategy
   state.
3. Gateway events are broadcast to connected clients and filtered by symbol on the Java side. Server-side per-session
   event subscription/filtering is not yet implemented.
4. Direct per-bot `EClientSocket` ownership is still present in `IBKRTrader` and remains the default in base
   `application.properties`; it is bypassed only when shared-gateway skip-direct mode is enabled by launcher/runtime
   args.

Practical conclusion: the system is no longer merely in the first patch limitation state, but it is still in a
**partial migration / shared-gateway hardening** phase rather than a complete direct-IBKR removal phase.

