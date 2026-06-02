# Shared IBKR Execution Gateway Audit

## Goal

Replace the current **one-IBKR-client-per-symbol-process** model with a **single shared IBKR execution gateway** that owns the socket once and routes commands + callbacks across many symbols.

## What the current Java bot does today

The existing `IBKRTrader` is tightly single-symbol and per-process:

- connects once with one `clientId`
- owns one `symbol`
- owns one `PingPongStrategy`
- owns one `currentOrderId`
- owns one `marketDataRequestId`
- handles callbacks directly into singleton fields

That means the following state is **not share-safe today** without explicit routing:

- `symbol`
- `shopStrategy`
- `currentOrderId`
- `marketDataRequestId`
- `orderContextById`
- `orderSentTimes`
- `cumulativeFilledByOrderId`
- `positionSyncComplete`
- `symbolPositionSeenThisCycle`
- `latestShortableShares`
- `latestPutVolume` / `latestCallVolume`
- reconnect state (`isReconnecting`, `reconnectionAttempts`)

## Commands that must be routed through a shared gateway

These are the command surfaces a shared gateway must accept from many symbol sessions:

| Command | Current source | Routing key | Notes |
|---|---|---|---|
| `connect` | app startup | global | one gateway-wide socket |
| `disconnect` | shutdown | global | one gateway-wide socket |
| `register symbol session` | `onConnected()` | symbol | create per-symbol state + request-id bundle |
| `submit order` | `placeTrade(...)` | symbol + gateway order id | must map `gatewayOrderId -> ibOrderId/permId -> symbol` |
| `cancel symbol orders` | `cancelOpenOrders()` | symbol + gateway order ids | today uses global cancel; shared gateway should narrow this to symbol-owned orders |
| `flatten symbol` | `flattenPosition()` | symbol | needs current routed position snapshot |
| `sync positions` | `reqPositions()` | global request + symbol callbacks | one request, many symbol callbacks |
| `sync open orders` | `reqOpenOrders()` | global request + symbol/order callbacks | one request, many order callbacks |
| `manual pause/resume` | control API | symbol | strategy control stays symbol-local even if execution is shared |
| `status snapshot` | control API | symbol + global | both symbol-local and gateway-wide health are needed |

## IBKR callbacks/events that must be demultiplexed

These are the minimum inbound surfaces that a shared gateway must route back to the correct symbol session.

| Callback / event | Current usage | Required shared routing |
|---|---|---|
| `nextValidId(orderId)` | seeds `currentOrderId` | global allocator seed for all symbols |
| `position(account, contract, pos, avgCost)` | sync one symbol position | route by `contract.symbol()` |
| `positionEnd()` | marks sync complete | global barrier, then notify all registered symbol sessions |
| `openOrder(orderId, contract, order, orderState)` | currently mostly stubbed | route by `orderId -> symbol` and maintain open-order map |
| `orderStatus(...)` | close order + capital release | route by `orderId/permId -> symbol`, update partial fills + terminal states |
| `execDetails(reqId, contract, execution)` | currently stubbed in trader | route by `permId/orderId -> symbol`, publish fill events |
| `commissionReport(...)` | not yet routed | attach to execution/order lifecycle |
| `error(id, code, message, explain)` | logs globally | route using `id` when it matches req/order ids; otherwise keep global |
| `connectionClosed()` | triggers reconnect | global broadcast to all sessions |
| `managedAccounts()` | not used today | useful for shared account-aware risk |
| `accountSummary/accountValue/updatePortfolio` | not used today | future shared risk and status layer |

## State that the shared gateway must own centrally

The first shared gateway must own these registries:

- one global IBKR socket / client lifecycle
- one global order-id allocator
- one global request-id allocator
- `gatewayOrderId -> RoutedOrder`
- `ibOrderId -> gatewayOrderId`
- `permId -> gatewayOrderId`
- `symbol -> SymbolExecutionState`
- `symbol -> open gateway order ids`
- `symbol -> last position / avg cost`
- recent gateway events for health + audit

## Current implementation status in this repo

The original scaffold has been extended into a live shared-gateway path. The core Python contract lives under
`databento_ibkr_bridge`:

- `shared_ibkr_execution_models.py`
- `shared_ibkr_execution_gateway.py`

What is implemented now:

- one shared gateway object
- one shared connect/disconnect lifecycle
- symbol registration
- deterministic request-id bundles per symbol
- gateway-order-id allocation
- dry-run order routing
- live batch submission through one `ib_async.IB()` client
- live order-status fanout for gateway-routed orders through `ib_async` trade events and open-trade polling
- live position fanout through explicit sync and background polling
- Java `SharedIbkrGatewayClient` integration for order submit, cancel, flatten, position sync, and open-order sync
- Java `IBKRTrader` reconciliation of `position_updated`, `position_sync_completed`, `order_status`, and
  `order_cancelled` into strategy/order/shared-capital state
- bulk launcher shared-gateway-only mode via `trading.ibkr.shared-gateway.skip-direct-connection=true`
- order / position / error event recording
- snapshot export for health/debugging
- bridge runner mode to inspect the gateway scaffold
- backfill consumer routed through the gateway abstraction

What is still **not** fully migrated yet:

- live `orderStatus` for gateway-routed orders is wired, but `openOrder`, `execDetails`, `commissionReport`,
  account summary, portfolio/account-value, and finely-routed error callbacks still need fuller demultiplexing
- broker-existing/manual open orders that were not created by the gateway are not fully imported into Java strategy state
- server-side per-session event filtering is not implemented yet; the server broadcasts gateway events and Java filters by symbol
- direct per-bot `EClientSocket` ownership still exists for fallback/default single-bot operation
- account summary and portfolio routing are still future work

## Migration path from current Java bots

1. Keep strategy logic per symbol.
2. Move raw `EClientSocket` ownership out of `IBKRTrader` into one gateway process/service.
3. Replace direct `placeTrade`, `cancelOpenOrders`, `flattenPosition`, `reqPositions`, `reqOpenOrders` calls with gateway commands.
4. Route callbacks back into symbol sessions keyed by:
   - `symbol`
   - `gatewayOrderId`
   - `ibOrderId`
   - `permId`
   - request ids
5. Once routing is stable, remove one-client-per-symbol launch assumptions.

## Why this is the right first patch

It gives the repo a real shared execution contract now, without risking the live Java fleet in one step.

This patch is enough to:

- prove the state model
- prove routing IDs are unique and symbol-scoped
- reuse one IBKR connection for multi-symbol order submission in the bridge path
- define the callback demux contract needed for full Java migration

