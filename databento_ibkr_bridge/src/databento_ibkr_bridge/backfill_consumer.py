from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from databento_ibkr_bridge.config import BridgeConfig
from databento_ibkr_bridge.shared_ibkr_execution_gateway import SharedIBKRExecutionGateway
from databento_ibkr_bridge.shared_ibkr_execution_models import OrderSubmitCommand


@dataclass(frozen=True)
class SymbolSnapshot:
    symbol: str
    ts_event: datetime
    bid_px: float | None
    ask_px: float | None
    bid_sz: float | None
    ask_sz: float | None
    trade_px: float | None


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    action: str
    quantity: int
    reference_price: float
    reason: str


def _connect_db(cfg: BridgeConfig):
    if not cfg.harvest_db_url:
        raise RuntimeError("HARVEST_DB_URL or spring.datasource.url is required for consume_backfill mode.")

    try:
        import psycopg  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError('Install psycopg first: pip install "psycopg[binary]"') from exc

    return psycopg.connect(
        cfg.harvest_db_url,
        user=cfg.harvest_db_user or None,
        password=cfg.harvest_db_password or None,
        autocommit=True,
        application_name="databento_ibkr_backfill_consumer",
    )


def _latest_run_id(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM databento_backfill_runs
            WHERE status IN ('running', 'paused', 'completed')
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("No rows found in databento_backfill_runs.")
    return str(row[0])


def _completed_symbols(conn, run_id: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol
            FROM databento_backfill_progress
            WHERE run_id = %s
            GROUP BY symbol
            HAVING COUNT(*) > 0 AND BOOL_AND(status = 'completed')
            ORDER BY symbol
            """,
            (run_id,),
        )
        rows = cur.fetchall()
    return [str(row[0]) for row in rows]


def _load_latest_snapshot(conn, symbol: str, max_age_minutes: int) -> SymbolSnapshot | None:
    min_ts = datetime.now(UTC) - timedelta(minutes=max_age_minutes)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, ts_event, bid_px, ask_px, bid_sz, ask_sz, trade_px
            FROM databento_tbbo
            WHERE symbol = %s
              AND ts_event >= %s
            ORDER BY ts_event DESC
            LIMIT 1
            """,
            (symbol, min_ts),
        )
        row = cur.fetchone()

    if not row:
        return None

    return SymbolSnapshot(
        symbol=str(row[0]),
        ts_event=row[1],
        bid_px=float(row[2]) if row[2] is not None else None,
        ask_px=float(row[3]) if row[3] is not None else None,
        bid_sz=float(row[4]) if row[4] is not None else None,
        ask_sz=float(row[5]) if row[5] is not None else None,
        trade_px=float(row[6]) if row[6] is not None else None,
    )


def _build_intent(snapshot: SymbolSnapshot, cfg: BridgeConfig) -> OrderIntent | None:
    if snapshot.bid_px is None or snapshot.ask_px is None or snapshot.bid_px <= 0 or snapshot.ask_px <= 0:
        return None

    bid_sz = snapshot.bid_sz or 0.0
    ask_sz = snapshot.ask_sz or 0.0
    size_total = bid_sz + ask_sz
    if size_total <= 0:
        return None

    imbalance = (bid_sz - ask_sz) / size_total
    if imbalance >= cfg.min_imbalance_ratio:
        action = "BUY"
        reason = f"imbalance={imbalance:.3f}"
    elif imbalance <= -cfg.min_imbalance_ratio:
        action = "SELL"
        reason = f"imbalance={imbalance:.3f}"
    else:
        return None

    reference_price = (snapshot.bid_px + snapshot.ask_px) / 2.0
    quantity = max(1, int(cfg.order_notional_usd / reference_price))
    return OrderIntent(
        symbol=snapshot.symbol,
        action=action,
        quantity=quantity,
        reference_price=reference_price,
        reason=reason,
    )


def _route_intents(cfg: BridgeConfig, intents: list[OrderIntent]) -> list[dict[str, Any]]:
    gateway = SharedIBKRExecutionGateway.from_config(cfg)
    routed: list[dict[str, Any]] = []
    try:
        gateway.connect()
        gateway.register_symbols(sorted({intent.symbol for intent in intents}))
        for intent in intents:
            routed_order = gateway.submit_order(
                OrderSubmitCommand(
                    symbol=intent.symbol,
                    action=intent.action,
                    quantity=intent.quantity,
                    order_type="MKT",
                    reference_price=intent.reference_price,
                    reason=intent.reason,
                    metadata={"source": "consume_backfill"},
                )
            )
            routed.append(
                {
                    "symbol": intent.symbol,
                    "action": intent.action,
                    "quantity": intent.quantity,
                    "price": intent.reference_price,
                    "status": "dry_run" if cfg.dry_run else "submitted",
                    "detail": (
                        f"gateway_order_id={routed_order.gateway_order_id} "
                        f"ib_order_id={routed_order.ib_order_id} reason={intent.reason}"
                    ),
                }
            )
    finally:
        gateway.disconnect()

    return routed


def run_backfill_consumer(cfg: BridgeConfig) -> int:
    conn = _connect_db(cfg)
    try:
        run_id = cfg.backfill_run_id or _latest_run_id(conn)
        symbols = _completed_symbols(conn, run_id=run_id)
        print(f"[CONSUMER] run_id={run_id} completed_symbols={len(symbols)}")
        if not symbols:
            print("[CONSUMER] No fully-completed symbols found yet; nothing to route.")
            return 0

        intents: list[OrderIntent] = []
        for symbol in symbols:
            snapshot = _load_latest_snapshot(conn, symbol, cfg.max_quote_age_minutes)
            if snapshot is None:
                continue
            intent = _build_intent(snapshot, cfg)
            if intent is not None:
                intents.append(intent)
            if len(intents) >= cfg.max_orders_per_cycle:
                break

        if not intents:
            print("[CONSUMER] No trade intents generated from latest snapshots.")
            return 0

        routed = _route_intents(cfg, intents)
        for row in routed:
            print(
                f"[CONSUMER] {row['status']} symbol={row['symbol']} action={row['action']} "
                f"qty={row['quantity']} px={row['price']:.4f} detail={row['detail']}"
            )
        return 0
    finally:
        conn.close()


