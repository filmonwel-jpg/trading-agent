from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from databento_ibkr_bridge.backfill_consumer import run_backfill_consumer
from databento_ibkr_bridge.config import BridgeConfig, load_config
from databento_ibkr_bridge.shared_ibkr_execution_gateway import SharedIBKRExecutionGateway
from databento_ibkr_bridge.shared_ibkr_execution_gateway_server import SharedIbkrGatewayProtocolServer


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


def _ensure_paths(cfg: BridgeConfig) -> None:
    cfg.output_root.mkdir(parents=True, exist_ok=True)
    cfg.log_root.mkdir(parents=True, exist_ok=True)
    cfg.state_root.mkdir(parents=True, exist_ok=True)


def _print_plan(cfg: BridgeConfig) -> None:
    print("[BRIDGE] Databento -> IBKR scaffold")
    print(f"[BRIDGE] mode={cfg.bridge_mode}")
    print(f"[BRIDGE] symbols_file={cfg.symbols_file}")
    print(f"[BRIDGE] symbols_loaded={len(cfg.symbols)}")
    print(f"[BRIDGE] sample_symbols={','.join(cfg.symbols[:10])}")
    print(f"[BRIDGE] databento_dataset={cfg.databento_dataset} schema={cfg.databento_schema}")
    print(f"[BRIDGE] ibkr={cfg.ibkr_host}:{cfg.ibkr_port} client_id_base={cfg.ibkr_client_id_base}")
    print(f"[BRIDGE] order_notional_usd={cfg.order_notional_usd}")
    print(f"[BRIDGE] output_root={cfg.output_root}")
    print(f"[BRIDGE] log_root={cfg.log_root}")
    print(f"[BRIDGE] state_root={cfg.state_root}")
    if cfg.bridge_mode == "consume_backfill":
        run_id_display = cfg.backfill_run_id or "<latest resumable>"
        print(f"[BRIDGE] backfill_run_id={run_id_display}")
        print(f"[BRIDGE] max_orders_per_cycle={cfg.max_orders_per_cycle}")
        print(f"[BRIDGE] min_imbalance_ratio={cfg.min_imbalance_ratio}")
        print(f"[BRIDGE] max_quote_age_minutes={cfg.max_quote_age_minutes}")


def _run_dry(cfg: BridgeConfig) -> None:
    _print_plan(cfg)
    print("[BRIDGE] DRY RUN complete. No Databento connection and no IBKR order placement were attempted.")


def _run_live_placeholder(cfg: BridgeConfig) -> None:
    _print_plan(cfg)
    print("[BRIDGE] LIVE MODE PLACEHOLDER")
    print("[BRIDGE] Next implementation step:")
    print("  1) Databento live subscription initialization")
    print("  2) Normalized quote/trade event pipeline")
    print("  3) Signal engine")
    print("  4) IBKR order adapter + risk checks + idempotency")
    print("[BRIDGE] This scaffold intentionally stops before sending orders.")


def _run_consume_backfill(cfg: BridgeConfig) -> int:
    _print_plan(cfg)
    return run_backfill_consumer(cfg)


def _run_shared_ibkr_gateway_scaffold(cfg: BridgeConfig, forced_dry: bool) -> int:
    _configure_logging()
    gateway = SharedIBKRExecutionGateway.from_config(cfg, dry_run=forced_dry)
    gateway.connect()
    try:
        gateway.register_symbols(cfg.symbols)
        gateway.request_position_sync()
        gateway.request_open_orders_sync()
        snapshot = gateway.snapshot()
        print("[BRIDGE] SHARED IBKR EXECUTION GATEWAY SCAFFOLD")
        print(f"[BRIDGE] dry_run={snapshot.dry_run} connected={snapshot.connected}")
        print(f"[BRIDGE] ibkr={snapshot.host}:{snapshot.port} client_id={snapshot.client_id}")
        print(f"[BRIDGE] registered_symbols={len(snapshot.registered_symbols)}")
        for symbol in snapshot.registered_symbols[:10]:
            request_ids = snapshot.symbols[symbol]["request_ids"]
            print(f"[BRIDGE] symbol={symbol} request_ids={request_ids}")
        print("[BRIDGE] This is the first scaffold patch: command routing, ID allocation, and state maps are live;")
        print("[BRIDGE] Java bot migration and callback demultiplexing to symbol sessions are the next step.")
        return 0
    finally:
        gateway.disconnect()


def _run_shared_ibkr_gateway_server(cfg: BridgeConfig, forced_dry: bool) -> int:
    _configure_logging()
    print("[BRIDGE] SHARED IBKR GATEWAY SERVER")
    print(f"[BRIDGE] dry_run={forced_dry} listen={cfg.shared_ibkr_gateway_host}:{cfg.shared_ibkr_gateway_port}")
    server = SharedIbkrGatewayProtocolServer(cfg, dry_run=forced_dry)
    server.serve_forever()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Databento to IBKR bridge scaffold runner")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run even if DRY_RUN=false in env")
    parser.add_argument(
        "--consume-backfill",
        action="store_true",
        help="Consume databento_backfill_runs/progress tables and route intents to IBKR",
    )
    parser.add_argument(
        "--shared-ibkr-gateway",
        action="store_true",
        help="Run the shared IBKR execution gateway scaffold and print the routed symbol/request state",
    )
    parser.add_argument(
        "--shared-ibkr-gateway-server",
        action="store_true",
        help="Run the shared IBKR gateway JSON socket server for Java bot clients",
    )
    parser.add_argument("--run-id", default="", help="Backfill run_id to consume (defaults to latest)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    cfg = load_config(project_root=project_root)
    if args.consume_backfill:
        from dataclasses import replace

        cfg = replace(
            cfg,
            bridge_mode="consume_backfill",
            backfill_run_id=(args.run_id.strip() or cfg.backfill_run_id),
        )
    elif cfg.bridge_mode == "consume_backfill" and args.run_id.strip():
        from dataclasses import replace

        cfg = replace(cfg, backfill_run_id=args.run_id.strip())

    _ensure_paths(cfg)

    forced_dry = args.dry_run or cfg.dry_run
    if args.shared_ibkr_gateway_server or cfg.bridge_mode == "shared_ibkr_gateway_server":
        return _run_shared_ibkr_gateway_server(cfg, forced_dry=forced_dry)

    if args.shared_ibkr_gateway or cfg.bridge_mode == "shared_ibkr_gateway":
        return _run_shared_ibkr_gateway_scaffold(cfg, forced_dry=forced_dry)

    if cfg.bridge_mode == "consume_backfill":
        return _run_consume_backfill(cfg)

    if forced_dry:
        _run_dry(cfg)
        return 0

    _run_live_placeholder(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

