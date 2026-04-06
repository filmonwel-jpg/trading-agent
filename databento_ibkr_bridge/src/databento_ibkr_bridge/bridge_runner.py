from __future__ import annotations

import argparse
from pathlib import Path

from databento_ibkr_bridge.config import BridgeConfig, load_config


def _ensure_paths(cfg: BridgeConfig) -> None:
    cfg.output_root.mkdir(parents=True, exist_ok=True)
    cfg.log_root.mkdir(parents=True, exist_ok=True)
    cfg.state_root.mkdir(parents=True, exist_ok=True)


def _print_plan(cfg: BridgeConfig) -> None:
    print("[BRIDGE] Databento -> IBKR scaffold")
    print(f"[BRIDGE] symbols_file={cfg.symbols_file}")
    print(f"[BRIDGE] symbols_loaded={len(cfg.symbols)}")
    print(f"[BRIDGE] sample_symbols={','.join(cfg.symbols[:10])}")
    print(f"[BRIDGE] databento_dataset={cfg.databento_dataset} schema={cfg.databento_schema}")
    print(f"[BRIDGE] ibkr={cfg.ibkr_host}:{cfg.ibkr_port} client_id_base={cfg.ibkr_client_id_base}")
    print(f"[BRIDGE] order_notional_usd={cfg.order_notional_usd}")
    print(f"[BRIDGE] output_root={cfg.output_root}")
    print(f"[BRIDGE] log_root={cfg.log_root}")
    print(f"[BRIDGE] state_root={cfg.state_root}")


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Databento to IBKR bridge scaffold runner")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run even if DRY_RUN=false in env")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    cfg = load_config(project_root=project_root)

    _ensure_paths(cfg)

    forced_dry = args.dry_run or cfg.dry_run
    if forced_dry:
        _run_dry(cfg)
        return 0

    _run_live_placeholder(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

