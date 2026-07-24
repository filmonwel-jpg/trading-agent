#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime"
DATABENTO_RUNTIME_DIR = RUNTIME_DIR / "databento"
BOTS_DIR = DATABENTO_RUNTIME_DIR / "bots"
ROUTING_CSV = DATABENTO_RUNTIME_DIR / "model-routing.csv"
PLAN_CSV = REPO_ROOT / "training_data" / "databento_30s" / "symbol_model_plan.csv"
FINAL_REPORT_JSON = REPO_ROOT / "training_data" / "compare_runs_20260407_meta_ab" / "final_comparison_report.json"
THRESHOLD_SUMMARY_CSV = DATABENTO_RUNTIME_DIR / "bot-thresholds.csv"
SYMBOLS_FILE = RUNTIME_DIR / "symbols_100.txt"
LEGACY_RUNTIME_GLOB = "trading-*.properties"
TEMPLATE_PATH = BOTS_DIR / "trading-databento-template.properties"

BASE_CLIENT_ID = 210
BASE_SERVER_PORT = 9081
BASE_MARKET_DATA_REQUEST_ID = 2101
SHARED_FEED_SHARD_COUNT = 5
SHARED_FEED_BASE_PORT = 9800
SHARED_FEED_CLIENT_WAIT_TIMEOUT_MS = 60000
DATABENTO_STARTUP_DELAY_SECONDS = 40
DATABENTO_MAX_SILENCE_MS = 120000

OVERRIDE_KEYS = [
    "trading.trade-amount",
    "trading.risk.max-order-notional",
    "trading.risk.max-share-cap",
    "trading.stop-loss-percentage",
    "trading.ai.long-entry-threshold",
    "trading.ai.short-entry-threshold",
    "trading.ai.long-exit-threshold",
    "trading.ai.short-exit-threshold",
    "trading.ai.regime-threshold",
    "trading.log.storage-mode",
]

DEFAULT_TEMPLATE = {
    "trading.symbol": "SPY",
    "trading.client-id": str(BASE_CLIENT_ID),
    "server.port": str(BASE_SERVER_PORT),
    "trading.market-data.provider": "databento",
    "trading.market-data-request-id": str(BASE_MARKET_DATA_REQUEST_ID),
    "trading.databento.python-bin": "python3",
    "trading.databento.startup-delay-seconds": str(DATABENTO_STARTUP_DELAY_SECONDS),
    "trading.databento.startup-history-seconds": "360",
    "trading.databento.shared-feed.startup-history-seconds": "0",
    "trading.databento.startup-history-schema": "ohlcv-1s",
    "trading.databento.max-silence-ms": str(DATABENTO_MAX_SILENCE_MS),
    "trading.databento.shared-feed.enabled": "true",
    "trading.databento.shared-feed.host": "127.0.0.1",
    "trading.databento.shared-feed.port": "9800",
    "trading.databento.shared-feed.shard-id": "0",
    "trading.databento.shared-feed.expected-client-count": "1",
    "trading.databento.shared-feed.client-wait-timeout-ms": str(SHARED_FEED_CLIENT_WAIT_TIMEOUT_MS),
    "trading.databento.shared-feed.bots-dir": "runtime/databento/bots",
    "trading.databento.shared-feed.lock-file": "runtime/databento/shared-feed-relay-shard0.lock",
    "trading.databento.shared-feed.pid-file": "runtime/databento/shared-feed-relay-shard0.pid",
    "trading.databento.shared-feed.log-file": "runtime/databento/logs/databento-shared-feed-relay-shard0.log",
    "trading.trade-amount": "500000",
    "trading.risk.max-order-notional": "500000",
    "trading.risk.max-share-cap": "2000",
    "trading.ai.long-entry-threshold": "0.68",
    "trading.ai.short-entry-threshold": "0.63",
    "trading.ai.long-exit-threshold": "0.60",
    "trading.ai.short-exit-threshold": "0.60",
    "trading.ai.regime-threshold": "0.50",
    "trading.ai.entry-threshold-raise-percent": "10.0",
    "trading.ai.open30.long-entry-threshold": "0.68",
    "trading.ai.open30.short-entry-threshold": "0.63",
    "trading.ai.open30.long-exit-threshold": "0.60",
    "trading.ai.open30.short-exit-threshold": "0.60",
    "trading.ai.regime.choppy.long-entry-threshold": "0.68",
    "trading.ai.regime.choppy.short-entry-threshold": "0.63",
    "trading.ai.regime.choppy.long-exit-threshold": "0.60",
    "trading.ai.regime.choppy.short-exit-threshold": "0.60",
    "trading.ai.regime.trend.long-entry-threshold": "0.68",
    "trading.ai.regime.trend.short-entry-threshold": "0.63",
    "trading.ai.regime.trend.long-exit-threshold": "0.60",
    "trading.ai.regime.trend.short-exit-threshold": "0.60",
    "trading.ai.regime.volatile.long-entry-threshold": "0.68",
    "trading.ai.regime.volatile.short-entry-threshold": "0.63",
    "trading.ai.regime.volatile.long-exit-threshold": "0.60",
    "trading.ai.regime.volatile.short-exit-threshold": "0.60",
    "trading.shared-capital.enabled": "true",
    "trading.shared-capital.file": "runtime/databento/shared-capital.properties",
    "trading.shared-capital.total-notional": "500000",
    "trading.databento.option-parents": "SPY.OPT",
    "trading.databento.model-routing-csv": "runtime/databento/model-routing.csv",
    "trading.databento.symbol-plan-csv": "training_data/databento_30s/symbol_model_plan.csv",
    "trading.model.dir": "runtime/models/SPY",
    "trading.state.file": "runtime/databento/state/trader-state-SPY.properties",
    "trading.log.file": "runtime/databento/output/trades-SPY.csv",
    "trading.log.storage-mode": "both",
    "logging.file.name": "runtime/databento/logs/trading-agent-SPY.log",
    "trading.databento.live-gateway": "",
}

MODEL_TO_PROPERTY = {
    "LONG ENTRY (Dip Buyer)": "trading.ai.long-entry-threshold",
    "SHORT ENTRY (Rip Seller)": "trading.ai.short-entry-threshold",
    "LONG EXIT (Top Detector)": "trading.ai.long-exit-threshold",
    "SHORT EXIT (Bottom Detector)": "trading.ai.short-exit-threshold",
}

OPEN_MODEL_TO_PROPERTY = {
    "LONG ENTRY (Dip Buyer)": "trading.ai.open30.long-entry-threshold",
    "SHORT ENTRY (Rip Seller)": "trading.ai.open30.short-entry-threshold",
    "LONG EXIT (Top Detector)": "trading.ai.open30.long-exit-threshold",
    "SHORT EXIT (Bottom Detector)": "trading.ai.open30.short-exit-threshold",
}

REGIME_MODEL_TO_PROPERTY = {
    ("choppy", "LONG ENTRY (Dip Buyer)"): "trading.ai.regime.choppy.long-entry-threshold",
    ("choppy", "SHORT ENTRY (Rip Seller)"): "trading.ai.regime.choppy.short-entry-threshold",
    ("choppy", "LONG EXIT (Top Detector)"): "trading.ai.regime.choppy.long-exit-threshold",
    ("choppy", "SHORT EXIT (Bottom Detector)"): "trading.ai.regime.choppy.short-exit-threshold",
    ("trend", "LONG ENTRY (Dip Buyer)"): "trading.ai.regime.trend.long-entry-threshold",
    ("trend", "SHORT ENTRY (Rip Seller)"): "trading.ai.regime.trend.short-entry-threshold",
    ("trend", "LONG EXIT (Top Detector)"): "trading.ai.regime.trend.long-exit-threshold",
    ("trend", "SHORT EXIT (Bottom Detector)"): "trading.ai.regime.trend.short-exit-threshold",
    ("volatile", "LONG ENTRY (Dip Buyer)"): "trading.ai.regime.volatile.long-entry-threshold",
    ("volatile", "SHORT ENTRY (Rip Seller)"): "trading.ai.regime.volatile.short-entry-threshold",
    ("volatile", "LONG EXIT (Top Detector)"): "trading.ai.regime.volatile.long-exit-threshold",
    ("volatile", "SHORT EXIT (Bottom Detector)"): "trading.ai.regime.volatile.short-exit-threshold",
}

VARIANT_ALIAS = {
    "baseline": "baseline_no_generated_meta",
    "enhanced": "with_timesfm_and_sequence_proxy",
}

ROUTING_FALLBACKS = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Databento bot property files from routing and compare-run artifacts.")
    parser.add_argument("--routing-csv", type=Path, default=ROUTING_CSV)
    parser.add_argument("--plan-csv", type=Path, default=PLAN_CSV)
    parser.add_argument("--final-report-json", type=Path, default=FINAL_REPORT_JSON)
    parser.add_argument("--bots-dir", type=Path, default=BOTS_DIR)
    parser.add_argument("--threshold-summary-csv", type=Path, default=THRESHOLD_SUMMARY_CSV)
    parser.add_argument("--symbols-file", type=Path, default=SYMBOLS_FILE)
    parser.add_argument("--template-path", type=Path, default=TEMPLATE_PATH)
    parser.add_argument("--model-routing-property", default="runtime/databento/model-routing.csv")
    parser.add_argument("--symbol-plan-property", default="training_data/databento_30s/symbol_model_plan.csv")
    parser.add_argument("--shared-feed-bots-dir-property", default=None, help="Property value to embed for trading.databento.shared-feed.bots-dir. Defaults to the generated bots dir relative to the repo root when possible.")
    parser.add_argument("--restrict-to-requested-symbols", action="store_true", help="Only generate configs for the symbols listed in --symbols-file. Useful for controlled promotions that should not expand the live bot universe.")
    parser.add_argument("--prune-stale", action="store_true", help="Remove generated symbol property files in the target bots dir that are not in the current routing/plan universe.")
    return parser.parse_args()


def property_path_value(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def load_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def write_properties(path: Path, props: dict[str, str], header_lines: list[str]) -> None:
    lines = list(header_lines)
    for key, value in props.items():
        lines.append(f"{key}={value}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_template(template_path: Path) -> dict[str, str]:
    props = load_properties(template_path)
    merged = dict(DEFAULT_TEMPLATE)
    merged.update(props)
    return merged


def load_routing(routing_csv: Path) -> dict[str, dict[str, str]]:
    routing: dict[str, dict[str, str]] = {}
    with routing_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = (row.get("symbol") or "").strip().upper()
            if symbol:
                routing[symbol] = row
    return routing


def load_thresholds_by_variant_and_job(final_report_json: Path) -> dict[tuple[str, str], dict[str, float]]:
    if not final_report_json.exists():
        return {}
    report = json.loads(final_report_json.read_text(encoding="utf-8"))
    out: dict[tuple[str, str], dict[str, float]] = {}
    for variant, payload in report.get("variants", {}).items():
        for row in payload.get("primary_score_rows", []):
            job_log = str(row.get("job_log", "")).strip()
            model = str(row.get("model", "")).strip()
            avg_threshold = row.get("avg_threshold")
            if not job_log or model not in MODEL_TO_PROPERTY or avg_threshold is None:
                continue
            job_name = Path(job_log).stem
            key = (variant, job_name)
            slot = out.setdefault(key, {})
            slot[MODEL_TO_PROPERTY[model]] = float(avg_threshold)
        for row in payload.get("open_score_rows", []):
            job_log = str(row.get("job_log", "")).strip()
            model = str(row.get("model", "")).strip()
            avg_threshold = row.get("avg_threshold")
            if not job_log or model not in OPEN_MODEL_TO_PROPERTY or avg_threshold is None:
                continue
            job_name = Path(job_log).stem
            key = (variant, job_name)
            slot = out.setdefault(key, {})
            slot[OPEN_MODEL_TO_PROPERTY[model]] = float(avg_threshold)
        for row in payload.get("regime_score_rows", []):
            job_log = str(row.get("job_log", "")).strip()
            regime = str(row.get("regime", "")).strip().lower()
            model = str(row.get("model", "")).strip()
            avg_threshold = row.get("avg_threshold")
            property_key = REGIME_MODEL_TO_PROPERTY.get((regime, model))
            if not job_log or property_key is None or avg_threshold is None:
                continue
            job_name = Path(job_log).stem
            key = (variant, job_name)
            slot = out.setdefault(key, {})
            slot[property_key] = float(avg_threshold)
    return out


def load_plan_symbols(plan_csv: Path) -> set[str]:
    symbols: set[str] = set()
    with plan_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = (row.get("Symbol") or "").strip().upper()
            if symbol:
                symbols.add(symbol)
    return symbols


def load_requested_symbols(symbols_file: Path) -> set[str]:
    if not symbols_file.exists():
        return set()
    return {
        line.strip().upper()
        for line in symbols_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def load_legacy_overrides() -> dict[str, dict[str, str]]:
    overrides: dict[str, dict[str, str]] = {}
    for path in sorted(RUNTIME_DIR.glob(LEGACY_RUNTIME_GLOB)):
        if path.name == "trading-databento-template.properties":
            continue
        props = load_properties(path)
        symbol = (props.get("trading.symbol") or path.stem.replace("trading-", "")).strip().upper()
        if symbol:
            overrides[symbol] = props
    return overrides


def load_existing_bot_properties(bots_dir: Path, template_path: Path) -> dict[str, dict[str, str]]:
    overrides: dict[str, dict[str, str]] = {}
    if not bots_dir.exists():
        return overrides
    for path in sorted(bots_dir.glob("trading-*.properties")):
        if path.name == template_path.name:
            continue
        props = load_properties(path)
        symbol = (props.get("trading.symbol") or path.stem.replace("trading-", "")).strip().upper()
        if symbol:
            overrides[symbol] = props
    return overrides


def prune_stale_bot_properties(keep_symbols: set[str], bots_dir: Path, template_path: Path) -> list[Path]:
    removed: list[Path] = []
    if not bots_dir.exists():
        return removed
    keep = {symbol.strip().upper() for symbol in keep_symbols if symbol and symbol.strip()}
    for path in sorted(bots_dir.glob("trading-*.properties")):
        if path.name == template_path.name:
            continue
        props = load_properties(path)
        symbol = (props.get("trading.symbol") or path.stem.replace("trading-", "")).strip().upper()
        if symbol and symbol not in keep:
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def assign_runtime_ids(symbols: list[str], existing_bot_props: dict[str, dict[str, str]]) -> dict[str, tuple[int, int, int]]:
    assigned: dict[str, tuple[int, int, int]] = {}
    used_client_ids: set[int] = set()
    used_server_ports: set[int] = set()
    used_request_ids: set[int] = set()

    for symbol in symbols:
        props = existing_bot_props.get(symbol, {})
        client_id = parse_int(props.get("trading.client-id"))
        server_port = parse_int(props.get("server.port"))
        request_id = parse_int(props.get("trading.market-data-request-id"))
        if client_id is not None and server_port is not None and request_id is not None:
            assigned[symbol] = (client_id, server_port, request_id)
            used_client_ids.add(client_id)
            used_server_ports.add(server_port)
            used_request_ids.add(request_id)

    next_client_id = max([BASE_CLIENT_ID - 1, *used_client_ids]) + 1
    next_server_port = max([BASE_SERVER_PORT - 1, *used_server_ports]) + 1
    next_request_id = max([BASE_MARKET_DATA_REQUEST_ID - 1, *used_request_ids]) + 1

    for symbol in symbols:
        if symbol in assigned:
            continue
        while next_client_id in used_client_ids:
            next_client_id += 1
        while next_server_port in used_server_ports:
            next_server_port += 1
        while next_request_id in used_request_ids:
            next_request_id += 1
        assigned[symbol] = (next_client_id, next_server_port, next_request_id)
        used_client_ids.add(next_client_id)
        used_server_ports.add(next_server_port)
        used_request_ids.add(next_request_id)
        next_client_id += 1
        next_server_port += 1
        next_request_id += 1

    return assigned


def assign_shared_feed_shards(symbols: list[str]) -> tuple[dict[str, int], dict[int, int]]:
    assignments: dict[str, int] = {}
    counts: dict[int, int] = {shard_id: 0 for shard_id in range(SHARED_FEED_SHARD_COUNT)}
    for index, symbol in enumerate(symbols):
        shard_id = index % SHARED_FEED_SHARD_COUNT
        assignments[symbol] = shard_id
        counts[shard_id] += 1
    return assignments, counts


def shared_feed_props_for_shard(shard_id: int, expected_client_count: int, shared_feed_bots_dir_property: str) -> dict[str, str]:
    normalized_shard_id = max(0, int(shard_id))
    shard_suffix = f"shard{normalized_shard_id}"
    return {
        "trading.databento.shared-feed.host": "127.0.0.1",
        "trading.databento.shared-feed.port": str(SHARED_FEED_BASE_PORT + normalized_shard_id),
        "trading.databento.shared-feed.shard-id": str(normalized_shard_id),
        "trading.databento.shared-feed.expected-client-count": str(max(1, int(expected_client_count))),
        "trading.databento.shared-feed.client-wait-timeout-ms": str(SHARED_FEED_CLIENT_WAIT_TIMEOUT_MS),
        "trading.databento.shared-feed.bots-dir": shared_feed_bots_dir_property,
        "trading.databento.shared-feed.lock-file": f"runtime/databento/shared-feed-relay-{shard_suffix}.lock",
        "trading.databento.shared-feed.pid-file": f"runtime/databento/shared-feed-relay-{shard_suffix}.pid",
        "trading.databento.shared-feed.log-file": f"runtime/databento/logs/databento-shared-feed-relay-{shard_suffix}.log",
    }


def resolve_routing_row(symbol: str, routing: dict[str, dict[str, str]]) -> tuple[dict[str, str], str]:
    direct = routing.get(symbol)
    if direct:
        return direct, symbol
    fallback_symbol = ROUTING_FALLBACKS.get(symbol, "")
    fallback = routing.get(fallback_symbol)
    if fallback:
        row = dict(fallback)
        row["symbol"] = symbol
        note = (row.get("note") or "").strip()
        row["note"] = (note + f" fallback_from={fallback_symbol}").strip()
        return row, fallback_symbol
    return {}, ""


def property_path_for_symbol(symbol: str, bots_dir: Path) -> Path:
    return bots_dir / f"trading-{symbol.lower()}.properties"


def symbol_order(symbols: set[str]) -> list[str]:
    rest = sorted(symbol for symbol in symbols if symbol != "SPY")
    return (["SPY"] if "SPY" in symbols else []) + rest


def build_symbol_props(symbol: str,
                       runtime_ids: tuple[int, int, int],
                       shared_feed: dict[str, str],
                       template: dict[str, str],
                       routing_row: dict[str, str] | None,
                       legacy_overrides: dict[str, str] | None,
                       thresholds: dict[str, float] | None,
                       model_routing_property: str,
                       symbol_plan_property: str) -> dict[str, str]:
    props = dict(template)
    client_id, server_port, request_id = runtime_ids
    props["trading.symbol"] = symbol
    props["trading.client-id"] = str(client_id)
    props["server.port"] = str(server_port)
    props["trading.market-data.provider"] = "databento"
    props["trading.market-data-request-id"] = str(request_id)
    props.update(shared_feed)
    props["trading.shared-capital.enabled"] = legacy_overrides.get("trading.shared-capital.enabled", props.get("trading.shared-capital.enabled", "true")) if legacy_overrides else props.get("trading.shared-capital.enabled", "true")
    props["trading.shared-capital.file"] = "runtime/databento/shared-capital.properties"
    props["trading.shared-capital.total-notional"] = legacy_overrides.get("trading.shared-capital.total-notional", props.get("trading.shared-capital.total-notional", "500000")) if legacy_overrides else props.get("trading.shared-capital.total-notional", "500000")
    props["trading.databento.option-parents"] = f"{symbol}.OPT"
    props["trading.databento.model-routing-csv"] = model_routing_property
    props["trading.databento.symbol-plan-csv"] = symbol_plan_property
    routed_model_dir = (routing_row or {}).get("model_dir", "").strip() if routing_row else ""
    props["trading.model.dir"] = routed_model_dir or (legacy_overrides.get("trading.model.dir", "") if legacy_overrides else "") or f"runtime/models/{symbol}"
    props["trading.state.file"] = f"runtime/databento/state/trader-state-{symbol}.properties"
    props["trading.log.file"] = f"runtime/databento/output/trades-{symbol}.csv"
    props["logging.file.name"] = f"runtime/databento/logs/trading-agent-{symbol}.log"
    props["trading.log.storage-mode"] = legacy_overrides.get("trading.log.storage-mode", props.get("trading.log.storage-mode", "both")) if legacy_overrides else props.get("trading.log.storage-mode", "both")

    if thresholds:
        for key, value in thresholds.items():
            props[key] = f"{value:.2f}"

    if legacy_overrides:
        for key in OVERRIDE_KEYS:
            if thresholds and key in thresholds:
                continue
            value = legacy_overrides.get(key, "").strip()
            if value:
                props[key] = value

    ordered_keys = [
        "trading.symbol",
        "trading.client-id",
        "server.port",
        "trading.market-data.provider",
        "trading.market-data-request-id",
        "trading.trade-amount",
        "trading.risk.max-order-notional",
        "trading.risk.max-share-cap",
        "trading.stop-loss-percentage",
        "trading.ai.long-entry-threshold",
        "trading.ai.short-entry-threshold",
        "trading.ai.long-exit-threshold",
        "trading.ai.short-exit-threshold",
        "trading.ai.regime-threshold",
        "trading.ai.open30.long-entry-threshold",
        "trading.ai.open30.short-entry-threshold",
        "trading.ai.open30.long-exit-threshold",
        "trading.ai.open30.short-exit-threshold",
        "trading.ai.regime.choppy.long-entry-threshold",
        "trading.ai.regime.choppy.short-entry-threshold",
        "trading.ai.regime.choppy.long-exit-threshold",
        "trading.ai.regime.choppy.short-exit-threshold",
        "trading.ai.regime.trend.long-entry-threshold",
        "trading.ai.regime.trend.short-entry-threshold",
        "trading.ai.regime.trend.long-exit-threshold",
        "trading.ai.regime.trend.short-exit-threshold",
        "trading.ai.regime.volatile.long-entry-threshold",
        "trading.ai.regime.volatile.short-entry-threshold",
        "trading.ai.regime.volatile.long-exit-threshold",
        "trading.ai.regime.volatile.short-exit-threshold",
        "trading.shared-capital.enabled",
        "trading.shared-capital.file",
        "trading.shared-capital.total-notional",
        "trading.databento.python-bin",
        "trading.databento.startup-delay-seconds",
        "trading.databento.max-silence-ms",
        "trading.databento.shared-feed.enabled",
        "trading.databento.shared-feed.host",
        "trading.databento.shared-feed.port",
        "trading.databento.shared-feed.shard-id",
        "trading.databento.shared-feed.expected-client-count",
        "trading.databento.shared-feed.client-wait-timeout-ms",
        "trading.databento.shared-feed.bots-dir",
        "trading.databento.shared-feed.lock-file",
        "trading.databento.shared-feed.pid-file",
        "trading.databento.shared-feed.log-file",
        "trading.model.dir",
        "trading.databento.option-parents",
        "trading.databento.model-routing-csv",
        "trading.databento.symbol-plan-csv",
        "trading.state.file",
        "trading.log.file",
        "trading.log.storage-mode",
        "logging.file.name",
    ]
    return {key: props[key] for key in ordered_keys if key in props and str(props[key]).strip() != ""}


def main() -> int:
    args = parse_args()
    bots_dir = args.bots_dir.expanduser().resolve()
    routing_csv = args.routing_csv.expanduser().resolve()
    plan_csv = args.plan_csv.expanduser().resolve()
    final_report_json = args.final_report_json.expanduser().resolve()
    threshold_summary_csv = args.threshold_summary_csv.expanduser().resolve()
    symbols_file = args.symbols_file.expanduser().resolve()
    template_path = args.template_path.expanduser().resolve()
    shared_feed_bots_dir_property = args.shared_feed_bots_dir_property or property_path_value(bots_dir)

    bots_dir.mkdir(parents=True, exist_ok=True)
    template = load_template(template_path)
    routing = load_routing(routing_csv)
    plan_symbols = load_plan_symbols(plan_csv)
    requested_symbols = load_requested_symbols(symbols_file)
    legacy_overrides = load_legacy_overrides()
    existing_bot_props = load_existing_bot_properties(bots_dir, template_path)
    thresholds_by_variant_and_job = load_thresholds_by_variant_and_job(final_report_json)

    if args.restrict_to_requested_symbols:
        if not requested_symbols:
            raise SystemExit("--restrict-to-requested-symbols requires a non-empty --symbols-file.")
        all_symbols = set(requested_symbols)
    else:
        all_symbols = set(routing) | plan_symbols | requested_symbols
    if not all_symbols:
        raise SystemExit("No symbols found in routing or plan CSV.")

    removed_stale = prune_stale_bot_properties(all_symbols, bots_dir, template_path) if args.prune_stale else []
    existing_bot_props = {symbol: props for symbol, props in existing_bot_props.items() if symbol in all_symbols}

    if not template_path.exists():
        write_properties(
            template_path,
            DEFAULT_TEMPLATE,
            [
                "# Databento bot template.",
                "# Copy this file or re-run scripts/generate_databento_bot_properties.py to regenerate symbol-specific configs.",
            ],
        )

    if args.restrict_to_requested_symbols:
        missing_from_routing = sorted(all_symbols - set(routing))
        missing_from_plan = sorted(all_symbols - plan_symbols)
    else:
        missing_from_routing = sorted(plan_symbols - set(routing))
        missing_from_plan = sorted(set(routing) - plan_symbols)

    order = symbol_order(all_symbols)
    runtime_ids = assign_runtime_ids(order, existing_bot_props)
    shard_assignments, shard_counts = assign_shared_feed_shards(order)
    generated = []
    threshold_rows: list[dict[str, str]] = []
    for symbol in order:
        routing_row, routing_source = resolve_routing_row(symbol, routing)
        routing_variant = str(routing_row.get("variant", "default") or "default").strip()
        report_variant = VARIANT_ALIAS.get(routing_variant, routing_variant)
        routing_job = str(routing_row.get("job_name", "") or "").strip()
        thresholds = thresholds_by_variant_and_job.get((report_variant, routing_job), {})
        shard_id = shard_assignments[symbol]
        props = build_symbol_props(
            symbol,
            runtime_ids[symbol],
            shared_feed_props_for_shard(shard_id, shard_counts[shard_id], shared_feed_bots_dir_property),
            template,
            routing_row,
            legacy_overrides.get(symbol),
            thresholds,
            args.model_routing_property,
            args.symbol_plan_property,
        )
        header = [
            f"# Auto-generated Databento bot config for {symbol}.",
            "# Regenerate with: python3 scripts/generate_databento_bot_properties.py",
            f"# routing_job={routing_row.get('job_name', '').strip() or 'n/a'} variant={routing_row.get('variant', '').strip() or 'n/a'} cohort={routing_row.get('cohort', '').strip() or 'n/a'}",
            f"# thresholds_source={report_variant}:{routing_job if routing_job else 'n/a'} regime_threshold=0.50(default) routing_source={routing_source or 'missing'}",
            f"# shared_feed_shard={shard_id} shared_feed_port={props.get('trading.databento.shared-feed.port', '')} shard_expected_clients={props.get('trading.databento.shared-feed.expected-client-count', '')}",
        ]
        write_properties(property_path_for_symbol(symbol, bots_dir), props, header)
        generated.append(symbol)
        threshold_rows.append(
            {
                "symbol": symbol,
                "job_name": routing_job,
                "variant": routing_variant,
                "long_entry_threshold": props.get("trading.ai.long-entry-threshold", ""),
                "short_entry_threshold": props.get("trading.ai.short-entry-threshold", ""),
                "long_exit_threshold": props.get("trading.ai.long-exit-threshold", ""),
                "short_exit_threshold": props.get("trading.ai.short-exit-threshold", ""),
                "regime_threshold": props.get("trading.ai.regime-threshold", ""),
                "open30_long_entry_threshold": props.get("trading.ai.open30.long-entry-threshold", ""),
                "open30_short_entry_threshold": props.get("trading.ai.open30.short-entry-threshold", ""),
                "open30_long_exit_threshold": props.get("trading.ai.open30.long-exit-threshold", ""),
                "open30_short_exit_threshold": props.get("trading.ai.open30.short-exit-threshold", ""),
                "choppy_long_entry_threshold": props.get("trading.ai.regime.choppy.long-entry-threshold", ""),
                "choppy_short_entry_threshold": props.get("trading.ai.regime.choppy.short-entry-threshold", ""),
                "choppy_long_exit_threshold": props.get("trading.ai.regime.choppy.long-exit-threshold", ""),
                "choppy_short_exit_threshold": props.get("trading.ai.regime.choppy.short-exit-threshold", ""),
                "trend_long_entry_threshold": props.get("trading.ai.regime.trend.long-entry-threshold", ""),
                "trend_short_entry_threshold": props.get("trading.ai.regime.trend.short-entry-threshold", ""),
                "trend_long_exit_threshold": props.get("trading.ai.regime.trend.long-exit-threshold", ""),
                "trend_short_exit_threshold": props.get("trading.ai.regime.trend.short-exit-threshold", ""),
                "volatile_long_entry_threshold": props.get("trading.ai.regime.volatile.long-entry-threshold", ""),
                "volatile_short_entry_threshold": props.get("trading.ai.regime.volatile.short-entry-threshold", ""),
                "volatile_long_exit_threshold": props.get("trading.ai.regime.volatile.long-exit-threshold", ""),
                "volatile_short_exit_threshold": props.get("trading.ai.regime.volatile.short-exit-threshold", ""),
                "model_dir": props.get("trading.model.dir", ""),
            }
        )

    threshold_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with threshold_summary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "job_name",
                "variant",
                "long_entry_threshold",
                "short_entry_threshold",
                "long_exit_threshold",
                "short_exit_threshold",
                "regime_threshold",
                "open30_long_entry_threshold",
                "open30_short_entry_threshold",
                "open30_long_exit_threshold",
                "open30_short_exit_threshold",
                "choppy_long_entry_threshold",
                "choppy_short_entry_threshold",
                "choppy_long_exit_threshold",
                "choppy_short_exit_threshold",
                "trend_long_entry_threshold",
                "trend_short_entry_threshold",
                "trend_long_exit_threshold",
                "trend_short_exit_threshold",
                "volatile_long_entry_threshold",
                "volatile_short_entry_threshold",
                "volatile_long_exit_threshold",
                "volatile_short_exit_threshold",
                "model_dir",
            ],
        )
        writer.writeheader()
        writer.writerows(threshold_rows)

    print(f"Generated {len(generated)} bot property files under {bots_dir}")
    if removed_stale:
        print(f"Removed {len(removed_stale)} stale bot property files from {bots_dir}")
    print(f"Wrote threshold summary to {threshold_summary_csv}")
    if missing_from_routing:
        print("Symbols present in plan CSV but missing from routing CSV:", ", ".join(missing_from_routing))
    if missing_from_plan:
        print("Symbols present in routing CSV but missing from plan CSV:", ", ".join(missing_from_plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

