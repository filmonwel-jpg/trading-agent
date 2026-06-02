#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import os
import sys
import threading
import time
from argparse import Namespace
from contextlib import redirect_stdout
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parents[1]
NORMALIZER_SCRIPT = REPO_ROOT / "scripts" / "databento_live_normalizer.py"


def install_fake_databento_modules() -> None:
    fake_databento = ModuleType("databento")
    fake_dbn = ModuleType("databento_dbn")

    class FakeFrame:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = list(rows)

        @property
        def empty(self) -> bool:
            return not self._rows

        def reset_index(self, drop: bool = False):
            return self

        def sort_values(self, by):
            keys = list(by) if isinstance(by, (list, tuple)) else [by]
            return FakeFrame(sorted(self._rows, key=lambda row: tuple(str(row.get(key, "")) for key in keys)))

        def to_dict(self, orient: str = "records"):
            if orient != "records":
                raise AssertionError(f"unsupported orient {orient!r}")
            return list(self._rows)

    class FakeHistoricalResult:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = list(rows)

        def to_df(self):
            return FakeFrame(self._rows)

    class FakeTimeseries:
        def __init__(self, owner: "FakeHistorical") -> None:
            self.owner = owner

        def get_range(self, dataset: str, schema: str, stype_in: str, symbols: list[str], start, end):
            symbol = symbols[0]
            self.owner.calls.append(
                {
                    "dataset": dataset,
                    "schema": schema,
                    "stype_in": stype_in,
                    "symbols": list(symbols),
                    "start": start,
                    "end": end,
                }
            )
            scripted = self.owner.scripted_responses.get(symbol, [])
            if scripted:
                response = scripted.pop(0)
                if isinstance(response, Exception):
                    raise response
                rows = list(response)
            else:
                rows = list(type(self.owner).rows_by_symbol.get(symbol, []))
            return FakeHistoricalResult(rows)

    class FakeHistorical:
        rows_by_symbol: dict[str, list[dict[str, object]]] = {}
        scripted_responses_by_symbol: dict[str, list[object]] = {}
        instances: list["FakeHistorical"] = []

        def __init__(self, key: str) -> None:
            self.key = key
            self.calls: list[dict[str, object]] = []
            self.scripted_responses = {
                symbol: list(responses)
                for symbol, responses in type(self).scripted_responses_by_symbol.items()
            }
            self.timeseries = FakeTimeseries(self)
            type(self).instances.append(self)

    class FakeLive:
        behaviors: dict[str, str] = {}
        scripted_records: dict[str, list[object]] = {}
        instances: list["FakeLive"] = []

        def __init__(self, key: str, gateway: str | None = None, reconnect_policy: str = "reconnect", **_kwargs) -> None:
            self.key = key
            self.gateway = gateway
            self.reconnect_policy = reconnect_policy
            self.dataset = ""
            self.schema = ""
            self.symbols: list[str] = []
            self.snapshot: bool | None = None
            self.terminated = False
            type(self).instances.append(self)

        def subscribe(self, dataset: str, schema: str, symbols: list[str], stype_in: str, snapshot: bool) -> None:
            self.dataset = dataset
            self.schema = schema
            self.symbols = list(symbols)
            self.snapshot = snapshot

        def __iter__(self):
            scripted = list(type(self).scripted_records.get(self.schema, []))
            behavior = type(self).behaviors.get(self.schema, "wait")
            if scripted:
                def generator():
                    for record in scripted:
                        if self.terminated:
                            break
                        yield record
                    if behavior == "end":
                        return
                    if behavior == "raise":
                        raise RuntimeError(f"{self.schema}-boom")
                    while not self.terminated:
                        time.sleep(0.01)
                    if False:
                        yield None
                return generator()
            if behavior == "end":
                return iter(())
            if behavior == "raise":
                def generator():
                    raise RuntimeError(f"{self.schema}-boom")
                    yield None
                return generator()

            def generator():
                while not self.terminated:
                    time.sleep(0.01)
                if False:
                    yield None

            return generator()

        def terminate(self) -> None:
            self.terminated = True

    fake_databento.Live = FakeLive
    fake_databento.Historical = FakeHistorical

    for class_name in [
        "ErrorMsg",
        "SystemMsg",
        "SymbolMappingMsg",
        "MBP1Msg",
        "CMBP1Msg",
        "TradeMsg",
        "BBOMsg",
        "CBBOMsg",
        "OHLCVMsg",
    ]:
        setattr(fake_dbn, class_name, type(class_name, (), {}))

    sys.modules["databento"] = fake_databento
    sys.modules["databento_dbn"] = fake_dbn


def load_normalizer_module():
    install_fake_databento_modules()
    module_name = f"databento_live_normalizer_test_{time.time_ns()}"
    spec = spec_from_file_location(module_name, NORMALIZER_SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"unable to load module from {NORMALIZER_SCRIPT}")
    module = module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_args() -> Namespace:
    return Namespace(
        dry_run=False,
        symbols=["TSLA"],
        option_parents=["TSLA"],
        equity_dataset="EQUS.MINI",
        equity_schema="tbbo",
        startup_history_seconds=0.0,
        startup_history_schema="ohlcv-1s",
        options_dataset="OPRA.PILLAR",
        options_schema="ohlcv-1s",
        live_gateway="",
        equity_flush_lag_ms=50.0,
        heartbeat_seconds=1,
        startup_delay_seconds=0.0,
    )


def run_failfast_scenario(module, behaviors: dict[str, str], expected_status_fragment: str) -> None:
    module.db.Live.behaviors = dict(behaviors)
    module.db.Live.scripted_records = {}
    module.db.Live.instances = []
    args = build_args()
    previous_api_key = os.environ.get("DATABENTO_API_KEY")
    os.environ["DATABENTO_API_KEY"] = "test-key"
    output = io.StringIO()
    try:
        with redirect_stdout(output):
            exit_code = module.DatabentoNormalizer(args).run()
    finally:
        if previous_api_key is None:
            os.environ.pop("DATABENTO_API_KEY", None)
        else:
            os.environ["DATABENTO_API_KEY"] = previous_api_key

    if exit_code != module.STREAM_FAILURE_EXIT_CODE:
        raise AssertionError(f"expected fail-fast exit code {module.STREAM_FAILURE_EXIT_CODE}, got {exit_code}")

    payloads = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    status_messages = [str(payload.get("message") or "") for payload in payloads if payload.get("event") == "status"]

    if not any(expected_status_fragment in message for message in status_messages):
        raise AssertionError(
            f"missing expected fatal status fragment {expected_status_fragment!r}; got {status_messages!r}"
        )
    if not any("normalizer-fatal exitCode=" in message for message in status_messages):
        raise AssertionError(f"missing final fatal-exit status; got {status_messages!r}")
    if any("heartbeat symbols=" in message for message in status_messages):
        raise AssertionError(f"unexpected heartbeat emitted after stream death; got {status_messages!r}")
    option_clients = [client for client in module.db.Live.instances if client.schema == "ohlcv-1s"]
    if not option_clients or option_clients[0].symbols != ["TSLA.OPT"]:
        raise AssertionError(f"expected normalized OPRA parent ['TSLA.OPT'], got {[client.symbols for client in option_clients]!r}")
    if option_clients[0].snapshot is not False:
        raise AssertionError(f"expected OPRA options subscribe snapshot=False, got {option_clients[0].snapshot!r}")


def run_startup_history_scenario(module) -> None:
    module.db.Historical.rows_by_symbol = {
        "TSLA": [
            {
                "symbol": "TSLA",
                "ts_event": "2026-04-13T13:30:00+00:00",
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.5,
                "volume": 10,
            },
            {
                "symbol": "TSLA",
                "ts_event": "2026-04-13T13:30:01+00:00",
                "open": 100.5,
                "high": 101.5,
                "low": 100.0,
                "close": 101.0,
                "volume": 12,
            },
        ]
    }
    module.db.Historical.scripted_responses_by_symbol = {}
    module.db.Historical.instances = []
    args = build_args()
    args.startup_history_seconds = 360.0
    output = io.StringIO()
    with redirect_stdout(output):
        module.DatabentoNormalizer(args)._emit_startup_equity_history("test-key")

    payloads = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    bars = [payload for payload in payloads if payload.get("event") == "equity_bar"]
    statuses = [str(payload.get("message") or "") for payload in payloads if payload.get("event") == "status"]

    if len(bars) != 2:
        raise AssertionError(f"expected 2 startup history bars, got {bars!r}")
    if bars[0].get("historical") is not True or bars[1].get("historical") is not True:
        raise AssertionError(f"expected historical flag on startup bars, got {bars!r}")
    if [bars[0].get("close"), bars[1].get("close")] != [100.5, 101.0]:
        raise AssertionError(f"unexpected startup history close sequence: {bars!r}")
    if not any("startup-history-begin" in message for message in statuses):
        raise AssertionError(f"missing startup-history-begin status, got {statuses!r}")
    if not any("startup-history-complete emittedBars=2" in message for message in statuses):
        raise AssertionError(f"missing startup-history-complete status, got {statuses!r}")

    historical_instances = getattr(module.db, "Historical").instances
    if len(historical_instances) != 1:
        raise AssertionError(f"expected one historical client, got {len(historical_instances)}")
    historical_calls = historical_instances[0].calls
    if not historical_calls:
        raise AssertionError("expected startup history fetch call")
    call = historical_calls[0]
    if call.get("schema") != "ohlcv-1s" or call.get("stype_in") != "raw_symbol":
        raise AssertionError(f"unexpected historical call args: {call!r}")


def run_startup_history_clamp_scenario(module) -> None:
    rows = [
        {
            "symbol": "TSLA",
            "ts_event": "2026-04-14T13:34:00+00:00",
            "open": 99.0,
            "high": 100.0,
            "low": 98.5,
            "close": 99.5,
            "volume": 8,
        },
        {
            "symbol": "TSLA",
            "ts_event": "2026-04-14T13:39:59+00:00",
            "open": 100.0,
            "high": 101.0,
            "low": 99.8,
            "close": 100.75,
            "volume": 14,
        },
    ]
    module.db.Historical.rows_by_symbol = {}
    module.db.Historical.scripted_responses_by_symbol = {
        "TSLA": [
            RuntimeError(
                "422 data_start_after_available_end `start` in query ('2026-04-14 13:47:14+00:00') "
                "was after the available end of dataset EQUS.MINI ('2026-04-14 13:40:00+00:00')."
            ),
            rows,
        ]
    }
    module.db.Historical.instances = []
    args = build_args()
    args.startup_history_seconds = 360.0
    normalizer = module.DatabentoNormalizer(args)
    normalizer._utc_now = lambda: module.datetime(2026, 4, 14, 13, 53, 14, tzinfo=module.timezone.utc)
    output = io.StringIO()
    with redirect_stdout(output):
        normalizer._emit_startup_equity_history("test-key")

    payloads = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    statuses = [str(payload.get("message") or "") for payload in payloads if payload.get("event") == "status"]
    bars = [payload for payload in payloads if payload.get("event") == "equity_bar"]

    if len(bars) != 2:
        raise AssertionError(f"expected 2 startup history bars after clamp, got {bars!r}")
    if not any("startup-history-clamped symbol=TSLA start=2026-04-14T13:34:00+00:00 end=2026-04-14T13:40:00+00:00" in message for message in statuses):
        raise AssertionError(f"missing startup-history-clamped status, got {statuses!r}")
    if not any("startup-history-complete emittedBars=2" in message for message in statuses):
        raise AssertionError(f"missing startup-history-complete status, got {statuses!r}")

    historical_instances = getattr(module.db, "Historical").instances
    if len(historical_instances) != 1:
        raise AssertionError(f"expected one historical client, got {len(historical_instances)}")
    historical_calls = historical_instances[0].calls
    if len(historical_calls) != 2:
        raise AssertionError(f"expected two historical calls due to clamp retry, got {historical_calls!r}")
    retried_call = historical_calls[1]
    if retried_call.get("start").isoformat() != "2026-04-14T13:34:00+00:00":
        raise AssertionError(f"unexpected clamped retry start: {retried_call!r}")
    if retried_call.get("end").isoformat() != "2026-04-14T13:40:00+00:00":
        raise AssertionError(f"unexpected clamped retry end: {retried_call!r}")


def run_timer_flush_scenario(module) -> None:
    module.db.Live.behaviors = {"tbbo": "wait"}
    module.db.Live.scripted_records = {"tbbo": []}
    module.db.Live.instances = []

    mapping = module.dbn.SymbolMappingMsg()
    mapping.instrument_id = 101
    mapping.stype_out_symbol = "TSLA"
    mapping.stype_in_symbol = "TSLA"

    trade = module.dbn.TradeMsg()
    trade.instrument_id = 101
    trade.ts_event = (int(time.time()) - 2) * 1_000_000_000
    trade.price = 250.25 * 1_000_000_000
    trade.size = 10
    trade.side = "A"

    module.db.Live.scripted_records["tbbo"] = [mapping, trade]

    args = build_args()
    args.equity_flush_lag_ms = 50.0
    normalizer = module.DatabentoNormalizer(args)
    output = io.StringIO()

    def target() -> None:
        with redirect_stdout(output):
            normalizer._run_equity_stream("test-key")

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    time.sleep(0.35)

    live_instances = list(module.db.Live.instances)
    if len(live_instances) != 1:
        raise AssertionError(f"expected one live instance for direct equity stream test, got {len(live_instances)}")
    normalizer.stop_event.set()
    live_instances[0].terminate()
    worker.join(timeout=3)
    if worker.is_alive():
        raise AssertionError("timer flush scenario worker did not terminate")

    payloads = [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]
    bars = [payload for payload in payloads if payload.get("event") == "equity_bar"]
    if len(bars) != 1:
        raise AssertionError(f"expected exactly one flushed equity bar and no synthetic gap bars, got {bars!r}")
    bar = bars[0]
    if bar.get("symbol") != "TSLA" or bar.get("barEpochSec") != int(trade.ts_event // 1_000_000_000):
        raise AssertionError(f"unexpected flushed bar payload: {bar!r}")
    if bar.get("volume") != 10 or bar.get("close") != 250.25:
        raise AssertionError(f"unexpected trade aggregation in flushed bar: {bar!r}")


def run_live_gateway_override_scenario(module) -> None:
    module.db.Live.behaviors = {"tbbo": "end", "ohlcv-1s": "end"}
    module.db.Live.scripted_records = {}
    module.db.Live.instances = []
    args = build_args()
    args.live_gateway = "live.databento.test"
    previous_api_key = os.environ.get("DATABENTO_API_KEY")
    os.environ["DATABENTO_API_KEY"] = "test-key"
    try:
        with redirect_stdout(io.StringIO()):
            exit_code = module.DatabentoNormalizer(args).run()
    finally:
        if previous_api_key is None:
            os.environ.pop("DATABENTO_API_KEY", None)
        else:
            os.environ["DATABENTO_API_KEY"] = previous_api_key

    if exit_code != module.STREAM_FAILURE_EXIT_CODE:
        raise AssertionError(f"expected fail-fast exit code {module.STREAM_FAILURE_EXIT_CODE}, got {exit_code}")
    live_instances = list(module.db.Live.instances)
    if len(live_instances) != 2:
        raise AssertionError(f"expected 2 live clients, got {len(live_instances)}")
    gateways = {client.gateway for client in live_instances}
    if gateways != {"live.databento.test"}:
        raise AssertionError(f"expected live gateway override to reach both clients, got {gateways!r}")


def main() -> int:
    module = load_normalizer_module()
    run_startup_history_scenario(module)
    run_startup_history_clamp_scenario(module)
    run_timer_flush_scenario(module)
    run_live_gateway_override_scenario(module)
    run_failfast_scenario(module, {"tbbo": "end", "ohlcv-1s": "wait"}, "equity-stream-fatal stream-ended")
    run_failfast_scenario(module, {"tbbo": "wait", "ohlcv-1s": "end"}, "options-stream-fatal stream-ended")
    print("normalizer-failfast-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


