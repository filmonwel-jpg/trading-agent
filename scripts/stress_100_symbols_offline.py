#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_SYMBOL = REPO_ROOT / "run_symbol.sh"
MVNW = REPO_ROOT / "mvnw"
SHARED_RELAY = REPO_ROOT / "scripts" / "databento_shared_feed_relay.py"
MOCK_GATEWAY = REPO_ROOT / "scripts" / "mock_shared_ibkr_gateway.py"
MOCK_NORMALIZER = REPO_ROOT / "scripts" / "mock_databento_burst_normalizer.py"
DEFAULT_SYMBOLS_FILE = REPO_ROOT / "runtime" / "symbols_100.txt"
DEFAULT_BOTS_DIR = REPO_ROOT / "runtime" / "databento" / "bots"


def log(message: str) -> None:
    print(f"[STRESS-100][{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def normalize_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path


def read_symbols(path: Path, limit: int | None) -> list[str]:
    symbols = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if limit is not None:
        symbols = symbols[: max(0, limit)]
    return symbols


def resolve_symbol_paths(symbol: str, default_relay_host: str, default_relay_port: int) -> tuple[Path, Path, str, int]:
    properties_path = DEFAULT_BOTS_DIR / f"trading-{symbol.lower()}.properties"
    if not properties_path.exists():
        raise FileNotFoundError(f"Missing bot properties for {symbol}: {properties_path}")
    props = read_properties(properties_path)
    app_log = normalize_path(props.get("logging.file.name"))
    if app_log is None:
        raise FileNotFoundError(f"Missing logging.file.name for {symbol} in {properties_path}")
    relay_host = (props.get("trading.databento.shared-feed.host") or "").strip() or default_relay_host
    try:
        relay_port = int((props.get("trading.databento.shared-feed.port") or "").strip() or default_relay_port)
    except ValueError:
        relay_port = default_relay_port
    return properties_path, app_log, relay_host, relay_port


def split_symbols_by_config(symbols: list[str]) -> tuple[list[str], list[str]]:
    available: list[str] = []
    missing: list[str] = []
    for symbol in symbols:
        properties_path = DEFAULT_BOTS_DIR / f"trading-{symbol.lower()}.properties"
        if properties_path.exists():
            available.append(symbol)
        else:
            missing.append(symbol)
    return available, missing


def wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        probe = subprocess.run(
            ["/usr/bin/nc", "-z", host, str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.1)
    raise TimeoutError(f"port {host}:{port} did not become reachable within {timeout_seconds}s")


def ensure_packaged_jar() -> None:
    if not MVNW.exists():
        raise FileNotFoundError(f"Missing Maven wrapper: {MVNW}")
    log("building packaged jar before offline stress run")
    subprocess.run([str(MVNW), "-q", "-DskipTests", "package"], cwd=REPO_ROOT, check=True)


@dataclass
class ManagedProcess:
    name: str
    popen: subprocess.Popen[str]

    @property
    def pid(self) -> int:
        return self.popen.pid


class ProcessSampler(threading.Thread):
    def __init__(self, pids: list[int], interval_seconds: float = 0.5):
        super().__init__(daemon=True)
        self.pids = list(pids)
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, float | int]] = []
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            live_pids = [pid for pid in self.pids if process_alive(pid)]
            total_cpu = 0.0
            total_rss_kb = 0
            if live_pids:
                command = ["ps", "-o", "pid=,%cpu=,rss=", "-p", ",".join(str(pid) for pid in live_pids)]
                result = subprocess.run(command, capture_output=True, text=True, check=False)
                for raw_line in result.stdout.splitlines():
                    parts = raw_line.split()
                    if len(parts) != 3:
                        continue
                    try:
                        total_cpu += float(parts[1])
                        total_rss_kb += int(parts[2])
                    except ValueError:
                        continue
            self.samples.append(
                {
                    "ts": time.time(),
                    "alive": len(live_pids),
                    "total_cpu_pct": total_cpu,
                    "total_rss_mb": total_rss_kb / 1024.0,
                }
            )
            time.sleep(self.interval_seconds)


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_process_group(process: subprocess.Popen[str], grace_seconds: float = 5.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def launch_background(command: list[str], log_path: Path, name: str) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    return ManagedProcess(name=name, popen=process)


def build_run_symbol_command(symbol: str, args: argparse.Namespace, relay_host: str, relay_port: int) -> list[str]:
    return [
        str(RUN_SYMBOL),
        symbol,
        "--start",
        "--skip-ibkr-preflight",
        "--max-trades=1",
        "--",
        "--trading.ai.long-entry-threshold=1.0",
        "--trading.ai.short-entry-threshold=1.0",
        "--trading.ai.long-exit-threshold=1.0",
        "--trading.ai.short-exit-threshold=1.0",
        f"--trading.ibkr.shared-gateway.enabled=true",
        f"--trading.ibkr.shared-gateway.host={args.gateway_host}",
        f"--trading.ibkr.shared-gateway.port={args.gateway_port}",
        "--trading.ibkr.shared-gateway.skip-direct-connection=true",
        "--trading.databento.shared-feed.enabled=true",
        f"--trading.databento.shared-feed.host={relay_host}",
        f"--trading.databento.shared-feed.port={relay_port}",
        "--trading.databento.shared-feed.start-if-missing=false",
        "--trading.databento.shared-feed.fallback-to-private-sidecar=false",
    ]


def summarize_log_tail(content: str) -> dict[str, int | bool]:
    dispatches = len(re.findall(r"Dispatching AI evaluation symbol=", content))
    predictions = len(re.findall(r"running prediction featureCount=", content))
    errors = len(re.findall(r"\[FLOW\]\[ERROR\]", content))
    strategy_bars = len(re.findall(r"\[STRATEGY\.BAR\]", content))
    sync_complete = "POSITION_SYNC_COMPLETE" in content or "position_sync_completed" in content
    return {
        "dispatches": dispatches,
        "predictions": predictions,
        "errors": errors,
        "strategy_bars": strategy_bars,
        "position_sync_complete": sync_complete,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline 100-symbol stress test using local mock feed and mock shared IBKR gateway")
    parser.add_argument("--symbols-file", type=Path, default=DEFAULT_SYMBOLS_FILE)
    parser.add_argument("--symbols-limit", type=int, default=None)
    parser.add_argument("--relay-host", default="127.0.0.1")
    parser.add_argument("--relay-port", type=int, default=19800)
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=19910)
    # Full 100-symbol runs were intermittently under-started at 25s; 40s produced repeatable 100/100 bar+dispatch coverage.
    parser.add_argument("--startup-delay-seconds", type=float, default=40.0)
    parser.add_argument("--burst-count", type=int, default=18)
    parser.add_argument("--slice-sleep-seconds", type=float, default=0.05)
    parser.add_argument("--post-run-settle-seconds", type=float, default=6.0)
    parser.add_argument("--sample-interval-seconds", type=float, default=0.5)
    parser.add_argument("--relay-client-wait-seconds", type=float, default=90.0)
    parser.add_argument("--report-dir", type=Path, default=REPO_ROOT / "runtime" / "stress-reports")
    args = parser.parse_args()

    requested_symbols = read_symbols(args.symbols_file, args.symbols_limit)
    if not requested_symbols:
        raise SystemExit(f"No symbols found in {args.symbols_file}")
    symbols, missing_symbols = split_symbols_by_config(requested_symbols)
    if missing_symbols:
        log(f"skipping {len(missing_symbols)} symbols without generated bot configs: {','.join(missing_symbols)}")
    if not symbols:
        raise SystemExit("No symbols with generated bot configs remain after filtering missing configs")

    ensure_packaged_jar()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = args.report_dir.resolve() / f"offline_100_symbols_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    relay_groups: dict[tuple[str, int], list[str]] = {}
    symbol_info: dict[str, dict[str, object]] = {}
    for symbol in symbols:
        properties_path, app_log, relay_host, relay_port = resolve_symbol_paths(symbol, args.relay_host, args.relay_port)
        prior_size = app_log.stat().st_size if app_log.exists() else 0
        symbol_info[symbol] = {
            "properties": str(properties_path),
            "app_log": str(app_log),
            "prior_size": prior_size,
            "relay_host": relay_host,
            "relay_port": relay_port,
        }
        relay_groups.setdefault((relay_host, relay_port), []).append(symbol)

    gateway_log = report_dir / "mock_shared_ibkr_gateway.log"
    summary_json = report_dir / "summary.json"

    managed: list[ManagedProcess] = []
    sampler: ProcessSampler | None = None
    bot_processes: list[ManagedProcess] = []
    relay_processes: list[ManagedProcess] = []
    relay_logs: dict[str, str] = {}
    try:
        gateway_proc = launch_background(
            [sys.executable, str(MOCK_GATEWAY), "--host", args.gateway_host, "--port", str(args.gateway_port)],
            gateway_log,
            "mock-shared-ibkr-gateway",
        )
        managed.append(gateway_proc)
        wait_for_port(args.gateway_host, args.gateway_port, 10.0)

        for relay_host, relay_port in sorted(relay_groups, key=lambda target: (target[0], target[1])):
            shard_symbols = relay_groups[(relay_host, relay_port)]
            relay_log = report_dir / f"mock_databento_relay_{relay_port}.log"
            relay_logs[f"{relay_host}:{relay_port}"] = str(relay_log)
            relay_command = [
                sys.executable,
                str(SHARED_RELAY),
                "--python-bin",
                sys.executable,
                "--normalizer-script",
                str(MOCK_NORMALIZER),
                "--bots-dir",
                str(DEFAULT_BOTS_DIR),
                "--working-dir",
                str(report_dir),
                "--listen-host",
                relay_host,
                "--listen-port",
                str(relay_port),
                "--pid-file",
                str(report_dir / f"mock_databento_relay_{relay_port}.pid"),
                "--equity-dataset",
                "DBEQ.BASIC",
                "--equity-schema",
                "tbbo",
                "--options-dataset",
                "OPRA.PILLAR",
                "--options-schema",
                "ohlcv-1s",
                "--heartbeat-seconds",
                "15",
                "--expected-client-count",
                str(len(shard_symbols)),
                "--wait-for-clients-timeout-seconds",
                str(max(1.0, args.relay_client_wait_seconds)),
            ]
            relay_env = os.environ.copy()
            relay_env["PYTHONUNBUFFERED"] = "1"
            relay_env["MOCK_DATABENTO_BURST_STARTUP_DELAY_SECONDS"] = str(args.startup_delay_seconds)
            relay_env["MOCK_DATABENTO_BURST_COUNT"] = str(args.burst_count)
            relay_env["MOCK_DATABENTO_SLICE_SLEEP_SECONDS"] = str(args.slice_sleep_seconds)
            relay_log_handle = relay_log.open("w", encoding="utf-8")
            relay_process = subprocess.Popen(
                relay_command,
                cwd=REPO_ROOT,
                stdout=relay_log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
                env=relay_env,
            )
            relay_proc = ManagedProcess(name=f"mock-databento-relay-{relay_port}", popen=relay_process)
            relay_processes.append(relay_proc)
            managed.append(relay_proc)
            wait_for_port(relay_host, relay_port, 10.0)

        log(f"launching {len(symbols)} symbol JVMs")
        for symbol in symbols:
            launch_log = report_dir / f"launch_{symbol}.log"
            relay_host = str(symbol_info[symbol]["relay_host"])
            relay_port = int(symbol_info[symbol]["relay_port"])
            proc = launch_background(build_run_symbol_command(symbol, args, relay_host, relay_port), launch_log, symbol)
            bot_processes.append(proc)
            managed.append(proc)
            time.sleep(0.05)

        sampler = ProcessSampler([proc.pid for proc in bot_processes], interval_seconds=args.sample_interval_seconds)
        sampler.start()

        normalizer_runtime_budget = (
            max(0.0, args.relay_client_wait_seconds)
            + args.startup_delay_seconds
            + (args.burst_count * 6 * args.slice_sleep_seconds)
            + 45.0
        )
        deadline = time.time() + max(60.0, normalizer_runtime_budget)
        for relay_proc in relay_processes:
            relay_proc.popen.wait(timeout=max(1.0, deadline - time.time()))
        time.sleep(max(0.0, args.post_run_settle_seconds))
        for proc in reversed(bot_processes):
            kill_process_group(proc.popen)

        sampler.stop()
        sampler.join(timeout=5.0)

        sample_count = len(sampler.samples) if sampler else 0
        peak_alive = max((int(sample["alive"]) for sample in sampler.samples), default=0)
        peak_cpu_pct = max((float(sample["total_cpu_pct"]) for sample in sampler.samples), default=0.0)
        peak_rss_mb = max((float(sample["total_rss_mb"]) for sample in sampler.samples), default=0.0)
        avg_cpu_pct = (sum(float(sample["total_cpu_pct"]) for sample in sampler.samples) / sample_count) if sample_count else 0.0
        avg_rss_mb = (sum(float(sample["total_rss_mb"]) for sample in sampler.samples) / sample_count) if sample_count else 0.0

        symbol_results: dict[str, dict[str, object]] = {}
        dispatching_symbols = 0
        live_bar_symbols = 0
        sync_complete_symbols = 0
        total_prediction_calls = 0
        total_error_lines = 0
        total_strategy_bars = 0
        for symbol, info in symbol_info.items():
            app_log = Path(str(info["app_log"]))
            prior_size = int(info["prior_size"])
            tail = ""
            if app_log.exists():
                with app_log.open("r", encoding="utf-8", errors="ignore") as handle:
                    current_size = app_log.stat().st_size
                    handle.seek(prior_size if current_size >= prior_size else 0)
                    tail = handle.read()
            summary = summarize_log_tail(tail)
            dispatching_symbols += 1 if int(summary["dispatches"]) > 0 else 0
            live_bar_symbols += 1 if int(summary["strategy_bars"]) > 0 else 0
            sync_complete_symbols += 1 if bool(summary["position_sync_complete"]) else 0
            total_prediction_calls += int(summary["predictions"])
            total_error_lines += int(summary["errors"])
            total_strategy_bars += int(summary["strategy_bars"])
            symbol_results[symbol] = {
                **info,
                **summary,
            }

        report = {
            "generated_at": datetime.now().isoformat(),
            "symbols_requested": len(requested_symbols),
            "symbols_started": len(symbols),
            "symbols_missing_configs": missing_symbols,
            "symbols_with_live_bars": live_bar_symbols,
            "symbols_with_ai_dispatch": dispatching_symbols,
            "symbols_with_position_sync": sync_complete_symbols,
            "total_strategy_bars_logged": total_strategy_bars,
            "total_prediction_calls_logged": total_prediction_calls,
            "total_error_lines": total_error_lines,
            "sampler": {
                "samples": sample_count,
                "peak_alive": peak_alive,
                "peak_cpu_pct_total": round(peak_cpu_pct, 2),
                "avg_cpu_pct_total": round(avg_cpu_pct, 2),
                "peak_rss_mb_total": round(peak_rss_mb, 2),
                "avg_rss_mb_total": round(avg_rss_mb, 2),
            },
            "paths": {
                "report_dir": str(report_dir),
                "summary_json": str(summary_json),
                "gateway_log": str(gateway_log),
                "relay_logs": relay_logs,
            },
            "relay_groups": {
                f"{relay_host}:{relay_port}": relay_groups[(relay_host, relay_port)]
                for relay_host, relay_port in sorted(relay_groups, key=lambda target: (target[0], target[1]))
            },
            "symbols": symbol_results,
        }
        summary_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(json.dumps(report, indent=2))
        return 0
    finally:
        if sampler is not None:
            sampler.stop()
        for proc in reversed(managed):
            kill_process_group(proc.popen)


if __name__ == "__main__":
    raise SystemExit(main())







