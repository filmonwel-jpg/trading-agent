#!/usr/bin/env python3
"""Run Databento recorded-file backtests for multiple symbols concurrently.

This is a paper-shadow replay harness: it launches the existing single-symbol
Databento historical IBKR simulation backtester once per symbol, all pointed at
the same recorded NDJSON/NDJSON.GZ event file, and monitors the processes as a
cohort. That mirrors the live/paper deployment shape more closely than the
sequential wrapper because all symbol bots are alive at the same wall-clock time.

Outputs under --output-dir:
- logs/<SYMBOL>.log                        per-symbol Java/wrapper log
- parallel_file_replay.log                 parent monitor/build log
- parallel_file_replay_events.jsonl        structured launch/monitor/finish events
- controlled_java_replay.parallel_combined.log combined child logs + aggregate completion marker
- parallel_file_replay_summary.csv/json    per-symbol status and output counts
- parallel_pnl_period_summary.*            optional existing lifecycle PnL summary outputs
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_SYMBOLS = "NVDA,QQQ,SPY,TQQQ,TSLA"
DEFAULT_TRADE_AMOUNT = 500_000
DEFAULT_MAX_ORDER_NOTIONAL = 500_000
DEFAULT_MAX_TRADES = 2_000
DEFAULT_MAX_SHARE_CAP = 2_000


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_symbols(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in str(raw or "").replace("\n", ",").split(","):
        symbol = token.strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def load_symbols_file(path: Path) -> list[str]:
    tokens: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].replace(",", " ").strip()
        if not line:
            continue
        tokens.extend(part.strip().upper() for part in line.split() if part.strip())
    return parse_symbols(",".join(tokens))


def split_recorded_events(raw: str) -> list[Path]:
    return [Path(part.strip()).expanduser() for part in str(raw or "").split(",") if part.strip()]


def shell_join(command: list[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in command)


def tail_line(path: Path, max_bytes: int = 8192) -> str:
    if not path.is_file() or path.stat().st_size <= 0:
        return ""
    with path.open("rb") as handle:
        size = handle.seek(0, os.SEEK_END)
        handle.seek(max(0, size - max_bytes), os.SEEK_SET)
        chunk = handle.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in chunk.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def count_csv_rows(path: Path) -> int:
    if not path.is_file() or path.stat().st_size <= 0:
        return 0
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        # Count data rows only. Avoid pandas so monitor stays dependency-light.
        return max(0, sum(1 for _ in handle) - 1)


def latest_matching(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


@dataclass
class SymbolRun:
    symbol: str
    command: list[str]
    log_path: Path
    process: subprocess.Popen[str] | None = None
    status: str = "pending"
    started_at_utc: str = ""
    finished_at_utc: str = ""
    start_monotonic: float = 0.0
    end_monotonic: float = 0.0
    returncode: int | None = None
    pid: int | None = None
    launch_index: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def elapsed_seconds(self) -> float:
        if not self.start_monotonic:
            return 0.0
        end = self.end_monotonic or time.monotonic()
        return max(0.0, end - self.start_monotonic)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recorded-events", required=True, help="Recorded normalized NDJSON/NDJSON.GZ file. Comma-separated files are allowed.")
    parser.add_argument("--symbols", default="", help=f"Comma-separated symbols. Default when --symbols-file is omitted: {DEFAULT_SYMBOLS}")
    parser.add_argument("--symbols-file", type=Path, default=None, help="Optional symbol file. Used only when --symbols is empty.")
    parser.add_argument("--exclude", default="", help="Comma-separated symbols to exclude.")
    parser.add_argument("--start", default="", help="Optional replay start date/time passed to the child backtester.")
    parser.add_argument("--end", default="", help="Optional replay end date/time passed to the child backtester.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory. Default: runtime/backtests/parallel-file-replay-<timestamp>")
    parser.add_argument("--jobs", type=int, default=0, help="Concurrent symbols. Default 0 means all selected symbols start together.")
    parser.add_argument("--monitor-interval-sec", type=float, default=30.0, help="Structured monitor interval. Default: 30 seconds.")
    parser.add_argument("--timeout-seconds", type=int, default=0, help="Per-symbol child stream timeout passed to the child wrapper. Default: 0.")
    parser.add_argument("--trade-amount", type=int, default=DEFAULT_TRADE_AMOUNT, help=f"Per-trade notional. Default: {DEFAULT_TRADE_AMOUNT}")
    parser.add_argument("--max-order-notional", type=int, default=DEFAULT_MAX_ORDER_NOTIONAL, help=f"Per-order notional risk cap. Default: {DEFAULT_MAX_ORDER_NOTIONAL}")
    parser.add_argument("--max-trades", type=int, default=DEFAULT_MAX_TRADES, help=f"Per-symbol max trades. Default: {DEFAULT_MAX_TRADES}")
    parser.add_argument("--max-share-cap", type=int, default=DEFAULT_MAX_SHARE_CAP, help=f"Per-order share cap. Default: {DEFAULT_MAX_SHARE_CAP}")
    parser.add_argument("--model-dir", type=Path, default=None, help="Optional shared setup model dir. If omitted, child wrapper resolves per-symbol routing.")
    parser.add_argument("--lifecycle-model-dir", type=Path, default=None, help="Optional lifecycle/micro model dir.")
    parser.add_argument("--setup-thresholds-file", type=Path, default=None, help="Optional setup threshold properties file.")
    parser.add_argument("--python-bin", type=Path, default=None, help="Python interpreter for child Databento streamer.")
    parser.add_argument("--downstream-setup-filter-manifest", type=Path, default=None, help="Enable downstream setup filter route manifest.")
    parser.add_argument("--downstream-setup-filter-features-csv", type=Path, default=None, help="Optional downstream setup sidecar CSV.")
    parser.add_argument("--downstream-setup-filter-fail-open", action="store_true", help="Pass fail-open mode to child wrapper.")
    parser.add_argument("--micro-entry-research-no-trade", action="store_true", help="Record micro-entry confirmations but do not trade.")
    parser.add_argument("--disable-lifecycle-micro", action="store_true", help="Disable lifecycle/micro routes in child wrapper.")
    parser.add_argument("--dry-run", action="store_true", help="Pass --dry-run to child wrapper.")
    parser.add_argument("--skip-build", action="store_true", help="Skip parent Maven package/classpath build. Children still receive --skip-build.")
    parser.add_argument("--classpath-file", type=Path, default=None, help="Classpath cache file. Default: <output-dir>/parallel_databento_backtest_cp.txt")
    parser.add_argument("--backtest-script", type=Path, default=None, help="Child backtest script. Default: scripts/run_databento_historical_ibkr_sim_backtest.sh")
    parser.add_argument("--run-summary", dest="run_summary", action="store_true", default=True, help="Run lifecycle PnL summarizer after replay. Default: on.")
    parser.add_argument("--no-summary", dest="run_summary", action="store_false", help="Do not run lifecycle PnL summarizer after replay.")
    parser.add_argument("--title", default="Parallel Databento file replay paper-shadow", help="Title for optional PnL summary.")
    return parser


def make_child_command(args: argparse.Namespace, repo_root: Path, output_dir: Path, classpath_file: Path, symbol: str, passthrough: list[str]) -> list[str]:
    backtest_script = args.backtest_script or (repo_root / "scripts/run_databento_historical_ibkr_sim_backtest.sh")
    if not backtest_script.is_absolute():
        backtest_script = repo_root / backtest_script
    command = [
        str(backtest_script),
        "--symbol", symbol,
        "--source", "ndjson",
        "--recorded-events", args.recorded_events,
        "--output-dir", str(output_dir),
        "--trade-amount", str(args.trade_amount),
        "--max-order-notional", str(args.max_order_notional),
        "--max-trades", str(args.max_trades),
        "--max-share-cap", str(args.max_share_cap),
        "--classpath-file", str(classpath_file),
        "--skip-build",
    ]
    if args.start:
        command.extend(["--start", args.start])
    if args.end:
        command.extend(["--end", args.end])
    if args.timeout_seconds > 0:
        command.extend(["--timeout-seconds", str(args.timeout_seconds)])
    if args.model_dir:
        command.extend(["--model-dir", str(args.model_dir)])
    if args.lifecycle_model_dir:
        command.extend(["--lifecycle-model-dir", str(args.lifecycle_model_dir)])
    if args.setup_thresholds_file:
        command.extend(["--setup-thresholds-file", str(args.setup_thresholds_file)])
    if args.python_bin:
        command.extend(["--python-bin", str(args.python_bin)])
    if args.downstream_setup_filter_manifest:
        command.extend(["--downstream-setup-filter-manifest", str(args.downstream_setup_filter_manifest)])
    if args.downstream_setup_filter_features_csv:
        command.extend(["--downstream-setup-filter-features-csv", str(args.downstream_setup_filter_features_csv)])
    if args.downstream_setup_filter_fail_open:
        command.append("--downstream-setup-filter-fail-open")
    if args.micro_entry_research_no_trade:
        command.append("--micro-entry-research-no-trade")
    if args.disable_lifecycle_micro:
        command.append("--disable-lifecycle-micro")
    if args.dry_run:
        command.append("--dry-run")
    command.extend(passthrough)
    return command


def output_snapshot(output_dir: Path, symbol: str) -> dict[str, Any]:
    lifecycle_files = sorted(output_dir.glob(f"{symbol}-*-trade-lifecycle-summary.csv"))
    trade_files = sorted(path for path in output_dir.glob(f"{symbol}-*-trades.csv") if "lifecycle" not in path.name)
    order_files = sorted(output_dir.glob(f"{symbol}-*-orders.csv"))
    latest_lifecycle = latest_matching(lifecycle_files)
    return {
        "trade_csv_count": len(trade_files),
        "order_csv_count": len(order_files),
        "lifecycle_csv_count": len(lifecycle_files),
        "latest_lifecycle_csv": str(latest_lifecycle) if latest_lifecycle else "",
        "lifecycle_rows": count_csv_rows(latest_lifecycle) if latest_lifecycle else 0,
    }


def write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def log_parent(path: Path, message: str) -> None:
    line = f"[{utc_now()}] {message}"
    print(line, flush=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def run_logged(command: list[str], cwd: Path, log_path: Path, event_path: Path, event_name: str) -> int:
    log_parent(log_path, f"{event_name} command={shell_join(command)}")
    write_jsonl(event_path, {"ts_utc": utc_now(), "event": event_name, "command": command})
    start = time.monotonic()
    with log_path.open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"[{utc_now()}] {event_name} START command={shell_join(command)}\n")
        log_handle.flush()
        completed = subprocess.run(command, cwd=str(cwd), stdout=log_handle, stderr=subprocess.STDOUT, text=True)
        log_handle.write(f"[{utc_now()}] {event_name} END rc={completed.returncode} elapsed_seconds={time.monotonic() - start:.3f}\n")
    write_jsonl(event_path, {"ts_utc": utc_now(), "event": f"{event_name}_complete", "returncode": completed.returncode, "elapsed_seconds": round(time.monotonic() - start, 3)})
    return completed.returncode


def ensure_build(args: argparse.Namespace, repo_root: Path, classpath_file: Path, log_path: Path, event_path: Path, default_backtest_script: Path) -> None:
    classpath_file.parent.mkdir(parents=True, exist_ok=True)
    backtest_script = args.backtest_script or default_backtest_script
    if not backtest_script.is_absolute():
        backtest_script = repo_root / backtest_script
    using_default_child = backtest_script.resolve() == default_backtest_script.resolve()
    if args.skip_build:
        if using_default_child and not classpath_file.is_file():
            rc = run_logged([str(repo_root / "mvnw"), "-q", "dependency:build-classpath", f"-Dmdep.outputFile={classpath_file}"], repo_root, log_path, event_path, "classpath_build")
            if rc != 0:
                raise SystemExit(rc)
        else:
            log_parent(log_path, "skip_build=true; parent Maven package build skipped")
        return
    rc = run_logged([str(repo_root / "mvnw"), "-q", "-DskipTests", "package"], repo_root, log_path, event_path, "maven_package")
    if rc != 0:
        raise SystemExit(rc)
    rc = run_logged([str(repo_root / "mvnw"), "-q", "dependency:build-classpath", f"-Dmdep.outputFile={classpath_file}"], repo_root, log_path, event_path, "classpath_build")
    if rc != 0:
        raise SystemExit(rc)


def launch_run(run: SymbolRun, repo_root: Path, env: dict[str, str]) -> None:
    run.log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = run.log_path.open("w", encoding="utf-8")
    log_handle.write(f"[{utc_now()}] PARALLEL_SYMBOL_START symbol={run.symbol} command={shell_join(run.command)}\n")
    log_handle.flush()
    run.started_at_utc = utc_now()
    run.start_monotonic = time.monotonic()
    run.process = subprocess.Popen(run.command, cwd=str(repo_root), stdout=log_handle, stderr=subprocess.STDOUT, text=True, env=env)
    run.pid = run.process.pid
    run.status = "running"
    run.extra["_log_handle"] = log_handle


def finish_run(run: SymbolRun, returncode: int) -> None:
    run.returncode = returncode
    run.end_monotonic = time.monotonic()
    run.finished_at_utc = utc_now()
    run.status = "completed" if returncode == 0 else "failed"
    handle = run.extra.pop("_log_handle", None)
    if handle:
        handle.write(f"[{utc_now()}] PARALLEL_SYMBOL_END symbol={run.symbol} rc={returncode} elapsed_seconds={run.elapsed_seconds():.3f}\n")
        handle.close()


def monitor_payload(runs: list[SymbolRun], output_dir: Path, started_monotonic: float) -> dict[str, Any]:
    symbols = []
    for run in runs:
        snap = output_snapshot(output_dir, run.symbol)
        symbols.append({
            "symbol": run.symbol,
            "status": run.status,
            "pid": run.pid,
            "returncode": run.returncode,
            "elapsed_seconds": round(run.elapsed_seconds(), 3),
            "log_bytes": run.log_path.stat().st_size if run.log_path.exists() else 0,
            "last_log_line": tail_line(run.log_path),
            **snap,
        })
    return {
        "ts_utc": utc_now(),
        "event": "monitor",
        "elapsed_seconds": round(time.monotonic() - started_monotonic, 3),
        "requested": len(runs),
        "running": sum(1 for run in runs if run.status == "running"),
        "completed": sum(1 for run in runs if run.status == "completed"),
        "failed": sum(1 for run in runs if run.status == "failed"),
        "pending": sum(1 for run in runs if run.status == "pending"),
        "symbols": symbols,
    }


def write_summary(output_dir: Path, runs: list[SymbolRun], config: dict[str, Any], started_at_utc: str, finished_at_utc: str, combined_log: Path) -> dict[str, Any]:
    rows = []
    for run in runs:
        snap = output_snapshot(output_dir, run.symbol)
        rows.append({
            "symbol": run.symbol,
            "status": run.status,
            "returncode": run.returncode,
            "pid": run.pid,
            "launch_index": run.launch_index,
            "started_at_utc": run.started_at_utc,
            "finished_at_utc": run.finished_at_utc,
            "elapsed_seconds": round(run.elapsed_seconds(), 3),
            "log_file": str(run.log_path),
            **snap,
        })
    summary_csv = output_dir / "parallel_file_replay_summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "symbol", "status", "returncode", "pid", "launch_index", "started_at_utc", "finished_at_utc", "elapsed_seconds",
            "log_file", "trade_csv_count", "order_csv_count", "lifecycle_csv_count", "latest_lifecycle_csv", "lifecycle_rows",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    payload = {
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "requested": len(runs),
        "completed": sum(1 for run in runs if run.status == "completed"),
        "failed": sum(1 for run in runs if run.status == "failed"),
        "output_dir": str(output_dir),
        "combined_log": str(combined_log),
        "summary_csv": str(summary_csv),
        "config": config,
        "symbols": rows,
    }
    summary_json = output_dir / "parallel_file_replay_summary.json"
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    payload["summary_json"] = str(summary_json)
    return payload


def write_combined_log(output_dir: Path, runs: list[SymbolRun], completed: int, failed: int) -> Path:
    combined_log = output_dir / "controlled_java_replay.parallel_combined.log"
    with combined_log.open("w", encoding="utf-8", errors="ignore") as out:
        for run in runs:
            out.write(f"\n===== PARALLEL_SYMBOL_LOG symbol={run.symbol} status={run.status} rc={run.returncode} file={run.log_path} =====\n")
            if run.log_path.exists():
                with run.log_path.open("r", encoding="utf-8", errors="ignore") as handle:
                    for line in handle:
                        out.write(line)
        out.write(f"\n[BACKTEST] completed={completed} failed={failed} requested={len(runs)} output_dir={output_dir}\n")
    return combined_log


def run_summarizer(repo_root: Path, output_dir: Path, combined_log: Path, title: str, parent_log: Path, event_path: Path) -> int:
    command = [
        sys.executable,
        str(repo_root / "scripts/summarize_trade_lifecycle_periods.py"),
        "--output-dir", str(output_dir),
        "--log-file", str(combined_log),
        "--out-prefix", str(output_dir / "parallel_pnl_period_summary"),
        "--title", title,
    ]
    return run_logged(command, repo_root, parent_log, event_path, "pnl_summary")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args, passthrough = parser.parse_known_args(argv)
    if passthrough and passthrough[0] == "--":
        passthrough = passthrough[1:]

    repo_root = Path(__file__).resolve().parents[1]
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = (args.output_dir or repo_root / "runtime/backtests" / f"parallel-file-replay-{run_stamp}").expanduser()
    if not output_dir.is_absolute():
        output_dir = repo_root / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    parent_log = output_dir / "parallel_file_replay.log"
    event_path = output_dir / "parallel_file_replay_events.jsonl"

    recorded_paths = split_recorded_events(args.recorded_events)
    missing_recorded = [str(path) for path in recorded_paths if not (path if path.is_absolute() else repo_root / path).is_file()]
    if missing_recorded:
        raise SystemExit(f"Recorded event file(s) not found: {', '.join(missing_recorded)}")

    symbols = parse_symbols(args.symbols)
    if not symbols and args.symbols_file:
        symbols_file = args.symbols_file if args.symbols_file.is_absolute() else repo_root / args.symbols_file
        symbols = load_symbols_file(symbols_file)
    if not symbols:
        symbols = parse_symbols(DEFAULT_SYMBOLS)
    excludes = set(parse_symbols(args.exclude))
    symbols = [symbol for symbol in symbols if symbol not in excludes]
    if not symbols:
        raise SystemExit("No symbols selected after --symbols/--symbols-file/--exclude resolution.")

    jobs = args.jobs if args.jobs and args.jobs > 0 else len(symbols)
    jobs = max(1, min(jobs, len(symbols)))
    classpath_file = (args.classpath_file or output_dir / "parallel_databento_backtest_cp.txt").expanduser()
    if not classpath_file.is_absolute():
        classpath_file = repo_root / classpath_file
    default_backtest_script = repo_root / "scripts/run_databento_historical_ibkr_sim_backtest.sh"

    config = {
        "symbols": symbols,
        "jobs": jobs,
        "recorded_events": args.recorded_events,
        "start": args.start,
        "end": args.end,
        "trade_amount": args.trade_amount,
        "max_order_notional": args.max_order_notional,
        "max_trades": args.max_trades,
        "max_share_cap": args.max_share_cap,
        "monitor_interval_sec": args.monitor_interval_sec,
        "output_dir": str(output_dir),
        "classpath_file": str(classpath_file),
        "backtest_script": str(args.backtest_script or default_backtest_script),
        "passthrough": passthrough,
        "note": "Each symbol runs in its own Java simulated-broker process. This starts all symbols together and is closer to live paper timing than sequential replay, but it does not enforce a single shared simulated order book across symbols.",
    }
    parent_log.write_text("", encoding="utf-8")
    event_path.write_text("", encoding="utf-8")
    log_parent(parent_log, f"PARALLEL_FILE_REPLAY_START config={json.dumps(config, sort_keys=True)}")
    write_jsonl(event_path, {"ts_utc": utc_now(), "event": "run_start", "config": config})

    ensure_build(args, repo_root, classpath_file, parent_log, event_path, default_backtest_script)

    runs: list[SymbolRun] = []
    for index, symbol in enumerate(symbols, start=1):
        command = make_child_command(args, repo_root, output_dir, classpath_file, symbol, passthrough)
        runs.append(SymbolRun(symbol=symbol, command=command, log_path=logs_dir / f"{symbol}.log", launch_index=index))

    env = os.environ.copy()
    started_at_utc = utc_now()
    started_monotonic = time.monotonic()
    pending = list(runs)
    active: list[SymbolRun] = []
    last_monitor = 0.0
    interrupted = False

    try:
        while pending or active:
            while pending and len(active) < jobs:
                run = pending.pop(0)
                launch_run(run, repo_root, env)
                active.append(run)
                log_parent(parent_log, f"SYMBOL_LAUNCHED symbol={run.symbol} pid={run.pid} log={run.log_path}")
                write_jsonl(event_path, {"ts_utc": utc_now(), "event": "symbol_launched", "symbol": run.symbol, "pid": run.pid, "command": run.command, "log_file": str(run.log_path)})

            still_active: list[SymbolRun] = []
            for run in active:
                assert run.process is not None
                rc = run.process.poll()
                if rc is None:
                    still_active.append(run)
                    continue
                finish_run(run, rc)
                snap = output_snapshot(output_dir, run.symbol)
                log_parent(parent_log, f"SYMBOL_FINISHED symbol={run.symbol} rc={rc} status={run.status} elapsed_seconds={run.elapsed_seconds():.3f} lifecycle_rows={snap['lifecycle_rows']} log={run.log_path}")
                write_jsonl(event_path, {"ts_utc": utc_now(), "event": "symbol_finished", "symbol": run.symbol, "returncode": rc, "status": run.status, "elapsed_seconds": round(run.elapsed_seconds(), 3), **snap})
            active = still_active

            now = time.monotonic()
            if args.monitor_interval_sec >= 0 and (now - last_monitor >= max(0.1, args.monitor_interval_sec) or not active and not pending):
                payload = monitor_payload(runs, output_dir, started_monotonic)
                write_jsonl(event_path, payload)
                log_parent(parent_log, f"MONITOR running={payload['running']} completed={payload['completed']} failed={payload['failed']} pending={payload['pending']} elapsed_seconds={payload['elapsed_seconds']}")
                last_monitor = now

            if active or pending:
                time.sleep(0.5)
    except KeyboardInterrupt:
        interrupted = True
        log_parent(parent_log, "INTERRUPTED terminating active child processes")
        write_jsonl(event_path, {"ts_utc": utc_now(), "event": "interrupted"})
        for run in active:
            if run.process and run.process.poll() is None:
                run.process.terminate()
        time.sleep(2.0)
        for run in active:
            if run.process and run.process.poll() is None:
                run.process.kill()
        for run in active:
            if run.process:
                finish_run(run, run.process.wait())

    completed = sum(1 for run in runs if run.status == "completed")
    failed = sum(1 for run in runs if run.status != "completed")
    combined_log = write_combined_log(output_dir, runs, completed, failed)
    finished_at_utc = utc_now()
    summary = write_summary(output_dir, runs, config, started_at_utc, finished_at_utc, combined_log)
    write_jsonl(event_path, {"ts_utc": utc_now(), "event": "run_finished", "summary": summary})
    log_parent(parent_log, f"PARALLEL_FILE_REPLAY_FINISHED completed={completed} failed={failed} requested={len(runs)} combined_log={combined_log} summary_json={summary['summary_json']}")

    if args.run_summary:
        summary_rc = run_summarizer(repo_root, output_dir, combined_log, args.title, parent_log, event_path)
        if summary_rc != 0:
            log_parent(parent_log, f"PNL_SUMMARY_FAILED rc={summary_rc}")

    print(f"PARALLEL_FILE_REPLAY output_dir={output_dir}")
    print(f"PARALLEL_FILE_REPLAY completed={completed} failed={failed} requested={len(runs)}")
    print(f"PARALLEL_FILE_REPLAY summary_json={summary['summary_json']}")
    print(f"PARALLEL_FILE_REPLAY events_jsonl={event_path}")
    print(f"PARALLEL_FILE_REPLAY combined_log={combined_log}")
    if interrupted:
        return 130
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
