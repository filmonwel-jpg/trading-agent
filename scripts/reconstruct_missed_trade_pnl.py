#!/usr/bin/env python3
"""Reconstruct missed-trade P&L from per-symbol live process logs.

This script scans the cumulative `runtime/*_live_trade_logs_YYYY-MM-DD.txt` files,
filters only rows whose own timestamp falls on the requested market day, finds entry
 signals that passed + fired but never became a real order submit/fill, and pairs each
missed long/short attempt to the first later matching exit-model PASS. If no later exit
PASS exists, it falls back to the last later matching exit evaluation mark.

Outputs:
  * runtime/missed_trade_pnl_<day>_raw_attempts.csv
  * runtime/missed_trade_pnl_<day>_episodes.csv
  * runtime/missed_trade_pnl_<day>_per_symbol.csv
  * runtime/missed_trade_pnl_<day>_summary.json
  * runtime/missed_trade_pnl_<day>_report.md
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = REPO_ROOT / "runtime"
TIMESTAMP_PREFIX_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))")

ENTRY_PASS_RE = re.compile(
    r"\[FLOW\]\[COND\]\[AI\.(LONG|SHORT)\.ENTRY\]\s+AI_PREDICTS_ENTRY=PASS\s+\|\s+"
    r"symbol=([A-Z0-9._-]+).*?(askOrFallback|bidOrFallback)=([0-9.]+)\s+qty=(\d+)\s+prob=([0-9.]+)\s+threshold=([0-9.]+)"
)
FIRE_RE = re.compile(
    r"\[FLOW\]\[INFO\]\[AI\.(LONG|SHORT)\.ENTRY\]\s+(Dip buyer|Rip seller) firing order symbol=([A-Z0-9._-]+)"
)
ORDER_SEND_RE = re.compile(r"\[FLOW\]\[DATA\]\[ORDER\.SEND\]\s+")
STRATEGY_SUBMITTED_RE = re.compile(r"\[FLOW\]\[DATA\]\[STRATEGY\.ORDER\]\s+submitted\s+")
STRATEGY_FILL_RE = re.compile(r"\[FLOW\]\[ANALYZE\]\[STRATEGY\.ORDER\]\s+fill\s+")
SUBMIT_FAIL_RE = re.compile(r"\[ERROR\]\[IBKR\.GATEWAY\]\s+submit_order failed reason=(.*)")
SUBMIT_REJECT_RE = re.compile(r"\[ERROR\]\[IBKR\.GATEWAY\]\s+submit_order rejected detail=(.*)")
PLACEORDER_FAIL_RE = re.compile(r"\[ERROR\]\[ORDER\.SEND\]\s+placeOrder failed orderId=.* reason=(.*)")
ORDER_CLOSED_RE = re.compile(r"\[FLOW\]\[DATA\]\[STRATEGY\.ORDER\]\s+closed orderId=(-?\d+) status=([A-Za-z]+) symbol=([A-Z0-9._-]+)")
EXIT_EVAL_RE = re.compile(
    r"\[FLOW\]\[DATA\]\[AI\.(LONG|SHORT)\.EXIT\]\s+EVAL_RESULT\s+"
    r"symbol=([A-Z0-9._-]+)\s+action=(BUY|SELL)\s+prob=(\S+)\s+expectedThreshold=([0-9.]+)\s+"
    r"close=([0-9.]+)(?:\s+bid=([0-9.]+)\s+ask=([0-9.]+))?\s+execPx=([0-9.]+)\s+decision=(true|false)"
)
EOD_RE = re.compile(r"\[FLOW\]\[INFO\]\[STRATEGY\.EOD\]\s+Closing position size=(\d+) symbol=([A-Z0-9._-]+) price=([0-9.]+)")


@dataclass
class EntrySignal:
    seq: int
    ts: datetime
    side: str
    symbol: str
    entry_px: float
    qty: int
    prob: float
    threshold: float
    line: str


@dataclass
class ExitEval:
    seq: int
    ts: datetime
    side: str
    symbol: str
    action: str
    prob: float | None
    threshold: float
    close_px: float
    bid: float | None
    ask: float | None
    exec_px: float
    decision: bool
    line: str


@dataclass
class MissedAttempt:
    symbol: str
    side: str
    entry_signal: EntrySignal
    fire_seq: int
    fire_ts: datetime
    fire_line: str
    failure_lines: list[str] = field(default_factory=list)
    failure_reasons: list[str] = field(default_factory=list)
    close_seq: int | None = None
    close_ts: datetime | None = None
    close_line: str | None = None
    had_order_send: bool = False
    had_strategy_submit: bool = False
    had_fill: bool = False
    paired_exit: ExitEval | None = None
    exit_source: str | None = None
    pnl: float | None = None

    @property
    def qty(self) -> int:
        return self.entry_signal.qty

    @property
    def entry_px(self) -> float:
        return self.entry_signal.entry_px

    @property
    def entry_prob(self) -> float:
        return self.entry_signal.prob

    @property
    def entry_threshold(self) -> float:
        return self.entry_signal.threshold

    @property
    def end_seq(self) -> int:
        return self.close_seq or self.fire_seq

    @property
    def end_ts(self) -> datetime:
        return self.close_ts or self.fire_ts

    @property
    def is_failed_without_submit(self) -> bool:
        has_failure_evidence = bool(self.failure_lines or self.close_line)
        return has_failure_evidence and not self.had_order_send and not self.had_strategy_submit and not self.had_fill


@dataclass
class SymbolParseResult:
    symbol: str
    attempts: list[MissedAttempt] = field(default_factory=list)
    exit_evals: list[ExitEval] = field(default_factory=list)
    eod_marks: list[tuple[int, datetime, float, str]] = field(default_factory=list)
    order_send_count: int = 0
    strategy_submit_count: int = 0
    fill_count: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct missed-trade P&L from live process logs.")
    parser.add_argument("--day", default="", help="Market day to analyze, e.g. 2026-04-23. Defaults to yesterday by local date.")
    parser.add_argument(
        "--glob",
        default="runtime/*_live_trade_logs_2026-04-23.txt",
        help="Glob for cumulative per-symbol live logs. Defaults to the currently investigated day-specific files.",
    )
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol filter.")
    return parser.parse_args()


def default_day() -> date:
    today = datetime.now().astimezone().date()
    return date.fromordinal(today.toordinal() - 1)


def resolve_day(raw: str) -> date:
    text = str(raw or "").strip()
    return date.fromisoformat(text) if text else default_day()


def normalize_symbols(raw: str) -> set[str]:
    out: set[str] = set()
    for token in str(raw or "").split(","):
        cleaned = token.strip().upper()
        if cleaned:
            out.add(cleaned)
    return out


def parse_ts(line: str) -> datetime | None:
    match = TIMESTAMP_PREFIX_RE.match(line)
    if not match:
        return None
    try:
        return datetime.fromisoformat(match.group("ts").replace("Z", "+00:00"))
    except ValueError:
        return None


def symbol_from_path(path: Path) -> str:
    name = path.name
    marker = "_live_trade_logs_"
    index = name.lower().find(marker)
    if index <= 0:
        raise ValueError(f"Unsupported live-log filename: {name}")
    return name[:index].upper()


def parse_symbol_log(path: Path, target_day: date) -> SymbolParseResult:
    symbol = symbol_from_path(path)
    result = SymbolParseResult(symbol=symbol)
    pending_entry_by_side: dict[str, EntrySignal] = {}
    active_attempt: MissedAttempt | None = None
    seq = 0

    for raw_line in iter_day_lines(path, target_day):
            ts = parse_ts(raw_line)
            if ts is None:
                continue
            seq += 1
            line = raw_line.rstrip("\n")

            if match := ENTRY_PASS_RE.search(line):
                side = match.group(1)
                pending_entry_by_side[side] = EntrySignal(
                    seq=seq,
                    ts=ts,
                    side=side,
                    symbol=match.group(2),
                    entry_px=float(match.group(4)),
                    qty=int(match.group(5)),
                    prob=float(match.group(6)),
                    threshold=float(match.group(7)),
                    line=line,
                )

            if match := FIRE_RE.search(line):
                side = match.group(1)
                signal = pending_entry_by_side.get(side)
                if signal and signal.symbol == symbol and signal.seq <= seq:
                    if active_attempt and active_attempt.is_failed_without_submit:
                        result.attempts.append(active_attempt)
                    active_attempt = MissedAttempt(
                        symbol=symbol,
                        side=side,
                        entry_signal=signal,
                        fire_seq=seq,
                        fire_ts=ts,
                        fire_line=line,
                    )

            if match := EXIT_EVAL_RE.search(line):
                prob_raw = match.group(4)
                result.exit_evals.append(
                    ExitEval(
                        seq=seq,
                        ts=ts,
                        side=match.group(1),
                        symbol=match.group(2),
                        action=match.group(3),
                        prob=None if prob_raw == "NA" else float(prob_raw),
                        threshold=float(match.group(5)),
                        close_px=float(match.group(6)),
                        bid=float(match.group(7)) if match.group(7) else None,
                        ask=float(match.group(8)) if match.group(8) else None,
                        exec_px=float(match.group(9)),
                        decision=match.group(10) == "true",
                        line=line,
                    )
                )

            if match := EOD_RE.search(line):
                result.eod_marks.append((seq, ts, float(match.group(3)), line))

            if ORDER_SEND_RE.search(line):
                result.order_send_count += 1
                if active_attempt is not None:
                    active_attempt.had_order_send = True
            if STRATEGY_SUBMITTED_RE.search(line):
                result.strategy_submit_count += 1
                if active_attempt is not None:
                    active_attempt.had_strategy_submit = True
            if STRATEGY_FILL_RE.search(line):
                result.fill_count += 1
                if active_attempt is not None:
                    active_attempt.had_fill = True

            if active_attempt is not None:
                if match := SUBMIT_FAIL_RE.search(line):
                    active_attempt.failure_lines.append(line)
                    active_attempt.failure_reasons.append(match.group(1).strip())
                if match := SUBMIT_REJECT_RE.search(line):
                    active_attempt.failure_lines.append(line)
                    active_attempt.failure_reasons.append(match.group(1).strip())
                if match := PLACEORDER_FAIL_RE.search(line):
                    active_attempt.failure_lines.append(line)
                    active_attempt.failure_reasons.append(match.group(1).strip())
                if match := ORDER_CLOSED_RE.search(line):
                    status = match.group(2)
                    if match.group(3) == symbol and status == "Cancelled":
                        active_attempt.close_seq = seq
                        active_attempt.close_ts = ts
                        active_attempt.close_line = line
                        if active_attempt.is_failed_without_submit:
                            result.attempts.append(active_attempt)
                        active_attempt = None

    if active_attempt is not None and active_attempt.is_failed_without_submit:
        result.attempts.append(active_attempt)
    return result


def iter_day_lines(path: Path, target_day: date):
    day_prefix = f"^{target_day.isoformat()}T"
    rg_path = shutil.which("rg")
    if rg_path:
        try:
            process = subprocess.Popen(
                [rg_path, "--no-messages", day_prefix, str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            assert process.stdout is not None
            try:
                for line in process.stdout:
                    yield line
            finally:
                process.stdout.close()
                process.wait()
            if process.returncode in (0, 1):
                return
        except Exception:
            pass

    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            if raw_line.startswith(target_day.isoformat() + "T"):
                yield raw_line


def pair_attempts(result: SymbolParseResult) -> None:
    exit_by_side: dict[str, list[ExitEval]] = {"LONG": [], "SHORT": []}
    for exit_eval in result.exit_evals:
        exit_by_side[exit_eval.side].append(exit_eval)

    for attempt in result.attempts:
        candidates = [event for event in exit_by_side[attempt.side] if event.seq > attempt.end_seq]
        chosen = next((event for event in candidates if event.decision), None)
        exit_source = "first_exit_pass"
        if chosen is None and candidates:
            chosen = candidates[-1]
            exit_source = "last_exit_eval_mark"
        if chosen is None and result.eod_marks:
            later_eod = [mark for mark in result.eod_marks if mark[0] > attempt.end_seq]
            if later_eod:
                seq, ts, price, line = later_eod[-1]
                action = "SELL" if attempt.side == "LONG" else "BUY"
                chosen = ExitEval(
                    seq=seq,
                    ts=ts,
                    side=attempt.side,
                    symbol=attempt.symbol,
                    action=action,
                    prob=None,
                    threshold=0.0,
                    close_px=price,
                    bid=None,
                    ask=None,
                    exec_px=price,
                    decision=False,
                    line=line,
                )
                exit_source = "eod_mark"
        if chosen is None:
            continue
        attempt.paired_exit = chosen
        attempt.exit_source = exit_source
        if attempt.side == "LONG":
            attempt.pnl = round((chosen.exec_px - attempt.entry_px) * attempt.qty, 2)
        else:
            attempt.pnl = round((attempt.entry_px - chosen.exec_px) * attempt.qty, 2)


def build_episode_rows(attempts: list[MissedAttempt]) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    by_symbol_side: dict[tuple[str, str], list[MissedAttempt]] = {}
    for attempt in attempts:
        by_symbol_side.setdefault((attempt.symbol, attempt.side), []).append(attempt)

    for (symbol, side), side_attempts in sorted(by_symbol_side.items()):
        ordered = sorted(side_attempts, key=lambda item: (item.fire_seq, item.fire_ts))
        idx = 0
        episode_number = 0
        while idx < len(ordered):
            episode_number += 1
            first = ordered[idx]
            exit_seq = first.paired_exit.seq if first.paired_exit is not None else 10**18
            j = idx + 1
            while j < len(ordered) and ordered[j].fire_seq < exit_seq:
                j += 1
            chunk = ordered[idx:j]
            pnls = [item.pnl for item in chunk if item.pnl is not None]
            grouped.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "episode_id": f"{symbol}_{side}_{episode_number:03d}",
                    "attempts_in_episode": len(chunk),
                    "entry_ts": first.fire_ts.isoformat(),
                    "qty": first.qty,
                    "entry_px": round(first.entry_px, 4),
                    "entry_prob": round(first.entry_prob, 4),
                    "entry_threshold": round(first.entry_threshold, 4),
                    "exit_ts": first.paired_exit.ts.isoformat() if first.paired_exit else "",
                    "exit_px": round(first.paired_exit.exec_px, 4) if first.paired_exit else "",
                    "exit_source": first.exit_source or "",
                    "pnl": first.pnl if first.pnl is not None else "",
                    "raw_attempt_pnl_sum": round(sum(pnls), 2) if pnls else "",
                    "best_raw_attempt_pnl": round(max(pnls), 2) if pnls else "",
                    "worst_raw_attempt_pnl": round(min(pnls), 2) if pnls else "",
                    "failure_reasons": " | ".join(dict.fromkeys(reason for item in chunk for reason in item.failure_reasons)),
                    "entry_line": first.entry_signal.line,
                    "fire_line": first.fire_line,
                    "exit_line": first.paired_exit.line if first.paired_exit else "",
                }
            )
            idx = j
    return grouped


def maybe_query_trade_db(target_day: date, symbols: set[str]) -> dict[str, int]:
    try:
        import psycopg  # type: ignore
    except Exception:
        return {}

    try:
        conn = psycopg.connect(
            os.getenv("TRADING_DB_URL", "postgresql://trading_agent_databento@127.0.0.1:5432/trading_agent"),
            connect_timeout=3,
        )
    except Exception:
        return {}

    sql = """
        SELECT symbol, COUNT(*)
        FROM databento_live_trade_logs
        WHERE market_day = %s
          AND symbol = ANY(%s)
        GROUP BY symbol
    """
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (target_day, sorted(symbols)))
            return {str(symbol).upper(): int(count) for symbol, count in cur.fetchall()}
    except Exception:
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_report_markdown(day_text: str, summary: dict[str, Any], per_symbol_rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]]) -> str:
    top_gain = sorted((row for row in raw_rows if isinstance(row["pnl"], (int, float)) and row["pnl"] > 0), key=lambda row: row["pnl"], reverse=True)[:15]
    top_loss = sorted((row for row in raw_rows if isinstance(row["pnl"], (int, float)) and row["pnl"] < 0), key=lambda row: row["pnl"])[:15]

    def table(rows: list[dict[str, Any]], headers: list[str]) -> str:
        if not rows:
            return "_none_"
        lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(header, "")) for header in headers) + " |")
        return "\n".join(lines)

    return "\n".join(
        [
            f"# Missed-trade P&L reconstruction for {day_text}",
            "",
            "## Summary",
            "",
            f"- Symbols scanned: {summary['symbols_scanned']}",
            f"- Raw missed attempts: {summary['raw_missed_attempts']}",
            f"- Canonical episodes: {summary['canonical_episodes']}",
            f"- Attempts with matched/marked exits: {summary['paired_attempts']}",
            f"- Raw gross gains: {summary['raw_gross_gain']}",
            f"- Raw gross losses: {summary['raw_gross_loss']}",
            f"- Raw net missed P&L: {summary['raw_net_pnl']}",
            f"- Canonical gross gains: {summary['canonical_gross_gain']}",
            f"- Canonical gross losses: {summary['canonical_gross_loss']}",
            f"- Canonical net missed P&L: {summary['canonical_net_pnl']}",
            f"- Winning raw attempts: {summary['winning_attempts']}",
            f"- Losing raw attempts: {summary['losing_attempts']}",
            f"- Breakeven raw attempts: {summary['breakeven_attempts']}",
            f"- Winning canonical episodes: {summary['canonical_winning_episodes']}",
            f"- Losing canonical episodes: {summary['canonical_losing_episodes']}",
            f"- Breakeven canonical episodes: {summary['canonical_breakeven_episodes']}",
            f"- Process-log ORDER.SEND rows seen: {summary['process_log_order_send_count']}",
            f"- Process-log STRATEGY.ORDER submitted rows seen: {summary['process_log_strategy_submit_count']}",
            f"- Process-log STRATEGY.ORDER fill rows seen: {summary['process_log_fill_count']}",
            f"- DB fills on day (cross-check): {summary['db_fill_rows_on_day']}",
            "",
            "## Per-symbol totals",
            "",
            table(per_symbol_rows[:25], ["symbol", "raw_attempts", "canonical_episodes", "raw_net_pnl", "canonical_net_pnl", "raw_winning_attempts", "raw_losing_attempts", "canonical_winning_episodes", "canonical_losing_episodes", "db_fill_rows"]),
            "",
            "## Top missed gains",
            "",
            table(top_gain, ["symbol", "side", "entry_ts", "qty", "entry_px", "exit_ts", "exit_px", "exit_source", "pnl"]),
            "",
            "## Top missed losses",
            "",
            table(top_loss, ["symbol", "side", "entry_ts", "qty", "entry_px", "exit_ts", "exit_px", "exit_source", "pnl"]),
            "",
            "## Notes",
            "",
            "- Raw attempts count every failed fire/cancel cycle independently.",
            "- Canonical episodes dedupe same-symbol same-side retries until the first later matching exit mark and use the first missed entry in that retry cluster as the episode entry.",
            "- Exit pricing uses the exit-model `EVAL_RESULT ... execPx=` when available; otherwise it falls back to the last later matching exit evaluation mark or EOD mark.",
            "- Real fills were cross-checked against both process logs and `databento_live_trade_logs`.",
        ]
    )


def main() -> int:
    args = parse_args()
    target_day = resolve_day(args.day)
    day_text = target_day.isoformat()
    symbol_filter = normalize_symbols(args.symbols)
    paths = sorted(REPO_ROOT.glob(args.glob))
    if symbol_filter:
        paths = [path for path in paths if symbol_from_path(path) in symbol_filter]
    if not paths:
        print(f"No files matched glob {args.glob!r}", file=sys.stderr)
        return 1

    parsed: list[SymbolParseResult] = []
    for path in paths:
        parsed_result = parse_symbol_log(path, target_day)
        pair_attempts(parsed_result)
        parsed.append(parsed_result)

    all_attempts = [attempt for result in parsed for attempt in result.attempts]
    all_attempts.sort(key=lambda item: (item.symbol, item.fire_ts, item.fire_seq))
    episode_rows = build_episode_rows(all_attempts)
    canonical_episode_pnls = [row["pnl"] for row in episode_rows if isinstance(row.get("pnl"), (int, float))]
    symbols = {result.symbol for result in parsed}
    db_fill_counts = maybe_query_trade_db(target_day, symbols)

    raw_rows: list[dict[str, Any]] = []
    for attempt in all_attempts:
        raw_rows.append(
            {
                "symbol": attempt.symbol,
                "side": attempt.side,
                "entry_ts": attempt.fire_ts.isoformat(),
                "qty": attempt.qty,
                "entry_px": round(attempt.entry_px, 4),
                "entry_prob": round(attempt.entry_prob, 4),
                "entry_threshold": round(attempt.entry_threshold, 4),
                "failure_ts": attempt.end_ts.isoformat(),
                "failure_reasons": " | ".join(dict.fromkeys(attempt.failure_reasons)),
                "exit_ts": attempt.paired_exit.ts.isoformat() if attempt.paired_exit else "",
                "exit_px": round(attempt.paired_exit.exec_px, 4) if attempt.paired_exit else "",
                "exit_source": attempt.exit_source or "",
                "pnl": attempt.pnl if attempt.pnl is not None else "",
                "entry_line": attempt.entry_signal.line,
                "fire_line": attempt.fire_line,
                "failure_line": attempt.failure_lines[0] if attempt.failure_lines else "",
                "close_line": attempt.close_line or "",
                "exit_line": attempt.paired_exit.line if attempt.paired_exit else "",
            }
        )

    per_symbol_rows: list[dict[str, Any]] = []
    attempts_by_symbol: dict[str, list[MissedAttempt]] = {}
    for attempt in all_attempts:
        attempts_by_symbol.setdefault(attempt.symbol, []).append(attempt)
    episode_count_by_symbol: dict[str, int] = {}
    episode_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in episode_rows:
        episode_count_by_symbol[row["symbol"]] = episode_count_by_symbol.get(row["symbol"], 0) + 1
        episode_rows_by_symbol.setdefault(row["symbol"], []).append(row)

    for symbol in sorted(symbols):
        symbol_attempts = attempts_by_symbol.get(symbol, [])
        pnls = [attempt.pnl for attempt in symbol_attempts if attempt.pnl is not None]
        episode_symbol_rows = episode_rows_by_symbol.get(symbol, [])
        canonical_pnls = [row["pnl"] for row in episode_symbol_rows if isinstance(row.get("pnl"), (int, float))]
        raw_gross_gain = round(sum(pnl for pnl in pnls if pnl > 0), 2)
        raw_gross_loss = round(sum(pnl for pnl in pnls if pnl < 0), 2)
        canonical_gross_gain = round(sum(pnl for pnl in canonical_pnls if pnl > 0), 2)
        canonical_gross_loss = round(sum(pnl for pnl in canonical_pnls if pnl < 0), 2)
        per_symbol_rows.append(
            {
                "symbol": symbol,
                "raw_attempts": len(symbol_attempts),
                "canonical_episodes": episode_count_by_symbol.get(symbol, 0),
                "raw_gross_gain": raw_gross_gain,
                "raw_gross_loss": raw_gross_loss,
                "raw_net_pnl": round(raw_gross_gain + raw_gross_loss, 2),
                "canonical_gross_gain": canonical_gross_gain,
                "canonical_gross_loss": canonical_gross_loss,
                "canonical_net_pnl": round(canonical_gross_gain + canonical_gross_loss, 2),
                "raw_winning_attempts": sum(1 for pnl in pnls if pnl > 0),
                "raw_losing_attempts": sum(1 for pnl in pnls if pnl < 0),
                "canonical_winning_episodes": sum(1 for pnl in canonical_pnls if pnl > 0),
                "canonical_losing_episodes": sum(1 for pnl in canonical_pnls if pnl < 0),
                "db_fill_rows": db_fill_counts.get(symbol, 0),
            }
        )
    per_symbol_rows.sort(key=lambda row: row["canonical_net_pnl"], reverse=True)

    paired_pnls = [attempt.pnl for attempt in all_attempts if attempt.pnl is not None]
    summary = {
        "day": day_text,
        "symbols_scanned": len(parsed),
        "raw_missed_attempts": len(all_attempts),
        "canonical_episodes": len(episode_rows),
        "paired_attempts": len(paired_pnls),
        "unpaired_attempts": len(all_attempts) - len(paired_pnls),
        "raw_gross_gain": round(sum(pnl for pnl in paired_pnls if pnl > 0), 2),
        "raw_gross_loss": round(sum(pnl for pnl in paired_pnls if pnl < 0), 2),
        "raw_net_pnl": round(sum(paired_pnls), 2),
        "canonical_gross_gain": round(sum(pnl for pnl in canonical_episode_pnls if pnl > 0), 2),
        "canonical_gross_loss": round(sum(pnl for pnl in canonical_episode_pnls if pnl < 0), 2),
        "canonical_net_pnl": round(sum(canonical_episode_pnls), 2),
        "winning_attempts": sum(1 for pnl in paired_pnls if pnl > 0),
        "losing_attempts": sum(1 for pnl in paired_pnls if pnl < 0),
        "breakeven_attempts": sum(1 for pnl in paired_pnls if pnl == 0),
        "canonical_winning_episodes": sum(1 for pnl in canonical_episode_pnls if pnl > 0),
        "canonical_losing_episodes": sum(1 for pnl in canonical_episode_pnls if pnl < 0),
        "canonical_breakeven_episodes": sum(1 for pnl in canonical_episode_pnls if pnl == 0),
        "process_log_order_send_count": sum(result.order_send_count for result in parsed),
        "process_log_strategy_submit_count": sum(result.strategy_submit_count for result in parsed),
        "process_log_fill_count": sum(result.fill_count for result in parsed),
        "db_fill_rows_on_day": sum(db_fill_counts.values()),
        "symbols_with_db_fills": sorted(symbol for symbol, count in db_fill_counts.items() if count > 0),
    }

    base = RUNTIME_DIR / f"missed_trade_pnl_{day_text}"
    raw_csv = base.with_name(base.name + "_raw_attempts.csv")
    episode_csv = base.with_name(base.name + "_episodes.csv")
    per_symbol_csv = base.with_name(base.name + "_per_symbol.csv")
    summary_json = base.with_name(base.name + "_summary.json")
    report_md = base.with_name(base.name + "_report.md")

    write_csv(raw_csv, raw_rows)
    write_csv(episode_csv, episode_rows)
    write_csv(per_symbol_csv, per_symbol_rows)
    summary_json.write_text(json.dumps({"summary": summary, "per_symbol": per_symbol_rows, "episodes": episode_rows[:200], "raw_attempts": raw_rows[:200]}, indent=2), encoding="utf-8")
    report_md.write_text(build_report_markdown(day_text, summary, per_symbol_rows, raw_rows), encoding="utf-8")

    print(json.dumps({
        "summary": summary,
        "outputs": {
            "raw_attempts_csv": str(raw_csv),
            "episodes_csv": str(episode_csv),
            "per_symbol_csv": str(per_symbol_csv),
            "summary_json": str(summary_json),
            "report_md": str(report_md),
        },
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



