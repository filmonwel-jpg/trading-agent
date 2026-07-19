#!/usr/bin/env python3
"""Generate setup-to-micro counterfactual profitability reports from replay logs.

The report joins three evidence streams:

1. setup/arbitration/armed-entry lines from a Databento historical replay log,
2. 5-second micro-entry probability evaluations observed while each arm is live,
3. recorded equity bars used by the replay.

It emits per-arm, per-micro-evaluation, and per-threshold counterfactual outputs plus
``setup_downstream_confirmable_labels_v1.csv`` for downstream-aware setup retraining.
The script is research tooling only; generated labels are candidate evidence, not a
promotion signal by themselves.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

LABEL_SCHEMA_VERSION = "setup_downstream_confirmable_labels_v1"
DEFAULT_MICRO_THRESHOLD_GRID = (
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
    0.45,
    0.50,
    0.52,
    0.55,
    0.57,
    0.58,
    0.60,
    0.62,
    0.64,
)

SETUP_RE = re.compile(
    r"AI\.(LONG|SHORT)\.ENTRY\] AI_PREDICTS_ENTRY=(PASS|FAIL) \| symbol=([A-Z0-9_.-]+).*?\bprob=([-+0-9.Ee]+)\s+threshold=([-+0-9.Ee]+)"
)
ARBITRATION_RE = re.compile(
    r"AI\.ENTRY\.ARBITRATION\] ENTRY_SIDE_SELECTED=(PASS|FAIL) \| symbol=([A-Z0-9_.-]+)"
)
ARM_RE = re.compile(
    r"Armed (long|short) micro-entry symbol=([A-Z0-9_.-]+) epoch=(\d+) ttlSeconds=(\d+) setupProb=([-+0-9.Ee]+)"
)
MICRO_RE = re.compile(
    r"AI\.MICRO\.(LONG|SHORT)\.ENTRY\] MICRO_ENTRY_CONFIRMS=(PASS|FAIL) \| symbol=([A-Z0-9_.-]+)\s+prob=([-+0-9.Ee]+)\s+threshold=([-+0-9.Ee]+)\s+secondsSinceArm=(\d+)"
)
KEY_VALUE_RE = re.compile(r"([A-Za-z0-9_.-]+)=([^\s]+)")


@dataclass
class SetupEval:
    symbol: str
    side: str
    result: str
    prob: float
    threshold: float
    margin: float
    qty: int | None
    rsi: float | None
    price: float | None
    price_field: str
    line_no: int


@dataclass
class ArbitrationEval:
    symbol: str
    result: str
    selected: str
    reason: str
    fields: dict[str, Any]
    line_no: int


@dataclass
class Arm:
    arm_id: str
    symbol: str
    side: str
    arm_epoch: int
    ttl_seconds: int
    setup_prob: float
    setup_threshold: float | None
    setup_margin: float | None
    setup_qty: int | None
    setup_rsi: float | None
    setup_price: float | None
    setup_price_field: str
    arbitration_reason: str
    arbitration_fields: dict[str, Any]
    line_no: int
    micro_eval_indices: list[int] = field(default_factory=list)


@dataclass
class MicroEval:
    eval_id: str
    arm_id: str
    symbol: str
    side: str
    arm_epoch: int
    eval_epoch: int
    seconds_since_arm: int
    within_ttl: bool
    prob: float
    threshold: float
    margin: float
    result: str
    line_no: int
    outcome: dict[str, Any] = field(default_factory=dict)


@dataclass
class Bar:
    epoch: int
    open: float
    high: float
    low: float
    close: float
    bid: float | None
    ask: float | None


@dataclass
class BarSeries:
    symbol: str
    bars: list[Bar]
    epochs: list[int]


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def epoch_utc(epoch: int | float | None) -> str:
    if epoch is None:
        return ""
    try:
        if not math.isfinite(float(epoch)):
            return ""
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def open_text(path: Path):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("rt", encoding="utf-8", errors="replace")


def parse_float(value: Any, default: float = math.nan) -> float:
    if value is None:
        return default
    try:
        out = float(str(value).rstrip(","))
    except Exception:
        return default
    return out if math.isfinite(out) else default


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).rstrip(",")))
    except Exception:
        return None


def finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except Exception:
        return False


def clean_optional_float(value: Any) -> float | None:
    return float(value) if finite(value) else None


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return value if math.isfinite(value) else ""
    if isinstance(value, bool):
        return int(value)
    return value


def parse_key_values(line: str) -> dict[str, str]:
    tail = line.split(" | ", 1)[1] if " | " in line else line
    return {match.group(1): match.group(2).rstrip(",") for match in KEY_VALUE_RE.finditer(tail)}


def parse_thresholds(raw: str) -> list[float]:
    values: list[float] = []
    for piece in raw.replace(",", " ").split():
        value = parse_float(piece)
        if finite(value):
            values.append(round(float(value), 6))
    if not values:
        raise ValueError("threshold list is empty after parsing")
    return sorted(set(values))


def quantile(values: list[float], q: float) -> float | None:
    finite_values = sorted(float(v) for v in values if finite(v))
    if not finite_values:
        return None
    if len(finite_values) == 1:
        return finite_values[0]
    q = min(max(q, 0.0), 1.0)
    pos = (len(finite_values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return finite_values[lo]
    weight = pos - lo
    return finite_values[lo] * (1.0 - weight) + finite_values[hi] * weight


def mean(values: Iterable[float]) -> float | None:
    finite_values = [float(v) for v in values if finite(v)]
    if not finite_values:
        return None
    return sum(finite_values) / len(finite_values)


def side_key(symbol: str, side: str) -> str:
    return f"{symbol}.{side}"


def parse_replay_log(log_path: Path, *, max_arms: int = 0) -> tuple[list[Arm], list[MicroEval], dict[str, Any]]:
    arms: list[Arm] = []
    micro_evals: list[MicroEval] = []
    last_setup: dict[tuple[str, str], SetupEval] = {}
    last_arbitration: dict[str, ArbitrationEval] = {}
    active_arm_by_symbol_side: dict[tuple[str, str], str] = {}
    arm_index_by_id: dict[str, int] = {}
    counts: Counter[str] = Counter()
    unmatched_micro_examples: list[dict[str, Any]] = []

    with open_text(log_path) as handle:
        for line_no, line in enumerate(handle, start=1):
            if "AI_PREDICTS_ENTRY=" in line:
                match = SETUP_RE.search(line)
                if not match:
                    counts["setup_unparsed"] += 1
                    continue
                kv = parse_key_values(line)
                side = match.group(1).lower()
                result = match.group(2)
                symbol = match.group(3).upper()
                prob = parse_float(kv.get("prob", match.group(4)))
                threshold = parse_float(kv.get("threshold", match.group(5)))
                price_field = "askOrFallback" if side == "long" else "bidOrFallback"
                price = parse_float(kv.get(price_field))
                setup = SetupEval(
                    symbol=symbol,
                    side=side,
                    result=result,
                    prob=prob,
                    threshold=threshold,
                    margin=prob - threshold if finite(prob) and finite(threshold) else math.nan,
                    qty=parse_int(kv.get("qty")),
                    rsi=clean_optional_float(parse_float(kv.get("rsi"))),
                    price=clean_optional_float(price),
                    price_field=price_field if price_field in kv else "",
                    line_no=line_no,
                )
                last_setup[(symbol, side)] = setup
                counts[f"setup_{side}_{result.lower()}"] += 1
                continue

            if "ENTRY_SIDE_SELECTED=" in line:
                match = ARBITRATION_RE.search(line)
                if not match:
                    counts["arbitration_unparsed"] += 1
                    continue
                kv = parse_key_values(line)
                result = match.group(1)
                symbol = match.group(2).upper()
                selected = kv.get("selected", "").lower()
                arbitration = ArbitrationEval(
                    symbol=symbol,
                    result=result,
                    selected=selected,
                    reason=kv.get("reason", ""),
                    fields={key: parse_float(value) if key.lower().endswith(("prob", "threshold", "margin", "difference", "advantage")) else value for key, value in kv.items()},
                    line_no=line_no,
                )
                last_arbitration[symbol] = arbitration
                counts[f"arbitration_{result.lower()}"] += 1
                if selected:
                    counts[f"arbitration_selected_{selected}"] += 1
                continue

            if "Armed " in line and " micro-entry" in line:
                match = ARM_RE.search(line)
                if not match:
                    counts["arm_unparsed"] += 1
                    continue
                side = match.group(1).lower()
                symbol = match.group(2).upper()
                arm_epoch = int(match.group(3))
                ttl_seconds = int(match.group(4))
                setup_prob = parse_float(match.group(5))
                setup = last_setup.get((symbol, side))
                arbitration = last_arbitration.get(symbol)
                if arbitration and arbitration.selected and arbitration.selected != side:
                    arbitration = None
                arm_id = f"{symbol}-{side}-{arm_epoch}-{len(arms) + 1}"
                arm = Arm(
                    arm_id=arm_id,
                    symbol=symbol,
                    side=side,
                    arm_epoch=arm_epoch,
                    ttl_seconds=ttl_seconds,
                    setup_prob=setup_prob,
                    setup_threshold=setup.threshold if setup else None,
                    setup_margin=(setup_prob - setup.threshold) if setup and finite(setup.threshold) and finite(setup_prob) else (setup.margin if setup else None),
                    setup_qty=setup.qty if setup else None,
                    setup_rsi=setup.rsi if setup else None,
                    setup_price=setup.price if setup else None,
                    setup_price_field=setup.price_field if setup else "",
                    arbitration_reason=arbitration.reason if arbitration else "",
                    arbitration_fields=arbitration.fields if arbitration else {},
                    line_no=line_no,
                )
                arms.append(arm)
                arm_index_by_id[arm_id] = len(arms) - 1
                active_arm_by_symbol_side[(symbol, side)] = arm_id
                counts["arms_total"] += 1
                counts[f"arms_{side}"] += 1
                counts[f"arms_{symbol}_{side}"] += 1
                if max_arms and len(arms) >= max_arms:
                    counts["stopped_at_max_arms"] = max_arms
                    break
                continue

            if "MICRO_ENTRY_CONFIRMS=" in line:
                match = MICRO_RE.search(line)
                if not match:
                    counts["micro_unparsed"] += 1
                    continue
                side = match.group(1).lower()
                result = match.group(2)
                symbol = match.group(3).upper()
                prob = parse_float(match.group(4))
                threshold = parse_float(match.group(5))
                seconds_since_arm = int(match.group(6))
                active_id = active_arm_by_symbol_side.get((symbol, side))
                if not active_id:
                    counts["micro_unmatched_no_active_arm"] += 1
                    if len(unmatched_micro_examples) < 10:
                        unmatched_micro_examples.append({"line_no": line_no, "symbol": symbol, "side": side, "seconds_since_arm": seconds_since_arm})
                    continue
                arm = arms[arm_index_by_id[active_id]]
                eval_epoch = arm.arm_epoch + seconds_since_arm
                within_ttl = 0 <= seconds_since_arm <= arm.ttl_seconds
                eval_id = f"{active_id}-micro-{seconds_since_arm}-{len(arm.micro_eval_indices) + 1}"
                micro = MicroEval(
                    eval_id=eval_id,
                    arm_id=active_id,
                    symbol=symbol,
                    side=side,
                    arm_epoch=arm.arm_epoch,
                    eval_epoch=eval_epoch,
                    seconds_since_arm=seconds_since_arm,
                    within_ttl=within_ttl,
                    prob=prob,
                    threshold=threshold,
                    margin=prob - threshold if finite(prob) and finite(threshold) else math.nan,
                    result=result,
                    line_no=line_no,
                )
                micro_evals.append(micro)
                arm.micro_eval_indices.append(len(micro_evals) - 1)
                counts["micro_total"] += 1
                counts[f"micro_{result.lower()}"] += 1
                counts[f"micro_{symbol}_{side}"] += 1
                if within_ttl:
                    counts["micro_within_ttl"] += 1
                else:
                    counts["micro_outside_ttl"] += 1
                continue

    summary = {
        "log_path": str(log_path),
        "counts": dict(counts),
        "arms": len(arms),
        "micro_evals": len(micro_evals),
        "unmatched_micro_examples": unmatched_micro_examples,
    }
    return arms, micro_evals, summary


def load_equity_bars(paths: list[Path], wanted_symbols: set[str] | None = None) -> tuple[dict[str, BarSeries], dict[str, Any]]:
    by_symbol_epoch: dict[str, dict[int, Bar]] = defaultdict(dict)
    counts: Counter[str] = Counter()
    errors: list[str] = []

    for path in paths:
        with open_text(path) as handle:
            for line_no, line in enumerate(handle, start=1):
                if '"event":"equity_bar"' not in line and '"event": "equity_bar"' not in line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    counts["json_decode_errors"] += 1
                    if len(errors) < 10:
                        errors.append(f"{path}:{line_no}: {exc}")
                    continue
                symbol = str(event.get("symbol", "")).strip().upper()
                if not symbol or (wanted_symbols is not None and symbol not in wanted_symbols):
                    continue
                epoch = parse_int(event.get("barEpochSec"))
                if epoch is None:
                    counts["missing_epoch"] += 1
                    continue
                open_px = parse_float(event.get("open"))
                high = parse_float(event.get("high"))
                low = parse_float(event.get("low"))
                close = parse_float(event.get("close"))
                if not all(finite(v) and float(v) > 0.0 for v in (open_px, high, low, close)):
                    counts["invalid_ohlc"] += 1
                    continue
                bid = clean_optional_float(parse_float(event.get("bid")))
                ask = clean_optional_float(parse_float(event.get("ask")))
                by_symbol_epoch[symbol][epoch] = Bar(
                    epoch=epoch,
                    open=float(open_px),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    bid=bid if bid and bid > 0.0 else None,
                    ask=ask if ask and ask > 0.0 else None,
                )
                counts["equity_bars"] += 1
                counts[f"equity_bars_{symbol}"] += 1

    series_by_symbol: dict[str, BarSeries] = {}
    for symbol, epoch_map in by_symbol_epoch.items():
        epochs = sorted(epoch_map)
        bars = [epoch_map[epoch] for epoch in epochs]
        series_by_symbol[symbol] = BarSeries(symbol=symbol, bars=bars, epochs=epochs)

    summary = {
        "counts": dict(counts),
        "symbols": sorted(series_by_symbol),
        "bars_by_symbol": {symbol: len(series.bars) for symbol, series in sorted(series_by_symbol.items())},
        "errors": errors,
    }
    return series_by_symbol, summary


def half_spread_pct(bar: Bar, default_spread_bps: float) -> float:
    if bar.bid is not None and bar.ask is not None and bar.ask >= bar.bid:
        mid = (bar.bid + bar.ask) / 2.0
        if mid > 0.0:
            return max(0.0, (bar.ask - bar.bid) / mid) / 2.0
    return max(0.0, default_spread_bps) / 20000.0


def entry_fill_price(side: str, bar: Bar, *, entry_slippage_bps: float, default_spread_bps: float) -> float:
    slip = max(0.0, entry_slippage_bps) / 10000.0
    if side == "long" and bar.ask is not None and (bar.bid is None or bar.ask >= bar.bid):
        return bar.ask * (1.0 + slip)
    if side == "short" and bar.bid is not None and (bar.ask is None or bar.ask >= bar.bid):
        return max(0.0, bar.bid * (1.0 - slip))
    spread = half_spread_pct(bar, default_spread_bps)
    if side == "long":
        return bar.close * (1.0 + spread + slip)
    return max(0.0, bar.close * (1.0 - spread - slip))


def exit_fill_price(side: str, base_price: float, bar: Bar, *, exit_slippage_bps: float, default_spread_bps: float) -> float:
    if not finite(base_price) or float(base_price) <= 0.0:
        return math.nan
    slip = max(0.0, exit_slippage_bps) / 10000.0
    spread = half_spread_pct(bar, default_spread_bps)
    if side == "long":
        return max(0.0, float(base_price) * (1.0 - spread - slip))
    return float(base_price) * (1.0 + spread + slip)


def evaluate_trade_from_epoch(
    series: BarSeries | None,
    side: str,
    entry_epoch: int,
    *,
    horizon_seconds: int,
    profit_pct: float,
    risk_pct: float,
    entry_slippage_bps: float,
    exit_slippage_bps: float,
    default_spread_bps: float,
    round_trip_commission_bps: float,
    fill_probability: float,
    partial_fill_penalty_r: float,
    missed_fill_penalty_r: float,
    max_entry_bar_gap_seconds: int,
) -> dict[str, Any]:
    if series is None or not series.bars:
        return {"status": "missing_symbol_bars", "entry_epoch": entry_epoch, "entry_epoch_utc": epoch_utc(entry_epoch)}

    idx = bisect.bisect_left(series.epochs, entry_epoch)
    if idx >= len(series.bars):
        return {"status": "entry_after_last_bar", "entry_epoch": entry_epoch, "entry_epoch_utc": epoch_utc(entry_epoch)}
    entry_bar = series.bars[idx]
    entry_gap = entry_bar.epoch - entry_epoch
    if entry_gap < 0 or entry_gap > max_entry_bar_gap_seconds:
        return {
            "status": "entry_bar_gap_too_large",
            "entry_epoch": entry_epoch,
            "entry_epoch_utc": epoch_utc(entry_epoch),
            "entry_bar_epoch": entry_bar.epoch,
            "entry_bar_gap_seconds": entry_gap,
        }

    entry_fill = entry_fill_price(side, entry_bar, entry_slippage_bps=entry_slippage_bps, default_spread_bps=default_spread_bps)
    if not finite(entry_fill) or entry_fill <= 0.0 or risk_pct <= 0.0:
        return {"status": "invalid_entry_fill", "entry_epoch": entry_epoch, "entry_epoch_utc": epoch_utc(entry_epoch)}

    risk_dollars = entry_fill * risk_pct
    if side == "long":
        target = entry_fill * (1.0 + profit_pct)
        stop = entry_fill * (1.0 - risk_pct)
    else:
        target = entry_fill * (1.0 - profit_pct)
        stop = entry_fill * (1.0 + risk_pct)

    future: list[tuple[int, Bar]] = []
    horizon_epoch = entry_epoch + max(1, int(horizon_seconds))
    for j in range(idx + 1, len(series.bars)):
        bar = series.bars[j]
        if bar.epoch > horizon_epoch:
            break
        future.append((j, bar))

    if not future:
        return {
            "status": "no_future_bars",
            "entry_epoch": entry_epoch,
            "entry_epoch_utc": epoch_utc(entry_epoch),
            "entry_bar_epoch": entry_bar.epoch,
            "entry_bar_gap_seconds": entry_gap,
            "entry_fill": entry_fill,
            "risk_dollars": risk_dollars,
            "target_price": target,
            "stop_price": stop,
            "future_bar_count": 0,
        }

    future_high = max(bar.high for _, bar in future)
    future_low = min(bar.low for _, bar in future)
    if side == "long":
        mfe_r = (future_high - entry_fill) / risk_dollars
        mae_r = (future_low - entry_fill) / risk_dollars
    else:
        mfe_r = (entry_fill - future_low) / risk_dollars
        mae_r = (entry_fill - future_high) / risk_dollars

    exit_idx = future[-1][0]
    exit_bar = future[-1][1]
    exit_reason = "horizon"
    exit_base = exit_bar.close
    target_hit = False
    stop_hit = False
    for j, bar in future:
        if side == "long":
            hit_target = bar.high >= target
            hit_stop = bar.low <= stop
        else:
            hit_target = bar.low <= target
            hit_stop = bar.high >= stop
        if hit_target and hit_stop:
            exit_idx = j
            exit_bar = bar
            exit_reason = "ambiguous_stop_first"
            exit_base = stop
            target_hit = True
            stop_hit = True
            break
        if hit_target:
            exit_idx = j
            exit_bar = bar
            exit_reason = "target"
            exit_base = target
            target_hit = True
            break
        if hit_stop:
            exit_idx = j
            exit_bar = bar
            exit_reason = "stop"
            exit_base = stop
            stop_hit = True
            break

    exit_fill = exit_fill_price(side, exit_base, exit_bar, exit_slippage_bps=exit_slippage_bps, default_spread_bps=default_spread_bps)
    if not finite(exit_fill) or exit_fill <= 0.0:
        return {"status": "invalid_exit_fill", "entry_epoch": entry_epoch, "entry_epoch_utc": epoch_utc(entry_epoch)}

    if side == "long":
        gross_pnl = exit_fill - entry_fill
    else:
        gross_pnl = entry_fill - exit_fill
    commission_cost = entry_fill * max(0.0, round_trip_commission_bps) / 10000.0
    realized_net_r = (gross_pnl - commission_cost) / risk_dollars
    fill_probability = min(max(fill_probability, 0.0), 1.0)
    expected_net_r = (
        fill_probability * realized_net_r
        - max(0.0, partial_fill_penalty_r)
        - (1.0 - fill_probability) * max(0.0, missed_fill_penalty_r)
    )

    return {
        "status": "ok",
        "entry_epoch": entry_epoch,
        "entry_epoch_utc": epoch_utc(entry_epoch),
        "entry_bar_epoch": entry_bar.epoch,
        "entry_bar_utc": epoch_utc(entry_bar.epoch),
        "entry_bar_gap_seconds": entry_gap,
        "entry_fill": entry_fill,
        "risk_dollars": risk_dollars,
        "target_price": target,
        "stop_price": stop,
        "future_bar_count": len(future),
        "future_high": future_high,
        "future_low": future_low,
        "mfe_r": mfe_r,
        "mae_r": mae_r,
        "exit_epoch": exit_bar.epoch,
        "exit_epoch_utc": epoch_utc(exit_bar.epoch),
        "exit_bar_index": exit_idx,
        "exit_reason": exit_reason,
        "target_hit": target_hit,
        "stop_hit": stop_hit,
        "exit_fill": exit_fill,
        "gross_pnl_per_share": gross_pnl,
        "round_trip_commission_cost_per_share": commission_cost,
        "realized_net_r": realized_net_r,
        "expected_net_r": expected_net_r,
    }


def add_micro_outcomes(
    micro_evals: list[MicroEval],
    bars_by_symbol: dict[str, BarSeries],
    args: argparse.Namespace,
) -> Counter[str]:
    counts: Counter[str] = Counter()
    for micro in micro_evals:
        if not micro.within_ttl:
            micro.outcome = {"status": "outside_ttl"}
            counts["outside_ttl"] += 1
            continue
        outcome = evaluate_trade_from_epoch(
            bars_by_symbol.get(micro.symbol),
            micro.side,
            micro.eval_epoch,
            horizon_seconds=args.horizon_seconds,
            profit_pct=args.profit_pct,
            risk_pct=args.risk_pct,
            entry_slippage_bps=args.entry_slippage_bps,
            exit_slippage_bps=args.exit_slippage_bps,
            default_spread_bps=args.default_spread_bps,
            round_trip_commission_bps=args.round_trip_commission_bps,
            fill_probability=args.fill_probability,
            partial_fill_penalty_r=args.partial_fill_penalty_r,
            missed_fill_penalty_r=args.missed_fill_penalty_r,
            max_entry_bar_gap_seconds=args.max_entry_bar_gap_seconds,
        )
        micro.outcome = outcome
        counts[str(outcome.get("status", "unknown"))] += 1
    return counts


def micro_eval_to_row(micro: MicroEval) -> dict[str, Any]:
    outcome = micro.outcome or {}
    return {
        "eval_id": micro.eval_id,
        "arm_id": micro.arm_id,
        "symbol": micro.symbol,
        "side": micro.side,
        "arm_epoch": micro.arm_epoch,
        "arm_epoch_utc": epoch_utc(micro.arm_epoch),
        "eval_epoch": micro.eval_epoch,
        "eval_epoch_utc": epoch_utc(micro.eval_epoch),
        "seconds_since_arm": micro.seconds_since_arm,
        "within_ttl": micro.within_ttl,
        "micro_prob": micro.prob,
        "micro_threshold": micro.threshold,
        "micro_margin": micro.margin,
        "actual_micro_result": micro.result,
        "outcome_status": outcome.get("status", ""),
        "expected_net_r": outcome.get("expected_net_r", ""),
        "realized_net_r": outcome.get("realized_net_r", ""),
        "mfe_r": outcome.get("mfe_r", ""),
        "mae_r": outcome.get("mae_r", ""),
        "entry_fill": outcome.get("entry_fill", ""),
        "exit_fill": outcome.get("exit_fill", ""),
        "exit_reason": outcome.get("exit_reason", ""),
        "target_hit": outcome.get("target_hit", ""),
        "stop_hit": outcome.get("stop_hit", ""),
        "future_bar_count": outcome.get("future_bar_count", ""),
        "line_no": micro.line_no,
    }


def choose_best_candidate(candidates: list[MicroEval]) -> MicroEval | None:
    usable = [m for m in candidates if m.outcome.get("status") == "ok" and finite(m.outcome.get("expected_net_r"))]
    if not usable:
        return None
    return max(usable, key=lambda m: (float(m.outcome["expected_net_r"]), float(m.prob), -m.seconds_since_arm))


def choose_first_cross(candidates: list[MicroEval], threshold: float) -> MicroEval | None:
    for micro in sorted(candidates, key=lambda m: (m.seconds_since_arm, m.line_no)):
        if micro.within_ttl and finite(micro.prob) and micro.prob >= threshold:
            return micro
    return None


def build_arm_rows_and_labels(
    arms: list[Arm],
    micro_evals: list[MicroEval],
    *,
    label_min_micro_prob: float,
    min_expected_net_r: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    arm_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    by_symbol_side: dict[str, dict[str, Any]] = defaultdict(lambda: {"arms": 0, "candidate_arms": 0, "positive_arms": 0, "best_expected_net_r": []})
    max_prob_by_symbol_side: dict[str, float] = {}

    for arm in arms:
        evals = [micro_evals[i] for i in arm.micro_eval_indices]
        within = [m for m in evals if m.within_ttl]
        actual_pass = [m for m in within if m.result == "PASS"]
        max_micro = max(within, key=lambda m: m.prob) if within else None
        floor_candidates = [m for m in within if finite(m.prob) and m.prob >= label_min_micro_prob]
        best_floor = choose_best_candidate(floor_candidates)
        best_any = choose_best_candidate(within)
        first_floor = choose_first_cross(within, label_min_micro_prob)
        downstream_positive = bool(
            best_floor
            and best_floor.outcome.get("status") == "ok"
            and finite(best_floor.outcome.get("expected_net_r"))
            and float(best_floor.outcome["expected_net_r"]) > min_expected_net_r
        )

        key = side_key(arm.symbol, arm.side)
        by_symbol_side[key]["arms"] += 1
        if floor_candidates:
            by_symbol_side[key]["candidate_arms"] += 1
        if downstream_positive:
            by_symbol_side[key]["positive_arms"] += 1
        if best_floor and finite(best_floor.outcome.get("expected_net_r")):
            by_symbol_side[key]["best_expected_net_r"].append(float(best_floor.outcome["expected_net_r"]))
        if max_micro and finite(max_micro.prob):
            max_prob_by_symbol_side[key] = max(float(max_micro.prob), max_prob_by_symbol_side.get(key, float("-inf")))

        selected_expected = best_floor.outcome.get("expected_net_r", math.nan) if best_floor else math.nan
        selected_realized = best_floor.outcome.get("realized_net_r", math.nan) if best_floor else math.nan
        selected_mfe = best_floor.outcome.get("mfe_r", math.nan) if best_floor else math.nan
        selected_mae = best_floor.outcome.get("mae_r", math.nan) if best_floor else math.nan
        arm_row = {
            "arm_id": arm.arm_id,
            "symbol": arm.symbol,
            "side": arm.side,
            "arm_epoch": arm.arm_epoch,
            "arm_epoch_utc": epoch_utc(arm.arm_epoch),
            "ttl_seconds": arm.ttl_seconds,
            "setup_prob": arm.setup_prob,
            "setup_threshold": arm.setup_threshold,
            "setup_margin": arm.setup_margin,
            "setup_qty": arm.setup_qty,
            "setup_rsi": arm.setup_rsi,
            "setup_price": arm.setup_price,
            "setup_price_field": arm.setup_price_field,
            "arbitration_reason": arm.arbitration_reason,
            "micro_eval_count": len(evals),
            "micro_eval_within_ttl_count": len(within),
            "actual_micro_pass_count": len(actual_pass),
            "max_micro_prob": max_micro.prob if max_micro else math.nan,
            "max_micro_delay_seconds": max_micro.seconds_since_arm if max_micro else "",
            "max_micro_threshold": max_micro.threshold if max_micro else math.nan,
            "label_min_micro_prob": label_min_micro_prob,
            "label_floor_candidate_count": len(floor_candidates),
            "label_floor_first_cross_delay_seconds": first_floor.seconds_since_arm if first_floor else "",
            "label_floor_first_cross_micro_prob": first_floor.prob if first_floor else math.nan,
            "best_floor_entry_delay_seconds": best_floor.seconds_since_arm if best_floor else "",
            "best_floor_micro_prob": best_floor.prob if best_floor else math.nan,
            "best_floor_expected_net_r": selected_expected,
            "best_floor_realized_net_r": selected_realized,
            "best_floor_mfe_r": selected_mfe,
            "best_floor_mae_r": selected_mae,
            "best_floor_exit_reason": best_floor.outcome.get("exit_reason", "") if best_floor else "",
            "best_floor_outcome_status": best_floor.outcome.get("status", "") if best_floor else "no_micro_floor_candidate",
            "best_any_entry_delay_seconds": best_any.seconds_since_arm if best_any else "",
            "best_any_micro_prob": best_any.prob if best_any else math.nan,
            "best_any_expected_net_r": best_any.outcome.get("expected_net_r", math.nan) if best_any else math.nan,
            "downstream_positive": downstream_positive,
            "line_no": arm.line_no,
        }
        arm_rows.append(arm_row)

        long_selected = arm.side == "long"
        short_selected = arm.side == "short"
        label_row = {
            "Label_Version": LABEL_SCHEMA_VERSION,
            "arm_id": arm.arm_id,
            "Symbol": arm.symbol,
            "SetupEpochSec": arm.arm_epoch,
            "SetupTimeUtc": epoch_utc(arm.arm_epoch),
            "SetupSide": arm.side,
            "SetupProb": arm.setup_prob,
            "SetupThreshold": arm.setup_threshold,
            "SetupThresholdMargin": arm.setup_margin,
            "SetupArbitrationReason": arm.arbitration_reason,
            "SetupQty": arm.setup_qty,
            "MicroEvalWithinTtlCount": len(within),
            "Label_Min_Micro_Prob": label_min_micro_prob,
            "Label_Min_Expected_Net_R": min_expected_net_r,
            "Label_Long_Setup_DownstreamPositive": 1 if long_selected and downstream_positive else 0,
            "Label_Short_Setup_DownstreamPositive": 1 if short_selected and downstream_positive else 0,
            "Expected_Long_Setup_DownstreamNetR": selected_expected if long_selected else math.nan,
            "Expected_Short_Setup_DownstreamNetR": selected_expected if short_selected else math.nan,
            "Max_Future_Micro_Long_Prob": max_micro.prob if long_selected and max_micro else math.nan,
            "Max_Future_Micro_Short_Prob": max_micro.prob if short_selected and max_micro else math.nan,
            "Best_Entry_Delay_Seconds": best_floor.seconds_since_arm if best_floor else "",
            "Best_Entry_EpochSec": best_floor.eval_epoch if best_floor else "",
            "Best_Entry_TimeUtc": epoch_utc(best_floor.eval_epoch) if best_floor else "",
            "Best_Micro_Prob": best_floor.prob if best_floor else math.nan,
            "Best_Realized_Net_R": selected_realized,
            "Best_MFE_R": selected_mfe,
            "Best_MAE_R": selected_mae,
            "Best_Exit_Reason": best_floor.outcome.get("exit_reason", "") if best_floor else "",
            "Best_Outcome_Status": best_floor.outcome.get("status", "") if best_floor else "no_micro_floor_candidate",
        }
        label_rows.append(label_row)

    symbol_side_summary = {}
    for key, row in sorted(by_symbol_side.items()):
        values = row.pop("best_expected_net_r")
        symbol_side_summary[key] = {
            **row,
            "candidate_rate": row["candidate_arms"] / row["arms"] if row["arms"] else 0.0,
            "positive_rate": row["positive_arms"] / row["arms"] if row["arms"] else 0.0,
            "best_expected_net_r_mean": mean(values),
            "best_expected_net_r_p50": quantile(values, 0.50),
        }

    aggregate_summary = {
        "symbol_side": symbol_side_summary,
        "max_micro_probability_by_symbol_side": {
            key: value for key, value in sorted(max_prob_by_symbol_side.items()) if finite(value)
        },
    }
    return arm_rows, label_rows, aggregate_summary


def build_decision_rows(
    arms: list[Arm],
    micro_evals: list[MicroEval],
    thresholds: list[float],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    threshold_expected: dict[float, list[float]] = defaultdict(list)
    threshold_realized: dict[float, list[float]] = defaultdict(list)
    threshold_counts: dict[float, Counter[str]] = defaultdict(Counter)
    threshold_by_symbol_side: dict[float, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))

    for arm in arms:
        within = [micro_evals[i] for i in arm.micro_eval_indices if micro_evals[i].within_ttl]
        within.sort(key=lambda m: (m.seconds_since_arm, m.line_no))
        for threshold in thresholds:
            selected = choose_first_cross(within, threshold)
            key = side_key(arm.symbol, arm.side)
            base = {
                "arm_id": arm.arm_id,
                "symbol": arm.symbol,
                "side": arm.side,
                "arm_epoch": arm.arm_epoch,
                "arm_epoch_utc": epoch_utc(arm.arm_epoch),
                "setup_prob": arm.setup_prob,
                "setup_threshold": arm.setup_threshold,
                "setup_margin": arm.setup_margin,
                "arbitration_reason": arm.arbitration_reason,
                "counterfactual_micro_threshold": threshold,
            }
            threshold_counts[threshold]["arms"] += 1
            threshold_by_symbol_side[threshold][key]["arms"] += 1
            if selected is None:
                rows.append({
                    **base,
                    "counterfactual_confirms": 0,
                    "confirm_delay_seconds": "",
                    "confirm_micro_prob": "",
                    "outcome_status": "no_micro_cross",
                    "expected_net_r": "",
                    "realized_net_r": "",
                    "mfe_r": "",
                    "mae_r": "",
                    "exit_reason": "",
                    "target_hit": "",
                    "stop_hit": "",
                })
                threshold_counts[threshold]["no_cross"] += 1
                threshold_by_symbol_side[threshold][key]["no_cross"] += 1
                continue
            outcome = selected.outcome or {}
            expected = outcome.get("expected_net_r")
            realized = outcome.get("realized_net_r")
            positive = finite(expected) and float(expected) > 0.0
            rows.append({
                **base,
                "counterfactual_confirms": 1,
                "confirm_delay_seconds": selected.seconds_since_arm,
                "confirm_micro_prob": selected.prob,
                "confirm_eval_epoch": selected.eval_epoch,
                "confirm_eval_epoch_utc": epoch_utc(selected.eval_epoch),
                "outcome_status": outcome.get("status", ""),
                "expected_net_r": expected,
                "realized_net_r": realized,
                "mfe_r": outcome.get("mfe_r", ""),
                "mae_r": outcome.get("mae_r", ""),
                "exit_reason": outcome.get("exit_reason", ""),
                "target_hit": outcome.get("target_hit", ""),
                "stop_hit": outcome.get("stop_hit", ""),
            })
            threshold_counts[threshold]["confirms"] += 1
            threshold_by_symbol_side[threshold][key]["confirms"] += 1
            if positive:
                threshold_counts[threshold]["positive"] += 1
                threshold_by_symbol_side[threshold][key]["positive"] += 1
            if outcome.get("exit_reason") == "target":
                threshold_counts[threshold]["target"] += 1
            if outcome.get("exit_reason") in {"stop", "ambiguous_stop_first"}:
                threshold_counts[threshold]["stop"] += 1
            if finite(expected):
                threshold_expected[threshold].append(float(expected))
            if finite(realized):
                threshold_realized[threshold].append(float(realized))

    threshold_summary: list[dict[str, Any]] = []
    for threshold in thresholds:
        counts = threshold_counts[threshold]
        arms_count = counts["arms"]
        confirms = counts["confirms"]
        expected_values = threshold_expected[threshold]
        realized_values = threshold_realized[threshold]
        threshold_summary.append({
            "threshold": threshold,
            "arms": arms_count,
            "confirms": confirms,
            "confirm_rate": confirms / arms_count if arms_count else 0.0,
            "positive": counts["positive"],
            "positive_rate_per_arm": counts["positive"] / arms_count if arms_count else 0.0,
            "positive_rate_per_confirm": counts["positive"] / confirms if confirms else 0.0,
            "target": counts["target"],
            "stop": counts["stop"],
            "expected_net_r_mean": mean(expected_values),
            "expected_net_r_p10": quantile(expected_values, 0.10),
            "expected_net_r_p50": quantile(expected_values, 0.50),
            "expected_net_r_p90": quantile(expected_values, 0.90),
            "realized_net_r_mean": mean(realized_values),
            "realized_net_r_p50": quantile(realized_values, 0.50),
            "by_symbol_side": {
                key: {
                    "arms": c["arms"],
                    "confirms": c["confirms"],
                    "positive": c["positive"],
                    "confirm_rate": c["confirms"] / c["arms"] if c["arms"] else 0.0,
                    "positive_rate_per_arm": c["positive"] / c["arms"] if c["arms"] else 0.0,
                }
                for key, c in sorted(threshold_by_symbol_side[threshold].items())
            },
        })
    return rows, {"thresholds": threshold_summary}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)
    else:
        fieldnames = []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if fieldnames:
            writer.writeheader()
            for row in rows:
                writer.writerow({key: csv_value(row.get(key)) for key in fieldnames})


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    threshold_rows = summary.get("threshold_summary", {}).get("thresholds", [])
    symbol_side = summary.get("label_summary", {}).get("symbol_side", {})
    max_prob = summary.get("label_summary", {}).get("max_micro_probability_by_symbol_side", {})
    lines = [
        "# Setup → Micro Counterfactual Profitability Report",
        "",
        f"Generated: `{summary.get('generated_at_utc', '')}`",
        "",
        "## Inputs",
        "",
        f"- Replay log: `{summary.get('inputs', {}).get('log', '')}`",
        f"- Recorded events: `{', '.join(summary.get('inputs', {}).get('recorded_events', []))}`",
        "",
        "## Assumptions",
        "",
        f"- Label schema: `{LABEL_SCHEMA_VERSION}`",
        f"- Label minimum micro probability: `{summary.get('assumptions', {}).get('label_min_micro_prob')}`",
        f"- Minimum expected net R for a positive label: `{summary.get('assumptions', {}).get('min_expected_net_r')}`",
        f"- Horizon seconds: `{summary.get('assumptions', {}).get('horizon_seconds')}`",
        f"- Profit/risk pct: `{summary.get('assumptions', {}).get('profit_pct')}` / `{summary.get('assumptions', {}).get('risk_pct')}`",
        f"- Entry/exit slippage bps: `{summary.get('assumptions', {}).get('entry_slippage_bps')}` / `{summary.get('assumptions', {}).get('exit_slippage_bps')}`",
        "",
        "## Parse totals",
        "",
        f"- Arms: `{summary.get('arms_total')}`",
        f"- Micro evaluations: `{summary.get('micro_evals_total')}`",
        f"- Recorded equity bars: `{summary.get('bars_total')}`",
        "",
        "## Downstream label summary by symbol/side",
        "",
        "| symbol.side | arms | candidate arms | positive arms | candidate rate | positive rate | max micro prob | mean best expected R |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, row in symbol_side.items():
        lines.append(
            "| {key} | {arms} | {candidate_arms} | {positive_arms} | {candidate_rate:.4f} | {positive_rate:.4f} | {max_prob} | {mean_r} |".format(
                key=key,
                arms=row.get("arms", 0),
                candidate_arms=row.get("candidate_arms", 0),
                positive_arms=row.get("positive_arms", 0),
                candidate_rate=float(row.get("candidate_rate") or 0.0),
                positive_rate=float(row.get("positive_rate") or 0.0),
                max_prob="" if max_prob.get(key) is None else f"{float(max_prob[key]):.4f}",
                mean_r="" if row.get("best_expected_net_r_mean") is None else f"{float(row['best_expected_net_r_mean']):.4f}",
            )
        )
    lines.extend([
        "",
        "## Counterfactual first-cross threshold grid",
        "",
        "| threshold | confirms | confirm rate | positive | positive / confirm | mean expected R | p50 expected R | target | stop |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in threshold_rows:
        lines.append(
            "| {threshold:.4f} | {confirms} | {confirm_rate:.4f} | {positive} | {positive_per_confirm:.4f} | {mean_r} | {p50_r} | {target} | {stop} |".format(
                threshold=float(row.get("threshold") or 0.0),
                confirms=row.get("confirms", 0),
                confirm_rate=float(row.get("confirm_rate") or 0.0),
                positive=row.get("positive", 0),
                positive_per_confirm=float(row.get("positive_rate_per_confirm") or 0.0),
                mean_r="" if row.get("expected_net_r_mean") is None else f"{float(row['expected_net_r_mean']):.4f}",
                p50_r="" if row.get("expected_net_r_p50") is None else f"{float(row['expected_net_r_p50']):.4f}",
                target=row.get("target", 0),
                stop=row.get("stop", 0),
            )
        )
    lines.extend([
        "",
        "## Output artifacts",
        "",
    ])
    for name, output_path in summary.get("outputs", {}).items():
        lines.append(f"- `{name}`: `{output_path}`")
    lines.extend([
        "",
        "Promotion status: **NO-GO** until a retrained bundle passes replay volume, expected net R, calibration, dominance, parity, and paper/shadow drift gates.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def labels_manifest(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": LABEL_SCHEMA_VERSION,
        "generated_at_utc": summary.get("generated_at_utc"),
        "inputs": summary.get("inputs"),
        "labels_csv": Path(summary.get("outputs", {}).get("labels_csv", "")).name,
        "assumptions": summary.get("assumptions"),
        "label_columns": {
            "long_binary": "Label_Long_Setup_DownstreamPositive",
            "short_binary": "Label_Short_Setup_DownstreamPositive",
            "long_expected_net_r": "Expected_Long_Setup_DownstreamNetR",
            "short_expected_net_r": "Expected_Short_Setup_DownstreamNetR",
            "max_future_micro_long_prob": "Max_Future_Micro_Long_Prob",
            "max_future_micro_short_prob": "Max_Future_Micro_Short_Prob",
            "best_entry_delay_seconds": "Best_Entry_Delay_Seconds",
        },
        "summary": summary.get("label_summary"),
        "warnings": [
            "Counterfactual labels are generated from replay-observed setup arms only; use for research/retraining, not direct promotion.",
            "Positive labels require a configurable micro-probability floor and positive expected net R after spread/slippage/fill penalties.",
        ],
    }


def build_summary(
    *,
    args: argparse.Namespace,
    outputs: dict[str, Path],
    parse_summary: dict[str, Any],
    bars_summary: dict[str, Any],
    outcome_counts: Counter[str],
    arms: list[Arm],
    micro_evals: list[MicroEval],
    label_summary: dict[str, Any],
    threshold_summary: dict[str, Any],
) -> dict[str, Any]:
    arms_by_symbol_side: Counter[str] = Counter(side_key(arm.symbol, arm.side) for arm in arms)
    micro_by_symbol_side: Counter[str] = Counter(side_key(micro.symbol, micro.side) for micro in micro_evals)
    positive_labels = {
        key: row.get("positive_arms", 0)
        for key, row in label_summary.get("symbol_side", {}).items()
    }
    return {
        "generated_at_utc": utc_now(),
        "schema_version": "setup_micro_counterfactual_report_v1",
        "inputs": {
            "log": str(args.log),
            "recorded_events": [str(path) for path in args.recorded_events],
        },
        "outputs": {name: str(path) for name, path in outputs.items()},
        "assumptions": {
            "label_schema_version": LABEL_SCHEMA_VERSION,
            "label_min_micro_prob": args.label_min_micro_prob,
            "min_expected_net_r": args.min_expected_net_r,
            "horizon_seconds": args.horizon_seconds,
            "profit_pct": args.profit_pct,
            "risk_pct": args.risk_pct,
            "entry_slippage_bps": args.entry_slippage_bps,
            "exit_slippage_bps": args.exit_slippage_bps,
            "default_spread_bps": args.default_spread_bps,
            "round_trip_commission_bps": args.round_trip_commission_bps,
            "fill_probability": args.fill_probability,
            "partial_fill_penalty_r": args.partial_fill_penalty_r,
            "missed_fill_penalty_r": args.missed_fill_penalty_r,
            "micro_threshold_grid": args.micro_threshold_grid,
        },
        "parse_summary": parse_summary,
        "bars_summary": bars_summary,
        "outcome_counts": dict(outcome_counts),
        "arms_total": len(arms),
        "micro_evals_total": len(micro_evals),
        "bars_total": int(sum(bars_summary.get("bars_by_symbol", {}).values())),
        "arms_by_symbol_side": dict(sorted(arms_by_symbol_side.items())),
        "micro_evals_by_symbol_side": dict(sorted(micro_by_symbol_side.items())),
        "positive_labels_by_symbol_side": positive_labels,
        "label_summary": label_summary,
        "threshold_summary": threshold_summary,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate setup→micro counterfactual profitability and downstream setup labels from a replay log."
    )
    parser.add_argument("--log", required=True, type=Path, help="Replay log containing setup, arbitration, arm, and micro evaluation lines.")
    parser.add_argument(
        "--recorded-events",
        required=True,
        action="append",
        type=Path,
        help="Recorded normalized NDJSON/NDJSON.GZ file with equity_bar events. Repeat for multiple files.",
    )
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for generated CSV/JSON/Markdown artifacts.")
    parser.add_argument("--label-min-micro-prob", type=float, default=0.30, help="Minimum micro probability considered downstream-confirmable for labels.")
    parser.add_argument("--min-expected-net-r", type=float, default=0.0, help="Minimum expected net R required for a positive downstream setup label.")
    parser.add_argument("--micro-threshold-grid", default=" ".join(str(v) for v in DEFAULT_MICRO_THRESHOLD_GRID), help="Comma/space-separated counterfactual first-cross micro threshold grid.")
    parser.add_argument("--horizon-seconds", type=int, default=600, help="Future equity-bar horizon used to compute MFE/MAE/net-R from micro entry.")
    parser.add_argument("--profit-pct", type=float, default=0.0035, help="Target move as a fraction of entry fill price.")
    parser.add_argument("--risk-pct", type=float, default=0.0025, help="Stop/risk move as a fraction of entry fill price.")
    parser.add_argument("--entry-slippage-bps", type=float, default=2.0, help="Entry slippage in basis points.")
    parser.add_argument("--exit-slippage-bps", type=float, default=2.0, help="Exit slippage in basis points.")
    parser.add_argument("--default-spread-bps", type=float, default=0.0, help="Fallback full spread in bps when bid/ask are unavailable.")
    parser.add_argument("--round-trip-commission-bps", type=float, default=0.0, help="Round-trip commission model in bps of entry price.")
    parser.add_argument("--fill-probability", type=float, default=0.98, help="Expected fill probability used to discount realized net R.")
    parser.add_argument("--partial-fill-penalty-r", type=float, default=0.02, help="Expected R penalty for partial fill/friction.")
    parser.add_argument("--missed-fill-penalty-r", type=float, default=0.05, help="Expected R penalty for missed fill probability.")
    parser.add_argument("--max-entry-bar-gap-seconds", type=int, default=10, help="Maximum allowed gap between micro eval epoch and next available equity bar.")
    parser.add_argument("--max-arms", type=int, default=0, help="Optional smoke-test cap on parsed arms; 0 means all arms.")
    args = parser.parse_args(argv)
    args.micro_threshold_grid = parse_thresholds(args.micro_threshold_grid)
    return args


def validate_inputs(args: argparse.Namespace) -> None:
    if not args.log.is_file():
        raise FileNotFoundError(f"missing replay log: {args.log}")
    for path in args.recorded_events:
        if not path.is_file():
            raise FileNotFoundError(f"missing recorded-events file: {path}")
    if args.horizon_seconds <= 0:
        raise ValueError("--horizon-seconds must be positive")
    if args.risk_pct <= 0.0:
        raise ValueError("--risk-pct must be positive")
    if args.profit_pct <= 0.0:
        raise ValueError("--profit-pct must be positive")
    if not 0.0 <= args.fill_probability <= 1.0:
        raise ValueError("--fill-probability must be within [0, 1]")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validate_inputs(args)
    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[COUNTERFACTUAL] parsing log: {args.log}", flush=True)
    arms, micro_evals, parse_summary = parse_replay_log(args.log, max_arms=args.max_arms)
    wanted_symbols = {arm.symbol for arm in arms}
    print(f"[COUNTERFACTUAL] parsed arms={len(arms)} micro_evals={len(micro_evals)}", flush=True)

    print(f"[COUNTERFACTUAL] loading recorded equity bars for symbols={sorted(wanted_symbols)}", flush=True)
    bars_by_symbol, bars_summary = load_equity_bars(args.recorded_events, wanted_symbols=wanted_symbols or None)
    print(f"[COUNTERFACTUAL] loaded bars={sum(len(series.bars) for series in bars_by_symbol.values())}", flush=True)

    print("[COUNTERFACTUAL] evaluating micro-entry future paths", flush=True)
    outcome_counts = add_micro_outcomes(micro_evals, bars_by_symbol, args)
    arm_rows, label_rows, label_summary = build_arm_rows_and_labels(
        arms,
        micro_evals,
        label_min_micro_prob=args.label_min_micro_prob,
        min_expected_net_r=args.min_expected_net_r,
    )
    decision_rows, threshold_summary = build_decision_rows(arms, micro_evals, args.micro_threshold_grid)
    micro_rows = [micro_eval_to_row(micro) for micro in micro_evals]

    outputs = {
        "arms_csv": out_dir / "setup_micro_counterfactual_arms.csv",
        "micro_evals_csv": out_dir / "setup_micro_counterfactual_micro_evals.csv",
        "decisions_csv": out_dir / "setup_micro_counterfactual_decisions.csv",
        "labels_csv": out_dir / f"{LABEL_SCHEMA_VERSION}.csv",
        "summary_json": out_dir / "setup_micro_counterfactual_summary.json",
        "summary_md": out_dir / "setup_micro_counterfactual_summary.md",
        "labels_manifest_json": out_dir / f"{LABEL_SCHEMA_VERSION}_manifest.json",
    }

    print("[COUNTERFACTUAL] writing CSV artifacts", flush=True)
    write_csv(outputs["arms_csv"], arm_rows)
    write_csv(outputs["micro_evals_csv"], micro_rows)
    write_csv(outputs["decisions_csv"], decision_rows)
    write_csv(outputs["labels_csv"], label_rows)

    summary = build_summary(
        args=args,
        outputs=outputs,
        parse_summary=parse_summary,
        bars_summary=bars_summary,
        outcome_counts=outcome_counts,
        arms=arms,
        micro_evals=micro_evals,
        label_summary=label_summary,
        threshold_summary=threshold_summary,
    )
    outputs["summary_json"].write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")
    write_markdown(outputs["summary_md"], summary)
    outputs["labels_manifest_json"].write_text(json.dumps(json_safe(labels_manifest(summary)), indent=2), encoding="utf-8")

    best_threshold = None
    thresholds = threshold_summary.get("thresholds", [])
    if thresholds:
        best_threshold = max(
            thresholds,
            key=lambda row: (
                float(row.get("expected_net_r_mean") or -999.0),
                int(row.get("positive") or 0),
                int(row.get("confirms") or 0),
            ),
        )
    positive_total = sum(row.get("positive_arms", 0) for row in label_summary.get("symbol_side", {}).values())
    print(
        "[COUNTERFACTUAL] done "
        f"arms={len(arms)} micro_evals={len(micro_evals)} label_positive_arms={positive_total} "
        f"best_threshold={best_threshold.get('threshold') if best_threshold else ''} "
        f"summary={outputs['summary_json']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(1)

