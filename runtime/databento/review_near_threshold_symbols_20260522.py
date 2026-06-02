#!/usr/bin/env python3
"""Review symbols whose live AI entry signals repeatedly fire near threshold.

This is intentionally self-contained/std-lib only so it can be run on the
trading host after a live session without modifying the Java bot.
"""

import csv
import gzip
import glob
import math
import os
import re
import statistics
from collections import Counter, defaultdict, deque
from datetime import datetime

DATE = "2026-05-22"
DATE_COMPACT = "20260522"
ROOT = "/Users/filmonghezehey/trading-agent/worktrees/databento"
LOG_ROOT = os.path.join(ROOT, "runtime/databento/logs")
TRADE_CSV = os.path.expanduser("~/Downloads/trades.20260522.csv")
OUT = os.path.join(ROOT, "runtime/databento/near_threshold_symbol_review_20260522.txt")

NEAR_005 = 0.005
NEAR_010 = 0.010
NEAR_025 = 0.025
NEAR_050 = 0.050
ENTRY_TAGS = {"AI.LONG.ENTRY", "AI.SHORT.ENTRY"}


def percentile(values, pct):
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - rank) + ordered[hi] * (rank - lo)


def fmt_float(value, digits=4):
    if value is None or math.isnan(value):
        return "nan"
    return f"{value:.{digits}f}"


def open_text(path):
    return gzip.open(path, "rt", encoding="utf-8", errors="ignore") if path.endswith(".gz") else open(path, "rt", encoding="utf-8", errors="ignore")


def log_paths():
    paths = glob.glob(os.path.join(LOG_ROOT, "trading-agent-*.log"))
    paths += glob.glob(os.path.join(LOG_ROOT, f"trading-agent-*.log.{DATE}.*.gz"))
    return sorted(set(paths))


def read_ibkr_executions():
    if not os.path.exists(TRADE_CSV):
        return [], []
    rows = []
    with open(TRADE_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Date") != DATE_COMPACT or row.get("Security Type") != "STK":
                continue
            try:
                qty = int(float(row["Quantity"]))
                price = float(row["Price"])
            except Exception:
                continue
            rows.append(
                {
                    "sym": row["Symbol"].strip().upper(),
                    "action": row["Action"].strip().upper(),
                    "qty": qty,
                    "price": price,
                    "time": row["Time"].strip(),
                    "notional": qty * price,
                }
            )
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["sym"], row["action"], row["time"], round(row["price"], 4))].append(row)
    groups = []
    for (sym, action, time, price), items in grouped.items():
        groups.append(
            {
                "sym": sym,
                "action": action,
                "time": time,
                "price": price,
                "qty": sum(item["qty"] for item in items),
                "rows": len(items),
                "notional": sum(item["notional"] for item in items),
            }
        )
    groups.sort(key=lambda item: (item["time"], item["sym"], item["action"]))
    return rows, groups


def rough_fifo(groups):
    pnl = Counter()
    matched_qty = Counter()
    matched_slices = Counter()
    inventories = defaultdict(deque)
    for group in groups:
        sym = group["sym"]
        side = 1 if group["action"] == "BOT" else -1
        qty = group["qty"]
        price = group["price"]
        inventory = inventories[sym]
        while qty > 0 and inventory and inventory[0][0] == -side:
            lot_side, lot_qty, lot_price = inventory[0]
            matched = min(qty, lot_qty)
            pnl[sym] += (price - lot_price) * matched if lot_side == 1 else (lot_price - price) * matched
            matched_qty[sym] += matched
            matched_slices[sym] += 1
            qty -= matched
            lot_qty -= matched
            if lot_qty:
                inventory[0] = (lot_side, lot_qty, lot_price)
            else:
                inventory.popleft()
        if qty > 0:
            inventory.append((side, qty, price))
    return pnl, matched_qty, matched_slices


def transition_times(groups):
    last_by_symbol = {}
    transitions = []
    for group in groups:
        try:
            dt = datetime.strptime(group["time"], "%H:%M:%S")
        except ValueError:
            continue
        previous = last_by_symbol.get(group["sym"])
        if previous and previous["action"] != group["action"]:
            seconds = (dt - previous["dt"]).total_seconds()
            if seconds >= 0:
                transitions.append((group["sym"], seconds, previous["action"] + "->" + group["action"]))
        last_by_symbol[group["sym"]] = {**group, "dt": dt}
    return transitions


def parse_logs():
    re_ai = re.compile(
        r"\[FLOW\]\[COND\]\[(AI\.(?:LONG|SHORT)\.(?:ENTRY|EXIT))\].*?=PASS.*?"
        r"symbol=([A-Z0-9.]+).*?prob=([0-9.]+).*?threshold=([0-9.]+)"
    )
    re_order = re.compile(r"ORDER\.SEND.*?orderId=([-0-9]+) action=(BUY|SELL) type=([A-Z_]+)")
    re_symbol = re.compile(r"symbol=([A-Z][A-Z0-9.]*)")
    re_entry_gate = re.compile(r"ENTRY_GATE_OPEN=PASS.*?symbol=([A-Z0-9.]+).*?tradeCount=([0-9]+).*?maxTrades=([0-9]+)")
    re_file_symbol = re.compile(r"trading-agent-([A-Z0-9.]+)\.log")

    files = log_paths()
    seen_lines = set()
    entry_events = defaultdict(list)
    exit_events = defaultdict(list)
    all_ai_counts = Counter()
    order_send = Counter()
    order_type = Counter()
    hard_stop = Counter()
    entry_gate = Counter()
    max_trade_count = Counter()
    samples = defaultdict(list)
    unique_log_lines = 0

    for path in files:
        file_symbol_match = re_file_symbol.search(os.path.basename(path))
        file_symbol = file_symbol_match.group(1) if file_symbol_match else None
        with open_text(path) as f:
            for raw_line in f:
                if DATE not in raw_line:
                    continue
                line = raw_line.rstrip("\n")
                if line in seen_lines:
                    continue
                seen_lines.add(line)
                unique_log_lines += 1

                symbol_match = re_symbol.search(line)
                symbol = symbol_match.group(1) if symbol_match else file_symbol

                order_match = re_order.search(line)
                if order_match and symbol:
                    order_send[symbol] += 1
                    order_type[(symbol, order_match.group(3))] += 1

                if (
                    "LONG_HARD_STOP_TRIGGER=PASS" in line
                    or "SHORT_HARD_STOP_TRIGGER=PASS" in line
                    or "HARD_STOP_TRIGGERED=PASS" in line
                ) and symbol:
                    hard_stop[symbol] += 1

                gate_match = re_entry_gate.search(line)
                if gate_match:
                    gate_symbol = gate_match.group(1)
                    trade_count = int(gate_match.group(2))
                    entry_gate[gate_symbol] += 1
                    max_trade_count[gate_symbol] = max(max_trade_count[gate_symbol], trade_count)

                ai_match = re_ai.search(line)
                if not ai_match:
                    continue
                tag, ai_symbol, prob_text, threshold_text = ai_match.groups()
                probability = float(prob_text)
                threshold = float(threshold_text)
                margin = probability - threshold
                event = {"tag": tag, "prob": probability, "threshold": threshold, "margin": margin, "line": line}
                all_ai_counts[(ai_symbol, tag)] += 1
                if tag in ENTRY_TAGS:
                    entry_events[ai_symbol].append(event)
                    if margin <= NEAR_025 and len(samples[ai_symbol]) < 4:
                        samples[ai_symbol].append(line)
                else:
                    exit_events[ai_symbol].append(event)

    return {
        "files": files,
        "unique_log_lines": unique_log_lines,
        "entry_events": entry_events,
        "exit_events": exit_events,
        "all_ai_counts": all_ai_counts,
        "order_send": order_send,
        "order_type": order_type,
        "hard_stop": hard_stop,
        "entry_gate": entry_gate,
        "max_trade_count": max_trade_count,
        "samples": samples,
    }


def build_trade_stats(groups):
    by_symbol = defaultdict(lambda: {"groups": 0, "rows": 0, "notional": 0.0, "bot": 0, "sld": 0, "net": 0})
    for group in groups:
        data = by_symbol[group["sym"]]
        data["groups"] += 1
        data["rows"] += group["rows"]
        data["notional"] += group["notional"]
        data[group["action"].lower()] += 1
        data["net"] += group["qty"] if group["action"] == "BOT" else -group["qty"]
    return by_symbol


def classify(row):
    near_count = row["near025"]
    near_rate = row["near025_rate"]
    total = row["entry_pass"]
    pnl = row["rough_pnl"]
    hard_stops = row["hard_stops"]
    groups = row["groups"]
    fast_reversals = row["fast_reversals_180s"]
    p50 = row["p50_margin"]

    if total >= 10 and near_count >= 8 and (hard_stops >= 8 or groups >= 20 or pnl <= -75 or fast_reversals >= 10):
        return "HIGH_RISK"
    if total >= 8 and (near_count >= 5 or near_rate >= 0.45):
        return "REVIEW"
    if total >= 5 and (near_rate >= 0.50 or (not math.isnan(p50) and p50 <= 0.030)):
        return "WATCH"
    return "OK"


def main():
    rows, groups = read_ibkr_executions()
    pnl, matched_qty, matched_slices = rough_fifo(groups)
    transitions = transition_times(groups)
    fast_transitions = Counter(sym for sym, seconds, _transition in transitions if seconds <= 180)
    trade_stats = build_trade_stats(groups)
    logs = parse_logs()

    symbols = set(trade_stats) | set(logs["entry_events"]) | set(logs["order_send"]) | set(logs["hard_stop"])
    report_rows = []
    for symbol in sorted(symbols):
        events = logs["entry_events"].get(symbol, [])
        margins = [event["margin"] for event in events]
        probs = [event["prob"] for event in events]
        thresholds = [event["threshold"] for event in events]
        total = len(events)
        near005 = sum(m <= NEAR_005 for m in margins)
        near010 = sum(m <= NEAR_010 for m in margins)
        near025 = sum(m <= NEAR_025 for m in margins)
        near050 = sum(m <= NEAR_050 for m in margins)
        long_entries = sum(1 for event in events if event["tag"] == "AI.LONG.ENTRY")
        short_entries = sum(1 for event in events if event["tag"] == "AI.SHORT.ENTRY")
        stats = trade_stats[symbol]
        row = {
            "symbol": symbol,
            "entry_pass": total,
            "long_entry": long_entries,
            "short_entry": short_entries,
            "near005": near005,
            "near010": near010,
            "near025": near025,
            "near050": near050,
            "near025_rate": (near025 / total) if total else 0.0,
            "p50_margin": statistics.median(margins) if margins else math.nan,
            "p25_margin": percentile(margins, 0.25),
            "p10_margin": percentile(margins, 0.10),
            "p90_prob": percentile(probs, 0.90),
            "median_threshold": statistics.median(thresholds) if thresholds else math.nan,
            "would_pass_068": sum(event["prob"] >= 0.68 for event in events),
            "would_pass_070": sum(event["prob"] >= 0.70 for event in events),
            "orders": logs["order_send"][symbol],
            "hard_stops": logs["hard_stop"][symbol],
            "entry_gate": logs["entry_gate"][symbol],
            "max_trade_count": logs["max_trade_count"][symbol],
            "groups": stats["groups"],
            "rough_pnl": pnl[symbol],
            "fast_reversals_180s": fast_transitions[symbol],
            "matched_qty": matched_qty[symbol],
            "matched_slices": matched_slices[symbol],
        }
        row["classification"] = classify(row)
        report_rows.append(row)

    high_risk = [row for row in report_rows if row["classification"] == "HIGH_RISK"]
    review = [row for row in report_rows if row["classification"] == "REVIEW"]
    watch = [row for row in report_rows if row["classification"] == "WATCH"]

    with open(OUT, "w", encoding="utf-8") as w:
        w.write(f"date={DATE}\n")
        w.write(f"log_root={LOG_ROOT}\n")
        w.write(f"log_files_scanned={len(logs['files'])}\n")
        w.write(f"unique_today_log_lines={logs['unique_log_lines']}\n")
        w.write(f"ibkr_csv={TRADE_CSV if os.path.exists(TRADE_CSV) else 'MISSING'}\n")
        w.write(f"symbols_with_entry_passes={sum(1 for row in report_rows if row['entry_pass'])}\n")
        w.write(f"total_entry_passes={sum(row['entry_pass'] for row in report_rows)}\n")
        w.write(f"entry_passes_margin_le_0.005={sum(row['near005'] for row in report_rows)}\n")
        w.write(f"entry_passes_margin_le_0.010={sum(row['near010'] for row in report_rows)}\n")
        w.write(f"entry_passes_margin_le_0.025={sum(row['near025'] for row in report_rows)}\n")
        w.write(f"entry_passes_margin_le_0.050={sum(row['near050'] for row in report_rows)}\n")
        w.write("near_margin_definition=probability - configured threshold; ENTRY tags only; duplicate exact log lines removed\n")
        w.write("\nRECOMMENDED_ACTIONS\n")
        w.write("pause_new_live_entries_high_risk=" + ",".join(row["symbol"] for row in sorted(high_risk, key=lambda r: (-r["near025"], r["rough_pnl"]))) + "\n")
        w.write("manual_calibration_review=" + ",".join(row["symbol"] for row in sorted(review, key=lambda r: (-r["near025"], r["rough_pnl"]))) + "\n")
        w.write("watch_only=" + ",".join(row["symbol"] for row in sorted(watch, key=lambda r: (-r["near025"], r["rough_pnl"]))) + "\n")
        w.write("classification_note=HIGH_RISK requires repeated near-threshold ENTRY passes plus traded churn/loss/stop evidence; REVIEW can include untraded near-threshold firers.\n")
        w.write("\nTOP_NEAR_THRESHOLD_ENTRY_FIRERS_MARGIN_LE_0.025\n")
        w.write("sym   cls    entry long short near005 near010 near025 near%  p10m   p25m   p50m   p90prob medThr pass>=.68 pass>=.70 orders stops groups fastRev pnl\n")
        for row in sorted(report_rows, key=lambda r: (-r["near025"], -r["entry_pass"], r["rough_pnl"]))[:40]:
            if row["entry_pass"] == 0:
                continue
            w.write(
                f"{row['symbol']:5s} {row['classification']:6s} {row['entry_pass']:5d} {row['long_entry']:4d} {row['short_entry']:5d} "
                f"{row['near005']:7d} {row['near010']:7d} {row['near025']:7d} {row['near025_rate']*100:5.1f} "
                f"{fmt_float(row['p10_margin']):>6s} {fmt_float(row['p25_margin']):>6s} {fmt_float(row['p50_margin']):>6s} "
                f"{fmt_float(row['p90_prob']):>7s} {fmt_float(row['median_threshold']):>6s} "
                f"{row['would_pass_068']:8d} {row['would_pass_070']:8d} {row['orders']:6d} {row['hard_stops']:5d} "
                f"{row['groups']:6d} {row['fast_reversals_180s']:7d} {row['rough_pnl']:8.2f}\n"
            )

        w.write("\nHIGH_NEAR_THRESHOLD_RATE_ENTRY_SYMBOLS_MIN_8_PASSES\n")
        w.write("sym   cls    entry near025 near%  p50m orders stops groups fastRev pnl\n")
        for row in sorted((r for r in report_rows if r["entry_pass"] >= 8), key=lambda r: (-r["near025_rate"], -r["near025"], r["rough_pnl"]))[:40]:
            w.write(
                f"{row['symbol']:5s} {row['classification']:6s} {row['entry_pass']:5d} {row['near025']:7d} {row['near025_rate']*100:5.1f} "
                f"{fmt_float(row['p50_margin']):>6s} {row['orders']:6d} {row['hard_stops']:5d} {row['groups']:6d} "
                f"{row['fast_reversals_180s']:7d} {row['rough_pnl']:8.2f}\n"
            )

        w.write("\nCHURN_LOSS_OVERLAP_WITH_NEAR_THRESHOLD_ENTRIES\n")
        w.write("sym   cls    pnl      groups stops orders near025 entry near% fastRev pass>=.70\n")
        for row in sorted((r for r in report_rows if r["groups"] or r["hard_stops"]), key=lambda r: (r["rough_pnl"], -r["groups"]))[:40]:
            w.write(
                f"{row['symbol']:5s} {row['classification']:6s} {row['rough_pnl']:8.2f} {row['groups']:6d} {row['hard_stops']:5d} "
                f"{row['orders']:6d} {row['near025']:7d} {row['entry_pass']:5d} {row['near025_rate']*100:5.1f} "
                f"{row['fast_reversals_180s']:7d} {row['would_pass_070']:8d}\n"
            )

        w.write("\nLOW_MARGIN_ENTRY_SAMPLES_BY_HIGH_RISK_SYMBOL\n")
        for row in sorted(high_risk, key=lambda r: (-r["near025"], r["rough_pnl"]))[:20]:
            w.write(f"[{row['symbol']}]\n")
            for line in logs["samples"].get(row["symbol"], [])[:4]:
                w.write("  " + line + "\n")

    print(OUT)


if __name__ == "__main__":
    main()



