#!/usr/bin/env python3
import csv
import gzip
import glob
import os
import re
import statistics
from collections import Counter, defaultdict, deque
from datetime import datetime

ROOT = "/Users/filmonghezehey/trading-agent/worktrees/databento"
LOG_ROOT = os.path.join(ROOT, "runtime/databento/logs")
TRADE_CSV = os.path.expanduser("~/Downloads/trades.20260522.csv")
OUT = os.path.join(ROOT, "runtime/databento/order_loss_investigation_20260522.txt")


def read_ibkr_executions():
	rows = []
	with open(TRADE_CSV, newline="", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		for row in reader:
			if row.get("Date") != "20260522" or row.get("Security Type") != "STK":
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
		sym = group["sym"]
		action = group["action"]
		time = datetime.strptime(group["time"], "%H:%M:%S")
		previous = last_by_symbol.get(sym)
		if previous and previous["action"] != action:
			seconds = (time - previous["dt"]).total_seconds()
			if seconds >= 0:
				transitions.append((sym, seconds, previous["action"] + "->" + action, previous["time"], group["time"]))
		last_by_symbol[sym] = {**group, "dt": time}
	return transitions


def parse_logs():
	files = glob.glob(os.path.join(LOG_ROOT, "trading-agent-*.log"))
	files += glob.glob(os.path.join(LOG_ROOT, "trading-agent-*.log.2026-05-22.*.gz"))
	re_symbol = re.compile(r"symbol=([A-Z][A-Z0-9.]*)")
	re_order = re.compile(r"ORDER\.SEND.*?orderId=([-0-9]+) action=(BUY|SELL) type=([A-Z_]+)")
	re_ai = re.compile(r"\[FLOW\]\[COND\]\[(AI\.(?:LONG|SHORT)\.(?:ENTRY|EXIT))\].*?=PASS.*?symbol=([A-Z0-9.]+).*?prob=([0-9.]+).*?threshold=([0-9.]+)")
	re_entry_gate = re.compile(r"ENTRY_GATE_OPEN=PASS.*?symbol=([A-Z0-9.]+).*?tradeCount=([0-9]+).*?maxTrades=([0-9]+)")
	order_send = Counter()
	order_type = Counter()
	hard_stop = Counter()
	ai_pass = Counter()
	entry_gate = Counter()
	max_trade_count = Counter()
	min_threshold = defaultdict(lambda: 9.0)
	low_margin_ai = []
	samples = defaultdict(list)

	re_file_symbol = re.compile(r"trading-agent-([A-Z0-9.]+)\.log")
	for path in files:
		file_symbol_match = re_file_symbol.search(os.path.basename(path))
		file_symbol = file_symbol_match.group(1) if file_symbol_match else None
		opener = gzip.open if path.endswith(".gz") else open
		with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
			for line in f:
				if "2026-05-22" not in line:
					continue
				symbol_match = re_symbol.search(line)
				symbol = symbol_match.group(1) if symbol_match else file_symbol
				if "ORDER.SEND" in line:
					order_match = re_order.search(line)
					if order_match and symbol:
						order_send[symbol] += 1
						order_type[(symbol, order_match.group(3))] += 1
						if len(samples[symbol]) < 4:
							samples[symbol].append(line.strip())
				if (
					"LONG_HARD_STOP_TRIGGER=PASS" in line
					or "SHORT_HARD_STOP_TRIGGER=PASS" in line
					or "HARD_STOP_TRIGGERED=PASS" in line
				) and symbol:
					hard_stop[symbol] += 1
				ai_match = re_ai.search(line)
				if ai_match:
					tag, ai_symbol, prob_text, threshold_text = ai_match.groups()
					probability = float(prob_text)
					threshold = float(threshold_text)
					ai_pass[(ai_symbol, tag)] += 1
					min_threshold[tag] = min(min_threshold[tag], threshold)
					margin = probability - threshold
					if margin < 0.025:
						low_margin_ai.append((margin, ai_symbol, tag, probability, threshold))
				gate_match = re_entry_gate.search(line)
				if gate_match:
					gate_symbol = gate_match.group(1)
					trade_count = int(gate_match.group(2))
					entry_gate[gate_symbol] += 1
					max_trade_count[gate_symbol] = max(max_trade_count[gate_symbol], trade_count)
	return {
		"files": files,
		"order_send": order_send,
		"order_type": order_type,
		"hard_stop": hard_stop,
		"ai_pass": ai_pass,
		"entry_gate": entry_gate,
		"max_trade_count": max_trade_count,
		"min_threshold": min_threshold,
		"low_margin_ai": low_margin_ai,
		"samples": samples,
	}


def main():
	rows, groups = read_ibkr_executions()
	pnl, matched_qty, matched_slices = rough_fifo(groups)
	transitions = transition_times(groups)
	logs = parse_logs()
	by_symbol = defaultdict(lambda: {"groups": 0, "rows": 0, "notional": 0.0, "bot": 0, "sld": 0, "net": 0})
	for group in groups:
		data = by_symbol[group["sym"]]
		data["groups"] += 1
		data["rows"] += group["rows"]
		data["notional"] += group["notional"]
		data[group["action"].lower()] += 1
		data["net"] += group["qty"] if group["action"] == "BOT" else -group["qty"]

	total_pnl = sum(pnl.values())
	wins = sum(1 for value in pnl.values() if value > 0)
	losses = sum(1 for value in pnl.values() if value < 0)
	transition_seconds = [item[1] for item in transitions]

	with open(OUT, "w", encoding="utf-8") as w:
		w.write("date=2026-05-22\n")
		w.write(f"ibkr_csv={TRADE_CSV}\n")
		w.write(f"ibkr_execution_rows={len(rows)}\n")
		w.write(f"ibkr_execution_groups_split_fills_collapsed={len(groups)}\n")
		w.write(f"ibkr_symbols={len(by_symbol)}\n")
		w.write(f"gross_shares={sum(row['qty'] for row in rows)}\n")
		w.write(f"gross_notional={sum(row['notional'] for row in rows):.2f}\n")
		w.write(f"rough_flat_start_fifo_pnl={total_pnl:.2f}\n")
		w.write(f"rough_win_loss_symbols={wins}/{losses}\n")
		if transition_seconds:
			w.write(
				"opposite_side_group_transitions="
				f"{len(transition_seconds)} median_seconds={statistics.median(transition_seconds):.1f} "
				f"under_60s={sum(sec <= 60 for sec in transition_seconds)} "
				f"under_180s={sum(sec <= 180 for sec in transition_seconds)}\n"
			)
		w.write(f"log_files_scanned={len(logs['files'])}\n")
		w.write(f"log_order_send_total={sum(logs['order_send'].values())}\n")
		w.write(f"log_hard_stop_total={sum(logs['hard_stop'].values())}\n")
		w.write(f"log_entry_gate_pass_total={sum(logs['entry_gate'].values())}\n")
		w.write("min_ai_thresholds=" + ",".join(f"{k}:{v:.4f}" for k, v in sorted(logs["min_threshold"].items())) + "\n")
		w.write(f"low_margin_ai_passes_lt_0.025={len(logs['low_margin_ai'])}\n")

		w.write("\nTOP_SYMBOLS_BY_IBKR_EXEC_GROUPS\n")
		for symbol, data in sorted(by_symbol.items(), key=lambda item: item[1]["groups"], reverse=True)[:30]:
			tags = {tag: count for (s, tag), count in logs["ai_pass"].items() if s == symbol}
			w.write(
				f"{symbol:5s} groups={data['groups']:3d} rows={data['rows']:3d} net={data['net']:6d} "
				f"BOT={data['bot']:3d} SLD={data['sld']:3d} notional={data['notional']:10.2f} "
				f"roughPnl={pnl[symbol]:9.2f} logOrders={logs['order_send'][symbol]:3d} "
				f"hardStops={logs['hard_stop'][symbol]:3d} entryGate={logs['entry_gate'][symbol]:3d} "
				f"maxTradeCount={logs['max_trade_count'][symbol]:3d} aiPass={tags}\n"
			)

		w.write("\nWORST_ROUGH_FIFO_PNL\n")
		for symbol, value in sorted(pnl.items(), key=lambda item: item[1])[:30]:
			data = by_symbol[symbol]
			w.write(
				f"{symbol:5s} roughPnl={value:9.2f} matchedQty={matched_qty[symbol]:6d} "
				f"matchedSlices={matched_slices[symbol]:3d} groups={data['groups']:3d} "
				f"logOrders={logs['order_send'][symbol]:3d} hardStops={logs['hard_stop'][symbol]:3d}\n"
			)

		w.write("\nFAST_OPPOSITE_SIDE_TRANSITIONS_SAMPLE\n")
		for symbol, seconds, transition, start, end in sorted(transitions, key=lambda item: item[1])[:40]:
			w.write(f"{symbol:5s} dt={seconds:5.1f}s {transition} {start}->{end}\n")

		w.write("\nLOW_MARGIN_AI_PASS_SAMPLE\n")
		for margin, symbol, tag, probability, threshold in sorted(logs["low_margin_ai"])[:30]:
			w.write(f"{symbol:5s} {tag:14s} prob={probability:.4f} threshold={threshold:.4f} margin={margin:.4f}\n")

		w.write("\nSAMPLE_ORDER_EVIDENCE_TOP_8\n")
		for symbol, _data in sorted(by_symbol.items(), key=lambda item: item[1]["groups"], reverse=True)[:8]:
			w.write(f"[{symbol}]\n")
			for line in logs["samples"][symbol][:4]:
				w.write("  " + line + "\n")

	print(OUT)


if __name__ == "__main__":
	main()


