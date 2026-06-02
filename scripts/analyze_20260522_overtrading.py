#!/usr/bin/env python3
"""Analyze 2026-05-22 overtrading from IBKR fills and bot logs.

The IBKR CSV is expected at ~/Downloads/trades.20260522.csv by default.
The script writes a compact text report under runtime/databento so the
investigation can be re-run without relying on terminal scrollback.
"""

from __future__ import annotations

import argparse
import collections
import csv
import re
from dataclasses import dataclass
from pathlib import Path


ORDER_SEND_RE = re.compile(r"ORDER\.SEND.*?(?:orderId|sharedGateway orderId)=(-?\d+).*? action=(BUY|SELL).*? type=([A-Z_]+)")
ORDER_SUBMITTED_RE = re.compile(r"STRATEGY\.ORDER.*?submitted orderId=(-?\d+) action=(BUY|SELL) qty=(\d+) symbol=([A-Z]+)")
ORDER_FILL_RE = re.compile(r"STRATEGY\.ORDER.*?fill orderId=(-?\d+) action=(BUY|SELL) newPos=(-?\d+) netPnL=([-0-9.]+) dailyPnL=([-0-9.]+)")
ORDER_CLOSED_RE = re.compile(r"STRATEGY\.ORDER.*?closed orderId=(-?\d+) status=([^ ]+).*?symbol=([A-Z]+)")
AI_PASS_RE = re.compile(r"\[(AI\.(?:LONG|SHORT)\.(?:ENTRY|EXIT))\].*?(AI_PREDICTS_(?:ENTRY|EXIT))=PASS.*?prob=([0-9.]+) threshold=([0-9.]+)")
HARD_STOP_RE = re.compile(r"\[STRATEGY\.STOP\].*?((?:LONG|SHORT)_HARD_STOP_TRIGGER)=PASS")
ENTRY_GATE_RE = re.compile(r"\[AI\.ENTRY\].*?ENTRY_GATE_OPEN=(PASS|FAIL).*?tradeCount=(\d+) maxTrades=(\d+)")


@dataclass
class FillRow:
	symbol: str
	action: str
	quantity: int
	price: float
	time: str
	date: str

	@property
	def signed_quantity(self) -> int:
		return self.quantity if self.action == "BOT" else -self.quantity


@dataclass
class FillGroup:
	symbol: str
	action: str
	time: str
	price: float
	quantity: int
	fills: int

	@property
	def signed_quantity(self) -> int:
		return self.quantity if self.action == "BOT" else -self.quantity


def read_ibkr_csv(path: Path) -> list[FillRow]:
	rows: list[FillRow] = []
	with path.open(newline="", errors="replace") as handle:
		for row in csv.DictReader(handle):
			symbol = (row.get("Symbol") or "").strip().upper()
			action = (row.get("Action") or "").strip().upper()
			if not symbol or action not in {"BOT", "SLD"}:
				continue
			rows.append(
				FillRow(
					symbol=symbol,
					action=action,
					quantity=int(float(row["Quantity"])),
					price=float(row["Price"]),
					time=row["Time"],
					date=row["Date"],
				)
			)
	return rows


def group_fills(rows: list[FillRow]) -> list[FillGroup]:
	grouped: dict[tuple[str, str, str, float], list[FillRow]] = collections.defaultdict(list)
	for row in rows:
		# IBKR often splits one order across venues; same symbol/action/second/price is a useful lower-noise proxy.
		grouped[(row.symbol, row.action, row.time, round(row.price, 4))].append(row)
	return [
		FillGroup(symbol=symbol, action=action, time=time, price=price, quantity=sum(r.quantity for r in rows), fills=len(rows))
		for (symbol, action, time, price), rows in grouped.items()
	]


def rough_fifo_pnl(groups: list[FillGroup]) -> dict[str, dict[str, object]]:
	by_symbol: dict[str, list[FillGroup]] = collections.defaultdict(list)
	for group in groups:
		by_symbol[group.symbol].append(group)

	out: dict[str, dict[str, object]] = {}
	for symbol, symbol_groups in by_symbol.items():
		lots: list[tuple[int, int, float]] = []  # sign, qty, price
		pnl = 0.0
		matched_qty = 0
		matched_slices = 0
		for group in sorted(symbol_groups, key=lambda g: g.time):
			qty = group.quantity
			price = group.price
			sign = 1 if group.action == "BOT" else -1
			while qty > 0 and lots and lots[0][0] == -sign:
				lot_sign, lot_qty, lot_price = lots[0]
				match_qty = min(qty, lot_qty)
				pnl += (price - lot_price) * match_qty * lot_sign
				matched_qty += match_qty
				matched_slices += 1
				qty -= match_qty
				lot_qty -= match_qty
				if lot_qty == 0:
					lots.pop(0)
				else:
					lots[0] = (lot_sign, lot_qty, lot_price)
			if qty > 0:
				lots.append((sign, qty, price))
		if matched_qty:
			out[symbol] = {
				"pnl": pnl,
				"matched_qty": matched_qty,
				"matched_slices": matched_slices,
				"ending_lots": lots,
			}
	return out


def scan_logs(log_dir: Path, symbols: set[str]) -> dict[str, dict[str, object]]:
	stats: dict[str, dict[str, object]] = {
		symbol: {
			"order_send": 0,
			"submitted": 0,
			"fills": 0,
			"closed": 0,
			"ai_pass": collections.Counter(),
			"hard_stops": 0,
			"entry_gate_pass": 0,
			"entry_gate_fail": 0,
			"max_trade_count_seen": 0,
			"sample_order_lines": [],
			"sample_ai_lines": [],
			"sample_stop_lines": [],
		}
		for symbol in symbols
	}

	for symbol in sorted(symbols):
		path = log_dir / f"{symbol.lower()}_live_trade_logs.txt"
		if not path.exists():
			continue
		data = stats[symbol]
		with path.open(errors="replace") as handle:
			for line in handle:
				if "2026-05-22" not in line:
					continue
				if "ORDER.SEND" in line and ORDER_SEND_RE.search(line):
					data["order_send"] += 1
					if len(data["sample_order_lines"]) < 3:
						data["sample_order_lines"].append(line.strip())
				if "STRATEGY.ORDER" in line:
					if ORDER_SUBMITTED_RE.search(line):
						data["submitted"] += 1
					if ORDER_FILL_RE.search(line):
						data["fills"] += 1
					if ORDER_CLOSED_RE.search(line):
						data["closed"] += 1
				ai_match = AI_PASS_RE.search(line)
				if ai_match:
					data["ai_pass"][ai_match.group(1)] += 1
					if len(data["sample_ai_lines"]) < 3:
						data["sample_ai_lines"].append(line.strip())
				if HARD_STOP_RE.search(line):
					data["hard_stops"] += 1
					if len(data["sample_stop_lines"]) < 3:
						data["sample_stop_lines"].append(line.strip())
				gate_match = ENTRY_GATE_RE.search(line)
				if gate_match:
					if gate_match.group(1) == "PASS":
						data["entry_gate_pass"] += 1
					else:
						data["entry_gate_fail"] += 1
					data["max_trade_count_seen"] = max(data["max_trade_count_seen"], int(gate_match.group(2)))
	return stats


def main() -> int:
	parser = argparse.ArgumentParser()
	parser.add_argument("--trades", default="/Users/filmonghezehey/Downloads/trades.20260522.csv")
	parser.add_argument("--log-dir", default="runtime")
	parser.add_argument("--out", default="runtime/databento/overtrading_investigation_20260522.txt")
	args = parser.parse_args()

	trades_path = Path(args.trades).expanduser()
	rows = read_ibkr_csv(trades_path)
	groups = group_fills(rows)
	by_symbol_rows: dict[str, list[FillRow]] = collections.defaultdict(list)
	by_symbol_groups: dict[str, list[FillGroup]] = collections.defaultdict(list)
	for row in rows:
		by_symbol_rows[row.symbol].append(row)
	for group in groups:
		by_symbol_groups[group.symbol].append(group)
	pnl = rough_fifo_pnl(groups)
	symbols = set(by_symbol_rows)
	log_stats = scan_logs(Path(args.log_dir), symbols)

	lines: list[str] = []
	lines.append("date=2026-05-22")
	lines.append(f"ibkr_csv={trades_path}")
	lines.append(f"ibkr_fill_rows={len(rows)}")
	lines.append(f"ibkr_symbols={len(symbols)}")
	lines.append(f"order_like_groups_same_symbol_side_second_price={len(groups)}")
	lines.append(f"gross_shares={sum(r.quantity for r in rows)}")
	lines.append(f"gross_notional={sum(r.quantity * r.price for r in rows):.2f}")
	lines.append("")

	total_rough = sum(v["pnl"] for v in pnl.values())
	winners = sum(1 for v in pnl.values() if v["pnl"] > 0)
	losers = sum(1 for v in pnl.values() if v["pnl"] < 0)
	lines.append("rough_intraday_fifo_pnl_assuming_flat_start=%.2f" % total_rough)
	lines.append(f"rough_symbols_win_loss={winners}/{losers}")
	lines.append("note=rough FIFO ignores pre-existing broker avgCost from startup sync, so use it for churn direction, not tax/exact PnL")
	lines.append("")

	lines.append("top_symbols_by_order_like_groups:")
	for symbol, symbol_groups in sorted(by_symbol_groups.items(), key=lambda item: len(item[1]), reverse=True)[:30]:
		row_count = len(by_symbol_rows[symbol])
		group_count = len(symbol_groups)
		sides = collections.Counter(g.action for g in symbol_groups)
		net = sum(g.signed_quantity for g in symbol_groups)
		gross_notional = sum(abs(g.quantity * g.price) for g in symbol_groups)
		symbol_pnl = pnl.get(symbol, {}).get("pnl")
		pnl_text = "NA" if symbol_pnl is None else f"{symbol_pnl:.2f}"
		stats = log_stats.get(symbol, {})
		ai_pass = stats.get("ai_pass", collections.Counter())
		lines.append(
			f"  {symbol:5s} groups={group_count:3d} rows={row_count:3d} net={net:5d} "
			f"sides={dict(sides)} grossNotional={gross_notional:10.2f} roughPnl={pnl_text:>9s} "
			f"logOrderSend={stats.get('order_send', 0):3d} aiPass={dict(ai_pass)} hardStops={stats.get('hard_stops', 0):3d} "
			f"entryGatePass={stats.get('entry_gate_pass', 0):3d} maxTradeCountSeen={stats.get('max_trade_count_seen', 0)}"
		)
	lines.append("")

	lines.append("worst_rough_intraday_pnl_symbols:")
	for symbol, vals in sorted(pnl.items(), key=lambda item: item[1]["pnl"])[:25]:
		stats = log_stats.get(symbol, {})
		lines.append(
			f"  {symbol:5s} roughPnl={vals['pnl']:9.2f} matchedQty={vals['matched_qty']:5d} "
			f"matchedSlices={vals['matched_slices']:3d} groups={len(by_symbol_groups[symbol]):3d} "
			f"logOrderSend={stats.get('order_send', 0):3d} hardStops={stats.get('hard_stops', 0):3d} aiPass={dict(stats.get('ai_pass', collections.Counter()))}"
		)
	lines.append("")

	lines.append("sample_log_evidence_by_symbol:")
	for symbol, stats in sorted(log_stats.items(), key=lambda item: item[1].get("order_send", 0), reverse=True)[:12]:
		if not stats.get("order_send") and not stats.get("hard_stops"):
			continue
		lines.append(f"  [{symbol}] orderSend={stats.get('order_send', 0)} submitted={stats.get('submitted', 0)} fills={stats.get('fills', 0)} closed={stats.get('closed', 0)}")
		for sample in stats.get("sample_ai_lines", [])[:2]:
			lines.append(f"    AI: {sample}")
		for sample in stats.get("sample_stop_lines", [])[:2]:
			lines.append(f"    STOP: {sample}")
		for sample in stats.get("sample_order_lines", [])[:2]:
			lines.append(f"    ORDER: {sample}")

	out = Path(args.out)
	out.parent.mkdir(parents=True, exist_ok=True)
	out.write_text("\n".join(lines) + "\n")
	print(out)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())

