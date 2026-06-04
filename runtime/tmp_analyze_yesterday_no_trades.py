from __future__ import annotations

import json
import re
from pathlib import Path

DAY = "2026-04-23"
ROOT = Path(__file__).resolve().parents[1] / "runtime"
PATTERN = "*_live_trade_logs_2026-04-23.txt"
OUT = ROOT / "yesterday_no_trades_analysis.json"

entry_re = re.compile(r"\[FLOW\]\[COND\]\[(AI\.(LONG|SHORT)\.ENTRY)\]\s+AI_PREDICTS_ENTRY=(PASS|FAIL)\s+\|\s+symbol=([A-Z0-9._-]+).*?prob=([0-9.]+)\s+threshold=([0-9.]+)")
fire_re = re.compile(r"\[FLOW\]\[INFO\]\[(AI\.(LONG|SHORT)\.ENTRY)\]\s+(Dip buyer|Rip seller) firing order symbol=([A-Z0-9._-]+)")
order_send_re = re.compile(r"\[FLOW\]\[DATA\]\[ORDER\.SEND\]\s+(sharedGateway\s+)?orderId=")
submit_fail_re = re.compile(r"\[ERROR\]\[IBKR\.GATEWAY\]\s+submit_order failed reason=(.*)")
submit_reject_re = re.compile(r"\[ERROR\]\[IBKR\.GATEWAY\]\s+submit_order rejected detail=(.*)")
fallback_re = re.compile(r"Falling back to direct IBKR order placement")
boot_shared_re = re.compile(r"\[FLOW\]\[DATA\]\[BOOT\]\s+ibkr sharedGatewayEnabled=(true|false).*skipDirectConnection=(true|false)")
entry_gate_fail_re = re.compile(r"\[FLOW\]\[COND\]\[AI\.ENTRY\]\s+ENTRY_GATE_OPEN=FAIL\s+\|\s+symbol=([A-Z0-9._-]+)\s+allowNewEntries=(true|false)\s+tradeCount=(\d+)\s+maxTrades=(\d+)\s+positionSynced=(true|false)")
position_sync_re = re.compile(r"\[FLOW\]\[COND\]\[AI\.ENTRY\]\s+POSITION_SYNCED=(PASS|FAIL)\s+\|\s+symbol=([A-Z0-9._-]+)\s+positionSynced=(true|false)")


def update_best(best: dict[str, object] | None, passed: bool, prob: float, threshold: float, line: str) -> dict[str, object]:
    margin = round(prob - threshold, 6)
    rec = {
        "pass": passed,
        "prob": prob,
        "threshold": threshold,
        "margin": margin,
        "line": line.strip(),
    }
    if best is None or (margin, prob) > (best["margin"], best["prob"]):
        return rec
    return best


per_symbol: dict[str, dict[str, object]] = {}
for path in sorted(ROOT.glob(PATTERN)):
    symbol = path.name.split("_live_trade_logs_")[0].upper()
    data = per_symbol.setdefault(
        symbol,
        {
            "file": str(path),
            "boot": {"sharedGatewayEnabled": None, "skipDirectConnection": None},
            "long": {"count": 0, "best": None},
            "short": {"count": 0, "best": None},
            "firing_orders": [],
            "order_send_count": 0,
            "submit_failures": [],
            "submit_rejections": [],
            "direct_fallback_count": 0,
            "entry_gate_failures": [],
            "position_sync_fail_count": 0,
        },
    )
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.startswith(DAY):
                continue
            if match := boot_shared_re.search(line):
                data["boot"] = {
                    "sharedGatewayEnabled": match.group(1) == "true",
                    "skipDirectConnection": match.group(2) == "true",
                }
            if match := entry_re.search(line):
                branch = "long" if match.group(2) == "LONG" else "short"
                passed = match.group(3) == "PASS"
                prob = float(match.group(5))
                threshold = float(match.group(6))
                branch_data = data[branch]
                branch_data["count"] += 1
                branch_data["best"] = update_best(branch_data["best"], passed, prob, threshold, line)
            if match := fire_re.search(line):
                data["firing_orders"].append({
                    "side": "LONG" if match.group(2) == "LONG" else "SHORT",
                    "line": line.strip(),
                })
            if order_send_re.search(line):
                data["order_send_count"] += 1
            if match := submit_fail_re.search(line):
                data["submit_failures"].append(match.group(1))
            if match := submit_reject_re.search(line):
                data["submit_rejections"].append(match.group(1))
            if fallback_re.search(line):
                data["direct_fallback_count"] += 1
            if match := entry_gate_fail_re.search(line):
                data["entry_gate_failures"].append({
                    "allowNewEntries": match.group(2) == "true",
                    "tradeCount": int(match.group(3)),
                    "maxTrades": int(match.group(4)),
                    "positionSynced": match.group(5) == "true",
                    "line": line.strip(),
                })
            if match := position_sync_re.search(line):
                if match.group(3) == "false":
                    data["position_sync_fail_count"] += 1

summary = {
    "day": DAY,
    "symbol_files": len(per_symbol),
    "symbols_with_shared_gateway_skip_direct": sum(
        1
        for data in per_symbol.values()
        if data["boot"]["sharedGatewayEnabled"] and data["boot"]["skipDirectConnection"]
    ),
    "symbols_with_any_entry_pass": sum(
        1
        for data in per_symbol.values()
        if (data["long"]["best"] and data["long"]["best"]["pass"])
        or (data["short"]["best"] and data["short"]["best"]["pass"])
    ),
    "symbols_with_firing_orders": sum(1 for data in per_symbol.values() if data["firing_orders"]),
    "symbols_with_order_send": sum(1 for data in per_symbol.values() if data["order_send_count"] > 0),
    "symbols_with_submit_failures": sum(
        1 for data in per_symbol.values() if data["submit_failures"] or data["submit_rejections"]
    ),
    "total_firing_orders": sum(len(data["firing_orders"]) for data in per_symbol.values()),
    "total_order_send": sum(data["order_send_count"] for data in per_symbol.values()),
    "total_submit_failures": sum(len(data["submit_failures"]) for data in per_symbol.values()),
    "total_submit_rejections": sum(len(data["submit_rejections"]) for data in per_symbol.values()),
    "total_direct_fallbacks": sum(data["direct_fallback_count"] for data in per_symbol.values()),
    "symbols_blocked_by_position_sync": sum(
        1
        for data in per_symbol.values()
        if data["position_sync_fail_count"] > 0 and not data["firing_orders"]
    ),
}

firing_but_no_send = []
all_symbols = []
for symbol, data in sorted(per_symbol.items()):
    row = {
        "symbol": symbol,
        "long_best": data["long"]["best"],
        "short_best": data["short"]["best"],
        "firing_orders": len(data["firing_orders"]),
        "order_send_count": data["order_send_count"],
        "submit_failures": len(data["submit_failures"]),
        "submit_rejections": len(data["submit_rejections"]),
        "skipDirectConnection": data["boot"]["skipDirectConnection"],
        "position_sync_fail_count": data["position_sync_fail_count"],
    }
    all_symbols.append(row)
    if data["firing_orders"] and data["order_send_count"] == 0:
        firing_but_no_send.append(row)

rank_long = sorted(
    [row for row in all_symbols if row["long_best"] is not None],
    key=lambda row: (row["long_best"]["margin"], row["long_best"]["prob"]),
    reverse=True,
)
rank_short = sorted(
    [row for row in all_symbols if row["short_best"] is not None],
    key=lambda row: (row["short_best"]["margin"], row["short_best"]["prob"]),
    reverse=True,
)

report = {
    "summary": summary,
    "firing_but_no_send": firing_but_no_send,
    "top_long_by_margin": rank_long[:25],
    "top_short_by_margin": rank_short[:25],
    "all_symbols": all_symbols,
}
OUT.write_text(json.dumps(report, indent=2))
print(OUT)

