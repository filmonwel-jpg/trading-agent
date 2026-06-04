#!/usr/bin/env python3
from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path

DATE_PREFIX = "2026-05-22"
ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "runtime" / "databento" / "logs"

ORDER_RE = re.compile(r"^([^ ]+).*\[FLOW\]\[DATA\]\[ORDER\.SEND\] (?P<msg>.*)$")
FLOW_RE = re.compile(r"^([^ ]+).*\[FLOW\]\[(?P<level>[^]]+)\]\[(?P<tag>[^]]+)\] (?P<msg>.*)$")
PRED_RE = re.compile(
    r"\[(?P<tag>AI\.(?:LONG|SHORT)\.(?:ENTRY|EXIT))\] "
    r"AI_PREDICTS_(?P<kind>ENTRY|EXIT)=PASS .*?"
    r"prob=(?P<prob>[0-9.]+) threshold=(?P<threshold>[0-9.]+)"
)
SYMBOL_RE = re.compile(r"symbol=([A-Z][A-Z0-9.]*)")
ACTION_RE = re.compile(r"action=(BUY|SELL)")
TYPE_RE = re.compile(r"type=([A-Z_]+)")
ORDER_ID_RE = re.compile(r"orderId=(-?\d+)")


def seconds_of_day(timestamp: str) -> float:
    # Example: 2026-05-22T07:42:31.491-06:00
    time_part = timestamp.split("T", 1)[1].split("-", 1)[0].split("+", 1)[0]
    hms, _, frac = time_part.partition(".")
    hh, mm, ss = [int(x) for x in hms.split(":")]
    return hh * 3600 + mm * 60 + ss + (float("0." + frac) if frac else 0.0)


def parse_symbol_from_path(path: Path) -> str:
    name = path.name
    return name.replace("trading-agent-", "").replace(".log", "")


def classify_order(action: str, order_type: str, preceding_text: str) -> str:
    if "Dip buyer firing order" in preceding_text or "Rip seller firing order" in preceding_text:
        return "ai_entry"
    if "Top detector signaled exit" in preceding_text or "Bottom detector signaled cover" in preceding_text:
        return "ai_exit"
    if "HARD_STOP_TRIGGER" in preceding_text or "STRATEGY.STOP" in preceding_text:
        return "hard_stop_or_risk_exit"
    if order_type == "FAST_LMT":
        return "likely_ai_entry_unmatched"
    if order_type == "MKT":
        return "likely_exit_unmatched"
    return "unmatched"


def compatible(pred: dict, action: str, order_class: str) -> bool:
    tag = pred["tag"]
    if order_class in {"ai_entry", "likely_ai_entry_unmatched"}:
        return (
            (action == "BUY" and tag == "AI.LONG.ENTRY")
            or (action == "SELL" and tag == "AI.SHORT.ENTRY")
        )
    if order_class in {"ai_exit", "likely_exit_unmatched"}:
        return (
            (action == "SELL" and tag == "AI.LONG.EXIT")
            or (action == "BUY" and tag == "AI.SHORT.EXIT")
        )
    return False


def main() -> int:
    orders: list[dict] = []
    ai_pass_events: list[dict] = []
    threshold_events = 0

    for path in sorted(LOG_DIR.glob("trading-agent-*.log")):
        symbol = parse_symbol_from_path(path)
        history: list[dict] = []
        symbol_ai_passes: list[dict] = []
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line_no, line in enumerate(lines, start=1):
            if not line.startswith(DATE_PREFIX):
                continue
            flow_match = FLOW_RE.match(line)
            if flow_match:
                timestamp = line.split(" ", 1)[0]
                tag = flow_match.group("tag")
                msg = flow_match.group("msg")
                history.append({
                    "timestamp": timestamp,
                    "seconds": seconds_of_day(timestamp),
                    "line_no": line_no,
                    "tag": tag,
                    "msg": msg,
                    "raw": line,
                })
                if "threshold=" in msg and "prob=" in msg:
                    threshold_events += 1
                pred_match = PRED_RE.search(line)
                if pred_match:
                    event = {
                        "file": str(path),
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "seconds": seconds_of_day(timestamp),
                        "line_no": line_no,
                        "tag": pred_match.group("tag"),
                        "kind": pred_match.group("kind"),
                        "prob": float(pred_match.group("prob")),
                        "threshold": float(pred_match.group("threshold")),
                        "raw": line,
                    }
                    symbol_ai_passes.append(event)
                    ai_pass_events.append(event)

            order_match = ORDER_RE.match(line)
            if not order_match:
                continue
            timestamp = line.split(" ", 1)[0]
            msg = order_match.group("msg")
            action_match = ACTION_RE.search(msg)
            type_match = TYPE_RE.search(msg)
            order_id_match = ORDER_ID_RE.search(msg)
            action = action_match.group(1) if action_match else ""
            order_type = type_match.group(1) if type_match else ""
            order_id = order_id_match.group(1) if order_id_match else ""
            recent_history = [h for h in history if 0 <= seconds_of_day(timestamp) - h["seconds"] <= 90]
            recent_text = "\n".join(h["raw"] for h in recent_history[-80:])
            order_class = classify_order(action, order_type, recent_text)
            matching_passes = [
                p for p in symbol_ai_passes
                if 0 <= seconds_of_day(timestamp) - p["seconds"] <= 90 and compatible(p, action, order_class)
            ]
            pred = matching_passes[-1] if matching_passes else None
            orders.append({
                "file": str(path),
                "symbol": symbol,
                "timestamp": timestamp,
                "seconds": seconds_of_day(timestamp),
                "line_no": line_no,
                "orderId": order_id,
                "action": action,
                "type": order_type,
                "class": order_class,
                "pred": pred,
                "raw": line,
            })

    class_counts = Counter(o["class"] for o in orders)
    ai_orders = [o for o in orders if o["class"] in {"ai_entry", "ai_exit", "likely_ai_entry_unmatched", "likely_exit_unmatched"}]
    ai_with_pred = [o for o in ai_orders if o["pred"] is not None]
    ai_without_pred = [o for o in ai_orders if o["pred"] is None]
    violations_prob_lt_threshold = [o for o in ai_with_pred if o["pred"]["prob"] + 1e-12 < o["pred"]["threshold"]]
    violations_threshold_lt_060 = [o for o in ai_with_pred if o["pred"]["threshold"] + 1e-12 < 0.60]
    violations_prob_lt_060 = [o for o in ai_with_pred if o["pred"]["prob"] + 1e-12 < 0.60]

    min_threshold_by_tag: dict[str, float] = {}
    min_prob_by_tag: dict[str, float] = {}
    ai_order_by_tag = Counter()
    for order in ai_with_pred:
        pred = order["pred"]
        tag = pred["tag"]
        ai_order_by_tag[tag] += 1
        min_threshold_by_tag[tag] = min(min_threshold_by_tag.get(tag, pred["threshold"]), pred["threshold"])
        min_prob_by_tag[tag] = min(min_prob_by_tag.get(tag, pred["prob"]), pred["prob"])

    print(f"date={DATE_PREFIX}")
    print(f"log_files_scanned={len(list(LOG_DIR.glob('trading-agent-*.log')))}")
    print(f"today_order_send_count={len(orders)}")
    print("order_class_counts=" + ",".join(f"{k}:{class_counts[k]}" for k in sorted(class_counts)))
    print(f"today_ai_pass_events={len(ai_pass_events)}")
    print(f"today_prob_threshold_log_events={threshold_events}")
    print(f"ai_or_likely_ai_orders={len(ai_orders)}")
    print(f"ai_or_likely_ai_orders_with_matching_pass={len(ai_with_pred)}")
    print(f"ai_or_likely_ai_orders_without_matching_pass={len(ai_without_pred)}")
    print(f"violations_prob_lt_threshold={len(violations_prob_lt_threshold)}")
    print(f"violations_threshold_lt_0_60={len(violations_threshold_lt_060)}")
    print(f"violations_prob_lt_0_60={len(violations_prob_lt_060)}")
    print("ai_order_counts_by_model_tag=" + ",".join(f"{k}:{ai_order_by_tag[k]}" for k in sorted(ai_order_by_tag)))
    print("min_threshold_by_model_tag=" + ",".join(f"{k}:{min_threshold_by_tag[k]:.4f}" for k in sorted(min_threshold_by_tag)))
    print("min_prob_by_model_tag=" + ",".join(f"{k}:{min_prob_by_tag[k]:.4f}" for k in sorted(min_prob_by_tag)))

    def print_examples(label: str, rows: list[dict], limit: int = 12) -> None:
        if not rows:
            return
        print(f"{label}_examples:")
        for order in rows[:limit]:
            pred = order.get("pred")
            pred_text = "no_pred"
            if pred is not None:
                pred_text = f"{pred['tag']} prob={pred['prob']:.4f} threshold={pred['threshold']:.4f} pred_time={pred['timestamp']}"
            print(
                f"  {order['timestamp']} {order['symbol']} orderId={order['orderId']} "
                f"action={order['action']} type={order['type']} class={order['class']} {pred_text}"
            )

    print_examples("violations_prob_lt_threshold", violations_prob_lt_threshold)
    print_examples("violations_threshold_lt_0_60", violations_threshold_lt_060)
    print_examples("violations_prob_lt_0_60", violations_prob_lt_060)
    print_examples("unmatched_ai_like", ai_without_pred)

    print("sample_valid_ai_orders:")
    for order in ai_with_pred[:20]:
        pred = order["pred"]
        print(
            f"  {order['timestamp']} {order['symbol']} orderId={order['orderId']} "
            f"action={order['action']} type={order['type']} class={order['class']} "
            f"{pred['tag']} prob={pred['prob']:.4f} threshold={pred['threshold']:.4f} pred_time={pred['timestamp']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

