#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1] / "runtime" / "databento" / "logs"
DAY = "2026-04-23"

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{2}:\d{2})")
BAR_RE = re.compile(r"\[STRATEGY\.BAR\]\s+symbol=([A-Z0-9._-]+)\b")
REQ_RE = re.compile(r"\[AI\.REQUEST\].*model=")
RESP_RE = re.compile(r"\[AI\.RESPONSE\].*model=")


def summarize(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min_ms": None, "avg_ms": None, "max_ms": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min_ms": round(ordered[0], 3),
        "avg_ms": round(mean(ordered), 3),
        "max_ms": round(ordered[-1], 3),
    }


def main() -> int:
    per_symbol: dict[str, dict[str, object]] = {}
    overall_bar_to_req: list[float] = []
    overall_req_to_resp: list[float] = []
    overall_bar_to_resp: list[float] = []

    symbols_with_logs = 0
    symbols_with_bars = 0
    symbols_with_ai = 0
    symbols_with_complete_cycles = 0

    for path in sorted(ROOT.glob("trading-agent-*.log")):
        symbol = path.name.removeprefix("trading-agent-").removesuffix(".log")
        symbols_with_logs += 1
        bar_count = 0
        req_count = 0
        resp_count = 0
        bar_to_req: list[float] = []
        req_to_resp: list[float] = []
        bar_to_resp: list[float] = []
        current: dict[str, datetime | None] | None = None

        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if not line.startswith(DAY):
                    continue
                ts_match = TS_RE.match(line)
                if not ts_match:
                    continue
                ts = datetime.fromisoformat(ts_match.group(1))

                bar_match = BAR_RE.search(line)
                if bar_match:
                    bar_symbol = bar_match.group(1)
                    if bar_symbol != symbol:
                        continue
                    if current is not None and current.get("first_req") and current.get("last_resp"):
                        b2r = (current["first_req"] - current["bar"]).total_seconds() * 1000.0
                        r2resp = (current["last_resp"] - current["first_req"]).total_seconds() * 1000.0
                        b2resp = (current["last_resp"] - current["bar"]).total_seconds() * 1000.0
                        if b2r >= 0.0:
                            bar_to_req.append(b2r)
                            overall_bar_to_req.append(b2r)
                        if r2resp >= 0.0:
                            req_to_resp.append(r2resp)
                            overall_req_to_resp.append(r2resp)
                        if b2resp >= 0.0:
                            bar_to_resp.append(b2resp)
                            overall_bar_to_resp.append(b2resp)
                    current = {"bar": ts, "first_req": None, "last_resp": None}
                    bar_count += 1
                    continue

                if current is None:
                    continue

                if REQ_RE.search(line):
                    req_count += 1
                    if current["first_req"] is None:
                        current["first_req"] = ts
                    continue

                if RESP_RE.search(line):
                    resp_count += 1
                    current["last_resp"] = ts
                    continue

        if current is not None and current.get("first_req") and current.get("last_resp"):
            b2r = (current["first_req"] - current["bar"]).total_seconds() * 1000.0
            r2resp = (current["last_resp"] - current["first_req"]).total_seconds() * 1000.0
            b2resp = (current["last_resp"] - current["bar"]).total_seconds() * 1000.0
            if b2r >= 0.0:
                bar_to_req.append(b2r)
                overall_bar_to_req.append(b2r)
            if r2resp >= 0.0:
                req_to_resp.append(r2resp)
                overall_req_to_resp.append(r2resp)
            if b2resp >= 0.0:
                bar_to_resp.append(b2resp)
                overall_bar_to_resp.append(b2resp)

        if bar_count > 0:
            symbols_with_bars += 1
        if req_count > 0 or resp_count > 0:
            symbols_with_ai += 1
        if bar_to_resp:
            symbols_with_complete_cycles += 1

        per_symbol[symbol] = {
            "bars_today": bar_count,
            "ai_requests_today": req_count,
            "ai_responses_today": resp_count,
            "bar_to_request_ms": summarize(bar_to_req),
            "request_to_response_ms": summarize(req_to_resp),
            "bar_to_response_ms": summarize(bar_to_resp),
        }

    ranked = sorted(
        (
            {
                "symbol": sym,
                "cycles": stats["bar_to_response_ms"]["count"],
                "bar_to_response_avg_ms": stats["bar_to_response_ms"]["avg_ms"],
                "bar_to_response_max_ms": stats["bar_to_response_ms"]["max_ms"],
                "bars_today": stats["bars_today"],
                "ai_requests_today": stats["ai_requests_today"],
                "ai_responses_today": stats["ai_responses_today"],
            }
            for sym, stats in per_symbol.items()
            if stats["bar_to_response_ms"]["count"] > 0
        ),
        key=lambda item: (-item["cycles"], item["symbol"]),
    )

    report = {
        "day": DAY,
        "symbols_total_logs": symbols_with_logs,
        "symbols_with_bars": symbols_with_bars,
        "symbols_with_ai_markers": symbols_with_ai,
        "symbols_with_complete_cycles": symbols_with_complete_cycles,
        "overall": {
            "bar_to_request_ms": summarize(overall_bar_to_req),
            "request_to_response_ms": summarize(overall_req_to_resp),
            "bar_to_response_ms": summarize(overall_bar_to_resp),
        },
        "top_symbols_by_completed_cycles": ranked[:20],
        "symbols_with_complete_cycles": {
            sym: stats for sym, stats in sorted(per_symbol.items()) if stats["bar_to_response_ms"]["count"] > 0
        },
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

