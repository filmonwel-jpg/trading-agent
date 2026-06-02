#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
BOTS_DIR = ROOT / "runtime" / "databento" / "bots"
OVERVIEW_URL = "http://127.0.0.1:9081/api/stack/overview"
RECENT_BYTES = 750_000
LOOKBACK_BYTES = 20_000_000
BAR_MARKERS = ("STRATEGY.BAR", ">>> [30s BUCKET]", ">>> [30s DIAGNOSTIC]")
FEATURE_MARKERS = ("STRATEGY->AI", "AI.INPUT", "AI.ROUTER", "AI.REGIME")
AI_REQUEST_MARKERS = ("AI.REQUEST",)
AI_RESPONSE_MARKERS = ("AI.RESPONSE",)


def read_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def tail_text(path: Path, max_bytes: int) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(0, size - max_bytes))
        return handle.read().decode("utf-8", errors="ignore")


def latest_age_seconds(text: str, markers: tuple[str, ...], now: datetime) -> float | None:
    latest: datetime | None = None
    for line in text.splitlines():
        if not any(marker in line for marker in markers):
            continue
        timestamp_text = line[:29]
        try:
            candidate = datetime.fromisoformat(timestamp_text)
        except ValueError:
            continue
        latest = candidate
    if latest is None:
        return None
    return round((now - latest.astimezone(now.tzinfo)).total_seconds(), 1)


def find_live_log(symbol: str) -> Path:
    lower = ROOT / "runtime" / f"{symbol.lower()}_live_trade_logs.txt"
    if lower.exists():
        return lower
    upper = ROOT / "runtime" / f"{symbol}_live_trade_logs.txt"
    return upper


def main() -> int:
    now = datetime.now().astimezone()
    with urlopen(OVERVIEW_URL, timeout=30) as response:
        overview = json.load(response)
    current = {
        item["symbol"]: item
        for item in overview.get("symbols", [])
        if int(item.get("port", 0) or 0) >= 9081
    }

    rows: list[dict[str, object]] = []
    by_shard: dict[int, list[dict[str, object]]] = defaultdict(list)
    for properties_path in sorted(BOTS_DIR.glob("trading-*.properties")):
        if properties_path.name == "trading-databento-template.properties":
            continue
        props = read_properties(properties_path)
        symbol = props.get("trading.symbol", properties_path.stem.replace("trading-", "")).upper()
        live_log = find_live_log(symbol)
        recent_text = tail_text(live_log, RECENT_BYTES)
        lookback_text = tail_text(live_log, LOOKBACK_BYTES)
        info = current.get(symbol, {})
        feed = info.get("databentoFeed") or {}
        primary = feed.get("primary") or {}
        feed_ok = bool(info.get("connected")) and bool(info.get("healthOk")) and bool(info.get("databentoFeedHealthy"))
        has_bar = any(marker in recent_text for marker in BAR_MARKERS)
        has_feature = any(marker in recent_text for marker in FEATURE_MARKERS)
        has_ai_request = any(marker in recent_text for marker in AI_REQUEST_MARKERS)
        has_ai_response = any(marker in recent_text for marker in AI_RESPONSE_MARKERS)
        if not feed_ok:
            stop_stage = "feed"
        elif not has_bar:
            stop_stage = "bar_build"
        elif not has_feature:
            stop_stage = "feature_build"
        elif not has_ai_request:
            stop_stage = "ai_request"
        elif not has_ai_response:
            stop_stage = "ai_response"
        else:
            stop_stage = "active"
        row = {
            "symbol": symbol,
            "shard": int(props.get("trading.databento.shared-feed.shard-id", "-1") or -1),
            "port": int(props.get("server.port", "0") or 0),
            "relayLogFile": props.get("trading.databento.shared-feed.log-file", ""),
            "stopStage": stop_stage,
            "feedOk": feed_ok,
            "marketDataAgeMs": primary.get("marketDataAgeMs"),
            "quoteAgeMs": primary.get("quoteAgeMs"),
            "optionAgeMs": primary.get("optionAgeMs"),
            "recentBar": has_bar,
            "recentFeature": has_feature,
            "recentAiRequest": has_ai_request,
            "recentAiResponse": has_ai_response,
            "lastBarAgeSec": latest_age_seconds(lookback_text, BAR_MARKERS, now),
            "lastFeatureAgeSec": latest_age_seconds(lookback_text, FEATURE_MARKERS, now),
            "lastAiRequestAgeSec": latest_age_seconds(lookback_text, AI_REQUEST_MARKERS, now),
            "lastAiResponseAgeSec": latest_age_seconds(lookback_text, AI_RESPONSE_MARKERS, now),
        }
        rows.append(row)
        by_shard[row["shard"]].append(row)

    result = {
        "checkedAt": now.isoformat(),
        "overviewGeneratedAt": overview.get("generatedAt"),
        "symbolCount": len(rows),
        "stageCounts": dict(Counter(row["stopStage"] for row in rows)),
        "shards": {
            str(shard): {
                "count": len(items),
                "relayLogFile": items[0]["relayLogFile"] if items else "",
                "stageCounts": dict(Counter(item["stopStage"] for item in items)),
                "symbols": [item["symbol"] for item in items],
            }
            for shard, items in sorted(by_shard.items())
        },
        "staleSymbols": [row for row in rows if row["stopStage"] != "active"],
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

