#!/usr/bin/env python3
"""Build a four-week Databento core-five recorded NDJSON replay slice.

The recorded NDJSON streamer filters symbols but not dates, so the backtest needs a
physically sliced input file. This builder also emits one previous_close event per
symbol per included session before that session's market bars. The close value is
computed from the last equity_bar close observed before the session boundary.
"""
from __future__ import annotations

import gzip
import json
import re
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path('/Users/filmonghezehey/trading-agent/worktrees/databento')
INPUT_PATH = ROOT / 'runtime/replay/databento-20260523-core5.ndjson.gz'
OUT_DIR = ROOT / 'runtime/local-backtests/databento-core5-4week-20260427-20260522-recent'
OUTPUT_PATH = OUT_DIR / 'databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz'
MANIFEST_PATH = OUT_DIR / 'databento-20260427-20260522-core5-4week-daily-prevclose.manifest.json'
SYMBOLS = {'TSLA', 'TQQQ', 'NVDA', 'SPY', 'QQQ'}
NY = ZoneInfo('America/New_York')
START_SESSION = date(2026, 4, 27)
END_SESSION = date(2026, 5, 22)
START_DT = datetime(2026, 4, 27, 9, 30, tzinfo=NY).astimezone(timezone.utc)
END_DT = datetime(2026, 5, 22, 16, 0, tzinfo=NY).astimezone(timezone.utc)
START_EPOCH = int(START_DT.timestamp())
END_EPOCH = int(END_DT.timestamp())
DAY_RE = re.compile(r'\bday=(\d{8})\b')


def event_symbol(payload: dict[str, Any]) -> str:
    event = str(payload.get('event', '')).lower()
    if event == 'option_bar':
        return str(payload.get('underlying', payload.get('symbol', ''))).upper()
    return str(payload.get('symbol', payload.get('underlying', ''))).upper()


def event_epoch(payload: dict[str, Any]) -> int | None:
    for key in ('barEpochSec', 'tsEventNs', 'EventTsEventNs', 'event_ts_event_ns'):
        value = payload.get(key)
        if value is None or value == '':
            continue
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            continue
        if key == 'barEpochSec':
            return numeric
        return numeric // 1_000_000_000
    return None


def session_from_epoch(epoch: int) -> date:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(NY).date()


def status_day(payload: dict[str, Any]) -> date | None:
    message = str(payload.get('message', ''))
    match = DAY_RE.search(message)
    if not match:
        return None
    raw = match.group(1)
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def in_target_session(day: date) -> bool:
    return START_SESSION <= day <= END_SESSION


def emit_json(dst, payload: dict[str, Any]) -> None:
    dst.write(json.dumps(payload, separators=(',', ':'), sort_keys=False) + '\n')


def previous_close_payload(symbol: str, session_day: date, last_close: dict[str, Any]) -> dict[str, Any]:
    epoch = last_close.get('epoch')
    source_time = None
    source_session = None
    if isinstance(epoch, int):
        source_dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(NY)
        source_time = source_dt.isoformat()
        source_session = source_dt.date().isoformat()
    close = float(last_close['close'])
    return {
        'event': 'previous_close',
        'symbol': symbol,
        'sessionDate': session_day.isoformat(),
        'previousClose': close,
        'close': close,
        'sourceBarEpochSec': epoch,
        'sourceSessionDate': source_session,
        'sourceTime': source_time,
        'historical': True,
        'EventSchemaVersion': 'databento_ndjson_v2',
        'EventSource': 'recorded_ndjson_4week_daily_prevclose',
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    counts: Counter[str] = Counter()
    input_lines = 0
    kept = 0
    skipped_symbol = 0
    skipped_time = 0
    malformed = 0
    day_status_seen: list[str] = []
    previous_close_by_day: dict[str, dict[str, dict[str, Any]]] = {}
    emitted_prevclose_days: set[date] = set()
    missing_prevclose: dict[str, list[str]] = {}
    last_close_by_symbol: dict[str, dict[str, Any]] = {}
    first_event_epoch: int | None = None
    last_event_epoch: int | None = None

    def emit_previous_closes_for_day(dst, session_day: date) -> None:
        nonlocal kept
        if session_day in emitted_prevclose_days:
            return
        day_key = session_day.isoformat()
        previous_close_by_day.setdefault(day_key, {})
        missing: list[str] = []
        for symbol in sorted(SYMBOLS):
            last_close = last_close_by_symbol.get(symbol)
            if not last_close or float(last_close.get('close', 0.0) or 0.0) <= 0.0:
                missing.append(symbol)
                continue
            payload = previous_close_payload(symbol, session_day, last_close)
            emit_json(dst, payload)
            previous_close_by_day[day_key][symbol] = {
                'previousClose': payload['previousClose'],
                'sourceBarEpochSec': payload['sourceBarEpochSec'],
                'sourceSessionDate': payload['sourceSessionDate'],
                'sourceTime': payload['sourceTime'],
            }
            counts['previous_close'] += 1
            kept += 1
        if missing:
            missing_prevclose[day_key] = missing
        emitted_prevclose_days.add(session_day)

    with gzip.open(INPUT_PATH, 'rt', encoding='utf-8') as src, gzip.open(OUTPUT_PATH, 'wt', encoding='utf-8', compresslevel=6) as dst:
        start_status = {
            'event': 'status',
            'message': 'recorded-ndjson-4week-slice-start',
            'EventSchemaVersion': 'databento_ndjson_v2',
            'symbols': sorted(SYMBOLS),
            'sourceFile': str(INPUT_PATH),
            'targetStartUtc': START_DT.isoformat(),
            'targetEndUtc': END_DT.isoformat(),
            'targetStartEpochSec': START_EPOCH,
            'targetEndEpochSec': END_EPOCH,
            'previousClosePolicy': 'daily previous_close events emitted at day-begin from prior observed equity_bar close',
        }
        emit_json(dst, start_status)
        counts['status'] += 1
        kept += 1

        for raw in src:
            input_lines += 1
            raw = raw.strip()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                malformed += 1
                continue
            if not isinstance(payload, dict):
                malformed += 1
                continue

            event = str(payload.get('event', '')).lower()
            symbol = event_symbol(payload)
            epoch = event_epoch(payload)

            if event == 'status':
                day = status_day(payload)
                if day and in_target_session(day):
                    payload.setdefault('historical', True)
                    payload.setdefault('EventSchemaVersion', 'databento_ndjson_v2')
                    payload.setdefault('EventSource', 'recorded_ndjson_4week_slice')
                    emit_json(dst, payload)
                    counts['status'] += 1
                    kept += 1
                    day_status_seen.append(day.isoformat())
                    emit_previous_closes_for_day(dst, day)
                continue

            if not symbol or symbol not in SYMBOLS:
                skipped_symbol += 1
                continue

            if event == 'equity_bar' and epoch is not None:
                close = payload.get('close')
                try:
                    close_value = float(close)
                except (TypeError, ValueError):
                    close_value = 0.0
                if close_value > 0.0 and epoch < START_EPOCH:
                    last_close_by_symbol[symbol] = {'close': close_value, 'epoch': epoch}

            if epoch is None or epoch < START_EPOCH or epoch > END_EPOCH:
                skipped_time += 1
                continue

            day = session_from_epoch(epoch)
            if not in_target_session(day):
                skipped_time += 1
                continue

            # If the source did not contain/retain a day-begin status for this session,
            # still guarantee previous_close context before the first market event.
            emit_previous_closes_for_day(dst, day)

            payload.setdefault('historical', True)
            payload.setdefault('EventSchemaVersion', 'databento_ndjson_v2')
            payload.setdefault('EventSource', 'recorded_ndjson_4week_slice')
            emit_json(dst, payload)
            counts[event] += 1
            kept += 1
            if first_event_epoch is None:
                first_event_epoch = epoch
            last_event_epoch = epoch

            if event == 'equity_bar':
                close = payload.get('close')
                try:
                    close_value = float(close)
                except (TypeError, ValueError):
                    close_value = 0.0
                if close_value > 0.0:
                    last_close_by_symbol[symbol] = {'close': close_value, 'epoch': epoch}

        complete_status = {
            'event': 'status',
            'message': f'recorded-ndjson-4week-slice-complete events={kept} inputLines={input_lines}',
            'EventSchemaVersion': 'databento_ndjson_v2',
        }
        emit_json(dst, complete_status)
        counts['status'] += 1
        kept += 1

    manifest = {
        'source_file': str(INPUT_PATH),
        'output_file': str(OUTPUT_PATH),
        'symbols': sorted(SYMBOLS),
        'target_start_utc': START_DT.isoformat(),
        'target_end_utc': END_DT.isoformat(),
        'target_start_epoch_sec': START_EPOCH,
        'target_end_epoch_sec': END_EPOCH,
        'input_lines': input_lines,
        'output_events': kept,
        'counts': dict(sorted(counts.items())),
        'day_status_seen': day_status_seen,
        'day_count': len(set(day_status_seen) | {day.isoformat() for day in emitted_prevclose_days}),
        'previous_close_by_day': previous_close_by_day,
        'previous_close_days': sorted(day.isoformat() for day in emitted_prevclose_days),
        'previous_close_event_count_expected': len(emitted_prevclose_days) * len(SYMBOLS),
        'previous_close_emitted_before_session_bars': True,
        'missing_previous_close': missing_prevclose,
        'skipped_symbol': skipped_symbol,
        'skipped_time': skipped_time,
        'malformed': malformed,
        'first_event_epoch_sec': first_event_epoch,
        'last_event_epoch_sec': last_event_epoch,
        'last_close_by_symbol_after_slice': last_close_by_symbol,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    print(json.dumps(manifest, indent=2, sort_keys=True))
    if malformed or missing_prevclose:
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


