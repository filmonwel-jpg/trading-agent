#!/usr/bin/env python3
import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parent
DEFAULT_MANIFEST = BASE / 'databento-20260427-20260522-core5-4week-daily-prevclose.manifest.json'
DEFAULT_SLICE = BASE / 'databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz'
NY = ZoneInfo('America/New_York')

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Validate a four-week Databento slice with daily previous_close events.')
    parser.add_argument('--manifest', default=str(DEFAULT_MANIFEST), help='Slice manifest JSON path.')
    parser.add_argument('--slice', dest='slice_path', default=str(DEFAULT_SLICE), help='Sliced replay NDJSON/NDJSON.GZ path.')
    parser.add_argument('--symbols', default='', help='Comma-separated expected symbols. Defaults to manifest symbols.')
    parser.add_argument('--first-events', type=int, default=35, help='Number of first events to print for inspection.')
    return parser.parse_args()


def parse_symbols(raw: str, manifest: dict) -> set[str]:
    if raw:
        symbols = {token.strip().upper() for token in raw.replace('\n', ',').split(',') if token.strip()}
    else:
        symbols = {str(symbol).upper() for symbol in manifest.get('symbols', []) if str(symbol).strip()}
    return symbols or {'NVDA', 'QQQ', 'SPY', 'TQQQ', 'TSLA'}


def open_text(path: Path):
    if path.name.endswith('.gz'):
        return gzip.open(path, 'rt', encoding='utf-8')
    return path.open('rt', encoding='utf-8')


def event_epoch(event: dict) -> int | None:
    for key in ('barEpochSec', 'tsEventNs', 'EventTsEventNs', 'event_ts_event_ns'):
        value = event.get(key)
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


def session_from_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(NY).date().isoformat()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser().resolve()
    slice_path = Path(args.slice_path).expanduser().resolve()
    manifest = json.loads(manifest_path.read_text())
    expected_symbols = parse_symbols(args.symbols, manifest)

    print('MANIFEST', manifest_path)
    print('SLICE', slice_path)
    print('EXPECTED_SYMBOLS', sorted(expected_symbols))
    print('MANIFEST_COUNTS', manifest['counts'])
    print('DAY_COUNT', manifest['day_count'], 'PREVCLOSE_DAYS', len(manifest['previous_close_days']))
    print('EXPECTED_PREVCLOSE', manifest['previous_close_event_count_expected'], 'ACTUAL', manifest['counts'].get('previous_close'))
    print('MISSING_PREVCLOSE', manifest['missing_previous_close'])
    print('MALFORMED', manifest['malformed'])
    print('FIRST_PREVCLOSE_DAYS')
    for day in manifest['previous_close_days'][:3]:
        print(day, manifest['previous_close_by_day'][day])
    print('LAST_PREVCLOSE_DAYS')
    for day in manifest['previous_close_days'][-3:]:
        print(day, manifest['previous_close_by_day'][day])

    first = []
    with open_text(slice_path) as stream:
        for _, line in zip(range(max(args.first_events, 0)), stream):
            first.append(json.loads(line))
    print('FIRST_EVENTS')
    for i, event in enumerate(first):
        print(i, event.get('event'), event.get('message'), event.get('symbol') or event.get('underlying'), event.get('sessionDate'), event.get('previousClose'), event.get('barEpochSec'))

    # Verify every day has expected previous_close records before the first market event for that day.
    seen_previous: dict[str, set[str]] = {}
    first_market_event_by_day: dict[str, int] = {}
    symbol_violations = []
    order_violations = []
    previous_close_days = set(manifest['previous_close_days'])
    with open_text(slice_path) as stream:
        for line_no, line in enumerate(stream, 1):
            event = json.loads(line)
            event_type = str(event.get('event', '')).lower()
            if event_type == 'previous_close':
                day = event.get('sessionDate')
                symbol = str(event.get('symbol', '')).upper()
                if day in first_market_event_by_day:
                    order_violations.append((day, symbol, line_no, first_market_event_by_day[day]))
                seen_previous.setdefault(day, set()).add(symbol)
            elif event_type in {'equity_bar', 'option_bar'}:
                epoch = event_epoch(event)
                if epoch is None:
                    continue
                day = session_from_epoch(epoch)
                if day not in previous_close_days:
                    continue
                if day not in first_market_event_by_day:
                    first_market_event_by_day[day] = line_no
                    symbols = seen_previous.get(day, set())
                    if symbols != expected_symbols:
                        order_violations.append((day, 'first_market_before_all_prevclose', line_no, sorted(symbols)))

    for day in manifest['previous_close_days']:
        symbols = seen_previous.get(day, set())
        if symbols != expected_symbols:
            symbol_violations.append((day, sorted(symbols)))
    print('PREVCLOSE_SYMBOL_VIOLATIONS', symbol_violations)
    print('PREVCLOSE_ORDER_VIOLATIONS', order_violations[:20], 'COUNT', len(order_violations))
    print('FIRST_MARKET_LINE_BY_DAY_FIRST3', list(first_market_event_by_day.items())[:3])
    return 1 if symbol_violations or order_violations or manifest['missing_previous_close'] or manifest['malformed'] else 0


if __name__ == '__main__':
    raise SystemExit(main())

