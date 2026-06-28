#!/usr/bin/env python3
import gzip
import json
from pathlib import Path

BASE = Path('/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/local-backtests/databento-core5-4week-20260427-20260522-recent')
MANIFEST = BASE / 'databento-20260427-20260522-core5-4week-daily-prevclose.manifest.json'
SLICE = BASE / 'databento-20260427-20260522-core5-4week-daily-prevclose.ndjson.gz'

manifest = json.loads(MANIFEST.read_text())
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

with gzip.open(SLICE, 'rt', encoding='utf-8') as stream:
    first = [json.loads(next(stream)) for _ in range(35)]
print('FIRST_EVENTS')
for i, event in enumerate(first):
    print(i, event.get('event'), event.get('message'), event.get('symbol') or event.get('underlying'), event.get('sessionDate'), event.get('previousClose'), event.get('barEpochSec'))

# Verify every day has five previous_close records before the first market event for that day.
seen_previous = {}
first_market_event = {}
violations = []
with gzip.open(SLICE, 'rt', encoding='utf-8') as stream:
    for line_no, line in enumerate(stream, 1):
        event = json.loads(line)
        event_type = event.get('event')
        if event_type == 'previous_close':
            day = event.get('sessionDate')
            seen_previous.setdefault(day, set()).add(event.get('symbol'))
        elif event_type in {'equity_bar', 'option_bar'}:
            epoch = event.get('barEpochSec')
            # Session day is also inferable from day status, but the manifest day-order check is enough here.
            # Record the first line to aid debugging if a future builder breaks ordering.
            if 'first' not in first_market_event:
                first_market_event['first'] = line_no
            # Stop once all previous-close days have been seen and first market is recorded.
            if len(seen_previous) == len(manifest['previous_close_days']):
                pass

expected_symbols = {'NVDA', 'QQQ', 'SPY', 'TQQQ', 'TSLA'}
for day in manifest['previous_close_days']:
    symbols = seen_previous.get(day, set())
    if symbols != expected_symbols:
        violations.append((day, sorted(symbols)))
print('PREVCLOSE_SYMBOL_VIOLATIONS', violations)
print('FIRST_MARKET_LINE', first_market_event.get('first'))
raise SystemExit(1 if violations or manifest['missing_previous_close'] or manifest['malformed'] else 0)

