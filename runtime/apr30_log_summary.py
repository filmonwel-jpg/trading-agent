from pathlib import Path
import gzip
import json
import re
from collections import defaultdict

ROOT = Path('/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/databento/logs')
DAY = '2026-04-30'
EXPECTED = [
    s.strip()
    for s in Path('/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/symbols_100.txt').read_text().splitlines()
    if s.strip()
]
PATTERN = re.compile(rf'^trading-agent-(?P<symbol>[A-Z0-9._-]+)\.log(?:\.{DAY}\.\d+\.gz)?$')
BAR_PATTERN = re.compile(r'\[STRATEGY\.BAR\]\s+symbol=([A-Z0-9._-]+)\b')

per_symbol = defaultdict(lambda: {
    'bars': 0,
    'option_msgs': 0,
    'connected': False,
    'warning': False,
    'first_bar': None,
    'last_bar': None,
})

for path in ROOT.iterdir():
    match = PATTERN.match(path.name)
    if not match:
        continue
    symbol = match.group('symbol')
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', errors='ignore') as handle:
        for line in handle:
            if not line.startswith(DAY):
                continue
            if 'Connected to shared relay' in line and f'symbol={symbol}' in line:
                per_symbol[symbol]['connected'] = True
            if 'options-system code=end_of_interval' in line:
                per_symbol[symbol]['option_msgs'] += 1
            if '[STRATEGY.OPTIONS] OPTION_VOLUME_STREAM_AVAILABLE=FAIL' in line and f'symbol={symbol}' in line:
                per_symbol[symbol]['warning'] = True
            if '[STRATEGY.BAR]' in line:
                bar_match = BAR_PATTERN.search(line)
                if bar_match and bar_match.group(1) == symbol:
                    per_symbol[symbol]['bars'] += 1
                    ts = line.split(' ', 1)[0]
                    if per_symbol[symbol]['first_bar'] is None:
                        per_symbol[symbol]['first_bar'] = ts
                    per_symbol[symbol]['last_bar'] = ts

summary = {
    'expected_symbols': len(EXPECTED),
    'symbols_seen': len(per_symbol),
    'symbols_with_bars': sum(1 for symbol in EXPECTED if per_symbol[symbol]['bars'] > 0),
    'symbols_with_option_msgs': sum(1 for symbol in EXPECTED if per_symbol[symbol]['option_msgs'] > 0),
    'symbols_connected': sum(1 for symbol in EXPECTED if per_symbol[symbol]['connected']),
    'symbols_with_option_warning': sum(1 for symbol in EXPECTED if per_symbol[symbol]['warning']),
    'missing_bars': [symbol for symbol in EXPECTED if per_symbol[symbol]['bars'] == 0],
    'missing_option_msgs': [symbol for symbol in EXPECTED if per_symbol[symbol]['option_msgs'] == 0],
    'missing_connections': [symbol for symbol in EXPECTED if not per_symbol[symbol]['connected']],
    'with_option_warning': [symbol for symbol in EXPECTED if per_symbol[symbol]['warning']],
    'top_bar_counts': sorted(
        ({'symbol': symbol, 'bars': per_symbol[symbol]['bars']} for symbol in EXPECTED),
        key=lambda item: (-item['bars'], item['symbol'])
    )[:15],
    'sample': {symbol: per_symbol[symbol] for symbol in EXPECTED[:10]},
}

print(json.dumps(summary, indent=2))

