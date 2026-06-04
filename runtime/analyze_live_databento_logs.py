from pathlib import Path
import re

symbols = ['AAPL', 'NVDA', 'TSLA', 'SPY', 'AMZN', 'MU', 'TQQQ', 'LRCX']
base = Path('/Users/filmonghezehey/trading-agent/worktrees/databento/runtime/databento/logs')
patterns = {
    'databento_heartbeat': 'heartbeat symbols=',
    'options_1s_end': 'options-system code=end_of_interval',
    'strategy_1s_bar': '[FLOW][DATA][STRATEGY.BAR]',
    'bucket_30s': '[30s BUCKET]',
    'ai_request': '[FLOW][ANALYZE][AI.REQUEST]',
    'ai_response': '[FLOW][DATA][AI.RESPONSE]',
    'ai_predicts_entry_fail': 'AI_PREDICTS_ENTRY=FAIL',
    'ai_predicts_entry_pass': 'AI_PREDICTS_ENTRY=PASS',
    'entry_gate_fail': '[FLOW][COND][AI.ENTRY] ENTRY_GATE_OPEN=FAIL',
    'entry_gate_pass': '[FLOW][COND][AI.ENTRY] ENTRY_GATE_OPEN=PASS',
    'micro': 'AI.MICRO',
    'order': 'ORDER',
    'position': 'POSITION',
    'error': '[ERROR]',
    'exception': 'Exception',
    'feature_mismatch': 'FEATURE_COUNT_MATCH=FAIL',
    'model_load_fail': 'Failed to load',
}
prob_re = re.compile(r'model=([^ ]+).*positiveProb=([0-9.]+)')
gate_re = re.compile(r'ENTRY_GATE_OPEN=(PASS|FAIL).*tradeCount=([0-9]+) maxTrades=([0-9]+)')
entry_fail_re = re.compile(r'AI_PREDICTS_ENTRY=FAIL \\| symbol=([^ ]+).*prob=([0-9.]+) threshold=([0-9.]+)')
bar_epoch_re = re.compile(r'epoch=([0-9]+)')

for sym in symbols:
    path = base / f'trading-agent-{sym}.log'
    print(f'--- {sym} exists={path.exists()} size={path.stat().st_size if path.exists() else 0} ---')
    if not path.exists():
        continue
    counts = {k: 0 for k in patterns}
    last = {}
    first_ts = None
    last_ts = None
    with path.open(errors='ignore') as fh:
        for line in fh:
            if not line.strip():
                continue
            if line.startswith('2026-'):
                ts = line[:29]
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
            for key, needle in patterns.items():
                if needle in line:
                    counts[key] += 1
                    last[key] = line.strip()
            match = prob_re.search(line)
            if match:
                last['last_prob'] = f'{match.group(1)}={match.group(2)}'
            match = gate_re.search(line)
            if match:
                last['last_gate'] = f'{match.group(1)} tradeCount={match.group(2)} maxTrades={match.group(3)}'
            match = entry_fail_re.search(line)
            if match:
                last['last_entry_threshold_fail'] = f'prob={match.group(2)} threshold={match.group(3)}'
            match = bar_epoch_re.search(line)
            if match and '[FLOW][DATA][STRATEGY.BAR]' in line:
                last['last_1s_epoch'] = match.group(1)
    nonzero_counts = ' '.join(f'{k}={v}' for k, v in counts.items() if v)
    print('time_range', first_ts, '...', last_ts)
    print('counts', nonzero_counts)
    for key in ['last_1s_epoch', 'last_gate', 'last_prob', 'last_entry_threshold_fail']:
        if key in last:
            print(key, last[key])
    for key in ['bucket_30s', 'entry_gate_fail', 'entry_gate_pass', 'micro', 'order', 'position', 'error', 'feature_mismatch', 'model_load_fail']:
        if key in last:
            print('last_' + key, last[key][:260])
