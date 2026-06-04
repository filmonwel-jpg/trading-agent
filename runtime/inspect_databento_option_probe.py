import os
import time

import databento as db

api_key = os.environ.get('DATABENTO_API_KEY', '').strip()
if not api_key:
    raise SystemExit('missing-api-key')

client = db.Live(key=api_key, reconnect_policy='reconnect')
client.subscribe(
    dataset='OPRA.PILLAR',
    schema='ohlcv-1s',
    symbols=['AAPL.OPT'],
    stype_in='parent',
    snapshot=False,
)
start = time.time()
count = 0
try:
    for record in client:
        count += 1
        print('TYPE', type(record).__name__)
        attrs = {}
        for name in [
            'instrument_id',
            'stype_in_symbol',
            'stype_out_symbol',
            'raw_symbol',
            'symbol',
            'ts_event',
            'volume',
            'open',
            'high',
            'low',
            'close',
            'msg',
            'code',
        ]:
            if hasattr(record, name):
                try:
                    attrs[name] = getattr(record, name)
                except Exception:
                    pass
        print('ATTRS', attrs)
        if count >= 8 or time.time() - start > 15:
            break
finally:
    client.terminate()

