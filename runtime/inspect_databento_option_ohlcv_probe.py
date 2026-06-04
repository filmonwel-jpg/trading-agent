import os
import time

import databento as db
import databento_dbn as dbn

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
mapping_count = 0
try:
    for record in client:
        if isinstance(record, dbn.SymbolMappingMsg):
            mapping_count += 1
            if mapping_count <= 3:
                print('MAP', getattr(record, 'instrument_id', None), getattr(record, 'stype_in_symbol', None), getattr(record, 'stype_out_symbol', None))
            continue
        print('TYPE', type(record).__name__)
        attrs = {}
        for name in [
            'instrument_id',
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
        if isinstance(record, dbn.OHLCVMsg):
            break
        if time.time() - start > 20:
            break
finally:
    client.terminate()

