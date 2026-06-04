import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

root = Path('/tmp/lifecycle_micro_smoke')
root.mkdir(exist_ok=True)
rows30 = []
rows5 = []
for sym in ['TSLA']:
    ts30 = pd.date_range('2026-05-22 09:30:00', periods=80, freq='30s', tz='America/New_York')
    for i, t in enumerate(ts30):
        price = 100 + 0.02 * i + 0.2 * np.sin(i / 4)
        rows30.append({
            'Symbol': sym,
            'Timestamp': t.strftime('%Y%m%d %H:%M:%S America/New_York'),
            'Open': price,
            'High': price + 0.2,
            'Low': price - 0.2,
            'Close': price + 0.05,
            'Volume': 100 + i,
            'WAP': price + 0.02,
            'Count': 5,
            'SpreadBps': 2.0,
            'PutVolDelta5s': i % 3,
            'CallVolDelta5s': (i + 1) % 5,
            'PutVol': i * 2,
            'CallVol': i * 3,
            'Label_Long_Entry': 1 if i in [5, 15, 25, 35, 45] else 0,
            'Label_Short_Entry': 1 if i in [10, 20, 30, 40, 50] else 0,
        })
    ts5 = pd.date_range('2026-05-22 09:30:00', periods=480, freq='5s', tz='America/New_York')
    for i, t in enumerate(ts5):
        price = 100 + 0.003 * i + 0.05 * np.sin(i / 3)
        rows5.append({
            'Symbol': sym,
            'Timestamp': t.strftime('%Y%m%d %H:%M:%S America/New_York'),
            'Open': price,
            'High': price + 0.05,
            'Low': price - 0.05,
            'Close': price + 0.01,
            'Volume': 20 + i % 10,
            'WAP': price + 0.005,
            'Count': 2,
            'SpreadBps': 2.0,
            'PutVolDelta5s': i % 2,
            'CallVolDelta5s': (i + 1) % 3,
            'PutVol': i,
            'CallVol': i * 2,
        })

pd.DataFrame(rows30).to_csv(root / '30s.csv', index=False)
pd.DataFrame(rows5).to_csv(root / '5s.csv', index=False)
subprocess.run([
    'python', 'train_lifecycle_micro_models.py',
    '--input-30s-csv', str(root / '30s.csv'),
    '--input-5s-csv', str(root / '5s.csv'),
    '--output-dir', str(root / 'out'),
    '--max-entry-events', '12',
    '--min-rows', '10',
    '--no-onnx',
], check=True)
print('lifecycle_micro_smoke_test passed')

