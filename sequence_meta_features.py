import argparse
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
except Exception:
    torch = None
    nn = None
    optim = None


_BaseModule = nn.Module if nn is not None else object


SEQ_OUTPUT_COLUMNS = [
    'SeqLSTM_UpProb30s',
    'SeqTCN_UpProb30s',
    'SeqTransformer_UpProb30s',
    'SeqPatchTST_UpProb30s',
    'SeqModelConsensus_UpProb30s',
]


def _proxy_features(df):
    close = pd.to_numeric(df['Close'], errors='coerce').ffill().fillna(0.0)
    volume = pd.to_numeric(df.get('Volume', 0.0), errors='coerce').fillna(0.0)
    ret = close.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    trend = ret.rolling(window=12, min_periods=4).mean().fillna(0.0)
    vol = ret.rolling(window=16, min_periods=6).std(ddof=1).fillna(0.0)
    impulse = (volume / (volume.rolling(window=20, min_periods=6).median().fillna(1.0) + 1.0)) - 1.0
    sig = lambda x: 1.0 / (1.0 + np.exp(-np.clip(x, -40.0, 40.0)))

    df['SeqLSTM_UpProb30s'] = sig((trend / (vol + 1e-6)) * 0.8 + 0.3 * impulse)
    df['SeqTCN_UpProb30s'] = sig(0.9 * trend.shift(1).fillna(0.0) / (vol + 1e-6) + 0.2 * ret)
    df['SeqTransformer_UpProb30s'] = sig(0.7 * trend + 0.5 * ret.rolling(4, min_periods=2).mean().fillna(0.0))
    df['SeqPatchTST_UpProb30s'] = (df['SeqTCN_UpProb30s'] + df['SeqTransformer_UpProb30s']) / 2.0
    df['SeqModelConsensus_UpProb30s'] = (
        df['SeqLSTM_UpProb30s']
        + df['SeqTCN_UpProb30s']
        + df['SeqTransformer_UpProb30s']
        + df['SeqPatchTST_UpProb30s']
    ) / 4.0
    return df


class _SeqModel(_BaseModule):
    def __init__(self, in_dim, arch):
        super().__init__()
        if arch == 'lstm':
            self.net = nn.LSTM(in_dim, 32, num_layers=1, batch_first=True)
            self.fc = nn.Linear(32, 1)
            self.arch = 'lstm'
        elif arch == 'tcn':
            self.net = nn.Sequential(
                nn.Conv1d(in_dim, 32, kernel_size=3, padding=2, dilation=1),
                nn.ReLU(),
                nn.Conv1d(32, 32, kernel_size=3, padding=4, dilation=2),
                nn.ReLU(),
            )
            self.fc = nn.Linear(32, 1)
            self.arch = 'tcn'
        else:
            self.in_proj = nn.Linear(in_dim, 48)
            layer = nn.TransformerEncoderLayer(d_model=48, nhead=4, dim_feedforward=96, batch_first=True)
            self.net = nn.TransformerEncoder(layer, num_layers=1)
            self.fc = nn.Linear(48, 1)
            self.arch = 'transformer'

    def forward(self, x):
        if self.arch == 'lstm':
            out, _ = self.net(x)
            return self.fc(out[:, -1, :]).squeeze(-1)
        if self.arch == 'tcn':
            out = self.net(x.transpose(1, 2))[:, :, -1]
            return self.fc(out).squeeze(-1)
        out = self.net(self.in_proj(x))
        return self.fc(out[:, -1, :]).squeeze(-1)


def _build_windows(df, window):
    close = pd.to_numeric(df['Close'], errors='coerce').ffill().fillna(0.0)
    high = pd.to_numeric(df.get('High', close), errors='coerce').fillna(close)
    low = pd.to_numeric(df.get('Low', close), errors='coerce').fillna(close)
    volume = pd.to_numeric(df.get('Volume', 0.0), errors='coerce').fillna(0.0)

    ret = close.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    spread = ((high - low) / (close.abs() + 1e-9)).fillna(0.0)
    vol_norm = (volume / (volume.rolling(window=60, min_periods=8).median().fillna(1.0) + 1.0)) - 1.0
    feat = pd.concat([ret, spread, vol_norm], axis=1).fillna(0.0).values.astype(np.float32)

    xs, ys, idx = [], [], []
    for i in range(window, len(df) - 1):
        xs.append(feat[i - window:i])
        ys.append(1.0 if close.iloc[i + 1] > close.iloc[i] else 0.0)
        idx.append(i)
    return np.asarray(xs), np.asarray(ys, dtype=np.float32), np.asarray(idx, dtype=np.int64)


def _torch_features(df, window, epochs, lr):
    x_np, y_np, idx = _build_windows(df, window)
    if len(x_np) < 300:
        raise ValueError(f'Need >=300 windows, got {len(x_np)}')

    split = min(max(int(len(x_np) * 0.8), 64), len(x_np) - 32)
    x_train = torch.tensor(x_np[:split], dtype=torch.float32)
    y_train = torch.tensor(y_np[:split], dtype=torch.float32)
    x_all = torch.tensor(x_np, dtype=torch.float32)

    names_arch = [
        ('SeqLSTM_UpProb30s', 'lstm'),
        ('SeqTCN_UpProb30s', 'tcn'),
        ('SeqTransformer_UpProb30s', 'transformer'),
    ]
    crit = nn.BCEWithLogitsLoss()

    for name, arch in names_arch:
        model = _SeqModel(x_np.shape[-1], arch)
        opt = optim.Adam(model.parameters(), lr=lr)
        dataset = torch.utils.data.TensorDataset(x_train, y_train)
        loader = torch.utils.data.DataLoader(dataset, batch_size=256, shuffle=True)
        model.train()
        for _ in range(max(1, epochs)):
            for xb, yb in loader:
                opt.zero_grad()
                loss = crit(model(xb), yb)
                loss.backward()
                opt.step()
        model.eval()
        with torch.no_grad():
            prob = torch.sigmoid(model(x_all)).cpu().numpy()
        out = np.full(len(df), 0.5, dtype=float)
        out[idx] = np.clip(prob, 0.0, 1.0)
        df[name] = out

    df['SeqPatchTST_UpProb30s'] = (df['SeqTCN_UpProb30s'] + df['SeqTransformer_UpProb30s']) / 2.0
    df['SeqModelConsensus_UpProb30s'] = (
        df['SeqLSTM_UpProb30s']
        + df['SeqTCN_UpProb30s']
        + df['SeqTransformer_UpProb30s']
        + df['SeqPatchTST_UpProb30s']
    ) / 4.0
    return df


def main():
    parser = argparse.ArgumentParser(description='Generate sequence-model meta-feature columns.')
    parser.add_argument('--input-csv', required=True)
    parser.add_argument('--output-csv', required=True)
    parser.add_argument('--backend', choices=['torch', 'proxy'], default='torch')
    parser.add_argument('--window', type=int, default=64)
    parser.add_argument('--epochs', type=int, default=4)
    parser.add_argument('--lr', type=float, default=1e-3)
    parser.add_argument('--max-rows', type=int, default=0)
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)
    if args.max_rows > 0:
        df = df.head(args.max_rows).copy()

    if args.backend == 'torch' and (torch is None or nn is None or optim is None):
        print('[SEQ] torch unavailable; using proxy fallback.')
        args.backend = 'proxy'

    try:
        if args.backend == 'torch':
            df = _torch_features(df, window=max(16, args.window), epochs=max(1, args.epochs), lr=args.lr)
        else:
            df = _proxy_features(df)
    except Exception as exc:
        print(f'[SEQ] backend={args.backend} failed ({exc}); using proxy fallback.')
        df = _proxy_features(df)

    for col in SEQ_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = 0.5
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.5).clip(0.0, 1.0)

    out = Path(args.output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f'[SEQ] wrote {out} rows={len(df)}')


if __name__ == '__main__':
    main()

