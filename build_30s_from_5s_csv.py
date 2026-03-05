import pandas as pd
import numpy as np
from pathlib import Path


def build_30s_from_5s_csv(input_csv, output_csv):
    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv)

    # 1. Parse your exact legacy string format
    df['dt_str'] = df['Timestamp'].str.extract(r'(\d{8} \d{2}:\d{2}:\d{2})')[0]
    df['datetime'] = pd.to_datetime(df['dt_str'], format='%Y%m%d %H:%M:%S')
    df.set_index('datetime', inplace=True)

    for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'Count', 'YesterdayClose']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Custom function to mathematically recalculate true WAP across 6 bars
    def safe_wap(x):
        vols = df.loc[x.index, 'Volume']
        sum_vol = vols.sum()
        if sum_vol > 0:
            return (x * vols).sum() / sum_vol
        else:
            return df.loc[x.index, 'Close'].iloc[-1] if not x.empty else np.nan

    print("Rolling 5s bars into 30s buckets...")
    # 2. Resample into 30-second blocks
    df_30s = df.resample('30s', closed='left', label='left').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'WAP': safe_wap,
        'Count': 'sum',
        'YesterdayClose': 'last',
        'Timestamp': 'first'  # Keeps the String "America/New_York" format alive
    }).dropna(subset=['Close'])

    df_30s.reset_index(inplace=True)

    # 3. Format it perfectly for the C#/Java parser
    output_df = pd.DataFrame()
    output_df['Timestamp'] = df_30s['Timestamp']
    output_df['Open'] = df_30s['Open'].map(lambda x: f"{x:.4f}")
    output_df['High'] = df_30s['High'].map(lambda x: f"{x:.4f}")
    output_df['Low'] = df_30s['Low'].map(lambda x: f"{x:.4f}")
    output_df['Close'] = df_30s['Close'].map(lambda x: f"{x:.4f}")
    output_df['Volume'] = df_30s['Volume'].map(lambda x: f"{x:.18f}")
    output_df['WAP'] = df_30s['WAP'].map(lambda x: f"{x:.18f}")
    output_df['Count'] = df_30s['Count'].astype(int)
    output_df['YesterdayClose'] = df_30s['YesterdayClose'].map(lambda x: f"{x:.4f}")

    output_df.to_csv(output_csv, index=False)
    print(f"Success! Converted {len(df)} 5s bars into {len(output_df)} 30s bars.")


def pick_source_csv(source_dir: Path) -> Path:
    preferred = sorted(source_dir.glob("*_clean.csv"))
    if preferred:
        return preferred[0]

    all_csv = sorted(source_dir.glob("*.csv"))
    if all_csv:
        return all_csv[0]

    raise FileNotFoundError(f"No CSV files found in {source_dir}")


# Run it
source_dir = Path(r"C:\data harvested")
source_csv = pick_source_csv(source_dir)
output_csv = source_dir / "TSLA_30Sec_Historical_Bulk.csv"

build_30s_from_5s_csv(str(source_csv), str(output_csv))
