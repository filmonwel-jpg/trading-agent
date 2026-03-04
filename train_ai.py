import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import ta

# ONNX EXPORT
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

print(">>> Loading TSLA data...")
# Make sure your CSV file is in the same directory
df = pd.read_csv('TSLA_1_Sec_Data.csv')

# Clean timestamps
df['time'] = df['time'].str.replace(' America/Edmonton', '')
df['time'] = pd.to_datetime(df['time'], format='%Y%m%d %H:%M:%S')
df = df.sort_values('time').reset_index(drop=True)
df['date'] = df['time'].dt.date

print(">>> Calculating Features...")

# 1. NEW: Yesterday's Close Context (Feature 13)
# Get the last close price of every date
daily_closes = df.groupby('date')['close'].last()
# Shift them so that "yesterday's close" is aligned with "today"
prev_day_closes = daily_closes.shift(1)
# Map that back to every row in the main dataframe
df['prev_day_close'] = df['date'].map(prev_day_closes)
# Calculate the percentage gap from yesterday's close
df['gap_from_prev_close'] = (df['close'] - df['prev_day_close']) / df['prev_day_close']

# 2. VWAP
df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
df['cum_vol'] = df.groupby('date')['volume'].cumsum()
df['cum_pv'] = df.groupby('date').apply(lambda x: (x['typical_price'] * x['volume']).cumsum()).reset_index(level=0, drop=True)
df['vwap'] = df['cum_pv'] / df['cum_vol']
df['dist_vwap'] = (df['close'] - df['vwap']) / df['vwap'] 

# 3. Bollinger Bands (2.5 Std Dev)
indicator_bb = ta.volatility.BollingerBands(close=df['close'], window=60, window_dev=2.5)
df['bb_lower_dist'] = (df['close'] - indicator_bb.bollinger_lband()) / df['close']
df['bb_upper_dist'] = (indicator_bb.bollinger_hband() - df['close']) / df['close']

# 4. MACD
macd = ta.trend.MACD(close=df['close'])
df['macd_diff'] = macd.macd_diff() 

# 5. Candle Micro-Structure
df['body_size'] = abs(df['close'] - df['open']) / df['close']
df['lower_wick'] = (df[['open', 'close']].min(axis=1) - df['low']) / df['close']
df['upper_wick'] = (df['high'] - df[['open', 'close']].max(axis=1)) / df['close']

# 6. ATR (Volatility)
df['atr_60'] = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=60).average_true_range()
df['atr_normalized'] = df['atr_60'] / df['close']

# 7. 5-Minute Moving Average
df['sma_5min'] = df['close'].rolling(window=300).mean()
df['dist_sma_5min'] = (df['close'] - df['sma_5min']) / df['sma_5min']

# 8. Daily High/Low Resistance
df['day_high'] = df.groupby('date')['high'].cummax()
df['day_low'] = df.groupby('date')['low'].cummin()
df['dist_to_day_high'] = (df['day_high'] - df['close']) / df['close']
df['dist_to_day_low'] = (df['close'] - df['day_low']) / df['close']

# 9. RSI
df['rsi_14'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()

# Cleanup NaN rows created by indicators
df = df.dropna().reset_index(drop=True)

print(">>> Labeling Target (0.15% Profit Target in 30 Seconds)...")
PROFIT_TARGET = 0.0015
LOOK_AHEAD = 30 
df['future_max'] = df['high'].rolling(window=LOOK_AHEAD).max().shift(-LOOK_AHEAD)
df['target'] = (df['future_max'] >= (df['close'] * (1 + PROFIT_TARGET))).astype(int)
df = df.dropna()

print(f">>> Total rows after calculating indicators: {len(df)}")
print(">>> Filtering for DIPS (RSI < 40)...")
setups = df[df['rsi_14'] < 40].copy()
print(f">>> Rows found for training: {len(setups)}")

if len(setups) < 100:
    print(">>> [CRITICAL] Not enough data for training! Lowering RSI filter to 50 just to test...")
    setups = df[df['rsi_14'] < 50].copy()

# EXACT 13 FEATURES - DO NOT CHANGE ORDER
features = [
    'dist_vwap', 'bb_lower_dist', 'bb_upper_dist', 'macd_diff', 
    'body_size', 'lower_wick', 'upper_wick', 'atr_normalized', 
    'dist_sma_5min', 'dist_to_day_high', 'dist_to_day_low', 'rsi_14',
    'gap_from_prev_close'  # <--- THE 13TH FEATURE
]

X = setups[features]
y = setups['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

print(f">>> Training Model on {len(X_train)} setups...")
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1, class_weight='balanced')
model.fit(X_train, y_train)

# --- EXPORT TO ONNX ---
print(">>> Exporting Model to ONNX (13 Features)...")
# Shape must be [None, 13]
initial_type = [('float_input', FloatTensorType([None, 13]))]
onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)

with open("tsla_dip_model.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())

print(">>> SUCCESS: Model exported as 'tsla_dip_model.onnx'")