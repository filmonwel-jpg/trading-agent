import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import ta
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

print(">>> Loading data for TSLA SHORT Model...")
df = pd.read_csv('TSLA_1_Sec_Data.csv')
df['time'] = pd.to_datetime(df['time'].str.replace(' America/Edmonton', ''), format='%Y%m%d %H:%M:%S')
df = df.sort_values('time').reset_index(drop=True)
df['date'] = df['time'].dt.date

# 1. Context: Yesterday's Close
daily_closes = df.groupby('date')['close'].last()
df['prev_day_close'] = df['date'].map(daily_closes.shift(1))
df['gap_from_prev_close'] = (df['close'] - df['prev_day_close']) / df['prev_day_close']

# 2. Indicators (Same 13 as Long model)
df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
df['vwap'] = (df['typical_price'] * df['volume']).cumsum() / df['volume'].cumsum() # Simplified for script
df['dist_vwap'] = (df['close'] - df['vwap']) / df['vwap'] 

indicator_bb = ta.volatility.BollingerBands(close=df['close'], window=60, window_dev=2.5)
df['bb_lower_dist'] = (df['close'] - indicator_bb.bollinger_lband()) / df['close']
df['bb_upper_dist'] = (indicator_bb.bollinger_hband() - df['close']) / df['close']

df['macd_diff'] = ta.trend.MACD(close=df['close']).macd_diff() 
df['body_size'] = abs(df['close'] - df['open']) / df['close']
df['lower_wick'] = (df[['open', 'close']].min(axis=1) - df['low']) / df['close']
df['upper_wick'] = (df['high'] - df[['open', 'close']].max(axis=1)) / df['close']
df['atr_norm'] = ta.volatility.AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=60).average_true_range() / df['close']
df['dist_sma'] = (df['close'] - df['close'].rolling(300).mean()) / df['close'].rolling(300).mean()
df['dist_high'] = (df.groupby('date')['high'].cummax() - df['close']) / df['close']
df['dist_low'] = (df['close'] - df.groupby('date')['low'].cummin()) / df['close']
df['rsi'] = ta.momentum.RSIIndicator(close=df['close'], window=14).rsi()

df = df.dropna()

# 3. Labeling for SHORT (Target 0.15% drop)
PROFIT_TARGET = 0.0015
LOOK_AHEAD = 30 
df['future_min'] = df['low'].rolling(window=LOOK_AHEAD).min().shift(-LOOK_AHEAD)
df['target'] = (df['future_min'] <= (df['close'] * (1 - PROFIT_TARGET))).astype(int)

# 4. Filter for RIPS
setups = df[df['rsi'] > 60].copy()

features = [
    'dist_vwap', 'bb_lower_dist', 'bb_upper_dist', 'macd_diff', 
    'body_size', 'lower_wick', 'upper_wick', 'atr_norm', 
    'dist_sma', 'dist_high', 'dist_low', 'rsi', 'gap_from_prev_close'
]

X = setups[features]
y = setups['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

print(f">>> Training Short Model on {len(X_train)} setups...")
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced')
model.fit(X_train, y_train)

# 5. Export
initial_type = [('float_input', FloatTensorType([None, 13]))]
onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=12)
with open("tsla_rip_model.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())
print(">>> SUCCESS: 'tsla_rip_model.onnx' exported.")